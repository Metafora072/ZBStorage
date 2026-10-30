#include <fstream>
#include <unordered_map>
#include <stdexcept>

namespace {
using BulkEntry = MasstreeLHM::LhmNamespace::bulk_entry;
std::vector<BulkEntry> ReadBulkManifest(const char* path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot open manifest");
    std::string line;
    if (!std::getline(input, line) || line != "type\tpath")
        throw std::runtime_error("line 1: expected type<TAB>path header");
    std::vector<BulkEntry> entries;
    std::unordered_map<std::string, size_t> seen;
    uint64_t number = 1;
    const auto now = std::time(nullptr);
    while (std::getline(input, line)) {
        ++number;
        auto bad = [&](const char* reason) {
            throw std::runtime_error("line " + std::to_string(number) + ": " + reason);
        };
        const auto tab = line.find('\t');
        if (tab == std::string::npos || line.find('\t', tab + 1) != std::string::npos)
            bad("expected exactly two columns");
        const auto type = line.substr(0, tab), name = line.substr(tab + 1);
        if (type != "dir" && type != "file") bad("type must be dir or file");
        if (name.empty() || name[0] != '/' || name.find('\0') != std::string::npos ||
            name.find('\r') != std::string::npos) bad("invalid absolute path");
        auto parsed = MasstreeLHM::PathKey::parse_absolute_path(name);
        if (parsed.normalized_path != name) bad("path must be canonical");
        for (const auto& c : parsed.components)
            if (c.empty() || c == "." || c == ".." || c.size() > 64) bad("invalid component (maximum 64 bytes)");
        if (seen.count(name)) bad("duplicate path");
        BulkEntry e; e.path = name;
        if (entries.empty()) {
            if (name != "/" || type != "dir") bad("first entry must be dir /");
        } else {
            auto parent = name.substr(0, name.rfind('/'));
            if (parent.empty()) parent = "/";
            auto it = seen.find(parent);
            if (it == seen.end() || entries[it->second].attributes.type != 2)
                bad("parent directory must precede child");
            e.parent = it->second;
        }
        zb_lhm_full_inode attr{};
        attr.inode_id = 1; attr.type = type == "dir" ? 2 : 1;
        attr.mode = attr.type == 2 ? 0755 : 0644;
        attr.nlink = attr.type == 2 ? 2 : 1; attr.version = 1;
        attr.atime = attr.mtime = attr.ctime = now;
        const auto base = name == "/" ? name : parsed.components.back();
        attr.name_length = base.size(); std::memcpy(attr.file_name, base.data(), base.size());
        if (!zb::mds::lhm_codec::Encode(attr, e.attributes)) bad("invalid inode attributes");
        seen.emplace(name, entries.size()); entries.push_back(std::move(e));
    }
    if (input.bad()) throw std::runtime_error("manifest read failed");
    if (entries.empty()) throw std::runtime_error("manifest has no root");
    return entries;
}
}
extern "C" zb_lhm_context* zb_lhm_open_bulk(const char* file, const char* manifest,
    zb_lhm_bulk_stats* stats, char* error, size_t error_size) {
    if (stats) *stats = {};
    if (error && error_size) error[0] = 0;
    bool created = false;
    std::string path;
    try {
        if (!file || !*file || !manifest || !*manifest) throw std::runtime_error("missing output or manifest path");
        auto entries = ReadBulkManifest(manifest); // Validate before creating output.
        path = std::filesystem::absolute(file).string();
        int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0) throw std::runtime_error("output must be a new file in a writable directory");
        ::close(fd); created = true;
        return worker().run([&](threadinfo& ti) -> zb_lhm_context* {
            auto ctx = std::make_unique<zb_lhm_context>();
            try {
                ctx->ns.initialize(ti, path);
                MasstreeLHM::LhmNamespace::bulk_stats result;
                if (!ctx->ns.build_fresh_namespace(entries, result, ti))
                    throw std::runtime_error("bulk block/index construction failed");
                if (stats) *stats = {result.files, result.directories, result.inode_blocks, result.buckets};
                return ctx.release();
            } catch (...) { ctx->ns.destroy(ti); throw; }
        });
    } catch (const std::exception& e) {
        if (created) ::unlink(path.c_str());
        if (error && error_size) {
            std::strncpy(error, e.what(), error_size - 1); error[error_size - 1] = 0;
        }
        return nullptr;
    } catch (...) {
        if (created) ::unlink(path.c_str());
        if (error && error_size) {
            std::strncpy(error, "unexpected bulk import failure", error_size - 1); error[error_size - 1] = 0;
        }
        return nullptr;
    }
}
