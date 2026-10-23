#include "NamespaceStore.h"
#include <algorithm>

namespace zb::mds {
namespace {
class ReaddirScanner final : public NamespaceScanner {
    struct Frame {
        std::string path, token;
        std::vector<zb::rpc::Dentry> page;
        size_t pos = 0;
        bool last_page = false;
    };
    const NamespaceStore& store_;
    SubtreeScanOptions options_;
    std::vector<Frame> stack_;
    NamespaceStatus failure_;
public:
    ReaddirScanner(const NamespaceStore& store, std::string path, SubtreeScanOptions options)
        : store_(store), options_(options) { stack_.push_back({std::move(path), {}, {}}); }
    NamespaceStatus Next(uint32_t max_entries, SubtreeScanBatch* batch) override {
        if (batch) *batch = {};
        if (!batch || !max_entries)
            return {zb::rpc::MDS_INVALID_ARGUMENT, "batch and positive max_entries required"};
        if (!failure_.ok()) return failure_;
        auto fail = [&](const std::string& error) {
            *batch = {};
            stack_.clear();
            failure_ = {zb::rpc::MDS_INTERNAL_ERROR, error};
            return failure_;
        };
        for (uint32_t work = 0; work < options_.work_budget && !stack_.empty()
                && batch->entries.size() < max_entries; ++work) {
            auto& frame = stack_.back();
            if (frame.pos == frame.page.size()) {
                if (frame.last_page) { stack_.pop_back(); continue; }
                bool more = false;
                std::string next, error;
                if (!store_.Readdir(frame.path, frame.token, std::min(max_entries, 128u),
                                     &frame.page, &more, &next, &error))
                    return fail(error.empty() ? "scan directory disappeared" : error);
                if (more && (frame.page.empty() || next == frame.token))
                    return fail("Readdir made no progress");
                frame.token = std::move(next);
                frame.pos = 0;
                frame.last_page = !more;
                continue;
            }
            const auto entry = frame.page[frame.pos++];
            zb::rpc::InodeAttr attr;
            std::string error;
            if (!store_.GetInode(entry.inode_id(), &attr, &error))
                return fail(error.empty() ? "scan inode disappeared" : error);
            if (attr.inode_id() != entry.inode_id() || attr.type() != entry.type())
                return fail("scan inode identity/type mismatch");
            if (entry.type() == zb::rpc::INODE_DIR) {
                auto path = frame.path == "/" ? "/" + entry.name() : frame.path + "/" + entry.name();
                stack_.push_back({std::move(path), {}, {}});
            }
            if (!options_.files_only || attr.type() == zb::rpc::INODE_FILE)
                batch->entries.push_back(std::move(attr));
        }
        batch->done = stack_.empty();
        return {};
    }
};
}

NamespaceStatus NamespaceStore::BeginSubtreeScan(const std::string& path,
    const SubtreeScanOptions& options, std::unique_ptr<NamespaceScanner>* scanner) const {
    if (scanner) scanner->reset();
    if (!scanner || !options.work_budget || path.empty() || path[0] != '/'
        || path.find('\0') != std::string::npos)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid subtree scan arguments"};
    // Normalize and validate once; backends share the same public scan contract.
    std::string normalized;
    for (size_t i = 0; i < path.size();) {
        if (path[i] == '/') { ++i; continue; }
        auto end = path.find('/', i);
        if (end == std::string::npos) end = path.size();
        auto name = path.substr(i, end - i);
        if (name == "." || name == "..")
            return {zb::rpc::MDS_INVALID_ARGUMENT, "dot components are not supported"};
        normalized += "/" + name;
        i = end;
    }
    if (normalized.empty()) normalized = "/";
    uint64_t id = 0;
    zb::rpc::InodeAttr attr;
    std::string error;
    if (!ResolvePath(normalized, &id, &attr, &error))
        return {error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                error.empty() ? "scan root not found" : error};
    if (attr.type() != zb::rpc::INODE_DIR)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "scan root must be a directory"};
    *scanner = std::make_unique<ReaddirScanner>(*this, normalized, options);
    return {};
}
} // namespace zb::mds
