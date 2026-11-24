#include "mds/masstree_meta/MasstreeImportService.h"
#include "mds/masstree_meta/MasstreeNamespaceCatalog.h"
#include "mds/storage/RocksMetaStore.h"

#include <iostream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <rocksdb/iterator.h>

namespace {

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  masstree_meta_build_tool --db_path=<path> --inspect\n"
        << "  masstree_meta_build_tool --db_path=<path> --masstree_root=<dir>"
           " --namespace_id=<id> --generation_id=<id> --path_prefix=<prefix>"
           " --template_id=<id> [--template_mode=<mode>] [--inode_start=<id>] [--verify_inode_samples=<n>]"
           " [--verify_dentry_samples=<n>] [--publish_route=0|1]\n";
}

bool ParseArgs(int argc, char* argv[], std::unordered_map<std::string, std::string>* args) {
    if (!args) {
        return false;
    }
    for (int i = 1; i < argc; ++i) {
        std::string token = argv[i];
        if (token.rfind("--", 0) != 0) {
            continue;
        }
        const size_t eq = token.find('=');
        if (eq == std::string::npos) {
            (*args)[token.substr(2)] = "1";
        } else {
            (*args)[token.substr(2, eq - 2)] = token.substr(eq + 1);
        }
    }
    return true;
}

std::string GetArg(const std::unordered_map<std::string, std::string>& args, const std::string& key) {
    auto it = args.find(key);
    return it == args.end() ? std::string() : it->second;
}

bool ParseU64(const std::string& value, uint64_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseU32(const std::string& value, uint32_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint32_t>(std::stoul(value));
        return true;
    } catch (...) {
        return false;
    }
}

std::string JsonString(const std::string& value) {
    std::ostringstream out;
    out << '"';
    for (unsigned char c : value) {
        if (c == '"' || c == '\\') out << '\\' << c;
        else if (c < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c);
        else out << c;
    }
    out << '"';
    return out.str();
}

std::string Cursor(const zb::mds::MasstreeOpticalClusterCursor& c) {
    return std::to_string(c.node_index) + "," + std::to_string(c.disk_index) + "," +
           std::to_string(c.image_index_in_disk) + "," + std::to_string(c.image_used_bytes);
}

// Offline inspection takes the same RocksDB lock as import. It also includes
// statistics without a published route, so a controller can detect a crash
// between statistics commit and generation publication instead of replaying it.
bool Inspect(zb::mds::RocksMetaStore* store, std::string* error) {
    zb::mds::MasstreeStatsStore stats_store(store);
    zb::mds::MasstreeClusterStatsRecord stats;
    zb::mds::MasstreeNamespaceCatalog catalog(store);
    std::vector<zb::mds::MasstreeNamespaceRoute> routes;
    if (!stats_store.LoadClusterStats(&stats, error) || !catalog.ListRoutes(&routes, error)) return false;
    std::ostringstream out;
    out << "{\"optical_layout_version\":" << stats.optical_layout_version
        << ",\"file_count\":" << stats.total_file_count
        << ",\"total_file_bytes\":" << JsonString(stats.total_file_bytes)
        << ",\"total_metadata_bytes\":" << JsonString(stats.total_metadata_bytes)
        << ",\"cursor\":" << JsonString(Cursor(stats.cursor)) << ",\"routes\":[";
    bool first = true;
    for (const auto& r : routes) {
        if (!first) out << ',';
        first = false;
        out << "{\"namespace_id\":" << JsonString(r.namespace_id)
            << ",\"generation_id\":" << JsonString(r.generation_id)
            << ",\"path_prefix\":" << JsonString(r.path_prefix)
            << ",\"manifest_path\":" << JsonString(r.manifest_path)
            << ",\"inode_min\":" << r.inode_min << ",\"inode_max\":" << r.inode_max << '}';
    }
    out << "],\"namespace_stats\":{";
    first = true;
    const std::string prefix = "MTS/namespace/";
    std::unique_ptr<rocksdb::Iterator> it(store->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix); it->Next()) {
        if (!first) out << ',';
        first = false;
        out << JsonString(it->key().ToString()) << ':' << JsonString(it->value().ToString());
    }
    if (!it->status().ok()) { *error = it->status().ToString(); return false; }
    out << "}}\n";
    std::cout << out.str();
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> args;
    if (!ParseArgs(argc, argv, &args)) {
        PrintUsage();
        return 1;
    }

    const std::string db_path = GetArg(args, "db_path");
    const std::string masstree_root = GetArg(args, "masstree_root");
    const std::string namespace_id = GetArg(args, "namespace_id");
    const std::string generation_id = GetArg(args, "generation_id");
    const std::string path_prefix = GetArg(args, "path_prefix");
    const std::string template_id = GetArg(args, "template_id");
    const std::string template_mode = GetArg(args, "template_mode");
    const bool inspect = args.find("inspect") != args.end();
    if (db_path.empty() || (!inspect && (masstree_root.empty() || namespace_id.empty() ||
        generation_id.empty() || path_prefix.empty() || template_id.empty()))) {
        PrintUsage();
        return 1;
    }

    zb::mds::RocksMetaStore store;
    std::string error;
    if (!store.Open(db_path, &error)) {
        std::cerr << "failed to open db: " << error << "\n";
        return 1;
    }

    if (inspect) {
        if (!Inspect(&store, &error)) { std::cerr << "inspect failed: " << error << '\n'; return 1; }
        return 0;
    }

    zb::mds::MasstreeNamespaceCatalog catalog(&store);
    zb::mds::MasstreeImportService importer(&store, &catalog);
    zb::mds::MasstreeImportService::TemplateImportRequest request;
    request.masstree_root = masstree_root;
    request.namespace_id = namespace_id;
    request.generation_id = generation_id;
    request.path_prefix = path_prefix;
    request.template_id = template_id;
    request.template_mode = template_mode;

    const std::string inode_start = GetArg(args, "inode_start");
    const std::string verify_inode_samples = GetArg(args, "verify_inode_samples");
    const std::string verify_dentry_samples = GetArg(args, "verify_dentry_samples");
    const std::string publish_route = GetArg(args, "publish_route");
    if (!inode_start.empty() && !ParseU64(inode_start, &request.inode_start)) {
        std::cerr << "invalid inode_start\n";
        return 1;
    }
    if (!verify_inode_samples.empty() &&
        !ParseU32(verify_inode_samples, &request.verify_inode_samples)) {
        std::cerr << "invalid verify_inode_samples\n";
        return 1;
    }
    if (!verify_dentry_samples.empty() &&
        !ParseU32(verify_dentry_samples, &request.verify_dentry_samples)) {
        std::cerr << "invalid verify_dentry_samples\n";
        return 1;
    }
    if (!publish_route.empty()) {
        request.publish_route = (publish_route != "0");
    }

    zb::mds::MasstreeImportService::Result result;
    if (!importer.ImportTemplateNamespace(request, &result, &error)) {
        std::cerr << "import failed: " << error << "\n";
        return 1;
    }

    std::cout << "manifest_path=" << result.manifest_path << "\n";
    std::cout << "root_inode_id=" << result.root_inode_id << "\n";
    std::cout << "inode_min=" << result.inode_min << "\n";
    std::cout << "inode_max=" << result.inode_max << "\n";
    std::cout << "inode_count=" << result.inode_count << "\n";
    std::cout << "dentry_count=" << result.dentry_count << "\n";
    std::cout << "file_count=" << result.file_count << "\n";
    std::cout << "inode_pages_bytes=" << result.inode_pages_bytes << "\n";
    return 0;
}
