#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

#include "mds/masstree_meta/MasstreeBulkImporter.h"
#include "mds/masstree_meta/MasstreeBulkMetaGenerator.h"

namespace fs = std::filesystem;

namespace {

constexpr uint64_t kDefaultInodeStart = 1;
constexpr uint32_t kDefaultMaxFilesPerLeafDir = 120;
constexpr uint32_t kDefaultMaxSubdirsPerDir = 100;
constexpr uint32_t kDefaultVerifyInodeSamples = 32;
constexpr uint32_t kDefaultVerifyDentrySamples = 32;

std::string TimestampToken() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        now.time_since_epoch()) %
                    1000;
    std::tm tm_now{};
#ifdef _WIN32
    localtime_s(&tm_now, &now_time);
#else
    localtime_r(&now_time, &tm_now);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm_now, "%Y%m%d_%H%M%S")
        << "_" << std::setw(3) << std::setfill('0') << ms.count();
    return oss.str();
}

bool ParsePositiveU64(const std::string& value, uint64_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        size_t consumed = 0;
        const uint64_t parsed = static_cast<uint64_t>(std::stoull(value, &consumed));
        if (consumed != value.size() || parsed == 0) {
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::string GetEnvString(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::string() : std::string(value);
}

std::string TrimCopy(std::string value) {
    while (!value.empty() && (value.back() == '\r' || value.back() == '\n' ||
                              value.back() == ' ' || value.back() == '\t')) {
        value.pop_back();
    }
    size_t start = 0;
    while (start < value.size() && (value[start] == ' ' || value[start] == '\t')) {
        ++start;
    }
    return value.substr(start);
}

std::string ReadRootPathFromBaseConf() {
    std::ifstream input("config/base.conf");
    if (!input.is_open()) {
        return {};
    }
    std::string line;
    while (std::getline(input, line)) {
        const size_t comment = line.find('#');
        if (comment != std::string::npos) {
            line.erase(comment);
        }
        line = TrimCopy(std::move(line));
        const std::string prefix = "ROOT_PATH=";
        if (line.rfind(prefix, 0) == 0) {
            return TrimCopy(line.substr(prefix.size()));
        }
    }
    return {};
}

fs::path DefaultOutputRoot() {
    const std::string explicit_output_root = GetEnvString("MASSTREE_RANDOM_IMPORT_OUTPUT_ROOT");
    if (!explicit_output_root.empty()) {
        return fs::path(explicit_output_root);
    }

    const std::string masstree_root = GetEnvString("MASSTREE_ROOT");
    if (!masstree_root.empty()) {
        return fs::path(masstree_root) / "generated_imports";
    }

    const std::string demo_root = GetEnvString("DEMO_ROOT");
    if (!demo_root.empty()) {
        return fs::path(demo_root) / "data" / "mds" / "masstree_meta" / "generated_imports";
    }

    const std::string configured_root = ReadRootPathFromBaseConf();
    if (!configured_root.empty()) {
        return fs::path(configured_root) / "data" / "mds" / "masstree_meta" / "generated_imports";
    }

    return fs::path(".demo_run") / "data" / "mds" / "masstree_meta" / "generated_imports";
}

void PrintUsage(const char* program) {
    std::cerr
        << "Usage:\n"
        << "  " << program << " <file_count>\n\n"
        << "Required:\n"
        << "  file_count  Number of file metadata records to generate and import.\n\n"
        << "Defaults:\n"
        << "  namespace_id              demo-ns-YYYYMMDD_HHMMSS\n"
        << "  generation_id             gen-YYYYMMDD_HHMMSS\n"
        << "  path_prefix               /masstree_demo/<namespace_id>\n"
        << "  output_root               $MASSTREE_RANDOM_IMPORT_OUTPUT_ROOT, or\n"
        << "                            $MASSTREE_ROOT/generated_imports, or\n"
        << "                            $DEMO_ROOT/data/mds/masstree_meta/generated_imports, or\n"
        << "                            config/base.conf ROOT_PATH/data/mds/masstree_meta/generated_imports\n"
        << "  max_files_per_leaf_dir    " << kDefaultMaxFilesPerLeafDir << "\n"
        << "  max_subdirs_per_dir       " << kDefaultMaxSubdirsPerDir << "\n";
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc == 2 && (std::string(argv[1]) == "-h" || std::string(argv[1]) == "--help")) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (argc != 2) {
        PrintUsage(argv[0]);
        return 1;
    }

    uint64_t file_count = 0;
    if (!ParsePositiveU64(argv[1], &file_count)) {
        std::cerr << "invalid file_count: " << argv[1] << "\n";
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string timestamp = TimestampToken();
    const std::string namespace_id = "demo-ns-" + timestamp;
    const std::string generation_id = "gen-" + timestamp;
    const std::string path_prefix = "/masstree_demo/" + namespace_id;
    const fs::path output_root = DefaultOutputRoot();

    zb::mds::MasstreeBulkMetaGenerator::Request generate_request;
    generate_request.output_root = output_root.string();
    generate_request.namespace_id = namespace_id;
    generate_request.generation_id = generation_id;
    generate_request.path_prefix = path_prefix;
    generate_request.source_mode = "synthetic";
    generate_request.inode_start = kDefaultInodeStart;
    generate_request.file_count = file_count;
    generate_request.max_files_per_leaf_dir = kDefaultMaxFilesPerLeafDir;
    generate_request.max_subdirs_per_dir = kDefaultMaxSubdirsPerDir;

    zb::mds::MasstreeBulkMetaGenerator generator;
    zb::mds::MasstreeBulkMetaGenerator::Result generate_result;
    std::string error;
    if (!generator.Generate(generate_request, &generate_result, &error)) {
        std::cerr << "generate failed: " << error << "\n";
        return 1;
    }

    zb::mds::MasstreeBulkImporter::Request import_request;
    import_request.manifest_path = generate_result.manifest_path;
    import_request.verify_inode_samples = kDefaultVerifyInodeSamples;
    import_request.verify_dentry_samples = kDefaultVerifyDentrySamples;

    zb::mds::MasstreeBulkImporter importer;
    zb::mds::MasstreeBulkImporter::Result import_result;
    if (!importer.Import(import_request, nullptr, &import_result, &error)) {
        std::cerr << "import failed: " << error << "\n";
        return 1;
    }

    std::cout << "namespace_id=" << namespace_id << "\n";
    std::cout << "generation_id=" << generation_id << "\n";
    std::cout << "path_prefix=" << path_prefix << "\n";
    std::cout << "output_root=" << output_root.string() << "\n";
    std::cout << "staging_dir=" << generate_result.staging_dir << "\n";
    std::cout << "manifest_path=" << generate_result.manifest_path << "\n";
    std::cout << "inode_records_path=" << generate_result.inode_records_path << "\n";
    std::cout << "dentry_records_path=" << generate_result.dentry_records_path << "\n";
    std::cout << "verify_manifest_path=" << generate_result.verify_manifest_path << "\n";
    std::cout << "structure_stats_path=" << generate_result.structure_stats_path << "\n";
    std::cout << "source_mode=" << generate_result.source_mode << "\n";
    std::cout << "root_inode_id=" << generate_result.root_inode_id << "\n";
    std::cout << "inode_min=" << generate_result.inode_min << "\n";
    std::cout << "inode_max=" << generate_result.inode_max << "\n";
    std::cout << "target_file_count=" << generate_result.target_file_count << "\n";
    std::cout << "file_count=" << import_result.file_count << "\n";
    std::cout << "actual_file_count=" << import_result.file_count << "\n";
    std::cout << "inode_count=" << generate_result.inode_count << "\n";
    std::cout << "dentry_count=" << generate_result.dentry_count << "\n";
    std::cout << "dir_count=" << generate_result.dir_count << "\n";
    std::cout << "level1_dir_count=" << generate_result.level1_dir_count << "\n";
    std::cout << "leaf_dir_count=" << generate_result.leaf_dir_count << "\n";
    std::cout << "max_depth=" << generate_result.max_depth << "\n";
    std::cout << "total_file_bytes=" << generate_result.total_file_bytes << "\n";
    std::cout << "avg_file_size_bytes=" << generate_result.avg_file_size_bytes << "\n";
    std::cout << "inode_imported=" << import_result.inode_imported << "\n";
    std::cout << "dentry_imported=" << import_result.dentry_imported << "\n";
    std::cout << "inode_page_count=" << import_result.inode_page_count << "\n";
    std::cout << "dentry_page_count=" << import_result.dentry_page_count << "\n";
    std::cout << "inode_pages_bytes=" << import_result.inode_pages_bytes << "\n";
    std::cout << "verified_inode_samples=" << import_result.verified_inode_samples << "\n";
    std::cout << "verified_dentry_samples=" << import_result.verified_dentry_samples << "\n";
    std::cout << "start_global_image_id=" << import_result.start_global_image_id << "\n";
    std::cout << "end_global_image_id=" << import_result.end_global_image_id << "\n";
    std::cout << "status=completed\n";
    return 0;
}
