#include "sim/SimCluster.h"
#include "sim/SimFileIO.h"
#include "sim/masstree/SimMasstreeStore.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultOnlineDiskCapacityBytes = 2000000000000ULL;
constexpr uint64_t kDefaultDisksPerLogicalNode = 24ULL;
constexpr uint64_t kDefaultOpticalNodes = 10000ULL;
constexpr uint64_t kDefaultOpticalDiscsPerNode = 10000ULL;
constexpr uint64_t kDefaultOpticalOneTbDiscsPerNode = kDefaultOpticalDiscsPerNode / 2ULL;
constexpr uint64_t kDefaultOpticalTenTbDiscsPerNode = kDefaultOpticalDiscsPerNode / 2ULL;
constexpr uint64_t kDecimalTb = 1000000000000ULL;
constexpr uint64_t kDefaultDiskNodeCapacityBytes = kDefaultDisksPerLogicalNode * kDefaultOnlineDiskCapacityBytes;
constexpr uint64_t kDefaultOpticalNodeCapacityBytes =
    kDefaultOpticalOneTbDiscsPerNode * kDecimalTb + kDefaultOpticalTenTbDiscsPerNode * 10ULL * kDecimalTb;

struct Options {
    std::string root{"sim_root"};
    std::string scenario{"interactive"};
    uint32_t disk_nodes{100};
    uint32_t optical_nodes{static_cast<uint32_t>(kDefaultOpticalNodes)};
    uint64_t disk_capacity_gb{0};
    uint64_t disk_capacity_bytes{kDefaultDiskNodeCapacityBytes};
    uint64_t optical_capacity_gb{0};
    uint64_t optical_capacity_bytes{kDefaultOpticalNodeCapacityBytes};

    std::string path{"/demo/sim_file.bin"};
    uint64_t size_mb{100};
    uint32_t chunk_kb{1024};
    bool sync_on_close{false};

    uint64_t import_files{1000000};
    uint64_t default_file_size_mb{100};
    std::string path_list_file;

    uint32_t query_samples{1};
    bool query_sleep{true};
    bool reset_files{true};
    bool reset_imports{true};

    std::string template_id;
    std::string namespace_id{"demo-ns"};
    std::string generation_id{"gen-001"};
    std::string path_prefix;
    uint64_t target_file_count{0};
    std::string masstree_query_mode{"random_path_lookup"};
    uint64_t inode_id{0};
    uint32_t readdir_limit{100};
    std::string repeat_dir_prefix{"copy"};
    bool leaf_nodes_are_files{false};
    uint32_t max_files_per_leaf_dir{2048};
    uint32_t max_subdirs_per_dir{256};
    uint64_t inode_start{0};
};

std::string FormatDouble(double value, int precision = 2) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

std::string ToLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool ParseUint64(const std::string& key, const std::string& value, uint64_t* out, std::string* error) {
    if (!out) {
        return false;
    }
    try {
        size_t parsed = 0;
        const uint64_t number = std::stoull(value, &parsed, 10);
        if (parsed != value.size()) {
            throw std::invalid_argument("trailing characters");
        }
        *out = number;
        return true;
    } catch (...) {
        if (error) {
            *error = "invalid uint64 for --" + key + ": " + value;
        }
        return false;
    }
}

bool ParseUint32(const std::string& key, const std::string& value, uint32_t* out, std::string* error) {
    uint64_t number = 0;
    if (!ParseUint64(key, value, &number, error)) {
        return false;
    }
    if (number > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "value is too large for --" + key + ": " + value;
        }
        return false;
    }
    *out = static_cast<uint32_t>(number);
    return true;
}

bool ParseBool(const std::string& key, const std::string& value, bool* out, std::string* error) {
    if (!out) {
        return false;
    }
    const std::string normalized = ToLowerCopy(value);
    if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on") {
        *out = true;
        return true;
    }
    if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off") {
        *out = false;
        return true;
    }
    if (error) {
        *error = "invalid bool for --" + key + ": " + value;
    }
    return false;
}

void PrintUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0 << " [--key=value ...]\n\n"
        << "Scenarios:\n"
        << "  --scenario=interactive     Show menu and run operations by number\n"
        << "  --scenario=cluster|stats   Print simulated cluster nodes and statistics\n"
        << "  --scenario=io|write        Write one file under --root and read it back\n"
        << "  --scenario=import          Simulate direct optical-node batch import\n"
        << "  --scenario=query           Return simulated 100-200ms metadata query latency\n"
        << "  --scenario=masstree_template Generate standalone Masstree template metadata\n"
        << "  --scenario=masstree_import Import a template into a namespace generation\n"
        << "  --scenario=masstree_query  Query standalone Masstree metadata\n"
        << "  --scenario=all             Run cluster, io, import, query, cluster\n"
        << "  --scenario=reset           Remove simulated files/import manifests\n\n"
        << "Common options:\n"
        << "  --root=PATH                Default: sim_root\n"
        << "  --disk_nodes=N             Default: 100 (1 real + 99 virtual logical nodes)\n"
        << "  --optical_nodes=N          Default: 10000\n"
        << "  --disk_capacity_bytes=N    Default: 48000000000000 (24 * 2000000000000)\n"
        << "  --disk_capacity_gb=N       Optional GiB override for disk_capacity_bytes\n"
        << "  --optical_capacity_bytes=N Default: 55000000000000000 (5000 * 1TB + 5000 * 10TB)\n"
        << "  --optical_capacity_gb=N    Optional GiB override for optical_capacity_bytes\n\n"
        << "I/O options:\n"
        << "  --path=/demo/a.bin         Default: /demo/sim_file.bin\n"
        << "  --size_mb=N                Default: 100\n"
        << "  --chunk_kb=N               Default: 1024\n"
        << "  --sync_on_close=true|false Default: false\n\n"
        << "Import/query/reset options:\n"
        << "  --import_files=N           Default: 1000000\n"
        << "  --default_file_size_mb=N   Default: 100\n"
        << "  --path_list_file=PATH      Optional\n"
        << "  --query_samples=N          Default: 1\n"
        << "  --query_sleep=true|false   Default: true\n"
        << "  --reset_files=true|false   Default: true\n"
        << "  --reset_imports=true|false Default: true\n\n"
        << "Masstree options:\n"
        << "  --template_id=ID           Required for masstree_template/import\n"
        << "  --namespace_id=ID          Default: demo-ns\n"
        << "  --generation_id=ID         Default: gen-001\n"
        << "  --path_prefix=PATH         Default: /masstree_demo/<namespace_id>\n"
        << "  --target_file_count=N      Template target file count\n"
        << "  --repeat_dir_prefix=NAME   Default: copy\n"
        << "  --leaf_nodes_are_files=true|false Default: false\n"
        << "  --inode_start=N            Optional namespace import inode start\n"
        << "  --masstree_query_mode=MODE Default: random_path_lookup\n"
        << "  --inode_id=N               Query inode for get_inode/build_full_path\n"
        << "  --readdir_limit=N          Default: 100\n";
}

bool ApplyOption(Options* options, const std::string& key, const std::string& value, std::string* error) {
    if (key == "root") {
        options->root = value;
    } else if (key == "scenario") {
        options->scenario = value;
    } else if (key == "disk_nodes") {
        return ParseUint32(key, value, &options->disk_nodes, error);
    } else if (key == "optical_nodes") {
        return ParseUint32(key, value, &options->optical_nodes, error);
    } else if (key == "disk_capacity_gb") {
        return ParseUint64(key, value, &options->disk_capacity_gb, error);
    } else if (key == "disk_capacity_bytes") {
        return ParseUint64(key, value, &options->disk_capacity_bytes, error);
    } else if (key == "optical_capacity_gb") {
        return ParseUint64(key, value, &options->optical_capacity_gb, error);
    } else if (key == "optical_capacity_bytes") {
        return ParseUint64(key, value, &options->optical_capacity_bytes, error);
    } else if (key == "path") {
        options->path = value;
    } else if (key == "size_mb") {
        return ParseUint64(key, value, &options->size_mb, error);
    } else if (key == "chunk_kb") {
        return ParseUint32(key, value, &options->chunk_kb, error);
    } else if (key == "sync_on_close") {
        return ParseBool(key, value, &options->sync_on_close, error);
    } else if (key == "import_files") {
        return ParseUint64(key, value, &options->import_files, error);
    } else if (key == "default_file_size_mb") {
        return ParseUint64(key, value, &options->default_file_size_mb, error);
    } else if (key == "path_list_file") {
        options->path_list_file = value;
    } else if (key == "query_samples") {
        return ParseUint32(key, value, &options->query_samples, error);
    } else if (key == "query_sleep") {
        return ParseBool(key, value, &options->query_sleep, error);
    } else if (key == "reset_files") {
        return ParseBool(key, value, &options->reset_files, error);
    } else if (key == "reset_imports") {
        return ParseBool(key, value, &options->reset_imports, error);
    } else if (key == "template_id") {
        options->template_id = value;
    } else if (key == "namespace_id") {
        options->namespace_id = value;
    } else if (key == "generation_id") {
        options->generation_id = value;
    } else if (key == "path_prefix") {
        options->path_prefix = value;
    } else if (key == "target_file_count") {
        return ParseUint64(key, value, &options->target_file_count, error);
    } else if (key == "masstree_query_mode") {
        options->masstree_query_mode = value;
    } else if (key == "inode_id") {
        return ParseUint64(key, value, &options->inode_id, error);
    } else if (key == "readdir_limit") {
        return ParseUint32(key, value, &options->readdir_limit, error);
    } else if (key == "repeat_dir_prefix") {
        options->repeat_dir_prefix = value;
    } else if (key == "leaf_nodes_are_files") {
        return ParseBool(key, value, &options->leaf_nodes_are_files, error);
    } else if (key == "max_files_per_leaf_dir") {
        return ParseUint32(key, value, &options->max_files_per_leaf_dir, error);
    } else if (key == "max_subdirs_per_dir") {
        return ParseUint32(key, value, &options->max_subdirs_per_dir, error);
    } else if (key == "inode_start") {
        return ParseUint64(key, value, &options->inode_start, error);
    } else {
        if (error) {
            *error = "unknown option: --" + key;
        }
        return false;
    }
    return true;
}

bool ParseArgs(int argc, char* argv[], Options* options, bool* show_help, std::string* error) {
    if (show_help) {
        *show_help = false;
    }
    for (int i = 1; i < argc; ++i) {
        std::string token = argv[i];
        if (token == "--help" || token == "-h") {
            if (show_help) {
                *show_help = true;
            }
            return true;
        }
        if (token.rfind("--", 0) != 0) {
            if (error) {
                *error = "arguments must use --key=value form: " + token;
            }
            return false;
        }
        token = token.substr(2);
        const size_t eq = token.find('=');
        const std::string key = eq == std::string::npos ? token : token.substr(0, eq);
        const std::string value = eq == std::string::npos ? "true" : token.substr(eq + 1);
        if (key.empty()) {
            if (error) {
                *error = "empty option name";
            }
            return false;
        }
        if (!ApplyOption(options, key, value, error)) {
            return false;
        }
    }
    return true;
}

std::vector<std::string> SplitWords(const std::string& line) {
    std::vector<std::string> words;
    std::istringstream input(line);
    std::string word;
    while (input >> word) {
        words.push_back(word);
    }
    return words;
}

bool ApplyInteractiveOverrides(Options* options,
                               const std::vector<std::string>& words,
                               size_t begin,
                               std::string* error) {
    for (size_t i = begin; i < words.size(); ++i) {
        std::string token = words[i];
        if (token.rfind("--", 0) == 0) {
            token = token.substr(2);
        }
        const size_t eq = token.find('=');
        if (eq == std::string::npos || eq == 0) {
            if (error) {
                *error = "interactive arguments must use key=value: " + words[i];
            }
            return false;
        }
        const std::string key = token.substr(0, eq);
        const std::string value = token.substr(eq + 1);
        if (!ApplyOption(options, key, value, error)) {
            return false;
        }
    }
    return true;
}

void PrintSection(const std::string& title) {
    std::cout << "\n==== " << title << " ====\n";
}

void PrintBoolMetric(const std::string& key, bool value) {
    std::cout << key << "=" << (value ? "true" : "false") << "\n";
}

void PrintByteMetric(const std::string& key, uint64_t value) {
    std::cout << key << "=" << value << " (" << zb::sim::FormatBytes(value) << ")\n";
}

double ThroughputMiBS(uint64_t bytes, uint64_t elapsed_us) {
    if (elapsed_us == 0) {
        return 0.0;
    }
    return (static_cast<double>(bytes) / static_cast<double>(kMiB)) /
           (static_cast<double>(elapsed_us) / 1000000.0);
}

void PrintCluster(const zb::sim::SimCluster& cluster) {
    PrintSection("Sim Cluster");
    const auto stats = cluster.Stats();
    std::cout << "cluster_nodes=" << cluster.nodes().size() << "\n";
    std::cout << "disk_nodes=" << stats.disk_node_count << "\n";
    std::cout << "optical_nodes=" << stats.optical_node_count << "\n";
    std::cout << "online_nodes=" << stats.online_node_count << "\n";
    PrintByteMetric("total_capacity_bytes", stats.total_capacity_bytes);
    PrintByteMetric("used_capacity_bytes", stats.used_capacity_bytes);
    PrintByteMetric("free_capacity_bytes", stats.free_capacity_bytes);
    std::cout << "total_file_count=" << stats.total_file_count << "\n";
    PrintByteMetric("total_file_bytes", stats.total_file_bytes);
    std::cout << "disk_file_count=" << stats.disk_file_count << "\n";
    PrintByteMetric("disk_file_bytes", stats.disk_file_bytes);
    std::cout << "imported_file_count=" << stats.imported_file_count << "\n";
    PrintByteMetric("imported_file_bytes", stats.imported_file_bytes);

    for (const auto& node : cluster.nodes()) {
        std::cout << "node id=" << node.id
                  << " type=" << zb::sim::SimNodeTypeName(node.type)
                  << " online=" << (node.online ? "true" : "false")
                  << " used_capacity=" << zb::sim::FormatBytes(node.used_bytes)
                  << " capacity=" << zb::sim::FormatBytes(node.capacity_bytes) << "\n";
    }
}

bool RunIo(zb::sim::SimCluster* cluster, const Options& flags) {
    PrintSection("Sim File IO");
    zb::sim::SimWriteOptions options;
    options.logical_path = flags.path;
    options.size_bytes = flags.size_mb * kMiB;
    options.chunk_size_bytes = std::max<uint32_t>(1U, flags.chunk_kb) * 1024U;
    options.sync_on_close = flags.sync_on_close;

    zb::sim::SimWriteResult result;
    std::string error;
    if (!cluster->WriteAndVerify(options, &result, &error)) {
        std::cerr << "io_failed=" << error << "\n";
        return false;
    }

    std::cout << "logical_path=" << result.logical_path << "\n";
    std::cout << "physical_path=" << result.physical_path.string() << "\n";
    std::cout << "target_node=" << result.node_id << "\n";
    std::cout << "bytes_written=" << result.io.bytes_written << "\n";
    std::cout << "bytes_read=" << result.io.bytes_read << "\n";
    std::cout << "write_hash=" << zb::sim::FormatHex64(result.io.write_hash) << "\n";
    std::cout << "read_hash=" << zb::sim::FormatHex64(result.io.read_hash) << "\n";
    PrintBoolMetric("hash_match", result.hash_match);
    std::cout << "write_latency_ms=" << FormatDouble(static_cast<double>(result.io.write_elapsed_us) / 1000.0)
              << "\n";
    std::cout << "read_latency_ms=" << FormatDouble(static_cast<double>(result.io.read_elapsed_us) / 1000.0)
              << "\n";
    std::cout << "write_throughput_mib_s="
              << FormatDouble(ThroughputMiBS(result.io.bytes_written, result.io.write_elapsed_us)) << "\n";
    std::cout << "read_throughput_mib_s="
              << FormatDouble(ThroughputMiBS(result.io.bytes_read, result.io.read_elapsed_us)) << "\n";
    std::cout << "after_total_file_count=" << result.stats_after.total_file_count << "\n";
    PrintByteMetric("after_used_capacity_bytes", result.stats_after.used_capacity_bytes);
    PrintByteMetric("after_free_capacity_bytes", result.stats_after.free_capacity_bytes);
    return true;
}

bool RunImport(zb::sim::SimCluster* cluster, const Options& flags) {
    PrintSection("Sim Batch Import");
    zb::sim::SimImportOptions options;
    options.import_files = flags.import_files;
    options.default_file_size_bytes = flags.default_file_size_mb * kMiB;
    if (!flags.path_list_file.empty()) {
        options.path_list_file = flags.path_list_file;
    }

    zb::sim::SimImportResult result;
    std::string error;
    if (!cluster->Import(options, &result, &error)) {
        std::cerr << "import_failed=" << error << "\n";
        return false;
    }
    std::cout << "job_id=" << result.job_id << "\n";
    std::cout << "job_status=completed\n";
    std::cout << "target_node=" << result.target_node_id << "\n";
    std::cout << "import_file_count=" << result.imported_file_count << "\n";
    PrintByteMetric("import_total_file_bytes", result.imported_file_bytes);
    std::cout << "manifest_path=" << result.manifest_path.string() << "\n";
    std::cout << "before_total_file_count=" << result.stats_before.total_file_count << "\n";
    PrintByteMetric("before_used_capacity_bytes", result.stats_before.used_capacity_bytes);
    std::cout << "after_total_file_count=" << result.stats_after.total_file_count << "\n";
    PrintByteMetric("after_used_capacity_bytes", result.stats_after.used_capacity_bytes);
    PrintByteMetric("after_free_capacity_bytes", result.stats_after.free_capacity_bytes);
    return true;
}

bool RunQuery(zb::sim::SimCluster* cluster, const Options& flags) {
    PrintSection("Sim Metadata Query");
    const auto result = cluster->Query(flags.query_samples, flags.query_sleep);
    std::cout << "query_samples=" << result.samples << "\n";
    std::cout << "query_success_count=" << result.success_count << "\n";
    std::cout << "query_failure_count=" << result.failure_count << "\n";
    std::cout << "min_query_latency_ms=" << result.min_latency_ms << "\n";
    std::cout << "max_query_latency_ms=" << result.max_latency_ms << "\n";
    std::cout << "avg_query_latency_ms=" << result.avg_latency_ms << "\n";
    return result.failure_count == 0;
}

bool RunReset(zb::sim::SimCluster* cluster, const Options& flags) {
    PrintSection("Sim Reset");
    std::string error;
    if (!cluster->Reset(flags.reset_files, flags.reset_imports, &error)) {
        std::cerr << "reset_failed=" << error << "\n";
        return false;
    }
    PrintBoolMetric("reset_files", flags.reset_files);
    PrintBoolMetric("reset_imports", flags.reset_imports);
    PrintCluster(*cluster);
    return true;
}

bool RunMasstreeTemplate(const Options& flags) {
    PrintSection("Sim Masstree Template");
    zb::sim::masstree::SimMasstreeStore store;
    std::string error;
    if (!store.Init(flags.root, &error)) {
        std::cerr << "masstree_init_failed=" << error << "\n";
        return false;
    }
    zb::sim::masstree::TemplateRequest request;
    request.template_id = flags.template_id;
    request.path_list_file = flags.path_list_file;
    request.target_file_count = flags.target_file_count;
    request.default_file_size_bytes = flags.default_file_size_mb * kMiB;
    request.repeat_dir_prefix = flags.repeat_dir_prefix;
    request.leaf_nodes_are_files = flags.leaf_nodes_are_files;
    request.max_files_per_leaf_dir = flags.max_files_per_leaf_dir;
    request.max_subdirs_per_dir = flags.max_subdirs_per_dir;
    zb::sim::masstree::TemplateResult result;
    if (!store.GenerateTemplate(request, &result, &error)) {
        std::cerr << "masstree_template_failed=" << error << "\n";
        return false;
    }
    const auto& manifest = result.manifest;
    std::cout << "template_id=" << manifest.template_id << "\n";
    std::cout << "manifest_path=" << manifest.manifest_path.string() << "\n";
    std::cout << "root_inode_id=" << manifest.root_inode_id << "\n";
    std::cout << "inode_count=" << manifest.inode_count << "\n";
    std::cout << "dentry_count=" << manifest.dentry_count << "\n";
    std::cout << "inode_page_count=" << manifest.inode_page_count << "\n";
    std::cout << "dentry_page_count=" << manifest.dentry_page_count << "\n";
    std::cout << "file_count=" << manifest.file_count << "\n";
    std::cout << "dir_count=" << manifest.dir_count << "\n";
    std::cout << "target_file_count=" << manifest.target_file_count << "\n";
    std::cout << "template_repeat_count=" << manifest.template_repeat_count << "\n";
    std::cout << "level1_dir_count=" << manifest.level1_dir_count << "\n";
    std::cout << "leaf_dir_count=" << manifest.leaf_dir_count << "\n";
    PrintByteMetric("total_file_bytes", manifest.total_file_bytes);
    PrintByteMetric("inode_pages_bytes", manifest.inode_pages_bytes);
    PrintByteMetric("dentry_pages_bytes", manifest.dentry_pages_bytes);
    return true;
}

bool RunMasstreeImport(const Options& flags) {
    PrintSection("Sim Masstree Import");
    zb::sim::masstree::SimMasstreeStore store;
    std::string error;
    if (!store.Init(flags.root, &error)) {
        std::cerr << "masstree_init_failed=" << error << "\n";
        return false;
    }
    zb::sim::masstree::NamespaceImportRequest request;
    request.template_id = flags.template_id;
    request.namespace_id = flags.namespace_id;
    request.generation_id = flags.generation_id;
    request.path_prefix = flags.path_prefix;
    request.inode_start = flags.inode_start;
    zb::sim::masstree::NamespaceImportResult result;
    if (!store.ImportNamespace(request, &result, &error)) {
        std::cerr << "masstree_import_failed=" << error << "\n";
        return false;
    }
    const auto& manifest = result.manifest;
    std::cout << "namespace_id=" << manifest.namespace_id << "\n";
    std::cout << "generation_id=" << manifest.generation_id << "\n";
    std::cout << "path_prefix=" << manifest.path_prefix << "\n";
    std::cout << "manifest_path=" << manifest.manifest_path.string() << "\n";
    std::cout << "root_inode_id=" << manifest.root_inode_id << "\n";
    std::cout << "inode_count=" << manifest.inode_count << "\n";
    std::cout << "dentry_count=" << manifest.dentry_count << "\n";
    std::cout << "file_count=" << manifest.file_count << "\n";
    PrintByteMetric("total_file_bytes", manifest.total_file_bytes);
    std::cout << "start_global_image_id=" << manifest.start_global_image_id << "\n";
    std::cout << "end_global_image_id=" << manifest.end_global_image_id << "\n";
    std::cout << "start_cursor="
              << manifest.start_cursor.node_index << ","
              << manifest.start_cursor.disk_index << ","
              << manifest.start_cursor.image_index << ","
              << manifest.start_cursor.image_used_bytes << "\n";
    std::cout << "end_cursor="
              << manifest.end_cursor.node_index << ","
              << manifest.end_cursor.disk_index << ","
              << manifest.end_cursor.image_index << ","
              << manifest.end_cursor.image_used_bytes << "\n";
    return true;
}

bool RunMasstreeQuery(const Options& flags) {
    PrintSection("Sim Masstree Query");
    zb::sim::masstree::SimMasstreeStore store;
    std::string error;
    if (!store.Init(flags.root, &error)) {
        std::cerr << "masstree_init_failed=" << error << "\n";
        return false;
    }
    uint32_t success = 0;
    uint32_t failure = 0;
    uint64_t total_latency = 0;
    uint64_t min_latency = std::numeric_limits<uint64_t>::max();
    uint64_t max_latency = 0;
    for (uint32_t i = 0; i < std::max<uint32_t>(1, flags.query_samples); ++i) {
        zb::sim::masstree::QueryResult sample;
        if (store.Query(flags.namespace_id,
                        flags.masstree_query_mode,
                        flags.path,
                        flags.inode_id,
                        flags.readdir_limit,
                        flags.query_sleep,
                        &sample,
                        &error)) {
            ++success;
            total_latency += sample.latency_ms;
            min_latency = std::min<uint64_t>(min_latency, sample.latency_ms);
            max_latency = std::max<uint64_t>(max_latency, sample.latency_ms);
            std::cout << "sample_index=" << (i + 1)
                      << " ok=true"
                      << " latency_ms=" << sample.latency_ms
                      << " mode=" << sample.query_mode
                      << " full_path=" << sample.full_path
                      << " inode_id=" << sample.inode_id
                      << " type=" << (sample.record.type == zb::sim::masstree::SimInodeType::Dir ? "dir" : "file")
                      << " size_bytes=" << sample.record.size_bytes;
            if (sample.query_mode == "get_optical_location") {
                std::cout << " optical_node=" << sample.optical_location.node_index
                          << " optical_disk=" << sample.optical_location.disk_index
                          << " optical_image=" << sample.optical_location.image_index
                          << " optical_offset=" << sample.optical_location.offset_bytes
                          << " optical_length=" << sample.optical_location.length_bytes;
            }
            if (sample.query_mode == "readdir") {
                std::cout << " entry_count=" << sample.dentries.size()
                          << " has_more=" << (sample.has_more ? "true" : "false")
                          << " next_token=" << sample.next_token;
            }
            std::cout << "\n";
            if (sample.query_mode == "readdir") {
                for (const auto& dentry : sample.dentries) {
                    std::cout << "  dentry parent_inode=" << dentry.parent_inode_id
                              << " name=" << dentry.name
                              << " child_inode=" << dentry.child_inode_id
                              << " type=" << (dentry.type == zb::sim::masstree::SimInodeType::Dir ? "dir" : "file")
                              << "\n";
                }
            }
        } else {
            ++failure;
            std::cout << "sample_index=" << (i + 1)
                      << " ok=false"
                      << " error=" << error << "\n";
        }
    }
    const uint32_t samples = std::max<uint32_t>(1, flags.query_samples);
    std::cout << "query_samples=" << samples << "\n";
    std::cout << "query_success_count=" << success << "\n";
    std::cout << "query_failure_count=" << failure << "\n";
    std::cout << "min_query_latency_ms=" << (success == 0 ? 0 : min_latency) << "\n";
    std::cout << "max_query_latency_ms=" << max_latency << "\n";
    std::cout << "avg_query_latency_ms=" << (success == 0 ? 0 : total_latency / success) << "\n";
    return failure == 0;
}

void PrintInteractiveMenu() {
    std::cout << "\n==== ZB Storage Sim Menu ====\n";
    std::cout << "1. 集群统计\n";
    std::cout << "2. 文件写入与回读校验\n";
    std::cout << "3. 批量导入统计模拟\n";
    std::cout << "4. 元数据查询时延模拟\n";
    std::cout << "q. 退出\n";
    std::cout << "可附带 key=value 参数，例如: 2 path=/demo/a.bin size_mb=10\n";
}

void PrintInteractiveMenuV2() {
    std::cout << "\n==== ZB Storage Sim Menu ====\n";
    std::cout << "1. cluster statistics\n";
    std::cout << "2. file write and read-back verification\n";
    std::cout << "3. batch import statistics simulation\n";
    std::cout << "4. metadata query latency simulation\n";
    std::cout << "5. file metadata template generation\n";
    std::cout << "6. batch file metadata import\n";
    std::cout << "7. file metadata query\n";
    std::cout << "q. quit\n";
    std::cout << "arguments use key=value, for example: 7 namespace_id=ns1 masstree_query_mode=resolve_path path=/data/ns1/a.bin\n";
}

bool RebuildCluster(const Options& flags, zb::sim::SimCluster* cluster, std::string* error) {
    zb::sim::SimClusterConfig config;
    config.root = flags.root;
    config.disk_nodes = flags.disk_nodes;
    config.optical_nodes = flags.optical_nodes;
    config.disk_capacity_bytes = flags.disk_capacity_gb == 0 ? flags.disk_capacity_bytes : flags.disk_capacity_gb * kGiB;
    config.optical_capacity_bytes =
        flags.optical_capacity_gb == 0 ? flags.optical_capacity_bytes : flags.optical_capacity_gb * kGiB;
    return cluster->Init(config, error);
}

int RunInteractive(const Options& base_flags) {
    Options current = base_flags;
    std::string error;
    zb::sim::SimCluster cluster;
    if (!RebuildCluster(current, &cluster, &error)) {
        std::cerr << "init_failed=" << error << "\n";
        return 1;
    }

    for (;;) {
        PrintInteractiveMenuV2();
        std::cout << "输入> " << std::flush;
        std::string line;
        if (!std::getline(std::cin, line)) {
            return 0;
        }
        const auto words = SplitWords(line);
        if (words.empty()) {
            continue;
        }
        const std::string action = ToLowerCopy(words.front());
        if (action == "q" || action == "quit" || action == "exit") {
            return 0;
        }

        Options command_flags = current;
        if (!ApplyInteractiveOverrides(&command_flags, words, 1, &error)) {
            std::cerr << "argument_error=" << error << "\n";
            continue;
        }
        if (!RebuildCluster(command_flags, &cluster, &error)) {
            std::cerr << "init_failed=" << error << "\n";
            continue;
        }

        bool ok = false;
        if (action == "1") {
            PrintCluster(cluster);
            ok = true;
        } else if (action == "2") {
            ok = RunIo(&cluster, command_flags);
        } else if (action == "3") {
            ok = RunImport(&cluster, command_flags);
        } else if (action == "4") {
            ok = RunQuery(&cluster, command_flags);
        } else if (action == "5") {
            ok = RunMasstreeTemplate(command_flags);
        } else if (action == "6") {
            ok = RunMasstreeImport(command_flags);
        } else if (action == "7") {
            ok = RunMasstreeQuery(command_flags);
        } else {
            std::cerr << "unknown_menu_action=" << action << "\n";
            continue;
        }
        current = command_flags;
        std::cout << "operation_ok=" << (ok ? "true" : "false") << "\n";
    }
}

} // namespace

int main(int argc, char* argv[]) {
    Options flags;
    bool show_help = false;
    std::string error;
    if (!ParseArgs(argc, argv, &flags, &show_help, &error)) {
        std::cerr << "argument_error=" << error << "\n";
        PrintUsage(argv[0]);
        return 1;
    }
    if (show_help) {
        PrintUsage(argv[0]);
        return 0;
    }

    zb::sim::SimClusterConfig config;
    config.root = flags.root;
    config.disk_nodes = flags.disk_nodes;
    config.optical_nodes = flags.optical_nodes;
    config.disk_capacity_bytes = flags.disk_capacity_gb == 0 ? flags.disk_capacity_bytes : flags.disk_capacity_gb * kGiB;
    config.optical_capacity_bytes =
        flags.optical_capacity_gb == 0 ? flags.optical_capacity_bytes : flags.optical_capacity_gb * kGiB;

    zb::sim::SimCluster cluster;
    if (!cluster.Init(config, &error)) {
        std::cerr << "init_failed=" << error << "\n";
        return 1;
    }

    const std::string scenario = flags.scenario;
    if (scenario == "interactive" || scenario == "menu") {
        return RunInteractive(flags);
    }
    if (scenario == "cluster" || scenario == "stats") {
        PrintCluster(cluster);
        return 0;
    }
    if (scenario == "io" || scenario == "write") {
        return RunIo(&cluster, flags) ? 0 : 1;
    }
    if (scenario == "import") {
        return RunImport(&cluster, flags) ? 0 : 1;
    }
    if (scenario == "query") {
        return RunQuery(&cluster, flags) ? 0 : 1;
    }
    if (scenario == "masstree_template") {
        return RunMasstreeTemplate(flags) ? 0 : 1;
    }
    if (scenario == "masstree_import") {
        return RunMasstreeImport(flags) ? 0 : 1;
    }
    if (scenario == "masstree_query") {
        return RunMasstreeQuery(flags) ? 0 : 1;
    }
    if (scenario == "reset") {
        return RunReset(&cluster, flags) ? 0 : 1;
    }
    if (scenario == "all") {
        PrintCluster(cluster);
        if (!RunIo(&cluster, flags)) {
            return 1;
        }
        if (!RunImport(&cluster, flags)) {
            return 1;
        }
        if (!RunQuery(&cluster, flags)) {
            return 1;
        }
        PrintCluster(cluster);
        return 0;
    }

    std::cerr << "unknown_scenario=" << scenario << "\n";
    return 1;
}
