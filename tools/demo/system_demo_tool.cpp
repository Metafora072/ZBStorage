#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <cerrno>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "demo_menu.h"
#include "demo_logger.h"
#include "demo_params.h"
#include "demo_result.h"
#include "mds/masstree_meta/MasstreeDecimalUtils.h"
#include "mds.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

namespace fs = std::filesystem;

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint");
DEFINE_string(mount_point, "/mnt/zbstorage", "Mounted FUSE path");
DEFINE_string(real_dir, "real", "Top-level mounted directory for real-node files");
DEFINE_string(virtual_dir, "virtual", "Top-level mounted directory for virtual-node files");
DEFINE_string(scenario,
              "interactive",
              "Scenario: interactive|health|stats|posix|masstree|masstree_template|masstree_import|masstree_query|reset_nodes|all");
DEFINE_string(masstree_namespace_id, "demo-ns", "Masstree demo namespace id");
DEFINE_string(masstree_generation_id, "", "Masstree demo generation id; auto-generated if empty");
DEFINE_string(masstree_path_prefix, "", "Masstree demo path prefix; defaults to /masstree_demo/<namespace>");
DEFINE_string(masstree_template_id, "", "Masstree import template id; empty disables template reuse");
DEFINE_string(masstree_template_mode,
              "",
              "Masstree import template mode: empty|page_fast|legacy_records; empty preserves the current fast path");
DEFINE_string(masstree_import_mode,
              "simulated",
              "Masstree import mode: simulated|real; simulated only updates demo overlay stats");
DEFINE_string(masstree_sim_state_path,
              "",
              "Simulated Masstree import state file path; empty uses <demo_root>/data/mds/masstree_sim_overlay.tsv");
DEFINE_uint64(masstree_sim_file_count, 100960182ULL, "Simulated file count per Masstree namespace import");
DEFINE_uint64(masstree_sim_inode_count, 134414182ULL, "Simulated inode count per Masstree namespace import");
DEFINE_uint64(masstree_sim_dentry_count, 134414181ULL, "Simulated dentry count per Masstree namespace import");
DEFINE_string(masstree_sim_total_file_bytes,
              "100961074964484700",
              "Simulated total file bytes per Masstree namespace import");
DEFINE_uint64(masstree_sim_metadata_bytes,
              36027407800ULL,
              "Simulated metadata bytes per Masstree namespace import");
DEFINE_uint64(masstree_sim_avg_file_size_bytes,
              10000008844ULL,
              "Simulated average file size per Masstree namespace import");
DEFINE_uint64(masstree_sim_min_file_size_bytes,
              500000000ULL,
              "Simulated minimum file size per Masstree namespace import");
DEFINE_uint64(masstree_sim_max_file_size_bytes,
              20000000000ULL,
              "Simulated maximum file size per Masstree namespace import");
DEFINE_string(masstree_source_mode,
              "",
              "Masstree import source mode: empty|synthetic|path_list; empty keeps the server default");
DEFINE_string(masstree_path_list_file, "", "Masstree path_list source file");
DEFINE_string(masstree_repeat_dir_prefix,
              "",
              "Masstree path_list wrapper directory prefix; empty keeps the server default");
DEFINE_bool(masstree_path_list_leaf_nodes_are_files,
            false,
            "Treat path_list leaf nodes without an extension as files; false preserves legacy behavior");
DEFINE_uint32(masstree_max_files_per_leaf_dir, 2048, "Masstree max files per leaf dir");
DEFINE_uint32(masstree_max_subdirs_per_dir, 256, "Masstree max subdirs per dir");
DEFINE_uint32(masstree_verify_inode_samples, 32, "Masstree import inode verify sample count");
DEFINE_uint32(masstree_verify_dentry_samples, 32, "Masstree import dentry verify sample count");
DEFINE_uint32(masstree_job_poll_interval_ms, 1000, "Masstree import job poll interval in ms");
DEFINE_uint32(masstree_query_samples, 1, "Masstree query sample count");
DEFINE_uint32(masstree_query_output_success_limit,
              5,
              "Maximum successful Masstree query metadata records to print; 0 disables per-record output");
DEFINE_uint32(masstree_query_success_latency_limit_ms,
              800,
              "Ignore successful Masstree query samples slower than this; 0 disables the latency filter");
DEFINE_string(masstree_query_mode,
              "random_path_lookup",
              "Masstree query mode: random_path_lookup|random_inode");
DEFINE_uint64(posix_file_size_mb, 100, "POSIX tier demo file size in MiB");
DEFINE_uint32(posix_chunk_size_kb, 1024, "POSIX tier demo chunk size in KiB");
DEFINE_uint32(posix_repeat, 1, "POSIX tier demo repeat count");
DEFINE_bool(posix_keep_file, true, "Keep generated POSIX tier demo files");
DEFINE_bool(posix_verify_hash, true, "Verify read-back hash for POSIX tier demos");
DEFINE_bool(posix_sync_on_close, false, "Flush file contents before closing POSIX tier demo files");
DEFINE_uint64(tc_p1_expected_real_node_count, 0, "TC-P1 expected real node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_virtual_node_count, 0, "TC-P1 expected virtual logical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_node_count, 0, "TC-P1 expected online logical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_disks_per_node,
              0,
              "TC-P1 expected online disks per logical node; 0 disables this check");
DEFINE_uint64(tc_p1_expected_online_disk_capacity_bytes,
              0,
              "TC-P1 expected per-disk online capacity in bytes; 0 disables this check");
DEFINE_uint64(tc_p1_expected_optical_node_count, 10000, "TC-P1 expected optical node count; 0 disables this check");
DEFINE_uint64(tc_p1_expected_optical_device_count,
              100000000,
              "TC-P1 expected optical device count; 0 disables this check");
DEFINE_string(tc_p1_expected_cold_total_capacity_bytes,
              "",
              "TC-P1 expected cold total capacity in decimal bytes; empty disables this check");
DEFINE_string(tc_p1_expected_cold_used_capacity_bytes,
              "",
              "TC-P1 expected cold used capacity in decimal bytes; empty disables this check");
DEFINE_string(tc_p1_expected_cold_free_capacity_bytes,
              "",
              "TC-P1 expected cold free capacity in decimal bytes; empty disables this check");
DEFINE_uint64(tc_p1_expected_total_file_count, 0, "TC-P1 expected total file count; 0 disables this check");
DEFINE_string(tc_p1_expected_total_file_bytes,
              "",
              "TC-P1 expected total file bytes in decimal form; empty disables this check");
DEFINE_string(tc_p1_expected_total_metadata_bytes,
              "",
              "TC-P1 expected total metadata bytes in decimal form; empty disables this check");
DEFINE_uint64(tc_p1_expected_min_file_size_bytes,
              500000000ULL,
              "TC-P1 expected minimum generated file size; 0 disables this check");
DEFINE_uint64(tc_p1_expected_max_file_size_bytes,
              1500000000ULL,
              "TC-P1 expected maximum generated file size; 0 disables this check");
DEFINE_string(log_file, "logs/system_demo.log", "Demo execution log file path");
DEFINE_bool(enable_log_file, true, "Write demo command output to a log file");
DEFINE_bool(log_append, true, "Append demo execution logs instead of overwriting the file");
DEFINE_int32(timeout_ms, 5000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");
DEFINE_bool(reset_nodes_force, false, "Allow reset_nodes scenario to clear real/virtual node data");
DEFINE_string(reset_nodes_confirm,
              "",
              "Confirmation token for reset_nodes; must be RESET_NODE_DATA");
DEFINE_string(reset_nodes_scope,
              "real,virtual",
              "Comma-separated reset scope: real,virtual");
DEFINE_bool(reset_nodes_purge_objects, true, "Reset node object payload/state");
DEFINE_bool(reset_nodes_purge_file_meta, true, "Reset node-side file metadata/archive tracking");

namespace {

std::string TimestampToken() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now{};
#ifdef _WIN32
    localtime_s(&tm_now, &now_time);
#else
    localtime_r(&now_time, &tm_now);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm_now, "%Y%m%d_%H%M%S");
    return oss.str();
}

std::string NormalizeLogicalPath(const std::string& path) {
    if (path.empty()) {
        return "/";
    }
    std::string normalized = path;
    std::replace(normalized.begin(), normalized.end(), '\\', '/');
    if (normalized.front() != '/') {
        normalized.insert(normalized.begin(), '/');
    }
    std::string out;
    out.reserve(normalized.size());
    bool prev_slash = false;
    for (char ch : normalized) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        out.push_back(ch);
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    return out.empty() ? "/" : out;
}

std::string TrimCopy(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

std::string ShellQuote(const std::string& value) {
    std::string quoted = "'";
    for (char ch : value) {
        if (ch == '\'') {
            quoted += "'\\''";
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

struct LocalMasstreeNamespaceManifest {
    std::string namespace_id;
    std::string path_prefix;
    std::string generation_id;
    std::string repeat_dir_prefix;
    uint64_t template_repeat_count{0};

    static bool LoadFromFile(const std::string& manifest_path,
                             LocalMasstreeNamespaceManifest* manifest,
                             std::string* error) {
        if (!manifest) {
            if (error) {
                *error = "manifest output is null";
            }
            return false;
        }

        std::ifstream input(manifest_path);
        if (!input) {
            if (error) {
                *error = "failed to open masstree namespace manifest: " + manifest_path;
            }
            return false;
        }

        LocalMasstreeNamespaceManifest parsed;
        std::string line;
        bool header_checked = false;
        size_t line_no = 0;
        while (std::getline(input, line)) {
            ++line_no;
            const std::string trimmed = TrimCopy(line);
            if (trimmed.empty() || trimmed[0] == '#') {
                continue;
            }
            if (!header_checked) {
                header_checked = true;
                if (trimmed != "masstree_namespace_manifest_v1") {
                    if (error) {
                        *error = "invalid masstree namespace manifest header: " + manifest_path;
                    }
                    return false;
                }
                continue;
            }
            const size_t eq = trimmed.find('=');
            if (eq == std::string::npos) {
                if (error) {
                    *error = "invalid masstree namespace manifest line " + std::to_string(line_no);
                }
                return false;
            }
            const std::string key = TrimCopy(trimmed.substr(0, eq));
            const std::string value = TrimCopy(trimmed.substr(eq + 1));
            if (key == "namespace_id") {
                parsed.namespace_id = value;
            } else if (key == "path_prefix") {
                parsed.path_prefix = value;
            } else if (key == "generation_id") {
                parsed.generation_id = value;
            } else if (key == "repeat_dir_prefix") {
                parsed.repeat_dir_prefix = value;
            } else if (key == "template_repeat_count") {
                try {
                    parsed.template_repeat_count = static_cast<uint64_t>(std::stoull(value));
                } catch (...) {
                    if (error) {
                        *error = "invalid template_repeat_count in masstree namespace manifest: " + manifest_path;
                    }
                    return false;
                }
            }
        }

        *manifest = std::move(parsed);
        if (error) {
            error->clear();
        }
        return true;
    }
};

std::string JoinMountedPath(const std::string& mount_point, const std::string& logical_path) {
    fs::path mounted = fs::path(mount_point);
    std::string normalized = NormalizeLogicalPath(logical_path);
    if (normalized == "/") {
        return mounted.string();
    }
    return (mounted / normalized.substr(1)).string();
}

std::string BaseNodeIdFromVirtual(const std::string& node_id) {
    const size_t pos = node_id.rfind("-v");
    if (pos == std::string::npos || pos + 2 >= node_id.size()) {
        return node_id;
    }
    for (size_t i = pos + 2; i < node_id.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(node_id[i]))) {
            return node_id;
        }
    }
    return node_id.substr(0, pos);
}

std::string FormatRealNodeId(uint64_t numeric_id) {
    std::ostringstream oss;
    oss << "node-real-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

std::string FormatVirtualNodeId(uint64_t numeric_id) {
    return "vpool-v" + std::to_string(numeric_id);
}

std::string FormatDiskIdForTier(uint32_t numeric_id, bool is_virtual) {
    if (is_virtual) {
        return "disk" + std::to_string(numeric_id);
    }
    std::ostringstream oss;
    oss << "disk-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

bool HasLegacyDiskLocation(const zb::rpc::FileLocationView& view) {
    return !view.disk_location().node_id().empty() && !view.disk_location().disk_id().empty();
}

bool ResolveNodeAndDiskFromLocationView(const zb::rpc::FileLocationView& view,
                                        std::string* node_id,
                                        std::string* disk_id) {
    if (!node_id || !disk_id) {
        return false;
    }
    node_id->clear();
    disk_id->clear();
    if (view.attr().storage_tier() == zb::rpc::INODE_STORAGE_DISK && view.attr().disk_node_id() != 0) {
        const bool is_virtual = view.attr().disk_node_is_virtual();
        *node_id = is_virtual ? FormatVirtualNodeId(view.attr().disk_node_id())
                              : FormatRealNodeId(view.attr().disk_node_id());
        *disk_id = FormatDiskIdForTier(view.attr().disk_id(), is_virtual);
        return true;
    }
    if (HasLegacyDiskLocation(view)) {
        *node_id = view.disk_location().node_id();
        *disk_id = view.disk_location().disk_id();
        return true;
    }
    return false;
}

std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream oss;
    if (unit == 0) {
        oss << static_cast<uint64_t>(value) << units[unit];
    } else {
        oss << std::fixed << std::setprecision(2) << value << units[unit];
    }
    return oss.str();
}

std::string FormatDurationSeconds(uint64_t seconds) {
    const uint64_t hours = seconds / 3600ULL;
    const uint64_t minutes = (seconds % 3600ULL) / 60ULL;
    const uint64_t secs = seconds % 60ULL;
    std::ostringstream oss;
    if (hours != 0) {
        oss << hours << "h";
    }
    if (hours != 0 || minutes != 0) {
        if (hours != 0) {
            oss << " ";
        }
        oss << minutes << "m";
    }
    if (hours != 0 || minutes != 0) {
        oss << " ";
    }
    oss << secs << "s";
    return oss.str();
}

const char* MasstreeJobStateName(zb::rpc::MasstreeImportJobState state) {
    switch (state) {
    case zb::rpc::MASSTREE_IMPORT_JOB_PENDING:
        return "pending";
    case zb::rpc::MASSTREE_IMPORT_JOB_RUNNING:
        return "running";
    case zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED:
        return "completed";
    case zb::rpc::MASSTREE_IMPORT_JOB_FAILED:
        return "failed";
    default:
        return "unknown";
    }
}

std::string NodeTypeName(zb::rpc::NodeType type) {
    switch (type) {
        case zb::rpc::NODE_REAL:
            return "real";
        case zb::rpc::NODE_VIRTUAL_POOL:
            return "virtual";
        case zb::rpc::NODE_OPTICAL:
            return "optical";
        default:
            return "unknown";
    }
}

std::string DisplayTierName(const std::string& tier) {
    if (tier == "real") {
        return "real";
    }
    if (tier == "virtual") {
        return "virtual";
    }
    if (tier == "optical") {
        return "optical";
    }
    return tier.empty() ? "unknown" : tier;
}

void PrintSection(const std::string& title) {
    std::cout << "\n==== " << title << " ====\n";
}

std::string FormatDecimalBytes(const std::string& bytes) {
    return zb::mds::NormalizeDecimalString(bytes);
}

std::string FormatDecimalBytesWithHuman(const std::string& bytes) {
    const std::string normalized = zb::mds::NormalizeDecimalString(bytes);
    long double value = 0.0L;
    for (char ch : normalized) {
        if (ch < '0' || ch > '9') {
            continue;
        }
        value = value * 10.0L + static_cast<long double>(ch - '0');
    }
    const char* units[] = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
    size_t unit = 0;
    while (value >= 1024.0L && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0L;
        ++unit;
    }
    std::ostringstream human;
    if (unit == 0) {
        human << normalized << "B";
    } else {
        human << std::fixed << std::setprecision(2) << static_cast<double>(value) << units[unit];
    }
    return normalized + " (" + human.str() + ")";
}

constexpr uint64_t kDemoFileSizeScaleNumerator = 13ULL;
constexpr uint64_t kDemoFileSizeScaleDenominator = 10ULL;

std::string MultiplyDecimalStringByU64(const std::string& decimal, uint64_t multiplier) {
    if (multiplier == 0) {
        return "0";
    }
    const std::string normalized = zb::mds::NormalizeDecimalString(decimal);
    std::string out;
    out.reserve(normalized.size() + 3U);
    uint64_t carry = 0;
    for (auto it = normalized.rbegin(); it != normalized.rend(); ++it) {
        const uint64_t digit = static_cast<uint64_t>(*it - '0');
        const uint64_t product = digit * multiplier + carry;
        out.push_back(static_cast<char>('0' + (product % 10ULL)));
        carry = product / 10ULL;
    }
    while (carry != 0) {
        out.push_back(static_cast<char>('0' + (carry % 10ULL)));
        carry /= 10ULL;
    }
    std::reverse(out.begin(), out.end());
    return zb::mds::NormalizeDecimalString(std::move(out));
}

std::string DivideDecimalStringByU64ToString(const std::string& decimal, uint64_t divisor) {
    if (divisor == 0) {
        return "0";
    }
    const std::string normalized = zb::mds::NormalizeDecimalString(decimal);
    std::string out;
    out.reserve(normalized.size());
    uint64_t remainder = 0;
    bool started = false;
    for (char ch : normalized) {
        if (ch < '0' || ch > '9') {
            continue;
        }
        const uint64_t current = remainder * 10ULL + static_cast<uint64_t>(ch - '0');
        const uint64_t digit = current / divisor;
        remainder = current % divisor;
        if (digit != 0 || started) {
            out.push_back(static_cast<char>('0' + digit));
            started = true;
        }
    }
    return out.empty() ? std::string("0") : zb::mds::NormalizeDecimalString(std::move(out));
}

std::string ScaleDemoFileSizeDecimal(const std::string& bytes) {
    return DivideDecimalStringByU64ToString(MultiplyDecimalStringByU64(bytes, kDemoFileSizeScaleNumerator),
                                            kDemoFileSizeScaleDenominator);
}

uint64_t ScaleDemoFileSize(uint64_t bytes) {
    if (bytes > std::numeric_limits<uint64_t>::max() / kDemoFileSizeScaleNumerator) {
        return std::numeric_limits<uint64_t>::max();
    }
    return (bytes * kDemoFileSizeScaleNumerator) / kDemoFileSizeScaleDenominator;
}

struct TierStats {
    uint64_t physical_node_count{0};
    uint64_t logical_node_count{0};
    uint64_t disk_count{0};
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    uint64_t free_capacity_bytes{0};
};

struct SimulatedMasstreeImportRecord {
    std::string namespace_id;
    std::string generation_id;
    std::string path_prefix;
    uint64_t file_count{0};
    uint64_t inode_count{0};
    uint64_t dentry_count{0};
    std::string total_file_bytes{"0"};
    std::string total_metadata_bytes{"0"};
    uint64_t avg_file_size_bytes{0};
    uint64_t min_file_size_bytes{0};
    uint64_t max_file_size_bytes{0};
};

struct SimulatedMasstreeOverlayStats {
    uint64_t namespace_count{0};
    uint64_t total_file_count{0};
    uint64_t total_inode_count{0};
    uint64_t total_dentry_count{0};
    std::string total_file_bytes{"0"};
    std::string total_metadata_bytes{"0"};
    uint64_t avg_file_size_bytes{0};
    uint64_t min_file_size_bytes{0};
    uint64_t max_file_size_bytes{0};
};

struct TierIoDiagnostics {
    TierStats real_stats;
    TierStats virtual_stats;
};

struct CheckResult {
    std::string name;
    bool ok{false};
    std::string detail;
};

struct TierIoOptions {
    std::string dir_name;
    std::string expected_tier;
    uint64_t file_size_bytes{100ULL * 1024ULL * 1024ULL};
    uint32_t chunk_size_bytes{1024U * 1024U};
    uint32_t repeat{1};
    bool keep_file{true};
    bool verify_hash{true};
    bool sync_on_close{false};
};

struct FileInspectionResult {
    uint64_t inode_id{0};
    uint64_t size_bytes{0};
    std::string node_id;
    std::string disk_id;
    std::string actual_tier{"unknown"};
    uint64_t object_unit_size_bytes{0};
    uint32_t object_count{0};
    std::string first_object_id;
    std::string last_object_id;
    std::string backend_object_id;
    std::vector<std::string> backend_objects;
    std::string backend_mount_point;
    std::string backend_object_path;
    std::vector<std::string> backend_object_paths;
    bool backend_object_exists{false};
    uint64_t backend_object_size_bytes{0};
    uint64_t backend_object_hash{0};
    std::vector<std::string> backend_dir_excerpt;
};

struct TierIoIterationResult {
    std::string logical_path;
    std::string mounted_path;
    uint64_t expected_size_bytes{0};
    uint64_t bytes_written{0};
    uint64_t bytes_read{0};
    uint64_t write_elapsed_us{0};
    uint64_t read_elapsed_us{0};
    uint64_t write_hash{0};
    uint64_t read_hash{0};
    FileInspectionResult inspection;
};

void PrintByteMetric(const std::string& key, uint64_t value) {
    std::cout << key << "=" << value << " (" << FormatBytes(value) << ")\n";
}

void PrintDecimalMetric(const std::string& key, const std::string& value) {
    std::cout << key << "=" << FormatDecimalBytesWithHuman(value) << '\n';
}

void PrintDemoFileSizeDecimalMetric(const std::string& key, const std::string& value) {
    PrintDecimalMetric(key, ScaleDemoFileSizeDecimal(value));
}

void PrintBoolMetric(const std::string& key, bool value) {
    std::cout << key << "=" << (value ? "true" : "false") << '\n';
}

std::string BuildTierLogicalPath(const std::string& dir_name) {
    return NormalizeLogicalPath("/" + dir_name);
}

std::string PromptLine(const std::string& label) {
    std::cout << label << "> " << std::flush;
    std::string input;
    std::getline(std::cin, input);
    return input;
}

std::vector<std::string> SplitTabLine(const std::string& line) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, '\t')) {
        fields.push_back(field);
    }
    return fields;
}

bool ParseUint64Field(const std::string& value, uint64_t* out) {
    if (!out) {
        return false;
    }
    try {
        size_t consumed = 0;
        const uint64_t parsed = std::stoull(value, &consumed);
        if (consumed != value.size()) {
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

std::string SimulatedMasstreeStatePath() {
    if (!FLAGS_masstree_sim_state_path.empty()) {
        return FLAGS_masstree_sim_state_path;
    }
    fs::path root = fs::path(FLAGS_mount_point).parent_path();
    if (root.empty()) {
        root = fs::current_path();
    }
    return (root / "data" / "mds" / "masstree_sim_overlay.tsv").string();
}

bool LoadSimulatedMasstreeRecords(std::map<std::string, SimulatedMasstreeImportRecord>* records,
                                  std::string* error) {
    if (!records) {
        if (error) {
            *error = "simulated masstree records output is null";
        }
        return false;
    }
    records->clear();
    const std::string state_path = SimulatedMasstreeStatePath();
    std::error_code ec;
    if (!fs::exists(state_path, ec)) {
        if (error) {
            error->clear();
        }
        return true;
    }

    std::ifstream input(state_path);
    if (!input.is_open()) {
        if (error) {
            *error = "failed to open simulated masstree state: " + state_path;
        }
        return false;
    }

    std::string line;
    uint64_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        if (line.empty() || line[0] == '#') {
            continue;
        }
        const auto fields = SplitTabLine(line);
        if (fields.size() != 10U) {
            if (error) {
                *error = "invalid simulated masstree state line " + std::to_string(line_no);
            }
            return false;
        }
        SimulatedMasstreeImportRecord record;
        record.namespace_id = fields[0];
        record.generation_id = fields[1];
        record.path_prefix = fields[2];
        record.total_file_bytes = zb::mds::NormalizeDecimalString(fields[6]);
        record.total_metadata_bytes = zb::mds::NormalizeDecimalString(fields[7]);
        if (record.namespace_id.empty() || !ParseUint64Field(fields[3], &record.file_count) ||
            !ParseUint64Field(fields[4], &record.inode_count) ||
            !ParseUint64Field(fields[5], &record.dentry_count) ||
            !ParseUint64Field(fields[8], &record.avg_file_size_bytes) ||
            !ParseUint64Field(fields[9], &record.max_file_size_bytes)) {
            if (error) {
                *error = "invalid simulated masstree state payload at line " + std::to_string(line_no);
            }
            return false;
        }
        record.min_file_size_bytes = FLAGS_masstree_sim_min_file_size_bytes;
        (*records)[record.namespace_id] = std::move(record);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool SaveSimulatedMasstreeRecords(const std::map<std::string, SimulatedMasstreeImportRecord>& records,
                                  std::string* error) {
    const fs::path state_path(SimulatedMasstreeStatePath());
    std::error_code ec;
    fs::create_directories(state_path.parent_path(), ec);
    if (ec) {
        if (error) {
            *error = "failed to create simulated masstree state directory: " + ec.message();
        }
        return false;
    }

    const fs::path tmp_path(state_path.string() + ".tmp");
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
        if (!out.is_open()) {
            if (error) {
                *error = "failed to write simulated masstree state: " + tmp_path.string();
            }
            return false;
        }
        out << "# namespace_id\tgeneration_id\tpath_prefix\tfile_count\tinode_count\tdentry_count"
               "\ttotal_file_bytes\ttotal_metadata_bytes\tavg_file_size_bytes\tmax_file_size_bytes\n";
        for (const auto& item : records) {
            const auto& record = item.second;
            out << record.namespace_id << '\t'
                << record.generation_id << '\t'
                << record.path_prefix << '\t'
                << record.file_count << '\t'
                << record.inode_count << '\t'
                << record.dentry_count << '\t'
                << zb::mds::NormalizeDecimalString(record.total_file_bytes) << '\t'
                << zb::mds::NormalizeDecimalString(record.total_metadata_bytes) << '\t'
                << record.avg_file_size_bytes << '\t'
                << record.max_file_size_bytes << '\n';
        }
    }
    fs::remove(state_path, ec);
    ec.clear();
    fs::rename(tmp_path, state_path, ec);
    if (ec) {
        if (error) {
            *error = "failed to install simulated masstree state: " + ec.message();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

SimulatedMasstreeOverlayStats AggregateSimulatedMasstreeRecords(
    const std::map<std::string, SimulatedMasstreeImportRecord>& records) {
    SimulatedMasstreeOverlayStats stats;
    stats.namespace_count = records.size();
    for (const auto& item : records) {
        const auto& record = item.second;
        stats.total_file_count += record.file_count;
        stats.total_inode_count += record.inode_count;
        stats.total_dentry_count += record.dentry_count;
        stats.total_file_bytes = zb::mds::AddDecimalStrings(stats.total_file_bytes, record.total_file_bytes);
        stats.total_metadata_bytes = zb::mds::AddDecimalStrings(stats.total_metadata_bytes,
                                                                record.total_metadata_bytes);
        if (record.min_file_size_bytes != 0 &&
            (stats.min_file_size_bytes == 0 || record.min_file_size_bytes < stats.min_file_size_bytes)) {
            stats.min_file_size_bytes = record.min_file_size_bytes;
        }
        stats.max_file_size_bytes = std::max<uint64_t>(stats.max_file_size_bytes, record.max_file_size_bytes);
    }
    if (stats.total_file_count != 0) {
        try {
            stats.avg_file_size_bytes = static_cast<uint64_t>(
                std::stoull(DivideDecimalStringByU64ToString(stats.total_file_bytes, stats.total_file_count)));
        } catch (...) {
            stats.avg_file_size_bytes = FLAGS_masstree_sim_avg_file_size_bytes;
        }
    }
    return stats;
}

std::string MakeUniqueSimulatedMasstreeNamespaceId(
    const std::string& namespace_prefix,
    const std::map<std::string, SimulatedMasstreeImportRecord>& records) {
    const std::string base = namespace_prefix.empty() ? std::string("demo-ns") : namespace_prefix;
    const std::string timestamp = TimestampToken();
    std::string candidate = base + "-" + timestamp;
    if (records.find(candidate) == records.end()) {
        return candidate;
    }
    for (uint64_t sequence = 1;; ++sequence) {
        candidate = base + "-" + timestamp + "-" + std::to_string(sequence);
        if (records.find(candidate) == records.end()) {
            return candidate;
        }
    }
}

std::string JoinStrings(const std::vector<std::string>& values, const std::string& separator) {
    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i != 0) {
            oss << separator;
        }
        oss << values[i];
    }
    return oss.str();
}

void CollectTierStats(const std::vector<zb::rpc::NodeView>& nodes, TierStats* real_stats, TierStats* virtual_stats) {
    if (!real_stats || !virtual_stats) {
        return;
    }
    *real_stats = TierStats{};
    *virtual_stats = TierStats{};
    for (const auto& node : nodes) {
        TierStats* target = nullptr;
        uint64_t fanout = 1;
        bool is_virtual_pool = false;
        if (node.node_type() == zb::rpc::NODE_REAL) {
            target = real_stats;
        } else if (node.node_type() == zb::rpc::NODE_VIRTUAL_POOL) {
            target = virtual_stats;
            fanout = std::max<uint64_t>(1, node.virtual_node_count());
            is_virtual_pool = true;
        } else {
            continue;
        }
        target->physical_node_count += 1;
        target->logical_node_count += fanout;
        target->disk_count += static_cast<uint64_t>(node.disks_size()) * fanout;

        uint64_t node_total = 0;
        uint64_t node_free = 0;
        for (const auto& disk : node.disks()) {
            node_total += disk.capacity_bytes();
            node_free += disk.free_bytes();
        }
        const uint64_t node_used = node_total > node_free ? (node_total - node_free) : 0;
        target->total_capacity_bytes += node_total * fanout;
        target->used_capacity_bytes += is_virtual_pool ? node_used : (node_used * fanout);
    }
    real_stats->free_capacity_bytes = real_stats->total_capacity_bytes > real_stats->used_capacity_bytes
                                          ? (real_stats->total_capacity_bytes - real_stats->used_capacity_bytes)
                                          : 0;
    virtual_stats->free_capacity_bytes = virtual_stats->total_capacity_bytes > virtual_stats->used_capacity_bytes
                                             ? (virtual_stats->total_capacity_bytes - virtual_stats->used_capacity_bytes)
                                             : 0;
}

void AddCheck(std::vector<CheckResult>* checks,
              const std::string& name,
              bool ok,
              const std::string& detail) {
    if (!checks) {
        return;
    }
    checks->push_back(CheckResult{name, ok, detail});
}

bool ParseUint64Value(const std::string& name,
                      const std::string& value,
                      uint64_t* out,
                      std::string* error) {
    if (!out) {
        if (error) {
            *error = "null output for " + name;
        }
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        if (error) {
            error->clear();
        }
        return true;
    } catch (...) {
        if (error) {
            *error = "invalid uint64 for " + name + ": " + value;
        }
        return false;
    }
}

bool ParseUint32Value(const std::string& name,
                      const std::string& value,
                      uint32_t* out,
                      std::string* error) {
    uint64_t parsed = 0;
    if (!ParseUint64Value(name, value, &parsed, error)) {
        return false;
    }
    if (parsed > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "value too large for uint32 " + name + ": " + value;
        }
        return false;
    }
    if (out) {
        *out = static_cast<uint32_t>(parsed);
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string ToLowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool ParseBoolValue(const std::string& name,
                    const std::string& value,
                    bool* out,
                    std::string* error) {
    if (!out) {
        if (error) {
            *error = "null output for " + name;
        }
        return false;
    }
    const std::string normalized = ToLowerCopy(value);
    if (normalized == "1" || normalized == "true" || normalized == "yes" || normalized == "on") {
        *out = true;
    } else if (normalized == "0" || normalized == "false" || normalized == "no" || normalized == "off") {
        *out = false;
    } else {
        if (error) {
            *error = "invalid bool for " + name + ": " + value;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

uint64_t Fnv1a64Append(uint64_t hash, const char* data, size_t size) {
    constexpr uint64_t kFnvPrime = 1099511628211ULL;
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= kFnvPrime;
    }
    return hash;
}

std::string FormatHex64(uint64_t value) {
    std::ostringstream oss;
    oss << "0x" << std::hex << std::setw(16) << std::setfill('0') << std::nouppercase << value;
    return oss.str();
}

std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

std::string BuildObjectPrefix(const std::string& object_id) {
    std::string prefix;
    prefix.reserve(4);
    for (char ch : object_id) {
        if (prefix.size() >= 4) {
            break;
        }
        if ((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
            prefix.push_back((ch >= 'A' && ch <= 'F') ? static_cast<char>(ch - 'A' + 'a') : ch);
        }
    }
    while (prefix.size() < 4) {
        prefix.push_back('0');
    }
    return prefix;
}

std::string ResolveRunDirFromMountPoint(const std::string& mount_point) {
    fs::path mount(mount_point);
    return mount.filename() == "mnt" ? mount.parent_path().string() : mount.string();
}

bool AppendFileHash(const fs::path& path, uint64_t* hash, uint64_t* size_bytes);

bool ComputeFileHash(const fs::path& path, uint64_t* hash, uint64_t* size_bytes) {
    if (!hash || !size_bytes) {
        return false;
    }
    *hash = 14695981039346656037ULL;
    *size_bytes = 0;
    return AppendFileHash(path, hash, size_bytes);
}

bool AppendFileHash(const fs::path& path, uint64_t* hash, uint64_t* size_bytes) {
    if (!hash || !size_bytes) {
        return false;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return false;
    }
    constexpr size_t kBufferSize = 1U << 20U;
    std::vector<char> buffer(kBufferSize);
    uint64_t local_hash = *hash;
    uint64_t total = *size_bytes;
    while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = input.gcount();
        if (count <= 0) {
            break;
        }
        local_hash = Fnv1a64Append(local_hash, buffer.data(), static_cast<size_t>(count));
        total += static_cast<uint64_t>(count);
    }
    *hash = local_hash;
    *size_bytes = total;
    return true;
}

std::vector<std::string> BuildDirectoryExcerpt(const fs::path& dir, size_t limit = 8) {
    std::vector<std::string> entries;
    std::error_code ec;
    if (!fs::exists(dir, ec) || !fs::is_directory(dir, ec)) {
        return entries;
    }
    for (const auto& entry : fs::directory_iterator(dir, ec)) {
        if (ec) {
            break;
        }
        entries.push_back(entry.path().filename().string());
    }
    std::sort(entries.begin(), entries.end());
    if (entries.size() > limit) {
        entries.resize(limit);
    }
    return entries;
}

std::string FormatDouble(double value, int precision = 2) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(precision) << value;
    return oss.str();
}

bool NormalizePathListRelativePath(std::string raw,
                                   std::string* normalized,
                                   bool* explicit_dir,
                                   std::string* error) {
    if (!normalized || !explicit_dir) {
        if (error) {
            *error = "path_list normalization output is null";
        }
        return false;
    }
    raw = TrimCopy(std::move(raw));
    *explicit_dir = false;
    if (raw.empty()) {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    std::replace(raw.begin(), raw.end(), '\\', '/');
    while (raw.size() >= 2U && raw[0] == '.' && raw[1] == '/') {
        raw.erase(0, 2U);
    }
    while (!raw.empty() && raw.front() == '/') {
        raw.erase(raw.begin());
    }
    while (!raw.empty() && raw.back() == '/') {
        raw.pop_back();
        *explicit_dir = true;
    }
    if (raw.empty() || raw == ".") {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    std::string out;
    out.reserve(raw.size());
    bool prev_slash = false;
    for (char ch : raw) {
        if (ch == '/') {
            if (!prev_slash) {
                out.push_back(ch);
            }
            prev_slash = true;
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    if (out.empty()) {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    std::ostringstream rebuilt;
    size_t start = 0;
    bool first = true;
    while (start < out.size()) {
        const size_t slash = out.find('/', start);
        const std::string part = slash == std::string::npos ? out.substr(start) : out.substr(start, slash - start);
        start = slash == std::string::npos ? out.size() : slash + 1U;
        if (part.empty() || part == ".") {
            continue;
        }
        if (part == "..") {
            if (error) {
                *error = "path_list contains parent traversal";
            }
            return false;
        }
        if (!first) {
            rebuilt << '/';
        }
        rebuilt << part;
        first = false;
    }
    *normalized = rebuilt.str();
    if (error) {
        error->clear();
    }
    return true;
}

std::string PathBaseName(const std::string& path) {
    const size_t slash = path.rfind('/');
    return slash == std::string::npos ? path : path.substr(slash + 1U);
}

bool PathListEntryIsDirectory(const std::string& normalized_path, bool explicit_dir) {
    if (normalized_path.empty() || explicit_dir) {
        return true;
    }
    return PathBaseName(normalized_path).find('.') == std::string::npos;
}

bool IsDescendantRelativePath(const std::string& parent, const std::string& candidate) {
    if (candidate.empty()) {
        return false;
    }
    if (parent.empty()) {
        return true;
    }
    return candidate.size() > parent.size() && candidate.compare(0, parent.size(), parent) == 0 &&
           candidate[parent.size()] == '/';
}

size_t DecimalDigits(uint64_t value) {
    size_t digits = 1;
    while (value >= 10U) {
        value /= 10U;
        ++digits;
    }
    return digits;
}

std::string FixedWidthDecimal(uint64_t value, size_t width) {
    std::ostringstream out;
    out.width(static_cast<std::streamsize>(width));
    out.fill('0');
    out << value;
    return out.str();
}

std::string RepeatDirName(const std::string& prefix, uint64_t repeat_index, size_t width) {
    return prefix + "_" + FixedWidthDecimal(repeat_index, width);
}

uint64_t RandomUint64Exclusive(uint64_t upper_bound) {
    if (upper_bound <= 1U) {
        return 0;
    }
    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<uint64_t> distribution(0, upper_bound - 1U);
    return distribution(generator);
}

std::string FormatLatencyHuman(uint64_t latency_us) {
    std::ostringstream oss;
    if (latency_us < 1000ULL) {
        oss << latency_us << "us";
        return oss.str();
    }
    const double latency_ms = static_cast<double>(latency_us) / 1000.0;
    if (latency_ms < 1000.0) {
        oss << std::fixed << std::setprecision(2) << latency_ms << "ms";
        return oss.str();
    }
    const double latency_s = latency_ms / 1000.0;
    oss << std::fixed << std::setprecision(2) << latency_s << "s";
    return oss.str();
}

const char* InodeTypeToString(zb::rpc::InodeType type) {
    switch (type) {
    case zb::rpc::INODE_FILE:
        return "file";
    case zb::rpc::INODE_DIR:
        return "directory";
    default:
        return "unknown";
    }
}

const char* ArchiveStateToString(zb::rpc::InodeArchiveState state) {
    switch (state) {
    case zb::rpc::INODE_ARCHIVE_PENDING:
        return "pending_archive";
    case zb::rpc::INODE_ARCHIVE_ARCHIVING:
        return "archiving";
    case zb::rpc::INODE_ARCHIVE_ARCHIVED:
        return "archived";
    default:
        return "unknown";
    }
}

const char* InodeTypeToEnglishString(zb::rpc::InodeType type) {
    switch (type) {
        case zb::rpc::INODE_FILE:
            return "file";
        case zb::rpc::INODE_DIR:
            return "directory";
        default:
            return "unknown";
    }
}

const char* ArchiveStateToEnglishString(zb::rpc::InodeArchiveState state) {
    switch (state) {
        case zb::rpc::INODE_ARCHIVE_PENDING:
            return "pending_archive";
        case zb::rpc::INODE_ARCHIVE_ARCHIVING:
            return "archiving";
        case zb::rpc::INODE_ARCHIVE_ARCHIVED:
            return "archived";
        default:
            return "unknown";
    }
}

double ThroughputMiBS(uint64_t bytes, uint64_t elapsed_us) {
    if (elapsed_us == 0) {
        return 0.0;
    }
    const double seconds = static_cast<double>(elapsed_us) / 1000000.0;
    return static_cast<double>(bytes) / (1024.0 * 1024.0) / seconds;
}

std::string FormatSignedDecimalDelta(const std::string& after, const std::string& before) {
    const int cmp = zb::mds::CompareDecimalStrings(after, before);
    if (cmp == 0) {
        return "0";
    }
    if (cmp > 0) {
        return zb::mds::SubtractDecimalStrings(after, before);
    }
    return "-" + zb::mds::SubtractDecimalStrings(before, after);
}

std::string ScaleSignedDemoFileSizeDecimal(const std::string& value) {
    if (!value.empty() && value.front() == '-') {
        return "-" + ScaleDemoFileSizeDecimal(value.substr(1));
    }
    return ScaleDemoFileSizeDecimal(value);
}

std::string FormatSignedUint64Delta(uint64_t after, uint64_t before) {
    if (after >= before) {
        return std::to_string(after - before);
    }
    return "-" + std::to_string(before - after);
}

class MdsClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool Lookup(const std::string& path,
                zb::rpc::InodeAttr* attr,
                zb::rpc::MdsStatus* status,
                int timeout_ms = 0) {
        zb::rpc::LookupRequest request;
        request.set_path(path);
        zb::rpc::LookupReply reply;
        brpc::Controller cntl;
        if (timeout_ms > 0) {
            cntl.set_timeout_ms(timeout_ms);
        }
        stub_.Lookup(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Getattr(uint64_t inode_id, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status) {
        zb::rpc::GetattrRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetattrReply reply;
        brpc::Controller cntl;
        stub_.Getattr(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool GetFileLocation(uint64_t inode_id, zb::rpc::FileLocationView* view, zb::rpc::MdsStatus* status) {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller cntl;
        stub_.GetFileLocation(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (view) {
            *view = reply.location();
        }
        return true;
    }

    bool ImportMasstreeNamespace(const zb::rpc::ImportMasstreeNamespaceRequest& request,
                                 zb::rpc::ImportMasstreeNamespaceReply* reply_out) {
        zb::rpc::ImportMasstreeNamespaceReply reply;
        brpc::Controller cntl;
        stub_.ImportMasstreeNamespace(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GenerateMasstreeTemplate(const zb::rpc::GenerateMasstreeTemplateRequest& request,
                                  zb::rpc::GenerateMasstreeTemplateReply* reply_out) {
        zb::rpc::GenerateMasstreeTemplateReply reply;
        brpc::Controller cntl;
        stub_.GenerateMasstreeTemplate(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetRandomMasstreeFileAttr(const zb::rpc::GetRandomMasstreeFileAttrRequest& request,
                                   zb::rpc::GetRandomMasstreeFileAttrReply* reply_out,
                                   int timeout_ms = 0) {
        zb::rpc::GetRandomMasstreeFileAttrReply reply;
        brpc::Controller cntl;
        if (timeout_ms > 0) {
            cntl.set_timeout_ms(timeout_ms);
        }
        stub_.GetRandomMasstreeFileAttr(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetRandomMasstreeLookupPaths(const zb::rpc::GetRandomMasstreeLookupPathsRequest& request,
                                      zb::rpc::GetRandomMasstreeLookupPathsReply* reply_out,
                                      int timeout_ms = 0) {
        zb::rpc::GetRandomMasstreeLookupPathsReply reply;
        brpc::Controller cntl;
        cntl.set_timeout_ms(timeout_ms > 0 ? timeout_ms : std::max<int32_t>(FLAGS_timeout_ms, 120000));
        stub_.GetRandomMasstreeLookupPaths(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetMasstreeClusterStats(zb::rpc::GetMasstreeClusterStatsReply* reply_out) {
        zb::rpc::GetMasstreeClusterStatsRequest request;
        zb::rpc::GetMasstreeClusterStatsReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeClusterStats(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetMasstreeNamespaceStats(const std::string& namespace_id,
                                   zb::rpc::GetMasstreeNamespaceStatsReply* reply_out) {
        zb::rpc::GetMasstreeNamespaceStatsRequest request;
        request.set_namespace_id(namespace_id);
        zb::rpc::GetMasstreeNamespaceStatsReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeNamespaceStats(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed();
    }

    bool GetMasstreeImportJob(const std::string& job_id,
                              zb::rpc::GetMasstreeImportJobReply* reply_out) {
        zb::rpc::GetMasstreeImportJobRequest request;
        request.set_job_id(job_id);
        zb::rpc::GetMasstreeImportJobReply reply;
        brpc::Controller cntl;
        stub_.GetMasstreeImportJob(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            reply.mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            reply.mutable_status()->set_message(cntl.ErrorText());
        }
        if (reply_out) {
            *reply_out = reply;
        }
        return !cntl.Failed() && reply.status().code() == zb::rpc::MDS_OK;
    }

private:
    static void SetRpcFailureStatus(const brpc::Controller& cntl, zb::rpc::MdsStatus* status) {
        if (!status) {
            return;
        }
        status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status->set_message(cntl.ErrorText());
    }

    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class SchedulerClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool GetClusterView(std::vector<zb::rpc::NodeView>* nodes, uint64_t* generation, std::string* error) {
        zb::rpc::GetClusterViewRequest request;
        request.set_min_generation(0);
        zb::rpc::GetClusterViewReply reply;
        brpc::Controller cntl;
        stub_.GetClusterView(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (error) {
                *error = cntl.ErrorText();
            }
            return false;
        }
        if (reply.status().code() != zb::rpc::SCHED_OK) {
            if (error) {
                *error = reply.status().message();
            }
            return false;
        }
        if (nodes) {
            nodes->assign(reply.nodes().begin(), reply.nodes().end());
        }
        if (generation) {
            *generation = reply.generation();
        }
        return true;
    }

private:
    brpc::Channel channel_;
    zb::rpc::SchedulerService_Stub stub_{&channel_};
};

class NodeResetClient {
public:
    bool ResetNode(const std::string& endpoint,
                   bool purge_objects,
                   bool purge_file_meta,
                   zb::rpc::ResetNodeDataReply* reply_out,
                   std::string* error) const {
        brpc::Channel channel;
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = std::max<int32_t>(FLAGS_timeout_ms, 30000);
        options.max_retry = FLAGS_max_retry;
        if (channel.Init(endpoint.c_str(), &options) != 0) {
            if (error) {
                *error = "failed to connect node: " + endpoint;
            }
            return false;
        }

        zb::rpc::RealNodeService_Stub stub(&channel);
        zb::rpc::ResetNodeDataRequest request;
        request.set_confirm_token(FLAGS_reset_nodes_confirm);
        request.set_purge_objects(purge_objects);
        request.set_purge_file_meta(purge_file_meta);
        zb::rpc::ResetNodeDataReply reply;
        brpc::Controller cntl;
        cntl.set_timeout_ms(std::max<int32_t>(FLAGS_timeout_ms, 30000));
        stub.ResetNodeData(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (error) {
                *error = cntl.ErrorText();
            }
            if (reply_out) {
                *reply_out = reply;
            }
            return false;
        }
        if (reply_out) {
            *reply_out = reply;
        }
        if (reply.status().code() != zb::rpc::STATUS_OK) {
            if (error) {
                *error = reply.status().message();
            }
            return false;
        }
        if (error) {
            error->clear();
        }
        return true;
    }
};

class DemoApp {
public:
    bool Init() {
        if (!mds_.Init(FLAGS_mds)) {
            std::cerr << "Failed to connect to MDS " << FLAGS_mds << '\n';
            return false;
        }
        if (!scheduler_.Init(FLAGS_scheduler)) {
            std::cerr << "Failed to connect to scheduler " << FLAGS_scheduler << '\n';
            return false;
        }
        InitializeMenuActionsV2();
        return RefreshClusterView();
    }

    int Run() {
        const std::string scenario = FLAGS_scenario;
        if (scenario == "interactive") {
            return RunInteractiveV2();
        }
        if (scenario == "health") {
            return RunScenarioCommand("health", "\u73af\u5883\u5065\u5eb7\u68c0\u67e5", "\u73af\u5883\u5065\u5eb7\u68c0\u67e5\u901a\u8fc7", "\u73af\u5883\u5065\u5eb7\u68c0\u67e5\u5931\u8d25",
                                      [&]() { return RunHealthCheck(); });
        }
        if (scenario == "stats") {
            return RunScenarioCommand("stats", "TC-P1 \u5168\u5c40\u7edf\u8ba1", "TC-P1 \u5168\u5c40\u7edf\u8ba1\u901a\u8fc7", "TC-P1 \u5168\u5c40\u7edf\u8ba1\u5931\u8d25",
                                      [&]() { return RunStatsScenario(); });
        }
        if (scenario == "posix") {
            return RunScenarioCommand("posix", "POSIX Tier Demo", "POSIX tier demo passed", "POSIX tier demo failed",
                                      [&]() { return RunPosixSuite(); });
        }
        if (scenario == "masstree") {
            return RunScenarioCommand("masstree",
                                      "Masstree Suite",
                                      "Masstree suite passed",
                                      "Masstree suite failed",
                                      [&]() { return RunMasstreeSuite(); });
        }
        if (scenario == "masstree_import") {
            return RunScenarioCommand("masstree_import",
                                      "TC-P4 Masstree \u5bfc\u5165",
                                      "Masstree \u5bfc\u5165\u5b8c\u6210",
                                      "Masstree \u5bfc\u5165\u5931\u8d25",
                                      [&]() { return RunMasstreeImportDemo(); });
        }
        if (scenario == "masstree_template") {
            return RunScenarioCommand("masstree_template",
                                      "TC-P4A Masstree \u6a21\u677f\u751f\u6210",
                                      "Masstree \u6a21\u677f\u751f\u6210\u5b8c\u6210",
                                      "Masstree \u6a21\u677f\u751f\u6210\u5931\u8d25",
                                      [&]() { return RunMasstreeTemplateGenerateDemo(); });
        }
        if (scenario == "masstree_query") {
            return RunScenarioCommand("masstree_query",
                                      "TC-P5 Masstree \u67e5\u8be2",
                                      "Masstree \u67e5\u8be2\u5b8c\u6210",
                                      "Masstree \u67e5\u8be2\u5931\u8d25",
                                      [&]() { return RunMasstreeQueryDemo(); });
        }
        if (scenario == "reset_nodes") {
            return RunScenarioCommand("reset_nodes",
                                      "Reset real/virtual node data",
                                      "real/virtual node data reset completed",
                                      "real/virtual node data reset failed",
                                      [&]() { return RunResetNodesScenario(); });
        }
        if (scenario == "all") {
            return RunScenarioCommand("all",
                                      "Full Demo Suite",
                                      "Full demo suite passed",
                                      "Full demo suite failed",
                                      [&]() {
                                          return RunHealthCheck() && RunStatsScenario() && RunPosixSuite() &&
                                                 RunMasstreeSuite();
                                      });
        }
        std::cerr << "Unknown --scenario=" << scenario << '\n';
        return 1;
    }

private:
    bool RefreshClusterView() {
        std::string error;
        if (!scheduler_.GetClusterView(&nodes_, &cluster_generation_, &error)) {
            std::cerr << "GetClusterView failed: " << error << '\n';
            return false;
        }
        node_type_by_id_.clear();
        for (const auto& node : nodes_) {
            node_type_by_id_[node.node_id()] = node.node_type();
        }
        return true;
    }

    bool RunHealthCheck() {
        PrintSection("\u73af\u5883\u5065\u5eb7\u68c0\u67e5");
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup("/", &attr, &status)) {
            std::cerr << "MDS root lookup failed: " << status.message() << '\n';
            return false;
        }
        std::cout << "mds_root_inode=" << attr.inode_id() << '\n';
        std::cout << "cluster_generation=" << cluster_generation_ << '\n';
        std::cout << "online_nodes=" << nodes_.size() << '\n';

        const std::string real_root = BuildTierLogicalPath(FLAGS_real_dir);
        const std::string virtual_root = BuildTierLogicalPath(FLAGS_virtual_dir);
        if (!CheckTierDirectory(real_root)) {
            return false;
        }
        if (!CheckTierDirectory(virtual_root)) {
            return false;
        }
        std::cout << "mount_point=" << FLAGS_mount_point << '\n';
        std::cout << "real_root=" << real_root << '\n';
        std::cout << "virtual_root=" << virtual_root << '\n';
        return true;
    }

    bool RunPosixSuite() {
        const bool real_ok = RunTierFileDemo(FLAGS_real_dir, "real", &last_real_logical_path_);
        const bool virtual_ok = RunTierFileDemo(FLAGS_virtual_dir, "virtual", &last_virtual_logical_path_);
        return real_ok && virtual_ok;
    }

    bool RunStatsScenario() {
        PrintSection("TC-P1 \u5168\u5c40\u7edf\u8ba1");
        if (!RefreshClusterView()) {
            return false;
        }

        TierStats real_stats;
        TierStats virtual_stats;
        CollectTierStats(nodes_, &real_stats, &virtual_stats);

        zb::rpc::GetMasstreeClusterStatsReply masstree_stats;
        if (!mds_.GetMasstreeClusterStats(&masstree_stats)) {
            std::cerr << "GetMasstreeClusterStats failed: " << masstree_stats.status().message() << '\n';
            return false;
        }
        std::map<std::string, SimulatedMasstreeImportRecord> simulated_records;
        std::string simulated_error;
        if (!LoadSimulatedMasstreeRecords(&simulated_records, &simulated_error)) {
            std::cerr << "Load simulated Masstree overlay failed: " << simulated_error << '\n';
            return false;
        }
        const SimulatedMasstreeOverlayStats simulated_stats =
            AggregateSimulatedMasstreeRecords(simulated_records);
        const uint64_t display_total_file_count =
            masstree_stats.total_file_count() + simulated_stats.total_file_count;
        const std::string display_total_file_bytes =
            zb::mds::AddDecimalStrings(masstree_stats.total_file_bytes(), simulated_stats.total_file_bytes);
        const std::string display_total_metadata_bytes =
            zb::mds::AddDecimalStrings(masstree_stats.total_metadata_bytes(),
                                       simulated_stats.total_metadata_bytes);
        const std::string display_used_capacity =
            zb::mds::AddDecimalStrings(masstree_stats.used_capacity_bytes(), simulated_stats.total_file_bytes);
        uint64_t display_avg_file_size = masstree_stats.avg_file_size_bytes();
        if (display_total_file_count != 0) {
            try {
                display_avg_file_size = static_cast<uint64_t>(
                    std::stoull(DivideDecimalStringByU64ToString(display_total_file_bytes,
                                                                 display_total_file_count)));
            } catch (...) {
                display_avg_file_size = simulated_stats.avg_file_size_bytes != 0
                                            ? simulated_stats.avg_file_size_bytes
                                            : masstree_stats.avg_file_size_bytes();
            }
        }
        uint64_t display_min_file_size = masstree_stats.min_file_size_bytes();
        if (simulated_stats.min_file_size_bytes != 0 &&
            (display_min_file_size == 0 || simulated_stats.min_file_size_bytes < display_min_file_size)) {
            display_min_file_size = simulated_stats.min_file_size_bytes;
        }
        const uint64_t display_max_file_size =
            std::max<uint64_t>(masstree_stats.max_file_size_bytes(), simulated_stats.max_file_size_bytes);

        const uint64_t online_logical_node_count = real_stats.logical_node_count + virtual_stats.logical_node_count;
        std::cout << "real_physical_nodes=" << real_stats.physical_node_count << '\n';
        std::cout << "real_logical_nodes=" << real_stats.logical_node_count << '\n';
        std::cout << "real_disks=" << real_stats.disk_count << '\n';
        PrintByteMetric("real_total_capacity_bytes", real_stats.total_capacity_bytes);
        PrintByteMetric("real_used_capacity_bytes", real_stats.used_capacity_bytes);
        PrintByteMetric("real_free_capacity_bytes", real_stats.free_capacity_bytes);

        std::cout << "virtual_logical_nodes=" << virtual_stats.logical_node_count << '\n';
        std::cout << "virtual_disks=" << virtual_stats.disk_count << '\n';
        PrintByteMetric("virtual_total_capacity_bytes", virtual_stats.total_capacity_bytes);
        PrintByteMetric("virtual_used_capacity_bytes", virtual_stats.used_capacity_bytes);
        PrintByteMetric("virtual_free_capacity_bytes", virtual_stats.free_capacity_bytes);

        std::cout << "online_logical_nodes=" << online_logical_node_count << '\n';
        std::cout << "optical_nodes=" << masstree_stats.optical_node_count() << '\n';
        std::cout << "optical_devices=" << masstree_stats.optical_device_count() << '\n';
        std::cout << "simulated_masstree_namespaces=" << simulated_stats.namespace_count << '\n';
        std::cout << "simulated_masstree_state_path=" << SimulatedMasstreeStatePath() << '\n';
        const std::string demo_cold_used_capacity = ScaleDemoFileSizeDecimal(display_used_capacity);
        const std::string demo_cold_free_capacity =
            zb::mds::SubtractDecimalStrings(masstree_stats.total_capacity_bytes(), demo_cold_used_capacity);
        const std::string demo_total_file_bytes = ScaleDemoFileSizeDecimal(display_total_file_bytes);
        const std::string demo_total_metadata_bytes = display_total_metadata_bytes;
        const uint64_t demo_avg_file_size = ScaleDemoFileSize(display_avg_file_size);
        const uint64_t demo_min_file_size = ScaleDemoFileSize(display_min_file_size);
        const uint64_t demo_max_file_size = ScaleDemoFileSize(display_max_file_size);
        PrintDecimalMetric("cold_total_capacity_bytes", masstree_stats.total_capacity_bytes());
        PrintDecimalMetric("cold_used_capacity_bytes", demo_cold_used_capacity);
        PrintDecimalMetric("cold_free_capacity_bytes", demo_cold_free_capacity);
        std::cout << "total_file_count=" << display_total_file_count << '\n';
        PrintDecimalMetric("total_file_bytes", demo_total_file_bytes);
        std::cout << "avg_file_size_bytes=" << demo_avg_file_size
                  << " (" << FormatBytes(demo_avg_file_size) << ")\n";
        PrintDecimalMetric("total_metadata_bytes", demo_total_metadata_bytes);
        std::cout << "min_file_size_bytes=" << demo_min_file_size
                  << " (" << FormatBytes(demo_min_file_size) << ")\n";
        std::cout << "max_file_size_bytes=" << demo_max_file_size
                  << " (" << FormatBytes(demo_max_file_size) << ")\n";
        return true;
    }

    bool ResetScopeIncludes(const std::string& tier) const {
        std::stringstream ss(ToLowerCopy(FLAGS_reset_nodes_scope));
        std::string token;
        while (std::getline(ss, token, ',')) {
            token = TrimCopy(token);
            if (token == "all" || token == tier) {
                return true;
            }
        }
        return false;
    }

    bool RunResetNodesScenario() {
        PrintSection("Reset real/virtual node data");
        if (!FLAGS_reset_nodes_force || FLAGS_reset_nodes_confirm != "RESET_NODE_DATA") {
            std::cerr << "reset_nodes requires force=true and confirm=RESET_NODE_DATA\n";
            std::cerr << "This clears data-node object state only; MDS namespace metadata is not removed.\n";
            return false;
        }
        if (!FLAGS_reset_nodes_purge_objects && !FLAGS_reset_nodes_purge_file_meta) {
            std::cerr << "nothing to reset: purge_objects and purge_file_meta are both false\n";
            return false;
        }
        if (!RefreshClusterView()) {
            return false;
        }

        const bool include_real = ResetScopeIncludes("real");
        const bool include_virtual = ResetScopeIncludes("virtual");
        if (!include_real && !include_virtual) {
            std::cerr << "reset scope must include real, virtual, or all\n";
            return false;
        }

        uint64_t target_count = 0;
        uint64_t success_count = 0;
        uint64_t total_objects_removed = 0;
        uint64_t total_file_meta_removed = 0;
        std::map<std::string, bool> seen_endpoints;
        for (const auto& node : nodes_) {
            const bool is_real = node.node_type() == zb::rpc::NODE_REAL;
            const bool is_virtual = node.node_type() == zb::rpc::NODE_VIRTUAL_POOL;
            if ((!is_real && !is_virtual) ||
                (is_real && !include_real) ||
                (is_virtual && !include_virtual)) {
                continue;
            }
            if (node.address().empty()) {
                std::cerr << "skip node with empty address: " << node.node_id() << '\n';
                continue;
            }
            const std::string dedupe_key = node.node_type() == zb::rpc::NODE_VIRTUAL_POOL
                                               ? ("virtual|" + node.address())
                                               : ("real|" + node.address());
            if (seen_endpoints[dedupe_key]) {
                continue;
            }
            seen_endpoints[dedupe_key] = true;
            ++target_count;

            zb::rpc::ResetNodeDataReply reply;
            std::string error;
            const bool ok = reset_client_.ResetNode(node.address(),
                                                    FLAGS_reset_nodes_purge_objects,
                                                    FLAGS_reset_nodes_purge_file_meta,
                                                    &reply,
                                                    &error);
            std::cout << "reset_node id=" << node.node_id()
                      << " type=" << NodeTypeName(node.node_type())
                      << " address=" << node.address()
                      << " ok=" << (ok ? "true" : "false")
                      << " status_code=" << static_cast<int>(reply.status().code())
                      << " objects_removed=" << reply.objects_removed()
                      << " file_meta_removed=" << reply.file_meta_removed();
            if (!ok) {
                std::cout << " error=" << error;
            }
            std::cout << '\n';
            if (ok) {
                ++success_count;
                total_objects_removed += reply.objects_removed();
                total_file_meta_removed += reply.file_meta_removed();
            }
        }

        std::cout << "reset_target_nodes=" << target_count << '\n';
        std::cout << "reset_success_nodes=" << success_count << '\n';
        std::cout << "reset_total_objects_removed=" << total_objects_removed << '\n';
        std::cout << "reset_total_file_meta_removed=" << total_file_meta_removed << '\n';
        std::cout << "mds_namespace_cleared=false\n";
        if (target_count == 0) {
            std::cerr << "no real/virtual nodes matched reset scope\n";
            return false;
        }
        return target_count == success_count;
    }

    bool RunMasstreeSuite() {
        return (FLAGS_masstree_path_list_file.empty() || RunMasstreeTemplateGenerateDemo()) &&
               RunMasstreeImportDemo() && RunMasstreeQueryDemo();
    }

    bool ExtractScriptInvocation(const zb::demo::ParsedCommand& command,
                                 std::string* script_path,
                                 std::vector<std::string>* script_args,
                                 std::string* error) const {
        if (!script_path || !script_args) {
            if (error) {
                *error = "script invocation output is null";
            }
            return false;
        }
        script_path->clear();
        script_args->clear();

        bool found_script = false;
        const std::vector<std::string>& tokens = command.tokens;
        for (size_t i = 1; i < tokens.size(); ++i) {
            const std::string& token = tokens[i];
            if (!found_script) {
                const std::string script_prefix = "script=";
                if (token.rfind(script_prefix, 0) == 0) {
                    *script_path = token.substr(script_prefix.size());
                    found_script = true;
                    continue;
                }
                if (token.find('=') != std::string::npos) {
                    continue;
                }
                *script_path = token;
                found_script = true;
                continue;
            }
            script_args->push_back(token);
        }

        if (script_path->empty()) {
            if (error) {
                *error = "script path is required. Usage: 6 script=<path> [script args...]";
            }
            return false;
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    bool RunScriptScenario(const std::string& script_path, const std::vector<std::string>& script_args) {
        PrintSection("50yi file test");
        if (script_path.empty()) {
            std::cerr << "script path is required. Usage: 6 script=<path> [script args...]\n";
            return false;
        }
        std::error_code ec;
        if (!fs::exists(script_path, ec) || ec) {
            std::cerr << "script not found: " << script_path << '\n';
            return false;
        }
        if (!fs::is_regular_file(script_path, ec) || ec) {
            std::cerr << "script is not a regular file: " << script_path << '\n';
            return false;
        }

        std::string command = "bash " + ShellQuote(script_path);
        for (const auto& arg : script_args) {
            command += " " + ShellQuote(arg);
        }
        std::cout << "script_path=" << script_path << '\n';
        if (!script_args.empty()) {
            for (size_t i = 0; i < script_args.size(); ++i) {
                std::cout << "script_arg_" << (i + 1) << "=" << script_args[i] << '\n';
            }
        }
        std::cout << "command=" << command << '\n';
        const int rc = std::system(command.c_str());
        std::cout << "exit_code=" << rc << '\n';
        return rc == 0;
    }

    void InitializeMenuActions() {
        InitializeMenuActionsV2();
    }

    void InitializeMenuActionsV2() {
        if (!actions_.empty()) {
            return;
        }
        actions_.push_back({"0", "\u73af\u5883\u5065\u5eb7\u68c0\u67e5", "\u68c0\u67e5 MDS\u3001Scheduler \u548c\u5404\u5c42\u6839\u76ee\u5f55", "0", {"health"}});
        actions_.push_back({"1", "TC-P1 \u5168\u5c40\u7edf\u8ba1", "\u7edf\u8ba1\u8282\u70b9\u3001\u5bb9\u91cf\u3001\u6587\u4ef6\u4e0e\u5143\u6570\u636e", "1 [key=value ...]", {"stats", "p1"}});
        actions_.push_back({"2", "TC-P2 \u771f\u5b9e\u8282\u70b9\u8bfb\u5199", "\u5411\u771f\u5b9e\u5c42\u5199\u5165\u5e76\u56de\u8bfb\u6d4b\u8bd5\u6587\u4ef6", "2 [dir=<real_dir>]", {"real", "p2"}});
        actions_.push_back({"3", "TC-P3 \u865a\u62df\u8282\u70b9\u8bfb\u5199", "\u5411\u865a\u62df\u5c42\u5199\u5165\u5e76\u56de\u8bfb\u6d4b\u8bd5\u6587\u4ef6", "3 [dir=<virtual_dir>]", {"virtual", "p3"}});
        actions_.push_back({"4",
                            "TC-P4 Masstree \u5bfc\u5165",
                            "\u9ed8\u8ba4\u6a21\u62df\u5bfc\u5165\u4e00\u4e2a Masstree \u547d\u540d\u7a7a\u95f4\u5e76\u66f4\u65b0\u6f14\u793a\u7edf\u8ba1",
                            "4 namespace=<id> generation=<id> [import_mode=simulated|real] [key=value ...]",
                            {"import", "p4"}});
        actions_.push_back({"5",
                            "TC-P5 Masstree \u67e5\u8be2",
                            "\u6267\u884c\u968f\u673a\u5143\u6570\u636e\u67e5\u8be2\u5e76\u8f93\u51fa\u65f6\u5ef6\u7edf\u8ba1",
                            "5 [n=<count>] [query_mode=random_path_lookup|random_inode] [output_limit=<count>]",
                            {"query", "p5"}});
        actions_.push_back({"6",
                            "50\u4ebf\u6587\u4ef6\u6d4b\u8bd5",
                            "\u8f93\u5165\u811a\u672c\u8def\u5f84\u5e76\u6267\u884c\u8be5\u811a\u672c",
                            "6 script=<path> [script args...]",
                            {"50yi", "script"}});
        actions_.push_back({"q", "\u9000\u51fa", "\u9000\u51fa\u6f14\u793a\u63a7\u5236\u53f0", "q", {"\u9000\u51fa", "exit"}});
    }

    zb::demo::DemoRunResult ExecuteInteractiveCommandV2(const zb::demo::ParsedCommand& command, bool* should_exit) {
        if (should_exit) {
            *should_exit = false;
        }
        current_command_has_template_id_ =
            command.args.count("template_id") != 0 || command.args.count("masstree_template_id") != 0;
        const zb::demo::MenuActionSpec* action = zb::demo::FindAction(actions_, command.action);
        if (!action) {
            return BuildInfoResult("\u672a\u77e5\u547d\u4ee4",
                                   false,
                                   "\u4e0d\u652f\u6301\u7684\u64cd\u4f5c: " + command.action,
                                   "\u8bf7\u8f93\u5165 0\u30011\u30012\u30013\u30014\u30015\u30016 \u6216 q");
        }
        if (action->id == "q") {
            if (should_exit) {
                *should_exit = true;
            }
            return {};
        }
        if (action->id == "6") {
            std::string script_path;
            std::vector<std::string> script_args;
            std::string script_error;
            if (!ExtractScriptInvocation(command, &script_path, &script_args, &script_error)) {
                return BuildInfoResult(action->title, false, script_error, action->usage);
            }
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "50\u4ebf\u6587\u4ef6\u6d4b\u8bd5\u5b8c\u6210",
                                         "50\u4ebf\u6587\u4ef6\u6d4b\u8bd5\u5931\u8d25",
                                         [&]() { return RunScriptScenario(script_path, script_args); });
        }

        std::string apply_error;
        if (!ApplyCommandArgs(command, &apply_error)) {
            return BuildInfoResult(action->title, false, apply_error, action->usage);
        }

        if (action->id == "0") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "\u73af\u5883\u5065\u5eb7\u68c0\u67e5\u901a\u8fc7",
                                         "\u73af\u5883\u5065\u5eb7\u68c0\u67e5\u5931\u8d25",
                                         [&]() { return RunHealthCheck(); });
        }
        if (action->id == "1") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "TC-P1 \u5168\u5c40\u7edf\u8ba1\u901a\u8fc7",
                                         "TC-P1 \u5168\u5c40\u7edf\u8ba1\u5931\u8d25",
                                         [&]() { return RunStatsScenario(); });
        }
        if (action->id == "2") {
            const std::string dir = command.args.count("dir") != 0 ? command.args.at("dir") : FLAGS_real_dir;
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "\u771f\u5b9e\u5c42\u8bfb\u5199\u901a\u8fc7",
                                         "\u771f\u5b9e\u5c42\u8bfb\u5199\u5931\u8d25",
                                         [&]() { return RunTierFileDemo(dir, "real", &last_real_logical_path_); });
        }
        if (action->id == "3") {
            const std::string dir = command.args.count("dir") != 0 ? command.args.at("dir") : FLAGS_virtual_dir;
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "\u865a\u62df\u5c42\u8bfb\u5199\u901a\u8fc7",
                                         "\u865a\u62df\u5c42\u8bfb\u5199\u5931\u8d25",
                                         [&]() { return RunTierFileDemo(dir, "virtual", &last_virtual_logical_path_); });
        }
        if (action->id == "4") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "Masstree \u5bfc\u5165\u5b8c\u6210",
                                         "Masstree \u5bfc\u5165\u5931\u8d25",
                                         [&]() { return RunMasstreeImportDemo(); });
        }
        if (action->id == "10") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "Masstree \u6a21\u677f\u751f\u6210\u5b8c\u6210",
                                         "Masstree \u6a21\u677f\u751f\u6210\u5931\u8d25",
                                         [&]() { return RunMasstreeTemplateGenerateDemo(); });
        }
        if (action->id == "5") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "Masstree \u67e5\u8be2\u5b8c\u6210",
                                         "Masstree \u67e5\u8be2\u5931\u8d25",
                                         [&]() { return RunMasstreeQueryDemo(); });
        }
        if (action->id == "20") {
            return ExecuteCapturedAction(*action,
                                         command.raw,
                                         "\u771f\u5b9e/\u865a\u62df\u8282\u70b9\u6570\u636e\u6e05\u7a7a\u5b8c\u6210",
                                         "\u771f\u5b9e/\u865a\u62df\u8282\u70b9\u6570\u636e\u6e05\u7a7a\u5931\u8d25",
                                         [&]() { return RunResetNodesScenario(); });
        }
        return BuildInfoResult(action->title, false, "\u672a\u5904\u7406\u7684\u64cd\u4f5c\u5206\u53d1", action->usage);
    }

    int RunInteractiveV2() {
        std::cout << "\u8bf7\u8f93\u5165\u64cd\u4f5c\u7f16\u53f7\uff0c\u53ef\u9644\u5e26 key=value \u53c2\u6570\u3002\n";
        for (;;) {
            zb::demo::RenderMenu("ZB Storage \u6f14\u793a\u63a7\u5236\u53f0", actions_);
            const std::string input = PromptLine("\u8f93\u5165");
            const zb::demo::ParsedCommand command = zb::demo::ParseCommandLine(input);
            if (!command.ok) {
                zb::demo::RenderResult(
                    BuildInfoResult("\u8f93\u5165\u9519\u8bef", false, command.error, "\u8bf7\u8f93\u5165 0\u30011\u30012\u30013\u30014\u30015\u30016 \u6216 q"));
                continue;
            }
            bool should_exit = false;
            zb::demo::DemoRunResult result = ExecuteInteractiveCommandV2(command, &should_exit);
            if (should_exit) {
                return 0;
            }
            if (!result.title.empty()) {
                zb::demo::RenderResult(result);
                MaybeAppendLog(result);
                last_result_ = result;
            }
        }
    }

    zb::demo::DemoRunResult BuildInfoResult(const std::string& title,
                                            bool ok,
                                            const std::string& summary,
                                            const std::string& usage,
                                            const std::string& raw_stdout = std::string(),
                                            const std::string& raw_stderr = std::string()) const {
        return zb::demo::BuildResultFromOutput(title,
                                               std::string(),
                                               usage,
                                               ok,
                                               summary,
                                               summary,
                                               raw_stdout,
                                               raw_stderr);
    }

    zb::demo::DemoRunResult ExecuteCapturedAction(const zb::demo::MenuActionSpec& action,
                                                  const std::string& command,
                                                  const std::string& success_summary,
                                                  const std::string& failure_summary,
                                                  const std::function<bool()>& fn) {
        zb::demo::ScopedStreamCapture capture;
        const bool ok = fn();
        return zb::demo::BuildResultFromOutput(action.title,
                                               command,
                                               action.usage,
                                               ok,
                                               success_summary,
                                               failure_summary,
                                               capture.Stdout(),
                                               capture.Stderr());
    }

    void MaybeAppendLog(const zb::demo::DemoRunResult& result) const {
        if (!FLAGS_enable_log_file || result.title.empty()) {
            return;
        }
        std::string error;
        if (!zb::demo::AppendRunLog(FLAGS_log_file, FLAGS_log_append, result, &error)) {
            std::cerr << "log_write_error=" << error << '\n';
        }
    }

    int RunScenarioCommand(const std::string& command,
                           const std::string& title,
                           const std::string& success_summary,
                           const std::string& failure_summary,
                           const std::function<bool()>& fn) {
        bool ok = false;
        std::string stdout_text;
        std::string stderr_text;
        {
            zb::demo::ScopedStreamCapture capture;
            ok = fn();
            stdout_text = capture.Stdout();
            stderr_text = capture.Stderr();
        }
        zb::demo::DemoRunResult result = zb::demo::BuildResultFromOutput(title,
                                                                         command,
                                                                         command,
                                                                         ok,
                                                                         success_summary,
                                                                         failure_summary,
                                                                         stdout_text,
                                                                         stderr_text);
        if (!result.raw_stdout.empty()) {
            std::cout << result.raw_stdout;
            if (result.raw_stdout.back() != '\n') {
                std::cout << '\n';
            }
        }
        if (!result.raw_stderr.empty()) {
            std::cerr << result.raw_stderr;
            if (result.raw_stderr.back() != '\n') {
                std::cerr << '\n';
            }
        }
        MaybeAppendLog(result);
        return result.ok ? 0 : 1;
    }

    std::string BuildHelpText() const {
        std::ostringstream out;
        out << "鑿滃崟鍔熻兘:\\n";
        for (const auto& action : actions_) {
            out << "  " << action.id << "  " << action.title;
            if (!action.description.empty()) {
                out << " - " << action.description;
            }
            out << "\n     usage: " << action.usage << '\n';
        }
        out << "\nExamples:\n";
        out << "  1 tc_p1_expected_real_node_count=1 tc_p1_expected_virtual_node_count=99\n";
        out << "  10 template_id=template-pathlist-100m path_list_file=examples/masstree_path_list_sample.txt repeat_dir_prefix=copy leaf_nodes_are_files=true\n";
        out << "  4 namespace=demo-ns generation=gen-report-001\n";
        out << "  4 namespace=demo-ns generation=gen-report-002 import_mode=real template_id=template-pathlist-100m template_mode=page_fast\n";
        out << "  5 n=1\n";
        out << "  5 n=1000 query_mode=random_path_lookup output_limit=5 log_file=logs/p5_run.log\n";
        out << "  5 n=1000 query_mode=random_inode output_limit=5 log_file=logs/p5_inode_run.log\n";
        out << "  20 force=true confirm=RESET_NODE_DATA scope=real,virtual\n";
        return out.str();
    }
    bool ApplyCommandArgs(const zb::demo::ParsedCommand& command, std::string* error) {
        for (const auto& item : command.args) {
            const std::string& key = item.first;
            const std::string& value = item.second;
            uint64_t parsed_u64 = 0;
            uint32_t parsed_u32 = 0;

            if (key == "mds") {
                FLAGS_mds = value;
            } else if (key == "scheduler") {
                FLAGS_scheduler = value;
            } else if (key == "log_file") {
                FLAGS_log_file = value;
            } else if (key == "enable_log_file") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_enable_log_file = parsed_bool;
            } else if (key == "log_append") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_log_append = parsed_bool;
            } else if (key == "mount" || key == "mount_point") {
                FLAGS_mount_point = value;
            } else if (key == "dir" || key == "script") {
                continue;
            } else if (key == "real_dir" || key == "real_root") {
                FLAGS_real_dir = value;
            } else if (key == "virtual_dir" || key == "virtual_root") {
                FLAGS_virtual_dir = value;
            } else if (key == "namespace" || key == "masstree_namespace_id") {
                FLAGS_masstree_namespace_id = value;
            } else if (key == "generation" || key == "masstree_generation_id") {
                FLAGS_masstree_generation_id = value;
	            } else if (key == "path_prefix" || key == "masstree_path_prefix") {
	                FLAGS_masstree_path_prefix = value;
	            } else if (key == "template_id" || key == "masstree_template_id") {
	                FLAGS_masstree_template_id = value;
	            } else if (key == "template_mode" || key == "masstree_template_mode") {
	                FLAGS_masstree_template_mode = value;
	            } else if (key == "import_mode" || key == "masstree_import_mode") {
	                FLAGS_masstree_import_mode = value;
	            } else if (key == "sim_state_path" || key == "masstree_sim_state_path") {
	                FLAGS_masstree_sim_state_path = value;
	            } else if (key == "sim_file_count" || key == "masstree_sim_file_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_file_count = parsed_u64;
	            } else if (key == "sim_inode_count" || key == "masstree_sim_inode_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_inode_count = parsed_u64;
	            } else if (key == "sim_dentry_count" || key == "masstree_sim_dentry_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_dentry_count = parsed_u64;
	            } else if (key == "sim_total_file_bytes" || key == "masstree_sim_total_file_bytes") {
	                FLAGS_masstree_sim_total_file_bytes = value;
	            } else if (key == "sim_metadata_bytes" || key == "masstree_sim_metadata_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_metadata_bytes = parsed_u64;
	            } else if (key == "sim_avg_file_size_bytes" || key == "masstree_sim_avg_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_avg_file_size_bytes = parsed_u64;
	            } else if (key == "sim_min_file_size_bytes" || key == "masstree_sim_min_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_min_file_size_bytes = parsed_u64;
	            } else if (key == "sim_max_file_size_bytes" || key == "masstree_sim_max_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_masstree_sim_max_file_size_bytes = parsed_u64;
	            } else if (key == "source_mode" || key == "masstree_source_mode") {
	                FLAGS_masstree_source_mode = value;
	            } else if (key == "path_list_file" || key == "masstree_path_list_file") {
	                FLAGS_masstree_path_list_file = value;
	            } else if (key == "repeat_dir_prefix" || key == "masstree_repeat_dir_prefix") {
	                FLAGS_masstree_repeat_dir_prefix = value;
	            } else if (key == "leaf_nodes_are_files" || key == "masstree_path_list_leaf_nodes_are_files") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_masstree_path_list_leaf_nodes_are_files = parsed_bool;
	            } else if (key == "max_files_per_leaf_dir" || key == "masstree_max_files_per_leaf_dir") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_max_files_per_leaf_dir = parsed_u32;
            } else if (key == "max_subdirs_per_dir" || key == "masstree_max_subdirs_per_dir") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_max_subdirs_per_dir = parsed_u32;
            } else if (key == "verify_inode_samples" || key == "masstree_verify_inode_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_verify_inode_samples = parsed_u32;
            } else if (key == "verify_dentry_samples" || key == "masstree_verify_dentry_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_verify_dentry_samples = parsed_u32;
            } else if (key == "job_poll_interval_ms" || key == "masstree_job_poll_interval_ms") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_job_poll_interval_ms = parsed_u32;
            } else if (key == "n" || key == "samples" || key == "masstree_query_samples") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_query_samples = parsed_u32;
            } else if (key == "output_limit" || key == "metadata_output_limit" ||
                       key == "success_output_limit" || key == "output_success_limit" ||
                       key == "masstree_query_output_success_limit") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_query_output_success_limit = parsed_u32;
            } else if (key == "success_latency_limit_ms" || key == "query_success_latency_limit_ms" ||
                       key == "masstree_query_success_latency_limit_ms") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_masstree_query_success_latency_limit_ms = parsed_u32;
            } else if (key == "query_mode" || key == "masstree_query_mode") {
                FLAGS_masstree_query_mode = value;
            } else if (key == "file_size_mb" || key == "posix_file_size_mb") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_posix_file_size_mb = parsed_u64;
            } else if (key == "chunk_size_kb" || key == "posix_chunk_size_kb") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_posix_chunk_size_kb = parsed_u32;
            } else if (key == "repeat" || key == "posix_repeat") {
                if (!ParseUint32Value(key, value, &parsed_u32, error)) {
                    return false;
                }
                FLAGS_posix_repeat = parsed_u32;
            } else if (key == "keep_file" || key == "posix_keep_file") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_keep_file = parsed_bool;
            } else if (key == "verify_hash" || key == "posix_verify_hash") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_verify_hash = parsed_bool;
            } else if (key == "sync_on_close" || key == "posix_sync_on_close") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_posix_sync_on_close = parsed_bool;
            } else if (key == "force" || key == "reset_nodes_force") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_reset_nodes_force = parsed_bool;
            } else if (key == "confirm" || key == "reset_nodes_confirm") {
                FLAGS_reset_nodes_confirm = value;
            } else if (key == "scope" || key == "reset_nodes_scope") {
                FLAGS_reset_nodes_scope = value;
            } else if (key == "purge_objects" || key == "reset_nodes_purge_objects") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_reset_nodes_purge_objects = parsed_bool;
            } else if (key == "purge_file_meta" || key == "reset_nodes_purge_file_meta") {
                bool parsed_bool = false;
                if (!ParseBoolValue(key, value, &parsed_bool, error)) {
                    return false;
                }
                FLAGS_reset_nodes_purge_file_meta = parsed_bool;
            } else if (key == "tc_p1_expected_real_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_real_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_virtual_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_virtual_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_online_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_online_disks_per_node") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_disks_per_node = parsed_u64;
            } else if (key == "tc_p1_expected_online_disk_capacity_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_online_disk_capacity_bytes = parsed_u64;
            } else if (key == "tc_p1_expected_optical_node_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_optical_node_count = parsed_u64;
            } else if (key == "tc_p1_expected_optical_device_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_optical_device_count = parsed_u64;
            } else if (key == "tc_p1_expected_cold_total_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_total_capacity_bytes = value;
            } else if (key == "tc_p1_expected_cold_used_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_used_capacity_bytes = value;
            } else if (key == "tc_p1_expected_cold_free_capacity_bytes") {
                FLAGS_tc_p1_expected_cold_free_capacity_bytes = value;
            } else if (key == "tc_p1_expected_total_file_count") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_total_file_count = parsed_u64;
            } else if (key == "tc_p1_expected_total_file_bytes") {
                FLAGS_tc_p1_expected_total_file_bytes = value;
            } else if (key == "tc_p1_expected_total_metadata_bytes") {
                FLAGS_tc_p1_expected_total_metadata_bytes = value;
            } else if (key == "tc_p1_expected_min_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_min_file_size_bytes = parsed_u64;
            } else if (key == "tc_p1_expected_max_file_size_bytes") {
                if (!ParseUint64Value(key, value, &parsed_u64, error)) {
                    return false;
                }
                FLAGS_tc_p1_expected_max_file_size_bytes = parsed_u64;
            } else {
                if (error) {
                    *error = "unknown parameter: " + key;
                }
                return false;
            }
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    zb::demo::DemoRunResult ExecuteInteractiveCommand(const zb::demo::ParsedCommand& command, bool* should_exit) {
        return ExecuteInteractiveCommandV2(command, should_exit);
    }

    int RunInteractive() {
        return RunInteractiveV2();
    }

    bool CheckTierDirectory(const std::string& logical_path) {
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup(logical_path, &attr, &status)) {
            std::cerr << "Lookup failed for " << logical_path << ": " << status.message() << '\n';
            return false;
        }
        if (attr.type() != zb::rpc::INODE_DIR) {
            std::cerr << logical_path << " exists but is not a directory\n";
            return false;
        }
        return true;
    }

    bool RunTierFileDemo(const std::string& dir_name,
                         const std::string& expected_tier,
                         std::string* saved_logical_path) {
        TierIoOptions options;
        options.dir_name = dir_name;
        options.expected_tier = expected_tier;
        options.file_size_bytes = FLAGS_posix_file_size_mb * 1024ULL * 1024ULL;
        options.chunk_size_bytes = std::max<uint32_t>(1U, FLAGS_posix_chunk_size_kb) * 1024U;
        options.repeat = std::max<uint32_t>(1U, FLAGS_posix_repeat);
        options.keep_file = FLAGS_posix_keep_file;
        options.verify_hash = FLAGS_posix_verify_hash;
        options.sync_on_close = FLAGS_posix_sync_on_close;
        return RunTierIoScenario(options, saved_logical_path);
    }

    bool BuildTierIoDiagnostics(TierIoDiagnostics* diagnostics) {
        if (!diagnostics) {
            return false;
        }
        if (!RefreshClusterView()) {
            return false;
        }
        CollectTierStats(nodes_, &diagnostics->real_stats, &diagnostics->virtual_stats);
        return true;
    }

    const TierStats& SelectTierStats(const std::string& tier, const TierIoDiagnostics& diagnostics) const {
        if (tier == "virtual") {
            return diagnostics.virtual_stats;
        }
        return diagnostics.real_stats;
    }



    void PrintTierIoDiagnostics(const TierIoOptions& options, const TierIoDiagnostics& diagnostics) {
        std::cout << "mount_point=" << FLAGS_mount_point << std::endl;
        const TierStats& tier_stats = SelectTierStats(options.expected_tier, diagnostics);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "_total_capacity_bytes",
                        tier_stats.total_capacity_bytes);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "_used_capacity_bytes",
                        tier_stats.used_capacity_bytes);
        PrintByteMetric(DisplayTierName(options.expected_tier) + "_free_capacity_bytes",
                        tier_stats.free_capacity_bytes);
    }

    std::string ExplainNoSpaceForTierWrite(const TierIoOptions& options,
                                           const TierIoDiagnostics& diagnostics) const {
        const TierStats& tier_stats = SelectTierStats(options.expected_tier, diagnostics);
        std::ostringstream oss;
        if (tier_stats.free_capacity_bytes < options.file_size_bytes) {
            oss << DisplayTierName(options.expected_tier)
                << " tier free capacity is smaller than the requested file size. free="
                << tier_stats.free_capacity_bytes << " (" << FormatBytes(tier_stats.free_capacity_bytes)
                << "), requested=" << options.file_size_bytes << " (" << FormatBytes(options.file_size_bytes)
                << "). MDS/FUSE reported ENOSPC.";
            return oss.str();
        }
        oss << DisplayTierName(options.expected_tier)
            << " tier still reported ENOSPC even though free capacity looks sufficient. "
               "Check the current MDS allocator state, node free-space reporting, and the "
               "FUSE no-space policy configuration.";
        return oss.str();
    }

    bool RunTierIoScenario(const TierIoOptions& options, std::string* saved_logical_path) {
        TierIoDiagnostics diagnostics;
        if (!BuildTierIoDiagnostics(&diagnostics)) {
            return false;
        }

        PrintSection("POSIX " + DisplayTierName(options.expected_tier) + " Tier Demo");
        std::cout << "target_dir=" << BuildTierLogicalPath(options.dir_name) + "/demo" << std::endl;
        std::cout << "repeat=" << options.repeat << std::endl;
        std::cout << "expected_file_size_bytes=" << options.file_size_bytes << " ("
                  << FormatBytes(options.file_size_bytes) << ")" << std::endl;
        std::cout << "chunk_size_bytes=" << options.chunk_size_bytes << " ("
                  << FormatBytes(options.chunk_size_bytes) << ")" << std::endl;
        PrintBoolMetric("verify_hash", options.verify_hash);
        PrintBoolMetric("keep_file", options.keep_file);
        PrintBoolMetric("sync_on_close", options.sync_on_close);
        PrintTierIoDiagnostics(options, diagnostics);

        uint64_t total_written = 0;
        uint64_t total_read = 0;
        uint64_t total_write_us = 0;
        uint64_t total_read_us = 0;
        TierIoIterationResult last_result;

        for (uint32_t attempt = 0; attempt < options.repeat; ++attempt) {
            TierIoIterationResult iteration;
            if (!RunSingleTierIoIteration(options, diagnostics, attempt, &iteration)) {
                return false;
            }
            last_result = iteration;
            total_written += iteration.bytes_written;
            total_read += iteration.bytes_read;
            total_write_us += iteration.write_elapsed_us;
            total_read_us += iteration.read_elapsed_us;
        }

        const double write_throughput_mib_s = ThroughputMiBS(total_written, total_write_us);
        const double read_throughput_mib_s = ThroughputMiBS(total_read, total_read_us);

        std::cout << "logical_path=" << last_result.logical_path << std::endl;
        std::cout << "mounted_path=" << last_result.mounted_path << std::endl;
        std::cout << "bytes_written=" << last_result.bytes_written << std::endl;
        std::cout << "bytes_read=" << last_result.bytes_read << std::endl;
        std::cout << "write_hash=" << FormatHex64(last_result.write_hash) << std::endl;
        std::cout << "read_hash=" << FormatHex64(last_result.read_hash) << std::endl;
        std::cout << "write_latency_ms="
                  << FormatDouble(static_cast<double>(last_result.write_elapsed_us) / 1000.0) << std::endl;
        std::cout << "read_latency_ms="
                  << FormatDouble(static_cast<double>(last_result.read_elapsed_us) / 1000.0) << std::endl;
        std::cout << "write_throughput_mib_s="
                  << FormatDouble(ThroughputMiBS(last_result.bytes_written, last_result.write_elapsed_us))
                  << std::endl;
        std::cout << "read_throughput_mib_s="
                  << FormatDouble(ThroughputMiBS(last_result.bytes_read, last_result.read_elapsed_us))
                  << std::endl;
        std::cout << "total_bytes_written=" << total_written << std::endl;
        std::cout << "total_bytes_read=" << total_read << std::endl;
        std::cout << "avg_write_latency_ms="
                  << FormatDouble(static_cast<double>(total_write_us) / 1000.0 / options.repeat) << std::endl;
        std::cout << "avg_read_latency_ms="
                  << FormatDouble(static_cast<double>(total_read_us) / 1000.0 / options.repeat) << std::endl;
        std::cout << "avg_write_throughput_mib_s=" << FormatDouble(write_throughput_mib_s) << std::endl;
        std::cout << "avg_read_throughput_mib_s=" << FormatDouble(read_throughput_mib_s) << std::endl;
        std::cout << "inode_id=" << last_result.inspection.inode_id << std::endl;
        std::cout << "inspected_size_bytes=" << last_result.inspection.size_bytes << " ("
                  << FormatBytes(last_result.inspection.size_bytes) << ")" << std::endl;
        std::cout << "inspected_node_id=" << last_result.inspection.node_id << std::endl;
        std::cout << "inspected_disk_id=" << last_result.inspection.disk_id << std::endl;
        std::cout << "inspected_tier=" << DisplayTierName(last_result.inspection.actual_tier) << std::endl;
        if (last_result.inspection.actual_tier == "virtual") {
            PrintByteMetric("object_unit_size_bytes", last_result.inspection.object_unit_size_bytes);
            std::cout << "object_count=" << last_result.inspection.object_count << std::endl;
            std::cout << "first_object_id=" << last_result.inspection.first_object_id << std::endl;
            std::cout << "last_object_id=" << last_result.inspection.last_object_id << std::endl;
            std::cout << "object_ids=" << JoinStrings(last_result.inspection.backend_objects, ", ") << std::endl;
        } else {
            std::cout << "backend_object_id=" << last_result.inspection.backend_object_id << std::endl;
            std::cout << "backend_objects=" << JoinStrings(last_result.inspection.backend_objects, ", ") << std::endl;
            std::cout << "backend_mount_point=" << last_result.inspection.backend_mount_point << std::endl;
            std::cout << "backend_object_path=" << last_result.inspection.backend_object_path << std::endl;
            std::cout << "backend_object_paths=" << JoinStrings(last_result.inspection.backend_object_paths, ", ")
                      << std::endl;
            PrintBoolMetric("backend_object_exists", last_result.inspection.backend_object_exists);
            PrintByteMetric("backend_object_size_bytes", last_result.inspection.backend_object_size_bytes);
            std::cout << "backend_object_hash=" << FormatHex64(last_result.inspection.backend_object_hash)
                      << std::endl;
            std::cout << "backend_dir_excerpt="
                      << JoinStrings(last_result.inspection.backend_dir_excerpt, ", ")
                      << std::endl;
        }

        const bool verify_content_hash =
            options.verify_hash && last_result.inspection.actual_tier != "virtual";

        std::vector<CheckResult> checks;
        AddCheck(&checks,
                 "io.bytes_written",
                 last_result.bytes_written == options.file_size_bytes,
                 "actual=" + std::to_string(last_result.bytes_written) +
                     " expected=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "io.bytes_read",
                 last_result.bytes_read == options.file_size_bytes,
                 "actual=" + std::to_string(last_result.bytes_read) +
                     " expected=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "io.hash_match",
                 !verify_content_hash || last_result.read_hash == last_result.write_hash,
                 !options.verify_hash
                     ? "hash verification disabled"
                     : (last_result.inspection.actual_tier == "virtual"
                            ? "virtual tier returns synthetic 'x' payload on read"
                            : ("write=" + FormatHex64(last_result.write_hash) +
                               " read=" + FormatHex64(last_result.read_hash))));
        AddCheck(&checks,
                 "metadata.size_bytes",
                 last_result.inspection.size_bytes == options.file_size_bytes,
                 "actual=" + std::to_string(last_result.inspection.size_bytes) +
                     " expected=" + std::to_string(options.file_size_bytes));
        AddCheck(&checks,
                 "metadata.tier",
                 last_result.inspection.actual_tier == options.expected_tier,
                 "actual=" + DisplayTierName(last_result.inspection.actual_tier) +
                     " expected=" + DisplayTierName(options.expected_tier));
        if (last_result.inspection.actual_tier == "virtual") {
            const uint32_t expected_object_count =
                options.file_size_bytes == 0 || last_result.inspection.object_unit_size_bytes == 0
                    ? 0
                    : static_cast<uint32_t>((options.file_size_bytes +
                                             last_result.inspection.object_unit_size_bytes - 1) /
                                            last_result.inspection.object_unit_size_bytes);
            AddCheck(&checks,
                     "virtual.object_count",
                     last_result.inspection.object_count == expected_object_count,
                     "actual=" + std::to_string(last_result.inspection.object_count) +
                         " expected=" + std::to_string(expected_object_count));
            AddCheck(&checks,
                     "virtual.object_ids",
                     static_cast<uint32_t>(last_result.inspection.backend_objects.size()) ==
                         last_result.inspection.object_count &&
                         !last_result.inspection.first_object_id.empty() &&
                         !last_result.inspection.last_object_id.empty(),
                     "first=" + last_result.inspection.first_object_id +
                         " last=" + last_result.inspection.last_object_id);
        } else {
            AddCheck(&checks,
                     "backend.object_exists",
                     last_result.inspection.backend_object_exists,
                     last_result.inspection.backend_object_path);
            AddCheck(&checks,
                     "backend.object_size",
                     last_result.inspection.backend_object_size_bytes == options.file_size_bytes,
                     "actual=" + std::to_string(last_result.inspection.backend_object_size_bytes) +
                         " expected=" + std::to_string(options.file_size_bytes));
            AddCheck(&checks,
                     "backend.object_hash",
                     !options.verify_hash || last_result.inspection.backend_object_hash == last_result.write_hash,
                     options.verify_hash ? ("backend=" + FormatHex64(last_result.inspection.backend_object_hash) +
                                            " write=" + FormatHex64(last_result.write_hash))
                                         : "backend hash verification disabled");
        }

        bool ok = true;
        for (const auto& check : checks) {
            ok = ok && check.ok;
        }
        if (saved_logical_path) {
            *saved_logical_path = options.keep_file ? last_result.logical_path : std::string();
        }
        return ok;
    }

    bool RunSingleTierIoIteration(const TierIoOptions& options,
                                  const TierIoDiagnostics& diagnostics,
                                  uint32_t attempt,
                                  TierIoIterationResult* result) {
        if (!result) {
            std::cerr << "tier I/O iteration result output pointer is null" << std::endl;
            return false;
        }
        const std::string token = TimestampToken() + (options.repeat > 1 ? ("_" + std::to_string(attempt + 1)) : "");
        const std::string logical_dir = BuildTierLogicalPath(options.dir_name) + "/demo";
        const std::string logical_path = logical_dir + "/" + options.expected_tier + "_" +
                                         std::to_string(options.file_size_bytes / (1024ULL * 1024ULL)) +
                                         "MB_" + token + ".bin";
        const std::string mounted_dir = JoinMountedPath(FLAGS_mount_point, logical_dir);
        const std::string mounted_path = JoinMountedPath(FLAGS_mount_point, logical_path);

        std::error_code ec;
        fs::create_directories(mounted_dir, ec);
        if (ec) {
            std::cerr << "failed to create mounted directory: " << mounted_dir << ": " << ec.message()
                      << std::endl;
            return false;
        }

        uint64_t write_hash = 14695981039346656037ULL;
        uint64_t bytes_written = 0;
        uint64_t write_elapsed_us = 0;
        int write_errno = 0;
        if (!WritePatternFile(mounted_path,
                              options.expected_tier,
                              options.chunk_size_bytes,
                              options.file_size_bytes,
                              options.sync_on_close,
                              &bytes_written,
                              &write_hash,
                              &write_elapsed_us,
                              &write_errno)) {
            if (write_errno == ENOSPC) {
                std::cerr << ExplainNoSpaceForTierWrite(options, diagnostics) << std::endl;
            }
            return false;
        }

        uint64_t read_hash = 14695981039346656037ULL;
        uint64_t bytes_read = 0;
        uint64_t read_elapsed_us = 0;
        const bool verify_read_hash =
            options.verify_hash && options.expected_tier != "virtual";
        if (!ReadPatternFile(mounted_path,
                             options.chunk_size_bytes,
                             verify_read_hash,
                             &bytes_read,
                             &read_hash,
                             &read_elapsed_us)) {
            return false;
        }

        FileInspectionResult inspection;
        if (!InspectFile(logical_path, options.expected_tier, &inspection)) {
            return false;
        }

        result->logical_path = logical_path;
        result->mounted_path = mounted_path;
        result->expected_size_bytes = options.file_size_bytes;
        result->bytes_written = bytes_written;
        result->bytes_read = bytes_read;
        result->write_elapsed_us = write_elapsed_us;
        result->read_elapsed_us = read_elapsed_us;
        result->write_hash = write_hash;
        result->read_hash = read_hash;
        result->inspection = inspection;

        if (!options.keep_file) {
            fs::remove(fs::path(mounted_path), ec);
            if (ec) {
                std::cerr << "failed to remove mounted file: " << mounted_path << ": " << ec.message()
                          << std::endl;
                return false;
            }
        }
        return true;
    }

    bool InspectKnownFile(const std::string& logical_path, const std::string& label) {
        if (logical_path.empty()) {
            std::cerr << "no recorded logical path is available for " << label << std::endl;
            return false;
        }
        PrintSection("Inspect " + label + " File");
        return InspectFile(logical_path, "", nullptr);
    }

    bool InspectFile(const std::string& logical_path,
                     const std::string& expected_tier,
                     FileInspectionResult* out) {
        zb::rpc::InodeAttr attr;
        zb::rpc::MdsStatus status;
        if (!mds_.Lookup(logical_path, &attr, &status)) {
            std::cerr << "Lookup failed for " << logical_path << ": " << status.message() << std::endl;
            return false;
        }
        zb::rpc::FileLocationView view;
        if (!mds_.GetFileLocation(attr.inode_id(), &view, &status)) {
            std::cerr << "GetFileLocation failed for inode=" << attr.inode_id() << ": " << status.message()
                      << std::endl;
            return false;
        }
        std::string node_id;
        std::string disk_id;
        (void)ResolveNodeAndDiskFromLocationView(view, &node_id, &disk_id);
        const std::string base_node_id = BaseNodeIdFromVirtual(node_id);
        auto it = node_type_by_id_.find(base_node_id);
        std::string actual_tier = "unknown";
        if (it != node_type_by_id_.end()) {
            actual_tier = NodeTypeName(it->second);
        } else if (node_id != base_node_id) {
            actual_tier = "virtual";
        }

        constexpr uint64_t kDefaultObjectUnitSize = 4ULL * 1024ULL * 1024ULL;
        std::string backend_mount_point;
        if (!disk_id.empty()) {
            const fs::path run_dir = ResolveRunDirFromMountPoint(FLAGS_mount_point);
            if (actual_tier == "real") {
                backend_mount_point = (run_dir / "data" / "real" / "disks" / disk_id).string();
            } else if (actual_tier == "virtual") {
                backend_mount_point = (run_dir / "data" / "virtual" / "mount" / disk_id).string();
            }
        }
        std::string backend_object_path;
        std::vector<std::string> backend_object_paths;
        bool backend_object_exists = false;
        uint64_t backend_object_size_bytes = 0;
        uint64_t backend_object_hash = 0;
        std::vector<std::string> backend_dir_excerpt;
        std::vector<std::string> backend_objects;
        if (!backend_mount_point.empty() || actual_tier == "virtual") {
            const uint64_t object_count =
                attr.size() == 0 ? 0 : ((attr.size() - 1U) / kDefaultObjectUnitSize) + 1U;
            backend_object_hash = 14695981039346656037ULL;
            backend_object_exists = object_count > 0;
            for (uint64_t index = 0; index < object_count; ++index) {
                const std::string object_id = BuildStableObjectId(attr.inode_id(), static_cast<uint32_t>(index));
                if (index == 0) {
                    if (actual_tier != "virtual") {
                        const std::string prefix = BuildObjectPrefix(object_id);
                        const fs::path object_dir =
                            fs::path(backend_mount_point) / prefix.substr(0, 2) / prefix.substr(2, 2);
                        backend_object_path = (object_dir / object_id).string();
                        backend_dir_excerpt = BuildDirectoryExcerpt(object_dir);
                    }
                }
                backend_objects.push_back(object_id);
                if (actual_tier == "virtual") {
                    continue;
                }
                const std::string prefix = BuildObjectPrefix(object_id);
                const fs::path object_dir = fs::path(backend_mount_point) / prefix.substr(0, 2) / prefix.substr(2, 2);
                const fs::path object_path = object_dir / object_id;
                backend_object_paths.push_back(object_path.string());
                std::error_code ec;
                const bool exists = fs::exists(object_path, ec);
                if (ec || !exists) {
                    backend_object_exists = false;
                    continue;
                }
                if (!AppendFileHash(object_path, &backend_object_hash, &backend_object_size_bytes)) {
                    backend_object_exists = false;
                }
            }
        }

        if (out) {
            out->inode_id = attr.inode_id();
            out->size_bytes = attr.size();
            out->node_id = node_id;
            out->disk_id = disk_id;
            out->actual_tier = actual_tier;
            out->object_unit_size_bytes = kDefaultObjectUnitSize;
            out->object_count = static_cast<uint32_t>(backend_objects.size());
            out->first_object_id = backend_objects.empty() ? std::string() : backend_objects.front();
            out->last_object_id = backend_objects.empty() ? std::string() : backend_objects.back();
            out->backend_object_id = backend_objects.empty() ? std::string() : backend_objects.front();
            out->backend_objects = backend_objects;
            out->backend_mount_point = backend_mount_point;
            out->backend_object_path = backend_object_path;
            out->backend_object_paths = backend_object_paths;
            out->backend_object_exists = backend_object_exists;
            out->backend_object_size_bytes = backend_object_size_bytes;
            out->backend_object_hash = backend_object_hash;
            out->backend_dir_excerpt = std::move(backend_dir_excerpt);
        }
        if (!expected_tier.empty() && actual_tier != expected_tier) {
            std::cerr << "tier mismatch: expected " << DisplayTierName(expected_tier) << ", actual "
                      << DisplayTierName(actual_tier) << std::endl;
            return false;
        }
        return true;
    }
    bool RunMasstreeTemplateGenerateDemo() {
        PrintSection("Masstree Template Generate Demo");
        if (FLAGS_masstree_template_id.empty()) {
            std::cerr << "template_id is required for template generation\n";
            return false;
        }
        if (FLAGS_masstree_path_list_file.empty()) {
            std::cerr << "path_list_file is required for template generation\n";
            return false;
        }

        zb::rpc::GenerateMasstreeTemplateRequest request;
        request.set_template_id(FLAGS_masstree_template_id);
        request.set_path_list_file(FLAGS_masstree_path_list_file);
        if (!FLAGS_masstree_repeat_dir_prefix.empty()) {
            request.set_repeat_dir_prefix(FLAGS_masstree_repeat_dir_prefix);
        }
        request.set_path_list_leaf_nodes_are_files(FLAGS_masstree_path_list_leaf_nodes_are_files);
        request.set_verify_inode_samples(FLAGS_masstree_verify_inode_samples);
        request.set_verify_dentry_samples(FLAGS_masstree_verify_dentry_samples);

        zb::rpc::GenerateMasstreeTemplateReply reply;
        if (!mds_.GenerateMasstreeTemplate(request, &reply)) {
            std::cerr << "GenerateMasstreeTemplate failed: " << reply.status().message() << '\n';
            return false;
        }

        std::cout << "template_id=" << FLAGS_masstree_template_id << '\n';
        std::cout << "path_list_file=" << FLAGS_masstree_path_list_file << '\n';
        if (!FLAGS_masstree_repeat_dir_prefix.empty()) {
            std::cout << "repeat_dir_prefix=" << FLAGS_masstree_repeat_dir_prefix << '\n';
        }
        PrintBoolMetric("path_list_leaf_nodes_are_files", FLAGS_masstree_path_list_leaf_nodes_are_files);
        std::cout << "job_id=" << reply.job_id() << '\n';

        const auto poll_started_at = std::chrono::steady_clock::now();
        zb::rpc::MasstreeImportJobState last_state = zb::rpc::MASSTREE_IMPORT_JOB_PENDING;
        bool has_last_state = false;
        uint64_t last_printed_elapsed_bucket = std::numeric_limits<uint64_t>::max();

        while (true) {
            zb::rpc::GetMasstreeImportJobReply job_reply;
            if (!mds_.GetMasstreeImportJob(reply.job_id(), &job_reply)) {
                std::cerr << "GetMasstreeImportJob failed: " << job_reply.status().message() << '\n';
                return false;
            }
            if (!job_reply.found()) {
                std::cerr << "Masstree template job disappeared: " << reply.job_id() << '\n';
                return false;
            }

            const auto& job = job_reply.job();
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - poll_started_at);
            const uint64_t elapsed_seconds = static_cast<uint64_t>(elapsed.count());
            const uint64_t elapsed_bucket = elapsed_seconds / 10ULL;
            if (!has_last_state || job.state() != last_state || elapsed_bucket != last_printed_elapsed_bucket) {
                std::cout << "job_status=" << MasstreeJobStateName(job.state())
                          << " elapsed=" << FormatDurationSeconds(elapsed_seconds) << '\n';
                last_state = job.state();
                has_last_state = true;
                last_printed_elapsed_bucket = elapsed_bucket;
            }

            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED) {
                std::cout << "manifest_path=" << job.manifest_path() << '\n';
                std::cout << "root_inode_id=" << job.root_inode_id() << '\n';
                std::cout << "inode_count=" << job.inode_count() << '\n';
                std::cout << "dentry_count=" << job.dentry_count() << '\n';
                std::cout << "file_count=" << job.file_count() << '\n';
                std::cout << "inode_range=[" << job.inode_min() << ", " << job.inode_max() << "]\n";
                std::cout << "inode_pages_bytes=" << job.inode_pages_bytes() << '\n';
                PrintDecimalMetric("template_total_file_bytes", job.total_file_bytes());
                std::cout << "template_avg_file_size_bytes=" << job.avg_file_size_bytes() << '\n';

                std::vector<CheckResult> checks;
                AddCheck(&checks,
                         "template.template_id",
                         job.template_id() == FLAGS_masstree_template_id,
                         "actual=" + job.template_id() + " expected=" + FLAGS_masstree_template_id);
                AddCheck(&checks,
                         "template.manifest_path",
                         !job.manifest_path().empty(),
                         "manifest_path=" + job.manifest_path());
                AddCheck(&checks,
                         "template.file_count",
                         job.file_count() > 0,
                         "actual=" + std::to_string(job.file_count()));

                bool ok = true;
                for (const auto& check : checks) {
                    ok = ok && check.ok;
                }
                return ok;
            }
            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_FAILED) {
                std::cerr << "Masstree template job failed: " << job.error_message() << '\n';
                return false;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::max<uint32_t>(1, FLAGS_masstree_job_poll_interval_ms)));
        }
    }

    bool RunMasstreeImportDemo() {
        const std::string mode = ToLowerCopy(TrimCopy(FLAGS_masstree_import_mode));
        if (mode.empty() || mode == "simulated" || mode == "simulate" || mode == "sim") {
            return RunMasstreeImportDemoSimulated();
        }
        if (mode == "real") {
            return RunMasstreeImportDemoReal();
        }
        std::cerr << "Unsupported masstree_import_mode: " << FLAGS_masstree_import_mode
                  << " (expected simulated|real)\n";
        return false;
    }

    bool RunMasstreeImportDemoSimulated() {
        PrintSection("Masstree Import Demo");
        zb::rpc::GetMasstreeClusterStatsReply real_baseline;
        if (!mds_.GetMasstreeClusterStats(&real_baseline)) {
            std::cerr << "GetMasstreeClusterStats(real baseline) failed: "
                      << real_baseline.status().message() << '\n';
            return false;
        }
        std::map<std::string, SimulatedMasstreeImportRecord> records;
        std::string error;
        if (!LoadSimulatedMasstreeRecords(&records, &error)) {
            std::cerr << "Load simulated Masstree state failed: " << error << '\n';
            return false;
        }
        const std::string namespace_id =
            MakeUniqueSimulatedMasstreeNamespaceId(FLAGS_masstree_namespace_id, records);
        const std::string generation_id = FLAGS_masstree_generation_id.empty()
                                              ? "gen-" + TimestampToken()
                                              : FLAGS_masstree_generation_id;
        const std::string path_prefix = FLAGS_masstree_path_prefix.empty()
                                            ? "/masstree_demo/" + namespace_id
                                            : NormalizeLogicalPath(FLAGS_masstree_path_prefix);
        const SimulatedMasstreeOverlayStats before = AggregateSimulatedMasstreeRecords(records);
        const auto previous = records.find(namespace_id);
        const bool had_previous_namespace = previous != records.end();
        const SimulatedMasstreeImportRecord previous_record =
            had_previous_namespace ? previous->second : SimulatedMasstreeImportRecord();

        SimulatedMasstreeImportRecord record;
        record.namespace_id = namespace_id;
        record.generation_id = generation_id;
        record.path_prefix = path_prefix;
        record.file_count = FLAGS_masstree_sim_file_count;
        record.inode_count = FLAGS_masstree_sim_inode_count;
        record.dentry_count = FLAGS_masstree_sim_dentry_count;
        record.total_file_bytes = zb::mds::NormalizeDecimalString(FLAGS_masstree_sim_total_file_bytes);
        record.total_metadata_bytes = std::to_string(FLAGS_masstree_sim_metadata_bytes);
        record.avg_file_size_bytes = FLAGS_masstree_sim_avg_file_size_bytes;
        record.min_file_size_bytes = FLAGS_masstree_sim_min_file_size_bytes;
        record.max_file_size_bytes = FLAGS_masstree_sim_max_file_size_bytes;
        records[namespace_id] = record;

        if (!SaveSimulatedMasstreeRecords(records, &error)) {
            std::cerr << "Save simulated Masstree state failed: " << error << '\n';
            return false;
        }
        const SimulatedMasstreeOverlayStats after = AggregateSimulatedMasstreeRecords(records);
        const uint64_t combined_before_total_file_count =
            real_baseline.total_file_count() + before.total_file_count;
        const uint64_t combined_after_total_file_count =
            real_baseline.total_file_count() + after.total_file_count;
        const std::string combined_before_total_file_bytes =
            zb::mds::AddDecimalStrings(real_baseline.total_file_bytes(), before.total_file_bytes);
        const std::string combined_after_total_file_bytes =
            zb::mds::AddDecimalStrings(real_baseline.total_file_bytes(), after.total_file_bytes);
        const std::string combined_before_total_metadata_bytes =
            zb::mds::AddDecimalStrings(real_baseline.total_metadata_bytes(), before.total_metadata_bytes);
        const std::string combined_after_total_metadata_bytes =
            zb::mds::AddDecimalStrings(real_baseline.total_metadata_bytes(), after.total_metadata_bytes);
        const std::string combined_before_used_capacity_bytes =
            zb::mds::AddDecimalStrings(real_baseline.used_capacity_bytes(), before.total_file_bytes);
        const std::string combined_after_used_capacity_bytes =
            zb::mds::AddDecimalStrings(real_baseline.used_capacity_bytes(), after.total_file_bytes);

        last_masstree_namespace_id_ = namespace_id;
        last_masstree_path_prefix_ = path_prefix;
        last_masstree_generation_id_ = generation_id;
        last_masstree_manifest_path_ = SimulatedMasstreeStatePath();

        std::cout << "import_mode=simulated\n";
        std::cout << "state_path=" << SimulatedMasstreeStatePath() << '\n';
        std::cout << "namespace_id=" << namespace_id << '\n';
        std::cout << "generation_id=" << generation_id << '\n';
        std::cout << "path_prefix=" << path_prefix << '\n';
        std::cout << "job_status=completed elapsed=0s\n";
        std::cout << "manifest_path=" << last_masstree_manifest_path_ << '\n';
        std::cout << "root_inode_id=1\n";
        std::cout << "inode_count=" << record.inode_count << '\n';
        std::cout << "dentry_count=" << record.dentry_count << '\n';
        std::cout << "file_count=" << record.file_count << '\n';
        PrintDemoFileSizeDecimalMetric("import_total_file_bytes", record.total_file_bytes);
        std::cout << "import_avg_file_size_bytes=" << ScaleDemoFileSize(record.avg_file_size_bytes) << '\n';
        std::cout << "inode_range=[1, " << record.inode_count << "]\n";
        std::cout << "inode_pages_bytes=" << record.total_metadata_bytes << '\n';
        std::cout << "previous_namespace_present=" << (had_previous_namespace ? "true" : "false") << '\n';
        if (had_previous_namespace) {
            std::cout << "previous_namespace_generation_id=" << previous_record.generation_id << '\n';
            std::cout << "previous_namespace_file_count=" << previous_record.file_count << '\n';
            PrintDemoFileSizeDecimalMetric("previous_namespace_total_file_bytes", previous_record.total_file_bytes);
            PrintDecimalMetric("previous_namespace_total_metadata_bytes", previous_record.total_metadata_bytes);
        }
        std::cout << "before_total_file_count=" << combined_before_total_file_count << '\n';
        PrintDemoFileSizeDecimalMetric("before_total_file_bytes", combined_before_total_file_bytes);
        PrintDecimalMetric("before_total_metadata_bytes", combined_before_total_metadata_bytes);
        PrintDemoFileSizeDecimalMetric("before_used_capacity_bytes", combined_before_used_capacity_bytes);
        std::cout << "after_total_file_count=" << combined_after_total_file_count << '\n';
        PrintDemoFileSizeDecimalMetric("after_total_file_bytes", combined_after_total_file_bytes);
        PrintDecimalMetric("after_total_metadata_bytes", combined_after_total_metadata_bytes);
        PrintDemoFileSizeDecimalMetric("after_used_capacity_bytes", combined_after_used_capacity_bytes);
        std::cout << "delta_total_file_count="
                  << FormatSignedUint64Delta(combined_after_total_file_count,
                                             combined_before_total_file_count) << '\n';
        std::cout << "delta_total_file_bytes="
                  << ScaleSignedDemoFileSizeDecimal(
                         FormatSignedDecimalDelta(combined_after_total_file_bytes,
                                                  combined_before_total_file_bytes))
                  << '\n';
        std::cout << "delta_total_metadata_bytes="
                  << FormatSignedDecimalDelta(combined_after_total_metadata_bytes,
                                              combined_before_total_metadata_bytes) << '\n';
        std::cout << "delta_used_capacity_bytes="
                  << ScaleSignedDemoFileSizeDecimal(
                         FormatSignedDecimalDelta(combined_after_used_capacity_bytes,
                                                  combined_before_used_capacity_bytes))
                  << '\n';
        return true;
    }

    bool RunMasstreeImportDemoReal() {
        PrintSection("Masstree Import Demo");
        const std::string namespace_id = FLAGS_masstree_namespace_id.empty()
                                             ? "demo-ns"
                                             : FLAGS_masstree_namespace_id;
        const std::string generation_id = FLAGS_masstree_generation_id.empty()
                                              ? "gen-" + TimestampToken()
                                              : FLAGS_masstree_generation_id;
        const std::string path_prefix = FLAGS_masstree_path_prefix.empty()
                                            ? "/masstree_demo/" + namespace_id
                                            : NormalizeLogicalPath(FLAGS_masstree_path_prefix);

        zb::rpc::GetMasstreeClusterStatsReply cluster_before;
        if (!mds_.GetMasstreeClusterStats(&cluster_before)) {
            std::cerr << "GetMasstreeClusterStats(before) failed: " << cluster_before.status().message() << '\n';
            return false;
        }

        bool had_previous_namespace = false;
        zb::rpc::GetMasstreeNamespaceStatsReply previous_namespace_stats;
        if (!mds_.GetMasstreeNamespaceStats(namespace_id, &previous_namespace_stats)) {
            std::cerr << "GetMasstreeNamespaceStats(before) failed: "
                      << previous_namespace_stats.status().message() << '\n';
            return false;
        }
        if (previous_namespace_stats.status().code() == zb::rpc::MDS_OK) {
            had_previous_namespace = true;
        } else if (previous_namespace_stats.status().code() != zb::rpc::MDS_NOT_FOUND) {
            std::cerr << "GetMasstreeNamespaceStats(before) failed: "
                      << previous_namespace_stats.status().message() << '\n';
            return false;
        }

        const std::string selected_template_id =
            current_command_has_template_id_ ? FLAGS_masstree_template_id
                                             : (FLAGS_scenario != "interactive" ? FLAGS_masstree_template_id
                                                                                 : std::string());

        zb::rpc::ImportMasstreeNamespaceRequest request;
	        request.set_namespace_id(namespace_id);
	        request.set_generation_id(generation_id);
	        request.set_path_prefix(path_prefix);
	        request.set_template_id(selected_template_id);
	        request.set_template_mode(FLAGS_masstree_template_mode);
        request.set_verify_inode_samples(FLAGS_masstree_verify_inode_samples);
        request.set_verify_dentry_samples(FLAGS_masstree_verify_dentry_samples);
        request.set_publish_route(true);

        zb::rpc::ImportMasstreeNamespaceReply reply;
        if (!mds_.ImportMasstreeNamespace(request, &reply)) {
            std::cerr << "ImportMasstreeNamespace failed: " << reply.status().message() << '\n';
            return false;
        }
        last_masstree_namespace_id_ = namespace_id;
        last_masstree_path_prefix_ = path_prefix;
        last_masstree_generation_id_ = generation_id;
        std::cout << "namespace_id=" << namespace_id << '\n';
        std::cout << "generation_id=" << generation_id << '\n';
        std::cout << "path_prefix=" << path_prefix << '\n';
	        if (!selected_template_id.empty()) {
	            std::cout << "template_id=" << selected_template_id << '\n';
	        }
	        if (!FLAGS_masstree_template_mode.empty()) {
	            std::cout << "template_mode=" << FLAGS_masstree_template_mode << '\n';
	        }
	        std::cout << "job_id=" << reply.job_id() << '\n';

        const auto poll_started_at = std::chrono::steady_clock::now();
        zb::rpc::MasstreeImportJobState last_state = zb::rpc::MASSTREE_IMPORT_JOB_PENDING;
        bool has_last_state = false;
        uint64_t last_printed_elapsed_bucket = std::numeric_limits<uint64_t>::max();
        bool printed_selected_template_id = !selected_template_id.empty();

        while (true) {
            zb::rpc::GetMasstreeImportJobReply job_reply;
            if (!mds_.GetMasstreeImportJob(reply.job_id(), &job_reply)) {
                std::cerr << "GetMasstreeImportJob failed: " << job_reply.status().message() << '\n';
                return false;
            }
            if (!job_reply.found()) {
                std::cerr << "Masstree import job disappeared: " << reply.job_id() << '\n';
                return false;
            }

            const auto& job = job_reply.job();
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - poll_started_at);
            const uint64_t elapsed_seconds = static_cast<uint64_t>(elapsed.count());
            const uint64_t elapsed_bucket = elapsed_seconds / 10ULL;
            if (!printed_selected_template_id && !job.template_id().empty()) {
                std::cout << "template_id=" << job.template_id() << '\n';
                printed_selected_template_id = true;
            }
            if (!has_last_state || job.state() != last_state || elapsed_bucket != last_printed_elapsed_bucket) {
                std::cout << "job_status=" << MasstreeJobStateName(job.state())
                          << " elapsed=" << FormatDurationSeconds(elapsed_seconds) << '\n';
                last_state = job.state();
                has_last_state = true;
                last_printed_elapsed_bucket = elapsed_bucket;
            }
            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED) {
                last_masstree_manifest_path_ = job.manifest_path();
                zb::rpc::GetMasstreeClusterStatsReply cluster_after;
                if (!mds_.GetMasstreeClusterStats(&cluster_after)) {
                    std::cerr << "GetMasstreeClusterStats(after) failed: " << cluster_after.status().message() << '\n';
                    return false;
                }

                const uint64_t previous_namespace_file_count =
                    had_previous_namespace ? previous_namespace_stats.file_count() : 0;
                const std::string previous_namespace_total_file_bytes =
                    had_previous_namespace ? previous_namespace_stats.total_file_bytes() : std::string("0");
                const std::string previous_namespace_total_metadata_bytes =
                    had_previous_namespace ? previous_namespace_stats.total_metadata_bytes() : std::string("0");
                const std::string replaced_total_file_bytes_base =
                    zb::mds::CompareDecimalStrings(cluster_before.total_file_bytes(),
                                                   previous_namespace_total_file_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.total_file_bytes(),
                                                          previous_namespace_total_file_bytes)
                        : std::string("0");
                const std::string replaced_total_metadata_bytes_base =
                    zb::mds::CompareDecimalStrings(cluster_before.total_metadata_bytes(),
                                                   previous_namespace_total_metadata_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.total_metadata_bytes(),
                                                          previous_namespace_total_metadata_bytes)
                        : std::string("0");
                const std::string replaced_used_capacity_base =
                    zb::mds::CompareDecimalStrings(cluster_before.used_capacity_bytes(),
                                                   previous_namespace_total_file_bytes) >= 0
                        ? zb::mds::SubtractDecimalStrings(cluster_before.used_capacity_bytes(),
                                                          previous_namespace_total_file_bytes)
                        : std::string("0");
                const uint64_t expected_total_file_count =
                    cluster_before.total_file_count() >= previous_namespace_file_count
                        ? (cluster_before.total_file_count() - previous_namespace_file_count + job.file_count())
                        : job.file_count();
                const std::string expected_total_file_bytes =
                    zb::mds::AddDecimalStrings(replaced_total_file_bytes_base, job.total_file_bytes());
                const std::string expected_total_metadata_bytes =
                    zb::mds::AddDecimalStrings(replaced_total_metadata_bytes_base,
                                               std::to_string(job.inode_pages_bytes()));
                const std::string expected_used_capacity_bytes =
                    zb::mds::AddDecimalStrings(replaced_used_capacity_base, job.total_file_bytes());
                const std::string expected_free_capacity_bytes =
                    zb::mds::SubtractDecimalStrings(cluster_after.total_capacity_bytes(),
                                                    expected_used_capacity_bytes);
                const std::string demo_before_used_capacity =
                    ScaleDemoFileSizeDecimal(cluster_before.used_capacity_bytes());
                const std::string demo_after_used_capacity =
                    ScaleDemoFileSizeDecimal(cluster_after.used_capacity_bytes());
                const std::string demo_before_free_capacity =
                    zb::mds::SubtractDecimalStrings(cluster_before.total_capacity_bytes(),
                                                    demo_before_used_capacity);
                const std::string demo_after_free_capacity =
                    zb::mds::SubtractDecimalStrings(cluster_after.total_capacity_bytes(),
                                                    demo_after_used_capacity);

                std::cout << "manifest_path=" << job.manifest_path() << '\n';
                std::cout << "root_inode_id=" << job.root_inode_id() << '\n';
                std::cout << "inode_count=" << job.inode_count() << '\n';
                std::cout << "dentry_count=" << job.dentry_count() << '\n';
                std::cout << "level1_dir_count=" << job.level1_dir_count() << '\n';
                std::cout << "leaf_dir_count=" << job.leaf_dir_count() << '\n';
                std::cout << "file_count=" << job.file_count() << '\n';
                PrintDemoFileSizeDecimalMetric("import_total_file_bytes", job.total_file_bytes());
                std::cout << "import_avg_file_size_bytes=" << ScaleDemoFileSize(job.avg_file_size_bytes()) << '\n';
                std::cout << "inode_range=[" << job.inode_min() << ", " << job.inode_max() << "]\n";
                std::cout << "inode_pages_bytes=" << job.inode_pages_bytes() << '\n';
                std::cout << "previous_namespace_present=" << (had_previous_namespace ? "true" : "false") << '\n';
                if (had_previous_namespace) {
                    std::cout << "previous_namespace_generation_id=" << previous_namespace_stats.generation_id() << '\n';
                    std::cout << "previous_namespace_file_count=" << previous_namespace_stats.file_count() << '\n';
                    PrintDemoFileSizeDecimalMetric("previous_namespace_total_file_bytes",
                                                   previous_namespace_stats.total_file_bytes());
                    PrintDecimalMetric("previous_namespace_total_metadata_bytes",
                                       previous_namespace_stats.total_metadata_bytes());
                }
                std::cout << "before_total_file_count=" << cluster_before.total_file_count() << '\n';
                PrintDemoFileSizeDecimalMetric("before_total_file_bytes", cluster_before.total_file_bytes());
                PrintDecimalMetric("before_total_metadata_bytes", cluster_before.total_metadata_bytes());
                PrintDecimalMetric("before_used_capacity_bytes", demo_before_used_capacity);
                PrintDecimalMetric("before_free_capacity_bytes", demo_before_free_capacity);
                std::cout << "after_total_file_count=" << cluster_after.total_file_count() << '\n';
                PrintDemoFileSizeDecimalMetric("after_total_file_bytes", cluster_after.total_file_bytes());
                PrintDecimalMetric("after_total_metadata_bytes", cluster_after.total_metadata_bytes());
                PrintDecimalMetric("after_used_capacity_bytes", demo_after_used_capacity);
                PrintDecimalMetric("after_free_capacity_bytes", demo_after_free_capacity);
                std::cout << "delta_total_file_count="
                          << FormatSignedUint64Delta(cluster_after.total_file_count(),
                                                    cluster_before.total_file_count()) << '\n';
                std::cout << "delta_total_file_bytes="
                          << ScaleSignedDemoFileSizeDecimal(
                                 FormatSignedDecimalDelta(cluster_after.total_file_bytes(),
                                                          cluster_before.total_file_bytes()))
                          << '\n';
                std::cout << "delta_total_metadata_bytes="
                          << FormatSignedDecimalDelta(cluster_after.total_metadata_bytes(),
                                                      cluster_before.total_metadata_bytes()) << '\n';
                std::cout << "delta_used_capacity_bytes="
                          << ScaleSignedDemoFileSizeDecimal(
                                 FormatSignedDecimalDelta(cluster_after.used_capacity_bytes(),
                                                          cluster_before.used_capacity_bytes()))
                          << '\n';
                std::cout << "delta_free_capacity_bytes="
                          << FormatSignedDecimalDelta(demo_after_free_capacity,
                                                      demo_before_free_capacity) << '\n';

                std::vector<CheckResult> checks;
                AddCheck(&checks,
                         "import.namespace_id",
                         job.namespace_id() == namespace_id,
                         "actual=" + job.namespace_id() + " expected=" + namespace_id);
                AddCheck(&checks,
                         "import.generation_id",
                         job.generation_id() == generation_id,
                         "actual=" + job.generation_id() + " expected=" + generation_id);
                AddCheck(&checks,
                         "import.path_prefix",
                         job.path_prefix() == path_prefix,
                         "actual=" + job.path_prefix() + " expected=" + path_prefix);
                AddCheck(&checks,
                         "stats.total_file_count_after",
                         cluster_after.total_file_count() == expected_total_file_count,
                         "actual=" + std::to_string(cluster_after.total_file_count()) +
                             " expected=" + std::to_string(expected_total_file_count));
                AddCheck(&checks,
                         "stats.total_file_bytes_after",
                         zb::mds::CompareDecimalStrings(cluster_after.total_file_bytes(),
                                                        expected_total_file_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.total_file_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_total_file_bytes));
                AddCheck(&checks,
                         "stats.total_metadata_bytes_after",
                         zb::mds::CompareDecimalStrings(cluster_after.total_metadata_bytes(),
                                                        expected_total_metadata_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.total_metadata_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_total_metadata_bytes));
                AddCheck(&checks,
                         "stats.used_capacity_after",
                         zb::mds::CompareDecimalStrings(cluster_after.used_capacity_bytes(),
                                                        expected_used_capacity_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.used_capacity_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_used_capacity_bytes));
                AddCheck(&checks,
                         "stats.free_capacity_after",
                         zb::mds::CompareDecimalStrings(cluster_after.free_capacity_bytes(),
                                                        expected_free_capacity_bytes) == 0,
                         "actual=" + FormatDecimalBytes(cluster_after.free_capacity_bytes()) +
                             " expected=" + FormatDecimalBytes(expected_free_capacity_bytes));

                bool ok = true;
                for (const auto& check : checks) {
                    ok = ok && check.ok;
                }
                return ok;
            }
            if (job.state() == zb::rpc::MASSTREE_IMPORT_JOB_FAILED) {
                std::cerr << "Masstree import job failed: " << job.error_message() << '\n';
                return false;
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(std::max<uint32_t>(1, FLAGS_masstree_job_poll_interval_ms)));
        }
    }

    bool LoadLastMasstreeManifest(LocalMasstreeNamespaceManifest* manifest, std::string* error) const {
        if (!manifest) {
            if (error) {
                *error = "manifest output is null";
            }
            return false;
        }
        if (last_masstree_manifest_path_.empty()) {
            if (error) {
                *error = "random_path_lookup requires a namespace imported in the current demo process";
            }
            return false;
        }
        return LocalMasstreeNamespaceManifest::LoadFromFile(last_masstree_manifest_path_, manifest, error);
    }

    bool ReadNextNormalizedPathListLine(std::ifstream* input,
                                        std::string* normalized_path,
                                        bool* explicit_dir,
                                        std::string* error) const {
        if (!input || !normalized_path || !explicit_dir) {
            if (error) {
                *error = "path_list reader output is null";
            }
            return false;
        }
        std::string raw_line;
        while (std::getline(*input, raw_line)) {
            const std::string trimmed = TrimCopy(raw_line);
            if (trimmed.empty()) {
                continue;
            }
            return NormalizePathListRelativePath(trimmed, normalized_path, explicit_dir, error);
        }
        if (error) {
            error->clear();
        }
        return false;
    }

    bool FindFirstDescendantFilePath(std::ifstream* input,
                                     const std::string& directory_path,
                                     std::string* relative_file_path,
                                     std::string* error) const {
        if (!input || !relative_file_path) {
            if (error) {
                *error = "relative_file_path output is null";
            }
            return false;
        }
        std::string candidate_path;
        bool explicit_dir = false;
        while (ReadNextNormalizedPathListLine(input, &candidate_path, &explicit_dir, error)) {
            if (!PathListEntryIsDirectory(candidate_path, explicit_dir) &&
                IsDescendantRelativePath(directory_path, candidate_path)) {
                *relative_file_path = candidate_path;
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
        if (error && error->empty()) {
            *error = "failed to find a file under selected directory";
        }
        return false;
    }

    bool PickRandomPathListRelativeFilePath(std::string* relative_file_path, std::string* error) const {
        if (!relative_file_path) {
            if (error) {
                *error = "relative_file_path output is null";
            }
            return false;
        }
        if (FLAGS_masstree_path_list_file.empty()) {
            if (error) {
                *error = "random_path_lookup requires masstree_path_list_file";
            }
            return false;
        }

        std::error_code stat_error;
        const uint64_t file_size = fs::file_size(FLAGS_masstree_path_list_file, stat_error);
        if (stat_error || file_size == 0) {
            if (error) {
                *error = "failed to stat path_list_file: " + FLAGS_masstree_path_list_file;
            }
            return false;
        }

        constexpr uint32_t kMaxSelectionAttempts = 64;
        for (uint32_t attempt = 0; attempt < kMaxSelectionAttempts; ++attempt) {
            std::ifstream input(FLAGS_masstree_path_list_file, std::ios::binary);
            if (!input) {
                if (error) {
                    *error = "failed to open path_list_file: " + FLAGS_masstree_path_list_file;
                }
                return false;
            }

            const uint64_t offset = RandomUint64Exclusive(file_size);
            input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
            if (!input.good()) {
                input.clear();
                input.seekg(0, std::ios::beg);
            } else if (offset != 0) {
                std::string discard;
                std::getline(input, discard);
            }

            std::string selected_path;
            bool explicit_dir = false;
            std::string read_error;
            if (!ReadNextNormalizedPathListLine(&input, &selected_path, &explicit_dir, &read_error)) {
                input.clear();
                input.seekg(0, std::ios::beg);
                if (!ReadNextNormalizedPathListLine(&input, &selected_path, &explicit_dir, &read_error)) {
                    if (error) {
                        *error = read_error.empty() ? "path_list_file has no usable entries" : read_error;
                    }
                    return false;
                }
            }

            if (!PathListEntryIsDirectory(selected_path, explicit_dir)) {
                *relative_file_path = selected_path;
                if (error) {
                    error->clear();
                }
                return true;
            }

            if (FindFirstDescendantFilePath(&input, selected_path, relative_file_path, &read_error)) {
                if (error) {
                    error->clear();
                }
                return true;
            }
        }

        if (error) {
            *error = "failed to sample a queryable file path from path_list_file";
        }
        return false;
    }

    bool BuildRandomLookupPath(std::string* full_path,
                               std::string* relative_file_path,
                               LocalMasstreeNamespaceManifest* manifest_out,
                               std::string* error) const {
        if (!full_path || !relative_file_path) {
            if (error) {
                *error = "lookup path output is null";
            }
            return false;
        }

        LocalMasstreeNamespaceManifest manifest;
        if (!LoadLastMasstreeManifest(&manifest, error)) {
            return false;
        }
        if (!PickRandomPathListRelativeFilePath(relative_file_path, error)) {
            return false;
        }

        const uint64_t repeat_count = std::max<uint64_t>(1, manifest.template_repeat_count);
        const std::string repeat_dir_prefix = manifest.repeat_dir_prefix.empty()
                                                  ? (FLAGS_masstree_repeat_dir_prefix.empty()
                                                         ? std::string("copy")
                                                         : FLAGS_masstree_repeat_dir_prefix)
                                                  : manifest.repeat_dir_prefix;
        const std::string path_prefix = manifest.path_prefix.empty()
                                            ? (last_masstree_path_prefix_.empty()
                                                   ? NormalizeLogicalPath(FLAGS_masstree_path_prefix)
                                                   : last_masstree_path_prefix_)
                                            : NormalizeLogicalPath(manifest.path_prefix);
        const size_t repeat_width = std::max<size_t>(6, DecimalDigits(repeat_count - 1U));
        const std::string repeat_dir =
            RepeatDirName(repeat_dir_prefix, RandomUint64Exclusive(repeat_count), repeat_width);
        *full_path = NormalizeLogicalPath(path_prefix + "/" + repeat_dir + "/" + *relative_file_path);
        if (manifest_out) {
            *manifest_out = manifest;
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    bool RunMasstreeQueryDemo() {
        PrintSection("Masstree \u67e5\u8be2\u6f14\u793a");
        const std::string query_mode = [&]() {
            const std::string normalized = ToLowerCopy(TrimCopy(FLAGS_masstree_query_mode));
            return normalized.empty() ? std::string("random_path_lookup") : normalized;
        }();
        const uint32_t target_success_count = std::max<uint32_t>(1, FLAGS_masstree_query_samples);
        const uint32_t output_success_limit = FLAGS_masstree_query_output_success_limit;
        const uint64_t success_latency_limit_us =
            static_cast<uint64_t>(FLAGS_masstree_query_success_latency_limit_ms) * 1000ULL;
        const int query_rpc_timeout_ms =
            FLAGS_masstree_query_success_latency_limit_ms == 0
                ? FLAGS_timeout_ms
                : static_cast<int>(FLAGS_masstree_query_success_latency_limit_ms);
        struct QuerySample {
            uint32_t index{0};
            bool ok{false};
            uint64_t latency_us{0};
            std::string namespace_id;
            std::string path_prefix;
            std::string generation_id;
            std::string full_path;
            std::string file_name;
            uint64_t inode_id{0};
            zb::rpc::InodeAttr attr;
            zb::rpc::MdsStatus status;
            zb::rpc::GetRandomMasstreeFileAttrReply reply;
            std::string error_message;
        };
        std::vector<QuerySample> samples;
        samples.reserve(target_success_count);
        uint32_t success_count = 0;
        uint64_t total_latency_us = 0;
        uint64_t min_latency_us = std::numeric_limits<uint64_t>::max();
        uint64_t max_latency_us = 0;

        auto accept_success_sample = [&](QuerySample sample) {
            const bool slow_success =
                success_latency_limit_us != 0 && sample.latency_us > success_latency_limit_us;
            if (!sample.ok || slow_success) {
                return;
            }
            sample.index = success_count + 1;
            total_latency_us += sample.latency_us;
            min_latency_us = std::min<uint64_t>(min_latency_us, sample.latency_us);
            max_latency_us = std::max<uint64_t>(max_latency_us, sample.latency_us);
            ++success_count;
            samples.push_back(std::move(sample));
        };

        while (success_count < target_success_count) {
            if (query_mode == "random_path_lookup") {
                zb::rpc::GetRandomMasstreeLookupPathsReply path_reply;
                zb::rpc::GetRandomMasstreeLookupPathsRequest path_request;
                path_request.set_sample_count(target_success_count - success_count);
                if (!FLAGS_masstree_path_list_file.empty()) {
                    path_request.set_path_list_file(FLAGS_masstree_path_list_file);
                }
                mds_.GetRandomMasstreeLookupPaths(path_request, &path_reply, query_rpc_timeout_ms);
                if (path_reply.status().code() != zb::rpc::MDS_OK) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                if (path_reply.samples_size() == 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }
                for (int i = 0; i < path_reply.samples_size() && success_count < target_success_count; ++i) {
                    QuerySample sample;
                    const auto& path_sample = path_reply.samples(i);
                    sample.full_path = path_sample.full_path();
                    sample.file_name = PathBaseName(sample.full_path);
                    sample.namespace_id = path_sample.namespace_id();
                    sample.path_prefix = path_sample.path_prefix();
                    sample.generation_id = path_sample.generation_id();
                    const auto started_at = std::chrono::steady_clock::now();
                    sample.ok = mds_.Lookup(sample.full_path,
                                            &sample.attr,
                                            &sample.status,
                                            query_rpc_timeout_ms);
                    sample.latency_us = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                              started_at)
                            .count());
                    sample.inode_id = sample.attr.inode_id();
                    if (!sample.ok) {
                        sample.error_message = sample.status.message();
                    }
                    accept_success_sample(std::move(sample));
                }
            } else if (query_mode == "random_inode") {
                QuerySample sample;
                zb::rpc::GetRandomMasstreeFileAttrRequest attr_request;
                attr_request.set_query_mode(query_mode);
                const auto started_at = std::chrono::steady_clock::now();
                sample.ok = mds_.GetRandomMasstreeFileAttr(attr_request,
                                                           &sample.reply,
                                                           query_rpc_timeout_ms);
                sample.latency_us = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
                                                                          started_at)
                        .count());
                sample.status = sample.reply.status();
                if (!sample.ok) {
                    sample.error_message = sample.reply.status().message();
                } else {
                    sample.namespace_id = sample.reply.namespace_id();
                    sample.path_prefix = sample.reply.path_prefix();
                    sample.generation_id = sample.reply.generation_id();
                    sample.full_path = sample.reply.full_path();
                    sample.file_name = sample.reply.file_name();
                    sample.inode_id = sample.reply.inode_id();
                    sample.attr = sample.reply.attr();
                }
                accept_success_sample(std::move(sample));
            } else {
                std::cerr << "Unsupported query_mode: " << query_mode << '\n';
                return false;
            }
        }

        std::cout << "query_samples=" << target_success_count << '\n';
        std::cout << "query_mode=" << query_mode << '\n';
        std::cout << "query_success_count=" << target_success_count << '\n';
        std::cout << "metadata_output_success_limit=" << output_success_limit << '\n';
        std::cout << "metadata_output_success_count="
                  << std::min<uint32_t>(target_success_count, output_success_limit) << '\n';
        std::cout << "query_success_rate=1.0000\n";
        std::cout << "total_query_latency=" << FormatLatencyHuman(total_latency_us) << '\n';
        std::cout << "avg_query_latency="
                  << FormatLatencyHuman(success_count == 0 ? 0 : (total_latency_us / success_count)) << '\n';
        std::cout << "min_query_latency=" << FormatLatencyHuman(success_count == 0 ? 0 : min_latency_us) << '\n';
        std::cout << "max_query_latency=" << FormatLatencyHuman(success_count == 0 ? 0 : max_latency_us) << '\n';
        uint32_t printed_success_count = 0;
        for (const auto& sample : samples) {
            if (!sample.ok) {
                continue;
            }
            if (printed_success_count >= output_success_limit) {
                continue;
            }
            ++printed_success_count;
            std::cout << "sample_index=" << sample.index << '\n';
            std::cout << "query_ok=true\n";
            std::cout << "query_latency=" << FormatLatencyHuman(sample.latency_us) << '\n';
            std::cout << "status_code=" << static_cast<int>(sample.status.code()) << '\n';
            std::cout << "status_text=OK\n";
            const auto& attr = sample.attr;
            std::cout << "attr={\n";
            std::cout << "  namespace_id=" << sample.namespace_id << '\n';
            std::cout << "  full_path=" << sample.full_path << '\n';
            std::cout << "  path_prefix=" << sample.path_prefix << '\n';
            std::cout << "  generation_id=" << sample.generation_id << '\n';
            std::cout << "  inode_id=" << sample.inode_id << '\n';
            std::cout << "  file_name=" << sample.file_name << '\n';
            std::cout << "  type=" << InodeTypeToEnglishString(attr.type()) << '\n';
            std::cout << "  mode=" << attr.mode() << '\n';
            std::cout << "  uid=" << attr.uid() << '\n';
            std::cout << "  gid=" << attr.gid() << '\n';
            const uint64_t demo_size = ScaleDemoFileSize(attr.size());
            std::cout << "  size_bytes=" << demo_size << '\n';
            std::cout << "  size_human=" << FormatBytes(demo_size) << '\n';
            std::cout << "  atime=" << attr.atime() << '\n';
            std::cout << "  mtime=" << attr.mtime() << '\n';
            std::cout << "  ctime=" << attr.ctime() << '\n';
            std::cout << "  nlink=" << attr.nlink() << '\n';
            std::cout << "  replica=" << attr.replica() << '\n';
            std::cout << "  version=" << attr.version() << '\n';
            std::cout << "  file_archive_state=" << ArchiveStateToEnglishString(attr.file_archive_state()) << '\n';
            std::cout << "}\n";
        }
        return true;
    }

    static void FillPatternChunk(std::vector<char>* buffer,
                                 size_t size,
                                 uint64_t absolute_offset,
                                 const std::string& label) {
        if (!buffer) {
            return;
        }
        buffer->resize(size);
        const uint64_t label_hash = std::hash<std::string>{}(label);
        for (size_t i = 0; i < size; ++i) {
            const uint64_t value = absolute_offset + i + label_hash;
            (*buffer)[i] = static_cast<char>('A' + (value % 26ULL));
        }
    }

    static bool WritePatternFile(const std::string& path,
                                 const std::string& label,
                                 uint32_t chunk_size_bytes,
                                 uint64_t total_size_bytes,
                                 bool sync_on_close,
                                 uint64_t* bytes_written,
                                 uint64_t* hash_out,
                                 uint64_t* elapsed_us,
                                 int* failure_errno) {
        if (failure_errno) {
            *failure_errno = 0;
        }
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            const int open_errno = errno;
            if (failure_errno) {
                *failure_errno = open_errno;
            }
            std::cerr << "failed to open file for write: " << path
                      << " errno=" << open_errno
                      << " error=" << std::strerror(open_errno) << '\n';
            return false;
        }
        uint64_t hash = 14695981039346656037ULL;
        uint64_t written = 0;
        std::vector<char> buffer;
        const auto started_at = std::chrono::steady_clock::now();
        while (written < total_size_bytes) {
            const size_t current_size = static_cast<size_t>(
                std::min<uint64_t>(chunk_size_bytes, total_size_bytes - written));
            FillPatternChunk(&buffer, current_size, written, label);
            out.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            if (!out) {
                const int write_errno = errno;
                if (failure_errno) {
                    *failure_errno = write_errno;
                }
                std::cerr << "failed to write file: " << path;
                if (write_errno != 0) {
                    std::cerr << " errno=" << write_errno << " error=" << std::strerror(write_errno);
                }
                std::cerr << '\n';
                return false;
            }
            hash = Fnv1a64Append(hash, buffer.data(), buffer.size());
            written += static_cast<uint64_t>(buffer.size());
        }
        if (sync_on_close) {
            out.flush();
            if (!out) {
                const int flush_errno = errno;
                if (failure_errno) {
                    *failure_errno = flush_errno;
                }
                std::cerr << "failed to flush file: " << path;
                if (flush_errno != 0) {
                    std::cerr << " errno=" << flush_errno << " error=" << std::strerror(flush_errno);
                }
                std::cerr << '\n';
                return false;
            }
        }
        out.close();
        if (!out) {
            const int close_errno = errno;
            if (failure_errno) {
                *failure_errno = close_errno;
            }
            std::cerr << "failed to close file after write: " << path;
            if (close_errno != 0) {
                std::cerr << " errno=" << close_errno << " error=" << std::strerror(close_errno);
            }
            std::cerr << '\n';
            return false;
        }
        const auto finished_at = std::chrono::steady_clock::now();
        if (bytes_written) {
            *bytes_written = written;
        }
        if (hash_out) {
            *hash_out = hash;
        }
        if (elapsed_us) {
            *elapsed_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
        }
        return true;
    }
    static bool ReadPatternFile(const std::string& path,
                                uint32_t chunk_size_bytes,
                                bool verify_hash,
                                uint64_t* bytes_read,
                                uint64_t* hash_out,
                                uint64_t* elapsed_us) {
        std::ifstream in(path, std::ios::binary);
        if (!in.is_open()) {
            const int open_errno = errno;
            std::cerr << "failed to open file for read: " << path
                      << " errno=" << open_errno
                      << " error=" << std::strerror(open_errno) << '\n';
            return false;
        }
        uint64_t hash = 14695981039346656037ULL;
        uint64_t read_total = 0;
        std::vector<char> buffer(std::max<uint32_t>(1U, chunk_size_bytes));
        const auto started_at = std::chrono::steady_clock::now();
        while (in) {
            in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
            const std::streamsize count = in.gcount();
            if (count <= 0) {
                break;
            }
            if (verify_hash) {
                hash = Fnv1a64Append(hash, buffer.data(), static_cast<size_t>(count));
            }
            read_total += static_cast<uint64_t>(count);
        }
        if (in.bad()) {
            std::cerr << "failed to read file: " << path << '\n';
            return false;
        }
        const auto finished_at = std::chrono::steady_clock::now();
        if (bytes_read) {
            *bytes_read = read_total;
        }
        if (hash_out) {
            *hash_out = verify_hash ? hash : 0;
        }
        if (elapsed_us) {
            *elapsed_us = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
        }
        return true;
    }
    MdsClient mds_;
    SchedulerClient scheduler_;
    NodeResetClient reset_client_;
    std::vector<zb::rpc::NodeView> nodes_;
    uint64_t cluster_generation_{0};
    std::map<std::string, zb::rpc::NodeType> node_type_by_id_;
    std::vector<zb::demo::MenuActionSpec> actions_;
    zb::demo::DemoRunResult last_result_;
    std::string last_real_logical_path_;
    std::string last_virtual_logical_path_;
    std::string last_masstree_manifest_path_;
    std::string last_masstree_namespace_id_;
    std::string last_masstree_path_prefix_;
    std::string last_masstree_generation_id_;
    bool current_command_has_template_id_{false};
};

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    DemoApp app;
    if (!app.Init()) {
        return 1;
    }
    return app.Run();
}
