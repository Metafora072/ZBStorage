#include "sim/SimCluster.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

namespace zb::sim {

namespace {

std::string TrimCopy(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

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

bool ParseManifestUint64(const std::string& line, const std::string& key, uint64_t* value) {
    const std::string prefix = key + "=";
    if (line.rfind(prefix, 0) != 0) {
        return false;
    }
    try {
        *value = static_cast<uint64_t>(std::stoull(line.substr(prefix.size())));
        return true;
    } catch (...) {
        return false;
    }
}

bool IsDirectoryMarker(const std::string& raw_path) {
    if (raw_path.empty()) {
        return true;
    }
    return raw_path.back() == '/' || raw_path.back() == '\\';
}

} // namespace

std::string SimNodeTypeName(SimNodeType type) {
    switch (type) {
    case SimNodeType::Disk:
        return "disk";
    case SimNodeType::Optical:
        return "optical";
    }
    return "unknown";
}

bool SimCluster::Init(const SimClusterConfig& config, std::string* error) {
    config_ = config;
    if (config_.root.empty()) {
        if (error) {
            *error = "root path is required";
        }
        return false;
    }

    std::error_code ec;
    std::filesystem::create_directories(config_.root, ec);
    if (ec) {
        if (error) {
            *error = "failed to create root: " + config_.root.string() + ": " + ec.message();
        }
        return false;
    }
    root_ = std::filesystem::weakly_canonical(config_.root, ec);
    if (ec) {
        root_ = std::filesystem::absolute(config_.root, ec);
    }
    if (ec) {
        if (error) {
            *error = "failed to resolve root path: " + config_.root.string();
        }
        return false;
    }

    nodes_.clear();
    files_.clear();
    if (!BuildNodes(error) || !LoadExistingFiles(error) || !LoadImportManifests(error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool SimCluster::BuildNodes(std::string* error) {
    if (config_.disk_nodes == 0 && config_.optical_nodes == 0) {
        if (error) {
            *error = "at least one disk or optical node is required";
        }
        return false;
    }
    for (uint32_t i = 0; i < config_.disk_nodes; ++i) {
        SimNode node;
        node.id = "disk-node-" + std::to_string(i);
        node.type = SimNodeType::Disk;
        node.capacity_bytes = config_.disk_capacity_bytes;
        nodes_.push_back(node);
    }
    for (uint32_t i = 0; i < config_.optical_nodes; ++i) {
        SimNode node;
        node.id = "optical-node-" + std::to_string(i);
        node.type = SimNodeType::Optical;
        node.capacity_bytes = config_.optical_capacity_bytes;
        nodes_.push_back(node);
    }
    return true;
}

bool SimCluster::NormalizeLogicalPath(const std::string& raw, std::string* normalized, std::string* error) {
    if (!normalized) {
        if (error) {
            *error = "normalized path output is null";
        }
        return false;
    }
    if (raw.empty()) {
        if (error) {
            *error = "logical path is empty";
        }
        return false;
    }
    std::string path = raw;
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.find(':') != std::string::npos) {
        if (error) {
            *error = "logical path must not contain a drive prefix";
        }
        return false;
    }
    if (path.front() != '/') {
        path.insert(path.begin(), '/');
    }

    std::vector<std::string> parts;
    std::stringstream ss(path);
    std::string part;
    while (std::getline(ss, part, '/')) {
        if (part.empty() || part == ".") {
            continue;
        }
        if (part == "..") {
            if (error) {
                *error = "logical path must not contain '..'";
            }
            return false;
        }
        parts.push_back(part);
    }
    std::ostringstream out;
    out << "/";
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            out << "/";
        }
        out << parts[i];
    }
    *normalized = out.str();
    if (error) {
        error->clear();
    }
    return true;
}

bool SimCluster::LogicalToPhysicalPath(const std::string& logical_path,
                                       std::filesystem::path* physical_path,
                                       std::string* error) const {
    std::string normalized;
    if (!NormalizeLogicalPath(logical_path, &normalized, error)) {
        return false;
    }
    std::filesystem::path out = root_;
    if (normalized != "/") {
        std::stringstream ss(normalized.substr(1));
        std::string part;
        while (std::getline(ss, part, '/')) {
            out /= part;
        }
    }
    if (!PathIsUnderRoot(out)) {
        if (error) {
            *error = "resolved path escapes root: " + out.string();
        }
        return false;
    }
    if (physical_path) {
        *physical_path = out;
    }
    return true;
}

bool SimCluster::PathIsUnderRoot(const std::filesystem::path& path) const {
    std::error_code ec;
    const std::filesystem::path absolute = std::filesystem::absolute(path, ec).lexically_normal();
    if (ec) {
        return false;
    }
    const std::filesystem::path root = std::filesystem::absolute(root_, ec).lexically_normal();
    if (ec) {
        return false;
    }
    auto root_it = root.begin();
    auto path_it = absolute.begin();
    for (; root_it != root.end(); ++root_it, ++path_it) {
        if (path_it == absolute.end() || *root_it != *path_it) {
            return false;
        }
    }
    return true;
}

SimNode* SimCluster::FirstNodeOfType(SimNodeType type) {
    for (auto& node : nodes_) {
        if (node.type == type) {
            return &node;
        }
    }
    return nullptr;
}

const SimNode* SimCluster::FirstNodeOfType(SimNodeType type) const {
    for (const auto& node : nodes_) {
        if (node.type == type) {
            return &node;
        }
    }
    return nullptr;
}

void SimCluster::ApplyNodeUsage(SimNode* node, uint64_t old_bytes, uint64_t new_bytes, bool new_file) {
    if (!node) {
        return;
    }
    if (new_bytes >= old_bytes) {
        node->used_bytes += new_bytes - old_bytes;
    } else {
        node->used_bytes -= std::min<uint64_t>(node->used_bytes, old_bytes - new_bytes);
    }
    if (new_file) {
        ++node->file_count;
    }
}

bool SimCluster::LoadExistingFiles(std::string* error) {
    SimNode* disk = FirstNodeOfType(SimNodeType::Disk);
    if (!disk) {
        return true;
    }
    std::error_code ec;
    if (!std::filesystem::exists(root_, ec)) {
        return true;
    }
    for (std::filesystem::recursive_directory_iterator it(root_, ec), end; it != end; it.increment(ec)) {
        if (ec) {
            if (error) {
                *error = "failed while scanning root: " + ec.message();
            }
            return false;
        }
        const auto& path = it->path();
        if (it->is_directory(ec) && path.filename() == ".sim") {
            it.disable_recursion_pending();
            continue;
        }
        if (!it->is_regular_file(ec)) {
            continue;
        }
        const uint64_t size = static_cast<uint64_t>(std::filesystem::file_size(path, ec));
        if (ec) {
            continue;
        }
        uint64_t hash = 0;
        uint64_t hashed_size = 0;
        std::string hash_error;
        if (!ComputeFileHash(path, &hash, &hashed_size, &hash_error)) {
            hash = 0;
            hashed_size = size;
        }
        std::filesystem::path relative = std::filesystem::relative(path, root_, ec);
        if (ec) {
            continue;
        }
        std::string logical = "/" + relative.generic_string();
        SimFileRecord record;
        record.logical_path = logical;
        record.physical_path = path;
        record.size_bytes = hashed_size;
        record.hash = hash;
        record.node_id = disk->id;
        files_[logical] = record;
        disk->used_bytes += hashed_size;
        ++disk->file_count;
    }
    return true;
}

bool SimCluster::LoadImportManifests(std::string* error) {
    SimNode* optical = FirstNodeOfType(SimNodeType::Optical);
    if (!optical) {
        return true;
    }
    const std::filesystem::path import_dir = root_ / ".sim" / "imports";
    std::error_code ec;
    if (!std::filesystem::exists(import_dir, ec)) {
        return true;
    }
    for (const auto& entry : std::filesystem::directory_iterator(import_dir, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to scan import manifests: " + ec.message();
            }
            return false;
        }
        if (!entry.is_regular_file(ec)) {
            continue;
        }
        std::ifstream in(entry.path());
        if (!in) {
            continue;
        }
        uint64_t file_count = 0;
        uint64_t file_bytes = 0;
        std::string line;
        while (std::getline(in, line)) {
            (void)ParseManifestUint64(line, "imported_file_count", &file_count);
            (void)ParseManifestUint64(line, "imported_file_bytes", &file_bytes);
        }
        if (file_count != 0 || file_bytes != 0) {
            optical->file_count += file_count;
            optical->used_bytes += file_bytes;
        }
    }
    return true;
}

SimClusterStats SimCluster::Stats() const {
    SimClusterStats stats;
    for (const auto& node : nodes_) {
        if (node.type == SimNodeType::Disk) {
            ++stats.disk_node_count;
            stats.disk_file_count += node.file_count;
            stats.disk_file_bytes += node.used_bytes;
        } else if (node.type == SimNodeType::Optical) {
            ++stats.optical_node_count;
            stats.imported_file_count += node.file_count;
            stats.imported_file_bytes += node.used_bytes;
        }
        if (node.online) {
            ++stats.online_node_count;
        }
        stats.total_capacity_bytes += node.capacity_bytes;
        stats.used_capacity_bytes += node.used_bytes;
        stats.total_file_count += node.file_count;
        stats.total_file_bytes += node.used_bytes;
    }
    stats.free_capacity_bytes = stats.total_capacity_bytes > stats.used_capacity_bytes
                                    ? stats.total_capacity_bytes - stats.used_capacity_bytes
                                    : 0;
    return stats;
}

bool SimCluster::WriteAndVerify(const SimWriteOptions& options, SimWriteResult* result, std::string* error) {
    if (!result) {
        if (error) {
            *error = "write result output is null";
        }
        return false;
    }
    SimNode* disk = FirstNodeOfType(SimNodeType::Disk);
    if (!disk) {
        if (error) {
            *error = "no disk node is configured for file writes";
        }
        return false;
    }
    std::string logical;
    if (!NormalizeLogicalPath(options.logical_path, &logical, error)) {
        return false;
    }
    std::filesystem::path physical;
    if (!LogicalToPhysicalPath(logical, &physical, error)) {
        return false;
    }
    std::error_code ec;
    std::filesystem::create_directories(physical.parent_path(), ec);
    if (ec) {
        if (error) {
            *error = "failed to create parent directory: " + physical.parent_path().string();
        }
        return false;
    }

    const auto existing = files_.find(logical);
    const bool new_file = existing == files_.end();
    const uint64_t old_size = new_file ? 0 : existing->second.size_bytes;
    const uint64_t current_free = disk->capacity_bytes > disk->used_bytes ? disk->capacity_bytes - disk->used_bytes : 0;
    const uint64_t required_extra = options.size_bytes > old_size ? options.size_bytes - old_size : 0;
    if (required_extra > current_free) {
        if (error) {
            *error = "not enough disk capacity: requested_extra=" + std::to_string(required_extra) +
                     " free=" + std::to_string(current_free);
        }
        return false;
    }

    SimIoResult io;
    if (!WritePatternFile(physical,
                          logical,
                          std::max<uint32_t>(1U, options.chunk_size_bytes),
                          options.size_bytes,
                          options.sync_on_close,
                          &io.bytes_written,
                          &io.write_hash,
                          &io.write_elapsed_us,
                          error)) {
        return false;
    }
    if (!ReadFileAndHash(physical,
                         std::max<uint32_t>(1U, options.chunk_size_bytes),
                         &io.bytes_read,
                         &io.read_hash,
                         &io.read_elapsed_us,
                         error)) {
        return false;
    }

    ApplyNodeUsage(disk, old_size, options.size_bytes, new_file);
    SimFileRecord record;
    record.logical_path = logical;
    record.physical_path = physical;
    record.size_bytes = options.size_bytes;
    record.hash = io.write_hash;
    record.node_id = disk->id;
    files_[logical] = record;

    result->logical_path = logical;
    result->physical_path = physical;
    result->node_id = disk->id;
    result->io = io;
    result->hash_match = io.bytes_written == options.size_bytes &&
                         io.bytes_read == options.size_bytes &&
                         io.write_hash == io.read_hash;
    result->stats_after = Stats();
    if (error) {
        error->clear();
    }
    return result->hash_match;
}

bool SimCluster::CountPathListEntries(const std::filesystem::path& path, uint64_t* file_count, std::string* error) const {
    if (!file_count) {
        if (error) {
            *error = "file_count output is null";
        }
        return false;
    }
    std::ifstream in(path);
    if (!in) {
        if (error) {
            *error = "failed to open path_list_file: " + path.string();
        }
        return false;
    }
    uint64_t count = 0;
    std::string line;
    while (std::getline(in, line)) {
        const std::string trimmed = TrimCopy(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        std::string normalized;
        std::string normalize_error;
        if (!NormalizeLogicalPath(trimmed, &normalized, &normalize_error)) {
            continue;
        }
        if (!IsDirectoryMarker(trimmed)) {
            ++count;
        }
    }
    *file_count = count;
    if (error) {
        error->clear();
    }
    return true;
}

bool SimCluster::WriteImportManifest(const SimImportResult& result, std::string* error) const {
    const std::filesystem::path import_dir = root_ / ".sim" / "imports";
    std::error_code ec;
    std::filesystem::create_directories(import_dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to create import manifest directory: " + import_dir.string();
        }
        return false;
    }
    std::ofstream out(result.manifest_path);
    if (!out) {
        if (error) {
            *error = "failed to write import manifest: " + result.manifest_path.string();
        }
        return false;
    }
    out << "sim_import_manifest_v1\n";
    out << "job_id=" << result.job_id << "\n";
    out << "target_node_id=" << result.target_node_id << "\n";
    out << "imported_file_count=" << result.imported_file_count << "\n";
    out << "imported_file_bytes=" << result.imported_file_bytes << "\n";
    return true;
}

bool SimCluster::Import(const SimImportOptions& options, SimImportResult* result, std::string* error) {
    if (!result) {
        if (error) {
            *error = "import result output is null";
        }
        return false;
    }
    SimNode* optical = FirstNodeOfType(SimNodeType::Optical);
    if (!optical) {
        if (error) {
            *error = "no optical node is configured for batch import";
        }
        return false;
    }

    uint64_t file_count = options.import_files;
    if (!options.path_list_file.empty()) {
        if (!CountPathListEntries(options.path_list_file, &file_count, error)) {
            return false;
        }
        if (file_count == 0) {
            if (error) {
                *error = "path_list_file has no file entries";
            }
            return false;
        }
    }
    const uint64_t file_bytes = file_count * options.default_file_size_bytes;
    const uint64_t free_bytes = optical->capacity_bytes > optical->used_bytes
                                    ? optical->capacity_bytes - optical->used_bytes
                                    : 0;
    if (file_bytes > free_bytes) {
        if (error) {
            *error = "not enough optical capacity: requested=" + std::to_string(file_bytes) +
                     " free=" + std::to_string(free_bytes);
        }
        return false;
    }

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    SimImportResult local;
    local.job_id = "import-" + TimestampToken() + "-" + std::to_string(now_ms % 1000000);
    local.target_node_id = optical->id;
    local.imported_file_count = file_count;
    local.imported_file_bytes = file_bytes;
    local.stats_before = Stats();
    local.manifest_path = root_ / ".sim" / "imports" / (local.job_id + ".manifest");

    optical->file_count += file_count;
    optical->used_bytes += file_bytes;
    local.stats_after = Stats();
    if (!WriteImportManifest(local, error)) {
        optical->file_count -= file_count;
        optical->used_bytes -= file_bytes;
        return false;
    }
    *result = local;
    if (error) {
        error->clear();
    }
    return true;
}

SimQueryResult SimCluster::Query(uint32_t samples, bool sleep_for_latency) {
    SimQueryResult result;
    result.samples = std::max<uint32_t>(1U, samples);
    result.success_count = result.samples;
    result.failure_count = 0;
    std::uniform_int_distribution<uint64_t> dist(100, 200);
    uint64_t total = 0;
    result.min_latency_ms = UINT64_MAX;
    for (uint32_t i = 0; i < result.samples; ++i) {
        const uint64_t latency = dist(rng_);
        if (sleep_for_latency) {
            std::this_thread::sleep_for(std::chrono::milliseconds(latency));
        }
        result.min_latency_ms = std::min<uint64_t>(result.min_latency_ms, latency);
        result.max_latency_ms = std::max<uint64_t>(result.max_latency_ms, latency);
        total += latency;
    }
    result.avg_latency_ms = total / result.samples;
    return result;
}

bool SimCluster::Reset(bool purge_files, bool purge_imports, std::string* error) {
    std::error_code ec;
    if (purge_files) {
        for (const auto& entry : std::filesystem::directory_iterator(root_, ec)) {
            if (ec) {
                if (error) {
                    *error = "failed to scan root for reset: " + ec.message();
                }
                return false;
            }
            if (entry.path().filename() == ".sim") {
                continue;
            }
            std::filesystem::remove_all(entry.path(), ec);
            if (ec) {
                if (error) {
                    *error = "failed to remove path during reset: " + entry.path().string();
                }
                return false;
            }
        }
    }
    if (purge_imports) {
        std::filesystem::remove_all(root_ / ".sim" / "imports", ec);
        if (ec) {
            if (error) {
                *error = "failed to remove import manifests: " + ec.message();
            }
            return false;
        }
    }
    return Init(config_, error);
}

} // namespace zb::sim
