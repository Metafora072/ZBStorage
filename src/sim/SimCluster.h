#pragma once

#include "sim/SimFileIO.h"
#include "sim/SimStats.h"

#include <filesystem>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::sim {

struct SimClusterConfig {
    std::filesystem::path root;
    uint32_t disk_nodes{100};
    uint32_t optical_nodes{10000};
    uint64_t disk_capacity_bytes{24ULL * 2000000000000ULL};
    uint64_t optical_capacity_bytes{5000ULL * 1000000000000ULL + 5000ULL * 10000000000000ULL};
};

struct SimWriteOptions {
    std::string logical_path{"/demo/sim_file.bin"};
    uint64_t size_bytes{100ULL * 1024ULL * 1024ULL};
    uint32_t chunk_size_bytes{1024U * 1024U};
    bool sync_on_close{false};
};

struct SimWriteResult {
    std::string logical_path;
    std::filesystem::path physical_path;
    std::string node_id;
    SimIoResult io;
    bool hash_match{false};
    SimClusterStats stats_after;
};

struct SimImportOptions {
    uint64_t import_files{1000000};
    uint64_t default_file_size_bytes{100ULL * 1024ULL * 1024ULL};
    std::filesystem::path path_list_file;
};

struct SimImportResult {
    std::string job_id;
    std::string target_node_id;
    uint64_t imported_file_count{0};
    uint64_t imported_file_bytes{0};
    SimClusterStats stats_before;
    SimClusterStats stats_after;
    std::filesystem::path manifest_path;
};

struct SimQueryResult {
    uint32_t samples{0};
    uint32_t success_count{0};
    uint32_t failure_count{0};
    uint64_t min_latency_ms{0};
    uint64_t max_latency_ms{0};
    uint64_t avg_latency_ms{0};
};

class SimCluster {
public:
    bool Init(const SimClusterConfig& config, std::string* error);

    const std::vector<SimNode>& nodes() const { return nodes_; }
    SimClusterStats Stats() const;

    bool WriteAndVerify(const SimWriteOptions& options, SimWriteResult* result, std::string* error);
    bool Import(const SimImportOptions& options, SimImportResult* result, std::string* error);
    SimQueryResult Query(uint32_t samples, bool sleep_for_latency);
    bool Reset(bool purge_files, bool purge_imports, std::string* error);

    static bool NormalizeLogicalPath(const std::string& raw, std::string* normalized, std::string* error);

private:
    bool BuildNodes(std::string* error);
    bool LoadExistingFiles(std::string* error);
    bool LoadImportManifests(std::string* error);
    bool LogicalToPhysicalPath(const std::string& logical_path,
                               std::filesystem::path* physical_path,
                               std::string* error) const;
    SimNode* FirstNodeOfType(SimNodeType type);
    const SimNode* FirstNodeOfType(SimNodeType type) const;
    bool CountPathListEntries(const std::filesystem::path& path, uint64_t* file_count, std::string* error) const;
    bool WriteImportManifest(const SimImportResult& result, std::string* error) const;
    void ApplyNodeUsage(SimNode* node, uint64_t old_bytes, uint64_t new_bytes, bool new_file);
    bool PathIsUnderRoot(const std::filesystem::path& path) const;

    SimClusterConfig config_;
    std::filesystem::path root_;
    std::vector<SimNode> nodes_;
    std::unordered_map<std::string, SimFileRecord> files_;
    std::mt19937_64 rng_{std::random_device{}()};
};

std::string SimNodeTypeName(SimNodeType type);

} // namespace zb::sim
