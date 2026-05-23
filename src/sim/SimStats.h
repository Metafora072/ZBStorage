#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace zb::sim {

enum class SimNodeType {
    Disk,
    Optical,
};

struct SimNode {
    std::string id;
    SimNodeType type{SimNodeType::Disk};
    uint64_t capacity_bytes{0};
    uint64_t used_bytes{0};
    uint64_t file_count{0};
    bool online{true};
};

struct SimFileRecord {
    std::string logical_path;
    std::filesystem::path physical_path;
    uint64_t size_bytes{0};
    uint64_t hash{0};
    std::string node_id;
};

struct SimClusterStats {
    uint64_t disk_node_count{0};
    uint64_t optical_node_count{0};
    uint64_t online_node_count{0};
    uint64_t total_capacity_bytes{0};
    uint64_t used_capacity_bytes{0};
    uint64_t free_capacity_bytes{0};
    uint64_t total_file_count{0};
    uint64_t total_file_bytes{0};
    uint64_t disk_file_count{0};
    uint64_t disk_file_bytes{0};
    uint64_t imported_file_count{0};
    uint64_t imported_file_bytes{0};
};

} // namespace zb::sim
