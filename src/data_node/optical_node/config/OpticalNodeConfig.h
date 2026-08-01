#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::optical_node {

struct OpticalNodeConfig {
    std::string node_id;
    std::string node_address;
    std::string scheduler_addr;
    std::string group_id;
    std::string node_role;
    std::string peer_node_id;
    std::string peer_address;
    bool replication_enabled{false};
    uint32_t replication_timeout_ms{2000};
    uint32_t node_weight{1};
    uint32_t virtual_node_count{1};
    uint32_t heartbeat_interval_ms{2000};

    std::vector<std::string> disk_ids;
    std::string archive_root{"/tmp/zb_optical"};
    std::string cache_root;
    bool simulate_io{true};
    uint64_t optical_read_bytes_per_sec{100ULL * 1024ULL * 1024ULL};   // 100MB/s
    uint64_t optical_write_bytes_per_sec{50ULL * 1024ULL * 1024ULL};   // 50MB/s
    uint64_t cache_read_bytes_per_sec{400ULL * 1024ULL * 1024ULL};     // 400MB/s
    uint32_t cache_disc_slots{4};
    uint32_t library_disc_slots{10000};
    uint64_t max_image_size_bytes{1024ULL * 1024ULL * 1024ULL};
    uint64_t disk_capacity_bytes{10ULL * 1024ULL * 1024ULL * 1024ULL};
    std::unordered_map<std::string, uint64_t> disk_capacity_map;
    std::string mount_point_prefix{"/optical"};
    std::string startup_scan_mode{"fast"};

    static OpticalNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::optical_node
