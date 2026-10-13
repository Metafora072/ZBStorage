#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::virtual_node {

struct VirtualNodeConfig {
    std::string node_id;
    uint64_t instance_generation{1};
    std::string node_address;
    std::string scheduler_addr;
    std::string group_id;
    std::string node_role;
    std::string peer_node_id;
    std::string peer_address;
    bool replication_enabled{false};
    uint32_t replication_timeout_ms{2000};
    uint32_t node_weight{1};
    uint32_t virtual_node_count{100000};
    uint32_t heartbeat_interval_ms{2000};
    std::string mds_addr;
    uint32_t archive_report_interval_ms{3000};
    uint32_t archive_report_topk{256};
    uint64_t archive_report_min_age_ms{30000};
    uint32_t archive_track_max_objects{500000};
    std::string archive_meta_dir;
    uint32_t archive_meta_snapshot_interval_ops{20000};
    bool archive_meta_wal_fsync{false};
    bool allow_dynamic_disks{true};

    std::vector<std::string> disk_ids;
    uint64_t read_bytes_per_sec{500ULL * 1024ULL * 1024ULL};
    uint64_t write_bytes_per_sec{500ULL * 1024ULL * 1024ULL};
    uint32_t read_base_latency_ms{1};
    uint32_t write_base_latency_ms{1};
    // In-process Scheduler model: wall-clock delay before a sleeping logical
    // node can serve an access. The standalone virtual node may ignore it.
    uint32_t wake_latency_ms{0};
    uint32_t jitter_ms{0};
    uint32_t jitter_seed{0}; // Zero derives a stable seed from NODE_ID.
    // Shared node I/O budget. At most max_concurrent_io requests may be in
    // service; io_queue_limit bounds additional waiting requests.
    uint32_t max_concurrent_io{5};
    uint32_t io_queue_limit{64};
    uint64_t disk_capacity_bytes{1024ULL * 1024ULL * 1024ULL * 1024ULL};
    std::string mount_point_prefix{"/virtual"};

    static VirtualNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::virtual_node
