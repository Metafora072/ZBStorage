#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::real_node {

struct DiskSpec {
    std::string id;
    std::string mount_point;
};

struct NodeConfig {
    // Raw values loaded from config file.
    std::string disks_env;
    std::string data_root;
    std::string disk_base_dir;
    uint32_t disk_count{0};
    uint64_t disk_capacity_bytes{0};
    uint64_t disk_read_bytes_per_sec{200ULL * 1000ULL * 1000ULL};
    uint64_t disk_write_bytes_per_sec{200ULL * 1000ULL * 1000ULL};
    std::string node_id;
    std::string node_address;
    std::string scheduler_addr;
    std::string mds_addr;
    std::string group_id;
    std::string node_role;
    std::string peer_node_id;
    std::string peer_address;
    bool replication_enabled{false};
    uint32_t replication_timeout_ms{2000};
    uint32_t node_weight{1};
    uint32_t heartbeat_interval_ms{2000};
    uint32_t archive_report_interval_ms{3000};
    uint32_t archive_report_topk{256};
    uint64_t archive_report_min_age_ms{30000};
    uint32_t archive_track_max_objects{500000};
    std::string archive_meta_dir;
    uint32_t archive_meta_snapshot_interval_ops{20000};
    bool archive_meta_wal_fsync{false};

    static NodeConfig LoadFromFile(const std::string& path, std::string* error);
    std::vector<DiskSpec> ParseDisksEnv(std::string* error) const;
};

} // namespace zb::real_node
