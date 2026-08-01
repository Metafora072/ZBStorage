#pragma once

#include <cstdint>
#include <string>

namespace zb::scheduler {

struct SchedulerConfig {
    uint64_t suspect_timeout_ms{6000};
    uint64_t dead_timeout_ms{15000};
    uint64_t tick_interval_ms{1000};
    uint64_t cluster_view_snapshot_interval_ms{5000};

    double power_peak_utilization{0.80};
    uint32_t power_peak_queue_depth{8};
    uint64_t power_metrics_freshness_ms{5000};
    uint64_t power_standby_after_ms{60000};
    uint64_t power_off_after_ms{600000};
    uint64_t power_minimum_residency_ms{1000};
    bool power_allow_off_with_resident_data{false};
    uint32_t power_min_online_storage_nodes{1};
    uint32_t power_min_online_metadata_nodes{1};
    uint32_t power_min_online_optical_nodes{0};
    uint64_t power_min_online_storage_capacity_bytes{0};
    uint64_t power_min_online_storage_free_bytes{0};
    bool power_actuation_enabled{false};
    uint64_t power_actuation_retry_ms{5000};
    std::string power_wake_cmd_template;
    std::string power_standby_cmd_template;
    std::string power_off_cmd_template;
    uint32_t metrics_history_max_samples{100000};

    std::string mds_address{"127.0.0.1:9000"};
    uint32_t migration_rpc_timeout_ms{10000};
    uint64_t migration_copy_chunk_bytes{4ULL * 1024ULL * 1024ULL};
    uint64_t migration_retry_base_ms{1000};
    uint32_t migration_max_retries{20};

    std::string start_cmd_template;
    std::string stop_cmd_template;
    std::string reboot_cmd_template;
    std::string cluster_view_snapshot_path;
    std::string managed_state_snapshot_path;

    static SchedulerConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::scheduler
