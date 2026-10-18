#pragma once

#include <cstdint>
#include <shared_mutex>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../../../msg/storage_node_messages.h"

namespace zb::real_node {

struct DiskContext {
    std::string id;
    std::string mount_point;
    uint64_t capacity_bytes{0};
    uint64_t free_bytes{0};
    bool is_healthy{false};
};

class DiskManager {
public:
    DiskManager() = default;

    zb::msg::Status Init(const std::string& config_str);
    zb::msg::Status InitFromConfig(const std::string& config_str);
    zb::msg::Status InitFromDataRoot(const std::string& data_root);
    zb::msg::Status InitFromBaseDir(const std::string& base_dir,
                                    uint32_t disk_count,
                                    uint64_t disk_capacity_bytes);
    zb::msg::Status Refresh();

    std::string GetMountPoint(const std::string& disk_id) const;
    bool IsHealthy(const std::string& disk_id) const;
    std::vector<zb::msg::DiskReport> GetReport() const;

private:
    static bool LoadDiskIdFromFile(const std::string& mount_point, std::string* out_id);
    static uint64_t CalculateDirectoryUsageBytes(const std::string& dir_path);
    static bool RefreshStats(DiskContext* disk);
    static bool RefreshSyntheticStats(DiskContext* disk, uint64_t synthetic_capacity_bytes);

    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, DiskContext> disks_;
    bool use_synthetic_capacity_{false};
    uint64_t synthetic_capacity_bytes_{0};
};

} // namespace zb::real_node
