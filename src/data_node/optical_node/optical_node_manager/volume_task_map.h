#pragma once

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace optical_node_manager {

/**
 * @brief 按 volume_id 索引 task_id 集合的线程安全映射。
 *
 * 一个 volume_id 可同时被多个 task 引用（读 / 写 / 刻录），
 * 用于"按卷批量推进"和"新读请求搭便车"判定。
 */
class VolumeTaskMap {
public:
    VolumeTaskMap() = default;
    ~VolumeTaskMap() = default;

    VolumeTaskMap(const VolumeTaskMap&) = delete;
    VolumeTaskMap& operator=(const VolumeTaskMap&) = delete;
    VolumeTaskMap(VolumeTaskMap&&) = delete;
    VolumeTaskMap& operator=(VolumeTaskMap&&) = delete;

    /**
     * @brief 将 task_id 登记到指定 volume_id 下；task_id 已存在则返回 false。
     */
    bool AddTask(const std::string& volume_id, uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& vec = map_[volume_id];
        if (std::find(vec.begin(), vec.end(), task_id) != vec.end()) {
            return false;
        }
        vec.push_back(task_id);
        return true;
    }

    /**
     * @brief 从指定 volume_id 下移除 task_id；移除后该 volume 项自动回收。
     * @return true 表示成功移除；false 表示未找到。
     */
    bool RemoveTask(const std::string& volume_id, uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return false;
        }
        auto& vec = it->second;
        auto vit = std::find(vec.begin(), vec.end(), task_id);
        if (vit == vec.end()) {
            return false;
        }
        vec.erase(vit);
        if (vec.empty()) {
            map_.erase(it);
        }
        return true;
    }

    /**
     * @brief 返回指定 volume_id 下所有 task_id（拷贝）；volume 不存在返回空。
     */
    std::vector<uint64_t> GetTasks(const std::string& volume_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return {};
        }
        return it->second;
    }

    /**
     * @brief 判断指定 volume_id 下是否登记了 task_id。
     */
    bool Contains(const std::string& volume_id, uint64_t task_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return false;
        }
        const auto& vec = it->second;
        return std::find(vec.begin(), vec.end(), task_id) != vec.end();
    }

    /**
     * @brief 判断指定 volume_id 下是否登记了任意 task。
     *
     * 用于"搭便车"预查询：若为 true，新任务直接登记即可，
     * 由 cd_manager / 镜像回灌链路负责后续唤醒。
     */
    bool ContainsVolume(const std::string& volume_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        return it != map_.end() && !it->second.empty();
    }

    /**
     * @brief 返回当前管理的 volume_id 数量。
     */
    size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return map_.size();
    }

    /**
     * @brief 清空整张表（用于 Stop / Reset 流程）。
     */
    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        map_.clear();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint64_t>> map_;
};

}  // namespace optical_node_manager
