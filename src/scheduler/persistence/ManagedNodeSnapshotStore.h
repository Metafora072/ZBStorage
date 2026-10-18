#pragma once

#include <cstdint>
#include <string>
#include <mutex>

#include "../power/NodePowerManager.h"

namespace zb::scheduler {

class ClusterState;

class ManagedNodeSnapshotStore {
public:
    explicit ManagedNodeSnapshotStore(std::string path, ClusterState* state = nullptr);

    bool Save(const NodePowerManager& manager, uint64_t now_ms, std::string* error) const;
    bool Load(NodePowerManager* manager,
              uint64_t restore_time_ms,
              bool* loaded,
              std::string* error) const;

private:
    std::string path_;
    ClusterState* state_{};
    mutable std::mutex write_mu_;
};

} // namespace zb::scheduler
