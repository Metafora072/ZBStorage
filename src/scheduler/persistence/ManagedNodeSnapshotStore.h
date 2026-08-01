#pragma once

#include <cstdint>
#include <string>

#include "../power/NodePowerManager.h"

namespace zb::scheduler {

class ManagedNodeSnapshotStore {
public:
    explicit ManagedNodeSnapshotStore(std::string path);

    bool Save(const NodePowerManager& manager, uint64_t now_ms, std::string* error) const;
    bool Load(NodePowerManager* manager,
              uint64_t restore_time_ms,
              bool* loaded,
              std::string* error) const;

private:
    std::string path_;
};

} // namespace zb::scheduler
