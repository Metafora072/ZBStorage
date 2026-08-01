#pragma once

#include <cstdint>
#include <string>

#include "../model/ClusterState.h"
#include "NodePowerManager.h"

namespace zb::scheduler {

struct PowerActuationResult {
    bool success{false};
    std::string message;
};

class ManagedPowerActuator {
public:
    virtual ~ManagedPowerActuator() = default;
    virtual PowerActuationResult Wake(const std::string& node_id, const std::string& address) = 0;
    virtual PowerActuationResult Standby(const std::string& node_id, const std::string& address) = 0;
    virtual PowerActuationResult PowerOff(const std::string& node_id, const std::string& address) = 0;
};

class ShellManagedPowerActuator : public ManagedPowerActuator {
public:
    ShellManagedPowerActuator(std::string wake_template,
                              std::string standby_template,
                              std::string off_template);
    PowerActuationResult Wake(const std::string& node_id, const std::string& address) override;
    PowerActuationResult Standby(const std::string& node_id, const std::string& address) override;
    PowerActuationResult PowerOff(const std::string& node_id, const std::string& address) override;

private:
    static PowerActuationResult Execute(const std::string& command_template,
                                        const std::string& node_id,
                                        const std::string& address);
    std::string wake_template_;
    std::string standby_template_;
    std::string off_template_;
};

class PowerActuationController {
public:
    PowerActuationController(NodePowerManager* nodes,
                             ClusterState* cluster,
                             ManagedPowerActuator* actuator,
                             bool enabled,
                             uint64_t retry_interval_ms);
    void Tick(uint64_t now_ms);

private:
    NodePowerManager* nodes_{};
    ClusterState* cluster_{};
    ManagedPowerActuator* actuator_{};
    bool enabled_{false};
    uint64_t retry_interval_ms_{5000};
};

} // namespace zb::scheduler
