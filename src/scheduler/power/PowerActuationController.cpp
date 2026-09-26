#include "PowerActuationController.h"

#include <cstdlib>
#include <utility>

namespace zb::scheduler {
namespace {

std::string ReplaceAll(std::string text, const std::string& from, const std::string& to) {
    size_t pos = 0;
    while (!from.empty() && (pos = text.find(from, pos)) != std::string::npos) {
        text.replace(pos, from.size(), to);
        pos += to.size();
    }
    return text;
}

std::string ShellQuote(const std::string& value) {
    std::string out{"'"};
    for (char ch : value) out += ch == '\'' ? "'\\''" : std::string(1, ch);
    out += "'";
    return out;
}

} // namespace

ShellManagedPowerActuator::ShellManagedPowerActuator(std::string wake_template,
                                                     std::string standby_template,
                                                     std::string off_template)
    : wake_template_(std::move(wake_template)),
      standby_template_(std::move(standby_template)),
      off_template_(std::move(off_template)) {}

PowerActuationResult ShellManagedPowerActuator::Wake(const std::string& node_id,
                                                     const std::string& address) {
    return Execute(wake_template_, node_id, address);
}

PowerActuationResult ShellManagedPowerActuator::Standby(const std::string& node_id,
                                                        const std::string& address) {
    return Execute(standby_template_, node_id, address);
}

PowerActuationResult ShellManagedPowerActuator::PowerOff(const std::string& node_id,
                                                         const std::string& address) {
    return Execute(off_template_, node_id, address);
}

PowerActuationResult ShellManagedPowerActuator::Execute(const std::string& command_template,
                                                        const std::string& node_id,
                                                        const std::string& address) {
    if (command_template.empty()) return {false, "power actuation command template is empty"};
    std::string command = ReplaceAll(command_template, "{node_id}", ShellQuote(node_id));
    command = ReplaceAll(command, "{address}", ShellQuote(address));
    const int result = std::system(command.c_str());
    if (result != 0) return {false, "power command failed with exit code " + std::to_string(result)};
    return {true, "power command completed"};
}

PowerActuationController::PowerActuationController(NodePowerManager* nodes,
                                                   ClusterState* cluster,
                                                   ManagedPowerActuator* actuator,
                                                   bool enabled,
                                                   uint64_t retry_interval_ms)
    : nodes_(nodes), cluster_(cluster), actuator_(actuator), enabled_(enabled),
      retry_interval_ms_(retry_interval_ms == 0 ? 5000 : retry_interval_ms) {}

void PowerActuationController::Tick(uint64_t now_ms) {
    if (!enabled_ || !nodes_ || !cluster_ || !actuator_) return;
    for (const auto& node : nodes_->ListNodes()) {
        if (node.profile.execution_mode != ExecutionMode::kPhysical ||
            node.power_level == node.actuated_power_level ||
            (node.last_power_actuation_ms != 0 && now_ms >= node.last_power_actuation_ms &&
             now_ms - node.last_power_actuation_ms < retry_interval_ms_)) {
            continue;
        }
        NodeState cluster_node;
        if (!cluster_->GetNode(node.profile.node_id, &cluster_node) || cluster_node.address.empty()) continue;
        PowerActuationResult result;
        if (node.power_level == PowerLevel::kOff) {
            result = actuator_->PowerOff(node.profile.node_id, cluster_node.address);
        } else if (node.power_level == PowerLevel::kStandby) {
            result = actuator_->Standby(node.profile.node_id, cluster_node.address);
        } else {
            result = actuator_->Wake(node.profile.node_id, cluster_node.address);
        }
        nodes_->ReportPowerActuation(node.profile.node_id, node.power_level, now_ms,
                                     result.success, result.message, nullptr);
        if (result.success) {
            const auto legacy = node.power_level == PowerLevel::kOff
                                    ? zb::rpc::NODE_POWER_OFF : zb::rpc::NODE_POWER_ON;
            cluster_->SetDesiredPowerState(node.profile.node_id, legacy, nullptr);
            cluster_->SetCurrentPowerState(node.profile.node_id, legacy, nullptr);
        }
    }
}

} // namespace zb::scheduler
