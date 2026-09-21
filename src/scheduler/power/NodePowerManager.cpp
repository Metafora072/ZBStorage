#include "NodePowerManager.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <cmath>
#include <random>
#include <stdexcept>

namespace zb::scheduler {

namespace {

bool MatchesFilter(const ManagedNodeRuntime& node, const ManagedNodeFilter& filter) {
    return (!filter.kind || node.profile.kind == *filter.kind) &&
           (!filter.execution_mode || node.profile.execution_mode == *filter.execution_mode) &&
           (!filter.lifecycle || node.lifecycle == *filter.lifecycle) &&
           (!filter.health || node.health == *filter.health) &&
           (!filter.power_level || node.power_level == *filter.power_level) &&
           (filter.include_retired || !IsTerminalLifecycle(node.lifecycle));
}

void SortByNodeId(std::vector<ManagedNodeRuntime>* nodes) {
    std::sort(nodes->begin(), nodes->end(), [](const ManagedNodeRuntime& lhs,
                                              const ManagedNodeRuntime& rhs) {
        return lhs.profile.node_id < rhs.profile.node_id;
    });
}

} // namespace

NodePowerManager::NodePowerManager(PowerPolicy policy) : policy_(policy) {
    std::string error;
    if (!ValidatePowerPolicy(policy_, &error)) {
        throw std::invalid_argument("invalid power policy: " + error);
    }
}

bool NodePowerManager::RegisterNode(const ManagedNodeProfile& profile,
                                    uint64_t join_time_ms,
                                    uint64_t lifetime_seed,
                                    std::string* error) {
    std::string validation_error;
    if (!ValidateManagedNodeProfile(profile, &validation_error)) {
        if (error) {
            *error = validation_error;
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    auto existing = nodes_.find(profile.node_id);
    if (existing != nodes_.end()) {
        if (error) {
            *error = existing->second.lifecycle == LifecycleState::kRetired
                         ? "node_id is retired and protected by a tombstone: " + profile.node_id
                         : "node already exists: " + profile.node_id;
        }
        return false;
    }

    ManagedNodeRuntime node;
    node.profile = profile;
    node.identity.node_id = profile.node_id;
    node.identity.kind = profile.kind;
    node.identity.execution_mode = profile.execution_mode;
    node.inventory.node_id = profile.node_id;
    node.join_time_ms = join_time_ms;
    node.sampled_lifetime_ms = SampleLifetimeMs(profile.reliability, lifetime_seed);
    node.planned_exit_time_ms = SaturatingAdd(join_time_ms, node.sampled_lifetime_ms);
    node.reliability_seed = lifetime_seed;
    node.next_failure_time_ms = ScheduleNextFailureTime(node, join_time_ms);
    node.last_access_ms = join_time_ms;
    node.power_state_enter_ms = join_time_ms;
    node.last_energy_update_ms = join_time_ms;
    node.last_actuated_energy_update_ms = join_time_ms;
    node.last_power_actuation_ms = join_time_ms;
    nodes_.emplace(profile.node_id, std::move(node));
    ++generation_;
    return true;
}

bool NodePowerManager::RegisterReplacementNode(
    const ManagedNodeProfile& profile,
    const std::string& predecessor_node_id,
    uint64_t join_time_ms,
    uint64_t lifetime_seed,
    std::string* error) {
    ManagedNodeRuntime predecessor;
    if (predecessor_node_id.empty() || !GetNode(predecessor_node_id, &predecessor)) {
        if (error) *error = "replacement predecessor not found: " + predecessor_node_id;
        return false;
    }
    if (predecessor.lifecycle != LifecycleState::kRetired) {
        if (error) *error = "replacement predecessor must be retired";
        return false;
    }
    if (profile.kind != predecessor.profile.kind) {
        if (error) *error = "replacement node kind must match predecessor";
        return false;
    }
    if (profile.technology_generation <= predecessor.profile.technology_generation) {
        if (error) *error = "replacement technology generation must increase";
        return false;
    }
    if (!RegisterNode(profile, join_time_ms, lifetime_seed, error)) return false;

    std::lock_guard<std::mutex> lock(mu_);
    auto predecessor_it = nodes_.find(predecessor_node_id);
    auto replacement_it = nodes_.find(profile.node_id);
    if (predecessor_it == nodes_.end() || replacement_it == nodes_.end()) {
        if (error) *error = "replacement registration lost catalog state";
        return false;
    }
    replacement_it->second.predecessor_node_id = predecessor_node_id;
    ++predecessor_it->second.replacement_count;
    ++generation_;
    return true;
}

bool NodePowerManager::ActivateNode(const std::string& node_id, uint64_t now_ms, std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle != LifecycleState::kJoining) {
        if (error) {
            *error = "only a joining node can be activated";
        }
        return false;
    }
    if (!node.readiness.Ready()) {
        if (node.readiness.observed) {
            if (error) *error = "joining node has not completed initialization, inventory, and metadata readiness";
            return false;
        }
        // An explicit manager activation is an operator readiness assertion.
        // Heartbeat-driven activation uses an explicit readiness report.
        node.readiness.observed = true;
        node.readiness.initialization_complete = true;
        node.readiness.inventory_ready = true;
        node.readiness.metadata_ready = true;
        node.readiness.observed_at_ms = now_ms;
        node.readiness.message = "explicit activation readiness assertion";
    }
    AdvancePowerPolicyLocked(&node, now_ms);
    node.lifecycle = LifecycleState::kWorking;
    node.health = ManagedHealthState::kHealthy;
    node.administrative_state = zb::storage_model::AdministrativeState::kEnabled;
    node.last_access_ms = std::max(node.last_access_ms, now_ms);
    SetPowerLevelLocked(&node, PowerLevel::kMedium, now_ms, true);
    if (node.profile.execution_mode != ExecutionMode::kPhysical) {
        node.actuated_power_level = PowerLevel::kMedium;
    }
    ++generation_;
    return true;
}

bool NodePowerManager::BeginRemoveNode(const std::string& node_id, uint64_t now_ms, std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle == LifecycleState::kDraining) {
        return true;
    }
    if (node.lifecycle == LifecycleState::kRetired || node.lifecycle == LifecycleState::kExiting) {
        if (error) {
            *error = "node is already exiting or retired";
        }
        return false;
    }
    AdvancePowerPolicyLocked(&node, now_ms);
    node.lifecycle = LifecycleState::kDraining;
    node.administrative_state = zb::storage_model::AdministrativeState::kDraining;
    node.drain_remaining_bytes = node.resident_data_bytes;
    node.drain_last_report_ms = now_ms;
    ++generation_;
    return true;
}

bool NodePowerManager::FinalizeRemoveNode(const std::string& node_id,
                                          uint64_t now_ms,
                                          bool force,
                                          std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (!force && node.lifecycle != LifecycleState::kExiting) {
        if (error) {
            *error = "node drain is not complete";
        }
        return false;
    }
    if (node.resident_data_bytes != 0) {
        if (error) {
            *error = "node still has resident data";
        }
        return false;
    }
    if (node.profile.kind == ManagedNodeKind::kOpticalLibrary &&
        (!node.optical_library.observed || node.optical_library.local_disc_count != 0)) {
        if (error) {
            *error = !node.optical_library.observed
                         ? "optical inventory must be observed before retirement"
                         : "optical library still has local discs";
        }
        return false;
    }
    if (node.profile.execution_mode == ExecutionMode::kPhysical &&
        node.actuated_power_level != PowerLevel::kOff) {
        if (error) *error = "physical node must be confirmed powered off before retirement";
        return false;
    }
    AdvancePowerPolicyLocked(&node, now_ms);
    node.lifecycle = LifecycleState::kRetired;
    node.administrative_state = zb::storage_model::AdministrativeState::kDisabled;
    node.actual_exit_time_ms = now_ms;
    node.transition = TransitionState::kStable;
    SetPowerLevelLocked(&node, PowerLevel::kOff, now_ms, true);
    node.actuated_power_level = PowerLevel::kOff;
    ++generation_;
    return true;
}

bool NodePowerManager::MarkNodeFailed(const std::string& node_id,
                                      uint64_t now_ms,
                                      const std::string& reason,
                                      std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle == LifecycleState::kRetired) {
        if (error) {
            *error = "retired node cannot fail";
        }
        return false;
    }
    AdvancePowerPolicyLocked(&node, now_ms);
    node.health = ManagedHealthState::kFailed;
    node.failure_reason = reason;
    node.failure_source = zb::storage_model::FailureSource::kInjected;
    ++node.failure_count;
    node.repair_due_time_ms = node.profile.reliability.mean_repair_time_ms == 0
                                   ? 0
                                   : SaturatingAdd(now_ms, node.profile.reliability.mean_repair_time_ms);
    SetPowerLevelLocked(&node, PowerLevel::kOff, now_ms, true);
    ++generation_;
    return true;
}

bool NodePowerManager::RepairNode(const std::string& node_id,
                                  uint64_t now_ms,
                                  std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.health != ManagedHealthState::kFailed || node.lifecycle == LifecycleState::kRetired) {
        if (error) *error = "node is not repairable in its current state";
        return false;
    }
    AdvancePowerPolicyLocked(&node, now_ms);
    node.health = ManagedHealthState::kHealthy;
    node.failure_reason.clear();
    node.failure_source = zb::storage_model::FailureSource::kNone;
    node.repair_due_time_ms = 0;
    node.next_failure_time_ms = ScheduleNextFailureTime(node, now_ms);
    SetPowerLevelLocked(&node, PowerLevel::kMedium, now_ms, true);
    if (node.profile.execution_mode != ExecutionMode::kPhysical)
        node.actuated_power_level = PowerLevel::kMedium;
    ++generation_;
    return true;
}

bool NodePowerManager::ReportHeartbeat(const std::string& node_id,
                                       const std::string& service_address,
                                       NodeInventorySnapshot inventory,
                                       uint64_t now_ms,
                                       std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle == LifecycleState::kRetired) {
        if (error) *error = "retired node heartbeat rejected: " + node_id;
        return false;
    }
    if (now_ms < node.last_heartbeat_ms) {
        if (error) *error = "stale heartbeat";
        return false;
    }
    inventory.node_id = node_id;
    if (inventory.observed_at_ms == 0) inventory.observed_at_ms = now_ms;
    if (inventory.generation == 0) inventory.generation = node.inventory.generation + 1;
    if (!zb::storage_model::ValidateNodeInventorySnapshot(inventory, error)) return false;
    if (node.inventory.generation != 0 && inventory.generation < node.inventory.generation) {
        if (error) *error = "stale inventory generation";
        return false;
    }

    AdvancePowerPolicyLocked(&node, now_ms);
    node.identity.node_id = node.profile.node_id;
    node.identity.kind = node.profile.kind;
    node.identity.execution_mode = node.profile.execution_mode;
    node.identity.service_address = service_address;
    node.inventory = std::move(inventory);
    node.last_heartbeat_ms = now_ms;
    node.resident_data_bytes = zb::storage_model::InventoryUsedBytes(node.inventory);
    uint64_t reported_accumulated_write_bytes = 0;
    for (const auto& device : node.inventory.devices) {
        reported_accumulated_write_bytes = SaturatingAdd(
            reported_accumulated_write_bytes, device.accumulated_write_bytes);
    }
    node.accumulated_write_bytes = std::max(node.accumulated_write_bytes,
                                            reported_accumulated_write_bytes);
    if (node.health == ManagedHealthState::kSuspect ||
        node.failure_source == zb::storage_model::FailureSource::kHeartbeatTimeout) {
        node.health = ManagedHealthState::kHealthy;
        node.failure_source = zb::storage_model::FailureSource::kNone;
        node.failure_reason.clear();
    }
    if (node.profile.execution_mode == ExecutionMode::kPhysical) {
        node.actuated_power_level = node.power_level == PowerLevel::kOff
                                        ? PowerLevel::kMedium : node.power_level;
        if (node.transition == TransitionState::kStarting ||
            node.transition == TransitionState::kRebooting) {
            node.transition = TransitionState::kStable;
        }
    }
    ++generation_;
    return true;
}

bool NodePowerManager::ReportReadiness(
    const std::string& node_id,
    const zb::storage_model::NodeReadinessStatus& readiness,
    std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle == LifecycleState::kRetired) {
        if (error) *error = "retired node cannot report readiness";
        return false;
    }
    if (!readiness.observed || readiness.observed_at_ms == 0) {
        if (error) *error = "readiness report must be observed and timestamped";
        return false;
    }
    if (readiness.observed_at_ms < node.readiness.observed_at_ms) {
        if (error) *error = "stale readiness report";
        return false;
    }
    node.readiness = readiness;
    ++generation_;
    return true;
}

bool NodePowerManager::ReportOpticalLibraryStatus(
    const std::string& node_id,
    const zb::storage_model::OpticalLibraryStatus& status,
    std::string* error) {
    if (!zb::storage_model::ValidateOpticalLibraryStatus(status, error)) return false;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.profile.kind != ManagedNodeKind::kOpticalLibrary) {
        if (error) *error = "node is not an optical library";
        return false;
    }
    if (node.lifecycle == LifecycleState::kRetired) {
        if (error) *error = "retired optical library cannot report inventory";
        return false;
    }
    if (status.observed_at_ms < node.optical_library.observed_at_ms) {
        if (error) *error = "stale optical library status";
        return false;
    }
    node.optical_library = status;
    if (status.observed && status.local_disc_count > 0 &&
        status.recorded_disc_count == status.local_disc_count &&
        status.pending_write_tasks == 0) {
        node.service_mode = zb::storage_model::NodeServiceMode::kReadOnly;
    }
    ++generation_;
    return true;
}

bool NodePowerManager::SetAdministrativeState(
    const std::string& node_id,
    zb::storage_model::AdministrativeState state,
    std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    if (it->second.lifecycle == LifecycleState::kRetired &&
        state != zb::storage_model::AdministrativeState::kDisabled) {
        if (error) *error = "retired node must remain disabled";
        return false;
    }
    if ((it->second.lifecycle == LifecycleState::kDraining ||
         it->second.lifecycle == LifecycleState::kExiting) &&
        state == zb::storage_model::AdministrativeState::kEnabled) {
        if (error) *error = "draining or exiting node cannot be administratively enabled";
        return false;
    }
    if (it->second.administrative_state != state) {
        it->second.administrative_state = state;
        ++generation_;
    }
    return true;
}

bool NodePowerManager::SetServiceMode(const std::string& node_id,
                                      NodeServiceMode mode,
                                      std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle == LifecycleState::kRetired) {
        if (error) *error = "retired node service mode cannot change";
        return false;
    }
    if (node.profile.kind != ManagedNodeKind::kOpticalLibrary) {
        if (error) *error = "service mode is only configurable for optical libraries";
        return false;
    }
    if (mode == NodeServiceMode::kReadWrite && node.optical_library.observed &&
        node.optical_library.local_disc_count > 0 &&
        node.optical_library.recorded_disc_count == node.optical_library.local_disc_count &&
        node.optical_library.pending_write_tasks == 0) {
        if (error) *error = "fully recorded optical library cannot return to read-write mode";
        return false;
    }
    if (node.service_mode != mode) {
        node.service_mode = mode;
        ++generation_;
    }
    return true;
}

bool NodePowerManager::AssignCmsCompactId(const std::string& node_id,
                                          uint32_t compact_id,
                                          std::string* error) {
    if (compact_id == 0) {
        if (error) *error = "CMS compact id must be non-zero";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    const uint32_t maximum = node.profile.kind == ManagedNodeKind::kOpticalLibrary
                                 ? 0x00ffffffU : 0x0000ffffU;
    if (compact_id > maximum) {
        if (error) *error = "CMS compact id exceeds node-kind width";
        return false;
    }
    if (node.identity.compact_id != 0 && node.identity.compact_id != compact_id) {
        if (error) *error = "CMS compact id is immutable";
        return false;
    }
    if (node.identity.compact_id == compact_id) return true;
    node.identity.compact_id = compact_id;
    ++generation_;
    return true;
}

bool NodePowerManager::SetTransitionState(const std::string& node_id,
                                          TransitionState state,
                                          uint64_t now_ms,
                                          std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    AdvancePowerPolicyLocked(&it->second, now_ms);
    if (it->second.transition != state) {
        it->second.transition = state;
        ++generation_;
    }
    return true;
}

bool NodePowerManager::RequestPowerLevel(const std::string& node_id,
                                         PowerLevel level,
                                         uint64_t now_ms,
                                         bool force,
                                         std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (level == PowerLevel::kOff && !force && !CanPowerOffLocked(node)) {
        if (error) *error = "power-off violates online node or capacity safety policy";
        return false;
    }
    const bool changed = SetPowerLevelLocked(&node, level, now_ms, force);
    if (node.profile.execution_mode != ExecutionMode::kPhysical) {
        node.actuated_power_level = level;
    }
    if (changed) ++generation_;
    return true;
}

uint64_t NodePowerManager::EvaluateHeartbeatHealth(uint64_t now_ms,
                                                   uint64_t suspect_timeout_ms,
                                                   uint64_t dead_timeout_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    bool changed = false;
    for (auto& entry : nodes_) {
        ManagedNodeRuntime& node = entry.second;
        if (node.profile.execution_mode == ExecutionMode::kSimulated ||
            node.lifecycle == LifecycleState::kRetired ||
            (node.power_level == PowerLevel::kOff &&
             (node.profile.execution_mode != ExecutionMode::kPhysical ||
              node.actuated_power_level == PowerLevel::kOff))) {
            continue;
        }
        const uint64_t reference = node.last_heartbeat_ms == 0 ? node.join_time_ms
                                                               : node.last_heartbeat_ms;
        const uint64_t elapsed = now_ms > reference ? now_ms - reference : 0;
        if (dead_timeout_ms != 0 && elapsed >= dead_timeout_ms) {
            if (node.failure_source == zb::storage_model::FailureSource::kNone ||
                node.failure_source == zb::storage_model::FailureSource::kHeartbeatTimeout) {
                if (node.health != ManagedHealthState::kFailed ||
                    node.failure_source != zb::storage_model::FailureSource::kHeartbeatTimeout) {
                    node.health = ManagedHealthState::kFailed;
                    node.failure_source = zb::storage_model::FailureSource::kHeartbeatTimeout;
                    node.failure_reason = "heartbeat timeout";
                    ++node.failure_count;
                    SetPowerLevelLocked(&node, PowerLevel::kOff, now_ms, true);
                    changed = true;
                }
            }
        } else if (suspect_timeout_ms != 0 && elapsed >= suspect_timeout_ms) {
            if (node.failure_source == zb::storage_model::FailureSource::kNone &&
                node.health != ManagedHealthState::kSuspect) {
                node.health = ManagedHealthState::kSuspect;
                changed = true;
            }
        } else if (node.health == ManagedHealthState::kSuspect) {
            node.health = ManagedHealthState::kHealthy;
            changed = true;
        }
    }
    if (changed) ++generation_;
    return generation_;
}

bool NodePowerManager::SetResidentDataBytes(const std::string& node_id,
                                            uint64_t bytes,
                                            std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    if (it->second.resident_data_bytes != bytes) {
        it->second.resident_data_bytes = bytes;
        ++generation_;
    }
    return true;
}

bool NodePowerManager::ReportMetrics(const std::string& node_id,
                                     const NodeAccessMetrics& metrics,
                                     std::string* error) {
    if (metrics.window_end_ms < metrics.window_start_ms) {
        if (error) {
            *error = "metrics window end precedes start";
        }
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (!IsServingLifecycle(node.lifecycle) ||
        node.health == ManagedHealthState::kFailed ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled) {
        if (error) {
            *error = "node is not available for metrics";
        }
        return false;
    }
    const bool same_reporter = metrics.reporter_epoch == 0 ||
                               metrics.reporter_epoch == node.last_metrics_reporter_epoch;
    if (same_reporter && metrics.report_sequence != 0 &&
        metrics.report_sequence == node.last_metrics_sequence) {
        return true;
    }
    if (metrics.window_end_ms < node.last_metrics_end_ms ||
        (same_reporter && metrics.report_sequence != 0 &&
         metrics.report_sequence < node.last_metrics_sequence) ||
        (metrics.report_sequence == 0 && metrics.window_end_ms <= node.last_metrics_end_ms &&
         node.last_metrics_end_ms != 0)) {
        if (error) {
            *error = "stale or duplicate metrics window";
        }
        return false;
    }

    AdvancePowerPolicyLocked(&node, metrics.window_end_ms);
    const uint64_t operations = SaturatingAdd(
        SaturatingAdd(metrics.read_operations, metrics.write_operations), metrics.other_operations);
    const uint64_t window_ms = std::max<uint64_t>(1, metrics.window_end_ms - metrics.window_start_ms);
    const uint64_t read_bandwidth = std::min(
        node.profile.external_network_bandwidth_bytes_per_sec,
        CalculateInternalBandwidthBytesPerSecond(
            node.profile, metrics.access_path, zb::storage_model::IoDirection::kRead));
    const uint64_t write_bandwidth = std::min(
        node.profile.external_network_bandwidth_bytes_per_sec,
        CalculateInternalBandwidthBytesPerSecond(
            node.profile, metrics.access_path, zb::storage_model::IoDirection::kWrite));
    const long double read_rate = static_cast<long double>(metrics.read_bytes) * 1000.0L /
                                  static_cast<long double>(window_ms);
    const long double write_rate = static_cast<long double>(metrics.write_bytes) * 1000.0L /
                                   static_cast<long double>(window_ms);
    const long double read_utilization = read_bandwidth == 0
                                             ? (metrics.read_bytes == 0 ? 0.0L : 1.0L)
                                             : read_rate / read_bandwidth;
    const long double write_utilization = write_bandwidth == 0
                                              ? (metrics.write_bytes == 0 ? 0.0L : 1.0L)
                                              : write_rate / write_bandwidth;
    const double bandwidth_utilization = static_cast<double>(
        std::min<long double>(1.0L, read_utilization + write_utilization));
    const double busy_utilization = std::min(
        1.0, static_cast<double>(metrics.busy_time_ms) / static_cast<double>(window_ms));
    node.last_utilization = std::min(1.0, std::max(bandwidth_utilization, busy_utilization));
    node.last_queue_depth = metrics.max_queue_depth;
    node.last_metrics_end_ms = std::max(node.last_metrics_end_ms, metrics.window_end_ms);
    node.last_metrics_sequence = metrics.report_sequence == 0
                                     ? node.last_metrics_sequence
                                     : metrics.report_sequence;
    if (metrics.reporter_epoch != 0) {
        node.last_metrics_reporter_epoch = metrics.reporter_epoch;
    }
    node.access_operation_count = SaturatingAdd(node.access_operation_count, operations);
    node.accumulated_write_bytes = SaturatingAdd(node.accumulated_write_bytes,
                                                 metrics.write_bytes);
    if (operations != 0 || metrics.read_bytes != 0 || metrics.write_bytes != 0 ||
        metrics.active_requests != 0) {
        const uint64_t reported_access = metrics.last_access_ms == 0
                                             ? metrics.window_end_ms
                                             : metrics.last_access_ms;
        node.last_access_ms = std::max(node.last_access_ms, reported_access);
        const bool peak = node.last_utilization >= policy_.peak_utilization ||
                          node.last_queue_depth >= policy_.peak_queue_depth;
        SetPowerLevelLocked(&node, peak ? PowerLevel::kPeak : PowerLevel::kMedium,
                            metrics.window_end_ms, true);
    }
    ++generation_;
    return true;
}

bool NodePowerManager::ReportAccessEvent(const std::string& node_id,
                                         const NodeAccessEvent& event,
                                         std::string* error) {
    const uint64_t event_ms = event.event_time_us / 1000;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (!IsServingLifecycle(node.lifecycle) ||
        node.health == ManagedHealthState::kFailed ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled) {
        if (error) {
            *error = "node is not available for access events";
        }
        return false;
    }
    if (event_ms < node.last_metrics_end_ms) {
        if (error) {
            *error = "late access event would move managed time backwards";
        }
        return false;
    }

    AdvancePowerPolicyLocked(&node, event_ms);
    const uint64_t interval_ms = node.last_metrics_end_ms == 0 || event_ms <= node.last_metrics_end_ms
                                     ? 1
                                     : event_ms - node.last_metrics_end_ms;
    const uint64_t internal = CalculateInternalBandwidthBytesPerSecond(
        node.profile, event.access_path,
        event.is_write ? zb::storage_model::IoDirection::kWrite
                       : zb::storage_model::IoDirection::kRead);
    const uint64_t effective = std::min(node.profile.external_network_bandwidth_bytes_per_sec, internal);
    const long double rate = static_cast<long double>(event.bytes) * 1000.0L /
                             static_cast<long double>(interval_ms);
    node.last_utilization = effective == 0
                                ? 0.0
                                : std::min(1.0, static_cast<double>(rate / effective));
    node.last_queue_depth = event.queue_depth;
    node.last_metrics_end_ms = std::max(node.last_metrics_end_ms, event_ms);
    node.last_access_ms = std::max(node.last_access_ms, event_ms);
    node.access_operation_count = SaturatingAdd(node.access_operation_count, 1);
    if (event.is_write) {
        node.accumulated_write_bytes = SaturatingAdd(node.accumulated_write_bytes, event.bytes);
    }
    const bool peak = node.last_utilization >= policy_.peak_utilization ||
                      event.queue_depth >= policy_.peak_queue_depth;
    SetPowerLevelLocked(&node, peak ? PowerLevel::kPeak : PowerLevel::kMedium, event_ms, true);
    if (node.lifecycle == LifecycleState::kWorking && event_ms >= node.planned_exit_time_ms) {
        node.lifecycle = LifecycleState::kDraining;
        node.drain_remaining_bytes = node.resident_data_bytes;
        node.drain_last_report_ms = event_ms;
    }
    ++generation_;
    return true;
}

bool NodePowerManager::ReportDrainProgress(const std::string& node_id,
                                           uint64_t report_ts_ms,
                                           uint64_t remaining_objects,
                                           uint64_t remaining_bytes,
                                           uint32_t active_requests,
                                           std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    if (node.lifecycle != LifecycleState::kDraining &&
        node.lifecycle != LifecycleState::kExiting) {
        if (error) {
            *error = "node is not draining";
        }
        return false;
    }
    if (report_ts_ms < node.drain_last_report_ms) {
        if (error) {
            *error = "stale drain progress report";
        }
        return false;
    }
    AdvancePowerPolicyLocked(&node, report_ts_ms);
    node.drain_remaining_objects = remaining_objects;
    node.drain_remaining_bytes = remaining_bytes;
    node.drain_active_requests = active_requests;
    node.drain_last_report_ms = report_ts_ms;
    node.resident_data_bytes = remaining_bytes;
    if (remaining_objects == 0 && remaining_bytes == 0 && active_requests == 0) {
        node.lifecycle = LifecycleState::kExiting;
    }
    ++generation_;
    return true;
}

bool NodePowerManager::ReportPowerActuation(const std::string& node_id,
                                            PowerLevel applied_level,
                                            uint64_t now_ms,
                                            bool success,
                                            const std::string& message,
                                            std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    ManagedNodeRuntime& node = it->second;
    IntegrateEnergyLocked(&node, now_ms);
    node.last_power_actuation_ms = now_ms;
    node.last_power_actuation_error = success ? std::string{} : message;
    if (success) node.actuated_power_level = applied_level;
    ++generation_;
    return true;
}

uint64_t NodePowerManager::Tick(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    return TickMatchingLocked(now_ms, -1);
}

uint64_t NodePowerManager::TickRealTime(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    return TickMatchingLocked(now_ms, 0);
}

uint64_t NodePowerManager::AdvanceSimulatedTime(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    return TickMatchingLocked(now_ms, 1);
}

uint64_t NodePowerManager::TickMatchingLocked(uint64_t now_ms, int execution_filter) {
    bool changed = false;
    for (auto& item : nodes_) {
        ManagedNodeRuntime& node = item.second;
        const bool simulated = node.profile.execution_mode == ExecutionMode::kSimulated;
        if ((execution_filter == 0 && simulated) || (execution_filter == 1 && !simulated)) {
            continue;
        }
        changed = AdvancePowerPolicyLocked(&node, now_ms) || changed;
        if (node.health == ManagedHealthState::kFailed && node.repair_due_time_ms != 0 &&
            now_ms >= node.repair_due_time_ms && node.lifecycle != LifecycleState::kRetired) {
            node.health = ManagedHealthState::kHealthy;
            node.failure_reason.clear();
            node.failure_source = zb::storage_model::FailureSource::kNone;
            node.repair_due_time_ms = 0;
            node.next_failure_time_ms = ScheduleNextFailureTime(node, now_ms);
            SetPowerLevelLocked(&node, PowerLevel::kMedium, now_ms, true);
            changed = true;
        }
        if (node.health == ManagedHealthState::kHealthy &&
            node.lifecycle == LifecycleState::kWorking && node.next_failure_time_ms != 0 &&
            now_ms >= node.next_failure_time_ms) {
            node.health = ManagedHealthState::kFailed;
            node.failure_reason = "stochastic reliability failure";
            node.failure_source = zb::storage_model::FailureSource::kReliabilityModel;
            ++node.failure_count;
            node.repair_due_time_ms = node.profile.reliability.mean_repair_time_ms == 0
                                           ? 0
                                           : SaturatingAdd(now_ms, node.profile.reliability.mean_repair_time_ms);
            SetPowerLevelLocked(&node, PowerLevel::kOff, now_ms, true);
            changed = true;
        }
        if (node.lifecycle == LifecycleState::kWorking &&
            now_ms >= node.planned_exit_time_ms) {
            node.lifecycle = LifecycleState::kDraining;
            node.administrative_state = zb::storage_model::AdministrativeState::kDraining;
            node.drain_remaining_bytes = node.resident_data_bytes;
            node.drain_last_report_ms = now_ms;
            changed = true;
        }
        const PowerLevel desired = DesiredPowerLevelLocked(node, now_ms);
        const PowerLevel safe_desired = desired == PowerLevel::kOff &&
                                                node.lifecycle == LifecycleState::kWorking &&
                                                !CanPowerOffLocked(node)
                                            ? PowerLevel::kStandby
                                            : desired;
        changed = SetPowerLevelLocked(&node, safe_desired, now_ms, false) || changed;
        if (node.profile.execution_mode != ExecutionMode::kPhysical) {
            node.actuated_power_level = node.power_level;
        }
    }
    if (changed) {
        ++generation_;
    }
    RecordHistoryLocked(now_ms);
    return generation_;
}

bool NodePowerManager::GetNode(const std::string& node_id, ManagedNodeRuntime* out) const {
    if (!out) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) {
        return false;
    }
    *out = it->second;
    return true;
}

std::vector<ManagedNodeRuntime> NodePowerManager::ListNodes() const {
    return ListNodes(ManagedNodeFilter{});
}

std::vector<ManagedNodeRuntime> NodePowerManager::ListNodes(
    const ManagedNodeFilter& filter) const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<ManagedNodeRuntime> result;
    result.reserve(nodes_.size());
    for (const auto& item : nodes_) {
        if (MatchesFilter(item.second, filter)) {
            result.push_back(item.second);
        }
    }
    SortByNodeId(&result);
    return result;
}

std::vector<ManagedNodeRuntime> NodePowerManager::ListByLifecycle(LifecycleState state) const {
    ManagedNodeFilter filter;
    filter.lifecycle = state;
    return ListNodes(filter);
}

std::vector<ManagedNodeRuntime> NodePowerManager::ListFailedNodes() const {
    ManagedNodeFilter filter;
    filter.health = ManagedHealthState::kFailed;
    filter.include_retired = false;
    return ListNodes(filter);
}

PowerAndEnergySummary NodePowerManager::Summary() const {
    std::lock_guard<std::mutex> lock(mu_);
    return SummaryLocked();
}

PowerAndEnergySummary NodePowerManager::SummaryLocked() const {
    PowerAndEnergySummary summary;
    summary.generation = generation_;
    summary.total_nodes = nodes_.size();
    for (const auto& item : nodes_) {
        const ManagedNodeRuntime& node = item.second;
        switch (node.profile.kind) {
        case ManagedNodeKind::kStorage: ++summary.storage_nodes; break;
        case ManagedNodeKind::kMetadata: ++summary.metadata_nodes; break;
        case ManagedNodeKind::kOpticalLibrary: ++summary.optical_library_nodes; break;
        }
        switch (node.lifecycle) {
        case LifecycleState::kJoining:
            ++summary.joining_nodes;
            break;
        case LifecycleState::kWorking:
            ++summary.working_nodes;
            break;
        case LifecycleState::kDraining:
        case LifecycleState::kExiting:
            ++summary.draining_nodes;
            break;
        case LifecycleState::kRetired:
            ++summary.retired_nodes;
            break;
        }
        if (node.health == ManagedHealthState::kFailed && node.lifecycle != LifecycleState::kRetired) {
            ++summary.failed_nodes;
        }
        switch (node.power_level) {
        case PowerLevel::kPeak:
            ++summary.peak_nodes;
            break;
        case PowerLevel::kMedium:
            ++summary.medium_nodes;
            break;
        case PowerLevel::kStandby:
            ++summary.standby_nodes;
            break;
        case PowerLevel::kOff:
            ++summary.off_nodes;
            break;
        }
        summary.total_power_milliwatts = SaturatingAdd(
            summary.total_power_milliwatts, PowerMilliwatts(node.profile.power, node.power_level));
        summary.total_energy_microjoules = SaturatingAdd(
            summary.total_energy_microjoules, node.energy_microjoules);
        summary.total_actuated_power_milliwatts = SaturatingAdd(
            summary.total_actuated_power_milliwatts,
            PowerMilliwatts(node.profile.power, node.actuated_power_level));
        summary.total_actuated_energy_microjoules = SaturatingAdd(
            summary.total_actuated_energy_microjoules,
            node.actuated_energy_microjoules);
        const uint64_t capacity = node.inventory.devices.empty()
                                      ? CalculateCapacityBytes(node.profile)
                                      : zb::storage_model::InventoryCapacityBytes(node.inventory);
        const uint64_t free = node.inventory.devices.empty()
                                  ? (capacity > node.resident_data_bytes
                                         ? capacity - node.resident_data_bytes : 0)
                                  : zb::storage_model::InventoryFreeBytes(node.inventory);
        const uint64_t used = capacity > free ? capacity - free : 0;
        summary.total_capacity_bytes = SaturatingAdd(summary.total_capacity_bytes, capacity);
        summary.total_free_bytes = SaturatingAdd(summary.total_free_bytes, free);
        summary.total_used_bytes = SaturatingAdd(summary.total_used_bytes, used);
        const bool available = node.profile.participates_in_placement &&
                               node.lifecycle == LifecycleState::kWorking &&
                               node.health == ManagedHealthState::kHealthy &&
                               node.administrative_state ==
                                   zb::storage_model::AdministrativeState::kEnabled &&
                               node.service_mode == zb::storage_model::NodeServiceMode::kReadWrite &&
                               node.power_level != PowerLevel::kOff &&
                               node.transition == TransitionState::kStable &&
                               (node.profile.execution_mode != ExecutionMode::kPhysical ||
                                node.actuated_power_level != PowerLevel::kOff);
        const uint64_t available_free = available ? FreeBytesForSafety(node) : 0;
        summary.available_capacity_bytes = SaturatingAdd(
            summary.available_capacity_bytes, available_free);
        summary.unavailable_capacity_bytes = SaturatingAdd(
            summary.unavailable_capacity_bytes,
            free > available_free ? free - available_free : 0);
        summary.accumulated_write_bytes = SaturatingAdd(
            summary.accumulated_write_bytes, node.accumulated_write_bytes);
        summary.wake_count = SaturatingAdd(summary.wake_count, node.wake_count);
        summary.standby_transition_count = SaturatingAdd(
            summary.standby_transition_count, node.standby_transition_count);
        summary.power_off_count = SaturatingAdd(summary.power_off_count, node.power_off_count);
        summary.replacement_count = SaturatingAdd(summary.replacement_count, node.replacement_count);
    }
    return summary;
}

std::vector<ManagedMetricsSample> NodePowerManager::MetricsHistory(uint64_t start_ms,
                                                                  uint64_t end_ms,
                                                                  uint32_t limit) const {
    std::lock_guard<std::mutex> lock(mu_);
    const uint32_t bounded = limit == 0 ? 1000 : std::min<uint32_t>(limit, 100000);
    std::vector<ManagedMetricsSample> out;
    out.reserve(std::min<size_t>(history_.size(), bounded));
    for (const auto& sample : history_) {
        if (sample.timestamp_ms < start_ms || (end_ms != 0 && sample.timestamp_ms > end_ms)) continue;
        out.push_back(sample);
        if (out.size() >= bounded) break;
    }
    return out;
}

void NodePowerManager::RecordHistoryLocked(uint64_t now_ms) {
    if (policy_.history_max_samples == 0) return;
    if (!history_.empty() && history_.back().timestamp_ms == now_ms) {
        history_.back().summary = SummaryLocked();
        return;
    }
    history_.push_back({now_ms, SummaryLocked()});
    while (history_.size() > policy_.history_max_samples) history_.pop_front();
}

uint64_t NodePowerManager::generation() const {
    std::lock_guard<std::mutex> lock(mu_);
    return generation_;
}

PowerPolicy NodePowerManager::policy() const {
    std::lock_guard<std::mutex> lock(mu_);
    return policy_;
}

NodePowerManagerSnapshot NodePowerManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mu_);
    NodePowerManagerSnapshot snapshot;
    snapshot.generation = generation_;
    snapshot.nodes.reserve(nodes_.size());
    for (const auto& item : nodes_) {
        snapshot.nodes.push_back(item.second);
    }
    snapshot.history.assign(history_.begin(), history_.end());
    SortByNodeId(&snapshot.nodes);
    return snapshot;
}

bool NodePowerManager::Restore(const NodePowerManagerSnapshot& snapshot, std::string* error) {
    if (snapshot.format_version == 0 ||
        snapshot.format_version > kNodePowerManagerSnapshotFormatVersion) {
        if (error) {
            *error = "unsupported managed snapshot version";
        }
        return false;
    }
    std::unordered_map<std::string, ManagedNodeRuntime> restored;
    restored.reserve(snapshot.nodes.size());
    for (auto node : snapshot.nodes) {
        std::string validation_error;
        if (!ValidateManagedNodeProfile(node.profile, &validation_error)) {
            if (error) {
                *error = "invalid node in managed snapshot: " + validation_error;
            }
            return false;
        }
        if (node.identity.node_id.empty()) {
            node.identity.node_id = node.profile.node_id;
            node.identity.kind = node.profile.kind;
            node.identity.execution_mode = node.profile.execution_mode;
        }
        if (node.inventory.node_id.empty()) node.inventory.node_id = node.profile.node_id;
        if (node.last_actuated_energy_update_ms == 0) {
            node.last_actuated_energy_update_ms = node.last_energy_update_ms;
        }
        if (!node.inventory.devices.empty() &&
            !zb::storage_model::ValidateNodeInventorySnapshot(node.inventory,
                                                               &validation_error)) {
            if (error) *error = "invalid inventory in managed snapshot: " + validation_error;
            return false;
        }
        if (node.profile.kind == ManagedNodeKind::kOpticalLibrary &&
            !zb::storage_model::ValidateOpticalLibraryStatus(node.optical_library,
                                                              &validation_error)) {
            if (error) *error = "invalid optical status in managed snapshot: " + validation_error;
            return false;
        }
        const std::string restored_node_id = node.profile.node_id;
        if (!restored.emplace(restored_node_id, std::move(node)).second) {
            if (error) {
                *error = "duplicate node in managed snapshot: " + restored_node_id;
            }
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    nodes_ = std::move(restored);
    generation_ = std::max<uint64_t>(1, snapshot.generation);
    history_.assign(snapshot.history.begin(), snapshot.history.end());
    while (history_.size() > policy_.history_max_samples) history_.pop_front();
    return true;
}

bool ValidatePowerPolicy(const PowerPolicy& policy, std::string* error) {
    if (!std::isfinite(policy.peak_utilization) ||
        policy.peak_utilization < 0.0 || policy.peak_utilization > 1.0) {
        if (error) {
            *error = "peak_utilization must be in [0, 1]";
        }
        return false;
    }
    if (policy.off_after_ms <= policy.standby_after_ms) {
        if (error) {
            *error = "off_after_ms must be greater than standby_after_ms";
        }
        return false;
    }
    return true;
}

uint64_t NodePowerManager::SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t NodePowerManager::SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

uint64_t NodePowerManager::SampleFailureIntervalMs(uint64_t mean_ms, uint64_t seed) {
    if (mean_ms == 0) return 0;
    std::mt19937_64 generator(seed);
    std::uniform_real_distribution<long double> distribution(0.0L, 1.0L);
    const long double u = std::max<long double>(distribution(generator), 1.0e-18L);
    const long double sampled = -std::log(1.0L - u) * static_cast<long double>(mean_ms);
    if (sampled >= static_cast<long double>(std::numeric_limits<uint64_t>::max()))
        return std::numeric_limits<uint64_t>::max();
    return std::max<uint64_t>(1, static_cast<uint64_t>(sampled));
}

uint64_t NodePowerManager::FailureMeanMsForFit(uint64_t failure_rate_fit) {
    if (failure_rate_fit == 0) return 0;
    constexpr long double kMillisecondsPerHour = 3600000.0L;
    const long double mean = 1000000000.0L * kMillisecondsPerHour /
                             static_cast<long double>(failure_rate_fit);
    return mean >= static_cast<long double>(std::numeric_limits<uint64_t>::max())
               ? std::numeric_limits<uint64_t>::max()
               : std::max<uint64_t>(1, static_cast<uint64_t>(mean));
}

uint64_t NodePowerManager::ScheduleNextFailureTime(const ManagedNodeRuntime& node,
                                                   uint64_t from_ms) {
    const ReliabilityProfile& reliability = node.profile.reliability;
    if (reliability.model != zb::storage_model::ReliabilityModel::kBathtubCurve) {
        return reliability.mean_time_between_failures_ms == 0
                   ? 0
                   : SaturatingAdd(from_ms, SampleFailureIntervalMs(
                         reliability.mean_time_between_failures_ms,
                         node.reliability_seed + node.failure_count));
    }

    uint64_t cursor = std::max(from_ms, node.join_time_ms);
    for (uint32_t phase_attempt = 0; phase_attempt < 3; ++phase_attempt) {
        const uint64_t age = cursor - node.join_time_ms;
        uint64_t fit = reliability.wearout_failure_rate_fit;
        uint64_t phase_end_age = 0;
        if (reliability.early_failure_window_ms != 0 &&
            age < reliability.early_failure_window_ms) {
            fit = reliability.early_failure_rate_fit;
            phase_end_age = reliability.early_failure_window_ms;
        } else if (reliability.wearout_start_age_ms == 0 ||
                   age < reliability.wearout_start_age_ms) {
            fit = reliability.useful_life_failure_rate_fit;
            phase_end_age = reliability.wearout_start_age_ms;
        }
        const uint64_t mean_ms = FailureMeanMsForFit(fit);
        if (mean_ms != 0) {
            const uint64_t candidate = SaturatingAdd(
                cursor, SampleFailureIntervalMs(
                            mean_ms, node.reliability_seed + node.failure_count * 7 + phase_attempt));
            const uint64_t phase_end = phase_end_age == 0
                                           ? 0 : SaturatingAdd(node.join_time_ms, phase_end_age);
            if (phase_end == 0 || candidate < phase_end) return candidate;
        }
        if (phase_end_age == 0) break;
        const uint64_t next = SaturatingAdd(node.join_time_ms, phase_end_age);
        if (next <= cursor) break;
        cursor = next;
    }
    return reliability.mean_time_between_failures_ms == 0
               ? 0
               : SaturatingAdd(from_ms, SampleFailureIntervalMs(
                     reliability.mean_time_between_failures_ms,
                     node.reliability_seed + node.failure_count));
}

uint64_t NodePowerManager::CapacityBytesForSafety(const ManagedNodeRuntime& node) {
    return node.inventory.devices.empty() ? CalculateCapacityBytes(node.profile)
                                          : zb::storage_model::InventoryHealthyCapacityBytes(
                                                node.inventory);
}

uint64_t NodePowerManager::FreeBytesForSafety(const ManagedNodeRuntime& node) {
    if (!node.inventory.devices.empty()) {
        return zb::storage_model::InventoryHealthyFreeBytes(node.inventory);
    }
    const uint64_t capacity = CalculateCapacityBytes(node.profile);
    return capacity > node.resident_data_bytes ? capacity - node.resident_data_bytes : 0;
}

void NodePowerManager::IntegrateEnergyLocked(ManagedNodeRuntime* node, uint64_t now_ms) {
    if (!node) return;
    if (now_ms > node->last_energy_update_ms) {
        const uint64_t elapsed_ms = now_ms - node->last_energy_update_ms;
        // mW * ms equals microjoules exactly.
        const uint64_t delta = SaturatingMultiply(
            PowerMilliwatts(node->profile.power, node->power_level), elapsed_ms);
        node->energy_microjoules = SaturatingAdd(node->energy_microjoules, delta);
        node->last_energy_update_ms = now_ms;
    }
    if (now_ms > node->last_actuated_energy_update_ms) {
        const uint64_t elapsed_ms = now_ms - node->last_actuated_energy_update_ms;
        const uint64_t delta = SaturatingMultiply(
            PowerMilliwatts(node->profile.power, node->actuated_power_level), elapsed_ms);
        node->actuated_energy_microjoules = SaturatingAdd(
            node->actuated_energy_microjoules, delta);
        node->last_actuated_energy_update_ms = now_ms;
    }
}

bool NodePowerManager::AdvancePowerPolicyLocked(ManagedNodeRuntime* node, uint64_t now_ms) {
    if (!node || now_ms <= node->last_energy_update_ms) {
        return false;
    }

    bool changed = false;
    while (node->last_energy_update_ms < now_ms) {
        const uint64_t cursor_ms = node->last_energy_update_ms;
        const PowerLevel desired = DesiredPowerLevelLocked(*node, cursor_ms);
        const PowerLevel safe_desired = desired == PowerLevel::kOff &&
                                                node->lifecycle == LifecycleState::kWorking &&
                                                !CanPowerOffLocked(*node)
                                            ? PowerLevel::kStandby
                                            : desired;
        changed = SetPowerLevelLocked(node, safe_desired, cursor_ms, false) || changed;

        uint64_t next_ms = now_ms;
        const auto consider_boundary = [&](uint64_t boundary_ms) {
            if (boundary_ms > cursor_ms && boundary_ms < next_ms) {
                next_ms = boundary_ms;
            }
        };
        consider_boundary(SaturatingAdd(node->last_metrics_end_ms,
                                        policy_.metrics_freshness_ms));
        consider_boundary(SaturatingAdd(node->last_access_ms, policy_.standby_after_ms));
        consider_boundary(SaturatingAdd(node->last_access_ms, policy_.off_after_ms));
        consider_boundary(SaturatingAdd(node->power_state_enter_ms,
                                        policy_.minimum_residency_ms));
        IntegrateEnergyLocked(node, next_ms);
    }

    const PowerLevel desired = DesiredPowerLevelLocked(*node, now_ms);
    const PowerLevel safe_desired = desired == PowerLevel::kOff &&
                                            node->lifecycle == LifecycleState::kWorking &&
                                            !CanPowerOffLocked(*node)
                                        ? PowerLevel::kStandby
                                        : desired;
    return SetPowerLevelLocked(node, safe_desired, now_ms, false) || changed;
}

bool NodePowerManager::SetPowerLevelLocked(ManagedNodeRuntime* node,
                                           PowerLevel level,
                                           uint64_t now_ms,
                                           bool force) {
    if (!node || node->power_level == level) {
        return false;
    }
    const bool downgrade = static_cast<int>(level) < static_cast<int>(node->power_level);
    if (!force && downgrade && now_ms >= node->power_state_enter_ms &&
        now_ms - node->power_state_enter_ms < policy_.minimum_residency_ms) {
        return false;
    }
    IntegrateEnergyLocked(node, now_ms);
    const PowerLevel previous = node->power_level;
    node->power_level = level;
    node->power_state_enter_ms = now_ms;
    if ((previous == PowerLevel::kOff || previous == PowerLevel::kStandby) &&
        (level == PowerLevel::kMedium || level == PowerLevel::kPeak)) {
        ++node->wake_count;
    }
    if (level == PowerLevel::kStandby) ++node->standby_transition_count;
    if (level == PowerLevel::kOff) ++node->power_off_count;
    return true;
}

PowerLevel NodePowerManager::DesiredPowerLevelLocked(const ManagedNodeRuntime& node,
                                                     uint64_t now_ms) const {
    if (node.health == ManagedHealthState::kFailed ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled ||
        node.lifecycle == LifecycleState::kJoining ||
        node.lifecycle == LifecycleState::kExiting ||
        node.lifecycle == LifecycleState::kRetired) {
        return PowerLevel::kOff;
    }
    const uint64_t idle_ms = now_ms > node.last_access_ms ? now_ms - node.last_access_ms : 0;
    const bool may_power_off = policy_.allow_off_with_resident_data || node.resident_data_bytes == 0;
    if (idle_ms >= policy_.off_after_ms && may_power_off) {
        return PowerLevel::kOff;
    }
    if (idle_ms >= policy_.standby_after_ms) {
        return PowerLevel::kStandby;
    }
    const bool metrics_fresh = now_ms >= node.last_metrics_end_ms &&
                               now_ms - node.last_metrics_end_ms < policy_.metrics_freshness_ms;
    if (metrics_fresh && (node.last_utilization >= policy_.peak_utilization ||
                          node.last_queue_depth >= policy_.peak_queue_depth)) {
        return PowerLevel::kPeak;
    }
    return PowerLevel::kMedium;
}

bool NodePowerManager::CanPowerOffLocked(const ManagedNodeRuntime& candidate) const {
    uint32_t remaining_same_kind = 0;
    uint64_t remaining_storage_capacity = 0;
    uint64_t remaining_storage_free = 0;
    for (const auto& entry : nodes_) {
        const ManagedNodeRuntime& node = entry.second;
        if (node.profile.node_id == candidate.profile.node_id ||
            node.lifecycle != LifecycleState::kWorking ||
            node.health == ManagedHealthState::kFailed ||
            node.administrative_state != zb::storage_model::AdministrativeState::kEnabled ||
            node.power_level == PowerLevel::kOff ||
            node.transition != TransitionState::kStable ||
            (node.profile.execution_mode == ExecutionMode::kPhysical &&
             node.actuated_power_level == PowerLevel::kOff)) {
            continue;
        }
        if (node.profile.kind == candidate.profile.kind) ++remaining_same_kind;
        if (node.profile.kind == ManagedNodeKind::kStorage) {
            const uint64_t capacity = CapacityBytesForSafety(node);
            remaining_storage_capacity = SaturatingAdd(remaining_storage_capacity, capacity);
            remaining_storage_free = SaturatingAdd(remaining_storage_free,
                                                   FreeBytesForSafety(node));
        }
    }
    uint32_t minimum = 0;
    switch (candidate.profile.kind) {
    case ManagedNodeKind::kStorage: minimum = policy_.min_online_storage_nodes; break;
    case ManagedNodeKind::kMetadata: minimum = policy_.min_online_metadata_nodes; break;
    case ManagedNodeKind::kOpticalLibrary: minimum = policy_.min_online_optical_nodes; break;
    }
    if (remaining_same_kind < minimum) return false;
    if (candidate.profile.kind == ManagedNodeKind::kStorage &&
        (remaining_storage_capacity < policy_.min_online_storage_capacity_bytes ||
         remaining_storage_free < policy_.min_online_storage_free_bytes)) {
        return false;
    }
    return true;
}

} // namespace zb::scheduler
