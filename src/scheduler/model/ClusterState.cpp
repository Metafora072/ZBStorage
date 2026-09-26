#include "ClusterState.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <utility>

namespace zb::scheduler {
namespace {

ManagedNodeProfile DefaultHeartbeatProfile(const zb::rpc::HeartbeatRequest& request) {
    ManagedNodeProfile profile;
    if (request.node_type() == zb::rpc::NODE_METADATA) {
        profile = DefaultMetadataNodeProfile(request.node_id());
    } else if (request.node_type() == zb::rpc::NODE_OPTICAL) {
        profile = DefaultOpticalLibraryProfile(request.node_id());
    } else {
        profile = DefaultStorageNodeProfile(request.node_id());
    }
    if (request.node_type() == zb::rpc::NODE_VIRTUAL_POOL) {
        profile.execution_mode = ExecutionMode::kVirtual;
        profile.logical_node_count = std::max<uint32_t>(1, request.virtual_node_count());
    }
    return profile;
}

DeviceKind HeartbeatDeviceKind(const zb::rpc::DiskHeartbeat& disk, ManagedNodeKind node_kind) {
    switch (disk.device_kind()) {
    case zb::rpc::MANAGED_DEVICE_SSD: return DeviceKind::kSsd;
    case zb::rpc::MANAGED_DEVICE_DISC: return DeviceKind::kDisc;
    case zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE: return DeviceKind::kOpticalDrive;
    case zb::rpc::MANAGED_DEVICE_HDD: return DeviceKind::kHdd;
    case zb::rpc::MANAGED_DEVICE_UNKNOWN:
        return node_kind == ManagedNodeKind::kMetadata ? DeviceKind::kSsd : DeviceKind::kHdd;
    }
    return DeviceKind::kHdd;
}

zb::rpc::NodeType LegacyNodeType(const ManagedNodeRuntime& node) {
    if (node.profile.kind == ManagedNodeKind::kMetadata) return zb::rpc::NODE_METADATA;
    if (node.profile.kind == ManagedNodeKind::kOpticalLibrary) return zb::rpc::NODE_OPTICAL;
    if (node.profile.execution_mode == ExecutionMode::kVirtual) return zb::rpc::NODE_VIRTUAL_POOL;
    return zb::rpc::NODE_REAL;
}

zb::rpc::NodeHealthState LegacyHealth(ManagedHealthState health) {
    switch (health) {
    case ManagedHealthState::kHealthy: return zb::rpc::NODE_HEALTH_HEALTHY;
    case ManagedHealthState::kSuspect: return zb::rpc::NODE_HEALTH_SUSPECT;
    case ManagedHealthState::kFailed: return zb::rpc::NODE_HEALTH_DEAD;
    }
    return zb::rpc::NODE_HEALTH_SUSPECT;
}

zb::rpc::NodeAdminState LegacyAdmin(const ManagedNodeRuntime& node) {
    if (node.lifecycle == LifecycleState::kRetired ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled) {
        return zb::rpc::NODE_ADMIN_DISABLED;
    }
    if (node.lifecycle == LifecycleState::kDraining ||
        node.lifecycle == LifecycleState::kExiting ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDraining ||
        !node.profile.participates_in_placement ||
        node.service_mode == zb::storage_model::NodeServiceMode::kReadOnly) {
        return zb::rpc::NODE_ADMIN_DRAINING;
    }
    return zb::rpc::NODE_ADMIN_ENABLED;
}

zb::rpc::NodePowerState LegacyPower(const ManagedNodeRuntime& node) {
    if (node.transition == TransitionState::kStarting ||
        node.transition == TransitionState::kRebooting) {
        return zb::rpc::NODE_POWER_STARTING;
    }
    if (node.transition == TransitionState::kStopping) return zb::rpc::NODE_POWER_STOPPING;
    return node.actuated_power_level == PowerLevel::kOff ? zb::rpc::NODE_POWER_OFF
                                                         : zb::rpc::NODE_POWER_ON;
}

} // namespace

ClusterState::ClusterState(FailureDetector detector, NodePowerManager* nodes)
    : detector_(std::move(detector)), nodes_(nodes) {}

HeartbeatAssignment ClusterState::ReportHeartbeat(const zb::rpc::HeartbeatRequest& request) {
    HeartbeatAssignment failed;
    failed.success = false;
    if (!nodes_ || request.node_id().empty()) {
        failed.error = "node catalog is unavailable or node_id is empty";
        return failed;
    }
    const uint64_t now_ms = request.report_ts_ms() == 0 ? NowMs() : request.report_ts_ms();
    ManagedNodeRuntime node;
    std::string error;
    if (!nodes_->GetNode(request.node_id(), &node)) {
        const ManagedNodeProfile profile = DefaultHeartbeatProfile(request);
        if (!nodes_->RegisterNode(profile, now_ms,
                                  std::hash<std::string>{}(request.node_id()), &error)) {
            failed.error = error;
            return failed;
        }
        nodes_->GetNode(request.node_id(), &node);
    }

    zb::storage_model::NodeInventorySnapshot inventory;
    inventory.node_id = request.node_id();
    inventory.generation = node.inventory.generation + 1;
    inventory.observed_at_ms = now_ms;
    inventory.devices.reserve(static_cast<size_t>(request.disks_size()));
    for (const auto& disk : request.disks()) {
        zb::storage_model::DeviceInventory device;
        device.disk_id = disk.disk_id();
        device.kind = HeartbeatDeviceKind(disk, node.profile.kind);
        device.capacity_bytes = disk.capacity_bytes();
        device.free_bytes = disk.free_bytes();
        device.is_healthy = disk.is_healthy();
        device.read_bandwidth_bytes_per_sec = disk.read_bandwidth_bytes_per_sec();
        device.write_bandwidth_bytes_per_sec = disk.write_bandwidth_bytes_per_sec();
        device.accumulated_write_bytes = disk.accumulated_write_bytes();
        device.last_update_ms = now_ms;
        inventory.devices.push_back(std::move(device));
    }
    if (!nodes_->ReportHeartbeat(request.node_id(), request.address(), std::move(inventory),
                                 now_ms, &error)) {
        failed.error = error;
        return failed;
    }
    if (request.readiness_reported()) {
        zb::storage_model::NodeReadinessStatus readiness;
        readiness.observed = true;
        readiness.initialization_complete = request.initialization_complete();
        readiness.inventory_ready = request.disks_size() > 0;
        readiness.metadata_ready = request.metadata_ready();
        readiness.observed_at_ms = now_ms;
        readiness.message = request.readiness_message();
        if (!nodes_->ReportReadiness(request.node_id(), readiness, &error)) {
            failed.error = error;
            return failed;
        }
        nodes_->GetNode(request.node_id(), &node);
        if (node.lifecycle == LifecycleState::kJoining && readiness.Ready() &&
            !nodes_->ActivateNode(request.node_id(), now_ms, &error)) {
            failed.error = error;
            return failed;
        }
    }
    nodes_->GetNode(request.node_id(), &node);
    return BuildAssignment(node, nodes_->generation());
}

uint64_t ClusterState::TickHealth() {
    return nodes_ ? nodes_->EvaluateHeartbeatHealth(NowMs(), detector_.suspect_timeout_ms(),
                                                    detector_.dead_timeout_ms())
                  : 0;
}

uint64_t ClusterState::SetNodeAdminState(const std::string& node_id,
                                         zb::rpc::NodeAdminState state,
                                         std::string* error) {
    if (!nodes_) {
        if (error) *error = "node catalog is unavailable";
        return 0;
    }
    zb::storage_model::AdministrativeState managed =
        zb::storage_model::AdministrativeState::kEnabled;
    if (state == zb::rpc::NODE_ADMIN_DRAINING) {
        managed = zb::storage_model::AdministrativeState::kDraining;
    } else if (state == zb::rpc::NODE_ADMIN_DISABLED) {
        managed = zb::storage_model::AdministrativeState::kDisabled;
    }
    (void)nodes_->SetAdministrativeState(node_id, managed, error);
    return nodes_->generation();
}

uint64_t ClusterState::SetDesiredPowerState(const std::string& node_id,
                                            zb::rpc::NodePowerState state,
                                            std::string* error) {
    if (!nodes_) {
        if (error) *error = "node catalog is unavailable";
        return 0;
    }
    const PowerLevel level = state == zb::rpc::NODE_POWER_OFF ? PowerLevel::kOff
                                                              : PowerLevel::kMedium;
    (void)nodes_->RequestPowerLevel(node_id, level, NowMs(), true, error);
    return nodes_->generation();
}

uint64_t ClusterState::SetCurrentPowerState(const std::string& node_id,
                                            zb::rpc::NodePowerState state,
                                            std::string* error) {
    if (!nodes_) {
        if (error) *error = "node catalog is unavailable";
        return 0;
    }
    const uint64_t now_ms = NowMs();
    if (state == zb::rpc::NODE_POWER_STARTING) {
        (void)nodes_->SetTransitionState(node_id, TransitionState::kStarting, now_ms, error);
    } else if (state == zb::rpc::NODE_POWER_STOPPING) {
        (void)nodes_->SetTransitionState(node_id, TransitionState::kStopping, now_ms, error);
    } else {
        const PowerLevel applied = state == zb::rpc::NODE_POWER_OFF ? PowerLevel::kOff
                                                                    : PowerLevel::kMedium;
        (void)nodes_->ReportPowerActuation(node_id, applied, now_ms, true,
                                           "legacy lifecycle confirmation", error);
        (void)nodes_->SetTransitionState(node_id, TransitionState::kStable, now_ms, nullptr);
    }
    return nodes_->generation();
}

bool ClusterState::Snapshot(uint64_t min_generation,
                            uint64_t* generation,
                            std::vector<NodeState>* nodes) const {
    const uint64_t current = nodes_ ? nodes_->generation() : 0;
    if (generation) *generation = current;
    if (!nodes) return true;
    nodes->clear();
    if (!nodes_ || current < min_generation) return true;
    for (const auto& node : nodes_->ListNodes()) nodes->push_back(BuildNodeState(node));
    return true;
}

bool ClusterState::GetNode(const std::string& node_id, NodeState* node) const {
    if (!nodes_ || !node) return false;
    ManagedNodeRuntime managed;
    if (!nodes_->GetNode(node_id, &managed)) return false;
    *node = BuildNodeState(managed);
    return true;
}

bool ClusterState::CreateOperation(const std::string& node_id,
                                   zb::rpc::NodeOperationType type,
                                   const std::string& message,
                                   NodeOperationState* out,
                                   std::string* error) {
    ManagedNodeRuntime node;
    if (!nodes_ || !nodes_->GetNode(node_id, &node)) {
        if (error) *error = "node not found: " + node_id;
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    NodeOperationState op;
    op.operation_id = "op-" + std::to_string(next_operation_id_++);
    op.node_id = node_id;
    op.operation_type = type;
    op.status = zb::rpc::NODE_OP_RUNNING;
    op.message = message;
    op.start_ts_ms = NowMs();
    operations_[op.operation_id] = op;
    if (out) *out = op;
    return true;
}

bool ClusterState::UpdateOperation(const std::string& operation_id,
                                   zb::rpc::NodeOperationStatus status,
                                   const std::string& message,
                                   std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = operations_.find(operation_id);
    if (it == operations_.end()) {
        if (error) *error = "operation not found: " + operation_id;
        return false;
    }
    it->second.status = status;
    it->second.message = message;
    if (status == zb::rpc::NODE_OP_SUCCEEDED || status == zb::rpc::NODE_OP_FAILED) {
        it->second.finish_ts_ms = NowMs();
    }
    return true;
}

bool ClusterState::GetOperation(const std::string& operation_id, NodeOperationState* out) const {
    if (!out) return false;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = operations_.find(operation_id);
    if (it == operations_.end()) return false;
    *out = it->second;
    return true;
}

NodeState ClusterState::BuildNodeState(const ManagedNodeRuntime& node) {
    NodeState out;
    out.node_id = node.profile.node_id;
    out.node_type = LegacyNodeType(node);
    out.address = node.identity.service_address;
    out.weight = 1;
    out.virtual_node_count = node.profile.logical_node_count;
    out.group_id = out.node_id;
    out.role = zb::rpc::NODE_ROLE_PRIMARY;
    out.epoch = 1;
    out.sync_ready = true;
    out.health_state = LegacyHealth(node.health);
    out.admin_state = LegacyAdmin(node);
    out.desired_admin_state = out.admin_state;
    out.power_state = LegacyPower(node);
    out.desired_power_state = node.power_level == PowerLevel::kOff
                                  ? zb::rpc::NODE_POWER_OFF : zb::rpc::NODE_POWER_ON;
    out.last_heartbeat_ms = node.last_heartbeat_ms;
    out.resident_data_bytes = node.resident_data_bytes;
    out.optical_inventory_observed = node.optical_library.observed;
    out.optical_local_disc_count = node.optical_library.local_disc_count;
    for (const auto& device : node.inventory.devices) out.disks[device.disk_id] = device;
    return out;
}

HeartbeatAssignment ClusterState::BuildAssignment(const ManagedNodeRuntime& node,
                                                   uint64_t generation) {
    HeartbeatAssignment out;
    out.generation = generation;
    out.group_id = node.profile.node_id;
    out.assigned_role = zb::rpc::NODE_ROLE_PRIMARY;
    out.epoch = 1;
    out.primary_node_id = node.profile.node_id;
    out.primary_address = node.identity.service_address;
    return out;
}

uint64_t ClusterState::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::scheduler
