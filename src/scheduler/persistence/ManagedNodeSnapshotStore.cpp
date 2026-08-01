#include "ManagedNodeSnapshotStore.h"

#include <filesystem>
#include <fstream>
#include <functional>
#include <utility>

#include "../service/ManagedNodeProtoAdapter.h"
#include "scheduler.pb.h"

namespace zb::scheduler {

namespace {

bool LifecycleFromProto(zb::rpc::ManagedLifecycleState input, LifecycleState* output) {
    if (!output) {
        return false;
    }
    switch (input) {
    case zb::rpc::MANAGED_LIFECYCLE_JOINING: *output = LifecycleState::kJoining; return true;
    case zb::rpc::MANAGED_LIFECYCLE_WORKING: *output = LifecycleState::kWorking; return true;
    case zb::rpc::MANAGED_LIFECYCLE_DRAINING: *output = LifecycleState::kDraining; return true;
    case zb::rpc::MANAGED_LIFECYCLE_EXITING: *output = LifecycleState::kExiting; return true;
    case zb::rpc::MANAGED_LIFECYCLE_RETIRED: *output = LifecycleState::kRetired; return true;
    case zb::rpc::MANAGED_LIFECYCLE_UNKNOWN: return false;
    }
    return false;
}

bool HealthFromProto(zb::rpc::ManagedHealthState input, ManagedHealthState* output) {
    if (!output) {
        return false;
    }
    switch (input) {
    case zb::rpc::MANAGED_HEALTH_HEALTHY: *output = ManagedHealthState::kHealthy; return true;
    case zb::rpc::MANAGED_HEALTH_SUSPECT: *output = ManagedHealthState::kSuspect; return true;
    case zb::rpc::MANAGED_HEALTH_FAILED: *output = ManagedHealthState::kFailed; return true;
    case zb::rpc::MANAGED_HEALTH_UNKNOWN: return false;
    }
    return false;
}

bool PowerFromProto(zb::rpc::ManagedPowerLevel input, PowerLevel* output) {
    if (!output) {
        return false;
    }
    switch (input) {
    case zb::rpc::MANAGED_POWER_OFF: *output = PowerLevel::kOff; return true;
    case zb::rpc::MANAGED_POWER_STANDBY: *output = PowerLevel::kStandby; return true;
    case zb::rpc::MANAGED_POWER_MEDIUM: *output = PowerLevel::kMedium; return true;
    case zb::rpc::MANAGED_POWER_PEAK: *output = PowerLevel::kPeak; return true;
    case zb::rpc::MANAGED_POWER_UNKNOWN: return false;
    }
    return false;
}

DeviceKind DeviceKindFromProto(zb::rpc::ManagedDeviceKind input) {
    switch (input) {
    case zb::rpc::MANAGED_DEVICE_SSD: return DeviceKind::kSsd;
    case zb::rpc::MANAGED_DEVICE_DISC: return DeviceKind::kDisc;
    case zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE: return DeviceKind::kOpticalDrive;
    case zb::rpc::MANAGED_DEVICE_HDD:
    case zb::rpc::MANAGED_DEVICE_UNKNOWN: return DeviceKind::kHdd;
    }
    return DeviceKind::kHdd;
}

PowerAndEnergySummary SummaryFromProto(
    const zb::rpc::ManagedPowerAndEnergySummary& input) {
    PowerAndEnergySummary out;
    out.generation = input.generation();
    out.total_nodes = input.total_nodes();
    out.joining_nodes = input.joining_nodes();
    out.working_nodes = input.working_nodes();
    out.draining_nodes = input.draining_nodes();
    out.failed_nodes = input.failed_nodes();
    out.retired_nodes = input.retired_nodes();
    out.storage_nodes = input.storage_nodes();
    out.metadata_nodes = input.metadata_nodes();
    out.optical_library_nodes = input.optical_library_nodes();
    out.peak_nodes = input.peak_nodes();
    out.medium_nodes = input.medium_nodes();
    out.standby_nodes = input.standby_nodes();
    out.off_nodes = input.off_nodes();
    out.total_power_milliwatts = input.total_power_milliwatts();
    out.total_energy_microjoules = input.total_energy_microjoules();
    out.total_capacity_bytes = input.total_capacity_bytes();
    out.total_free_bytes = input.total_free_bytes();
    out.total_used_bytes = input.total_used_bytes();
    out.available_capacity_bytes = input.available_capacity_bytes();
    out.unavailable_capacity_bytes = input.unavailable_capacity_bytes();
    out.accumulated_write_bytes = input.accumulated_write_bytes();
    out.wake_count = input.wake_count();
    out.standby_transition_count = input.standby_transition_count();
    out.power_off_count = input.power_off_count();
    out.replacement_count = input.replacement_count();
    out.total_actuated_power_milliwatts = input.total_actuated_power_milliwatts();
    out.total_actuated_energy_microjoules = input.total_actuated_energy_microjoules();
    return out;
}

bool RuntimeFromProto(const zb::rpc::ManagedNodeRuntimeSnapshot& input,
                      uint32_t snapshot_format_version,
                      uint64_t restore_time_ms,
                      ManagedNodeRuntime* output,
                      std::string* error) {
    if (!output || !input.has_node() || !input.node().has_spec()) {
        if (error) {
            *error = "managed snapshot node is missing its spec";
        }
        return false;
    }
    ManagedNodeRuntime node;
    zb::rpc::ManagedNodeSpec persisted_spec = input.node().spec();
    if (snapshot_format_version == 1 &&
        !persisted_spec.participates_in_placement_set()) {
        // Version 1 had no scalar presence marker; an omitted false value was
        // the authoritative persisted value.
        persisted_spec.set_participates_in_placement_set(true);
    }
    if (!ManagedNodeProfileFromProto(persisted_spec, &node.profile, error) ||
        !LifecycleFromProto(input.node().lifecycle(), &node.lifecycle) ||
        !HealthFromProto(input.node().health(), &node.health) ||
        !PowerFromProto(input.node().power_level(), &node.power_level)) {
        if (error && error->empty()) {
            *error = "managed snapshot contains an invalid state enum";
        }
        return false;
    }
    if (input.transition_state() > static_cast<uint32_t>(TransitionState::kRebooting)) {
        if (error) {
            *error = "managed snapshot contains an invalid transition state";
        }
        return false;
    }
    const auto& view = input.node();
    node.transition = static_cast<TransitionState>(input.transition_state());
    node.join_time_ms = view.join_time_ms();
    node.sampled_lifetime_ms = view.sampled_lifetime_ms();
    node.planned_exit_time_ms = view.planned_exit_time_ms();
    node.actual_exit_time_ms = view.actual_exit_time_ms();
    node.last_access_ms = view.last_access_ms();
    node.last_metrics_end_ms = input.last_metrics_end_ms();
    node.last_metrics_sequence = input.last_metrics_sequence();
    node.last_metrics_reporter_epoch = input.last_metrics_reporter_epoch();
    node.power_state_enter_ms = input.power_state_enter_ms();
    node.last_energy_update_ms = node.profile.execution_mode == ExecutionMode::kSimulated
                                     ? input.last_energy_update_ms()
                                     : restore_time_ms;
    node.energy_microjoules = view.energy_microjoules();
    node.actuated_energy_microjoules = view.actuated_energy_microjoules();
    node.last_actuated_energy_update_ms =
        node.profile.execution_mode == ExecutionMode::kSimulated
            ? (input.last_actuated_energy_update_ms() == 0
                   ? input.last_energy_update_ms()
                   : input.last_actuated_energy_update_ms())
            : restore_time_ms;
    node.resident_data_bytes = view.resident_data_bytes();
    node.last_utilization = view.last_utilization();
    node.last_queue_depth = view.last_queue_depth();
    node.access_operation_count = view.access_operation_count();
    node.drain_remaining_objects = view.drain_remaining_objects();
    node.drain_remaining_bytes = view.drain_remaining_bytes();
    node.drain_active_requests = view.drain_active_requests();
    node.drain_last_report_ms = view.drain_last_report_ms();
    node.failure_reason = view.failure_reason();
    if (!PowerFromProto(view.actuated_power_level(), &node.actuated_power_level)) {
        // Backward compatibility with snapshots written before physical power
        // actuation state was persisted.
        node.actuated_power_level = node.profile.execution_mode == ExecutionMode::kPhysical
                                        ? PowerLevel::kOff : node.power_level;
    }
    node.last_power_actuation_ms = view.last_power_actuation_ms();
    node.last_power_actuation_error = view.last_power_actuation_error();
    node.next_failure_time_ms = view.next_failure_time_ms();
    node.repair_due_time_ms = view.repair_due_time_ms();
    node.failure_count = view.failure_count();
    node.identity.node_id = node.profile.node_id;
    node.identity.compact_id = view.compact_id();
    node.identity.kind = node.profile.kind;
    node.identity.execution_mode = node.profile.execution_mode;
    node.identity.service_address = view.service_address();
    switch (view.administrative_state()) {
    case zb::rpc::MANAGED_ADMIN_ENABLED:
        node.administrative_state = zb::storage_model::AdministrativeState::kEnabled;
        break;
    case zb::rpc::MANAGED_ADMIN_DRAINING:
        node.administrative_state = zb::storage_model::AdministrativeState::kDraining;
        break;
    case zb::rpc::MANAGED_ADMIN_DISABLED:
        node.administrative_state = zb::storage_model::AdministrativeState::kDisabled;
        break;
    case zb::rpc::MANAGED_ADMIN_UNKNOWN:
        node.administrative_state = node.lifecycle == LifecycleState::kRetired
                                        ? zb::storage_model::AdministrativeState::kDisabled
                                        : (node.lifecycle == LifecycleState::kDraining ||
                                           node.lifecycle == LifecycleState::kExiting
                                               ? zb::storage_model::AdministrativeState::kDraining
                                               : zb::storage_model::AdministrativeState::kEnabled);
        break;
    }
    node.service_mode = view.service_mode() == zb::rpc::MANAGED_SERVICE_READ_ONLY
                            ? NodeServiceMode::kReadOnly : NodeServiceMode::kReadWrite;
    node.last_heartbeat_ms = view.last_heartbeat_ms();
    node.inventory.node_id = node.profile.node_id;
    node.inventory.generation = view.inventory_generation();
    node.inventory.observed_at_ms = view.inventory_observed_at_ms();
    for (const auto& item : view.inventory_devices()) {
        DeviceInventory device;
        device.disk_id = item.device_id();
        device.kind = DeviceKindFromProto(item.kind());
        device.capacity_bytes = item.capacity_bytes();
        device.free_bytes = item.free_bytes();
        device.is_healthy = item.is_healthy();
        device.read_bandwidth_bytes_per_sec = item.read_bandwidth_bytes_per_sec();
        device.write_bandwidth_bytes_per_sec = item.write_bandwidth_bytes_per_sec();
        device.accumulated_write_bytes = item.accumulated_write_bytes();
        device.last_update_ms = item.last_update_ms();
        node.inventory.devices.push_back(std::move(device));
    }
    node.accumulated_write_bytes = view.accumulated_write_bytes();
    switch (view.failure_source()) {
    case zb::rpc::MANAGED_FAILURE_HEARTBEAT_TIMEOUT:
        node.failure_source = FailureSource::kHeartbeatTimeout;
        break;
    case zb::rpc::MANAGED_FAILURE_RELIABILITY_MODEL:
        node.failure_source = FailureSource::kReliabilityModel;
        break;
    case zb::rpc::MANAGED_FAILURE_INJECTED:
        node.failure_source = FailureSource::kInjected;
        break;
    case zb::rpc::MANAGED_FAILURE_NONE:
        node.failure_source = FailureSource::kNone;
        break;
    }
    if (view.has_optical_library()) {
        const auto& optical = view.optical_library();
        node.optical_library.observed = optical.observed();
        node.optical_library.total_slots = optical.total_slots();
        node.optical_library.local_disc_count = optical.local_disc_count();
        node.optical_library.blank_disc_count = optical.blank_disc_count();
        node.optical_library.recording_disc_count = optical.recording_disc_count();
        node.optical_library.recorded_disc_count = optical.recorded_disc_count();
        node.optical_library.defective_disc_count = optical.defective_disc_count();
        node.optical_library.staging_capacity_bytes = optical.staging_capacity_bytes();
        node.optical_library.staging_used_bytes = optical.staging_used_bytes();
        node.optical_library.pending_write_tasks = optical.pending_write_tasks();
        node.optical_library.observed_at_ms = optical.observed_at_ms();
    }
    if (view.has_readiness()) {
        node.readiness.observed = view.readiness().observed();
        node.readiness.initialization_complete = view.readiness().initialization_complete();
        node.readiness.inventory_ready = view.readiness().inventory_ready();
        node.readiness.metadata_ready = view.readiness().metadata_ready();
        node.readiness.observed_at_ms = view.readiness().observed_at_ms();
        node.readiness.message = view.readiness().message();
    } else if (node.lifecycle != LifecycleState::kJoining) {
        node.readiness.observed = true;
        node.readiness.initialization_complete = true;
        node.readiness.inventory_ready = true;
        node.readiness.metadata_ready = true;
        node.readiness.observed_at_ms = node.join_time_ms;
        node.readiness.message = "restored from pre-readiness snapshot";
    }
    node.wake_count = view.wake_count();
    node.standby_transition_count = view.standby_transition_count();
    node.power_off_count = view.power_off_count();
    node.replacement_count = view.replacement_count();
    node.predecessor_node_id = view.predecessor_node_id();
    node.reliability_seed = input.reliability_seed() == 0
                                ? std::hash<std::string>{}(node.profile.node_id)
                                : input.reliability_seed();
    *output = std::move(node);
    return true;
}

} // namespace

ManagedNodeSnapshotStore::ManagedNodeSnapshotStore(std::string path) : path_(std::move(path)) {}

bool ManagedNodeSnapshotStore::Save(const NodePowerManager& manager,
                                    uint64_t now_ms,
                                    std::string* error) const {
    if (path_.empty()) {
        return true;
    }
    const NodePowerManagerSnapshot snapshot = manager.Snapshot();
    zb::rpc::ManagedNodeCatalogSnapshot encoded;
    encoded.set_format_version(snapshot.format_version);
    encoded.set_generation(snapshot.generation);
    encoded.set_saved_at_ms(now_ms);
    for (const auto& node : snapshot.nodes) {
        auto* out = encoded.add_nodes();
        FillManagedNodeView(node, out->mutable_node());
        out->set_last_metrics_end_ms(node.last_metrics_end_ms);
        out->set_power_state_enter_ms(node.power_state_enter_ms);
        out->set_last_energy_update_ms(node.last_energy_update_ms);
        out->set_transition_state(static_cast<uint32_t>(node.transition));
        out->set_last_metrics_sequence(node.last_metrics_sequence);
        out->set_last_metrics_reporter_epoch(node.last_metrics_reporter_epoch);
        out->set_reliability_seed(node.reliability_seed);
        out->set_last_actuated_energy_update_ms(node.last_actuated_energy_update_ms);
    }
    for (const auto& sample : snapshot.history) {
        auto* out = encoded.add_history();
        out->set_timestamp_ms(sample.timestamp_ms);
        FillManagedSummary(sample.summary, out->mutable_summary());
    }

    std::error_code ec;
    const std::filesystem::path target(path_);
    if (!target.parent_path().empty()) {
        std::filesystem::create_directories(target.parent_path(), ec);
        if (ec) {
            if (error) {
                *error = "failed to create managed snapshot directory: " + ec.message();
            }
            return false;
        }
    }
    const std::filesystem::path temporary = target.string() + ".tmp";
    {
        std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
        if (!output || !encoded.SerializeToOstream(&output)) {
            if (error) {
                *error = "failed to write managed snapshot: " + temporary.string();
            }
            return false;
        }
        output.flush();
        if (!output) {
            if (error) {
                *error = "failed to flush managed snapshot: " + temporary.string();
            }
            return false;
        }
    }
    std::filesystem::rename(temporary, target, ec);
    if (ec) {
        if (error) {
            *error = "failed to install managed snapshot: " + ec.message();
        }
        return false;
    }
    return true;
}

bool ManagedNodeSnapshotStore::Load(NodePowerManager* manager,
                                    uint64_t restore_time_ms,
                                    bool* loaded,
                                    std::string* error) const {
    if (loaded) {
        *loaded = false;
    }
    if (path_.empty() || !manager) {
        return path_.empty();
    }
    if (!std::filesystem::exists(path_)) {
        return true;
    }
    std::ifstream input(path_, std::ios::binary);
    zb::rpc::ManagedNodeCatalogSnapshot encoded;
    if (!input || !encoded.ParseFromIstream(&input)) {
        if (error) {
            *error = "failed to parse managed snapshot: " + path_;
        }
        return false;
    }
    NodePowerManagerSnapshot snapshot;
    snapshot.format_version = encoded.format_version();
    snapshot.generation = encoded.generation();
    snapshot.nodes.reserve(static_cast<size_t>(encoded.nodes_size()));
    for (const auto& item : encoded.nodes()) {
        ManagedNodeRuntime node;
        if (!RuntimeFromProto(item, encoded.format_version(), restore_time_ms, &node, error)) {
            return false;
        }
        snapshot.nodes.push_back(std::move(node));
    }
    snapshot.history.reserve(static_cast<size_t>(encoded.history_size()));
    for (const auto& item : encoded.history()) {
        snapshot.history.push_back({item.timestamp_ms(), SummaryFromProto(item.summary())});
    }
    if (!manager->Restore(snapshot, error)) {
        return false;
    }
    if (loaded) {
        *loaded = true;
    }
    return true;
}

} // namespace zb::scheduler
