#include "ManagedNodeProtoAdapter.h"

#include "../model/ClusterState.h"

#include <algorithm>

namespace zb::scheduler {

namespace {

ManagedNodeProfile DefaultProfile(const std::string& node_id, zb::rpc::ManagedNodeKind kind) {
    switch (kind) {
    case zb::rpc::MANAGED_KIND_METADATA:
        return DefaultMetadataNodeProfile(node_id);
    case zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY:
        return DefaultOpticalLibraryProfile(node_id);
    case zb::rpc::MANAGED_KIND_STORAGE:
    case zb::rpc::MANAGED_KIND_UNKNOWN:
        return DefaultStorageNodeProfile(node_id);
    }
    return DefaultStorageNodeProfile(node_id);
}

ManagedNodeKind NodeKindFromProto(zb::rpc::ManagedNodeKind kind) {
    switch (kind) {
    case zb::rpc::MANAGED_KIND_METADATA:
        return ManagedNodeKind::kMetadata;
    case zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY:
        return ManagedNodeKind::kOpticalLibrary;
    case zb::rpc::MANAGED_KIND_STORAGE:
    case zb::rpc::MANAGED_KIND_UNKNOWN:
        return ManagedNodeKind::kStorage;
    }
    return ManagedNodeKind::kStorage;
}

zb::rpc::ManagedNodeKind NodeKindToProto(ManagedNodeKind kind) {
    switch (kind) {
    case ManagedNodeKind::kStorage:
        return zb::rpc::MANAGED_KIND_STORAGE;
    case ManagedNodeKind::kMetadata:
        return zb::rpc::MANAGED_KIND_METADATA;
    case ManagedNodeKind::kOpticalLibrary:
        return zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY;
    }
    return zb::rpc::MANAGED_KIND_UNKNOWN;
}

ExecutionMode ExecutionModeFromProto(zb::rpc::ManagedExecutionMode mode) {
    switch (mode) {
    case zb::rpc::MANAGED_EXECUTION_VIRTUAL:
        return ExecutionMode::kVirtual;
    case zb::rpc::MANAGED_EXECUTION_SIMULATED:
        return ExecutionMode::kSimulated;
    case zb::rpc::MANAGED_EXECUTION_PHYSICAL:
    case zb::rpc::MANAGED_EXECUTION_UNKNOWN:
        return ExecutionMode::kPhysical;
    }
    return ExecutionMode::kPhysical;
}

zb::rpc::ManagedExecutionMode ExecutionModeToProto(ExecutionMode mode) {
    switch (mode) {
    case ExecutionMode::kPhysical:
        return zb::rpc::MANAGED_EXECUTION_PHYSICAL;
    case ExecutionMode::kVirtual:
        return zb::rpc::MANAGED_EXECUTION_VIRTUAL;
    case ExecutionMode::kSimulated:
        return zb::rpc::MANAGED_EXECUTION_SIMULATED;
    }
    return zb::rpc::MANAGED_EXECUTION_UNKNOWN;
}

DeviceKind DeviceKindFromProto(zb::rpc::ManagedDeviceKind kind) {
    switch (kind) {
    case zb::rpc::MANAGED_DEVICE_SSD:
        return DeviceKind::kSsd;
    case zb::rpc::MANAGED_DEVICE_DISC:
        return DeviceKind::kDisc;
    case zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE:
        return DeviceKind::kOpticalDrive;
    case zb::rpc::MANAGED_DEVICE_HDD:
    case zb::rpc::MANAGED_DEVICE_UNKNOWN:
        return DeviceKind::kHdd;
    }
    return DeviceKind::kHdd;
}

zb::rpc::ManagedDeviceKind DeviceKindToProto(DeviceKind kind) {
    switch (kind) {
    case DeviceKind::kHdd:
        return zb::rpc::MANAGED_DEVICE_HDD;
    case DeviceKind::kSsd:
        return zb::rpc::MANAGED_DEVICE_SSD;
    case DeviceKind::kDisc:
        return zb::rpc::MANAGED_DEVICE_DISC;
    case DeviceKind::kOpticalDrive:
        return zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE;
    }
    return zb::rpc::MANAGED_DEVICE_UNKNOWN;
}

zb::rpc::ManagedLifecycleState LifecycleToProto(LifecycleState state) {
    switch (state) {
    case LifecycleState::kJoining:
        return zb::rpc::MANAGED_LIFECYCLE_JOINING;
    case LifecycleState::kWorking:
        return zb::rpc::MANAGED_LIFECYCLE_WORKING;
    case LifecycleState::kDraining:
        return zb::rpc::MANAGED_LIFECYCLE_DRAINING;
    case LifecycleState::kExiting:
        return zb::rpc::MANAGED_LIFECYCLE_EXITING;
    case LifecycleState::kRetired:
        return zb::rpc::MANAGED_LIFECYCLE_RETIRED;
    }
    return zb::rpc::MANAGED_LIFECYCLE_UNKNOWN;
}

zb::rpc::ManagedHealthState HealthToProto(ManagedHealthState state) {
    switch (state) {
    case ManagedHealthState::kHealthy:
        return zb::rpc::MANAGED_HEALTH_HEALTHY;
    case ManagedHealthState::kSuspect:
        return zb::rpc::MANAGED_HEALTH_SUSPECT;
    case ManagedHealthState::kFailed:
        return zb::rpc::MANAGED_HEALTH_FAILED;
    }
    return zb::rpc::MANAGED_HEALTH_UNKNOWN;
}

zb::rpc::ManagedPowerLevel PowerToProto(PowerLevel level) {
    switch (level) {
    case PowerLevel::kOff:
        return zb::rpc::MANAGED_POWER_OFF;
    case PowerLevel::kStandby:
        return zb::rpc::MANAGED_POWER_STANDBY;
    case PowerLevel::kMedium:
        return zb::rpc::MANAGED_POWER_MEDIUM;
    case PowerLevel::kPeak:
        return zb::rpc::MANAGED_POWER_PEAK;
    }
    return zb::rpc::MANAGED_POWER_UNKNOWN;
}

zb::rpc::ManagedAdministrativeState AdministrativeStateToProto(
    zb::storage_model::AdministrativeState state) {
    switch (state) {
    case zb::storage_model::AdministrativeState::kEnabled:
        return zb::rpc::MANAGED_ADMIN_ENABLED;
    case zb::storage_model::AdministrativeState::kDraining:
        return zb::rpc::MANAGED_ADMIN_DRAINING;
    case zb::storage_model::AdministrativeState::kDisabled:
        return zb::rpc::MANAGED_ADMIN_DISABLED;
    }
    return zb::rpc::MANAGED_ADMIN_UNKNOWN;
}

zb::rpc::ManagedServiceMode ServiceModeToProto(NodeServiceMode mode) {
    return mode == NodeServiceMode::kReadOnly ? zb::rpc::MANAGED_SERVICE_READ_ONLY
                                              : zb::rpc::MANAGED_SERVICE_READ_WRITE;
}

zb::rpc::ManagedFailureSource FailureSourceToProto(FailureSource source) {
    switch (source) {
    case FailureSource::kNone: return zb::rpc::MANAGED_FAILURE_NONE;
    case FailureSource::kHeartbeatTimeout: return zb::rpc::MANAGED_FAILURE_HEARTBEAT_TIMEOUT;
    case FailureSource::kReliabilityModel: return zb::rpc::MANAGED_FAILURE_RELIABILITY_MODEL;
    case FailureSource::kInjected: return zb::rpc::MANAGED_FAILURE_INJECTED;
    }
    return zb::rpc::MANAGED_FAILURE_NONE;
}

void FillSpec(const ManagedNodeProfile& profile, zb::rpc::ManagedNodeSpec* output) {
    if (!output) {
        return;
    }
    output->set_node_id(profile.node_id);
    output->set_kind(NodeKindToProto(profile.kind));
    output->set_execution_mode(ExecutionModeToProto(profile.execution_mode));
    output->set_logical_node_count(profile.logical_node_count);
    output->set_technology_generation(profile.technology_generation);
    output->set_participates_in_placement(profile.participates_in_placement);
    output->set_participates_in_placement_set(true);
    output->set_external_network_bandwidth_bytes_per_sec(
        profile.external_network_bandwidth_bytes_per_sec);
    output->set_fixed_access_latency_us(profile.fixed_access_latency_us);
    output->set_optical_load_latency_us(profile.optical_load_latency_us);
    output->set_optical_seek_latency_us(profile.optical_seek_latency_us);
    for (const auto& group : profile.device_groups) {
        auto* out = output->add_device_groups();
        out->set_name(group.name);
        out->set_kind(DeviceKindToProto(group.kind));
        out->set_device_capacity_bytes(group.device_capacity_bytes);
        out->set_device_count(group.device_count);
        out->set_per_device_bandwidth_bytes_per_sec(group.per_device_bandwidth_bytes_per_sec);
        out->set_max_concurrency(group.max_concurrency);
        out->set_per_device_read_bandwidth_bytes_per_sec(
            group.per_device_read_bandwidth_bytes_per_sec);
        out->set_per_device_write_bandwidth_bytes_per_sec(
            group.per_device_write_bandwidth_bytes_per_sec);
    }
    auto* power = output->mutable_power();
    power->set_peak_milliwatts(profile.power.peak_milliwatts);
    power->set_medium_milliwatts(profile.power.medium_milliwatts);
    power->set_standby_milliwatts(profile.power.standby_milliwatts);
    power->set_off_milliwatts(profile.power.off_milliwatts);
    auto* reliability = output->mutable_reliability();
    reliability->set_mean_lifetime_ms(profile.reliability.mean_lifetime_ms);
    reliability->set_lifetime_stddev_ms(profile.reliability.lifetime_stddev_ms);
    reliability->set_minimum_lifetime_ms(profile.reliability.minimum_lifetime_ms);
    reliability->set_mean_time_between_failures_ms(profile.reliability.mean_time_between_failures_ms);
    reliability->set_mean_repair_time_ms(profile.reliability.mean_repair_time_ms);
    reliability->set_model(profile.reliability.model == zb::storage_model::ReliabilityModel::kBathtubCurve
                               ? zb::rpc::ManagedReliabilityProfile::BATHTUB_CURVE
                               : zb::rpc::ManagedReliabilityProfile::NORMAL_LIFETIME);
    reliability->set_early_failure_window_ms(profile.reliability.early_failure_window_ms);
    reliability->set_wearout_start_age_ms(profile.reliability.wearout_start_age_ms);
    reliability->set_early_failure_rate_fit(profile.reliability.early_failure_rate_fit);
    reliability->set_useful_life_failure_rate_fit(
        profile.reliability.useful_life_failure_rate_fit);
    reliability->set_wearout_failure_rate_fit(profile.reliability.wearout_failure_rate_fit);
}

} // namespace

ManagedNodeProfile DefaultProfileForLegacyNode(const std::string& node_id,
                                               zb::rpc::NodeType node_type,
                                               uint32_t logical_node_count) {
    ManagedNodeProfile profile;
    if (node_type == zb::rpc::NODE_METADATA) {
        profile = DefaultMetadataNodeProfile(node_id);
    } else if (node_type == zb::rpc::NODE_OPTICAL) {
        profile = DefaultOpticalLibraryProfile(node_id);
    } else {
        profile = DefaultStorageNodeProfile(node_id);
    }
    if (node_type == zb::rpc::NODE_VIRTUAL_POOL) {
        profile.execution_mode = ExecutionMode::kVirtual;
        profile.logical_node_count = std::max<uint32_t>(1, logical_node_count);
    }
    return profile;
}

bool ManagedNodeProfileFromProto(const zb::rpc::ManagedNodeSpec& input,
                                 ManagedNodeProfile* output,
                                 std::string* error) {
    if (!output || input.node_id().empty()) {
        if (error) {
            *error = "managed node spec requires node_id";
        }
        return false;
    }
    if (!zb::rpc::ManagedNodeKind_IsValid(input.kind()) ||
        input.kind() == zb::rpc::MANAGED_KIND_UNKNOWN) {
        if (error) {
            *error = "managed node kind must be a known non-UNKNOWN value";
        }
        return false;
    }
    if (!zb::rpc::ManagedExecutionMode_IsValid(input.execution_mode())) {
        if (error) *error = "invalid managed execution mode";
        return false;
    }

    ManagedNodeProfile profile = DefaultProfile(input.node_id(), input.kind());
    profile.kind = NodeKindFromProto(input.kind());
    profile.execution_mode = ExecutionModeFromProto(input.execution_mode());
    profile.logical_node_count = std::max<uint32_t>(1, input.logical_node_count());
    if (input.technology_generation() != 0) {
        profile.technology_generation = input.technology_generation();
    }
    if (input.participates_in_placement_set() || input.participates_in_placement()) {
        profile.participates_in_placement = input.participates_in_placement();
    }
    if (input.external_network_bandwidth_bytes_per_sec() != 0) {
        profile.external_network_bandwidth_bytes_per_sec =
            input.external_network_bandwidth_bytes_per_sec();
    }
    if (input.fixed_access_latency_us() != 0) {
        profile.fixed_access_latency_us = input.fixed_access_latency_us();
    }
    if (input.optical_load_latency_us() != 0) {
        profile.optical_load_latency_us = input.optical_load_latency_us();
    }
    if (input.optical_seek_latency_us() != 0) {
        profile.optical_seek_latency_us = input.optical_seek_latency_us();
    }
    if (input.device_groups_size() > 0) {
        profile.device_groups.clear();
        for (const auto& item : input.device_groups()) {
            if (!zb::rpc::ManagedDeviceKind_IsValid(item.kind())) {
                if (error) *error = "invalid managed device kind";
                return false;
            }
            profile.device_groups.push_back({item.name(),
                                             DeviceKindFromProto(item.kind()),
                                             item.device_capacity_bytes(),
                                             item.device_count(),
                                             item.per_device_bandwidth_bytes_per_sec(),
                                             item.max_concurrency(),
                                             item.per_device_read_bandwidth_bytes_per_sec(),
                                             item.per_device_write_bandwidth_bytes_per_sec()});
        }
    }
    if (input.has_power()) {
        profile.power = {input.power().peak_milliwatts(),
                         input.power().medium_milliwatts(),
                         input.power().standby_milliwatts(),
                         input.power().off_milliwatts()};
    }
    if (input.has_reliability()) {
        if (!zb::rpc::ManagedReliabilityProfile::Model_IsValid(input.reliability().model())) {
            if (error) *error = "invalid managed reliability model";
            return false;
        }
        profile.reliability.mean_lifetime_ms = input.reliability().mean_lifetime_ms();
        profile.reliability.lifetime_stddev_ms = input.reliability().lifetime_stddev_ms();
        profile.reliability.minimum_lifetime_ms = input.reliability().minimum_lifetime_ms();
        profile.reliability.mean_time_between_failures_ms =
            input.reliability().mean_time_between_failures_ms();
        profile.reliability.mean_repair_time_ms = input.reliability().mean_repair_time_ms();
        profile.reliability.model =
            input.reliability().model() == zb::rpc::ManagedReliabilityProfile::BATHTUB_CURVE
                ? zb::storage_model::ReliabilityModel::kBathtubCurve
                : zb::storage_model::ReliabilityModel::kNormalLifetime;
        profile.reliability.early_failure_window_ms =
            input.reliability().early_failure_window_ms();
        profile.reliability.wearout_start_age_ms = input.reliability().wearout_start_age_ms();
        profile.reliability.early_failure_rate_fit = input.reliability().early_failure_rate_fit();
        profile.reliability.useful_life_failure_rate_fit =
            input.reliability().useful_life_failure_rate_fit();
        profile.reliability.wearout_failure_rate_fit =
            input.reliability().wearout_failure_rate_fit();
    }
    if (!ValidateManagedNodeProfile(profile, error)) {
        return false;
    }
    *output = std::move(profile);
    return true;
}

AccessPath AccessPathFromProto(zb::rpc::ManagedAccessPath path) {
    switch (path) {
    case zb::rpc::MANAGED_ACCESS_OPTICAL_CACHE_HIT:
        return AccessPath::kOpticalCacheHit;
    case zb::rpc::MANAGED_ACCESS_OPTICAL_DISC:
        return AccessPath::kOpticalDisc;
    case zb::rpc::MANAGED_ACCESS_DEFAULT:
        return AccessPath::kDefault;
    }
    return AccessPath::kDefault;
}

bool ManagedNodeFilterFromProto(const zb::rpc::ListManagedNodesRequest& input,
                                ManagedNodeFilter* output,
                                std::string* error) {
    if (!output) {
        if (error) *error = "managed node filter output is null";
        return false;
    }
    ManagedNodeFilter filter;
    filter.include_retired = input.include_retired();
    if (!zb::rpc::ManagedNodeKind_IsValid(input.kind()) ||
        !zb::rpc::ManagedExecutionMode_IsValid(input.execution_mode())) {
        if (error) *error = "invalid node kind or execution mode filter";
        return false;
    }
    if (input.kind() != zb::rpc::MANAGED_KIND_UNKNOWN) {
        filter.kind = NodeKindFromProto(input.kind());
    }
    if (input.execution_mode() != zb::rpc::MANAGED_EXECUTION_UNKNOWN) {
        filter.execution_mode = ExecutionModeFromProto(input.execution_mode());
    }
    switch (input.lifecycle()) {
    case zb::rpc::MANAGED_LIFECYCLE_UNKNOWN: break;
    case zb::rpc::MANAGED_LIFECYCLE_JOINING: filter.lifecycle = LifecycleState::kJoining; break;
    case zb::rpc::MANAGED_LIFECYCLE_WORKING: filter.lifecycle = LifecycleState::kWorking; break;
    case zb::rpc::MANAGED_LIFECYCLE_DRAINING: filter.lifecycle = LifecycleState::kDraining; break;
    case zb::rpc::MANAGED_LIFECYCLE_EXITING: filter.lifecycle = LifecycleState::kExiting; break;
    case zb::rpc::MANAGED_LIFECYCLE_RETIRED: filter.lifecycle = LifecycleState::kRetired; break;
    default:
        if (error) *error = "invalid lifecycle filter";
        return false;
    }
    switch (input.health()) {
    case zb::rpc::MANAGED_HEALTH_UNKNOWN: break;
    case zb::rpc::MANAGED_HEALTH_HEALTHY: filter.health = ManagedHealthState::kHealthy; break;
    case zb::rpc::MANAGED_HEALTH_SUSPECT: filter.health = ManagedHealthState::kSuspect; break;
    case zb::rpc::MANAGED_HEALTH_FAILED: filter.health = ManagedHealthState::kFailed; break;
    default:
        if (error) *error = "invalid health filter";
        return false;
    }
    switch (input.power_level()) {
    case zb::rpc::MANAGED_POWER_UNKNOWN: break;
    case zb::rpc::MANAGED_POWER_OFF: filter.power_level = PowerLevel::kOff; break;
    case zb::rpc::MANAGED_POWER_STANDBY: filter.power_level = PowerLevel::kStandby; break;
    case zb::rpc::MANAGED_POWER_MEDIUM: filter.power_level = PowerLevel::kMedium; break;
    case zb::rpc::MANAGED_POWER_PEAK: filter.power_level = PowerLevel::kPeak; break;
    default:
        if (error) *error = "invalid power-level filter";
        return false;
    }
    *output = std::move(filter);
    return true;
}

bool OpticalLibraryStatusFromProto(const zb::rpc::ManagedOpticalLibraryStatus& input,
                                   OpticalLibraryStatus* output,
                                   std::string* error) {
    if (!output) {
        if (error) *error = "optical library status output is null";
        return false;
    }
    OpticalLibraryStatus status;
    status.observed = input.observed();
    status.total_slots = input.total_slots();
    status.local_disc_count = input.local_disc_count();
    status.blank_disc_count = input.blank_disc_count();
    status.recording_disc_count = input.recording_disc_count();
    status.recorded_disc_count = input.recorded_disc_count();
    status.defective_disc_count = input.defective_disc_count();
    status.staging_capacity_bytes = input.staging_capacity_bytes();
    status.staging_used_bytes = input.staging_used_bytes();
    status.pending_write_tasks = input.pending_write_tasks();
    status.observed_at_ms = input.observed_at_ms();
    if (!zb::storage_model::ValidateOpticalLibraryStatus(status, error)) return false;
    *output = status;
    return true;
}

void FillManagedNodeView(const ManagedNodeRuntime& node, zb::rpc::ManagedNodeView* output) {
    if (!output) {
        return;
    }
    FillSpec(node.profile, output->mutable_spec());
    output->set_lifecycle(LifecycleToProto(node.lifecycle));
    output->set_health(HealthToProto(node.health));
    output->set_power_level(PowerToProto(node.power_level));
    output->set_join_time_ms(node.join_time_ms);
    output->set_sampled_lifetime_ms(node.sampled_lifetime_ms);
    output->set_planned_exit_time_ms(node.planned_exit_time_ms);
    output->set_actual_exit_time_ms(node.actual_exit_time_ms);
    output->set_last_access_ms(node.last_access_ms);
    output->set_energy_microjoules(node.energy_microjoules);
    output->set_resident_data_bytes(node.resident_data_bytes);
    output->set_last_utilization(node.last_utilization);
    output->set_last_queue_depth(node.last_queue_depth);
    output->set_access_operation_count(node.access_operation_count);
    output->set_failure_reason(node.failure_reason);
    output->set_capacity_bytes(CalculateCapacityBytes(node.profile));
    output->set_drain_remaining_objects(node.drain_remaining_objects);
    output->set_drain_remaining_bytes(node.drain_remaining_bytes);
    output->set_drain_active_requests(node.drain_active_requests);
    output->set_drain_last_report_ms(node.drain_last_report_ms);
    output->set_actuated_power_level(PowerToProto(node.actuated_power_level));
    output->set_last_power_actuation_ms(node.last_power_actuation_ms);
    output->set_last_power_actuation_error(node.last_power_actuation_error);
    output->set_next_failure_time_ms(node.next_failure_time_ms);
    output->set_repair_due_time_ms(node.repair_due_time_ms);
    output->set_failure_count(node.failure_count);
    output->set_compact_id(node.identity.compact_id);
    output->set_service_address(node.identity.service_address);
    output->set_administrative_state(AdministrativeStateToProto(node.administrative_state));
    output->set_service_mode(ServiceModeToProto(node.service_mode));
    output->set_last_heartbeat_ms(node.last_heartbeat_ms);
    output->set_inventory_generation(node.inventory.generation);
    output->set_inventory_observed_at_ms(node.inventory.observed_at_ms);
    for (const auto& device : node.inventory.devices) {
        auto* out = output->add_inventory_devices();
        out->set_device_id(device.disk_id);
        out->set_kind(DeviceKindToProto(device.kind));
        out->set_capacity_bytes(device.capacity_bytes);
        out->set_free_bytes(device.free_bytes);
        out->set_used_bytes(zb::storage_model::DeviceUsedBytes(device));
        out->set_is_healthy(device.is_healthy);
        out->set_read_bandwidth_bytes_per_sec(device.read_bandwidth_bytes_per_sec);
        out->set_write_bandwidth_bytes_per_sec(device.write_bandwidth_bytes_per_sec);
        out->set_accumulated_write_bytes(device.accumulated_write_bytes);
        out->set_last_update_ms(device.last_update_ms);
    }
    output->set_accumulated_write_bytes(node.accumulated_write_bytes);
    output->set_failure_source(FailureSourceToProto(node.failure_source));
    auto* optical = output->mutable_optical_library();
    optical->set_observed(node.optical_library.observed);
    optical->set_total_slots(node.optical_library.total_slots);
    optical->set_local_disc_count(node.optical_library.local_disc_count);
    optical->set_blank_disc_count(node.optical_library.blank_disc_count);
    optical->set_recording_disc_count(node.optical_library.recording_disc_count);
    optical->set_recorded_disc_count(node.optical_library.recorded_disc_count);
    optical->set_defective_disc_count(node.optical_library.defective_disc_count);
    optical->set_staging_capacity_bytes(node.optical_library.staging_capacity_bytes);
    optical->set_staging_used_bytes(node.optical_library.staging_used_bytes);
    optical->set_pending_write_tasks(node.optical_library.pending_write_tasks);
    optical->set_observed_at_ms(node.optical_library.observed_at_ms);
    output->set_wake_count(node.wake_count);
    output->set_standby_transition_count(node.standby_transition_count);
    output->set_power_off_count(node.power_off_count);
    output->set_replacement_count(node.replacement_count);
    output->set_predecessor_node_id(node.predecessor_node_id);
    output->set_observed_capacity_bytes(
        zb::storage_model::InventoryCapacityBytes(node.inventory));
    output->set_observed_free_bytes(
        zb::storage_model::InventoryFreeBytes(node.inventory));
    output->set_observed_used_bytes(
        zb::storage_model::InventoryUsedBytes(node.inventory));
    output->set_actuated_energy_microjoules(node.actuated_energy_microjoules);
    output->set_last_actuated_energy_update_ms(node.last_actuated_energy_update_ms);
    auto* readiness = output->mutable_readiness();
    readiness->set_observed(node.readiness.observed);
    readiness->set_initialization_complete(node.readiness.initialization_complete);
    readiness->set_inventory_ready(node.readiness.inventory_ready);
    readiness->set_metadata_ready(node.readiness.metadata_ready);
    readiness->set_observed_at_ms(node.readiness.observed_at_ms);
    readiness->set_message(node.readiness.message);
}

void FillLegacyNodeView(const NodeState& node, zb::rpc::NodeView* out) {
    if (!out) return;
    out->set_node_id(node.node_id);
    out->set_node_type(node.node_type);
    out->set_address(node.address);
    out->set_weight(node.weight);
    out->set_virtual_node_count(node.virtual_node_count);
    out->set_health_state(node.health_state);
    out->set_admin_state(node.admin_state);
    out->set_power_state(node.power_state);
    out->set_desired_admin_state(node.desired_admin_state);
    out->set_desired_power_state(node.desired_power_state);
    out->set_last_heartbeat_ms(node.last_heartbeat_ms);
    out->set_group_id(node.group_id);
    out->set_role(node.role);
    out->set_epoch(node.epoch);
    out->set_applied_lsn(node.applied_lsn);
    out->set_peer_node_id(node.peer_node_id);
    out->set_peer_address(node.peer_address);
    out->set_sync_ready(node.sync_ready);
    for (const auto& item : node.disks) {
        const DiskState& disk = item.second;
        auto* device = out->add_disks();
        device->set_disk_id(disk.disk_id);
        device->set_capacity_bytes(disk.capacity_bytes);
        device->set_free_bytes(disk.free_bytes);
        device->set_used_bytes(zb::storage_model::DeviceUsedBytes(disk));
        device->set_is_healthy(disk.is_healthy);
        device->set_last_update_ms(disk.last_update_ms);
        switch (disk.kind) {
        case DeviceKind::kHdd: device->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD); break;
        case DeviceKind::kSsd: device->set_device_kind(zb::rpc::MANAGED_DEVICE_SSD); break;
        case DeviceKind::kDisc: device->set_device_kind(zb::rpc::MANAGED_DEVICE_DISC); break;
        case DeviceKind::kOpticalDrive:
            device->set_device_kind(zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE);
            break;
        }
        device->set_read_bandwidth_bytes_per_sec(disk.read_bandwidth_bytes_per_sec);
        device->set_write_bandwidth_bytes_per_sec(disk.write_bandwidth_bytes_per_sec);
        device->set_accumulated_write_bytes(disk.accumulated_write_bytes);
    }
}

void FillManagedSummary(const PowerAndEnergySummary& summary,
                        zb::rpc::ManagedPowerAndEnergySummary* output) {
    if (!output) {
        return;
    }
    output->set_generation(summary.generation);
    output->set_total_nodes(summary.total_nodes);
    output->set_joining_nodes(summary.joining_nodes);
    output->set_working_nodes(summary.working_nodes);
    output->set_draining_nodes(summary.draining_nodes);
    output->set_failed_nodes(summary.failed_nodes);
    output->set_retired_nodes(summary.retired_nodes);
    output->set_peak_nodes(summary.peak_nodes);
    output->set_medium_nodes(summary.medium_nodes);
    output->set_standby_nodes(summary.standby_nodes);
    output->set_off_nodes(summary.off_nodes);
    output->set_total_power_milliwatts(summary.total_power_milliwatts);
    output->set_total_energy_microjoules(summary.total_energy_microjoules);
    output->set_storage_nodes(summary.storage_nodes);
    output->set_metadata_nodes(summary.metadata_nodes);
    output->set_optical_library_nodes(summary.optical_library_nodes);
    output->set_total_capacity_bytes(summary.total_capacity_bytes);
    output->set_total_free_bytes(summary.total_free_bytes);
    output->set_total_used_bytes(summary.total_used_bytes);
    output->set_available_capacity_bytes(summary.available_capacity_bytes);
    output->set_unavailable_capacity_bytes(summary.unavailable_capacity_bytes);
    output->set_accumulated_write_bytes(summary.accumulated_write_bytes);
    output->set_wake_count(summary.wake_count);
    output->set_standby_transition_count(summary.standby_transition_count);
    output->set_power_off_count(summary.power_off_count);
    output->set_replacement_count(summary.replacement_count);
    output->set_total_actuated_power_milliwatts(
        summary.total_actuated_power_milliwatts);
    output->set_total_actuated_energy_microjoules(
        summary.total_actuated_energy_microjoules);
}

} // namespace zb::scheduler
