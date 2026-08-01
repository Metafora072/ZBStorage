#include "scheduler/model/ManagedNode.h"
#include "scheduler/power/NodePowerManager.h"

#include <cstdint>
#include <iostream>
#include <limits>
#include <string>

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        return false;
    }
    return true;
}

bool TestDefaultProfiles() {
    using namespace zb::scheduler;
    constexpr uint64_t kTB = 1000000000000ULL;
    constexpr uint64_t kMB = 1000000ULL;
    constexpr uint64_t kGB = 1000000000ULL;

    const ManagedNodeProfile storage = DefaultStorageNodeProfile("storage-1");
    if (!Expect(CalculateCapacityBytes(storage) == 432ULL * kTB, "storage capacity") ||
        !Expect(CalculateInternalBandwidthBytesPerSecond(storage, AccessPath::kDefault) == kGB,
                "storage internal bandwidth")) {
        return false;
    }
    PerformanceEstimate storage_estimate = EstimatePerformance(storage, kMB);
    if (!Expect(storage_estimate.valid, storage_estimate.error) ||
        !Expect(storage_estimate.effective_bandwidth_bytes_per_sec == kGB,
                "storage effective bandwidth") ||
        !Expect(storage_estimate.service_latency_us == 6000, "storage latency")) {
        return false;
    }
    const auto medium_estimate = EstimatePerformanceAtPower(
        storage, kMB, AccessPath::kDefault, zb::storage_model::IoDirection::kRead,
        PowerLevel::kMedium);
    if (!Expect(medium_estimate.valid, medium_estimate.error) ||
        !Expect(medium_estimate.internal_bandwidth_bytes_per_sec == 222222222ULL,
                "medium power linearly scales storage bandwidth")) {
        return false;
    }

    const ManagedNodeProfile metadata = DefaultMetadataNodeProfile("metadata-1");
    PerformanceEstimate metadata_estimate = EstimatePerformance(metadata, kMB);
    if (!Expect(CalculateCapacityBytes(metadata) == 64ULL * kTB, "metadata capacity") ||
        !Expect(metadata_estimate.valid, metadata_estimate.error) ||
        !Expect(metadata_estimate.effective_bandwidth_bytes_per_sec == 10ULL * kGB,
                "metadata network cap") ||
        !Expect(metadata_estimate.service_latency_us == 600, "metadata latency")) {
        return false;
    }

    ManagedNodeProfile optical = DefaultOpticalLibraryProfile("optical-1");
    PerformanceEstimate disc_estimate = EstimatePerformance(optical, kMB, AccessPath::kOpticalDisc);
    if (!Expect(CalculateCapacityBytes(optical) == 10280ULL * kTB, "optical total capacity") ||
        !Expect(disc_estimate.valid, disc_estimate.error) ||
        !Expect(disc_estimate.effective_bandwidth_bytes_per_sec == kGB,
                "optical drive aggregate bandwidth") ||
        !Expect(disc_estimate.service_latency_us == 60501000ULL, "optical miss latency")) {
        return false;
    }
    if (!Expect(CalculateInternalBandwidthBytesPerSecond(
                    optical, AccessPath::kOpticalDisc,
                    zb::storage_model::IoDirection::kWrite) == 100ULL * kMB,
                "optical write bandwidth is distinct from read bandwidth")) {
        return false;
    }
    PerformanceEstimate cache_estimate = EstimatePerformance(optical, kMB, AccessPath::kOpticalCacheHit);
    return Expect(cache_estimate.valid, cache_estimate.error) &&
           Expect(cache_estimate.service_latency_us == 1000, "optical cache latency");
}

bool TestMultipleDeviceGroups() {
    using namespace zb::scheduler;
    constexpr uint64_t kTB = 1000000000000ULL;
    ManagedNodeProfile profile = DefaultStorageNodeProfile("storage-mixed");
    profile.device_groups.push_back({"hdd-20tb", DeviceKind::kHdd, 20ULL * kTB, 20,
                                     100000000ULL, 2});
    std::string error;
    return Expect(ValidateManagedNodeProfile(profile, &error), error) &&
           Expect(CalculateCapacityBytes(profile) == 832ULL * kTB, "mixed device capacity") &&
           Expect(CalculateInternalBandwidthBytesPerSecond(profile, AccessPath::kDefault) == 1200000000ULL,
                  "mixed device bandwidth");
}

bool TestPowerAndLifecycle() {
    using namespace zb::scheduler;
    PowerPolicy policy;
    policy.peak_utilization = 0.8;
    policy.peak_queue_depth = 8;
    policy.metrics_freshness_ms = 50;
    policy.standby_after_ms = 100;
    policy.off_after_ms = 200;
    policy.minimum_residency_ms = 0;
    policy.allow_off_with_resident_data = false;
    NodePowerManager manager(policy);

    std::string error;
    ManagedNodeProfile profile = DefaultStorageNodeProfile("storage-power");
    if (!Expect(manager.RegisterNode(profile, 1000, 7, &error), error) ||
        !Expect(!manager.RegisterNode(profile, 1000, 7, &error), "duplicate registration") ||
        !Expect(manager.ActivateNode(profile.node_id, 1000, &error), error) ||
        !Expect(manager.SetResidentDataBytes(profile.node_id, 1234, &error), error)) {
        return false;
    }

    NodeAccessMetrics metrics;
    metrics.reporter_epoch = 77;
    metrics.report_sequence = 1;
    metrics.window_start_ms = 1000;
    metrics.window_end_ms = 1100;
    metrics.read_operations = 10;
    metrics.read_bytes = 90000000;
    metrics.max_queue_depth = 1;
    metrics.last_access_ms = 1100;
    if (!Expect(manager.ReportMetrics(profile.node_id, metrics, &error), error)) {
        return false;
    }
    if (!Expect(manager.ReportMetrics(profile.node_id, metrics, &error),
                "duplicate report is idempotent")) {
        return false;
    }

    ManagedNodeRuntime node;
    if (!Expect(manager.GetNode(profile.node_id, &node), "node lookup") ||
        !Expect(node.power_level == PowerLevel::kPeak, "high utilization enters PEAK") ||
        !Expect(node.access_operation_count == 10, "duplicate metrics are not counted twice")) {
        return false;
    }
    manager.Tick(1201);
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.power_level == PowerLevel::kStandby, "idle node enters STANDBY")) {
        return false;
    }
    manager.Tick(1400);
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.power_level == PowerLevel::kStandby,
                "resident data prevents automatic OFF")) {
        return false;
    }
    if (!Expect(manager.SetResidentDataBytes(profile.node_id, 0, &error), error)) {
        return false;
    }
    manager.Tick(1401);
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.power_level == PowerLevel::kOff, "drained idle node enters OFF") ||
        !Expect(node.energy_microjoules > 0, "energy is integrated")) {
        return false;
    }

    if (!Expect(manager.BeginRemoveNode(profile.node_id, 1500, &error), error) ||
        !Expect(manager.ReportDrainProgress(profile.node_id, 1501, 0, 0, 0, &error), error) ||
        !Expect(manager.ReportPowerActuation(profile.node_id, PowerLevel::kOff, 1501,
                                             true, "test confirmation", &error), error) ||
        !Expect(manager.FinalizeRemoveNode(profile.node_id, 1502, false, &error), error) ||
        !Expect(!manager.RegisterNode(profile, 1600, 7, &error), "retired tombstone blocks reuse")) {
        return false;
    }
    PowerAndEnergySummary summary = manager.Summary();
    return Expect(summary.total_nodes == 1, "summary total") &&
           Expect(summary.retired_nodes == 1, "summary retired") &&
           Expect(summary.off_nodes == 1, "summary off") &&
           Expect(summary.total_energy_microjoules > 0, "summary energy");
}

bool TestAccessEventAndSnapshot() {
    using namespace zb::scheduler;
    NodePowerManager manager;
    std::string error;
    ManagedNodeProfile profile = DefaultStorageNodeProfile("trace-node");
    profile.execution_mode = ExecutionMode::kSimulated;
    if (!Expect(manager.RegisterNode(profile, 1000, 9, &error), error) ||
        !Expect(manager.ActivateNode(profile.node_id, 1000, &error), error)) {
        return false;
    }
    NodeAccessEvent event;
    event.event_time_us = 1100000;
    event.is_read = true;
    event.bytes = 4096;
    event.queue_depth = 2;
    if (!Expect(manager.ReportAccessEvent(profile.node_id, event, &error), error)) {
        return false;
    }
    const NodePowerManagerSnapshot snapshot = manager.Snapshot();
    ManagedNodeRuntime before_tick;
    manager.GetNode(profile.node_id, &before_tick);
    manager.TickRealTime(5000);
    ManagedNodeRuntime after_real_tick;
    manager.GetNode(profile.node_id, &after_real_tick);
    if (!Expect(after_real_tick.last_energy_update_ms == before_tick.last_energy_update_ms,
                "wall clock tick skips simulated nodes")) {
        return false;
    }
    manager.AdvanceSimulatedTime(5000);
    NodePowerManager restored;
    ManagedNodeRuntime node;
    return Expect(restored.Restore(snapshot, &error), error) &&
           Expect(restored.GetNode(profile.node_id, &node), "restored trace node") &&
           Expect(node.access_operation_count == 1, "access event count restored") &&
           Expect(node.last_access_ms == 1100, "access event time restored");
}

bool TestFailureList() {
    using namespace zb::scheduler;
    NodePowerManager manager;
    std::string error;
    ManagedNodeProfile profile = DefaultMetadataNodeProfile("metadata-failed");
    if (!Expect(manager.RegisterNode(profile, 1, 11, &error), error) ||
        !Expect(manager.ActivateNode(profile.node_id, 2, &error), error) ||
        !Expect(manager.MarkNodeFailed(profile.node_id, 3, "injected", &error), error)) {
        return false;
    }
    const auto failed = manager.ListFailedNodes();
    return Expect(failed.size() == 1, "failed list size") &&
           Expect(failed.front().failure_reason == "injected", "failure reason") &&
           Expect(failed.front().power_level == PowerLevel::kOff, "failed node is off");
}

bool TestSafetyWatermarks() {
    using namespace zb::scheduler;
    PowerPolicy policy;
    policy.standby_after_ms = 10;
    policy.off_after_ms = 20;
    policy.minimum_residency_ms = 0;
    policy.min_online_storage_nodes = 1;
    policy.min_online_storage_capacity_bytes = CalculateCapacityBytes(DefaultStorageNodeProfile("x"));
    NodePowerManager manager(policy);
    std::string error;
    for (const std::string id : {"safe-a", "safe-b"}) {
        auto profile = DefaultStorageNodeProfile(id);
        profile.execution_mode = ExecutionMode::kVirtual;
        if (!Expect(manager.RegisterNode(profile, 1, 3, &error), error) ||
            !Expect(manager.ActivateNode(id, 1, &error), error) ||
            !Expect(manager.SetResidentDataBytes(id, 0, &error), error)) return false;
    }
    manager.Tick(100);
    const auto summary = manager.Summary();
    return Expect(summary.off_nodes == 1, "only one storage node may power off") &&
           Expect(summary.standby_nodes == 1, "minimum online node remains in standby");
}

bool TestReliabilityAndHistory() {
    using namespace zb::scheduler;
    PowerPolicy policy;
    policy.history_max_samples = 8;
    NodePowerManager manager(policy);
    auto profile = DefaultStorageNodeProfile("reliable-sim");
    profile.execution_mode = ExecutionMode::kSimulated;
    profile.reliability.mean_time_between_failures_ms = 1;
    profile.reliability.mean_repair_time_ms = 10;
    std::string error;
    if (!Expect(manager.RegisterNode(profile, 1, 17, &error), error) ||
        !Expect(manager.ActivateNode(profile.node_id, 1, &error), error)) return false;
    manager.AdvanceSimulatedTime(1000);
    ManagedNodeRuntime failed;
    manager.GetNode(profile.node_id, &failed);
    if (!Expect(failed.health == ManagedHealthState::kFailed, "configured MTBF injects failure") ||
        !Expect(failed.repair_due_time_ms == 1010, "configured MTTR schedules repair")) return false;
    manager.AdvanceSimulatedTime(1010);
    ManagedNodeRuntime repaired;
    manager.GetNode(profile.node_id, &repaired);
    const auto history = manager.MetricsHistory(0, 0, 10);
    if (!Expect(repaired.health == ManagedHealthState::kHealthy, "automatic repair") ||
        !Expect(history.size() == 2,
                "simulation ticks are observable as time-series samples")) {
        return false;
    }

    NodePowerManager bathtub_manager;
    auto bathtub = DefaultStorageNodeProfile("bathtub-sim");
    bathtub.execution_mode = ExecutionMode::kSimulated;
    bathtub.reliability.model = zb::storage_model::ReliabilityModel::kBathtubCurve;
    bathtub.reliability.early_failure_window_ms = 100;
    bathtub.reliability.wearout_start_age_ms = 1000;
    bathtub.reliability.early_failure_rate_fit = std::numeric_limits<uint64_t>::max();
    bathtub.reliability.useful_life_failure_rate_fit = 1;
    bathtub.reliability.wearout_failure_rate_fit = 2;
    if (!Expect(bathtub_manager.RegisterNode(bathtub, 1, 23, &error), error) ||
        !Expect(bathtub_manager.ActivateNode(bathtub.node_id, 1, &error), error)) {
        return false;
    }
    bathtub_manager.AdvanceSimulatedTime(10);
    ManagedNodeRuntime bathtub_failed;
    bathtub_manager.GetNode(bathtub.node_id, &bathtub_failed);
    return Expect(bathtub_failed.health == ManagedHealthState::kFailed,
                  "bathtub early-life FIT schedules a failure") &&
           Expect(bathtub_failed.failure_source == FailureSource::kReliabilityModel,
                  "bathtub failure source is retained");
}

bool TestPublicAttributeInterface() {
    using namespace zb::scheduler;
    if (!Expect(kManagedNodeAttributeModelVersion == 2, "attribute model version") ||
        !Expect(std::string(ExecutionModeName(ExecutionMode::kSimulated)) == "SIMULATED",
                "execution mode name") ||
        !Expect(std::string(DeviceKindName(DeviceKind::kOpticalDrive)) == "OPTICAL_DRIVE",
                "device kind name") ||
        !Expect(std::string(AccessPathName(AccessPath::kOpticalDisc)) == "OPTICAL_DISC",
                "access path name") ||
        !Expect(std::string(ManagedHealthStateName(ManagedHealthState::kSuspect)) == "SUSPECT",
                "health state name") ||
        !Expect(std::string(TransitionStateName(TransitionState::kRebooting)) == "REBOOTING",
                "transition state name") ||
        !Expect(std::string(NodeServiceModeName(NodeServiceMode::kReadOnly)) == "READ_ONLY",
                "service mode name") ||
        !Expect(std::string(FailureSourceName(FailureSource::kHeartbeatTimeout)) ==
                    "HEARTBEAT_TIMEOUT",
                "failure source name") ||
        !Expect(IsServingLifecycle(LifecycleState::kWorking), "working node serves") ||
        !Expect(IsServingLifecycle(LifecycleState::kDraining), "draining node serves") ||
        !Expect(IsTerminalLifecycle(LifecycleState::kRetired), "retired node is terminal")) {
        return false;
    }

    PowerPolicy invalid_policy;
    invalid_policy.standby_after_ms = 20;
    invalid_policy.off_after_ms = 10;
    std::string error;
    if (!Expect(!ValidatePowerPolicy(invalid_policy, &error),
                "public policy validation rejects inverted idle thresholds")) {
        return false;
    }

    NodePowerManager manager;
    for (const std::string id : {"storage-z", "storage-a"}) {
        auto profile = DefaultStorageNodeProfile(id);
        if (!Expect(manager.RegisterNode(profile, 1, 1, &error), error)) return false;
    }
    auto metadata = DefaultMetadataNodeProfile("metadata-m");
    if (!Expect(manager.RegisterNode(metadata, 1, 1, &error), error)) return false;

    const auto all = manager.ListNodes();
    ManagedNodeFilter storage_filter;
    storage_filter.kind = ManagedNodeKind::kStorage;
    const auto storage = manager.ListNodes(storage_filter);
    return Expect(all.size() == 3, "all-node query size") &&
           Expect(all[0].profile.node_id == "metadata-m" &&
                      all[1].profile.node_id == "storage-a" &&
                      all[2].profile.node_id == "storage-z",
                  "public queries are deterministically sorted") &&
           Expect(storage.size() == 2, "kind filter size") &&
           Expect(storage[0].profile.kind == ManagedNodeKind::kStorage &&
                      storage[1].profile.kind == ManagedNodeKind::kStorage,
                  "kind filter content");
}

bool TestOpticalRetirementAndReplacement() {
    using namespace zb::scheduler;
    NodePowerManager manager;
    std::string error;
    auto optical = DefaultOpticalLibraryProfile("optical-old");
    if (!Expect(manager.RegisterNode(optical, 1, 3, &error), error) ||
        !Expect(manager.ActivateNode(optical.node_id, 2, &error), error) ||
        !Expect(manager.BeginRemoveNode(optical.node_id, 3, &error), error) ||
        !Expect(manager.ReportDrainProgress(optical.node_id, 4, 0, 0, 0, &error), error) ||
        !Expect(!manager.FinalizeRemoveNode(optical.node_id, 5, false, &error),
                "unobserved optical inventory blocks retirement")) {
        return false;
    }
    OpticalLibraryStatus status;
    status.observed = true;
    status.total_slots = 10000;
    status.local_disc_count = 1;
    status.recorded_disc_count = 1;
    status.pending_write_tasks = 1;
    status.observed_at_ms = 6;
    if (!Expect(manager.ReportOpticalLibraryStatus(optical.node_id, status, &error), error)) {
        return false;
    }
    ManagedNodeRuntime old_node;
    manager.GetNode(optical.node_id, &old_node);
    if (!Expect(old_node.service_mode == NodeServiceMode::kReadWrite,
                "pending optical writes defer read-only mode")) {
        return false;
    }
    status.pending_write_tasks = 0;
    status.observed_at_ms = 7;
    if (!Expect(manager.ReportOpticalLibraryStatus(optical.node_id, status, &error), error)) {
        return false;
    }
    manager.GetNode(optical.node_id, &old_node);
    if (!Expect(old_node.service_mode == NodeServiceMode::kReadOnly,
                "all recorded optical media triggers read-only mode") ||
        !Expect(!manager.FinalizeRemoveNode(optical.node_id, 8, false, &error),
                "local optical media blocks retirement")) {
        return false;
    }
    status.local_disc_count = 0;
    status.recorded_disc_count = 0;
    status.observed_at_ms = 9;
    if (!Expect(manager.ReportOpticalLibraryStatus(optical.node_id, status, &error), error) ||
        !Expect(manager.ReportPowerActuation(optical.node_id, PowerLevel::kOff, 9,
                                             true, "test confirmation", &error), error) ||
        !Expect(manager.FinalizeRemoveNode(optical.node_id, 10, false, &error), error)) {
        return false;
    }
    auto replacement = DefaultOpticalLibraryProfile("optical-new");
    replacement.technology_generation = 2;
    if (!Expect(manager.RegisterReplacementNode(replacement, optical.node_id, 11, 4, &error), error)) {
        return false;
    }
    ManagedNodeRuntime new_node;
    manager.GetNode(optical.node_id, &old_node);
    manager.GetNode(replacement.node_id, &new_node);
    return Expect(old_node.replacement_count == 1, "retired predecessor tracks replacement") &&
           Expect(new_node.predecessor_node_id == optical.node_id,
                  "replacement tracks predecessor") &&
           Expect(new_node.profile.technology_generation == 2,
                  "replacement advances technology generation");
}

bool TestHeartbeatInventoryAndFailureDetection() {
    using namespace zb::scheduler;
    NodePowerManager manager;
    std::string error;
    auto profile = DefaultStorageNodeProfile("heartbeat-node");
    if (!Expect(manager.RegisterNode(profile, 1000, 8, &error), error)) return false;
    NodeInventorySnapshot inventory;
    inventory.node_id = profile.node_id;
    inventory.generation = 1;
    inventory.observed_at_ms = 1100;
    inventory.devices.push_back({"disk-0", DeviceKind::kHdd, 1000, 600, true,
                                 200000000, 150000000, 99, 1100});
    zb::storage_model::NodeReadinessStatus readiness;
    readiness.observed = true;
    readiness.initialization_complete = true;
    readiness.inventory_ready = true;
    readiness.metadata_ready = true;
    readiness.observed_at_ms = 1099;
    if (!Expect(manager.ReportReadiness(profile.node_id, readiness, &error), error) ||
        !Expect(manager.ActivateNode(profile.node_id, 1099, &error), error) ||
        !Expect(manager.ReportHeartbeat(profile.node_id, "node:9000", inventory, 1100, &error),
                error)) {
        return false;
    }
    ManagedNodeRuntime node;
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.lifecycle == LifecycleState::kWorking,
                "ready heartbeat node is working") ||
        !Expect(node.resident_data_bytes == 400, "inventory derives resident bytes") ||
        !Expect(node.accumulated_write_bytes == 99, "inventory derives write history")) {
        return false;
    }
    manager.EvaluateHeartbeatHealth(1160, 50, 200);
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.health == ManagedHealthState::kSuspect, "heartbeat timeout becomes suspect")) {
        return false;
    }
    inventory.generation = 2;
    inventory.observed_at_ms = 1170;
    if (!Expect(manager.ReportHeartbeat(profile.node_id, "node:9000", inventory, 1170, &error),
                error)) {
        return false;
    }
    manager.EvaluateHeartbeatHealth(1400, 50, 200);
    manager.GetNode(profile.node_id, &node);
    if (!Expect(node.health == ManagedHealthState::kFailed,
                "dead heartbeat timeout becomes failed") ||
        !Expect(node.failure_source == FailureSource::kHeartbeatTimeout,
                "heartbeat failure source is retained")) {
        return false;
    }
    inventory.generation = 3;
    inventory.observed_at_ms = 1410;
    return Expect(manager.ReportHeartbeat(profile.node_id, "node:9000", inventory, 1410, &error),
                  error) &&
           Expect(manager.GetNode(profile.node_id, &node), "heartbeat node lookup") &&
           Expect(node.health == ManagedHealthState::kHealthy,
                  "fresh heartbeat recovers timeout failure");
}

bool TestLongStepEnergyIntegration() {
    using namespace zb::scheduler;
    PowerPolicy policy;
    policy.metrics_freshness_ms = 50;
    policy.standby_after_ms = 100;
    policy.off_after_ms = 200;
    policy.minimum_residency_ms = 0;
    policy.allow_off_with_resident_data = true;
    NodePowerManager manager(policy);

    std::string error;
    auto profile = DefaultStorageNodeProfile("energy-node");
    if (!Expect(manager.RegisterNode(profile, 1000, 5, &error), error) ||
        !Expect(manager.ActivateNode(profile.node_id, 1000, &error), error)) {
        return false;
    }

    NodeAccessMetrics metrics;
    metrics.window_start_ms = 1000;
    metrics.window_end_ms = 1100;
    metrics.read_operations = 1;
    metrics.read_bytes = 900000000;
    metrics.last_access_ms = 1100;
    if (!Expect(manager.ReportMetrics(profile.node_id, metrics, &error), error)) return false;
    manager.Tick(1400);

    ManagedNodeRuntime node;
    if (!Expect(manager.GetNode(profile.node_id, &node), "energy node lookup")) return false;
    // 100 ms * 300 W + 50 ms * 1000 W + 50 ms * 300 W +
    // 100 ms * 100 W = 105,000,000 microjoules.
    return Expect(node.energy_microjoules == 105000000ULL,
                  "long ticks integrate each crossed power interval") &&
           Expect(node.power_level == PowerLevel::kOff, "long tick reaches OFF");
}

} // namespace

int main() {
    if (!TestDefaultProfiles() ||
        !TestMultipleDeviceGroups() ||
        !TestPowerAndLifecycle() ||
        !TestAccessEventAndSnapshot() ||
        !TestFailureList() ||
        !TestSafetyWatermarks() ||
        !TestReliabilityAndHistory() ||
        !TestPublicAttributeInterface() ||
        !TestOpticalRetirementAndReplacement() ||
        !TestHeartbeatInventoryAndFailureDetection() ||
        !TestLongStepEnergyIntegration()) {
        return 1;
    }
    std::cout << "PASS scheduler node model and power manager\n";
    return 0;
}
