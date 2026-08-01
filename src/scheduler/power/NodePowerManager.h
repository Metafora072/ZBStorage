#pragma once

#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "../model/ManagedNode.h"

namespace zb::scheduler {

inline constexpr uint32_t kNodePowerManagerSnapshotFormatVersion =
    zb::storage_model::kNodeCatalogSnapshotFormatVersion;

struct NodeAccessMetrics {
    // All timestamps and durations are milliseconds. Byte counters are decimal
    // bytes; report_sequence is monotonic within one reporter_epoch.
    uint64_t reporter_epoch{0};
    uint64_t report_sequence{0};
    uint64_t window_start_ms{0};
    uint64_t window_end_ms{0};
    uint64_t read_operations{0};
    uint64_t write_operations{0};
    uint64_t other_operations{0};
    uint64_t read_bytes{0};
    uint64_t write_bytes{0};
    uint32_t active_requests{0};
    uint32_t max_queue_depth{0};
    uint64_t busy_time_ms{0};
    uint64_t last_access_ms{0};
    AccessPath access_path{AccessPath::kDefault};
};

struct NodeAccessEvent {
    // Trace events use microseconds to preserve sub-millisecond ordering.
    uint64_t event_time_us{0};
    bool is_read{false};
    bool is_write{false};
    uint64_t bytes{0};
    uint32_t queue_depth{0};
    AccessPath access_path{AccessPath::kDefault};
};

struct PowerPolicy {
    // Utilization is a [0, 1] ratio. Time thresholds are milliseconds;
    // capacity watermarks are decimal bytes.
    double peak_utilization{0.80};
    uint32_t peak_queue_depth{8};
    uint64_t metrics_freshness_ms{5000};
    uint64_t standby_after_ms{60000};
    uint64_t off_after_ms{600000};
    uint64_t minimum_residency_ms{1000};
    bool allow_off_with_resident_data{false};
    uint32_t min_online_storage_nodes{0};
    uint32_t min_online_metadata_nodes{0};
    uint32_t min_online_optical_nodes{0};
    uint64_t min_online_storage_capacity_bytes{0};
    uint64_t min_online_storage_free_bytes{0};
    uint32_t history_max_samples{100000};
};

// Scheduler keeps the historical name while using the shared node record as
// the single definition for runtime, power, drain and reliability fields.
using ManagedNodeRuntime = zb::storage_model::NodeRecord;

// Optional fields are combined with AND. By default retired tombstones are
// included, matching ListNodes() and snapshot semantics.
struct ManagedNodeFilter {
    std::optional<ManagedNodeKind> kind;
    std::optional<ExecutionMode> execution_mode;
    std::optional<LifecycleState> lifecycle;
    std::optional<ManagedHealthState> health;
    std::optional<PowerLevel> power_level;
    bool include_retired{true};
};

using NodePowerManagerSnapshot = zb::storage_model::NodeCatalogSnapshot;
using PowerAndEnergySummary = zb::storage_model::NodePowerSummary;
using ManagedMetricsSample = zb::storage_model::NodePowerSample;

class NodePowerManager {
public:
    explicit NodePowerManager(PowerPolicy policy = {});

    bool RegisterNode(const ManagedNodeProfile& profile,
                      uint64_t join_time_ms,
                      uint64_t lifetime_seed,
                      std::string* error);
    bool RegisterReplacementNode(const ManagedNodeProfile& profile,
                                 const std::string& predecessor_node_id,
                                 uint64_t join_time_ms,
                                 uint64_t lifetime_seed,
                                 std::string* error);
    bool ActivateNode(const std::string& node_id, uint64_t now_ms, std::string* error);
    bool BeginRemoveNode(const std::string& node_id, uint64_t now_ms, std::string* error);
    bool FinalizeRemoveNode(const std::string& node_id,
                            uint64_t now_ms,
                            bool force,
                            std::string* error);
    bool MarkNodeFailed(const std::string& node_id,
                        uint64_t now_ms,
                        const std::string& reason,
                        std::string* error);
    bool RepairNode(const std::string& node_id, uint64_t now_ms, std::string* error);
    bool ReportHeartbeat(const std::string& node_id,
                         const std::string& service_address,
                         NodeInventorySnapshot inventory,
                         uint64_t now_ms,
                         std::string* error);
    bool ReportReadiness(const std::string& node_id,
                         const zb::storage_model::NodeReadinessStatus& readiness,
                         std::string* error);
    bool ReportOpticalLibraryStatus(const std::string& node_id,
                                    const zb::storage_model::OpticalLibraryStatus& status,
                                    std::string* error);
    bool SetAdministrativeState(const std::string& node_id,
                                zb::storage_model::AdministrativeState state,
                                std::string* error);
    bool SetServiceMode(const std::string& node_id,
                        NodeServiceMode mode,
                        std::string* error);
    // Only the CMS authority may assign this identifier. Reapplying the same
    // value is idempotent; changing an existing assignment is rejected.
    bool AssignCmsCompactId(const std::string& node_id,
                            uint32_t compact_id,
                            std::string* error);
    bool SetTransitionState(const std::string& node_id,
                            TransitionState state,
                            uint64_t now_ms,
                            std::string* error);
    bool RequestPowerLevel(const std::string& node_id,
                           PowerLevel level,
                           uint64_t now_ms,
                           bool force,
                           std::string* error);
    uint64_t EvaluateHeartbeatHealth(uint64_t now_ms,
                                     uint64_t suspect_timeout_ms,
                                     uint64_t dead_timeout_ms);
    bool SetResidentDataBytes(const std::string& node_id, uint64_t bytes, std::string* error);
    bool ReportMetrics(const std::string& node_id,
                       const NodeAccessMetrics& metrics,
                       std::string* error);
    bool ReportAccessEvent(const std::string& node_id,
                           const NodeAccessEvent& event,
                           std::string* error);
    bool ReportDrainProgress(const std::string& node_id,
                             uint64_t report_ts_ms,
                             uint64_t remaining_objects,
                             uint64_t remaining_bytes,
                             uint32_t active_requests,
                             std::string* error);
    bool ReportPowerActuation(const std::string& node_id,
                              PowerLevel applied_level,
                              uint64_t now_ms,
                              bool success,
                              const std::string& message,
                              std::string* error);
    uint64_t Tick(uint64_t now_ms);
    uint64_t TickRealTime(uint64_t now_ms);
    uint64_t AdvanceSimulatedTime(uint64_t now_ms);

    bool GetNode(const std::string& node_id, ManagedNodeRuntime* out) const;
    // All list methods return copies ordered lexicographically by node_id.
    std::vector<ManagedNodeRuntime> ListNodes() const;
    std::vector<ManagedNodeRuntime> ListNodes(const ManagedNodeFilter& filter) const;
    std::vector<ManagedNodeRuntime> ListByLifecycle(LifecycleState state) const;
    std::vector<ManagedNodeRuntime> ListFailedNodes() const;
    PowerAndEnergySummary Summary() const;
    std::vector<ManagedMetricsSample> MetricsHistory(uint64_t start_ms,
                                                     uint64_t end_ms,
                                                     uint32_t limit) const;
    uint64_t generation() const;
    PowerPolicy policy() const;
    NodePowerManagerSnapshot Snapshot() const;
    bool Restore(const NodePowerManagerSnapshot& snapshot, std::string* error);

private:
    static uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs);
    static uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs);
    static uint64_t SampleFailureIntervalMs(uint64_t mean_ms, uint64_t seed);
    static uint64_t FailureMeanMsForFit(uint64_t failure_rate_fit);
    static uint64_t ScheduleNextFailureTime(const ManagedNodeRuntime& node,
                                            uint64_t from_ms);
    static uint64_t CapacityBytesForSafety(const ManagedNodeRuntime& node);
    static uint64_t FreeBytesForSafety(const ManagedNodeRuntime& node);
    void IntegrateEnergyLocked(ManagedNodeRuntime* node, uint64_t now_ms);
    bool AdvancePowerPolicyLocked(ManagedNodeRuntime* node, uint64_t now_ms);
    bool SetPowerLevelLocked(ManagedNodeRuntime* node,
                             PowerLevel level,
                             uint64_t now_ms,
                             bool force);
    PowerLevel DesiredPowerLevelLocked(const ManagedNodeRuntime& node, uint64_t now_ms) const;
    bool CanPowerOffLocked(const ManagedNodeRuntime& candidate) const;
    PowerAndEnergySummary SummaryLocked() const;
    void RecordHistoryLocked(uint64_t now_ms);
    uint64_t TickMatchingLocked(uint64_t now_ms, int execution_filter);

    mutable std::mutex mu_;
    PowerPolicy policy_;
    uint64_t generation_{1};
    std::unordered_map<std::string, ManagedNodeRuntime> nodes_;
    std::deque<ManagedMetricsSample> history_;
};

// Public validation entry point for configuration loaders and future users of
// this header. NodePowerManager falls back to defaults for an invalid policy to
// retain the existing constructor contract.
bool ValidatePowerPolicy(const PowerPolicy& policy, std::string* error);

} // namespace zb::scheduler
