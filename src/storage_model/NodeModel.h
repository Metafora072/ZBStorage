#pragma once

#include "Identifiers.h"

#include <cstdint>
#include <string>
#include <vector>

namespace zb::storage_model {

enum class NodeKind {
    kStorage = 0,
    kMetadata = 1,
    kOpticalLibrary = 2,
};

enum class ExecutionMode {
    kPhysical = 0,
    kVirtual = 1,
    kSimulated = 2,
};

enum class DeviceKind {
    kHdd = 0,
    kSsd = 1,
    kDisc = 2,
    kOpticalDrive = 3,
};

enum class AccessPath {
    kDefault = 0,
    kOpticalCacheHit = 1,
    kOpticalDisc = 2,
};

enum class IoDirection {
    kRead = 0,
    kWrite = 1,
};

// Scheduler lifecycle. It maps design Online to Working, Maintenance to
// Draining/Exiting, and keeps Joining as an explicit operational state.
enum class LifecycleState {
    kJoining = 0,
    kWorking = 1,
    kDraining = 2,
    kExiting = 3,
    kRetired = 4,
};

enum class HealthState {
    kHealthy = 0,
    kSuspect = 1,
    kFailed = 2,
};

enum class AdministrativeState {
    kEnabled = 0,
    kDraining = 1,
    kDisabled = 2,
};

enum class NodeServiceMode {
    kReadWrite = 0,
    kReadOnly = 1,
};

enum class PowerLevel {
    kOff = 0,
    kStandby = 1,
    kMedium = 2,
    kPeak = 3,
};

enum class TransitionState {
    kStable = 0,
    kStarting = 1,
    kStopping = 2,
    kRebooting = 3,
};

enum class ReliabilityModel {
    kNormalLifetime = 0,
    kBathtubCurve = 1,
};

enum class FailureSource {
    kNone = 0,
    kHeartbeatTimeout = 1,
    kReliabilityModel = 2,
    kInjected = 3,
};

struct DeviceGroup {
    std::string name;
    DeviceKind kind{DeviceKind::kHdd};
    // Decimal units: one TB is 10^12 bytes and one MB/s is 10^6 bytes/s.
    uint64_t device_capacity_bytes{0};
    uint32_t device_count{0};
    // Compatibility bandwidth used when a direction-specific value is zero.
    uint64_t per_device_bandwidth_bytes_per_sec{0};
    uint32_t max_concurrency{0}; // Zero means all devices in this group.
    uint64_t per_device_read_bandwidth_bytes_per_sec{0};
    uint64_t per_device_write_bandwidth_bytes_per_sec{0};
};

struct PowerProfile {
    uint64_t peak_milliwatts{0};
    uint64_t medium_milliwatts{0};
    uint64_t standby_milliwatts{0};
    uint64_t off_milliwatts{0};
};

struct ReliabilityProfile {
    uint64_t mean_lifetime_ms{0};
    uint64_t lifetime_stddev_ms{0};
    uint64_t minimum_lifetime_ms{1};
    uint64_t mean_time_between_failures_ms{0};
    uint64_t mean_repair_time_ms{0};
    ReliabilityModel model{ReliabilityModel::kNormalLifetime};
    uint64_t early_failure_window_ms{0};
    uint64_t wearout_start_age_ms{0};
    // Failures per 10^9 operating hours; integers keep snapshots deterministic.
    uint64_t early_failure_rate_fit{0};
    uint64_t useful_life_failure_rate_fit{0};
    uint64_t wearout_failure_rate_fit{0};
};

// Configured/planned hardware. Observed capacity and health belong in
// NodeInventorySnapshot so configuration is never overwritten by a heartbeat.
struct NodeHardwareProfile {
    std::string node_id;
    NodeKind kind{NodeKind::kStorage};
    ExecutionMode execution_mode{ExecutionMode::kPhysical};
    uint32_t logical_node_count{1};
    uint32_t technology_generation{1};
    bool participates_in_placement{true};
    uint64_t external_network_bandwidth_bytes_per_sec{0};
    uint64_t fixed_access_latency_us{0};
    uint64_t optical_load_latency_us{0};
    uint64_t optical_seek_latency_us{0};
    std::vector<DeviceGroup> device_groups;
    PowerProfile power;
    ReliabilityProfile reliability;
};

struct NodeIdentity {
    std::string node_id;
    // CMS-assigned compact identifier. Storage/metadata nodes use the low
    // 16 bits; optical libraries use the low 24 bits.
    uint32_t compact_id{0}; // Zero means not assigned.
    NodeKind kind{NodeKind::kStorage};
    ExecutionMode execution_mode{ExecutionMode::kPhysical};
    std::string service_address;
};

struct DeviceInventory {
    std::string disk_id;
    DeviceKind kind{DeviceKind::kHdd};
    uint64_t capacity_bytes{0};
    uint64_t free_bytes{0};
    bool is_healthy{true};
    uint64_t read_bandwidth_bytes_per_sec{0};
    uint64_t write_bandwidth_bytes_per_sec{0};
    uint64_t accumulated_write_bytes{0};
    uint64_t last_update_ms{0};
};

struct NodeInventorySnapshot {
    std::string node_id;
    uint64_t generation{0};
    uint64_t observed_at_ms{0};
    std::vector<DeviceInventory> devices;
};

// Scheduler only needs aggregate optical-library state for admission and
// retirement decisions. Detailed slots/discs/drives remain in OpticalModel.
struct OpticalLibraryStatus {
    bool observed{false};
    uint32_t total_slots{0};
    uint32_t local_disc_count{0};
    uint32_t blank_disc_count{0};
    uint32_t recording_disc_count{0};
    uint32_t recorded_disc_count{0};
    uint32_t defective_disc_count{0};
    // L2 staging cache pressure is reported by the library controller. CMS
    // uses it to stop assigning new archive writes before the cache fills.
    uint64_t staging_capacity_bytes{0};
    uint64_t staging_used_bytes{0};
    uint32_t pending_write_tasks{0};
    uint64_t observed_at_ms{0};
};

struct NodeReadinessStatus {
    bool observed{false};
    bool initialization_complete{false};
    bool inventory_ready{false};
    bool metadata_ready{false};
    uint64_t observed_at_ms{0};
    std::string message;

    bool Ready() const {
        return observed && initialization_complete && inventory_ready && metadata_ready;
    }
};

// Scheduler-owned state shared by registration, power management, snapshots
// and status reporting. Every *_ms timestamp uses the node execution time domain.
struct NodeRecord {
    NodeHardwareProfile profile;
    NodeIdentity identity;
    NodeInventorySnapshot inventory;
    LifecycleState lifecycle{LifecycleState::kJoining};
    HealthState health{HealthState::kHealthy};
    AdministrativeState administrative_state{AdministrativeState::kEnabled};
    NodeServiceMode service_mode{NodeServiceMode::kReadWrite};
    PowerLevel power_level{PowerLevel::kOff};
    PowerLevel actuated_power_level{PowerLevel::kOff};
    TransitionState transition{TransitionState::kStable};
    uint64_t join_time_ms{0};
    uint64_t sampled_lifetime_ms{0};
    uint64_t planned_exit_time_ms{0};
    uint64_t actual_exit_time_ms{0};
    uint64_t last_heartbeat_ms{0};
    uint64_t last_access_ms{0};
    uint64_t last_metrics_end_ms{0};
    uint64_t last_metrics_sequence{0};
    uint64_t last_metrics_reporter_epoch{0};
    uint64_t power_state_enter_ms{0};
    uint64_t last_energy_update_ms{0};
    uint64_t energy_microjoules{0};
    uint64_t last_actuated_energy_update_ms{0};
    uint64_t actuated_energy_microjoules{0};
    uint64_t resident_data_bytes{0};
    double last_utilization{0.0};
    uint32_t last_queue_depth{0};
    uint64_t access_operation_count{0};
    uint64_t accumulated_write_bytes{0};
    uint64_t wake_count{0};
    uint64_t standby_transition_count{0};
    uint64_t power_off_count{0};
    uint64_t replacement_count{0};
    std::string predecessor_node_id;
    uint64_t drain_remaining_objects{0};
    uint64_t drain_remaining_bytes{0};
    uint32_t drain_active_requests{0};
    uint64_t drain_last_report_ms{0};
    std::string failure_reason;
    FailureSource failure_source{FailureSource::kNone};
    uint64_t last_power_actuation_ms{0};
    std::string last_power_actuation_error;
    uint64_t reliability_seed{0};
    uint64_t next_failure_time_ms{0};
    uint64_t repair_due_time_ms{0};
    uint64_t failure_count{0};
    OpticalLibraryStatus optical_library;
    NodeReadinessStatus readiness;
};

inline constexpr uint32_t kNodeCatalogSnapshotFormatVersion = 2;

// Active/Joining/Retired/Failed lists are derived from NodeRecord state instead
// of being stored as competing sources of truth.
struct NodePowerSummary {
    uint64_t generation{0};
    uint64_t total_nodes{0};
    uint64_t joining_nodes{0};
    uint64_t working_nodes{0};
    uint64_t draining_nodes{0};
    uint64_t failed_nodes{0};
    uint64_t retired_nodes{0};
    uint64_t storage_nodes{0};
    uint64_t metadata_nodes{0};
    uint64_t optical_library_nodes{0};
    uint64_t peak_nodes{0};
    uint64_t medium_nodes{0};
    uint64_t standby_nodes{0};
    uint64_t off_nodes{0};
    uint64_t total_power_milliwatts{0};
    uint64_t total_energy_microjoules{0};
    uint64_t total_actuated_power_milliwatts{0};
    uint64_t total_actuated_energy_microjoules{0};
    uint64_t total_capacity_bytes{0};
    uint64_t total_free_bytes{0};
    uint64_t total_used_bytes{0};
    uint64_t available_capacity_bytes{0};
    uint64_t unavailable_capacity_bytes{0};
    uint64_t accumulated_write_bytes{0};
    uint64_t wake_count{0};
    uint64_t standby_transition_count{0};
    uint64_t power_off_count{0};
    uint64_t replacement_count{0};
};

struct NodePowerSample {
    uint64_t timestamp_ms{0};
    NodePowerSummary summary;
};

struct NodeCatalogSnapshot {
    uint32_t format_version{kNodeCatalogSnapshotFormatVersion};
    uint64_t generation{1};
    std::vector<NodeRecord> nodes;
    std::vector<NodePowerSample> history;
};

struct PerformanceEstimate {
    bool valid{false};
    uint64_t capacity_bytes{0};
    uint64_t internal_bandwidth_bytes_per_sec{0};
    uint64_t effective_bandwidth_bytes_per_sec{0};
    uint64_t service_latency_us{0};
    std::string error;
};

uint64_t DeviceBandwidthBytesPerSecond(const DeviceGroup& group, IoDirection direction);
// Used space is derived instead of stored, keeping capacity/free/used mutually
// consistent even when a heartbeat is retried or arrives out of order.
uint64_t DeviceUsedBytes(const DeviceInventory& device);
uint64_t InventoryCapacityBytes(const NodeInventorySnapshot& snapshot);
uint64_t InventoryFreeBytes(const NodeInventorySnapshot& snapshot);
uint64_t InventoryUsedBytes(const NodeInventorySnapshot& snapshot);
uint64_t InventoryHealthyCapacityBytes(const NodeInventorySnapshot& snapshot);
uint64_t InventoryHealthyFreeBytes(const NodeInventorySnapshot& snapshot);
bool ValidateNodeHardwareProfile(const NodeHardwareProfile& profile, std::string* error);
bool ValidateNodeInventorySnapshot(const NodeInventorySnapshot& snapshot, std::string* error);
bool ValidateOpticalLibraryStatus(const OpticalLibraryStatus& status, std::string* error);

} // namespace zb::storage_model
