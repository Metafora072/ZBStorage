#pragma once

#include "storage_model/NodeModel.h"

#include <cstdint>
#include <string>

namespace zb::scheduler {

// Version of the in-process managed-node attribute contract. This is separate
// from protobuf and snapshot format versions; increment it when a consumer must
// change how it interprets fields in this header.
inline constexpr uint32_t kManagedNodeAttributeModelVersion =
    zb::storage_model::kStorageModelVersion;

// Compatibility exports. The authoritative definitions live in
// src/storage_model and can be consumed without depending on Scheduler.
using ManagedNodeKind = zb::storage_model::NodeKind;
using ExecutionMode = zb::storage_model::ExecutionMode;
using DeviceKind = zb::storage_model::DeviceKind;
using AccessPath = zb::storage_model::AccessPath;
using LifecycleState = zb::storage_model::LifecycleState;
using ManagedHealthState = zb::storage_model::HealthState;
using PowerLevel = zb::storage_model::PowerLevel;
using TransitionState = zb::storage_model::TransitionState;
using NodeServiceMode = zb::storage_model::NodeServiceMode;
using FailureSource = zb::storage_model::FailureSource;
using DeviceGroup = zb::storage_model::DeviceGroup;
using DeviceInventory = zb::storage_model::DeviceInventory;
using NodeInventorySnapshot = zb::storage_model::NodeInventorySnapshot;
using OpticalLibraryStatus = zb::storage_model::OpticalLibraryStatus;
using PowerProfile = zb::storage_model::PowerProfile;
using ReliabilityProfile = zb::storage_model::ReliabilityProfile;
using ManagedNodeProfile = zb::storage_model::NodeHardwareProfile;
using PerformanceEstimate = zb::storage_model::PerformanceEstimate;

ManagedNodeProfile DefaultStorageNodeProfile(const std::string& node_id);
ManagedNodeProfile DefaultMetadataNodeProfile(const std::string& node_id);
ManagedNodeProfile DefaultOpticalLibraryProfile(const std::string& node_id);

bool ValidateManagedNodeProfile(const ManagedNodeProfile& profile, std::string* error);
uint64_t CalculateCapacityBytes(const ManagedNodeProfile& profile);
uint64_t CalculateInternalBandwidthBytesPerSecond(const ManagedNodeProfile& profile, AccessPath path);
uint64_t CalculateInternalBandwidthBytesPerSecond(const ManagedNodeProfile& profile,
                                                  AccessPath path,
                                                  zb::storage_model::IoDirection direction);
PerformanceEstimate EstimatePerformance(const ManagedNodeProfile& profile,
                                        uint64_t request_size_bytes,
                                        AccessPath path = AccessPath::kDefault,
                                        uint64_t queue_delay_us = 0,
                                        uint64_t power_transition_delay_us = 0);
PerformanceEstimate EstimatePerformanceAtPower(
    const ManagedNodeProfile& profile,
    uint64_t request_size_bytes,
    AccessPath path,
    zb::storage_model::IoDirection direction,
    PowerLevel power_level,
    uint64_t queue_delay_us = 0,
    uint64_t power_transition_delay_us = 0);
uint64_t PowerMilliwatts(const PowerProfile& profile, PowerLevel level);
uint64_t SampleLifetimeMs(const ReliabilityProfile& profile, uint64_t seed);

// Classification helpers are intentionally kept next to the data model so
// callers do not duplicate lifecycle semantics.
bool IsServingLifecycle(LifecycleState state);
bool IsTerminalLifecycle(LifecycleState state);

const char* ManagedNodeKindName(ManagedNodeKind kind);
const char* ExecutionModeName(ExecutionMode mode);
const char* DeviceKindName(DeviceKind kind);
const char* AccessPathName(AccessPath path);
const char* LifecycleStateName(LifecycleState state);
const char* ManagedHealthStateName(ManagedHealthState state);
const char* PowerLevelName(PowerLevel level);
const char* TransitionStateName(TransitionState state);
const char* AdministrativeStateName(zb::storage_model::AdministrativeState state);
const char* NodeServiceModeName(NodeServiceMode mode);
const char* FailureSourceName(FailureSource source);

} // namespace zb::scheduler
