#include "ManagedNode.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <random>

namespace zb::scheduler {

namespace {

constexpr uint64_t kKilobyte = 1000ULL;
constexpr uint64_t kMegabyte = 1000ULL * kKilobyte;
constexpr uint64_t kGigabyte = 1000ULL * kMegabyte;
constexpr uint64_t kTerabyte = 1000ULL * kGigabyte;
constexpr uint64_t kDayMs = 24ULL * 60ULL * 60ULL * 1000ULL;
constexpr uint64_t kYearMs = 365ULL * kDayMs;

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

bool GroupMatchesPath(DeviceKind kind, AccessPath path) {
    if (path == AccessPath::kOpticalDisc) {
        return kind == DeviceKind::kOpticalDrive;
    }
    if (path == AccessPath::kOpticalCacheHit) {
        return kind == DeviceKind::kHdd || kind == DeviceKind::kSsd;
    }
    return kind == DeviceKind::kHdd || kind == DeviceKind::kSsd;
}

uint64_t CeilTransferMicroseconds(uint64_t bytes, uint64_t bandwidth_bytes_per_sec) {
    if (bytes == 0) {
        return 0;
    }
    if (bandwidth_bytes_per_sec == 0) {
        return std::numeric_limits<uint64_t>::max();
    }
    const long double micros = static_cast<long double>(bytes) * 1000000.0L /
                               static_cast<long double>(bandwidth_bytes_per_sec);
    if (micros >= static_cast<long double>(std::numeric_limits<uint64_t>::max())) {
        return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(std::ceil(micros));
}

PowerProfile StoragePowerProfile() {
    return {1000ULL * 1000ULL, 300ULL * 1000ULL, 100ULL * 1000ULL, 0};
}

PowerProfile MetadataPowerProfile() {
    return {500ULL * 1000ULL, 200ULL * 1000ULL, 50ULL * 1000ULL, 0};
}

PowerProfile OpticalPowerProfile() {
    return {400ULL * 1000ULL, 200ULL * 1000ULL, 50ULL * 1000ULL, 0};
}

} // namespace

ManagedNodeProfile DefaultStorageNodeProfile(const std::string& node_id) {
    ManagedNodeProfile profile;
    profile.node_id = node_id;
    profile.kind = ManagedNodeKind::kStorage;
    profile.external_network_bandwidth_bytes_per_sec = 10ULL * kGigabyte;
    profile.fixed_access_latency_us = 5000;
    profile.device_groups.push_back({"hdd-18tb", DeviceKind::kHdd, 18ULL * kTerabyte, 24,
                                     200ULL * kMegabyte, 5,
                                     200ULL * kMegabyte, 200ULL * kMegabyte});
    profile.power = StoragePowerProfile();
    profile.reliability = {5ULL * kYearMs, kYearMs, kYearMs};
    return profile;
}

ManagedNodeProfile DefaultMetadataNodeProfile(const std::string& node_id) {
    ManagedNodeProfile profile;
    profile.node_id = node_id;
    profile.kind = ManagedNodeKind::kMetadata;
    profile.external_network_bandwidth_bytes_per_sec = 10ULL * kGigabyte;
    profile.fixed_access_latency_us = 500;
    profile.device_groups.push_back({"ssd-4tb", DeviceKind::kSsd, 4ULL * kTerabyte, 16,
                                     5ULL * kGigabyte, 10,
                                     5ULL * kGigabyte, 5ULL * kGigabyte});
    profile.power = MetadataPowerProfile();
    profile.reliability = {5ULL * kYearMs, kYearMs, kYearMs};
    return profile;
}

ManagedNodeProfile DefaultOpticalLibraryProfile(const std::string& node_id) {
    ManagedNodeProfile profile;
    profile.node_id = node_id;
    profile.kind = ManagedNodeKind::kOpticalLibrary;
    profile.external_network_bandwidth_bytes_per_sec = 10ULL * kGigabyte;
    profile.fixed_access_latency_us = 500;
    profile.optical_load_latency_us = 60ULL * 1000ULL * 1000ULL;
    profile.optical_seek_latency_us = 500ULL * 1000ULL;
    // HDD cache geometry is retained from the implemented node profile; the
    // optical medium/image geometry follows the 2026-08-16 design.
    profile.device_groups.push_back({"hdd-cache-28tb", DeviceKind::kHdd, 28ULL * kTerabyte, 10,
                                     200ULL * kMegabyte, 10,
                                     200ULL * kMegabyte, 200ULL * kMegabyte});
    profile.device_groups.push_back({"disc-1tb", DeviceKind::kDisc, 1ULL * kTerabyte, 10000,
                                     0, 0, 0, 0});
    profile.device_groups.push_back({"optical-drive", DeviceKind::kOpticalDrive, 0, 10,
                                     100ULL * kMegabyte, 10,
                                     100ULL * kMegabyte, 10ULL * kMegabyte});
    profile.power = OpticalPowerProfile();
    profile.reliability = {10ULL * kYearMs, 2ULL * kYearMs, kYearMs};
    return profile;
}

bool ValidateManagedNodeProfile(const ManagedNodeProfile& profile, std::string* error) {
    return zb::storage_model::ValidateNodeHardwareProfile(profile, error);
}

uint64_t CalculateCapacityBytes(const ManagedNodeProfile& profile) {
    uint64_t total = 0;
    for (const auto& group : profile.device_groups) {
        if (group.kind == DeviceKind::kOpticalDrive) {
            continue;
        }
        total = SaturatingAdd(total, SaturatingMultiply(group.device_capacity_bytes, group.device_count));
    }
    return total;
}

uint64_t CalculateInternalBandwidthBytesPerSecond(const ManagedNodeProfile& profile, AccessPath path) {
    return CalculateInternalBandwidthBytesPerSecond(
        profile, path, zb::storage_model::IoDirection::kRead);
}

uint64_t CalculateInternalBandwidthBytesPerSecond(
    const ManagedNodeProfile& profile,
    AccessPath path,
    zb::storage_model::IoDirection direction) {
    uint64_t total = 0;
    for (const auto& group : profile.device_groups) {
        if (!GroupMatchesPath(group.kind, path)) continue;
        const uint64_t bandwidth =
            zb::storage_model::DeviceBandwidthBytesPerSecond(group, direction);
        if (bandwidth == 0) continue;
        const uint32_t concurrency = group.max_concurrency == 0
                                         ? group.device_count
                                         : std::min(group.device_count, group.max_concurrency);
        total = SaturatingAdd(total,
                              SaturatingMultiply(bandwidth, concurrency));
    }
    return total;
}

PerformanceEstimate EstimatePerformance(const ManagedNodeProfile& profile,
                                        uint64_t request_size_bytes,
                                        AccessPath path,
                                        uint64_t queue_delay_us,
                                        uint64_t power_transition_delay_us) {
    return EstimatePerformanceAtPower(profile, request_size_bytes, path,
                                      zb::storage_model::IoDirection::kRead,
                                      PowerLevel::kPeak, queue_delay_us,
                                      power_transition_delay_us);
}

PerformanceEstimate EstimatePerformanceAtPower(
    const ManagedNodeProfile& profile,
    uint64_t request_size_bytes,
    AccessPath path,
    zb::storage_model::IoDirection direction,
    PowerLevel power_level,
    uint64_t queue_delay_us,
    uint64_t power_transition_delay_us) {
    PerformanceEstimate estimate;
    estimate.capacity_bytes = CalculateCapacityBytes(profile);
    const uint64_t peak_bandwidth =
        CalculateInternalBandwidthBytesPerSecond(profile, path, direction);
    estimate.internal_bandwidth_bytes_per_sec = peak_bandwidth;
    if (power_level == PowerLevel::kOff || power_level == PowerLevel::kStandby) {
        estimate.internal_bandwidth_bytes_per_sec = 0;
    } else if (power_level == PowerLevel::kMedium &&
               profile.power.peak_milliwatts > profile.power.standby_milliwatts) {
        const uint64_t medium_above_standby =
            profile.power.medium_milliwatts > profile.power.standby_milliwatts
                ? profile.power.medium_milliwatts - profile.power.standby_milliwatts : 0;
        const uint64_t peak_above_standby =
            profile.power.peak_milliwatts - profile.power.standby_milliwatts;
        const long double scaled = static_cast<long double>(peak_bandwidth) *
                                   static_cast<long double>(medium_above_standby) /
                                   static_cast<long double>(peak_above_standby);
        estimate.internal_bandwidth_bytes_per_sec =
            scaled >= static_cast<long double>(std::numeric_limits<uint64_t>::max())
                ? std::numeric_limits<uint64_t>::max()
                : static_cast<uint64_t>(scaled);
    }
    estimate.effective_bandwidth_bytes_per_sec =
        std::min(profile.external_network_bandwidth_bytes_per_sec,
                 estimate.internal_bandwidth_bytes_per_sec);
    if (estimate.effective_bandwidth_bytes_per_sec == 0) {
        estimate.error = "effective bandwidth is zero for the selected access path";
        return estimate;
    }

    uint64_t latency = SaturatingAdd(queue_delay_us, power_transition_delay_us);
    if (path == AccessPath::kOpticalDisc) {
        latency = SaturatingAdd(latency, profile.optical_load_latency_us);
        latency = SaturatingAdd(latency, profile.optical_seek_latency_us);
    } else {
        latency = SaturatingAdd(latency, profile.fixed_access_latency_us);
    }
    latency = SaturatingAdd(latency,
                            CeilTransferMicroseconds(request_size_bytes,
                                                     estimate.effective_bandwidth_bytes_per_sec));
    estimate.service_latency_us = latency;
    estimate.valid = true;
    return estimate;
}

uint64_t PowerMilliwatts(const PowerProfile& profile, PowerLevel level) {
    switch (level) {
    case PowerLevel::kPeak:
        return profile.peak_milliwatts;
    case PowerLevel::kMedium:
        return profile.medium_milliwatts;
    case PowerLevel::kStandby:
        return profile.standby_milliwatts;
    case PowerLevel::kOff:
        return profile.off_milliwatts;
    }
    return 0;
}

uint64_t SampleLifetimeMs(const ReliabilityProfile& profile, uint64_t seed) {
    if (profile.lifetime_stddev_ms == 0) {
        return std::max(profile.mean_lifetime_ms, profile.minimum_lifetime_ms);
    }
    std::mt19937_64 rng(seed);
    std::normal_distribution<long double> distribution(
        static_cast<long double>(profile.mean_lifetime_ms),
        static_cast<long double>(profile.lifetime_stddev_ms));
    const long double sampled = distribution(rng);
    if (sampled <= static_cast<long double>(profile.minimum_lifetime_ms)) {
        return profile.minimum_lifetime_ms;
    }
    if (sampled >= static_cast<long double>(std::numeric_limits<uint64_t>::max())) {
        return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(sampled);
}

bool IsServingLifecycle(LifecycleState state) {
    return state == LifecycleState::kWorking || state == LifecycleState::kDraining;
}

bool IsTerminalLifecycle(LifecycleState state) {
    return state == LifecycleState::kRetired;
}

const char* ManagedNodeKindName(ManagedNodeKind kind) {
    switch (kind) {
    case ManagedNodeKind::kStorage:
        return "STORAGE";
    case ManagedNodeKind::kMetadata:
        return "METADATA";
    case ManagedNodeKind::kOpticalLibrary:
        return "OPTICAL_LIBRARY";
    }
    return "UNKNOWN";
}

const char* ExecutionModeName(ExecutionMode mode) {
    switch (mode) {
    case ExecutionMode::kPhysical:
        return "PHYSICAL";
    case ExecutionMode::kVirtual:
        return "VIRTUAL";
    case ExecutionMode::kSimulated:
        return "SIMULATED";
    }
    return "UNKNOWN";
}

const char* DeviceKindName(DeviceKind kind) {
    switch (kind) {
    case DeviceKind::kHdd:
        return "HDD";
    case DeviceKind::kSsd:
        return "SSD";
    case DeviceKind::kDisc:
        return "DISC";
    case DeviceKind::kOpticalDrive:
        return "OPTICAL_DRIVE";
    }
    return "UNKNOWN";
}

const char* AccessPathName(AccessPath path) {
    switch (path) {
    case AccessPath::kDefault:
        return "DEFAULT";
    case AccessPath::kOpticalCacheHit:
        return "OPTICAL_CACHE_HIT";
    case AccessPath::kOpticalDisc:
        return "OPTICAL_DISC";
    }
    return "UNKNOWN";
}

const char* LifecycleStateName(LifecycleState state) {
    switch (state) {
    case LifecycleState::kJoining:
        return "JOINING";
    case LifecycleState::kWorking:
        return "WORKING";
    case LifecycleState::kDraining:
        return "DRAINING";
    case LifecycleState::kExiting:
        return "EXITING";
    case LifecycleState::kRetired:
        return "RETIRED";
    }
    return "UNKNOWN";
}

const char* ManagedHealthStateName(ManagedHealthState state) {
    switch (state) {
    case ManagedHealthState::kHealthy:
        return "HEALTHY";
    case ManagedHealthState::kSuspect:
        return "SUSPECT";
    case ManagedHealthState::kFailed:
        return "FAILED";
    }
    return "UNKNOWN";
}

const char* PowerLevelName(PowerLevel level) {
    switch (level) {
    case PowerLevel::kOff:
        return "OFF";
    case PowerLevel::kStandby:
        return "STANDBY";
    case PowerLevel::kMedium:
        return "MEDIUM";
    case PowerLevel::kPeak:
        return "PEAK";
    }
    return "UNKNOWN";
}

const char* TransitionStateName(TransitionState state) {
    switch (state) {
    case TransitionState::kStable:
        return "STABLE";
    case TransitionState::kStarting:
        return "STARTING";
    case TransitionState::kStopping:
        return "STOPPING";
    case TransitionState::kRebooting:
        return "REBOOTING";
    }
    return "UNKNOWN";
}

const char* AdministrativeStateName(zb::storage_model::AdministrativeState state) {
    switch (state) {
    case zb::storage_model::AdministrativeState::kEnabled: return "ENABLED";
    case zb::storage_model::AdministrativeState::kDraining: return "DRAINING";
    case zb::storage_model::AdministrativeState::kDisabled: return "DISABLED";
    }
    return "UNKNOWN";
}

const char* NodeServiceModeName(NodeServiceMode mode) {
    switch (mode) {
    case NodeServiceMode::kReadWrite: return "READ_WRITE";
    case NodeServiceMode::kReadOnly: return "READ_ONLY";
    }
    return "UNKNOWN";
}

const char* FailureSourceName(FailureSource source) {
    switch (source) {
    case FailureSource::kNone: return "NONE";
    case FailureSource::kHeartbeatTimeout: return "HEARTBEAT_TIMEOUT";
    case FailureSource::kReliabilityModel: return "RELIABILITY_MODEL";
    case FailureSource::kInjected: return "INJECTED";
    }
    return "UNKNOWN";
}

} // namespace zb::scheduler
