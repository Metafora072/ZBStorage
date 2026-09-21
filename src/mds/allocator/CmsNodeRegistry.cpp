#include "CmsNodeRegistry.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_set>

#include "../storage/MetaSchema.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {
namespace {

constexpr uint64_t kWeightBandwidthUnit = 100000000ULL; // 100 MB/s.

bool IsDataDevice(zb::rpc::ManagedDeviceKind kind) {
    return kind == zb::rpc::MANAGED_DEVICE_HDD || kind == zb::rpc::MANAGED_DEVICE_SSD;
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max() : lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    return lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs
               ? std::numeric_limits<uint64_t>::max() : lhs * rhs;
}

} // namespace

CmsNodeRegistry::CmsNodeRegistry(NodeStateCache* cache,
                                 PGManager* pg_manager,
                                 RocksMetaStore* store)
    : cache_(cache), pg_manager_(pg_manager), store_(store) {}

bool CmsNodeRegistry::IsRetired(const zb::rpc::ManagedNodeView& node) {
    return node.lifecycle() == zb::rpc::MANAGED_LIFECYCLE_RETIRED;
}

bool CmsNodeRegistry::IsWriteAllocatable(const zb::rpc::ManagedNodeView& node) {
    if (!node.has_spec() || !node.spec().participates_in_placement() ||
        node.lifecycle() != zb::rpc::MANAGED_LIFECYCLE_WORKING ||
        node.health() != zb::rpc::MANAGED_HEALTH_HEALTHY ||
        node.administrative_state() != zb::rpc::MANAGED_ADMIN_ENABLED ||
        node.service_mode() != zb::rpc::MANAGED_SERVICE_READ_WRITE ||
        (node.power_level() != zb::rpc::MANAGED_POWER_MEDIUM &&
         node.power_level() != zb::rpc::MANAGED_POWER_PEAK)) {
        return false;
    }
    if (node.spec().kind() == zb::rpc::MANAGED_KIND_METADATA) return false;
    if (node.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY) {
        const auto& optical = node.optical_library();
        if (optical.staging_capacity_bytes() > 0 &&
            static_cast<long double>(optical.staging_used_bytes()) /
                    static_cast<long double>(optical.staging_capacity_bytes()) >= 0.80L) {
            return false;
        }
    }
    if (node.spec().execution_mode() == zb::rpc::MANAGED_EXECUTION_PHYSICAL &&
        node.actuated_power_level() != zb::rpc::MANAGED_POWER_MEDIUM &&
        node.actuated_power_level() != zb::rpc::MANAGED_POWER_PEAK) {
        return false;
    }
    for (const auto& device : node.inventory_devices()) {
        if (device.is_healthy() && device.free_bytes() > 0 &&
            (IsDataDevice(device.kind()) ||
             (node.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY &&
              device.kind() == zb::rpc::MANAGED_DEVICE_DISC))) {
            return true;
        }
    }
    return false;
}

uint32_t CmsNodeRegistry::SchedulingWeight(const zb::rpc::ManagedNodeView& node) {
    if (!IsWriteAllocatable(node)) return 1;
    uint64_t internal = 0;
    for (const auto& group : node.spec().device_groups()) {
        const bool selected = node.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY
                                  ? group.kind() == zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE
                                  : IsDataDevice(group.kind());
        if (!selected) continue;
        uint64_t per_device = group.per_device_write_bandwidth_bytes_per_sec();
        if (per_device == 0) per_device = group.per_device_bandwidth_bytes_per_sec();
        const uint32_t concurrency = group.max_concurrency() == 0
                                         ? group.device_count()
                                         : std::min(group.device_count(), group.max_concurrency());
        internal = SaturatingAdd(internal, SaturatingMultiply(per_device, concurrency));
    }
    uint64_t effective = std::min(internal, node.spec().external_network_bandwidth_bytes_per_sec());
    if (node.power_level() == zb::rpc::MANAGED_POWER_MEDIUM) {
        const auto& power = node.spec().power();
        if (power.peak_milliwatts() > power.standby_milliwatts()) {
            const uint64_t numerator = power.medium_milliwatts() > power.standby_milliwatts()
                                           ? power.medium_milliwatts() - power.standby_milliwatts() : 0;
            const uint64_t denominator = power.peak_milliwatts() - power.standby_milliwatts();
            effective = static_cast<uint64_t>(static_cast<long double>(effective) * numerator /
                                              denominator);
        }
    }
    const long double load_factor = std::max<long double>(0.10L, 1.0L - node.last_utilization());
    const long double queue_factor = 1.0L / (1.0L + static_cast<long double>(node.last_queue_depth()));
    const long double raw = static_cast<long double>(effective) /
                            static_cast<long double>(kWeightBandwidthUnit) *
                            load_factor * queue_factor;
    return static_cast<uint32_t>(std::clamp<long double>(std::ceil(raw), 1.0L, 1000.0L));
}

NodeInfo CmsNodeRegistry::BuildNodeInfo(const zb::rpc::CmsNodeCatalogEntry& entry) {
    const auto& placement = entry.placement_view();
    const auto& managed = entry.managed_view();
    NodeInfo out;
    out.node_id = managed.spec().node_id();
    out.address = managed.service_address().empty() ? placement.address()
                                                    : managed.service_address();
    out.group_id = out.node_id;
    out.epoch = 1;
    out.is_primary = true;
    out.sync_ready = true;
    out.virtual_node_count = std::max<uint32_t>(1, managed.spec().logical_node_count());
    if (managed.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY) {
        out.type = NodeType::kOptical;
    } else if (managed.spec().execution_mode() == zb::rpc::MANAGED_EXECUTION_VIRTUAL) {
        out.type = NodeType::kVirtual;
    } else {
        out.type = NodeType::kReal;
    }
    out.allocatable = IsWriteAllocatable(managed);
    out.weight = SchedulingWeight(managed);
    for (const auto& device : managed.inventory_devices()) {
        if (!IsDataDevice(device.kind()) &&
            !(managed.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY &&
              device.kind() == zb::rpc::MANAGED_DEVICE_DISC)) {
            continue;
        }
        DiskInfo disk;
        disk.disk_id = device.device_id();
        disk.capacity_bytes = device.capacity_bytes();
        disk.free_bytes = device.free_bytes();
        disk.is_healthy = device.is_healthy();
        out.disks.push_back(std::move(disk));
    }
    if (out.disks.empty()) out.allocatable = false;
    return out;
}

bool CmsNodeRegistry::PersistLocked(
    const std::map<std::string, zb::rpc::CmsNodeCatalogEntry>& nodes,
    uint64_t cms_generation,
    uint64_t scheduler_generation,
    uint32_t next_storage_compact_id,
    uint32_t next_optical_compact_id,
    std::string* error) const {
    if (!store_) return true;
    zb::rpc::CmsNodeCatalogSnapshot snapshot;
    snapshot.set_format_version(1);
    snapshot.set_cms_generation(cms_generation);
    snapshot.set_scheduler_generation(scheduler_generation);
    snapshot.set_next_storage_compact_id(next_storage_compact_id);
    snapshot.set_next_optical_compact_id(next_optical_compact_id);
    for (const auto& item : nodes) *snapshot.add_nodes() = item.second;
    std::string payload;
    if (!snapshot.SerializeToString(&payload)) {
        if (error) *error = "failed to serialize CMS node catalog snapshot";
        return false;
    }
    return store_->Put(CmsNodeCatalogSnapshotKey(), payload, error);
}

void CmsNodeRegistry::PublishPlacementLocked() {
    std::vector<NodeInfo> placement_nodes;
    placement_nodes.reserve(nodes_.size());
    for (const auto& item : nodes_) placement_nodes.push_back(BuildNodeInfo(item.second));
    if (cache_) cache_->ReplaceNodes(placement_nodes);
    bool has_allocatable_disk_node = false;
    for (const auto& node : placement_nodes) {
        if (node.allocatable && node.type != NodeType::kOptical) {
            has_allocatable_disk_node = true;
            break;
        }
    }
    if (pg_manager_ && has_allocatable_disk_node) {
        std::string ignored;
        (void)pg_manager_->RebuildFromNodes(placement_nodes, cms_generation_, &ignored);
    }
}

bool CmsNodeRegistry::Restore(std::string* error) {
    if (!store_) return true;
    std::string payload;
    std::string get_error;
    if (!store_->Get(CmsNodeCatalogSnapshotKey(), &payload, &get_error)) {
        if (get_error.empty()) return true;
        if (error) *error = get_error;
        return false;
    }
    zb::rpc::CmsNodeCatalogSnapshot snapshot;
    if (!snapshot.ParseFromString(payload) || snapshot.format_version() != 1 ||
        snapshot.cms_generation() == 0) {
        if (error) *error = "invalid CMS node catalog snapshot";
        return false;
    }
    std::map<std::string, zb::rpc::CmsNodeCatalogEntry> restored;
    std::unordered_set<uint32_t> storage_ids;
    std::unordered_set<uint32_t> optical_ids;
    uint32_t next_storage = std::max<uint32_t>(1, snapshot.next_storage_compact_id());
    uint32_t next_optical = std::max<uint32_t>(1, snapshot.next_optical_compact_id());
    for (const auto& entry : snapshot.nodes()) {
        if (!entry.has_managed_view() || !entry.managed_view().has_spec()) {
            if (error) *error = "CMS snapshot entry lacks managed identity";
            return false;
        }
        const auto& node = entry.managed_view();
        const std::string& node_id = node.spec().node_id();
        const uint64_t compact_id = node.compact_id();
        const bool optical = node.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY;
        const uint32_t maximum = optical ? 0x00ffffffU : 0x0000ffffU;
        auto& ids = optical ? optical_ids : storage_ids;
        if (node_id.empty() || compact_id == 0 || compact_id > maximum ||
            !ids.insert(compact_id).second || !restored.emplace(node_id, entry).second) {
            if (error) *error = "CMS snapshot contains an invalid or duplicate identity";
            return false;
        }
        auto& next = optical ? next_optical : next_storage;
        next = std::max(next, static_cast<uint32_t>(compact_id) + 1);
    }
    std::lock_guard<std::mutex> lock(mu_);
    nodes_ = std::move(restored);
    cms_generation_ = snapshot.cms_generation();
    scheduler_generation_ = snapshot.scheduler_generation();
    next_storage_compact_id_ = next_storage;
    next_optical_compact_id_ = next_optical;
    PublishPlacementLocked();
    return true;
}

bool CmsNodeRegistry::Commit(
    uint64_t scheduler_generation,
    const std::vector<zb::rpc::CmsNodeCatalogEntry>& proposed,
    uint64_t* cms_generation,
    std::vector<zb::rpc::CmsNodeCompactIdAssignment>* assignments,
    std::string* error) {
    if (scheduler_generation == 0) {
        if (error) *error = "scheduler generation is required";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (scheduler_generation < scheduler_generation_) {
        if (error) *error = "stale Scheduler catalog generation";
        return false;
    }
    if (scheduler_generation > scheduler_generation_) {
        std::map<std::string, zb::rpc::CmsNodeCatalogEntry> next_nodes;
        // Candidate counters belong to the proposal, just like candidate nodes.
        // Failed validation or persistence must not consume authoritative IDs.
        uint32_t next_storage = next_storage_compact_id_;
        uint32_t next_optical = next_optical_compact_id_;
        for (const auto& entry : proposed) {
            if (!entry.has_managed_view() || !entry.managed_view().has_spec() ||
                entry.managed_view().spec().node_id().empty()) {
                if (error) *error = "CMS catalog entry is missing managed node identity";
                return false;
            }
            const std::string& node_id = entry.managed_view().spec().node_id();
            if (entry.has_placement_view() && !entry.placement_view().node_id().empty() &&
                entry.placement_view().node_id() != node_id) {
                if (error) *error = "placement and managed node identities differ";
                return false;
            }
            if (next_nodes.find(node_id) != next_nodes.end()) {
                if (error) *error = "duplicate node in CMS catalog proposal: " + node_id;
                return false;
            }
            zb::rpc::CmsNodeCatalogEntry committed = entry;
            auto existing = nodes_.find(node_id);
            uint32_t compact_id = 0;
            if (existing != nodes_.end()) {
                if (existing->second.managed_view().spec().kind() !=
                    entry.managed_view().spec().kind()) {
                    if (error) *error = "CMS node kind cannot change: " + node_id;
                    return false;
                }
                compact_id = static_cast<uint32_t>(existing->second.managed_view().compact_id());
                if (IsRetired(existing->second.managed_view()) && !IsRetired(entry.managed_view())) {
                    if (error) *error = "retired CMS node cannot rejoin: " + node_id;
                    return false;
                }
            } else {
                const bool optical = entry.managed_view().spec().kind() ==
                                     zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY;
                auto& next = optical ? next_optical : next_storage;
                if (next > (optical ? 0x00ffffffU : 0x0000ffffU)) {
                    if (error) *error = "CMS compact identifier space exhausted";
                    return false;
                }
                compact_id = next++;
            }
            committed.mutable_managed_view()->set_compact_id(compact_id);
            next_nodes.emplace(node_id, std::move(committed));
        }
        for (const auto& current : nodes_) {
            if (next_nodes.find(current.first) == next_nodes.end()) {
                if (error) *error = "CMS catalog proposal omitted existing tombstone/member: " + current.first;
                return false;
            }
        }
        const uint64_t next_cms_generation = cms_generation_ + 1;
        if (!PersistLocked(next_nodes, next_cms_generation, scheduler_generation,
                           next_storage, next_optical, error)) {
            return false;
        }
        nodes_ = std::move(next_nodes);
        next_storage_compact_id_ = next_storage;
        next_optical_compact_id_ = next_optical;
        scheduler_generation_ = scheduler_generation;
        cms_generation_ = next_cms_generation;
        PublishPlacementLocked();
    }
    if (cms_generation) *cms_generation = cms_generation_;
    if (assignments) {
        assignments->clear();
        assignments->reserve(nodes_.size());
        for (const auto& item : nodes_) {
            zb::rpc::CmsNodeCompactIdAssignment assignment;
            assignment.set_node_id(item.first);
            assignment.set_compact_id(
                static_cast<uint32_t>(item.second.managed_view().compact_id()));
            assignments->push_back(std::move(assignment));
        }
    }
    return true;
}

std::vector<zb::rpc::CmsNodeCatalogEntry> CmsNodeRegistry::Snapshot(
    uint64_t* cms_generation, uint64_t* scheduler_generation) const {
    std::lock_guard<std::mutex> lock(mu_);
    if (cms_generation) *cms_generation = cms_generation_;
    if (scheduler_generation) *scheduler_generation = scheduler_generation_;
    std::vector<zb::rpc::CmsNodeCatalogEntry> out;
    out.reserve(nodes_.size());
    for (const auto& item : nodes_) out.push_back(item.second);
    return out;
}

bool CmsNodeRegistry::IsWriteAdmitted(const std::string& node_id) const {
    if (node_id.empty()) return false;
    std::lock_guard<std::mutex> lock(mu_);
    auto exact = nodes_.find(node_id);
    if (exact != nodes_.end()) return IsWriteAllocatable(exact->second.managed_view());
    for (const auto& item : nodes_) {
        const std::string prefix = item.first + "-v";
        if (node_id.size() > prefix.size() && node_id.rfind(prefix, 0) == 0) {
            return IsWriteAllocatable(item.second.managed_view());
        }
    }
    return false;
}

} // namespace zb::mds
