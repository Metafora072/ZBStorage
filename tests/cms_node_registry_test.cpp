#include "mds/allocator/CmsNodeRegistry.h"
#include "mds/storage/RocksMetaStore.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

namespace {

bool Expect(bool value, const std::string& message) {
    if (!value) std::cerr << "FAILED: " << message << '\n';
    return value;
}

zb::rpc::CmsNodeCatalogEntry MakeStorage(const std::string& id,
                                         uint64_t write_bandwidth) {
    zb::rpc::CmsNodeCatalogEntry entry;
    entry.mutable_placement_view()->set_node_id(id);
    entry.mutable_placement_view()->set_address("127.0.0.1:8000");
    auto* node = entry.mutable_managed_view();
    auto* spec = node->mutable_spec();
    spec->set_node_id(id);
    spec->set_kind(zb::rpc::MANAGED_KIND_STORAGE);
    spec->set_execution_mode(zb::rpc::MANAGED_EXECUTION_PHYSICAL);
    spec->set_logical_node_count(1);
    spec->set_participates_in_placement(true);
    spec->set_external_network_bandwidth_bytes_per_sec(10000000000ULL);
    auto* group = spec->add_device_groups();
    group->set_name("hdd");
    group->set_kind(zb::rpc::MANAGED_DEVICE_HDD);
    group->set_device_count(1);
    group->set_max_concurrency(1);
    group->set_per_device_write_bandwidth_bytes_per_sec(write_bandwidth);
    auto* power = spec->mutable_power();
    power->set_peak_milliwatts(1000000);
    power->set_medium_milliwatts(300000);
    power->set_standby_milliwatts(100000);
    node->set_lifecycle(zb::rpc::MANAGED_LIFECYCLE_WORKING);
    node->set_health(zb::rpc::MANAGED_HEALTH_HEALTHY);
    node->set_administrative_state(zb::rpc::MANAGED_ADMIN_ENABLED);
    node->set_service_mode(zb::rpc::MANAGED_SERVICE_READ_WRITE);
    node->set_power_level(zb::rpc::MANAGED_POWER_PEAK);
    node->set_actuated_power_level(zb::rpc::MANAGED_POWER_PEAK);
    node->set_service_address("127.0.0.1:8000");
    auto* device = node->add_inventory_devices();
    device->set_device_id("disk-01");
    device->set_kind(zb::rpc::MANAGED_DEVICE_HDD);
    device->set_capacity_bytes(1000000000000ULL);
    device->set_free_bytes(900000000000ULL);
    device->set_is_healthy(true);
    return entry;
}

zb::rpc::CmsNodeCatalogEntry MakeBackpressuredOptical() {
    auto entry = MakeStorage("optical-a", 100000000ULL);
    auto* node = entry.mutable_managed_view();
    node->mutable_spec()->set_kind(zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY);
    node->mutable_spec()->mutable_device_groups(0)->set_kind(
        zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE);
    node->mutable_inventory_devices(0)->set_kind(zb::rpc::MANAGED_DEVICE_DISC);
    node->mutable_optical_library()->set_observed(true);
    node->mutable_optical_library()->set_total_slots(10000);
    node->mutable_optical_library()->set_staging_capacity_bytes(1000);
    node->mutable_optical_library()->set_staging_used_bytes(800);
    return entry;
}

} // namespace

int main() {
    zb::mds::NodeStateCache cache({});
    zb::mds::PGManager::Options options;
    options.pg_count = 16;
    options.replica = 1;
    zb::mds::PGManager pg(options);
    zb::mds::CmsNodeRegistry registry(&cache, &pg);

    std::vector<zb::rpc::CmsNodeCatalogEntry> proposal;
    proposal.push_back(MakeStorage("fast", 1000000000ULL));
    proposal.push_back(MakeStorage("slow", 100000000ULL));
    uint64_t cms_generation = 0;
    std::vector<zb::rpc::CmsNodeCompactIdAssignment> assignments;
    std::string error;
    if (!Expect(registry.Commit(1, proposal, &cms_generation, &assignments, &error), error) ||
        !Expect(assignments.size() == 2, "CMS assigns every node") ||
        !Expect(assignments[0].compact_id() != assignments[1].compact_id(),
                "CMS storage compact IDs are unique")) {
        return 1;
    }

    const auto cache_view = cache.Snapshot();
    if (!Expect(cache_view.size() == 2, "only CMS commit changes placement cache") ||
        !Expect(cache_view[0].weight > cache_view[1].weight,
                "configured write performance becomes allocation weight") ||
        !Expect(registry.IsWriteAdmitted("fast") && cache.IsWriteAdmitted("fast"),
                "CMS and placement cache expose the same write-admission decision")) {
        return 1;
    }
    size_t fast = 0;
    size_t slow = 0;
    for (const auto& selected : cache.PickNodes(40)) {
        if (selected.node_id == "fast") ++fast;
        if (selected.node_id == "slow") ++slow;
    }
    if (!Expect(fast > slow, "real allocator consumes CMS performance weight")) return 1;

    error.clear();
    if (!Expect(!registry.Commit(2, {proposal[0]}, nullptr, nullptr, &error),
                "CMS rejects a proposal that silently drops a member")) {
        return 1;
    }

    proposal.push_back(MakeBackpressuredOptical());
    error.clear();
    if (!Expect(registry.Commit(2, proposal, &cms_generation, &assignments, &error), error)) {
        return 1;
    }
    bool optical_blocked = false;
    for (const auto& node : cache.Snapshot()) {
        if (node.node_id == "optical-a") optical_blocked = !node.allocatable;
    }
    if (!Expect(optical_blocked, "CMS enforces the documented 80% L2 backpressure gate")) {
        return 1;
    }
    if (!Expect(!registry.IsWriteAdmitted("optical-a") &&
                    !cache.IsWriteAdmitted("optical-a-v0"),
                "backpressured optical member and derived virtual id reject writes")) {
        return 1;
    }

    error.clear();
    if (!Expect(!registry.Commit(1, proposal, nullptr, nullptr, &error),
                "CMS rejects stale Scheduler generations")) {
        return 1;
    }

    const auto unique = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path db_path =
        std::filesystem::temp_directory_path() /
        ("zbstorage-cms-registry-test-" + std::to_string(unique));
    {
        zb::mds::RocksMetaStore durable_store;
        error.clear();
        if (!Expect(durable_store.Open(db_path.string(), &error), error)) return 1;
        zb::mds::NodeStateCache first_cache({});
        zb::mds::PGManager first_pg(options);
        zb::mds::CmsNodeRegistry first(&first_cache, &first_pg, &durable_store);
        std::vector<zb::rpc::CmsNodeCompactIdAssignment> original_ids;
        if (!Expect(first.Commit(9, proposal, nullptr, &original_ids, &error), error)) return 1;

        zb::mds::NodeStateCache restored_cache({});
        zb::mds::PGManager restored_pg(options);
        zb::mds::CmsNodeRegistry restored(&restored_cache, &restored_pg, &durable_store);
        if (!Expect(restored.Restore(&error), error)) return 1;
        uint64_t restored_scheduler_generation = 0;
        const auto restored_nodes = restored.Snapshot(nullptr, &restored_scheduler_generation);
        if (!Expect(restored_scheduler_generation == 9,
                    "CMS restores the committed Scheduler generation") ||
            !Expect(restored_nodes.size() == proposal.size(),
                    "CMS restores every committed member") ||
            !Expect(restored_nodes[0].managed_view().compact_id() == original_ids[0].compact_id(),
                    "CMS compact IDs survive process reconstruction") ||
            !Expect(restored_cache.IsWriteAdmitted("fast"),
                    "restored catalog republishes the placement view")) {
            return 1;
        }
    }
    std::error_code cleanup_error;
    std::filesystem::remove_all(db_path, cleanup_error);

    std::cout << "cms_node_registry_test passed\n";
    return 0;
}
