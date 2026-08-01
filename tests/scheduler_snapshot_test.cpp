#include "scheduler/persistence/ManagedNodeSnapshotStore.h"

#include <chrono>
#include <filesystem>
#include <iostream>

int main() {
    using namespace zb::scheduler;
    const auto unique = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path path = std::filesystem::temp_directory_path() /
                                       ("zbstorage-managed-" + std::to_string(unique) + ".pb");
    ManagedNodeSnapshotStore store(path.string());
    NodePowerManager manager;
    std::string error;
    ManagedNodeProfile profile = DefaultMetadataNodeProfile("snapshot-mds");
    profile.technology_generation = 7;
    profile.device_groups.front().per_device_read_bandwidth_bytes_per_sec = 6000000000ULL;
    profile.device_groups.front().per_device_write_bandwidth_bytes_per_sec = 4000000000ULL;
    profile.reliability.model = zb::storage_model::ReliabilityModel::kBathtubCurve;
    profile.reliability.early_failure_window_ms = 123;
    profile.reliability.wearout_start_age_ms = 456;
    profile.reliability.early_failure_rate_fit = 7;
    profile.reliability.useful_life_failure_rate_fit = 8;
    profile.reliability.wearout_failure_rate_fit = 9;
    if (!manager.RegisterNode(profile, 1000, 17, &error) ||
        !manager.ActivateNode(profile.node_id, 1000, &error)) {
        std::cerr << "FAIL setup: " << error << std::endl;
        return 1;
    }
    NodeInventorySnapshot inventory;
    inventory.node_id = profile.node_id;
    inventory.generation = 4;
    inventory.observed_at_ms = 1100;
    inventory.devices.push_back({"nvme-0", DeviceKind::kSsd, 1000, 400, true,
                                 6000000000ULL, 4000000000ULL, 123, 1100});
    if (!manager.ReportHeartbeat(profile.node_id, "127.0.0.1:9000", inventory, 1100, &error)) {
        std::cerr << "FAIL heartbeat: " << error << std::endl;
        return 2;
    }
    NodeAccessEvent event;
    event.event_time_us = 1200000;
    event.is_write = true;
    event.bytes = 1024;
    if (!manager.ReportAccessEvent(profile.node_id, event, &error) ||
        manager.Tick(1300) == 0 ||
        !store.Save(manager, 1300, &error)) {
        std::cerr << "FAIL save: " << error << std::endl;
        return 2;
    }

    NodePowerManager restored;
    bool loaded = false;
    if (!store.Load(&restored, 2000, &loaded, &error) || !loaded) {
        std::cerr << "FAIL load: " << error << std::endl;
        return 3;
    }
    ManagedNodeRuntime node;
    if (!restored.GetNode(profile.node_id, &node) ||
        node.access_operation_count != 1 || node.last_energy_update_ms != 2000 ||
        node.profile.device_groups.front().per_device_read_bandwidth_bytes_per_sec != 6000000000ULL ||
        node.profile.device_groups.front().per_device_write_bandwidth_bytes_per_sec != 4000000000ULL ||
        node.profile.reliability.model != zb::storage_model::ReliabilityModel::kBathtubCurve ||
        node.profile.reliability.early_failure_window_ms != 123 ||
        node.profile.reliability.wearout_start_age_ms != 456 ||
        node.profile.reliability.wearout_failure_rate_fit != 9 ||
        node.profile.technology_generation != 7 ||
        node.identity.service_address != "127.0.0.1:9000" ||
        node.inventory.generation != 4 || node.inventory.devices.size() != 1 ||
        node.inventory.devices.front().accumulated_write_bytes != 123 ||
        node.accumulated_write_bytes != 1147 ||
        node.actuated_energy_microjoules != 40000000ULL ||
        node.last_actuated_energy_update_ms != 2000 ||
        restored.MetricsHistory(0, 0, 10).empty() ||
        restored.generation() != manager.generation()) {
        std::cerr << "FAIL restored state" << std::endl;
        return 4;
    }
    std::error_code ignored;
    std::filesystem::remove(path, ignored);
    std::cout << "PASS managed Scheduler snapshot" << std::endl;
    return 0;
}
