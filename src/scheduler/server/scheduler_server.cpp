#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <set>
#include <thread>
#include <vector>

#include "../config/SchedulerConfig.h"
#include "../cms/CmsNodeCatalogPublisher.h"
#include "../health/FailureDetector.h"
#include "../lifecycle/LifecycleManager.h"
#include "../lifecycle/NodeActuator.h"
#include "../migration/BrpcObjectMigrationBackend.h"
#include "../migration/DrainMigrationCoordinator.h"
#include "../model/ClusterState.h"
#include "../power/NodePowerManager.h"
#include "../power/PowerActuationController.h"
#include "../persistence/ManagedNodeSnapshotStore.h"
#include "../service/SchedulerServiceImpl.h"
#include "../simulation/service/InProcessDiskService.h"
#include "../simulation/service/InProcessOpticalService.h"
#include "data_node/virtual_node/config/VirtualNodeConfig.h"

DEFINE_string(config, "", "Path to Scheduler config file");
DEFINE_int32(port, 9100, "Port for Scheduler brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

namespace {

volatile std::sig_atomic_t reload_models_requested = 0;

void RequestModelReload(int) {
    reload_models_requested = 1;
}

uint64_t NowMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

class SimulationSafeNodeActuator final : public zb::scheduler::NodeActuator {
public:
    SimulationSafeNodeActuator(zb::scheduler::NodeActuator* physical,
                               zb::scheduler::InProcessDiskService* simulation,
                               zb::scheduler::InProcessOpticalService* optical,
                               zb::scheduler::NodePowerManager* manager)
        : physical_(physical), simulation_(simulation), optical_(optical),
          manager_(manager) {}

    zb::scheduler::ActuatorResult StartNode(const std::string& id,
                                            const std::string& address) override {
        if (IsInternal(id)) {
            if (IsRetired(id)) return {false, "retired logical node cannot start"};
            std::string error;
            if (!manager_->SetAdministrativeState(
                    id, zb::storage_model::AdministrativeState::kEnabled, &error))
                return {false, "logical node start failed: " + error};
            if (!manager_->RequestPowerLevel(id, zb::scheduler::PowerLevel::kMedium,
                                             NowMs(), true, &error)) {
                manager_->SetAdministrativeState(
                    id, zb::storage_model::AdministrativeState::kDisabled, nullptr);
                return {false, "logical node start failed: " + error};
            }
            // Disk route remains closed until the next fresh inventory and
            // successful CMS publication. No physical power command runs.
            return {true, "logical node started; catalog confirmation pending"};
        }
        return physical_->StartNode(id, address);
    }
    zb::scheduler::ActuatorResult StopNode(const std::string& id,
                                           const std::string& address,
                                           bool force) override {
        if (IsInternal(id)) {
            if (IsRetired(id)) return {false, "retired logical node cannot stop"};
            std::string error;
            if (!manager_->SetAdministrativeState(
                    id, zb::storage_model::AdministrativeState::kDisabled, &error))
                return {false, "logical node stop failed: " + error};
            if (!manager_->RequestPowerLevel(id, zb::scheduler::PowerLevel::kOff,
                                             NowMs(), force, &error)) {
                manager_->SetAdministrativeState(
                    id, zb::storage_model::AdministrativeState::kEnabled, nullptr);
                return {false, "logical node stop failed: " + error};
            }
            if (simulation_) simulation_->RevokeCatalog(id);
            if (optical_) optical_->RevokeCatalog(id);
            return {true, "logical node stopped without a hardware command"};
        }
        return physical_->StopNode(id, address, force);
    }
    zb::scheduler::ActuatorResult RebootNode(const std::string& id,
                                             const std::string& address) override {
        if (IsInternal(id)) {
            if (IsRetired(id)) return {false, "retired logical node cannot reboot"};
            if (simulation_) simulation_->RevokeCatalog(id);
            if (optical_) optical_->RevokeCatalog(id);
            std::string error;
            if (!manager_->RequestPowerLevel(id, zb::scheduler::PowerLevel::kOff,
                                             NowMs(), true, &error) ||
                !manager_->RequestPowerLevel(id, zb::scheduler::PowerLevel::kMedium,
                                             NowMs(), true, &error))
                return {false, "logical node reboot failed: " + error};
            return {true, "logical node rebooted; catalog confirmation pending"};
        }
        return physical_->RebootNode(id, address);
    }

private:
    bool IsInternal(const std::string& id) const {
        return (simulation_ && simulation_->OwnsNodeIdentity(id)) ||
               (optical_ && optical_->OwnsNodeIdentity(id));
    }
    bool IsRetired(const std::string& id) const {
        zb::scheduler::ManagedNodeRuntime runtime;
        return manager_ && manager_->GetNode(id, &runtime) &&
               runtime.lifecycle == zb::scheduler::LifecycleState::kRetired;
    }
    zb::scheduler::NodeActuator* physical_{};
    zb::scheduler::InProcessDiskService* simulation_{};
    zb::scheduler::InProcessOpticalService* optical_{};
    zb::scheduler::NodePowerManager* manager_{};
};

void PersistClusterViewSnapshot(const std::string& path, zb::scheduler::ClusterState* state) {
    if (path.empty() || !state) {
        return;
    }

    uint64_t generation = 0;
    std::vector<zb::scheduler::NodeState> nodes;
    state->Snapshot(0, &generation, &nodes);

    std::error_code ec;
    const std::filesystem::path snapshot_path(path);
    if (!snapshot_path.parent_path().empty()) {
        std::filesystem::create_directories(snapshot_path.parent_path(), ec);
        if (ec) {
            std::cerr << "Failed to create scheduler snapshot directory: " << ec.message() << std::endl;
            return;
        }
    }

    const std::filesystem::path tmp_path = snapshot_path.string() + ".tmp";
    std::ofstream out(tmp_path, std::ios::trunc);
    if (!out) {
        std::cerr << "Failed to open scheduler snapshot file: " << tmp_path.string() << std::endl;
        return;
    }

    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    out << "scheduler_cluster_view_v1\n";
    out << "snapshot_ts_ms=" << now_ms << '\n';
    out << "generation=" << generation << '\n';
    out << "node_count=" << nodes.size() << '\n';
    for (const auto& node : nodes) {
        out << "[node]\n";
        out << "node_id=" << node.node_id << '\n';
        out << "node_type=" << static_cast<int>(node.node_type) << '\n';
        out << "address=" << node.address << '\n';
        out << "weight=" << node.weight << '\n';
        out << "virtual_node_count=" << node.virtual_node_count << '\n';
        out << "group_id=" << node.group_id << '\n';
        out << "role=" << static_cast<int>(node.role) << '\n';
        out << "epoch=" << node.epoch << '\n';
        out << "applied_lsn=" << node.applied_lsn << '\n';
        out << "peer_node_id=" << node.peer_node_id << '\n';
        out << "peer_address=" << node.peer_address << '\n';
        out << "sync_ready=" << (node.sync_ready ? 1 : 0) << '\n';
        out << "health_state=" << static_cast<int>(node.health_state) << '\n';
        out << "admin_state=" << static_cast<int>(node.admin_state) << '\n';
        out << "power_state=" << static_cast<int>(node.power_state) << '\n';
        out << "desired_admin_state=" << static_cast<int>(node.desired_admin_state) << '\n';
        out << "desired_power_state=" << static_cast<int>(node.desired_power_state) << '\n';
        out << "last_heartbeat_ms=" << node.last_heartbeat_ms << '\n';
        out << "disk_count=" << node.disks.size() << '\n';
        for (const auto& item : node.disks) {
            const auto& disk = item.second;
            out << "[disk]\n";
            out << "disk_id=" << disk.disk_id << '\n';
            out << "capacity_bytes=" << disk.capacity_bytes << '\n';
            out << "free_bytes=" << disk.free_bytes << '\n';
            out << "used_bytes=" << zb::storage_model::DeviceUsedBytes(disk) << '\n';
            out << "is_healthy=" << (disk.is_healthy ? 1 : 0) << '\n';
            out << "last_update_ms=" << disk.last_update_ms << '\n';
        }
    }
    out.close();
    if (!out) {
        std::cerr << "Failed to flush scheduler snapshot file: " << tmp_path.string() << std::endl;
        return;
    }

    std::filesystem::rename(tmp_path, snapshot_path, ec);
    if (ec) {
        std::filesystem::remove(snapshot_path, ec);
        ec.clear();
        std::filesystem::rename(tmp_path, snapshot_path, ec);
        if (ec) {
            std::cerr << "Failed to finalize scheduler snapshot file: " << ec.message() << std::endl;
        }
    }
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string error;
    zb::scheduler::SchedulerConfig cfg = zb::scheduler::SchedulerConfig::LoadFromFile(FLAGS_config, &error);
    if (!error.empty()) {
        std::cerr << "Failed to load config: " << error << std::endl;
        return 1;
    }

    zb::scheduler::FailureDetector detector(cfg.suspect_timeout_ms, cfg.dead_timeout_ms);
    zb::scheduler::PowerPolicy power_policy;
    power_policy.peak_utilization = cfg.power_peak_utilization;
    power_policy.peak_queue_depth = cfg.power_peak_queue_depth;
    power_policy.metrics_freshness_ms = cfg.power_metrics_freshness_ms;
    power_policy.standby_after_ms = cfg.power_standby_after_ms;
    power_policy.off_after_ms = cfg.power_off_after_ms;
    power_policy.minimum_residency_ms = cfg.power_minimum_residency_ms;
    power_policy.allow_off_with_resident_data = cfg.power_allow_off_with_resident_data;
    power_policy.min_online_storage_nodes = cfg.power_min_online_storage_nodes;
    power_policy.min_online_metadata_nodes = cfg.power_min_online_metadata_nodes;
    power_policy.min_online_optical_nodes = cfg.power_min_online_optical_nodes;
    power_policy.min_online_storage_capacity_bytes = cfg.power_min_online_storage_capacity_bytes;
    power_policy.min_online_storage_free_bytes = cfg.power_min_online_storage_free_bytes;
    power_policy.history_max_samples = cfg.metrics_history_max_samples;
    zb::scheduler::NodePowerManager node_manager(power_policy);
    zb::scheduler::ClusterState state(detector, &node_manager);
    zb::scheduler::ManagedNodeSnapshotStore managed_snapshot(cfg.managed_state_snapshot_path);
    bool managed_state_loaded = false;
    if (!managed_snapshot.Load(&node_manager, NowMs(), &managed_state_loaded, &error)) {
        std::cerr << "Failed to restore managed Scheduler state: " << error << std::endl;
        return 1;
    }
    if (managed_state_loaded) {
        std::cout << "Restored managed Scheduler state from "
                  << cfg.managed_state_snapshot_path << std::endl;
    }
    zb::scheduler::InProcessDiskService simulation_disks(&node_manager);
    zb::scheduler::InProcessOpticalService simulation_optical(
        &node_manager, cfg.mds_address, cfg.simulation_data_address, &simulation_disks);
    std::vector<zb::virtual_node::VirtualNodeConfig> simulated_configs;
    std::vector<zb::scheduler::OpticalReadConfig> simulated_optical_configs;
    auto register_or_restore_model = [&](const zb::scheduler::ManagedNodeProfile& profile,
                                         std::string* registration_error) {
        zb::scheduler::ManagedNodeRuntime existing;
        if (!node_manager.GetNode(profile.node_id, &existing)) {
            return node_manager.RegisterNode(profile, NowMs(),
                                             std::hash<std::string>{}(profile.node_id),
                                             registration_error);
        }
        const auto& saved = existing.profile;
        // A retired configured model is a durable tombstone. Reopen its local
        // control state for identity validation, but never re-register or
        // report it as placement-ready after restart.
        bool compatible =
            saved.kind == profile.kind && saved.execution_mode == profile.execution_mode &&
            saved.instance_generation == profile.instance_generation &&
            saved.logical_node_count == profile.logical_node_count &&
            saved.participates_in_placement == profile.participates_in_placement &&
            saved.external_network_bandwidth_bytes_per_sec ==
                profile.external_network_bandwidth_bytes_per_sec &&
            saved.fixed_access_latency_us == profile.fixed_access_latency_us &&
            saved.optical_load_latency_us == profile.optical_load_latency_us &&
            saved.optical_seek_latency_us == profile.optical_seek_latency_us &&
            saved.device_groups.size() == profile.device_groups.size();
        if (compatible) {
            for (size_t i = 0; i < profile.device_groups.size(); ++i) {
                const auto& a = saved.device_groups[i];
                const auto& b = profile.device_groups[i];
                if (a.name != b.name || a.kind != b.kind ||
                    a.device_count != b.device_count ||
                    a.device_capacity_bytes != b.device_capacity_bytes ||
                    a.max_concurrency != b.max_concurrency ||
                    a.per_device_bandwidth_bytes_per_sec !=
                        b.per_device_bandwidth_bytes_per_sec ||
                    a.per_device_read_bandwidth_bytes_per_sec !=
                        b.per_device_read_bandwidth_bytes_per_sec ||
                    a.per_device_write_bandwidth_bytes_per_sec !=
                        b.per_device_write_bandwidth_bytes_per_sec) {
                    compatible = false;
                    break;
                }
            }
        }
        if (!compatible && registration_error)
            *registration_error = "restored logical node profile differs from configuration: " +
                                  profile.node_id;
        return compatible;
    };
    auto add_disk_model = [&](const std::string& config_path, bool startup_restore) {
        error.clear();
        zb::virtual_node::VirtualNodeConfig disk_cfg =
            zb::virtual_node::VirtualNodeConfig::LoadFromFile(config_path, &error);
        if (!error.empty()) {
            std::cerr << "Failed to load simulation disk config: " << error << std::endl;
            return false;
        }
        if (disk_cfg.archive_meta_dir.empty()) {
            std::cerr << "ARCHIVE_META_DIR is required for in-process disk "
                      << disk_cfg.node_id << std::endl;
            return false;
        }
        zb::scheduler::ManagedNodeRuntime old_disk;
        if (!startup_restore && node_manager.GetNode(disk_cfg.node_id, &old_disk) &&
            old_disk.lifecycle == zb::scheduler::LifecycleState::kRetired) {
            std::cerr << "Retired logical disk identity cannot be hot-added: "
                      << disk_cfg.node_id << std::endl;
            return false;
        }
        if (!std::filesystem::path(disk_cfg.archive_meta_dir).is_absolute()) {
            disk_cfg.archive_meta_dir =
                (std::filesystem::path(config_path).parent_path() /
                 disk_cfg.archive_meta_dir).lexically_normal().string();
        }
        disk_cfg.node_address = cfg.simulation_data_address;
        if (simulation_optical.GetModel(disk_cfg.node_id)) {
            error = "duplicate node_id across media: " + disk_cfg.node_id;
            std::cerr << "Failed to add simulation disk: " << error << std::endl;
            return false;
        }
        if (!simulation_disks.AddNode(disk_cfg, &error)) {
            std::cerr << "Failed to add simulation disk: " << error << std::endl;
            return false;
        }
        auto profile = zb::scheduler::DefaultStorageNodeProfile(disk_cfg.node_id);
        profile.execution_mode = zb::scheduler::ExecutionMode::kVirtual;
        profile.instance_generation = disk_cfg.instance_generation;
        profile.logical_node_count = 1;
        profile.fixed_access_latency_us =
            static_cast<uint64_t>(std::max(disk_cfg.read_base_latency_ms,
                                           disk_cfg.write_base_latency_ms)) * 1000;
        profile.device_groups.clear();
        zb::scheduler::DeviceGroup group;
        group.name = "simulated-hdd";
        group.kind = zb::scheduler::DeviceKind::kHdd;
        group.device_capacity_bytes = disk_cfg.disk_capacity_bytes;
        group.device_count = static_cast<uint32_t>(disk_cfg.disk_ids.size());
        group.max_concurrency = std::min(group.device_count, disk_cfg.max_concurrent_io);
        // The runtime enforces a node-wide budget, while DeviceGroup expresses
        // bandwidth per simultaneously usable device. Divide to keep the CMS
        // estimate aligned with the configured aggregate rather than multiplying
        // the same budget by the number of disks.
        const auto per_device = [count = group.max_concurrency](uint64_t total) {
            return total / count + (total % count != 0);
        };
        group.per_device_read_bandwidth_bytes_per_sec =
            per_device(disk_cfg.read_bytes_per_sec);
        group.per_device_write_bandwidth_bytes_per_sec =
            per_device(disk_cfg.write_bytes_per_sec);
        group.per_device_bandwidth_bytes_per_sec =
            std::max(group.per_device_read_bandwidth_bytes_per_sec,
                     group.per_device_write_bandwidth_bytes_per_sec);
        profile.device_groups.push_back(group);
        if (!register_or_restore_model(profile, &error)) {
            simulation_disks.DiscardUnpublishedNode(disk_cfg.node_id);
            std::cerr << "Failed to register simulation disk: " << error << std::endl;
            return false;
        }
        simulated_configs.push_back(std::move(disk_cfg));
        return true;
    };
    auto add_optical_model = [&](const std::string& config_path, bool startup_restore) {
        error.clear();
        zb::scheduler::OpticalReadConfig optical_cfg =
            zb::scheduler::OpticalReadConfig::LoadFromFile(config_path, &error);
        zb::scheduler::ManagedNodeRuntime old_optical;
        if (error.empty() && !startup_restore &&
            node_manager.GetNode(optical_cfg.node_id, &old_optical) &&
            old_optical.lifecycle == zb::scheduler::LifecycleState::kRetired) {
            std::cerr << "Retired logical optical identity cannot be hot-added: "
                      << optical_cfg.node_id << std::endl;
            return false;
        }
        if (!optical_cfg.state_dir.empty() &&
            !std::filesystem::path(optical_cfg.state_dir).is_absolute()) {
            optical_cfg.state_dir =
                (std::filesystem::path(config_path).parent_path() /
                 optical_cfg.state_dir).lexically_normal().string();
        }
        if (!error.empty() || simulation_disks.GetStorage(optical_cfg.node_id) ||
            !simulation_optical.AddNode(optical_cfg, &error)) {
            std::cerr << "Failed to add simulation optical node: "
                      << (error.empty() ? "duplicate node_id across media" : error) << std::endl;
            return false;
        }
        auto profile = zb::scheduler::DefaultOpticalLibraryProfile(optical_cfg.node_id);
        profile.execution_mode = zb::scheduler::ExecutionMode::kVirtual;
        profile.instance_generation = optical_cfg.instance_generation;
        profile.logical_node_count = 1;
        profile.participates_in_placement = false;
        for (auto& group : profile.device_groups) {
            if (group.kind == zb::scheduler::DeviceKind::kHdd) {
                group.name = "model-cache";
                group.device_count = 1;
                group.device_capacity_bytes = optical_cfg.cache_capacity_bytes;
                group.max_concurrency = 1;
                group.per_device_read_bandwidth_bytes_per_sec =
                    optical_cfg.cache_read_bytes_per_sec;
                group.per_device_bandwidth_bytes_per_sec =
                    optical_cfg.cache_read_bytes_per_sec;
            } else if (group.kind == zb::scheduler::DeviceKind::kDisc) {
                group.name = "model-disc";
                group.device_count = optical_cfg.disc_count;
                group.device_capacity_bytes = optical_cfg.disc_capacity_bytes;
            } else if (group.kind == zb::scheduler::DeviceKind::kOpticalDrive) {
                group.device_count = optical_cfg.drive_count;
                group.max_concurrency = optical_cfg.drive_count;
                group.per_device_read_bandwidth_bytes_per_sec = optical_cfg.read_bytes_per_sec;
                group.per_device_bandwidth_bytes_per_sec = optical_cfg.read_bytes_per_sec;
            }
        }
        profile.optical_load_latency_us =
            static_cast<uint64_t>(optical_cfg.load_latency_ms) * 1000;
        profile.optical_seek_latency_us =
            static_cast<uint64_t>(optical_cfg.seek_latency_ms) * 1000;
        if (!register_or_restore_model(profile, &error)) {
            simulation_optical.DiscardUnpublishedNode(optical_cfg.node_id);
            std::cerr << "Failed to register simulation optical node: " << error << std::endl;
            return false;
        }
        simulated_optical_configs.push_back(std::move(optical_cfg));
        return true;
    };
    std::set<std::string> loaded_disk_paths;
    std::set<std::string> loaded_optical_paths;
    for (const auto& config_path : cfg.simulation_disk_configs) {
        if (!add_disk_model(config_path, true)) return 1;
        loaded_disk_paths.insert(config_path);
    }
    for (const auto& config_path : cfg.simulation_optical_configs) {
        if (!add_optical_model(config_path, true)) return 1;
        loaded_optical_paths.insert(config_path);
    }
    auto reload_model_configs = [&]() {
        std::string reload_error;
        const auto refreshed = zb::scheduler::SchedulerConfig::LoadFromFile(
            FLAGS_config, &reload_error);
        if (!reload_error.empty()) {
            std::cerr << "Simulation model reload rejected: " << reload_error << std::endl;
            return;
        }
        if (refreshed.simulation_data_port != cfg.simulation_data_port ||
            refreshed.simulation_data_address != cfg.simulation_data_address) {
            std::cerr << "Simulation model reload rejected: data endpoint is immutable"
                      << std::endl;
            return;
        }
        for (const auto& path : refreshed.simulation_disk_configs) {
            if (loaded_disk_paths.count(path) != 0) continue;
            if (cfg.simulation_data_port == 0) {
                std::cerr << "Simulation model reload needs a data endpoint configured at startup"
                          << std::endl;
                break;
            }
            if (add_disk_model(path, false)) {
                loaded_disk_paths.insert(path);
                std::cout << "Added in-process disk model from " << path << std::endl;
            }
        }
        for (const auto& path : refreshed.simulation_optical_configs) {
            if (loaded_optical_paths.count(path) != 0) continue;
            if (cfg.simulation_data_port == 0) {
                std::cerr << "Simulation model reload needs a data endpoint configured at startup"
                          << std::endl;
                break;
            }
            if (add_optical_model(path, false)) {
                loaded_optical_paths.insert(path);
                std::cout << "Added in-process optical model from " << path << std::endl;
            }
        }
        // Removing a path from the config never tears down an instance. The
        // lifecycle RPC and authoritative CMS zero-reference check own drain.
    };
    zb::scheduler::ShellNodeActuator actuator(cfg.start_cmd_template,
                                              cfg.stop_cmd_template,
                                              cfg.reboot_cmd_template);
    SimulationSafeNodeActuator safe_actuator(&actuator, &simulation_disks,
                                             &simulation_optical, &node_manager);
    zb::scheduler::LifecycleManager lifecycle(&state, &safe_actuator);
    zb::scheduler::BrpcObjectMigrationBackend migration_backend(
        cfg.mds_address, cfg.migration_rpc_timeout_ms, cfg.migration_copy_chunk_bytes,
        &simulation_disks);
    zb::scheduler::DrainMigrationCoordinator migration(
        &state, &node_manager, &lifecycle, &migration_backend,
        cfg.migration_retry_base_ms, cfg.migration_max_retries);
    migration.SetLogicalSourceEmptyVerifier(
        [&](const std::string& node_id, std::string* verify_error) {
            return simulation_disks.VerifyEmpty(node_id, verify_error);
        });
    migration.SetLogicalEmptyDrainBegin(
        [&](const std::string& node_id, uint64_t now_ms, std::string* begin_error) {
            return simulation_disks.BeginDataDrain(node_id, now_ms, begin_error);
        });
    zb::scheduler::SchedulerServiceImpl service(&state, &lifecycle, &node_manager, &migration);
    zb::scheduler::CmsNodeCatalogPublisher cms_publisher(
        cfg.mds_address, static_cast<uint32_t>(cfg.migration_rpc_timeout_ms));
    zb::scheduler::ShellManagedPowerActuator power_actuator(
        cfg.power_wake_cmd_template, cfg.power_standby_cmd_template, cfg.power_off_cmd_template);
    zb::scheduler::PowerActuationController power_controller(
        &node_manager, &state, &power_actuator,
        cfg.power_actuation_enabled, cfg.power_actuation_retry_ms);

    std::vector<std::string> freshly_reported_opticals;
    auto report_simulated_disks = [&]() {
        std::vector<std::string> freshly_reported_disks;
        freshly_reported_opticals.clear();
        for (const auto& disk_cfg : simulated_configs) {
            zb::scheduler::ManagedNodeRuntime managed;
            if (!node_manager.GetNode(disk_cfg.node_id, &managed) ||
                managed.lifecycle == zb::scheduler::LifecycleState::kRetired)
                continue;
            auto* storage = simulation_disks.GetStorage(disk_cfg.node_id);
            auto* collector = simulation_disks.GetMetrics(disk_cfg.node_id);
            if (!storage || !collector) continue;
            zb::rpc::HeartbeatRequest heartbeat;
            heartbeat.set_node_id(disk_cfg.node_id);
            heartbeat.set_node_type(zb::rpc::NODE_VIRTUAL_POOL);
            heartbeat.set_address(cfg.simulation_data_address);
            heartbeat.set_virtual_node_count(1);
            heartbeat.set_role(zb::rpc::NODE_ROLE_PRIMARY);
            heartbeat.set_report_ts_ms(NowMs());
            heartbeat.set_readiness_reported(true);
            heartbeat.set_initialization_complete(true);
            heartbeat.set_metadata_ready(true);
            heartbeat.set_readiness_message("in-process simulated disk initialized");
            for (const auto& disk : storage->GetDiskReport().reports) {
                auto* out = heartbeat.add_disks();
                out->set_disk_id(disk.id);
                out->set_capacity_bytes(disk.capacity_bytes);
                out->set_free_bytes(disk.free_bytes);
                out->set_is_healthy(disk.is_healthy);
                out->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD);
                out->set_read_bandwidth_bytes_per_sec(
                    disk_cfg.read_bytes_per_sec / disk_cfg.disk_ids.size());
                out->set_write_bandwidth_bytes_per_sec(
                    disk_cfg.write_bytes_per_sec / disk_cfg.disk_ids.size());
            }
            const auto assignment = state.ReportHeartbeat(heartbeat);
            if (!assignment.success) {
                std::cerr << "Simulation disk state report failed for " << disk_cfg.node_id
                          << ": " << assignment.error << std::endl;
                continue;
            }
            freshly_reported_disks.push_back(disk_cfg.node_id);
            const auto snapshot = collector->TakeForReport(NowMs());
            if (snapshot.report_sequence == 0) continue;
            zb::scheduler::NodeAccessMetrics metrics;
            metrics.reporter_epoch = snapshot.reporter_epoch;
            metrics.report_sequence = snapshot.report_sequence;
            metrics.window_start_ms = snapshot.window_start_ms;
            metrics.window_end_ms = snapshot.window_end_ms;
            metrics.read_operations = snapshot.read_operations;
            metrics.write_operations = snapshot.write_operations;
            metrics.other_operations = snapshot.other_operations;
            metrics.read_bytes = snapshot.read_bytes;
            metrics.write_bytes = snapshot.write_bytes;
            metrics.active_requests = snapshot.active_requests;
            metrics.max_queue_depth = snapshot.max_queue_depth;
            metrics.busy_time_ms = snapshot.busy_time_ms;
            metrics.last_access_ms = snapshot.last_access_ms;
            if (node_manager.ReportMetrics(disk_cfg.node_id, metrics, &error)) {
                collector->AcknowledgeReport(snapshot.report_sequence);
            } else {
                std::cerr << "Simulation disk metrics failed for " << disk_cfg.node_id
                          << ": " << error << std::endl;
            }
        }
        for (const auto& optical_cfg : simulated_optical_configs) {
            zb::scheduler::ManagedNodeRuntime managed;
            if (!node_manager.GetNode(optical_cfg.node_id, &managed) ||
                managed.lifecycle == zb::scheduler::LifecycleState::kRetired)
                continue;
            auto* optical_model = simulation_optical.GetModel(optical_cfg.node_id);
            if (!optical_model) continue;
            const auto archive_progress = optical_model->AdvanceArchives();
            if (archive_progress.code != zb::scheduler::OpticalModelCode::kOk) {
                std::cerr << "Simulation optical archive progress failed for "
                          << optical_cfg.node_id << ": " << archive_progress.message << std::endl;
                continue;
            }
            std::string reconcile_error;
            if (!simulation_optical.ReconcileArchives(optical_cfg.node_id, &reconcile_error) &&
                !reconcile_error.empty()) {
                std::cerr << "Simulation optical MDS reconciliation pending for "
                          << optical_cfg.node_id << ": " << reconcile_error << std::endl;
            }
            zb::scheduler::OpticalNodeManagementSnapshot model_state;
            const uint64_t report_time_ms = NowMs();
            if (!simulation_optical.GetManagementSnapshot(
                    optical_cfg.node_id, report_time_ms, &model_state)) {
                std::cerr << "Simulation optical model unavailable for "
                          << optical_cfg.node_id << std::endl;
                continue;
            }
            // Report recovered local control inventory. This is not an MDS
            // location commit, so metadata_ready remains false and the node
            // stays Joining/excluded from placement.
            zb::rpc::HeartbeatRequest heartbeat;
            heartbeat.set_node_id(optical_cfg.node_id);
            heartbeat.set_node_type(zb::rpc::NODE_OPTICAL);
            heartbeat.set_address(cfg.simulation_data_address);
            heartbeat.set_report_ts_ms(report_time_ms);
            heartbeat.set_readiness_reported(true);
            heartbeat.set_initialization_complete(true);
            heartbeat.set_metadata_ready(true);
            heartbeat.set_readiness_message(
                "durable modeled optical inventory and MDS completion protocol initialized");
            auto* disc = heartbeat.add_disks();
            disc->set_disk_id("model-disc-pool");
            disc->set_device_kind(zb::rpc::MANAGED_DEVICE_DISC);
            disc->set_capacity_bytes(model_state.disc_capacity_bytes);
            disc->set_free_bytes(model_state.disc_free_bytes);
            disc->set_is_healthy(true);
            disc->set_read_bandwidth_bytes_per_sec(
                optical_cfg.read_bytes_per_sec * optical_cfg.drive_count);
            auto* cache = heartbeat.add_disks();
            cache->set_disk_id("model-read-cache");
            cache->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD);
            cache->set_capacity_bytes(model_state.cache_capacity_bytes);
            cache->set_free_bytes(model_state.cache_free_bytes);
            cache->set_is_healthy(true);
            cache->set_read_bandwidth_bytes_per_sec(
                optical_cfg.cache_read_bytes_per_sec);
            const auto assignment = state.ReportHeartbeat(heartbeat);
            if (!assignment.success) {
                std::cerr << "Simulation optical state report failed for "
                          << optical_cfg.node_id << ": " << assignment.error << std::endl;
                continue;
            }
            if (!node_manager.ReportOpticalLibraryStatus(
                    optical_cfg.node_id, model_state.library, &error)) {
                std::cerr << "Simulation optical library report failed for "
                          << optical_cfg.node_id << ": " << error << std::endl;
                continue;
            }
            freshly_reported_opticals.push_back(optical_cfg.node_id);
            auto* collector = simulation_optical.GetMetrics(optical_cfg.node_id);
            zb::scheduler::ManagedNodeRuntime runtime;
            if (!collector || !node_manager.GetNode(optical_cfg.node_id, &runtime) ||
                runtime.lifecycle == zb::scheduler::LifecycleState::kJoining) continue;
            const auto snapshot = collector->TakeForReport(NowMs());
            if (snapshot.report_sequence == 0) continue;
            zb::scheduler::NodeAccessMetrics metrics;
            metrics.reporter_epoch = snapshot.reporter_epoch;
            metrics.report_sequence = snapshot.report_sequence;
            metrics.window_start_ms = snapshot.window_start_ms;
            metrics.window_end_ms = snapshot.window_end_ms;
            metrics.read_operations = snapshot.read_operations;
            metrics.write_operations = snapshot.write_operations;
            metrics.other_operations = snapshot.other_operations;
            metrics.read_bytes = snapshot.read_bytes;
            metrics.write_bytes = snapshot.write_bytes;
            metrics.active_requests = snapshot.active_requests;
            metrics.max_queue_depth = snapshot.max_queue_depth;
            metrics.busy_time_ms = snapshot.busy_time_ms;
            metrics.last_access_ms = snapshot.last_access_ms;
            if (node_manager.ReportMetrics(optical_cfg.node_id, metrics, &error)) {
                collector->AcknowledgeReport(snapshot.report_sequence);
            } else {
                std::cerr << "Simulation optical metrics failed for "
                          << optical_cfg.node_id << ": " << error << std::endl;
            }
        }
        return freshly_reported_disks;
    };

    std::atomic<bool> stop{false};
    std::atomic<bool> simulation_ready{false};
    std::atomic<bool> startup_complete{false};
    std::thread tick_thread([&]() {
        uint64_t interval_ms = cfg.tick_interval_ms > 0 ? cfg.tick_interval_ms : 1000;
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            if (!startup_complete.load()) continue;
            if (reload_models_requested != 0) {
                reload_models_requested = 0;
                reload_model_configs();
            }
            const auto freshly_reported_disks = simulation_ready.load()
                ? report_simulated_disks() : std::vector<std::string>{};
            state.TickHealth();
            const uint64_t now_ms = NowMs();
            node_manager.TickRealTime(now_ms);
            for (const auto& draining : node_manager.ListByLifecycle(
                     zb::scheduler::LifecycleState::kDraining)) {
                zb::scheduler::DrainMigrationTask existing;
                if (!migration.GetTask(draining.profile.node_id, &existing)) {
                    std::string migration_error;
                    if (!migration.Start(draining.profile.node_id, now_ms, &migration_error)) {
                        std::cerr << "Failed to start automatic lifetime drain for "
                                  << draining.profile.node_id << ": " << migration_error << std::endl;
                    }
                }
            }
            migration.Tick(now_ms);
            power_controller.Tick(now_ms);
            std::string cms_error;
            std::vector<std::string> committed_retired_ids;
            if (!cms_publisher.Publish(&state, &node_manager, &cms_error,
                                       &committed_retired_ids)) {
                std::cerr << "Failed to commit Scheduler node catalog to CMS: "
                          << cms_error << std::endl;
            } else {
                // Only a disk whose recovered inventory was reported in this
                // publish cycle may serve data. Restored readiness alone is
                // insufficient, even if a stale MDS location still exists.
                for (const auto& id : freshly_reported_disks)
                    simulation_disks.ConfirmCatalog(id);
                for (const auto& id : freshly_reported_opticals)
                    simulation_optical.ConfirmCatalog(id);
                for (const auto& id : committed_retired_ids) {
                    if (simulation_disks.ReleaseRetiredNode(id)) {
                        simulated_configs.erase(
                            std::remove_if(simulated_configs.begin(), simulated_configs.end(),
                                           [&](const auto& config) { return config.node_id == id; }),
                            simulated_configs.end());
                        std::cout << "Released retired logical disk: " << id << std::endl;
                    }
                    if (simulation_optical.ReleaseRetiredNode(id)) {
                        simulated_optical_configs.erase(
                            std::remove_if(simulated_optical_configs.begin(),
                                           simulated_optical_configs.end(),
                                           [&](const auto& config) { return config.node_id == id; }),
                            simulated_optical_configs.end());
                        std::cout << "Released retired logical optical library: "
                                  << id << std::endl;
                    }
                }
            }
        }
    });
    std::thread snapshot_thread;
    if ((!cfg.cluster_view_snapshot_path.empty() || !cfg.managed_state_snapshot_path.empty()) &&
        cfg.cluster_view_snapshot_interval_ms > 0) {
        snapshot_thread = std::thread([&]() {
            const uint64_t interval_ms = cfg.cluster_view_snapshot_interval_ms;
            while (!stop.load()) {
                if (!startup_complete.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
                    continue;
                }
                PersistClusterViewSnapshot(cfg.cluster_view_snapshot_path, &state);
                std::string snapshot_error;
                const uint64_t snapshot_time_ms = NowMs();
                node_manager.TickRealTime(snapshot_time_ms);
                if (!managed_snapshot.Save(node_manager, snapshot_time_ms, &snapshot_error)) {
                    std::cerr << "Failed to persist managed Scheduler state: "
                              << snapshot_error << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    }

    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add Scheduler service" << std::endl;
        stop.store(true);
        tick_thread.join();
        if (snapshot_thread.joinable()) {
            snapshot_thread.join();
        }
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;
    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start Scheduler server on port " << FLAGS_port << std::endl;
        stop.store(true);
        tick_thread.join();
        if (snapshot_thread.joinable()) {
            snapshot_thread.join();
        }
        return 1;
    }

    brpc::Server simulation_data_server;
    if (cfg.simulation_data_port != 0) {
        if (simulation_data_server.AddService(&simulation_disks,
                                               brpc::SERVER_DOESNT_OWN_SERVICE) != 0 ||
            simulation_data_server.AddService(&simulation_optical,
                                               brpc::SERVER_DOESNT_OWN_SERVICE) != 0 ||
            simulation_data_server.Start(cfg.simulation_data_port, &options) != 0) {
            std::cerr << "Failed to start in-process simulation data service on port "
                      << cfg.simulation_data_port << std::endl;
            server.Stop(0);
            server.Join();
            stop.store(true);
            tick_thread.join();
            if (snapshot_thread.joinable()) snapshot_thread.join();
            return 1;
        }
        simulation_ready.store(true);
        report_simulated_disks();
    }
    // Neither CMS publishing nor managed snapshots may expose a restored
    // logical node before its current-process inventory and data endpoint exist.
    startup_complete.store(true);

    std::signal(SIGHUP, RequestModelReload);

    server.RunUntilAskedToQuit();
    if (cfg.simulation_data_port != 0) {
        simulation_ready.store(false);
        simulation_data_server.Stop(0);
        simulation_data_server.Join();
    }
    stop.store(true);
    tick_thread.join();
    if (snapshot_thread.joinable()) {
        snapshot_thread.join();
    }
    std::string final_snapshot_error;
    const uint64_t final_snapshot_time_ms = NowMs();
    node_manager.TickRealTime(final_snapshot_time_ms);
    if (!managed_snapshot.Save(node_manager, final_snapshot_time_ms, &final_snapshot_error)) {
        std::cerr << "Failed to persist final managed Scheduler state: "
                  << final_snapshot_error << std::endl;
        return 1;
    }
    return 0;
}
