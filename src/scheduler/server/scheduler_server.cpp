#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
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

DEFINE_string(config, "", "Path to Scheduler config file");
DEFINE_int32(port, 9100, "Port for Scheduler brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

namespace {

uint64_t NowMs() {
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
}

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
    zb::scheduler::ShellNodeActuator actuator(cfg.start_cmd_template,
                                              cfg.stop_cmd_template,
                                              cfg.reboot_cmd_template);
    zb::scheduler::LifecycleManager lifecycle(&state, &actuator);
    zb::scheduler::BrpcObjectMigrationBackend migration_backend(
        cfg.mds_address, cfg.migration_rpc_timeout_ms, cfg.migration_copy_chunk_bytes);
    zb::scheduler::DrainMigrationCoordinator migration(
        &state, &node_manager, &lifecycle, &migration_backend,
        cfg.migration_retry_base_ms, cfg.migration_max_retries);
    zb::scheduler::SchedulerServiceImpl service(&state, &lifecycle, &node_manager, &migration);
    zb::scheduler::CmsNodeCatalogPublisher cms_publisher(
        cfg.mds_address, static_cast<uint32_t>(cfg.migration_rpc_timeout_ms));
    zb::scheduler::ShellManagedPowerActuator power_actuator(
        cfg.power_wake_cmd_template, cfg.power_standby_cmd_template, cfg.power_off_cmd_template);
    zb::scheduler::PowerActuationController power_controller(
        &node_manager, &state, &power_actuator,
        cfg.power_actuation_enabled, cfg.power_actuation_retry_ms);

    std::atomic<bool> stop{false};
    std::thread tick_thread([&]() {
        uint64_t interval_ms = cfg.tick_interval_ms > 0 ? cfg.tick_interval_ms : 1000;
        while (!stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
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
            if (!cms_publisher.Publish(&state, &node_manager, &cms_error)) {
                std::cerr << "Failed to commit Scheduler node catalog to CMS: "
                          << cms_error << std::endl;
            }
        }
    });
    std::thread snapshot_thread;
    if ((!cfg.cluster_view_snapshot_path.empty() || !cfg.managed_state_snapshot_path.empty()) &&
        cfg.cluster_view_snapshot_interval_ms > 0) {
        snapshot_thread = std::thread([&]() {
            const uint64_t interval_ms = cfg.cluster_view_snapshot_interval_ms;
            while (!stop.load()) {
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

    server.RunUntilAskedToQuit();
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
