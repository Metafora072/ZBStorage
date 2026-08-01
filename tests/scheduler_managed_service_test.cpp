#include "scheduler/health/FailureDetector.h"
#include "scheduler/model/ClusterState.h"
#include "scheduler/power/NodePowerManager.h"
#include "scheduler/service/SchedulerServiceImpl.h"

#include <iostream>
#include <string>

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        return false;
    }
    return true;
}

bool StatusOk(const zb::rpc::SchedulerStatus& status, const std::string& operation) {
    return Expect(status.code() == zb::rpc::SCHED_OK,
                  operation + ": " + status.message());
}

} // namespace

int main() {
    zb::scheduler::FailureDetector detector(6000, 15000);
    zb::scheduler::PowerPolicy policy;
    policy.minimum_residency_ms = 0;
    zb::scheduler::NodePowerManager manager(policy);
    zb::scheduler::ClusterState cluster_state(detector, &manager);
    zb::scheduler::SchedulerServiceImpl service(&cluster_state, nullptr, &manager);

    zb::rpc::RegisterManagedNodeRequest register_request;
    auto* spec = register_request.mutable_spec();
    spec->set_node_id("sim-storage-1");
    spec->set_kind(zb::rpc::MANAGED_KIND_STORAGE);
    spec->set_execution_mode(zb::rpc::MANAGED_EXECUTION_SIMULATED);
    spec->set_logical_node_count(1);
    spec->set_participates_in_placement(false);
    spec->set_participates_in_placement_set(true);
    register_request.set_join_time_ms(1000);
    register_request.set_lifetime_seed(42);
    register_request.set_activate_immediately(true);
    zb::rpc::ManagedNodeReply register_reply;
    service.RegisterManagedNode(nullptr, &register_request, &register_reply, nullptr);
    if (!StatusOk(register_reply.status(), "RegisterManagedNode") ||
        !Expect(register_reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_WORKING,
                "registered node is working") ||
        !Expect(register_reply.node().capacity_bytes() == 432000000000000ULL,
                "default storage capacity")) {
        return 1;
    }

    zb::rpc::ReportNodeMetricsRequest metrics_request;
    metrics_request.set_node_id("sim-storage-1");
    metrics_request.set_window_start_ms(1000);
    metrics_request.set_window_end_ms(1100);
    metrics_request.set_read_operations(20);
    metrics_request.set_read_bytes(90000000);
    metrics_request.set_last_access_ms(1100);
    zb::rpc::ManagedNodeReply metrics_reply;
    service.ReportNodeMetrics(nullptr, &metrics_request, &metrics_reply, nullptr);
    if (!StatusOk(metrics_reply.status(), "ReportNodeMetrics") ||
        !Expect(metrics_reply.node().power_level() == zb::rpc::MANAGED_POWER_PEAK,
                "metrics drive peak power")) {
        return 2;
    }

    zb::rpc::HeartbeatRequest heartbeat;
    heartbeat.set_node_id("legacy-vpool");
    heartbeat.set_node_type(zb::rpc::NODE_VIRTUAL_POOL);
    heartbeat.set_virtual_node_count(99);
    heartbeat.set_address("127.0.0.1:29080");
    heartbeat.set_report_ts_ms(1200);
    heartbeat.set_readiness_reported(true);
    heartbeat.set_initialization_complete(true);
    heartbeat.set_metadata_ready(true);
    auto* disk = heartbeat.add_disks();
    disk->set_disk_id("disk0");
    disk->set_capacity_bytes(2000000000000ULL);
    disk->set_free_bytes(1999999999000ULL);
    disk->set_is_healthy(true);
    disk->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD);
    disk->set_read_bandwidth_bytes_per_sec(200000000ULL);
    disk->set_write_bandwidth_bytes_per_sec(150000000ULL);
    disk->set_accumulated_write_bytes(7000);
    zb::rpc::HeartbeatReply heartbeat_reply;
    service.ReportHeartbeat(nullptr, &heartbeat, &heartbeat_reply, nullptr);
    if (!StatusOk(heartbeat_reply.status(), "ReportHeartbeat")) {
        return 3;
    }

    zb::rpc::GetClusterViewRequest cluster_view_request;
    zb::rpc::GetClusterViewReply cluster_view_reply;
    service.GetClusterView(nullptr, &cluster_view_request, &cluster_view_reply, nullptr);
    const zb::rpc::NodeView* legacy_node = nullptr;
    for (const auto& node : cluster_view_reply.nodes()) {
        if (node.node_id() == "legacy-vpool") legacy_node = &node;
    }
    if (!StatusOk(cluster_view_reply.status(), "GetClusterView") ||
        !Expect(cluster_view_reply.nodes_size() == 2, "unified catalog nodes are visible") ||
        !Expect(legacy_node && legacy_node->disks_size() == 1, "heartbeat disk is visible") ||
        !Expect(legacy_node && legacy_node->disks(0).used_bytes() == 1000,
                "disk used bytes are derived from capacity and free space") ||
        !Expect(legacy_node && legacy_node->disks(0).write_bandwidth_bytes_per_sec() ==
                                   150000000ULL,
                "legacy view carries directional device bandwidth")) {
        return 3;
    }

    zb::rpc::GetManagedNodeRequest get_request;
    get_request.set_node_id("legacy-vpool");
    zb::rpc::ManagedNodeReply get_reply;
    service.GetManagedNode(nullptr, &get_request, &get_reply, nullptr);
    if (!StatusOk(get_reply.status(), "GetManagedNode") ||
        !Expect(get_reply.node().service_address() == "127.0.0.1:29080",
                "managed view carries service address") ||
        !Expect(get_reply.node().inventory_devices_size() == 1,
                "managed view carries observed inventory") ||
        !Expect(get_reply.node().observed_capacity_bytes() == 2000000000000ULL &&
                    get_reply.node().observed_used_bytes() == 1000,
                "managed view exposes observed capacity totals") ||
        !Expect(get_reply.node().accumulated_write_bytes() == 7000,
                "heartbeat write counter is retained")) {
        return 4;
    }

    zb::rpc::ListManagedNodesRequest list_request;
    list_request.set_include_retired(false);
    list_request.set_limit(10);
    zb::rpc::ListManagedNodesReply list_reply;
    service.ListManagedNodes(nullptr, &list_request, &list_reply, nullptr);
    if (!StatusOk(list_reply.status(), "ListManagedNodes") ||
        !Expect(list_reply.nodes_size() == 2, "explicit and heartbeat nodes are listed") ||
        !Expect(list_reply.summary().working_nodes() == 2, "two working nodes")) {
        return 4;
    }
    list_request.set_execution_mode(zb::rpc::MANAGED_EXECUTION_VIRTUAL);
    list_reply.Clear();
    service.ListManagedNodes(nullptr, &list_request, &list_reply, nullptr);
    if (!StatusOk(list_reply.status(), "ListManagedNodes filtered") ||
        !Expect(list_reply.nodes_size() == 1 &&
                    list_reply.nodes(0).spec().node_id() == "legacy-vpool",
                "execution-mode filter is applied")) {
        return 4;
    }
    list_request.set_execution_mode(zb::rpc::MANAGED_EXECUTION_UNKNOWN);

    zb::rpc::ReportNodeAccessEventRequest access_request;
    access_request.set_node_id("sim-storage-1");
    access_request.set_event_time_us(1250000);
    access_request.set_operation(zb::rpc::MANAGED_OPERATION_READ);
    access_request.set_bytes(4096);
    access_request.set_queue_depth(1);
    zb::rpc::ManagedNodeReply access_reply;
    service.ReportNodeAccessEvent(nullptr, &access_request, &access_reply, nullptr);
    if (!StatusOk(access_reply.status(), "ReportNodeAccessEvent") ||
        !Expect(access_reply.node().access_operation_count() == 21,
                "window metrics and trace events share one counter")) {
        return 5;
    }
    zb::rpc::AdvanceSimulationTimeRequest advance_request;
    advance_request.set_target_time_us(1260000);
    zb::rpc::AdvanceSimulationTimeReply advance_reply;
    service.AdvanceSimulationTime(nullptr, &advance_request, &advance_reply, nullptr);
    if (!StatusOk(advance_reply.status(), "AdvanceSimulationTime") ||
        !Expect(advance_reply.simulation_time_us() == 1260000,
                "simulation time advances monotonically")) {
        return 6;
    }

    zb::rpc::RemoveManagedNodeRequest remove_request;
    remove_request.set_node_id("sim-storage-1");
    remove_request.set_request_ts_ms(1300);
    zb::rpc::ManagedNodeReply remove_reply;
    service.RemoveManagedNode(nullptr, &remove_request, &remove_reply, nullptr);
    if (!StatusOk(remove_reply.status(), "Begin RemoveManagedNode") ||
        !Expect(remove_reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_DRAINING,
                "node enters draining")) {
        return 7;
    }
    zb::rpc::ReportNodeDrainProgressRequest drain_request;
    drain_request.set_node_id("sim-storage-1");
    drain_request.set_report_ts_ms(1350);
    drain_request.set_remaining_objects(0);
    drain_request.set_remaining_bytes(0);
    drain_request.set_active_requests(0);
    zb::rpc::ManagedNodeReply drain_reply;
    service.ReportNodeDrainProgress(nullptr, &drain_request, &drain_reply, nullptr);
    if (!StatusOk(drain_reply.status(), "ReportNodeDrainProgress") ||
        !Expect(drain_reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_EXITING,
                "drained node enters exiting")) {
        return 8;
    }
    remove_request.set_finalize(true);
    remove_request.set_request_ts_ms(1400);
    remove_reply.Clear();
    service.RemoveManagedNode(nullptr, &remove_request, &remove_reply, nullptr);
    if (!StatusOk(remove_reply.status(), "Finalize RemoveManagedNode") ||
        !Expect(remove_reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_RETIRED,
                "node enters retired")) {
        return 9;
    }

    list_request.set_include_retired(false);
    list_reply.Clear();
    service.ListManagedNodes(nullptr, &list_request, &list_reply, nullptr);
    if (!StatusOk(list_reply.status(), "List without retired") ||
        !Expect(list_reply.nodes_size() == 1, "retired node filtered")) {
        return 10;
    }
    list_request.set_include_retired(true);
    list_reply.Clear();
    service.ListManagedNodes(nullptr, &list_request, &list_reply, nullptr);
    if (!StatusOk(list_reply.status(), "List with retired") ||
        !Expect(list_reply.nodes_size() == 2, "retired node retained as tombstone") ||
        !Expect(list_reply.summary().retired_nodes() == 1, "retired summary")) {
        return 11;
    }

    zb::rpc::RegisterManagedNodeRequest replacement_request = register_request;
    replacement_request.mutable_spec()->set_node_id("sim-storage-2");
    replacement_request.mutable_spec()->set_technology_generation(2);
    replacement_request.set_replaces_node_id("sim-storage-1");
    replacement_request.set_join_time_ms(1450);
    zb::rpc::ManagedNodeReply replacement_reply;
    service.RegisterManagedNode(nullptr, &replacement_request, &replacement_reply, nullptr);
    if (!StatusOk(replacement_reply.status(), "Register replacement") ||
        !Expect(replacement_reply.node().predecessor_node_id() == "sim-storage-1",
                "replacement relationship is visible") ||
        !Expect(replacement_reply.node().spec().technology_generation() == 2,
                "replacement technology generation is visible")) {
        return 12;
    }

    zb::rpc::RegisterManagedNodeRequest optical_request;
    optical_request.mutable_spec()->set_node_id("optical-1");
    optical_request.mutable_spec()->set_kind(zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY);
    optical_request.mutable_spec()->set_execution_mode(zb::rpc::MANAGED_EXECUTION_SIMULATED);
    optical_request.mutable_spec()->set_logical_node_count(1);
    optical_request.set_join_time_ms(1460);
    optical_request.set_activate_immediately(true);
    zb::rpc::ManagedNodeReply optical_reply;
    service.RegisterManagedNode(nullptr, &optical_request, &optical_reply, nullptr);
    if (!StatusOk(optical_reply.status(), "Register optical library")) return 12;

    zb::rpc::ReportOpticalLibraryStatusRequest optical_status;
    optical_status.set_node_id("optical-1");
    auto* library = optical_status.mutable_status();
    library->set_observed(true);
    library->set_total_slots(10000);
    library->set_local_disc_count(2);
    library->set_recorded_disc_count(2);
    library->set_observed_at_ms(1470);
    optical_reply.Clear();
    service.ReportOpticalLibraryStatus(nullptr, &optical_status, &optical_reply, nullptr);
    if (!StatusOk(optical_reply.status(), "Report optical status") ||
        !Expect(optical_reply.node().service_mode() == zb::rpc::MANAGED_SERVICE_READ_ONLY,
                "fully recorded optical library becomes read-only")) {
        return 12;
    }

    zb::rpc::RemoveManagedNodeRequest optical_remove;
    optical_remove.set_node_id("optical-1");
    optical_remove.set_request_ts_ms(1480);
    service.RemoveManagedNode(nullptr, &optical_remove, &optical_reply, nullptr);
    zb::rpc::ReportNodeDrainProgressRequest optical_drain;
    optical_drain.set_node_id("optical-1");
    optical_drain.set_report_ts_ms(1490);
    service.ReportNodeDrainProgress(nullptr, &optical_drain, &optical_reply, nullptr);
    optical_remove.set_finalize(true);
    optical_remove.set_request_ts_ms(1500);
    optical_reply.Clear();
    service.RemoveManagedNode(nullptr, &optical_remove, &optical_reply, nullptr);
    if (!Expect(optical_reply.status().code() == zb::rpc::SCHED_INVALID_ARGUMENT,
                "optical retirement is blocked while discs remain")) {
        return 12;
    }
    library->set_local_disc_count(0);
    library->set_recorded_disc_count(0);
    library->set_observed_at_ms(1510);
    service.ReportOpticalLibraryStatus(nullptr, &optical_status, &optical_reply, nullptr);
    optical_remove.set_request_ts_ms(1520);
    service.RemoveManagedNode(nullptr, &optical_remove, &optical_reply, nullptr);
    if (!StatusOk(optical_reply.status(), "Retire empty optical library")) return 12;

    zb::rpc::BatchRegisterManagedNodesRequest batch_register;
    for (const std::string id : {"batch-sim-a", "batch-sim-b"}) {
        auto* item = batch_register.add_requests();
        *item = register_request;
        item->mutable_spec()->set_node_id(id);
        item->set_join_time_ms(1500);
    }
    zb::rpc::BatchManagedNodeReply batch_reply;
    service.BatchRegisterManagedNodes(nullptr, &batch_register, &batch_reply, nullptr);
    if (!StatusOk(batch_reply.status(), "BatchRegisterManagedNodes") ||
        !Expect(batch_reply.results_size() == 2, "two batch results") ||
        !StatusOk(batch_reply.results(0).status(), "batch item 0") ||
        !StatusOk(batch_reply.results(1).status(), "batch item 1")) {
        return 12;
    }

    zb::rpc::RunLongTermSimulationRequest run_request;
    run_request.set_target_time_us(2500000);
    run_request.set_step_us(100000);
    run_request.set_max_steps(20);
    zb::rpc::RunLongTermSimulationReply run_reply;
    service.RunLongTermSimulation(nullptr, &run_request, &run_reply, nullptr);
    if (!StatusOk(run_reply.status(), "RunLongTermSimulation") ||
        !Expect(run_reply.final_time_us() == 2500000, "long simulation reaches target") ||
        !Expect(run_reply.steps() > 1, "long simulation uses bounded steps")) {
        return 13;
    }

    zb::rpc::GetManagedMetricsHistoryRequest history_request;
    history_request.set_limit(100);
    zb::rpc::GetManagedMetricsHistoryReply history_reply;
    service.GetManagedMetricsHistory(nullptr, &history_request, &history_reply, nullptr);
    if (!StatusOk(history_reply.status(), "GetManagedMetricsHistory") ||
        !Expect(history_reply.samples_size() >= 2, "time-series metrics returned")) {
        return 14;
    }

    zb::rpc::ManagedNodeFailureRequest failure_request;
    failure_request.set_node_id("batch-sim-a");
    failure_request.set_event_ts_ms(2600);
    failure_request.set_reason("test failure");
    zb::rpc::ManagedNodeReply failure_reply;
    service.InjectManagedNodeFailure(nullptr, &failure_request, &failure_reply, nullptr);
    if (!StatusOk(failure_reply.status(), "InjectManagedNodeFailure") ||
        !Expect(failure_reply.node().health() == zb::rpc::MANAGED_HEALTH_FAILED,
                "failure is visible")) {
        return 15;
    }
    failure_request.set_event_ts_ms(2700);
    zb::rpc::ManagedNodeReply repair_reply;
    service.RepairManagedNode(nullptr, &failure_request, &repair_reply, nullptr);
    if (!StatusOk(repair_reply.status(), "RepairManagedNode") ||
        !Expect(repair_reply.node().health() == zb::rpc::MANAGED_HEALTH_HEALTHY,
                "repair is visible")) {
        return 16;
    }

    std::cout << "PASS scheduler managed service RPCs\n";
    return 0;
}
