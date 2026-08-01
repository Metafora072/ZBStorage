#include "SchedulerMetricsClient.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

namespace zb::metrics {

bool ReportCollectedMetrics(brpc::Channel* channel,
                            const std::string& node_id,
                            NodeMetricsCollector* collector,
                            zb::rpc::ManagedAccessPath access_path,
                            uint64_t now_ms,
                            std::string* error) {
    if (!channel || node_id.empty() || !collector) {
        if (error) {
            *error = "invalid Scheduler metrics client arguments";
        }
        return false;
    }

    NodeMetricsSnapshot snapshot = collector->TakeForReport(now_ms);
    if (!snapshot.HasActivity()) {
        return true;
    }

    zb::rpc::ReportNodeMetricsRequest request;
    request.set_node_id(node_id);
    request.set_window_start_ms(snapshot.window_start_ms);
    request.set_window_end_ms(snapshot.window_end_ms);
    request.set_read_operations(snapshot.read_operations);
    request.set_write_operations(snapshot.write_operations);
    request.set_other_operations(snapshot.other_operations);
    request.set_read_bytes(snapshot.read_bytes);
    request.set_write_bytes(snapshot.write_bytes);
    request.set_active_requests(snapshot.active_requests);
    request.set_max_queue_depth(snapshot.max_queue_depth);
    request.set_busy_time_ms(snapshot.busy_time_ms);
    request.set_last_access_ms(snapshot.last_access_ms);
    request.set_access_path(access_path);
    request.set_report_sequence(snapshot.report_sequence);
    request.set_reporter_epoch(snapshot.reporter_epoch);

    zb::rpc::SchedulerService_Stub stub(channel);
    zb::rpc::ManagedNodeReply response;
    brpc::Controller controller;
    stub.ReportNodeMetrics(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.status().code() != zb::rpc::SCHED_OK) {
        if (error) {
            *error = controller.Failed() ? controller.ErrorText() : response.status().message();
        }
        return false;
    }
    collector->AcknowledgeReport(snapshot.report_sequence);
    return true;
}

bool EnsureMetadataNodeRegistered(brpc::Channel* channel,
                                  const std::string& node_id,
                                  uint64_t now_ms,
                                  std::string* error) {
    if (!channel || node_id.empty()) {
        if (error) {
            *error = "invalid metadata registration arguments";
        }
        return false;
    }
    zb::rpc::RegisterManagedNodeRequest request;
    auto* spec = request.mutable_spec();
    spec->set_node_id(node_id);
    spec->set_kind(zb::rpc::MANAGED_KIND_METADATA);
    spec->set_execution_mode(zb::rpc::MANAGED_EXECUTION_PHYSICAL);
    spec->set_logical_node_count(1);
    spec->set_participates_in_placement(false);
    spec->set_participates_in_placement_set(true);
    request.set_join_time_ms(now_ms);
    request.set_activate_immediately(true);

    zb::rpc::SchedulerService_Stub stub(channel);
    zb::rpc::ManagedNodeReply response;
    brpc::Controller controller;
    stub.RegisterManagedNode(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        if (error) {
            *error = controller.ErrorText();
        }
        return false;
    }
    if (response.status().code() == zb::rpc::SCHED_OK ||
        response.status().code() == zb::rpc::SCHED_ALREADY_EXISTS) {
        return true;
    }
    if (error) {
        *error = response.status().message();
    }
    return false;
}

bool ReportMetadataNodeHeartbeat(brpc::Channel* channel,
                                 const std::string& node_id,
                                 const std::string& service_address,
                                 uint64_t capacity_bytes,
                                 uint64_t free_bytes,
                                 uint64_t now_ms,
                                 std::string* error) {
    if (!channel || node_id.empty() || capacity_bytes == 0 || free_bytes > capacity_bytes) {
        if (error) *error = "invalid metadata heartbeat arguments";
        return false;
    }
    zb::rpc::HeartbeatRequest request;
    request.set_node_id(node_id);
    request.set_node_type(zb::rpc::NODE_METADATA);
    request.set_address(service_address);
    request.set_weight(1);
    request.set_virtual_node_count(1);
    request.set_report_ts_ms(now_ms);
    request.set_readiness_reported(true);
    request.set_initialization_complete(true);
    request.set_metadata_ready(true);
    request.set_readiness_message("CMS RocksDB opened and metadata RPC initialized");
    auto* device = request.add_disks();
    device->set_disk_id("cms-metadata-volume");
    device->set_capacity_bytes(capacity_bytes);
    device->set_free_bytes(free_bytes);
    device->set_is_healthy(true);
    device->set_device_kind(zb::rpc::MANAGED_DEVICE_SSD);
    device->set_read_bandwidth_bytes_per_sec(5000000000ULL);
    device->set_write_bandwidth_bytes_per_sec(5000000000ULL);

    zb::rpc::SchedulerService_Stub stub(channel);
    zb::rpc::HeartbeatReply response;
    brpc::Controller controller;
    stub.ReportHeartbeat(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.status().code() != zb::rpc::SCHED_OK) {
        if (error) *error = controller.Failed() ? controller.ErrorText()
                                                : response.status().message();
        return false;
    }
    return true;
}

} // namespace zb::metrics
