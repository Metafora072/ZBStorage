#include "SchedulerServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>

#include "ManagedNodeProtoAdapter.h"

namespace zb::scheduler {

SchedulerServiceImpl::SchedulerServiceImpl(ClusterState* state,
                                           LifecycleManager* lifecycle,
                                           NodePowerManager* node_manager,
                                           DrainMigrationCoordinator* migration)
    : state_(state), lifecycle_(lifecycle), node_manager_(node_manager), migration_(migration) {}

void SchedulerServiceImpl::ReportHeartbeat(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::HeartbeatRequest* request,
                                           zb::rpc::HeartbeatReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid heartbeat request");
        return;
    }

    HeartbeatAssignment assignment = state_->ReportHeartbeat(*request);
    if (!assignment.success) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT,
                   assignment.error);
        return;
    }
    response->set_generation(assignment.generation);
    response->set_assigned_role(assignment.assigned_role);
    response->set_epoch(assignment.epoch);
    response->set_group_id(assignment.group_id);
    response->set_primary_node_id(assignment.primary_node_id);
    response->set_primary_address(assignment.primary_address);
    response->set_secondary_node_id(assignment.secondary_node_id);
    response->set_secondary_address(assignment.secondary_address);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetClusterView(google::protobuf::RpcController* cntl_base,
                                          const zb::rpc::GetClusterViewRequest* request,
                                          zb::rpc::GetClusterViewReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INTERNAL_ERROR,
                   "service not initialized");
        return;
    }

    uint64_t generation = 0;
    std::vector<NodeState> nodes;
    state_->Snapshot(request->min_generation(), &generation, &nodes);
    response->set_generation(generation);
    for (const auto& node : nodes) {
        FillNodeView(node, response->add_nodes());
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::SetNodeAdminState(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::SetNodeAdminStateRequest* request,
                                             zb::rpc::SetNodeAdminStateReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    std::string error;
    uint64_t generation = state_->SetNodeAdminState(request->node_id(), request->admin_state(), &error);
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, error);
        return;
    }
    response->set_generation(generation);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::StartNode(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::StartNodeRequest* request,
                                     zb::rpc::NodeOperationReply* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->StartNode(request->node_id(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::StopNode(google::protobuf::RpcController* cntl_base,
                                    const zb::rpc::StopNodeRequest* request,
                                    zb::rpc::NodeOperationReply* response,
                                    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->StopNode(request->node_id(), request->force(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::RebootNode(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::RebootNodeRequest* request,
                                      zb::rpc::NodeOperationReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!lifecycle_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }
    NodeOperationState op;
    std::string error;
    if (!lifecycle_->RebootNode(request->node_id(), request->reason(), &op, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetOperationStatus(google::protobuf::RpcController* cntl_base,
                                              const zb::rpc::GetOperationStatusRequest* request,
                                              zb::rpc::NodeOperationReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!state_ || !request || !response || request->operation_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid request");
        return;
    }

    NodeOperationState op;
    if (!state_->GetOperation(request->operation_id(), &op)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, "operation not found");
        return;
    }
    FillOperation(op, response->mutable_operation());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::RegisterManagedNode(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::RegisterManagedNodeRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || !request->has_spec()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "managed node service is unavailable or spec is missing");
        return;
    }

    ManagedNodeProfile profile;
    std::string error;
    if (!ManagedNodeProfileFromProto(request->spec(), &profile, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    const uint64_t join_time_ms = request->join_time_ms() == 0 ? NowMs() : request->join_time_ms();
    const uint64_t lifetime_seed = request->lifetime_seed() == 0
                                       ? std::hash<std::string>{}(profile.node_id)
                                       : request->lifetime_seed();
    const bool registered = request->replaces_node_id().empty()
                                ? node_manager_->RegisterNode(profile, join_time_ms,
                                                              lifetime_seed, &error)
                                : node_manager_->RegisterReplacementNode(
                                      profile, request->replaces_node_id(), join_time_ms,
                                      lifetime_seed, &error);
    if (!registered) {
        ManagedNodeRuntime existing;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(profile.node_id, &existing)
                       ? zb::rpc::SCHED_ALREADY_EXISTS
                       : zb::rpc::SCHED_INVALID_ARGUMENT,
                   error);
        return;
    }
    if (request->activate_immediately()) {
        zb::storage_model::NodeReadinessStatus readiness;
        readiness.observed = true;
        readiness.initialization_complete = true;
        readiness.inventory_ready = true;
        readiness.metadata_ready = true;
        readiness.observed_at_ms = join_time_ms;
        readiness.message = "explicit managed-node activation";
        if (!node_manager_->ReportReadiness(profile.node_id, readiness, &error) ||
            !node_manager_->ActivateNode(profile.node_id, join_time_ms, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR, error);
            return;
        }
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(profile.node_id, &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::RemoveManagedNode(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::RemoveManagedNodeRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid managed node removal request");
        return;
    }
    ManagedNodeRuntime node;
    if (!node_manager_->GetNode(request->node_id(), &node)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, "managed node not found");
        return;
    }

    const uint64_t now_ms = request->request_ts_ms() == 0 ? NowMs() : request->request_ts_ms();
    std::string error;
    const bool ok = request->finalize()
                        ? node_manager_->FinalizeRemoveNode(request->node_id(), now_ms, request->force(), &error)
                        : node_manager_->BeginRemoveNode(request->node_id(), now_ms, &error);
    if (!ok) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }

    if (!request->finalize() && migration_ &&
        !migration_->Start(request->node_id(), now_ms, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INTERNAL_ERROR,
                   "node entered draining but migration could not start: " + error);
        return;
    }

    NodeState cluster_node;
    if (state_ && state_->GetNode(request->node_id(), &cluster_node)) {
        if (request->finalize()) {
            state_->SetNodeAdminState(request->node_id(), zb::rpc::NODE_ADMIN_DISABLED, nullptr);
            state_->SetDesiredPowerState(request->node_id(), zb::rpc::NODE_POWER_OFF, nullptr);
        } else {
            state_->SetNodeAdminState(request->node_id(), zb::rpc::NODE_ADMIN_DRAINING, nullptr);
        }
    }
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK,
               request->finalize() ? "node retired" : "node draining");
}

void SchedulerServiceImpl::ReportNodeMetrics(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReportNodeMetricsRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid node metrics request");
        return;
    }
    NodeAccessMetrics metrics;
    metrics.reporter_epoch = request->reporter_epoch();
    metrics.report_sequence = request->report_sequence();
    metrics.window_start_ms = request->window_start_ms();
    metrics.window_end_ms = request->window_end_ms() == 0 ? NowMs() : request->window_end_ms();
    metrics.read_operations = request->read_operations();
    metrics.write_operations = request->write_operations();
    metrics.other_operations = request->other_operations();
    metrics.read_bytes = request->read_bytes();
    metrics.write_bytes = request->write_bytes();
    metrics.active_requests = request->active_requests();
    metrics.max_queue_depth = request->max_queue_depth();
    metrics.busy_time_ms = request->busy_time_ms();
    metrics.last_access_ms = request->last_access_ms();
    metrics.access_path = AccessPathFromProto(request->access_path());

    std::string error;
    if (!node_manager_->ReportMetrics(request->node_id(), metrics, &error)) {
        ManagedNodeRuntime ignored;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(request->node_id(), &ignored)
                       ? zb::rpc::SCHED_INVALID_ARGUMENT
                       : zb::rpc::SCHED_NOT_FOUND,
                   error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::ReportNodeAccessEvent(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReportNodeAccessEventRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty() ||
        request->event_time_us() == 0) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid node access event");
        return;
    }
    NodeAccessEvent event;
    event.event_time_us = request->event_time_us();
    event.is_read = request->operation() == zb::rpc::MANAGED_OPERATION_READ;
    event.is_write = request->operation() == zb::rpc::MANAGED_OPERATION_WRITE;
    event.bytes = request->bytes();
    event.queue_depth = request->queue_depth();
    event.access_path = AccessPathFromProto(request->access_path());

    std::string error;
    if (!node_manager_->ReportAccessEvent(request->node_id(), event, &error)) {
        ManagedNodeRuntime ignored;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(request->node_id(), &ignored)
                       ? zb::rpc::SCHED_INVALID_ARGUMENT
                       : zb::rpc::SCHED_NOT_FOUND,
                   error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::ReportNodeDrainProgress(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReportNodeDrainProgressRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid node drain progress report");
        return;
    }
    const uint64_t report_ts_ms = request->report_ts_ms() == 0 ? NowMs() : request->report_ts_ms();
    std::string error;
    if (!node_manager_->ReportDrainProgress(request->node_id(),
                                            report_ts_ms,
                                            request->remaining_objects(),
                                            request->remaining_bytes(),
                                            request->active_requests(),
                                            &error)) {
        ManagedNodeRuntime ignored;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(request->node_id(), &ignored)
                       ? zb::rpc::SCHED_INVALID_ARGUMENT
                       : zb::rpc::SCHED_NOT_FOUND,
                   error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(),
               zb::rpc::SCHED_OK,
               node.lifecycle == LifecycleState::kExiting ? "drain complete" : "draining");
}

void SchedulerServiceImpl::ReportOpticalLibraryStatus(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReportOpticalLibraryStatusRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty() ||
        !request->has_status()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid optical library status report");
        return;
    }
    OpticalLibraryStatus status;
    std::string error;
    if (!OpticalLibraryStatusFromProto(request->status(), &status, &error) ||
        !node_manager_->ReportOpticalLibraryStatus(request->node_id(), status, &error)) {
        ManagedNodeRuntime ignored;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(request->node_id(), &ignored)
                       ? zb::rpc::SCHED_INVALID_ARGUMENT : zb::rpc::SCHED_NOT_FOUND,
                   error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK,
               node.service_mode == NodeServiceMode::kReadOnly
                   ? "optical library is read-only" : "OK");
}

void SchedulerServiceImpl::SetManagedNodeServiceMode(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::SetManagedNodeServiceModeRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty() ||
        request->service_mode() == zb::rpc::MANAGED_SERVICE_UNKNOWN) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "invalid service-mode request");
        return;
    }
    const NodeServiceMode mode =
        request->service_mode() == zb::rpc::MANAGED_SERVICE_READ_ONLY
            ? NodeServiceMode::kReadOnly : NodeServiceMode::kReadWrite;
    std::string error;
    if (!node_manager_->SetServiceMode(request->node_id(), mode, &error)) {
        ManagedNodeRuntime ignored;
        FillStatus(response->mutable_status(),
                   node_manager_->GetNode(request->node_id(), &ignored)
                       ? zb::rpc::SCHED_INVALID_ARGUMENT : zb::rpc::SCHED_NOT_FOUND,
                   error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::ReportManagedNodeReadiness(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReportManagedNodeReadinessRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty() ||
        !request->has_readiness()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "invalid node readiness report");
        return;
    }
    zb::storage_model::NodeReadinessStatus readiness;
    readiness.observed = request->readiness().observed();
    readiness.initialization_complete = request->readiness().initialization_complete();
    readiness.inventory_ready = request->readiness().inventory_ready();
    readiness.metadata_ready = request->readiness().metadata_ready();
    readiness.observed_at_ms = request->readiness().observed_at_ms() == 0
                                   ? NowMs() : request->readiness().observed_at_ms();
    readiness.message = request->readiness().message();
    std::string error;
    if (!node_manager_->ReportReadiness(request->node_id(), readiness, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    if (node.lifecycle == LifecycleState::kJoining && readiness.Ready() &&
        !node_manager_->ActivateNode(request->node_id(), readiness.observed_at_ms, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK,
               node.lifecycle == LifecycleState::kWorking ? "node activated" : "readiness recorded");
}

void SchedulerServiceImpl::AdvanceSimulationTime(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::AdvanceSimulationTimeRequest* request,
    zb::rpc::AdvanceSimulationTimeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->target_time_us() == 0) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "invalid simulation time request");
        return;
    }
    const uint64_t target_time_ms = request->target_time_us() / 1000;
    for (const auto& node : node_manager_->ListNodes()) {
        if (node.profile.execution_mode == ExecutionMode::kSimulated &&
            target_time_ms < node.last_energy_update_ms) {
            FillStatus(response->mutable_status(),
                       zb::rpc::SCHED_INVALID_ARGUMENT,
                       "simulation time cannot move backwards");
            return;
        }
    }
    response->set_generation(node_manager_->AdvanceSimulatedTime(target_time_ms));
    response->set_simulation_time_us(target_time_ms * 1000ULL);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::ListManagedNodes(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ListManagedNodesRequest* request,
    zb::rpc::ListManagedNodesReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INTERNAL_ERROR,
                   "managed node service is unavailable");
        return;
    }
    ManagedNodeFilter filter;
    std::string error;
    if (!ManagedNodeFilterFromProto(*request, &filter, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    std::vector<ManagedNodeRuntime> nodes = node_manager_->ListNodes(filter);
    const size_t offset = std::min<size_t>(request->offset(), nodes.size());
    const size_t limit = request->limit() == 0 ? 100 : std::min<uint32_t>(request->limit(), 1000);
    const size_t end = std::min(nodes.size(), offset + limit);
    for (size_t index = offset; index < end; ++index) {
        FillManagedNodeView(nodes[index], response->add_nodes());
    }
    response->set_generation(node_manager_->generation());
    response->set_next_offset(static_cast<uint32_t>(end));
    response->set_has_more(end < nodes.size());
    FillManagedSummary(node_manager_->Summary(), response->mutable_summary());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetManagedNode(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::GetManagedNodeRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "node_id is required");
        return;
    }
    ManagedNodeRuntime node;
    if (!node_manager_->GetNode(request->node_id(), &node)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND,
                   "managed node not found: " + request->node_id());
        return;
    }
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::GetDrainMigration(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::GetDrainMigrationRequest* request,
    zb::rpc::GetDrainMigrationReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!migration_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT,
                   "migration coordinator unavailable or node_id missing");
        return;
    }
    DrainMigrationTask task;
    if (!migration_->GetTask(request->node_id(), &task)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_NOT_FOUND, "drain migration not found");
        return;
    }
    auto* out = response->mutable_task();
    out->set_migration_id(task.migration_id);
    out->set_source_node_id(task.source_node_id);
    out->set_state(static_cast<zb::rpc::DrainMigrationState>(static_cast<int>(task.state)));
    out->set_created_at_ms(task.created_at_ms);
    out->set_updated_at_ms(task.updated_at_ms);
    out->set_total_objects(task.total_objects);
    out->set_total_bytes(task.total_bytes);
    out->set_remaining_objects(task.remaining_objects);
    out->set_remaining_bytes(task.remaining_bytes);
    out->set_migrated_objects(task.migrated_objects);
    out->set_migrated_bytes(task.migrated_bytes);
    out->set_retry_count(task.retry_count);
    out->set_next_retry_ms(task.next_retry_ms);
    out->set_last_error(task.last_error);
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::InjectManagedNodeFailure(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ManagedNodeFailureRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "invalid failure injection request");
        return;
    }
    std::string error;
    const uint64_t now_ms = request->event_ts_ms() == 0 ? NowMs() : request->event_ts_ms();
    if (!node_manager_->MarkNodeFailed(request->node_id(), now_ms,
                                       request->reason().empty() ? "injected failure" : request->reason(),
                                       &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::RepairManagedNode(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ManagedNodeFailureRequest* request,
    zb::rpc::ManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->node_id().empty()) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "invalid node repair request");
        return;
    }
    std::string error;
    const uint64_t now_ms = request->event_ts_ms() == 0 ? NowMs() : request->event_ts_ms();
    if (!node_manager_->RepairNode(request->node_id(), now_ms, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT, error);
        return;
    }
    ManagedNodeRuntime node;
    node_manager_->GetNode(request->node_id(), &node);
    response->set_generation(node_manager_->generation());
    FillManagedNodeView(node, response->mutable_node());
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::BatchRegisterManagedNodes(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::BatchRegisterManagedNodesRequest* request,
    zb::rpc::BatchManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!request || !response) return;
    for (const auto& item : request->requests()) {
        RegisterManagedNode(nullptr, &item, response->add_results(), nullptr);
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "batch processed; inspect per-item status");
}

void SchedulerServiceImpl::BatchRemoveManagedNodes(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::BatchRemoveManagedNodesRequest* request,
    zb::rpc::BatchManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!request || !response) return;
    for (const auto& item : request->requests()) {
        RemoveManagedNode(nullptr, &item, response->add_results(), nullptr);
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "batch processed; inspect per-item status");
}

void SchedulerServiceImpl::BatchReportNodeAccessEvents(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::BatchReportNodeAccessEventsRequest* request,
    zb::rpc::BatchManagedNodeReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!request || !response) return;
    for (const auto& item : request->requests()) {
        ReportNodeAccessEvent(nullptr, &item, response->add_results(), nullptr);
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "batch processed; inspect per-item status");
}

void SchedulerServiceImpl::RunLongTermSimulation(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::RunLongTermSimulationRequest* request,
    zb::rpc::RunLongTermSimulationReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response || request->target_time_us() == 0 ||
        request->step_us() == 0) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "target_time_us and step_us must be positive");
        return;
    }
    uint64_t current_ms = 0;
    bool has_simulated = false;
    for (const auto& node : node_manager_->ListNodes()) {
        if (node.profile.execution_mode == ExecutionMode::kSimulated) {
            has_simulated = true;
            current_ms = std::max(current_ms, node.last_energy_update_ms);
        }
    }
    const uint64_t target_ms = request->target_time_us() / 1000;
    const uint64_t step_ms = std::max<uint64_t>(1, request->step_us() / 1000);
    const uint64_t max_steps = request->max_steps() == 0
                                   ? 100000 : std::min<uint32_t>(request->max_steps(), 1000000);
    if (!has_simulated || target_ms < current_ms) {
        FillStatus(response->mutable_status(), zb::rpc::SCHED_INVALID_ARGUMENT,
                   !has_simulated ? "no simulated nodes registered" : "simulation time cannot move backwards");
        return;
    }
    uint64_t steps = 0;
    while (current_ms < target_ms && steps < max_steps) {
        current_ms = std::min<uint64_t>(target_ms, current_ms + step_ms);
        node_manager_->AdvanceSimulatedTime(current_ms);
        ++steps;
    }
    response->set_final_time_us(current_ms * 1000ULL);
    response->set_steps(steps);
    FillManagedSummary(node_manager_->Summary(), response->mutable_summary());
    FillStatus(response->mutable_status(),
               current_ms == target_ms ? zb::rpc::SCHED_OK : zb::rpc::SCHED_INVALID_ARGUMENT,
               current_ms == target_ms ? "OK" : "max_steps reached before target time");
}

void SchedulerServiceImpl::GetManagedMetricsHistory(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::GetManagedMetricsHistoryRequest* request,
    zb::rpc::GetManagedMetricsHistoryReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!node_manager_ || !request || !response ||
        (request->end_time_ms() != 0 && request->end_time_ms() < request->start_time_ms())) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::SCHED_INVALID_ARGUMENT, "invalid metrics history range");
        return;
    }
    for (const auto& sample : node_manager_->MetricsHistory(
             request->start_time_ms(), request->end_time_ms(), request->limit())) {
        auto* out = response->add_samples();
        out->set_timestamp_ms(sample.timestamp_ms);
        FillManagedSummary(sample.summary, out->mutable_summary());
    }
    FillStatus(response->mutable_status(), zb::rpc::SCHED_OK, "OK");
}

void SchedulerServiceImpl::FillStatus(zb::rpc::SchedulerStatus* status,
                                      zb::rpc::SchedulerStatusCode code,
                                      const std::string& message) {
    if (!status) {
        return;
    }
    status->set_code(code);
    status->set_message(message);
}

void SchedulerServiceImpl::FillNodeView(const NodeState& node, zb::rpc::NodeView* out) {
    if (!out) {
        return;
    }
    out->set_node_id(node.node_id);
    out->set_node_type(node.node_type);
    out->set_address(node.address);
    out->set_weight(node.weight);
    out->set_virtual_node_count(node.virtual_node_count);
    out->set_health_state(node.health_state);
    out->set_admin_state(node.admin_state);
    out->set_power_state(node.power_state);
    out->set_desired_admin_state(node.desired_admin_state);
    out->set_desired_power_state(node.desired_power_state);
    out->set_last_heartbeat_ms(node.last_heartbeat_ms);
    out->set_group_id(node.group_id);
    out->set_role(node.role);
    out->set_epoch(node.epoch);
    out->set_applied_lsn(node.applied_lsn);
    out->set_peer_node_id(node.peer_node_id);
    out->set_peer_address(node.peer_address);
    out->set_sync_ready(node.sync_ready);
    for (const auto& item : node.disks) {
        const DiskState& disk = item.second;
        zb::rpc::NodeDiskView* d = out->add_disks();
        d->set_disk_id(disk.disk_id);
        d->set_capacity_bytes(disk.capacity_bytes);
        d->set_free_bytes(disk.free_bytes);
        d->set_used_bytes(zb::storage_model::DeviceUsedBytes(disk));
        d->set_is_healthy(disk.is_healthy);
        d->set_last_update_ms(disk.last_update_ms);
        switch (disk.kind) {
        case DeviceKind::kHdd: d->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD); break;
        case DeviceKind::kSsd: d->set_device_kind(zb::rpc::MANAGED_DEVICE_SSD); break;
        case DeviceKind::kDisc: d->set_device_kind(zb::rpc::MANAGED_DEVICE_DISC); break;
        case DeviceKind::kOpticalDrive:
            d->set_device_kind(zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE);
            break;
        }
        d->set_read_bandwidth_bytes_per_sec(disk.read_bandwidth_bytes_per_sec);
        d->set_write_bandwidth_bytes_per_sec(disk.write_bandwidth_bytes_per_sec);
        d->set_accumulated_write_bytes(disk.accumulated_write_bytes);
    }
}

void SchedulerServiceImpl::FillOperation(const NodeOperationState& op, zb::rpc::NodeOperation* out) {
    if (!out) {
        return;
    }
    out->set_operation_id(op.operation_id);
    out->set_node_id(op.node_id);
    out->set_operation_type(op.operation_type);
    out->set_status(op.status);
    out->set_message(op.message);
    out->set_start_ts_ms(op.start_ts_ms);
    out->set_finish_ts_ms(op.finish_ts_ms);
}

uint64_t SchedulerServiceImpl::NowMs() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

} // namespace zb::scheduler
