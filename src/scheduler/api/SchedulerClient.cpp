#include "SchedulerClient.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>

namespace zb::scheduler::api {

SchedulerClient::SchedulerClient(std::string address, uint32_t timeout_ms)
    : address_(std::move(address)), timeout_ms_(timeout_ms == 0 ? 2000 : timeout_ms) {}

SchedulerClient::~SchedulerClient() = default;

bool SchedulerClient::Connect(std::string* error) {
    std::lock_guard<std::mutex> lock(connect_mu_);
    if (channel_) return true;
    if (address_.empty()) {
        if (error) *error = "Scheduler address is empty";
        return false;
    }
    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = static_cast<int>(std::min<uint32_t>(
        timeout_ms_, static_cast<uint32_t>(std::numeric_limits<int>::max())));
    options.max_retry = 0;
    if (channel->Init(address_.c_str(), &options) != 0) {
        if (error) *error = "failed to initialize Scheduler channel: " + address_;
        return false;
    }
    channel_ = std::move(channel);
    if (error) error->clear();
    return true;
}

template <class Request, class Reply, class Method>
bool SchedulerClient::Call(const char* name, const Request& request, Reply* response,
                           Method method, std::string* error) {
    if (!response) {
        if (error) *error = std::string(name) + ": response is null";
        return false;
    }
    response->Clear();
    if (!Connect(error)) return false;
    brpc::Controller controller;
    rpc::SchedulerService_Stub stub(channel_.get());
    std::invoke(method, &stub, &controller, &request, response, nullptr);
    if (controller.Failed()) {
        if (error) *error = std::string(name) + ": transport failure: " + controller.ErrorText();
        return false;
    }
    if (!response->has_status()) {
        if (error) *error = std::string(name) + ": missing Scheduler status";
        return false;
    }
    if (response->status().code() != rpc::SCHED_OK) {
        if (error) {
            *error = std::string(name) + ": " +
                     rpc::SchedulerStatusCode_Name(response->status().code()) +
                     ": " + response->status().message();
        }
        return false;
    }
    if (error) error->clear();
    return true;
}

// 为每个 RPC 生成轻量转发方法，统一复用 Call 的状态检查与错误处理。
#define ZB_DEFINE_SCHEDULER_CALL(Name, Request, Reply)                              \
    bool SchedulerClient::Name(const rpc::Request& request, rpc::Reply* response,    \
                               std::string* error) {                                \
        return Call(#Name, request, response, &rpc::SchedulerService_Stub::Name, error); \
    }

ZB_DEFINE_SCHEDULER_CALL(ReportHeartbeat, HeartbeatRequest, HeartbeatReply)
ZB_DEFINE_SCHEDULER_CALL(ReloadModelConfigs, ReloadModelConfigsRequest, ReloadModelConfigsReply)
ZB_DEFINE_SCHEDULER_CALL(GetClusterView, GetClusterViewRequest, GetClusterViewReply)
ZB_DEFINE_SCHEDULER_CALL(SetNodeAdminState, SetNodeAdminStateRequest, SetNodeAdminStateReply)
ZB_DEFINE_SCHEDULER_CALL(StartNode, StartNodeRequest, NodeOperationReply)
ZB_DEFINE_SCHEDULER_CALL(StopNode, StopNodeRequest, NodeOperationReply)
ZB_DEFINE_SCHEDULER_CALL(RebootNode, RebootNodeRequest, NodeOperationReply)
ZB_DEFINE_SCHEDULER_CALL(GetOperationStatus, GetOperationStatusRequest, NodeOperationReply)
ZB_DEFINE_SCHEDULER_CALL(RegisterManagedNode, RegisterManagedNodeRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(RemoveManagedNode, RemoveManagedNodeRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(ReportNodeMetrics, ReportNodeMetricsRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(ReportNodeAccessEvent, ReportNodeAccessEventRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(ReportNodeDrainProgress, ReportNodeDrainProgressRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(ReportOpticalLibraryStatus, ReportOpticalLibraryStatusRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(SetManagedNodeServiceMode, SetManagedNodeServiceModeRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(ReportManagedNodeReadiness, ReportManagedNodeReadinessRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(AdvanceSimulationTime, AdvanceSimulationTimeRequest, AdvanceSimulationTimeReply)
ZB_DEFINE_SCHEDULER_CALL(ListManagedNodes, ListManagedNodesRequest, ListManagedNodesReply)
ZB_DEFINE_SCHEDULER_CALL(GetManagedNode, GetManagedNodeRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(GetDrainMigration, GetDrainMigrationRequest, GetDrainMigrationReply)
ZB_DEFINE_SCHEDULER_CALL(InjectManagedNodeFailure, ManagedNodeFailureRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(RepairManagedNode, ManagedNodeFailureRequest, ManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(BatchRegisterManagedNodes, BatchRegisterManagedNodesRequest, BatchManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(BatchRemoveManagedNodes, BatchRemoveManagedNodesRequest, BatchManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(BatchReportNodeAccessEvents, BatchReportNodeAccessEventsRequest, BatchManagedNodeReply)
ZB_DEFINE_SCHEDULER_CALL(RunLongTermSimulation, RunLongTermSimulationRequest, RunLongTermSimulationReply)
ZB_DEFINE_SCHEDULER_CALL(GetManagedMetricsHistory, GetManagedMetricsHistoryRequest, GetManagedMetricsHistoryReply)

#undef ZB_DEFINE_SCHEDULER_CALL

bool SchedulerClient::ListAllManagedNodes(const rpc::ListManagedNodesRequest& filter,
                                          std::vector<rpc::ManagedNodeView>* nodes,
                                          uint64_t* generation,
                                          std::string* error) {
    if (!nodes) {
        if (error) *error = "ListAllManagedNodes: nodes is null";
        return false;
    }
    nodes->clear();
    if (generation) *generation = 0;
    rpc::ListManagedNodesRequest request = filter;
    request.set_offset(0);
    if (request.limit() == 0) request.set_limit(256);
    uint64_t first_generation = 0;
    bool first_page = true;
    for (;;) {
        rpc::ListManagedNodesReply response;
        if (!ListManagedNodes(request, &response, error)) {
            nodes->clear();
            return false;
        }
        if (first_page) {
            first_generation = response.generation();
            first_page = false;
        } else if (response.generation() != first_generation) {
            nodes->clear();
            if (error) *error = "ListAllManagedNodes: catalog changed during pagination";
            return false;
        }
        for (const auto& node : response.nodes()) nodes->push_back(node);
        if (!response.has_more()) {
            if (generation) *generation = first_generation;
            if (error) error->clear();
            return true;
        }
        if (response.next_offset() <= request.offset()) {
            nodes->clear();
            if (error) *error = "ListAllManagedNodes: invalid next_offset";
            return false;
        }
        request.set_offset(response.next_offset());
    }
}

} // namespace zb::scheduler::api
