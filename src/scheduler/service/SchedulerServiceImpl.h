#pragma once

#include "../lifecycle/LifecycleManager.h"
#include "../model/ClusterState.h"
#include "../power/NodePowerManager.h"
#include "../migration/DrainMigrationCoordinator.h"
#include "scheduler.pb.h"

namespace zb::scheduler {

class SchedulerServiceImpl : public zb::rpc::SchedulerService {
public:
    SchedulerServiceImpl(ClusterState* state,
                         LifecycleManager* lifecycle,
                         NodePowerManager* node_manager = nullptr,
                         DrainMigrationCoordinator* migration = nullptr);

    void ReportHeartbeat(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::HeartbeatRequest* request,
                         zb::rpc::HeartbeatReply* response,
                         google::protobuf::Closure* done) override;

    void GetClusterView(google::protobuf::RpcController* cntl_base,
                        const zb::rpc::GetClusterViewRequest* request,
                        zb::rpc::GetClusterViewReply* response,
                        google::protobuf::Closure* done) override;

    void SetNodeAdminState(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::SetNodeAdminStateRequest* request,
                           zb::rpc::SetNodeAdminStateReply* response,
                           google::protobuf::Closure* done) override;

    void StartNode(google::protobuf::RpcController* cntl_base,
                   const zb::rpc::StartNodeRequest* request,
                   zb::rpc::NodeOperationReply* response,
                   google::protobuf::Closure* done) override;

    void StopNode(google::protobuf::RpcController* cntl_base,
                  const zb::rpc::StopNodeRequest* request,
                  zb::rpc::NodeOperationReply* response,
                  google::protobuf::Closure* done) override;

    void RebootNode(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::RebootNodeRequest* request,
                    zb::rpc::NodeOperationReply* response,
                    google::protobuf::Closure* done) override;

    void GetOperationStatus(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::GetOperationStatusRequest* request,
                            zb::rpc::NodeOperationReply* response,
                            google::protobuf::Closure* done) override;

    void RegisterManagedNode(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::RegisterManagedNodeRequest* request,
                             zb::rpc::ManagedNodeReply* response,
                             google::protobuf::Closure* done) override;

    void RemoveManagedNode(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::RemoveManagedNodeRequest* request,
                           zb::rpc::ManagedNodeReply* response,
                           google::protobuf::Closure* done) override;

    void ReportNodeMetrics(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::ReportNodeMetricsRequest* request,
                           zb::rpc::ManagedNodeReply* response,
                           google::protobuf::Closure* done) override;

    void ReportNodeAccessEvent(google::protobuf::RpcController* cntl_base,
                               const zb::rpc::ReportNodeAccessEventRequest* request,
                               zb::rpc::ManagedNodeReply* response,
                               google::protobuf::Closure* done) override;

    void ReportNodeDrainProgress(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::ReportNodeDrainProgressRequest* request,
                                 zb::rpc::ManagedNodeReply* response,
                                 google::protobuf::Closure* done) override;

    void ReportOpticalLibraryStatus(
        google::protobuf::RpcController* cntl_base,
        const zb::rpc::ReportOpticalLibraryStatusRequest* request,
        zb::rpc::ManagedNodeReply* response,
        google::protobuf::Closure* done) override;

    void SetManagedNodeServiceMode(
        google::protobuf::RpcController* cntl_base,
        const zb::rpc::SetManagedNodeServiceModeRequest* request,
        zb::rpc::ManagedNodeReply* response,
        google::protobuf::Closure* done) override;

    void ReportManagedNodeReadiness(
        google::protobuf::RpcController* cntl_base,
        const zb::rpc::ReportManagedNodeReadinessRequest* request,
        zb::rpc::ManagedNodeReply* response,
        google::protobuf::Closure* done) override;

    void AdvanceSimulationTime(google::protobuf::RpcController* cntl_base,
                               const zb::rpc::AdvanceSimulationTimeRequest* request,
                               zb::rpc::AdvanceSimulationTimeReply* response,
                               google::protobuf::Closure* done) override;

    void ListManagedNodes(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ListManagedNodesRequest* request,
                          zb::rpc::ListManagedNodesReply* response,
                          google::protobuf::Closure* done) override;

    void GetManagedNode(google::protobuf::RpcController* cntl_base,
                        const zb::rpc::GetManagedNodeRequest* request,
                        zb::rpc::ManagedNodeReply* response,
                        google::protobuf::Closure* done) override;

    void GetDrainMigration(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::GetDrainMigrationRequest* request,
                           zb::rpc::GetDrainMigrationReply* response,
                           google::protobuf::Closure* done) override;
    void InjectManagedNodeFailure(google::protobuf::RpcController*,
                                  const zb::rpc::ManagedNodeFailureRequest*,
                                  zb::rpc::ManagedNodeReply*, google::protobuf::Closure*) override;
    void RepairManagedNode(google::protobuf::RpcController*,
                           const zb::rpc::ManagedNodeFailureRequest*,
                           zb::rpc::ManagedNodeReply*, google::protobuf::Closure*) override;
    void BatchRegisterManagedNodes(google::protobuf::RpcController*,
                                   const zb::rpc::BatchRegisterManagedNodesRequest*,
                                   zb::rpc::BatchManagedNodeReply*, google::protobuf::Closure*) override;
    void BatchRemoveManagedNodes(google::protobuf::RpcController*,
                                 const zb::rpc::BatchRemoveManagedNodesRequest*,
                                 zb::rpc::BatchManagedNodeReply*, google::protobuf::Closure*) override;
    void BatchReportNodeAccessEvents(google::protobuf::RpcController*,
                                     const zb::rpc::BatchReportNodeAccessEventsRequest*,
                                     zb::rpc::BatchManagedNodeReply*, google::protobuf::Closure*) override;
    void RunLongTermSimulation(google::protobuf::RpcController*,
                               const zb::rpc::RunLongTermSimulationRequest*,
                               zb::rpc::RunLongTermSimulationReply*, google::protobuf::Closure*) override;
    void GetManagedMetricsHistory(google::protobuf::RpcController*,
                                  const zb::rpc::GetManagedMetricsHistoryRequest*,
                                  zb::rpc::GetManagedMetricsHistoryReply*, google::protobuf::Closure*) override;

private:
    static void FillStatus(zb::rpc::SchedulerStatus* status,
                           zb::rpc::SchedulerStatusCode code,
                           const std::string& message);
    static void FillNodeView(const NodeState& node, zb::rpc::NodeView* out);
    static void FillOperation(const NodeOperationState& op, zb::rpc::NodeOperation* out);
    static uint64_t NowMs();

    ClusterState* state_{};
    LifecycleManager* lifecycle_{};
    NodePowerManager* node_manager_{};
    DrainMigrationCoordinator* migration_{};
};

} // namespace zb::scheduler
