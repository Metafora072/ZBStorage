#pragma once

#include <memory>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "optical_node.pb.h"
#include "common/metrics/NodeMetricsCollector.h"
#include "common/rpc/ModelWait.h"
#include "scheduler/simulation/optical/OpticalReadModel.h"
#include "scheduler/power/NodePowerManager.h"

namespace zb::scheduler {

class InProcessDiskService;
struct DrainMigrationTask;

struct OpticalNodeManagementSnapshot {
    uint64_t disc_capacity_bytes{0};
    uint64_t disc_free_bytes{0};
    uint64_t cache_capacity_bytes{0};
    uint64_t cache_free_bytes{0};
    OpticalModelStats model;
    zb::storage_model::OpticalLibraryStatus library;
};

// 共享光盘数据入口：按逻辑节点和代际路由，兼容正式准备/读会话协议与旧读接口。
class InProcessOpticalService final : public zb::rpc::OpticalNodeService {
public:
    explicit InProcessOpticalService(NodePowerManager* manager = nullptr,
                                     std::string mds_address = {},
                                     std::string service_address = {},
                                     InProcessDiskService* model_sources = nullptr);
    ~InProcessOpticalService() override;
    bool AddNode(OpticalReadConfig config, std::string* error);
    // Only use before the instance is exposed through Scheduler reporting.
    bool DiscardUnpublishedNode(const std::string& node_id);
    bool ReleaseRetiredNode(const std::string& node_id);
    bool OwnsNodeIdentity(const std::string& node_id) const;
    bool ConfirmCatalog(const std::string& node_id);
    bool RevokeCatalog(const std::string& node_id);
    OpticalReadModel* GetModel(const std::string& node_id) const;
    zb::metrics::NodeMetricsCollector* GetMetrics(const std::string& node_id) const;
    zb::metrics::NodeMetricsCollector* GetBackgroundMetrics(const std::string& node_id, bool migration);
    bool GetManagementSnapshot(const std::string& node_id, uint64_t observed_at_ms,
                               OpticalNodeManagementSnapshot* output) const;
    bool ReconcileArchives(const std::string& node_id, std::string* error);
    bool BeginDataDrain(const std::string& node_id, uint64_t now_ms, std::string* error);
    // 每次仅推进一个文件。false 且 error 为空表示任务正常在途，不阻塞管理线程。
    bool AdvanceDataDrain(DrainMigrationTask* task, uint64_t now_ms, std::string* error);
    bool PauseNode(const std::string& node_id, bool paused, std::string* error);
    uint32_t RebootLatencyMs(const std::string& node_id) const;

    // 与真实光盘共用的 MDS 只读清单协议，不推进模型时间或提交权威位置。
    void ListArchiveTasks(google::protobuf::RpcController*, const zb::rpc::ListArchiveTasksRequest*,
                          zb::rpc::ListArchiveTasksReply*, google::protobuf::Closure*) override;
    void ListArchiveImages(google::protobuf::RpcController*, const zb::rpc::ListArchiveImagesRequest*,
                           zb::rpc::ListArchiveImagesReply*, google::protobuf::Closure*) override;
    void GetArchiveImageFiles(google::protobuf::RpcController*, const zb::rpc::GetArchiveImageFilesRequest*,
                              zb::rpc::GetArchiveImageFilesReply*, google::protobuf::Closure*) override;

    // 正式协议：通知订阅、异步准备、独立读取会话和两级引用释放。
    void SubscribeReadEvents(google::protobuf::RpcController*, const zb::rpc::SubscribeReadEventsRequest*,
                             zb::rpc::SubscribeReadEventsReply*, google::protobuf::Closure*) override;
    void PrepareRead(google::protobuf::RpcController*, const zb::rpc::PrepareReadRequest*,
                      zb::rpc::PrepareReadReply*, google::protobuf::Closure*) override;
    void AcquireReadSession(google::protobuf::RpcController*, const zb::rpc::AcquireReadSessionRequest*,
                             zb::rpc::AcquireReadSessionReply*, google::protobuf::Closure*) override;
    void ReadPreparedFile(google::protobuf::RpcController*, const zb::rpc::ReadPreparedFileRequest*,
                           zb::rpc::ReadPreparedFileReply*, google::protobuf::Closure*) override;
    void ReleaseReadSession(google::protobuf::RpcController*, const zb::rpc::ReleaseReadSessionRequest*,
                             zb::rpc::ReleaseReadSessionReply*, google::protobuf::Closure*) override;
    void ReleasePreparedRead(google::protobuf::RpcController*, const zb::rpc::ReleasePreparedReadRequest*,
                              zb::rpc::ReleasePreparedReadReply*, google::protobuf::Closure*) override;

    void SendArchiveMetadata(google::protobuf::RpcController* controller,
                             const zb::rpc::SendArchiveMetadataRequest* request,
                             zb::rpc::ArchiveReply* response,
                             google::protobuf::Closure* done) override;
    void RequestAsyncReadFile(google::protobuf::RpcController* controller,
                              const zb::rpc::RequestAsyncReadFileRequest* request,
                              zb::rpc::RequestAsyncReadFileReply* response,
                              google::protobuf::Closure* done) override;
    void ReadObjectByTaskId(google::protobuf::RpcController* controller,
                            const zb::rpc::ReadObjectByTaskIdRequest* request,
                            zb::rpc::ReadObjectByTaskIdReply* response,
                            google::protobuf::Closure* done) override;
    void ReadObjectByInodeId(google::protobuf::RpcController* controller,
                             const zb::rpc::ReadObjectByInodeIdRequest* request,
                             zb::rpc::ReadObjectByInodeIdReply* response,
                             google::protobuf::Closure* done) override;

private:
    std::shared_ptr<OpticalReadModel> ResolveInventory(const std::string& node_id,
        uint64_t generation, zb::rpc::OpticalStatus* status) const;
    void SubmitArchiveFile(const std::shared_ptr<OpticalReadModel>& model,
                           const zb::rpc::ArchiveFile& file, zb::rpc::ArchiveReply* response);
    struct PreparedState;
    std::shared_ptr<OpticalReadModel> ResolvePrepared(google::protobuf::RpcController*,
                                                       zb::rpc::OpticalStatus*, bool ready) const;
    uint32_t ActiveReadSessions(const std::string& node_id) const;
    std::shared_ptr<OpticalReadModel> Resolve(google::protobuf::RpcController* controller,
                                              zb::rpc::MdsStatus* status,
                                              bool requires_ready) const;
    std::shared_ptr<zb::metrics::NodeMetricsCollector> GetMetricsShared(
        const std::string& node_id) const;
    static void FillStatus(zb::rpc::MdsStatus* status, const OpticalModelResult& result);
    mutable std::mutex mu_;
    mutable zb::rpc_route::ModelSharedMutex write_admission_mu_;
    std::unordered_map<std::string, std::shared_ptr<OpticalReadModel>> models_;
    std::unordered_map<std::string, std::shared_ptr<zb::metrics::NodeMetricsCollector>> metrics_;
    struct BackgroundMetrics {
        zb::metrics::NodeMetricsCollector archive, migration;
        std::mutex mu;
        std::unordered_set<std::string> observed_completions;
    };
    std::unordered_map<std::string, std::shared_ptr<BackgroundMetrics>> background_metrics_;
    std::unordered_set<std::string> known_node_ids_;
    std::unordered_set<std::string> catalog_confirmed_ids_;
    mutable std::unordered_map<std::string, std::chrono::steady_clock::time_point> wake_ready_at_;
    NodePowerManager* node_manager_{};
    std::string mds_address_;
    std::string service_address_;
    InProcessDiskService* model_sources_{};
    std::unique_ptr<PreparedState> prepared_;
};

} // namespace zb::scheduler
