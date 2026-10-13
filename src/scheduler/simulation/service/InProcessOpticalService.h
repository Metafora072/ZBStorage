#pragma once

#include <memory>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "optical_node.pb.h"
#include "common/metrics/NodeMetricsCollector.h"
#include "scheduler/simulation/optical/OpticalReadModel.h"
#include "scheduler/power/NodePowerManager.h"

namespace zb::scheduler {

class InProcessDiskService;

struct OpticalNodeManagementSnapshot {
    uint64_t disc_capacity_bytes{0};
    uint64_t disc_free_bytes{0};
    uint64_t cache_capacity_bytes{0};
    uint64_t cache_free_bytes{0};
    OpticalModelStats model;
    zb::storage_model::OpticalLibraryStatus library;
};

// Routes the existing formal optical RPCs to isolated read models. Archive
// metadata is deliberately rejected until the MDS completion contract exists.
class InProcessOpticalService final : public zb::rpc::OpticalNodeService {
public:
    explicit InProcessOpticalService(NodePowerManager* manager = nullptr,
                                     std::string mds_address = {},
                                     std::string service_address = {},
                                     InProcessDiskService* model_sources = nullptr)
        : node_manager_(manager), mds_address_(std::move(mds_address)),
          service_address_(std::move(service_address)), model_sources_(model_sources) {}
    bool AddNode(OpticalReadConfig config, std::string* error);
    // Only use before the instance is exposed through Scheduler reporting.
    bool DiscardUnpublishedNode(const std::string& node_id);
    bool ReleaseRetiredNode(const std::string& node_id);
    bool OwnsNodeIdentity(const std::string& node_id) const;
    bool ConfirmCatalog(const std::string& node_id);
    bool RevokeCatalog(const std::string& node_id);
    OpticalReadModel* GetModel(const std::string& node_id) const;
    zb::metrics::NodeMetricsCollector* GetMetrics(const std::string& node_id) const;
    bool GetManagementSnapshot(const std::string& node_id, uint64_t observed_at_ms,
                               OpticalNodeManagementSnapshot* output) const;
    bool ReconcileArchives(const std::string& node_id, std::string* error);

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
    std::shared_ptr<OpticalReadModel> Resolve(google::protobuf::RpcController* controller,
                                              zb::rpc::MdsStatus* status,
                                              bool requires_ready) const;
    std::shared_ptr<zb::metrics::NodeMetricsCollector> GetMetricsShared(
        const std::string& node_id) const;
    static void FillStatus(zb::rpc::MdsStatus* status, const OpticalModelResult& result);
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<OpticalReadModel>> models_;
    std::unordered_map<std::string, std::shared_ptr<zb::metrics::NodeMetricsCollector>> metrics_;
    std::unordered_set<std::string> known_node_ids_;
    std::unordered_set<std::string> catalog_confirmed_ids_;
    mutable std::unordered_map<std::string, std::chrono::steady_clock::time_point> wake_ready_at_;
    NodePowerManager* node_manager_{};
    std::string mds_address_;
    std::string service_address_;
    InProcessDiskService* model_sources_{};
};

} // namespace zb::scheduler
