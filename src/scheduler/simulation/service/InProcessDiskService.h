#pragma once

#include <cstdint>
#include <chrono>
#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/metrics/NodeMetricsCollector.h"
#include "data_node/virtual_node/config/VirtualNodeConfig.h"
#include "data_node/virtual_node/service/BrpcVirtualStorageService.h"
#include "real_node.pb.h"
#include "scheduler/power/NodePowerManager.h"

namespace zb::scheduler {

// A single RealNodeService endpoint owns multiple independently configured
// logical disk instances. Every RPC requires a valid target attachment.
class InProcessDiskService final : public zb::rpc::RealNodeService {
public:
    explicit InProcessDiskService(NodePowerManager* node_manager) : node_manager_(node_manager) {}
    bool AddNode(zb::virtual_node::VirtualNodeConfig config, std::string* error);
    // Roll back an instance that has not passed CMS catalog admission.
    bool DiscardUnpublishedNode(const std::string& node_id);
    // Keeps an in-process identity tombstone; only the active Slot is freed.
    bool ReleaseRetiredNode(const std::string& node_id);
    bool OwnsNodeIdentity(const std::string& node_id) const;
    bool VerifyEmpty(const std::string& node_id, std::string* error) const;
    // Excludes in-flight write RPCs, closes admission for future writes, then
    // atomically verifies local emptiness and enters Draining.
    bool BeginEmptyDrain(const std::string& node_id, uint64_t now_ms,
                         std::string* error);
    // Closes writes without requiring an empty model; migration then moves
    // durable control state before CMS retires this source.
    bool BeginDataDrain(const std::string& node_id, uint64_t now_ms,
                        std::string* error);
    std::vector<std::string> NodeIds() const;
    bool ConfirmCatalog(const std::string& node_id);
    bool RevokeCatalog(const std::string& node_id);
    zb::virtual_node::VirtualStorageServiceImpl* GetStorage(const std::string& node_id) const;
    uint64_t GetInstanceGeneration(const std::string& node_id) const;
    zb::metrics::NodeMetricsCollector* GetMetrics(const std::string& node_id) const;

#define ZB_DISK_METHOD(name, request_type, reply_type)                                  \
    void name(google::protobuf::RpcController* controller,                            \
              const request_type* request, reply_type* reply,                          \
              google::protobuf::Closure* done) override;
    ZB_DISK_METHOD(WriteObject, zb::rpc::WriteObjectRequest, zb::rpc::WriteObjectReply)
    ZB_DISK_METHOD(ReadObject, zb::rpc::ReadObjectRequest, zb::rpc::ReadObjectReply)
    ZB_DISK_METHOD(DeleteObject, zb::rpc::DeleteObjectRequest, zb::rpc::DeleteObjectReply)
    ZB_DISK_METHOD(ListObjects, zb::rpc::ListObjectsRequest, zb::rpc::ListObjectsReply)
    ZB_DISK_METHOD(ResetNodeData, zb::rpc::ResetNodeDataRequest, zb::rpc::ResetNodeDataReply)
    ZB_DISK_METHOD(ReadArchivedFile, zb::rpc::ReadArchivedFileRequest, zb::rpc::ReadArchivedFileReply)
    ZB_DISK_METHOD(UpdateArchiveState, zb::rpc::UpdateArchiveStateRequest, zb::rpc::UpdateArchiveStateReply)
    ZB_DISK_METHOD(UpdateFileArchiveState, zb::rpc::UpdateFileArchiveStateRequest, zb::rpc::UpdateFileArchiveStateReply)
    ZB_DISK_METHOD(GetDiskReport, google::protobuf::Empty, zb::rpc::DiskReportReply)
    ZB_DISK_METHOD(DeleteFileMeta, zb::rpc::DeleteFileMetaRequest, zb::rpc::DeleteFileMetaReply)
    ZB_DISK_METHOD(ResolveFileRead, zb::rpc::ResolveFileReadRequest, zb::rpc::ResolveFileReadReply)
    ZB_DISK_METHOD(AllocateFileWrite, zb::rpc::AllocateFileWriteRequest, zb::rpc::AllocateFileWriteReply)
    ZB_DISK_METHOD(CommitFileWrite, zb::rpc::CommitFileWriteRequest, zb::rpc::CommitFileWriteReply)
#undef ZB_DISK_METHOD

private:
    struct Slot {
        explicit Slot(zb::virtual_node::VirtualNodeConfig config);
        uint64_t generation{1};
        std::string node_id;
        std::atomic<bool> catalog_confirmed{false};
        mutable std::shared_mutex write_mu;
        bool drain_closed{false}; // guarded by write_mu
        std::mutex wake_mu;
        std::chrono::steady_clock::time_point wake_ready_at{};
        uint32_t wake_latency_ms{0};
        zb::virtual_node::VirtualStorageServiceImpl storage;
        zb::metrics::NodeMetricsCollector metrics;
        zb::virtual_node::BrpcVirtualStorageService service;
    };

    std::shared_ptr<Slot> Resolve(google::protobuf::RpcController* controller,
                                  zb::rpc::Status* status,
                                  bool new_write,
                                  bool wake_on_access) const;
    static bool VerifyEmptySlot(const Slot& slot, std::string* error);

    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<Slot>> nodes_;
    std::unordered_set<std::string> known_node_ids_;
    NodePowerManager* node_manager_{};
};

} // namespace zb::scheduler
