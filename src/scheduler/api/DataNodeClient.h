#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include "common/rpc/LogicalNodeRoute.h"
#include "mds.pb.h"
#include "optical_node.pb.h"
#include "real_node.pb.h"

namespace zb::scheduler::api {

// MDS 已提交位置中的数据目标。共享地址不等于同一节点，必须保留身份与代际。
// 本结构不是节点分配器，也不把 Scheduler 提案升级为 CMS 权威目录。
struct DataNodeTarget {
    std::string address;
    std::string node_id;
    uint64_t instance_generation{0};
};

// 统一的数据 RPC 传输层：不重试、不改 MDS、不保存业务数据。
// 返回 true 仅表示传输成功且协议状态为 OK，业务完成阶段仍以各 RPC 契约为准。
// 失败保留收到的原始回复；初始化后允许并发调用，不允许并发销毁。
class RoutedDataNodeClient {
public:
    explicit RoutedDataNodeClient(DataNodeTarget target, uint32_t timeout_ms = 5000)
        : target_(std::move(target)), timeout_ms_(timeout_ms ? timeout_ms : 5000) {}
    const DataNodeTarget& target() const { return target_; }
    RoutedDataNodeClient(const RoutedDataNodeClient&) = delete;
    RoutedDataNodeClient& operator=(const RoutedDataNodeClient&) = delete;

protected:
    template <class Stub, class Request, class Reply, class Method>
    bool Call(const Request& request, Reply* reply, Method method, std::string* error,
              brpc::Controller* supplied) {
        const auto fail = [error](const std::string& message) {
            if (error) *error = message;
            return false;
        };
        if (!reply) return fail("data RPC reply is null");
        reply->Clear();
        const auto* identity = request.GetDescriptor()->FindFieldByName("node_id");
        if (identity && request.GetReflection()->GetString(request, identity) != target_.node_id)
            return fail("request node_id conflicts with configured CMS target");
        const auto* generation = request.GetDescriptor()->FindFieldByName("instance_generation");
        if (generation && request.GetReflection()->GetUInt64(request, generation) != target_.instance_generation)
            return fail("request generation conflicts with configured CMS target");
        {
            std::lock_guard<std::mutex> lock(connect_mu_);
            if (!channel_) {
                if (target_.address.empty() || target_.node_id.empty() || target_.instance_generation == 0)
                    return fail("data target requires CMS address, node_id and nonzero generation");
                auto channel = std::make_unique<brpc::Channel>();
                brpc::ChannelOptions options;
                options.protocol = "baidu_std";
                options.timeout_ms = static_cast<int>(std::min<uint32_t>(timeout_ms_, std::numeric_limits<int>::max()));
                options.max_retry = 0;
                if (channel->Init(target_.address.c_str(), &options) != 0)
                    return fail("cannot initialize data channel");
                channel_ = std::move(channel);
            }
        }
        brpc::Controller local;
        auto* controller = supplied ? supplied : &local;
        if (controller->request_attachment().empty()) {
            if (!zb::rpc_route::SetLogicalNodeRoute(controller, target_.node_id, target_.instance_generation))
                return fail("invalid logical node route");
        } else {
            std::string node_id;
            uint64_t generation = 0;
            if (!zb::rpc_route::GetLogicalNodeRoute(*controller, &node_id, &generation) ||
                node_id != target_.node_id || generation != target_.instance_generation)
                return fail("controller route conflicts with configured CMS target");
        }
        Stub stub(channel_.get());
        std::invoke(method, &stub, controller, &request, reply, nullptr);
        if (controller->Failed()) return fail("data transport failure: " + controller->ErrorText());
        if (!reply->has_status()) return fail("data RPC missing status");
        if (static_cast<int>(reply->status().code()) != 0)
            return fail("data RPC status=" + std::to_string(reply->status().code()) + ": " + reply->status().message());
        if (error) error->clear();
        return true;
    }

private:
    DataNodeTarget target_;
    uint32_t timeout_ms_;
    std::mutex connect_mu_;
    std::unique_ptr<brpc::Channel> channel_;
};

// 磁盘正式协议的全部入口，真实磁盘与进程内仿真磁盘共用。
// 使用 MDS 位置中的节点身份；不将生成式模型读取当作真实复制校验。
class DiskNodeClient final : public RoutedDataNodeClient {
public:
    using RoutedDataNodeClient::RoutedDataNodeClient;
#define ZB_DISK_API(Name, Request, Reply) \
    bool Name(const Request& request, rpc::Reply* reply, std::string* error, brpc::Controller* c = nullptr) { \
        return Call<rpc::RealNodeService_Stub>(request, reply, &rpc::RealNodeService_Stub::Name, error, c); \
    }
    // 对象区间读写、删除和库存枚举；越界/空间不足按正式状态返回。
    ZB_DISK_API(WriteObject, rpc::WriteObjectRequest, WriteObjectReply)
    ZB_DISK_API(ReadObject, rpc::ReadObjectRequest, ReadObjectReply)
    ZB_DISK_API(DeleteObject, rpc::DeleteObjectRequest, DeleteObjectReply)
    ZB_DISK_API(ListObjects, rpc::ListObjectsRequest, ListObjectsReply)
    // 清空仅由显式调用触发，不能用于绕过排空/零引用门禁。
    ZB_DISK_API(ResetNodeData, rpc::ResetNodeDataRequest, ResetNodeDataReply)
    // 整文件归档搬运读取，以及对象/文件归档状态反馈。
    ZB_DISK_API(ReadArchivedFile, rpc::ReadArchivedFileRequest, ReadArchivedFileReply)
    ZB_DISK_API(UpdateArchiveState, rpc::UpdateArchiveStateRequest, UpdateArchiveStateReply)
    ZB_DISK_API(UpdateFileArchiveState, rpc::UpdateFileArchiveStateRequest, UpdateFileArchiveStateReply)
    ZB_DISK_API(GetDiskReport, google::protobuf::Empty, DiskReportReply)
    // 文件级源清理、读计划、写计划和版本条件提交。
    ZB_DISK_API(DeleteFileMeta, rpc::DeleteFileMetaRequest, DeleteFileMetaReply)
    ZB_DISK_API(ResolveFileRead, rpc::ResolveFileReadRequest, ResolveFileReadReply)
    ZB_DISK_API(AllocateFileWrite, rpc::AllocateFileWriteRequest, AllocateFileWriteReply)
    ZB_DISK_API(CommitFileWrite, rpc::CommitFileWriteRequest, CommitFileWriteReply)
    // 排空先关闭写入准入，迁入安装完成后才能请求 MDS 切换位置。
    ZB_DISK_API(BeginNodeDrain, rpc::BeginNodeDrainRequest, BeginNodeDrainReply)
    ZB_DISK_API(InstallMigratedFile, rpc::InstallMigratedFileRequest, InstallMigratedFileReply)
#undef ZB_DISK_API
};

// 光盘正式协议的全部现有入口；不自行发明光盘前台写会话协议。
// 异步受理不表示介质完成，清单查询也不表示 MDS 已提交。
class OpticalNodeClient final : public RoutedDataNodeClient {
public:
    using RoutedDataNodeClient::RoutedDataNodeClient;
#define ZB_OPTICAL_API(Name, Request, Reply) \
    bool Name(const rpc::Request& request, rpc::Reply* reply, std::string* error, brpc::Controller* c = nullptr) { \
        return Call<rpc::OpticalNodeService_Stub>(request, reply, &rpc::OpticalNodeService_Stub::Name, error, c); \
    }
    // MDS 归档下发。部分受理必须检查原始 admissions，不得丢弃成功项。
    // 真实整库交接：冻结、分块复制、重建接收、权威提交后隔离旧源。
    ZB_OPTICAL_API(ManageLibraryHandoff, OpticalLibraryHandoffRequest, OpticalLibraryHandoffReply)
    ZB_OPTICAL_API(SendArchiveMetadata, SendArchiveMetadataRequest, ArchiveReply)
    // 查询现存阶段任务/失败信息；任务终态不自动表示 MDS 提交或介质可读。
    ZB_OPTICAL_API(ListArchiveTasks, ListArchiveTasksRequest, ListArchiveTasksReply)
    // MDS 拉取完成介质与文件清单；需完整自动翻页时使用 OpticalArchiveClient。
    ZB_OPTICAL_API(ListArchiveImages, ListArchiveImagesRequest, ListArchiveImagesReply)
    ZB_OPTICAL_API(GetArchiveImageFiles, GetArchiveImageFilesRequest, GetArchiveImageFilesReply)
    // Subscribe 必须传调用方创建了 brpc Stream 的 Controller，并维护 Stream 生命周期。
    ZB_OPTICAL_API(SubscribeReadEvents, SubscribeReadEventsRequest, SubscribeReadEventsReply)
    // 异步准备、独立读会话、按区间取数和两级引用释放。
    ZB_OPTICAL_API(PrepareRead, PrepareReadRequest, PrepareReadReply)
    ZB_OPTICAL_API(AcquireReadSession, AcquireReadSessionRequest, AcquireReadSessionReply)
    ZB_OPTICAL_API(ReadPreparedFile, ReadPreparedFileRequest, ReadPreparedFileReply)
    ZB_OPTICAL_API(ReleaseReadSession, ReleaseReadSessionRequest, ReleaseReadSessionReply)
    ZB_OPTICAL_API(ReleasePreparedRead, ReleasePreparedReadRequest, ReleasePreparedReadReply)
    // 兼容旧调用方；新代码优先使用上面的准备/会话协议。
    ZB_OPTICAL_API(RequestAsyncReadFile, RequestAsyncReadFileRequest, RequestAsyncReadFileReply)
    ZB_OPTICAL_API(ReadObjectByTaskId, ReadObjectByTaskIdRequest, ReadObjectByTaskIdReply)
    ZB_OPTICAL_API(ReadObjectByInodeId, ReadObjectByInodeIdRequest, ReadObjectByInodeIdReply)
#undef ZB_OPTICAL_API
};

} // namespace zb::scheduler::api
