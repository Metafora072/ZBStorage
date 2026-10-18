#pragma once
#include <memory>
#include <cstdint>
#include <utility>
#include <mutex>
#include <string>
#include <algorithm>
#include <limits>
#include <functional>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include "mds.pb.h"

namespace zb::scheduler::api {
// Scheduler 配套的 MDS 权威提交入口，不修改调用方清单，不自动重试变更。
// false 保留原始回复；true 的含义依具体 RPC（例如 commit=false 仅查询）。
class MdsPlacementClient {
public:
    explicit MdsPlacementClient(std::string address, uint32_t timeout_ms = 5000)
        : address_(std::move(address)), timeout_ms_(timeout_ms ? timeout_ms : 5000) {}
#define ZB_MDS_PLACEMENT_API(Name, Request, Reply) \
    bool Name(const rpc::Request& request, rpc::Reply* reply, std::string* error) { \
        return Call(request, reply, &rpc::MdsService_Stub::Name, error); \
    }
    // 模型磁盘归档：租约、版本和位置的验证/条件提交，不是实际载荷复制。
    ZB_MDS_PLACEMENT_API(ModelOpticalArchive, ModelOpticalArchiveRequest, ModelOpticalArchiveReply)
    // 已排空真实磁盘源的归档完成确认，额外校验真实源与介质解压摘要。
    ZB_MDS_PLACEMENT_API(CommitPhysicalOpticalArchive, ModelOpticalArchiveRequest, ModelOpticalArchiveReply)
    // 模型光盘间逐文件位置 CAS；必须先有目标持久接收。
    ZB_MDS_PLACEMENT_API(CommitModelOpticalMigration, CommitModelOpticalMigrationRequest, CommitModelOpticalMigrationReply)
    // 真实整库交接；commit=false 仅查同一清单，不能凭回复状态码推断已提交。
    ZB_MDS_PLACEMENT_API(CommitOpticalLibraryHandoff, CommitOpticalLibraryHandoffRequest, CommitOpticalLibraryHandoffReply)
    // 权威位置查询与零引用验证，不能用本地完成/计数替代。
    ZB_MDS_PLACEMENT_API(GetFileLocation, GetFileLocationRequest, GetFileLocationReply)
    ZB_MDS_PLACEMENT_API(VerifyNodeReferences, VerifyNodeReferencesRequest, VerifyNodeReferencesReply)
#undef ZB_MDS_PLACEMENT_API
private:
    template <class Request, class Reply, class Method>
    bool Call(const Request& request, Reply* reply, Method method, std::string* error) {
        const auto fail = [&](const std::string& message) { if (error) *error = message; return false; };
        if (!reply) return fail("MDS reply is null");
        reply->Clear();
        {
            std::lock_guard<std::mutex> lock(connect_mu_);
            if (!channel_) {
                if (address_.empty()) return fail("MDS address required");
                auto channel = std::make_unique<brpc::Channel>(); brpc::ChannelOptions options;
                options.timeout_ms = std::min<uint32_t>(timeout_ms_, std::numeric_limits<int>::max()); options.max_retry = 0;
                if (channel->Init(address_.c_str(), &options) != 0) return fail("cannot initialize MDS channel");
                channel_ = std::move(channel);
            }
        }
        brpc::Controller controller; rpc::MdsService_Stub stub(channel_.get());
        std::invoke(method, &stub, &controller, &request, reply, nullptr);
        if (controller.Failed()) return fail(controller.ErrorText());
        if (!reply->has_status()) return fail("MDS response missing status");
        if (reply->status().code() != rpc::MDS_OK) return fail(reply->status().message());
        if (error) error->clear(); return true;
    }
    std::string address_;
    uint32_t timeout_ms_;
    std::mutex connect_mu_;
    std::unique_ptr<brpc::Channel> channel_;
};
} // namespace zb::scheduler::api
