#include "OpticalArchiveClient.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include "common/rpc/LogicalNodeRoute.h"

namespace zb::scheduler::api {
namespace {
bool Fail(std::string* error, const std::string& message) {
    if (error) *error = message;
    return false;
}
} // namespace

OpticalArchiveClient::OpticalArchiveClient(std::string address, std::string node_id,
    uint64_t instance_generation, bool modeled, uint32_t timeout_ms)
    : address_(std::move(address)), node_id_(std::move(node_id)), generation_(instance_generation),
      modeled_(modeled), timeout_ms_(timeout_ms ? timeout_ms : 5000) {}
OpticalArchiveClient::~OpticalArchiveClient() = default;

bool OpticalArchiveClient::Connect(std::string* error) {
    std::lock_guard<std::mutex> lock(connect_mu_);
    if (channel_) return true;
    if (address_.empty() || node_id_.empty() || generation_ == 0)
        return Fail(error, "CMS address, node_id and nonzero instance generation required");
    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = static_cast<int>(std::min<uint32_t>(timeout_ms_, std::numeric_limits<int>::max()));
    options.max_retry = 0;
    if (channel->Init(address_.c_str(), &options) != 0) return Fail(error, "cannot initialize optical channel");
    channel_ = std::move(channel);
    return true;
}

template <class Request, class Reply, class Method>
bool OpticalArchiveClient::Call(const Request& request, Reply* reply, Method method, std::string* error) {
    if (!reply) return Fail(error, "optical reply is null");
    reply->Clear();
    if (request.node_id() != node_id_ || request.instance_generation() != generation_)
        return Fail(error, "request identity differs from configured CMS identity");
    if (!Connect(error)) return false;
    brpc::Controller controller;
    if (!zb::rpc_route::SetLogicalNodeRoute(&controller, node_id_, generation_))
        return Fail(error, "cannot attach optical logical route");
    rpc::OpticalNodeService_Stub stub(channel_.get());
    std::invoke(method, &stub, &controller, &request, reply, nullptr);
    if (controller.Failed()) return Fail(error, "optical transport failure: " + controller.ErrorText());
    if (!reply->has_status()) return Fail(error, "missing optical status");
    if (reply->status().code() != rpc::OPTICAL_OK)
        return Fail(error, rpc::OpticalStatusCode_Name(reply->status().code()) + ": " + reply->status().message());
    if (reply->node_id() != node_id_ || reply->instance_generation() != generation_ || reply->modeled() != modeled_)
        return Fail(error, "optical reply identity or backend differs from CMS expectation");
    if (reply->snapshot_token().empty() || reply->has_more() != (reply->next_offset() != 0))
        return Fail(error, "invalid optical pagination response");
    if (!request.snapshot_token().empty() && request.snapshot_token() != reply->snapshot_token())
        return Fail(error, "optical snapshot changed without conflict status");
    if (error) error->clear();
    return true;
}

bool OpticalArchiveClient::ListArchiveImages(const rpc::ListArchiveImagesRequest& request,
    rpc::ListArchiveImagesReply* reply, std::string* error) {
    return Call(request, reply, &rpc::OpticalNodeService_Stub::ListArchiveImages, error);
}
bool OpticalArchiveClient::GetArchiveImageFiles(const rpc::GetArchiveImageFilesRequest& request,
    rpc::GetArchiveImageFilesReply* reply, std::string* error) {
    if (!Call(request, reply, &rpc::OpticalNodeService_Stub::GetArchiveImageFiles, error)) return false;
    if (!reply->has_image() || reply->image().disk_id() != request.disk_id() ||
        reply->image().image_id() != request.image_id()) return Fail(error, "optical image identity mismatch");
    return true;
}

bool OpticalArchiveClient::ListAllImages(std::vector<rpc::ArchiveImageInfo>* output,
    std::string* error, uint32_t page_size) {
    if (!output) return Fail(error, "image output is null");
    output->clear();
    if (page_size == 0 || page_size > 1024) return Fail(error, "page_size must be 1..1024");
    rpc::ListArchiveImagesRequest request;
    request.set_node_id(node_id_);
    request.set_instance_generation(generation_);
    request.set_limit(page_size);
    std::vector<rpc::ArchiveImageInfo> result;
    for (;;) {
        rpc::ListArchiveImagesReply reply;
        if (!ListArchiveImages(request, &reply, error)) return false;
        result.insert(result.end(), reply.images().begin(), reply.images().end());
        if (!reply.has_more()) break;
        if (reply.images_size() == 0 || reply.next_offset() != request.offset() + reply.images_size())
            return Fail(error, "invalid optical image continuation");
        request.set_offset(reply.next_offset());
        request.set_snapshot_token(reply.snapshot_token());
    }
    *output = std::move(result);
    return true;
}

bool OpticalArchiveClient::ListArchiveTasks(const rpc::ListArchiveTasksRequest& request,
    rpc::ListArchiveTasksReply* reply, std::string* error) {
    return Call(request, reply, &rpc::OpticalNodeService_Stub::ListArchiveTasks, error);
}

bool OpticalArchiveClient::ListAllTasks(std::vector<rpc::ArchiveTaskInfo>* output,
    std::string* error, uint32_t page_size) {
    if (!output) return Fail(error, "task output is null");
    output->clear();
    if (page_size == 0 || page_size > 1024) return Fail(error, "page_size must be 1..1024");
    rpc::ListArchiveTasksRequest request;
    request.set_node_id(node_id_);
    request.set_instance_generation(generation_);
    request.set_limit(page_size);
    std::vector<rpc::ArchiveTaskInfo> result;
    for (;;) {
        rpc::ListArchiveTasksReply reply;
        if (!ListArchiveTasks(request, &reply, error)) return false;
        result.insert(result.end(), reply.tasks().begin(), reply.tasks().end());
        if (!reply.has_more()) break;
        if (reply.tasks_size() == 0 || reply.next_offset() != request.offset() + reply.tasks_size())
            return Fail(error, "invalid optical task continuation");
        request.set_offset(reply.next_offset());
        request.set_snapshot_token(reply.snapshot_token());
    }
    *output = std::move(result);
    return true;
}

bool OpticalArchiveClient::GetAllImageFiles(const std::string& disk_id, const std::string& image_id,
    std::vector<rpc::ArchiveImageFile>* output, std::string* error, uint32_t page_size) {
    if (!output) return Fail(error, "file output is null");
    output->clear();
    if (page_size == 0 || page_size > 1024) return Fail(error, "page_size must be 1..1024");
    rpc::GetArchiveImageFilesRequest request;
    request.set_node_id(node_id_);
    request.set_instance_generation(generation_);
    request.set_disk_id(disk_id);
    request.set_image_id(image_id);
    request.set_limit(page_size);
    std::vector<rpc::ArchiveImageFile> result;
    for (;;) {
        rpc::GetArchiveImageFilesReply reply;
        if (!GetArchiveImageFiles(request, &reply, error)) return false;
        result.insert(result.end(), reply.files().begin(), reply.files().end());
        if (!reply.has_more()) break;
        if (reply.files_size() == 0 || reply.next_offset() != request.offset() + reply.files_size())
            return Fail(error, "invalid optical file continuation");
        request.set_offset(reply.next_offset());
        request.set_snapshot_token(reply.snapshot_token());
    }
    *output = std::move(result);
    return true;
}

} // namespace zb::scheduler::api
