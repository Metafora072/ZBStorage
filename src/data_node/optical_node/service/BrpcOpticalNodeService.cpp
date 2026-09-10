#include "BrpcOpticalNodeService.h"

#include <brpc/controller.h>

#include <cstddef>
#include <utility>

namespace zb::optical_node {

namespace {

// volumemanager::ErrorCode -> MdsStatus 的最小映射，仅区分调用方能处理的类别。
zb::rpc::MdsStatusCode ToMdsStatus(volumemanager::ErrorCode code) {
    switch (code) {
        case volumemanager::ErrorCode::SUCCESS:
            return zb::rpc::MDS_OK;
        case volumemanager::ErrorCode::FILE_NOT_FOUND:
        case volumemanager::ErrorCode::VOLUME_NOT_FOUND:
        case volumemanager::ErrorCode::INODE_NOT_FOUND:
        case volumemanager::ErrorCode::TASK_NOT_FOUND:
            return zb::rpc::MDS_NOT_FOUND;
        case volumemanager::ErrorCode::INVALID_PATH:
        case volumemanager::ErrorCode::INVALID_VOLUME_FORMAT:
        case volumemanager::ErrorCode::INVALID_VOLUME_ID:
        case volumemanager::ErrorCode::INVALID_PARAMETER:
            return zb::rpc::MDS_INVALID_ARGUMENT;
        default:
            return zb::rpc::MDS_INTERNAL_ERROR;
    }
}

} // namespace

BrpcOpticalNodeService::BrpcOpticalNodeService(OpticalStorageServiceImpl* service)
    : service_(service) {}

void BrpcOpticalNodeService::SendArchiveMetadata(google::protobuf::RpcController* cntl_base,
                                                 const zb::rpc::SendArchiveMetadataRequest* request,
                                                 zb::rpc::ArchiveReply* response,
                                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    // protobuf 请求 -> manager 领域对象。
    optical_node_manager::SendArchiveMetadataRequest domain_request;
    domain_request.batch_id = request->batch_id();
    domain_request.files.reserve(static_cast<std::size_t>(request->files_size()));
    for (const zb::rpc::ArchiveFile& file : request->files()) {
        optical_node_manager::ArchiveFileInfo info;
        info.inode_id = file.inode_id();
        info.size = file.size();
        info.object_unit_size = file.object_unit_size();
        info.target_node_id = file.target_node_id();
        info.target_disk_id = file.target_disk_id();
        domain_request.files.push_back(std::move(info));
    }

    const volumemanager::ErrorCode code = service_->SendArchiveMetadata(domain_request);
    response->mutable_status()->set_code(ToMdsStatus(code));
    if (code != volumemanager::ErrorCode::SUCCESS) {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

} // namespace zb::optical_node
