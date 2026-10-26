#include "BrpcOpticalNodeService.h"

#include <brpc/controller.h>
#include <brpc/stream.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "common/rpc/ArchiveInventoryPage.h"

namespace zb::optical_node {

namespace {

// volumemanager::ErrorCode -> MdsStatus 的最小映射，仅区分调用方能处理的类别。
zb::rpc::MdsStatusCode ToMdsStatus(volumemanager::ErrorCode code) {
    switch (code) {
        case volumemanager::ErrorCode::SUCCESS:
            return zb::rpc::MDS_OK;
        // READ_DELAY 表示异步读任务已受理（task_id 有效），不算失败。
        case volumemanager::ErrorCode::READ_DELAY:
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

// volumemanager::ErrorCode -> OpticalStatusCode，供光盘读接口使用（不复用 MdsStatus）。
zb::rpc::OpticalStatusCode ToOpticalStatus(volumemanager::ErrorCode code) {
    switch (code) {
        case volumemanager::ErrorCode::SUCCESS:
        // READ_DELAY 表示准备任务已受理（task_id 有效），不算失败。
        case volumemanager::ErrorCode::READ_DELAY:
            return zb::rpc::OPTICAL_OK;
        // 文件准备尚未完成：既不是失败，也不是"不存在"。
        case volumemanager::ErrorCode::TASK_NOT_FINISH:
            return zb::rpc::OPTICAL_NOT_READY;
        case volumemanager::ErrorCode::FILE_NOT_FOUND:
        case volumemanager::ErrorCode::VOLUME_NOT_FOUND:
        case volumemanager::ErrorCode::INODE_NOT_FOUND:
        case volumemanager::ErrorCode::TASK_NOT_FOUND:
        case volumemanager::ErrorCode::TASK_ALREADY_FINISH:
            return zb::rpc::OPTICAL_NOT_FOUND;
        case volumemanager::ErrorCode::INVALID_PATH:
        case volumemanager::ErrorCode::INVALID_VOLUME_FORMAT:
        case volumemanager::ErrorCode::INVALID_VOLUME_ID:
        case volumemanager::ErrorCode::INVALID_PARAMETER:
            return zb::rpc::OPTICAL_INVALID_ARGUMENT;
        default:
            // 含 MANAGER_NOT_READY：引擎未就绪属内部错误。
            // OPTICAL_NOT_READY 的语义是"文件准备尚未完成"，不能用于此处。
            return zb::rpc::OPTICAL_INTERNAL_ERROR;
    }
}

} // namespace

BrpcOpticalNodeService::BrpcOpticalNodeService(OpticalStorageServiceImpl* service)
    : service_(service) {}

void BrpcOpticalNodeService::ManageLibraryHandoff(google::protobuf::RpcController*,
    const zb::rpc::OpticalLibraryHandoffRequest* request, zb::rpc::OpticalLibraryHandoffReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!request || !response || !service_) return;
    service_->ManageLibraryHandoff(*request, response);
}

namespace {
zb::rpc::ArchiveImageInfo ImageInfo(const optical_node_manager::OpticalNodeManager::ArchivedImage& image) {
    zb::rpc::ArchiveImageInfo result;
    result.set_disk_id("optical-disk-" + image.location.disk_id);
    result.set_image_id("img-" + std::to_string(image.volume_id));
    result.set_image_offset(image.location.offset_in_disc);
    result.set_image_length(image.location.image_size_bytes);
    result.set_sha256(image.location.sha256_hex);
    return result;
}

template <class Request, class Reply>
bool ValidateInventory(OpticalStorageServiceImpl* service, const Request* request, Reply* reply) {
    if (!reply) return false;
    reply->Clear();
    if (!request) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        return false;
    }
    if (!zb::archive_inventory::ValidatePage(*request, reply->mutable_status())) return false;
    if (!service) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INTERNAL_ERROR);
        return false;
    }
    const auto identity = service->GetReplicationStatus();
    if (request->node_id() != identity.node_id ||
        request->instance_generation() != service->instance_generation()) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_STALE_GENERATION);
        reply->mutable_status()->set_message("node identity changed; refresh CMS catalog");
        return false;
    }
    reply->set_node_id(identity.node_id);
    reply->set_instance_generation(service->instance_generation());
    reply->set_modeled(false);
    if (!service->IsArchiveEngineReady() || !service->ArchiveIndexReady() ||
        !service->GetLibraryInventory().metadata_ready) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_READY);
        reply->mutable_status()->set_message("archive metadata recovery not ready");
        return false;
    }
    return true;
}
} // namespace

void BrpcOpticalNodeService::ListArchiveTasks(google::protobuf::RpcController*,
    const zb::rpc::ListArchiveTasksRequest* request, zb::rpc::ListArchiveTasksReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!ValidateInventory(service_, request, reply)) return;
    std::vector<zb::rpc::ArchiveTaskInfo> tasks;
    for (const auto& item : service_->ListArchiveTasks()) {
        auto& task = tasks.emplace_back();
        task.set_task_id(std::to_string(item.task_id));
        task.set_inode_id(item.inode_id_num == WR_task::WRTask::INVALID_INODE_ID ? 0 : item.inode_id_num);
        task.set_phase(std::string(WR_task::WRTaskTypeToString(item.type)) + "/" + WR_task::WRTaskStateToString(item.state));
        task.set_terminal(item.state == WR_task::WRTaskState::FINISH || item.state == WR_task::WRTaskState::FAILED);
        task.set_failed(item.state == WR_task::WRTaskState::FAILED);
        task.set_error(item.last_error_detail);
        task.set_state_changed_at_ms(item.state_changed_at_ms);
        if (!item.disk_id.empty()) task.set_disk_id("optical-disk-" + item.disk_id);
        if (!item.volume_id.empty()) task.add_image_ids("img-" + item.volume_id);
        for (const auto& image : item.volume_ids) task.add_image_ids("img-" + image);
    }
    size_t end = 0;
    if (!zb::archive_inventory::Page(*request, zb::archive_inventory::TaskToken(tasks), tasks.size(), reply, &end)) return;
    for (size_t i = request->offset(); i < end; ++i) *reply->add_tasks() = tasks[i];
}

void BrpcOpticalNodeService::ListArchiveImages(google::protobuf::RpcController*,
    const zb::rpc::ListArchiveImagesRequest* request, zb::rpc::ListArchiveImagesReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!ValidateInventory(service_, request, reply)) return;
    std::vector<zb::rpc::ArchiveImageInfo> images;
    for (const auto& image : service_->ListArchivedImages()) images.push_back(ImageInfo(image));
    size_t end = 0;
    if (!zb::archive_inventory::Page(*request, zb::archive_inventory::Token(images), images.size(), reply, &end)) return;
    for (size_t i = request->offset(); i < end; ++i) *reply->add_images() = images[i];
}

void BrpcOpticalNodeService::GetArchiveImageFiles(google::protobuf::RpcController*,
    const zb::rpc::GetArchiveImageFilesRequest* request, zb::rpc::GetArchiveImageFilesReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!ValidateInventory(service_, request, reply)) return;
    if (request->image_id().empty() || request->disk_id().empty()) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        reply->mutable_status()->set_message("disk_id and image_id are required");
        return;
    }
    for (const auto& image : service_->ListArchivedImages()) {
        const auto info = ImageInfo(image);
        if (request->image_id() != info.image_id() || request->disk_id() != info.disk_id()) continue;
        std::vector<volumemanager::FileMetadata> metadata;
        std::string error;
        if (!service_->ReadArchivedImageFiles(image, &metadata, &error)) {
            reply->mutable_status()->set_code(zb::rpc::OPTICAL_INTERNAL_ERROR);
            reply->mutable_status()->set_message(error);
            return;
        }
        std::vector<zb::rpc::ArchiveImageFile> files;
        for (const auto& item : metadata) {
            auto& file = files.emplace_back();
            file.set_inode_id(item.inode_id);
            file.set_size(item.file_size);
            // 卷格式没有业务文件版本；不能用卷格式版本或默认值冒充。
            file.set_has_file_version(false);
        }
        size_t end = 0;
        if (!zb::archive_inventory::Page(*request, zb::archive_inventory::Token({info}, files),
                                        files.size(), reply, &end)) return;
        *reply->mutable_image() = info;
        for (size_t i = request->offset(); i < end; ++i) {
            auto* file = reply->add_files(); *file = files[i];
            if (request->digest_inode_id() == file->inode_id()) {
                std::string sha;
                if (!service_->HashArchivedFile(image, metadata[i], &sha, &error)) {
                    reply->mutable_status()->set_code(zb::rpc::OPTICAL_INTERNAL_ERROR);
                    reply->mutable_status()->set_message(error); return;
                }
                file->set_payload_sha256(sha);
            }
        }
        return;
    }
    reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND);
    reply->mutable_status()->set_message("image absent from completed local media index");
}

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
    domain_request.end_of_batch = request->end_of_batch();
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

void BrpcOpticalNodeService::SubscribeReadEvents(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::SubscribeReadEventsRequest* request,
    zb::rpc::SubscribeReadEventsReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    // 建立通知通道要求客户端在本 RPC 上附带 Stream；client_id 标识对应 FUSE 实例。
    auto* cntl = static_cast<brpc::Controller*>(cntl_base);
    if (request->client_id().empty() || !cntl->has_remote_stream()) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing client_id or remote stream");
        return;
    }

    ReadEventSessionRegistry* registry = service_->read_event_sessions();
    brpc::StreamOptions stream_options;
    stream_options.handler = registry->handler();
    brpc::StreamId server_stream = brpc::INVALID_STREAM_ID;
    // StreamAccept 必须在 done->Run() 之前完成：客户端收到 session_id 时通道已建立。
    if (brpc::StreamAccept(&server_stream, *cntl, &stream_options) != 0) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("accept remote stream failed");
        return;
    }

    const uint64_t session_id = service_->SubscribeReadEvents(request->client_id(), server_stream);
    if (session_id == 0) {
        // 注册失败：回收刚接受的 Stream，不能返回一个无效的 session_id。
        brpc::StreamClose(server_stream);
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INTERNAL_ERROR);
        response->mutable_status()->set_message("register read event session failed");
        return;
    }

    response->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
    response->set_session_id(session_id);
}

void BrpcOpticalNodeService::PrepareRead(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::PrepareReadRequest* request,
    zb::rpc::PrepareReadReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    // 定位字段与 request_id 均为功能必填（只有 file_version 可选）。
    if (request->request_id().empty() || request->disk_id().empty() ||
        request->image_id().empty() || request->inode_id() == 0) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing required prepare read field");
        return;
    }
    // session_id 必须是已建立的通知会话，否则完成后无从通知。
    if (!service_->read_event_sessions()->HasSession(request->session_id())) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND);
        response->mutable_status()->set_message("read event session not found");
        return;
    }

    OpticalStorageServiceImpl::PrepareReadParams params;
    params.session_id = request->session_id();
    params.request_id = request->request_id();
    params.inode_id = std::to_string(request->inode_id());
    params.disk_id = request->disk_id();
    params.image_id = request->image_id();
    params.file_size = request->file_size();
    params.has_file_version = request->has_file_version();
    params.file_version = request->file_version();

    uint64_t task_id = 0;
    const volumemanager::ErrorCode code = service_->PrepareRead(params, &task_id);
    response->mutable_status()->set_code(ToOpticalStatus(code));
    if (code == volumemanager::ErrorCode::READ_DELAY ||
        code == volumemanager::ErrorCode::SUCCESS) {
        // 已受理：回传原 request_id 与 task_id；成功不代表文件可读。
        response->set_request_id(request->request_id());
        response->set_task_id(task_id);
    } else {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

void BrpcOpticalNodeService::AcquireReadSession(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::AcquireReadSessionRequest* request,
    zb::rpc::AcquireReadSessionReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    optical_node_manager::AcquireReadSessionResult result;
    const volumemanager::ErrorCode code = service_->AcquireReadSession(request->task_id(), &result);
    response->mutable_status()->set_code(ToOpticalStatus(code));
    if (code == volumemanager::ErrorCode::SUCCESS) {
        response->set_read_handle(result.read_handle);
        response->set_file_size(result.file_size);
        response->set_max_read_bytes(result.max_read_bytes);
    } else {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

void BrpcOpticalNodeService::ReadPreparedFile(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReadPreparedFileRequest* request,
    zb::rpc::ReadPreparedFileReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    std::string data;
    optical_node_manager::ReadPreparedFileResult result;
    const volumemanager::ErrorCode code = service_->ReadPreparedFile(
        request->read_handle(), request->offset(), request->size(), &data, &result);
    response->mutable_status()->set_code(ToOpticalStatus(code));
    if (code == volumemanager::ErrorCode::SUCCESS) {
        response->set_data(std::move(data));
        response->set_bytes_read(result.bytes_read);
        response->set_eof(result.eof);
    } else {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

void BrpcOpticalNodeService::ReleaseReadSession(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReleaseReadSessionRequest* request,
    zb::rpc::ReleaseReadSessionReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    const volumemanager::ErrorCode code = service_->ReleaseReadSession(request->read_handle());
    response->mutable_status()->set_code(ToOpticalStatus(code));
    if (code != volumemanager::ErrorCode::SUCCESS) {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

void BrpcOpticalNodeService::ReleasePreparedRead(
    google::protobuf::RpcController* cntl_base,
    const zb::rpc::ReleasePreparedReadRequest* request,
    zb::rpc::ReleasePreparedReadReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!response) {
        return;
    }
    if (!service_ || !request) {
        response->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        response->mutable_status()->set_message("optical node service not initialized");
        return;
    }

    const volumemanager::ErrorCode code = service_->ReleasePreparedRead(request->task_id());
    response->mutable_status()->set_code(ToOpticalStatus(code));
    if (code != volumemanager::ErrorCode::SUCCESS) {
        response->mutable_status()->set_message(volumemanager::GetErrorMessage(code));
    }
}

} // namespace zb::optical_node
