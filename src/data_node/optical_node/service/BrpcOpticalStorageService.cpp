#include "BrpcOpticalStorageService.h"

#include <brpc/controller.h>

namespace zb::optical_node {

namespace {

zb::rpc::StatusCode ToProtoStatusCode(zb::msg::StatusCode code) {
    switch (code) {
        case zb::msg::StatusCode::kOk:
            return zb::rpc::STATUS_OK;
        case zb::msg::StatusCode::kInvalidArgument:
            return zb::rpc::STATUS_INVALID_ARGUMENT;
        case zb::msg::StatusCode::kNotFound:
            return zb::rpc::STATUS_NOT_FOUND;
        case zb::msg::StatusCode::kIoError:
            return zb::rpc::STATUS_IO_ERROR;
        case zb::msg::StatusCode::kInternalError:
        default:
            return zb::rpc::STATUS_INTERNAL_ERROR;
    }
}

void FillStatus(const zb::msg::Status& status, zb::rpc::Status* out) {
    if (!out) {
        return;
    }
    out->set_code(ToProtoStatusCode(status.code));
    out->set_message(status.message);
}

} // namespace

BrpcOpticalStorageService::BrpcOpticalStorageService(OpticalStorageServiceImpl* service) : service_(service) {}

void BrpcOpticalStorageService::WriteObject(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::WriteObjectRequest* request,
                                            zb::rpc::WriteObjectReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::WriteObjectRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.SetArchiveObjectId(request->object_id());
    internal_req.offset = request->offset();
    internal_req.data = request->data();
    internal_req.is_replication = request->is_replication();
    internal_req.epoch = request->epoch();
    internal_req.archive_op_id = request->archive_op_id();
    internal_req.inode_id = request->inode_id();
    internal_req.file_id = request->file_id();
    internal_req.file_path = request->file_path();
    internal_req.file_size = request->file_size();
    internal_req.file_offset = request->file_offset();
    internal_req.file_mode = request->file_mode();
    internal_req.file_uid = request->file_uid();
    internal_req.file_gid = request->file_gid();
    internal_req.file_mtime = request->file_mtime();
    internal_req.file_object_index = request->file_object_index();

    const zb::msg::WriteObjectReply internal_reply = service_->WriteObject(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
    response->set_image_id(internal_reply.image_id);
    response->set_image_offset(internal_reply.image_offset);
    response->set_image_length(internal_reply.image_length);
}

void BrpcOpticalStorageService::ReadObject(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::ReadObjectRequest* request,
                                           zb::rpc::ReadObjectReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::ReadObjectRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.SetArchiveObjectId(request->object_id());
    internal_req.offset = request->offset();
    internal_req.size = request->size();
    internal_req.image_id = request->image_id();
    internal_req.image_offset = request->image_offset();
    internal_req.image_length = request->image_length();

    const zb::msg::ReadObjectReply internal_reply = service_->ReadObject(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
    response->set_data(internal_reply.data);
}

void BrpcOpticalStorageService::DeleteObject(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::DeleteObjectRequest* request,
                                             zb::rpc::DeleteObjectReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::DeleteObjectRequest internal_req;
    internal_req.disk_id = request->disk_id();
    internal_req.SetArchiveObjectId(request->object_id());
    const zb::msg::DeleteObjectReply internal_reply = service_->DeleteObject(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
}

void BrpcOpticalStorageService::ResetNodeData(google::protobuf::RpcController* cntl_base,
                                              const zb::rpc::ResetNodeDataRequest* request,
                                              zb::rpc::ResetNodeDataReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("ResetNodeData is only supported for real and virtual nodes");
}

void BrpcOpticalStorageService::ReadArchivedFile(google::protobuf::RpcController* cntl_base,
                                                 const zb::rpc::ReadArchivedFileRequest* request,
                                                 zb::rpc::ReadArchivedFileReply* response,
                                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::ReadArchivedFileRequest internal_req;
    internal_req.disc_id = request->disc_id();
    internal_req.inode_id = request->inode_id();
    internal_req.file_id = request->file_id();
    internal_req.offset = request->offset();
    internal_req.size = request->size();

    const zb::msg::ReadArchivedFileReply internal_reply = service_->ReadArchivedFile(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_bytes(internal_reply.bytes);
    response->set_data(internal_reply.data);
}

void BrpcOpticalStorageService::UpdateArchiveState(google::protobuf::RpcController* cntl_base,
                                                   const zb::rpc::UpdateArchiveStateRequest* request,
                                                   zb::rpc::UpdateArchiveStateReply* response,
                                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!service_ || !request || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    const std::string object_id = request->object_id();
    const zb::msg::Status internal_status =
        service_->UpdateArchiveState(request->disk_id(), object_id, request->archive_state(), request->version());
    FillStatus(internal_status, response->mutable_status());
}

void BrpcOpticalStorageService::UpdateFileArchiveState(google::protobuf::RpcController* cntl_base,
                                                       const zb::rpc::UpdateFileArchiveStateRequest* request,
                                                       zb::rpc::UpdateFileArchiveStateReply* response,
                                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    zb::rpc::Status* status = response->mutable_status();
    status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    status->set_message("optical node does not support UpdateFileArchiveState");
}

void BrpcOpticalStorageService::GetDiskReport(google::protobuf::RpcController* cntl_base,
                                              const google::protobuf::Empty* request,
                                              zb::rpc::DiskReportReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!service_ || !response) {
        zb::rpc::Status* status = response ? response->mutable_status() : nullptr;
        if (status) {
            status->set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            status->set_message("Service not initialized");
        }
        return;
    }

    zb::msg::DiskReportReply internal_reply = service_->GetDiskReport();
    FillStatus(internal_reply.status, response->mutable_status());
    for (const auto& report : internal_reply.reports) {
        zb::rpc::DiskReport* item = response->add_reports();
        item->set_id(report.id);
        item->set_mount_point(report.mount_point);
        item->set_capacity_bytes(report.capacity_bytes);
        item->set_free_bytes(report.free_bytes);
        item->set_is_healthy(report.is_healthy);
    }
}

void BrpcOpticalStorageService::DeleteFileMeta(google::protobuf::RpcController* cntl_base,
                                               const zb::rpc::DeleteFileMetaRequest* request,
                                               zb::rpc::DeleteFileMetaReply* response,
                                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    zb::rpc::Status* status = response->mutable_status();
    status->set_code(zb::rpc::STATUS_NOT_FOUND);
    status->set_message("optical node does not host file metadata");
}

void BrpcOpticalStorageService::ResolveFileRead(google::protobuf::RpcController* cntl_base,
                                                const zb::rpc::ResolveFileReadRequest* request,
                                                zb::rpc::ResolveFileReadReply* response,
                                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    zb::rpc::Status* status = response->mutable_status();
    status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    status->set_message("optical node does not support ResolveFileRead");
}

void BrpcOpticalStorageService::AllocateFileWrite(google::protobuf::RpcController* cntl_base,
                                                  const zb::rpc::AllocateFileWriteRequest* request,
                                                  zb::rpc::AllocateFileWriteReply* response,
                                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    zb::rpc::Status* status = response->mutable_status();
    status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    status->set_message("optical node does not support AllocateFileWrite");
}

void BrpcOpticalStorageService::CommitFileWrite(google::protobuf::RpcController* cntl_base,
                                                const zb::rpc::CommitFileWriteRequest* request,
                                                zb::rpc::CommitFileWriteReply* response,
                                                google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    zb::rpc::Status* status = response->mutable_status();
    status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    status->set_message("optical node does not support CommitFileWrite");
}

} // namespace zb::optical_node
