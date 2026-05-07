#include "BrpcStorageService.h"

#include <brpc/controller.h>
#include <iostream>

#include "../../common/ObjectStore.h"
#include "../../../msg/status.h"

namespace zb::real_node {

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

void LogFailure(const char* method, const zb::msg::Status& status, const std::string& detail) {
    if (status.ok()) {
        return;
    }
    std::cerr << "[real_node] " << method
              << " failed: code=" << static_cast<int>(status.code)
              << " msg=" << status.message;
    if (!detail.empty()) {
        std::cerr << " detail=" << detail;
    }
    std::cerr << std::endl;
}

void FillFileMeta(const zb::msg::FileMeta& in, zb::rpc::FileMeta* out) {
    if (!out) {
        return;
    }
    out->set_inode_id(in.inode_id);
    out->set_file_size(in.file_size);
    out->set_object_unit_size(in.object_unit_size);
    out->set_version(in.version);
    out->set_mtime_sec(in.mtime_sec);
    out->set_update_ts_ms(in.update_ts_ms);
}

zb::msg::FileMeta ToInternalFileMeta(const zb::rpc::FileMeta& in) {
    zb::msg::FileMeta out;
    out.inode_id = in.inode_id();
    out.file_size = in.file_size();
    out.object_unit_size = in.object_unit_size();
    out.version = in.version();
    out.mtime_sec = in.mtime_sec();
    out.update_ts_ms = in.update_ts_ms();
    return out;
}

FileArchiveState ToInternalFileArchiveState(zb::rpc::FileArchiveState state) {
    switch (state) {
        case zb::rpc::FILE_ARCHIVE_ARCHIVING:
            return FileArchiveState::kArchiving;
        case zb::rpc::FILE_ARCHIVE_ARCHIVED:
            return FileArchiveState::kArchived;
        case zb::rpc::FILE_ARCHIVE_PENDING:
        default:
            return FileArchiveState::kPending;
    }
}

void FillFileObjectSlice(const zb::msg::FileObjectSlice& in, zb::rpc::FileObjectSlice* out) {
    if (!out) {
        return;
    }
    out->set_object_index(in.object_index);
    out->set_object_id(in.object_id);
    out->set_disk_id(in.disk_id);
    out->set_object_offset(in.object_offset);
    out->set_length(in.length);
}

} // namespace

BrpcStorageService::BrpcStorageService(StorageServiceImpl* service) : service_(service) {}

void BrpcStorageService::WriteObject(google::protobuf::RpcController* cntl_base,
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

    zb::data_node::ObjectWriteRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    object_req.offset = request->offset();
    object_req.data = request->data();
    object_req.epoch = request->epoch();
    object_req.is_replication = request->is_replication();

    const zb::msg::Status st = service_->PutObject(object_req);
    FillStatus(st, response->mutable_status());
    LogFailure("WriteObject", st, "disk_id=" + request->disk_id() + " object_id=" + request->object_id());
    if (st.ok()) {
        response->set_bytes(static_cast<uint64_t>(request->data().size()));
    }
}

void BrpcStorageService::ReadObject(google::protobuf::RpcController* cntl_base,
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

    zb::data_node::ObjectReadRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    object_req.offset = request->offset();
    object_req.size = request->size();

    const zb::data_node::ObjectReadResult result = service_->GetObject(object_req);
    FillStatus(result.status, response->mutable_status());
    LogFailure("ReadObject", result.status, "disk_id=" + request->disk_id() + " object_id=" + request->object_id());
    if (result.status.ok()) {
        response->set_bytes(static_cast<uint64_t>(result.data.size()));
        response->set_data(result.data);
    }
}

void BrpcStorageService::DeleteObject(google::protobuf::RpcController* cntl_base,
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

    zb::data_node::ObjectDeleteRequest object_req;
    object_req.disk_id = request->disk_id();
    object_req.object_id = request->object_id();
    const zb::msg::Status st = service_->DeleteObject(object_req);
    FillStatus(st, response->mutable_status());
    LogFailure("DeleteObject", st, "disk_id=" + request->disk_id() + " object_id=" + request->object_id());
}

void BrpcStorageService::ResetNodeData(google::protobuf::RpcController* cntl_base,
                                       const zb::rpc::ResetNodeDataRequest* request,
                                       zb::rpc::ResetNodeDataReply* response,
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

    zb::msg::ResetNodeDataRequest internal_req;
    internal_req.confirm_token = request->confirm_token();
    internal_req.purge_objects = request->purge_objects();
    internal_req.purge_file_meta = request->purge_file_meta();
    const zb::msg::ResetNodeDataReply internal_reply = service_->ResetNodeData(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    response->set_objects_removed(internal_reply.objects_removed);
    response->set_file_meta_removed(internal_reply.file_meta_removed);
    LogFailure("ResetNodeData", internal_reply.status, "");
}

void BrpcStorageService::ReadArchivedFile(google::protobuf::RpcController* cntl_base,
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

void BrpcStorageService::UpdateArchiveState(google::protobuf::RpcController* cntl_base,
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

void BrpcStorageService::UpdateFileArchiveState(google::protobuf::RpcController* cntl_base,
                                                const zb::rpc::UpdateFileArchiveStateRequest* request,
                                                zb::rpc::UpdateFileArchiveStateReply* response,
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

    const zb::msg::Status internal_status =
        service_->UpdateFileArchiveState(request->inode_id(),
                                         request->disk_id(),
                                         ToInternalFileArchiveState(request->archive_state()),
                                         request->version());
    FillStatus(internal_status, response->mutable_status());
}

void BrpcStorageService::GetDiskReport(google::protobuf::RpcController* cntl_base,
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
    LogFailure("GetDiskReport", internal_reply.status, "");
    for (const auto& report : internal_reply.reports) {
        zb::rpc::DiskReport* item = response->add_reports();
        item->set_id(report.id);
        item->set_mount_point(report.mount_point);
        item->set_capacity_bytes(report.capacity_bytes);
        item->set_free_bytes(report.free_bytes);
        item->set_is_healthy(report.is_healthy);
    }
}

void BrpcStorageService::DeleteFileMeta(google::protobuf::RpcController* cntl_base,
                                        const zb::rpc::DeleteFileMetaRequest* request,
                                        zb::rpc::DeleteFileMetaReply* response,
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

    zb::msg::DeleteFileMetaRequest internal_req;
    internal_req.inode_id = request->inode_id();
    internal_req.disk_id = request->disk_id();
    internal_req.purge_objects = request->purge_objects();
    const zb::msg::DeleteFileMetaReply internal_reply = service_->DeleteFileMeta(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
}

void BrpcStorageService::ResolveFileRead(google::protobuf::RpcController* cntl_base,
                                         const zb::rpc::ResolveFileReadRequest* request,
                                         zb::rpc::ResolveFileReadReply* response,
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

    zb::msg::ResolveFileReadRequest internal_req;
    internal_req.inode_id = request->inode_id();
    internal_req.offset = request->offset();
    internal_req.size = request->size();
    internal_req.disk_id = request->disk_id();
    internal_req.object_unit_size_hint = request->object_unit_size_hint();
    const zb::msg::ResolveFileReadReply internal_reply = service_->ResolveFileRead(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    if (internal_reply.status.ok()) {
        FillFileMeta(internal_reply.meta, response->mutable_meta());
        for (const auto& s : internal_reply.slices) {
            FillFileObjectSlice(s, response->add_slices());
        }
    }
}

void BrpcStorageService::AllocateFileWrite(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::AllocateFileWriteRequest* request,
                                           zb::rpc::AllocateFileWriteReply* response,
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

    zb::msg::AllocateFileWriteRequest internal_req;
    internal_req.inode_id = request->inode_id();
    internal_req.offset = request->offset();
    internal_req.size = request->size();
    internal_req.disk_id = request->disk_id();
    internal_req.object_unit_size_hint = request->object_unit_size_hint();
    const zb::msg::AllocateFileWriteReply internal_reply = service_->AllocateFileWrite(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    LogFailure("AllocateFileWrite",
               internal_reply.status,
               "inode_id=" + std::to_string(request->inode_id()) +
                   " disk_id=" + request->disk_id() +
                   " offset=" + std::to_string(request->offset()) +
                   " size=" + std::to_string(request->size()));
    if (internal_reply.status.ok()) {
        FillFileMeta(internal_reply.meta, response->mutable_meta());
        response->set_txid(internal_reply.txid);
        for (const auto& s : internal_reply.slices) {
            FillFileObjectSlice(s, response->add_slices());
        }
    }
}

void BrpcStorageService::CommitFileWrite(google::protobuf::RpcController* cntl_base,
                                         const zb::rpc::CommitFileWriteRequest* request,
                                         zb::rpc::CommitFileWriteReply* response,
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

    zb::msg::CommitFileWriteRequest internal_req;
    internal_req.inode_id = request->inode_id();
    internal_req.txid = request->txid();
    internal_req.file_size = request->file_size();
    internal_req.object_unit_size = request->object_unit_size();
    internal_req.expected_version = request->expected_version();
    internal_req.allow_create = request->allow_create();
    internal_req.mtime_sec = request->mtime_sec();
    const zb::msg::CommitFileWriteReply internal_reply = service_->CommitFileWrite(internal_req);
    FillStatus(internal_reply.status, response->mutable_status());
    LogFailure("CommitFileWrite",
               internal_reply.status,
               "inode_id=" + std::to_string(request->inode_id()) +
                   " txid=" + request->txid() +
                   " file_size=" + std::to_string(request->file_size()) +
                   " expected_version=" + std::to_string(request->expected_version()));
    if (internal_reply.status.ok()) {
        FillFileMeta(internal_reply.meta, response->mutable_meta());
    }
}

} // namespace zb::real_node
