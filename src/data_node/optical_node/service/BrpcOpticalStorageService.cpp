#include "BrpcOpticalStorageService.h"

#include <brpc/controller.h>

namespace zb::optical_node {

void BrpcOpticalStorageService::WriteObject(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::WriteObjectRequest* request,
                                            zb::rpc::WriteObjectReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
}

void BrpcOpticalStorageService::ReadObject(google::protobuf::RpcController* cntl_base,
                                           const zb::rpc::ReadObjectRequest* request,
                                           zb::rpc::ReadObjectReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
}

void BrpcOpticalStorageService::DeleteObject(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::DeleteObjectRequest* request,
                                             zb::rpc::DeleteObjectReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
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
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
}

void BrpcOpticalStorageService::UpdateArchiveState(google::protobuf::RpcController* cntl_base,
                                                   const zb::rpc::UpdateArchiveStateRequest* request,
                                                   zb::rpc::UpdateArchiveStateReply* response,
                                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
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
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node does not support UpdateFileArchiveState");
}

void BrpcOpticalStorageService::GetDiskReport(google::protobuf::RpcController* cntl_base,
                                              const google::protobuf::Empty* request,
                                              zb::rpc::DiskReportReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!response) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node storage not implemented");
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
    response->mutable_status()->set_code(zb::rpc::STATUS_NOT_FOUND);
    response->mutable_status()->set_message("optical node does not host file metadata");
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
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node does not support ResolveFileRead");
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
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node does not support AllocateFileWrite");
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
    response->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
    response->mutable_status()->set_message("optical node does not support CommitFileWrite");
}

} // namespace zb::optical_node
