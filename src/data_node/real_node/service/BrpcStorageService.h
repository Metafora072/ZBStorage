#pragma once

#include "StorageServiceImpl.h"
#include "real_node.pb.h"

namespace zb::real_node {

class BrpcStorageService : public zb::rpc::RealNodeService {
public:
    explicit BrpcStorageService(StorageServiceImpl* service);

    void WriteObject(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::WriteObjectRequest* request,
                     zb::rpc::WriteObjectReply* response,
                     google::protobuf::Closure* done) override;

    void ReadObject(google::protobuf::RpcController* cntl_base,
                    const zb::rpc::ReadObjectRequest* request,
                    zb::rpc::ReadObjectReply* response,
                    google::protobuf::Closure* done) override;

    void DeleteObject(google::protobuf::RpcController* cntl_base,
                      const zb::rpc::DeleteObjectRequest* request,
                      zb::rpc::DeleteObjectReply* response,
                      google::protobuf::Closure* done) override;

    void ResetNodeData(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::ResetNodeDataRequest* request,
                       zb::rpc::ResetNodeDataReply* response,
                       google::protobuf::Closure* done) override;

    void ReadArchivedFile(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReadArchivedFileRequest* request,
                          zb::rpc::ReadArchivedFileReply* response,
                          google::protobuf::Closure* done) override;

    void UpdateArchiveState(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::UpdateArchiveStateRequest* request,
                            zb::rpc::UpdateArchiveStateReply* response,
                            google::protobuf::Closure* done) override;

    void UpdateFileArchiveState(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::UpdateFileArchiveStateRequest* request,
                                zb::rpc::UpdateFileArchiveStateReply* response,
                                google::protobuf::Closure* done) override;

    void GetDiskReport(google::protobuf::RpcController* cntl_base,
                       const google::protobuf::Empty* request,
                       zb::rpc::DiskReportReply* response,
                       google::protobuf::Closure* done) override;

    void DeleteFileMeta(google::protobuf::RpcController* cntl_base,
                        const zb::rpc::DeleteFileMetaRequest* request,
                        zb::rpc::DeleteFileMetaReply* response,
                        google::protobuf::Closure* done) override;

    void ResolveFileRead(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::ResolveFileReadRequest* request,
                         zb::rpc::ResolveFileReadReply* response,
                         google::protobuf::Closure* done) override;

    void AllocateFileWrite(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::AllocateFileWriteRequest* request,
                           zb::rpc::AllocateFileWriteReply* response,
                           google::protobuf::Closure* done) override;

    void CommitFileWrite(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::CommitFileWriteRequest* request,
                         zb::rpc::CommitFileWriteReply* response,
                         google::protobuf::Closure* done) override;

private:
    StorageServiceImpl* service_{};
};

} // namespace zb::real_node
