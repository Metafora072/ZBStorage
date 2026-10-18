#pragma once

#include "StorageServiceImpl.h"
#include "FileWriteLocks.h"
#include "common/concurrency/BoundedExecutor.h"
#include "common/metrics/NodeMetricsCollector.h"
#include "real_node.pb.h"

namespace zb::real_node {

class BrpcStorageService : public zb::rpc::RealNodeService {
public:
    void InstallMigratedFile(google::protobuf::RpcController*,
                              const zb::rpc::InstallMigratedFileRequest*,
                              zb::rpc::InstallMigratedFileReply*, google::protobuf::Closure*) override;
    void BeginNodeDrain(google::protobuf::RpcController*,
                        const zb::rpc::BeginNodeDrainRequest*,
                        zb::rpc::BeginNodeDrainReply*, google::protobuf::Closure*) override;
    explicit BrpcStorageService(StorageServiceImpl* service,
                                zb::metrics::NodeMetricsCollector* metrics = nullptr,
                                zb::concurrency::ExecutorOptions options = {},
                                uint64_t max_request_bytes = 16ULL * 1024 * 1024);
    ~BrpcStorageService() override;
    void Drain() { io_.Stop(); }

    void AcquireFileWriteLock(google::protobuf::RpcController*,
        const zb::rpc::AcquireFileWriteLockRequest*, zb::rpc::AcquireFileWriteLockReply*,
        google::protobuf::Closure*) override;
    void ReleaseFileWriteLock(google::protobuf::RpcController*,
        const zb::rpc::ReleaseFileWriteLockRequest*, zb::rpc::ReleaseFileWriteLockReply*,
        google::protobuf::Closure*) override;

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

    void ListObjects(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::ListObjectsRequest* request,
                     zb::rpc::ListObjectsReply* response,
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
    FileWriteLocks file_write_locks_;
    zb::concurrency::BoundedExecutor io_;
    uint64_t max_request_bytes_;
    void ReadObjectNow(google::protobuf::RpcController*, const zb::rpc::ReadObjectRequest*,
                     zb::rpc::ReadObjectReply*, google::protobuf::Closure*);
    void WriteObjectNow(google::protobuf::RpcController*, const zb::rpc::WriteObjectRequest*,
                     zb::rpc::WriteObjectReply*, google::protobuf::Closure*);
    void DeleteObjectNow(google::protobuf::RpcController*, const zb::rpc::DeleteObjectRequest*,
                     zb::rpc::DeleteObjectReply*, google::protobuf::Closure*);
    StorageServiceImpl* service_{};
    zb::metrics::NodeMetricsCollector* metrics_{};
};

} // namespace zb::real_node
