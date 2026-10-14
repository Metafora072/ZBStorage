#pragma once

#include "OpticalStorageServiceImpl.h"
#include "optical_node.pb.h"

namespace zb::optical_node {

// OpticalNodeService 的 brpc 协议适配层：把 protobuf 请求转换为领域对象/参数，
// 再交给 OpticalStorageServiceImpl 处理。
class BrpcOpticalNodeService : public zb::rpc::OpticalNodeService {
public:
    explicit BrpcOpticalNodeService(OpticalStorageServiceImpl* service);

    void SendArchiveMetadata(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::SendArchiveMetadataRequest* request,
                             zb::rpc::ArchiveReply* response,
                             google::protobuf::Closure* done) override;

    void SubscribeReadEvents(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::SubscribeReadEventsRequest* request,
                             zb::rpc::SubscribeReadEventsReply* response,
                             google::protobuf::Closure* done) override;

    void PrepareRead(google::protobuf::RpcController* cntl_base,
                     const zb::rpc::PrepareReadRequest* request,
                     zb::rpc::PrepareReadReply* response,
                     google::protobuf::Closure* done) override;

    void AcquireReadSession(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::AcquireReadSessionRequest* request,
                            zb::rpc::AcquireReadSessionReply* response,
                            google::protobuf::Closure* done) override;

    void ReadPreparedFile(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ReadPreparedFileRequest* request,
                          zb::rpc::ReadPreparedFileReply* response,
                          google::protobuf::Closure* done) override;

    void ReleaseReadSession(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::ReleaseReadSessionRequest* request,
                            zb::rpc::ReleaseReadSessionReply* response,
                            google::protobuf::Closure* done) override;

    void ReleasePreparedRead(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::ReleasePreparedReadRequest* request,
                             zb::rpc::ReleasePreparedReadReply* response,
                             google::protobuf::Closure* done) override;

private:
    OpticalStorageServiceImpl* service_{nullptr};
};

} // namespace zb::optical_node
