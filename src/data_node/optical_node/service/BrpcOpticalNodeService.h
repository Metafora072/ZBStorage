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

    void RequestAsyncReadFile(google::protobuf::RpcController* cntl_base,
                              const zb::rpc::RequestAsyncReadFileRequest* request,
                              zb::rpc::RequestAsyncReadFileReply* response,
                              google::protobuf::Closure* done) override;

    void ReadObjectByTaskId(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::ReadObjectByTaskIdRequest* request,
                            zb::rpc::ReadObjectByTaskIdReply* response,
                            google::protobuf::Closure* done) override;

    void ReadObjectByInodeId(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::ReadObjectByInodeIdRequest* request,
                             zb::rpc::ReadObjectByInodeIdReply* response,
                             google::protobuf::Closure* done) override;

private:
    OpticalStorageServiceImpl* service_{nullptr};
};

} // namespace zb::optical_node
