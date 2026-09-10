#pragma once

#include "OpticalStorageServiceImpl.h"
#include "optical_node.pb.h"

namespace zb::optical_node {

// OpticalNodeService 的 brpc 协议适配层：把 MDS 下发的 protobuf 请求转换为领域对象，
// 再交给 OpticalStorageServiceImpl 处理。
// 当前 SendArchiveMetadata 业务逻辑尚未落地，处理函数仅返回未实现错误。
class BrpcOpticalNodeService : public zb::rpc::OpticalNodeService {
public:
    explicit BrpcOpticalNodeService(OpticalStorageServiceImpl* service);

    void SendArchiveMetadata(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::SendArchiveMetadataRequest* request,
                             zb::rpc::ArchiveReply* response,
                             google::protobuf::Closure* done) override;

private:
    OpticalStorageServiceImpl* service_{nullptr};
};

} // namespace zb::optical_node
