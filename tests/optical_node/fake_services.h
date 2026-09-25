#pragma once

// 仿真上层所需的假服务：real_node（提供归档文件分片数据）与 scheduler（返回节点视图）。
// 两者都是真 brpc server，监听 127.0.0.1 的临时端口。

#include "fake_real_node_data.h"

#include <brpc/server.h>
#include <real_node.pb.h>
#include <scheduler.pb.h>

#include <atomic>
#include <string>

namespace zb::optical_node::test {

// 假 real_node：按语料 manifest 的分片布局答复 ResolveFileRead / ReadObject。
// 数据从磁盘对象文件按偏移读取，不在内存驻留整份语料。
class FakeRealNodeService : public zb::rpc::RealNodeService {
public:
    explicit FakeRealNodeService(const Corpus* corpus) : corpus_(corpus) {}

    void ResolveFileRead(google::protobuf::RpcController* controller,
                         const zb::rpc::ResolveFileReadRequest* request,
                         zb::rpc::ResolveFileReadReply* response,
                         google::protobuf::Closure* done) override;

    void ReadObject(google::protobuf::RpcController* controller,
                    const zb::rpc::ReadObjectRequest* request,
                    zb::rpc::ReadObjectReply* response,
                    google::protobuf::Closure* done) override;

    // 调用计数：用于断言下载链路确实按分片走完全程（非竞态可观测量）。
    std::atomic<int> resolve_calls{0};
    std::atomic<int> read_calls{0};

private:
    const Corpus* corpus_{nullptr};
};

// 假 scheduler：GetClusterView 返回单个 NODE_REAL 节点，地址指向假 real_node。
class FakeSchedulerService : public zb::rpc::SchedulerService {
public:
    std::string node_id{"real-1"};
    std::string node_address;

    std::atomic<int> get_cluster_view_calls{0};

    void GetClusterView(google::protobuf::RpcController* controller,
                        const zb::rpc::GetClusterViewRequest* request,
                        zb::rpc::GetClusterViewReply* response,
                        google::protobuf::Closure* done) override;

    void ReportHeartbeat(google::protobuf::RpcController* controller,
                         const zb::rpc::HeartbeatRequest* request,
                         zb::rpc::HeartbeatReply* response,
                         google::protobuf::Closure* done) override;
};

// loopback 临时端口上的 brpc server；端口取 0 由内核分配，避免端口冲突。
class LocalServer {
public:
    explicit LocalServer(google::protobuf::Service* service);
    ~LocalServer();

    LocalServer(const LocalServer&) = delete;
    LocalServer& operator=(const LocalServer&) = delete;

    const std::string& Address() const { return address_; }

private:
    brpc::Server server_;
    std::string address_;
};

}  // namespace zb::optical_node::test