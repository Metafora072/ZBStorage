#include "fake_services.h"

#include "optical_node_test_support.h"

#include <brpc/controller.h>
#include <butil/endpoint.h>

#include <utility>

namespace zb::optical_node::test {

void FakeRealNodeService::ResolveFileRead(google::protobuf::RpcController* controller,
                                          const zb::rpc::ResolveFileReadRequest* request,
                                          zb::rpc::ResolveFileReadReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)controller;
    ++resolve_calls;
    if (response == nullptr) {
        return;
    }
    const CorpusFile* file =
        (corpus_ != nullptr && request != nullptr) ? corpus_->Find(request->inode_id()) : nullptr;
    if (file == nullptr) {
        response->mutable_status()->set_code(zb::rpc::STATUS_NOT_FOUND);
        response->mutable_status()->set_message("unknown inode");
        return;
    }

    // 一次性返回整份文件的分片布局：被测节点按 object_index*object_unit_size+object_offset
    // 校验连续性，并要求各分片 length 之和等于文件大小。
    response->mutable_status()->set_code(zb::rpc::STATUS_OK);
    zb::rpc::FileMeta* meta = response->mutable_meta();
    meta->set_inode_id(file->inode_id);
    meta->set_file_size(file->file_size);
    meta->set_object_unit_size(file->object_unit_size);
    meta->set_version(1);
    for (const ObjectInfo& object : file->objects) {
        zb::rpc::FileObjectSlice* slice = response->add_slices();
        slice->set_object_index(static_cast<uint32_t>(object.object_index));
        slice->set_object_id(object.object_id);
        slice->set_disk_id(file->disk_id);
        slice->set_object_offset(object.object_offset);
        slice->set_length(object.length);
    }
}

void FakeRealNodeService::ReadObject(google::protobuf::RpcController* controller,
                                     const zb::rpc::ReadObjectRequest* request,
                                     zb::rpc::ReadObjectReply* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)controller;
    ++read_calls;
    if (response == nullptr) {
        return;
    }
    const ObjectInfo* object =
        (corpus_ != nullptr && request != nullptr) ? corpus_->FindObject(request->object_id()) : nullptr;
    if (object == nullptr) {
        response->mutable_status()->set_code(zb::rpc::STATUS_NOT_FOUND);
        response->mutable_status()->set_message("unknown object");
        return;
    }

    // 被测节点强校验返回长度 == 分片 length，因此这里必须精确返回请求的字节数。
    std::string data;
    if (!ReadFileRange(object->path, request->offset(), request->size(), &data)) {
        response->mutable_status()->set_code(zb::rpc::STATUS_IO_ERROR);
        response->mutable_status()->set_message("object range read failed");
        return;
    }
    response->mutable_status()->set_code(zb::rpc::STATUS_OK);
    response->set_bytes(static_cast<uint64_t>(data.size()));
    response->set_data(std::move(data));
}

void FakeSchedulerService::GetClusterView(google::protobuf::RpcController* controller,
                                          const zb::rpc::GetClusterViewRequest* request,
                                          zb::rpc::GetClusterViewReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)controller;
    (void)request;
    ++get_cluster_view_calls;
    if (response == nullptr) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::SCHED_OK);
    response->set_generation(1);
    zb::rpc::NodeView* node = response->add_nodes();
    node->set_node_id(node_id);
    node->set_node_type(zb::rpc::NODE_REAL);
    node->set_address(node_address);
    node->set_health_state(zb::rpc::NODE_HEALTH_HEALTHY);
    node->set_admin_state(zb::rpc::NODE_ADMIN_ENABLED);
    node->set_weight(1);
    node->set_virtual_node_count(1);
}

void FakeSchedulerService::ReportHeartbeat(google::protobuf::RpcController* controller,
                                           const zb::rpc::HeartbeatRequest* request,
                                           zb::rpc::HeartbeatReply* response,
                                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)controller;
    (void)request;
    if (response == nullptr) {
        return;
    }
    response->mutable_status()->set_code(zb::rpc::SCHED_OK);
    response->set_generation(1);
}

LocalServer::LocalServer(google::protobuf::Service* service) {
    Require(server_.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0,
            "AddService 失败");
    Require(server_.Start("127.0.0.1:0", nullptr) == 0, "Server.Start 失败");
    address_ = butil::endpoint2str(server_.listen_address()).c_str();
    Require(!address_.empty(), "取回监听地址失败");
}

LocalServer::~LocalServer() {
    server_.Stop(0);
    server_.Join();
}

}  // namespace zb::optical_node::test