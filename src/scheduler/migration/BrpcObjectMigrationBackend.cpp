#include "BrpcObjectMigrationBackend.h"

#include <algorithm>
#include <unordered_set>

#include <brpc/controller.h>

#include "mds.pb.h"
#include "real_node.pb.h"

namespace zb::scheduler {

BrpcObjectMigrationBackend::BrpcObjectMigrationBackend(std::string mds_address,
                                                       uint32_t timeout_ms,
                                                       uint64_t copy_chunk_bytes)
    : mds_address_(std::move(mds_address)),
      timeout_ms_(std::max<uint32_t>(100, timeout_ms)),
      copy_chunk_bytes_(std::max<uint64_t>(4096, copy_chunk_bytes)) {}

brpc::Channel* BrpcObjectMigrationBackend::ChannelFor(const std::string& address,
                                                      std::string* error) {
    if (address.empty()) {
        if (error) *error = "RPC address is empty";
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(channel_mu_);
    auto it = channels_.find(address);
    if (it != channels_.end()) return it->second.get();
    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.timeout_ms = static_cast<int>(timeout_ms_);
    options.max_retry = 1;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) *error = "failed to initialize RPC channel: " + address;
        return nullptr;
    }
    brpc::Channel* result = channel.get();
    channels_[address] = std::move(channel);
    return result;
}

bool BrpcObjectMigrationBackend::ListObjects(const std::string& source_address,
                                             std::vector<MigrationObject>* objects,
                                             std::string* error) {
    if (!objects) return false;
    objects->clear();
    brpc::Channel* channel = ChannelFor(source_address, error);
    if (!channel) return false;
    zb::rpc::RealNodeService_Stub stub(channel);
    std::string token;
    std::unordered_set<std::string> seen_tokens;
    do {
        zb::rpc::ListObjectsRequest request;
        request.set_page_token(token);
        request.set_max_objects(10000);
        zb::rpc::ListObjectsReply response;
        brpc::Controller controller;
        controller.set_timeout_ms(static_cast<int>(timeout_ms_));
        stub.ListObjects(&controller, &request, &response, nullptr);
        if (controller.Failed() || response.status().code() != zb::rpc::STATUS_OK) {
            if (error) *error = controller.Failed() ? controller.ErrorText() : response.status().message();
            return false;
        }
        for (const auto& object : response.objects()) {
            MigrationObject out;
            out.disk_id = object.disk_id();
            out.object_id = object.object_id();
            out.size_bytes = object.size_bytes();
            objects->push_back(std::move(out));
        }
        token = response.next_page_token();
        if (!token.empty() && !seen_tokens.insert(token).second) {
            if (error) *error = "data node returned a repeated pagination token";
            return false;
        }
    } while (!token.empty());
    return true;
}

bool BrpcObjectMigrationBackend::CopyAndVerify(const std::string& source_address,
                                               const MigrationObject& object,
                                               const MigrationTarget& target,
                                               std::string* error) {
    brpc::Channel* source_channel = ChannelFor(source_address, error);
    brpc::Channel* target_channel = ChannelFor(target.address, error);
    if (!source_channel || !target_channel) return false;
    zb::rpc::RealNodeService_Stub source(source_channel);
    zb::rpc::RealNodeService_Stub destination(target_channel);
    uint64_t offset = 0;
    bool first = true;
    while (first || offset < object.size_bytes) {
        first = false;
        const uint64_t length = std::min<uint64_t>(copy_chunk_bytes_, object.size_bytes - offset);
        zb::rpc::ReadObjectRequest read_request;
        read_request.set_disk_id(object.disk_id);
        read_request.set_object_id(object.object_id);
        read_request.set_offset(offset);
        read_request.set_size(length);
        zb::rpc::ReadObjectReply read_response;
        brpc::Controller read_controller;
        read_controller.set_timeout_ms(static_cast<int>(timeout_ms_));
        source.ReadObject(&read_controller, &read_request, &read_response, nullptr);
        if (read_controller.Failed() || read_response.status().code() != zb::rpc::STATUS_OK ||
            read_response.data().size() != length) {
            if (error) *error = read_controller.Failed() ? read_controller.ErrorText() :
                (read_response.status().code() != zb::rpc::STATUS_OK ? read_response.status().message() :
                 "source object size changed during drain");
            return false;
        }
        zb::rpc::WriteObjectRequest write_request;
        write_request.set_disk_id(target.disk_id);
        write_request.set_object_id(object.object_id);
        write_request.set_offset(offset);
        write_request.set_data(read_response.data());
        write_request.set_is_replication(true);
        zb::rpc::WriteObjectReply write_response;
        brpc::Controller write_controller;
        write_controller.set_timeout_ms(static_cast<int>(timeout_ms_));
        destination.WriteObject(&write_controller, &write_request, &write_response, nullptr);
        if (write_controller.Failed() || write_response.status().code() != zb::rpc::STATUS_OK) {
            if (error) *error = write_controller.Failed() ? write_controller.ErrorText() : write_response.status().message();
            return false;
        }
        zb::rpc::ReadObjectRequest verify_request;
        verify_request.set_disk_id(target.disk_id);
        verify_request.set_object_id(object.object_id);
        verify_request.set_offset(offset);
        verify_request.set_size(length);
        zb::rpc::ReadObjectReply verify_response;
        brpc::Controller verify_controller;
        verify_controller.set_timeout_ms(static_cast<int>(timeout_ms_));
        destination.ReadObject(&verify_controller, &verify_request, &verify_response, nullptr);
        if (verify_controller.Failed() || verify_response.status().code() != zb::rpc::STATUS_OK ||
            verify_response.data() != read_response.data()) {
            if (error) *error = verify_controller.Failed() ? verify_controller.ErrorText() : "target byte verification failed";
            return false;
        }
        if (object.size_bytes == 0) break;
        offset += length;
    }
    return true;
}

bool BrpcObjectMigrationBackend::CommitFileLocation(const std::string& migration_id,
                                                    uint64_t inode_id,
                                                    const std::string& source_node_id,
                                                    const std::string& source_address,
                                                    const std::string& source_disk_id,
                                                    const MigrationTarget& target,
                                                    std::string* error) {
    brpc::Channel* channel = ChannelFor(mds_address_, error);
    if (!channel) return false;
    zb::rpc::MdsService_Stub stub(channel);
    zb::rpc::CommitFileMigrationRequest request;
    request.set_migration_id(migration_id + "-inode-" + std::to_string(inode_id));
    request.set_inode_id(inode_id);
    request.mutable_expected_source()->set_node_id(source_node_id);
    request.mutable_expected_source()->set_node_address(source_address);
    request.mutable_expected_source()->set_disk_id(source_disk_id);
    request.mutable_target()->set_node_id(target.node_id);
    request.mutable_target()->set_node_address(target.address);
    request.mutable_target()->set_disk_id(target.disk_id);
    zb::rpc::CommitFileMigrationReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(static_cast<int>(timeout_ms_));
    stub.CommitFileMigration(&controller, &request, &response, nullptr);
    if (controller.Failed() || response.status().code() != zb::rpc::MDS_OK) {
        if (error) *error = controller.Failed() ? controller.ErrorText() : response.status().message();
        return false;
    }
    return true;
}

bool BrpcObjectMigrationBackend::VerifyCmsNodeReferencesZero(
    const std::string& node_id,
    uint64_t* disk_references,
    uint64_t* optical_references,
    std::string* error) {
    if (mds_address_.empty() || node_id.empty()) {
        if (error) *error = "MDS address and node id are required for reference verification";
        return false;
    }
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = static_cast<int>(timeout_ms_);
    options.max_retry = 0;
    if (channel.Init(mds_address_.c_str(), &options) != 0) {
        if (error) *error = "failed to initialize MDS channel";
        return false;
    }
    zb::rpc::VerifyNodeReferencesRequest request;
    request.set_node_id(node_id);
    zb::rpc::VerifyNodeReferencesReply response;
    brpc::Controller controller;
    zb::rpc::MdsService_Stub stub(&channel);
    stub.VerifyNodeReferences(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        if (error) *error = controller.ErrorText();
        return false;
    }
    if (response.status().code() != zb::rpc::MDS_OK) {
        if (error) *error = response.status().message();
        return false;
    }
    if (disk_references) *disk_references = response.disk_inode_references();
    if (optical_references) *optical_references = response.optical_inode_references();
    if (!response.zero_references()) {
        if (error) {
            *error = "CMS still references node (disk=" +
                     std::to_string(response.disk_inode_references()) +
                     ", optical=" + std::to_string(response.optical_inode_references()) + ")";
        }
        return false;
    }
    return true;
}

bool BrpcObjectMigrationBackend::DeleteSourceObject(const std::string& source_address,
                                                    const MigrationObject& object,
                                                    std::string* error) {
    brpc::Channel* channel = ChannelFor(source_address, error);
    if (!channel) return false;
    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::DeleteObjectRequest request;
    request.set_disk_id(object.disk_id);
    request.set_object_id(object.object_id);
    zb::rpc::DeleteObjectReply response;
    brpc::Controller controller;
    controller.set_timeout_ms(static_cast<int>(timeout_ms_));
    stub.DeleteObject(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        if (error) *error = controller.ErrorText();
        return false;
    }
    if (response.status().code() != zb::rpc::STATUS_OK &&
        response.status().code() != zb::rpc::STATUS_NOT_FOUND) {
        if (error) *error = response.status().message();
        return false;
    }
    return true;
}

} // namespace zb::scheduler
