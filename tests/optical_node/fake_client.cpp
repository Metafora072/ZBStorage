#include "fake_client.h"

#include <brpc/controller.h>

#include <utility>

namespace zb::optical_node::test {

namespace {

// 传输层调用失败的统一处理：状态置 MDS_INTERNAL_ERROR 并回填错误文本。
void FillTransportFailure(brpc::Controller* controller, zb::rpc::MdsStatusCode* status,
                          std::string* error) {
    if (status != nullptr) {
        *status = zb::rpc::MDS_INTERNAL_ERROR;
    }
    if (error != nullptr) {
        *error = controller->ErrorText();
    }
}

}  // namespace

FakeMdsDriver::FakeMdsDriver(const std::string& node_address) : stub_(&channel_) {
    init_ok_ = (channel_.Init(node_address.c_str(), nullptr) == 0);
}

bool FakeMdsDriver::SendBatch(uint64_t batch_id,
                              const std::vector<zb::rpc::ArchiveFile>& files,
                              std::string* error) {
    if (!init_ok_) {
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::SendArchiveMetadataRequest request;
    request.set_batch_id(batch_id);
    for (const zb::rpc::ArchiveFile& file : files) {
        *request.add_files() = file;
    }
    zb::rpc::ArchiveReply response;
    brpc::Controller controller;
    stub_.SendArchiveMetadata(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, nullptr, error);
        return false;
    }
    if (response.status().code() != zb::rpc::MDS_OK) {
        if (error != nullptr) {
            *error = "status=" + std::to_string(static_cast<int>(response.status().code())) +
                     " message=" + response.status().message();
        }
        return false;
    }
    return true;
}

FakeReadClient::FakeReadClient(const std::string& node_address) : stub_(&channel_) {
    init_ok_ = (channel_.Init(node_address.c_str(), nullptr) == 0);
}

bool FakeReadClient::RequestRead(const std::string& disk_id,
                                 const std::string& image_id,
                                 uint64_t inode_id,
                                 uint64_t* task_id,
                                 zb::rpc::MdsStatusCode* status,
                                 std::string* error) {
    if (!init_ok_) {
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::RequestAsyncReadFileRequest request;
    request.set_disk_id(disk_id);
    request.set_image_id(image_id);
    request.set_inode_id(inode_id);
    zb::rpc::RequestAsyncReadFileReply response;
    brpc::Controller controller;
    stub_.RequestAsyncReadFile(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (task_id != nullptr) {
        *task_id = response.task_id();
    }
    if (response.status().code() != zb::rpc::MDS_OK && error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::ReadByTask(uint64_t task_id,
                                uint64_t offset,
                                uint64_t read_size,
                                std::string* data,
                                zb::rpc::MdsStatusCode* status,
                                std::string* error) {
    if (!init_ok_) {
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::ReadObjectByTaskIdRequest request;
    request.set_task_id(task_id);
    request.set_offset(offset);
    request.set_read_size(read_size);
    zb::rpc::ReadObjectByTaskIdReply response;
    brpc::Controller controller;
    stub_.ReadObjectByTaskId(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() == zb::rpc::MDS_OK && data != nullptr) {
        data->append(response.data());
    } else if (error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::ReadByInode(uint64_t inode_id,
                                 uint64_t offset,
                                 uint64_t read_size,
                                 std::string* data,
                                 zb::rpc::MdsStatusCode* status,
                                 std::string* error) {
    if (!init_ok_) {
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::ReadObjectByInodeIdRequest request;
    request.set_inode_id(inode_id);
    request.set_offset(offset);
    request.set_read_size(read_size);
    zb::rpc::ReadObjectByInodeIdReply response;
    brpc::Controller controller;
    stub_.ReadObjectByInodeId(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() == zb::rpc::MDS_OK && data != nullptr) {
        data->append(response.data());
    } else if (error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

}  // namespace zb::optical_node::test