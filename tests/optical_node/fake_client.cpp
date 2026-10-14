#include "fake_client.h"

#include <brpc/controller.h>

#include <chrono>
#include <utility>

namespace zb::optical_node::test {

namespace {

// 传输层调用失败的统一处理：状态置内部错误并回填错误文本。
void FillTransportFailure(brpc::Controller* controller,
                          zb::rpc::OpticalStatusCode* status,
                          std::string* error) {
    if (status != nullptr) {
        *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
    }
    if (error != nullptr) {
        *error = controller->ErrorText();
    }
}

void FillMdsTransportFailure(brpc::Controller* controller,
                             zb::rpc::MdsStatusCode* status,
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
        FillMdsTransportFailure(&controller, nullptr, error);
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

// ---------------------------------------------------------------------------
// ReadReady 通知接收器
// ---------------------------------------------------------------------------

int FakeReadClient::Receiver::on_received_messages(brpc::StreamId id,
                                                   butil::IOBuf* const messages[],
                                                   size_t size) {
    (void)id;
    for (size_t i = 0; i < size; ++i) {
        std::string bytes;
        messages[i]->copy_to(&bytes);
        zb::rpc::ReadReadyEvent event;
        if (!event.ParseFromArray(bytes.data(), static_cast<int>(bytes.size()))) {
            continue;
        }
        ReadReadyNotice notice;
        notice.request_id = event.request_id();
        notice.task_id = event.task_id();
        notice.inode_id = event.inode_id();
        notice.file_size = event.file_size();
        notice.has_file_version = event.has_file_version();
        notice.file_version = event.file_version();
        Push(notice);
    }
    return 0;
}

void FakeReadClient::Receiver::on_idle_timeout(brpc::StreamId id) {
    (void)id;
}

void FakeReadClient::Receiver::on_closed(brpc::StreamId id) {
    (void)id;
}

void FakeReadClient::Receiver::Push(const ReadReadyNotice& notice) {
    {
        std::lock_guard<std::mutex> lock(mu_);
        pending_.push_back(notice);
    }
    cv_.notify_all();
}

bool FakeReadClient::Receiver::Wait(const std::string& request_id,
                                    ReadReadyNotice* out,
                                    int timeout_ms) {
    std::unique_lock<std::mutex> lock(mu_);
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_ms);
    for (;;) {
        for (auto it = pending_.begin(); it != pending_.end(); ++it) {
            if (it->request_id == request_id) {
                if (out != nullptr) {
                    *out = *it;
                }
                pending_.erase(it);
                return true;
            }
        }
        if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            return false;
        }
    }
}

std::vector<ReadReadyNotice> FakeReadClient::Receiver::Drain() {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<ReadReadyNotice> out;
    out.swap(pending_);
    return out;
}

// ---------------------------------------------------------------------------
// FakeReadClient
// ---------------------------------------------------------------------------

FakeReadClient::FakeReadClient(const std::string& node_address) : stub_(&channel_) {
    init_ok_ = (channel_.Init(node_address.c_str(), nullptr) == 0);
    auto receiver = std::make_unique<Receiver>();
    receiver_ = receiver.get();
    RetainedReceivers().push_back(std::move(receiver));
}

std::vector<std::unique_ptr<FakeReadClient::Receiver>>& FakeReadClient::RetainedReceivers() {
    static std::vector<std::unique_ptr<Receiver>> receivers;
    return receivers;
}

FakeReadClient::~FakeReadClient() {
    // 只关闭 Stream；接收器由 RetainedReceivers() 持有，避免对端随后拆除连接时
    // 回调到已析构的 handler。
    if (stream_created_) {
        brpc::StreamClose(stream_id_);
        stream_created_ = false;
    }
}

bool FakeReadClient::SubscribeReadEvents(const std::string& client_id,
                                         uint64_t* session_id,
                                         zb::rpc::OpticalStatusCode* status,
                                         std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }

    subscribe_controller_.Reset();
    subscribe_controller_.set_timeout_ms(10000);
    brpc::StreamOptions options;
    options.handler = receiver_;
    // 每条 ReadReady 单独回调，便于逐条解析。
    options.messages_in_batch = 1;
    brpc::StreamId stream = brpc::INVALID_STREAM_ID;
    if (brpc::StreamCreate(&stream, subscribe_controller_, &options) != 0) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "StreamCreate failed";
        }
        return false;
    }

    zb::rpc::SubscribeReadEventsRequest request;
    request.set_client_id(client_id);
    zb::rpc::SubscribeReadEventsReply response;
    stub_.SubscribeReadEvents(&subscribe_controller_, &request, &response, nullptr);
    if (subscribe_controller_.Failed()) {
        brpc::StreamClose(stream);
        FillTransportFailure(&subscribe_controller_, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() != zb::rpc::OPTICAL_OK) {
        brpc::StreamClose(stream);
        if (error != nullptr) {
            *error = response.status().message();
        }
        return false;
    }

    stream_id_ = stream;
    stream_created_ = true;
    session_id_ = response.session_id();
    client_id_ = client_id;
    if (session_id != nullptr) {
        *session_id = response.session_id();
    }
    return true;
}

bool FakeReadClient::PrepareRead(uint64_t session_id,
                                 const std::string& request_id,
                                 uint64_t inode_id,
                                 const std::string& disk_id,
                                 const std::string& image_id,
                                 uint64_t file_size,
                                 uint64_t* task_id,
                                 zb::rpc::OpticalStatusCode* status,
                                 std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::PrepareReadRequest request;
    request.set_session_id(session_id);
    request.set_request_id(request_id);
    request.set_inode_id(inode_id);
    request.set_disk_id(disk_id);
    request.set_image_id(image_id);
    request.set_file_size(file_size);
    zb::rpc::PrepareReadReply response;
    brpc::Controller controller;
    stub_.PrepareRead(&controller, &request, &response, nullptr);
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
    if (response.status().code() != zb::rpc::OPTICAL_OK && error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::AcquireReadSession(uint64_t task_id,
                                        uint64_t* read_handle,
                                        uint64_t* file_size,
                                        uint32_t* max_read_bytes,
                                        zb::rpc::OpticalStatusCode* status,
                                        std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::AcquireReadSessionRequest request;
    request.set_task_id(task_id);
    zb::rpc::AcquireReadSessionReply response;
    brpc::Controller controller;
    stub_.AcquireReadSession(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() == zb::rpc::OPTICAL_OK) {
        if (read_handle != nullptr) {
            *read_handle = response.read_handle();
        }
        if (file_size != nullptr) {
            *file_size = response.file_size();
        }
        if (max_read_bytes != nullptr) {
            *max_read_bytes = response.max_read_bytes();
        }
    } else if (error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::ReadPreparedFile(uint64_t read_handle,
                                      uint64_t offset,
                                      uint64_t size,
                                      std::string* out,
                                      uint64_t* bytes_read,
                                      bool* eof,
                                      zb::rpc::OpticalStatusCode* status,
                                      std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::ReadPreparedFileRequest request;
    request.set_read_handle(read_handle);
    request.set_offset(offset);
    request.set_size(size);
    zb::rpc::ReadPreparedFileReply response;
    brpc::Controller controller;
    stub_.ReadPreparedFile(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() == zb::rpc::OPTICAL_OK) {
        if (out != nullptr) {
            out->append(response.data());
        }
        if (bytes_read != nullptr) {
            *bytes_read = response.bytes_read();
        }
        if (eof != nullptr) {
            *eof = response.eof();
        }
    } else if (error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::ReleaseReadSession(uint64_t read_handle,
                                        zb::rpc::OpticalStatusCode* status,
                                        std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::ReleaseReadSessionRequest request;
    request.set_read_handle(read_handle);
    zb::rpc::ReleaseReadSessionReply response;
    brpc::Controller controller;
    stub_.ReleaseReadSession(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() != zb::rpc::OPTICAL_OK && error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::ReleasePreparedRead(uint64_t task_id,
                                         zb::rpc::OpticalStatusCode* status,
                                         std::string* error) {
    if (!init_ok_) {
        if (status != nullptr) {
            *status = zb::rpc::OPTICAL_INTERNAL_ERROR;
        }
        if (error != nullptr) {
            *error = "channel init failed";
        }
        return false;
    }
    zb::rpc::ReleasePreparedReadRequest request;
    request.set_task_id(task_id);
    zb::rpc::ReleasePreparedReadReply response;
    brpc::Controller controller;
    stub_.ReleasePreparedRead(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        FillTransportFailure(&controller, status, error);
        return false;
    }
    if (status != nullptr) {
        *status = response.status().code();
    }
    if (response.status().code() != zb::rpc::OPTICAL_OK && error != nullptr) {
        *error = response.status().message();
    }
    return true;
}

bool FakeReadClient::WaitReadReady(const std::string& request_id,
                                   ReadReadyNotice* out,
                                   int timeout_ms) {
    return receiver_->Wait(request_id, out, timeout_ms);
}

std::vector<ReadReadyNotice> FakeReadClient::DrainReadReady() {
    return receiver_->Drain();
}

}  // namespace zb::optical_node::test
