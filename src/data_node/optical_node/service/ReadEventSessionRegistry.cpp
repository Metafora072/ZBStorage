#include "ReadEventSessionRegistry.h"

#include <utility>
#include <vector>

namespace zb::optical_node {

ReadEventSessionRegistry::~ReadEventSessionRegistry() {
    CloseAll();
}

uint64_t ReadEventSessionRegistry::RegisterSession(const std::string& client_id,
                                                   brpc::StreamId server_stream) {
    if (server_stream == brpc::INVALID_STREAM_ID) {
        return 0;
    }

    auto session = std::make_shared<Session>();
    session->client_id = client_id;
    session->stream_id = server_stream;
    const uint64_t session_id = next_session_id_.fetch_add(1);

    std::lock_guard<std::mutex> lock(mu_);
    sessions_[session_id] = std::move(session);
    stream_to_session_[server_stream] = session_id;
    return session_id;
}

bool ReadEventSessionRegistry::SendMessage(uint64_t session_id,
                                           const butil::IOBuf& message) {
    std::shared_ptr<Session> session;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = sessions_.find(session_id);
        if (it == sessions_.end()) {
            return false;
        }
        // 取 shared_ptr 副本：离开锁后即使对端并发关闭会话，对象仍存活。
        session = it->second;
    }
    // 同一会话的写入串行化（brpc::StreamWrite 非线程安全）。
    std::lock_guard<std::mutex> write_lock(session->write_mu);
    return brpc::StreamWrite(session->stream_id, message) == 0;
}

bool ReadEventSessionRegistry::HasSession(uint64_t session_id) {
    std::lock_guard<std::mutex> lock(mu_);
    return sessions_.find(session_id) != sessions_.end();
}

void ReadEventSessionRegistry::CloseAll() {
    std::vector<std::shared_ptr<Session>> sessions;
    {
        std::lock_guard<std::mutex> lock(mu_);
        sessions.reserve(sessions_.size());
        for (auto& entry : sessions_) {
            sessions.push_back(std::move(entry.second));
        }
        sessions_.clear();
        stream_to_session_.clear();
    }
    for (const auto& session : sessions) {
        brpc::StreamClose(session->stream_id);
    }
}

void ReadEventSessionRegistry::OnStreamClosed(brpc::StreamId stream_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = stream_to_session_.find(stream_id);
    if (it == stream_to_session_.end()) {
        return;
    }
    sessions_.erase(it->second);
    stream_to_session_.erase(it);
}

int ReadEventSessionRegistry::Handler::on_received_messages(brpc::StreamId id,
                                                            butil::IOBuf* const messages[],
                                                            size_t size) {
    // 通知通道是服务端 → 客户端的单向下行通道，客户端不应写入；忽略收到的数据。
    (void)id;
    (void)messages;
    (void)size;
    return 0;
}

void ReadEventSessionRegistry::Handler::on_idle_timeout(brpc::StreamId id) {
    (void)id;
}

void ReadEventSessionRegistry::Handler::on_closed(brpc::StreamId id) {
    if (owner_ != nullptr) {
        owner_->OnStreamClosed(id);
    }
}

}  // namespace zb::optical_node