#pragma once
#include "optical_node.pb.h"
#include <brpc/closure_guard.h>
#include <brpc/stream.h>
#include <bthread/bthread.h>
#include <algorithm>
#include <atomic>
#include <map>
#include <mutex>
#include <stdexcept>

// Protocol fixture only: this is not an implementation of the physical node.
class OpticalReadTestService : public zb::rpc::OpticalNodeService {
public:
    struct Task {
        zb::rpc::PrepareReadRequest request;
        std::string data;
        bool ready{false}, pinned{false};
        size_t readers{0};
    };
    std::atomic<int> subscriptions{0}, preparations{0}, reads{0}, releases{0};
    std::atomic<bool> auto_ready{true}, corrupt_read{false};
    std::mutex mu;
    std::map<uint64_t, brpc::StreamId> streams;
    std::map<uint64_t, Task> tasks;
    std::map<uint64_t, uint64_t> sessions;
    uint64_t next_task{1}, next_read{1}, next_session{1};
    static std::string Content(uint64_t inode) {
        return inode == 2 ? std::string() : std::string("ab\0cdefghi", 10);
    }
    uint64_t TaskFor(uint64_t inode) {
        std::lock_guard<std::mutex> lock(mu);
        for (auto it = tasks.rbegin(); it != tasks.rend(); ++it)
            if (it->second.request.inode_id() == inode) return it->first;
        throw std::runtime_error("task missing");
    }
    size_t LiveArtifacts() {
        std::lock_guard<std::mutex> lock(mu);
        size_t result = 0;
        for (const auto& entry : tasks) result += entry.second.pinned || entry.second.readers;
        return result;
    }
    bool Expire(uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mu);
        auto& task = tasks.at(task_id);
        if (task.readers) return false;
        task.pinned = false;
        return true;
    }
    void Complete(uint64_t task_id) {
        zb::rpc::ReadReadyEvent event;
        brpc::StreamId stream;
        {
            std::lock_guard<std::mutex> lock(mu);
            auto& task = tasks.at(task_id);
            task.ready = task.pinned = true;
            event.set_task_id(task_id);
            event.set_request_id(task.request.request_id());
            event.set_inode_id(task.request.inode_id());
            event.set_file_size(task.data.size());
            event.set_has_file_version(task.request.has_file_version());
            if (task.request.has_file_version()) event.set_file_version(task.request.file_version());
            stream = streams.at(task.request.session_id());
        }
        butil::IOBuf data;
        data.append(event.SerializeAsString());
        if (brpc::StreamWrite(stream, data)) throw std::runtime_error("fixture StreamWrite failed");
    }
    void SubscribeReadEvents(google::protobuf::RpcController* base,
        const zb::rpc::SubscribeReadEventsRequest*, zb::rpc::SubscribeReadEventsReply* reply,
        google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(base);
        brpc::StreamId stream;
        if (brpc::StreamAccept(&stream, *cntl, nullptr)) {
            reply->mutable_status()->set_code(zb::rpc::OPTICAL_INTERNAL_ERROR); return;
        }
        std::lock_guard<std::mutex> lock(mu);
        auto session = next_session++;
        streams.emplace(session, stream);
        reply->set_session_id(session);
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
        ++subscriptions;
    }
    void PrepareRead(google::protobuf::RpcController*, const zb::rpc::PrepareReadRequest* request,
                     zb::rpc::PrepareReadReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        uint64_t id;
        {
            std::lock_guard<std::mutex> lock(mu);
            id = next_task++;
            tasks.emplace(id, Task{*request, Content(request->inode_id())});
        }
        ++preparations;
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
        reply->set_request_id(request->request_id()); reply->set_task_id(id);
        if (auto_ready) {
            Complete(id);
            // Exercise a legitimate ready-before-acceptance ordering.
            bthread_usleep(30000);
        }
    }
    void AcquireReadSession(google::protobuf::RpcController*, const zb::rpc::AcquireReadSessionRequest* request,
        zb::rpc::AcquireReadSessionReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::lock_guard<std::mutex> lock(mu);
        auto& task = tasks.at(request->task_id());
        if (!task.ready) {
            reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_READY); return;
        }
        if (!task.pinned) {
            reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND); return;
        }
        uint64_t handle = next_read++;
        sessions.emplace(handle, request->task_id()); ++task.readers;
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
        reply->set_read_handle(handle); reply->set_file_size(task.data.size()); reply->set_max_read_bytes(4);
    }
    void ReadPreparedFile(google::protobuf::RpcController*, const zb::rpc::ReadPreparedFileRequest* request,
        zb::rpc::ReadPreparedFileReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        ++reads;
        std::lock_guard<std::mutex> lock(mu);
        auto it = sessions.find(request->read_handle());
        if (it == sessions.end()) { reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND); return; }
        if (request->size() > 4) { reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT); return; }
        auto& task = tasks.at(it->second);
        auto offset = std::min<uint64_t>(request->offset(), task.data.size());
        auto data = task.data.substr(offset, request->size());
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
        reply->set_data(data); reply->set_bytes_read(data.size() + (corrupt_read ? 1 : 0));
        reply->set_eof(offset + data.size() >= task.data.size());
    }
    void ReleaseReadSession(google::protobuf::RpcController*, const zb::rpc::ReleaseReadSessionRequest* request,
        zb::rpc::ReleaseReadSessionReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::lock_guard<std::mutex> lock(mu);
        auto it = sessions.find(request->read_handle());
        if (it == sessions.end()) { reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND); return; }
        --tasks.at(it->second).readers; sessions.erase(it);
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
    }
    void ReleasePreparedRead(google::protobuf::RpcController*, const zb::rpc::ReleasePreparedReadRequest* request,
        zb::rpc::ReleasePreparedReadReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::lock_guard<std::mutex> lock(mu);
        tasks.at(request->task_id()).pinned = false;
        ++releases;
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_OK);
    }
};
