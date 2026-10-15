#include "client/optical/OpticalReadClient.h"
#include "optical_node.pb.h"
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <random>
#include <sstream>
#include <vector>
#include <unistd.h>

namespace zb::client::optical {
namespace {
using Clock = std::chrono::steady_clock;
int StatusErrno(zb::rpc::OpticalStatusCode code, bool handle = false) {
    switch (code) {
    case zb::rpc::OPTICAL_OK: return 0;
    case zb::rpc::OPTICAL_INVALID_ARGUMENT: return EINVAL;
    case zb::rpc::OPTICAL_NOT_FOUND: return handle ? EBADF : ENOENT;
    case zb::rpc::OPTICAL_NOT_READY: return EAGAIN;
    default: return EIO;
    }
}
template<class Reply>
void Check(const brpc::Controller& controller, const Reply& reply, bool handle = false) {
    if (controller.Failed()) throw ClientError(EIO, controller.ErrorText());
    if (!reply.has_status()) throw ClientError(EIO, "missing OpticalStatus");
    if (int error = StatusErrno(reply.status().code(), handle))
        throw ClientError(error, reply.status().message());
}
std::string Key(uint64_t inode, std::optional<uint64_t> version) {
    return std::to_string(inode) + ":" + (version ? std::to_string(*version) : "none");
}
std::string InstanceId() {
    std::random_device random;
    std::ostringstream id;
    id << std::hex << getpid() << '-' << Clock::now().time_since_epoch().count()
       << '-' << random() << random();
    return id.str();
}
void Deliver(const std::vector<OpticalReadClient::ReadyCallback>& callbacks, const Completion& result) {
    for (const auto& callback : callbacks) {
        try { callback(result); }
        catch (...) { std::fprintf(stderr, "[optical] completion callback threw\n"); }
    }
}
}

struct OpticalReadClient::Impl {
    enum class State { Submitting, Accepted, Ready, Releasing, Released, Failed };
    struct Connection : brpc::StreamInputHandler {
        Impl* owner;
        std::string address;
        brpc::Channel channel;
        std::mutex init_mu;
        uint64_t session_id{0};
        brpc::StreamId stream{brpc::INVALID_STREAM_ID};
        std::mutex close_mu;
        std::condition_variable close_cv;
        bool closed{false};
        Connection(Impl* o, std::string a) : owner(o), address(std::move(a)) {}
        int on_received_messages(brpc::StreamId, butil::IOBuf* const messages[], size_t count) override {
            for (size_t i = 0; i < count; ++i) {
                zb::rpc::ReadReadyEvent event;
                if (event.ParseFromString(messages[i]->to_string())) owner->OnReady(address, event);
                else std::fprintf(stderr, "[optical] malformed ReadReady from %s\n", address.c_str());
            }
            return 0;
        }
        void on_idle_timeout(brpc::StreamId) override {}
        void on_closed(brpc::StreamId) override {
            std::lock_guard<std::mutex> lock(close_mu);
            closed = true;
            close_cv.notify_all();
        }
        void CloseStream() {
            if (stream == brpc::INVALID_STREAM_ID) return;
            brpc::StreamClose(stream);
            std::unique_lock<std::mutex> lock(close_mu);
            close_cv.wait(lock, [&] { return closed; });
            stream = brpc::INVALID_STREAM_ID;
        }
    };
    struct Record {
        FileInfo file;
        PreparedFile result;
        std::string key;
        uint64_t task_id{0};
        State state{State::Submitting};
        int error{0};
        std::string message;
        std::vector<ReadyCallback> watchers;
        std::shared_ptr<Connection> connection;
        // Only open/release for this file are serialized across RPCs.
        std::mutex operation_mu;
        Clock::time_point submitted{Clock::now()};
    };
    Options options;
    const std::string instance{InstanceId()};
    std::mutex mu;
    bool stopped{false};
    uint64_t next_request{1};
    std::map<std::string, std::shared_ptr<Connection>> connections;
    std::map<std::string, std::shared_ptr<Record>> records;
    std::map<std::string, std::shared_ptr<Record>> active;

    static void Log(const char* phase, const Record& r, uint64_t read_handle = 0) {
        std::fprintf(stderr, "[optical] %s request=%s node=%s inode=%llu task=%llu read_handle=%llu\n",
            phase, r.result.handle.value.c_str(), r.file.node_address.c_str(),
            (unsigned long long)r.file.inode_id, (unsigned long long)r.task_id,
            (unsigned long long)read_handle);
    }
    explicit Impl(Options o) : options(o) {
        if (o.rpc_timeout_ms <= 0) throw ClientError(EINVAL, "RPC timeout must be positive");
    }
    std::shared_ptr<Connection> Connect(const std::string& address) {
        std::shared_ptr<Connection> c;
        {
            std::lock_guard<std::mutex> lock(mu);
            if (stopped) throw ClientError(ESHUTDOWN, "optical client stopped");
            auto& slot = connections[address];
            if (!slot) slot = std::make_shared<Connection>(this, address);
            c = slot;
        }
        std::lock_guard<std::mutex> lock(c->init_mu);
        if (c->session_id) return c;
        brpc::ChannelOptions opts;
        opts.protocol = "baidu_std";
        opts.timeout_ms = options.rpc_timeout_ms;
        opts.max_retry = 0;
        if (c->channel.Init(address.c_str(), &opts) != 0)
            throw ClientError(EIO, "cannot initialize optical channel: " + address);
        { std::lock_guard<std::mutex> closing(c->close_mu); c->closed = false; }
        brpc::Controller controller;
        brpc::StreamOptions stream_options;
        stream_options.handler = c.get();
        stream_options.idle_timeout_ms = -1;
        if (brpc::StreamCreate(&c->stream, controller, &stream_options) != 0)
            throw ClientError(EIO, "cannot create optical notification stream");
        zb::rpc::SubscribeReadEventsRequest request;
        request.set_client_id(instance);
        zb::rpc::SubscribeReadEventsReply reply;
        zb::rpc::OpticalNodeService_Stub stub(&c->channel);
        stub.SubscribeReadEvents(&controller, &request, &reply, nullptr);
        try {
            Check(controller, reply);
            if (!reply.session_id()) throw ClientError(EIO, "empty notification session");
            c->session_id = reply.session_id();
        } catch (...) {
            c->CloseStream();
            throw;
        }
        return c;
    }
    std::shared_ptr<Record> Find(const PrepareHandle& handle) {
        auto it = records.find(handle.value);
        if (it == records.end()) throw ClientError(ENOENT, "unknown preparation for this mount");
        return it->second;
    }
    void OnReady(const std::string& address, const zb::rpc::ReadReadyEvent& event) {
        std::vector<ReadyCallback> callbacks;
        Completion completion;
        {
            std::lock_guard<std::mutex> lock(mu);
            auto it = records.find(event.request_id());
            if (stopped || it == records.end()) return;
            auto& r = *it->second;
            if (r.state != State::Submitting && r.state != State::Accepted) return;
            if (address != r.file.node_address || !event.task_id() ||
                (r.task_id && r.task_id != event.task_id()) || event.inode_id() != r.file.inode_id ||
                event.file_size() != r.file.file_size ||
                event.has_file_version() != r.file.file_version.has_value() ||
                (r.file.file_version && event.file_version() != *r.file.file_version)) {
                r.state = State::Failed;
                r.error = EIO;
                r.message = "ReadReady does not match the submitted file";
            } else {
                if (!r.task_id) r.task_id = event.task_id();
                r.state = State::Ready; // Published before notifying any watcher.
                const auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - r.submitted).count();
                std::fprintf(stderr, "[optical] ready request=%s inode=%llu task=%llu prepare_us=%lld\n",
                    r.result.handle.value.c_str(), (unsigned long long)r.file.inode_id,
                    (unsigned long long)r.task_id, (long long)us);
            }
            completion = {r.result, r.error, r.message};
            callbacks.swap(r.watchers);
        }
        Deliver(callbacks, completion);
    }
};

struct OpticalReadClient::ReadSession {
    std::shared_ptr<Impl::Record> record;
    uint64_t read_handle{0};
    uint64_t file_size{0};
    uint32_t max_read_bytes{0};
    // Shared reads can proceed concurrently; close waits for in-flight reads.
    std::mutex mu;
    std::condition_variable cv;
    size_t readers{0};
    bool closed{false};
};

OpticalReadClient::OpticalReadClient(Options options) : impl_(std::make_unique<Impl>(options)) {}
OpticalReadClient::~OpticalReadClient() { Stop(); }
const std::string& OpticalReadClient::instance_id() const { return impl_->instance; }

PrepareHandle OpticalReadClient::Prepare(const FileInfo& file, const std::string& path) {
    if (!file.inode_id || file.node_address.empty() || file.disk_id.empty() || file.image_id.empty() || path.empty())
        throw ClientError(EINVAL, "incomplete optical file information");
    auto c = impl_->Connect(file.node_address);
    auto r = std::make_shared<Impl::Record>();
    r->file = file;
    r->key = Key(file.inode_id, file.file_version);
    r->connection = c;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (impl_->stopped) throw ClientError(ESHUTDOWN, "optical client stopped");
        auto existing = impl_->active.find(r->key);
        if (existing != impl_->active.end()) {
            const auto& old = *existing->second;
            if ((old.state == Impl::State::Accepted || old.state == Impl::State::Ready) &&
                old.file.node_address == file.node_address && old.file.disk_id == file.disk_id &&
                old.file.image_id == file.image_id && old.file.file_size == file.file_size)
                return old.result.handle; // Repeated prepare reuses the node-owned cached result.
            throw ClientError(EEXIST, "file already has an active preparation");
        }
        r->result = {{impl_->instance + ":" + std::to_string(impl_->next_request++)}, path, file.file_size};
        impl_->records.emplace(r->result.handle.value, r);
        impl_->active.emplace(r->key, r);
    }
    zb::rpc::PrepareReadRequest request;
    request.set_session_id(c->session_id);
    request.set_request_id(r->result.handle.value);
    request.set_inode_id(file.inode_id);
    request.set_disk_id(file.disk_id);
    request.set_image_id(file.image_id);
    request.set_file_size(file.file_size);
    request.set_has_file_version(file.file_version.has_value());
    if (file.file_version) request.set_file_version(*file.file_version);
    zb::rpc::PrepareReadReply reply;
    brpc::Controller controller;
    zb::rpc::OpticalNodeService_Stub stub(&c->channel);
    try {
        stub.PrepareRead(&controller, &request, &reply, nullptr);
        Check(controller, reply);
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (reply.request_id() != request.request_id() || !reply.task_id() ||
            (r->task_id && r->task_id != reply.task_id()))
            throw ClientError(EIO, "invalid preparation acceptance");
        if (!r->task_id) r->task_id = reply.task_id();
        if (r->state == Impl::State::Submitting) r->state = Impl::State::Accepted;
        if (r->state == Impl::State::Failed) throw ClientError(r->error, r->message);
        Impl::Log("accepted", *r);
    } catch (...) {
        std::lock_guard<std::mutex> lock(impl_->mu);
        impl_->active.erase(r->key);
        impl_->records.erase(r->result.handle.value);
        throw;
    }
    return r->result.handle;
}

void OpticalReadClient::Watch(const PrepareHandle& handle, ReadyCallback callback) {
    Completion result;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (impl_->stopped) throw ClientError(ESHUTDOWN, "optical client stopped");
        auto r = impl_->Find(handle);
        if (r->state == Impl::State::Submitting || r->state == Impl::State::Accepted) {
            r->watchers.push_back(std::move(callback));
            return;
        }
        if (r->state != Impl::State::Ready && r->state != Impl::State::Failed)
            throw ClientError(ENOENT, "preparation released");
        result = {r->result, r->error, r->message};
    }
    Deliver({callback}, result);
}

OpticalReadClient::Session OpticalReadClient::Open(uint64_t inode, std::optional<uint64_t> version) {
    std::shared_ptr<Impl::Record> r;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (impl_->stopped) throw ClientError(ESHUTDOWN, "optical client stopped");
        auto it = impl_->active.find(Key(inode, version));
        if (it == impl_->active.end()) throw ClientError(EAGAIN, "optical file must be prepared before open");
        r = it->second;
    }
    std::lock_guard<std::mutex> operation(r->operation_mu);
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (r->state != Impl::State::Ready) throw ClientError(EAGAIN, "optical file is not ready for open");
    }
    zb::rpc::AcquireReadSessionRequest request;
    request.set_task_id(r->task_id);
    zb::rpc::AcquireReadSessionReply reply;
    brpc::Controller controller;
    zb::rpc::OpticalNodeService_Stub stub(&r->connection->channel);
    stub.AcquireReadSession(&controller, &request, &reply, nullptr);
    if (!controller.Failed() && reply.has_status() && reply.status().code() == zb::rpc::OPTICAL_NOT_FOUND) {
        // The node may reclaim an idle prepared result. Discard only our stale local
        // handle; never ask the node to release its cache. The caller can prepare again.
        std::lock_guard<std::mutex> lock(impl_->mu);
        r->state = Impl::State::Released;
        impl_->active.erase(r->key);
        impl_->records.erase(r->result.handle.value);
        throw ClientError(EAGAIN, "optical cache expired; prepare the file again");
    }
    Check(controller, reply);
    if (!reply.read_handle() || !reply.max_read_bytes() || reply.file_size() != r->file.file_size)
        throw ClientError(EIO, "invalid optical read session");
    auto session = std::make_shared<ReadSession>();
    session->record = r;
    session->read_handle = reply.read_handle();
    session->file_size = reply.file_size();
    session->max_read_bytes = impl_->options.read_chunk_bytes
        ? std::min(impl_->options.read_chunk_bytes, reply.max_read_bytes()) : reply.max_read_bytes();
    Impl::Log("opened", *r, session->read_handle);
    return session;
}

size_t OpticalReadClient::Read(const Session& s, uint64_t offset, size_t size, char* buffer,
                               metrics::IoTraceContext* trace) {
    if (!s) throw ClientError(EBADF, "missing optical read session");
    if (size && !buffer) throw ClientError(EINVAL, "null read buffer");
    {
        std::lock_guard<std::mutex> lock(s->mu);
        if (s->closed) throw ClientError(EBADF, "optical read session closed");
        ++s->readers;
    }
    struct Guard {
        ReadSession* session;
        ~Guard() {
            std::lock_guard<std::mutex> lock(session->mu);
            --session->readers;
            session->cv.notify_all();
        }
    } guard{s.get()};
    if (offset >= s->file_size || size == 0) return 0;
    size_t total = 0;
    const uint64_t wanted = std::min<uint64_t>(size, s->file_size - offset);
    zb::rpc::OpticalNodeService_Stub stub(&s->record->connection->channel);
    while (total < wanted) {
        uint64_t chunk = std::min<uint64_t>(wanted - total, s->max_read_bytes);
        zb::rpc::ReadPreparedFileRequest request;
        request.set_read_handle(s->read_handle);
        request.set_offset(offset + total);
        request.set_size(chunk);
        zb::rpc::ReadPreparedFileReply reply;
        brpc::Controller controller;
        {
            metrics::RpcLatencyScope timing(trace, metrics::RpcTarget::DataNode);
            stub.ReadPreparedFile(&controller, &request, &reply, nullptr);
        }
        Check(controller, reply, true);
        if (reply.bytes_read() != chunk || reply.data().size() != chunk ||
            reply.eof() != (chunk == s->file_size - request.offset()))
            throw ClientError(EIO, "invalid optical byte count or EOF");
        std::memcpy(buffer + total, reply.data().data(), chunk);
        total += chunk;
    }
    return total;
}

void OpticalReadClient::Close(const Session& s) {
    if (!s) throw ClientError(EBADF, "missing optical read session");
    std::unique_lock<std::mutex> lock(s->mu);
    if (s->closed) throw ClientError(EBADF, "optical read session closed");
    s->closed = true;
    s->cv.wait(lock, [&] { return s->readers == 0; });
    lock.unlock();
    zb::rpc::ReleaseReadSessionRequest request;
    request.set_read_handle(s->read_handle);
    zb::rpc::ReleaseReadSessionReply reply;
    brpc::Controller controller;
    zb::rpc::OpticalNodeService_Stub stub(&s->record->connection->channel);
    stub.ReleaseReadSession(&controller, &request, &reply, nullptr);
    Check(controller, reply, true);
    Impl::Log("closed", *s->record, s->read_handle);
}

void OpticalReadClient::Release(const PrepareHandle& handle) {
    std::shared_ptr<Impl::Record> r;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        r = impl_->Find(handle);
    }
    std::lock_guard<std::mutex> operation(r->operation_mu);
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (r->state != Impl::State::Ready) throw ClientError(EAGAIN, "only ready preparations can be released");
        r->state = Impl::State::Releasing;
    }
    zb::rpc::ReleasePreparedReadRequest request;
    request.set_task_id(r->task_id);
    zb::rpc::ReleasePreparedReadReply reply;
    brpc::Controller controller;
    zb::rpc::OpticalNodeService_Stub stub(&r->connection->channel);
    stub.ReleasePreparedRead(&controller, &request, &reply, nullptr);
    Check(controller, reply);
    std::lock_guard<std::mutex> lock(impl_->mu);
    r->state = Impl::State::Released;
    Impl::Log("released", *r);
    impl_->active.erase(r->key);
    impl_->records.erase(handle.value); // Existing sessions keep their own shared record.
}

void OpticalReadClient::Stop() {
    if (!impl_) return;
    std::vector<std::shared_ptr<Impl::Connection>> connections;
    std::vector<ReadyCallback> callbacks;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        if (impl_->stopped) return;
        impl_->stopped = true;
        for (auto& entry : impl_->connections) connections.push_back(entry.second);
        for (auto& entry : impl_->records) {
            auto& watchers = entry.second->watchers;
            callbacks.insert(callbacks.end(), watchers.begin(), watchers.end());
            watchers.clear();
        }
    }
    for (const auto& c : connections) c->CloseStream();
    Deliver(callbacks, {{}, ESHUTDOWN, "optical client stopped"});
}
} // namespace zb::client::optical
