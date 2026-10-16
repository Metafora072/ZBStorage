#include "InProcessOpticalService.h"

#include <brpc/controller.h>
#include <brpc/stream.h>
#include <condition_variable>
#include <map>
#include <random>
#include <thread>

namespace zb::scheduler {
namespace {
using Clock = std::chrono::steady_clock;
constexpr uint32_t kMaxReadBytes = 4 * 1024 * 1024;
constexpr size_t kMaxSessions = 4096, kMaxHandles = 65536;

void Status(zb::rpc::OpticalStatus* out, zb::rpc::OpticalStatusCode code,
            const std::string& message = {}) {
    out->set_code(code);
    out->set_message(message);
}
void Status(zb::rpc::OpticalStatus* out, const OpticalModelResult& result) {
    auto code = zb::rpc::OPTICAL_INTERNAL_ERROR;
    switch (result.code) {
    case OpticalModelCode::kOk: code = zb::rpc::OPTICAL_OK; break;
    case OpticalModelCode::kInvalid: code = zb::rpc::OPTICAL_INVALID_ARGUMENT; break;
    case OpticalModelCode::kNotFound: code = zb::rpc::OPTICAL_NOT_FOUND; break;
    case OpticalModelCode::kBusy:
    case OpticalModelCode::kNotReady: code = zb::rpc::OPTICAL_NOT_READY; break;
    default: break;
    }
    Status(out, code, result.message);
}
uint64_t InitialId() {
    std::random_device random;
    return ((uint64_t(random()) << 32) | random()) & 0x3fffffffffffffffULL;
}
} // namespace

// 每个共享服务只有一个推进线程；不为逻辑节点或准备请求创建独立线程。
// 会话只存控制状态，业务字节仍由 OpticalReadModel 按需生成。
struct InProcessOpticalService::PreparedState : brpc::StreamInputHandler {
    struct Subscription {
        std::string node_id;
        brpc::StreamId stream;
    };
    struct Preparation {
        zb::rpc::PrepareReadRequest request;
        std::shared_ptr<OpticalReadModel> model;
        uint64_t task_id{0};
        size_t readers{0};
        bool held{true}, notified{false}, removed{false};
        Clock::time_point ready_since{}, removed_at{};
    };
    using Key = std::pair<std::string, uint64_t>;
    mutable std::mutex mu;
    std::condition_variable cv;
    bool stopping{false};
    std::map<uint64_t, Subscription> subscriptions;
    std::map<Key, std::shared_ptr<Preparation>> tasks;
    std::map<std::pair<uint64_t, std::string>, std::shared_ptr<Preparation>> requests;
    std::map<uint64_t, std::shared_ptr<Preparation>> handles;
    uint64_t next_session{InitialId() + 1}, next_handle{InitialId() + 1};
    NodePowerManager* manager;
    InProcessOpticalService* owner;
    std::thread worker;

    PreparedState(NodePowerManager* nodes, InProcessOpticalService* service)
        : manager(nodes), owner(service), worker([this] { Run(); }) {}
    ~PreparedState() override {
        std::vector<brpc::StreamId> streams;
        {
            std::lock_guard<std::mutex> lock(mu);
            stopping = true;
            for (const auto& [_, subscription] : subscriptions) streams.push_back(subscription.stream);
            cv.notify_all();
        }
        worker.join();
        for (auto stream : streams) brpc::StreamClose(stream);
        // 回调结束前不能销毁 StreamInputHandler。
        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&] { return subscriptions.empty(); });
        for (const auto& [_, task] : tasks) {
            task->held = false;
            task->readers = 0;
            Cleanup(task);
        }
    }
    OpticalModelResult Cleanup(const std::shared_ptr<Preparation>& task) {
        if (task->held || task->readers || task->removed) return {};
        const auto result = task->model->ReleasePrepared(task->task_id);
        if (result.code == OpticalModelCode::kOk || result.code == OpticalModelCode::kNotFound) {
            task->removed = true;
            task->removed_at = Clock::now();
            return {};
        }
        return result;
    }
    int on_received_messages(brpc::StreamId, butil::IOBuf* const[], size_t) override { return 0; }
    void on_idle_timeout(brpc::StreamId) override {}
    void on_closed(brpc::StreamId stream) override {
        std::lock_guard<std::mutex> lock(mu);
        for (auto it = subscriptions.begin(); it != subscriptions.end(); ++it) {
            if (it->second.stream != stream) continue;
            const uint64_t session_id = it->first;
            for (auto handle = handles.begin(); handle != handles.end();) {
                if (handle->second->request.session_id() == session_id) {
                    --handle->second->readers;
                    handle = handles.erase(handle);
                } else ++handle;
            }
            for (const auto& [key, task] : requests) {
                if (key.first == session_id) { task->held = false; Cleanup(task); }
            }
            subscriptions.erase(it);
            break;
        }
        cv.notify_all();
    }
    void Run() {
        std::unique_lock<std::mutex> lock(mu);
        while (!stopping) {
            cv.wait_for(lock, std::chrono::milliseconds(5));
            if (stopping) break;
            struct Notice { brpc::StreamId stream; std::shared_ptr<Preparation> task; std::string bytes; };
            std::vector<Notice> notices;
            const auto now = Clock::now();
            for (auto it = tasks.begin(); it != tasks.end();) {
                const auto task = it->second;
                const auto retention = std::chrono::milliseconds(task->model->config().retry_retention_ms);
                if (task->removed && now - task->removed_at > retention) {
                    requests.erase({task->request.session_id(), task->request.request_id()});
                    it = tasks.erase(it);
                    continue;
                }
                ++it;
                if (!task->held) { Cleanup(task); continue; }
                // 引用回收属于控制维护，不因节点休眠或禁用而停止。
                if (task->notified && task->readers == 0 && now - task->ready_since > retention) {
                    task->held = false;
                    Cleanup(task);
                    continue;
                }
                if (manager) {
                    ManagedNodeRuntime runtime;
                    if (!manager->GetNode(task->model->config().node_id, &runtime) ||
                        (runtime.lifecycle != LifecycleState::kWorking && runtime.lifecycle != LifecycleState::kDraining) ||
                        !runtime.readiness.Ready() ||
                        runtime.health != ManagedHealthState::kHealthy || runtime.power_level < PowerLevel::kMedium ||
                        runtime.administrative_state == zb::storage_model::AdministrativeState::kDisabled)
                        continue;
                    std::lock_guard<std::mutex> catalog(owner->mu_);
                    if (owner->catalog_confirmed_ids_.count(task->model->config().node_id) == 0) continue;
                }
                OpticalPreparedInfo info;
                const auto result = task->model->InspectPrepared(task->task_id, &info);
                if (result.code != OpticalModelCode::kOk) continue;
                if (!info.ready) continue;
                if (task->ready_since == Clock::time_point{}) task->ready_since = now;
                const auto subscription = subscriptions.find(task->request.session_id());
                if (task->notified || subscription == subscriptions.end()) continue;
                zb::rpc::ReadReadyEvent event;
                event.set_request_id(task->request.request_id());
                event.set_task_id(task->task_id);
                event.set_inode_id(info.inode_id);
                event.set_file_size(info.size);
                event.set_has_file_version(task->request.has_file_version());
                if (task->request.has_file_version()) event.set_file_version(info.version);
                notices.push_back({subscription->second.stream, task, event.SerializeAsString()});
            }
            // StreamWrite 不持有目录锁，避免慢客户端阻塞其他节点控制操作。
            lock.unlock();
            for (const auto& notice : notices) {
                butil::IOBuf bytes;
                bytes.append(notice.bytes);
                const bool sent = brpc::StreamWrite(notice.stream, bytes) == 0;
                if (sent) {
                    std::lock_guard<std::mutex> guard(mu);
                    notice.task->notified = true;
                }
            }
            lock.lock();
        }
    }
};

InProcessOpticalService::InProcessOpticalService(NodePowerManager* manager,
    std::string mds_address, std::string service_address, InProcessDiskService* sources)
    : node_manager_(manager), mds_address_(std::move(mds_address)),
      service_address_(std::move(service_address)), model_sources_(sources),
      prepared_(std::make_unique<PreparedState>(manager, this)) {}
InProcessOpticalService::~InProcessOpticalService() = default;

std::shared_ptr<OpticalReadModel> InProcessOpticalService::ResolvePrepared(
    google::protobuf::RpcController* controller, zb::rpc::OpticalStatus* status, bool ready) const {
    zb::rpc::MdsStatus legacy;
    auto model = Resolve(controller, &legacy, ready);
    if (model) Status(status, zb::rpc::OPTICAL_OK);
    else Status(status, legacy.code() == zb::rpc::MDS_NOT_FOUND ? zb::rpc::OPTICAL_NOT_FOUND :
        (legacy.code() == zb::rpc::MDS_INTERNAL_ERROR ? zb::rpc::OPTICAL_NOT_READY :
                                                      zb::rpc::OPTICAL_INVALID_ARGUMENT), legacy.message());
    return model;
}

uint32_t InProcessOpticalService::ActiveReadSessions(const std::string& id) const {
    std::lock_guard<std::mutex> lock(prepared_->mu);
    uint32_t count = 0;
    for (const auto& [_, task] : prepared_->handles)
        if (task->model->config().node_id == id) ++count;
    return count;
}

void InProcessOpticalService::SubscribeReadEvents(google::protobuf::RpcController* controller,
    const zb::rpc::SubscribeReadEventsRequest* request, zb::rpc::SubscribeReadEventsReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), true);
    if (!model) return;
    if (!request || request->client_id().empty() || request->client_id().size() > 256) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_INVALID_ARGUMENT, "invalid client identity"); return;
    }
    std::lock_guard<std::mutex> lock(prepared_->mu);
    if (prepared_->stopping || prepared_->subscriptions.size() >= kMaxSessions) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_READY, "notification session budget exceeded"); return;
    }
    brpc::StreamOptions options;
    options.handler = prepared_.get();
    options.idle_timeout_ms = -1;
    brpc::StreamId stream;
    if (brpc::StreamAccept(&stream, *static_cast<brpc::Controller*>(controller), &options) != 0) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_INVALID_ARGUMENT, "notification stream required"); return;
    }
    const uint64_t id = prepared_->next_session++;
    prepared_->subscriptions.emplace(id, PreparedState::Subscription{model->config().node_id, stream});
    reply->set_session_id(id);
}

void InProcessOpticalService::PrepareRead(google::protobuf::RpcController* controller,
    const zb::rpc::PrepareReadRequest* request, zb::rpc::PrepareReadReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), true);
    if (!model) return;
    if (!request || request->request_id().empty() || request->request_id().size() > 256 ||
        (request->has_file_version() && request->file_version() == 0)) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_INVALID_ARGUMENT, "invalid preparation request"); return;
    }
    std::lock_guard<std::mutex> lock(prepared_->mu);
    const auto subscription = prepared_->subscriptions.find(request->session_id());
    if (subscription == prepared_->subscriptions.end() || subscription->second.node_id != model->config().node_id) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "notification session not found at target node"); return;
    }
    const auto key = std::make_pair(request->session_id(), request->request_id());
    const auto old = prepared_->requests.find(key);
    if (old != prepared_->requests.end()) {
        if (old->second->request.SerializeAsString() != request->SerializeAsString())
            Status(reply->mutable_status(), zb::rpc::OPTICAL_INVALID_ARGUMENT, "request ID reused with different parameters");
        else if (!old->second->held)
            Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "preparation already released");
        else { reply->set_request_id(request->request_id()); reply->set_task_id(old->second->task_id); }
        return;
    }
    if (prepared_->tasks.size() >= kMaxHandles) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_READY, "preparation budget exceeded"); return;
    }
    OpticalPreparedInfo expected;
    expected.size = request->file_size();
    expected.version = request->has_file_version() ? request->file_version() : 0;
    const auto result = model->RequestRead(request->inode_id(), request->disk_id(), request->image_id(), true, &expected);
    Status(reply->mutable_status(), result);
    if (result.code != OpticalModelCode::kOk) return;
    auto task = std::make_shared<PreparedState::Preparation>();
    task->request = *request;
    task->model = model;
    task->task_id = result.task_id;
    prepared_->tasks.emplace(PreparedState::Key{model->config().node_id, task->task_id}, task);
    prepared_->requests.emplace(key, task);
    reply->set_task_id(task->task_id);
    reply->set_request_id(request->request_id());
    prepared_->cv.notify_all();
}

void InProcessOpticalService::AcquireReadSession(google::protobuf::RpcController* controller,
    const zb::rpc::AcquireReadSessionRequest* request, zb::rpc::AcquireReadSessionReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), true);
    if (!model) return;
    std::lock_guard<std::mutex> lock(prepared_->mu);
    const auto it = prepared_->tasks.find({model->config().node_id, request ? request->task_id() : 0});
    if (it == prepared_->tasks.end() || !it->second->held) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "preparation not found"); return;
    }
    OpticalPreparedInfo info;
    const auto result = model->InspectPrepared(it->second->task_id, &info);
    Status(reply->mutable_status(), result);
    if (result.code != OpticalModelCode::kOk) return;
    if (!info.ready || prepared_->handles.size() >= kMaxHandles) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_READY, "preparation pending or read session budget exceeded"); return;
    }
    const uint64_t handle = prepared_->next_handle++;
    ++it->second->readers;
    prepared_->handles.emplace(handle, it->second);
    reply->set_read_handle(handle);
    reply->set_file_size(info.size);
    reply->set_max_read_bytes(kMaxReadBytes);
}

void InProcessOpticalService::ReadPreparedFile(google::protobuf::RpcController* controller,
    const zb::rpc::ReadPreparedFileRequest* request, zb::rpc::ReadPreparedFileReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), true);
    if (!model) return;
    std::lock_guard<std::mutex> lock(prepared_->mu);
    const auto it = prepared_->handles.find(request ? request->read_handle() : 0);
    if (it == prepared_->handles.end() || it->second->model != model) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "read session not found at target node"); return;
    }
    if (request->size() > kMaxReadBytes) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_INVALID_ARGUMENT, "read exceeds session limit"); return;
    }
    auto metrics = GetMetricsShared(model->config().node_id);
    auto operation = metrics ? metrics->Start(zb::metrics::OperationKind::kRead) :
                               zb::metrics::NodeMetricsCollector::ScopedOperation{};
    auto result = model->ReadTask(it->second->task_id, request->offset(), request->size());
    Status(reply->mutable_status(), result);
    if (result.code != OpticalModelCode::kOk) return;
    operation.SetBytes(result.data.size());
    reply->set_bytes_read(result.data.size());
    reply->set_eof(request->offset() >= it->second->request.file_size() ||
        result.data.size() == it->second->request.file_size() - request->offset());
    reply->set_data(std::move(result.data));
}

void InProcessOpticalService::ReleaseReadSession(google::protobuf::RpcController* controller,
    const zb::rpc::ReleaseReadSessionRequest* request, zb::rpc::ReleaseReadSessionReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), false);
    if (!model) return;
    std::lock_guard<std::mutex> lock(prepared_->mu);
    const auto it = prepared_->handles.find(request ? request->read_handle() : 0);
    if (it == prepared_->handles.end() || it->second->model != model) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "read session not found at target node"); return;
    }
    auto task = it->second;
    --task->readers;
    prepared_->handles.erase(it);
    Status(reply->mutable_status(), prepared_->Cleanup(task));
}

void InProcessOpticalService::ReleasePreparedRead(google::protobuf::RpcController* controller,
    const zb::rpc::ReleasePreparedReadRequest* request, zb::rpc::ReleasePreparedReadReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    auto model = ResolvePrepared(controller, reply->mutable_status(), false);
    if (!model) return;
    std::lock_guard<std::mutex> lock(prepared_->mu);
    const auto it = prepared_->tasks.find({model->config().node_id, request ? request->task_id() : 0});
    if (it == prepared_->tasks.end()) {
        Status(reply->mutable_status(), zb::rpc::OPTICAL_NOT_FOUND, "preparation not found"); return;
    }
    it->second->held = false;
    Status(reply->mutable_status(), prepared_->Cleanup(it->second));
}
} // namespace zb::scheduler
