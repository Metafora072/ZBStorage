#include "client/sdk/PrepareClient.h"
#include "client/control/LocalProtocol.h"
#include "client_prepare_control.pb.h"
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace zb::client {
namespace {
control::Reply Receive(int fd) {
    control::Reply reply;
    control::ReadFrame(fd, &reply);
    if (reply.error_code()) throw ClientError(reply.error_code(), reply.message());
    return reply;
}
PreparedFile Decode(const control::Reply& reply, const PrepareHandle& handle) {
    if (reply.prepare_handle() != handle.value || reply.path().empty())
        throw ClientError(EIO, "invalid local preparation result");
    return {handle, reply.path(), reply.size()};
}
}
struct PrepareClient::Impl {
    struct Watcher {
        std::shared_ptr<control::Socket> socket;
        std::thread thread;
        std::atomic<bool> done{false};
    };
    std::string path;
    std::atomic<bool> stopping{false};
    std::mutex mu;
    std::vector<std::unique_ptr<Watcher>> watchers;
    explicit Impl(std::string p) : path(std::move(p)) {}
    void Reap() {
        std::lock_guard<std::mutex> lock(mu);
        for (auto it = watchers.begin(); it != watchers.end();) {
            if ((*it)->done) {
                if ((*it)->thread.joinable()) (*it)->thread.join();
                it = watchers.erase(it);
            } else ++it;
        }
    }
    control::Reply Call(const control::Request& request) {
        control::Socket socket(control::Connect(path));
        control::SendFrame(socket.fd(), request);
        return Receive(socket.fd());
    }
};
PrepareClient::PrepareClient(std::string socket) : impl_(std::make_unique<Impl>(std::move(socket))) {}
PrepareClient::~PrepareClient() {
    impl_->stopping = true;
    for (auto& w : impl_->watchers) w->socket->Shutdown();
    for (auto& w : impl_->watchers) if (w->thread.joinable()) w->thread.join();
}
PrepareHandle PrepareClient::submit(const std::string& path) {
    control::Request request;
    request.set_operation(control::SUBMIT);
    request.set_path(path);
    auto reply = impl_->Call(request);
    if (reply.prepare_handle().empty()) throw ClientError(EIO, "missing preparation handle");
    return {reply.prepare_handle()};
}
PreparedFile PrepareClient::wait(const PrepareHandle& handle) {
    control::Request request;
    request.set_operation(control::WATCH);
    request.set_prepare_handle(handle.value);
    return Decode(impl_->Call(request), handle);
}
PrepareHandle PrepareClient::prepare_read(const std::string& path,
                                         std::function<void(const PreparedFile&)> ready,
                                         std::function<void(const ClientError&)> error) {
    if (!ready) throw ClientError(EINVAL, "on_ready callback is required");
    impl_->Reap();
    auto handle = submit(path);
    auto watcher = std::make_unique<Impl::Watcher>();
    watcher->socket = std::make_shared<control::Socket>(control::Connect(impl_->path));
    control::Request request;
    request.set_operation(control::WATCH);
    request.set_prepare_handle(handle.value);
    control::SendFrame(watcher->socket->fd(), request);
    auto socket = watcher->socket;
    auto* tracking = watcher.get();
    watcher->thread = std::thread([this, tracking, socket, handle, ready = std::move(ready), error = std::move(error)] {
        try {
            auto result = Decode(Receive(socket->fd()), handle);
            if (!impl_->stopping) ready(result);
        } catch (const ClientError& e) {
            if (!impl_->stopping) {
                try {
                    if (error) error(e);
                    else std::fprintf(stderr, "[prepare_sdk] %s\n", e.what());
                } catch (...) { std::fprintf(stderr, "[prepare_sdk] on_error threw\n"); }
            }
        } catch (...) { std::fprintf(stderr, "[prepare_sdk] on_ready threw\n"); }
        tracking->done = true;
    });
    std::lock_guard<std::mutex> lock(impl_->mu);
    impl_->watchers.push_back(std::move(watcher));
    return handle;
}
void PrepareClient::release_prepared(const PrepareHandle& handle) {
    impl_->Reap();
    control::Request request;
    request.set_operation(control::RELEASE);
    request.set_prepare_handle(handle.value);
    impl_->Call(request);
}
} // namespace zb::client
