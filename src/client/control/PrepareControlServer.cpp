#include "client/control/PrepareControlServer.h"
#include "client/control/LocalProtocol.h"
#include "client_prepare_control.pb.h"
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <filesystem>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

namespace zb::client::control {
struct PrepareControlServer::State : std::enable_shared_from_this<State> {
    optical::OpticalReadClient& client;
    std::string mount, socket_path;
    Resolver resolver;
    std::shared_ptr<Socket> listener;
    std::thread acceptor;
    std::vector<std::thread> workers;
    std::mutex mu;
    std::condition_variable cv;
    bool stopping{false};
    std::deque<std::function<void()>> jobs;
    std::map<int, std::shared_ptr<Socket>> peers;
    State(optical::OpticalReadClient& c, std::string m, std::string s, Resolver r)
        : client(c), mount(std::move(m)), socket_path(std::move(s)), resolver(std::move(r)) {}
    void Enqueue(std::function<void()> job) {
        std::lock_guard<std::mutex> lock(mu);
        if (stopping) return;
        jobs.push_back(std::move(job));
        cv.notify_one();
    }
    void Run() {
        while (true) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mu);
                cv.wait(lock, [&] { return stopping || !jobs.empty(); });
                if (jobs.empty() && stopping) return;
                job = std::move(jobs.front()); jobs.pop_front();
            }
            try { job(); } catch (...) { /* An individual control call cannot terminate the server. */ }
        }
    }
    void ReplyTo(const std::shared_ptr<Socket>& peer, const Reply& reply) {
        try { SendFrame(peer->fd(), reply); } catch (...) {}
        peer->Shutdown();
        std::lock_guard<std::mutex> lock(mu);
        peers.erase(peer->fd());
    }
    std::string RelativePath(const std::string& path, std::string* normalized) {
        const auto p = std::filesystem::path(path);
        if (!p.is_absolute()) throw optical::ClientError(EINVAL, "prepare requires an absolute mounted path");
        *normalized = p.lexically_normal().string();
        const std::string prefix = mount == "/" ? "/" : mount + "/";
        if (normalized->compare(0, prefix.size(), prefix) != 0)
            throw optical::ClientError(EINVAL, "path is outside this mount");
        return "/" + normalized->substr(prefix.size());
    }
    void Handle(const std::shared_ptr<Socket>& peer) {
        Reply reply;
        try {
            Request request;
            ReadFrame(peer->fd(), &request);
            switch (request.operation()) {
            case SUBMIT: {
                std::string normalized;
                auto path = RelativePath(request.path(), &normalized);
                auto handle = client.Prepare(resolver(path), normalized);
                reply.set_prepare_handle(handle.value);
                break;
            }
            case WATCH: {
                std::weak_ptr<State> owner = shared_from_this();
                std::weak_ptr<Socket> socket = peer;
                client.Watch({request.prepare_handle()}, [owner, socket](const optical::Completion& completion) {
                    auto s = owner.lock(); auto p = socket.lock();
                    if (!s || !p) return;
                    Reply ready;
                    ready.set_error_code(completion.error);
                    ready.set_message(completion.message);
                    ready.set_prepare_handle(completion.file.handle.value);
                    ready.set_path(completion.file.path);
                    ready.set_size(completion.file.size);
                    // Stream callback never writes to a possibly slow local socket.
                    s->Enqueue([s, p, ready] { s->ReplyTo(p, ready); });
                });
                return; // No worker is occupied while optical preparation runs.
            }
            case RELEASE:
                client.Release({request.prepare_handle()});
                break;
            default: throw optical::ClientError(EINVAL, "unknown control operation");
            }
        } catch (const optical::ClientError& e) {
            reply.set_error_code(e.code()); reply.set_message(e.what());
        } catch (const std::exception& e) {
            reply.set_error_code(EIO); reply.set_message(e.what());
        }
        ReplyTo(peer, reply);
    }
};
PrepareControlServer::PrepareControlServer(optical::OpticalReadClient& client, std::string mount,
                                          std::string path, Resolver resolver)
    : state_(std::make_shared<State>(client, std::move(mount), std::move(path), std::move(resolver))) {}
PrepareControlServer::~PrepareControlServer() { Stop(); }
void PrepareControlServer::Start() {
    auto s = state_;
    if (s->listener) throw optical::ClientError(EALREADY, "control service already started");
    if (!std::filesystem::path(s->mount).is_absolute() || !std::filesystem::path(s->socket_path).is_absolute())
        throw optical::ClientError(EINVAL, "mount and control socket paths must be absolute");
    s->mount = std::filesystem::path(s->mount).lexically_normal().string();
    while (s->mount.size() > 1 && s->mount.back() == '/') s->mount.pop_back();
    s->socket_path = std::filesystem::path(s->socket_path).lexically_normal().string();
    if (s->mount == "/" || s->socket_path == s->mount || s->socket_path.rfind(s->mount + "/", 0) == 0)
        throw optical::ClientError(EINVAL, "control socket must be outside the FUSE mount");
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (s->socket_path.size() >= sizeof(address.sun_path))
        throw optical::ClientError(ENAMETOOLONG, "control socket path too long");
    std::memcpy(address.sun_path, s->socket_path.c_str(), s->socket_path.size() + 1);
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw optical::ClientError(errno, "cannot create control listener");
    auto listener = std::make_shared<Socket>(fd);
    // Never unlink an existing path: it could belong to another live mount.
    if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0)
        throw optical::ClientError(errno, "cannot bind control socket: " + s->socket_path);
    if (chmod(s->socket_path.c_str(), 0600) < 0 || listen(fd, 64) < 0) {
        int error = errno; unlink(s->socket_path.c_str());
        throw optical::ClientError(error, "cannot start control listener");
    }
    s->listener = listener;
    for (int i = 0; i < 3; ++i) s->workers.emplace_back([s] { s->Run(); });
    s->acceptor = std::thread([s] {
        while (true) {
            int fd = accept4(s->listener->fd(), nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EINTR) continue;
                return;
            }
            auto peer = std::make_shared<Socket>(fd);
            // Bounds initial request I/O only, not the asynchronous WATCH lifetime.
            timeval timeout{5, 0};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
            {
                std::lock_guard<std::mutex> lock(s->mu);
                if (s->stopping) return;
                s->peers.emplace(fd, peer);
            }
            s->Enqueue([s, peer] { s->Handle(peer); });
        }
    });
}
void PrepareControlServer::Stop() {
    auto s = state_;
    if (!s->listener) return;
    {
        std::lock_guard<std::mutex> lock(s->mu);
        if (s->stopping) return;
        s->stopping = true;
        for (auto& p : s->peers) p.second->Shutdown();
        s->cv.notify_all();
    }
    s->listener->Shutdown();
    if (s->acceptor.joinable()) s->acceptor.join();
    for (auto& worker : s->workers) worker.join();
    s->peers.clear();
    s->listener.reset();
    unlink(s->socket_path.c_str());
}
} // namespace zb::client::control
