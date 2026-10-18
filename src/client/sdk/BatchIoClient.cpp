#include "BatchIoClient.h"
#include "common/concurrency/BoundedExecutor.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <fcntl.h>
#include <limits>
#include <map>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace zb::client {
namespace {
using Clock = std::chrono::steady_clock;
struct File {
    int fd{-1};
    ~File() { if (fd >= 0) ::close(fd); }
};
BatchIoResult Execute(const BatchIoTask& t, uint64_t chunk) {
    const auto start = Clock::now();
    BatchIoResult r; r.request_id = t.request_id;
    auto run = [&] {
        File target{::open(t.path.c_str(), (t.write ? O_WRONLY : O_RDONLY) | O_CLOEXEC)};
        if (target.fd < 0) { r.error = errno; return; }
        File local{::open(t.local_path.c_str(), (t.write ? O_RDONLY : O_WRONLY | O_CREAT) | O_CLOEXEC, 0644)};
        if (local.fd < 0) { r.error = errno; return; }
        struct stat a{}, b{};
        if (::fstat(target.fd, &a) || ::fstat(local.fd, &b)) { r.error = errno; return; }
        if (!S_ISREG(a.st_mode) || !S_ISREG(b.st_mode) || (a.st_dev == b.st_dev && a.st_ino == b.st_ino)) {
            r.error = EINVAL; return;
        }
        if (!t.write && ::ftruncate(local.fd, 0)) { r.error = errno; return; }
        std::vector<char> buffer(static_cast<size_t>(std::min(chunk, t.size)));
        while (r.bytes < t.size) {
            const size_t size = std::min<uint64_t>(chunk, t.size - r.bytes);
            const int from = t.write ? local.fd : target.fd;
            const int to = t.write ? target.fd : local.fd;
            const auto src_offset = t.write ? r.bytes : t.offset + r.bytes;
            ssize_t n;
            do { n = ::pread(from, buffer.data(), size, src_offset); } while (n < 0 && errno == EINTR);
            if (n < 0) { r.error = errno; return; }
            if (!n) { if (t.write) r.error = EIO; return; } // Read EOF is a successful short read.
            size_t sent = 0;
            while (sent < static_cast<size_t>(n)) {
                const auto dst_offset = t.write ? t.offset + r.bytes : r.bytes;
                if (t.write) r.may_have_written = true;
                ssize_t count = ::pwrite(to, buffer.data() + sent, n - sent, dst_offset);
                if (count < 0 && errno == EINTR) continue;
                if (count <= 0) { r.error = count < 0 ? errno : EIO; return; }
                sent += count; r.bytes += count;
            }
        }
        int fd = t.write ? target.fd : local.fd;
        if (t.write) target.fd = -1; else local.fd = -1;
        if (::close(fd)) r.error = errno;
    };
    try { run(); } catch (const std::bad_alloc&) { r.error = ENOMEM; } catch (...) { r.error = EIO; }
    r.elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start).count();
    if (!r.error) r.may_have_written = false;
    return r;
}
}
struct BatchIoClient::Impl {
    BatchIoOptions options;
    concurrency::BoundedExecutor pool;
    std::mutex mu;
    std::vector<Handle> batches;
    explicit Impl(BatchIoOptions o) : options(o), pool({o.max_parallel_files, o.queue_depth,
                                                       o.max_parallel_files, o.max_buffer_bytes}) {
        if (!o.chunk_bytes || o.chunk_bytes > o.max_buffer_bytes / 2 || !o.max_batches ||
            o.chunk_bytes > static_cast<uint64_t>(std::numeric_limits<ssize_t>::max()))
            throw std::invalid_argument("invalid batch buffer limits");
    }
};
BatchIoClient::BatchIoClient(BatchIoOptions o) : impl_(std::make_unique<Impl>(o)) {}
BatchIoClient::~BatchIoClient() {
    for (auto& h : impl_->batches) h.wait();
}
BatchIoClient::Handle BatchIoClient::submit_batch(std::vector<BatchIoTask> tasks, Callback callback) {
    std::lock_guard<std::mutex> guard(impl_->mu);
    auto& batches = impl_->batches;
    batches.erase(std::remove_if(batches.begin(), batches.end(), [](const Handle& h) {
        return h.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }), batches.end());
    if (batches.size() >= impl_->options.max_batches || tasks.size() > impl_->options.queue_depth)
        throw std::system_error(EAGAIN, std::generic_category(), "batch queue full");
    std::map<std::string, size_t> ids, files;
    std::vector<std::vector<size_t>> dependencies(tasks.size());
    std::vector<size_t> previous(tasks.size(), tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& t = tasks[i];
        if (t.request_id.empty() || ids.count(t.request_id) || t.path.empty() || t.local_path.empty() ||
            t.offset > INT64_MAX || t.size > INT64_MAX - t.offset)
            throw std::invalid_argument("invalid batch request");
        for (const auto& id : t.after) {
            auto it = ids.find(id);
            if (it == ids.end()) throw std::invalid_argument("dependency must name an earlier request");
            dependencies[i].push_back(it->second);
        }
        ids[t.request_id] = i;
        struct stat st{};
        const std::string key = ::stat(t.path.c_str(), &st) == 0
            ? std::to_string(st.st_dev) + ":" + std::to_string(st.st_ino) : t.path;
        auto it = files.find(key);
        if (it != files.end()) previous[i] = it->second;
        files[key] = i;
    }
    auto* impl = impl_.get();
    auto h = std::async(std::launch::async,
        [impl, tasks = std::move(tasks), dependencies = std::move(dependencies), previous = std::move(previous), callback] {
            std::vector<BatchIoResult> results(tasks.size());
            std::vector<std::future<BatchIoResult>> running(tasks.size());
            struct Drain { std::vector<std::future<BatchIoResult>>& fs; ~Drain() {
                for (auto& f : fs) if (f.valid()) f.wait();
            }} drain{running};
            std::vector<bool> done(tasks.size(), false);
            size_t completed = 0;
            while (completed < tasks.size()) {
                bool progress = false;
                for (size_t i = 0; i < tasks.size(); ++i) {
                    if (done[i]) continue;
                    if (running[i].valid()) {
                        if (running[i].wait_for(std::chrono::seconds(0)) != std::future_status::ready) continue;
                        results[i] = running[i].get();
                    } else {
                        if (previous[i] < tasks.size() && !done[previous[i]]) continue;
                        bool ready = true, failed = false;
                        for (auto dep : dependencies[i]) { ready &= done[dep]; failed |= done[dep] && results[dep].error; }
                        if (!ready) continue;
                        if (failed) {
                            results[i].request_id = tasks[i].request_id;
                            results[i].error = ECANCELED; results[i].skipped = true;
                        } else {
                            auto task = std::make_shared<std::packaged_task<BatchIoResult()>>([&, i] {
                                return Execute(tasks[i], impl->options.chunk_bytes);
                            });
                            auto future = task->get_future();
                            if (impl->pool.Submit("files", impl->options.chunk_bytes * 2, [task] { (*task)(); })) {
                                running[i] = std::move(future); progress = true; continue;
                            }
                            // Only bounded coordinators wait for SDK resources; no storage RPC polling.
                            continue;
                        }
                    }
                    done[i] = true; ++completed; progress = true;
                    if (callback) { try { callback(results[i]); } catch (...) {} }
                }
                if (!progress) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return results;
        }).share();
    batches.push_back(h);
    return h;
}
}
