#pragma once
#include <algorithm>
#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace zb::concurrency {
struct ExecutorOptions {
    size_t workers{8}, queue_depth{256}, per_key{4};
    uint64_t max_bytes{64ULL * 1024 * 1024};
};
struct ExecutorStats {
    size_t active{0}, peak_active{0}, queued{0};
    uint64_t bytes{0}, rejected{0}, completed{0}, queue_ns{0}, execution_ns{0};
};
// Per-key limits are enforced before picking a task, not by blocking a worker.
// Started lazily: safe to construct before FUSE daemonizes. Stop drains accepted work.
class BoundedExecutor {
public:
    explicit BoundedExecutor(ExecutorOptions options = {}) : options_(options) {
        if (!options.workers || !options.queue_depth || !options.per_key || !options.max_bytes)
            throw std::invalid_argument("executor limits must be positive");
    }
    ~BoundedExecutor() { Stop(); }
    bool Submit(std::string key, uint64_t bytes, std::function<void()> fn) {
        std::unique_lock<std::mutex> lock(mu_);
        if (stopping_ || bytes > options_.max_bytes || stats_.bytes > options_.max_bytes - bytes ||
            queue_.size() >= options_.queue_depth) { ++stats_.rejected; return false; }
        if (threads_.empty()) {
            try {
                for (size_t i = 0; i < options_.workers; ++i) threads_.emplace_back([this] { Run(); });
            } catch (...) {
                stopping_ = true; cv_.notify_all();
                throw;
            }
        }
        queue_.push_back({std::move(key), bytes, std::move(fn), std::chrono::steady_clock::now()});
        stats_.bytes += bytes;
        cv_.notify_all();
        return true;
    }
    ExecutorStats Stats() const {
        std::lock_guard<std::mutex> lock(mu_);
        auto s = stats_; s.queued = queue_.size(); return s;
    }
    void Stop() {
        { std::lock_guard<std::mutex> lock(mu_); stopping_ = true; cv_.notify_all(); }
        for (auto& t : threads_) if (t.joinable()) t.join();
    }
private:
    struct Task { std::string key; uint64_t bytes; std::function<void()> run; std::chrono::steady_clock::time_point enqueued; };
    auto Eligible() {
        return std::find_if(queue_.begin(), queue_.end(), [this](const Task& t) {
            auto it = active_.find(t.key);
            return it == active_.end() || it->second < options_.per_key;
        });
    }
    void Run() {
        for (;;) {
            Task task;
            std::chrono::steady_clock::time_point started;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait(lock, [this] { return (stopping_ && queue_.empty()) || Eligible() != queue_.end(); });
                if (queue_.empty() && stopping_) return;
                auto it = Eligible(); task = std::move(*it); queue_.erase(it);
                started = std::chrono::steady_clock::now();
                stats_.queue_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(started - task.enqueued).count();
                ++active_[task.key]; ++stats_.active;
                stats_.peak_active = std::max(stats_.peak_active, stats_.active);
            }
            // Callers own completion/error delivery; tasks must not leak exceptions.
            try { task.run(); } catch (...) { std::terminate(); }
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (--active_[task.key] == 0) active_.erase(task.key);
                stats_.execution_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started).count();
                --stats_.active; stats_.bytes -= task.bytes; ++stats_.completed;
                cv_.notify_all();
            }
        }
    }
    ExecutorOptions options_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    bool stopping_{false};
    ExecutorStats stats_;
    std::unordered_map<std::string, size_t> active_;
    std::deque<Task> queue_;
    std::vector<std::thread> threads_;
};
}
