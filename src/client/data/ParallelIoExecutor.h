#pragma once
#include "common/concurrency/BoundedExecutor.h"
#include "common/concurrency/KeyedLocks.h"
#include "client/metrics/IoLatency.h"
#include <cerrno>
#include <cstdio>
#include <future>

namespace zb::client::data {
class ParallelIoExecutor {
public:
    explicit ParallelIoExecutor(concurrency::ExecutorOptions options = {}) : pool_(options) {}
    ~ParallelIoExecutor() {
        pool_.Stop();
        const auto s = pool_.Stats();
        std::fprintf(stderr, "[client_io] peak_active=%zu completed=%llu rejected=%llu queue_us=%llu execution_us=%llu\n", s.peak_active,
            (unsigned long long)s.completed, (unsigned long long)s.rejected, (unsigned long long)(s.queue_ns / 1000),
            (unsigned long long)(s.execution_ns / 1000));
    }
    // fn(index, private trace) returns errno; join all accepted work even on error.
    int Run(const std::string& node, const std::vector<uint64_t>& lengths,
            const std::function<int(size_t, metrics::IoTraceContext*)>& fn,
            metrics::IoTraceContext* parent) {
        struct Result { int error{0}; metrics::IoTraceContext trace; };
        std::vector<std::future<Result>> futures;
        futures.reserve(lengths.size());
        struct Drain { std::vector<std::future<Result>>& fs; ~Drain() {
            for (auto& f : fs) if (f.valid()) f.wait();
        }} drain{futures};
        int error = 0;
        for (size_t i = 0; i < lengths.size(); ++i) {
            auto task = std::make_shared<std::packaged_task<Result()>>([&, i] {
                Result r;
                try { r.error = fn(i, &r.trace); }
                catch (...) { r.error = EIO; }
                return r;
            });
            auto future = task->get_future();
            try {
                // Budget request, response and the task's intermediate copy.
                if (lengths[i] > UINT64_MAX / 3 || !pool_.Submit(node, lengths[i] * 3, [task] { (*task)(); })) {
                    error = EAGAIN; break;
                }
                futures.push_back(std::move(future));
            } catch (...) { error = EIO; break; }
        }
        for (auto& future : futures) {
            const auto r = future.get();
            if (!error && r.error) error = r.error;
            if (parent) {
                parent->record.mds_ns += r.trace.record.mds_ns;
                parent->record.data_node_ns += r.trace.record.data_node_ns;
            }
        }
        return error;
    }
private:
    concurrency::BoundedExecutor pool_;
};
}
