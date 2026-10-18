#pragma once
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <vector>

namespace zb::client {
struct BatchIoTask {
    std::string request_id;
    bool write{false};
    std::string path;          // Existing file in the mounted filesystem.
    uint64_t offset{0}, size{0};
    std::string local_path;    // Write source, or read destination; distinct from path.
    std::vector<std::string> after; // Earlier request IDs, successful completion required.
};
struct BatchIoResult {
    std::string request_id;
    int error{0};
    uint64_t bytes{0}, elapsed_us{0};
    bool skipped{false}, may_have_written{false};
};
struct BatchIoOptions {
    size_t max_parallel_files{4}, queue_depth{256}, max_batches{8};
    uint64_t max_buffer_bytes{16ULL * 1024 * 1024}, chunk_bytes{65536};
};
class BatchIoClient {
public:
    using Handle = std::shared_future<std::vector<BatchIoResult>>;
    using Callback = std::function<void(const BatchIoResult&)>;
    explicit BatchIoClient(BatchIoOptions options = {});
    ~BatchIoClient();
    // Callback runs on the batch coordinator; must not wait on its own batch.
    Handle submit_batch(std::vector<BatchIoTask> tasks, Callback callback = {});
    static std::vector<BatchIoResult> wait_batch(const Handle& handle) { return handle.get(); }
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
}
