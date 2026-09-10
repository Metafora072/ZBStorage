#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <sys/types.h>

namespace zb::client::metrics {

using SteadyClock = std::chrono::steady_clock;
enum class RpcTarget { Mds, DataNode };
enum class StorageTier { Unknown, Disk, Optical };

struct IoLatencyRecord {
    uint64_t request_id{0};
    uint64_t start_time_unix_us{0};
    int64_t pid{0};
    bool write{false};
    uint64_t inode_id{0};
    int64_t offset{0};
    uint64_t requested_bytes{0};
    int ret{0};
    StorageTier storage_tier{StorageTier::Unknown};
    uint64_t mds_ns{0};
    uint64_t data_node_ns{0};
    uint64_t total_ns{0};
};

struct IoTraceContext {
    IoLatencyRecord record;
};

// Scope must surround only the synchronous stub call, not its wrapper method.
class RpcLatencyScope {
public:
    RpcLatencyScope(IoTraceContext* context, RpcTarget target) noexcept
        : context_(context), target_(target) {
        if (context_) begin_ = SteadyClock::now();
    }
    ~RpcLatencyScope() noexcept {
        if (!context_) return;
        const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            SteadyClock::now() - begin_).count();
        auto& total = target_ == RpcTarget::Mds ? context_->record.mds_ns : context_->record.data_node_ns;
        total += static_cast<uint64_t>(ns);
    }
    RpcLatencyScope(const RpcLatencyScope&) = delete;
    RpcLatencyScope& operator=(const RpcLatencyScope&) = delete;
private:
    IoTraceContext* context_;
    RpcTarget target_;
    SteadyClock::time_point begin_{};
};

struct OutputDirectory {
    std::string path;
    std::string source;
};

bool ResolveOutputDirectory(const std::string& explicit_dir, const std::string& base_conf,
                            const std::string& repo_root, OutputDirectory* output, std::string* error);
bool ValidateOutputDirectory(const std::string& directory, const std::string& mount_point,
                             std::string* normalized, std::string* error);

struct RecorderOptions {
    uint32_t flush_ms{1000};
    size_t batch_records{1024};
    size_t queue_capacity{65536};
};

// Defaults use POSIX I/O. Tests can inject short writes and sync failures.
struct RecorderIo {
    std::function<ssize_t(int, const void*, size_t)> write;
    std::function<int(int)> sync;
};

struct RecorderStats {
    uint64_t submitted{0};
    uint64_t written{0};
    uint64_t synced{0};
    uint64_t dropped{0};
    bool failed{false};
};

class LatencyRecorder {
public:
    explicit LatencyRecorder(RecorderIo io = {});
    ~LatencyRecorder();
    LatencyRecorder(const LatencyRecorder&) = delete;
    LatencyRecorder& operator=(const LatencyRecorder&) = delete;

    // Prepare before FUSE mounts/daemonizes; Start its thread from FUSE init.
    bool Prepare(const std::string& directory, const std::string& mount_point,
                 const RecorderOptions& options, std::string* error);
    bool Start(std::string* error);
    void Submit(const IoLatencyRecord& record) noexcept;
    void Stop() noexcept; // Lifecycle calls are serialized by the owner; idempotent.
    uint64_t NextRequestId() noexcept { return next_id_.fetch_add(1, std::memory_order_relaxed); }
    RecorderStats Stats() const noexcept;
    const std::string& FilePath() const { return file_path_; }
private:
    bool WriteAll(const std::string& data, size_t* completed_bytes);
    bool Sync();
    void Run() noexcept;
    void Fail(size_t unwritten, const char* reason) noexcept;

    RecorderIo io_;
    RecorderOptions options_;
    std::string directory_;
    std::string file_path_;
    int fd_{-1};
    bool prepared_{false};
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<IoLatencyRecord> queue_;
    size_t head_{0};
    size_t size_{0};
    bool accepting_{false};
    bool stopping_{false};
    std::thread worker_;
    std::atomic<uint64_t> next_id_{1};
    std::atomic<uint64_t> submitted_{0}, written_{0}, synced_{0}, dropped_{0};
    std::atomic<bool> failed_{false};
};

class RequestTraceScope {
public:
    RequestTraceScope(LatencyRecorder& recorder, bool write, int64_t offset, uint64_t size) noexcept;
    ~RequestTraceScope() noexcept;
    RequestTraceScope(const RequestTraceScope&) = delete;
    RequestTraceScope& operator=(const RequestTraceScope&) = delete;
    IoTraceContext* Context() { return &context_; }
    int Finish(int ret) noexcept;
private:
    LatencyRecorder& recorder_;
    SteadyClock::time_point begin_;
    IoTraceContext context_;
    bool finished_{false};
};

} // namespace zb::client::metrics
