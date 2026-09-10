#include "client/metrics/IoLatency.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <locale>
#include <sstream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>

namespace zb::client::metrics {
namespace fs = std::filesystem;
namespace {
constexpr const char* kHeader =
    "schema_version,request_id,start_time_unix_us,pid,op,inode_id,offset,requested_bytes,"
    "returned_bytes,ret,storage_tier,mds_us,data_node_us,total_us\n";
uint64_t UnixMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
std::string Trim(const std::string& s) {
    const auto first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    return s.substr(first, s.find_last_not_of(" \t\r\n") - first + 1);
}
bool Error(std::string* error, const std::string& text) {
    if (error) *error = text;
    return false;
}
void Micros(std::ostream& out, uint64_t ns) {
    out << ns / 1000 << '.' << std::setw(3) << std::setfill('0') << ns % 1000;
}
std::string Encode(const std::vector<IoLatencyRecord>& batch) {
    std::ostringstream out;
    out.imbue(std::locale::classic());
    out.exceptions(std::ios::badbit | std::ios::failbit);
    for (const auto& r : batch) {
        const char* tier = r.storage_tier == StorageTier::Disk ? "disk" :
                           r.storage_tier == StorageTier::Optical ? "optical" : "unknown";
        out << "1," << r.request_id << ',' << r.start_time_unix_us << ',' << r.pid << ','
            << (r.write ? "write" : "read") << ',' << r.inode_id << ',' << r.offset << ','
            << r.requested_bytes << ',' << std::max(r.ret, 0) << ',' << r.ret << ',' << tier << ',';
        Micros(out, r.mds_ns); out << ',';
        Micros(out, r.data_node_ns); out << ',';
        Micros(out, r.total_ns); out << '\n';
    }
    return out.str();
}
} // namespace

bool ResolveOutputDirectory(const std::string& explicit_dir, const std::string& base_conf,
                            const std::string& repo_root, OutputDirectory* output, std::string* error) {
    try {
        fs::path dir;
        if (!explicit_dir.empty()) {
            dir = explicit_dir;
            output->source = "explicit";
        } else {
            std::string root;
            const fs::path config = base_conf.empty() ? fs::path(repo_root) / "config/base.conf" : fs::path(base_conf);
            // status errors (e.g. permissions) propagate; only absence is a fallback.
            if (fs::exists(config)) {
                if (!fs::is_regular_file(config)) return Error(error, "base config is not a regular file: " + config.string());
                std::ifstream input(config);
                if (!input) return Error(error, "cannot read base config: " + config.string());
                std::string line;
                while (std::getline(input, line)) {
                    line = Trim(line.substr(0, line.find('#')));
                    if (line.compare(0, 10, "ROOT_PATH=") == 0) {
                        root = Trim(line.substr(10));
                        break;
                    }
                }
                if (input.bad()) return Error(error, "failed reading base config: " + config.string());
            }
            if (root.empty()) {
                dir = fs::path(repo_root) / ".demo_run";
                output->source = "fallback: base config missing or ROOT_PATH empty";
            } else {
                dir = fs::path(root).is_absolute() ? fs::path(root) : fs::path(repo_root) / root;
                output->source = "base config: " + config.string();
            }
            dir /= "client/metrics";
        }
        output->path = fs::absolute(dir).lexically_normal().string();
        return true;
    } catch (const std::exception& e) { return Error(error, e.what()); }
}

bool ValidateOutputDirectory(const std::string& directory, const std::string& mount_point,
                             std::string* normalized, std::string* error) {
    try {
        const auto dir = fs::weakly_canonical(fs::absolute(directory));
        if (!mount_point.empty()) {
            const auto mount = fs::weakly_canonical(fs::absolute(mount_point));
            auto d = dir.begin();
            auto m = mount.begin();
            for (; d != dir.end() && m != mount.end() && *d == *m; ++d, ++m) {}
            if (m == mount.end()) return Error(error, "latency directory must be outside FUSE mount: " + dir.string());
        }
        *normalized = dir.string();
        return true;
    } catch (const std::exception& e) { return Error(error, e.what()); }
}

LatencyRecorder::LatencyRecorder(RecorderIo io) : io_(std::move(io)) {
    if (!io_.write) io_.write = [](int fd, const void* buf, size_t size) { return ::write(fd, buf, size); };
    if (!io_.sync) io_.sync = [](int fd) { return ::fdatasync(fd); };
}
LatencyRecorder::~LatencyRecorder() { Stop(); }

bool LatencyRecorder::Prepare(const std::string& directory, const std::string& mount_point,
                              const RecorderOptions& options, std::string* error) {
    if (prepared_) return Error(error, "recorder already prepared");
    if (directory.empty() || options.flush_ms == 0 || options.batch_records == 0 ||
        options.queue_capacity == 0 || options.batch_records > options.queue_capacity) {
        return Error(error, "invalid latency options (positive interval and 0 < batch <= capacity required)");
    }
    if (!ValidateOutputDirectory(directory, mount_point, &directory_, error)) return false;
    try {
        fs::create_directories(directory_);
        if (!fs::is_directory(directory_) || ::access(directory_.c_str(), W_OK | X_OK) != 0)
            return Error(error, "latency directory is not writable: " + directory_);
        queue_.resize(options.queue_capacity);
        options_ = options;
        prepared_ = true;
        return true;
    } catch (const std::exception& e) { return Error(error, e.what()); }
}

bool LatencyRecorder::WriteAll(const std::string& data, size_t* completed) {
    *completed = 0;
    while (*completed < data.size()) {
        const auto n = io_.write(fd_, data.data() + *completed, data.size() - *completed);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { if (n == 0) errno = EIO; return false; }
        *completed += static_cast<size_t>(n);
    }
    return true;
}
bool LatencyRecorder::Sync() {
    int rc;
    do { rc = io_.sync(fd_); } while (rc < 0 && errno == EINTR);
    if (rc != 0) return false;
    synced_.store(written_.load());
    return true;
}

bool LatencyRecorder::Start(std::string* error) {
    if (!prepared_ || fd_ >= 0 || stopping_) return Error(error, "recorder not prepared or already started/stopped");
    try {
        auto name = directory_ + "/io_latency_" + std::to_string(UnixMicros()) + "_" +
                    std::to_string(::getpid()) + "_XXXXXX.csv";
        std::vector<char> path(name.begin(), name.end());
        path.push_back('\0');
        fd_ = ::mkstemps(path.data(), 4);
        if (fd_ < 0) return Error(error, "cannot create latency file: " + std::string(std::strerror(errno)));
        ::fcntl(fd_, F_SETFD, FD_CLOEXEC);
        file_path_ = path.data();
        size_t completed;
        if (!WriteAll(kHeader, &completed) || !Sync()) throw std::runtime_error("cannot write/sync CSV header");
        const int dir_fd = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        if (dir_fd < 0) throw std::runtime_error("cannot open latency directory for sync");
        int rc;
        do { rc = ::fsync(dir_fd); } while (rc < 0 && errno == EINTR);
        ::close(dir_fd);
        if (rc != 0) throw std::runtime_error("cannot sync latency directory");
        accepting_ = true;
        worker_ = std::thread(&LatencyRecorder::Run, this);
        std::fprintf(stderr, "[io_latency] file=%s\n", file_path_.c_str());
        return true;
    } catch (const std::exception& e) {
        accepting_ = false;
        failed_ = true;
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }
        return Error(error, std::string(e.what()) + ": " + std::strerror(errno));
    }
}

void LatencyRecorder::Submit(const IoLatencyRecord& record) noexcept {
    submitted_.fetch_add(1, std::memory_order_relaxed);
    bool dropped = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!accepting_ || size_ == queue_.size()) {
            dropped = true;
        } else {
            queue_[(head_ + size_) % queue_.size()] = record;
            ++size_;
        }
    }
    if (dropped) {
        const auto count = dropped_.fetch_add(1) + 1;
        if ((count & (count - 1)) == 0)
            std::fprintf(stderr, "[io_latency] records dropped=%llu (queue full or recorder unavailable)\n",
                         static_cast<unsigned long long>(count));
    } else cv_.notify_one();
}

void LatencyRecorder::Fail(size_t unwritten, const char* reason) noexcept {
    failed_ = true;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        accepting_ = false;
        dropped_.fetch_add(size_ + unwritten);
        size_ = 0;
    }
    std::fprintf(stderr, "[io_latency] recording disabled: %s; file=%s; errno=%d (%s)\n",
                 reason, file_path_.c_str(), errno, std::strerror(errno));
}

void LatencyRecorder::Run() noexcept {
    size_t in_flight = 0;
    try {
        std::vector<IoLatencyRecord> batch;
        batch.reserve(options_.batch_records);
        auto deadline = SteadyClock::now() + std::chrono::milliseconds(options_.flush_ms);
        for (;;) {
            bool stop;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_until(lock, deadline, [this] { return stopping_ || size_ >= options_.batch_records; });
                const auto n = std::min(size_, options_.batch_records);
                batch.clear();
                for (size_t i = 0; i < n; ++i) batch.push_back(queue_[(head_ + i) % queue_.size()]);
                head_ = (head_ + n) % queue_.size();
                size_ -= n;
                stop = stopping_ && size_ == 0;
            }
            in_flight = batch.size();
            if (!batch.empty()) {
                const auto encoded = Encode(batch);
                size_t completed;
                if (!WriteAll(encoded, &completed)) {
                    const auto lines = std::count(encoded.begin(), encoded.begin() + completed, '\n');
                    written_.fetch_add(lines);
                    Fail(in_flight - lines, "CSV write failed");
                    return;
                }
                written_.fetch_add(in_flight);
                in_flight = 0;
            }
            if (stop || SteadyClock::now() >= deadline) {
                if (written_.load() != synced_.load() && !Sync()) {
                    Fail(0, "CSV sync failed; written records may not be durable");
                    return;
                }
                deadline = SteadyClock::now() + std::chrono::milliseconds(options_.flush_ms);
            }
            if (stop) return;
        }
    } catch (const std::exception& e) { Fail(in_flight, e.what()); }
    catch (...) { Fail(in_flight, "unexpected recorder error"); }
}

void LatencyRecorder::Stop() noexcept {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        accepting_ = false;
        stopping_ = true;
    }
    cv_.notify_all();
    if (worker_.joinable()) worker_.join();
    if (fd_ >= 0) {
        if (::close(fd_) != 0) failed_ = true;
        fd_ = -1;
        const auto s = Stats();
        std::fprintf(stderr, "[io_latency] submitted=%llu written=%llu synced=%llu dropped=%llu failed=%d\n",
            static_cast<unsigned long long>(s.submitted), static_cast<unsigned long long>(s.written),
            static_cast<unsigned long long>(s.synced), static_cast<unsigned long long>(s.dropped), s.failed);
    }
}
RecorderStats LatencyRecorder::Stats() const noexcept {
    return {submitted_.load(), written_.load(), synced_.load(), dropped_.load(), failed_.load()};
}

RequestTraceScope::RequestTraceScope(LatencyRecorder& recorder, bool write, int64_t offset, uint64_t size) noexcept
    : recorder_(recorder), begin_(SteadyClock::now()) {
    context_.record.request_id = recorder_.NextRequestId();
    context_.record.start_time_unix_us = UnixMicros();
    context_.record.pid = ::getpid();
    context_.record.write = write;
    context_.record.offset = offset;
    context_.record.requested_bytes = size;
}
RequestTraceScope::~RequestTraceScope() noexcept { if (!finished_) Finish(-EIO); }
int RequestTraceScope::Finish(int ret) noexcept {
    if (!finished_) {
        context_.record.total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(SteadyClock::now() - begin_).count();
        context_.record.ret = ret;
        finished_ = true;
        recorder_.Submit(context_.record);
    }
    return ret;
}
} // namespace zb::client::metrics
