#include "fuse_executor.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <linux/magic.h>
#include <mutex>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <sys/statfs.h>
#include <thread>
#include <unistd.h>

#ifndef FUSE_SUPER_MAGIC
#define FUSE_SUPER_MAGIC 0x65735546
#endif

namespace zb::time_simulator {
namespace {

using Clock = std::chrono::steady_clock;
constexpr std::size_t kIoBufferBytes = 1024 * 1024;

std::string CsvEscape(const std::string& value) {
    if (value.find_first_of(",\"\n\r") == std::string::npos) {
        return value;
    }
    std::string escaped = "\"";
    for (const char c : value) {
        if (c == '"') {
            escaped += "\"\"";
        } else {
            escaped += c;
        }
    }
    escaped += '"';
    return escaped;
}

std::string MakeOperationId(
    std::size_t step_id,
    const std::string& type,
    std::uint64_t sequence) {
    std::ostringstream out;
    out << "step_" << std::setw(4) << std::setfill('0') << step_id
        << '_' << type << '_' << std::setw(8) << sequence;
    return out.str();
}

std::string NormalizeLogicalRoot(std::string root) {
    if (root.empty() || root.front() != '/') {
        throw std::invalid_argument("simulation.namespace_root must start with '/'");
    }
    while (root.size() > 1 && root.back() == '/') {
        root.pop_back();
    }
    if (root.find("..") != std::string::npos) {
        throw std::invalid_argument("simulation.namespace_root must not contain '..'");
    }
    return root;
}

std::string JoinLogicalPath(const std::string& root, const std::string& child) {
    std::string result = NormalizeLogicalRoot(root);
    if (!child.empty()) {
        result += '/';
        result += child.front() == '/' ? child.substr(1) : child;
    }
    return result;
}

void FillPattern(
    char* data,
    std::size_t length,
    std::uint64_t seed,
    std::uint64_t absolute_offset) {
    for (std::size_t i = 0; i < length; ++i) {
        const std::uint64_t value = seed + (absolute_offset + i) * 131ULL;
        data[i] = static_cast<char>(value % 251ULL);
    }
}

void WriteWholeFile(
    int fd,
    std::uint64_t size_bytes,
    std::uint64_t pattern_seed) {
    std::vector<char> buffer(kIoBufferBytes);
    std::uint64_t offset = 0;
    while (offset < size_bytes) {
        const std::size_t chunk = static_cast<std::size_t>(
            std::min<std::uint64_t>(buffer.size(), size_bytes - offset));
        FillPattern(buffer.data(), chunk, pattern_seed, offset);

        std::size_t completed = 0;
        while (completed < chunk) {
            const ssize_t written = ::pwrite(
                fd,
                buffer.data() + completed,
                chunk - completed,
                static_cast<off_t>(offset + completed));
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "pwrite through ZBStorage FUSE failed");
            }
            if (written == 0) {
                throw std::runtime_error("pwrite returned 0 before the request completed");
            }
            completed += static_cast<std::size_t>(written);
        }
        offset += chunk;
    }
}

void WritePatternRange(
    int fd,
    std::uint64_t offset,
    std::uint64_t size_bytes,
    std::uint64_t pattern_seed) {
    std::vector<char> buffer(kIoBufferBytes);
    std::uint64_t completed_total = 0;
    while (completed_total < size_bytes) {
        const std::size_t chunk = static_cast<std::size_t>(
            std::min<std::uint64_t>(buffer.size(), size_bytes - completed_total));
        const std::uint64_t absolute_offset = offset + completed_total;
        FillPattern(buffer.data(), chunk, pattern_seed, absolute_offset);

        std::size_t completed = 0;
        while (completed < chunk) {
            const ssize_t written = ::pwrite(
                fd,
                buffer.data() + completed,
                chunk - completed,
                static_cast<off_t>(absolute_offset + completed));
            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "trace pwrite through ZBStorage FUSE failed");
            }
            if (written == 0) {
                throw std::runtime_error(
                    "trace pwrite returned 0 before the request completed");
            }
            completed += static_cast<std::size_t>(written);
        }
        completed_total += chunk;
    }
}

void ReadAndVerify(
    int fd,
    std::uint64_t offset,
    std::uint64_t size_bytes,
    std::uint64_t pattern_seed) {
    std::vector<char> actual(kIoBufferBytes);
    std::vector<char> expected(kIoBufferBytes);
    std::uint64_t completed_total = 0;

    while (completed_total < size_bytes) {
        const std::size_t chunk = static_cast<std::size_t>(
            std::min<std::uint64_t>(actual.size(), size_bytes - completed_total));
        std::size_t completed = 0;
        while (completed < chunk) {
            const ssize_t read_size = ::pread(
                fd,
                actual.data() + completed,
                chunk - completed,
                static_cast<off_t>(offset + completed_total + completed));
            if (read_size < 0) {
                if (errno == EINTR) {
                    continue;
                }
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "pread through ZBStorage FUSE failed");
            }
            if (read_size == 0) {
                throw std::runtime_error("unexpected EOF while reading through ZBStorage FUSE");
            }
            completed += static_cast<std::size_t>(read_size);
        }

        FillPattern(
            expected.data(),
            chunk,
            pattern_seed,
            offset + completed_total);
        if (!std::equal(actual.begin(), actual.begin() + chunk, expected.begin())) {
            throw std::runtime_error("read verification failed: data returned by ZBStorage differs");
        }
        completed_total += chunk;
    }
}

template <typename Function>
void ParallelFor(std::size_t count, std::size_t concurrency, Function function) {
    if (count == 0) {
        return;
    }
    const std::size_t worker_count = std::max<std::size_t>(
        1,
        std::min<std::size_t>(count, concurrency));
    std::atomic<std::size_t> next{0};
    std::vector<std::thread> workers;
    workers.reserve(worker_count);
    for (std::size_t worker_id = 0; worker_id < worker_count; ++worker_id) {
        workers.emplace_back([&, worker_id] {
            while (true) {
                const std::size_t index = next.fetch_add(1);
                if (index >= count) {
                    break;
                }
                function(index, worker_id);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
}

double Percentile(std::vector<double> values, double p) {
    if (values.empty()) {
        return 0.0;
    }
    std::sort(values.begin(), values.end());
    const double position = p * static_cast<double>(values.size() - 1);
    const std::size_t lower = static_cast<std::size_t>(std::floor(position));
    const std::size_t upper = static_cast<std::size_t>(std::ceil(position));
    if (lower == upper) {
        return values[lower];
    }
    const double weight = position - static_cast<double>(lower);
    return values[lower] * (1.0 - weight) + values[upper] * weight;
}

}  // namespace

FuseExecutor::FuseExecutor(const SimulatorConfig& config)
    : config_(config) {}

void FuseExecutor::ValidateMountPoint() const {
    const auto& mount_point = config_.simulation.mount_point;
    if (!std::filesystem::exists(mount_point)) {
        throw std::runtime_error(
            "ZBStorage mount point does not exist: " + mount_point.string());
    }
    if (!std::filesystem::is_directory(mount_point)) {
        throw std::runtime_error(
            "ZBStorage mount point is not a directory: " + mount_point.string());
    }

    struct statfs file_system_info {};
    if (::statfs(mount_point.c_str(), &file_system_info) != 0) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "statfs failed for ZBStorage mount point");
    }
    if (static_cast<unsigned long>(file_system_info.f_type) !=
        static_cast<unsigned long>(FUSE_SUPER_MAGIC)) {
        std::ostringstream message;
        message << "mount_point is not a live FUSE mount: " << mount_point
                << ", filesystem magic=0x" << std::hex << file_system_info.f_type
                << ". Start scripts/start_demo_stack.sh first.";
        throw std::runtime_error(message.str());
    }

    const std::string root = NormalizeLogicalRoot(config_.simulation.namespace_root);
    if (root != "/real" && root.rfind("/real/", 0) != 0 &&
        root != "/virtual" && root.rfind("/virtual/", 0) != 0) {
        throw std::runtime_error(
            "simulation.namespace_root must be under /real or /virtual, "
            "because ZBStorage creates these tier roots");
    }
}

std::filesystem::path FuseExecutor::ToPhysicalPath(
    const std::string& logical_path) const {
    if (logical_path.empty() || logical_path.front() != '/') {
        throw std::invalid_argument("logical path must start with '/'");
    }
    if (logical_path.find("..") != std::string::npos) {
        throw std::invalid_argument("logical path must not contain '..'");
    }
    std::string relative = logical_path;
    while (!relative.empty() && relative.front() == '/') {
        relative.erase(relative.begin());
    }
    return config_.simulation.mount_point / relative;
}

void FuseExecutor::Run() {
    ValidateMountPoint();
    std::filesystem::create_directories(config_.simulation.output_dir);

    const std::filesystem::path summary_path =
        config_.simulation.output_dir / "step_summary.csv";
    {
        std::ofstream summary(summary_path, std::ios::trunc);
        if (!summary) {
            throw std::runtime_error("cannot create summary CSV: " + summary_path.string());
        }
        summary << "step_id,operation_count,success_count,failed_count,"
                   "written_bytes,read_bytes,average_latency_ms,p95_latency_ms,p99_latency_ms,"
                   "average_queue_wait_ms,p95_queue_wait_ms,p99_queue_wait_ms,"
                   "average_response_time_ms,p95_response_time_ms,p99_response_time_ms\n";
    }

    std::cout << "[FUSE] verified ZBStorage mount: "
              << config_.simulation.mount_point << '\n'
              << "[FUSE] namespace root: "
              << config_.simulation.namespace_root << '\n';

    for (std::size_t step_id = 1;
         step_id <= config_.simulation.total_steps;
         ++step_id) {
        std::vector<OperationRecord> records = RunStep(step_id);
        WriteStepCsv(step_id, records);
        const StepExecutionSummary summary = Summarize(step_id, records);
        AppendSummaryCsv(summary);

        std::cout << "[STEP " << step_id << "] operations="
                  << summary.operation_count
                  << " success=" << summary.success_count
                  << " failed=" << summary.failed_count
                  << " write_bytes=" << summary.written_bytes
                  << " read_bytes=" << summary.read_bytes
                  << " p99_ms=" << summary.p99_latency_ms
                  << " avg_queue_wait_ms=" << summary.average_queue_wait_ms
                  << " p99_response_ms=" << summary.p99_response_time_ms
                  << '\n';
    }
}

std::vector<OperationRecord> FuseExecutor::RunStep(std::size_t step_id) {
    if (config_.trace.enabled) {
        return RunTraceStep(step_id);
    }

    std::mt19937_64 rng(
        config_.simulation.random_seed ^
        (0x9E3779B97F4A7C15ULL * static_cast<std::uint64_t>(step_id)));

    std::ostringstream step_name;
    step_name << "step_" << std::setw(4) << std::setfill('0') << step_id;
    const std::string step_root = JoinLogicalPath(
        config_.simulation.namespace_root,
        step_name.str());
    const std::filesystem::path step_physical = ToPhysicalPath(step_root);

    std::vector<OperationRecord> records;
    std::mutex records_mutex;
    std::atomic<std::uint64_t> sequence{1};

    auto append_record = [&](OperationRecord record) {
        std::lock_guard<std::mutex> lock(records_mutex);
        records.push_back(std::move(record));
    };

    OperationRecord mkdir_record;
    mkdir_record.step_id = step_id;
    mkdir_record.operation_id = MakeOperationId(
        step_id,
        "mkdir",
        sequence.fetch_add(1));
    mkdir_record.operation_type = "mkdir";
    mkdir_record.logical_path = step_root;
    {
        const auto start = Clock::now();
        try {
            std::filesystem::create_directories(step_physical);
            mkdir_record.status = "success";
        } catch (const std::system_error& error) {
            mkdir_record.status = "failed";
            mkdir_record.error_code = error.code().value();
            mkdir_record.error_message = error.what();
        }
        mkdir_record.latency_ms =
            std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    }
    records.push_back(mkdir_record);
    if (mkdir_record.status != "success") {
        return records;
    }

    std::vector<std::string> bucket_names;
    std::vector<double> bucket_weights;
    for (const auto& [name, bucket] : config_.workload.file_size_distribution) {
        bucket_names.push_back(name);
        bucket_weights.push_back(bucket.ratio);
    }
    std::discrete_distribution<std::size_t> choose_bucket(
        bucket_weights.begin(),
        bucket_weights.end());

    struct WriteTask {
        FileInfo file;
        std::filesystem::path physical_path;
    };
    std::vector<WriteTask> write_tasks;
    write_tasks.reserve(static_cast<std::size_t>(config_.workload.writes_per_step));
    for (std::uint64_t index = 0;
         index < config_.workload.writes_per_step;
         ++index) {
        const std::string& bucket_name = bucket_names[choose_bucket(rng)];
        const FileSizeBucket& bucket =
            config_.workload.file_size_distribution.at(bucket_name);
        std::uniform_int_distribution<std::uint64_t> choose_size(
            bucket.min_bytes,
            bucket.max_bytes);

        std::ostringstream filename;
        filename << "file_" << std::setw(8) << std::setfill('0') << index << ".bin";
        FileInfo file;
        file.logical_path = JoinLogicalPath(step_root, filename.str());
        file.size_bytes = choose_size(rng);
        file.pattern_seed = rng();
        write_tasks.push_back({file, ToPhysicalPath(file.logical_path)});
    }

    std::vector<bool> write_success(write_tasks.size(), false);
    ParallelFor(
        write_tasks.size(),
        config_.simulation.real_concurrency,
        [&](std::size_t index, std::size_t worker_id) {
            const WriteTask& task = write_tasks[index];
            OperationRecord create_record;
            create_record.step_id = step_id;
            create_record.operation_id = MakeOperationId(
                step_id,
                "create",
                sequence.fetch_add(1));
            create_record.operation_type = "create_file";
            create_record.logical_path = task.file.logical_path;
            create_record.worker_id = worker_id;

            int fd = -1;
            const auto create_start = Clock::now();
            try {
                fd = ::open(
                    task.physical_path.c_str(),
                    O_CREAT | O_TRUNC | O_RDWR,
                    0644);
                if (fd < 0) {
                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "open(O_CREAT) through ZBStorage FUSE failed");
                }
                create_record.status = "success";
            } catch (const std::system_error& error) {
                create_record.status = "failed";
                create_record.error_code = error.code().value();
                create_record.error_message = error.what();
            }
            create_record.latency_ms =
                std::chrono::duration<double, std::milli>(Clock::now() - create_start).count();
            append_record(std::move(create_record));

            OperationRecord write_record;
            write_record.step_id = step_id;
            write_record.operation_id = MakeOperationId(
                step_id,
                "write",
                sequence.fetch_add(1));
            write_record.operation_type = "write";
            write_record.logical_path = task.file.logical_path;
            write_record.size_bytes = task.file.size_bytes;
            write_record.worker_id = worker_id;
            const auto write_start = Clock::now();
            try {
                if (fd < 0) {
                    throw std::runtime_error("write skipped because create_file failed");
                }
                WriteWholeFile(fd, task.file.size_bytes, task.file.pattern_seed);
                if (config_.simulation.fsync_writes && ::fsync(fd) != 0) {
                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "fsync through ZBStorage FUSE failed");
                }
                if (::close(fd) != 0) {
                    fd = -1;
                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "close after ZBStorage write failed");
                }
                fd = -1;
                write_record.status = "success";
                write_success[index] = true;
            } catch (const std::system_error& error) {
                write_record.status = "failed";
                write_record.error_code = error.code().value();
                write_record.error_message = error.what();
            } catch (const std::exception& error) {
                write_record.status = "failed";
                write_record.error_code = -1;
                write_record.error_message = error.what();
            }
            if (fd >= 0) {
                ::close(fd);
            }
            write_record.latency_ms =
                std::chrono::duration<double, std::milli>(Clock::now() - write_start).count();
            append_record(std::move(write_record));
        });

    for (std::size_t index = 0; index < write_tasks.size(); ++index) {
        if (write_success[index]) {
            files_.push_back(write_tasks[index].file);
        }
    }

    struct ReadTask {
        FileInfo file;
        std::uint64_t offset = 0;
        std::uint64_t size_bytes = 0;
    };
    std::vector<ReadTask> read_tasks;
    if (!files_.empty()) {
        read_tasks.reserve(static_cast<std::size_t>(config_.workload.reads_per_step));
        std::uniform_int_distribution<std::size_t> choose_file(0, files_.size() - 1);
        for (std::uint64_t index = 0;
             index < config_.workload.reads_per_step;
             ++index) {
            const FileInfo& file = files_[choose_file(rng)];
            const std::uint64_t size = std::min(
                config_.workload.read_size_bytes,
                file.size_bytes);
            std::uint64_t offset = 0;
            if (file.size_bytes > size) {
                std::uniform_int_distribution<std::uint64_t> choose_offset(
                    0,
                    file.size_bytes - size);
                offset = choose_offset(rng);
            }
            read_tasks.push_back({file, offset, size});
        }
    }

    ParallelFor(
        read_tasks.size(),
        config_.simulation.real_concurrency,
        [&](std::size_t index, std::size_t worker_id) {
            const ReadTask& task = read_tasks[index];
            OperationRecord read_record;
            read_record.step_id = step_id;
            read_record.operation_id = MakeOperationId(
                step_id,
                "read",
                sequence.fetch_add(1));
            read_record.operation_type = "read";
            read_record.logical_path = task.file.logical_path;
            read_record.offset = task.offset;
            read_record.size_bytes = task.size_bytes;
            read_record.worker_id = worker_id;

            int fd = -1;
            const auto start = Clock::now();
            try {
                const std::filesystem::path physical =
                    ToPhysicalPath(task.file.logical_path);
                fd = ::open(physical.c_str(), O_RDONLY);
                if (fd < 0) {
                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "open(O_RDONLY) through ZBStorage FUSE failed");
                }
                ReadAndVerify(
                    fd,
                    task.offset,
                    task.size_bytes,
                    task.file.pattern_seed);
                if (::close(fd) != 0) {
                    fd = -1;
                    throw std::system_error(
                        errno,
                        std::generic_category(),
                        "close after ZBStorage read failed");
                }
                fd = -1;
                read_record.status = "success";
            } catch (const std::system_error& error) {
                read_record.status = "failed";
                read_record.error_code = error.code().value();
                read_record.error_message = error.what();
            } catch (const std::exception& error) {
                read_record.status = "failed";
                read_record.error_code = -1;
                read_record.error_message = error.what();
            }
            if (fd >= 0) {
                ::close(fd);
            }
            read_record.latency_ms =
                std::chrono::duration<double, std::milli>(Clock::now() - start).count();
            append_record(std::move(read_record));
        });

    for (std::uint64_t index = 0;
         index < config_.workload.readdirs_per_step;
         ++index) {
        OperationRecord readdir_record;
        readdir_record.step_id = step_id;
        readdir_record.operation_id = MakeOperationId(
            step_id,
            "readdir",
            sequence.fetch_add(1));
        readdir_record.operation_type = "readdir";
        readdir_record.logical_path = step_root;

        const auto start = Clock::now();
        DIR* directory = nullptr;
        try {
            directory = ::opendir(step_physical.c_str());
            if (directory == nullptr) {
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "opendir through ZBStorage FUSE failed");
            }
            errno = 0;
            while (::readdir(directory) != nullptr) {
            }
            if (errno != 0) {
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "readdir through ZBStorage FUSE failed");
            }
            if (::closedir(directory) != 0) {
                directory = nullptr;
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "closedir through ZBStorage FUSE failed");
            }
            directory = nullptr;
            readdir_record.status = "success";
        } catch (const std::system_error& error) {
            readdir_record.status = "failed";
            readdir_record.error_code = error.code().value();
            readdir_record.error_message = error.what();
        }
        if (directory != nullptr) {
            ::closedir(directory);
        }
        readdir_record.latency_ms =
            std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        records.push_back(std::move(readdir_record));
    }

    std::sort(
        records.begin(),
        records.end(),
        [](const OperationRecord& left, const OperationRecord& right) {
            return left.operation_id < right.operation_id;
        });
    return records;
}

std::vector<FuseExecutor::FileInfo> FuseExecutor::PrepareTraceFiles(
    std::size_t step_id,
    const std::string& step_root,
    std::mt19937_64& rng) const {
    const std::filesystem::path step_physical = ToPhysicalPath(step_root);
    try {
        std::filesystem::create_directories(step_physical);
    } catch (const std::system_error& error) {
        throw std::runtime_error(
            "trace setup failed to create step directory: " +
            std::string(error.what()));
    }

    std::vector<std::string> bucket_names;
    std::vector<double> bucket_weights;
    for (const auto& [name, bucket] : config_.workload.file_size_distribution) {
        bucket_names.push_back(name);
        bucket_weights.push_back(bucket.ratio);
    }
    std::discrete_distribution<std::size_t> choose_bucket(
        bucket_weights.begin(),
        bucket_weights.end());

    std::vector<FileInfo> files;
    files.reserve(static_cast<std::size_t>(config_.trace.file_count));

    for (std::uint64_t index = 0; index < config_.trace.file_count; ++index) {
        const std::string& bucket_name = bucket_names[choose_bucket(rng)];
        const FileSizeBucket& bucket =
            config_.workload.file_size_distribution.at(bucket_name);
        const std::uint64_t min_size =
            std::max(bucket.min_bytes, config_.trace.io_size_bytes);
        const std::uint64_t max_size = std::max(bucket.max_bytes, min_size);
        std::uniform_int_distribution<std::uint64_t> choose_size(min_size, max_size);

        std::ostringstream filename;
        filename << "trace_file_" << std::setw(8) << std::setfill('0')
                 << index << ".bin";

        FileInfo file;
        file.logical_path = JoinLogicalPath(step_root, filename.str());
        file.size_bytes = choose_size(rng);
        file.pattern_seed = rng();

        const std::filesystem::path physical = ToPhysicalPath(file.logical_path);
        int fd = ::open(physical.c_str(), O_CREAT | O_TRUNC | O_RDWR, 0644);
        if (fd < 0) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "trace setup open(O_CREAT) through ZBStorage FUSE failed");
        }
        try {
            WriteWholeFile(fd, file.size_bytes, file.pattern_seed);
            if (config_.simulation.fsync_writes && ::fsync(fd) != 0) {
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "trace setup fsync through ZBStorage FUSE failed");
            }
            if (::close(fd) != 0) {
                fd = -1;
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "trace setup close failed");
            }
            fd = -1;
        } catch (...) {
            if (fd >= 0) {
                ::close(fd);
            }
            throw;
        }
        files.push_back(std::move(file));
    }

    std::cout << "[TRACE] step " << step_id
              << " prepared_files=" << files.size() << '\n';
    return files;
}

OperationRecord FuseExecutor::ExecuteTraceRequest(
    std::size_t step_id,
    const TraceRequest& request,
    const FileInfo& file,
    const std::chrono::steady_clock::time_point& replay_start) const {
    OperationRecord record;
    record.step_id = step_id;
    record.operation_id = MakeOperationId(
        step_id,
        "trace",
        request.request_id);
    record.operation_type = ToString(request.operation_type);
    record.logical_path = request.logical_path;
    record.offset = request.offset;
    record.size_bytes = request.size_bytes;
    record.worker_id = 0;  // Trace V1 is strictly serial.
    record.scheduled_time_us = request.timestamp_us;

    const auto scheduled_time =
        replay_start + std::chrono::microseconds(request.timestamp_us);
    const auto now = Clock::now();
    if (now < scheduled_time) {
        std::this_thread::sleep_until(scheduled_time);
    }

    const auto start = Clock::now();
    record.start_time_us = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            start - replay_start).count());
    record.queue_wait_us = record.start_time_us > record.scheduled_time_us
        ? record.start_time_us - record.scheduled_time_us
        : 0;

    int fd = -1;
    try {
        const std::filesystem::path physical = ToPhysicalPath(request.logical_path);
        const int flags = request.operation_type == TraceOperationType::kRead
            ? O_RDONLY
            : O_RDWR;
        fd = ::open(physical.c_str(), flags);
        if (fd < 0) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "trace open through ZBStorage FUSE failed");
        }

        if (request.operation_type == TraceOperationType::kRead) {
            ReadAndVerify(
                fd,
                request.offset,
                request.size_bytes,
                file.pattern_seed);
        } else {
            // Write the same deterministic pattern used during setup. This keeps
            // later reads verifiable even when read/write requests are mixed.
            WritePatternRange(
                fd,
                request.offset,
                request.size_bytes,
                file.pattern_seed);
            if (config_.simulation.fsync_writes && ::fsync(fd) != 0) {
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "trace fsync through ZBStorage FUSE failed");
            }
        }

        if (::close(fd) != 0) {
            fd = -1;
            throw std::system_error(
                errno,
                std::generic_category(),
                "trace close through ZBStorage FUSE failed");
        }
        fd = -1;
        record.status = "success";
    } catch (const std::system_error& error) {
        record.status = "failed";
        record.error_code = error.code().value();
        record.error_message = error.what();
    } catch (const std::exception& error) {
        record.status = "failed";
        record.error_code = -1;
        record.error_message = error.what();
    }
    if (fd >= 0) {
        ::close(fd);
    }

    const auto finish = Clock::now();
    record.finish_time_us = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            finish - replay_start).count());
    record.latency_ms =
        std::chrono::duration<double, std::milli>(finish - start).count();
    record.response_time_us = record.finish_time_us >= record.scheduled_time_us
        ? record.finish_time_us - record.scheduled_time_us
        : 0;
    return record;
}

void FuseExecutor::WriteGeneratedTraceCsv(
    std::size_t step_id,
    const std::vector<TraceRequest>& trace) const {
    std::ostringstream filename;
    filename << "generated_trace_step_" << std::setw(4) << std::setfill('0')
             << step_id << ".csv";
    const std::filesystem::path path =
        config_.simulation.output_dir / filename.str();
    std::ofstream output(path, std::ios::trunc);
    if (!output) {
        throw std::runtime_error("cannot create generated trace CSV: " + path.string());
    }
    output << "request_id,timestamp_us,inter_arrival_us,operation_type,logical_path,offset,size_bytes\n";
    for (const TraceRequest& request : trace) {
        output << request.request_id << ','
               << request.timestamp_us << ','
               << request.inter_arrival_us << ','
               << CsvEscape(ToString(request.operation_type)) << ','
               << CsvEscape(request.logical_path) << ','
               << request.offset << ','
               << request.size_bytes << '\n';
    }
}

std::vector<OperationRecord> FuseExecutor::RunTraceStep(std::size_t step_id) {
    std::mt19937_64 rng(
        config_.simulation.random_seed ^
        (0xD1B54A32D192ED03ULL * static_cast<std::uint64_t>(step_id)));

    std::ostringstream step_name;
    step_name << "step_" << std::setw(4) << std::setfill('0') << step_id;
    const std::string step_root = JoinLogicalPath(
        config_.simulation.namespace_root,
        step_name.str());

    std::vector<FileInfo> trace_files =
        PrepareTraceFiles(step_id, step_root, rng);

    std::vector<TraceFileTarget> targets;
    targets.reserve(trace_files.size());
    for (const FileInfo& file : trace_files) {
        targets.push_back({file.logical_path, file.size_bytes});
    }

    TraceGenerationConfig generation;
    generation.total_operations = config_.trace.total_operations;
    generation.read_ratio = config_.trace.read_ratio;
    generation.write_ratio = config_.trace.write_ratio;
    generation.io_size_bytes = config_.trace.io_size_bytes;
    generation.request_rate_ops_per_sec = config_.trace.request_rate_ops_per_sec;
    generation.interval_distribution =
        ParseTraceIntervalDistribution(config_.trace.interval_distribution);

    TraceGenerator generator(
        config_.simulation.random_seed ^
        (0x94D049BB133111EBULL * static_cast<std::uint64_t>(step_id)));
    const std::vector<TraceRequest> trace = generator.Generate(generation, targets);
    WriteGeneratedTraceCsv(step_id, trace);

    const std::uint64_t expected_reads = static_cast<std::uint64_t>(std::llround(
        static_cast<double>(generation.total_operations) * generation.read_ratio));
    std::cout << "[TRACE] generated requests=" << trace.size()
              << " read=" << expected_reads
              << " write=" << (generation.total_operations - expected_reads)
              << " serial=true"
              << " request_rate_ops_per_sec=" << generation.request_rate_ops_per_sec
              << " interval_distribution="
              << ToString(generation.interval_distribution)
              << " logical_span_us="
              << (trace.empty() ? 0 : trace.back().timestamp_us)
              << '\n';

    std::vector<OperationRecord> records;
    records.reserve(trace.size());
    const auto replay_start = Clock::now();

    for (const TraceRequest& request : trace) {
        if (request.file_index >= trace_files.size()) {
            throw std::logic_error("generated trace file index is out of range");
        }
        records.push_back(ExecuteTraceRequest(
            step_id,
            request,
            trace_files[request.file_index],
            replay_start));
    }

    return records;
}

StepExecutionSummary FuseExecutor::Summarize(
    std::size_t step_id,
    const std::vector<OperationRecord>& records) const {
    StepExecutionSummary summary;
    summary.step_id = step_id;
    summary.operation_count = records.size();
    std::vector<double> latencies;
    latencies.reserve(records.size());
    std::vector<double> queue_waits_ms;
    std::vector<double> response_times_ms;
    if (config_.trace.enabled) {
        queue_waits_ms.reserve(records.size());
        response_times_ms.reserve(records.size());
    }

    for (const OperationRecord& record : records) {
        latencies.push_back(record.latency_ms);
        if (config_.trace.enabled) {
            queue_waits_ms.push_back(
                static_cast<double>(record.queue_wait_us) / 1000.0);
            response_times_ms.push_back(
                static_cast<double>(record.response_time_us) / 1000.0);
        }
        if (record.status == "success") {
            ++summary.success_count;
            if (record.operation_type == "write") {
                summary.written_bytes += record.size_bytes;
            } else if (record.operation_type == "read") {
                summary.read_bytes += record.size_bytes;
            }
        } else {
            ++summary.failed_count;
        }
    }
    if (!latencies.empty()) {
        summary.average_latency_ms =
            std::accumulate(latencies.begin(), latencies.end(), 0.0) /
            static_cast<double>(latencies.size());
        summary.p95_latency_ms = Percentile(latencies, 0.95);
        summary.p99_latency_ms = Percentile(latencies, 0.99);
    }
    if (!queue_waits_ms.empty()) {
        summary.average_queue_wait_ms =
            std::accumulate(queue_waits_ms.begin(), queue_waits_ms.end(), 0.0) /
            static_cast<double>(queue_waits_ms.size());
        summary.p95_queue_wait_ms = Percentile(queue_waits_ms, 0.95);
        summary.p99_queue_wait_ms = Percentile(queue_waits_ms, 0.99);
    }
    if (!response_times_ms.empty()) {
        summary.average_response_time_ms =
            std::accumulate(response_times_ms.begin(), response_times_ms.end(), 0.0) /
            static_cast<double>(response_times_ms.size());
        summary.p95_response_time_ms = Percentile(response_times_ms, 0.95);
        summary.p99_response_time_ms = Percentile(response_times_ms, 0.99);
    }
    return summary;
}

void FuseExecutor::WriteStepCsv(
    std::size_t step_id,
    const std::vector<OperationRecord>& records) const {
    std::ostringstream filename;
    filename << "operations_step_" << std::setw(4) << std::setfill('0')
             << step_id << ".csv";
    const std::filesystem::path path =
        config_.simulation.output_dir / filename.str();
    std::ofstream output(path, std::ios::trunc);
    if (!output) {
        throw std::runtime_error("cannot create operation CSV: " + path.string());
    }
    output << "step_id,operation_id,operation_type,logical_path,offset,size_bytes,"
              "latency_ms,status,error_code,error_message,worker_id,"
              "scheduled_time_us,start_time_us,finish_time_us,"
              "queue_wait_us,response_time_us\n";
    output << std::fixed << std::setprecision(3);
    for (const OperationRecord& record : records) {
        output << record.step_id << ','
               << CsvEscape(record.operation_id) << ','
               << CsvEscape(record.operation_type) << ','
               << CsvEscape(record.logical_path) << ','
               << record.offset << ','
               << record.size_bytes << ','
               << record.latency_ms << ','
               << CsvEscape(record.status) << ','
               << record.error_code << ','
               << CsvEscape(record.error_message) << ','
               << record.worker_id << ','
               << record.scheduled_time_us << ','
               << record.start_time_us << ','
               << record.finish_time_us << ','
               << record.queue_wait_us << ','
               << record.response_time_us << '\n';
    }
}

void FuseExecutor::AppendSummaryCsv(
    const StepExecutionSummary& summary) const {
    const std::filesystem::path path =
        config_.simulation.output_dir / "step_summary.csv";
    std::ofstream output(path, std::ios::app);
    if (!output) {
        throw std::runtime_error("cannot append summary CSV: " + path.string());
    }
    output << std::fixed << std::setprecision(3)
           << summary.step_id << ','
           << summary.operation_count << ','
           << summary.success_count << ','
           << summary.failed_count << ','
           << summary.written_bytes << ','
           << summary.read_bytes << ','
           << summary.average_latency_ms << ','
           << summary.p95_latency_ms << ','
           << summary.p99_latency_ms << ','
           << summary.average_queue_wait_ms << ','
           << summary.p95_queue_wait_ms << ','
           << summary.p99_queue_wait_ms << ','
           << summary.average_response_time_ms << ','
           << summary.p95_response_time_ms << ','
           << summary.p99_response_time_ms << '\n';
}

}  // namespace zb::time_simulator
