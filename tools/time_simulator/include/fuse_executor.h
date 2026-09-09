#pragma once

#include "time_simulator.h"
#include "trace_generator.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

namespace zb::time_simulator {

struct OperationRecord {
    std::size_t step_id = 0;
    std::string operation_id;
    std::string operation_type;
    std::string logical_path;
    std::uint64_t offset = 0;
    std::uint64_t size_bytes = 0;
    double latency_ms = 0.0;
    std::string status = "pending";
    int error_code = 0;
    std::string error_message;
    std::size_t worker_id = 0;
    std::uint64_t scheduled_time_us = 0;
    std::uint64_t start_time_us = 0;
    std::uint64_t finish_time_us = 0;
    // Time spent waiting after the logical arrival time before service starts.
    std::uint64_t queue_wait_us = 0;
    // End-to-end response time measured from logical arrival to completion.
    std::uint64_t response_time_us = 0;
};

struct StepExecutionSummary {
    std::size_t step_id = 0;
    std::uint64_t operation_count = 0;
    std::uint64_t success_count = 0;
    std::uint64_t failed_count = 0;
    std::uint64_t written_bytes = 0;
    std::uint64_t read_bytes = 0;
    double average_latency_ms = 0.0;
    double p95_latency_ms = 0.0;
    double p99_latency_ms = 0.0;
    double average_queue_wait_ms = 0.0;
    double p95_queue_wait_ms = 0.0;
    double p99_queue_wait_ms = 0.0;
    double average_response_time_ms = 0.0;
    double p95_response_time_ms = 0.0;
    double p99_response_time_ms = 0.0;
};

class FuseExecutor {
public:
    explicit FuseExecutor(const SimulatorConfig& config);

    // Executes all configured time steps through the mounted ZBStorage FUSE path.
    // Throws when mount_point is not a live FUSE mount.
    void Run();

private:
    struct FileInfo {
        std::string logical_path;
        std::uint64_t size_bytes = 0;
        std::uint64_t pattern_seed = 0;
    };

    void ValidateMountPoint() const;
    std::filesystem::path ToPhysicalPath(const std::string& logical_path) const;
    std::vector<OperationRecord> RunStep(std::size_t step_id);
    std::vector<OperationRecord> RunTraceStep(std::size_t step_id);
    std::vector<FileInfo> PrepareTraceFiles(
        std::size_t step_id,
        const std::string& step_root,
        std::mt19937_64& rng) const;
    OperationRecord ExecuteTraceRequest(
        std::size_t step_id,
        const TraceRequest& request,
        const FileInfo& file,
        const std::chrono::steady_clock::time_point& replay_start) const;
    void WriteGeneratedTraceCsv(
        std::size_t step_id,
        const std::vector<TraceRequest>& trace) const;
    StepExecutionSummary Summarize(
        std::size_t step_id,
        const std::vector<OperationRecord>& records) const;
    void WriteStepCsv(
        std::size_t step_id,
        const std::vector<OperationRecord>& records) const;
    void AppendSummaryCsv(const StepExecutionSummary& summary) const;

    const SimulatorConfig& config_;
    std::vector<FileInfo> files_;
};

}  // namespace zb::time_simulator
