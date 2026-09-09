#pragma once

#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace zb::time_simulator {

enum class TraceOperationType {
    kRead,
    kWrite,
};

enum class TraceIntervalDistribution {
    kFixed,
    kPoisson,
    kNegativeExponential,
};

struct TraceFileTarget {
    std::string logical_path;
    std::uint64_t size_bytes = 0;
};

struct TraceRequest {
    std::uint64_t request_id = 0;
    std::uint64_t timestamp_us = 0;
    std::uint64_t inter_arrival_us = 0;
    TraceOperationType operation_type = TraceOperationType::kRead;
    std::size_t file_index = 0;
    std::string logical_path;
    std::uint64_t offset = 0;
    std::uint64_t size_bytes = 0;
};

struct TraceGenerationConfig {
    std::uint64_t total_operations = 100;
    double read_ratio = 0.7;
    double write_ratio = 0.3;
    std::uint64_t io_size_bytes = 256ULL * 1024ULL;
    double request_rate_ops_per_sec = 1000.0;
    TraceIntervalDistribution interval_distribution =
        TraceIntervalDistribution::kFixed;
};

class TraceGenerator {
public:
    explicit TraceGenerator(std::uint64_t seed);

    // Generates an exact read/write mix and shuffles the operation order.
    // Request timestamps are logical release times within the trace. The
    // serial executor guarantees that request i+1 never starts before request
    // i has finished, even when its logical release time has already passed.
    [[nodiscard]] std::vector<TraceRequest> Generate(
        const TraceGenerationConfig& config,
        const std::vector<TraceFileTarget>& files);

private:
    [[nodiscard]] std::uint64_t NextIntervalUs(
        const TraceGenerationConfig& config);

    std::mt19937_64 rng_;
};

[[nodiscard]] std::string ToString(TraceOperationType value);
[[nodiscard]] std::string ToString(TraceIntervalDistribution value);
[[nodiscard]] TraceIntervalDistribution ParseTraceIntervalDistribution(
    const std::string& value);

}  // namespace zb::time_simulator
