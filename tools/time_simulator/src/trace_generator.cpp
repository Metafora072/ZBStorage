#include "trace_generator.h"

#include <algorithm>
#include <cmath>
#include <cctype>
#include <limits>
#include <stdexcept>

namespace zb::time_simulator {
namespace {

std::string Normalize(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        if (ch == '-') {
            return '_';
        }
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::uint64_t ClampInterval(double value) {
    if (!std::isfinite(value) || value < 1.0) {
        return 1;
    }
    if (value >= static_cast<double>(std::numeric_limits<std::uint64_t>::max())) {
        return std::numeric_limits<std::uint64_t>::max();
    }
    return static_cast<std::uint64_t>(std::llround(value));
}

}  // namespace

TraceGenerator::TraceGenerator(std::uint64_t seed)
    : rng_(seed) {}

std::string ToString(TraceOperationType value) {
    switch (value) {
        case TraceOperationType::kRead:
            return "read";
        case TraceOperationType::kWrite:
            return "write";
    }
    throw std::logic_error("unknown trace operation type");
}

std::string ToString(TraceIntervalDistribution value) {
    switch (value) {
        case TraceIntervalDistribution::kFixed:
            return "fixed";
        case TraceIntervalDistribution::kPoisson:
            return "poisson";
        case TraceIntervalDistribution::kNegativeExponential:
            return "negative_exponential";
    }
    throw std::logic_error("unknown trace interval distribution");
}

TraceIntervalDistribution ParseTraceIntervalDistribution(const std::string& value) {
    const std::string normalized = Normalize(value);
    if (normalized == "fixed" || normalized == "constant") {
        return TraceIntervalDistribution::kFixed;
    }
    if (normalized == "poisson") {
        return TraceIntervalDistribution::kPoisson;
    }
    if (normalized == "negative_exponential" ||
        normalized == "negativeexp" ||
        normalized == "exponential") {
        return TraceIntervalDistribution::kNegativeExponential;
    }
    throw std::invalid_argument(
        "unknown trace interval_distribution: " + value +
        "; supported: fixed, poisson, negative_exponential");
}

std::uint64_t TraceGenerator::NextIntervalUs(
    const TraceGenerationConfig& config) {
    if (!(config.request_rate_ops_per_sec > 0.0) ||
        !std::isfinite(config.request_rate_ops_per_sec)) {
        throw std::invalid_argument(
            "trace request_rate_ops_per_sec must be finite and greater than 0");
    }

    const double mean_interval_us =
        1'000'000.0 / config.request_rate_ops_per_sec;

    switch (config.interval_distribution) {
        case TraceIntervalDistribution::kFixed:
            return ClampInterval(mean_interval_us);

        case TraceIntervalDistribution::kPoisson: {
            // The project test document lists Poisson as an inter-arrival
            // distribution. V2 follows that wording literally and samples an
            // integer microsecond interval from Poisson(mean_interval_us).
            std::poisson_distribution<std::uint64_t> distribution(mean_interval_us);
            return std::max<std::uint64_t>(1, distribution(rng_));
        }

        case TraceIntervalDistribution::kNegativeExponential: {
            // lambda is expressed per microsecond, therefore E[X] is exactly
            // 1e6 / request_rate_ops_per_sec microseconds.
            const double lambda_per_us =
                config.request_rate_ops_per_sec / 1'000'000.0;
            std::exponential_distribution<double> distribution(lambda_per_us);
            return ClampInterval(distribution(rng_));
        }
    }

    throw std::logic_error("unknown trace interval distribution");
}

std::vector<TraceRequest> TraceGenerator::Generate(
    const TraceGenerationConfig& config,
    const std::vector<TraceFileTarget>& files) {
    if (config.total_operations == 0) {
        return {};
    }
    if (files.empty()) {
        throw std::invalid_argument("trace generation requires at least one file");
    }
    if (config.io_size_bytes == 0) {
        throw std::invalid_argument("trace io_size_bytes must be greater than 0");
    }
    if (!(config.request_rate_ops_per_sec > 0.0) ||
        !std::isfinite(config.request_rate_ops_per_sec)) {
        throw std::invalid_argument(
            "trace request_rate_ops_per_sec must be finite and greater than 0");
    }
    if (config.read_ratio < 0.0 || config.read_ratio > 1.0 ||
        config.write_ratio < 0.0 || config.write_ratio > 1.0 ||
        std::abs(config.read_ratio + config.write_ratio - 1.0) > 1e-6) {
        throw std::invalid_argument(
            "trace read_ratio and write_ratio must be in [0,1] and sum to 1");
    }

    const std::uint64_t read_count = static_cast<std::uint64_t>(std::llround(
        static_cast<double>(config.total_operations) * config.read_ratio));
    const std::uint64_t write_count = config.total_operations - read_count;

    std::vector<TraceOperationType> operation_types;
    operation_types.reserve(static_cast<std::size_t>(config.total_operations));
    operation_types.insert(
        operation_types.end(),
        static_cast<std::size_t>(read_count),
        TraceOperationType::kRead);
    operation_types.insert(
        operation_types.end(),
        static_cast<std::size_t>(write_count),
        TraceOperationType::kWrite);
    std::shuffle(operation_types.begin(), operation_types.end(), rng_);

    std::uniform_int_distribution<std::size_t> choose_file(0, files.size() - 1);
    std::vector<TraceRequest> trace;
    trace.reserve(operation_types.size());

    std::uint64_t timestamp_us = 0;
    for (std::size_t index = 0; index < operation_types.size(); ++index) {
        const std::size_t file_index = choose_file(rng_);
        const TraceFileTarget& file = files[file_index];
        if (file.size_bytes == 0) {
            throw std::invalid_argument("trace target file size must be greater than 0");
        }

        const std::uint64_t request_size =
            std::min(config.io_size_bytes, file.size_bytes);
        std::uint64_t offset = 0;
        if (file.size_bytes > request_size) {
            std::uniform_int_distribution<std::uint64_t> choose_offset(
                0,
                file.size_bytes - request_size);
            offset = choose_offset(rng_);
        }

        TraceRequest request;
        request.request_id = static_cast<std::uint64_t>(index + 1);
        request.timestamp_us = timestamp_us;
        request.inter_arrival_us = index == 0 ? 0 : timestamp_us - trace.back().timestamp_us;
        request.operation_type = operation_types[index];
        request.file_index = file_index;
        request.logical_path = file.logical_path;
        request.offset = offset;
        request.size_bytes = request_size;
        trace.push_back(std::move(request));

        if (index + 1 < operation_types.size()) {
            const std::uint64_t interval_us = NextIntervalUs(config);
            if (timestamp_us > std::numeric_limits<std::uint64_t>::max() - interval_us) {
                throw std::overflow_error("trace timestamp overflow");
            }
            timestamp_us += interval_us;
        }
    }

    return trace;
}

}  // namespace zb::time_simulator
