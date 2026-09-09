#include "time_simulator.h"
#include "trace_generator.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace zb::time_simulator {
namespace {

std::string Trim(std::string value) {
    const auto first = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto last = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }).base();

    if (first >= last) {
        return {};
    }
    return std::string(first, last);
}

std::string Lower(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string StripQuotes(std::string value) {
    value = Trim(std::move(value));
    if (value.size() >= 2) {
        const char first = value.front();
        const char last = value.back();
        if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
            return value.substr(1, value.size() - 2);
        }
    }
    return value;
}

std::string RemoveComment(const std::string& line) {
    bool in_single_quotes = false;
    bool in_double_quotes = false;

    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (ch == '\'' && !in_double_quotes) {
            in_single_quotes = !in_single_quotes;
        } else if (ch == '"' && !in_single_quotes) {
            in_double_quotes = !in_double_quotes;
        } else if (ch == '#' && !in_single_quotes && !in_double_quotes) {
            return line.substr(0, i);
        }
    }
    return line;
}

class FlatYaml {
public:
    explicit FlatYaml(const std::filesystem::path& path) {
        std::ifstream input(path);
        if (!input.is_open()) {
            throw std::runtime_error("cannot open config file: " + path.string());
        }

        std::vector<std::pair<std::size_t, std::string>> parents;
        std::string raw_line;
        std::size_t line_number = 0;

        while (std::getline(input, raw_line)) {
            ++line_number;
            if (!raw_line.empty() && raw_line.back() == '\r') {
                raw_line.pop_back();
            }
            if (raw_line.find('\t') != std::string::npos) {
                throw std::runtime_error(
                    "tabs are not allowed in YAML indentation, line " +
                    std::to_string(line_number));
            }

            const std::string uncommented = RemoveComment(raw_line);
            const auto first_non_space = uncommented.find_first_not_of(' ');
            if (first_non_space == std::string::npos) {
                continue;
            }

            const std::size_t indent = first_non_space;
            const std::string content = Trim(uncommented.substr(first_non_space));
            while (!parents.empty() && parents.back().first >= indent) {
                parents.pop_back();
            }

            if (content.rfind("- ", 0) == 0 || content == "-") {
                if (parents.empty()) {
                    throw std::runtime_error(
                        "YAML list item has no parent key, line " +
                        std::to_string(line_number));
                }
                const std::string value = content == "-" ? "" : StripQuotes(content.substr(2));
                sequences_[parents.back().second].push_back(value);
                continue;
            }

            const auto separator = content.find(':');
            if (separator == std::string::npos) {
                throw std::runtime_error(
                    "expected 'key: value' in config, line " +
                    std::to_string(line_number));
            }

            const std::string key = Trim(content.substr(0, separator));
            const std::string value = Trim(content.substr(separator + 1));
            if (key.empty()) {
                throw std::runtime_error("empty YAML key, line " + std::to_string(line_number));
            }

            std::string full_key;
            if (!parents.empty()) {
                full_key = parents.back().second + ".";
            }
            full_key += key;

            if (value.empty()) {
                parents.emplace_back(indent, full_key);
            } else {
                scalars_[full_key] = StripQuotes(value);
            }
        }
    }

    [[nodiscard]] std::optional<std::string> String(const std::string& key) const {
        const auto it = scalars_.find(key);
        if (it == scalars_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

    [[nodiscard]] std::vector<std::string> Sequence(const std::string& key) const {
        const auto it = sequences_.find(key);
        if (it == sequences_.end()) {
            return {};
        }
        return it->second;
    }

    [[nodiscard]] std::uint64_t UInt64(
        const std::string& key,
        std::uint64_t default_value) const {
        const auto value = String(key);
        if (!value.has_value()) {
            return default_value;
        }
        try {
            std::size_t parsed = 0;
            const auto result = std::stoull(*value, &parsed, 0);
            if (parsed != value->size()) {
                throw std::invalid_argument("trailing characters");
            }
            return result;
        } catch (const std::exception&) {
            throw std::runtime_error("invalid unsigned integer for '" + key + "': " + *value);
        }
    }

    [[nodiscard]] std::size_t Size(
        const std::string& key,
        std::size_t default_value) const {
        const std::uint64_t value = UInt64(key, default_value);
        if (value > std::numeric_limits<std::size_t>::max()) {
            throw std::runtime_error("value for '" + key + "' is too large");
        }
        return static_cast<std::size_t>(value);
    }

    [[nodiscard]] double Double(const std::string& key, double default_value) const {
        const auto value = String(key);
        if (!value.has_value()) {
            return default_value;
        }
        try {
            std::size_t parsed = 0;
            const double result = std::stod(*value, &parsed);
            if (parsed != value->size() || !std::isfinite(result)) {
                throw std::invalid_argument("invalid floating point value");
            }
            return result;
        } catch (const std::exception&) {
            throw std::runtime_error("invalid number for '" + key + "': " + *value);
        }
    }

    [[nodiscard]] bool Boolean(const std::string& key, bool default_value) const {
        const auto value = String(key);
        if (!value.has_value()) {
            return default_value;
        }
        const std::string normalized = Lower(*value);
        if (normalized == "true" || normalized == "yes" || normalized == "1") {
            return true;
        }
        if (normalized == "false" || normalized == "no" || normalized == "0") {
            return false;
        }
        throw std::runtime_error("invalid boolean for '" + key + "': " + *value);
    }

private:
    std::map<std::string, std::string> scalars_;
    std::map<std::string, std::vector<std::string>> sequences_;
};

ResourceConfig MakeResource(
    std::size_t workers,
    std::uint64_t base_latency_us,
    std::uint64_t jitter_us,
    double max_ops_per_sec,
    double bandwidth_bytes_per_sec) {
    ResourceConfig resource;
    resource.workers = workers;
    resource.base_latency_us = base_latency_us;
    resource.jitter_us = jitter_us;
    resource.max_ops_per_sec = max_ops_per_sec;
    resource.bandwidth_bytes_per_sec = bandwidth_bytes_per_sec;
    return resource;
}

void PopulateDefaults(SimulatorConfig& config) {
    config.workload.file_size_distribution = {
        {"small", {0.70, 4ULL * 1024ULL, 1ULL * 1024ULL * 1024ULL}},
        {"medium", {0.25, 1ULL * 1024ULL * 1024ULL, 128ULL * 1024ULL * 1024ULL}},
        {"large", {0.05, 128ULL * 1024ULL * 1024ULL, 1ULL * 1024ULL * 1024ULL * 1024ULL}},
    };
    config.sampling.strata = {
        "op_type",
        "file_size_class",
        "heat_class",
        "access_pattern",
        "storage_tier",
    };
    config.resources = {
        {"client", MakeResource(8, 10, 5, 0.0, 0.0)},
        {"mds_worker", MakeResource(16, 80, 20, 0.0, 0.0)},
        {"rocksdb_read", MakeResource(4, 100, 20, 100000.0, 0.0)},
        {"rocksdb_write", MakeResource(2, 200, 50, 50000.0, 0.0)},
        {"data_node_read", MakeResource(8, 100, 20, 0.0, 0.0)},
        {"data_node_write", MakeResource(8, 120, 30, 0.0, 0.0)},
        {"disk_read", MakeResource(4, 150, 50, 0.0, 1'048'576'000.0)},
        {"disk_write", MakeResource(4, 200, 80, 0.0, 629'145'600.0)},
        {"network", MakeResource(4, 50, 20, 0.0, 1'250'000'000.0)},
        {"cold_storage", MakeResource(1, 5'000'000, 500'000, 0.0, 50'000'000.0)},
    };
}

void LoadResource(const FlatYaml& yaml, const std::string& name, ResourceConfig& resource) {
    const std::string prefix = "resources." + name + ".";
    resource.workers = yaml.Size(prefix + "workers", resource.workers);
    resource.base_latency_us = yaml.UInt64(prefix + "base_latency_us", resource.base_latency_us);
    resource.jitter_us = yaml.UInt64(prefix + "jitter_us", resource.jitter_us);
    resource.max_ops_per_sec = yaml.Double(prefix + "max_ops_per_sec", resource.max_ops_per_sec);
    resource.bandwidth_bytes_per_sec =
        yaml.Double(prefix + "bandwidth_bytes_per_sec", resource.bandwidth_bytes_per_sec);
}

void RequireRatio(const std::string& name, double value) {
    if (value < 0.0 || value > 1.0) {
        throw std::invalid_argument(name + " must be in [0, 1]");
    }
}

}  // namespace

std::uint64_t SimulatorConfig::StepDurationUs() const {
    constexpr std::uint64_t kDayUs = 86'400'000'000ULL;
    switch (simulation.time_unit) {
        case TimeUnit::kDay:
            return kDayUs;
        case TimeUnit::kWeek:
            return 7ULL * kDayUs;
        case TimeUnit::kMonth:
            return 30ULL * kDayUs;
        case TimeUnit::kYear:
            return 365ULL * kDayUs;
        case TimeUnit::kCustom:
            return simulation.custom_step_duration_us;
    }
    throw std::logic_error("unknown time unit");
}

SimulatorConfig ConfigLoader::Load(const std::filesystem::path& config_path) {
    FlatYaml yaml(config_path);
    SimulatorConfig config;
    PopulateDefaults(config);

    config.simulation.total_steps =
        yaml.Size("simulation.total_steps", config.simulation.total_steps);
    config.simulation.time_unit = ParseTimeUnit(
        yaml.String("simulation.time_unit").value_or(ToString(config.simulation.time_unit)));
    config.simulation.execution_mode = ParseExecutionMode(
        yaml.String("simulation.execution_mode")
            .value_or(ToString(config.simulation.execution_mode)));
    config.simulation.output_dir =
        yaml.String("simulation.output_dir").value_or(config.simulation.output_dir.string());
    config.simulation.mount_point =
        yaml.String("simulation.mount_point").value_or(config.simulation.mount_point.string());
    config.simulation.namespace_root =
        yaml.String("simulation.namespace_root").value_or(config.simulation.namespace_root);
    config.simulation.random_seed =
        yaml.UInt64("simulation.random_seed", config.simulation.random_seed);
    config.simulation.custom_step_duration_us = yaml.UInt64(
        "simulation.custom_step_duration_us",
        config.simulation.custom_step_duration_us);
    config.simulation.max_materialized_requests_per_step = yaml.Size(
        "simulation.max_materialized_requests_per_step",
        config.simulation.max_materialized_requests_per_step);
    config.simulation.real_concurrency =
        yaml.Size("simulation.real_concurrency", config.simulation.real_concurrency);
    config.simulation.fsync_writes =
        yaml.Boolean("simulation.fsync_writes", config.simulation.fsync_writes);

    config.workload.writes_per_step =
        yaml.UInt64("workload.writes_per_step", config.workload.writes_per_step);
    config.workload.reads_per_step =
        yaml.UInt64("workload.reads_per_step", config.workload.reads_per_step);
    config.workload.readdirs_per_step =
        yaml.UInt64("workload.readdirs_per_step", config.workload.readdirs_per_step);
    config.workload.growth_rate_per_step =
        yaml.Double("workload.growth_rate_per_step", config.workload.growth_rate_per_step);
    config.workload.hot_file_ratio =
        yaml.Double("workload.hot_file_ratio", config.workload.hot_file_ratio);
    config.workload.hot_request_ratio =
        yaml.Double("workload.hot_request_ratio", config.workload.hot_request_ratio);
    config.workload.read_size_bytes =
        yaml.UInt64("workload.read_size_bytes", config.workload.read_size_bytes);
    config.workload.failure_probability =
        yaml.Double("workload.failure_probability", config.workload.failure_probability);

    config.trace.enabled =
        yaml.Boolean("trace.enabled", config.trace.enabled);
    config.trace.total_operations =
        yaml.UInt64("trace.total_operations", config.trace.total_operations);
    config.trace.read_ratio =
        yaml.Double("trace.read_ratio", config.trace.read_ratio);
    config.trace.write_ratio =
        yaml.Double("trace.write_ratio", config.trace.write_ratio);
    config.trace.file_count =
        yaml.UInt64("trace.file_count", config.trace.file_count);
    config.trace.io_size_bytes =
        yaml.UInt64("trace.io_size_bytes", config.trace.io_size_bytes);
    config.trace.interval_distribution =
        yaml.String("trace.interval_distribution")
            .value_or(config.trace.interval_distribution);

    // V2 prefers an explicit request intensity in operations per second. Keep
    // request_interval_us as a V1 compatibility input so existing configs can
    // still be loaded without changes.
    const auto configured_rate = yaml.String("trace.request_rate_ops_per_sec");
    const auto configured_interval = yaml.String("trace.request_interval_us");
    if (configured_rate.has_value()) {
        config.trace.request_rate_ops_per_sec = yaml.Double(
            "trace.request_rate_ops_per_sec",
            config.trace.request_rate_ops_per_sec);
        if (config.trace.request_rate_ops_per_sec > 0.0) {
            config.trace.request_interval_us = static_cast<std::uint64_t>(std::llround(
                1'000'000.0 / config.trace.request_rate_ops_per_sec));
        }
    } else if (configured_interval.has_value()) {
        config.trace.request_interval_us = yaml.UInt64(
            "trace.request_interval_us",
            config.trace.request_interval_us);
        if (config.trace.request_interval_us > 0) {
            config.trace.request_rate_ops_per_sec =
                1'000'000.0 / static_cast<double>(config.trace.request_interval_us);
        }
    }

    for (auto& [name, bucket] : config.workload.file_size_distribution) {
        const std::string prefix = "workload.file_size_distribution." + name + ".";
        bucket.ratio = yaml.Double(prefix + "ratio", bucket.ratio);
        bucket.min_bytes = yaml.UInt64(prefix + "min_bytes", bucket.min_bytes);
        bucket.max_bytes = yaml.UInt64(prefix + "max_bytes", bucket.max_bytes);
    }

    config.sampling.strategy =
        yaml.String("sampling.strategy").value_or(config.sampling.strategy);
    config.sampling.min_samples_per_stratum = yaml.Size(
        "sampling.min_samples_per_stratum",
        config.sampling.min_samples_per_stratum);
    config.sampling.default_sample_ratio = yaml.Double(
        "sampling.default_sample_ratio",
        config.sampling.default_sample_ratio);
    const auto configured_strata = yaml.Sequence("sampling.strata");
    if (!configured_strata.empty()) {
        config.sampling.strata = configured_strata;
    }

    config.capacity.total_capacity_bytes = yaml.UInt64(
        "capacity.total_capacity_bytes",
        config.capacity.total_capacity_bytes);
    config.capacity.object_unit_size =
        yaml.UInt64("capacity.object_unit_size", config.capacity.object_unit_size);
    config.capacity.replica_factor =
        yaml.Double("capacity.replica_factor", config.capacity.replica_factor);
    config.capacity.compression_ratio =
        yaml.Double("capacity.compression_ratio", config.capacity.compression_ratio);
    config.capacity.metadata_bytes_per_file = yaml.UInt64(
        "capacity.metadata_bytes_per_file",
        config.capacity.metadata_bytes_per_file);

    for (auto& [name, resource] : config.resources) {
        LoadResource(yaml, name, resource);
    }

    Validate(config);
    return config;
}

void ConfigLoader::Validate(const SimulatorConfig& config) {
    if (config.simulation.total_steps == 0) {
        throw std::invalid_argument("simulation.total_steps must be greater than 0");
    }
    if (config.simulation.output_dir.empty()) {
        throw std::invalid_argument("simulation.output_dir must not be empty");
    }
    if (config.simulation.namespace_root.empty() ||
        config.simulation.namespace_root.front() != '/') {
        throw std::invalid_argument("simulation.namespace_root must start with '/'");
    }
    if (config.simulation.real_concurrency == 0) {
        throw std::invalid_argument("simulation.real_concurrency must be greater than 0");
    }
    if (config.simulation.max_materialized_requests_per_step == 0) {
        throw std::invalid_argument(
            "simulation.max_materialized_requests_per_step must be greater than 0");
    }
    if (config.simulation.time_unit == TimeUnit::kCustom &&
        config.simulation.custom_step_duration_us == 0) {
        throw std::invalid_argument(
            "simulation.custom_step_duration_us must be set for custom time_unit");
    }
    if (config.simulation.execution_mode != ExecutionMode::kModel &&
        config.simulation.mount_point.empty()) {
        throw std::invalid_argument(
            "simulation.mount_point is required for full or sampled execution");
    }

    if (config.workload.growth_rate_per_step < -1.0) {
        throw std::invalid_argument("workload.growth_rate_per_step must be >= -1");
    }
    RequireRatio("workload.hot_file_ratio", config.workload.hot_file_ratio);
    RequireRatio("workload.hot_request_ratio", config.workload.hot_request_ratio);
    RequireRatio("workload.failure_probability", config.workload.failure_probability);
    if (config.workload.read_size_bytes == 0) {
        throw std::invalid_argument("workload.read_size_bytes must be greater than 0");
    }
    if (config.workload.file_size_distribution.empty()) {
        throw std::invalid_argument("workload.file_size_distribution must not be empty");
    }

    if (config.trace.enabled) {
        if (config.trace.total_operations == 0) {
            throw std::invalid_argument("trace.total_operations must be greater than 0");
        }
        RequireRatio("trace.read_ratio", config.trace.read_ratio);
        RequireRatio("trace.write_ratio", config.trace.write_ratio);
        if (std::abs(config.trace.read_ratio + config.trace.write_ratio - 1.0) > 1e-6) {
            throw std::invalid_argument(
                "trace.read_ratio + trace.write_ratio must equal 1.0");
        }
        if (config.trace.file_count == 0) {
            throw std::invalid_argument("trace.file_count must be greater than 0");
        }
        if (config.trace.io_size_bytes == 0) {
            throw std::invalid_argument("trace.io_size_bytes must be greater than 0");
        }
        if (!(config.trace.request_rate_ops_per_sec > 0.0) ||
            !std::isfinite(config.trace.request_rate_ops_per_sec)) {
            throw std::invalid_argument(
                "trace.request_rate_ops_per_sec must be finite and greater than 0");
        }
        // Parsing here provides an early, clear config error instead of
        // waiting until trace generation starts.
        static_cast<void>(
            ParseTraceIntervalDistribution(config.trace.interval_distribution));
        if (config.trace.total_operations >
            config.simulation.max_materialized_requests_per_step) {
            throw std::invalid_argument(
                "trace.total_operations exceeds simulation.max_materialized_requests_per_step");
        }
    }

    double ratio_sum = 0.0;
    for (const auto& [name, bucket] : config.workload.file_size_distribution) {
        RequireRatio("file size ratio for " + name, bucket.ratio);
        if (bucket.min_bytes == 0 || bucket.max_bytes == 0) {
            throw std::invalid_argument("file size bounds for " + name + " must be positive");
        }
        if (bucket.min_bytes > bucket.max_bytes) {
            throw std::invalid_argument("min_bytes exceeds max_bytes for file size class " + name);
        }
        ratio_sum += bucket.ratio;
    }
    if (std::abs(ratio_sum - 1.0) > 1e-6) {
        std::ostringstream message;
        message << "file size distribution ratios must sum to 1.0, actual=" << ratio_sum;
        throw std::invalid_argument(message.str());
    }

    if (Lower(config.sampling.strategy) != "stratified") {
        throw std::invalid_argument("sampling.strategy currently supports only 'stratified'");
    }
    if (config.sampling.min_samples_per_stratum == 0) {
        throw std::invalid_argument("sampling.min_samples_per_stratum must be greater than 0");
    }
    if (config.sampling.default_sample_ratio <= 0.0 ||
        config.sampling.default_sample_ratio > 1.0) {
        throw std::invalid_argument("sampling.default_sample_ratio must be in (0, 1]");
    }
    if (config.sampling.strata.empty()) {
        throw std::invalid_argument("sampling.strata must not be empty");
    }

    if (config.capacity.total_capacity_bytes == 0) {
        throw std::invalid_argument("capacity.total_capacity_bytes must be greater than 0");
    }
    if (config.capacity.object_unit_size == 0) {
        throw std::invalid_argument("capacity.object_unit_size must be greater than 0");
    }
    if (config.capacity.replica_factor <= 0.0) {
        throw std::invalid_argument("capacity.replica_factor must be greater than 0");
    }
    if (config.capacity.compression_ratio <= 0.0) {
        throw std::invalid_argument("capacity.compression_ratio must be greater than 0");
    }

    for (const auto& [name, resource] : config.resources) {
        if (resource.workers == 0) {
            throw std::invalid_argument("resource workers must be greater than 0: " + name);
        }
        if (resource.max_ops_per_sec < 0.0) {
            throw std::invalid_argument("resource max_ops_per_sec must be non-negative: " + name);
        }
        if (resource.bandwidth_bytes_per_sec < 0.0) {
            throw std::invalid_argument(
                "resource bandwidth_bytes_per_sec must be non-negative: " + name);
        }
    }
}

std::string ToString(TimeUnit value) {
    switch (value) {
        case TimeUnit::kDay:
            return "day";
        case TimeUnit::kWeek:
            return "week";
        case TimeUnit::kMonth:
            return "month";
        case TimeUnit::kYear:
            return "year";
        case TimeUnit::kCustom:
            return "custom";
    }
    throw std::logic_error("unknown time unit");
}

std::string ToString(ExecutionMode value) {
    switch (value) {
        case ExecutionMode::kFull:
            return "full";
        case ExecutionMode::kSampled:
            return "sampled";
        case ExecutionMode::kModel:
            return "model";
    }
    throw std::logic_error("unknown execution mode");
}

TimeUnit ParseTimeUnit(const std::string& value) {
    const std::string normalized = Lower(Trim(value));
    if (normalized == "day") {
        return TimeUnit::kDay;
    }
    if (normalized == "week") {
        return TimeUnit::kWeek;
    }
    if (normalized == "month") {
        return TimeUnit::kMonth;
    }
    if (normalized == "year") {
        return TimeUnit::kYear;
    }
    if (normalized == "custom") {
        return TimeUnit::kCustom;
    }
    throw std::invalid_argument("unknown time_unit: " + value);
}

ExecutionMode ParseExecutionMode(const std::string& value) {
    const std::string normalized = Lower(Trim(value));
    if (normalized == "full") {
        return ExecutionMode::kFull;
    }
    if (normalized == "sampled") {
        return ExecutionMode::kSampled;
    }
    if (normalized == "model") {
        return ExecutionMode::kModel;
    }
    throw std::invalid_argument("unknown execution_mode: " + value);
}

}  // namespace zb::time_simulator