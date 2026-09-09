#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <string>
#include <vector>

namespace zb::time_simulator {

enum class TimeUnit {
    kDay,
    kWeek,
    kMonth,
    kYear,
    kCustom,
};

enum class ExecutionMode {
    kFull,
    kSampled,
    kModel,
};

struct SimulationSection {
    std::size_t total_steps = 1;
    TimeUnit time_unit = TimeUnit::kDay;
    ExecutionMode execution_mode = ExecutionMode::kModel;
    std::filesystem::path output_dir = "./time_sim_output";
    std::filesystem::path mount_point = "./time_sim_mount";
    std::string namespace_root = "/time_sim";
    std::uint64_t random_seed = 20260624;
    std::uint64_t custom_step_duration_us = 0;
    std::size_t max_materialized_requests_per_step = 10000;
    std::size_t real_concurrency = 8;
    bool fsync_writes = false;
};

struct FileSizeBucket {
    double ratio = 0.0;
    std::uint64_t min_bytes = 0;
    std::uint64_t max_bytes = 0;
};

struct WorkloadSection {
    std::uint64_t writes_per_step = 1000;
    std::uint64_t reads_per_step = 5000;
    std::uint64_t readdirs_per_step = 10;
    double growth_rate_per_step = 0.0;
    double hot_file_ratio = 0.2;
    double hot_request_ratio = 0.8;
    std::uint64_t read_size_bytes = 4ULL * 1024ULL * 1024ULL;
    double failure_probability = 0.0;
    std::map<std::string, FileSizeBucket> file_size_distribution;
};


struct TraceSection {
    bool enabled = false;
    std::uint64_t total_operations = 100;
    double read_ratio = 0.7;
    double write_ratio = 0.3;
    std::uint64_t file_count = 10;
    std::uint64_t io_size_bytes = 256ULL * 1024ULL;
    // Request intensity, in operations per second. The trace generator derives
    // the mean inter-arrival time from this value.
    double request_rate_ops_per_sec = 1000.0;

    // Supported values: fixed, poisson, negative_exponential.
    std::string interval_distribution = "fixed";

    // Kept for compatibility with the V1 configuration. When
    // request_rate_ops_per_sec is absent in YAML and request_interval_us is
    // present, ConfigLoader converts it to request_rate_ops_per_sec.
    std::uint64_t request_interval_us = 1000;
};

struct SamplingSection {
    std::string strategy = "stratified";
    std::size_t min_samples_per_stratum = 100;
    double default_sample_ratio = 0.01;
    std::vector<std::string> strata;
};

struct ResourceConfig {
    std::size_t workers = 1;
    std::uint64_t base_latency_us = 0;
    std::uint64_t jitter_us = 0;
    double max_ops_per_sec = 0.0;
    double bandwidth_bytes_per_sec = 0.0;
};

struct CapacitySection {
    std::uint64_t total_capacity_bytes = 20ULL * 1024ULL * 1024ULL * 1024ULL;
    std::uint64_t object_unit_size = 4ULL * 1024ULL * 1024ULL;
    double replica_factor = 1.0;
    double compression_ratio = 1.0;
    std::uint64_t metadata_bytes_per_file = 512;
};

struct SimulatorConfig {
    SimulationSection simulation;
    WorkloadSection workload;
    TraceSection trace;
    SamplingSection sampling;
    CapacitySection capacity;
    std::map<std::string, ResourceConfig> resources;

    [[nodiscard]] std::uint64_t StepDurationUs() const;
};

class ConfigLoader {
public:
    // Loads the subset of YAML used by time_simulator. No third-party YAML
    // dependency is required.
    static SimulatorConfig Load(const std::filesystem::path& config_path);

    // Throws std::invalid_argument when a semantic constraint is violated.
    static void Validate(const SimulatorConfig& config);
};

[[nodiscard]] std::string ToString(TimeUnit value);
[[nodiscard]] std::string ToString(ExecutionMode value);
[[nodiscard]] TimeUnit ParseTimeUnit(const std::string& value);
[[nodiscard]] ExecutionMode ParseExecutionMode(const std::string& value);

}  // namespace zb::time_simulator