#include "time_simulator.h"
#include "trace_generator.h"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

std::filesystem::path WriteTempConfig(const std::string& content) {
    const auto path = std::filesystem::temp_directory_path() /
                      "zbstorage_time_simulator_config_test.yaml";
    std::ofstream output(path, std::ios::trunc);
    output << content;
    output.close();
    return path;
}

void TestValidConfig() {
    const auto path = WriteTempConfig(R"yaml(
simulation:
  total_steps: 2
  time_unit: week
  execution_mode: sampled
  output_dir: ./out
  mount_point: /mnt/zbstorage
  namespace_root: /sim
  random_seed: 7
  max_materialized_requests_per_step: 1000

trace:
  enabled: true
  total_operations: 100
  read_ratio: 0.7
  write_ratio: 0.3
  file_count: 10
  io_size_bytes: 4096
  request_rate_ops_per_sec: 4000
  interval_distribution: fixed

workload:
  writes_per_step: 10
  reads_per_step: 20
  hot_file_ratio: 0.1
  hot_request_ratio: 0.9
  read_size_bytes: 4096
  file_size_distribution:
    small:
      ratio: 0.7
      min_bytes: 4096
      max_bytes: 8192
    medium:
      ratio: 0.2
      min_bytes: 8192
      max_bytes: 16384
    large:
      ratio: 0.1
      min_bytes: 16384
      max_bytes: 32768

sampling:
  strategy: stratified
  min_samples_per_stratum: 2
  default_sample_ratio: 0.5
  strata:
    - op_type
    - heat_class

resources:
  mds_worker:
    workers: 32
)yaml");

    const auto config = zb::time_simulator::ConfigLoader::Load(path);
    assert(config.simulation.total_steps == 2);
    assert(config.simulation.time_unit == zb::time_simulator::TimeUnit::kWeek);
    assert(config.simulation.execution_mode ==
           zb::time_simulator::ExecutionMode::kSampled);
    assert(config.StepDurationUs() == 7ULL * 86'400'000'000ULL);
    assert(config.workload.writes_per_step == 10);
    assert(config.sampling.strata.size() == 2);
    assert(config.resources.at("mds_worker").workers == 32);
    assert(config.trace.enabled);
    assert(config.trace.total_operations == 100);
    assert(config.trace.read_ratio == 0.7);
    assert(config.trace.write_ratio == 0.3);
    assert(config.trace.file_count == 10);
    assert(config.trace.io_size_bytes == 4096);
    assert(config.trace.request_rate_ops_per_sec == 4000.0);
    assert(config.trace.interval_distribution == "fixed");
    assert(config.trace.request_interval_us == 250);
}

void TestInvalidFileSizeRatioSum() {
    const auto path = WriteTempConfig(R"yaml(
simulation:
  total_steps: 1
  execution_mode: model
  namespace_root: /sim
workload:
  file_size_distribution:
    small:
      ratio: 0.6
    medium:
      ratio: 0.3
    large:
      ratio: 0.2
)yaml");

    bool rejected = false;
    try {
        static_cast<void>(zb::time_simulator::ConfigLoader::Load(path));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
}

void TestInvalidTraceRatio() {
    const auto path = WriteTempConfig(R"yaml(
simulation:
  total_steps: 1
  execution_mode: model
  namespace_root: /sim
trace:
  enabled: true
  total_operations: 100
  read_ratio: 0.8
  write_ratio: 0.3
  file_count: 2
  io_size_bytes: 4096
)yaml");

    bool rejected = false;
    try {
        static_cast<void>(zb::time_simulator::ConfigLoader::Load(path));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
}

void TestTraceGeneratorExactRatioAndTimestamps() {
    zb::time_simulator::TraceGenerationConfig config;
    config.total_operations = 100;
    config.read_ratio = 0.7;
    config.write_ratio = 0.3;
    config.io_size_bytes = 4096;
    config.request_rate_ops_per_sec = 4000.0;
    config.interval_distribution =
        zb::time_simulator::TraceIntervalDistribution::kFixed;

    const std::vector<zb::time_simulator::TraceFileTarget> files = {
        {"/real/test/f0", 16384},
        {"/real/test/f1", 8192},
        {"/real/test/f2", 4096},
    };

    zb::time_simulator::TraceGenerator generator(12345);
    const auto trace = generator.Generate(config, files);
    assert(trace.size() == 100);

    std::size_t reads = 0;
    std::size_t writes = 0;
    for (std::size_t i = 0; i < trace.size(); ++i) {
        const auto& request = trace[i];
        assert(request.request_id == i + 1);
        assert(request.timestamp_us == i * 250ULL);
        assert(request.inter_arrival_us == (i == 0 ? 0ULL : 250ULL));
        assert(request.file_index < files.size());
        assert(request.logical_path == files[request.file_index].logical_path);
        assert(request.size_bytes > 0);
        assert(request.size_bytes <= config.io_size_bytes);
        assert(request.offset + request.size_bytes <= files[request.file_index].size_bytes);
        if (request.operation_type == zb::time_simulator::TraceOperationType::kRead) {
            ++reads;
        } else {
            ++writes;
        }
    }
    assert(reads == 70);
    assert(writes == 30);
}

void TestTraceGeneratorDeterministicSeed() {
    zb::time_simulator::TraceGenerationConfig config;
    config.total_operations = 20;
    config.read_ratio = 0.5;
    config.write_ratio = 0.5;
    config.io_size_bytes = 1024;
    config.request_rate_ops_per_sec = 100000.0;
    config.interval_distribution =
        zb::time_simulator::TraceIntervalDistribution::kNegativeExponential;

    const std::vector<zb::time_simulator::TraceFileTarget> files = {
        {"/real/test/a", 8192},
        {"/real/test/b", 8192},
    };

    zb::time_simulator::TraceGenerator left(777);
    zb::time_simulator::TraceGenerator right(777);
    const auto trace_left = left.Generate(config, files);
    const auto trace_right = right.Generate(config, files);
    assert(trace_left.size() == trace_right.size());
    for (std::size_t i = 0; i < trace_left.size(); ++i) {
        assert(trace_left[i].request_id == trace_right[i].request_id);
        assert(trace_left[i].timestamp_us == trace_right[i].timestamp_us);
        assert(trace_left[i].inter_arrival_us == trace_right[i].inter_arrival_us);
        assert(trace_left[i].operation_type == trace_right[i].operation_type);
        assert(trace_left[i].file_index == trace_right[i].file_index);
        assert(trace_left[i].logical_path == trace_right[i].logical_path);
        assert(trace_left[i].offset == trace_right[i].offset);
        assert(trace_left[i].size_bytes == trace_right[i].size_bytes);
    }
}

void TestPoissonIntervalsFollowConfiguredMean() {
    zb::time_simulator::TraceGenerationConfig config;
    config.total_operations = 10000;
    config.read_ratio = 0.5;
    config.write_ratio = 0.5;
    config.io_size_bytes = 1024;
    config.request_rate_ops_per_sec = 1000.0;  // mean interval = 1000 us
    config.interval_distribution =
        zb::time_simulator::TraceIntervalDistribution::kPoisson;

    const std::vector<zb::time_simulator::TraceFileTarget> files = {
        {"/real/test/a", 8192},
        {"/real/test/b", 8192},
    };

    zb::time_simulator::TraceGenerator generator(20260908);
    const auto trace = generator.Generate(config, files);
    assert(trace.size() == 10000);

    std::uint64_t interval_sum = 0;
    for (std::size_t i = 1; i < trace.size(); ++i) {
        assert(trace[i].timestamp_us > trace[i - 1].timestamp_us);
        assert(trace[i].inter_arrival_us > 0);
        interval_sum += trace[i].inter_arrival_us;
    }

    const double mean = static_cast<double>(interval_sum) /
                        static_cast<double>(trace.size() - 1);
    assert(mean > 950.0 && mean < 1050.0);
}

void TestNegativeExponentialIntervalsFollowConfiguredMean() {
    zb::time_simulator::TraceGenerationConfig config;
    config.total_operations = 20000;
    config.read_ratio = 0.5;
    config.write_ratio = 0.5;
    config.io_size_bytes = 1024;
    config.request_rate_ops_per_sec = 2000.0;  // mean interval = 500 us
    config.interval_distribution =
        zb::time_simulator::TraceIntervalDistribution::kNegativeExponential;

    const std::vector<zb::time_simulator::TraceFileTarget> files = {
        {"/real/test/a", 8192},
        {"/real/test/b", 8192},
    };

    zb::time_simulator::TraceGenerator generator(20260909);
    const auto trace = generator.Generate(config, files);
    assert(trace.size() == 20000);

    std::uint64_t interval_sum = 0;
    bool saw_short_interval = false;
    bool saw_long_interval = false;
    for (std::size_t i = 1; i < trace.size(); ++i) {
        assert(trace[i].timestamp_us > trace[i - 1].timestamp_us);
        assert(trace[i].inter_arrival_us > 0);
        interval_sum += trace[i].inter_arrival_us;
        saw_short_interval = saw_short_interval || trace[i].inter_arrival_us < 100;
        saw_long_interval = saw_long_interval || trace[i].inter_arrival_us > 1500;
    }

    const double mean = static_cast<double>(interval_sum) /
                        static_cast<double>(trace.size() - 1);
    assert(mean > 450.0 && mean < 550.0);
    assert(saw_short_interval);
    assert(saw_long_interval);
}

void TestV1RequestIntervalCompatibility() {
    const auto path = WriteTempConfig(R"yaml(
simulation:
  total_steps: 1
  execution_mode: model
  namespace_root: /sim
trace:
  enabled: true
  total_operations: 10
  read_ratio: 0.5
  write_ratio: 0.5
  file_count: 2
  io_size_bytes: 4096
  request_interval_us: 2000
)yaml");

    const auto config = zb::time_simulator::ConfigLoader::Load(path);
    assert(config.trace.request_interval_us == 2000);
    assert(config.trace.request_rate_ops_per_sec == 500.0);
}

void TestInvalidIntervalDistribution() {
    const auto path = WriteTempConfig(R"yaml(
simulation:
  total_steps: 1
  execution_mode: model
  namespace_root: /sim
trace:
  enabled: true
  total_operations: 10
  read_ratio: 0.5
  write_ratio: 0.5
  file_count: 2
  io_size_bytes: 4096
  request_rate_ops_per_sec: 1000
  interval_distribution: bad_mode
)yaml");

    bool rejected = false;
    try {
        static_cast<void>(zb::time_simulator::ConfigLoader::Load(path));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }
    assert(rejected);
}

}  // namespace

int main() {
    TestValidConfig();
    TestInvalidFileSizeRatioSum();
    TestInvalidTraceRatio();
    TestTraceGeneratorExactRatioAndTimestamps();
    TestTraceGeneratorDeterministicSeed();
    TestPoissonIntervalsFollowConfiguredMean();
    TestNegativeExponentialIntervalsFollowConfiguredMean();
    TestV1RequestIntervalCompatibility();
    TestInvalidIntervalDistribution();
    std::cout << "config and trace generator tests passed\n";
    return 0;
}
