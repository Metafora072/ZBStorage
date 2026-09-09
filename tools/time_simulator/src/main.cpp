#include "fuse_executor.h"
#include "time_simulator.h"

#include <exception>
#include <iostream>
#include <string>

namespace {

void PrintUsage(const char* program) {
    std::cerr << "Usage: " << program << " --config <path>\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3 || std::string(argv[1]) != "--config") {
        PrintUsage(argv[0]);
        return 2;
    }

    try {
        const auto config =
            zb::time_simulator::ConfigLoader::Load(argv[2]);

        std::cout << "Config loaded successfully\n"
                  << "  total_steps: " << config.simulation.total_steps << '\n'
                  << "  time_unit: "
                  << zb::time_simulator::ToString(config.simulation.time_unit) << '\n'
                  << "  execution_mode: "
                  << zb::time_simulator::ToString(config.simulation.execution_mode) << '\n'
                  << "  mount_point: " << config.simulation.mount_point << '\n'
                  << "  namespace_root: " << config.simulation.namespace_root << '\n'
                  << "  writes_per_step: " << config.workload.writes_per_step << '\n'
                  << "  reads_per_step: " << config.workload.reads_per_step << '\n'
                  << "  trace_enabled: " << (config.trace.enabled ? "true" : "false") << '\n';
        if (config.trace.enabled) {
            std::cout << "  trace_total_operations: " << config.trace.total_operations << '\n'
                      << "  trace_read_ratio: " << config.trace.read_ratio << '\n'
                      << "  trace_write_ratio: " << config.trace.write_ratio << '\n'
                      << "  trace_file_count: " << config.trace.file_count << '\n'
                      << "  trace_request_rate_ops_per_sec: "
                      << config.trace.request_rate_ops_per_sec << '\n'
                      << "  trace_interval_distribution: "
                      << config.trace.interval_distribution << '\n';
        }

        if (config.simulation.execution_mode ==
            zb::time_simulator::ExecutionMode::kModel) {
            std::cout << "[INFO] model mode only validates configuration.\n"
                      << "[INFO] use execution_mode: full to drive ZBStorage FUSE.\n";
            return 0;
        }
        if (config.simulation.execution_mode ==
            zb::time_simulator::ExecutionMode::kSampled) {
            throw std::runtime_error(
                "sampled mode is not implemented in this stage; use full mode first");
        }

        zb::time_simulator::FuseExecutor executor(config);
        executor.Run();
        std::cout << "[DONE] real ZBStorage FUSE workload completed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "[ERROR] " << error.what() << '\n';
        return 1;
    }
}
