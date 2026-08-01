#include "common/metrics/NodeMetricsCollector.h"

#include <iostream>

int main() {
    zb::metrics::NodeMetricsCollector collector(1000);
    {
        auto read = collector.Start(zb::metrics::OperationKind::kRead);
        read.SetBytes(4096);
    }
    {
        auto write = collector.Start(zb::metrics::OperationKind::kWrite);
        write.SetBytes(8192);
    }
    auto first = collector.TakeForReport(1100);
    if (first.read_operations != 1 || first.write_operations != 1 ||
        first.read_bytes != 4096 || first.write_bytes != 8192 ||
        first.max_queue_depth == 0) {
        std::cerr << "FAIL collector counters" << std::endl;
        return 1;
    }
    auto retried = collector.TakeForReport(1200);
    if (retried.read_operations != 1 || retried.write_operations != 1 ||
        retried.window_start_ms != 1000 || retried.report_sequence != first.report_sequence) {
        std::cerr << "FAIL exact retry" << std::endl;
        return 2;
    }
    collector.AcknowledgeReport(first.report_sequence);
    auto empty = collector.TakeForReport(1300);
    if (empty.HasActivity()) {
        std::cerr << "FAIL acknowledge" << std::endl;
        return 3;
    }
    std::cout << "PASS node metrics collector" << std::endl;
    return 0;
}
