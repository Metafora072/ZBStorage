#pragma once

#include <chrono>
#include <cstdint>
#include <mutex>

namespace zb::metrics {

enum class OperationKind {
    kRead,
    kWrite,
    kOther,
};

struct NodeMetricsSnapshot {
    uint64_t reporter_epoch{0};
    uint64_t report_sequence{0};
    uint64_t window_start_ms{0};
    uint64_t window_end_ms{0};
    uint64_t read_operations{0};
    uint64_t write_operations{0};
    uint64_t other_operations{0};
    uint64_t read_bytes{0};
    uint64_t write_bytes{0};
    uint32_t active_requests{0};
    uint32_t max_queue_depth{0};
    uint64_t busy_time_ms{0};
    uint64_t last_access_ms{0};

    bool HasActivity() const;
};

class NodeMetricsCollector {
public:
    class ScopedOperation {
    public:
        ScopedOperation() = default;
        ScopedOperation(NodeMetricsCollector* collector, OperationKind kind);
        ScopedOperation(const ScopedOperation&) = delete;
        ScopedOperation& operator=(const ScopedOperation&) = delete;
        ScopedOperation(ScopedOperation&& other) noexcept;
        ScopedOperation& operator=(ScopedOperation&& other) noexcept;
        ~ScopedOperation();

        void SetBytes(uint64_t bytes);

    private:
        void Finish();

        NodeMetricsCollector* collector_{};
        OperationKind kind_{OperationKind::kOther};
        uint64_t bytes_{0};
        std::chrono::steady_clock::time_point started_{};
    };

    explicit NodeMetricsCollector(uint64_t initial_time_ms = 0);

    ScopedOperation Start(OperationKind kind);
    NodeMetricsSnapshot TakeForReport(uint64_t window_end_ms);
    void AcknowledgeReport(uint64_t report_sequence);

private:
    friend class ScopedOperation;
    static uint64_t NowMs();
    void BeginRequest();
    void FinishRequest(OperationKind kind, uint64_t bytes, uint64_t busy_time_us);

    mutable std::mutex mu_;
    NodeMetricsSnapshot pending_;
    NodeMetricsSnapshot in_flight_;
    uint64_t next_report_sequence_{1};
    uint64_t reporter_epoch_{0};
    bool has_in_flight_{false};
    uint32_t active_requests_{0};
};

} // namespace zb::metrics
