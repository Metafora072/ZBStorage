#include "NodeMetricsCollector.h"

#include <algorithm>
#include <atomic>
#include <limits>

namespace zb::metrics {

namespace {

std::atomic<uint64_t> g_reporter_counter{1};

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max()
               : lhs + rhs;
}

} // namespace

bool NodeMetricsSnapshot::HasActivity() const {
    return read_operations != 0 || write_operations != 0 || other_operations != 0 ||
           read_bytes != 0 || write_bytes != 0 || active_requests != 0;
}

NodeMetricsCollector::ScopedOperation::ScopedOperation(NodeMetricsCollector* collector,
                                                       OperationKind kind)
    : collector_(collector), kind_(kind), started_(std::chrono::steady_clock::now()) {
    if (collector_) {
        collector_->BeginRequest();
    }
}

NodeMetricsCollector::ScopedOperation::ScopedOperation(ScopedOperation&& other) noexcept
    : collector_(other.collector_),
      kind_(other.kind_),
      bytes_(other.bytes_),
      started_(other.started_) {
    other.collector_ = nullptr;
}

NodeMetricsCollector::ScopedOperation& NodeMetricsCollector::ScopedOperation::operator=(
    ScopedOperation&& other) noexcept {
    if (this != &other) {
        Finish();
        collector_ = other.collector_;
        kind_ = other.kind_;
        bytes_ = other.bytes_;
        started_ = other.started_;
        other.collector_ = nullptr;
    }
    return *this;
}

NodeMetricsCollector::ScopedOperation::~ScopedOperation() {
    Finish();
}

void NodeMetricsCollector::ScopedOperation::SetBytes(uint64_t bytes) {
    bytes_ = bytes;
}

void NodeMetricsCollector::ScopedOperation::Finish() {
    if (!collector_) {
        return;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - started_);
    collector_->FinishRequest(kind_, bytes_, static_cast<uint64_t>(std::max<int64_t>(0, elapsed.count())));
    collector_ = nullptr;
}

NodeMetricsCollector::NodeMetricsCollector(uint64_t initial_time_ms) {
    pending_.window_start_ms = initial_time_ms == 0 ? NowMs() : initial_time_ms;
    reporter_epoch_ = SaturatingAdd(NowMs() * 1000ULL, g_reporter_counter.fetch_add(1));
}

NodeMetricsCollector::ScopedOperation NodeMetricsCollector::Start(OperationKind kind) {
    return ScopedOperation(this, kind);
}

NodeMetricsSnapshot NodeMetricsCollector::TakeForReport(uint64_t window_end_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    if (has_in_flight_) {
        return in_flight_;
    }
    NodeMetricsSnapshot result = pending_;
    result.reporter_epoch = reporter_epoch_;
    result.window_end_ms = std::max(window_end_ms, result.window_start_ms);
    result.active_requests = active_requests_;
    pending_ = {};
    pending_.window_start_ms = result.window_end_ms;
    pending_.max_queue_depth = active_requests_;
    if (result.HasActivity()) {
        result.report_sequence = next_report_sequence_++;
        in_flight_ = result;
        has_in_flight_ = true;
    }
    return result;
}

void NodeMetricsCollector::AcknowledgeReport(uint64_t report_sequence) {
    std::lock_guard<std::mutex> lock(mu_);
    if (has_in_flight_ && in_flight_.report_sequence == report_sequence) {
        in_flight_ = {};
        has_in_flight_ = false;
    }
}

uint64_t NodeMetricsCollector::NowMs() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

void NodeMetricsCollector::BeginRequest() {
    std::lock_guard<std::mutex> lock(mu_);
    ++active_requests_;
    pending_.max_queue_depth = std::max(pending_.max_queue_depth, active_requests_);
}

void NodeMetricsCollector::FinishRequest(OperationKind kind,
                                         uint64_t bytes,
                                         uint64_t busy_time_us) {
    std::lock_guard<std::mutex> lock(mu_);
    if (active_requests_ != 0) {
        --active_requests_;
    }
    switch (kind) {
    case OperationKind::kRead:
        ++pending_.read_operations;
        pending_.read_bytes = SaturatingAdd(pending_.read_bytes, bytes);
        break;
    case OperationKind::kWrite:
        ++pending_.write_operations;
        pending_.write_bytes = SaturatingAdd(pending_.write_bytes, bytes);
        break;
    case OperationKind::kOther:
        ++pending_.other_operations;
        break;
    }
    pending_.busy_time_ms = SaturatingAdd(pending_.busy_time_ms, (busy_time_us + 999) / 1000);
    pending_.last_access_ms = std::max(pending_.last_access_ms, NowMs());
}

} // namespace zb::metrics
