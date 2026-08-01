#pragma once

#include <string>

#include "NodeMetricsCollector.h"
#include "scheduler.pb.h"

namespace brpc {
class Channel;
}

namespace zb::metrics {

bool ReportCollectedMetrics(brpc::Channel* channel,
                            const std::string& node_id,
                            NodeMetricsCollector* collector,
                            zb::rpc::ManagedAccessPath access_path,
                            uint64_t now_ms,
                            std::string* error);

bool EnsureMetadataNodeRegistered(brpc::Channel* channel,
                                  const std::string& node_id,
                                  uint64_t now_ms,
                                  std::string* error);

bool ReportMetadataNodeHeartbeat(brpc::Channel* channel,
                                 const std::string& node_id,
                                 const std::string& service_address,
                                 uint64_t capacity_bytes,
                                 uint64_t free_bytes,
                                 uint64_t now_ms,
                                 std::string* error);

} // namespace zb::metrics
