#ifndef ZB_GATEWAY_DEMO_RUNNER_H
#define ZB_GATEWAY_DEMO_RUNNER_H

#include <string>
#include <utility>
#include <vector>

#include "json.hpp"

namespace zb {
namespace gateway {

// One key=value metric parsed from the demo tool stdout.
struct DemoMetric {
  std::string key;    // e.g. "real_total_capacity_bytes"
  std::string label;  // e.g. "真实层总容量字节" (display name, may be empty)
  std::string value;  // raw numeric/text value, e.g. "48000000000000"
  std::string human;  // friendly unit if present, e.g. "48.00TB" (may be empty)
};

// A [group] section in the demo tool output.
struct DemoSection {
  std::string title;  // e.g. "容量信息"
  std::vector<DemoMetric> metrics;
};

// A parsed check.* line.
struct DemoCheck {
  std::string name;
  bool ok{false};
  std::string detail;
};

// The structured result of running one demo command.
struct DemoResult {
  bool ok{false};
  std::string title;    // e.g. "TC-P1 全局统计"
  std::string summary;  // e.g. "TC-P1 全局统计通过"
  std::string command;  // e.g. "1"
  std::vector<DemoSection> sections;
  // Query samples (TC-P5): each sample is a flat list of metrics grouped by
  // sample_index boundaries.
  std::vector<std::vector<DemoMetric>> samples;
  std::vector<DemoCheck> checks;
  std::string raw;       // full raw stdout, for fallback display
  std::string error;     // process-level error (spawn/timeout); empty if none
  int exit_code{0};

  nlohmann::json ToJson() const;
};

// Options for spawning the demo tool.
struct DemoRunOptions {
  std::string demo_tool;        // path to system_demo_tool executable
  std::string scheduler;        // scheduler endpoint
  std::string mds;              // mds endpoint
  std::string mount_point;      // FUSE mount point
  std::string optical_disc_dir; // optical inventory dir (may be empty)
  std::string optical_delta;    // optical catalog delta path (may be empty)
  int timeout_ms{120000};       // kill the child after this long
  // Extra gflags args (already validated/escaped by caller), e.g.
  // {"--scenario=stats"} or {"--masstree_query_samples=100"}.
  std::vector<std::string> extra_args;
  // Optional stdin text to feed (interactive optical mode). When non-empty the
  // tool runs in interactive scenario and reads these lines.
  std::string stdin_text;
};

// Run the demo tool once and parse its output into a DemoResult. Never throws;
// on failure DemoResult.error is set and ok=false.
DemoResult RunDemo(const DemoRunOptions& opts);

// Parse demo tool stdout (one or more result blocks) into a DemoResult.
// When the output contains multiple result blocks (interactive multi-command),
// the LAST block's header is used and metrics are merged. Exposed for testing.
DemoResult ParseDemoOutput(const std::string& stdout_text);

}  // namespace gateway
}  // namespace zb

#endif  // ZB_GATEWAY_DEMO_RUNNER_H
