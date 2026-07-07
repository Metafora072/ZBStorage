#include "demo_runner.h"

#include <array>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <sstream>

#if defined(_WIN32)
// Windows fallback: subprocess spawning is not supported here. The gateway is
// intended to run on the Ubuntu server alongside the backend. Provide stubs so
// the file still compiles on dev machines.
#else
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace zb {
namespace gateway {

namespace {

std::string Trim(const std::string& s) {
  size_t b = 0;
  size_t e = s.size();
  while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) ++b;
  while (e > b && std::isspace(static_cast<unsigned char>(s[e - 1]))) --e;
  return s.substr(b, e - b);
}

bool StartsWith(const std::string& s, const std::string& p) {
  return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
}

// Extract "label (key)" -> {label, key}. Returns false if no "(key)" suffix.
bool SplitLabelKey(const std::string& field, std::string* label, std::string* key) {
  const size_t open = field.rfind('(');
  const size_t close = field.rfind(')');
  if (open == std::string::npos || close == std::string::npos || close < open) {
    return false;
  }
  *key = Trim(field.substr(open + 1, close - open - 1));
  *label = Trim(field.substr(0, open));
  return !key->empty();
}

// From a value like "48000000000000 (48.00TB)" extract raw="4800..." human="48.00TB".
void SplitValueHuman(const std::string& raw_value, std::string* value, std::string* human) {
  const size_t open = raw_value.rfind('(');
  const size_t close = raw_value.rfind(')');
  if (open != std::string::npos && close != std::string::npos && close > open) {
    *value = Trim(raw_value.substr(0, open));
    *human = Trim(raw_value.substr(open + 1, close - open - 1));
  } else {
    *value = Trim(raw_value);
    human->clear();
  }
}

}  // namespace

nlohmann::json DemoResult::ToJson() const {
  nlohmann::json j;
  j["ok"] = ok;
  j["title"] = title;
  j["summary"] = summary;
  j["command"] = command;
  if (!error.empty()) j["error"] = error;
  j["exitCode"] = exit_code;

  nlohmann::json sections_json = nlohmann::json::array();
  for (const auto& sec : sections) {
    nlohmann::json metrics_json = nlohmann::json::array();
    for (const auto& m : sec.metrics) {
      metrics_json.push_back({{"key", m.key},
                              {"label", m.label},
                              {"value", m.value},
                              {"human", m.human}});
    }
    sections_json.push_back({{"title", sec.title}, {"metrics", metrics_json}});
  }
  j["sections"] = sections_json;

  nlohmann::json samples_json = nlohmann::json::array();
  for (const auto& sample : samples) {
    nlohmann::json sample_obj = nlohmann::json::object();
    for (const auto& m : sample) {
      sample_obj[m.key] = m.value;
      if (!m.human.empty()) sample_obj[m.key + "_human"] = m.human;
    }
    samples_json.push_back(sample_obj);
  }
  j["samples"] = samples_json;

  nlohmann::json checks_json = nlohmann::json::array();
  for (const auto& c : checks) {
    checks_json.push_back({{"name", c.name}, {"ok", c.ok}, {"detail", c.detail}});
  }
  j["checks"] = checks_json;

  j["raw"] = raw;
  return j;
}

// PLACEHOLDER_PARSE
DemoResult ParseDemoOutput(const std::string& stdout_text) {
  DemoResult result;
  result.raw = stdout_text;

  std::istringstream input(stdout_text);
  std::string raw_line;
  std::string current_section;        // current [group] title
  bool in_sample = false;             // currently accumulating a query sample
  std::vector<DemoMetric> sample_acc; // current sample metrics

  auto flush_sample = [&]() {
    if (in_sample && !sample_acc.empty()) {
      result.samples.push_back(sample_acc);
    }
    sample_acc.clear();
    in_sample = false;
  };

  auto ensure_section = [&](const std::string& title) -> DemoSection& {
    for (auto& sec : result.sections) {
      if (sec.title == title) return sec;
    }
    result.sections.push_back(DemoSection{title, {}});
    return result.sections.back();
  };

  while (std::getline(input, raw_line)) {
    if (!raw_line.empty() && raw_line.back() == '\r') raw_line.pop_back();
    const std::string line = Trim(raw_line);
    if (line.empty()) continue;

    // Block separators and menu banners: skip.
    if (StartsWith(line, "====")) continue;
    if (StartsWith(line, "ZB Storage")) continue;
    if (StartsWith(line, "请输入") || StartsWith(line, "输入格式") ||
        StartsWith(line, "示例:") || StartsWith(line, "输入>") ||
        StartsWith(line, "输入") ) {
      // menu help lines; skip
      continue;
    }
    // Numbered menu items like "0) ...", "q) 退出".
    if (line.size() >= 2 && (std::isdigit(static_cast<unsigned char>(line[0])) || line[0] == 'q') &&
        line[1] == ')') {
      continue;
    }

    // Header fields.
    if (StartsWith(line, "结果:")) {
      const std::string v = Trim(line.substr(std::string("结果:").size()));
      result.ok = (v == "通过");
      continue;
    }
    if (StartsWith(line, "摘要:")) {
      result.summary = Trim(line.substr(std::string("摘要:").size()));
      continue;
    }
    if (StartsWith(line, "命令:")) {
      result.command = Trim(line.substr(std::string("命令:").size()));
      continue;
    }
    if (StartsWith(line, "用法:")) {
      continue;  // ignore usage line
    }

    // Section group header "[xxx]".
    if (line.front() == '[' && line.back() == ']') {
      flush_sample();
      current_section = line.substr(1, line.size() - 2);
      // The title line right under "====" (e.g. " TC-P1 全局统计") arrives
      // before "结果:" and is not bracketed, so handle below.
      continue;
    }

    // check.* lines.
    if (StartsWith(line, "check.") || StartsWith(line, "校验.")) {
      const size_t eq = line.find('=');
      if (eq == std::string::npos) continue;
      DemoCheck c;
      const size_t prefix_len =
          StartsWith(line, "校验.") ? std::string("校验.").size() : 6;
      c.name = line.substr(prefix_len, eq - prefix_len);
      const size_t detail_pos = line.find(" detail=\"", eq + 1);
      const std::string status = detail_pos == std::string::npos
                                     ? line.substr(eq + 1)
                                     : line.substr(eq + 1, detail_pos - (eq + 1));
      c.ok = (status == "PASS" || status == "通过");
      if (detail_pos != std::string::npos) {
        const size_t db = detail_pos + 9;
        const size_t de = (!line.empty() && line.back() == '"') ? line.size() - 1 : line.size();
        c.detail = line.substr(db, de - db);
      }
      result.checks.push_back(std::move(c));
      continue;
    }

    // Metric lines. Two shapes:
    //  (a) "label (key)   value"   (RenderResult section output, space-aligned)
    //  (b) "key=value"             (raw PrintXxx output before RenderResult)
    // Try shape (a): find "(key)" then the remainder is the value.
    std::string label, key, value, human;
    const size_t close_paren = line.find(')');
    bool parsed = false;
    if (close_paren != std::string::npos && line.find('(') != std::string::npos &&
        line.find('(') < close_paren) {
      // The field up to and including ')' is "label (key)", rest is value.
      const std::string field = Trim(line.substr(0, close_paren + 1));
      const std::string rest = Trim(line.substr(close_paren + 1));
      if (SplitLabelKey(field, &label, &key) && !rest.empty()) {
        SplitValueHuman(rest, &value, &human);
        parsed = true;
      }
    }
    if (!parsed) {
      // Shape (b): key=value (key has no spaces).
      const size_t eq = line.find('=');
      if (eq != std::string::npos) {
        const std::string lhs = Trim(line.substr(0, eq));
        if (!lhs.empty() && lhs.find(' ') == std::string::npos &&
            lhs.find('(') == std::string::npos) {
          key = lhs;
          label.clear();
          SplitValueHuman(Trim(line.substr(eq + 1)), &value, &human);
          parsed = true;
        }
      }
    }
    if (!parsed) {
      // Title line (e.g. "TC-P1 全局统计") or packed optical summary line.
      // If it looks like a section/title under the banner and we have no title
      // yet, record it as the title.
      if (result.title.empty() && line.find('=') == std::string::npos &&
          current_section.empty()) {
        result.title = line;
      } else {
        // Keep packed lines (optical "光盘库ID lib_xxx ...") as a metric in the
        // current section so the frontend can display the raw text.
        DemoMetric m;
        m.key = "_text";
        m.label = "";
        m.value = line;
        ensure_section(current_section.empty() ? "信息" : current_section)
            .metrics.push_back(std::move(m));
      }
      continue;
    }

    // Sample boundary detection: a new sample_index starts a new sample.
    if (key == "sample_index") {
      flush_sample();
      in_sample = true;
    }

    DemoMetric metric;
    metric.key = key;
    metric.label = label;
    metric.value = value;
    metric.human = human;

    if (in_sample) {
      sample_acc.push_back(metric);
    } else {
      ensure_section(current_section.empty() ? "关键指标" : current_section)
          .metrics.push_back(metric);
    }
  }
  flush_sample();
  return result;
}

// PLACEHOLDER_SPAWN
#if defined(_WIN32)

DemoResult RunDemo(const DemoRunOptions& opts) {
  DemoResult r;
  r.ok = false;
  r.error = "demo runner is not supported on Windows; run the gateway on the server";
  return r;
}

#else

namespace {

// Build the argv vector for the demo tool.
std::vector<std::string> BuildArgs(const DemoRunOptions& opts) {
  std::vector<std::string> args;
  args.push_back(opts.demo_tool);
  args.push_back("--scheduler=" + opts.scheduler);
  args.push_back("--mds=" + opts.mds);
  if (!opts.mount_point.empty()) args.push_back("--mount_point=" + opts.mount_point);
  if (!opts.optical_disc_dir.empty())
    args.push_back("--optical_disc_dir=" + opts.optical_disc_dir);
  if (!opts.optical_delta.empty())
    args.push_back("--optical_disc_delta_path=" + opts.optical_delta);
  // Avoid writing demo log files from the gateway-driven runs.
  args.push_back("--enable_log_file=false");
  for (const auto& a : opts.extra_args) args.push_back(a);
  return args;
}

}  // namespace

DemoResult RunDemo(const DemoRunOptions& opts) {
  DemoResult result;

  int out_pipe[2];  // child stdout -> parent
  int in_pipe[2];   // parent -> child stdin
  if (pipe(out_pipe) != 0 || pipe(in_pipe) != 0) {
    result.error = "pipe() failed";
    return result;
  }

  const std::vector<std::string> args = BuildArgs(opts);

  pid_t pid = fork();
  if (pid < 0) {
    result.error = "fork() failed";
    return result;
  }

  if (pid == 0) {
    // Child.
    dup2(in_pipe[0], STDIN_FILENO);
    dup2(out_pipe[1], STDOUT_FILENO);
    dup2(out_pipe[1], STDERR_FILENO);
    close(in_pipe[0]);
    close(in_pipe[1]);
    close(out_pipe[0]);
    close(out_pipe[1]);

    std::vector<char*> c_args;
    c_args.reserve(args.size() + 1);
    for (const auto& a : args) c_args.push_back(const_cast<char*>(a.c_str()));
    c_args.push_back(nullptr);
    execv(opts.demo_tool.c_str(), c_args.data());
    // If execv returns, it failed.
    const char* msg = "execv failed\n";
    write(STDERR_FILENO, msg, std::strlen(msg));
    _exit(127);
  }

  // Parent.
  close(in_pipe[0]);
  close(out_pipe[1]);

  // Feed stdin (interactive mode) then close to signal EOF.
  if (!opts.stdin_text.empty()) {
    const std::string& s = opts.stdin_text;
    size_t off = 0;
    while (off < s.size()) {
      ssize_t n = write(in_pipe[1], s.data() + off, s.size() - off);
      if (n <= 0) break;
      off += static_cast<size_t>(n);
    }
  }
  close(in_pipe[1]);

  // Read stdout with a timeout.
  std::string output;
  const int fd = out_pipe[0];
  fcntl(fd, F_SETFL, O_NONBLOCK);
  const long deadline_ms = opts.timeout_ms;
  long elapsed_ms = 0;
  const int poll_step_ms = 100;
  bool timed_out = false;
  std::array<char, 8192> buf{};
  for (;;) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    int pr = poll(&pfd, 1, poll_step_ms);
    if (pr > 0 && (pfd.revents & (POLLIN | POLLHUP))) {
      ssize_t n = read(fd, buf.data(), buf.size());
      if (n > 0) {
        output.append(buf.data(), static_cast<size_t>(n));
        continue;  // drain quickly
      }
      if (n == 0) break;  // EOF: child closed stdout
    }
    elapsed_ms += poll_step_ms;
    if (deadline_ms > 0 && elapsed_ms >= deadline_ms) {
      timed_out = true;
      break;
    }
  }
  close(fd);

  int status = 0;
  if (timed_out) {
    kill(pid, SIGKILL);
  }
  waitpid(pid, &status, 0);

  result = ParseDemoOutput(output);
  if (timed_out) {
    result.ok = false;
    result.error = "demo tool timed out after " + std::to_string(opts.timeout_ms) + "ms";
  } else if (WIFEXITED(status)) {
    result.exit_code = WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    result.exit_code = 128 + WTERMSIG(status);
  }
  return result;
}

#endif  // _WIN32


}  // namespace gateway
}  // namespace zb
