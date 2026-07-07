#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include <atomic>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "httplib.h"
#include "json.hpp"
#include "scheduler.pb.h"
#include "mds.pb.h"
#include "demo_runner.h"

using json = nlohmann::json;

DEFINE_string(scheduler, "222.20.95.28:9100", "Scheduler endpoint");
DEFINE_string(mds, "222.20.95.28:9000", "MDS endpoint");
DEFINE_int32(port, 8080, "HTTP server port");
DEFINE_int32(rpc_timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(rpc_max_retry, 1, "RPC max retry");

// ---- Demo tool integration ----
DEFINE_string(demo_tool, "./system_demo_tool", "Path to system_demo_tool executable");
DEFINE_string(mount_point, "/mnt/md0/wjh/zb_run_dir_v3/mnt", "FUSE mount point for demo I/O");
DEFINE_string(optical_disc_dir, "", "Optical disc inventory dir (empty = tool default)");
DEFINE_string(optical_disc_delta_path, "", "Optical catalog delta path (empty = tool default)");
DEFINE_int32(demo_timeout_ms, 60000, "Default demo command timeout in ms");
DEFINE_int32(demo_import_timeout_ms, 600000, "Masstree import timeout in ms");

namespace {

// ---- BRPC channels (initialized once) ----
brpc::Channel g_scheduler_channel;
brpc::Channel g_mds_channel;

bool InitChannels(std::string* error) {
  brpc::ChannelOptions opts;
  opts.protocol = "baidu_std";
  opts.timeout_ms = FLAGS_rpc_timeout_ms;
  opts.max_retry = FLAGS_rpc_max_retry;

  if (g_scheduler_channel.Init(FLAGS_scheduler.c_str(), &opts) != 0) {
    if (error) *error = "Failed to init scheduler channel: " + FLAGS_scheduler;
    return false;
  }
  if (g_mds_channel.Init(FLAGS_mds.c_str(), &opts) != 0) {
    if (error) *error = "Failed to init mds channel: " + FLAGS_mds;
    return false;
  }
  return true;
}

// ---- HTTP helpers ----
void set_cors(httplib::Response& res) {
  res.set_header("Access-Control-Allow-Origin", "*");
  res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.set_header("Access-Control-Allow-Headers", "Content-Type, Authorization");
}

void send_json(httplib::Response& res, const json& body, int status = 200) {
  set_cors(res);
  res.status = status;
  res.set_content(body.dump(), "application/json; charset=utf-8");
}

void send_error(httplib::Response& res, int status, const std::string& message) {
  send_json(res,
            {{"ok", false}, {"error", {{"code", status}, {"message", message}}}},
            status);
}

// ---- Helper: map NodeType enum to frontend string ----
std::string NodeTypeStr(zb::rpc::NodeType t) {
  switch (t) {
    case zb::rpc::NODE_REAL: return "Real Node";
    case zb::rpc::NODE_VIRTUAL_POOL: return "Virtual Node";
    case zb::rpc::NODE_OPTICAL: return "Optical Node";
    default: return "Unknown";
  }
}

std::string NodeTypeColor(zb::rpc::NodeType t) {
  switch (t) {
    case zb::rpc::NODE_REAL: return "var(--accent)";
    case zb::rpc::NODE_VIRTUAL_POOL: return "var(--green)";
    case zb::rpc::NODE_OPTICAL: return "var(--amber)";
    default: return "var(--accent)";
  }
}

std::string HealthStatusStr(zb::rpc::NodeHealthState h) {
  switch (h) {
    case zb::rpc::NODE_HEALTH_HEALTHY: return "Healthy";
    case zb::rpc::NODE_HEALTH_SUSPECT: return "Warning";
    case zb::rpc::NODE_HEALTH_DEAD: return "Offline";
    default: return "Unknown";
  }
}

std::string HealthStatusClass(zb::rpc::NodeHealthState h) {
  switch (h) {
    case zb::rpc::NODE_HEALTH_HEALTHY: return "ok";
    case zb::rpc::NODE_HEALTH_SUSPECT: return "warn";
    case zb::rpc::NODE_HEALTH_DEAD: return "err";
    default: return "warn";
  }
}

// ---- Data builders (real BRPC calls) ----

json build_nodes() {
  zb::rpc::SchedulerService_Stub stub(&g_scheduler_channel);
  zb::rpc::GetClusterViewRequest req;
  req.set_min_generation(0);
  zb::rpc::GetClusterViewReply resp;
  brpc::Controller cntl;
  stub.GetClusterView(&cntl, &req, &resp, nullptr);

  if (cntl.Failed()) {
    return json::array();
  }

  json nodes = json::array();
  for (int i = 0; i < resp.nodes_size(); ++i) {
    const auto& nv = resp.nodes(i);
    uint64_t total_cap = 0;
    uint64_t used_cap = 0;
    for (int d = 0; d < nv.disks_size(); ++d) {
      total_cap += nv.disks(d).capacity_bytes();
      used_cap += (nv.disks(d).capacity_bytes() - nv.disks(d).free_bytes());
    }
    int disk_count = nv.disks_size();
    double used_pct = total_cap > 0 ? (double)used_cap / total_cap * 100.0 : 0;

    // Format capacity string
    std::string cap_str;
    double cap_tb = (double)total_cap / (1024.0 * 1024 * 1024 * 1024);
    if (cap_tb >= 1.0) {
      char buf[64];
      snprintf(buf, sizeof(buf), "%.0f TB", cap_tb);
      cap_str = buf;
    } else {
      double cap_gb = (double)total_cap / (1024.0 * 1024 * 1024);
      char buf[64];
      snprintf(buf, sizeof(buf), "%.0f GB", cap_gb);
      cap_str = buf;
    }

    nodes.push_back({
        {"id", nv.node_id()},
        {"type", NodeTypeStr(nv.node_type())},
        {"typeColor", NodeTypeColor(nv.node_type())},
        {"disks", disk_count},
        {"cap", cap_str},
        {"usedPct", (int)used_pct},
        {"status", HealthStatusStr(nv.health_state())},
        {"statusClass", HealthStatusClass(nv.health_state())},
    });
  }
  return nodes;
}

json build_capacity() {
  // Hot layer: from Scheduler cluster view (disk nodes)
  zb::rpc::SchedulerService_Stub sched_stub(&g_scheduler_channel);
  zb::rpc::GetClusterViewRequest sched_req;
  sched_req.set_min_generation(0);
  zb::rpc::GetClusterViewReply sched_resp;
  brpc::Controller sched_cntl;
  sched_stub.GetClusterView(&sched_cntl, &sched_req, &sched_resp, nullptr);

  double hot_used_tb = 0, hot_total_tb = 0;
  double cold_used_eb = 0, cold_total_eb = 0;
  if (!sched_cntl.Failed()) {
    uint64_t hot_total = 0, hot_used = 0;
    uint64_t cold_total = 0, cold_used = 0;
    for (int i = 0; i < sched_resp.nodes_size(); ++i) {
      const auto& nv = sched_resp.nodes(i);
      for (int d = 0; d < nv.disks_size(); ++d) {
        uint64_t cap = nv.disks(d).capacity_bytes();
        uint64_t free = nv.disks(d).free_bytes();
        if (nv.node_type() == zb::rpc::NODE_OPTICAL) {
          cold_total += cap;
          cold_used += (cap - free);
        } else {
          hot_total += cap;
          hot_used += (cap - free);
        }
      }
    }
    const double TB = 1024.0 * 1024 * 1024 * 1024;
    const double EB = TB * 1024 * 1024;
    hot_used_tb = (double)hot_used / TB;
    hot_total_tb = (double)hot_total / TB;
    cold_used_eb = (double)cold_used / EB;
    cold_total_eb = (double)cold_total / EB;
  }

  // Metadata from MDS
  uint64_t meta_files = 0;
  int meta_pct = 0;
  zb::rpc::MdsService_Stub mds_stub(&g_mds_channel);
  zb::rpc::GetMasstreeClusterStatsRequest mds_req;
  zb::rpc::GetMasstreeClusterStatsReply mds_resp;
  brpc::Controller mds_cntl;
  mds_stub.GetMasstreeClusterStats(&mds_cntl, &mds_req, &mds_resp, nullptr);
  if (!mds_cntl.Failed() && mds_resp.status().code() == zb::rpc::MDS_OK) {
    meta_files = mds_resp.total_file_count();
    uint64_t total_cap = 0;
    uint64_t meta_bytes = 0;
    try { total_cap = std::stoull(mds_resp.total_capacity_bytes()); } catch (...) {}
    try { meta_bytes = std::stoull(mds_resp.total_metadata_bytes()); } catch (...) {}
    meta_pct = total_cap > 0 ? (int)((double)meta_bytes / total_cap * 100) : 0;
  }

  // Format meta_files string
  std::string meta_files_str;
  if (meta_files >= 1000000000ULL) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.1fB", (double)meta_files / 1e9);
    meta_files_str = buf;
  } else if (meta_files >= 1000000ULL) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.1fM", (double)meta_files / 1e6);
    meta_files_str = buf;
  } else {
    meta_files_str = std::to_string(meta_files);
  }

  return {
      {"hotUsedTB", (int)hot_used_tb},
      {"hotTotalTB", (int)hot_total_tb},
      {"coldUsedEB", cold_used_eb},
      {"coldTotalEB", cold_total_eb},
      {"metaFiles", meta_files_str},
      {"metaPct", meta_pct},
  };
}

json build_archive_stats() {
  zb::rpc::MdsService_Stub stub(&g_mds_channel);
  zb::rpc::GetMasstreeClusterStatsRequest req;
  zb::rpc::GetMasstreeClusterStatsReply resp;
  brpc::Controller cntl;
  stub.GetMasstreeClusterStats(&cntl, &req, &resp, nullptr);

  if (cntl.Failed() || resp.status().code() != zb::rpc::MDS_OK) {
    return {{"pending", "0"}, {"batchSize", "0"}, {"totalArchived", "0"}};
  }

  // total_file_bytes is a string like "1234567890"
  std::string total_archived;
  try {
    double bytes = std::stod(resp.total_file_bytes());
    double eb = bytes / (1024.0 * 1024 * 1024 * 1024 * 1024 * 1024);
    char buf[32];
    snprintf(buf, sizeof(buf), "%.1f", eb);
    total_archived = buf;
  } catch (...) {
    total_archived = "0";
  }

  // pending and batchSize: derived from cluster cursor state
  // These are approximate values based on available data
  std::string pending = std::to_string(resp.total_file_count());
  std::string batch_size = "512";  // default batch size from config

  return {
      {"pending", pending},
      {"batchSize", batch_size},
      {"totalArchived", total_archived},
  };
}

json build_archive_records() {
  // The current BRPC backend does not have a dedicated "list archive records" RPC.
  // We return an empty array for now; this can be extended when such an RPC is added.
  // Alternatively, this could query MDS archive lease state.
  return json::array();
}

// ===========================================================================
// Demo tool integration
// ===========================================================================

// Build base run options from gflags.
zb::gateway::DemoRunOptions base_demo_opts() {
  zb::gateway::DemoRunOptions o;
  o.demo_tool = FLAGS_demo_tool;
  o.scheduler = FLAGS_scheduler;
  o.mds = FLAGS_mds;
  o.mount_point = FLAGS_mount_point;
  o.optical_disc_dir = FLAGS_optical_disc_dir;
  o.optical_delta = FLAGS_optical_disc_delta_path;
  o.timeout_ms = FLAGS_demo_timeout_ms;
  return o;
}

// Validate an identifier: letters, digits, '_', '-', '.', '/' only. Used to
// guard values that are spliced into demo menu commands fed via stdin.
bool is_safe_token(const std::string& s, bool allow_slash = false) {
  if (s.empty() || s.size() > 256) return false;
  for (char c : s) {
    const bool ok = std::isalnum(static_cast<unsigned char>(c)) || c == '_' ||
                    c == '-' || c == '.' || (allow_slash && c == '/');
    if (!ok) return false;
  }
  return true;
}

bool is_digits(const std::string& s) {
  if (s.empty() || s.size() > 20) return false;
  for (char c : s) {
    if (!std::isdigit(static_cast<unsigned char>(c))) return false;
  }
  return true;
}

// Run the demo tool interactively by feeding one menu command followed by 'q'.
// This produces the rich "结果/摘要/[group]" format that ParseDemoOutput expects.
zb::gateway::DemoResult run_menu_command(const std::string& menu_line, int timeout_ms) {
  zb::gateway::DemoRunOptions o = base_demo_opts();
  o.timeout_ms = timeout_ms;
  o.extra_args.push_back("--scenario=interactive");
  o.stdin_text = menu_line + "\nq\n";
  return zb::gateway::RunDemo(o);
}

// ---- Async import job manager ----
struct ImportJob {
  std::string id;
  std::string state;  // "running" | "completed" | "failed"
  json result;        // final DemoResult json when done
  std::string error;
};

std::mutex g_jobs_mu;
std::map<std::string, std::shared_ptr<ImportJob>> g_jobs;
std::atomic<uint64_t> g_job_seq{0};

std::string start_import_job(const std::string& namespace_id,
                             const std::string& generation_id,
                             const std::string& template_id,
                             const std::string& template_mode) {
  const std::string id = "import-" + std::to_string(++g_job_seq);
  auto job = std::make_shared<ImportJob>();
  job->id = id;
  job->state = "running";
  {
    std::lock_guard<std::mutex> lk(g_jobs_mu);
    g_jobs[id] = job;
  }

  std::thread([job, namespace_id, generation_id, template_id, template_mode]() {
    std::string line = "4";
    if (!namespace_id.empty()) line += " namespace=" + namespace_id;
    if (!generation_id.empty()) line += " generation=" + generation_id;
    if (!template_id.empty()) line += " template_id=" + template_id;
    if (!template_mode.empty()) line += " template_mode=" + template_mode;
    zb::gateway::DemoResult r = run_menu_command(line, FLAGS_demo_import_timeout_ms);
    std::lock_guard<std::mutex> lk(g_jobs_mu);
    job->result = r.ToJson();
    if (!r.error.empty()) {
      job->state = "failed";
      job->error = r.error;
    } else {
      job->state = r.ok ? "completed" : "failed";
    }
  }).detach();

  return id;
}

void register_options_handler(httplib::Server& svr) {
  svr.Options(R"(.*)", [](const httplib::Request&, httplib::Response& res) {
    set_cors(res);
    res.status = 204;
  });
}

void register_routes(httplib::Server& svr) {
  svr.Get("/api/cluster/nodes", [](const httplib::Request&, httplib::Response& res) {
    send_json(res, build_nodes());
  });

  svr.Get("/api/cluster/capacity", [](const httplib::Request&, httplib::Response& res) {
    send_json(res, build_capacity());
  });

  svr.Get("/api/archive/stats", [](const httplib::Request&, httplib::Response& res) {
    send_json(res, build_archive_stats());
  });

  svr.Get("/api/archive/records", [](const httplib::Request&, httplib::Response& res) {
    send_json(res, build_archive_records());
  });

  svr.Get("/api/health", [](const httplib::Request&, httplib::Response& res) {
    // Test scheduler connectivity
    zb::rpc::SchedulerService_Stub stub(&g_scheduler_channel);
    zb::rpc::GetClusterViewRequest req;
    req.set_min_generation(0);
    zb::rpc::GetClusterViewReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    stub.GetClusterView(&cntl, &req, &resp, nullptr);

    if (cntl.Failed()) {
      send_error(res, 503, "scheduler unreachable: " + cntl.ErrorText());
    } else {
      send_json(res, {{"status", "ok"}, {"nodes", resp.nodes_size()}});
    }
  });

  // -------- Demo: TC-P1 全局统计 (read-only) --------
  svr.Get("/api/demo/stats", [](const httplib::Request&, httplib::Response& res) {
    auto r = run_menu_command("1", FLAGS_demo_timeout_ms);
    send_json(res, r.ToJson());
  });

  // -------- Demo: TC-P2 真实节点读写 (writes 100MB) --------
  svr.Post("/api/demo/real-rw", [](const httplib::Request& req, httplib::Response& res) {
    std::string line = "2";
    // Optional dir param.
    if (req.has_param("dir")) {
      const std::string dir = req.get_param_value("dir");
      if (!is_safe_token(dir, true)) {
        return send_error(res, 400, "invalid dir");
      }
      line += " dir=" + dir;
    }
    auto r = run_menu_command(line, FLAGS_demo_timeout_ms);
    send_json(res, r.ToJson());
  });

  // -------- Demo: TC-P3 虚拟节点读写 (writes 100MB) --------
  svr.Post("/api/demo/virtual-rw", [](const httplib::Request& req, httplib::Response& res) {
    std::string line = "3";
    if (req.has_param("dir")) {
      const std::string dir = req.get_param_value("dir");
      if (!is_safe_token(dir, true)) {
        return send_error(res, 400, "invalid dir");
      }
      line += " dir=" + dir;
    }
    auto r = run_menu_command(line, FLAGS_demo_timeout_ms);
    send_json(res, r.ToJson());
  });

  // -------- Demo: TC-P5 Masstree 查询 --------
  svr.Post("/api/demo/masstree/query", [](const httplib::Request& req, httplib::Response& res) {
    std::string line = "5";
    if (req.has_param("n")) {
      const std::string n = req.get_param_value("n");
      if (!is_digits(n)) return send_error(res, 400, "invalid n");
      line += " n=" + n;
    }
    if (req.has_param("query_mode")) {
      const std::string m = req.get_param_value("query_mode");
      if (m != "random_inode" && m != "random_path_lookup") {
        return send_error(res, 400, "invalid query_mode");
      }
      line += " query_mode=" + m;
    }
    if (req.has_param("output_limit")) {
      const std::string l = req.get_param_value("output_limit");
      if (!is_digits(l)) return send_error(res, 400, "invalid output_limit");
      line += " output_limit=" + l;
    }
    auto r = run_menu_command(line, FLAGS_demo_timeout_ms);
    send_json(res, r.ToJson());
  });

  // -------- Demo: TC-P4 Masstree 导入 (async, ~1.5min) --------
  svr.Post("/api/demo/masstree/import", [](const httplib::Request& req, httplib::Response& res) {
    auto checked = [&](const char* name, bool slash, std::string* out) -> bool {
      if (req.has_param(name)) {
        const std::string v = req.get_param_value(name);
        if (!is_safe_token(v, slash)) return false;
        *out = v;
      }
      return true;
    };
    std::string ns, gen, tid, mode;
    if (!checked("namespace", false, &ns) || !checked("generation", false, &gen) ||
        !checked("template_id", false, &tid)) {
      return send_error(res, 400, "invalid import parameter");
    }
    if (req.has_param("template_mode")) {
      mode = req.get_param_value("template_mode");
      if (mode != "page_fast" && mode != "legacy_records") {
        return send_error(res, 400, "invalid template_mode");
      }
    }
    const std::string id = start_import_job(ns, gen, tid, mode);
    send_json(res, {{"ok", true}, {"jobId", id}, {"state", "running"}});
  });

  // -------- Demo: import job status --------
  svr.Get("/api/demo/masstree/import/status", [](const httplib::Request& req, httplib::Response& res) {
    if (!req.has_param("job")) return send_error(res, 400, "missing job");
    const std::string id = req.get_param_value("job");
    std::shared_ptr<ImportJob> job;
    {
      std::lock_guard<std::mutex> lk(g_jobs_mu);
      auto it = g_jobs.find(id);
      if (it != g_jobs.end()) job = it->second;
    }
    if (!job) return send_error(res, 404, "job not found");
    json j;
    j["ok"] = true;
    j["jobId"] = job->id;
    j["state"] = job->state;
    if (!job->error.empty()) j["error"] = job->error;
    if (job->state != "running") j["result"] = job->result;
    send_json(res, j);
  });

  // -------- Demo: 光盘管理 --------
  svr.Post("/api/demo/optical", [](const httplib::Request& req, httplib::Response& res) {
    const std::string op = req.has_param("op") ? req.get_param_value("op") : "stats";
    static const std::set<std::string> kOps = {"stats", "inventory_stats", "get",
                                               "list", "add", "delete"};
    if (kOps.count(op) == 0) return send_error(res, 400, "invalid op");
    std::string line = "6 op=" + op;

    if (req.has_param("target")) {
      const std::string t = req.get_param_value("target");
      if (t != "disc" && t != "library") return send_error(res, 400, "invalid target");
      line += " target=" + t;
    }
    if (req.has_param("library_id")) {
      const std::string v = req.get_param_value("library_id");
      if (!is_safe_token(v)) return send_error(res, 400, "invalid library_id");
      line += " library_id=" + v;
    }
    if (req.has_param("disc_id")) {
      const std::string v = req.get_param_value("disc_id");
      if (!is_safe_token(v)) return send_error(res, 400, "invalid disc_id");
      line += " disc_id=" + v;
    }
    if (req.has_param("capacity_bytes")) {
      const std::string v = req.get_param_value("capacity_bytes");
      if (!is_digits(v)) return send_error(res, 400, "invalid capacity_bytes");
      line += " capacity_bytes=" + v;
    }
    if (req.has_param("detail")) {
      const std::string v = req.get_param_value("detail");
      if (v != "summary" && v != "global" && v != "library") {
        return send_error(res, 400, "invalid detail");
      }
      line += " detail=" + v;
    }
    if (req.has_param("offset")) {
      const std::string v = req.get_param_value("offset");
      if (!is_digits(v)) return send_error(res, 400, "invalid offset");
      line += " offset=" + v;
    }
    if (req.has_param("limit")) {
      const std::string v = req.get_param_value("limit");
      if (!is_digits(v)) return send_error(res, 400, "invalid limit");
      line += " limit=" + v;
    }
    auto r = run_menu_command(line, FLAGS_demo_timeout_ms);
    send_json(res, r.ToJson());
  });
}

}  // namespace

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  std::string error;
  if (!InitChannels(&error)) {
    std::cerr << error << std::endl;
    return 1;
  }

  httplib::Server svr;
  register_options_handler(svr);
  register_routes(svr);

  std::cout << "ZB API gateway listening on http://0.0.0.0:" << FLAGS_port << "\n";
  std::cout << "  Scheduler: " << FLAGS_scheduler << "\n";
  std::cout << "  MDS:       " << FLAGS_mds << "\n";
  std::cout << "  DemoTool:  " << FLAGS_demo_tool << "\n";
  std::cout << "  Mount:     " << FLAGS_mount_point << "\n";
  if (!svr.listen("0.0.0.0", FLAGS_port)) {
    std::cerr << "Failed to listen on port " << FLAGS_port << "\n";
    return 1;
  }

  return 0;
}
