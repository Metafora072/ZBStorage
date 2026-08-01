#include <brpc/channel.h>
#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>
#include <utility>

#include "../config/OpticalNodeConfig.h"
#include "../service/BrpcOpticalNodeService.h"
#include "../service/OpticalStorageServiceImpl.h"
#include "scheduler.pb.h"

DEFINE_string(config, "", "Path to optical node config file");
DEFINE_int32(port, 39080, "Port for optical node brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

namespace {

uint64_t NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

class SchedulerHeartbeatReporter {
public:
    SchedulerHeartbeatReporter(std::string scheduler_addr,
                               std::string node_id,
                               std::string node_address,
                               std::string group_id,
                               zb::rpc::NodeRole configured_role,
                               std::string peer_node_id,
                               std::string peer_address,
                               uint32_t node_weight,
                               uint32_t virtual_node_count,
                               uint32_t interval_ms,
                               uint32_t library_disc_slots,
                               uint64_t read_bandwidth_bytes_per_sec,
                               uint64_t write_bandwidth_bytes_per_sec,
                               std::string staging_path,
                               zb::optical_node::OpticalStorageServiceImpl* service,
                               zb::metrics::NodeMetricsCollector* metrics)
        : scheduler_addr_(std::move(scheduler_addr)),
          node_id_(std::move(node_id)),
          node_address_(std::move(node_address)),
          group_id_(std::move(group_id)),
          configured_role_(configured_role),
          peer_node_id_(std::move(peer_node_id)),
          peer_address_(std::move(peer_address)),
          node_weight_(node_weight == 0 ? 1 : node_weight),
          virtual_node_count_(virtual_node_count == 0 ? 1 : virtual_node_count),
          interval_ms_(interval_ms == 0 ? 2000 : interval_ms),
          library_disc_slots_(library_disc_slots == 0 ? 10000 : library_disc_slots),
          read_bandwidth_bytes_per_sec_(read_bandwidth_bytes_per_sec),
          write_bandwidth_bytes_per_sec_(write_bandwidth_bytes_per_sec),
          staging_path_(std::move(staging_path)),
          service_(service),
          metrics_(metrics) {}

    bool Start() {
        if (scheduler_addr_.empty() || !service_) {
            return false;
        }
        stop_.store(false);
        thread_ = std::thread([this]() { Run(); });
        return true;
    }

    void Stop() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    ~SchedulerHeartbeatReporter() { Stop(); }

private:
    void Run() {
        std::unique_ptr<brpc::Channel> channel;
        while (!stop_.load()) {
            if (!channel) {
                auto new_channel = std::make_unique<brpc::Channel>();
                brpc::ChannelOptions options;
                options.protocol = "baidu_std";
                options.timeout_ms = 2000;
                options.max_retry = 0;
                if (new_channel->Init(scheduler_addr_.c_str(), &options) != 0) {
                    std::cerr << "Failed to init Scheduler channel: " << scheduler_addr_ << std::endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                    continue;
                }
                channel = std::move(new_channel);
            }
            zb::rpc::SchedulerService_Stub stub(channel.get());
            zb::rpc::HeartbeatRequest request;
            request.set_node_id(node_id_);
            request.set_node_type(zb::rpc::NODE_OPTICAL);
            request.set_address(node_address_);
            request.set_weight(node_weight_);
            request.set_virtual_node_count(virtual_node_count_);
            request.set_report_ts_ms(NowMs());
            request.set_group_id(group_id_);
            request.set_role(configured_role_);
            request.set_peer_node_id(peer_node_id_);
            request.set_peer_address(peer_address_);
            request.set_applied_lsn(service_->GetReplicationStatus().applied_lsn);
            request.set_readiness_reported(true);
            request.set_initialization_complete(true);
            request.set_metadata_ready(true);
            request.set_readiness_message("optical image store initialized");

            zb::rpc::HeartbeatReply response;
            brpc::Controller cntl;
            stub.ReportHeartbeat(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                std::cerr << "Scheduler heartbeat failed: " << cntl.ErrorText() << std::endl;
                channel.reset();
            } else if (response.status().code() == zb::rpc::SCHED_OK) {
                bool is_primary = response.assigned_role() == zb::rpc::NODE_ROLE_PRIMARY;
                configured_role_ = response.assigned_role();
                service_->ApplySchedulerAssignment(is_primary,
                                                   response.epoch(),
                                                   response.group_id(),
                                                   response.primary_node_id(),
                                                   response.primary_address(),
                                                   response.secondary_node_id(),
                                                   response.secondary_address());
                zb::rpc::ReportOpticalLibraryStatusRequest status_request;
                status_request.set_node_id(node_id_);
                auto* status = status_request.mutable_status();
                status->set_observed(true);
                status->set_total_slots(std::max<uint32_t>(library_disc_slots_, reports.reports.size()));
                status->set_local_disc_count(static_cast<uint32_t>(reports.reports.size()));
                for (const auto& disc : reports.reports) {
                    if (!disc.is_healthy) {
                        status->set_defective_disc_count(status->defective_disc_count() + 1);
                    } else if (disc.free_bytes == disc.capacity_bytes) {
                        status->set_blank_disc_count(status->blank_disc_count() + 1);
                    } else {
                        status->set_recorded_disc_count(status->recorded_disc_count() + 1);
                    }
                }
                status->set_observed_at_ms(NowMs());
                std::error_code space_error;
                const auto space = std::filesystem::space(staging_path_, space_error);
                if (!space_error) {
                    status->set_staging_capacity_bytes(space.capacity);
                    status->set_staging_used_bytes(space.capacity - space.available);
                }
                // Writes in the current optical service are synchronous; the
                // controller therefore has no hidden queued write after an RPC returns.
                status->set_pending_write_tasks(0);
                zb::rpc::ManagedNodeReply status_reply;
                brpc::Controller status_cntl;
                stub.ReportOpticalLibraryStatus(&status_cntl, &status_request, &status_reply, nullptr);
                if (status_cntl.Failed() ||
                    status_reply.status().code() != zb::rpc::SCHED_OK) {
                    std::cerr << "Optical library inventory report failed: "
                              << (status_cntl.Failed() ? status_cntl.ErrorText()
                                                      : status_reply.status().message())
                              << std::endl;
                }
                std::string metrics_error;
                if (!zb::metrics::ReportCollectedMetrics(channel.get(),
                                                         node_id_,
                                                         metrics_,
                                                         zb::rpc::MANAGED_ACCESS_OPTICAL_DISC,
                                                         NowMs(),
                                                         &metrics_error)) {
                    std::cerr << "Scheduler metrics report failed: " << metrics_error << std::endl;
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        }
    }

    std::string scheduler_addr_;
    std::string node_id_;
    std::string node_address_;
    std::string group_id_;
    zb::rpc::NodeRole configured_role_{zb::rpc::NODE_ROLE_PRIMARY};
    std::string peer_node_id_;
    std::string peer_address_;
    uint32_t node_weight_{1};
    uint32_t virtual_node_count_{1};
    uint32_t interval_ms_{2000};
    uint32_t library_disc_slots_{10000};
    uint64_t read_bandwidth_bytes_per_sec_{100000000ULL};
    uint64_t write_bandwidth_bytes_per_sec_{10000000ULL};
    std::string staging_path_;
    zb::optical_node::OpticalStorageServiceImpl* service_{};
    zb::metrics::NodeMetricsCollector* metrics_{};
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

zb::rpc::NodeRole ParseRole(const std::string& role) {
    std::string lowered = role;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (lowered == "secondary" || lowered == "slave") {
        return zb::rpc::NODE_ROLE_SECONDARY;
    }
    return zb::rpc::NODE_ROLE_PRIMARY;
}

} // namespace

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string config_error;
    zb::optical_node::OpticalNodeConfig cfg =
        zb::optical_node::OpticalNodeConfig::LoadFromFile(FLAGS_config, &config_error);
    if (!config_error.empty()) {
        std::cerr << "Failed to load config: " << config_error << std::endl;
        return 1;
    }

    zb::optical_node::OpticalStorageServiceImpl storage_service(cfg);
    if (!storage_service.IsArchiveEngineReady()) {
        std::cerr << "Failed to start archive engine: "
                  << storage_service.GetArchiveStatusDetail() << std::endl;
        return 1;
    }
    zb::optical_node::BrpcOpticalNodeService brpc_optical_node_service(&storage_service);

    brpc::Server server;
    if (server.AddService(&brpc_optical_node_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add brpc optical node service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    std::string node_id = cfg.node_id.empty() ? ("optical-node-" + std::to_string(FLAGS_port)) : cfg.node_id;
    std::string node_address = cfg.node_address.empty()
                                   ? ("127.0.0.1:" + std::to_string(FLAGS_port))
                                   : cfg.node_address;
    std::string group_id = cfg.group_id.empty() ? node_id : cfg.group_id;
    zb::rpc::NodeRole configured_role = ParseRole(cfg.node_role);
    storage_service.ConfigureReplication(node_id,
                                         group_id,
                                         cfg.replication_enabled,
                                         configured_role == zb::rpc::NODE_ROLE_PRIMARY,
                                         cfg.peer_node_id,
                                         cfg.peer_address,
                                         cfg.replication_timeout_ms);

    SchedulerHeartbeatReporter reporter(cfg.scheduler_addr,
                                        node_id,
                                        node_address,
                                        group_id,
                                        configured_role,
                                        cfg.peer_node_id,
                                        cfg.peer_address,
                                        cfg.node_weight,
                                        cfg.virtual_node_count,
                                        cfg.heartbeat_interval_ms,
                                        cfg.library_disc_slots,
                                        cfg.optical_read_bytes_per_sec,
                                        cfg.optical_write_bytes_per_sec,
                                        cfg.cache_root,
                                        &storage_service,
                                        &node_metrics);
    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start brpc server on port " << FLAGS_port << std::endl;
        return 1;
    }

    if (!cfg.scheduler_addr.empty()) reporter.Start();

    server.RunUntilAskedToQuit();
    reporter.Stop();
    return 0;
}
