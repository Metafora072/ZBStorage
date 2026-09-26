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

#include "../config/VirtualNodeConfig.h"
#include "../service/BrpcVirtualStorageService.h"
#include "../service/VirtualStorageServiceImpl.h"
#include "common/metrics/SchedulerMetricsClient.h"
#include "mds.pb.h"
#include "scheduler.pb.h"

DEFINE_string(config, "", "Path to virtual node config file");
DEFINE_int32(port, 29080, "Port for virtual node brpc server");
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
                               uint64_t read_bandwidth_bytes_per_sec,
                               uint64_t write_bandwidth_bytes_per_sec,
                               zb::virtual_node::VirtualStorageServiceImpl* service,
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
          read_bandwidth_bytes_per_sec_(read_bandwidth_bytes_per_sec),
          write_bandwidth_bytes_per_sec_(write_bandwidth_bytes_per_sec),
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
            request.set_node_type(zb::rpc::NODE_VIRTUAL_POOL);
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
            request.set_readiness_message("virtual node storage initialized");

            zb::msg::DiskReportReply reports = service_->GetDiskReport();
            for (const auto& disk : reports.reports) {
                zb::rpc::DiskHeartbeat* out = request.add_disks();
                out->set_disk_id(disk.id);
                out->set_capacity_bytes(disk.capacity_bytes);
                out->set_free_bytes(disk.free_bytes);
                out->set_is_healthy(disk.is_healthy);
                out->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD);
                out->set_read_bandwidth_bytes_per_sec(read_bandwidth_bytes_per_sec_);
                out->set_write_bandwidth_bytes_per_sec(write_bandwidth_bytes_per_sec_);
            }

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
                std::string metrics_error;
                if (!zb::metrics::ReportCollectedMetrics(channel.get(),
                                                         node_id_,
                                                         metrics_,
                                                         zb::rpc::MANAGED_ACCESS_DEFAULT,
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
    uint64_t read_bandwidth_bytes_per_sec_{0};
    uint64_t write_bandwidth_bytes_per_sec_{0};
    zb::virtual_node::VirtualStorageServiceImpl* service_{};
    zb::metrics::NodeMetricsCollector* metrics_{};
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

class ArchiveCandidateReporter {
public:
    ArchiveCandidateReporter(std::string mds_addr,
                             std::string node_id,
                             std::string node_address,
                             uint32_t interval_ms,
                             uint32_t topk,
                             uint64_t min_age_ms,
                             zb::virtual_node::VirtualStorageServiceImpl* storage_service)
        : mds_addr_(std::move(mds_addr)),
          node_id_(std::move(node_id)),
          node_address_(std::move(node_address)),
          interval_ms_(interval_ms == 0 ? 3000 : interval_ms),
          topk_(topk == 0 ? 1 : topk),
          min_age_ms_(min_age_ms),
          storage_service_(storage_service) {}

    bool Start() {
        if (mds_addr_.empty() || !storage_service_) {
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

    ~ArchiveCandidateReporter() { Stop(); }

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
                if (new_channel->Init(mds_addr_.c_str(), &options) != 0) {
                    std::cerr << "Failed to init MDS channel: " << mds_addr_ << std::endl;
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                    continue;
                }
                channel = std::move(new_channel);
            }

            std::vector<zb::virtual_node::FileArchiveCandidateStat> samples =
                storage_service_->CollectFileArchiveCandidates(topk_, min_age_ms_);
            if (!samples.empty()) {
                zb::rpc::MdsService_Stub stub(channel.get());
                zb::rpc::ReportFileArchiveCandidatesRequest request;
                request.set_node_id(node_id_);
                request.set_node_address(node_address_);
                request.set_report_ts_ms(NowMs());
                for (const auto& sample : samples) {
                    zb::rpc::FileArchiveCandidate* candidate = request.add_candidates();
                    candidate->set_inode_id(sample.inode_id);
                    candidate->set_node_id(node_id_);
                    candidate->set_node_address(node_address_);
                    candidate->set_disk_id(sample.disk_id);
                    candidate->set_file_size(sample.file_size);
                    candidate->set_object_count(sample.object_count);
                    candidate->set_last_access_ts_ms(sample.last_access_ts_ms);
                    candidate->set_archive_state(sample.archive_state);
                    candidate->set_version(sample.version);
                    candidate->set_report_ts_ms(request.report_ts_ms());
                }

                zb::rpc::ReportFileArchiveCandidatesReply response;
                brpc::Controller cntl;
                stub.ReportFileArchiveCandidates(&cntl, &request, &response, nullptr);
                if (cntl.Failed()) {
                    std::cerr << "ReportFileArchiveCandidates failed: " << cntl.ErrorText() << std::endl;
                    channel.reset();
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
        }
    }

    std::string mds_addr_;
    std::string node_id_;
    std::string node_address_;
    uint32_t interval_ms_{3000};
    uint32_t topk_{1};
    uint64_t min_age_ms_{30000};
    zb::virtual_node::VirtualStorageServiceImpl* storage_service_{};
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
    zb::virtual_node::VirtualNodeConfig cfg =
        zb::virtual_node::VirtualNodeConfig::LoadFromFile(FLAGS_config, &config_error);
    if (!config_error.empty()) {
        std::cerr << "Failed to load config: " << config_error << std::endl;
        return 1;
    }

    zb::virtual_node::VirtualStorageServiceImpl storage_service(cfg);
    storage_service.SetArchiveTrackingMaxObjects(cfg.archive_track_max_objects);
    std::string archive_meta_dir = cfg.archive_meta_dir;
    if (archive_meta_dir.empty()) {
        archive_meta_dir = (std::filesystem::path(".") /
                            ("virtual_archive_meta_" + std::to_string(FLAGS_port))).string();
    }
    storage_service.SetFileMetaStoreDir((std::filesystem::path(archive_meta_dir) / "file_meta").string());
    std::string archive_meta_error;
    if (!storage_service.InitArchiveMetaStore(archive_meta_dir,
                                              cfg.archive_track_max_objects,
                                              cfg.archive_meta_snapshot_interval_ops,
                                              cfg.archive_meta_wal_fsync,
                                              &archive_meta_error)) {
        std::cerr << "Archive meta init failed: " << archive_meta_error << std::endl;
        return 1;
    }
    zb::metrics::NodeMetricsCollector node_metrics;
    zb::virtual_node::BrpcVirtualStorageService brpc_service(&storage_service, &node_metrics);

    brpc::Server server;
    if (server.AddService(&brpc_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add brpc service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    std::string node_id = cfg.node_id.empty() ? ("virtual-pool-" + std::to_string(FLAGS_port)) : cfg.node_id;
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
                                        cfg.read_bytes_per_sec,
                                        cfg.write_bytes_per_sec,
                                        &storage_service,
                                        &node_metrics);
    ArchiveCandidateReporter archive_reporter(cfg.mds_addr,
                                              node_id,
                                              node_address,
                                              cfg.archive_report_interval_ms,
                                              cfg.archive_report_topk,
                                              cfg.archive_report_min_age_ms,
                                              &storage_service);
    if (!cfg.mds_addr.empty()) {
        archive_reporter.Start();
    }

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start brpc server on port " << FLAGS_port << std::endl;
        return 1;
    }
    if (!cfg.scheduler_addr.empty()) reporter.Start();

    server.RunUntilAskedToQuit();
    std::string snapshot_error;
    if (!storage_service.FlushArchiveMetaSnapshot(&snapshot_error) && !snapshot_error.empty()) {
        std::cerr << "Archive meta snapshot flush failed: " << snapshot_error << std::endl;
    }
    archive_reporter.Stop();
    reporter.Stop();
    return 0;
}
