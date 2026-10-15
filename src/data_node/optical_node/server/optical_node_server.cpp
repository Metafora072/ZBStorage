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

// 心跳设备清单所需的静态量（从节点配置取），避免把整个 cfg 传入 reporter。
// 库存快照只给「镜像个数 / 盘片数」等计数，这里提供换算成字节所需的单卷镜像大小与单盘容量。
struct HeartbeatInventoryConfig {
    uint64_t volume_size_bytes{0};           // 单卷镜像大小（换算 cache 容量）
    uint64_t disc_capacity_bytes{0};         // 单盘容量
    uint64_t capacity_in_images{0};          // image_dir 镜像数上限
    uint64_t cache_read_bytes_per_sec{0};    // hdd-cache 读带宽
    uint64_t cache_write_bytes_per_sec{0};   // hdd-cache 写带宽
    uint64_t drive_read_bytes_per_sec{0};    // 单光驱读带宽
    uint64_t drive_write_bytes_per_sec{0};   // 单光驱写带宽
};

// 一次性注册光盘库受管节点身份。启动阶段同步执行，带有限次重试；成功后进程内不再发送
// 注册请求（重复发送也会被服务端 ALREADY_EXISTS 幂等吸收）。
// 与心跳上报分离：注册只在生命周期入口执行一次，心跳是周期性运行态上报。
class OpticalNodeSchedulerRegistrar {
public:
    OpticalNodeSchedulerRegistrar(std::string scheduler_addr,
                                  zb::rpc::RegisterManagedNodeRequest register_request,
                                  uint32_t interval_ms)
        : scheduler_addr_(std::move(scheduler_addr)),
          register_request_(std::move(register_request)),
          interval_ms_(interval_ms == 0 ? 2000 : interval_ms) {}

    // 同步注册：返回是否已确认（SCHED_OK / SCHED_ALREADY_EXISTS）。
    // 传输层失败重试至多 kMaxAttempts 次；服务端明确拒绝则立即停止并回填 *error。
    bool Register(std::string* error) {
        for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
            if (!EnsureChannel()) {
                if (attempt < kMaxAttempts) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                }
                continue;
            }
            std::string attempt_error;
            const RegisterOutcome outcome = TryRegister(&attempt_error);
            if (outcome == RegisterOutcome::kRegistered) {
                return true;
            }
            if (error) *error = attempt_error;
            if (outcome == RegisterOutcome::kRejected) {
                return false;
            }
            std::cerr << "Scheduler managed-node registration failed (attempt " << attempt << "/"
                      << kMaxAttempts << "): " << attempt_error << std::endl;
            channel_.reset();
            if (attempt < kMaxAttempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
            }
        }
        return false;
    }

private:
    // 单次注册尝试的结果：已注册（含 ALREADY_EXISTS）/ 服务端明确拒绝 / 传输层失败可重试。
    enum class RegisterOutcome { kRegistered, kRejected, kTransient };

    // 出站 channel 懒初始化；失败返回 false，由调用方按间隔重试。
    bool EnsureChannel() {
        if (channel_) {
            return true;
        }
        auto channel = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = 2000;
        options.max_retry = 0;
        if (channel->Init(scheduler_addr_.c_str(), &options) != 0) {
            std::cerr << "Failed to init Scheduler channel: " << scheduler_addr_ << std::endl;
            return false;
        }
        channel_ = std::move(channel);
        return true;
    }

    // 发送一次 RegisterManagedNode。请求内容由 BuildRegisterRequest 从节点配置逐项构建
    // （设备组、功率、可靠性、光盘延迟、代际），此处只补 join_time_ms / activate_immediately。
    RegisterOutcome TryRegister(std::string* error) {
        zb::rpc::RegisterManagedNodeRequest request = register_request_;
        request.set_join_time_ms(NowMs());
        // 不立即激活：真实盘片/任务清单由心跳与 ReportOpticalLibraryStatus 补齐，
        // 节点须在库存就绪后才进入 WORKING，避免仅凭注册就取得写入准入。
        request.set_activate_immediately(false);

        zb::rpc::SchedulerService_Stub stub(channel_.get());
        zb::rpc::ManagedNodeReply response;
        brpc::Controller cntl;
        stub.RegisterManagedNode(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            if (error) *error = cntl.ErrorText();
            return RegisterOutcome::kTransient;
        }
        if (response.status().code() == zb::rpc::SCHED_OK ||
            response.status().code() == zb::rpc::SCHED_ALREADY_EXISTS) {
            WarnIfProfileMismatch(request.spec(), response);
            return RegisterOutcome::kRegistered;
        }
        if (error) *error = response.status().message();
        return RegisterOutcome::kRejected;
    }

    // 校验服务端最终采纳的 profile 是否等于我们发送的 spec，防止被静默覆盖：
    //   - SCHED_OK 下应完全一致；
    //   - SCHED_ALREADY_EXISTS 下若命中“之前由心跳自动注册的默认模板”，就会不一致。
    // 不一致只告警、不回退：注册本身已确认成功，但节点属性可能非预期。
    void WarnIfProfileMismatch(const zb::rpc::ManagedNodeSpec& intended,
                               const zb::rpc::ManagedNodeReply& reply) {
        if (!reply.has_node()) {
            std::cerr << "Scheduler registration reply has no node view; cannot verify profile"
                      << std::endl;
            return;
        }
        const zb::rpc::ManagedNodeSpec& actual = reply.node().spec();
        if (intended.SerializeAsString() == actual.SerializeAsString()) {
            return;
        }
        std::cerr << "Scheduler registration profile mismatch (node=" << actual.node_id()
                  << "): device_groups expected=" << intended.device_groups_size()
                  << " actual=" << actual.device_groups_size()
                  << ", logical_node_count actual=" << actual.logical_node_count()
                  << ", participates_in_placement actual=" << actual.participates_in_placement()
                  << ", peak_milliwatts actual="
                  << (actual.has_power() ? actual.power().peak_milliwatts() : 0)
                  << "; the node may be running with a different profile than configured"
                  << std::endl;
    }

    std::string scheduler_addr_;
    zb::rpc::RegisterManagedNodeRequest register_request_;
    uint32_t interval_ms_{2000};
    std::unique_ptr<brpc::Channel> channel_;

    // 注册尝试上限（每次间隔 interval_ms_）；用尽后由心跳的隐式自动注册兜底。
    static constexpr int kMaxAttempts = 5;
};

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
                               HeartbeatInventoryConfig inventory_config,
                               zb::optical_node::OpticalStorageServiceImpl* service)
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
          inventory_config_(inventory_config),
          service_(service) {}

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
    // 出站 channel 懒初始化；失败返回 false，由调用方按间隔重试。
    bool EnsureChannel(std::unique_ptr<brpc::Channel>* channel) {
        if (*channel) {
            return true;
        }
        auto new_channel = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = 2000;
        options.max_retry = 0;
        if (new_channel->Init(scheduler_addr_.c_str(), &options) != 0) {
            std::cerr << "Failed to init Scheduler channel: " << scheduler_addr_ << std::endl;
            return false;
        }
        *channel = std::move(new_channel);
        return true;
    }

    // 上报光盘库专属状态：盘片分类（blank/recording/recorded/defective）、暂存区、后台任务、忙光驱。
    // 口径：local = 光盘数量；recording = cd_manager 正在执行的刻录任务数；recorded = 已用光盘数；
    // defective 恒 0；blank = local − recording − recorded − defective；total_slots = 光盘架槽位数。
    void ReportLibraryStatus(zb::rpc::SchedulerService_Stub& stub) {
        const auto inv = service_->GetLibraryInventory();
        const uint32_t disc_count =
            static_cast<uint32_t>(std::min<uint64_t>(inv.disc_count, 0xFFFFFFFFull));
        const uint32_t total_slots = static_cast<uint32_t>(
            std::min<uint64_t>(std::max(inv.slot_count, inv.disc_count), 0xFFFFFFFFull));
        const uint32_t recorded =
            static_cast<uint32_t>(std::min<uint64_t>(inv.disc_used, disc_count));
        const uint32_t recording =
            static_cast<uint32_t>(std::min<uint64_t>(inv.disc_burning, disc_count - recorded));
        const uint64_t staging_capacity =
            inventory_config_.capacity_in_images * inventory_config_.volume_size_bytes;
        const uint64_t staging_used =
            std::min(inv.staging_used_images, inventory_config_.capacity_in_images) *
            inventory_config_.volume_size_bytes;

        zb::rpc::ReportOpticalLibraryStatusRequest request;
        request.set_node_id(node_id_);
        auto* status = request.mutable_status();
        status->set_observed(true);
        status->set_total_slots(total_slots);
        status->set_local_disc_count(disc_count);
        status->set_blank_disc_count(disc_count - recorded - recording);
        status->set_recording_disc_count(recording);
        status->set_recorded_disc_count(recorded);
        status->set_defective_disc_count(0);
        status->set_staging_capacity_bytes(staging_capacity);
        status->set_staging_used_bytes(staging_used);
        status->set_pending_write_tasks(
            static_cast<uint32_t>(std::min<uint64_t>(inv.pending_write_tasks, 0xFFFFFFFFull)));
        status->set_pending_read_tasks(
            static_cast<uint32_t>(std::min<uint64_t>(inv.pending_read_tasks, 0xFFFFFFFFull)));
        status->set_busy_drives(inv.drive_busy);
        status->set_observed_at_ms(NowMs());

        zb::rpc::ManagedNodeReply response;
        brpc::Controller cntl;
        stub.ReportOpticalLibraryStatus(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            std::cerr << "Scheduler optical library status failed: " << cntl.ErrorText() << std::endl;
        } else if (response.status().code() != zb::rpc::SCHED_OK) {
            std::cerr << "Scheduler optical library status rejected: "
                      << response.status().message() << std::endl;
        }
    }

    void Run() {
        std::unique_ptr<brpc::Channel> channel;
        while (!stop_.load()) {
            if (!EnsureChannel(&channel)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms_));
                continue;
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
            request.set_initialization_complete(service_->IsArchiveEngineReady());
            request.set_metadata_ready(service_->IsArchiveEngineReady());
            request.set_readiness_message(
                "archive engine running; library inventory attached");

            // 设备清单：与 profile 三组对齐——hdd-cache ×1、disc ×1（盘片容量聚合）、
            // optical-drive 逐光驱 drive_total 条。inventory 只参与容量/空闲/健康/带宽求和。
            const auto inv = service_->GetLibraryInventory();
            {
                zb::rpc::DiskHeartbeat* cache = request.add_disks();
                cache->set_disk_id("hdd-cache");
                cache->set_device_kind(zb::rpc::MANAGED_DEVICE_HDD);
                const uint64_t capacity_images = inventory_config_.capacity_in_images;
                const uint64_t used_images = std::min(inv.staging_used_images, capacity_images);
                cache->set_capacity_bytes(capacity_images * inventory_config_.volume_size_bytes);
                cache->set_free_bytes(
                    (capacity_images - used_images) * inventory_config_.volume_size_bytes);
                cache->set_is_healthy(true);
                cache->set_read_bandwidth_bytes_per_sec(inventory_config_.cache_read_bytes_per_sec);
                cache->set_write_bandwidth_bytes_per_sec(inventory_config_.cache_write_bytes_per_sec);
                // 累计写入字节挂在写入目标（image_dir 缓存）上；调度器按设备求和，位置不影响汇总。
                cache->set_accumulated_write_bytes(inv.accumulated_write_bytes);
            }
            {
                zb::rpc::DiskHeartbeat* disc = request.add_disks();
                disc->set_disk_id("disc");
                disc->set_device_kind(zb::rpc::MANAGED_DEVICE_DISC);
                const uint64_t used_discs = std::min(inv.disc_used, inv.disc_count);
                disc->set_capacity_bytes(inventory_config_.disc_capacity_bytes * inv.disc_count);
                disc->set_free_bytes(
                    inventory_config_.disc_capacity_bytes * (inv.disc_count - used_discs));
                disc->set_is_healthy(true);
                // 盘片介质本身无读写带宽（与 profile 的 disc 组一致）；带宽由光驱条目承载。
            }
            for (uint32_t i = 0; i < inv.drive_total; ++i) {
                zb::rpc::DiskHeartbeat* drive = request.add_disks();
                drive->set_disk_id("optical-drive-" + std::to_string(i));
                drive->set_device_kind(zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE);
                drive->set_capacity_bytes(0);
                drive->set_free_bytes(0);
                drive->set_is_healthy(true);
                drive->set_read_bandwidth_bytes_per_sec(inventory_config_.drive_read_bytes_per_sec);
                drive->set_write_bandwidth_bytes_per_sec(inventory_config_.drive_write_bytes_per_sec);
            }

            zb::rpc::HeartbeatReply response;
            brpc::Controller cntl;
            stub.ReportHeartbeat(&cntl, &request, &response, nullptr);
            if (cntl.Failed()) {
                std::cerr << "Scheduler heartbeat failed: " << cntl.ErrorText() << std::endl;
                channel.reset();
            } else {
                if (response.status().code() == zb::rpc::SCHED_OK) {
                    bool is_primary = response.assigned_role() == zb::rpc::NODE_ROLE_PRIMARY;
                    configured_role_ = response.assigned_role();
                    service_->ApplySchedulerAssignment(is_primary,
                                                       response.epoch(),
                                                       response.group_id(),
                                                       response.primary_node_id(),
                                                       response.primary_address(),
                                                       response.secondary_node_id(),
                                                       response.secondary_address());
                }
                // 同一通道、同一节拍上报光盘库专属状态；失败只记日志，不影响心跳循环。
                ReportLibraryStatus(stub);
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
    HeartbeatInventoryConfig inventory_config_;
    zb::optical_node::OpticalStorageServiceImpl* service_{};
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

// 从节点配置逐项构建 RegisterManagedNode 请求（设备组 + 功率 + 可靠性 + 光盘延迟 + 代际）。
// 客户端显式给出硬件几何，节点属性不再依赖服务端默认光盘库模板。
zb::rpc::RegisterManagedNodeRequest BuildRegisterRequest(const zb::optical_node::OpticalNodeConfig& cfg,
                                                         const std::string& node_id) {
    zb::rpc::RegisterManagedNodeRequest request;
    auto* spec = request.mutable_spec();
    spec->set_node_id(node_id);
    spec->set_kind(zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY);
    spec->set_execution_mode(zb::rpc::MANAGED_EXECUTION_PHYSICAL);
    spec->set_logical_node_count(1);
    // 光盘库默认不参与普通副本放置，归档准入由 CMS 目录决定。
    spec->set_participates_in_placement(false);
    spec->set_participates_in_placement_set(true);
    spec->set_technology_generation(cfg.technology_generation);
    spec->set_instance_generation(cfg.instance_generation);
    spec->set_external_network_bandwidth_bytes_per_sec(cfg.external_network_bandwidth_bytes_per_sec);
    spec->set_fixed_access_latency_us(cfg.fixed_access_latency_us);
    spec->set_optical_load_latency_us(cfg.optical_load_latency_us);
    spec->set_optical_seek_latency_us(cfg.optical_seek_latency_us);

    // hdd-cache：image_dir 总大小 = 镜像数上限 × 单卷镜像大小；单设备、不并发。
    auto* cache = spec->add_device_groups();
    cache->set_name("hdd-cache");
    cache->set_kind(zb::rpc::MANAGED_DEVICE_HDD);
    cache->set_device_capacity_bytes(cfg.capacity_in_images * cfg.volume_size_bytes);
    cache->set_device_count(1);
    cache->set_max_concurrency(1);
    cache->set_per_device_bandwidth_bytes_per_sec(cfg.cache_read_bytes_per_sec);
    cache->set_per_device_read_bandwidth_bytes_per_sec(cfg.cache_read_bytes_per_sec);
    cache->set_per_device_write_bandwidth_bytes_per_sec(cfg.cache_write_bytes_per_sec);

    // disc：单盘容量 × 盘片总数。
    auto* disc = spec->add_device_groups();
    disc->set_name("disc");
    disc->set_kind(zb::rpc::MANAGED_DEVICE_DISC);
    disc->set_device_capacity_bytes(cfg.disc_capacity_bytes);
    disc->set_device_count(cfg.total_disc_count);

    // optical-drive：光驱组，本身无容量，按读写带宽计。
    auto* drive = spec->add_device_groups();
    drive->set_name("optical-drive");
    drive->set_kind(zb::rpc::MANAGED_DEVICE_OPTICAL_DRIVE);
    drive->set_device_capacity_bytes(0);
    drive->set_device_count(cfg.drive_count);
    drive->set_max_concurrency(cfg.drive_count);
    drive->set_per_device_bandwidth_bytes_per_sec(cfg.drive_read_bytes_per_sec);
    drive->set_per_device_read_bandwidth_bytes_per_sec(cfg.drive_read_bytes_per_sec);
    drive->set_per_device_write_bandwidth_bytes_per_sec(cfg.drive_write_bytes_per_sec);

    auto* power = spec->mutable_power();
    power->set_peak_milliwatts(cfg.power_peak_milliwatts);
    power->set_medium_milliwatts(cfg.power_medium_milliwatts);
    power->set_standby_milliwatts(cfg.power_standby_milliwatts);
    power->set_off_milliwatts(cfg.power_off_milliwatts);

    auto* reliability = spec->mutable_reliability();
    reliability->set_mean_lifetime_ms(cfg.reliability_mean_lifetime_ms);
    reliability->set_lifetime_stddev_ms(cfg.reliability_lifetime_stddev_ms);
    reliability->set_minimum_lifetime_ms(cfg.reliability_minimum_lifetime_ms);
    reliability->set_model(zb::rpc::ManagedReliabilityProfile::NORMAL_LIFETIME);

    return request;
}

// 从节点配置提取心跳设备清单所需的静态量。
HeartbeatInventoryConfig BuildHeartbeatInventoryConfig(const zb::optical_node::OpticalNodeConfig& cfg) {
    HeartbeatInventoryConfig out;
    out.volume_size_bytes = cfg.volume_size_bytes;
    out.disc_capacity_bytes = cfg.disc_capacity_bytes;
    out.capacity_in_images = cfg.capacity_in_images;
    out.cache_read_bytes_per_sec = cfg.cache_read_bytes_per_sec;
    out.cache_write_bytes_per_sec = cfg.cache_write_bytes_per_sec;
    out.drive_read_bytes_per_sec = cfg.drive_read_bytes_per_sec;
    out.drive_write_bytes_per_sec = cfg.drive_write_bytes_per_sec;
    return out;
}

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
                                        BuildHeartbeatInventoryConfig(cfg),
                                        &storage_service);
    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start brpc server on port " << FLAGS_port << std::endl;
        return 1;
    }

    if (!cfg.scheduler_addr.empty()) {
        // 启动阶段一次性注册（同步、带有限重试）；成功后才开始周期心跳。
        // 未确认时仍继续心跳，由心跳的隐式自动注册兜底（profile 退化为默认模板）。
        OpticalNodeSchedulerRegistrar registrar(cfg.scheduler_addr,
                                                BuildRegisterRequest(cfg, node_id),
                                                cfg.heartbeat_interval_ms);
        std::string register_error;
        if (!registrar.Register(&register_error)) {
            std::cerr << "Scheduler managed-node registration not confirmed: " << register_error
                      << "; heartbeat will rely on implicit auto-registration" << std::endl;
        }
        reporter.Start();
    }

    server.RunUntilAskedToQuit();

    // 正常关机（SIGINT / SIGTERM 由 brpc 处理）：先停心跳上报，避免调度器在节点
    // 停止期间继续派发归档任务，再把归档引擎的运行期状态落盘，供下次启动恢复。
    reporter.Stop();
    storage_service.Shutdown();
    return 0;
}
