#include "OpticalStorageServiceImpl.h"

#include <cctype>
#include <mutex>

#include <butil/iobuf.h>

#include "optical_node.pb.h"

namespace zb::optical_node {

namespace {

// 取上层标识的尾部序号：disk_id 形如 "optical-disk-<seq>"、image_id 形如 "img-<seq>"，
// optical_node_manager 只使用其中的 <seq>（纯十进制数字串）。
bool ExtractTrailingSeq(const std::string& value, std::string* seq) {
    if (seq == nullptr || value.empty()) {
        return false;
    }
    size_t end = value.size();
    size_t begin = end;
    while (begin > 0 && std::isdigit(static_cast<unsigned char>(value[begin - 1])) != 0) {
        --begin;
    }
    if (begin == end) {
        return false;
    }
    *seq = value.substr(begin, end - begin);
    return true;
}

// 从节点配置逐项构建光盘库/模拟参数（原 cd_manager 内写死的常量现全部外部化）。
// workspace_root 不在此设置——由 OpticalNodeManager::InitializeDir 按 disc_sim_dir_ 覆盖。
cd_manager_sim::CDManagerConfig BuildCDManagerConfig(const OpticalNodeConfig& cfg) {
    cd_manager_sim::CDManagerConfig out;
    out.num_drives = static_cast<int>(cfg.drive_count);
    out.num_arms = static_cast<int>(cfg.arm_count);
    out.total_disc_count = cfg.total_disc_count;
    out.slot_count = cfg.slot_count;
    out.volume_size = cfg.volume_size_bytes;
    out.burn_image_size = cfg.burn_image_size_bytes;
    out.burn_bandwidth_mbps = cfg.burn_bandwidth_mbps;
    out.time_burn_fixed_seconds = cfg.burn_fixed_seconds;
    out.read_bandwidth_mbps = cfg.read_bandwidth_mbps;
    out.time_read_fixed_seconds = cfg.read_fixed_seconds;
    out.time_drive_load_seconds = cfg.drive_load_seconds;
    out.time_drive_eject_seconds = cfg.drive_eject_seconds;
    out.time_grasp_seconds = cfg.grasp_seconds;
    out.arm_speed_x = cfg.arm_speed_x;
    out.arm_speed_y = cfg.arm_speed_y;
    out.arm_speed_z = cfg.arm_speed_z;
    out.rack_rows = cfg.rack_rows;
    out.rack_cols = cfg.rack_cols;
    out.rack_depth = cfg.rack_depth;
    out.slot_width = cfg.slot_width;
    out.slot_height = cfg.slot_height;
    out.slot_depth = cfg.slot_depth;
    return out;
}

}  // namespace

OpticalStorageServiceImpl::OpticalStorageServiceImpl(const OpticalNodeConfig& config)
    : optical_node_manager_(config.volume_size_bytes,
                            config.size_threshold,
                            config.archive_root,
                            config.capacity_in_images,
                            config.available_id_count,
                            config.disc_capacity_bytes,
                            config.standard_images_per_disc,
                            config.disc_block_size_bytes,
                            config.max_write_images,
                            config.scheduler_addr,
                            BuildCDManagerConfig(config)) {
    // 在 Run() 拉起后台线程之前注册完成回调：任何准备任务 READY 都能通知到客户端。
    optical_node_manager_.SetPreparedFileReadyCallback(
        [this](const optical_node_manager::PreparedFileReadyEvent& ev) { SendReadReady(ev); });
    optical_node_manager_.Run();
}

void OpticalStorageServiceImpl::ConfigureReplication(const std::string& node_id,
                                                     const std::string& group_id,
                                                     bool replication_enabled,
                                                     bool is_primary,
                                                     const std::string& peer_node_id,
                                                     const std::string& peer_address,
                                                     uint32_t replication_timeout_ms) {
    std::lock_guard<std::mutex> lock(repl_mu_);
    repl_.node_id = node_id;
    repl_.group_id = group_id;
    repl_.replication_enabled = replication_enabled;
    repl_.is_primary = is_primary;
    repl_.peer_node_id = peer_node_id;
    repl_.peer_address = peer_address;
    repl_.epoch = 1;
    repl_.primary_node_id = is_primary ? node_id : peer_node_id;
    repl_.primary_address = is_primary ? "" : peer_address;
    repl_.secondary_node_id = is_primary ? peer_node_id : node_id;
    repl_.secondary_address = is_primary ? peer_address : "";
    replication_timeout_ms_ = replication_timeout_ms > 0 ? replication_timeout_ms : 2000;
}

void OpticalStorageServiceImpl::ApplySchedulerAssignment(bool is_primary,
                                                         uint64_t epoch,
                                                         const std::string& group_id,
                                                         const std::string& primary_node_id,
                                                         const std::string& primary_address,
                                                         const std::string& secondary_node_id,
                                                         const std::string& secondary_address) {
    std::lock_guard<std::mutex> lock(repl_mu_);
    repl_.is_primary = is_primary;
    repl_.epoch = epoch > 0 ? epoch : repl_.epoch;
    if (!group_id.empty()) {
        repl_.group_id = group_id;
    }
    repl_.primary_node_id = primary_node_id;
    repl_.primary_address = primary_address;
    repl_.secondary_node_id = secondary_node_id;
    repl_.secondary_address = secondary_address;
    if (repl_.node_id == primary_node_id) {
        repl_.peer_node_id = secondary_node_id;
        repl_.peer_address = secondary_address;
    } else if (repl_.node_id == secondary_node_id) {
        repl_.peer_node_id = primary_node_id;
        repl_.peer_address = primary_address;
    }
}

ReplicationStatusSnapshot OpticalStorageServiceImpl::GetReplicationStatus() const {
    std::lock_guard<std::mutex> lock(repl_mu_);
    return repl_;
}

volumemanager::ErrorCode OpticalStorageServiceImpl::SendArchiveMetadata(
    const optical_node_manager::SendArchiveMetadataRequest& request) {
    return optical_node_manager_.SendArchiveMetadata(request);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::PrepareRead(const PrepareReadParams& params,
                                                                uint64_t* task_id) {
    // 上层 disk_id / image_id 带前缀，manager 只用尾部 <seq>。
    std::string volume_id;
    if (!ExtractTrailingSeq(params.image_id, &volume_id)) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }
    std::string disk_seq;
    if (!params.disk_id.empty() && !ExtractTrailingSeq(params.disk_id, &disk_seq)) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }

    optical_node_manager::PrepareReadParams manager_params;
    manager_params.disk_id = disk_seq;
    manager_params.volume_id = volume_id;
    manager_params.inode_id = params.inode_id;
    manager_params.request_id = params.request_id;
    manager_params.session_id = params.session_id;
    manager_params.expected_file_size = params.file_size;
    manager_params.has_file_version = params.has_file_version;
    manager_params.file_version = params.file_version;
    return optical_node_manager_.SubmitPrepareTask(manager_params, task_id);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::AcquireReadSession(
        uint64_t task_id, optical_node_manager::AcquireReadSessionResult* out) {
    return optical_node_manager_.AcquireReadSession(task_id, out);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::ReadPreparedFile(
        uint64_t read_handle, uint64_t offset, uint64_t size, std::string* out,
        optical_node_manager::ReadPreparedFileResult* result) {
    return optical_node_manager_.ReadPreparedFile(read_handle, offset, size, out, result);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::ReleaseReadSession(uint64_t read_handle) {
    return optical_node_manager_.ReleaseReadSession(read_handle);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::ReleasePreparedRead(uint64_t task_id) {
    return optical_node_manager_.ReleasePreparedRead(task_id);
}

uint64_t OpticalStorageServiceImpl::SubscribeReadEvents(const std::string& client_id,
                                                        brpc::StreamId server_stream) {
    return read_event_sessions_.RegisterSession(client_id, server_stream);
}

void OpticalStorageServiceImpl::SendReadReady(
        const optical_node_manager::PreparedFileReadyEvent& ev) {
    zb::rpc::ReadReadyEvent msg;
    msg.set_request_id(ev.request_id);
    msg.set_task_id(ev.task_id);
    msg.set_inode_id(ev.inode_id);
    msg.set_file_size(ev.file_size);
    if (ev.has_file_version) {
        msg.set_has_file_version(true);
        msg.set_file_version(ev.file_version);
    }

    std::string bytes;
    if (!msg.SerializeToString(&bytes)) {
        // 序列化失败：本阶段仅忽略，不重试。
        return;
    }
    butil::IOBuf buf;
    buf.append(bytes);
    // 会话可能已被对端关闭：SendMessage 返回 false，本阶段仅忽略，不重试。
    read_event_sessions_.SendMessage(ev.session_id, buf);
}

bool OpticalStorageServiceImpl::IsArchiveEngineReady() const {
    return optical_node_manager_.IsReady();
}

bool OpticalStorageServiceImpl::Shutdown() {
    // 先关闭通知会话，停止向客户端推送后再停归档引擎。
    read_event_sessions_.CloseAll();
    return optical_node_manager_.Shutdown();
}

std::string OpticalStorageServiceImpl::GetArchiveStatusDetail() const {
    return optical_node_manager_.GetStatusDetail();
}

optical_node_manager::OpticalLibraryInventory OpticalStorageServiceImpl::GetLibraryInventory() const {
    return optical_node_manager_.GetInventory();
}

} // namespace zb::optical_node
