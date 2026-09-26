#include "OpticalStorageServiceImpl.h"

#include <cctype>
#include <mutex>

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

}  // namespace

OpticalStorageServiceImpl::OpticalStorageServiceImpl(const OpticalNodeConfig& config)
    : optical_node_manager_(config.volume_size_bytes,
                            config.size_threshold,
                            config.archive_root,
                            config.capacity_in_images,
                            config.available_volume_id_count,
                            config.disc_capacity_bytes,
                            config.standard_images_per_disc,
                            config.disc_block_size_bytes,
                            config.max_write_images,
                            config.scheduler_addr) {
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

volumemanager::ErrorCode OpticalStorageServiceImpl::RequestAsyncReadFile(
    const std::string& disk_id,
    const std::string& image_id,
    const std::string& inode_id,
    uint64_t* task_id) {
    // 上层 disk_id / image_id 带前缀，manager 只用尾部 <seq>。
    std::string volume_id;
    if (!ExtractTrailingSeq(image_id, &volume_id)) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }
    std::string disk_seq;
    if (!disk_id.empty() && !ExtractTrailingSeq(disk_id, &disk_seq)) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }
    return optical_node_manager_.RequestAsyncReadFile(disk_seq, volume_id, inode_id, task_id);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::ReadObjectByTaskId(uint64_t task_id,
                                                                      std::string* out,
                                                                      uint64_t offset,
                                                                      uint64_t read_size) {
    return optical_node_manager_.ReadObjectByTaskId(task_id, out, offset, read_size);
}

volumemanager::ErrorCode OpticalStorageServiceImpl::ReadObjectByInodeId(const std::string& inode_id,
                                                                       std::string* out,
                                                                       uint64_t offset,
                                                                       uint64_t read_size) {
    return optical_node_manager_.ReadObjectByInodeId(inode_id, out, offset, read_size);
}

bool OpticalStorageServiceImpl::IsArchiveEngineReady() const {
    return optical_node_manager_.IsReady();
}

std::string OpticalStorageServiceImpl::GetArchiveStatusDetail() const {
    return optical_node_manager_.GetStatusDetail();
}

} // namespace zb::optical_node
