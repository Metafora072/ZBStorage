#include "OpticalStorageServiceImpl.h"

#include <mutex>

namespace zb::optical_node {

OpticalStorageServiceImpl::OpticalStorageServiceImpl(const OpticalNodeConfig& config)
    : optical_node_manager_(config.volume_size_bytes,
                            config.size_threshold,
                            config.archive_root,
                            config.capacity_in_images,
                            config.available_volume_id_count) {
    optical_node_manager_.Run(config.initial_available_volume_ids);
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

bool OpticalStorageServiceImpl::IsArchiveEngineReady() const {
    return optical_node_manager_.IsReady();
}

std::string OpticalStorageServiceImpl::GetArchiveStatusDetail() const {
    return optical_node_manager_.GetStatusDetail();
}

} // namespace zb::optical_node
