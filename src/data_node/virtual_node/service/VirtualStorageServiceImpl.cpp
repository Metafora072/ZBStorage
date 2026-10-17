#include "VirtualStorageServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <chrono>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <thread>
#include <utility>

#include "real_node.pb.h"
#include "common/rpc/ModelWait.h"

namespace zb::virtual_node {

namespace fs = std::filesystem;

namespace {

constexpr uint32_t kReplicationRepairMaxRetry = 8;
constexpr int kReplicationRepairRetryIntervalMs = 500;
constexpr size_t kReplicationRepairMaxQueue = 10000;
constexpr uint64_t kDefaultObjectUnitSize = 4ULL * 1024ULL * 1024ULL;
constexpr uint64_t kMaxFileSlicesPerRequest = 4096;

struct FileArchiveAggregate {
    uint64_t file_size{0};
    uint32_t object_count{0};
};

uint32_t ComputeObjectCount(uint64_t file_size, uint64_t object_unit_size) {
    if (file_size == 0 || object_unit_size == 0) {
        return 0;
    }
    const uint64_t count = 1 + (file_size - 1) / object_unit_size;
    return static_cast<uint32_t>(std::min<uint64_t>(count, std::numeric_limits<uint32_t>::max()));
}

bool ValidateFileSliceRange(uint64_t offset, uint64_t size,
                            uint64_t object_unit_size, std::string* error) {
    if (object_unit_size == 0 || size > std::numeric_limits<uint64_t>::max() - offset) {
        if (error) *error = "invalid file slice range or object unit size";
        return false;
    }
    if (size == 0) return true;
    const uint64_t first = offset / object_unit_size;
    const uint64_t last = (offset + size - 1) / object_unit_size;
    if (last >= std::numeric_limits<uint32_t>::max()) {
        if (error) *error = "file object index exceeds supported range";
        return false;
    }
    if (last - first + 1 > kMaxFileSlicesPerRequest) {
        if (error) *error = "MODEL_SLICE_LIMIT";
        return false;
    }
    return true;
}

uint32_t StableJitterSeed(const std::string& node_id, uint32_t configured) {
    if (configured != 0) return configured;
    uint32_t hash = 2166136261u;
    for (const unsigned char ch : node_id) {
        hash ^= ch;
        hash *= 16777619u;
    }
    return hash;
}

} // namespace

VirtualStorageServiceImpl::VirtualStorageServiceImpl(VirtualNodeConfig config)
    : config_(std::move(config)),
      io_faults_(config_.io_failure_probability_ppm, config_.io_timeout_probability_ppm,
                 config_.jitter_seed, config_.node_id),
      rng_(StableJitterSeed(config_.node_id, config_.jitter_seed)) {
    for (const auto& disk_id : config_.disk_ids) {
        if (!disk_id.empty()) {
            disk_set_.insert(disk_id);
        }
    }
    if (disk_set_.empty()) {
        disk_set_.insert("disk-01");
    }
    for (const auto& disk_id : disk_set_) {
        disk_used_bytes_[disk_id] = 0;
    }
    if (config_.replication_enabled) {
        repl_repair_thread_ = std::thread([this]() { ReplicationRepairLoop(); });
    }
}

VirtualStorageServiceImpl::~VirtualStorageServiceImpl() {
    stop_repl_repair_.store(true);
    repl_repair_cv_.notify_all();
    if (repl_repair_thread_.joinable()) {
        repl_repair_thread_.join();
    }
}

void VirtualStorageServiceImpl::SetFileMetaStoreDir(const std::string& dir_path) {
    if (dir_path.empty()) {
        return;
    }
    std::error_code ec;
    fs::path dir(dir_path);
    fs::create_directories(dir, ec);
    if (ec) {
        return;
    }
    std::lock_guard<std::mutex> lock(object_mu_);
    if (model_state_store_) return;
    file_meta_store_path_ = (dir / "file_meta.tsv").string();
    file_meta_loaded_ = false;
    file_meta_by_inode_.clear();
    preloaded_file_objects_.clear();
}

bool VirtualStorageServiceImpl::InitModelStateStore(const std::string& state_dir,
                                                    std::string* error) {
    std::lock_guard<std::mutex> lock(object_mu_);
    if (model_state_store_ || !object_sizes_.empty() || !file_meta_by_inode_.empty() ||
        file_meta_loaded_) {
        if (error) *error = "model state must be opened before serving requests";
        return false;
    }
    auto store = std::make_unique<ModelStateStore>();
    std::vector<ModelObjectRecord> objects;
    std::vector<ModelFileRecord> files;
    if (!store->Open(state_dir, config_.node_id, config_.disk_ids,
                     config_.disk_capacity_bytes, &objects, &files, error)) return false;
    for (const auto& object : objects) {
        object_sizes_[BuildObjectKey(object.disk_id, object.object_id)] = object.size;
        object_home_disk_[object.object_id] = object.disk_id;
        disk_used_bytes_[object.disk_id] += object.size;
    }
    for (const auto& file : files) {
        file_meta_by_inode_[file.meta.inode_id] = file.meta;
        if (!file.txid.empty()) last_commit_txid_by_inode_[file.meta.inode_id] = file.txid;
        if (!file.commit_signature.empty())
            last_commit_signature_by_inode_[file.meta.inode_id] = file.commit_signature;
    }
    file_meta_loaded_ = true;
    model_state_store_ = std::move(store);
    return true;
}

zb::msg::Status VirtualStorageServiceImpl::ImportModelObject(
    const std::string& disk_id, const std::string& object_id, uint64_t size) {
    if (!ValidateDisk(disk_id) || object_id.empty())
        return zb::msg::Status::InvalidArgument("invalid model object target");
    std::lock_guard<std::mutex> lock(object_mu_);
    if (!model_state_store_) return zb::msg::Status::IoError("model state is not durable");
    const auto home = object_home_disk_.find(object_id);
    if (home != object_home_disk_.end() && home->second != disk_id)
        return zb::msg::Status::IoError("OBJECT_ID_DISK_CONFLICT");
    const std::string key = BuildObjectKey(disk_id, object_id);
    const auto existing = object_sizes_.find(key);
    if (existing != object_sizes_.end())
        return existing->second == size ? zb::msg::Status::Ok()
                                        : zb::msg::Status::IoError("MODEL_OBJECT_SIZE_CONFLICT");
    const uint64_t used = disk_used_bytes_[disk_id];
    if (used > config_.disk_capacity_bytes || size > config_.disk_capacity_bytes - used)
        return zb::msg::Status::IoError("NO_SPACE");
    std::string error;
    if (!model_state_store_->PutObject(disk_id, object_id, size, &error))
        return zb::msg::Status::IoError(error);
    object_sizes_[key] = size;
    object_home_disk_[object_id] = disk_id;
    disk_used_bytes_[disk_id] = used + size;
    return zb::msg::Status::Ok();
}

zb::msg::Status VirtualStorageServiceImpl::ExportResidentModelObject(
    const std::string& disk_id, const std::string& object_id,
    uint64_t expected_size) const {
    std::lock_guard<std::mutex> lock(object_mu_);
    const auto it = object_sizes_.find(BuildObjectKey(disk_id, object_id));
    if (it == object_sizes_.end())
        return zb::msg::Status::IoError(
            "preloaded/generated file object needs a seed-preserving migration format");
    return it->second == expected_size ? zb::msg::Status::Ok()
                                       : zb::msg::Status::IoError("source model object size changed");
}

zb::msg::Status VirtualStorageServiceImpl::ExportModelFile(
    uint64_t inode_id, zb::msg::FileMeta* meta) const {
    if (!meta || inode_id == 0) return zb::msg::Status::InvalidArgument("invalid inode");
    std::lock_guard<std::mutex> lock(object_mu_);
    const auto it = file_meta_by_inode_.find(inode_id);
    if (it == file_meta_by_inode_.end())
        return zb::msg::Status::NotFound("model file metadata not found");
    *meta = it->second;
    return zb::msg::Status::Ok();
}

zb::msg::Status VirtualStorageServiceImpl::ImportModelFile(const zb::msg::FileMeta& meta, const std::string& disk_id) {
    if (meta.inode_id == 0 || meta.object_unit_size == 0 || meta.version == 0)
        return zb::msg::Status::InvalidArgument("invalid migrated file metadata");
    std::lock_guard<std::mutex> lock(object_mu_);
    if (!model_state_store_) return zb::msg::Status::IoError("model state is not durable");
    const uint64_t count = meta.file_size == 0 ? 0 : 1 + (meta.file_size - 1) / meta.object_unit_size;
    if (count > UINT32_MAX) return zb::msg::Status::InvalidArgument("migrated file object count exceeds limit");
    for (uint64_t index = 0; index < count; ++index) {
        const auto object_id = BuildStableObjectId(meta.inode_id, static_cast<uint32_t>(index));
        const auto home = object_home_disk_.find(object_id);
        if (home == object_home_disk_.end() || (!disk_id.empty() && home->second != disk_id))
            return zb::msg::Status::IoError("migrated file object missing or disk mismatch");
        const auto object = object_sizes_.find(BuildObjectKey(home->second, object_id));
        const uint64_t expected = std::min<uint64_t>(meta.object_unit_size, meta.file_size - index * meta.object_unit_size);
        if (object == object_sizes_.end() || object->second != expected)
            return zb::msg::Status::IoError("migrated file object size mismatch");
    }
    const auto existing = file_meta_by_inode_.find(meta.inode_id);
    if (existing != file_meta_by_inode_.end()) {
        const auto& old = existing->second;
        return old.version == meta.version && old.file_size == meta.file_size &&
                       old.object_unit_size == meta.object_unit_size
                   ? zb::msg::Status::Ok()
                   : zb::msg::Status::IoError("MODEL_FILE_VERSION_CONFLICT");
    }
    ModelFileRecord row;
    row.meta = meta;
    std::string error;
    if (!model_state_store_->PutFile(row, &error)) return zb::msg::Status::IoError(error);
    file_meta_by_inode_[meta.inode_id] = meta;
    return zb::msg::Status::Ok();
}

zb::msg::Status VirtualStorageServiceImpl::VerifyModelArchiveSource(
    uint64_t inode_id, const std::string& disk_id,
    uint64_t expected_size, uint64_t expected_version) const {
    // Reject sparse and preloaded objects because their generated bytes differ.
    if (inode_id == 0 || disk_id.empty())
        return zb::msg::Status::InvalidArgument("invalid archive source");
    std::lock_guard<std::mutex> lock(object_mu_);
    const auto file = file_meta_by_inode_.find(inode_id);
    if (file == file_meta_by_inode_.end() ||
        file->second.file_size != expected_size ||
        file->second.version != expected_version ||
        file->second.object_unit_size == 0 ||
        preloaded_file_objects_.count(inode_id) != 0)
        return zb::msg::Status::IoError("model archive source metadata is incomplete or changed");
    const uint64_t unit = file->second.object_unit_size;
    const uint64_t count = expected_size == 0 ? 0 : 1 + (expected_size - 1) / unit;
    if (count > UINT32_MAX)
        return zb::msg::Status::IoError("model archive object count exceeds supported range");
    for (uint64_t index = 0; index < count; ++index) {
        const std::string key = BuildObjectKey(
            disk_id, BuildStableObjectId(inode_id, static_cast<uint32_t>(index)));
        const auto object = object_sizes_.find(key);
        const uint64_t required = std::min<uint64_t>(unit, expected_size - index * unit);
        if (object == object_sizes_.end() || object->second != required)
            return zb::msg::Status::IoError("model archive source has a sparse or changed object");
    }
    return zb::msg::Status::Ok();
}

void VirtualStorageServiceImpl::ConfigureReplication(const std::string& node_id,
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

void VirtualStorageServiceImpl::ApplySchedulerAssignment(bool is_primary,
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

ReplicationStatusSnapshot VirtualStorageServiceImpl::GetReplicationStatus() const {
    std::lock_guard<std::mutex> lock(repl_mu_);
    return repl_;
}

zb::msg::WriteObjectReply VirtualStorageServiceImpl::WriteObject(const zb::msg::WriteObjectRequest& request) {
    zb::msg::WriteObjectReply reply;
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
        return reply;
    }
    std::string effective_disk;
    if (!ResolveEffectiveDisk(request.disk_id, object_id, true, &effective_disk)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
        return reply;
    }
    zb::msg::WriteObjectRequest normalized_request = request;
    normalized_request.disk_id = effective_disk;
    normalized_request.SetArchiveObjectId(object_id);

    ReplicationStatusSnapshot repl_snapshot = GetReplicationStatus();
    if (repl_snapshot.replication_enabled && !request.is_replication && !repl_snapshot.is_primary) {
        reply.status = zb::msg::Status::IoError("NOT_LEADER");
        return reply;
    }
    if (request.is_replication && repl_snapshot.replication_enabled &&
        request.epoch > 0 && request.epoch < repl_snapshot.epoch) {
        reply.status = zb::msg::Status::IoError("STALE_EPOCH");
        return reply;
    }

    reply.bytes = static_cast<uint64_t>(request.data.size());
    if (normalized_request.offset > std::numeric_limits<uint64_t>::max() - reply.bytes) {
        reply.status = zb::msg::Status::InvalidArgument("write range overflows uint64_t");
        reply.bytes = 0;
        return reply;
    }
    if (reply.bytes == 0) {
        reply.status = zb::msg::Status::Ok();
        return reply;
    }
    reply.status = SimulateIo(reply.bytes, false);
    if (!reply.status.ok()) {
        reply.bytes = 0;
        return reply;
    }
    {
        std::lock_guard<std::mutex> lock(object_mu_);
        const std::string key = BuildObjectKey(normalized_request.disk_id, object_id);
        const auto home = object_home_disk_.find(object_id);
        if (model_state_store_ && home != object_home_disk_.end() &&
            home->second != normalized_request.disk_id) {
            reply.status = zb::msg::Status::IoError("OBJECT_ID_DISK_CONFLICT");
            reply.bytes = 0;
            return reply;
        }
        const auto existing = object_sizes_.find(key);
        const uint64_t old_size = existing == object_sizes_.end() ? 0 : existing->second;
        const uint64_t new_size = std::max(old_size, normalized_request.offset + reply.bytes);
        if (new_size > old_size) {
            const uint64_t increase = new_size - old_size;
            const uint64_t used = disk_used_bytes_[normalized_request.disk_id];
            if (used > config_.disk_capacity_bytes ||
                increase > config_.disk_capacity_bytes - used) {
                reply.status = zb::msg::Status::IoError("NO_SPACE");
                reply.bytes = 0;
                return reply;
            }
            if (model_state_store_) {
                std::string persist_error;
                if (!model_state_store_->PutObject(normalized_request.disk_id, object_id,
                                                   new_size, &persist_error)) {
                    reply.status = zb::msg::Status::IoError(persist_error);
                    reply.bytes = 0;
                    return reply;
                }
            }
            disk_used_bytes_[normalized_request.disk_id] = used + increase;
        } else if (existing == object_sizes_.end() && model_state_store_) {
            std::string persist_error;
            if (!model_state_store_->PutObject(normalized_request.disk_id, object_id,
                                               new_size, &persist_error)) {
                reply.status = zb::msg::Status::IoError(persist_error);
                reply.bytes = 0;
                return reply;
            }
        }
        object_home_disk_[object_id] = normalized_request.disk_id;
        object_sizes_[key] = new_size;
    }
    TrackObjectAccess(normalized_request.disk_id,
                      object_id,
                      normalized_request.offset + reply.bytes,
                      true,
                      FastChecksum64(normalized_request.data));
    {
        std::lock_guard<std::mutex> lock(repl_mu_);
        ++repl_.applied_lsn;
    }

    if (repl_snapshot.replication_enabled && repl_snapshot.is_primary && !normalized_request.is_replication &&
        !repl_snapshot.peer_address.empty()) {
        const uint64_t generation = BumpReplicationRepairGeneration(normalized_request);
        zb::msg::Status repl_status = ReplicateWriteToSecondary(normalized_request, repl_snapshot.epoch);
        if (!repl_status.ok()) {
            EnqueueReplicationRepair(normalized_request, repl_snapshot.epoch);
            reply.status = repl_status;
            return reply;
        }
        std::lock_guard<std::mutex> lock(repl_repair_mu_);
        const std::string key = BuildReplicationRepairKey(normalized_request);
        auto it = repl_repair_generation_.find(key);
        if (it != repl_repair_generation_.end() && it->second == generation) {
            repl_repair_generation_.erase(it);
        }
    }

    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::ReadObjectReply VirtualStorageServiceImpl::ReadObject(const zb::msg::ReadObjectRequest& request) {
    zb::msg::ReadObjectReply reply;
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
        return reply;
    }
    std::string effective_disk;
    if (!ResolveEffectiveDisk(request.disk_id, object_id, false, &effective_disk)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
        return reply;
    }

    {
        std::lock_guard<std::mutex> lock(object_mu_);
        const std::string key = BuildObjectKey(effective_disk, object_id);
        auto it = object_sizes_.find(key);
        if (it == object_sizes_.end()) {
            if (!TryReadPreloadedObjectLocked(effective_disk,
                                              object_id,
                                              request.offset,
                                              request.size,
                                              &reply.data,
                                              &reply.bytes)) {
                reply.status = zb::msg::Status::NotFound("object not found");
                return reply;
            }
            // Tracking may inspect file metadata under object_mu_. Do it only
            // after leaving this critical section, like the ordinary path.
        }
        if (it != object_sizes_.end()) {
            const uint64_t object_size = it->second;
            if (request.offset >= object_size) {
                reply.bytes = 0;
                reply.data.clear();
            } else {
                const uint64_t remain = object_size - request.offset;
                const uint64_t read_len = std::min<uint64_t>(remain, request.size);
                reply.bytes = read_len;
                reply.data.assign(static_cast<size_t>(read_len), 'x');
            }
        }
    }
    // Charge the bytes actually returned, not an oversized request extending
    // past EOF. The generated reply is a snapshot taken before model latency.
    reply.status = SimulateIo(reply.bytes, true);
    if (!reply.status.ok()) {
        reply.data.clear();
        reply.bytes = 0;
        return reply;
    }
    TrackObjectAccess(effective_disk, object_id, request.offset + reply.bytes, false, 0);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::ReadArchivedFileReply VirtualStorageServiceImpl::ReadArchivedFile(
    const zb::msg::ReadArchivedFileRequest& request) {
    (void)request;
    zb::msg::ReadArchivedFileReply reply;
    reply.status = zb::msg::Status::InvalidArgument("virtual node does not support archived-file read");
    return reply;
}

zb::msg::DeleteObjectReply VirtualStorageServiceImpl::DeleteObject(const zb::msg::DeleteObjectRequest& request) {
    zb::msg::DeleteObjectReply reply;
    const std::string object_id = request.ArchiveObjectId();
    if (request.disk_id.empty() || object_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id or object_id is empty");
        return reply;
    }
    std::string effective_disk;
    if (!ResolveEffectiveDisk(request.disk_id, object_id, false, &effective_disk)) {
        reply.status = zb::msg::Status::NotFound("Unknown disk_id: " + request.disk_id);
        return reply;
    }
    {
        std::lock_guard<std::mutex> lock(object_mu_);
        const std::string key = BuildObjectKey(effective_disk, object_id);
        auto it = object_sizes_.find(key);
        if (it != object_sizes_.end()) {
            if (model_state_store_) {
                std::string persist_error;
                if (!model_state_store_->DeleteObject(effective_disk, object_id, &persist_error)) {
                    reply.status = zb::msg::Status::IoError(persist_error);
                    return reply;
                }
            }
            const uint64_t size = it->second;
            object_sizes_.erase(key);
            auto home_it = object_home_disk_.find(object_id);
            if (home_it != object_home_disk_.end() && home_it->second == effective_disk) {
                object_home_disk_.erase(home_it);
            }
            uint64_t& used = disk_used_bytes_[effective_disk];
            used = used > size ? (used - size) : 0;
        }
    }
    RemoveObjectTracking(effective_disk, object_id);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::Status VirtualStorageServiceImpl::PutObject(const zb::data_node::ObjectWriteRequest& request) {
    zb::msg::WriteObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    object_request.offset = request.offset;
    object_request.data.assign(request.data.data(), request.data.size());
    object_request.epoch = request.epoch;
    object_request.is_replication = request.is_replication;
    return WriteObject(object_request).status;
}

zb::data_node::ObjectReadResult VirtualStorageServiceImpl::GetObject(const zb::data_node::ObjectReadRequest& request) {
    zb::msg::ReadObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    object_request.offset = request.offset;
    object_request.size = request.size;
    const zb::msg::ReadObjectReply reply = ReadObject(object_request);
    zb::data_node::ObjectReadResult result;
    result.status = reply.status;
    if (reply.status.ok()) {
        result.data = reply.data;
    }
    return result;
}

zb::msg::Status VirtualStorageServiceImpl::DeleteObject(const zb::data_node::ObjectDeleteRequest& request) {
    zb::msg::DeleteObjectRequest object_request;
    object_request.disk_id = request.disk_id;
    object_request.SetArchiveObjectId(request.object_id);
    return DeleteObject(object_request).status;
}

zb::msg::ResetNodeDataReply VirtualStorageServiceImpl::ResetNodeData(const zb::msg::ResetNodeDataRequest& request) {
    zb::msg::ResetNodeDataReply reply;
    if (request.confirm_token != "RESET_NODE_DATA") {
        reply.status = zb::msg::Status::InvalidArgument("confirm_token must be RESET_NODE_DATA");
        return reply;
    }
    if (!request.purge_objects && !request.purge_file_meta) {
        reply.status = zb::msg::Status::InvalidArgument("nothing to reset");
        return reply;
    }

    {
        std::lock_guard<std::mutex> lock(object_mu_);
        if (model_state_store_) {
            std::string persist_error;
            if (!model_state_store_->Clear(request.purge_objects, request.purge_file_meta,
                                           &persist_error)) {
                reply.status = zb::msg::Status::IoError(persist_error);
                return reply;
            }
        }
        if (request.purge_objects) {
            reply.objects_removed = static_cast<uint64_t>(object_sizes_.size());
            object_home_disk_.clear();
            object_sizes_.clear();
            for (auto& [_, used] : disk_used_bytes_) {
                used = 0;
            }
        }
        if (request.purge_file_meta) {
            std::string load_error;
            if (!file_meta_loaded_ && InitFileMetaStorePath()) {
                (void)LoadFileMetaStoreLocked(&load_error);
            }
            reply.file_meta_removed = static_cast<uint64_t>(file_meta_by_inode_.size());
            file_meta_by_inode_.clear();
            preloaded_file_objects_.clear();
            last_commit_txid_by_inode_.clear();
            last_commit_signature_by_inode_.clear();
            file_meta_loaded_ = true;
            std::string persist_error;
            if (!model_state_store_ &&
                (!InitFileMetaStorePath() || !PersistFileMetaStoreLocked(&persist_error))) {
                reply.status = zb::msg::Status::IoError(
                    persist_error.empty() ? "failed to persist empty file meta store" : persist_error);
                return reply;
            }
        }
    }

    if (request.purge_objects || request.purge_file_meta) {
        std::string clear_error;
        if (!archive_meta_store_.ClearAll(&clear_error)) {
            reply.status = zb::msg::Status::IoError(clear_error.empty() ? "failed to clear archive object meta"
                                                                        : clear_error);
            return reply;
        }
    }
    if (request.purge_file_meta) {
        std::string clear_error;
        if (!archive_file_meta_store_.ClearAll(&clear_error)) {
            reply.status = zb::msg::Status::IoError(clear_error.empty() ? "failed to clear archive file meta"
                                                                        : clear_error);
            return reply;
        }
    }
    {
        std::lock_guard<std::mutex> lock(repl_repair_mu_);
        repl_repair_queue_.clear();
        repl_repair_generation_.clear();
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::DiskReportReply VirtualStorageServiceImpl::GetDiskReport() const {
    zb::msg::DiskReportReply reply;
    std::unordered_map<std::string, uint64_t> used_snapshot;
    std::vector<std::string> disks;
    {
        std::lock_guard<std::mutex> lock(object_mu_);
        used_snapshot = disk_used_bytes_;
        disks.reserve(disk_set_.size());
        for (const auto& disk_id : disk_set_) {
            disks.push_back(disk_id);
        }
    }
    std::sort(disks.begin(), disks.end());
    for (const auto& disk_id : disks) {
        zb::msg::DiskReport report;
        report.id = disk_id;
        report.mount_point = config_.mount_point_prefix + "/" + disk_id;
        report.capacity_bytes = config_.disk_capacity_bytes;
        const uint64_t used = used_snapshot[disk_id];
        report.free_bytes = used >= report.capacity_bytes ? 0 : (report.capacity_bytes - used);
        report.is_healthy = true;
        reply.reports.push_back(std::move(report));
    }
    reply.status = zb::msg::Status::Ok();
    return reply;
}

bool VirtualStorageServiceImpl::ApplyFileMetaInternal(const zb::msg::ApplyFileMetaRequest& request,
                                                      const std::string* txid,
                                                      const std::string* commit_signature,
                                                      zb::msg::ApplyFileMetaReply* out) {
    if (!out) {
        return false;
    }
    zb::msg::ApplyFileMetaReply reply;
    if (request.meta.inode_id == 0) {
        reply.status = zb::msg::Status::InvalidArgument("inode_id is empty");
        *out = std::move(reply);
        return true;
    }
    if (request.meta.object_unit_size == 0) {
        reply.status = zb::msg::Status::InvalidArgument("object_unit_size is zero");
        *out = std::move(reply);
        return true;
    }

    const uint64_t now_ms = NowMilliseconds();
    const uint64_t now_sec = now_ms / 1000;
    std::lock_guard<std::mutex> lock(object_mu_);
    std::string load_error;
    if (!file_meta_loaded_) {
        if (!InitFileMetaStorePath() || !LoadFileMetaStoreLocked(&load_error)) {
            reply.status = zb::msg::Status::IoError(load_error.empty() ? "failed to load file meta store" : load_error);
            *out = std::move(reply);
            return true;
        }
    }
    if (txid && !txid->empty()) {
        auto tx_it = last_commit_txid_by_inode_.find(request.meta.inode_id);
        auto meta_it = file_meta_by_inode_.find(request.meta.inode_id);
        if (tx_it != last_commit_txid_by_inode_.end() &&
            tx_it->second == *txid &&
            meta_it != file_meta_by_inode_.end()) {
            if (model_state_store_) {
                const auto signature_it = last_commit_signature_by_inode_.find(request.meta.inode_id);
                if (!commit_signature || signature_it == last_commit_signature_by_inode_.end() ||
                    signature_it->second != *commit_signature) {
                    reply.status = zb::msg::Status::IoError("TXID_PARAMETER_MISMATCH");
                    *out = std::move(reply);
                    return true;
                }
            }
            reply.meta = meta_it->second;
            reply.status = zb::msg::Status::Ok();
            *out = std::move(reply);
            return true;
        }
    }
    auto it = file_meta_by_inode_.find(request.meta.inode_id);
    if (it == file_meta_by_inode_.end()) {
        if (!request.allow_create) {
            reply.status = zb::msg::Status::NotFound("file meta not found");
            *out = std::move(reply);
            return true;
        }
        if (request.expected_version > 0) {
            reply.status = zb::msg::Status::IoError("VERSION_MISMATCH");
            *out = std::move(reply);
            return true;
        }
        const auto tx_old_it = last_commit_txid_by_inode_.find(request.meta.inode_id);
        const bool had_old_tx = tx_old_it != last_commit_txid_by_inode_.end();
        const std::string old_txid = had_old_tx ? tx_old_it->second : std::string();
        zb::msg::FileMeta created = request.meta;
        created.version = created.version > 0 ? created.version : 1;
        created.mtime_sec = created.mtime_sec > 0 ? created.mtime_sec : now_sec;
        created.update_ts_ms = now_ms;
        file_meta_by_inode_[created.inode_id] = created;
        if (txid && !txid->empty()) {
            last_commit_txid_by_inode_[created.inode_id] = *txid;
            if (model_state_store_ && commit_signature)
                last_commit_signature_by_inode_[created.inode_id] = *commit_signature;
        }
        std::string persist_error;
        if (!PersistFileMetaRowLocked(created.inode_id, &persist_error)) {
            file_meta_by_inode_.erase(created.inode_id);
            if (had_old_tx) {
                last_commit_txid_by_inode_[created.inode_id] = old_txid;
            } else {
                last_commit_txid_by_inode_.erase(created.inode_id);
            }
            last_commit_signature_by_inode_.erase(created.inode_id);
            reply.status = zb::msg::Status::IoError(
                persist_error.empty() ? "failed to persist file meta store" : persist_error);
            *out = std::move(reply);
            return true;
        }
        reply.meta = created;
        reply.status = zb::msg::Status::Ok();
        *out = std::move(reply);
        return true;
    }

    const zb::msg::FileMeta old = it->second;
    const auto tx_old_it = last_commit_txid_by_inode_.find(request.meta.inode_id);
    const bool had_old_tx = tx_old_it != last_commit_txid_by_inode_.end();
    const std::string old_txid = had_old_tx ? tx_old_it->second : std::string();
    const auto signature_old_it = last_commit_signature_by_inode_.find(request.meta.inode_id);
    const bool had_old_signature = signature_old_it != last_commit_signature_by_inode_.end();
    const std::string old_signature = had_old_signature ? signature_old_it->second : std::string();
    zb::msg::FileMeta next = it->second;
    if (request.expected_version > 0 && request.expected_version != next.version) {
        reply.status = zb::msg::Status::IoError("VERSION_MISMATCH");
        *out = std::move(reply);
        return true;
    }
    next.file_size = request.meta.file_size;
    next.object_unit_size = request.meta.object_unit_size;
    next.version = next.version + 1;
    next.mtime_sec = request.meta.mtime_sec > 0 ? request.meta.mtime_sec : now_sec;
    next.update_ts_ms = now_ms;
    it->second = next;
    if (txid && !txid->empty()) {
        last_commit_txid_by_inode_[request.meta.inode_id] = *txid;
        if (model_state_store_ && commit_signature)
            last_commit_signature_by_inode_[request.meta.inode_id] = *commit_signature;
    }
    std::string persist_error;
    if (!PersistFileMetaRowLocked(request.meta.inode_id, &persist_error)) {
        it->second = old;
        if (had_old_tx) {
            last_commit_txid_by_inode_[request.meta.inode_id] = old_txid;
        } else {
            last_commit_txid_by_inode_.erase(request.meta.inode_id);
        }
        if (had_old_signature) {
            last_commit_signature_by_inode_[request.meta.inode_id] = old_signature;
        } else {
            last_commit_signature_by_inode_.erase(request.meta.inode_id);
        }
        reply.status = zb::msg::Status::IoError(
            persist_error.empty() ? "failed to persist file meta store" : persist_error);
        *out = std::move(reply);
        return true;
    }
    reply.meta = next;
    reply.status = zb::msg::Status::Ok();
    *out = std::move(reply);
    return true;
}

zb::msg::DeleteFileMetaReply VirtualStorageServiceImpl::DeleteFileMeta(const zb::msg::DeleteFileMetaRequest& request) {
    zb::msg::DeleteFileMetaReply reply;
    if (request.inode_id == 0) {
        reply.status = zb::msg::Status::InvalidArgument("inode_id is empty");
        return reply;
    }
    if (request.purge_objects && request.disk_id.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("disk_id is empty when purge_objects=true");
        return reply;
    }
    std::vector<std::pair<std::string, std::string>> removed_objects;
    {
        std::lock_guard<std::mutex> lock(object_mu_);
        std::string load_error;
        if (!file_meta_loaded_) {
            if (!InitFileMetaStorePath() || !LoadFileMetaStoreLocked(&load_error)) {
                reply.status = zb::msg::Status::IoError(
                    load_error.empty() ? "failed to load file meta store" : load_error);
                return reply;
            }
        }
        auto it = file_meta_by_inode_.find(request.inode_id);
        if (it == file_meta_by_inode_.end()) {
            reply.status = zb::msg::Status::NotFound("file meta not found");
            return reply;
        }
        const zb::msg::FileMeta old = it->second;

        if (request.purge_objects) {
            // Enumerate actual committed objects, including a stale object
            // beyond the latest file length, without allocating for a sparse
            // or maliciously large file_size.
            for (const auto& [key, size] : object_sizes_) {
                (void)size;
                const size_t split = key.find('|');
                if (split == std::string::npos) continue;
                const std::string object_id = key.substr(split + 1);
                uint64_t inode_id = 0;
                uint32_t index = 0;
                if (ParseStableObjectId(object_id, &inode_id, &index) &&
                    inode_id == request.inode_id)
                    removed_objects.emplace_back(key.substr(0, split), object_id);
            }
        }

        if (model_state_store_) {
            std::string persist_error;
            if (!model_state_store_->DeleteFileAndObjects(request.inode_id,
                                                           removed_objects, &persist_error)) {
                reply.status = zb::msg::Status::IoError(persist_error);
                return reply;
            }
        }
        for (const auto& [disk_id, object_id] : removed_objects) {
            const std::string key = BuildObjectKey(disk_id, object_id);
            auto obj_it = object_sizes_.find(key);
            if (obj_it == object_sizes_.end()) continue;
            const uint64_t size = obj_it->second;
            object_sizes_.erase(obj_it);
            auto home_it = object_home_disk_.find(object_id);
            if (home_it != object_home_disk_.end() && home_it->second == disk_id)
                object_home_disk_.erase(home_it);
            uint64_t& used = disk_used_bytes_[disk_id];
            used = used > size ? used - size : 0;
        }

        const auto tx_old_it = last_commit_txid_by_inode_.find(request.inode_id);
        const bool had_old_tx = tx_old_it != last_commit_txid_by_inode_.end();
        const std::string old_txid = had_old_tx ? tx_old_it->second : std::string();
        const auto preload_old_it = preloaded_file_objects_.find(request.inode_id);
        const bool had_old_preload = preload_old_it != preloaded_file_objects_.end();
        const PreloadedFileObjectInfo old_preload =
            had_old_preload ? preload_old_it->second : PreloadedFileObjectInfo{};
        file_meta_by_inode_.erase(it);
        last_commit_txid_by_inode_.erase(request.inode_id);
        last_commit_signature_by_inode_.erase(request.inode_id);
        preloaded_file_objects_.erase(request.inode_id);
        std::string persist_error;
        if (!model_state_store_ && !PersistFileMetaStoreLocked(&persist_error)) {
            file_meta_by_inode_[old.inode_id] = old;
            if (had_old_tx) {
                last_commit_txid_by_inode_[request.inode_id] = old_txid;
            }
            if (had_old_preload) {
                preloaded_file_objects_[request.inode_id] = old_preload;
            }
            reply.status = zb::msg::Status::IoError(
                persist_error.empty() ? "failed to persist file meta store" : persist_error);
            return reply;
        }
    }
    for (const auto& removed : removed_objects) {
        RemoveObjectTracking(removed.first, removed.second);
    }
    archive_file_meta_store_.RemoveFile(request.inode_id);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

bool VirtualStorageServiceImpl::PersistFileMetaRowLocked(uint64_t inode_id,
                                                         std::string* error) {
    if (!model_state_store_) return PersistFileMetaStoreLocked(error);
    auto it = file_meta_by_inode_.find(inode_id);
    if (it == file_meta_by_inode_.end()) {
        return model_state_store_->DeleteFileAndObjects(inode_id, {}, error);
    }
    ModelFileRecord row;
    row.meta = it->second;
    auto tx_it = last_commit_txid_by_inode_.find(inode_id);
    if (tx_it != last_commit_txid_by_inode_.end()) row.txid = tx_it->second;
    auto signature_it = last_commit_signature_by_inode_.find(inode_id);
    if (signature_it != last_commit_signature_by_inode_.end())
        row.commit_signature = signature_it->second;
    return model_state_store_->PutFile(row, error);
}

zb::msg::ResolveFileReadReply VirtualStorageServiceImpl::ResolveFileRead(
    const zb::msg::ResolveFileReadRequest& request) const {
    zb::msg::ResolveFileReadReply reply;
    if (request.inode_id == 0) {
        reply.status = zb::msg::Status::InvalidArgument("inode_id is empty");
        return reply;
    }
    if (request.size > std::numeric_limits<uint64_t>::max() - request.offset) {
        reply.status = zb::msg::Status::InvalidArgument("offset + size overflow");
        return reply;
    }

    std::lock_guard<std::mutex> lock(object_mu_);
    std::string load_error;
    if (!file_meta_loaded_) {
        if (!InitFileMetaStorePath() || !LoadFileMetaStoreLocked(&load_error)) {
            reply.status = zb::msg::Status::IoError(load_error.empty() ? "failed to load file meta store" : load_error);
            return reply;
        }
    }
    auto it = file_meta_by_inode_.find(request.inode_id);
    if (it == file_meta_by_inode_.end()) {
        reply.status = zb::msg::Status::NotFound("file meta not found");
        return reply;
    }

    reply.meta = it->second;
    const uint64_t object_unit_size = reply.meta.object_unit_size > 0 ? reply.meta.object_unit_size : kDefaultObjectUnitSize;
    if (request.size == 0) {
        reply.status = zb::msg::Status::Ok();
        return reply;
    }
    if (request.offset >= reply.meta.file_size) {
        reply.status = zb::msg::Status::Ok();
        return reply;
    }
    const uint64_t read_size = std::min<uint64_t>(request.size, reply.meta.file_size - request.offset);
    std::string range_error;
    if (!ValidateFileSliceRange(request.offset, read_size, object_unit_size, &range_error)) {
        reply.status = zb::msg::Status::InvalidArgument(range_error);
        return reply;
    }
    BuildObjectSlices(request.inode_id, request.offset, read_size, object_unit_size, request.disk_id, &reply.slices);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::AllocateFileWriteReply VirtualStorageServiceImpl::AllocateFileWrite(
    const zb::msg::AllocateFileWriteRequest& request) const {
    zb::msg::AllocateFileWriteReply reply;
    if (request.inode_id == 0) {
        reply.status = zb::msg::Status::InvalidArgument("inode_id is empty");
        return reply;
    }
    if (request.size > std::numeric_limits<uint64_t>::max() - request.offset) {
        reply.status = zb::msg::Status::InvalidArgument("offset + size overflow");
        return reply;
    }

    std::lock_guard<std::mutex> lock(object_mu_);
    std::string load_error;
    if (!file_meta_loaded_) {
        if (!InitFileMetaStorePath() || !LoadFileMetaStoreLocked(&load_error)) {
            reply.status = zb::msg::Status::IoError(load_error.empty() ? "failed to load file meta store" : load_error);
            return reply;
        }
    }
    auto it = file_meta_by_inode_.find(request.inode_id);
    if (it != file_meta_by_inode_.end()) {
        reply.meta = it->second;
    } else {
        reply.meta.inode_id = request.inode_id;
        reply.meta.file_size = 0;
        reply.meta.object_unit_size =
            request.object_unit_size_hint > 0 ? request.object_unit_size_hint : kDefaultObjectUnitSize;
        reply.meta.version = 0;
        reply.meta.mtime_sec = NowMilliseconds() / 1000;
        reply.meta.update_ts_ms = NowMilliseconds();
    }

    const uint64_t object_unit_size = reply.meta.object_unit_size > 0 ? reply.meta.object_unit_size : kDefaultObjectUnitSize;
    std::string range_error;
    if (!ValidateFileSliceRange(request.offset, request.size, object_unit_size, &range_error)) {
        reply.status = zb::msg::Status::InvalidArgument(range_error);
        return reply;
    }
    if (request.size > 0) {
        BuildObjectSlices(request.inode_id, request.offset, request.size, object_unit_size, request.disk_id, &reply.slices);
    }
    reply.txid = "tx-" + std::to_string(request.inode_id) + "-" + std::to_string(NowMilliseconds()) + "-" +
                 std::to_string(request.offset) + "-" + std::to_string(request.size);
    reply.status = zb::msg::Status::Ok();
    return reply;
}

zb::msg::CommitFileWriteReply VirtualStorageServiceImpl::CommitFileWrite(const zb::msg::CommitFileWriteRequest& request) {
    zb::msg::CommitFileWriteReply reply;
    if (request.inode_id == 0) {
        reply.status = zb::msg::Status::InvalidArgument("inode_id is empty");
        return reply;
    }
    if (request.txid.empty()) {
        reply.status = zb::msg::Status::InvalidArgument("txid is empty");
        return reply;
    }
    if (request.object_unit_size == 0) {
        reply.status = zb::msg::Status::InvalidArgument("object_unit_size is zero");
        return reply;
    }
    if (request.file_size > 0 &&
        1 + (request.file_size - 1) / request.object_unit_size >
            std::numeric_limits<uint32_t>::max()) {
        reply.status = zb::msg::Status::InvalidArgument("file object count exceeds supported range");
        return reply;
    }

    zb::msg::ApplyFileMetaRequest apply_req;
    apply_req.meta.inode_id = request.inode_id;
    apply_req.meta.file_size = request.file_size;
    apply_req.meta.object_unit_size = request.object_unit_size;
    apply_req.meta.version = request.expected_version;
    apply_req.meta.mtime_sec = request.mtime_sec > 0 ? request.mtime_sec : (NowMilliseconds() / 1000);
    apply_req.meta.update_ts_ms = NowMilliseconds();
    apply_req.expected_version = request.expected_version;
    apply_req.allow_create = request.allow_create;

    // Include all caller-controlled commit parameters. In particular, the
    // same txid cannot silently acknowledge a different size or CAS version.
    const std::string commit_signature =
        std::to_string(request.file_size) + ":" +
        std::to_string(request.object_unit_size) + ":" +
        std::to_string(request.expected_version) + ":" +
        (request.allow_create ? "1" : "0") + ":" +
        std::to_string(request.mtime_sec);

    zb::msg::ApplyFileMetaReply meta_reply;
    ApplyFileMetaInternal(apply_req, &request.txid, &commit_signature, &meta_reply);
    reply.status = meta_reply.status;
    reply.meta = meta_reply.meta;
    if (reply.status.ok()) {
        const uint64_t object_unit_size =
            reply.meta.object_unit_size > 0 ? reply.meta.object_unit_size : kDefaultObjectUnitSize;
        const uint32_t object_count = ComputeObjectCount(reply.meta.file_size, object_unit_size);
        if (!archive_file_meta_store_.UpsertFile(request.inode_id,
                                                 std::string(),
                                                 reply.meta.file_size,
                                                 object_count,
                                                 reply.meta.version,
                                                 true)) {
            reply.status = zb::msg::Status::IoError("failed to persist archive file meta");
        }
    }
    return reply;
}

bool VirtualStorageServiceImpl::InitFileMetaStorePath() const {
    if (!file_meta_store_path_.empty()) {
        return true;
    }
    std::string base_dir = !config_.archive_meta_dir.empty() ? config_.archive_meta_dir : config_.mount_point_prefix;
    if (base_dir.empty()) {
        return false;
    }
    std::error_code ec;
    fs::path meta_dir = fs::path(base_dir) / ".zb_meta";
    fs::create_directories(meta_dir, ec);
    if (ec) {
        return false;
    }
    file_meta_store_path_ = (meta_dir / "file_meta.tsv").string();
    return true;
}

bool VirtualStorageServiceImpl::LoadFileMetaStoreLocked(std::string* error) const {
    if (file_meta_loaded_) {
        return true;
    }
    if (file_meta_store_path_.empty() && !InitFileMetaStorePath()) {
        if (error) {
            *error = "file meta store path is unavailable";
        }
        return false;
    }
    std::ifstream in(file_meta_store_path_, std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        std::error_code ec;
        const bool exists = fs::exists(file_meta_store_path_, ec);
        if (ec || exists) {
            if (error) *error = "cannot open file meta store: " + file_meta_store_path_;
            return false;
        }
        file_meta_by_inode_.clear();
        preloaded_file_objects_.clear();
        last_commit_txid_by_inode_.clear();
        file_meta_loaded_ = true;
        return true;
    }
    std::unordered_map<uint64_t, zb::msg::FileMeta> loaded;
    std::unordered_map<uint64_t, PreloadedFileObjectInfo> loaded_preload;
    std::unordered_map<uint64_t, std::string> loaded_txids;
    std::string line;
    size_t line_number = 0;
    while (std::getline(in, line)) {
        ++line_number;
        if (line.empty()) {
            continue;
        }
        std::istringstream iss(line);
        std::string token;
        zb::msg::FileMeta meta;
        const auto invalid_row = [&]() {
            if (error) *error = "invalid file meta row " + std::to_string(line_number) +
                                " in " + file_meta_store_path_;
            return false;
        };
        const auto parse_number = [](const std::string& field, uint64_t* value) {
            const auto parsed = std::from_chars(field.data(), field.data() + field.size(), *value);
            return parsed.ec == std::errc{} && parsed.ptr == field.data() + field.size();
        };
        const auto read_number = [&](uint64_t* value) {
            return bool(std::getline(iss, token, '\t')) && parse_number(token, value);
        };
        if (!read_number(&meta.inode_id) || !read_number(&meta.file_size) ||
            !read_number(&meta.object_unit_size) || !read_number(&meta.version) ||
            !read_number(&meta.mtime_sec) || !read_number(&meta.update_ts_ms) ||
            meta.inode_id == 0 || meta.object_unit_size == 0 || loaded.count(meta.inode_id)) {
            return invalid_row();
        }
        if (std::getline(iss, token, '\t') && !token.empty()) {
            loaded_txids[meta.inode_id] = token;
        }
        PreloadedFileObjectInfo preload;
        bool has_preload = false;
        if (std::getline(iss, token, '\t') && !token.empty()) {
            preload.home_disk_id = token;
            has_preload = true;
        }
        if (std::getline(iss, token, '\t') && !token.empty()) {
            if (!has_preload || !parse_number(token, &preload.seed)) return invalid_row();
        }
        if (has_preload) {
            loaded_preload[meta.inode_id] = std::move(preload);
        }
        loaded[meta.inode_id] = meta;
    }
    if (in.bad() || !in.eof()) {
        if (error) *error = "cannot read file meta store: " + file_meta_store_path_;
        return false;
    }
    file_meta_by_inode_.swap(loaded);
    preloaded_file_objects_.swap(loaded_preload);
    last_commit_txid_by_inode_.swap(loaded_txids);
    file_meta_loaded_ = true;
    return true;
}

bool VirtualStorageServiceImpl::PersistFileMetaStoreLocked(std::string* error) const {
    if (file_meta_store_path_.empty()) {
        if (error) {
            *error = "file meta store path is empty";
        }
        return false;
    }
    const std::string tmp_path = file_meta_store_path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            if (error) {
                *error = "failed to open temp file for file meta store";
            }
            return false;
        }
        for (const auto& [inode, meta] : file_meta_by_inode_) {
            std::string txid;
            const auto tx_it = last_commit_txid_by_inode_.find(inode);
            if (tx_it != last_commit_txid_by_inode_.end()) {
                txid = tx_it->second;
            }
            out << inode << '\t'
                << meta.file_size << '\t'
                << meta.object_unit_size << '\t'
                << meta.version << '\t'
                << meta.mtime_sec << '\t'
                << meta.update_ts_ms << '\t'
                << txid;
            const auto preload_it = preloaded_file_objects_.find(inode);
            if (preload_it != preloaded_file_objects_.end()) {
                out << '\t'
                    << preload_it->second.home_disk_id << '\t'
                    << preload_it->second.seed;
            }
            out << '\n';
        }
        out.flush();
        if (!out.good()) {
            if (error) {
                *error = "failed to write file meta store";
            }
            return false;
        }
    }
    std::error_code ec;
    fs::remove(file_meta_store_path_, ec);
    ec.clear();
    fs::rename(tmp_path, file_meta_store_path_, ec);
    if (ec) {
        fs::remove(tmp_path, ec);
        if (error) {
            *error = "failed to rotate file meta store";
        }
        return false;
    }
    return true;
}

bool VirtualStorageServiceImpl::InitArchiveMetaStore(const std::string& meta_dir,
                                                     size_t max_objects,
                                                     uint32_t snapshot_interval_ops,
                                                     bool wal_fsync,
                                                     std::string* error) {
    archive_tracking_max_objects_ = max_objects > 0 ? max_objects : 1;
    if (!archive_meta_store_.Init(meta_dir,
                                  archive_tracking_max_objects_,
                                  snapshot_interval_ops,
                                  error,
                                  wal_fsync)) {
        return false;
    }
    if (!archive_file_meta_store_.Init(meta_dir,
                                       archive_tracking_max_objects_,
                                       snapshot_interval_ops,
                                       error,
                                       wal_fsync)) {
        return false;
    }
    return true;
}

bool VirtualStorageServiceImpl::FlushArchiveMetaSnapshot(std::string* error) {
    if (!archive_meta_store_.FlushSnapshot(error)) {
        return false;
    }
    return archive_file_meta_store_.FlushSnapshot(error);
}

zb::msg::Status VirtualStorageServiceImpl::UpdateArchiveState(const std::string& disk_id,
                                                              const std::string& object_id,
                                                              const std::string& archive_state,
                                                              uint64_t version) {
    if (disk_id.empty() || object_id.empty() || archive_state.empty()) {
        return zb::msg::Status::InvalidArgument("disk_id/object_id/archive_state is empty");
    }
    std::string effective_disk;
    if (!ResolveEffectiveDisk(disk_id, object_id, false, &effective_disk)) {
        effective_disk = disk_id;
    }
    if (!archive_meta_store_.UpdateObjectArchiveState(effective_disk, object_id, archive_state, version)) {
        return zb::msg::Status::IoError("failed to update archive state");
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status VirtualStorageServiceImpl::UpdateFileArchiveState(uint64_t inode_id,
                                                                  const std::string& disk_id,
                                                                  real_node::FileArchiveState archive_state,
                                                                  uint64_t version) {
    if (inode_id == 0) {
        return zb::msg::Status::InvalidArgument("inode_id is empty");
    }
    if (!archive_file_meta_store_.UpdateFileArchiveState(inode_id, disk_id, archive_state, version)) {
        return zb::msg::Status::IoError("failed to update file archive state");
    }
    return zb::msg::Status::Ok();
}

void VirtualStorageServiceImpl::SetArchiveTrackingMaxObjects(size_t max_objects) {
    archive_tracking_max_objects_ = max_objects > 0 ? max_objects : 1;
    archive_meta_store_.SetMaxObjects(archive_tracking_max_objects_);
    archive_file_meta_store_.SetMaxFiles(archive_tracking_max_objects_);
}

std::vector<ArchiveCandidateStat> VirtualStorageServiceImpl::CollectArchiveCandidates(uint32_t max_candidates,
                                                                                       uint64_t min_age_ms) const {
    const std::vector<real_node::ArchiveCandidateView> views =
        archive_meta_store_.CollectCandidates(max_candidates, min_age_ms, NowMilliseconds());
    std::vector<ArchiveCandidateStat> out;
    out.reserve(views.size());
    for (const auto& view : views) {
        ArchiveCandidateStat stat;
        stat.disk_id = view.disk_id;
        stat.SetArchiveObjectId(view.ArchiveObjectId());
        stat.last_access_ts_ms = view.last_access_ts_ms;
        stat.size_bytes = view.size_bytes;
        stat.checksum = view.checksum;
        stat.heat_score = view.heat_score;
        stat.archive_state = view.archive_state;
        stat.version = view.version;
        stat.score = view.score;
        stat.read_ops = view.read_ops;
        stat.write_ops = view.write_ops;
        out.push_back(std::move(stat));
    }
    return out;
}

std::vector<FileArchiveCandidateStat> VirtualStorageServiceImpl::CollectFileArchiveCandidates(uint32_t max_candidates,
                                                                                               uint64_t min_age_ms) const {
    const std::vector<real_node::ArchiveFileMeta> metas =
        archive_file_meta_store_.CollectCandidates(max_candidates, min_age_ms, NowMilliseconds());
    std::vector<FileArchiveCandidateStat> out;
    out.reserve(metas.size());
    for (const auto& meta : metas) {
        FileArchiveCandidateStat stat;
        stat.inode_id = meta.inode_id;
        stat.disk_id = meta.disk_id;
        stat.file_size = meta.file_size;
        stat.object_count = meta.object_count;
        stat.last_access_ts_ms = meta.last_access_ts_ms;
        stat.archive_state = "pending";
        stat.version = meta.version;
        out.push_back(std::move(stat));
    }
    return out;
}

bool VirtualStorageServiceImpl::ResolveEffectiveDisk(const std::string& requested_disk_id,
                                                     const std::string& object_id,
                                                     bool allow_dynamic_register,
                                                     std::string* effective_disk) {
    if (!effective_disk || requested_disk_id.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(object_mu_);
    auto disk_it = disk_set_.find(requested_disk_id);
    if (disk_it != disk_set_.end()) {
        *effective_disk = requested_disk_id;
        return true;
    }

    auto home_it = object_home_disk_.find(object_id);
    if (home_it != object_home_disk_.end() && disk_set_.find(home_it->second) != disk_set_.end()) {
        *effective_disk = home_it->second;
        return true;
    }

    if (!object_id.empty()) {
        const std::string suffix = "|" + object_id;
        for (const auto& entry : object_sizes_) {
            const std::string& key = entry.first;
            if (key.size() <= suffix.size() || key.compare(key.size() - suffix.size(), suffix.size(), suffix) != 0) {
                continue;
            }
            const size_t split = key.find('|');
            if (split == std::string::npos || split == 0) {
                continue;
            }
            const std::string disk_from_key = key.substr(0, split);
            if (disk_set_.find(disk_from_key) == disk_set_.end()) {
                continue;
            }
            object_home_disk_[object_id] = disk_from_key;
            *effective_disk = disk_from_key;
            return true;
        }
    }

    if (allow_dynamic_register && config_.allow_dynamic_disks) {
        disk_set_.insert(requested_disk_id);
        disk_used_bytes_.emplace(requested_disk_id, 0);
        *effective_disk = requested_disk_id;
        return true;
    }
    return false;
}

bool VirtualStorageServiceImpl::ValidateDisk(const std::string& disk_id) const {
    std::lock_guard<std::mutex> lock(object_mu_);
    return disk_set_.find(disk_id) != disk_set_.end();
}

void VirtualStorageServiceImpl::TrackObjectAccess(const std::string& disk_id,
                                                  const std::string& object_id,
                                                  uint64_t end_offset,
                                                  bool is_write,
                                                  uint64_t checksum) {
    const uint64_t now_ms = NowMilliseconds();
    archive_meta_store_.TrackObjectAccess(disk_id, object_id, end_offset, is_write, checksum, now_ms);

    uint64_t inode_id = 0;
    uint32_t object_index = 0;
    if (!ParseStableObjectId(object_id, &inode_id, &object_index) || inode_id == 0) {
        return;
    }

    FileArchiveAggregate aggregate;
    {
        std::lock_guard<std::mutex> lock(object_mu_);
        auto meta_it = file_meta_by_inode_.find(inode_id);
        if (meta_it != file_meta_by_inode_.end()) {
            aggregate.file_size = meta_it->second.file_size;
            const uint64_t object_unit_size =
                meta_it->second.object_unit_size > 0 ? meta_it->second.object_unit_size : kDefaultObjectUnitSize;
            aggregate.object_count = ComputeObjectCount(meta_it->second.file_size, object_unit_size);
        }
    }
    aggregate.object_count = std::max<uint32_t>(aggregate.object_count, object_index + 1);
    archive_file_meta_store_.TrackFileAccess(inode_id,
                                             disk_id,
                                             aggregate.file_size,
                                             aggregate.object_count,
                                             is_write,
                                             now_ms,
                                             0);
}

void VirtualStorageServiceImpl::RemoveObjectTracking(const std::string& disk_id, const std::string& object_id) {
    archive_meta_store_.RemoveObject(disk_id, object_id);
}

zb::msg::Status VirtualStorageServiceImpl::SimulateIo(uint64_t bytes, bool is_read) {
    using Clock = std::chrono::steady_clock;
    const uint64_t bandwidth = is_read ? config_.read_bytes_per_sec : config_.write_bytes_per_sec;
    if (bandwidth == 0 || config_.max_concurrent_io == 0) {
        return zb::msg::Status::IoError("INVALID_IO_RESOURCE_CONFIG");
    }
    // Keep model reservations within a finite horizon so a malformed huge RPC
    // cannot overflow steady_clock arithmetic or occupy the queue indefinitely.
    const long double transfer_us =
        std::ceil(static_cast<long double>(bytes) * 1000000.0L / bandwidth);
    constexpr long double kMaxTransferUs = 24.0L * 60 * 60 * 1000000;
    if (transfer_us > kMaxTransferUs) {
        return zb::msg::Status::IoError("MODEL_IO_DURATION_LIMIT");
    }
    const auto transfer = std::chrono::microseconds(static_cast<int64_t>(transfer_us));
    const auto fault = io_faults_.Next();
    const auto latency = std::chrono::milliseconds(
        static_cast<uint64_t>(is_read ? config_.read_base_latency_ms
                                      : config_.write_base_latency_ms) + RandomJitterMs() +
        (fault == zb::model::IoFault::kTimeout ? config_.io_timeout_ms : 0));
    Clock::time_point finish;
    {
        std::lock_guard<std::mutex> lock(io_mu_);
        const uint64_t budget = static_cast<uint64_t>(config_.max_concurrent_io) +
                                config_.io_queue_limit;
        if (io_outstanding_ >= budget) {
            return zb::msg::Status::IoError("MODEL_IO_QUEUE_FULL");
        }
        const auto now = Clock::now();
        if (io_slot_available_.empty()) {
            io_slot_available_.resize(config_.max_concurrent_io, now);
            io_bandwidth_available_ = now;
        }
        auto slot = std::min_element(io_slot_available_.begin(), io_slot_available_.end());
        const auto start = std::max(now, *slot);
        // Transfers share one node-wide read/write budget. Fixed device latency
        // may overlap across slots; the transfer timeline may not overlap.
        const auto transfer_start = std::max(start, io_bandwidth_available_);
        io_bandwidth_available_ = transfer_start + transfer;
        finish = io_bandwidth_available_ + latency;
        *slot = finish;
        ++io_outstanding_;
    }
    zb::rpc_route::WaitModelUntil(finish);
    {
        std::lock_guard<std::mutex> lock(io_mu_);
        --io_outstanding_;
    }
    if (fault == zb::model::IoFault::kFailure) return zb::msg::Status::IoError("MODEL_IO_INJECTED_FAILURE");
    if (fault == zb::model::IoFault::kTimeout) return zb::msg::Status::IoError("MODEL_IO_INJECTED_TIMEOUT");
    return zb::msg::Status::Ok();
}

IoResourceSnapshot VirtualStorageServiceImpl::GetIoResourceSnapshot() const {
    std::lock_guard<std::mutex> lock(io_mu_);
    return {io_outstanding_, config_.max_concurrent_io, config_.io_queue_limit};
}

uint32_t VirtualStorageServiceImpl::RandomJitterMs() {
    if (config_.jitter_ms == 0) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(rng_mu_);
    std::uniform_int_distribution<uint32_t> dist(0, config_.jitter_ms);
    return dist(rng_);
}

zb::msg::Status VirtualStorageServiceImpl::ReplicateWriteToSecondary(const zb::msg::WriteObjectRequest& request,
                                                                     uint64_t epoch) {
    ReplicationStatusSnapshot repl_snapshot = GetReplicationStatus();
    if (!repl_snapshot.replication_enabled || repl_snapshot.peer_address.empty()) {
        return zb::msg::Status::Ok();
    }

    brpc::Channel* channel = nullptr;
    {
        std::lock_guard<std::mutex> lock(channel_mu_);
        auto it = peer_channels_.find(repl_snapshot.peer_address);
        if (it == peer_channels_.end()) {
            auto new_channel = std::make_unique<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = static_cast<int>(replication_timeout_ms_);
            options.max_retry = 0;
            if (new_channel->Init(repl_snapshot.peer_address.c_str(), &options) != 0) {
                return zb::msg::Status::IoError("Failed to connect secondary " + repl_snapshot.peer_address);
            }
            channel = new_channel.get();
            peer_channels_[repl_snapshot.peer_address] = std::move(new_channel);
        } else {
            channel = it->second.get();
        }
    }

    if (!channel) {
        return zb::msg::Status::IoError("secondary channel unavailable");
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteObjectRequest replicate_req;
    const std::string object_id = request.ArchiveObjectId();
    replicate_req.set_disk_id(request.disk_id);
    replicate_req.set_object_id(object_id);
    replicate_req.set_offset(request.offset);
    replicate_req.set_data(request.data);
    replicate_req.set_is_replication(true);
    replicate_req.set_epoch(epoch);
    replicate_req.set_archive_op_id(request.archive_op_id);
    replicate_req.set_inode_id(request.inode_id);
    replicate_req.set_file_id(request.file_id);
    replicate_req.set_file_path(request.file_path);
    replicate_req.set_file_size(request.file_size);
    replicate_req.set_file_offset(request.file_offset);
    replicate_req.set_file_mode(request.file_mode);
    replicate_req.set_file_uid(request.file_uid);
    replicate_req.set_file_gid(request.file_gid);
    replicate_req.set_file_mtime(request.file_mtime);
    replicate_req.set_file_object_index(request.file_object_index);

    zb::rpc::WriteObjectReply replicate_resp;
    brpc::Controller cntl;
    stub.WriteObject(&cntl, &replicate_req, &replicate_resp, nullptr);
    if (cntl.Failed()) {
        return zb::msg::Status::IoError("replication rpc failed: " + cntl.ErrorText());
    }
    if (replicate_resp.status().code() != zb::rpc::STATUS_OK) {
        return zb::msg::Status::IoError("replication rejected: " + replicate_resp.status().message());
    }
    return zb::msg::Status::Ok();
}

void VirtualStorageServiceImpl::EnqueueReplicationRepair(const zb::msg::WriteObjectRequest& request, uint64_t epoch) {
    std::lock_guard<std::mutex> lock(repl_repair_mu_);
    const std::string key = BuildReplicationRepairKey(request);
    uint64_t generation = 1;
    auto gen_it = repl_repair_generation_.find(key);
    if (gen_it != repl_repair_generation_.end()) {
        generation = gen_it->second;
    } else {
        repl_repair_generation_[key] = generation;
    }
    for (auto qit = repl_repair_queue_.begin(); qit != repl_repair_queue_.end();) {
        if (qit->key == key) {
            qit = repl_repair_queue_.erase(qit);
        } else {
            ++qit;
        }
    }
    if (repl_repair_queue_.size() >= kReplicationRepairMaxQueue) {
        const ReplicationRepairTask dropped = std::move(repl_repair_queue_.front());
        repl_repair_queue_.pop_front();
        auto dropped_it = repl_repair_generation_.find(dropped.key);
        if (dropped_it != repl_repair_generation_.end() && dropped_it->second == dropped.generation) {
            repl_repair_generation_.erase(dropped_it);
        }
    }
    ReplicationRepairTask task;
    task.key = key;
    task.request = request;
    task.epoch = epoch;
    task.attempts = 0;
    task.generation = generation;
    repl_repair_queue_.push_back(std::move(task));
    repl_repair_cv_.notify_one();
}

uint64_t VirtualStorageServiceImpl::BumpReplicationRepairGeneration(const zb::msg::WriteObjectRequest& request) {
    std::lock_guard<std::mutex> lock(repl_repair_mu_);
    const std::string key = BuildReplicationRepairKey(request);
    uint64_t& generation = repl_repair_generation_[key];
    generation += 1;
    return generation;
}

std::string VirtualStorageServiceImpl::BuildReplicationRepairKey(const zb::msg::WriteObjectRequest& request) {
    return request.disk_id + "|" + request.ArchiveObjectId() + "|" +
           std::to_string(request.offset) + "|" + std::to_string(request.data.size());
}

void VirtualStorageServiceImpl::ReplicationRepairLoop() {
    while (true) {
        ReplicationRepairTask task;
        {
            std::unique_lock<std::mutex> lock(repl_repair_mu_);
            repl_repair_cv_.wait(lock, [this]() {
                return stop_repl_repair_.load() || !repl_repair_queue_.empty();
            });
            if (stop_repl_repair_.load() && repl_repair_queue_.empty()) {
                break;
            }
            task = std::move(repl_repair_queue_.front());
            repl_repair_queue_.pop_front();
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second != task.generation) {
                continue;
            }
        }

        zb::msg::Status status = ReplicateWriteToSecondary(task.request, task.epoch);
        if (status.ok()) {
            std::lock_guard<std::mutex> lock(repl_repair_mu_);
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second == task.generation) {
                repl_repair_generation_.erase(gen_it);
            }
            continue;
        }
        if (task.attempts + 1 >= kReplicationRepairMaxRetry) {
            std::lock_guard<std::mutex> lock(repl_repair_mu_);
            auto gen_it = repl_repair_generation_.find(task.key);
            if (gen_it != repl_repair_generation_.end() && gen_it->second == task.generation) {
                repl_repair_generation_.erase(gen_it);
            }
            continue;
        }

        ++task.attempts;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kReplicationRepairRetryIntervalMs * static_cast<int>(task.attempts)));
        std::lock_guard<std::mutex> lock(repl_repair_mu_);
        if (stop_repl_repair_.load()) {
            continue;
        }
        auto gen_it = repl_repair_generation_.find(task.key);
        if (gen_it != repl_repair_generation_.end() && gen_it->second != task.generation) {
            continue;
        }
        if (repl_repair_queue_.size() >= kReplicationRepairMaxQueue) {
            const ReplicationRepairTask dropped = std::move(repl_repair_queue_.front());
            repl_repair_queue_.pop_front();
            auto dropped_it = repl_repair_generation_.find(dropped.key);
            if (dropped_it != repl_repair_generation_.end() && dropped_it->second == dropped.generation) {
                repl_repair_generation_.erase(dropped_it);
            }
        }
        repl_repair_queue_.push_back(std::move(task));
        repl_repair_cv_.notify_one();
    }
}

std::string VirtualStorageServiceImpl::BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

zb::msg::Status VirtualStorageServiceImpl::ListTrackedObjects(
    std::vector<real_node::ArchiveObjectMeta>* objects) const {
    if (!objects) return zb::msg::Status::InvalidArgument("object listing output is null");
    objects->clear();
    const auto tracked = archive_meta_store_.SnapshotMetas();
    std::unordered_map<std::string, real_node::ArchiveObjectMeta> tracked_by_key;
    for (const auto& meta : tracked) tracked_by_key[BuildObjectKey(meta.disk_id, meta.object_id)] = meta;
    std::vector<real_node::ArchiveObjectMeta> out;
    std::lock_guard<std::mutex> lock(object_mu_);
    if (!file_meta_loaded_) {
        std::string error;
        if (!LoadFileMetaStoreLocked(&error)) return zb::msg::Status::IoError(error);
    }
    out.reserve(object_sizes_.size() + preloaded_file_objects_.size());
    std::unordered_set<std::string> emitted;
    for (const auto& entry : object_sizes_) {
        const size_t split = entry.first.find('|');
        if (split == std::string::npos || split == 0 || split + 1 == entry.first.size()) continue;
        real_node::ArchiveObjectMeta meta;
        meta.disk_id = entry.first.substr(0, split);
        meta.object_id = entry.first.substr(split + 1);
        meta.size_bytes = entry.second;
        auto tracked_it = tracked_by_key.find(entry.first);
        if (tracked_it != tracked_by_key.end()) {
            meta.checksum = tracked_it->second.checksum;
            meta.last_access_ts_ms = tracked_it->second.last_access_ts_ms;
        }
        out.push_back(std::move(meta));
        emitted.insert(entry.first);
    }
    for (const auto& preload : preloaded_file_objects_) {
        auto file_it = file_meta_by_inode_.find(preload.first);
        if (file_it == file_meta_by_inode_.end() || file_it->second.object_unit_size == 0) continue;
        const uint64_t count = file_it->second.file_size == 0 ? 0 :
            (file_it->second.file_size + file_it->second.object_unit_size - 1) /
            file_it->second.object_unit_size;
        for (uint64_t index = 0; index < count && index <= UINT32_MAX; ++index) {
            real_node::ArchiveObjectMeta meta;
            meta.disk_id = preload.second.home_disk_id;
            meta.object_id = BuildStableObjectId(preload.first, static_cast<uint32_t>(index));
            const std::string key = BuildObjectKey(meta.disk_id, meta.object_id);
            if (!emitted.insert(key).second) continue;
            const uint64_t offset = index * file_it->second.object_unit_size;
            meta.size_bytes = std::min<uint64_t>(file_it->second.object_unit_size,
                                                 file_it->second.file_size - offset);
            out.push_back(std::move(meta));
        }
    }
    *objects = std::move(out);
    return zb::msg::Status::Ok();
}

bool VirtualStorageServiceImpl::ParseStableObjectId(const std::string& object_id,
                                                    uint64_t* inode_id,
                                                    uint32_t* object_index) {
    if (!inode_id || !object_index) {
        return false;
    }
    constexpr const char* kPrefix = "obj-";
    if (object_id.rfind(kPrefix, 0) != 0) {
        return false;
    }
    const size_t split = object_id.find('-', 4);
    if (split == std::string::npos || split + 1 >= object_id.size()) {
        return false;
    }
    try {
        *inode_id = static_cast<uint64_t>(std::stoull(object_id.substr(4, split - 4)));
        *object_index = static_cast<uint32_t>(std::stoul(object_id.substr(split + 1)));
        return *inode_id > 0;
    } catch (const std::exception&) {
        return false;
    }
}

bool VirtualStorageServiceImpl::TryReadPreloadedObjectLocked(const std::string& effective_disk,
                                                             const std::string& object_id,
                                                             uint64_t offset,
                                                             uint64_t size,
                                                             std::string* out,
                                                             uint64_t* bytes_read) const {
    if (!out || !bytes_read) {
        return false;
    }
    *bytes_read = 0;
    out->clear();
    uint64_t inode_id = 0;
    uint32_t object_index = 0;
    if (!ParseStableObjectId(object_id, &inode_id, &object_index)) {
        return false;
    }
    const auto meta_it = file_meta_by_inode_.find(inode_id);
    if (meta_it == file_meta_by_inode_.end()) {
        return false;
    }
    const uint64_t object_unit_size =
        meta_it->second.object_unit_size > 0 ? meta_it->second.object_unit_size : kDefaultObjectUnitSize;
    if (object_unit_size == 0) {
        return false;
    }
    const auto preload_it = preloaded_file_objects_.find(inode_id);
    if (preload_it != preloaded_file_objects_.end() &&
        !preload_it->second.home_disk_id.empty() &&
        preload_it->second.home_disk_id != effective_disk) {
        return false;
    }

    const uint64_t object_begin = static_cast<uint64_t>(object_index) * object_unit_size;
    if (object_begin >= meta_it->second.file_size) {
        return false;
    }
    if (size == 0) return true;
    const uint64_t object_size = std::min<uint64_t>(object_unit_size, meta_it->second.file_size - object_begin);
    if (offset >= object_size) {
        return true;
    }
    const uint64_t read_len = std::min<uint64_t>(size, object_size - offset);
    out->assign(static_cast<size_t>(read_len), 'x');
    *bytes_read = read_len;
    return true;
}

void VirtualStorageServiceImpl::BuildObjectSlices(uint64_t inode_id,
                                                  uint64_t offset,
                                                  uint64_t size,
                                                  uint64_t object_unit_size,
                                                  const std::string& disk_id,
                                                  std::vector<zb::msg::FileObjectSlice>* slices) {
    if (!slices) {
        return;
    }
    slices->clear();
    if (inode_id == 0 || size == 0 || object_unit_size == 0) {
        return;
    }
    const uint64_t end = offset + size;
    const uint64_t start_index = offset / object_unit_size;
    const uint64_t end_index = (end - 1) / object_unit_size;
    slices->reserve(static_cast<size_t>(end_index - start_index + 1));
    for (uint64_t i = start_index; i <= end_index; ++i) {
        const uint64_t object_start = i * object_unit_size;
        const uint64_t seg_start = std::max<uint64_t>(object_start, offset);
        const uint64_t seg_end = seg_start +
            std::min<uint64_t>(object_unit_size - (seg_start - object_start), end - seg_start);
        if (seg_end <= seg_start) {
            continue;
        }
        zb::msg::FileObjectSlice slice;
        slice.object_index = static_cast<uint32_t>(i);
        slice.object_id = BuildStableObjectId(inode_id, slice.object_index);
        slice.disk_id = disk_id;
        slice.object_offset = seg_start - object_start;
        slice.length = seg_end - seg_start;
        slices->push_back(std::move(slice));
    }
}

uint64_t VirtualStorageServiceImpl::FastChecksum64(const std::string& data) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : data) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

uint64_t VirtualStorageServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

std::string VirtualStorageServiceImpl::BuildObjectKey(const std::string& disk_id, const std::string& object_id) {
    return disk_id + "|" + object_id;
}

} // namespace zb::virtual_node
