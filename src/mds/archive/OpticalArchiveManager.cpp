#include "OpticalArchiveManager.h"

#include <brpc/controller.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"
#include "../storage/UnifiedInodeLocation.h"
#include "../storage/UnifiedInodeRecord.h"
#include "mds.pb.h"
#include "real_node.pb.h"

namespace zb::mds {

namespace {

uint64_t NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

bool IsDiskReplica(const zb::rpc::ReplicaLocation& replica) {
    return replica.storage_tier() == zb::rpc::STORAGE_TIER_DISK;
}

bool IsReadyOpticalReplica(const zb::rpc::ReplicaLocation& replica) {
    return replica.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL &&
           replica.replica_state() == zb::rpc::REPLICA_READY;
}

uint32_t ComputeObjectCount(uint64_t file_size, uint64_t object_unit_size) {
    if (file_size == 0 || object_unit_size == 0) {
        return 0;
    }
    return static_cast<uint32_t>((file_size + object_unit_size - 1) / object_unit_size);
}

zb::rpc::InodeArchiveState ToInodeArchiveState(zb::rpc::FileArchiveState state) {
    switch (state) {
        case zb::rpc::FILE_ARCHIVE_ARCHIVING:
            return zb::rpc::INODE_ARCHIVE_ARCHIVING;
        case zb::rpc::FILE_ARCHIVE_ARCHIVED:
            return zb::rpc::INODE_ARCHIVE_ARCHIVED;
        case zb::rpc::FILE_ARCHIVE_PENDING:
        default:
            return zb::rpc::INODE_ARCHIVE_PENDING;
    }
}

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

std::string ArchiveObjectId(const zb::rpc::ArchiveLeaseRecord& record) {
    return record.object_id();
}

void EnsureReplicaObjectId(zb::rpc::ReplicaLocation* replica, const std::string& object_id) {
    if (!replica || object_id.empty()) {
        return;
    }
    replica->set_object_id(object_id);
}

void SetArchiveTaskIdentity(zb::rpc::ClaimArchiveTaskRequest* request,
                            uint64_t inode_id,
                            const std::string& object_id) {
    if (!request) {
        return;
    }
    if (inode_id != 0) {
        request->set_inode_id(inode_id);
    }
    if (!object_id.empty()) {
        request->set_object_id(object_id);
    }
}

void SetArchiveTaskIdentity(zb::rpc::RenewArchiveLeaseRequest* request,
                            uint64_t inode_id,
                            const std::string& object_id) {
    if (!request) {
        return;
    }
    if (inode_id != 0) {
        request->set_inode_id(inode_id);
    }
    if (!object_id.empty()) {
        request->set_object_id(object_id);
    }
}

void SetArchiveTaskIdentity(zb::rpc::CommitArchiveTaskRequest* request,
                            uint64_t inode_id,
                            const std::string& object_id) {
    if (!request) {
        return;
    }
    if (inode_id != 0) {
        request->set_inode_id(inode_id);
    }
    if (!object_id.empty()) {
        request->set_object_id(object_id);
    }
}

bool MatchesCandidateNodeId(const std::string& candidate_node_id, const std::string& replica_node_id) {
    if (candidate_node_id.empty()) {
        return true;
    }
    if (replica_node_id == candidate_node_id) {
        return true;
    }
    const std::string prefix = candidate_node_id + "-v";
    return replica_node_id.size() > prefix.size() && replica_node_id.rfind(prefix, 0) == 0;
}

bool ParseStableObjectId(const std::string& object_id, uint64_t* inode_id, uint32_t* object_index) {
    if (!inode_id || !object_index) {
        return false;
    }
    // Format: obj-<inode_id>-<object_index>
    constexpr char kPrefix[] = "obj-";
    if (object_id.rfind(kPrefix, 0) != 0) {
        return false;
    }
    const size_t second_dash = object_id.find('-', sizeof(kPrefix) - 1);
    if (second_dash == std::string::npos || second_dash + 1 >= object_id.size()) {
        return false;
    }
    const std::string inode_part = object_id.substr(sizeof(kPrefix) - 1, second_dash - (sizeof(kPrefix) - 1));
    const std::string index_part = object_id.substr(second_dash + 1);
    if (inode_part.empty() || index_part.empty()) {
        return false;
    }
    char* inode_end = nullptr;
    char* index_end = nullptr;
    const unsigned long long inode_value = std::strtoull(inode_part.c_str(), &inode_end, 10);
    const unsigned long long index_value = std::strtoull(index_part.c_str(), &index_end, 10);
    if (!inode_end || *inode_end != '\0' || !index_end || *index_end != '\0') {
        return false;
    }
    if (inode_value == 0 || index_value > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    *inode_id = static_cast<uint64_t>(inode_value);
    *object_index = static_cast<uint32_t>(index_value);
    return true;
}

} // namespace

OpticalArchiveManager::OpticalArchiveManager(RocksMetaStore* store,
                                             NodeStateCache* cache,
                                             FileArchiveCandidateQueue* candidate_queue,
                                             ArchiveBatchStager* batch_stager,
                                             ArchiveLeaseManager* lease_manager,
                                             Options options,
                                             std::shared_mutex* placement_transaction_mu)
    : store_(store),
      cache_(cache),
      candidate_queue_(candidate_queue),
      batch_stager_(batch_stager),
      lease_manager_(lease_manager),
      options_(options),
      placement_transaction_mu_(placement_transaction_mu) {
    if (options_.archive_target_bytes > options_.archive_trigger_bytes) {
        options_.archive_target_bytes = options_.archive_trigger_bytes;
    }
    if (options_.max_objects_per_round == 0) {
        options_.max_objects_per_round = 1;
    }
}

bool OpticalArchiveManager::RunOnce(std::string* error) {
    if (!store_ || !cache_) {
        if (error) {
            *error = "archive manager not initialized";
        }
        return false;
    }

    std::vector<NodeInfo> nodes = cache_->Snapshot();
    bool do_archive = ShouldArchiveNow(nodes);
    std::vector<NodeSelection> optical_nodes = cache_->PickNodesByType(1, NodeType::kOptical);

    if (!do_archive && options_.cold_file_ttl_sec == 0) {
        return true;
    }

    uint32_t archived_count = 0;
    uint64_t now = NowSeconds();

    rocksdb::WriteBatch batch;
    std::unordered_set<std::string> touched_object_keys;
    if (do_archive && !optical_nodes.empty() && candidate_queue_) {
        const NodeSelection& optical = optical_nodes.front();
        struct FileObjectTask {
            ArchiveCandidateEntry candidate;
            std::string object_key;
            zb::rpc::ReplicaLocation source_disk;
            uint64_t object_size{0};
            uint32_t object_index{0};
        };
        auto collect_file_object_tasks =
            [&](const FileArchiveCandidateEntry& file_candidate,
                std::vector<FileObjectTask>* out,
                uint64_t* total_size,
                std::string* local_error) -> bool {
            if (!out || !total_size || file_candidate.inode_id == 0) {
                if (local_error) {
                    *local_error = "invalid file task output buffers";
                }
                return false;
            }
            out->clear();
            *total_size = 0;
            zb::rpc::InodeAttr inode;
            if (!LoadInodeAttr(file_candidate.inode_id, &inode, local_error)) {
                return false;
            }
            if (inode.type() != zb::rpc::INODE_FILE) {
                if (local_error) {
                    *local_error = "inode is not file";
                }
                return false;
            }
            if (inode.file_archive_state() == zb::rpc::INODE_ARCHIVE_ARCHIVED) {
                return true;
            }
            zb::rpc::DiskFileLocation disk_location;
            std::string disk_error;
            if (!BuildDiskFileLocationFromAttr(inode, &disk_location, &disk_error)) {
                if (!disk_error.empty()) {
                    if (local_error) {
                        *local_error = disk_error;
                    }
                    return false;
                }
                if (!LoadDiskFileLocation(file_candidate.inode_id, &disk_location, &disk_error)) {
                    if (disk_error.empty()) {
                        return true;
                    }
                    if (local_error) {
                        *local_error = disk_error;
                    }
                    return false;
                }
            }
            uint64_t object_unit_size = inode.object_unit_size();
            if (object_unit_size == 0) {
                object_unit_size = options_.default_object_unit_size;
            }
            const uint64_t file_size = inode.size();
            const uint32_t object_count = ComputeObjectCount(file_size, object_unit_size);

            for (uint32_t object_index = 0; object_index < object_count; ++object_index) {
                FileObjectTask task;
                task.object_key.clear();
                task.object_index = object_index;
                task.source_disk.set_node_id(disk_location.node_id());
                task.source_disk.set_node_address(disk_location.node_address());
                task.source_disk.set_disk_id(disk_location.disk_id());
                task.source_disk.set_object_id("obj-" + std::to_string(file_candidate.inode_id) + "-" + std::to_string(object_index));
                task.source_disk.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
                task.source_disk.set_replica_state(zb::rpc::REPLICA_READY);
                const uint64_t object_offset = static_cast<uint64_t>(object_index) * object_unit_size;
                uint64_t object_size = object_unit_size;
                if (file_size > object_offset) {
                    object_size = std::min<uint64_t>(object_unit_size, file_size - object_offset);
                }
                task.object_size = object_size;
                task.source_disk.set_size(object_size);
                task.candidate.node_id = disk_location.node_id();
                task.candidate.node_address = disk_location.node_address();
                task.candidate.disk_id = disk_location.disk_id();
                task.candidate.SetArchiveObjectId(task.source_disk.object_id());
                task.candidate.size_bytes = object_size;
                task.candidate.archive_state = "pending";
                task.candidate.version = file_candidate.version;
                task.candidate.report_ts_ms = file_candidate.report_ts_ms;
                out->push_back(std::move(task));
                *total_size += object_size;
            }
            return true;
        };
        if (batch_stager_) {
            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_object_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }

            uint32_t staged_count = 0;
            std::unordered_set<uint64_t> seen_inodes;
            while (staged_count < options_.max_objects_per_round && !batch_stager_->HasSealedBatch()) {
                FileArchiveCandidateEntry file_candidate;
                if (!candidate_queue_->PopTop(&file_candidate)) {
                    break;
                }
                uint64_t file_total_size = 0;
                std::vector<FileObjectTask> file_tasks;
                std::string local_error;
                if (!collect_file_object_tasks(file_candidate, &file_tasks, &file_total_size, &local_error)) {
                    if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }
                if (!seen_inodes.insert(file_candidate.inode_id).second) {
                    continue;
                }
                if (file_tasks.empty()) {
                    std::string update_error;
                    (void)UpdateSourceFileArchiveState(file_candidate,
                                                       zb::rpc::FILE_ARCHIVE_ARCHIVED,
                                                       file_candidate.version,
                                                       &update_error);
                    if (error && error->empty() && !update_error.empty()) {
                        *error = update_error;
                    }
                    continue;
                }

                const uint32_t remaining_budget = options_.max_objects_per_round - staged_count;
                if (file_tasks.size() > static_cast<size_t>(remaining_budget)) {
                    std::vector<FileArchiveCandidateEntry> deferred_batch;
                    deferred_batch.push_back(file_candidate);
                    (void)candidate_queue_->PushBatch(deferred_batch);
                    continue;
                }

                const uint64_t disc_size = batch_stager_->DiscSizeBytes();
                const uint64_t current_bytes = batch_stager_->CurrentBytes();
                if (file_total_size > disc_size) {
                    if (error && error->empty()) {
                        *error = "file size exceeds configured archive disc size";
                    }
                    continue;
                }
                if (current_bytes > 0 && current_bytes + file_total_size > disc_size) {
                    std::vector<FileArchiveCandidateEntry> deferred_batch;
                    deferred_batch.push_back(file_candidate);
                    (void)candidate_queue_->PushBatch(deferred_batch);
                    continue;
                }
                if (!UpdateSourceFileArchiveState(file_candidate,
                                                  zb::rpc::FILE_ARCHIVE_ARCHIVING,
                                                  file_candidate.version,
                                                  &local_error)) {
                    if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }
                bool needs_staging = false;
                for (const auto& task : file_tasks) {
                    if (!batch_stager_->ContainsObject(task.candidate.ArchiveObjectId())) {
                        needs_staging = true;
                        break;
                    }
                }
                if (!needs_staging) {
                    continue;
                }

                std::string file_lease_id;
                std::string file_op_id;
                uint64_t file_lease_version = file_candidate.version > 0 ? file_candidate.version : 1;
                if (!ClaimFileLease(file_candidate,
                                    "inode:" + std::to_string(file_candidate.inode_id) + ":" + batch_stager_->CurrentBatchId(),
                                    &file_lease_id,
                                    &file_op_id,
                                    &file_lease_version,
                                    &local_error)) {
                    bool file_archived = false;
                    std::string state_error;
                    const zb::rpc::FileArchiveState file_state =
                        (HasUsableOpticalLocation(file_candidate.inode_id, &file_archived, &state_error) && file_archived)
                            ? zb::rpc::FILE_ARCHIVE_ARCHIVED
                            : zb::rpc::FILE_ARCHIVE_PENDING;
                    std::string update_error;
                    (void)UpdateSourceFileArchiveState(file_candidate,
                                                       file_state,
                                                       file_lease_version,
                                                       &update_error);
                    if (error && error->empty()) {
                        *error = !local_error.empty() ? local_error : update_error;
                    }
                    continue;
                }

                std::vector<std::pair<ArchiveCandidateEntry, uint64_t>> archiving_updates;
                std::vector<std::string> staged_object_ids;
                bool file_failed = false;
                bool file_deferred = false;

                for (const auto& task : file_tasks) {
                    if (batch_stager_->ContainsObject(task.candidate.ArchiveObjectId())) {
                        continue;
                    }

                    std::string data;
                    if (!ReadObjectFromReplica(task.source_disk, task.object_size, &data, &local_error)) {
                        file_failed = true;
                        break;
                    }

                    bool inserted = false;
                    bool deferred = false;
                    if (!batch_stager_->StageObject(task.candidate,
                                                   file_lease_id,
                                                   file_op_id,
                                                   task.object_key,
                                                   file_lease_version,
                                                   data,
                                                   &inserted,
                                                   &deferred,
                                                   &local_error)) {
                        file_failed = true;
                        break;
                    }

                    if (inserted) {
                        staged_object_ids.push_back(task.candidate.ArchiveObjectId());
                        archiving_updates.emplace_back(task.candidate, file_lease_version);
                        continue;
                    }
                    if (deferred) {
                        file_deferred = true;
                        file_failed = true;
                        break;
                    }
                    if (lease_manager_ && !file_lease_id.empty()) {
                        (void)batch_stager_->UpdateObjectLease(task.candidate.ArchiveObjectId(),
                                                               file_lease_id,
                                                               file_op_id,
                                                               file_lease_version,
                                                               nullptr);
                    }
                }

                if (file_failed) {
                    for (const auto& object_id : staged_object_ids) {
                        (void)batch_stager_->RemoveObject(object_id, nullptr);
                    }
                    std::string commit_error;
                    uint64_t commit_version = file_lease_version;
                    (void)CommitFileLease(file_candidate,
                                          file_lease_id,
                                          file_op_id,
                                          false,
                                          file_deferred ? "deferred to next archive batch"
                                                        : "file-level archive staging failed",
                                          &commit_version,
                                          &commit_error);
                    if (file_deferred) {
                        std::vector<FileArchiveCandidateEntry> deferred_batch;
                        deferred_batch.push_back(file_candidate);
                        (void)candidate_queue_->PushBatch(deferred_batch);
                    } else if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    std::string update_error;
                    (void)UpdateSourceFileArchiveState(file_candidate,
                                                       zb::rpc::FILE_ARCHIVE_PENDING,
                                                       commit_version,
                                                       &update_error);
                    if (error && error->empty() && !update_error.empty()) {
                        *error = update_error;
                    }
                    continue;
                }

                for (const auto& update : archiving_updates) {
                    std::string update_error;
                    (void)UpdateSourceArchiveState(update.first, "archiving", update.second, &update_error);
                }
                staged_count += static_cast<uint32_t>(staged_object_ids.size());
                std::string seal_error;
                (void)batch_stager_->SealIfReady(&seal_error);
                if (error && error->empty() && !seal_error.empty()) {
                    *error = seal_error;
                }
            }

            if (batch_stager_->HasSealedBatch()) {
                std::string burn_error;
                (void)BurnSealedBatch(optical, &archived_count, &touched_object_keys, &burn_error);
                if (error && error->empty() && !burn_error.empty()) {
                    *error = burn_error;
                }
            }
        } else {
            while (archived_count < options_.max_objects_per_round) {
                FileArchiveCandidateEntry file_candidate;
                if (!candidate_queue_->PopTop(&file_candidate)) {
                    break;
                }

                uint64_t file_total_size = 0;
                std::vector<FileObjectTask> file_tasks;
                std::string local_error;
                if (!collect_file_object_tasks(file_candidate, &file_tasks, &file_total_size, &local_error)) {
                    if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }
                if (file_tasks.empty()) {
                    std::string update_error;
                    (void)UpdateSourceFileArchiveState(file_candidate,
                                                       zb::rpc::FILE_ARCHIVE_ARCHIVED,
                                                       file_candidate.version,
                                                       &update_error);
                    if (error && error->empty() && !update_error.empty()) {
                        *error = update_error;
                    }
                    continue;
                }
                if (!UpdateSourceFileArchiveState(file_candidate,
                                                  zb::rpc::FILE_ARCHIVE_ARCHIVING,
                                                  file_candidate.version,
                                                  &local_error)) {
                    if (error && error->empty() && !local_error.empty()) {
                        *error = local_error;
                    }
                    continue;
                }
                bool file_failed = false;
                std::string file_lease_id;
                std::string file_op_id;
                uint64_t file_lease_version = file_candidate.version > 0 ? file_candidate.version : 1;
                if (!ClaimFileLease(file_candidate,
                                    "inode:" + std::to_string(file_candidate.inode_id),
                                    &file_lease_id,
                                    &file_op_id,
                                    &file_lease_version,
                                    &local_error)) {
                    bool file_archived = false;
                    std::string state_error;
                    const zb::rpc::FileArchiveState file_state =
                        (HasUsableOpticalLocation(file_candidate.inode_id, &file_archived, &state_error) && file_archived)
                            ? zb::rpc::FILE_ARCHIVE_ARCHIVED
                            : zb::rpc::FILE_ARCHIVE_PENDING;
                    std::string update_error;
                    (void)UpdateSourceFileArchiveState(file_candidate,
                                                       file_state,
                                                       file_lease_version,
                                                       &update_error);
                    if (error && error->empty()) {
                        *error = !local_error.empty() ? local_error : update_error;
                    }
                    continue;
                }
                std::vector<ArchiveCandidateEntry> archived_objects;
                zb::rpc::ReplicaLocation last_optical_location;
                for (const auto& task : file_tasks) {
                    zb::rpc::InodeAttr inode;
                    if (!LoadInodeAttr(file_candidate.inode_id, &inode, &local_error)) {
                        file_failed = true;
                        break;
                    }
                    std::string data;
                    if (!ReadObjectFromReplica(task.source_disk, task.object_size, &data, &local_error)) {
                        file_failed = true;
                        break;
                    }
                    if (!WriteObjectToOptical(optical,
                                              task.candidate.ArchiveObjectId(),
                                              file_op_id,
                                              data,
                                              file_candidate.inode_id,
                                              task.object_index,
                                              &inode,
                                              &last_optical_location,
                                              &local_error)) {
                        file_failed = true;
                        break;
                    }
                    if (!task.object_key.empty()) {
                        touched_object_keys.insert(task.object_key);
                    }
                    {
                        ++archived_count;
                        archived_objects.push_back(task.candidate);
                    }
                }
                uint64_t file_commit_version = file_lease_version;
                std::string commit_error;
                if (!CommitFileLease(file_candidate,
                                     file_lease_id,
                                     file_op_id,
                                     !file_failed,
                                     file_failed ? (local_error.empty() ? "archive candidate skipped/failed" : local_error)
                                                 : std::string(),
                                     &file_commit_version,
                                     &commit_error)) {
                    file_failed = true;
                    if (error && error->empty() && !commit_error.empty()) {
                        *error = commit_error;
                    }
                }
                if (!file_failed) {
                    std::shared_lock<std::shared_mutex> placement_guard;
                    if (placement_transaction_mu_) {
                        placement_guard = std::shared_lock<std::shared_mutex>(*placement_transaction_mu_);
                    }
                    if (!cache_->IsWriteAdmitted(last_optical_location.node_id())) {
                        file_failed = true;
                        local_error = "optical target is no longer admitted by CMS";
                    }
                    if (!file_failed) {
                        zb::rpc::InodeAttr inode;
                        if (!LoadInodeAttr(file_candidate.inode_id, &inode, &local_error)) {
                            file_failed = true;
                            if (error && error->empty()) {
                                *error = local_error;
                            }
                        } else {
                            rocksdb::WriteBatch metadata_batch;
                            zb::rpc::OpticalFileLocation location;
                            location.set_node_id(last_optical_location.node_id());
                            location.set_node_address(last_optical_location.node_address());
                            location.set_disk_id(last_optical_location.disk_id());
                            location.set_image_id(last_optical_location.image_id());
                            location.set_file_id("inode-" + std::to_string(file_candidate.inode_id));
                            location.set_file_path("/inode/" + std::to_string(file_candidate.inode_id));
                            if (!SaveOpticalFileLocation(file_candidate.inode_id, location, &metadata_batch, &local_error)) {
                                file_failed = true;
                            } else if (!store_->WriteBatch(&metadata_batch, &local_error)) {
                                file_failed = true;
                            }
                            if (file_failed && error && error->empty() && !local_error.empty()) {
                                *error = local_error;
                            }
                        }
                    }
                }
                if (!file_failed) {
                    for (const auto& archived : archived_objects) {
                        std::string update_error;
                        (void)UpdateSourceArchiveState(archived,
                                                       "archived",
                                                       file_commit_version,
                                                       &update_error);
                    }
                }
                std::string update_error;
                (void)UpdateSourceFileArchiveState(file_candidate,
                                                   file_failed ? zb::rpc::FILE_ARCHIVE_PENDING
                                                               : zb::rpc::FILE_ARCHIVE_ARCHIVED,
                                                   file_commit_version,
                                                   &update_error);
                if (error && error->empty() && !update_error.empty()) {
                    *error = update_error;
                }
            }
        }
    }

    if (options_.cold_file_ttl_sec == 0) {
        const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_objects_per_round * 4);
        std::string reconcile_error;
        (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
        if (error && error->empty() && !reconcile_error.empty()) {
            *error = reconcile_error;
        }
        return true;
    }

    uint32_t evicted_files = 0;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek("I/"); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind("I/", 0) != 0) {
            break;
        }
        if (evicted_files >= options_.max_objects_per_round) {
            break;
        }

        zb::rpc::InodeAttr inode;
        std::string decode_error;
        if (!MetaCodec::DecodeUnifiedInodeAttr(it->value().ToString(), &inode, &decode_error) ||
            inode.type() != zb::rpc::INODE_FILE) {
            continue;
        }
        if (inode.file_archive_state() != zb::rpc::INODE_ARCHIVE_ARCHIVED) {
            continue;
        }
        const bool cold =
            inode.atime() > 0 && inode.atime() + options_.cold_file_ttl_sec <= now;
        if (!cold) {
            continue;
        }

        zb::rpc::DiskFileLocation disk_location;
        std::string local_error;
        if (!BuildDiskFileLocationFromAttr(inode, &disk_location, &local_error)) {
            if (!local_error.empty()) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                continue;
            }
            if (!LoadDiskFileLocation(inode.inode_id(), &disk_location, &local_error)) {
                if (error && error->empty() && !local_error.empty()) {
                    *error = local_error;
                }
                continue;
            }
        }
        zb::rpc::OpticalFileLocation optical_location;
        if (!BuildOpticalFileLocationFromAttr(inode, inode.inode_id(), &optical_location, &local_error)) {
            if (!local_error.empty()) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                continue;
            }
            if (!LoadOpticalFileLocation(inode.inode_id(), &optical_location, &local_error)) {
                if (error && error->empty() && !local_error.empty()) {
                    *error = local_error;
                }
                continue;
            }
        }
        if (!DeleteDiskFile(inode.inode_id(), disk_location, &local_error)) {
            if (error && error->empty() && !local_error.empty()) {
                *error = local_error;
            }
            continue;
        }
        if (!DeleteDiskFileLocation(inode.inode_id(), &batch, &local_error)) {
            if (error && error->empty() && !local_error.empty()) {
                *error = local_error;
            }
            continue;
        }
        ++evicted_files;
    }

    if (batch.Count() > 0) {
        std::string write_error;
        if (!store_->WriteBatch(&batch, &write_error)) {
            if (error) {
                *error = write_error;
            }
            return false;
        }
    }

    const uint32_t reconcile_budget = std::max<uint32_t>(64, options_.max_objects_per_round * 4);
    std::string reconcile_error;
    (void)ReconcileArchiveStates(reconcile_budget, &reconcile_error);
    if (error && error->empty() && !reconcile_error.empty()) {
        *error = reconcile_error;
    }

    return true;
}

bool OpticalArchiveManager::BurnSealedBatch(const NodeSelection& optical,
                                            uint32_t* archived_count,
                                            std::unordered_set<std::string>* touched_object_keys,
                                            std::string* error) {
    if (!batch_stager_) {
        return true;
    }
    struct PreparedBurnTask {
        StagedArchiveObject staged;
        std::string lease_id;
        std::string op_id;
        uint64_t version{0};
        std::string object_key;
        uint64_t data_size{0};
        zb::rpc::ReplicaLocation optical_location;
        uint64_t inode_id{0};
    };
    struct FileLeaseContext {
        FileArchiveCandidateEntry candidate;
        std::string lease_id;
        std::string op_id;
        uint64_t version{0};
        bool ready{false};
        bool already_archived{false};
    };

    std::vector<StagedArchiveObject> tasks = batch_stager_->SnapshotSealedBatch();
    std::vector<PreparedBurnTask> prepared;
    prepared.reserve(tasks.size());
    std::unordered_map<uint64_t, FileLeaseContext> file_leases;
    bool batch_failed = false;

    for (const auto& staged : tasks) {
        std::string local_error;
        uint64_t inode_id = 0;
        uint32_t object_index = 0;
        if (!ResolveObjectOwner(staged.candidate.ArchiveObjectId(), &inode_id, &object_index, &local_error)) {
            if (error && error->empty()) {
                *error = local_error.empty()
                             ? "failed to resolve object owner for " + staged.candidate.ArchiveObjectId()
                             : local_error;
            }
            batch_failed = true;
            break;
        }

        FileLeaseContext& file_lease = file_leases[inode_id];
        if (!file_lease.ready && !file_lease.already_archived) {
            file_lease.candidate.inode_id = inode_id;
            file_lease.candidate.node_id = staged.candidate.node_id;
            file_lease.candidate.node_address = staged.candidate.node_address;
            file_lease.candidate.disk_id = staged.candidate.disk_id;
            file_lease.candidate.version = staged.version > 0 ? staged.version : 1;

            bool lease_ready = false;
            if (lease_manager_ && !staged.lease_id.empty()) {
                lease_ready = RenewFileLease(file_lease.candidate,
                                             staged.lease_id,
                                             &file_lease.lease_id,
                                             &file_lease.op_id,
                                             &file_lease.version,
                                             &local_error);
            } else {
                lease_ready = ClaimFileLease(file_lease.candidate,
                                             "inode:" + std::to_string(inode_id) + ":" + batch_stager_->CurrentBatchId(),
                                             &file_lease.lease_id,
                                             &file_lease.op_id,
                                             &file_lease.version,
                                             &local_error);
            }

            if (!lease_ready) {
                bool file_archived = false;
                std::string archived_error;
                if (HasUsableOpticalLocation(inode_id, &file_archived, &archived_error) && file_archived) {
                    file_lease.already_archived = true;
                    file_lease.version = std::max<uint64_t>(file_lease.version, staged.version);
                } else {
                    if (error && error->empty()) {
                        *error = !local_error.empty() ? local_error : archived_error;
                    }
                    batch_failed = true;
                    break;
                }
            } else {
                file_lease.ready = true;
            }
        }

        if (file_lease.already_archived) {
            std::string update_error;
            (void)UpdateSourceArchiveState(staged.candidate, "archived", file_lease.version, &update_error);
            (void)batch_stager_->MarkObjectDone(staged.candidate.ArchiveObjectId(), nullptr);
            continue;
        }
        if (lease_manager_ && !file_lease.lease_id.empty()) {
            (void)batch_stager_->UpdateObjectLease(staged.candidate.ArchiveObjectId(),
                                                   file_lease.lease_id,
                                                   file_lease.op_id,
                                                   file_lease.version,
                                                   nullptr);
        }

        std::string data;
        if (!batch_stager_->ReadObjectData(staged, &data, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }

        std::string object_key = staged.object_key;
        if (!ResolveObjectOwner(staged.candidate.ArchiveObjectId(),
                                &inode_id,
                                &object_index,
                                &local_error)) {
            if (error && error->empty()) {
                *error = local_error.empty()
                             ? "failed to resolve object owner for " + staged.candidate.ArchiveObjectId()
                             : local_error;
            }
            batch_failed = true;
            break;
        }
        zb::rpc::InodeAttr inode;
        if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
            if (error && error->empty()) {
                *error = local_error.empty() ? "failed to load inode for staged object" : local_error;
            }
            batch_failed = true;
            break;
        }

        zb::rpc::ReplicaLocation optical_location;
        if (!WriteObjectToOptical(optical,
                                 staged.candidate.ArchiveObjectId(),
                                 file_lease.op_id,
                                 data,
                                 inode_id,
                                 object_index,
                                 &inode,
                                 &optical_location,
                                 &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            batch_failed = true;
            break;
        }
        PreparedBurnTask item;
        item.staged = staged;
        item.lease_id = file_lease.lease_id;
        item.op_id = file_lease.op_id;
        item.version = file_lease.version;
        item.object_key = object_key;
        item.data_size = static_cast<uint64_t>(data.size());
        item.optical_location = optical_location;
        item.inode_id = inode_id;
        prepared.push_back(std::move(item));
    }

    if (batch_failed) {
        for (const auto& item : file_leases) {
            if (!item.second.ready) {
                continue;
            }
            std::string commit_error;
            uint64_t commit_version = item.second.version;
            (void)CommitFileLease(item.second.candidate,
                                  item.second.lease_id,
                                  item.second.op_id,
                                  false,
                                  "sealed archive batch failed",
                                  &commit_version,
                                  &commit_error);
        }
        return false;
    }

    if (!prepared.empty()) {
        std::unordered_map<uint64_t, uint64_t> archived_versions;
        bool all_committed = true;
        for (auto& item : file_leases) {
            if (!item.second.ready) {
                continue;
            }
            std::string commit_error;
            uint64_t commit_version = item.second.version;
            if (!CommitFileLease(item.second.candidate,
                                 item.second.lease_id,
                                 item.second.op_id,
                                 true,
                                 std::string(),
                                 &commit_version,
                                 &commit_error)) {
                all_committed = false;
                if (error && error->empty()) {
                    *error = commit_error.empty() ? "failed to commit file archive task" : commit_error;
                }
                continue;
            }
            archived_versions[item.first] = commit_version;
        }
        if (!all_committed) {
            return false;
        }

        rocksdb::WriteBatch metadata_batch;
        std::string local_error;
        std::shared_lock<std::shared_mutex> placement_guard;
        if (placement_transaction_mu_) {
            placement_guard = std::shared_lock<std::shared_mutex>(*placement_transaction_mu_);
        }
        std::unordered_map<uint64_t, const PreparedBurnTask*> representative_tasks;
        for (const auto& item : prepared) {
            if (representative_tasks.find(item.inode_id) == representative_tasks.end()) {
                representative_tasks[item.inode_id] = &item;
            }
        }
        for (const auto& entry : representative_tasks) {
            const uint64_t inode_id = entry.first;
            const PreparedBurnTask& task = *entry.second;
            if (!cache_->IsWriteAdmitted(task.optical_location.node_id())) {
                if (error && error->empty()) {
                    *error = "optical target is no longer admitted by CMS";
                }
                return false;
            }
            zb::rpc::InodeAttr inode;
            if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                return false;
            }
            zb::rpc::OpticalFileLocation location;
            location.set_node_id(task.optical_location.node_id());
            location.set_node_address(task.optical_location.node_address());
            location.set_disk_id(task.optical_location.disk_id());
            location.set_image_id(task.optical_location.image_id());
            location.set_file_id("inode-" + std::to_string(inode_id));
            location.set_file_path("/inode/" + std::to_string(inode_id));
            if (!SaveOpticalFileLocation(inode_id, location, &metadata_batch, &local_error)) {
                if (error && error->empty()) {
                    *error = local_error;
                }
                return false;
            }
        }
        if (!store_->WriteBatch(&metadata_batch, &local_error)) {
            if (error && error->empty()) {
                *error = local_error;
            }
            return false;
        }

        std::unordered_map<uint64_t, FileArchiveCandidateEntry> archived_files;
        for (const auto& item : prepared) {
            const uint64_t archived_version =
                archived_versions.count(item.inode_id) > 0 ? archived_versions[item.inode_id] : item.version;
            std::string update_error;
            (void)UpdateSourceArchiveState(item.staged.candidate, "archived", archived_version, &update_error);
            (void)batch_stager_->MarkObjectDone(item.staged.candidate.ArchiveObjectId(), nullptr);
            FileArchiveCandidateEntry& file_entry = archived_files[item.inode_id];
            file_entry.inode_id = item.inode_id;
            file_entry.node_id = item.staged.candidate.node_id;
            file_entry.node_address = item.staged.candidate.node_address;
            file_entry.disk_id = item.staged.candidate.disk_id;
            file_entry.version = std::max<uint64_t>(file_entry.version, archived_version);
            if (archived_count) {
                ++(*archived_count);
            }
        }
        for (const auto& item : archived_files) {
            std::string file_error;
            (void)UpdateSourceFileArchiveState(item.second,
                                               zb::rpc::FILE_ARCHIVE_ARCHIVED,
                                               item.second.version,
                                               &file_error);
            if (error && error->empty() && !file_error.empty()) {
                *error = file_error;
            }
        }
    }

    std::string reset_error;
    if (!batch_stager_->ResetIfDrained(&reset_error) && error && error->empty()) {
        *error = reset_error;
    }
    return true;
}

bool OpticalArchiveManager::UpdateSourceArchiveState(const ArchiveCandidateEntry& candidate,
                                                     const std::string& archive_state,
                                                     uint64_t version,
                                                     std::string* error) {
    if (candidate.node_address.empty() || candidate.disk_id.empty() || candidate.ArchiveObjectId().empty() || archive_state.empty()) {
        return false;
    }
    brpc::Channel* channel = GetChannel(candidate.node_address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::UpdateArchiveStateRequest req;
    req.set_disk_id(candidate.disk_id);
    req.set_object_id(candidate.ArchiveObjectId());
    req.set_archive_state(archive_state);
    req.set_version(version);

    zb::rpc::UpdateArchiveStateReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    stub.UpdateArchiveState(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::UpdateInodeArchiveState(uint64_t inode_id,
                                                    zb::rpc::InodeArchiveState archive_state,
                                                    std::string* error) {
    if (!store_ || inode_id == 0) {
        if (error) {
            *error = "invalid inode archive state update args";
        }
        return false;
    }

    zb::rpc::InodeAttr inode;
    std::string local_error;
    if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to load inode for archive state update" : local_error;
        }
        return false;
    }
    if (inode.type() != zb::rpc::INODE_FILE) {
        if (error) {
            *error = "inode is not a file";
        }
        return false;
    }
    if (inode.file_archive_state() == archive_state) {
        return true;
    }
    inode.set_file_archive_state(archive_state);
    std::string current_payload;
    if (store_->Get(InodeKey(inode_id), &current_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(current_payload, &record, nullptr)) {
            ApplyAttrToUnifiedRecord(inode, &record);
            std::string encoded;
            if (!MetaCodec::EncodeUnifiedInodeRecord(record, &encoded, &local_error) ||
                !store_->Put(InodeKey(inode_id), encoded, &local_error)) {
                if (error) {
                    *error = local_error.empty() ? "failed to persist inode archive state" : local_error;
                }
                return false;
            }
            return true;
        }
    }
    UnifiedInodeRecord fallback_record;
    if (!AttrToUnifiedInodeRecord(inode, 0, UnifiedStorageTier::kNone, &fallback_record, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to normalize inode archive state" : local_error;
        }
        return false;
    }
    std::string encoded;
    if (!MetaCodec::EncodeUnifiedInodeRecord(fallback_record, &encoded, &local_error) ||
        !store_->Put(InodeKey(inode_id), encoded, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "failed to persist inode archive state" : local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::UpdateSourceFileArchiveState(const FileArchiveCandidateEntry& candidate,
                                                         zb::rpc::FileArchiveState archive_state,
                                                         uint64_t version,
                                                         std::string* error) {
    if (candidate.inode_id == 0) {
        if (error) {
            *error = "invalid file archive state update args";
        }
        return false;
    }
    if (!UpdateInodeArchiveState(candidate.inode_id, ToInodeArchiveState(archive_state), error)) {
        return false;
    }
    if (candidate.node_address.empty() || candidate.disk_id.empty()) {
        return true;
    }
    brpc::Channel* channel = GetChannel(candidate.node_address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::UpdateFileArchiveStateRequest req;
    req.set_inode_id(candidate.inode_id);
    req.set_disk_id(candidate.disk_id);
    req.set_archive_state(archive_state);
    req.set_version(version);

    zb::rpc::UpdateFileArchiveStateReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(2000);
    stub.UpdateFileArchiveState(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::HasUsableOpticalLocation(uint64_t inode_id,
                                                     bool* archived,
                                                     std::string* error) {
    if (!archived || inode_id == 0 || !store_) {
        if (error) {
            *error = "invalid file archive check args";
        }
        return false;
    }
    *archived = false;

    zb::rpc::InodeAttr inode;
    if (!LoadInodeAttr(inode_id, &inode, error)) {
        return false;
    }
    zb::rpc::OpticalFileLocation location;
    std::string local_error;
    if (!BuildOpticalFileLocationFromAttr(inode, inode_id, &location, &local_error)) {
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
        if (!LoadOpticalFileLocation(inode_id, &location, &local_error)) {
            if (local_error.empty()) {
                return true;
            }
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    *archived = !location.node_id().empty() && !location.disk_id().empty();
    return true;
}

bool OpticalArchiveManager::ClaimFileLease(const FileArchiveCandidateEntry& candidate,
                                           const std::string& request_id,
                                           std::string* lease_id,
                                           std::string* op_id,
                                           uint64_t* version,
                                           std::string* error) {
    if (lease_id) {
        lease_id->clear();
    }
    if (op_id) {
        op_id->clear();
    }
    if (version) {
        *version = candidate.version > 0 ? candidate.version : 1;
    }
    if (candidate.inode_id == 0) {
        if (error) {
            *error = "invalid file lease candidate";
        }
        return false;
    }
    if (!lease_manager_) {
        if (op_id) {
            *op_id = "inode:" + std::to_string(candidate.inode_id);
        }
        return true;
    }

    zb::rpc::ClaimArchiveTaskRequest req;
    req.set_node_id(candidate.node_id);
    req.set_node_address(candidate.node_address);
    req.set_disk_id(candidate.disk_id);
    SetArchiveTaskIdentity(&req, candidate.inode_id, std::string());
    req.set_requested_lease_ms(0);
    req.set_request_id(request_id.empty() ? ("inode:" + std::to_string(candidate.inode_id)) : request_id);

    zb::rpc::ClaimArchiveTaskReply reply;
    std::string local_error;
    if (!lease_manager_->Claim(req, &reply, &local_error) || !reply.granted()) {
        if (error) {
            *error = local_error.empty() ? "failed to claim file archive lease" : local_error;
        }
        return false;
    }
    if (lease_id) {
        *lease_id = reply.record().lease_id();
    }
    if (op_id) {
        *op_id = !reply.record().lease_id().empty() ? reply.record().lease_id()
                                                    : ("inode:" + std::to_string(candidate.inode_id));
    }
    if (version) {
        *version = reply.record().version() > 0 ? reply.record().version() : (candidate.version > 0 ? candidate.version : 1);
    }
    return true;
}

bool OpticalArchiveManager::RenewFileLease(const FileArchiveCandidateEntry& candidate,
                                           const std::string& lease_id,
                                           std::string* renewed_lease_id,
                                           std::string* renewed_op_id,
                                           uint64_t* version,
                                           std::string* error) {
    if (renewed_lease_id) {
        renewed_lease_id->clear();
    }
    if (renewed_op_id) {
        renewed_op_id->clear();
    }
    if (version) {
        *version = candidate.version > 0 ? candidate.version : 1;
    }
    if (candidate.inode_id == 0) {
        if (error) {
            *error = "invalid file lease candidate";
        }
        return false;
    }
    if (!lease_manager_) {
        if (renewed_op_id) {
            *renewed_op_id = !lease_id.empty() ? lease_id : ("inode:" + std::to_string(candidate.inode_id));
        }
        if (renewed_lease_id) {
            *renewed_lease_id = lease_id;
        }
        return true;
    }
    if (lease_id.empty()) {
        if (error) {
            *error = "file lease id is empty";
        }
        return false;
    }

    zb::rpc::RenewArchiveLeaseRequest req;
    req.set_node_id(candidate.node_id);
    req.set_lease_id(lease_id);
    req.set_requested_lease_ms(0);
    SetArchiveTaskIdentity(&req, candidate.inode_id, std::string());

    zb::rpc::RenewArchiveLeaseReply reply;
    std::string local_error;
    if (!lease_manager_->Renew(req, &reply, &local_error) || !reply.renewed()) {
        if (error) {
            *error = local_error.empty() ? "failed to renew file archive lease" : local_error;
        }
        return false;
    }
    if (renewed_lease_id) {
        *renewed_lease_id = reply.record().lease_id();
    }
    if (renewed_op_id) {
        *renewed_op_id = !reply.record().lease_id().empty() ? reply.record().lease_id()
                                                            : ("inode:" + std::to_string(candidate.inode_id));
    }
    if (version) {
        *version = reply.record().version() > 0 ? reply.record().version() : (candidate.version > 0 ? candidate.version : 1);
    }
    return true;
}

bool OpticalArchiveManager::CommitFileLease(const FileArchiveCandidateEntry& candidate,
                                            const std::string& lease_id,
                                            const std::string& op_id,
                                            bool success,
                                            const std::string& error_message,
                                            uint64_t* version,
                                            std::string* error) {
    if (version) {
        *version = candidate.version > 0 ? candidate.version : 1;
    }
    if (candidate.inode_id == 0) {
        if (error) {
            *error = "invalid file lease candidate";
        }
        return false;
    }
    if (!lease_manager_) {
        return true;
    }
    if (lease_id.empty()) {
        if (error) {
            *error = "file lease id is empty";
        }
        return false;
    }

    zb::rpc::CommitArchiveTaskRequest req;
    req.set_node_id(candidate.node_id);
    req.set_lease_id(lease_id);
    req.set_op_id(op_id);
    req.set_success(success);
    if (!success && !error_message.empty()) {
        req.set_error_message(error_message);
    }
    SetArchiveTaskIdentity(&req, candidate.inode_id, std::string());

    zb::rpc::CommitArchiveTaskReply reply;
    std::string local_error;
    if (!lease_manager_->Commit(req, &reply, &local_error) || !reply.committed()) {
        if (error) {
            *error = local_error.empty() ? "failed to commit file archive lease" : local_error;
        }
        return false;
    }
    if (version) {
        *version = reply.record().version() > 0 ? reply.record().version() : (candidate.version > 0 ? candidate.version : 1);
    }
    return true;
}

bool OpticalArchiveManager::ReconcileArchiveStates(uint32_t max_records, std::string* error) {
    if (!store_ || max_records == 0) {
        return true;
    }

    const uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
    rocksdb::WriteBatch batch;
    struct FileStateUpdate {
        FileArchiveCandidateEntry candidate;
        zb::rpc::FileArchiveState archive_state{zb::rpc::FILE_ARCHIVE_PENDING};
        uint64_t version{0};
    };
    std::vector<FileStateUpdate> source_updates;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    uint32_t visited = 0;
    for (it->Seek(FileArchiveStatePrefix()); it->Valid() && visited < max_records; it->Next()) {
        const std::string key = it->key().ToString();
        if (key.rfind(FileArchiveStatePrefix(), 0) != 0) {
            break;
        }
        ++visited;

        zb::rpc::ArchiveLeaseRecord record;
        if (!record.ParseFromString(it->value().ToString())) {
            continue;
        }
        const uint64_t inode_id = record.inode_id();
        if (inode_id == 0) {
            batch.Delete(key);
            continue;
        }

        std::string local_error;
        zb::rpc::InodeAttr inode;
        if (!LoadInodeAttr(inode_id, &inode, &local_error)) {
            continue;
        }
        bool file_archived = false;
        if (!HasUsableOpticalLocation(inode_id, &file_archived, &local_error)) {
            continue;
        }

        bool changed = false;
        bool delete_record = false;
        zb::rpc::FileArchiveState state_to_push = zb::rpc::FILE_ARCHIVE_PENDING;
        if (file_archived) {
            delete_record = true;
            changed = true;
            state_to_push = zb::rpc::FILE_ARCHIVE_ARCHIVED;
        } else if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVING &&
                   record.lease_expire_ts_ms() > 0 &&
                   record.lease_expire_ts_ms() <= now_ms) {
            record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
            record.clear_lease_id();
            record.set_lease_expire_ts_ms(0);
            record.set_update_ts_ms(now_ms);
            record.set_version(record.version() + 1);
            changed = true;
            state_to_push = zb::rpc::FILE_ARCHIVE_PENDING;
        } else if (record.state() != zb::rpc::ARCHIVE_STATE_PENDING &&
                   record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING) {
            record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
            record.clear_lease_id();
            record.set_lease_expire_ts_ms(0);
            record.set_update_ts_ms(now_ms);
            record.set_version(record.version() + 1);
            changed = true;
            state_to_push = zb::rpc::FILE_ARCHIVE_PENDING;
        }

        if (!changed) {
            continue;
        }

        if (delete_record) {
            batch.Delete(FileArchiveStateKey(inode_id));
        } else {
            std::string value;
            if (!record.SerializeToString(&value)) {
                continue;
            }
            batch.Put(FileArchiveStateKey(inode_id), value);
        }

        FileStateUpdate source;
        source.candidate.inode_id = inode_id;
        source.candidate.node_id = record.owner_node_id();
        if (cache_) {
            (void)cache_->ResolveNodeAddress(source.candidate.node_id, &source.candidate.node_address);
        }
        source.candidate.disk_id = record.owner_disk_id();
        source.archive_state = state_to_push;
        source.version = delete_record ? (record.version() + 1) : record.version();
        source_updates.push_back(std::move(source));
    }

    if (batch.Count() > 0) {
        std::string write_error;
        if (!store_->WriteBatch(&batch, &write_error)) {
            if (error) {
                *error = write_error;
            }
            return false;
        }
    }
    for (const auto& item : source_updates) {
        std::string update_error;
        (void)UpdateSourceFileArchiveState(item.candidate, item.archive_state, item.version, &update_error);
        if (error && error->empty() && !update_error.empty()) {
            *error = update_error;
        }
    }

    return true;
}

bool OpticalArchiveManager::ResolveObjectOwner(const std::string& object_id,
                                               uint64_t* inode_id,
                                               uint32_t* object_index,
                                               std::string* error) {
    if (object_id.empty() || !inode_id || !object_index) {
        if (error) {
            *error = "invalid object owner resolve args";
        }
        return false;
    }

    if (!ParseStableObjectId(object_id, inode_id, object_index)) {
        if (error) {
            *error = "object id is not stable: " + object_id;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::ShouldArchiveNow(const std::vector<NodeInfo>& nodes) {
    uint64_t max_used = 0;
    bool found = false;
    for (const auto& node : nodes) {
        if (!node.allocatable || !node.is_primary || node.type == NodeType::kOptical) {
            continue;
        }
        for (const auto& disk : node.disks) {
            if (!disk.is_healthy || disk.capacity_bytes == 0) {
                continue;
            }
            found = true;
            uint64_t used = disk.capacity_bytes > disk.free_bytes ? (disk.capacity_bytes - disk.free_bytes) : 0;
            max_used = std::max<uint64_t>(max_used, used);
        }
    }
    if (!found) {
        return false;
    }
    if (max_used >= options_.archive_trigger_bytes) {
        archive_mode_ = true;
    } else if (max_used <= options_.archive_target_bytes) {
        archive_mode_ = false;
    }
    return archive_mode_;
}

bool OpticalArchiveManager::LoadInodeAttr(uint64_t inode_id,
                                          zb::rpc::InodeAttr* attr,
                                          std::string* error) const {
    if (!attr) {
        if (error) {
            *error = "inode attr pointer is null";
        }
        return false;
    }

    std::string data;
    if (!store_->Get(InodeKey(inode_id), &data, error)) {
        return false;
    }
    std::string decode_error;
    if (!MetaCodec::DecodeUnifiedInodeAttr(data, attr, &decode_error)) {
        if (error) {
            *error = decode_error.empty() ? "invalid inode attr" : decode_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::BuildDiskFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                                          zb::rpc::DiskFileLocation* location,
                                                          std::string* error) const {
    if (!location) {
        if (error) {
            *error = "disk location output is null";
        }
        return false;
    }
    if (!PopulateDiskFileLocationFromInodeAttr(attr, location)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (cache_) {
        std::string resolved_disk_id;
        if (cache_->ResolveDiskId(location->node_id(), attr.disk_id(), &resolved_disk_id) &&
            !resolved_disk_id.empty()) {
            location->set_disk_id(resolved_disk_id);
        }
        std::string node_address;
        if (cache_->ResolveNodeAddress(location->node_id(), &node_address) && !node_address.empty()) {
            location->set_node_address(node_address);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalArchiveManager::BuildOpticalFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                                             uint64_t inode_id,
                                                             zb::rpc::OpticalFileLocation* location,
                                                             std::string* error) const {
    if (!location) {
        if (error) {
            *error = "optical location output is null";
        }
        return false;
    }
    if (!PopulateOpticalFileLocationFromInodeAttr(attr, inode_id, location)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (cache_) {
        if (attr.optical_disk_id() <= static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            std::string resolved_disk_id;
            if (cache_->ResolveDiskId(location->node_id(),
                                      static_cast<uint32_t>(attr.optical_disk_id()),
                                      &resolved_disk_id) &&
                !resolved_disk_id.empty()) {
                location->set_disk_id(resolved_disk_id);
            }
        }
        std::string node_address;
        if (cache_->ResolveNodeAddress(location->node_id(), &node_address) && !node_address.empty()) {
            location->set_node_address(node_address);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalArchiveManager::ReadObjectFromReplica(const zb::rpc::ReplicaLocation& source,
                                                 uint64_t read_size,
                                                 std::string* data,
                                                 std::string* error) {
    if (!data) {
        if (error) {
            *error = "output buffer is null";
        }
        return false;
    }
    brpc::Channel* channel = GetChannel(source.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::ReadObjectRequest req;
    req.set_disk_id(source.disk_id());
    req.set_object_id(ReplicaObjectId(source));
    req.set_offset(0);
    req.set_size(read_size);
    if (!source.image_id().empty()) {
        req.set_image_id(source.image_id());
        req.set_image_offset(source.image_offset());
        req.set_image_length(source.image_length());
    }

    zb::rpc::ReadObjectReply resp;
    stub.ReadObject(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    *data = resp.data();
    return true;
}

bool OpticalArchiveManager::WriteObjectToOptical(const NodeSelection& optical,
                                                 const std::string& object_id,
                                                 const std::string& op_id,
                                                 const std::string& data,
                                                 uint64_t inode_id,
                                                 uint32_t object_index,
                                                 const zb::rpc::InodeAttr* inode_attr,
                                                 zb::rpc::ReplicaLocation* optical_location,
                                                 std::string* error) {
    const std::string effective_op_id = !op_id.empty() ? op_id : object_id;

    brpc::Channel* channel = GetChannel(optical.address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    zb::rpc::WriteObjectRequest req;
    req.set_disk_id(optical.disk_id);
    req.set_object_id(object_id);
    req.set_offset(0);
    req.set_data(data);
    req.set_epoch(optical.epoch);
    req.set_archive_op_id(effective_op_id);
    req.set_inode_id(inode_id);
    req.set_file_object_index(object_index);
    if (inode_attr) {
        req.set_file_size(inode_attr->size());
        req.set_file_mode(inode_attr->mode());
        req.set_file_uid(inode_attr->uid());
        req.set_file_gid(inode_attr->gid());
        req.set_file_mtime(inode_attr->mtime());
        const uint64_t object_unit_size =
            inode_attr->object_unit_size() > 0 ? inode_attr->object_unit_size() : options_.default_object_unit_size;
        req.set_file_offset(static_cast<uint64_t>(object_index) * object_unit_size);
    }
    if (inode_id != 0) {
        req.set_file_id("inode-" + std::to_string(inode_id));
        req.set_file_path("/inode/" + std::to_string(inode_id));
    }

    zb::rpc::WriteObjectReply resp;
    brpc::Controller cntl;
    cntl.set_timeout_ms(5000);
    stub.WriteObject(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    if (optical_location) {
        optical_location->set_node_id(optical.node_id);
        optical_location->set_node_address(optical.address);
        optical_location->set_disk_id(optical.disk_id);
        EnsureReplicaObjectId(optical_location, object_id);
        optical_location->set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        optical_location->set_replica_state(zb::rpc::REPLICA_READY);
        optical_location->set_image_id(resp.image_id());
        optical_location->set_image_offset(resp.image_offset());
        optical_location->set_image_length(resp.image_length());
    }
    return true;
}

bool OpticalArchiveManager::LoadDiskFileLocation(uint64_t inode_id,
                                                 zb::rpc::DiskFileLocation* location,
                                                 std::string* error) const {
    if (!location || inode_id == 0 || !store_) {
        if (error) {
            *error = "invalid disk file location load args";
        }
        return false;
    }
    location->Clear();
    std::string inode_payload;
    std::string inode_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &inode_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            PopulateDiskFileLocationFromUnifiedRecord(record, location)) {
            if (cache_) {
                std::string resolved_disk_id;
                if (cache_->ResolveDiskId(location->node_id(), record.disk_id, &resolved_disk_id) &&
                    !resolved_disk_id.empty()) {
                    location->set_disk_id(resolved_disk_id);
                }
            }
            if (cache_) {
                std::string node_address;
                if (cache_->ResolveNodeAddress(location->node_id(), &node_address) && !node_address.empty()) {
                    location->set_node_address(node_address);
                }
            }
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool OpticalArchiveManager::LoadOpticalFileLocation(uint64_t inode_id,
                                                    zb::rpc::OpticalFileLocation* location,
                                                    std::string* error) const {
    if (!location || inode_id == 0 || !store_) {
        if (error) {
            *error = "invalid optical file location load args";
        }
        return false;
    }
    location->Clear();
    std::string inode_payload;
    std::string inode_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &inode_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            PopulateOpticalFileLocationFromUnifiedRecord(record, location)) {
            if (cache_ && record.optical_disk_id <= static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
                std::string resolved_disk_id;
                if (cache_->ResolveDiskId(location->node_id(),
                                          static_cast<uint32_t>(record.optical_disk_id),
                                          &resolved_disk_id) &&
                    !resolved_disk_id.empty()) {
                    location->set_disk_id(resolved_disk_id);
                }
            }
            if (cache_) {
                std::string node_address;
                if (cache_->ResolveNodeAddress(location->node_id(), &node_address) && !node_address.empty()) {
                    location->set_node_address(node_address);
                }
            }
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool OpticalArchiveManager::SaveOpticalFileLocation(uint64_t inode_id,
                                                    const zb::rpc::OpticalFileLocation& location,
                                                    rocksdb::WriteBatch* batch,
                                                    std::string* error) const {
    if (!batch || inode_id == 0 || !store_) {
        if (error) {
            *error = "invalid optical file location save args";
        }
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr)) {
            ApplyOpticalLocationToUnifiedRecord(location, &record);
            std::string encoded;
            if (!MetaCodec::EncodeUnifiedInodeRecord(record, &encoded, error)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
            return true;
        }
        if (error) {
            *error = "invalid unified inode payload";
        }
        return false;
    } else if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    return true;
}

bool OpticalArchiveManager::DeleteDiskFileLocation(uint64_t inode_id,
                                                   rocksdb::WriteBatch* batch,
                                                   std::string* error) const {
    if (!batch || inode_id == 0 || !store_) {
        if (error) {
            *error = "invalid disk file location delete args";
        }
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            record.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kDisk)) {
            record.storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kNone);
            ClearDiskLocationInUnifiedRecord(&record);
            std::string encoded;
            if (!MetaCodec::EncodeUnifiedInodeRecord(record, &encoded, error)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalArchiveManager::DeleteDiskFile(uint64_t inode_id,
                                           const zb::rpc::DiskFileLocation& location,
                                           std::string* error) {
    brpc::Channel* channel = GetChannel(location.node_address(), error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::DeleteFileMetaRequest req;
    req.set_inode_id(inode_id);
    req.set_disk_id(location.disk_id());
    req.set_purge_objects(true);
    zb::rpc::DeleteFileMetaReply resp;
    stub.DeleteFileMeta(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() != zb::rpc::STATUS_OK && resp.status().code() != zb::rpc::STATUS_NOT_FOUND) {
        if (error) {
            *error = resp.status().message();
        }
        return false;
    }
    return true;
}

brpc::Channel* OpticalArchiveManager::GetChannel(const std::string& address, std::string* error) {
    if (address.empty()) {
        if (error) {
            *error = "empty address";
        }
        return nullptr;
    }

    auto it = channels_.find(address);
    if (it != channels_.end()) {
        return it->second.get();
    }

    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    options.max_retry = 0;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) {
            *error = "failed to init channel to " + address;
        }
        return nullptr;
    }

    brpc::Channel* raw = channel.get();
    channels_[address] = std::move(channel);
    return raw;
}

} // namespace zb::mds
