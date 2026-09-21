#include "DrainMigrationCoordinator.h"

#include <algorithm>
#include <charconv>
#include <map>

namespace zb::scheduler {

bool ObjectMigrationBackend::VerifySourceEmpty(const std::string& source_address,
                                               std::string* error) {
    std::vector<MigrationObject> remaining;
    if (!ListObjects(source_address, &remaining, error)) return false;
    if (!remaining.empty()) {
        if (error) *error = "source still contains " + std::to_string(remaining.size()) +
                            " objects after migration";
        return false;
    }
    return true;
}

DrainMigrationCoordinator::DrainMigrationCoordinator(ClusterState* cluster,
                                                     NodePowerManager* nodes,
                                                     LifecycleManager* lifecycle,
                                                     ObjectMigrationBackend* backend,
                                                     uint64_t retry_base_ms,
                                                     uint32_t max_retries)
    : cluster_(cluster),
      nodes_(nodes),
      lifecycle_(lifecycle),
      backend_(backend),
      retry_base_ms_(std::max<uint64_t>(1, retry_base_ms)),
      max_retries_(std::max<uint32_t>(1, max_retries)) {}

bool DrainMigrationCoordinator::Start(const std::string& node_id,
                                      uint64_t now_ms,
                                      std::string* error) {
    if (!cluster_ || !nodes_ || !backend_ || node_id.empty()) {
        if (error) *error = "migration coordinator is not initialized";
        return false;
    }
    ManagedNodeRuntime runtime;
    NodeState source;
    if (!nodes_->GetNode(node_id, &runtime)) {
        if (error) *error = "managed source node not found: " + node_id;
        return false;
    }
    const bool requires_data_plane = runtime.profile.kind == ManagedNodeKind::kStorage &&
                                     runtime.profile.execution_mode != ExecutionMode::kSimulated;
    if (runtime.profile.execution_mode != ExecutionMode::kSimulated &&
        !cluster_->GetNode(node_id, &source)) {
        if (error) *error = "source heartbeat address not found: " + node_id;
        return false;
    }
    if (requires_data_plane && source.address.empty()) {
        if (error) *error = "source node has no service address";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    auto existing = tasks_.find(node_id);
    if (existing != tasks_.end() && existing->second.view.state != DrainMigrationState::kCompleted &&
        existing->second.view.state != DrainMigrationState::kBlocked) {
        return true;
    }
    InternalTask task;
    task.view.migration_id = "drain-" + std::to_string(now_ms) + "-" + std::to_string(next_id_++);
    task.view.source_node_id = node_id;
    task.view.source_address = source.address;
    task.view.created_at_ms = now_ms;
    task.view.updated_at_ms = now_ms;
    if (!requires_data_plane) {
        // Metadata and optical nodes have no movable disk-file objects in the
        // RealNodeService namespace. Keep an empty task so Tick still performs
        // the common stop-and-retire closeout.
        task.enumerated = true;
        task.view.state = DrainMigrationState::kRunning;
    }
    tasks_[node_id] = std::move(task);
    return true;
}

void DrainMigrationCoordinator::Tick(uint64_t now_ms) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto& entry : tasks_) {
        InternalTask& task = entry.second;
        if (task.view.state == DrainMigrationState::kCompleted ||
            task.view.state == DrainMigrationState::kBlocked ||
            (task.view.state == DrainMigrationState::kRetryWait && now_ms < task.view.next_retry_ms)) {
            continue;
        }
        if (!task.enumerated) {
            (void)EnumerateLocked(&task, now_ms);
        } else {
            (void)ProcessWorkLocked(&task, now_ms);
        }
    }
}

bool DrainMigrationCoordinator::GetTask(const std::string& node_id, DrainMigrationTask* out) const {
    if (!out) return false;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = tasks_.find(node_id);
    if (it == tasks_.end()) return false;
    *out = it->second.view;
    return true;
}

std::vector<DrainMigrationTask> DrainMigrationCoordinator::ListTasks() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<DrainMigrationTask> out;
    out.reserve(tasks_.size());
    for (const auto& entry : tasks_) out.push_back(entry.second.view);
    std::sort(out.begin(), out.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.created_at_ms < rhs.created_at_ms;
    });
    return out;
}

bool DrainMigrationCoordinator::ParseStableObjectId(const std::string& object_id,
                                                     uint64_t* inode_id,
                                                     uint32_t* object_index) {
    if (!inode_id || !object_index || object_id.rfind("obj-", 0) != 0) return false;
    const size_t split = object_id.find('-', 4);
    if (split == std::string::npos || split + 1 == object_id.size()) return false;
    uint64_t inode = 0;
    uint32_t index = 0;
    const char* begin = object_id.data();
    const char* end = begin + object_id.size();
    const auto inode_result = std::from_chars(begin + 4, begin + split, inode);
    const auto index_result = std::from_chars(begin + split + 1, end, index);
    if (inode_result.ec != std::errc{} || inode_result.ptr != begin + split || inode == 0 ||
        index_result.ec != std::errc{} || index_result.ptr != end) {
        return false;
    }
    *inode_id = inode;
    *object_index = index;
    return true;
}

bool DrainMigrationCoordinator::EnumerateLocked(InternalTask* task, uint64_t now_ms) {
    std::vector<MigrationObject> objects;
    std::string error;
    if (!backend_->ListObjects(task->view.source_address, &objects, &error)) {
        RetryLocked(task, now_ms, "object enumeration failed: " + error);
        return false;
    }
    std::map<std::pair<uint64_t, std::string>, std::vector<MigrationObject>> grouped;
    std::unordered_map<uint64_t, std::string> inode_disks;
    uint64_t total_bytes = 0;
    for (auto& object : objects) {
        if (!ParseStableObjectId(object.object_id, &object.inode_id, &object.object_index)) {
            task->view.state = DrainMigrationState::kBlocked;
            task->view.last_error = "unmanaged object id cannot be committed in MDS: " + object.object_id;
            task->view.updated_at_ms = now_ms;
            return false;
        }
        auto inserted = inode_disks.emplace(object.inode_id, object.disk_id);
        if (!inserted.second && inserted.first->second != object.disk_id) {
            task->view.state = DrainMigrationState::kBlocked;
            task->view.last_error = "one inode is spread across multiple source disks";
            task->view.updated_at_ms = now_ms;
            return false;
        }
        grouped[{object.inode_id, object.disk_id}].push_back(object);
        total_bytes += object.size_bytes;
    }
    task->work.clear();
    for (auto& entry : grouped) {
        WorkItem work;
        work.inode_id = entry.first.first;
        work.source_disk_id = entry.first.second;
        work.objects = std::move(entry.second);
        std::sort(work.objects.begin(), work.objects.end(), [](const auto& lhs, const auto& rhs) {
            return lhs.object_index < rhs.object_index;
        });
        task->work.push_back(std::move(work));
    }
    task->enumerated = true;
    task->view.state = DrainMigrationState::kRunning;
    task->view.total_objects = objects.size();
    task->view.remaining_objects = objects.size();
    task->view.total_bytes = total_bytes;
    task->view.remaining_bytes = total_bytes;
    task->view.updated_at_ms = now_ms;
    nodes_->SetResidentDataBytes(task->view.source_node_id, total_bytes, nullptr);
    nodes_->ReportDrainProgress(task->view.source_node_id, now_ms,
                                objects.size(), total_bytes, 0, nullptr);
    if (task->work.empty()) CompleteLocked(task, now_ms);
    return true;
}

bool DrainMigrationCoordinator::SelectTargetLocked(const InternalTask& task,
                                                   const WorkItem& work,
                                                   uint64_t now_ms,
                                                   MigrationTarget* target,
                                                   std::string* error) const {
    uint64_t required = 0;
    for (const auto& object : work.objects) required += object.size_bytes;
    uint64_t generation = 0;
    std::vector<NodeState> nodes;
    cluster_->Snapshot(0, &generation, &nodes);
    (void)generation;
    uint64_t best_free = 0;
    bool found = false;
    bool wake_requested = false;
    for (const auto& node : nodes) {
        if (node.node_id == task.view.source_node_id || node.address.empty() ||
            (node.node_type != zb::rpc::NODE_REAL && node.node_type != zb::rpc::NODE_VIRTUAL_POOL) ||
            node.health_state != zb::rpc::NODE_HEALTH_HEALTHY ||
            node.admin_state != zb::rpc::NODE_ADMIN_ENABLED) {
            continue;
        }
        ManagedNodeRuntime runtime;
        if (!nodes_->GetNode(node.node_id, &runtime)) continue;
        // VirtualNode currently models capacity and latency but does not retain
        // source bytes. Verified physical-data drains therefore select only a
        // byte-preserving physical storage node.
        if (runtime.profile.execution_mode != ExecutionMode::kPhysical ||
            runtime.lifecycle != LifecycleState::kWorking ||
             runtime.health != ManagedHealthState::kHealthy ||
             !runtime.profile.participates_in_placement ||
             runtime.administrative_state !=
                 zb::storage_model::AdministrativeState::kEnabled ||
             runtime.service_mode != NodeServiceMode::kReadWrite) {
            continue;
        }
        if (runtime.power_level < PowerLevel::kMedium ||
            runtime.actuated_power_level < PowerLevel::kMedium) {
            NodeAccessEvent reservation;
            reservation.event_time_us = now_ms * 1000ULL;
            reservation.is_write = true;
            reservation.bytes = required;
            reservation.access_path = AccessPath::kDefault;
            std::string ignored;
            if (nodes_->ReportAccessEvent(node.node_id, reservation, &ignored)) {
                wake_requested = true;
            }
            continue;
        }
        for (const auto& disk_entry : node.disks) {
            const DiskState& disk = disk_entry.second;
            if (!disk.is_healthy || disk.free_bytes < required || disk.free_bytes <= best_free) continue;
            best_free = disk.free_bytes;
            target->node_id = node.node_id;
            target->address = node.address;
            target->disk_id = disk.disk_id;
            found = true;
        }
    }
    if (!found && error) {
        *error = wake_requested ? "physical migration target wake requested"
                                : "no healthy enabled physical target has enough free capacity";
    }
    return found;
}

bool DrainMigrationCoordinator::ProcessWorkLocked(InternalTask* task, uint64_t now_ms) {
    if (task->next_work >= task->work.size()) {
        CompleteLocked(task, now_ms);
        return task->view.state == DrainMigrationState::kCompleted;
    }
    WorkItem& work = task->work[task->next_work];
    std::string error;
    if (!work.target_selected) {
        if (!SelectTargetLocked(*task, work, now_ms, &work.target, &error)) {
            RetryLocked(task, now_ms, error);
            return false;
        }
        work.target_selected = true;
    }
    if (!work.metadata_committed) {
        for (const auto& object : work.objects) {
            if (!backend_->CopyAndVerify(task->view.source_address, object, work.target, &error)) {
                RetryLocked(task, now_ms, "copy/verify " + object.object_id + " failed: " + error);
                return false;
            }
        }
        if (!backend_->CommitFileLocation(task->view.migration_id, work.inode_id,
                                          task->view.source_node_id, task->view.source_address,
                                          work.source_disk_id, work.target, &error)) {
            RetryLocked(task, now_ms, "MDS location commit failed: " + error);
            return false;
        }
        work.metadata_committed = true;
    }
    for (; work.cleanup_index < work.objects.size(); ++work.cleanup_index) {
        const auto& object = work.objects[work.cleanup_index];
        if (!backend_->DeleteSourceObject(task->view.source_address, object, &error)) {
            RetryLocked(task, now_ms, "source cleanup " + object.object_id + " failed: " + error);
            return false;
        }
    }
    uint64_t bytes = 0;
    for (const auto& object : work.objects) bytes += object.size_bytes;
    task->view.migrated_objects += work.objects.size();
    task->view.migrated_bytes += bytes;
    task->view.remaining_objects -= std::min<uint64_t>(task->view.remaining_objects, work.objects.size());
    task->view.remaining_bytes -= std::min<uint64_t>(task->view.remaining_bytes, bytes);
    task->view.state = DrainMigrationState::kRunning;
    task->view.last_error.clear();
    task->view.updated_at_ms = now_ms;
    ++task->next_work;
    if (task->next_work >= task->work.size()) {
        CompleteLocked(task, now_ms);
    } else {
        nodes_->ReportDrainProgress(task->view.source_node_id, now_ms,
                                    task->view.remaining_objects,
                                    task->view.remaining_bytes, 0, nullptr);
    }
    return true;
}

void DrainMigrationCoordinator::RetryLocked(InternalTask* task,
                                            uint64_t now_ms,
                                            const std::string& error) {
    ++task->view.retry_count;
    task->view.updated_at_ms = now_ms;
    task->view.last_error = error;
    if (task->view.retry_count >= max_retries_) {
        task->view.state = DrainMigrationState::kBlocked;
        return;
    }
    task->view.state = DrainMigrationState::kRetryWait;
    const uint32_t shift = std::min<uint32_t>(task->view.retry_count - 1, 10);
    task->view.next_retry_ms = now_ms + retry_base_ms_ * (1ULL << shift);
}

void DrainMigrationCoordinator::CompleteLocked(InternalTask* task, uint64_t now_ms) {
    std::string error;
    ManagedNodeRuntime runtime;
    if (!nodes_->GetNode(task->view.source_node_id, &runtime)) {
        RetryLocked(task, now_ms, "source node disappeared from catalog");
        return;
    }
    if (runtime.profile.kind == ManagedNodeKind::kStorage &&
        runtime.profile.execution_mode != ExecutionMode::kSimulated &&
        !backend_->VerifySourceEmpty(task->view.source_address, &error)) {
        RetryLocked(task, now_ms, "final source verification failed: " + error);
        return;
    }
    uint64_t disk_references = 0;
    uint64_t optical_references = 0;
    if (!backend_->VerifyCmsNodeReferencesZero(task->view.source_node_id,
                                               &disk_references,
                                               &optical_references,
                                               &error)) {
        RetryLocked(task, now_ms, "CMS zero-reference verification failed: " + error);
        return;
    }
    if (runtime.profile.kind == ManagedNodeKind::kMetadata &&
        runtime.resident_data_bytes != 0) {
        task->view.state = DrainMigrationState::kBlocked;
        task->view.last_error = "metadata node still has referenced metadata";
        task->view.updated_at_ms = now_ms;
        return;
    }
    if (runtime.profile.kind == ManagedNodeKind::kOpticalLibrary &&
        (!runtime.optical_library.observed || runtime.optical_library.local_disc_count != 0 ||
         runtime.resident_data_bytes != 0)) {
        task->view.state = DrainMigrationState::kBlocked;
        task->view.last_error = !runtime.optical_library.observed
                                    ? "optical inventory has not been observed"
                                    : (runtime.optical_library.local_disc_count != 0
                                           ? "optical library still has local discs"
                                           : "optical library still has referenced cache or metadata");
        task->view.updated_at_ms = now_ms;
        return;
    }
    if (!nodes_->ReportDrainProgress(task->view.source_node_id, now_ms, 0, 0, 0, &error)) {
        RetryLocked(task, now_ms, "final drain report failed: " + error);
        return;
    }
    NodeOperationState operation;
    if (runtime.profile.execution_mode != ExecutionMode::kSimulated && lifecycle_ &&
        !lifecycle_->StopNode(task->view.source_node_id, false,
                                            "automatic stop after verified drain",
                                            &operation, &error)) {
        RetryLocked(task, now_ms, "post-drain stop failed: " + error);
        return;
    }
    if (!nodes_->FinalizeRemoveNode(task->view.source_node_id, now_ms, false, &error)) {
        RetryLocked(task, now_ms, "post-drain retirement failed: " + error);
        return;
    }
    task->view.state = DrainMigrationState::kCompleted;
    task->view.updated_at_ms = now_ms;
    task->view.remaining_objects = 0;
    task->view.remaining_bytes = 0;
    task->view.last_error.clear();
}

} // namespace zb::scheduler
