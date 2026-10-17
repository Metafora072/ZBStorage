#include "InProcessDiskService.h"
#include "common/rpc/ModelWait.h"

#include <brpc/controller.h>
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <thread>
#include <unordered_set>

#include "common/rpc/LogicalNodeRoute.h"

namespace zb::scheduler {

InProcessDiskService::Slot::Slot(zb::virtual_node::VirtualNodeConfig config)
    : generation(config.instance_generation), node_id(config.node_id),
      wake_latency_ms(config.wake_latency_ms),
      reboot_latency_ms(config.reboot_latency_ms),
      storage(std::move(config)), service(&storage, &metrics) {}

bool InProcessDiskService::AddNode(zb::virtual_node::VirtualNodeConfig config,
                                   std::string* error) {
    if (config.node_id.empty() || config.node_id.size() > 128 || config.instance_generation == 0 ||
        config.disk_ids.empty() ||
        config.archive_meta_dir.empty() || config.disk_capacity_bytes == 0 ||
        config.replication_enabled || config.read_bytes_per_sec == 0 ||
        config.write_bytes_per_sec == 0 || config.max_concurrent_io == 0 ||
        config.max_concurrent_io > 1024 || config.io_queue_limit > 65536 ||
        config.node_id.find('\n') != std::string::npos) {
        if (error) *error = "in-process disk requires NODE_ID, DISK_IDS, ARCHIVE_META_DIR, "
                            "positive DISK_CAPACITY_BYTES and disabled replication";
        return false;
    }
    std::unordered_set<std::string> disk_ids;
    for (const auto& disk_id : config.disk_ids) {
        if (disk_id.empty() || disk_id.find_first_of("|\n\r") != std::string::npos ||
            disk_id.find('\0') != std::string::npos || !disk_ids.insert(disk_id).second) {
            if (error) *error = "DISK_IDS must be unique, nonempty and delimiter-free for " + config.node_id;
            return false;
        }
    }
    std::error_code path_error;
    const bool root_exists = std::filesystem::exists(config.archive_meta_dir, path_error);
    if (!path_error && root_exists &&
        !std::filesystem::is_empty(config.archive_meta_dir, path_error) &&
        !std::filesystem::exists(std::filesystem::path(config.archive_meta_dir) /
                                     "model_state", path_error)) {
        if (error) *error = "existing control state has no model_state; migration is required: " +
                            config.archive_meta_dir;
        return false;
    }
    if (path_error) {
        if (error) *error = "invalid ARCHIVE_META_DIR: " + path_error.message();
        return false;
    }
    config.allow_dynamic_disks = false;
    config.max_concurrent_io = std::min<uint32_t>(
        config.max_concurrent_io, static_cast<uint32_t>(config.disk_ids.size()));
    std::lock_guard<std::mutex> lock(mu_);
    if (known_node_ids_.count(config.node_id) != 0) {
        if (error) *error = "duplicate in-process node_id: " + config.node_id;
        return false;
    }
    auto slot = std::make_shared<Slot>(config);
    std::filesystem::create_directories(config.archive_meta_dir, path_error);
    if (path_error) {
        if (error) *error = "cannot create ARCHIVE_META_DIR: " + path_error.message();
        return false;
    }
    if (!slot->storage.InitModelStateStore(config.archive_meta_dir + "/model_state", error))
        return false;
    if (!slot->storage.InitArchiveMetaStore(config.archive_meta_dir,
                                            config.archive_track_max_objects,
                                            config.archive_meta_snapshot_interval_ops,
                                            config.archive_meta_wal_fsync, error)) {
        return false;
    }
    known_node_ids_.insert(config.node_id);
    nodes_.emplace(config.node_id, std::move(slot));
    return true;
}

uint32_t InProcessDiskService::RebootLatencyMs(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(id);
    return it == nodes_.end() ? 0 : it->second->reboot_latency_ms;
}

std::shared_ptr<zb::virtual_node::VirtualStorageServiceImpl>
InProcessDiskService::BorrowStorage(const std::string& id, bool new_write, std::string* error) const {
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot) { if (error) *error = "model disk instance unavailable: " + id; return {}; }
    struct Reference {
        std::shared_ptr<Slot> slot;
        std::shared_lock<zb::rpc_route::ModelSharedMutex> admission;
        explicit Reference(std::shared_ptr<Slot> value) : slot(std::move(value)), admission(slot->write_mu) {}
    };
    auto reference = std::make_shared<Reference>(slot);
    ManagedNodeRuntime node;
    if (!node_manager_ || !node_manager_->GetNode(id, &node) ||
        !slot->catalog_confirmed.load() || !node.readiness.Ready() ||
        node.health != ManagedHealthState::kHealthy ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled ||
        node.lifecycle == LifecycleState::kRetired || node.lifecycle == LifecycleState::kJoining ||
        (new_write && (slot->drain_closed || node.lifecycle != LifecycleState::kWorking ||
         node.service_mode != NodeServiceMode::kReadWrite))) {
        if (error) *error = "model disk internal operation not admitted: " + id;
        return {};
    }
    return {reference, &slot->storage};
}

bool InProcessDiskService::QuiesceNode(const std::string& id, std::string* error) {
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot || !node_manager_) { if (error) *error = "model disk unavailable"; return false; }
    std::unique_lock<zb::rpc_route::ModelSharedMutex> admission(slot->write_mu);
    if (!node_manager_->SetAdministrativeState(id, zb::storage_model::AdministrativeState::kDisabled, error))
        return false;
    slot->catalog_confirmed.store(false);
    return true;
}

bool InProcessDiskService::DiscardUnpublishedNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(node_id);
    if (it == nodes_.end() || it->second->catalog_confirmed.load()) return false;
    nodes_.erase(it);
    known_node_ids_.erase(node_id);
    return true;
}

bool InProcessDiskService::ReleaseRetiredNode(const std::string& node_id) {
    ManagedNodeRuntime runtime;
    if (!node_manager_ || !node_manager_->GetNode(node_id, &runtime) ||
        runtime.lifecycle != LifecycleState::kRetired ||
        runtime.profile.execution_mode != ExecutionMode::kVirtual ||
        runtime.profile.kind != ManagedNodeKind::kStorage) return false;
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return false;
    it->second->catalog_confirmed.store(false);
    nodes_.erase(it);
    return true;
}

bool InProcessDiskService::OwnsNodeIdentity(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    return known_node_ids_.count(node_id) != 0;
}

bool InProcessDiskService::VerifyEmptySlot(const Slot& slot, std::string* error) {
    std::vector<zb::real_node::ArchiveObjectMeta> objects;
    const auto listing = slot.storage.ListTrackedObjects(&objects);
    if (!listing.ok()) {
        if (error) *error = "logical disk inventory failed: " + listing.message;
        return false;
    }
    if (!objects.empty()) {
        if (error) *error = "logical disk still contains objects; "
            "model-to-model migration is not available";
        return false;
    }
    const auto inventory = slot.storage.GetDiskReport();
    if (!inventory.status.ok()) {
        if (error) *error = "logical disk report failed: " + inventory.status.message;
        return false;
    }
    for (const auto& disk : inventory.reports) {
        if (disk.free_bytes != disk.capacity_bytes) {
            if (error) *error = "logical disk still has resident bytes: " + disk.id;
            return false;
        }
    }
    return true;
}

bool InProcessDiskService::VerifyEmpty(const std::string& node_id,
                                        std::string* error) const {
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(node_id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot) {
        if (error) *error = "logical disk instance is unavailable: " + node_id;
        return false;
    }
    // Also protects automatic lifetime drains, which can already be Draining
    // before the coordinator starts its final verification.
    std::unique_lock<zb::rpc_route::ModelSharedMutex> write_lock(slot->write_mu);
    return VerifyEmptySlot(*slot, error);
}

bool InProcessDiskService::BeginEmptyDrain(const std::string& node_id,
                                            uint64_t now_ms, std::string* error) {
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(node_id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot || !node_manager_) {
        if (error) *error = "logical disk instance is unavailable: " + node_id;
        return false;
    }
    std::unique_lock<zb::rpc_route::ModelSharedMutex> lock(slot->write_mu);
    const bool was_closed = slot->drain_closed;
    slot->drain_closed = true;
    if (!VerifyEmptySlot(*slot, error) ||
        !node_manager_->BeginRemoveNode(node_id, now_ms, error)) {
        slot->drain_closed = was_closed;
        return false;
    }
    return true;
}

bool InProcessDiskService::BeginDataDrain(const std::string& node_id,
                                          uint64_t now_ms, std::string* error) {
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(node_id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot || !node_manager_) {
        if (error) *error = "logical disk instance is unavailable: " + node_id;
        return false;
    }
    std::unique_lock<zb::rpc_route::ModelSharedMutex> lock(slot->write_mu);
    const bool was_closed = slot->drain_closed;
    slot->drain_closed = true;
    if (!node_manager_->BeginRemoveNode(node_id, now_ms, error)) {
        slot->drain_closed = was_closed;
        return false;
    }
    return true;
}

std::vector<std::string> InProcessDiskService::NodeIds() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<std::string> ids;
    ids.reserve(nodes_.size());
    for (const auto& item : nodes_) ids.push_back(item.first);
    return ids;
}

bool InProcessDiskService::ConfirmCatalog(const std::string& node_id) {
    ManagedNodeRuntime node;
    if (!node_manager_ || !node_manager_->GetNode(node_id, &node) ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled ||
        (node.lifecycle != LifecycleState::kWorking && node.lifecycle != LifecycleState::kDraining &&
         node.lifecycle != LifecycleState::kExiting) ||
        !node.readiness.Ready() || node.health != ManagedHealthState::kHealthy)
        return false;
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return false;
    it->second->catalog_confirmed.store(true);
    node_manager_->SetTransitionState(node_id, TransitionState::kStable, node.last_heartbeat_ms, nullptr);
    return true;
}

bool InProcessDiskService::RevokeCatalog(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(node_id);
    if (it == nodes_.end()) return false;
    it->second->catalog_confirmed.store(false);
    return true;
}

zb::virtual_node::VirtualStorageServiceImpl* InProcessDiskService::GetStorage(
    const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(node_id);
    return it == nodes_.end() ? nullptr : &it->second->storage;
}

uint64_t InProcessDiskService::GetInstanceGeneration(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(node_id);
    return it == nodes_.end() ? 0 : it->second->generation;
}

zb::metrics::NodeMetricsCollector* InProcessDiskService::GetBackgroundMetrics(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = nodes_.find(id);
    return it == nodes_.end() ? nullptr : &it->second->migration_metrics;
}

zb::metrics::NodeMetricsCollector* InProcessDiskService::GetMetrics(
    const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = nodes_.find(node_id);
    return it == nodes_.end() ? nullptr : &it->second->metrics;
}

std::shared_ptr<InProcessDiskService::Slot> InProcessDiskService::Resolve(
    google::protobuf::RpcController* controller, zb::rpc::Status* status,
    bool new_write, bool wake_on_access) const {
    // BRPC invokes service methods with brpc::Controller in both debug and
    // release builds; avoid RTTI, which BRPC disables in release mode.
    auto* brpc_controller = controller ? static_cast<brpc::Controller*>(controller) : nullptr;
    std::string node_id;
    uint64_t generation = 0;
    if (!brpc_controller ||
        !zb::rpc_route::GetLogicalNodeRoute(*brpc_controller, &node_id, &generation)) {
        status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
        status->set_message("missing or invalid logical node route");
        return nullptr;
    }
    std::shared_ptr<Slot> slot;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = nodes_.find(node_id);
        if (it != nodes_.end()) slot = it->second;
    }
    if (!slot) {
        status->set_code(zb::rpc::STATUS_NOT_FOUND);
        status->set_message("logical node not found: " + node_id);
        return nullptr;
    }
    if (slot->generation != generation) {
        status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
        status->set_message("stale logical node generation");
        return nullptr;
    }
    if (!slot->catalog_confirmed.load()) {
        status->set_code(zb::rpc::STATUS_IO_ERROR);
        status->set_message("MODEL_CATALOG_NOT_CONFIRMED");
        return nullptr;
    }
    ManagedNodeRuntime node;
    if (!node_manager_ || !node_manager_->GetNode(node_id, &node) ||
        node.lifecycle == LifecycleState::kRetired) {
        status->set_code(zb::rpc::STATUS_NOT_FOUND);
        status->set_message("logical node is not registered or was retired");
        return nullptr;
    }
    if (node.lifecycle == LifecycleState::kJoining ||
        node.lifecycle == LifecycleState::kExiting ||
        !node.readiness.Ready() || node.health != ManagedHealthState::kHealthy ||
        node.administrative_state == zb::storage_model::AdministrativeState::kDisabled) {
        status->set_code(zb::rpc::STATUS_IO_ERROR);
        status->set_message("logical node is not ready");
        return nullptr;
    }
    if (new_write &&
        (node.lifecycle != LifecycleState::kWorking ||
         node.administrative_state != zb::storage_model::AdministrativeState::kEnabled ||
         node.service_mode != NodeServiceMode::kReadWrite)) {
        status->set_code(zb::rpc::STATUS_IO_ERROR);
        status->set_message("logical node does not accept new writes");
        return nullptr;
    }
    if (wake_on_access && node.power_level < PowerLevel::kMedium) {
        std::lock_guard<std::mutex> wake_lock(slot->wake_mu);
        // Record the deadline before changing the power level: a concurrent
        // request sees the same wake in progress, even after the manager has
        // moved the accounting state to Medium.
        if (slot->wake_ready_at <= std::chrono::steady_clock::now())
            slot->wake_ready_at = std::chrono::steady_clock::now() +
                std::chrono::milliseconds(slot->wake_latency_ms);
        const uint64_t now_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count());
        std::string error;
        if (!node_manager_->RequestPowerLevel(node_id, PowerLevel::kMedium,
                                               now_ms, true, &error)) {
            slot->wake_ready_at = {};
            status->set_code(zb::rpc::STATUS_IO_ERROR);
            status->set_message("logical node wake failed: " + error);
            return nullptr;
        }
    }
    if (wake_on_access) {
        std::chrono::steady_clock::time_point ready_at;
        {
            std::lock_guard<std::mutex> wake_lock(slot->wake_mu);
            ready_at = slot->wake_ready_at;
        }
        if (ready_at > std::chrono::steady_clock::now())
            zb::rpc_route::WaitModelUntil(ready_at);
        // A stop/failure can occur while this request is waking. Do not
        // dispatch an already admitted request into an offline node.
        ManagedNodeRuntime current;
        if (!slot->catalog_confirmed.load() ||
            !node_manager_->GetNode(node_id, &current) ||
            current.lifecycle == LifecycleState::kJoining ||
            current.lifecycle == LifecycleState::kExiting ||
            current.lifecycle == LifecycleState::kRetired ||
            current.health != ManagedHealthState::kHealthy ||
            current.administrative_state ==
                zb::storage_model::AdministrativeState::kDisabled ||
            (new_write && (current.lifecycle != LifecycleState::kWorking ||
                           current.service_mode != NodeServiceMode::kReadWrite))) {
            status->set_code(zb::rpc::STATUS_IO_ERROR);
            status->set_message("logical node changed state during wake");
            return nullptr;
        }
    }
    return slot;
}

#define ZB_DISK_FORWARD(name, request_type, reply_type, new_write, wake_on_access)       \
    void InProcessDiskService::name(google::protobuf::RpcController* controller,       \
                                     const request_type* request, reply_type* reply,   \
                                     google::protobuf::Closure* done) {                \
        brpc::ClosureGuard done_guard(done);                                            \
        if (!reply || !request) return;                                                 \
        auto slot = Resolve(controller, reply->mutable_status(),                       \
                            new_write, wake_on_access);                               \
        if (!slot) return;                                                              \
        std::shared_lock<zb::rpc_route::ModelSharedMutex> write_lock(slot->write_mu);     \
        if (!slot->catalog_confirmed.load()) {                                          \
            reply->mutable_status()->set_code(zb::rpc::STATUS_IO_ERROR);                \
            reply->mutable_status()->set_message("model node admission revoked");      \
            return;                                                                    \
        }                                                                              \
        if (new_write) {                                                                \
            ManagedNodeRuntime current;                                                 \
            if (slot->drain_closed || !node_manager_ ||                              \
                !node_manager_->GetNode(slot->node_id, &current) ||                     \
                current.lifecycle != LifecycleState::kWorking ||                       \
                current.administrative_state !=                                         \
                    zb::storage_model::AdministrativeState::kEnabled ||                 \
                current.service_mode != NodeServiceMode::kReadWrite) {                  \
                reply->mutable_status()->set_code(zb::rpc::STATUS_IO_ERROR);            \
                reply->mutable_status()->set_message("logical node is draining");      \
                return;                                                                \
            }                                                                           \
        }                                                                               \
        slot->service.name(controller, request, reply, nullptr);                       \
    }
ZB_DISK_FORWARD(WriteObject, zb::rpc::WriteObjectRequest, zb::rpc::WriteObjectReply, true, true)
ZB_DISK_FORWARD(ReadObject, zb::rpc::ReadObjectRequest, zb::rpc::ReadObjectReply, false, true)
ZB_DISK_FORWARD(DeleteObject, zb::rpc::DeleteObjectRequest, zb::rpc::DeleteObjectReply, false, true)
ZB_DISK_FORWARD(ListObjects, zb::rpc::ListObjectsRequest, zb::rpc::ListObjectsReply, false, true)
ZB_DISK_FORWARD(ResetNodeData, zb::rpc::ResetNodeDataRequest, zb::rpc::ResetNodeDataReply, false, true)
ZB_DISK_FORWARD(ReadArchivedFile, zb::rpc::ReadArchivedFileRequest, zb::rpc::ReadArchivedFileReply, false, true)
ZB_DISK_FORWARD(UpdateArchiveState, zb::rpc::UpdateArchiveStateRequest, zb::rpc::UpdateArchiveStateReply, false, true)
ZB_DISK_FORWARD(UpdateFileArchiveState, zb::rpc::UpdateFileArchiveStateRequest, zb::rpc::UpdateFileArchiveStateReply, false, true)
ZB_DISK_FORWARD(GetDiskReport, google::protobuf::Empty, zb::rpc::DiskReportReply, false, false)
ZB_DISK_FORWARD(DeleteFileMeta, zb::rpc::DeleteFileMetaRequest, zb::rpc::DeleteFileMetaReply, false, true)
ZB_DISK_FORWARD(ResolveFileRead, zb::rpc::ResolveFileReadRequest, zb::rpc::ResolveFileReadReply, false, true)
ZB_DISK_FORWARD(AllocateFileWrite, zb::rpc::AllocateFileWriteRequest, zb::rpc::AllocateFileWriteReply, true, true)
ZB_DISK_FORWARD(CommitFileWrite, zb::rpc::CommitFileWriteRequest, zb::rpc::CommitFileWriteReply, true, true)
#undef ZB_DISK_FORWARD

void InProcessDiskService::BeginNodeDrain(google::protobuf::RpcController* controller,
    const zb::rpc::BeginNodeDrainRequest* request, zb::rpc::BeginNodeDrainReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!request || !reply) return;
    auto slot = Resolve(controller, reply->mutable_status(), false, false);
    if (!slot) return;
    if (request->node_id() != slot->node_id) {
        reply->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
        reply->mutable_status()->set_message("drain node_id differs from logical route");
        return;
    }
    const uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    std::string error;
    if (!BeginDataDrain(slot->node_id, now, &error) ||
        (durable_state_commit_ && !durable_state_commit_(&error))) {
        reply->mutable_status()->set_code(zb::rpc::STATUS_IO_ERROR);
        reply->mutable_status()->set_message(error);
        return;
    }
    reply->mutable_status()->set_code(zb::rpc::STATUS_OK);
    reply->set_write_admission_closed(true);
}

void InProcessDiskService::InstallMigratedFile(google::protobuf::RpcController* controller,
    const zb::rpc::InstallMigratedFileRequest* request, zb::rpc::InstallMigratedFileReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!request || !reply) return;
    auto slot = Resolve(controller, reply->mutable_status(), true, true);
    if (!slot) return;
    if (request->node_id() != slot->node_id || request->migration_id().empty() ||
        request->disk_id().empty() || !request->has_meta()) {
        reply->mutable_status()->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
        reply->mutable_status()->set_message("incomplete migrated file identity");
        return;
    }
    std::string error;
    auto storage = BorrowStorage(slot->node_id, true, &error);
    if (!storage) {
        reply->mutable_status()->set_code(zb::rpc::STATUS_IO_ERROR);
        reply->mutable_status()->set_message(error);
        return;
    }
    zb::msg::FileMeta meta;
    meta.inode_id = request->meta().inode_id();
    meta.file_size = request->meta().file_size();
    meta.object_unit_size = request->meta().object_unit_size();
    meta.version = request->meta().version();
    meta.mtime_sec = request->meta().mtime_sec();
    meta.update_ts_ms = request->meta().update_ts_ms();
    const auto status = storage->ImportModelFile(meta, request->disk_id());
    reply->mutable_status()->set_code(status.ok() ? zb::rpc::STATUS_OK : zb::rpc::STATUS_IO_ERROR);
    reply->mutable_status()->set_message(status.message);
}

} // namespace zb::scheduler
