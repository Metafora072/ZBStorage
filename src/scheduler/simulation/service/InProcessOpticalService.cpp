#include "InProcessOpticalService.h"

#include <map>
#include "common/rpc/ArchiveInventoryPage.h"
#include "common/rpc/ModelWait.h"

#include <brpc/controller.h>
#include <brpc/channel.h>

#include <chrono>
#include <algorithm>
#include <limits>
#include <thread>

#include "common/rpc/LogicalNodeRoute.h"
#include "scheduler/migration/DrainMigrationCoordinator.h"
#include "scheduler/simulation/service/InProcessDiskService.h"

namespace zb::scheduler {

namespace {

zb::rpc::ModelOpticalArchiveRequest BuildMdsArchiveRequest(
    const OpticalArchiveRecord& record, const std::string& target_node_id,
    const std::string& target_address, uint64_t target_generation, bool commit) {
    zb::rpc::ModelOpticalArchiveRequest request;
    request.set_operation_id(record.operation_id);
    request.set_source_lease_id(record.source_lease_id);
    request.set_inode_id(record.inode_id);
    request.set_source_version(record.version);
    request.set_size(record.size);
    request.mutable_expected_source()->set_node_id(record.source_node_id);
    request.mutable_expected_source()->set_disk_id(record.source_disk_id);
    request.mutable_target()->set_node_id(target_node_id);
    request.mutable_target()->set_node_address(target_address);
    request.mutable_target()->set_disk_id(record.disc_id);
    request.mutable_target()->set_image_id(record.image_id);
    request.mutable_target()->set_file_id("inode-" + std::to_string(record.inode_id));
    request.mutable_target()->set_file_path("/inode/" + std::to_string(record.inode_id));
    request.mutable_target()->set_instance_generation(target_generation);
    request.set_commit(commit);
    return request;
}

bool InitMdsChannel(const std::string& address, brpc::Channel* channel,
                    std::string* error) {
    if (!channel || address.empty()) {
        if (error) *error = "MDS endpoint is not configured";
        return false;
    }
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 5000;
    options.max_retry = 0;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) *error = "cannot connect modeled optical MDS endpoint";
        return false;
    }
    return true;
}

} // namespace

bool InProcessOpticalService::AddNode(OpticalReadConfig config, std::string* error) {
    if (config.node_id.empty() || config.node_id.size() > 128 ||
        config.instance_generation == 0 ||
        config.node_id.find('\n') != std::string::npos || config.state_dir.empty() ||
        config.drive_count == 0 || config.drive_count > 1024 ||
        config.disc_count == 0 || config.disc_capacity_bytes == 0 ||
        config.disc_capacity_bytes > std::numeric_limits<uint64_t>::max() /
                                         config.disc_count ||
        config.cache_capacity_bytes == 0 || config.read_bytes_per_sec == 0 ||
        config.read_bytes_per_sec > std::numeric_limits<uint64_t>::max() /
                                        config.drive_count ||
        config.cache_read_bytes_per_sec == 0 || config.max_read_tasks == 0 ||
        config.max_read_tasks > 65536 || config.max_archive_tasks == 0 ||
        config.max_archive_tasks > 65536 ||
        config.archive_transfer_bytes_per_sec == 0 ||
        config.archive_pack_bytes_per_sec == 0 ||
        config.archive_burn_bytes_per_sec == 0 ||
        config.archive_compression_permille == 0 ||
        config.archive_compression_permille > 1000 ||
        config.archive_alignment_bytes == 0 ||
        config.archive_alignment_bytes > 1048576 ||
        config.wake_latency_ms > 3600000 || config.reboot_latency_ms > 3600000 ||
        config.archive_tail_wait_ms > 3600000 ||
        config.image_capacity_bytes > config.disc_capacity_bytes) {
        if (error) *error = "invalid in-process optical read configuration";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (known_node_ids_.count(config.node_id) != 0) {
        if (error) *error = "duplicate in-process optical node_id: " + config.node_id;
        return false;
    }
    const std::string id = config.node_id;
    auto model = std::make_shared<OpticalReadModel>(std::move(config));
    if (!model->InitStateStore(error)) return false;
    metrics_.emplace(id, std::make_shared<zb::metrics::NodeMetricsCollector>());
    auto background = std::make_shared<BackgroundMetrics>();
    for (const auto& record : model->ListArchives())
        if (record.phase == OpticalArchivePhase::kAwaitingMds || record.phase == OpticalArchivePhase::kCommitted)
            background->observed_completions.insert(record.operation_id);
    background_metrics_.emplace(id, std::move(background));
    wake_ready_at_[id] = {};
    models_.emplace(id, std::move(model));
    known_node_ids_.insert(id);
    return true;
}

bool InProcessOpticalService::DiscardUnpublishedNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (models_.erase(node_id) == 0) return false;
    metrics_.erase(node_id);
    background_metrics_.erase(node_id);
    wake_ready_at_.erase(node_id);
    known_node_ids_.erase(node_id);
    catalog_confirmed_ids_.erase(node_id);
    return true;
}

bool InProcessOpticalService::ReleaseRetiredNode(const std::string& node_id) {
    ManagedNodeRuntime runtime;
    if (!node_manager_ || !node_manager_->GetNode(node_id, &runtime) ||
        runtime.lifecycle != LifecycleState::kRetired ||
        runtime.profile.execution_mode != ExecutionMode::kVirtual ||
        runtime.profile.kind != ManagedNodeKind::kOpticalLibrary) return false;
    std::lock_guard<std::mutex> lock(mu_);
    if (models_.erase(node_id) == 0) return false;
    metrics_.erase(node_id);
    background_metrics_.erase(node_id);
    wake_ready_at_.erase(node_id);
    catalog_confirmed_ids_.erase(node_id);
    return true;
}

bool InProcessOpticalService::OwnsNodeIdentity(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    return known_node_ids_.count(node_id) != 0;
}

bool InProcessOpticalService::ConfirmCatalog(const std::string& node_id) {
    ManagedNodeRuntime runtime;
    if (!node_manager_ || !node_manager_->GetNode(node_id, &runtime) ||
        (runtime.lifecycle != LifecycleState::kWorking && runtime.lifecycle != LifecycleState::kDraining &&
         runtime.lifecycle != LifecycleState::kExiting) || !runtime.readiness.Ready() ||
        runtime.health != ManagedHealthState::kHealthy ||
        runtime.administrative_state == zb::storage_model::AdministrativeState::kDisabled) return false;
    std::lock_guard<std::mutex> lock(mu_);
    if (models_.count(node_id) == 0) return false;
    catalog_confirmed_ids_.insert(node_id);
    node_manager_->SetTransitionState(node_id, TransitionState::kStable, runtime.last_heartbeat_ms, nullptr);
    return true;
}

bool InProcessOpticalService::RevokeCatalog(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    return catalog_confirmed_ids_.erase(node_id) != 0;
}

std::shared_ptr<zb::metrics::NodeMetricsCollector>
InProcessOpticalService::GetMetricsShared(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = metrics_.find(node_id);
    return it == metrics_.end() ? nullptr : it->second;
}

zb::metrics::NodeMetricsCollector* InProcessOpticalService::GetMetrics(
    const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = metrics_.find(node_id);
    return it == metrics_.end() ? nullptr : it->second.get();
}

bool InProcessOpticalService::GetManagementSnapshot(
    const std::string& node_id, uint64_t observed_at_ms,
    OpticalNodeManagementSnapshot* output) const {
    if (!output) return false;
    std::shared_ptr<OpticalReadModel> model;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(node_id);
        if (it != models_.end()) model = it->second;
    }
    if (!model) return false;
    const auto& config = model->config();
    OpticalNodeManagementSnapshot snapshot;
    snapshot.model = model->Stats();
    snapshot.disc_capacity_bytes = config.disc_capacity_bytes * config.disc_count;
    snapshot.disc_free_bytes = snapshot.disc_capacity_bytes - snapshot.model.stored_bytes -
                               snapshot.model.reserved_disc_bytes;
    snapshot.cache_capacity_bytes = config.cache_capacity_bytes;
    snapshot.cache_free_bytes = config.cache_capacity_bytes -
        snapshot.model.cached_bytes - snapshot.model.staging_used_bytes;
    snapshot.library.observed = true;
    snapshot.library.total_slots = config.disc_count;
    snapshot.library.local_disc_count = config.disc_count;
    snapshot.library.blank_disc_count = config.disc_count - snapshot.model.occupied_discs;
    snapshot.library.recording_disc_count = snapshot.model.recording_discs;
    snapshot.library.recorded_disc_count = snapshot.model.recorded_discs;
    snapshot.library.staging_capacity_bytes = config.cache_capacity_bytes;
    snapshot.library.staging_used_bytes = snapshot.model.staging_used_bytes;
    snapshot.library.pending_write_tasks = snapshot.model.pending_write_tasks;
    snapshot.library.pending_read_tasks = snapshot.model.pending_tasks;
    snapshot.library.pending_read_tasks += ActiveReadSessions(node_id);
    snapshot.library.busy_drives = snapshot.model.busy_drives;
    snapshot.library.observed_at_ms = observed_at_ms;
    snapshot.library.telemetry_version = 1;
    snapshot.library.inventory_valid = true;
    snapshot.library.staging_valid = true;
    snapshot.library.background_valid = true;
    snapshot.library.observation_source = "scheduler-optical-model";
    snapshot.library.configured_disc_count = config.disc_count;
    snapshot.library.valid_until_ms = observed_at_ms + 30000;
    snapshot.library.accepts_archive_writes = true;
    snapshot.library.background_tasks = snapshot.model.pending_write_tasks + snapshot.model.pending_tasks;
    *output = std::move(snapshot);
    return true;
}

OpticalReadModel* InProcessOpticalService::GetModel(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = models_.find(node_id);
    return it == models_.end() ? nullptr : it->second.get();
}

std::shared_ptr<OpticalReadModel> InProcessOpticalService::Resolve(
    google::protobuf::RpcController* controller, zb::rpc::MdsStatus* status,
    bool requires_ready) const {
    auto* brpc_controller = controller ? static_cast<brpc::Controller*>(controller) : nullptr;
    std::string node_id;
    uint64_t generation = 0;
    if (!brpc_controller ||
        !zb::rpc_route::GetLogicalNodeRoute(*brpc_controller, &node_id, &generation)) {
        status->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        status->set_message("missing or invalid logical optical node route");
        return nullptr;
    }
    std::shared_ptr<OpticalReadModel> model;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(node_id);
        if (it != models_.end()) model = it->second;
    }
    if (!model) {
        status->set_code(zb::rpc::MDS_NOT_FOUND);
        status->set_message("logical optical node not found");
        return nullptr;
    }
    if (generation != model->config().instance_generation) {
        status->set_code(zb::rpc::MDS_STALE_EPOCH);
        status->set_message("stale optical node generation");
        return nullptr;
    }
    if (requires_ready && node_manager_) {
        ManagedNodeRuntime runtime;
        if (!node_manager_->GetNode(node_id, &runtime) ||
            runtime.lifecycle == LifecycleState::kJoining ||
            runtime.lifecycle == LifecycleState::kExiting ||
            runtime.lifecycle == LifecycleState::kRetired ||
            !runtime.readiness.Ready() ||
            runtime.health != ManagedHealthState::kHealthy ||
            runtime.administrative_state ==
                zb::storage_model::AdministrativeState::kDisabled) {
            status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            status->set_message("NODE_NOT_READY");
            return nullptr;
        }
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (catalog_confirmed_ids_.count(node_id) == 0) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message("MODEL_CATALOG_NOT_CONFIRMED");
                return nullptr;
            }
        }
        if (runtime.power_level < PowerLevel::kMedium) {
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto& ready_at = wake_ready_at_[node_id];
                if (ready_at <= std::chrono::steady_clock::now())
                    ready_at = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(model->config().wake_latency_ms);
            }
            const uint64_t now_ms = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
            std::string error;
            if (!node_manager_->RequestPowerLevel(node_id, PowerLevel::kMedium,
                                                  now_ms, true, &error)) {
                {
                    std::lock_guard<std::mutex> lock(mu_);
                    wake_ready_at_[node_id] = {};
                }
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message("NODE_WAKE_FAILED: " + error);
                return nullptr;
            }
        }
        std::chrono::steady_clock::time_point ready_at;
        {
            std::lock_guard<std::mutex> lock(mu_);
            ready_at = wake_ready_at_[node_id];
        }
        if (ready_at > std::chrono::steady_clock::now())
            zb::rpc_route::WaitModelUntil(ready_at);
        if (!node_manager_->GetNode(node_id, &runtime) ||
            runtime.lifecycle == LifecycleState::kJoining ||
            runtime.lifecycle == LifecycleState::kExiting ||
            runtime.lifecycle == LifecycleState::kRetired ||
            !runtime.readiness.Ready() ||
            runtime.health != ManagedHealthState::kHealthy ||
            runtime.administrative_state ==
                zb::storage_model::AdministrativeState::kDisabled) {
            status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            status->set_message("NODE_NOT_READY_AFTER_WAKE");
            return nullptr;
        }
    }
    return model;
}

void InProcessOpticalService::FillStatus(zb::rpc::MdsStatus* status,
                                         const OpticalModelResult& result) {
    switch (result.code) {
    case OpticalModelCode::kOk: status->set_code(zb::rpc::MDS_OK); break;
    case OpticalModelCode::kInvalid: status->set_code(zb::rpc::MDS_INVALID_ARGUMENT); break;
    case OpticalModelCode::kNotFound: status->set_code(zb::rpc::MDS_NOT_FOUND); break;
    case OpticalModelCode::kNotReady:
    case OpticalModelCode::kBusy:
    case OpticalModelCode::kNoSpace:
    case OpticalModelCode::kIoError:
        status->set_code(zb::rpc::MDS_INTERNAL_ERROR); break;
    }
    status->set_message(result.message);
}

void InProcessOpticalService::SendArchiveMetadata(
    google::protobuf::RpcController* controller,
    const zb::rpc::SendArchiveMetadataRequest* request,
    zb::rpc::ArchiveReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!response) return;
    if (!request) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing archive request");
        return;
    }
    std::shared_lock<zb::rpc_route::ModelSharedMutex> admission(write_admission_mu_);
    auto model = Resolve(controller, response->mutable_status(), true);
    if (!model) return;
    ManagedNodeRuntime runtime;
    if (!node_manager_ || !node_manager_->GetNode(model->config().node_id, &runtime) ||
        runtime.lifecycle != LifecycleState::kWorking ||
        runtime.service_mode != NodeServiceMode::kReadWrite ||
        runtime.administrative_state != zb::storage_model::AdministrativeState::kEnabled) {
        response->mutable_status()->set_code(zb::rpc::MDS_STALE_EPOCH);
        response->mutable_status()->set_message("optical node does not accept new archives");
        return;
    }
    if (mds_address_.empty() || service_address_.empty()) {
        response->mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        response->mutable_status()->set_message(
            "MODEL_ARCHIVE_UNSUPPORTED: MDS completion endpoint is not configured");
        return;
    }
    if (request->files_size() == 0 || request->files_size() > 4096) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message(
            "modeled optical archive batch requires 1..4096 files");
        return;
    }
    for (const auto& file : request->files()) {
        SubmitArchiveFile(model, file, response);
        auto* result = response->add_admissions();
        result->set_operation_id(file.operation_id());
        result->set_inode_id(file.inode_id());
        *result->mutable_status() = response->status();
        if (response->status().code() != zb::rpc::MDS_OK) return;
    }
}

void InProcessOpticalService::SubmitArchiveFile(const std::shared_ptr<OpticalReadModel>& model,
    const zb::rpc::ArchiveFile& file, zb::rpc::ArchiveReply* response) {
    if (file.operation_id().empty() || file.source_lease_id().empty() ||
        file.source_node_id().empty() || file.source_disk_id().empty() ||
        file.source_version() == 0 || file.inode_id() == 0 ||
        file.disc_id().empty() || file.image_id().empty() ||
        (file.target_node_id().size() != 0 &&
         file.target_node_id() != model->config().node_id)) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("incomplete modeled optical archive manifest");
        return;
    }
    OpticalArchiveRecord record;
    record.operation_id = file.operation_id();
    record.source_lease_id = file.source_lease_id();
    record.admission_lease_id = file.source_lease_id();
    record.inode_id = file.inode_id();
    record.version = file.source_version();
    record.size = file.size();
    record.source_node_id = file.source_node_id();
    record.source_disk_id = file.source_disk_id();
    record.disc_id = file.disc_id();
    record.image_id = file.image_id();
    OpticalArchiveRecord prior;
    if (model->GetArchive(record.operation_id, &prior)) {
        const auto original = prior.admission_lease_id.empty() ? prior.source_lease_id : prior.admission_lease_id;
        if ((record.source_lease_id != original && record.source_lease_id != prior.source_lease_id) ||
            record.inode_id != prior.inode_id || record.version != prior.version || record.size != prior.size ||
            record.source_node_id != prior.source_node_id || record.source_disk_id != prior.source_disk_id ||
            record.disc_id != prior.disc_id || record.image_id != prior.image_id || !prior.source_image_id.empty()) {
            response->mutable_status()->set_code(zb::rpc::MDS_STALE_EPOCH);
            response->mutable_status()->set_message("operation ID reused with different archive manifest");
            return;
        }
        record.source_lease_id = prior.source_lease_id;
        record.admission_lease_id = original;
    }
    brpc::Channel channel;
    std::string error;
    if (!InitMdsChannel(mds_address_, &channel, &error)) {
        response->mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        response->mutable_status()->set_message(error);
        return;
    }
    zb::rpc::MdsService_Stub stub(&channel);
    auto validation = BuildMdsArchiveRequest(
        record, model->config().node_id, service_address_,
        model->config().instance_generation, false);
    zb::rpc::ModelOpticalArchiveReply validated;
    brpc::Controller mds_controller;
    stub.ModelOpticalArchive(&mds_controller, &validation, &validated, nullptr);
    if (mds_controller.Failed() || validated.status().code() != zb::rpc::MDS_OK) {
        response->mutable_status()->set_code(zb::rpc::MDS_STALE_EPOCH);
        response->mutable_status()->set_message(
            mds_controller.Failed() ? mds_controller.ErrorText() : validated.status().message());
        return;
    }
    if (validated.already_committed()) {
        OpticalArchiveRecord local;
        if (!model->GetArchive(record.operation_id, &local) ||
            local.phase != OpticalArchivePhase::kCommitted) {
            response->mutable_status()->set_code(zb::rpc::MDS_STALE_EPOCH);
            response->mutable_status()->set_message(
                "MDS committed this archive but local optical inventory is missing");
            return;
        }
        response->mutable_status()->set_code(zb::rpc::MDS_OK);
        response->mutable_status()->set_message("already committed");
        return;
    }
    auto source = model_sources_ ? model_sources_->BorrowStorage(record.source_node_id, false, &error) : nullptr;
    if (!source) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("modeled optical source is not hosted by Scheduler");
        return;
    }
    const auto source_verified = source->VerifyModelArchiveSource(
        record.inode_id, record.source_disk_id, record.size, record.version);
    if (!source_verified.ok()) {
        response->mutable_status()->set_code(zb::rpc::MDS_STALE_EPOCH);
        response->mutable_status()->set_message(source_verified.message);
        return;
    }
    const auto accepted = model->SubmitVerifiedArchive(record);
    FillStatus(response->mutable_status(), accepted);
}

bool InProcessOpticalService::ReconcileArchives(const std::string& node_id,
                                                 std::string* error) {
    ManagedNodeRuntime state;
    if (node_manager_ && node_manager_->GetNode(node_id, &state) &&
        (state.health != ManagedHealthState::kHealthy ||
         state.administrative_state == zb::storage_model::AdministrativeState::kDisabled)) return true;
    std::shared_ptr<OpticalReadModel> model;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(node_id);
        if (it != models_.end()) model = it->second;
    }
    if (!model) { if (error) *error = "optical model unavailable"; return false; }
    const auto pending = model->PendingArchives();
    if (pending.empty()) return true;
    brpc::Channel channel;
    if (!InitMdsChannel(mds_address_, &channel, error)) return false;
    zb::rpc::MdsService_Stub stub(&channel);
    auto renew_or_recover = [&](OpticalArchiveRecord* record) {
        zb::rpc::RenewArchiveLeaseRequest renew;
        renew.set_inode_id(record->inode_id);
        renew.set_node_id(record->source_node_id);
        renew.set_lease_id(record->source_lease_id);
        renew.set_requested_lease_ms(model->config().archive_source_lease_ms);
        zb::rpc::RenewArchiveLeaseReply renewed;
        brpc::Controller renewal;
        stub.RenewArchiveLease(&renewal, &renew, &renewed, nullptr);
        if (renewal.Failed() || renewed.status().code() != zb::rpc::MDS_OK) {
            if (error) *error = renewal.Failed() ? renewal.ErrorText() : renewed.status().message();
            return false;
        }
        if (renewed.renewed() && renewed.record().owner_disk_id() == record->source_disk_id) return true;
        const uint64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        if (renewed.record().state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
            renewed.record().owner_node_id() != record->source_node_id ||
            renewed.record().owner_disk_id() != record->source_disk_id ||
            renewed.record().lease_expire_ts_ms() > now) {
            if (error) *error = "archive lease belongs to another active operation; source retained";
            return false;
        }
        // 过期后先核对 MDS 权威位置/版本及本地源，绝不直接修改租约或猜测提交成功。
        zb::rpc::GetFileLocationRequest get;
        get.set_inode_id(record->inode_id);
        zb::rpc::GetFileLocationReply current;
        brpc::Controller lookup;
        stub.GetFileLocation(&lookup, &get, &current, nullptr);
        if (lookup.Failed() || current.status().code() != zb::rpc::MDS_OK ||
            !current.location().has_disk_location() ||
            current.location().disk_location().node_id() != record->source_node_id ||
            current.location().disk_location().disk_id() != record->source_disk_id ||
            current.location().attr().version() != record->version ||
            current.location().attr().size() != record->size) {
            if (error) *error = "expired archive source changed in MDS; source retained";
            return false;
        }
        auto source = model_sources_ ? model_sources_->BorrowStorage(record->source_node_id, false, error) : nullptr;
        if (!source || !source->VerifyModelArchiveSource(record->inode_id, record->source_disk_id,
                                                        record->size, record->version).ok()) {
            if (error && error->empty()) *error = "expired archive local source changed";
            return false;
        }
        zb::rpc::ClaimArchiveTaskRequest claim;
        claim.set_inode_id(record->inode_id);
        claim.set_node_id(record->source_node_id);
        claim.set_disk_id(record->source_disk_id);
        claim.set_request_id(record->operation_id + "/recover-lease");
        claim.set_requested_lease_ms(model->config().archive_source_lease_ms);
        zb::rpc::ClaimArchiveTaskReply granted;
        brpc::Controller claiming;
        stub.ClaimArchiveTask(&claiming, &claim, &granted, nullptr);
        if (claiming.Failed() || granted.status().code() != zb::rpc::MDS_OK || !granted.granted() ||
            granted.record().owner_node_id() != record->source_node_id ||
            granted.record().owner_disk_id() != record->source_disk_id) {
            if (error) *error = "MDS refused archive lease recovery; source retained";
            return false;
        }
        const auto saved = model->ReplaceArchiveLease(record->operation_id, record->source_lease_id,
                                                      granted.record().lease_id());
        if (saved.code != OpticalModelCode::kOk) {
            if (error) *error = saved.message;
            return false;
        }
        record->source_lease_id = granted.record().lease_id();
        return true;
    };
    for (auto record : pending) {
        if (!record.source_image_id.empty()) {
            if (record.phase != OpticalArchivePhase::kAwaitingMds) continue;
            zb::rpc::CommitModelOpticalMigrationRequest request;
            request.set_operation_id(record.operation_id);
            request.set_inode_id(record.inode_id);
            request.set_expected_version(record.version);
            request.set_size(record.size);
            auto* source = request.mutable_expected_source();
            source->set_node_id(record.source_node_id);
            source->set_disk_id(record.source_disk_id);
            source->set_image_id(record.source_image_id);
            source->set_instance_generation(record.source_generation);
            *request.mutable_target() = BuildMdsArchiveRequest(
                record, node_id, service_address_, model->config().instance_generation, true).target();
            zb::rpc::CommitModelOpticalMigrationReply reply;
            brpc::Controller controller;
            stub.CommitModelOpticalMigration(&controller, &request, &reply, nullptr);
            if (controller.Failed() || reply.status().code() != zb::rpc::MDS_OK) {
                if (error) *error = controller.Failed() ? controller.ErrorText() : reply.status().message();
                return false;
            }
            std::shared_ptr<OpticalReadModel> source_model;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto it = models_.find(record.source_node_id);
                if (it != models_.end()) source_model = it->second;
            }
            if (source_model) {
                const auto removed = source_model->RemoveFile(record.inode_id, record.version,
                    record.source_disk_id, record.source_image_id);
                if (removed.code != OpticalModelCode::kOk && removed.code != OpticalModelCode::kNotFound) {
                    if (error) *error = removed.message;
                    return false;
                }
            } else {
                ManagedNodeRuntime source_runtime;
                if (!node_manager_->GetNode(record.source_node_id, &source_runtime) ||
                    source_runtime.lifecycle != LifecycleState::kRetired) {
                    if (error) *error = "optical migration source unavailable before cleanup";
                    return false;
                }
            }
            const auto ack = model->AcknowledgeArchive(record.operation_id, record.source_lease_id);
            if (ack.code != OpticalModelCode::kOk) { if (error) *error = ack.message; return false; }
            continue;
        }
        if (record.phase == OpticalArchivePhase::kAwaitingMds) {
            const auto request = BuildMdsArchiveRequest(
                record, node_id, service_address_,
                model->config().instance_generation, true);
            zb::rpc::ModelOpticalArchiveReply reply;
            brpc::Controller controller;
            stub.ModelOpticalArchive(&controller, &request, &reply, nullptr);
            if (controller.Failed() || reply.status().code() != zb::rpc::MDS_OK) {
                if (!controller.Failed() && reply.status().code() == zb::rpc::MDS_STALE_EPOCH)
                    (void)renew_or_recover(&record); // 新租约先落盘，下个 tick 再走原条件提交。
                if (error) *error = controller.Failed() ? controller.ErrorText()
                                                        : reply.status().message();
                return false;
            }
            auto source = model_sources_ ? model_sources_->BorrowStorage(record.source_node_id, false, error) : nullptr;
            if (!source) {
                ManagedNodeRuntime retired;
                if (!node_manager_ || !node_manager_->GetNode(record.source_node_id, &retired) ||
                    retired.lifecycle != LifecycleState::kRetired ||
                    retired.profile.execution_mode != ExecutionMode::kVirtual) {
                    if (error) *error = "modeled optical source unavailable for post-commit cleanup";
                    return false;
                }
            } else {
                zb::msg::DeleteFileMetaRequest cleanup;
                cleanup.inode_id = record.inode_id;
                cleanup.disk_id = record.source_disk_id;
                cleanup.purge_objects = true;
                const auto removed = source->DeleteFileMeta(cleanup);
                if (!removed.status.ok() && removed.status.code != zb::msg::StatusCode::kNotFound) {
                    if (error) *error = removed.status.message;
                    return false;
                }
            }
            const auto acknowledged = model->AcknowledgeArchive(
                record.operation_id, record.source_lease_id);
            if (acknowledged.code != OpticalModelCode::kOk) {
                if (error) *error = acknowledged.message;
                return false;
            }
        } else {
            if (!renew_or_recover(&record)) return false;
        }
    }
    return true;
}

bool InProcessOpticalService::BeginDataDrain(const std::string& id, uint64_t now_ms,
                                             std::string* error) {
    std::unique_lock<zb::rpc_route::ModelSharedMutex> admission(write_admission_mu_);
    if (!GetModel(id)) { if (error) *error = "optical model unavailable"; return false; }
    return node_manager_ && node_manager_->BeginRemoveNode(id, now_ms, error);
}

uint32_t InProcessOpticalService::RebootLatencyMs(const std::string& id) const {
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = models_.find(id);
    return it == models_.end() ? 0 : it->second->config().reboot_latency_ms;
}

bool InProcessOpticalService::PauseNode(const std::string& id, bool paused, std::string* error) {
    std::shared_ptr<OpticalReadModel> model;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(id);
        if (it != models_.end()) model = it->second;
    }
    if (!model) { if (error) *error = "optical model unavailable"; return false; }
    return model->SetPaused(paused, error);
}

bool InProcessOpticalService::AdvanceDataDrain(DrainMigrationTask* task, uint64_t now_ms,
                                               std::string* error) {
    if (error) error->clear();
    if (!task) return false;
    std::shared_ptr<OpticalReadModel> source;
    std::vector<std::pair<std::string, std::shared_ptr<OpticalReadModel>>> candidates;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(task->source_node_id);
        if (it == models_.end()) { if (error) *error = "optical source unavailable"; return false; }
        source = it->second;
        for (const auto& [id, model] : models_)
            if (id != task->source_node_id && catalog_confirmed_ids_.count(id))
                candidates.emplace_back(id, model);
    }
    const auto files = source->ListFiles();
    const auto stats = source->Stats();
    task->remaining_objects = files.size();
    task->remaining_bytes = 0;
    for (const auto& file : files) task->remaining_bytes += file.size;
    task->total_objects = std::max(task->total_objects, task->remaining_objects);
    task->total_bytes = std::max(task->total_bytes, task->remaining_bytes);
    task->migrated_objects = task->total_objects - task->remaining_objects;
    task->migrated_bytes = task->total_bytes - task->remaining_bytes;
    node_manager_->ReportDrainProgress(task->source_node_id, now_ms,
        task->remaining_objects, task->remaining_bytes, stats.pending_tasks, nullptr);
    if (stats.pending_write_tasks || stats.pending_tasks || ActiveReadSessions(task->source_node_id))
        return false;
    if (files.empty()) {
        // 清理发生在本轮库存采样之后时，不能用旧的非空库存直接做最终退役。
        // 等下一份完整库存上报，保留 Exiting 状态而不是永久误报 Blocked。
        ManagedNodeRuntime reported;
        if (!node_manager_->GetNode(task->source_node_id, &reported)) return false;
        const auto& library = reported.optical_library;
        return stats.cached_bytes == 0 && stats.busy_drives == 0 &&
            library.observed && library.blank_disc_count == library.local_disc_count &&
            library.recording_disc_count == 0 && library.pending_write_tasks == 0 &&
            library.pending_read_tasks == 0 && library.busy_drives == 0;
    }
    const auto& file = files.front();
    std::vector<OpticalFileRecord> image_files;
    uint64_t image_bytes = 0, staging_bytes = 0;
    for (const auto& entry : files) {
        if (entry.disc_id != file.disc_id || entry.image_id != file.image_id) continue;
        image_files.push_back(entry);
        image_bytes += entry.media_bytes;
        if (entry.size > UINT64_MAX - staging_bytes) {
            if (error) *error = "optical image logical size overflow";
            return false;
        }
        staging_bytes += entry.size;
    }
    const std::string op = "optical-drain-" + task->source_node_id + "-" +
        std::to_string(source->config().instance_generation) + "-" +
        std::to_string(file.inode_id) + "-" + std::to_string(file.version);
    std::sort(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    // 固定操作 ID 保存在目标 outbox，进程重启不依赖易失的排空任务表。
    for (const auto& [id, target] : candidates) {
        OpticalArchiveRecord existing;
        if (target->GetArchive(op, &existing)) return false;
    }
    brpc::Channel channel;
    if (!InitMdsChannel(mds_address_, &channel, error)) return false;
    zb::rpc::MdsService_Stub mds(&channel);
    zb::rpc::GetFileLocationRequest location_request;
    location_request.set_inode_id(file.inode_id);
    zb::rpc::GetFileLocationReply location_reply;
    brpc::Controller controller;
    mds.GetFileLocation(&controller, &location_request, &location_reply, nullptr);
    if (controller.Failed() || location_reply.status().code() != zb::rpc::MDS_OK ||
        !location_reply.location().has_optical_location()) {
        if (error) *error = "cannot verify authoritative optical source";
        return false;
    }
    const auto& current = location_reply.location().optical_location();
    if (current.node_id() != task->source_node_id || current.disk_id() != file.disc_id ||
        current.image_id() != file.image_id ||
        current.instance_generation() != source->config().instance_generation ||
        location_reply.location().attr().version() != file.version ||
        location_reply.location().attr().size() != file.size) {
        if (error) *error = "optical drain source no longer matches MDS; reconciliation required";
        return false;
    }
    for (const auto& [id, target] : candidates) {
        ManagedNodeRuntime runtime;
        if (!node_manager_->GetNode(id, &runtime) || runtime.lifecycle != LifecycleState::kWorking ||
            runtime.health != ManagedHealthState::kHealthy ||
            runtime.administrative_state != zb::storage_model::AdministrativeState::kEnabled ||
            runtime.service_mode != NodeServiceMode::kReadWrite) continue;
        const auto capacity = target->Stats();
        const uint64_t image_capacity = target->config().image_capacity_bytes == 0
            ? target->config().disc_capacity_bytes : target->config().image_capacity_bytes;
        if (capacity.occupied_discs >= target->config().disc_count ||
            image_bytes > image_capacity ||
            image_files.size() > target->config().max_archive_tasks - capacity.pending_write_tasks ||
            staging_bytes > target->config().cache_capacity_bytes - capacity.staging_used_bytes) continue;
        OpticalArchiveRecord record;
        record.operation_id = op;
        record.source_lease_id = op;
        record.inode_id = file.inode_id;
        record.version = file.version;
        record.size = file.size;
        record.media_bytes = file.media_bytes;
        record.source_node_id = task->source_node_id;
        record.source_disk_id = file.disc_id;
        record.source_image_id = file.image_id;
        record.source_generation = source->config().instance_generation;
        record.disc_id = op;
        record.image_id = "image-" + op;
        std::shared_lock<zb::rpc_route::ModelSharedMutex> admission(write_admission_mu_);
        // 排空与归档在同一准入锁内重新核对，不能向刚关闭写入的目标提交。
        if (!node_manager_->GetNode(id, &runtime) || runtime.lifecycle != LifecycleState::kWorking) continue;
        for (const auto& entry : image_files) {
            record.inode_id = entry.inode_id;
            record.version = entry.version;
            record.size = entry.size;
            record.media_bytes = entry.media_bytes;
            record.operation_id = "optical-drain-" + task->source_node_id + "-" +
                std::to_string(source->config().instance_generation) + "-" +
                std::to_string(entry.inode_id) + "-" + std::to_string(entry.version);
            record.source_lease_id = record.operation_id;
            const auto accepted = target->SubmitVerifiedArchive(record);
            if (accepted.code != OpticalModelCode::kOk) {
                if (error) *error = accepted.message;
                return false;
            }
        }
        node_manager_->RequestPowerLevel(id, PowerLevel::kMedium, now_ms, true, nullptr);
        return false;
    }
    if (error) *error = "no admitted model optical target has sufficient disc/staging capacity";
    return false;
}

void InProcessOpticalService::RequestAsyncReadFile(
    google::protobuf::RpcController* controller,
    const zb::rpc::RequestAsyncReadFileRequest* request,
    zb::rpc::RequestAsyncReadFileReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!response) return;
    if (!request) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing optical read request");
        return;
    }
    auto model = Resolve(controller, response->mutable_status(), true);
    if (!model) return;
    auto metrics = GetMetricsShared(model->config().node_id);
    if (!metrics) {
        response->mutable_status()->set_code(zb::rpc::MDS_NOT_FOUND);
        response->mutable_status()->set_message("logical optical node was released");
        return;
    }
    auto operation = metrics->Start(
        zb::metrics::OperationKind::kOther);
    const auto result = model->RequestRead(request->inode_id(), request->disk_id(),
                                           request->image_id());
    FillStatus(response->mutable_status(), result);
    if (result.code == OpticalModelCode::kOk) response->set_task_id(result.task_id);
}

void InProcessOpticalService::ReadObjectByTaskId(
    google::protobuf::RpcController* controller,
    const zb::rpc::ReadObjectByTaskIdRequest* request,
    zb::rpc::ReadObjectByTaskIdReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!response) return;
    if (!request) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing optical task read request");
        return;
    }
    auto model = Resolve(controller, response->mutable_status(), true);
    if (!model) return;
    auto metrics = GetMetricsShared(model->config().node_id);
    if (!metrics) {
        response->mutable_status()->set_code(zb::rpc::MDS_NOT_FOUND);
        response->mutable_status()->set_message("logical optical node was released");
        return;
    }
    auto operation = metrics->Start(
        zb::metrics::OperationKind::kRead);
    auto result = model->ReadTask(request->task_id(), request->offset(), request->read_size());
    FillStatus(response->mutable_status(), result);
    if (result.code == OpticalModelCode::kOk) {
        operation.SetBytes(result.data.size());
        response->set_data(std::move(result.data));
    }
}

void InProcessOpticalService::ReadObjectByInodeId(
    google::protobuf::RpcController* controller,
    const zb::rpc::ReadObjectByInodeIdRequest* request,
    zb::rpc::ReadObjectByInodeIdReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!response) return;
    if (!request) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message("missing optical inode read request");
        return;
    }
    auto model = Resolve(controller, response->mutable_status(), true);
    if (!model) return;
    auto metrics = GetMetricsShared(model->config().node_id);
    if (!metrics) {
        response->mutable_status()->set_code(zb::rpc::MDS_NOT_FOUND);
        response->mutable_status()->set_message("logical optical node was released");
        return;
    }
    auto operation = metrics->Start(
        zb::metrics::OperationKind::kRead);
    auto result = model->ReadLatest(request->inode_id(), request->offset(), request->read_size());
    FillStatus(response->mutable_status(), result);
    if (result.code == OpticalModelCode::kOk) {
        operation.SetBytes(result.data.size());
        response->set_data(std::move(result.data));
    }
}

zb::metrics::NodeMetricsCollector* InProcessOpticalService::GetBackgroundMetrics(
    const std::string& node_id, bool migration) {
    std::shared_ptr<OpticalReadModel> model;
    std::shared_ptr<BackgroundMetrics> counters;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto m = models_.find(node_id);
        auto b = background_metrics_.find(node_id);
        if (m == models_.end() || b == background_metrics_.end()) return nullptr;
        model = m->second;
        counters = b->second;
    }
    const auto records = model->ListArchives();
    {
        std::lock_guard<std::mutex> lock(counters->mu);
        for (const auto& item : records) {
            if (item.phase != OpticalArchivePhase::kAwaitingMds && item.phase != OpticalArchivePhase::kCommitted) continue;
            if (!counters->observed_completions.insert(item.operation_id).second) continue;
            auto& collector = item.source_image_id.empty() ? counters->archive : counters->migration;
            // 完成的是模型编码字节，不是假称发生了同等规模的网络或介质真实 IO。
            auto write = collector.Start(zb::metrics::OperationKind::kWrite);
            write.SetBytes(item.media_bytes);
        }
    }
    return migration ? &counters->migration : &counters->archive;
}

std::shared_ptr<OpticalReadModel> InProcessOpticalService::ResolveInventory(
    const std::string& node_id, uint64_t generation, zb::rpc::OpticalStatus* status) const {
    std::shared_ptr<OpticalReadModel> model;
    {
        std::lock_guard<std::mutex> lock(mu_);
        const auto it = models_.find(node_id);
        if (it != models_.end()) model = it->second;
    }
    if (!model) {
        status->set_code(zb::rpc::OPTICAL_NOT_FOUND);
        status->set_message("logical optical node not found");
        return {};
    }
    if (model->config().instance_generation != generation) {
        status->set_code(zb::rpc::OPTICAL_STALE_GENERATION);
        status->set_message("optical instance changed; refresh CMS catalog");
        return {};
    }
    if (node_manager_) {
        ManagedNodeRuntime runtime;
        if (!node_manager_->GetNode(node_id, &runtime) ||
            runtime.lifecycle == LifecycleState::kJoining || runtime.lifecycle == LifecycleState::kRetired ||
            !runtime.readiness.metadata_ready) {
            status->set_code(zb::rpc::OPTICAL_NOT_READY);
            status->set_message("logical optical metadata not available");
            return {};
        }
    }
    return model;
}

namespace {
std::vector<zb::rpc::ArchiveImageInfo> ModelImages(const std::vector<OpticalFileRecord>& files) {
    std::map<std::pair<std::string, std::string>, zb::rpc::ArchiveImageInfo> grouped;
    for (const auto& file : files) {
        auto& image = grouped[{file.disc_id, file.image_id}];
        image.set_disk_id(file.disc_id);
        image.set_image_id(file.image_id);
        image.set_image_length(image.image_length() + file.media_bytes);
    }
    std::vector<zb::rpc::ArchiveImageInfo> images;
    for (auto& item : grouped) images.push_back(std::move(item.second));
    return images;
}
} // namespace

void InProcessOpticalService::ListArchiveTasks(google::protobuf::RpcController*,
    const zb::rpc::ListArchiveTasksRequest* request, zb::rpc::ListArchiveTasksReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    reply->Clear();
    if (!request || !zb::archive_inventory::ValidatePage(*request, reply->mutable_status())) return;
    auto model = ResolveInventory(request->node_id(), request->instance_generation(), reply->mutable_status());
    if (!model) return;
    reply->set_node_id(request->node_id());
    reply->set_instance_generation(model->config().instance_generation);
    reply->set_modeled(true);
    std::vector<zb::rpc::ArchiveTaskInfo> tasks;
    for (const auto& item : model->ListArchives()) {
        auto& task = tasks.emplace_back();
        task.set_task_id(item.operation_id);
        task.set_operation_id(item.operation_id);
        task.set_inode_id(item.inode_id);
        const char* phases[] = {"UNKNOWN", "TRANSFER", "PACK", "BURN", "AWAITING_MDS", "COMMITTED", "FAILED"};
        task.set_phase(phases[static_cast<unsigned>(item.phase)]);
        task.set_terminal(item.phase == OpticalArchivePhase::kCommitted || item.phase == OpticalArchivePhase::kFailed);
        task.set_failed(item.phase == OpticalArchivePhase::kFailed);
        task.set_has_file_version(true);
        task.set_file_version(item.version);
        task.set_size(item.size);
        task.set_disk_id(item.disc_id);
        task.add_image_ids(item.image_id);
        task.set_local_media_complete(item.phase == OpticalArchivePhase::kAwaitingMds || item.phase == OpticalArchivePhase::kCommitted);
    }
    size_t end = 0;
    if (!zb::archive_inventory::Page(*request, zb::archive_inventory::TaskToken(tasks), tasks.size(), reply, &end)) return;
    for (size_t i = request->offset(); i < end; ++i) *reply->add_tasks() = tasks[i];
}

void InProcessOpticalService::ListArchiveImages(google::protobuf::RpcController*,
    const zb::rpc::ListArchiveImagesRequest* request, zb::rpc::ListArchiveImagesReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    reply->Clear();
    if (!request) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        return;
    }
    if (!zb::archive_inventory::ValidatePage(*request, reply->mutable_status())) return;
    auto model = ResolveInventory(request->node_id(), request->instance_generation(), reply->mutable_status());
    if (!model) return;
    reply->set_node_id(request->node_id());
    reply->set_instance_generation(model->config().instance_generation);
    reply->set_modeled(true);
    const auto images = ModelImages(model->ListFiles());
    size_t end = 0;
    if (!zb::archive_inventory::Page(*request, zb::archive_inventory::Token(images), images.size(), reply, &end)) return;
    for (size_t i = request->offset(); i < end; ++i) *reply->add_images() = images[i];
}

void InProcessOpticalService::GetArchiveImageFiles(google::protobuf::RpcController*,
    const zb::rpc::GetArchiveImageFilesRequest* request, zb::rpc::GetArchiveImageFilesReply* reply,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!reply) return;
    reply->Clear();
    if (!request) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        return;
    }
    if (!zb::archive_inventory::ValidatePage(*request, reply->mutable_status())) return;
    if (request->image_id().empty() || request->disk_id().empty()) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT);
        reply->mutable_status()->set_message("disk_id and image_id are required");
        return;
    }
    auto model = ResolveInventory(request->node_id(), request->instance_generation(), reply->mutable_status());
    if (!model) return;
    reply->set_node_id(request->node_id());
    reply->set_instance_generation(model->config().instance_generation);
    reply->set_modeled(true);
    const auto inventory = model->ListFiles();
    for (const auto& image : ModelImages(inventory)) {
        if (image.image_id() != request->image_id() || image.disk_id() != request->disk_id()) continue;
        std::vector<zb::rpc::ArchiveImageFile> files;
        for (const auto& item : inventory) {
            if (item.image_id != request->image_id() || item.disc_id != request->disk_id()) continue;
            auto& file = files.emplace_back();
            file.set_inode_id(item.inode_id);
            file.set_size(item.size);
            file.set_has_file_version(true);
            file.set_file_version(item.version);
        }
        std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) { return a.inode_id() < b.inode_id(); });
        size_t end = 0;
        if (!zb::archive_inventory::Page(*request, zb::archive_inventory::Token({image}, files),
                                        files.size(), reply, &end)) return;
        *reply->mutable_image() = image;
        for (size_t i = request->offset(); i < end; ++i) *reply->add_files() = files[i];
        return;
    }
    reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_FOUND);
    reply->mutable_status()->set_message("image absent from completed logical inventory");
}

} // namespace zb::scheduler
