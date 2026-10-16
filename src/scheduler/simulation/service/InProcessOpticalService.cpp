#include "InProcessOpticalService.h"

#include <brpc/controller.h>
#include <brpc/channel.h>

#include <chrono>
#include <limits>
#include <thread>

#include "common/rpc/LogicalNodeRoute.h"
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
        config.wake_latency_ms > 3600000) {
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
    wake_ready_at_[id] = {};
    models_.emplace(id, std::move(model));
    known_node_ids_.insert(id);
    return true;
}

bool InProcessOpticalService::DiscardUnpublishedNode(const std::string& node_id) {
    std::lock_guard<std::mutex> lock(mu_);
    if (models_.erase(node_id) == 0) return false;
    metrics_.erase(node_id);
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
        runtime.lifecycle != LifecycleState::kWorking || !runtime.readiness.Ready() ||
        runtime.health != ManagedHealthState::kHealthy) return false;
    std::lock_guard<std::mutex> lock(mu_);
    if (models_.count(node_id) == 0) return false;
    catalog_confirmed_ids_.insert(node_id);
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
            std::this_thread::sleep_until(ready_at);
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
    auto model = Resolve(controller, response->mutable_status(), false);
    if (!model) return;
    if (mds_address_.empty() || service_address_.empty()) {
        response->mutable_status()->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        response->mutable_status()->set_message(
            "MODEL_ARCHIVE_UNSUPPORTED: MDS completion endpoint is not configured");
        return;
    }
    if (request->files_size() != 1) {
        response->mutable_status()->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
        response->mutable_status()->set_message(
            "modeled optical archive requires exactly one file per operation");
        return;
    }
    const auto& file = request->files(0);
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
    record.inode_id = file.inode_id();
    record.version = file.source_version();
    record.size = file.size();
    record.source_node_id = file.source_node_id();
    record.source_disk_id = file.source_disk_id();
    record.disc_id = file.disc_id();
    record.image_id = file.image_id();
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
    auto* source = model_sources_ ? model_sources_->GetStorage(record.source_node_id) : nullptr;
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
    for (const auto& record : pending) {
        if (record.phase == OpticalArchivePhase::kAwaitingMds) {
            const auto request = BuildMdsArchiveRequest(
                record, node_id, service_address_,
                model->config().instance_generation, true);
            zb::rpc::ModelOpticalArchiveReply reply;
            brpc::Controller controller;
            stub.ModelOpticalArchive(&controller, &request, &reply, nullptr);
            if (controller.Failed() || reply.status().code() != zb::rpc::MDS_OK) {
                if (error) *error = controller.Failed() ? controller.ErrorText()
                                                        : reply.status().message();
                return false;
            }
            auto* source = model_sources_ ? model_sources_->GetStorage(record.source_node_id) : nullptr;
            if (!source) {
                if (error) *error = "modeled optical source unavailable for post-commit cleanup";
                return false;
            }
            zb::msg::DeleteFileMetaRequest cleanup;
            cleanup.inode_id = record.inode_id;
            cleanup.disk_id = record.source_disk_id;
            cleanup.purge_objects = true;
            const auto removed = source->DeleteFileMeta(cleanup);
            if (!removed.status.ok() &&
                removed.status.code != zb::msg::StatusCode::kNotFound) {
                if (error) *error = removed.status.message;
                return false;
            }
            const auto acknowledged = model->AcknowledgeArchive(
                record.operation_id, record.source_lease_id);
            if (acknowledged.code != OpticalModelCode::kOk) {
                if (error) *error = acknowledged.message;
                return false;
            }
        } else {
            zb::rpc::RenewArchiveLeaseRequest renew;
            renew.set_inode_id(record.inode_id);
            renew.set_node_id(record.source_node_id);
            renew.set_lease_id(record.source_lease_id);
            renew.set_requested_lease_ms(300000);
            zb::rpc::RenewArchiveLeaseReply reply;
            brpc::Controller controller;
            stub.RenewArchiveLease(&controller, &renew, &reply, nullptr);
            if (controller.Failed() || reply.status().code() != zb::rpc::MDS_OK ||
                !reply.renewed() || reply.record().owner_disk_id() != record.source_disk_id) {
                if (error) *error = controller.Failed() ? controller.ErrorText()
                    : "modeled archive source lease could not be renewed";
                return false;
            }
        }
    }
    return true;
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

} // namespace zb::scheduler
