#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include "common/rpc/LogicalNodeRoute.h"
#include "scheduler/simulation/service/InProcessOpticalService.h"

namespace {

void Check(bool ok, const std::string& reason) {
    if (!ok) throw std::runtime_error(reason);
}

class TempDirectory {
public:
    TempDirectory() {
        std::string pattern =
            (std::filesystem::temp_directory_path() / "zb-inprocess-optical-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        const char* created = mkdtemp(buffer.data());
        Check(created != nullptr, "create optical temporary directory");
        path_ = created;
    }
    ~TempDirectory() {
        std::error_code error;
        if (!path_.empty()) std::filesystem::remove_all(path_, error);
    }
    const std::filesystem::path& path() const { return path_; }
private:
    std::filesystem::path path_;
};

void Verify() {
    TempDirectory temporary;
    zb::scheduler::InProcessOpticalService service;
    for (const std::string& id : {"opt-a", "opt-b"}) {
        zb::scheduler::OpticalReadConfig config;
        config.node_id = id;
        config.state_dir = (temporary.path() / id).string();
        config.drive_count = 1;
        config.disc_count = 1;
        config.disc_capacity_bytes = 200;
        config.read_bytes_per_sec = 1000;
        config.cache_capacity_bytes = 100;
        config.cache_read_bytes_per_sec = 1000000000;
        config.cache_base_latency_us = 0;
        config.load_latency_ms = 30;
        config.seek_latency_ms = 10;
        config.max_read_tasks = 2;
        std::string error;
        Check(service.AddNode(config, &error), error);
    }
    auto* a = service.GetModel("opt-a");
    auto* b = service.GetModel("opt-b");
    Check(a && b, "models must exist");
    Check(a->InstallFile(7, 1, 100, "optical-disk-1", "img-1").code ==
          zb::scheduler::OpticalModelCode::kOk, "install A");
    Check(b->InstallFile(7, 1, 20, "optical-disk-1", "img-1").code ==
          zb::scheduler::OpticalModelCode::kOk, "install B");
    zb::scheduler::OpticalNodeManagementSnapshot management;
    Check(service.GetManagementSnapshot("opt-a", 1234, &management) &&
          management.disc_capacity_bytes == 200 && management.disc_free_bytes == 100 &&
          management.cache_capacity_bytes == 100 && management.cache_free_bytes == 100 &&
          management.model.occupied_discs == 1 &&
          management.library.observed && management.library.total_slots == 1 &&
          management.library.local_disc_count == 1 &&
          management.library.recorded_disc_count == 1 &&
          management.library.blank_disc_count == 0 &&
          management.library.observed_at_ms == 1234,
          "management snapshot reflects local persisted disc occupancy");
    std::string status_error;
    Check(zb::storage_model::ValidateOpticalLibraryStatus(management.library, &status_error),
          status_error);
    Check(a->InstallFile(8, 1, 101, "optical-disk-1", "img-2").code ==
          zb::scheduler::OpticalModelCode::kNoSpace, "disc capacity must bound files");

    brpc::Server server;
    Check(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0,
          "add optical service");
    Check(server.Start("127.0.0.1:0", nullptr) == 0, "start optical service");
    const std::string address = butil::endpoint2str(server.listen_address()).c_str();
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    Check(channel.Init(address.c_str(), &options) == 0, "connect optical service");
    zb::rpc::OpticalNodeService_Stub stub(&channel);

    auto request_read = [&](const std::string& id, uint64_t generation = 1) {
        brpc::Controller controller;
        Check(zb::rpc_route::SetLogicalNodeRoute(&controller, id, generation), "set route");
        zb::rpc::RequestAsyncReadFileRequest request;
        request.set_inode_id(7);
        request.set_disk_id("optical-disk-1");
        request.set_image_id("img-1");
        zb::rpc::RequestAsyncReadFileReply reply;
        stub.RequestAsyncReadFile(&controller, &request, &reply, nullptr);
        Check(!controller.Failed(), controller.ErrorText());
        return reply;
    };
    auto read_task = [&](const std::string& id, uint64_t task_id,
                         uint64_t offset, uint64_t size) {
        brpc::Controller controller;
        Check(zb::rpc_route::SetLogicalNodeRoute(&controller, id, 1), "set read route");
        zb::rpc::ReadObjectByTaskIdRequest request;
        request.set_task_id(task_id);
        request.set_offset(offset);
        request.set_read_size(size);
        zb::rpc::ReadObjectByTaskIdReply reply;
        stub.ReadObjectByTaskId(&controller, &request, &reply, nullptr);
        Check(!controller.Failed(), controller.ErrorText());
        return reply;
    };

    brpc::Controller missing;
    zb::rpc::RequestAsyncReadFileRequest initial;
    initial.set_inode_id(7);
    initial.set_disk_id("optical-disk-1");
    initial.set_image_id("img-1");
    zb::rpc::RequestAsyncReadFileReply missing_reply;
    stub.RequestAsyncReadFile(&missing, &initial, &missing_reply, nullptr);
    Check(!missing.Failed() && missing_reply.status().code() == zb::rpc::MDS_INVALID_ARGUMENT,
          "optical route is mandatory");
    Check(request_read("opt-a", 2).status().code() == zb::rpc::MDS_STALE_EPOCH,
          "stale optical generation rejected");

    auto a_task = request_read("opt-a");
    auto b_task = request_read("opt-b");
    Check(a_task.status().code() == zb::rpc::MDS_OK && a_task.task_id() == 1 &&
          b_task.status().code() == zb::rpc::MDS_OK && b_task.task_id() == 1,
          "same local task ID must be scoped by node");
    Check(read_task("opt-a", 1, 0, 100).status().message() == "MODEL_NOT_READY",
          "accepted is not completed");
    Check(a->Stats().pending_tasks == 1 && b->Stats().pending_tasks == 1,
          "each model has independent pending work");
    Check(service.GetManagementSnapshot("opt-a", 1235, &management) &&
          management.library.pending_read_tasks == 1 &&
          management.library.busy_drives == 1,
          "pending optical work is visible to management");
    std::this_thread::sleep_for(std::chrono::milliseconds(160));
    auto read_a = read_task("opt-a", 1, 0, 100);
    auto read_b = read_task("opt-b", 1, 0, 100);
    Check(read_a.status().code() == zb::rpc::MDS_OK && read_a.data().size() == 100 &&
          read_b.status().code() == zb::rpc::MDS_OK && read_b.data().size() == 20,
          "independent logical file lengths and EOF");
    Check(read_task("opt-a", 1, 0, 100).data().size() == 100,
          "terminal chunk retry retained");
    Check(a->Stats().cached_files == 1 && a->Stats().cached_bytes == 100,
          "completed read fills bounded cache");
    Check(service.GetManagementSnapshot("opt-a", 1236, &management) &&
          management.library.pending_read_tasks == 0 &&
          management.library.busy_drives == 0 &&
          management.cache_free_bytes == 0,
          "completed optical work releases management busy state");
    auto cached = request_read("opt-a");
    Check(cached.status().code() == zb::rpc::MDS_OK && cached.task_id() == 2,
          "cached read accepted");
    Check(read_task("opt-a", 2, 90, 20).data().size() == 10,
          "cache hit completes without drive delay and clips EOF");
    brpc::Controller inode_controller;
    Check(zb::rpc_route::SetLogicalNodeRoute(&inode_controller, "opt-a", 1),
          "route inode read");
    zb::rpc::ReadObjectByInodeIdRequest inode_request;
    inode_request.set_inode_id(7);
    inode_request.set_offset(98);
    inode_request.set_read_size(20);
    zb::rpc::ReadObjectByInodeIdReply inode_reply;
    stub.ReadObjectByInodeId(&inode_controller, &inode_request, &inode_reply, nullptr);
    Check(!inode_controller.Failed() && inode_reply.status().code() == zb::rpc::MDS_OK &&
          inode_reply.data().size() == 2,
          "inode read routes to this instance's latest task");
    Check(request_read("opt-a").status().code() == zb::rpc::MDS_INTERNAL_ERROR,
          "read task budget is bounded");
    auto* metrics = service.GetMetrics("opt-a");
    Check(metrics != nullptr, "optical metrics collector exists");
    const auto metric_snapshot = metrics->TakeForReport(0);
    Check(metric_snapshot.report_sequence != 0 &&
          metric_snapshot.other_operations == 3 &&
          metric_snapshot.read_operations == 5 &&
          metric_snapshot.read_bytes == 212,
          "optical RPC attempts and successfully delivered bytes are measured");
    metrics->AcknowledgeReport(metric_snapshot.report_sequence);

    brpc::Controller archive_controller;
    Check(zb::rpc_route::SetLogicalNodeRoute(&archive_controller, "opt-a", 1),
          "route archive");
    zb::rpc::SendArchiveMetadataRequest archive;
    archive.set_batch_id(99);
    zb::rpc::ArchiveReply archive_reply;
    stub.SendArchiveMetadata(&archive_controller, &archive, &archive_reply, nullptr);
    Check(!archive_controller.Failed() &&
          archive_reply.status().code() != zb::rpc::MDS_OK &&
          archive_reply.status().message().find("MODEL_ARCHIVE_UNSUPPORTED") != std::string::npos,
          "unimplemented archive completion must fail closed");

    server.Stop(0);
    server.Join();

    zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
    zb::scheduler::InProcessOpticalService guarded(&manager);
    zb::scheduler::OpticalReadConfig guarded_config;
    guarded_config.node_id = "opt-joining";
    guarded_config.state_dir = (temporary.path() / "opt-joining").string();
    guarded_config.wake_latency_ms = 120;
    std::string error;
    Check(guarded.AddNode(guarded_config, &error), error);
    auto profile = zb::scheduler::DefaultOpticalLibraryProfile("opt-joining");
    profile.execution_mode = zb::scheduler::ExecutionMode::kVirtual;
    profile.participates_in_placement = false;
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    Check(manager.RegisterNode(profile, static_cast<uint64_t>(now), 7, &error), error);
    Check(guarded.GetModel("opt-joining")->InstallFile(
              7, 1, 10, "optical-disk-1", "img-1").code ==
          zb::scheduler::OpticalModelCode::kOk, "install guarded fixture");
    brpc::Controller guarded_controller;
    Check(zb::rpc_route::SetLogicalNodeRoute(&guarded_controller, "opt-joining", 1),
          "route joining node");
    zb::rpc::RequestAsyncReadFileReply guarded_reply;
    guarded.RequestAsyncReadFile(&guarded_controller, &initial, &guarded_reply, nullptr);
    Check(guarded_reply.status().message() == "NODE_NOT_READY",
          "Joining optical instance must not serve data even if internally seeded");
    Check(manager.ActivateNode(profile.node_id, static_cast<uint64_t>(now + 1), &error),
          error);
    Check(guarded.ConfirmCatalog(profile.node_id), "confirm optical CMS publication");
    Check(manager.RequestPowerLevel(profile.node_id, zb::scheduler::PowerLevel::kOff,
                                    static_cast<uint64_t>(now + 2), true, &error), error);
    brpc::Controller waking_controller;
    Check(zb::rpc_route::SetLogicalNodeRoute(&waking_controller, "opt-joining", 1),
          "route sleeping optical node");
    zb::rpc::RequestAsyncReadFileReply waking_reply;
    const auto wake_started = std::chrono::steady_clock::now();
    guarded.RequestAsyncReadFile(&waking_controller, &initial, &waking_reply, nullptr);
    Check(waking_reply.status().code() == zb::rpc::MDS_OK &&
          std::chrono::steady_clock::now() - wake_started >=
              std::chrono::milliseconds(105),
          "ready optical node waits for configured wake before accepting read");
}

void VerifyDurableInventory() {
    TempDirectory temporary;
    zb::scheduler::OpticalReadConfig config;
    config.node_id = "durable-optical";
    config.state_dir = (temporary.path() / "state").string();
    config.disc_count = 1;
    config.disc_capacity_bytes = 150;
    config.load_latency_ms = 40;
    config.seek_latency_ms = 0;
    config.read_bytes_per_sec = 1000000;
    {
        zb::scheduler::InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model && model->InstallFile(5, 2, 100, "disc-a", "image-a").code ==
              zb::scheduler::OpticalModelCode::kOk, "persist optical file location");
        Check(model->InstallFile(5, 2, 100, "disc-a", "image-a").code ==
              zb::scheduler::OpticalModelCode::kOk, "same installed file is idempotent");
        Check(model->InstallFile(6, 1, 51, "disc-a", "image-b").code ==
              zb::scheduler::OpticalModelCode::kNoSpace, "disc capacity checked before persistence");
        Check(model->RemoveFile(5, 1, "disc-a", "image-a").code !=
              zb::scheduler::OpticalModelCode::kOk, "stale version cannot delete optical file");
        const auto pending = model->RequestRead(5, "disc-a", "image-a");
        Check(pending.code == zb::scheduler::OpticalModelCode::kOk &&
              pending.task_id == 1, "read task and identity persisted before response");
    }
    {
        auto changed = config;
        changed.disc_capacity_bytes = 151;
        zb::scheduler::InProcessOpticalService invalid;
        std::string error;
        Check(!invalid.AddNode(changed, &error) &&
              error.find("geometry") != std::string::npos,
              "recovered optical inventory rejects changed disc geometry");
    }
    {
        zb::scheduler::InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model && model->Stats().stored_bytes == 100,
              "optical stored bytes reconstructed from durable records");
        zb::scheduler::OpticalNodeManagementSnapshot recovered;
        Check(service.GetManagementSnapshot(config.node_id, 2000, &recovered) &&
              recovered.disc_capacity_bytes == 150 &&
              recovered.disc_free_bytes == 50 &&
              recovered.library.recorded_disc_count == 1 &&
              recovered.library.pending_read_tasks == 1,
              "management view uses recovered optical inventory and tasks");
        Check(model->ReadTask(1, 0, 100).code ==
              zb::scheduler::OpticalModelCode::kNotReady,
              "pending optical task resumes conservatively after restart");
        std::this_thread::sleep_for(std::chrono::milliseconds(60));
        Check(model->ReadTask(1, 0, 100).data.size() == 100,
              "recovered task completes with the original id and size");
        Check(model->InstallFile(6, 1, 51, "disc-a", "image-b").code ==
              zb::scheduler::OpticalModelCode::kNoSpace,
              "recovered disc usage still enforces capacity");
    }
    {
        zb::scheduler::InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model->ReadTask(1, 0, 100).data.size() == 100,
              "released task retry survives restart within retention");
        Check(model->RequestRead(5, "disc-a", "image-a").task_id == 2,
              "restarted node does not reuse a prior task id");
        Check(model->RemoveFile(5, 2, "disc-a", "image-a").code ==
              zb::scheduler::OpticalModelCode::kOk,
              "matched version and location may be durably removed");
        Check(model->Stats().stored_bytes == 0,
              "optical removal releases logical capacity");
        zb::scheduler::OpticalNodeManagementSnapshot emptied;
        Check(service.GetManagementSnapshot(config.node_id, 2001, &emptied) &&
              emptied.disc_free_bytes == 150 &&
              emptied.library.recorded_disc_count == 0 &&
              emptied.library.blank_disc_count == 1,
              "management view releases recorded disc after verified removal");
        Check(model->ReadTask(1, 0, 1).code == zb::scheduler::OpticalModelCode::kNotFound,
              "removing a file invalidates its read tasks");
    }
    {
        zb::scheduler::InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model && model->Stats().stored_bytes == 0,
              "optical deletion persists across restart");
        Check(model->InstallFile(6, 1, 100, "disc-b", "image-b").code ==
              zb::scheduler::OpticalModelCode::kOk,
              "released disc slot can be assigned to a new disc");
        Check(model->RequestRead(6, "disc-b", "image-b").task_id == 3,
              "deleted tasks do not permit task id reuse after restart");
    }
}

void VerifyDriveBudgetAndCache() {
    zb::scheduler::OpticalReadConfig config;
    config.node_id = "opt-budget";
    config.drive_count = 1;
    config.disc_count = 1;
    config.disc_capacity_bytes = 200;
    config.read_bytes_per_sec = 1000;
    config.cache_capacity_bytes = 60;
    config.load_latency_ms = 0;
    config.seek_latency_ms = 0;
    zb::scheduler::OpticalReadModel model(config);
    Check(model.InstallFile(1, 1, 60, "optical-disk-1", "img-1").code ==
          zb::scheduler::OpticalModelCode::kOk, "install first cache fixture");
    Check(model.InstallFile(2, 1, 60, "optical-disk-1", "img-2").code ==
          zb::scheduler::OpticalModelCode::kOk, "install second cache fixture");
    const auto first = model.RequestRead(1, "optical-disk-1", "img-1");
    const auto second = model.RequestRead(2, "optical-disk-1", "img-2");
    Check(first.code == zb::scheduler::OpticalModelCode::kOk &&
          second.code == zb::scheduler::OpticalModelCode::kOk &&
          first.modeled_wait_us >= 60000 &&
          second.modeled_wait_us >= first.modeled_wait_us + 40000,
          "one drive must queue the second whole-file transfer");
    std::this_thread::sleep_for(std::chrono::milliseconds(135));
    Check(model.ReadTask(first.task_id, 0, 60).data.size() == 60,
          "first drive task ready");
    Check(model.ReadTask(second.task_id, 0, 60).data.size() == 60,
          "second drive task ready");
    Check(model.Stats().cached_files == 1 && model.Stats().cached_bytes == 60,
          "bounded cache must evict LRU entry");
    const auto miss_again = model.RequestRead(1, "optical-disk-1", "img-1");
    Check(miss_again.code == zb::scheduler::OpticalModelCode::kOk &&
          !miss_again.cache_hit && miss_again.modeled_wait_us >= 60000,
          "evicted file must traverse the drive model again");
}

void VerifyDurableArchiveStages() {
    using namespace zb::scheduler;
    TempDirectory temporary;
    OpticalReadConfig config;
    config.node_id = "archive-optical";
    config.state_dir = (temporary.path() / "archive-state").string();
    config.disc_count = 2;
    config.disc_capacity_bytes = 100;
    config.cache_capacity_bytes = 120;
    config.drive_count = 1;
    config.archive_transfer_bytes_per_sec = 1000000;
    config.archive_pack_bytes_per_sec = 1000000;
    config.archive_burn_bytes_per_sec = 1000000;
    config.archive_burn_fixed_ms = 20;
    OpticalArchiveRecord first;
    first.operation_id = "archive-op-1";
    first.source_lease_id = "lease-1";
    first.inode_id = 50;
    first.version = 7;
    first.size = 80;
    first.source_node_id = "source-a";
    first.source_disk_id = "hdd-a";
    first.disc_id = "disc-a";
    first.image_id = "img-a";
    OpticalArchiveRecord second = first;
    second.operation_id = "archive-op-2";
    second.source_lease_id = "lease-2";
    second.inode_id = 51;
    second.version = 1;
    second.size = 50;
    second.disc_id = "disc-b";
    second.image_id = "img-b";
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model->SubmitVerifiedArchive(first).code == OpticalModelCode::kOk,
              "archive admission persists verified manifest");
        Check(model->SubmitVerifiedArchive(first).code == OpticalModelCode::kOk,
              "same operation and manifest is idempotent");
        auto changed = first;
        changed.version = 8;
        Check(model->SubmitVerifiedArchive(changed).code == OpticalModelCode::kInvalid,
              "operation ID cannot be reused for another version");
        Check(model->SubmitVerifiedArchive(second).code == OpticalModelCode::kNoSpace,
              "staging reservation enforces shared HDD capacity");
        Check(model->InstallFile(first.inode_id, first.version, first.size,
                                 first.disc_id, first.image_id).code == OpticalModelCode::kBusy,
              "unconfirmed archive prevents competing install");
        OpticalNodeManagementSnapshot state;
        Check(service.GetManagementSnapshot(config.node_id, 123, &state) &&
              state.library.pending_write_tasks == 1 &&
              state.library.recording_disc_count == 1 &&
              state.library.blank_disc_count == 1 &&
              state.library.staging_used_bytes == 80 &&
              state.cache_free_bytes == 40 &&
              state.disc_free_bytes == 120,
              "archive reservation is visible without prematurely using disc capacity");
        Check(zb::storage_model::ValidateOpticalLibraryStatus(state.library, &error), error);
    }
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        OpticalArchiveRecord restored;
        Check(model->GetArchive(first.operation_id, &restored) &&
              restored.phase == OpticalArchivePhase::kTransfer &&
              model->Stats().staging_used_bytes == 80,
              "restart restores phase and staging reservation");
        Check(model->AcknowledgeArchive(first.operation_id, first.source_lease_id).code ==
              OpticalModelCode::kNotReady,
              "MDS completion cannot be acknowledged before local burn");
        std::this_thread::sleep_for(std::chrono::milliseconds(45));
        const auto advance = model->AdvanceArchives();
        Check(advance.code == OpticalModelCode::kOk,
              "modeled transfer, pack and burn advance after restart: " + advance.message);
        Check(model->GetArchive(first.operation_id, &restored) &&
              restored.phase == OpticalArchivePhase::kAwaitingMds,
              "local burn produces a durable pending-MDS outbox state");
        OpticalNodeManagementSnapshot state;
        Check(service.GetManagementSnapshot(config.node_id, 124, &state) &&
              state.library.pending_write_tasks == 1 &&
              state.library.recording_disc_count == 0 &&
              state.library.recorded_disc_count == 1 &&
              state.library.staging_used_bytes == 0 &&
              state.disc_free_bytes == 120,
              "local burn atomically moves reservation to disc inventory");
        Check(model->RemoveFile(first.inode_id, first.version, first.disc_id,
                                first.image_id).code == OpticalModelCode::kBusy,
              "source cleanup cannot preempt unacknowledged outbox");
        Check(model->AcknowledgeArchive(first.operation_id, "wrong-lease").code ==
              OpticalModelCode::kInvalid, "wrong lease cannot acknowledge outbox");
    }
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        OpticalArchiveRecord restored;
        Check(model->GetArchive(first.operation_id, &restored) &&
              restored.phase == OpticalArchivePhase::kAwaitingMds &&
              model->Stats().stored_bytes == 80,
              "restart restores local file and unacknowledged outbox together");
        Check(model->AcknowledgeArchive(first.operation_id, first.source_lease_id).code ==
              OpticalModelCode::kOk, "verified MDS response may acknowledge outbox");
        Check(model->AcknowledgeArchive(first.operation_id, first.source_lease_id).code ==
              OpticalModelCode::kOk, "MDS acknowledgement is idempotent");
        Check(model->SubmitVerifiedArchive(second).code == OpticalModelCode::kOk,
              "staging capacity is reusable after burn");
        Check(model->FailArchive(second.operation_id, second.source_lease_id).code ==
              OpticalModelCode::kOk, "failed archive releases media and staging reservations");
        Check(model->Stats().pending_write_tasks == 0 &&
              model->Stats().staging_used_bytes == 0,
              "failed task no longer blocks power or admission");
        Check(model->RemoveFile(first.inode_id, first.version, first.disc_id,
                                first.image_id).code == OpticalModelCode::kOk,
              "MDS-acknowledged archive can be removed by trusted cleanup");
    }
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        OpticalArchiveRecord committed;
        OpticalArchiveRecord failed;
        Check(model->GetArchive(first.operation_id, &committed) &&
              committed.phase == OpticalArchivePhase::kCommitted &&
              model->GetArchive(second.operation_id, &failed) &&
              failed.phase == OpticalArchivePhase::kFailed &&
              model->Stats().stored_bytes == 0,
              "archive completion and failure tombstones survive restart");
        Check(model->SubmitVerifiedArchive(second).code == OpticalModelCode::kNotReady,
              "failed operation ID cannot silently become a fresh accepted task");
        Check(model->SubmitVerifiedArchive(first).code == OpticalModelCode::kOk &&
              model->Stats().pending_write_tasks == 0,
              "old operation cannot be replayed after source cleanup");
    }
}

void VerifyArchiveReadResourceCompetition() {
    using namespace zb::scheduler;
    TempDirectory temporary;
    OpticalReadConfig config;
    config.node_id = "competing-optical";
    config.state_dir = (temporary.path() / "state").string();
    config.drive_count = 1;
    config.disc_count = 2;
    config.disc_capacity_bytes = 100;
    config.cache_capacity_bytes = 100;
    config.load_latency_ms = 0;
    config.seek_latency_ms = 0;
    config.read_bytes_per_sec = 1000000;
    config.archive_transfer_bytes_per_sec = 1000000;
    config.archive_pack_bytes_per_sec = 1000000;
    config.archive_burn_bytes_per_sec = 1000000;
    config.archive_burn_fixed_ms = 80;
    InProcessOpticalService service;
    std::string error;
    Check(service.AddNode(config, &error), error);
    auto* model = service.GetModel(config.node_id);
    Check(model->InstallFile(1, 1, 60, "disc-read", "img-read").code ==
          OpticalModelCode::kOk, "install optical cache fixture");
    const auto warm = model->RequestRead(1, "disc-read", "img-read");
    Check(warm.code == OpticalModelCode::kOk, "warm read accepted");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    Check(model->ReadTask(warm.task_id, 0, 60).data.size() == 60 &&
          model->Stats().cached_bytes == 60, "warm read occupies cache");
    OpticalArchiveRecord archive;
    archive.operation_id = "compete-op";
    archive.source_lease_id = "compete-lease";
    archive.inode_id = 2;
    archive.version = 1;
    archive.size = 60;
    archive.source_node_id = "source";
    archive.source_disk_id = "hdd";
    archive.disc_id = "disc-write";
    archive.image_id = "img-write";
    Check(model->SubmitVerifiedArchive(archive).code == OpticalModelCode::kOk &&
          model->Stats().cached_bytes == 0,
          "staging reservation evicts read cache under shared HDD budget");
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    Check(model->AdvanceArchives().code == OpticalModelCode::kOk,
          "archive advances into burn stage");
    OpticalArchiveRecord phase;
    Check(model->GetArchive(archive.operation_id, &phase) &&
          phase.phase == OpticalArchivePhase::kBurn,
          "archive is still burning on the single drive");
    const auto competing_read = model->RequestRead(1, "disc-read", "img-read");
    Check(competing_read.code == OpticalModelCode::kOk &&
          !competing_read.cache_hit && competing_read.modeled_wait_us >= 30000,
          "optical read queues behind burn on the shared drive: " +
              competing_read.message + " wait_us=" +
              std::to_string(competing_read.modeled_wait_us));
    Check(model->Stats().pending_write_tasks == 1 &&
          model->Stats().pending_tasks == 1,
          "read and burn remain independently visible as background work");
}

void VerifyEncodedMediaAccounting() {
    using namespace zb::scheduler;
    TempDirectory temporary;
    OpticalReadConfig config;
    config.node_id = "encoded-optical";
    config.state_dir = (temporary.path() / "encoded-state").string();
    config.disc_count = 1;
    config.disc_capacity_bytes = 64;
    config.cache_capacity_bytes = 128;
    config.drive_count = 1;
    config.archive_compression_permille = 500;
    config.archive_alignment_bytes = 16;
    config.archive_transfer_bytes_per_sec = 1000000;
    config.archive_pack_bytes_per_sec = 1000000;
    config.archive_burn_bytes_per_sec = 1000000;
    config.archive_burn_fixed_ms = 1;
    OpticalArchiveRecord request;
    request.operation_id = "encoded-operation";
    request.source_lease_id = "encoded-lease";
    request.inode_id = 900;
    request.version = 1;
    request.size = 80;
    request.source_node_id = "source";
    request.source_disk_id = "disk";
    request.disc_id = "disc";
    request.image_id = "image";
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model->SubmitVerifiedArchive(request).code == OpticalModelCode::kOk,
              "compressed archive admission");
        OpticalArchiveRecord accepted;
        Check(model->GetArchive(request.operation_id, &accepted) &&
              accepted.media_bytes == 48 &&
              model->Stats().reserved_disc_bytes == 48 &&
              model->Stats().staging_used_bytes == 80,
              "logical staging and encoded disc allocation are distinct");
    }
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model->Stats().reserved_disc_bytes == 48,
              "encoded reservation survives restart");
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
        Check(model->AdvanceArchives().code == OpticalModelCode::kOk &&
              model->Stats().stored_bytes == 48 &&
              model->Stats().reserved_disc_bytes == 0,
              "burn commits encoded allocation, not logical input bytes");
        Check(model->AcknowledgeArchive(request.operation_id,
                                        request.source_lease_id).code == OpticalModelCode::kOk,
              "encoded archive can be acknowledged");
        auto read = model->RequestRead(request.inode_id, request.disc_id,
                                       request.image_id);
        Check(read.code == OpticalModelCode::kOk,
              "logical file remains readable after encoded allocation");
    }
    {
        InProcessOpticalService service;
        std::string error;
        Check(service.AddNode(config, &error), error);
        auto* model = service.GetModel(config.node_id);
        Check(model->Stats().stored_bytes == 48 &&
              model->RemoveFile(request.inode_id, request.version,
                                request.disc_id, request.image_id).code == OpticalModelCode::kOk &&
              model->Stats().stored_bytes == 0,
              "encoded allocation is released exactly once after restart");
    }
}

} // namespace

int main() {
    try {
        Verify();
        VerifyDriveBudgetAndCache();
        VerifyDurableInventory();
        VerifyDurableArchiveStages();
        VerifyArchiveReadResourceCompetition();
        VerifyEncodedMediaAccounting();
        std::cout << "in-process optical read model and RPC: PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "in-process optical read model and RPC: FAIL: " << error.what() << '\n';
        return 1;
    }
}
