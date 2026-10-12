#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <unistd.h>

#include "common/rpc/LogicalNodeRoute.h"
#include "scheduler/model/ManagedNode.h"
#include "scheduler/simulation/service/InProcessDiskService.h"

namespace {

void Check(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

uint64_t NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

class TempDirectory {
public:
    TempDirectory() {
        const auto parent = std::filesystem::temp_directory_path();
        std::string pattern = (parent / "zb-inprocess-disk-XXXXXX").string();
        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');
        const char* created = mkdtemp(buffer.data());
        Check(created != nullptr, "create temporary directory");
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

void Register(zb::scheduler::NodePowerManager* manager, const std::string& id) {
    auto profile = zb::scheduler::DefaultStorageNodeProfile(id);
    profile.execution_mode = zb::scheduler::ExecutionMode::kVirtual;
    profile.logical_node_count = 1;
    profile.device_groups[0].device_count = 1;
    profile.device_groups[0].device_capacity_bytes = 100;
    profile.device_groups[0].max_concurrency = 1;
    std::string error;
    const uint64_t now = NowMs();
    Check(manager->RegisterNode(profile, now, 123, &error), error);
    zb::scheduler::NodeInventorySnapshot inventory;
    inventory.node_id = id;
    inventory.generation = 1;
    inventory.observed_at_ms = now;
    zb::scheduler::DeviceInventory disk;
    disk.disk_id = "disk-01";
    disk.capacity_bytes = 100;
    disk.free_bytes = 100;
    disk.last_update_ms = now;
    inventory.devices.push_back(disk);
    Check(manager->ReportHeartbeat(id, "127.0.0.1:0", inventory, now, &error), error);
    zb::storage_model::NodeReadinessStatus readiness;
    readiness.observed = true;
    readiness.initialization_complete = true;
    readiness.inventory_ready = true;
    readiness.metadata_ready = true;
    readiness.observed_at_ms = now;
    Check(manager->ReportReadiness(id, readiness, &error), error);
    Check(manager->ActivateNode(id, now, &error), error);
}

void Verify() {
    TempDirectory temporary;
    zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
    zb::scheduler::InProcessDiskService disks(&manager);
    for (const std::string id : {"sim-a", "sim-b"}) {
        zb::virtual_node::VirtualNodeConfig config;
        config.node_id = id;
        config.disk_ids = {"disk-01"};
        config.archive_meta_dir = (temporary.path() / id).string();
        config.disk_capacity_bytes = 100;
        config.read_base_latency_ms = 0;
        config.write_base_latency_ms = 0;
        config.read_bytes_per_sec = 1000000000;
        config.write_bytes_per_sec = 1000000000;
        if (id == "sim-b") config.wake_latency_ms = 160;
        std::string error;
        Check(disks.AddNode(config, &error), error);
        Register(&manager, id);
    }

    brpc::Server server;
    Check(server.AddService(&disks, brpc::SERVER_DOESNT_OWN_SERVICE) == 0, "add disk service");
    Check(server.Start("127.0.0.1:0", nullptr) == 0, "start disk service");
    const std::string address = butil::endpoint2str(server.listen_address()).c_str();
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    Check(channel.Init(address.c_str(), &options) == 0, "connect disk service");
    zb::rpc::RealNodeService_Stub stub(&channel);

    auto write = [&](const std::string& id, uint64_t offset, const std::string& data,
                     uint64_t generation = 1) {
        brpc::Controller controller;
        Check(zb::rpc_route::SetLogicalNodeRoute(&controller, id, generation), "set route");
        zb::rpc::WriteObjectRequest request;
        request.set_disk_id("disk-01");
        request.set_object_id("same-local-id");
        request.set_offset(offset);
        request.set_data(data);
        zb::rpc::WriteObjectReply reply;
        stub.WriteObject(&controller, &request, &reply, nullptr);
        Check(!controller.Failed(), controller.ErrorText());
        return reply.status().code();
    };
    auto inventory_free = [&](const std::string& id) {
        brpc::Controller controller;
        Check(zb::rpc_route::SetLogicalNodeRoute(&controller, id, 1), "route inventory");
        google::protobuf::Empty request;
        zb::rpc::DiskReportReply reply;
        stub.GetDiskReport(&controller, &request, &reply, nullptr);
        Check(!controller.Failed(), controller.ErrorText());
        Check(reply.status().code() == zb::rpc::STATUS_OK && reply.reports_size() == 1,
              "inventory status");
        return reply.reports(0).free_bytes();
    };

    Check(write("sim-a", 0, "x") == zb::rpc::STATUS_IO_ERROR,
          "restored readiness alone must not open the data endpoint");
    Check(disks.ConfirmCatalog("sim-a") && disks.ConfirmCatalog("sim-b"),
          "confirm both published logical nodes");
    Check(!disks.ReleaseRetiredNode("sim-a") && disks.OwnsNodeIdentity("sim-a"),
          "working logical node cannot be released");

    Check(write("sim-a", 0, std::string(40, 'a')) == zb::rpc::STATUS_OK, "write a");
    Check(write("sim-b", 0, std::string(7, 'b')) == zb::rpc::STATUS_OK, "write b");
    Check(inventory_free("sim-a") == 60 && inventory_free("sim-b") == 93,
          "same local IDs must have independent capacity");
    Check(write("sim-a", 0, "abc") == zb::rpc::STATUS_OK, "short overwrite");
    Check(inventory_free("sim-a") == 60, "short overwrite must not release capacity");
    Check(write("sim-a", 0, std::string(61, 'c')) == zb::rpc::STATUS_OK,
          "replace within capacity");
    Check(inventory_free("sim-a") == 39, "overwrite growth counted once");
    Check(write("sim-a", 0, std::string(101, 'c')) == zb::rpc::STATUS_IO_ERROR,
          "capacity overflow rejected");
    Check(inventory_free("sim-a") == 39, "failed write has no capacity side effect");

    brpc::Controller read_controller;
    Check(zb::rpc_route::SetLogicalNodeRoute(&read_controller, "sim-a", 1), "route read");
    zb::rpc::ReadObjectRequest read;
    read.set_disk_id("disk-01");
    read.set_object_id("same-local-id");
    read.set_size(100);
    zb::rpc::ReadObjectReply read_reply;
    stub.ReadObject(&read_controller, &read, &read_reply, nullptr);
    Check(!read_controller.Failed() && read_reply.status().code() == zb::rpc::STATUS_OK &&
          read_reply.bytes() == 61 && read_reply.data().size() == 61,
          "read must preserve object size and EOF");

    Check(write("sim-a", 0, "x", 2) == zb::rpc::STATUS_INVALID_ARGUMENT,
          "stale generation rejected");
    std::string error;
    Check(manager.RequestPowerLevel("sim-b", zb::scheduler::PowerLevel::kOff,
                                    NowMs(), true, &error), error);
    Check(inventory_free("sim-b") == 93,
          "inventory remains visible while powered off");
    zb::scheduler::ManagedNodeRuntime powered_down;
    Check(manager.GetNode("sim-b", &powered_down) &&
          powered_down.power_level == zb::scheduler::PowerLevel::kOff,
          "inventory must not wake the node");
    const auto wake_started = std::chrono::steady_clock::now();
    auto waking_write = std::async(std::launch::async, [&] {
        return write("sim-b", 0, "z");
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
    const auto other_started = std::chrono::steady_clock::now();
    Check(write("sim-a", 0, "a") == zb::rpc::STATUS_OK,
          "other logical node serves while one node wakes");
    const auto other_elapsed = std::chrono::steady_clock::now() - other_started;
    Check(other_elapsed < std::chrono::milliseconds(100),
          "one node wake must not hold the shared route lock");
    Check(waking_write.get() == zb::rpc::STATUS_OK,
          "data request wakes powered-off node");
    Check(std::chrono::steady_clock::now() - wake_started >=
              std::chrono::milliseconds(140),
          "configured wake delay must precede data completion");
    Check(manager.GetNode("sim-b", &powered_down) &&
          powered_down.power_level >= zb::scheduler::PowerLevel::kMedium,
          "node power state advanced before data service");
    brpc::Controller missing_route;
    zb::rpc::DiskReportReply missing_reply;
    google::protobuf::Empty empty;
    stub.GetDiskReport(&missing_route, &empty, &missing_reply, nullptr);
    Check(!missing_route.Failed() &&
          missing_reply.status().code() == zb::rpc::STATUS_INVALID_ARGUMENT,
          "methods with empty requests also require a route");

    Check(manager.SetAdministrativeState(
              "sim-a", zb::storage_model::AdministrativeState::kDisabled, &error), error);
    Check(write("sim-a", 0, "x") == zb::rpc::STATUS_IO_ERROR,
          "disabled node must reject data requests");
    Check(disks.RevokeCatalog("sim-a") && !disks.ConfirmCatalog("sim-a"),
          "disabled logical node cannot regain the data route from a CMS publish");
    Check(manager.SetAdministrativeState(
              "sim-a", zb::storage_model::AdministrativeState::kEnabled, &error), error);
    Check(write("sim-a", 0, "x") == zb::rpc::STATUS_IO_ERROR,
          "restart cannot serve before a new catalog confirmation");
    Check(disks.ConfirmCatalog("sim-a") &&
          write("sim-a", 0, "x") == zb::rpc::STATUS_OK,
          "fresh confirmation reopens only the restarted logical node");
    Check(write("sim-b", 0, "z") == zb::rpc::STATUS_OK,
          "disabled node must not affect other instances");

    server.Stop(0);
    server.Join();
}

void VerifyInstanceGenerationAndModelTransfer() {
    TempDirectory temporary;
    zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
    zb::scheduler::InProcessDiskService disks(&manager);
    zb::virtual_node::VirtualNodeConfig source_cfg;
    source_cfg.node_id = "transfer-source";
    source_cfg.instance_generation = 7;
    source_cfg.disk_ids = {"disk-01"};
    source_cfg.archive_meta_dir = (temporary.path() / "source").string();
    source_cfg.disk_capacity_bytes = 1000;
    source_cfg.read_base_latency_ms = source_cfg.write_base_latency_ms = 0;
    source_cfg.read_bytes_per_sec = source_cfg.write_bytes_per_sec = 1000000000;
    auto target_cfg = source_cfg;
    target_cfg.node_id = "transfer-target";
    target_cfg.instance_generation = 8;
    target_cfg.archive_meta_dir = (temporary.path() / "target").string();
    std::string error;
    Check(disks.AddNode(source_cfg, &error) && disks.AddNode(target_cfg, &error), error);
    Register(&manager, source_cfg.node_id);
    Register(&manager, target_cfg.node_id);
    Check(disks.ConfirmCatalog(source_cfg.node_id) &&
          disks.ConfirmCatalog(target_cfg.node_id), "confirm transfer models");
    Check(disks.GetInstanceGeneration(source_cfg.node_id) == 7 &&
          disks.GetInstanceGeneration(target_cfg.node_id) == 8,
          "configured generations are stored in active slots");

    brpc::Server server;
    Check(server.AddService(&disks, brpc::SERVER_DOESNT_OWN_SERVICE) == 0,
          "add generation test service");
    Check(server.Start("127.0.0.1:0", nullptr) == 0, "start generation test service");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    Check(channel.Init(butil::endpoint2str(server.listen_address()).c_str(), &options) == 0,
          "connect generation test service");
    zb::rpc::RealNodeService_Stub stub(&channel);
    auto routed_read = [&](uint64_t generation) {
        brpc::Controller cntl;
        Check(zb::rpc_route::SetLogicalNodeRoute(&cntl, source_cfg.node_id, generation),
              "route generation read");
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id("disk-01");
        request.set_object_id("obj-77-0");
        request.set_size(1);
        zb::rpc::ReadObjectReply reply;
        stub.ReadObject(&cntl, &request, &reply, nullptr);
        Check(!cntl.Failed(), cntl.ErrorText());
        return reply.status().code();
    };
    Check(routed_read(1) == zb::rpc::STATUS_INVALID_ARGUMENT,
          "old generation is rejected before object lookup");
    Check(routed_read(7) == zb::rpc::STATUS_NOT_FOUND,
          "active generation reaches the correct model");

    auto* source = disks.GetStorage(source_cfg.node_id);
    auto* target = disks.GetStorage(target_cfg.node_id);
    Check(source && target, "transfer source and target exist");
    zb::msg::WriteObjectRequest write;
    write.disk_id = "disk-01";
    write.object_id = "obj-77-0";
    write.data = "abcdef";
    Check(source->WriteObject(write).status.ok(), "source resident model object");
    zb::msg::CommitFileWriteRequest commit;
    commit.inode_id = 77;
    commit.txid = "transfer-77";
    commit.file_size = 6;
    commit.object_unit_size = 6;
    commit.allow_create = true;
    const auto committed = source->CommitFileWrite(commit);
    Check(committed.status.ok(), "source file metadata committed");
    Check(source->ExportResidentModelObject("disk-01", "obj-77-0", 6).ok(),
          "resident model object is exportable");
    Check(target->ImportModelObject("disk-01", "obj-77-0", 6).ok() &&
          target->ImportModelObject("disk-01", "obj-77-0", 6).ok(),
          "target object import is durable and idempotent");
    zb::msg::FileMeta meta;
    Check(source->ExportModelFile(77, &meta).ok() &&
          target->ImportModelFile(meta).ok() && target->ImportModelFile(meta).ok(),
          "file version is preserved by idempotent import");
    zb::msg::ReadObjectRequest read;
    read.disk_id = "disk-01";
    read.object_id = "obj-77-0";
    read.size = 6;
    Check(target->ReadObject(read).data == source->ReadObject(read).data,
          "model transfer preserves generated read semantics");
    Check(target->VerifyModelArchiveSource(77, "disk-01", 6, meta.version).ok(),
          "complete resident model file is eligible for optical archiving");
    server.Stop(0);
    server.Join();
}

void VerifyEmptyDrainGate() {
    TempDirectory temporary;
    zb::scheduler::NodePowerManager manager;
    zb::scheduler::InProcessDiskService disks(&manager);
    zb::virtual_node::VirtualNodeConfig config;
    config.node_id = "drain-gate";
    config.disk_ids = {"disk-01"};
    config.archive_meta_dir = (temporary.path() / "control").string();
    config.disk_capacity_bytes = 100;
    config.read_bytes_per_sec = 1000000;
    config.write_bytes_per_sec = 100;
    std::string error;
    Check(disks.AddNode(config, &error), error);
    Register(&manager, config.node_id);
    Check(disks.ConfirmCatalog(config.node_id), "confirm drain test catalog");
    brpc::Server server;
    Check(server.AddService(&disks, brpc::SERVER_DOESNT_OWN_SERVICE) == 0,
          "add drain test service");
    Check(server.Start("127.0.0.1:0", nullptr) == 0, "start drain test service");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    const std::string address = butil::endpoint2str(server.listen_address()).c_str();
    Check(channel.Init(address.c_str(), &options) == 0, "connect drain test service");
    zb::rpc::RealNodeService_Stub stub(&channel);
    auto write = [&]() {
        brpc::Controller controller;
        Check(zb::rpc_route::SetLogicalNodeRoute(&controller, "drain-gate", 1),
              "route drain write");
        zb::rpc::WriteObjectRequest request;
        request.set_disk_id("disk-01");
        request.set_object_id("obj-1-0");
        request.set_data(std::string(50, 'd'));
        zb::rpc::WriteObjectReply reply;
        stub.WriteObject(&controller, &request, &reply, nullptr);
        Check(!controller.Failed(), controller.ErrorText());
        return reply.status().code();
    };
    auto active_write = std::async(std::launch::async, write);
    auto* storage = disks.GetStorage("drain-gate");
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (storage->GetIoResourceSnapshot().outstanding == 0 &&
           std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    Check(storage->GetIoResourceSnapshot().outstanding == 1,
          "slow writer entered the data service");
    const auto start = std::chrono::steady_clock::now();
    Check(!disks.BeginEmptyDrain("drain-gate", NowMs(), &error),
          "in-flight data must fail the empty check");
    Check(std::chrono::steady_clock::now() - start >= std::chrono::milliseconds(350),
          "drain waits for admitted writer to finish");
    Check(active_write.get() == zb::rpc::STATUS_OK, "admitted write completes");
    zb::scheduler::ManagedNodeRuntime node;
    manager.GetNode("drain-gate", &node);
    Check(node.lifecycle == zb::scheduler::LifecycleState::kWorking,
          "failed empty drain preserves working state");
    zb::msg::DeleteObjectRequest deletion;
    deletion.disk_id = "disk-01";
    deletion.object_id = "obj-1-0";
    Check(storage->DeleteObject(deletion).status.ok(), "remove test object");
    error.clear();
    Check(disks.BeginEmptyDrain("drain-gate", NowMs(), &error), error);
    manager.GetNode("drain-gate", &node);
    Check(node.lifecycle == zb::scheduler::LifecycleState::kDraining,
          "empty drain entered Draining");
    Check(write() == zb::rpc::STATUS_IO_ERROR,
          "new write cannot cross the drain admission gate");
    server.Stop(0);
    server.Join();
}

void VerifySharedIoBudget() {
    zb::virtual_node::VirtualNodeConfig config;
    config.node_id = "budget-node";
    config.disk_ids = {"disk-01"};
    config.disk_capacity_bytes = 500;
    config.write_bytes_per_sec = 1000;
    config.write_base_latency_ms = 0;
    config.max_concurrent_io = 1;
    config.io_queue_limit = 1;
    zb::virtual_node::VirtualStorageServiceImpl storage(config);
    auto write = [&](const std::string& object_id) {
        zb::msg::WriteObjectRequest request;
        request.disk_id = "disk-01";
        request.object_id = object_id;
        request.data.assign(100, 'a');
        return storage.WriteObject(request).status;
    };
    auto wait_outstanding = [&](uint32_t count) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (std::chrono::steady_clock::now() < deadline) {
            if (storage.GetIoResourceSnapshot().outstanding == count) return;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        Check(false, "model I/O count did not reach " + std::to_string(count));
    };
    const auto start = std::chrono::steady_clock::now();
    auto first = std::async(std::launch::async, [&]() { return write("one"); });
    wait_outstanding(1);
    auto second = std::async(std::launch::async, [&]() { return write("two"); });
    wait_outstanding(2);
    const auto rejected = write("three");
    Check(rejected.code == zb::msg::StatusCode::kIoError &&
          rejected.message == "MODEL_IO_QUEUE_FULL", "bounded queue must reject overload");
    Check(first.get().ok() && second.get().ok(), "admitted model I/O must finish");
    const auto elapsed = std::chrono::steady_clock::now() - start;
    Check(elapsed >= std::chrono::milliseconds(180),
          "shared bandwidth must serialize transfer budget");
    Check(storage.GetIoResourceSnapshot().outstanding == 0,
          "completed requests must release I/O budget");
    const auto report = storage.GetDiskReport();
    Check(report.status.ok() && report.reports.size() == 1 &&
          report.reports[0].free_bytes == 300,
          "rejected I/O must not consume disk capacity");
}

void VerifyResourceConfig() {
    TempDirectory temporary;
    const auto path = temporary.path() / "node.conf";
    {
        std::ofstream output(path);
        output << "NODE_ID=config-node\nDISK_IDS=disk-a,disk-b\n"
               << "MAX_CONCURRENT_IO=2\nIO_QUEUE_LIMIT=3\nJITTER_SEED=1234\n";
    }
    std::string error;
    auto config = zb::virtual_node::VirtualNodeConfig::LoadFromFile(path.string(), &error);
    Check(error.empty() && config.disk_ids.size() == 2 &&
          config.disk_ids[0] == "disk-a" && config.disk_ids[1] == "disk-b" &&
          config.max_concurrent_io == 2 && config.io_queue_limit == 3 &&
          config.jitter_seed == 1234,
          "resource and disk ID config must round-trip");
    {
        std::ofstream output(path);
        output << "NODE_ID=config-node\nMAX_CONCURRENT_IO=4294967297\n";
    }
    error.clear();
    zb::virtual_node::VirtualNodeConfig::LoadFromFile(path.string(), &error);
    Check(!error.empty(), "overflowing concurrency config must fail");
}

void VerifyFilePlanBounds() {
    TempDirectory temporary;
    zb::virtual_node::VirtualNodeConfig config;
    config.node_id = "bounded-plan";
    config.disk_ids = {"disk-01"};
    config.disk_capacity_bytes = 100;
    zb::virtual_node::VirtualStorageServiceImpl storage(config);
    std::string error;
    Check(storage.InitModelStateStore((temporary.path() / "model_state").string(), &error),
          error);

    zb::msg::AllocateFileWriteRequest plan;
    plan.inode_id = 1;
    plan.disk_id = "disk-01";
    plan.object_unit_size_hint = 1;
    plan.size = 4096;
    auto accepted = storage.AllocateFileWrite(plan);
    Check(accepted.status.ok() && accepted.slices.size() == 4096,
          "bounded file plan must preserve exact slice count");
    plan.size = 4097;
    auto excessive = storage.AllocateFileWrite(plan);
    Check(!excessive.status.ok() && excessive.status.message == "MODEL_SLICE_LIMIT" &&
          excessive.slices.empty(), "oversized slice plan must be rejected before allocation");

    plan.size = 1;
    plan.offset = std::numeric_limits<uint32_t>::max();
    auto aliased = storage.AllocateFileWrite(plan);
    Check(!aliased.status.ok() && aliased.slices.empty(),
          "object index must not wrap to another object");
    plan.offset = std::numeric_limits<uint32_t>::max() - 1ULL;
    auto last = storage.AllocateFileWrite(plan);
    Check(last.status.ok() && last.slices.size() == 1 &&
          last.slices[0].object_index == plan.offset,
          "last supported object index must remain distinct");

    plan.object_unit_size_hint = std::numeric_limits<uint64_t>::max();
    plan.offset = std::numeric_limits<uint64_t>::max() - 1;
    auto upper = storage.AllocateFileWrite(plan);
    Check(upper.status.ok() && upper.slices.size() == 1 &&
          upper.slices[0].length == 1 && upper.slices[0].object_offset == plan.offset,
          "object slice end must not overflow at uint64 boundary");

    zb::msg::CommitFileWriteRequest commit;
    commit.inode_id = 1;
    commit.txid = "oversized-commit";
    commit.file_size = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
    commit.object_unit_size = 1;
    commit.allow_create = true;
    Check(!storage.CommitFileWrite(commit).status.ok(),
          "unrepresentable file object count must not be committed");
    zb::msg::ResolveFileReadRequest read;
    read.inode_id = 1;
    read.disk_id = "disk-01";
    read.size = 1;
    Check(!storage.ResolveFileRead(read).status.ok(),
          "rejected commit must not create file metadata");
}

void VerifyDurableModelState() {
    TempDirectory temporary;
    zb::virtual_node::VirtualNodeConfig config;
    config.node_id = "recoverable-disk";
    config.disk_ids = {"disk-01"};
    config.archive_meta_dir = (temporary.path() / "control").string();
    config.disk_capacity_bytes = 100;
    config.read_base_latency_ms = 0;
    config.write_base_latency_ms = 0;
    config.read_bytes_per_sec = 1000000000;
    config.write_bytes_per_sec = 1000000000;
    auto open = [&](zb::scheduler::InProcessDiskService* service) {
        std::string error;
        Check(service->AddNode(config, &error), error);
        auto* storage = service->GetStorage(config.node_id);
        Check(storage != nullptr, "recoverable storage exists");
        return storage;
    };
    {
        zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
        zb::scheduler::InProcessDiskService service(&manager);
        auto* storage = open(&service);
        zb::msg::WriteObjectRequest write;
        write.disk_id = "disk-01";
        write.object_id = "obj-1-0";
        write.data.assign(40, 'x');
        Check(storage->WriteObject(write).status.ok(), "durable object write");
        zb::msg::CommitFileWriteRequest commit;
        commit.inode_id = 1;
        commit.txid = "txn-1";
        commit.file_size = 40;
        commit.object_unit_size = 64;
        commit.allow_create = true;
        auto result = storage->CommitFileWrite(commit);
        Check(result.status.ok() && result.meta.version == 1, "durable file commit");
        commit.file_size = 39;
        auto conflict = storage->CommitFileWrite(commit);
        Check(!conflict.status.ok() && conflict.status.message == "TXID_PARAMETER_MISMATCH",
              "same txid with different size must be rejected before restart");
        Check(storage->GetDiskReport().reports[0].free_bytes == 60,
              "initial capacity after commit");
    }
    {
        zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
        zb::scheduler::InProcessDiskService service(&manager);
        auto* storage = open(&service);
        Check(storage->GetDiskReport().reports[0].free_bytes == 60,
              "recovered used capacity");
        zb::msg::ReadObjectRequest read;
        read.disk_id = "disk-01";
        read.object_id = "obj-1-0";
        read.size = 100;
        auto read_reply = storage->ReadObject(read);
        Check(read_reply.status.ok() && read_reply.bytes == 40,
              "recovered generated object length");
        zb::msg::CommitFileWriteRequest retry;
        retry.inode_id = 1;
        retry.txid = "txn-1";
        retry.file_size = 40;
        retry.object_unit_size = 64;
        retry.allow_create = true;
        auto result = storage->CommitFileWrite(retry);
        Check(result.status.ok() && result.meta.version == 1,
              "recovered transaction idempotency");
        retry.expected_version = 1;
        auto conflict = storage->CommitFileWrite(retry);
        Check(!conflict.status.ok() && conflict.status.message == "TXID_PARAMETER_MISMATCH",
              "same txid with different CAS version must be rejected after restart");
        zb::msg::DeleteFileMetaRequest remove;
        remove.inode_id = 1;
        remove.disk_id = "disk-01";
        remove.purge_objects = true;
        Check(storage->DeleteFileMeta(remove).status.ok(), "atomic file/object delete");
    }
    {
        auto changed = config;
        changed.disk_capacity_bytes = 101;
        zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
        zb::scheduler::InProcessDiskService invalid(&manager);
        std::string error;
        Check(!invalid.AddNode(changed, &error) &&
              error.find("identity") != std::string::npos,
              "capacity drift must fail closed by identity check");
    }
    {
        zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
        zb::scheduler::InProcessDiskService service(&manager);
        auto* storage = open(&service);
        Check(storage->GetDiskReport().reports[0].free_bytes == 100,
              "capacity recovered after deletion");
        zb::msg::ResolveFileReadRequest read;
        read.inode_id = 1;
        read.disk_id = "disk-01";
        read.size = 1;
        Check(!storage->ResolveFileRead(read).status.ok(),
              "deleted file metadata stays deleted");
        zb::msg::ReadObjectRequest object;
        object.disk_id = "disk-01";
        object.object_id = "obj-1-0";
        object.size = 1;
        Check(!storage->ReadObject(object).status.ok(),
              "deleted object stays deleted");
        zb::msg::WriteObjectRequest write;
        write.disk_id = "disk-01";
        write.object_id = "orphan";
        write.data.assign(9, 'x');
        Check(storage->WriteObject(write).status.ok(), "write before reset");
        zb::msg::ResetNodeDataRequest reset;
        reset.confirm_token = "RESET_NODE_DATA";
        reset.purge_objects = true;
        reset.purge_file_meta = true;
        Check(storage->ResetNodeData(reset).status.ok(), "durable model reset");
    }
    {
        zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
        zb::scheduler::InProcessDiskService service(&manager);
        auto* storage = open(&service);
        Check(storage->GetDiskReport().reports[0].free_bytes == 100,
              "reset capacity persisted across restart");
        zb::msg::ReadObjectRequest orphan;
        orphan.disk_id = "disk-01";
        orphan.object_id = "orphan";
        orphan.size = 1;
        Check(!storage->ReadObject(orphan).status.ok(),
              "reset object remains absent after restart");
    }
    const auto legacy = temporary.path() / "legacy";
    std::filesystem::create_directory(legacy);
    { std::ofstream marker(legacy / "legacy.tsv"); marker << "legacy"; }
    config.archive_meta_dir = legacy.string();
    zb::scheduler::NodePowerManager manager(zb::scheduler::PowerPolicy{});
    zb::scheduler::InProcessDiskService service(&manager);
    std::string error;
    Check(!service.AddNode(config, &error), "unversioned control directory must fail closed");
}

} // namespace

int main() {
    try {
        Verify();
        VerifyInstanceGenerationAndModelTransfer();
        VerifyEmptyDrainGate();
        VerifySharedIoBudget();
        VerifyResourceConfig();
        VerifyFilePlanBounds();
        VerifyDurableModelState();
        std::cout << "in-process disk routing and capacity: PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "in-process disk routing and capacity: FAIL: " << error.what() << '\n';
        return 1;
    }
}
