#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "common/rpc/LogicalNodeRoute.h"
#include "mds.pb.h"
#include "optical_node.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"
#include "scheduler/api/SchedulerApi.h"

namespace {

void Check(bool condition, const std::string& what) {
    if (!condition) throw std::runtime_error(what);
}

bool Ready(brpc::Controller& controller, const zb::rpc::MdsStatus& status) {
    return !controller.Failed() && status.code() == zb::rpc::MDS_OK;
}

void Route(brpc::Controller* controller, const std::string& node_id,
           uint64_t generation) {
    Check(zb::rpc_route::SetLogicalNodeRoute(controller, node_id, generation),
          "cannot attach logical route");
}

template <typename F>
void Eventually(const std::string& what, F&& attempt, int seconds = 25) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        if (attempt()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("timed out: " + what);
}

void Run(const std::string& mds_address, const std::string& scheduler_address,
         const std::string& data_address, const std::string& disk_a,
         const std::string& disk_b, const std::string& optical_id) {
    brpc::Channel mds_channel, scheduler_channel, data_channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 5000;
    options.max_retry = 0;
    Check(mds_channel.Init(mds_address.c_str(), &options) == 0 &&
          scheduler_channel.Init(scheduler_address.c_str(), &options) == 0 &&
          data_channel.Init(data_address.c_str(), &options) == 0,
          "initialize workflow channels");
    zb::rpc::MdsService_Stub mds(&mds_channel);
    zb::rpc::SchedulerService_Stub scheduler(&scheduler_channel);
    zb::rpc::RealNodeService_Stub disk(&data_channel);
    zb::rpc::OpticalNodeService_Stub optical(&data_channel);

    Eventually("CMS reports both disks and optical working", [&] {
        for (const auto& id : {disk_a, disk_b, optical_id}) {
            brpc::Controller controller;
            zb::rpc::GetManagedNodeRequest request;
            request.set_node_id(id);
            zb::rpc::ManagedNodeReply reply;
            scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
            if (controller.Failed() || reply.status().code() != zb::rpc::SCHED_OK ||
                reply.node().lifecycle() != zb::rpc::MANAGED_LIFECYCLE_WORKING)
                return false;
        }
        zb::rpc::GetCmsNodeCatalogRequest request;
        zb::rpc::GetCmsNodeCatalogReply reply;
        brpc::Controller controller;
        mds.GetCmsNodeCatalog(&controller, &request, &reply, nullptr);
        return Ready(controller, reply.status()) && reply.nodes_size() >= 3;
    });

    {
        zb::rpc::MkdirRequest request;
        request.set_path("/virtual");
        request.set_mode(0755);
        zb::rpc::MkdirReply reply;
        brpc::Controller controller;
        mds.Mkdir(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()) ||
              (!controller.Failed() && reply.status().code() == zb::rpc::MDS_ALREADY_EXISTS),
              "create virtual test directory: " + reply.status().message());
    }
    {
        zb::rpc::SetPathPlacementPolicyRequest request;
        request.mutable_policy()->set_path_prefix("/virtual");
        request.mutable_policy()->set_target(zb::rpc::PATH_PLACEMENT_VIRTUAL_ONLY);
        request.mutable_policy()->set_strict(true);
        zb::rpc::SetPathPlacementPolicyReply reply;
        brpc::Controller controller;
        mds.SetPathPlacementPolicy(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()),
              "set virtual placement: " + reply.status().message());
    }
    zb::rpc::CreateReply created;
    {
        zb::rpc::CreateRequest request;
        request.set_path("/virtual/model-closure");
        request.set_mode(0644);
        request.set_replica(1);
        request.set_object_unit_size(40);
        brpc::Controller controller;
        mds.Create(&controller, &request, &created, nullptr);
        Check(Ready(controller, created.status()),
              "create modeled file: " + created.status().message());
    }
    const uint64_t inode = created.location().attr().inode_id();
    const auto source_location = created.location().disk_location();
    Check(inode != 0 && (source_location.node_id() == disk_a ||
                         source_location.node_id() == disk_b) &&
          source_location.instance_generation() > 1,
          "MDS returned full logical disk location and non-default generation: node=" +
              source_location.node_id() + " generation=" +
              std::to_string(source_location.instance_generation()));
    const std::string source_id = source_location.node_id();
    const std::string target_id = source_id == disk_a ? disk_b : disk_a;
    const std::string object_id = "obj-" + std::to_string(inode) + "-0";

    {
        brpc::Controller stale;
        Route(&stale, source_id, 1);
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id(source_location.disk_id());
        request.set_object_id(object_id);
        request.set_size(1);
        zb::rpc::ReadObjectReply reply;
        disk.ReadObject(&stale, &request, &reply, nullptr);
        Check(!stale.Failed() && reply.status().code() == zb::rpc::STATUS_INVALID_ARGUMENT,
              "stale logical incarnation rejected");
    }
    {
        brpc::Controller controller;
        Route(&controller, source_id, source_location.instance_generation());
        zb::rpc::WriteObjectRequest request;
        request.set_disk_id(source_location.disk_id());
        request.set_object_id(object_id);
        request.set_data(std::string(40, 'x'));
        zb::rpc::WriteObjectReply reply;
        disk.WriteObject(&controller, &request, &reply, nullptr);
        Check(!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK,
              "write modeled source object: " + reply.status().message());
    }
    uint64_t file_version = 0;
    {
        brpc::Controller controller;
        Route(&controller, source_id, source_location.instance_generation());
        zb::rpc::CommitFileWriteRequest request;
        request.set_inode_id(inode);
        request.set_txid("model-closure-" + std::to_string(inode));
        request.set_file_size(40);
        request.set_object_unit_size(40);
        request.set_allow_create(true);
        zb::rpc::CommitFileWriteReply reply;
        disk.CommitFileWrite(&controller, &request, &reply, nullptr);
        Check(!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK,
              "commit modeled file: " + reply.status().message());
        file_version = reply.meta().version();
    }
    {
        zb::rpc::UpdateInodeStatRequest request;
        request.set_inode_id(inode);
        request.set_file_size(40);
        request.set_object_unit_size(40);
        request.set_version(file_version);
        zb::rpc::UpdateInodeStatReply reply;
        brpc::Controller controller;
        mds.UpdateInodeStat(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()),
              "commit MDS file stat: " + reply.status().message());
    }
    {
        zb::rpc::RemoveManagedNodeRequest request;
        request.set_node_id(source_id);
        request.set_finalize(true);
        request.set_force(true);
        zb::rpc::ManagedNodeReply rejected;
        brpc::Controller direct;
        scheduler.RemoveManagedNode(&direct, &request, &rejected, nullptr);
        Check(!direct.Failed() && rejected.status().code() == zb::rpc::SCHED_INVALID_ARGUMENT,
              "manual force-finalize cannot bypass CMS/source verification");
        request.set_finalize(false);
        request.set_force(false);
        zb::rpc::ManagedNodeReply reply;
        brpc::Controller controller;
        scheduler.RemoveManagedNode(&controller, &request, &reply, nullptr);
        Check(!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK,
              "start nonempty model drain: " + reply.status().message());
    }
    zb::rpc::DiskFileLocation migrated;
    Eventually("model-to-model migration and CMS retirement", [&] {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller controller;
        mds.GetFileLocation(&controller, &request, &reply, nullptr);
        if (!Ready(controller, reply.status()) ||
            reply.location().disk_location().node_id() != target_id) return false;
        migrated = reply.location().disk_location();
        zb::rpc::GetManagedNodeRequest node_request;
        node_request.set_node_id(source_id);
        zb::rpc::ManagedNodeReply node_reply;
        brpc::Controller node_controller;
        scheduler.GetManagedNode(&node_controller, &node_request, &node_reply, nullptr);
        return !node_controller.Failed() &&
               node_reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_RETIRED;
    });
    Check(migrated.instance_generation() > 1, "migration preserves target generation");
    {
        zb::scheduler::api::DiskNodeClient api({data_address, target_id, migrated.instance_generation()});
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id(migrated.disk_id());
        request.set_object_id(object_id);
        request.set_size(40);
        zb::rpc::ReadObjectReply reply;
        std::string error;
        Check(api.ReadObject(request, &reply, &error) && reply.data() == std::string(40, 'x'),
              "public disk SDK reads migrated file: " + error);
        zb::rpc::InstallMigratedFileRequest install;
        install.set_node_id(target_id);
        install.set_migration_id("sdk-existing-model-file");
        install.set_disk_id(migrated.disk_id());
        auto* meta = install.mutable_meta();
        meta->set_inode_id(inode);
        meta->set_file_size(40);
        meta->set_object_unit_size(40);
        meta->set_version(file_version);
        zb::rpc::InstallMigratedFileReply installed;
        Check(api.InstallMigratedFile(install, &installed, &error), "public model install RPC: " + error);
        meta->set_file_size(41);
        Check(!api.InstallMigratedFile(install, &installed, &error), "incomplete migrated objects cannot install metadata");
    }
    {
        brpc::Controller controller;
        Route(&controller, target_id, migrated.instance_generation());
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id(migrated.disk_id());
        request.set_object_id(object_id);
        request.set_size(40);
        zb::rpc::ReadObjectReply reply;
        disk.ReadObject(&controller, &request, &reply, nullptr);
        Check(!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK &&
              reply.data() == std::string(40, 'x'),
              "migrated model data remains readable");
    }

    std::string lease_id;
    {
        zb::rpc::ClaimArchiveTaskRequest request;
        request.set_node_id(target_id);
        request.set_disk_id(migrated.disk_id());
        request.set_inode_id(inode);
        request.set_requested_lease_ms(1000);
        zb::rpc::ClaimArchiveTaskReply reply;
        brpc::Controller controller;
        mds.ClaimArchiveTask(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()) && reply.granted(),
              "claim MDS archive lease: " + reply.status().message());
        lease_id = reply.record().lease_id();
    }
    const std::string operation = "model-optical-" + std::to_string(inode);
    zb::rpc::SendArchiveMetadataRequest archive;
    archive.set_batch_id(inode);
    auto* file = archive.add_files();
    file->set_inode_id(inode);
    file->set_size(40);
    file->set_object_unit_size(40);
    file->set_target_node_id(optical_id);
    file->set_operation_id(operation);
    file->set_source_lease_id(lease_id);
    file->set_source_node_id(target_id);
    file->set_source_disk_id(migrated.disk_id());
    file->set_source_version(file_version);
    file->set_disc_id("model-disc-1");
    file->set_image_id("img-1");
    // 第二个文件使用同一卷/盘，验证批量受理与尾批封盘，而非一文件一盘。
    uint64_t extra_inode = 0;
    {
        zb::rpc::CreateRequest create;
        create.set_path("/virtual/model-batch");
        create.set_mode(0644);
        create.set_replica(1);
        create.set_object_unit_size(40);
        zb::rpc::CreateReply created_extra;
        brpc::Controller create_controller;
        mds.Create(&create_controller, &create, &created_extra, nullptr);
        Check(Ready(create_controller, created_extra.status()), "create batch file");
        extra_inode = created_extra.location().attr().inode_id();
        const auto position = created_extra.location().disk_location();
        zb::rpc::WriteObjectRequest write;
        write.set_disk_id(position.disk_id());
        write.set_object_id("obj-" + std::to_string(extra_inode) + "-0");
        write.set_data(std::string(40, 'x'));
        zb::rpc::WriteObjectReply written;
        brpc::Controller writer;
        Route(&writer, position.node_id(), position.instance_generation());
        disk.WriteObject(&writer, &write, &written, nullptr);
        Check(!writer.Failed() && written.status().code() == zb::rpc::STATUS_OK, "write batch file");
        zb::rpc::CommitFileWriteRequest commit;
        commit.set_inode_id(extra_inode);
        commit.set_txid("batch-" + std::to_string(extra_inode));
        commit.set_file_size(40);
        commit.set_object_unit_size(40);
        commit.set_allow_create(true);
        zb::rpc::CommitFileWriteReply committed;
        brpc::Controller committer;
        Route(&committer, position.node_id(), position.instance_generation());
        disk.CommitFileWrite(&committer, &commit, &committed, nullptr);
        Check(!committer.Failed() && committed.status().code() == zb::rpc::STATUS_OK, "commit batch file");
        zb::rpc::UpdateInodeStatRequest update;
        update.set_inode_id(extra_inode);
        update.set_file_size(40);
        update.set_object_unit_size(40);
        update.set_version(committed.meta().version());
        zb::rpc::UpdateInodeStatReply updated;
        brpc::Controller updater;
        mds.UpdateInodeStat(&updater, &update, &updated, nullptr);
        Check(Ready(updater, updated.status()), "update batch inode");
        zb::rpc::ClaimArchiveTaskRequest claim;
        claim.set_node_id(position.node_id());
        claim.set_disk_id(position.disk_id());
        claim.set_inode_id(extra_inode);
        claim.set_requested_lease_ms(1000);
        zb::rpc::ClaimArchiveTaskReply lease;
        brpc::Controller claimer;
        mds.ClaimArchiveTask(&claimer, &claim, &lease, nullptr);
        Check(Ready(claimer, lease.status()) && lease.granted(), "claim batch source lease");
        auto* extra = archive.add_files();
        *extra = archive.files(0);
        extra->set_inode_id(extra_inode);
        extra->set_operation_id("model-optical-" + std::to_string(extra_inode));
        extra->set_source_lease_id(lease.record().lease_id());
        extra->set_source_version(committed.meta().version());
        extra->set_source_node_id(position.node_id());
        extra->set_source_disk_id(position.disk_id());
    }
    {
        zb::rpc::GetCmsNodeCatalogRequest request;
        zb::rpc::GetCmsNodeCatalogReply reply;
        brpc::Controller controller;
        mds.GetCmsNodeCatalog(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()), "read CMS optical generation");
    }
    uint64_t optical_generation = 0;
    {
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(optical_id);
        zb::rpc::ManagedNodeReply reply;
        brpc::Controller controller;
        scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
        Check(!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK,
              "read optical managed generation");
        optical_generation = reply.node().spec().instance_generation();
        Check(optical_generation > 1, "optical has non-default generation");
    }
    {
        brpc::Controller controller;
        Route(&controller, optical_id, optical_generation);
        zb::rpc::ArchiveReply reply;
        optical.SendArchiveMetadata(&controller, &archive, &reply, nullptr);
        Check(Ready(controller, reply.status()),
              "submit formally validated optical archive: " + reply.status().message());
        Check(reply.admissions_size() == 2, "batch reports both file admissions");
    }
    {
        zb::rpc::StopNodeRequest stop;
        stop.set_node_id(optical_id);
        stop.set_force(true);
        zb::rpc::NodeOperationReply stopped;
        brpc::Controller stopping;
        scheduler.StopNode(&stopping, &stop, &stopped, nullptr);
        Check(!stopping.Failed() && stopped.status().code() == zb::rpc::SCHED_OK,
              "stop archive target before source lease expiry");
        std::this_thread::sleep_for(std::chrono::milliseconds(1300));
        zb::rpc::StartNodeRequest start;
        start.set_node_id(optical_id);
        zb::rpc::NodeOperationReply started;
        brpc::Controller starting;
        scheduler.StartNode(&starting, &start, &started, nullptr);
        Check(!starting.Failed() && started.status().code() == zb::rpc::SCHED_OK, "restart expired archive target");
    }
    zb::rpc::OpticalFileLocation optical_location;
    Eventually("modeled burn and atomic MDS optical commit", [&] {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller controller;
        mds.GetFileLocation(&controller, &request, &reply, nullptr);
        if (!Ready(controller, reply.status()) ||
            reply.location().optical_location().node_id() != optical_id ||
            reply.location().attr().file_archive_state() != zb::rpc::INODE_ARCHIVE_ARCHIVED)
            return false;
        optical_location = reply.location().optical_location();
        return true;
    });
    Check(optical_location.instance_generation() == optical_generation &&
          optical_location.disk_id() == "model-disc-1" &&
          optical_location.image_id() == "img-1",
          "MDS returned the exact committed optical model location");
    Eventually("source model cleanup after optical MDS commit", [&] {
        brpc::Controller controller;
        Route(&controller, target_id, migrated.instance_generation());
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id(migrated.disk_id());
        request.set_object_id(object_id);
        request.set_size(1);
        zb::rpc::ReadObjectReply reply;
        disk.ReadObject(&controller, &request, &reply, nullptr);
        return !controller.Failed() && reply.status().code() == zb::rpc::STATUS_NOT_FOUND;
    });
    {
        brpc::Controller controller;
        Route(&controller, optical_id, optical_generation);
        zb::rpc::ArchiveReply reply;
        optical.SendArchiveMetadata(&controller, &archive, &reply, nullptr);
        Check(Ready(controller, reply.status()),
              "completed modeled archive retries are idempotent");
        archive.mutable_files(0)->set_image_id("different-image");
        zb::rpc::ArchiveReply conflicting;
        brpc::Controller conflict_controller;
        Route(&conflict_controller, optical_id, optical_generation);
        optical.SendArchiveMetadata(&conflict_controller, &archive, &conflicting, nullptr);
        Check(!conflict_controller.Failed() &&
              conflicting.status().code() == zb::rpc::MDS_STALE_EPOCH,
              "operation ID reuse with a different image is rejected");
    }
    uint64_t task_id = 0;
    {
        brpc::Controller controller;
        Route(&controller, optical_id, optical_generation);
        zb::rpc::RequestAsyncReadFileRequest request;
        request.set_inode_id(inode);
        request.set_disk_id(optical_location.disk_id());
        request.set_image_id(optical_location.image_id());
        zb::rpc::RequestAsyncReadFileReply reply;
        optical.RequestAsyncReadFile(&controller, &request, &reply, nullptr);
        Check(Ready(controller, reply.status()) && reply.task_id() != 0,
              "open committed optical read task: " + reply.status().message());
        task_id = reply.task_id();
    }
    Eventually("modeled optical read after MDS commit", [&] {
        brpc::Controller controller;
        Route(&controller, optical_id, optical_generation);
        zb::rpc::ReadObjectByTaskIdRequest request;
        request.set_task_id(task_id);
        request.set_offset(0);
        request.set_read_size(40);
        zb::rpc::ReadObjectByTaskIdReply reply;
        optical.ReadObjectByTaskId(&controller, &request, &reply, nullptr);
        return Ready(controller, reply.status()) && reply.data() == std::string(40, 'x');
    });
    std::cout << "INODE=" << inode << '\n';
    std::cout << "EXTRA_INODE=" << extra_inode << '\n';
    {
        zb::rpc::GetManagedNodeRequest get;
        get.set_node_id(optical_id);
        zb::rpc::ManagedNodeReply state;
        brpc::Controller controller;
        scheduler.GetManagedNode(&controller, &get, &state, nullptr);
        Check(!controller.Failed() && state.node().optical_library().recorded_disc_count() == 1,
              "two files packed on one modeled disc");
    }
    std::cout << "model migration, generation, optical MDS commit and read: PASS\n";
}

void VerifyRestored(const std::string& mds_address,
                    const std::string& scheduler_address,
                    const std::string& data_address, uint64_t inode,
                    const std::string& optical_id) {
    brpc::Channel mds_channel, scheduler_channel, data_channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 5000;
    options.max_retry = 0;
    Check(mds_channel.Init(mds_address.c_str(), &options) == 0 &&
          scheduler_channel.Init(scheduler_address.c_str(), &options) == 0 &&
          data_channel.Init(data_address.c_str(), &options) == 0,
          "initialize recovery channels");
    zb::rpc::MdsService_Stub mds(&mds_channel);
    zb::rpc::SchedulerService_Stub scheduler(&scheduler_channel);
    zb::rpc::OpticalNodeService_Stub optical(&data_channel);
    uint64_t generation = 0;
    Eventually("restarted optical model becomes CMS-ready", [&] {
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(optical_id);
        zb::rpc::ManagedNodeReply reply;
        brpc::Controller controller;
        scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
        if (controller.Failed() || reply.status().code() != zb::rpc::SCHED_OK ||
            reply.node().lifecycle() != zb::rpc::MANAGED_LIFECYCLE_WORKING) return false;
        generation = reply.node().spec().instance_generation();
        return generation > 1;
    });
    zb::rpc::OpticalFileLocation restored;
    Eventually("MDS restores full optical location", [&] {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller controller;
        mds.GetFileLocation(&controller, &request, &reply, nullptr);
        if (!Ready(controller, reply.status()) ||
            reply.location().optical_location().node_id() != optical_id ||
            reply.location().optical_location().instance_generation() != generation) return false;
        restored = reply.location().optical_location();
        return true;
    });
    uint64_t task_id = 0;
    std::string last_read_error;
    try { Eventually("restored optical model passes fresh CMS catalog gate", [&] {
        zb::rpc::RequestAsyncReadFileRequest request;
        request.set_inode_id(inode);
        request.set_disk_id(restored.disk_id());
        request.set_image_id(restored.image_id());
        zb::rpc::RequestAsyncReadFileReply reply;
        brpc::Controller controller;
        Route(&controller, optical_id, generation);
        optical.RequestAsyncReadFile(&controller, &request, &reply, nullptr);
        if (!Ready(controller, reply.status()) || reply.task_id() == 0) {
            last_read_error = controller.Failed() ? controller.ErrorText() : reply.ShortDebugString();
            return false;
        }
        task_id = reply.task_id();
        return true;
    }); } catch (...) {
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(optical_id);
        zb::rpc::ManagedNodeReply node;
        brpc::Controller controller;
        scheduler.GetManagedNode(&controller, &request, &node, nullptr);
        std::cerr << "recovery read: " << last_read_error << " node: " << node.ShortDebugString() << '\n';
        throw;
    }
    Eventually("restored optical data remains readable", [&] {
        zb::rpc::ReadObjectByTaskIdRequest request;
        request.set_task_id(task_id);
        request.set_read_size(40);
        zb::rpc::ReadObjectByTaskIdReply reply;
        brpc::Controller controller;
        Route(&controller, optical_id, generation);
        optical.ReadObjectByTaskId(&controller, &request, &reply, nullptr);
        return Ready(controller, reply.status()) && reply.data() == std::string(40, 'x');
    });
    std::cout << "restarted Scheduler/MDS optical location and data: PASS\n";
}

void DrainOptical(const std::string& mds_address, const std::string& scheduler_address,
                  const std::string& data_address, uint64_t inode,
                  const std::string& source, const std::string& target) {
    brpc::Channel channel, mds_channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 5000;
    options.max_retry = 0;
    Check(channel.Init(scheduler_address.c_str(), &options) == 0 &&
          mds_channel.Init(mds_address.c_str(), &options) == 0, "drain channels");
    zb::rpc::SchedulerService_Stub scheduler(&channel);
    zb::rpc::MdsService_Stub mds(&mds_channel);
    zb::rpc::RemoveManagedNodeRequest request;
    request.set_node_id(source);
    zb::rpc::ManagedNodeReply reply;
    brpc::Controller controller;
    scheduler.RemoveManagedNode(&controller, &request, &reply, nullptr);
    Check(!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK,
          "request occupied optical drain: " + reply.status().message());
    try { Eventually("occupied optical transfer, MDS commit and retirement", [&] {
        zb::rpc::GetFileLocationRequest location;
        location.set_inode_id(inode);
        zb::rpc::GetFileLocationReply position;
        brpc::Controller query;
        mds.GetFileLocation(&query, &location, &position, nullptr);
        if (!Ready(query, position.status()) ||
            position.location().optical_location().node_id() != target) return false;
        zb::rpc::GetManagedNodeRequest get;
        get.set_node_id(source);
        zb::rpc::ManagedNodeReply node;
        brpc::Controller state;
        scheduler.GetManagedNode(&state, &get, &node, nullptr);
        return !state.Failed() && node.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_RETIRED &&
            node.node().resident_data_bytes() == 0;
    }); } catch (...) {
        zb::rpc::GetManagedNodeRequest node_request;
        node_request.set_node_id(source);
        zb::rpc::ManagedNodeReply node;
        brpc::Controller node_controller;
        scheduler.GetManagedNode(&node_controller, &node_request, &node, nullptr);
        std::cerr << "optical source state: " << node.ShortDebugString() << '\n';
        zb::rpc::GetDrainMigrationRequest drain_request;
        drain_request.set_node_id(source);
        zb::rpc::GetDrainMigrationReply drain;
        brpc::Controller drain_controller;
        scheduler.GetDrainMigration(&drain_controller, &drain_request, &drain, nullptr);
        std::cerr << "optical drain state: " << drain.ShortDebugString() << '\n';
        throw;
    }
    zb::rpc::VerifyNodeReferencesRequest verify;
    verify.set_node_id(source);
    zb::rpc::VerifyNodeReferencesReply zero;
    brpc::Controller check;
    mds.VerifyNodeReferences(&check, &verify, &zero, nullptr);
    Check(Ready(check, zero.status()) && zero.optical_inode_references() == 0,
          "MDS confirms drained optical zero references");
    VerifyRestored(mds_address, scheduler_address, data_address, inode, target);
    std::cout << "occupied optical logical transfer, source cleanup, CMS zero-reference retirement: PASS\n";
}

void VerifyArchiveInventory(const std::string& address, const std::string& node_id,
                            uint64_t generation, bool modeled, uint64_t expected_inode) {
    zb::scheduler::api::OpticalArchiveClient client(address, node_id, generation, modeled);
    std::vector<zb::rpc::ArchiveImageInfo> images;
    std::string error;
    Check(client.ListAllImages(&images, &error, 1), "MDS SDK list images: " + error);
    bool found = false;
    for (const auto& image : images) {
        std::vector<zb::rpc::ArchiveImageFile> files;
        Check(client.GetAllImageFiles(image.disk_id(), image.image_id(), &files, &error, 1),
              "MDS SDK query image files: " + error);
        for (const auto& file : files) {
            Check(file.inode_id() != 0 && file.has_file_version() == modeled,
                  "explicit file version provenance");
            if (file.inode_id() == expected_inode) found = true;
        }
    }
    Check(expected_inode == 0 || found, "expected archived inode in queried manifest");
    std::vector<zb::rpc::ArchiveTaskInfo> tasks;
    Check(client.ListAllTasks(&tasks, &error, 1), "MDS SDK queries archive tasks: " + error);
    for (const auto& task : tasks) Check(!task.task_id().empty() && !task.phase().empty(), "typed archive task identity");
    zb::rpc::ListArchiveImagesRequest request;
    request.set_node_id(node_id);
    request.set_instance_generation(generation);
    request.set_snapshot_token("invalid-token");
    zb::rpc::ListArchiveImagesReply reply;
    Check(!client.ListArchiveImages(request, &reply, &error) &&
          reply.status().code() == zb::rpc::OPTICAL_CONFLICT, "changed snapshot returns conflict");
    request.clear_snapshot_token();
    request.set_offset(1);
    Check(!client.ListArchiveImages(request, &reply, &error) &&
          reply.status().code() == zb::rpc::OPTICAL_INVALID_ARGUMENT, "unfenced continuation rejected");
    request.set_offset(0);
    request.set_instance_generation(generation + 1);
    zb::scheduler::api::OpticalArchiveClient stale(address, node_id, generation + 1, modeled);
    Check(!stale.ListArchiveImages(request, &reply, &error) &&
          reply.status().code() == zb::rpc::OPTICAL_STALE_GENERATION, "stale CMS generation rejected");
    zb::scheduler::api::OpticalArchiveClient wrong_backend(address, node_id, generation, !modeled);
    Check(!wrong_backend.ListAllImages(&images, &error) && images.empty(),
          "backend mismatch cannot return consumable manifest");
    std::cout << "MDS archive inventory RPC/SDK, paging, identity, backend and snapshot checks: PASS\n";
}

void VerifyModelFaults(const std::string& mds_address, const std::string& data_address,
    const std::string& disk_id, uint64_t disk_generation, const std::string& optical_id,
    uint64_t optical_generation, uint64_t inode, const std::string& expected) {
    zb::scheduler::api::DiskNodeClient disk({data_address, disk_id, disk_generation});
    std::string error;
    google::protobuf::Empty empty;
    zb::rpc::DiskReportReply before, after;
    Eventually("fault test disk becomes CMS-ready", [&] { return disk.GetDiskReport(empty, &before, &error); });
    zb::rpc::WriteObjectRequest write;
    write.set_disk_id("disk-01");
    write.set_object_id("fault-must-not-persist");
    write.set_data("generated-test-bytes");
    zb::rpc::WriteObjectReply written;
    auto start = std::chrono::steady_clock::now();
    Check(!disk.WriteObject(write, &written, &error) && error.find(expected) != std::string::npos,
          "disk request-level injected failure/timeout: " + error);
    if (expected.find("TIMEOUT") != std::string::npos)
        Check(std::chrono::steady_clock::now() - start >= std::chrono::milliseconds(45), "disk timeout delay respected");
    Check(disk.GetDiskReport(empty, &after, &error) && before.SerializeAsString() == after.SerializeAsString(),
          "failed model write cannot consume capacity");
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 5000;
    options.max_retry = 0;
    Check(channel.Init(mds_address.c_str(), &options) == 0, "fault test MDS channel");
    zb::rpc::MdsService_Stub mds(&channel);
    zb::rpc::GetFileLocationRequest request;
    request.set_inode_id(inode);
    zb::rpc::GetFileLocationReply location;
    brpc::Controller lookup;
    mds.GetFileLocation(&lookup, &request, &location, nullptr);
    Check(Ready(lookup, location.status()), "fault test authoritative optical position");
    zb::scheduler::api::OpticalNodeClient optical({data_address, optical_id, optical_generation});
    zb::rpc::RequestAsyncReadFileRequest read;
    read.set_inode_id(inode);
    read.set_disk_id(location.location().optical_location().disk_id());
    read.set_image_id(location.location().optical_location().image_id());
    zb::rpc::RequestAsyncReadFileReply received;
    start = std::chrono::steady_clock::now();
    Eventually("fault test optical passes catalog gate", [&] {
        return !optical.RequestAsyncReadFile(read, &received, &error) && error.find(expected) != std::string::npos;
    });
    if (expected.find("TIMEOUT") != std::string::npos)
        Check(std::chrono::steady_clock::now() - start >= std::chrono::milliseconds(45), "optical timeout delay respected");
    std::cout << "disk/optical request failure and timeout, source/capacity preservation: PASS\n";
}

void VerifyHotModel(const std::string& data, const std::string& node, uint64_t generation,
                    bool create, const std::string& control) {
    zb::scheduler::api::DiskNodeClient disk({data, node, generation});
    std::string error;
    google::protobuf::Empty empty;
    zb::rpc::DiskReportReply inventory;
    Eventually("hot-added model has CMS data admission", [&] { return disk.GetDiskReport(empty, &inventory, &error); });
    if (create) {
        zb::rpc::WriteObjectRequest write;
        write.set_disk_id("disk-01");
        write.set_object_id("hot-api-persisted");
        write.set_data("abc");
        zb::rpc::WriteObjectReply reply;
        Check(disk.WriteObject(write, &reply, &error), "write hot-added model via public SDK: " + error);
    }
    zb::rpc::ReadObjectRequest read;
    read.set_disk_id("disk-01");
    read.set_object_id("hot-api-persisted");
    read.set_size(3);
    zb::rpc::ReadObjectReply read_reply;
    Check(disk.ReadObject(read, &read_reply, &error) && read_reply.data() == "xxx", "hot model data recovery: " + error);
    if (create) {
        std::atomic<bool> go{false};
        std::atomic<unsigned> ready{0}, success{0}, unexpected{0};
        std::vector<std::thread> workers;
        for (unsigned i = 0; i < 6; ++i) workers.emplace_back([&] {
            ++ready;
            while (!go.load()) std::this_thread::yield();
            zb::rpc::ReadObjectReply reply;
            std::string detail;
            if (disk.ReadObject(read, &reply, &detail) && reply.data() == "xxx") ++success;
            else if (detail.find("MODEL_IO_QUEUE_FULL") == std::string::npos) ++unexpected;
        });
        while (ready.load() != 6) std::this_thread::yield();
        go.store(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        zb::scheduler::api::SchedulerClient scheduler(control);
        zb::rpc::GetManagedNodeRequest get;
        get.set_node_id(node);
        zb::rpc::ManagedNodeReply view;
        const auto start = std::chrono::steady_clock::now();
        const bool responsive = scheduler.GetManagedNode(get, &view, &error);
        const auto latency = std::chrono::steady_clock::now() - start;
        for (auto& worker : workers) worker.join();
        Check(responsive && latency < std::chrono::seconds(1) && success.load() > 0 && unexpected.load() == 0,
              "bounded concurrent data IO must not block management RPC");
    }
    if (!create) {
        zb::rpc::DeleteObjectRequest remove;
        remove.set_disk_id("disk-01");
        remove.set_object_id("hot-api-persisted");
        zb::rpc::DeleteObjectReply removed;
        Check(disk.DeleteObject(remove, &removed, &error), "cleanup hot model: " + error);
        zb::rpc::BeginNodeDrainRequest begin;
        begin.set_node_id(node);
        zb::rpc::BeginNodeDrainReply closed;
        Check(disk.BeginNodeDrain(begin, &closed, &error) && closed.write_admission_closed(), "public model drain RPC: " + error);
    }
    std::cout << "public API hot-add, routed IO, persistent recovery and drain: PASS\n";
}

} // namespace

int main(int argc, char** argv) {
    if (argc == 7 && std::string(argv[1]) == "--hot-model-check") {
        try { VerifyHotModel(argv[2], argv[3], std::stoull(argv[4]), std::string(argv[5]) == "write", argv[6]); return 0; }
        catch (const std::exception& e) { std::cerr << "hot model: FAIL: " << e.what() << '\n'; return 1; }
    }
    if (argc == 10 && std::string(argv[1]) == "--model-faults") {
        try {
            VerifyModelFaults(argv[2], argv[3], argv[4], std::stoull(argv[5]), argv[6],
                              std::stoull(argv[7]), std::stoull(argv[8]), argv[9]);
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "model fault closure: FAIL: " << e.what() << '\n';
            return 1;
        }
    }
    // 该模式复用正式 C++ 客户端，在真实/模型服务上验证 MDS 对接契约。
    if (argc == 7 && std::string(argv[1]) == "--archive-inventory") {
        try {
            VerifyArchiveInventory(argv[2], argv[3], std::stoull(argv[4]),
                                   std::string(argv[5]) == "model", std::stoull(argv[6]));
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "archive inventory: FAIL: " << e.what() << '\n';
            return 1;
        }
    }
    if (argc == 8 && std::string(argv[1]) == "--drain-optical") {
        try {
            DrainOptical(argv[2], argv[3], argv[4], std::stoull(argv[5]), argv[6], argv[7]);
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "optical drain: FAIL: " << e.what() << '\n';
            return 1;
        }
    }
    if (argc == 7 && std::string(argv[1]) == "--verify-restored") {
        try {
            VerifyRestored(argv[2], argv[3], argv[4], std::stoull(argv[5]), argv[6]);
            return 0;
        } catch (const std::exception& e) {
            std::cerr << "model recovery: FAIL: " << e.what() << '\n';
            return 1;
        }
    }
    if (argc != 7) {
        std::cerr << "usage: probe MDS SCHEDULER DATA DISK_A DISK_B OPTICAL\n";
        return 2;
    }
    try {
        Run(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6]);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "model closure: FAIL: " << e.what() << '\n';
        return 1;
    }
}
