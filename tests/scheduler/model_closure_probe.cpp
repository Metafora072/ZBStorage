#include <brpc/channel.h>
#include <brpc/controller.h>

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "common/rpc/LogicalNodeRoute.h"
#include "mds.pb.h"
#include "optical_node.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

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
        request.set_requested_lease_ms(300000);
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
    Eventually("restored optical model passes fresh CMS catalog gate", [&] {
        zb::rpc::RequestAsyncReadFileRequest request;
        request.set_inode_id(inode);
        request.set_disk_id(restored.disk_id());
        request.set_image_id(restored.image_id());
        zb::rpc::RequestAsyncReadFileReply reply;
        brpc::Controller controller;
        Route(&controller, optical_id, generation);
        optical.RequestAsyncReadFile(&controller, &request, &reply, nullptr);
        if (!Ready(controller, reply.status()) || reply.task_id() == 0) return false;
        task_id = reply.task_id();
        return true;
    });
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

} // namespace

int main(int argc, char** argv) {
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
