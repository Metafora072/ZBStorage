#include <brpc/channel.h>
#include <brpc/controller.h>

#include <cstdlib>
#include <iostream>
#include <string>

#include "common/rpc/LogicalNodeRoute.h"
#include "optical_node.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

int main(int argc, char** argv) {
    if (argc != 5) {
        std::cerr << "usage: probe ADDRESS NODE_ID ACTION FREE_BYTES\n";
        return 2;
    }
    const std::string action = argv[3];
    const uint64_t expected_free = std::strtoull(argv[4], nullptr, 10);
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 2000;
    if (channel.Init(argv[1], &options) != 0) return 2;
    if (action == "remove_model" || action == "remove_rejected") {
        zb::rpc::SchedulerService_Stub scheduler(&channel);
        brpc::Controller controller;
        zb::rpc::RemoveManagedNodeRequest request;
        request.set_node_id(argv[2]);
        zb::rpc::ManagedNodeReply reply;
        scheduler.RemoveManagedNode(&controller, &request, &reply, nullptr);
        if (!controller.Failed() &&
            ((action == "remove_model" && reply.status().code() == zb::rpc::SCHED_OK) ||
             (action == "remove_rejected" &&
              reply.status().code() == zb::rpc::SCHED_INVALID_ARGUMENT))) return 0;
        std::cerr << "logical model drain failed: " << controller.ErrorText() << ' '
                  << reply.status().message() << '\n';
        return 1;
    }
    if (action == "model_working") {
        zb::rpc::SchedulerService_Stub scheduler(&channel);
        brpc::Controller controller;
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(argv[2]);
        zb::rpc::ManagedNodeReply reply;
        scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
        if (!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK &&
            reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_WORKING) return 0;
        std::cerr << "logical model is not working: " << controller.ErrorText() << ' '
                  << reply.status().message() << ' ' << reply.node().ShortDebugString() << '\n';
        return 1;
    }
    if (action == "model_retired") {
        zb::rpc::SchedulerService_Stub scheduler(&channel);
        brpc::Controller controller;
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(argv[2]);
        zb::rpc::ManagedNodeReply reply;
        scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
        if (!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK &&
            reply.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_RETIRED) return 0;
        std::cerr << "logical model is not retired: " << controller.ErrorText() << ' '
                  << reply.status().message() << ' ' << reply.node().ShortDebugString() << '\n';
        return 1;
    }
    if (action == "stop_force" || action == "start" ||
        action == "start_rejected" || action == "reboot") {
        zb::rpc::SchedulerService_Stub scheduler(&channel);
        brpc::Controller controller;
        zb::rpc::NodeOperationReply reply;
        if (action == "stop_force") {
            zb::rpc::StopNodeRequest request;
            request.set_node_id(argv[2]);
            request.set_force(true);
            request.set_reason("in-process lifecycle regression");
            scheduler.StopNode(&controller, &request, &reply, nullptr);
        } else if (action == "start" || action == "start_rejected") {
            zb::rpc::StartNodeRequest request;
            request.set_node_id(argv[2]);
            request.set_reason("in-process lifecycle regression");
            scheduler.StartNode(&controller, &request, &reply, nullptr);
        } else {
            zb::rpc::RebootNodeRequest request;
            request.set_node_id(argv[2]);
            request.set_reason("in-process lifecycle regression");
            scheduler.RebootNode(&controller, &request, &reply, nullptr);
        }
        if (action == "start_rejected" && !controller.Failed() &&
            reply.status().code() == zb::rpc::SCHED_INTERNAL_ERROR) return 0;
        if (!controller.Failed() && reply.status().code() == zb::rpc::SCHED_OK &&
            reply.operation().status() == zb::rpc::NODE_OP_SUCCEEDED) return 0;
        std::cerr << "logical node operation failed: " << controller.ErrorText() << ' '
                  << reply.status().message() << ' '
                  << reply.operation().message() << '\n';
        return 1;
    }
    if (action == "optical_status") {
        zb::rpc::SchedulerService_Stub scheduler(&channel);
        brpc::Controller controller;
        zb::rpc::GetManagedNodeRequest request;
        request.set_node_id(argv[2]);
        zb::rpc::ManagedNodeReply reply;
        scheduler.GetManagedNode(&controller, &request, &reply, nullptr);
        if (controller.Failed() || reply.status().code() != zb::rpc::SCHED_OK) {
            std::cerr << "optical status RPC failed: " << controller.ErrorText() << ' '
                      << reply.status().message() << '\n';
            return 1;
        }
        const auto& node = reply.node();
        const auto& optical = node.optical_library();
        const auto& readiness = node.readiness();
        if (node.lifecycle() == zb::rpc::MANAGED_LIFECYCLE_WORKING &&
            node.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY &&
            node.observed_free_bytes() == expected_free &&
            node.observed_capacity_bytes() == expected_free &&
            node.observed_used_bytes() == 0 &&
            node.inventory_devices_size() == 2 &&
            node.inventory_devices(0).kind() == zb::rpc::MANAGED_DEVICE_DISC &&
            node.inventory_devices(1).kind() == zb::rpc::MANAGED_DEVICE_HDD &&
            optical.observed() && optical.total_slots() == 3 &&
            optical.local_disc_count() == 3 && optical.blank_disc_count() == 3 &&
            optical.recorded_disc_count() == 0 && optical.pending_read_tasks() == 0 &&
            optical.busy_drives() == 0 &&
            readiness.observed() && readiness.initialization_complete() &&
            readiness.inventory_ready() && readiness.metadata_ready()) return 0;
        std::cerr << "unexpected optical management state: " << node.ShortDebugString() << '\n';
        return 1;
    }
    if (action == "optical_blocked" || action == "optical_missing") {
        zb::rpc::OpticalNodeService_Stub optical(&channel);
        brpc::Controller controller;
        if (!zb::rpc_route::SetLogicalNodeRoute(&controller, argv[2], 1)) return 2;
        zb::rpc::RequestAsyncReadFileRequest request;
        request.set_inode_id(1);
        request.set_disk_id("disc-1");
        request.set_image_id("image-1");
        zb::rpc::RequestAsyncReadFileReply reply;
        optical.RequestAsyncReadFile(&controller, &request, &reply, nullptr);
        if (!controller.Failed() &&
            ((action == "optical_blocked" &&
              reply.status().code() == zb::rpc::MDS_INTERNAL_ERROR &&
              reply.status().message() == "MODEL_CATALOG_NOT_CONFIRMED") ||
             (action == "optical_missing" &&
              reply.status().code() == zb::rpc::MDS_NOT_FOUND))) return 0;
        std::cerr << "unexpected optical data gate: " << controller.ErrorText() << ' '
                  << reply.status().message() << '\n';
        return 1;
    }
    zb::rpc::RealNodeService_Stub stub(&channel);
    auto route = [&](brpc::Controller* controller) {
        return zb::rpc_route::SetLogicalNodeRoute(controller, argv[2], 1);
    };
    brpc::Controller controller;
    if (!route(&controller)) return 2;
    if (action == "blocked" || action == "report" || action == "missing") {
        google::protobuf::Empty request;
        zb::rpc::DiskReportReply reply;
        stub.GetDiskReport(&controller, &request, &reply, nullptr);
        if (controller.Failed()) {
            std::cerr << controller.ErrorText() << '\n';
            return 1;
        }
        if (action == "missing") {
            if (reply.status().code() == zb::rpc::STATUS_NOT_FOUND) return 0;
        } else if (action == "blocked") {
            if (reply.status().code() == zb::rpc::STATUS_IO_ERROR &&
                reply.status().message() == "MODEL_CATALOG_NOT_CONFIRMED") return 0;
        } else if (reply.status().code() == zb::rpc::STATUS_OK &&
                   reply.reports_size() == 1 &&
                   reply.reports(0).free_bytes() == expected_free) {
            return 0;
        }
        std::cerr << "unexpected inventory status=" << reply.status().code()
                  << " message=" << reply.status().message() << '\n';
        return 1;
    }
    if (action == "write") {
        zb::rpc::WriteObjectRequest request;
        request.set_disk_id("disk-01");
        request.set_object_id("obj-1-0");
        request.set_data(std::string(40, 'x'));
        zb::rpc::WriteObjectReply reply;
        stub.WriteObject(&controller, &request, &reply, nullptr);
        if (!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK &&
            reply.bytes() == 40) return 0;
        std::cerr << "write failed: " << controller.ErrorText() << ' '
                  << reply.status().message() << '\n';
        return 1;
    }
    if (action == "read") {
        zb::rpc::ReadObjectRequest request;
        request.set_disk_id("disk-01");
        request.set_object_id("obj-1-0");
        request.set_size(100);
        zb::rpc::ReadObjectReply reply;
        stub.ReadObject(&controller, &request, &reply, nullptr);
        if (!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK &&
            reply.bytes() == 40 && reply.data() == std::string(40, 'x')) return 0;
        std::cerr << "read failed: " << controller.ErrorText() << ' '
                  << reply.status().message() << '\n';
        return 1;
    }
    if (action == "commit" || action == "retry" || action == "conflict") {
        zb::rpc::CommitFileWriteRequest request;
        request.set_inode_id(1);
        request.set_txid("crash-tx-1");
        request.set_file_size(40);
        if (action == "conflict") request.set_file_size(41);
        request.set_object_unit_size(64);
        request.set_allow_create(true);
        zb::rpc::CommitFileWriteReply reply;
        stub.CommitFileWrite(&controller, &request, &reply, nullptr);
        if (action == "conflict" && !controller.Failed() &&
            reply.status().code() == zb::rpc::STATUS_IO_ERROR &&
            reply.status().message() == "TXID_PARAMETER_MISMATCH") return 0;
        if (!controller.Failed() && reply.status().code() == zb::rpc::STATUS_OK &&
            reply.meta().version() == 1) return 0;
        std::cerr << "commit/retry failed: " << controller.ErrorText() << ' '
                  << reply.status().message() << " version=" << reply.meta().version() << '\n';
        return 1;
    }
    return 2;
}
