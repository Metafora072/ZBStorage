// Tiny functional peer: real optical engine and RPC, fixture source/scheduler.
#include "data_node/optical_node/service/BrpcOpticalNodeService.h"
#include "scheduler.pb.h"
#include "real_node.pb.h"
#include <brpc/server.h>
#include <brpc/closure_guard.h>
#include <butil/endpoint.h>
#include <stdexcept>
#include <iostream>
#include <filesystem>
#include <fstream>
using namespace zb;
class Source final : public rpc::RealNodeService {
public:
    uint64_t nonempty = 0;
    void ResolveFileRead(google::protobuf::RpcController*, const rpc::ResolveFileReadRequest* r,
        rpc::ResolveFileReadReply* out, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); out->mutable_status()->set_code(rpc::STATUS_OK);
        auto* meta = out->mutable_meta(); meta->set_inode_id(r->inode_id());
        const uint64_t size = r->inode_id() == nonempty ? 12345 : 0;
        meta->set_file_size(size); meta->set_object_unit_size(4194304); meta->set_version(1);
        if (size) { auto* s = out->add_slices(); s->set_object_id("fixture");
            s->set_disk_id("disk-01"); s->set_length(size); }
    }
    void ReadObject(google::protobuf::RpcController*, const rpc::ReadObjectRequest* r,
        rpc::ReadObjectReply* out, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); out->mutable_status()->set_code(rpc::STATUS_OK);
        out->set_bytes(r->size()); out->set_data(std::string(r->size(), 'x'));
    }
};
class Scheduler final : public rpc::SchedulerService {
public:
    std::string address;
    void GetClusterView(google::protobuf::RpcController*, const rpc::GetClusterViewRequest*,
        rpc::GetClusterViewReply* out, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); out->mutable_status()->set_code(rpc::SCHED_OK);
        auto* n = out->add_nodes(); n->set_node_id("node-real-01"); n->set_address(address);
        n->set_node_type(rpc::NODE_REAL); n->set_health_state(rpc::NODE_HEALTH_HEALTHY);
        n->set_admin_state(rpc::NODE_ADMIN_ENABLED);
    }
};
static void Start(brpc::Server& s, google::protobuf::Service* service, int port) {
    if (s.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0 || s.Start(port, nullptr) != 0)
        throw std::runtime_error("start peer failed");
}
int main(int argc, char** argv) try {
    if (argc != 5) throw std::runtime_error("port mds_address work_dir nonempty_inode");
    // Regression: overflow must not pack two images with the same allocated ID.
    const auto packing = std::filesystem::path(argv[3]) / "packing_fixture";
    std::filesystem::create_directories(packing / "temp");
    for (int n : {12, 180}) {
        std::ofstream file(packing / std::to_string(n), std::ios::binary);
        for (int i = 0; i < n; ++i) file.put(static_cast<char>(i * 73));
    }
    volumemanager::VolumeManager vm(200, 0.9);
    vm.SetBasePath(packing.string()); vm.SetTempDir((packing / "temp").string());
    if (vm.AddFileToCollect("12", 11, 100) != volumemanager::ErrorCode::SUCCESS ||
        vm.AddFileToCollect("180", 12, 100) != volumemanager::ErrorCode::SUCCESS ||
        vm.pending_files_.size() != 1 || vm.pending_files_[0].inode_id != 12)
        throw std::runtime_error("overflow must leave second file pending for a fresh image ID");
    std::string tail;
    if (vm.PackVolume(tail, 101) != volumemanager::ErrorCode::SUCCESS ||
        !std::filesystem::exists(packing / "temp/volume_100.vimg") ||
        !std::filesystem::exists(packing / "temp/volume_101.vimg"))
        throw std::runtime_error("overflow images must both survive");
    Source source; source.nonempty = std::stoull(argv[4]); brpc::Server data;
    Start(data, &source, 0);
    Scheduler scheduler; scheduler.address = "127.0.0.1:" + std::to_string(data.listen_address().port);
    brpc::Server sched; Start(sched, &scheduler, 0);
    optical_node::OpticalNodeConfig cfg;
    cfg.node_id = "optical-simple"; cfg.node_address = "127.0.0.1:" + std::string(argv[1]);
    cfg.simple_archive_mds_enabled = true; cfg.mds_address = argv[2]; cfg.archive_root = argv[3];
    cfg.scheduler_addr = "127.0.0.1:" + std::to_string(sched.listen_address().port);
    // Both files are far below the packing threshold: only the end marker can finish them.
    cfg.volume_size_bytes = 65536; cfg.disc_capacity_bytes = 1048576;
    cfg.standard_images_per_disc = 1; cfg.capacity_in_images = 64; cfg.max_write_images = 32;
    cfg.total_disc_count = 4; cfg.slot_count = 4; cfg.available_id_count = 1;
    cfg.drive_count = 1; cfg.arm_count = 1; cfg.burn_bandwidth_mbps = 10000;
    cfg.burn_fixed_seconds = 0.001; cfg.drive_load_seconds = 0.001;
    cfg.drive_eject_seconds = 0.001; cfg.grasp_seconds = 0.001;
    cfg.arm_speed_x = cfg.arm_speed_y = cfg.arm_speed_z = 10000;
    optical_node::OpticalStorageServiceImpl service(cfg);
    service.ConfigureReplication(cfg.node_id, "simple", false, true, "", "", 2000);
    optical_node::BrpcOpticalNodeService rpc_service(&service);
    brpc::Server optical; Start(optical, &rpc_service, std::stoi(argv[1]));
    optical.RunUntilAskedToQuit(); service.Shutdown();
    sched.Stop(0); sched.Join(); data.Stop(0); data.Join(); return 0;
} catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
