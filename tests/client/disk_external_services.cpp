// External metadata and scheduler only. No RealNodeService or data implementation.
#include <brpc/server.h>
#include <brpc/closure_guard.h>
#include <butil/endpoint.h>
#include "mds.pb.h"
#include "scheduler.pb.h"
#include <unistd.h>
#include <iostream>
#include <map>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <brpc/channel.h>
#include "real_node.pb.h"

namespace rpc = zb::rpc;
struct State {
    std::mutex mu;
    std::condition_variable changed;
    uint64_t hold_inode{0};
    bool update_held{false};
    std::map<uint64_t, rpc::InodeAttr> attrs;
    std::map<uint64_t, uint64_t> handles;
    uint64_t next_handle{1}, opens{0}, closes{0}, updates{0}, heartbeats{0};
    std::string node;
    explicit State(std::string address) : node(std::move(address)) {
        for (uint64_t i = 1; i <= 17; ++i) {
            auto& a = attrs[i]; a.set_inode_id(i); a.set_type(i == 1 ? rpc::INODE_DIR : rpc::INODE_FILE);
            a.set_mode(i == 1 ? 0755 : 0644); a.set_uid(getuid()); a.set_gid(getgid());
            a.set_nlink(i == 1 ? 2 : 1); a.set_object_unit_size(16384); a.set_version(1);
            a.set_storage_tier(rpc::INODE_STORAGE_DISK);
        }
    }
    uint64_t Inode(const std::string& path) {
        if (path == "/") return 1;
        for (uint64_t i = 2; i <= 17; ++i) if (path == "/file" + std::to_string(i-2) + ".bin") return i;
        return 0;
    }
    void Location(uint64_t inode, rpc::FileLocationView* loc) {
        *loc->mutable_attr() = attrs.at(inode);
        auto* disk = loc->mutable_disk_location();
        disk->set_node_id("disk-test"); disk->set_node_address(node); disk->set_disk_id("disk0");
        disk->set_instance_generation(1);
    }
};
#define GUARD brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu)
class Metadata : public rpc::MdsService {
    State& s;
public:
    explicit Metadata(State& state) : s(state) {}
    void Lookup(google::protobuf::RpcController*, const rpc::LookupRequest* q, rpc::LookupReply* r,
                google::protobuf::Closure* done) override {
        GUARD; auto inode = s.Inode(q->path());
        if (!inode) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); else *r->mutable_attr() = s.attrs.at(inode);
    }
    void Getattr(google::protobuf::RpcController*, const rpc::GetattrRequest* q, rpc::GetattrReply* r,
                 google::protobuf::Closure* done) override {
        GUARD; auto it = s.attrs.find(q->inode_id());
        if (it == s.attrs.end()) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); else *r->mutable_attr() = it->second;
    }
    void GetFileLocation(google::protobuf::RpcController*, const rpc::GetFileLocationRequest* q,
                         rpc::GetFileLocationReply* r, google::protobuf::Closure* done) override {
        GUARD; if (q->inode_id() < 2 || !s.attrs.count(q->inode_id())) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
        else s.Location(q->inode_id(), r->mutable_location());
    }
    void Open(google::protobuf::RpcController*, const rpc::OpenRequest* q, rpc::OpenReply* r,
              google::protobuf::Closure* done) override {
        GUARD; auto inode = s.Inode(q->path());
        if (inode < 2) { r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); return; }
        auto handle = s.next_handle++; s.handles[handle] = inode; ++s.opens;
        r->set_handle_id(handle); s.Location(inode, r->mutable_location());
    }
    void Close(google::protobuf::RpcController*, const rpc::CloseRequest* q, rpc::CloseReply* r,
               google::protobuf::Closure* done) override {
        GUARD; if (s.handles.erase(q->handle_id())) ++s.closes; else r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
    }
    void Readdir(google::protobuf::RpcController*, const rpc::ReaddirRequest* q, rpc::ReaddirReply* r,
                 google::protobuf::Closure* done) override {
        GUARD; if (q->path() != "/") { r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); return; }
        for (uint64_t i = 2; i <= 17; ++i) {
            auto* e = r->add_entries(); e->set_inode_id(i); e->set_type(rpc::INODE_FILE);
            e->set_name("file" + std::to_string(i-2) + ".bin");
        }
    }
    void UpdateInodeStat(google::protobuf::RpcController*, const rpc::UpdateInodeStatRequest* q,
                         rpc::UpdateInodeStatReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::unique_lock<std::mutex> lock(s.mu);
        if (s.hold_inode == q->inode_id()) {
            s.update_held = true;
            s.changed.notify_all();
            s.changed.wait(lock, [&] { return s.hold_inode != q->inode_id(); });
        }
        auto it = s.attrs.find(q->inode_id());
        if (it == s.attrs.end()) { r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); return; }
        if (q->version() < it->second.version()) { r->mutable_status()->set_code(rpc::MDS_STALE_EPOCH); return; }
        auto& a = it->second; a.set_size(q->file_size()); a.set_version(q->version());
        a.set_object_unit_size(q->object_unit_size()); a.set_mtime(q->mtime()); ++s.updates;
        *r->mutable_attr() = a;
    }
};
class Scheduler : public rpc::SchedulerService {
    State& s;
public:
    explicit Scheduler(State& state) : s(state) {}
    void GetClusterView(google::protobuf::RpcController*, const rpc::GetClusterViewRequest*,
                        rpc::GetClusterViewReply* r, google::protobuf::Closure* done) override {
        GUARD; r->set_generation(1); auto* n = r->add_nodes(); n->set_node_id("disk-test");
        n->set_address(s.node); n->set_node_type(rpc::NODE_REAL); n->set_health_state(rpc::NODE_HEALTH_HEALTHY);
        n->set_admin_state(rpc::NODE_ADMIN_ENABLED); n->set_weight(1); n->set_virtual_node_count(1);
    }
    void ReportHeartbeat(google::protobuf::RpcController*, const rpc::HeartbeatRequest* q,
                         rpc::HeartbeatReply* r, google::protobuf::Closure* done) override {
        GUARD; ++s.heartbeats; r->set_generation(1); r->set_epoch(1); r->set_assigned_role(rpc::NODE_ROLE_PRIMARY);
        r->set_group_id(q->node_id()); r->set_primary_node_id(q->node_id()); r->set_primary_address(q->address());
    }
};
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    State state(argv[1]); Metadata mds(state); Scheduler scheduler(state); brpc::Server server;
    if (server.AddService(&mds, brpc::SERVER_DOESNT_OWN_SERVICE) ||
        server.AddService(&scheduler, brpc::SERVER_DOESNT_OWN_SERVICE) || server.Start("127.0.0.1:0", nullptr)) return 1;
    std::cout << "READY " << butil::endpoint2str(server.listen_address()).c_str() << std::endl;
    std::string cmd;
    while (std::getline(std::cin, cmd)) {
        if (cmd == "QUIT") break;
        if (cmd.rfind("HOLD ", 0) == 0) {
            std::lock_guard<std::mutex> lock(state.mu);
            state.hold_inode = std::stoull(cmd.substr(5)); state.update_held = false;
            std::cout << "HOLD_OK" << std::endl;
        }
        if (cmd == "WAIT_HELD") {
            std::unique_lock<std::mutex> lock(state.mu);
            const bool ready = state.changed.wait_for(lock, std::chrono::seconds(10),
                                                       [&] { return state.update_held; });
            std::cout << (ready ? "HELD" : "TIMEOUT") << std::endl;
        }
        if (cmd == "RESUME") {
            std::lock_guard<std::mutex> lock(state.mu);
            state.hold_inode = 0; state.changed.notify_all();
            std::cout << "RESUMED" << std::endl;
        }
        if (cmd.rfind("CHECK_LOCK ", 0) == 0) {
            const auto inode = std::stoull(cmd.substr(11));
            brpc::Channel channel; brpc::ChannelOptions options;
            options.timeout_ms = 2000; options.max_retry = 0;
            bool ok = channel.Init(state.node.c_str(), &options) == 0;
            rpc::RealNodeService_Stub stub(&channel);
            brpc::Controller acquire, allocate, write, release;
            rpc::AcquireFileWriteLockRequest a; a.set_inode_id(inode);
            rpc::AcquireFileWriteLockReply ar;
            stub.AcquireFileWriteLock(&acquire, &a, &ar, nullptr);
            rpc::AllocateFileWriteRequest b; b.set_inode_id(inode); b.set_disk_id("disk0"); b.set_size(1);
            rpc::AllocateFileWriteReply br;
            stub.AllocateFileWrite(&allocate, &b, &br, nullptr);
            rpc::WriteObjectRequest w; w.set_object_id("obj-" + std::to_string(inode) + "-0");
            w.set_disk_id("disk0"); w.set_data("X"); w.set_write_lock_token("wrong-token");
            rpc::WriteObjectReply wr;
            stub.WriteObject(&write, &w, &wr, nullptr);
            rpc::ReleaseFileWriteLockRequest r; r.set_inode_id(inode); r.set_token("wrong-token");
            rpc::ReleaseFileWriteLockReply rr;
            stub.ReleaseFileWriteLock(&release, &r, &rr, nullptr);
            ok = ok && !acquire.Failed() && !allocate.Failed() && !write.Failed() && !release.Failed()
                && ar.status().code() == rpc::STATUS_BUSY && br.status().code() == rpc::STATUS_BUSY
                && wr.status().code() == rpc::STATUS_BUSY && rr.status().code() == rpc::STATUS_BUSY;
            std::cout << (ok ? "LOCK_CHECK_OK" : "LOCK_CHECK_FAIL") << std::endl;
        }
        if (cmd == "STATS") {
            std::lock_guard<std::mutex> lock(state.mu);
            std::cout << "STATS " << state.opens << ' ' << state.closes << ' ' << state.handles.size()
                      << ' ' << state.updates << ' ' << state.heartbeats << std::endl;
        }
    }
    { std::lock_guard<std::mutex> lock(state.mu); state.hold_inode = 0; state.changed.notify_all(); }
    server.Stop(0); server.Join();
}
