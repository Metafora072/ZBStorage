// External dependencies for the unmodified FUSE client and optical_node_server.
// No optical read service or production implementation is linked into this process.
#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <mds.pb.h>
#include <optical_node.pb.h>
#include <real_node.pb.h>
#include <scheduler.pb.h>

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace rpc = zb::rpc;
namespace {
constexpr uint64_t kUnit = 256 * 1024;
struct File {
    uint64_t inode{}, size{}, disc{}, image{};
    std::string path, source;
};
struct State {
    std::mutex mu;
    std::map<uint64_t, File> files;
    std::map<std::string, uint64_t> paths;
    std::map<uint64_t, uint64_t> handles;
    std::map<std::string, uint64_t> calls;
    uint64_t next_handle{1};
    std::string address, optical;

    void Load(const std::string& manifest) {
        std::ifstream in(manifest);
        if (!in) throw std::runtime_error("cannot open corpus manifest");
        std::string line;
        while (std::getline(in, line)) {
            std::istringstream row(line);
            File f;
            if (!(row >> f.inode >> f.size >> f.path) || row.get() != '\t' ||
                !std::getline(row, f.source) || f.source.empty() || f.inode < 2 ||
                f.path.size() < 2 || f.path.front() != '/' ||
                f.path.find('/', 1) != std::string::npos || files.count(f.inode) || paths.count(f.path))
                throw std::runtime_error("invalid/duplicate corpus row: " + line);
            std::ifstream source(f.source, std::ios::binary | std::ios::ate);
            if (!source || source.tellg() != static_cast<std::streamoff>(f.size))
                throw std::runtime_error("source size mismatch: " + f.source);
            paths[f.path] = f.inode;
            files[f.inode] = f;
        }
        if (files.empty()) throw std::runtime_error("empty corpus");
    }
    const File* Published(uint64_t inode) const {
        auto it = files.find(inode);
        return it != files.end() && it->second.disc ? &it->second : nullptr;
    }
    uint64_t Inode(const std::string& path) const {
        if (path == "/") return 1;
        auto it = paths.find(path);
        return it == paths.end() ? 0 : it->second;
    }
    bool Attr(uint64_t inode, rpc::InodeAttr* attr) const {
        const auto* file = Published(inode);
        if (inode != 1 && !file) return false;
        attr->set_inode_id(inode);
        attr->set_type(inode == 1 ? rpc::INODE_DIR : rpc::INODE_FILE);
        attr->set_mode(inode == 1 ? 0755 : 0444);
        attr->set_uid(getuid()); attr->set_gid(getgid());
        attr->set_nlink(inode == 1 ? 2 : 1);
        attr->set_size(file ? file->size : 0);
        attr->set_version(7);
        attr->set_object_unit_size(kUnit);
        if (file) {
            attr->set_storage_tier(rpc::INODE_STORAGE_OPTICAL);
            attr->set_file_archive_state(rpc::INODE_ARCHIVE_ARCHIVED);
            attr->set_file_name(file->path.substr(1));
        }
        return true;
    }
    bool Location(uint64_t inode, rpc::FileLocationView* view) const {
        const auto* f = Published(inode);
        if (!f) return false;
        Attr(inode, view->mutable_attr());
        auto* loc = view->mutable_optical_location();
        loc->set_node_id("optical-test"); loc->set_node_address(optical);
        loc->set_disk_id("optical-disk-" + std::to_string(f->disc));
        loc->set_image_id("img-" + std::to_string(f->image));
        loc->set_instance_generation(1);
        return true;
    }
};

class Metadata final : public rpc::MdsService {
    State& s;
public:
    explicit Metadata(State& state) : s(state) {}
    void Lookup(google::protobuf::RpcController*, const rpc::LookupRequest* q,
                rpc::LookupReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["lookup"];
        if (!s.Attr(s.Inode(q->path()), r->mutable_attr())) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
    }
    void Getattr(google::protobuf::RpcController*, const rpc::GetattrRequest* q,
                 rpc::GetattrReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["getattr"];
        if (!s.Attr(q->inode_id(), r->mutable_attr())) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
    }
    void GetFileLocation(google::protobuf::RpcController*, const rpc::GetFileLocationRequest* q,
                        rpc::GetFileLocationReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["location"];
        if (!s.Location(q->inode_id(), r->mutable_location())) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
    }
    void Open(google::protobuf::RpcController*, const rpc::OpenRequest* q,
              rpc::OpenReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["open"];
        if ((q->flags() & O_ACCMODE) != O_RDONLY) {
            r->mutable_status()->set_code(rpc::MDS_INVALID_ARGUMENT); return;
        }
        auto inode = s.Inode(q->path());
        if (!s.Location(inode, r->mutable_location())) {
            r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); return;
        }
        uint64_t handle = s.next_handle++;
        s.handles[handle] = inode;
        r->set_handle_id(handle);
        ++s.calls["opened"];
    }
    void Close(google::protobuf::RpcController*, const rpc::CloseRequest* q,
               rpc::CloseReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["close"];
        if (!s.handles.erase(q->handle_id())) r->mutable_status()->set_code(rpc::MDS_NOT_FOUND);
    }
    void Readdir(google::protobuf::RpcController*, const rpc::ReaddirRequest* q,
                 rpc::ReaddirReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["readdir"];
        if (q->path() != "/") { r->mutable_status()->set_code(rpc::MDS_NOT_FOUND); return; }
        for (const auto& item : s.files) if (item.second.disc) {
            auto* e = r->add_entries(); e->set_inode_id(item.first);
            e->set_name(item.second.path.substr(1)); e->set_type(rpc::INODE_FILE);
        }
    }
};

class Scheduler final : public rpc::SchedulerService {
    State& s;
public:
    explicit Scheduler(State& state) : s(state) {}
    void GetClusterView(google::protobuf::RpcController*, const rpc::GetClusterViewRequest*,
                        rpc::GetClusterViewReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["cluster_view"]; r->set_generation(1);
        for (bool optical : {false, true}) {
            auto* n = r->add_nodes();
            n->set_node_id(optical ? "optical-test" : "real-test");
            n->set_address(optical ? s.optical : s.address);
            n->set_node_type(optical ? rpc::NODE_OPTICAL : rpc::NODE_REAL);
            n->set_health_state(rpc::NODE_HEALTH_HEALTHY);
            n->set_admin_state(rpc::NODE_ADMIN_ENABLED);
            n->set_weight(1); n->set_virtual_node_count(1);
        }
    }
    void ReportHeartbeat(google::protobuf::RpcController*, const rpc::HeartbeatRequest* q,
                         rpc::HeartbeatReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["heartbeat"]; r->set_generation(1); r->set_epoch(1);
        r->set_assigned_role(rpc::NODE_ROLE_PRIMARY);
        r->set_group_id(q->node_id()); r->set_primary_node_id(q->node_id());
        r->set_primary_address(q->address());
    }
};

class Disk final : public rpc::RealNodeService {
    State& s;
public:
    explicit Disk(State& state) : s(state) {}
    void ResolveFileRead(google::protobuf::RpcController*, const rpc::ResolveFileReadRequest* q,
                         rpc::ResolveFileReadReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); std::lock_guard<std::mutex> lock(s.mu);
        ++s.calls["resolve"];
        auto it = s.files.find(q->inode_id());
        if (it == s.files.end() || q->disk_id() != "disk0") {
            r->mutable_status()->set_code(rpc::STATUS_NOT_FOUND); return;
        }
        const auto& f = it->second;
        if (q->offset() != 0 || q->size() != f.size) {
            r->mutable_status()->set_code(rpc::STATUS_INVALID_ARGUMENT); return;
        }
        auto* meta = r->mutable_meta(); meta->set_inode_id(f.inode);
        meta->set_file_size(f.size); meta->set_object_unit_size(kUnit); meta->set_version(7);
        for (uint64_t offset = 0, index = 0; offset < f.size; offset += kUnit, ++index) {
            auto* slice = r->add_slices(); slice->set_object_index(index);
            slice->set_object_id(std::to_string(f.inode) + ":" + std::to_string(index));
            slice->set_disk_id("disk0"); slice->set_length(std::min(kUnit, f.size - offset));
        }
    }
    void ReadObject(google::protobuf::RpcController*, const rpc::ReadObjectRequest* q,
                    rpc::ReadObjectReply* r, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        File file; uint64_t index = 0, inode = 0; char separator = 0;
        std::istringstream object(q->object_id());
        if (!(object >> inode >> separator >> index) || separator != ':' || !object.eof()) {
            r->mutable_status()->set_code(rpc::STATUS_INVALID_ARGUMENT); return;
        }
        {
            std::lock_guard<std::mutex> lock(s.mu); ++s.calls["read_object"];
            auto it = s.files.find(inode);
            if (it == s.files.end() || q->disk_id() != "disk0") {
                r->mutable_status()->set_code(rpc::STATUS_NOT_FOUND); return;
            }
            file = it->second;
        }
        if (!file.size || index > (file.size - 1) / kUnit) {
            r->mutable_status()->set_code(rpc::STATUS_INVALID_ARGUMENT); return;
        }
        uint64_t length = std::min(kUnit, file.size - index * kUnit);
        if (q->offset() > length || q->size() > length - q->offset()) {
            r->mutable_status()->set_code(rpc::STATUS_INVALID_ARGUMENT); return;
        }
        std::ifstream in(file.source, std::ios::binary);
        in.seekg(index * kUnit + q->offset());
        std::string data(q->size(), '\0');
        if (!in.read(data.data(), data.size())) {
            r->mutable_status()->set_code(rpc::STATUS_IO_ERROR); return;
        }
        r->set_bytes(data.size()); r->set_data(std::move(data));
    }
};

void Archive(State& s) {
    brpc::Channel channel; brpc::ChannelOptions options;
    options.timeout_ms = 10000; options.max_retry = 0;
    if (channel.Init(s.optical.c_str(), &options)) throw std::runtime_error("channel init failed");
    rpc::OpticalNodeService_Stub stub(&channel);
    rpc::SendArchiveMetadataRequest q; q.set_batch_id(1);
    {
        std::lock_guard<std::mutex> lock(s.mu);
        if (s.calls["archive"]++) throw std::runtime_error("archive already submitted");
        for (const auto& item : s.files) {
            auto* f = q.add_files(); f->set_inode_id(item.first); f->set_size(item.second.size);
            f->set_object_unit_size(kUnit); f->set_target_node_id("real-test"); f->set_target_disk_id("disk0");
        }
    }
    rpc::ArchiveReply r; brpc::Controller cntl;
    stub.SendArchiveMetadata(&cntl, &q, &r, nullptr);
    if (cntl.Failed() || r.status().code() != rpc::MDS_OK)
        throw std::runtime_error("archive: " + cntl.ErrorText() + " " + r.status().message());
}
} // namespace

int main(int argc, char** argv) {
    if (argc < 3 || argc > 4) {
        std::cerr << "Usage: optical_external_services CORPUS.tsv OPTICAL_ADDRESS [LISTEN_ADDRESS]\n";
        return 2;
    }
    try {
        State state; state.Load(argv[1]); state.optical = argv[2];
        Metadata mds(state); Scheduler scheduler(state); Disk disk(state);
        brpc::Server server;
        for (google::protobuf::Service* service : std::vector<google::protobuf::Service*>{&mds, &scheduler, &disk})
            if (server.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE)) throw std::runtime_error("AddService failed");
        if (server.Start(argc == 4 ? argv[3] : "127.0.0.1:0", nullptr)) throw std::runtime_error("Start failed");
        {
            std::lock_guard<std::mutex> lock(state.mu);
            state.address = butil::endpoint2str(server.listen_address()).c_str();
        }
        std::cout << "READY " << state.address << std::endl;
        // stdin/stdout are test-harness control, never part of a production protocol.
        std::string line;
        while (std::getline(std::cin, line)) {
            std::istringstream command(line); std::string op; command >> op;
            try {
                if (op == "ARCHIVE") { Archive(state); std::cout << "OK ARCHIVE" << std::endl; }
                else if (op == "PUBLISH") {
                    uint64_t inode, disc, image;
                    if (!(command >> inode >> disc >> image) || !disc || !image)
                        throw std::runtime_error("PUBLISH inode disc image required");
                    std::lock_guard<std::mutex> lock(state.mu);
                    auto& f = state.files.at(inode);
                    if (f.disc && (f.disc != disc || f.image != image)) throw std::runtime_error("location changed");
                    f.disc = disc; f.image = image;
                    std::cout << "OK PUBLISH" << std::endl;
                } else if (op == "STATS") {
                    std::lock_guard<std::mutex> lock(state.mu);
                    std::cout << "STATS {\"live_handles\":" << state.handles.size();
                    for (const auto& c : state.calls) std::cout << ",\"" << c.first << "\":" << c.second;
                    std::cout << "}" << std::endl;
                } else if (op == "QUIT") { std::cout << "OK QUIT" << std::endl; break; }
                else throw std::runtime_error("unknown command");
            } catch (const std::exception& e) { std::cout << "ERROR " << e.what() << std::endl; }
        }
        server.Stop(0); server.Join();
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
