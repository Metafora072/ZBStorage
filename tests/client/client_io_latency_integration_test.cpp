#define ZBSTORAGE_FUSE_CLIENT_NO_MAIN
#include "client/fuse/zb_fuse_client.cpp"
#include "io_latency_test_support.h"
#include <brpc/server.h>
#include <brpc/closure_guard.h>
#include <bthread/bthread.h>
#include <map>
#include <csignal>

using namespace latency_test;
using namespace zb::client::fuse_client;
namespace rpc = zb::rpc;
namespace metrics = zb::client::metrics;

namespace {
constexpr int kMdsDelayUs = 5000;
constexpr int kNodeDelayUs = 7000;

class MetadataService : public rpc::MdsService {
public:
    std::string node_address;
    bool optical{false};
    bool missing{false};
    bool fail_update{false};
    bool archived{false};
    std::atomic<int> calls{0}, updates{0}, attrs{0};
    void Delay() { ++calls; bthread_usleep(kMdsDelayUs); }
    void Attr(rpc::InodeAttr* attr) {
        attr->set_inode_id(1); attr->set_size(8); attr->set_object_unit_size(4);
        attr->set_mode(0644); attr->set_type(rpc::INODE_FILE);
        if (archived) attr->set_file_archive_state(rpc::INODE_ARCHIVE_ARCHIVED);
    }
    void Lookup(google::protobuf::RpcController*, const rpc::LookupRequest* request, rpc::LookupReply* reply,
                google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); Attr(reply->mutable_attr());
        if (request->path() == "/") {
            reply->mutable_attr()->set_type(rpc::INODE_DIR);
            reply->mutable_attr()->set_inode_id(2);
            reply->mutable_attr()->set_mode(0755);
        }
        reply->mutable_status()->set_code(missing ? rpc::MDS_NOT_FOUND : rpc::MDS_OK);
    }
    void Getattr(google::protobuf::RpcController*, const rpc::GetattrRequest*, rpc::GetattrReply* reply,
                 google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++attrs; Attr(reply->mutable_attr());
    }
    void GetFileLocation(google::protobuf::RpcController*, const rpc::GetFileLocationRequest*,
                         rpc::GetFileLocationReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay();
        Location(reply->mutable_location());
    }
    void Location(rpc::FileLocationView* location) {
        Attr(location->mutable_attr());
        if (optical) {
            auto* anchor = location->mutable_optical_location();
            anchor->set_node_id("test-node"); anchor->set_disk_id("disc0"); anchor->set_node_address(node_address);
        } else {
            auto* anchor = location->mutable_disk_location();
            anchor->set_node_id("test-node"); anchor->set_disk_id("disk0"); anchor->set_node_address(node_address);
        }
    }
    void Open(google::protobuf::RpcController*, const rpc::OpenRequest*, rpc::OpenReply* reply,
              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        reply->set_handle_id(42); Location(reply->mutable_location());
    }
    void Close(google::protobuf::RpcController*, const rpc::CloseRequest*, rpc::CloseReply*,
               google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
    }

    void UpdateInodeStat(google::protobuf::RpcController*, const rpc::UpdateInodeStatRequest*,
                         rpc::UpdateInodeStatReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++updates; Attr(reply->mutable_attr());
        reply->mutable_status()->set_code(fail_update ? rpc::MDS_INTERNAL_ERROR : rpc::MDS_OK);
    }
};

class NodeService : public rpc::RealNodeService {
public:
    std::atomic<int> calls{0}, reads{0}, writes{0}, resolves{0}, allocates{0}, commits{0};
    bool fail_read{false};
    bool fail_commit{false};
    std::map<std::string, std::string> objects;
    std::mutex objects_mu;
    void Delay() { ++calls; bthread_usleep(kNodeDelayUs); }
    void Meta(rpc::FileMeta* meta) {
        meta->set_inode_id(1); meta->set_file_size(8); meta->set_object_unit_size(4); meta->set_version(1);
    }
    template<class Reply>
    void Slices(uint64_t offset, uint64_t size, Reply* reply) {
        if (offset >= 8) return;
        const auto end = std::min<uint64_t>(8, offset + size);
        for (uint64_t i = offset / 4; i < (end + 3) / 4; ++i) {
            auto* slice = reply->add_slices();
            slice->set_object_id("object-" + std::to_string(i));
            slice->set_disk_id("disk0");
            slice->set_object_offset(std::max(offset, i * 4) - i * 4);
            slice->set_length(std::min(end, i * 4 + 4) - std::max(offset, i * 4));
        }
    }
    void ResolveFileRead(google::protobuf::RpcController*, const rpc::ResolveFileReadRequest* request,
                         rpc::ResolveFileReadReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++resolves; Meta(reply->mutable_meta());
        Slices(request->offset(), request->size(), reply);
    }
    void AllocateFileWrite(google::protobuf::RpcController*, const rpc::AllocateFileWriteRequest* request,
                           rpc::AllocateFileWriteReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++allocates; Meta(reply->mutable_meta());
        reply->set_txid("test-tx"); Slices(request->offset(), request->size(), reply);
    }
    void CommitFileWrite(google::protobuf::RpcController*, const rpc::CommitFileWriteRequest*,
                         rpc::CommitFileWriteReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++commits; Meta(reply->mutable_meta());
        reply->mutable_status()->set_code(fail_commit ? rpc::STATUS_IO_ERROR : rpc::STATUS_OK);
    }
    void ReadObject(google::protobuf::RpcController*, const rpc::ReadObjectRequest* request,
                    rpc::ReadObjectReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++reads;
        if (fail_read) { reply->mutable_status()->set_code(rpc::STATUS_IO_ERROR); return; }
        std::lock_guard<std::mutex> lock(objects_mu);
        const auto it = objects.find(request->object_id());
        reply->set_data(it == objects.end() ? std::string(request->size(), 'a') :
                        it->second.substr(request->offset(), request->size()));
    }
    void WriteObject(google::protobuf::RpcController*, const rpc::WriteObjectRequest* request,
                     rpc::WriteObjectReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Delay(); ++writes;
        std::lock_guard<std::mutex> lock(objects_mu);
        objects[request->object_id()] = request->data();
        reply->set_bytes(request->data().size());
    }
};

struct LocalServer {
    brpc::Server server;
    explicit LocalServer(google::protobuf::Service* service) {
        Check(server.AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0, "add test service");
        Check(server.Start("127.0.0.1:0", nullptr) == 0, "start test server");
    }
    ~LocalServer() { server.Stop(0); server.Join(); }
    std::string Address() { return butil::endpoint2str(server.listen_address()).c_str(); }
};

class SchedulerService : public rpc::SchedulerService {
public:
    std::string node_address;
    void GetClusterView(google::protobuf::RpcController*, const rpc::GetClusterViewRequest*,
                        rpc::GetClusterViewReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        reply->set_generation(1);
        auto* node = reply->add_nodes(); node->set_node_id("test-node"); node->set_address(node_address);
    }
};

volatile std::sig_atomic_t fixture_stop = 0;
void StopFixture(int) { fixture_stop = 1; }
int ServeFixture() {
    NodeService node;
    LocalServer data_server(&node);
    MetadataService metadata; metadata.node_address = data_server.Address();
    LocalServer mds_server(&metadata);
    SchedulerService scheduler; scheduler.node_address = data_server.Address();
    LocalServer scheduler_server(&scheduler);
    std::signal(SIGTERM, StopFixture);
    std::cout << "{\"mds\":\"" << mds_server.Address() << "\",\"scheduler\":\""
              << scheduler_server.Address() << "\"}" << std::endl;
    while (!fixture_stop) bthread_usleep(100000);
    return 0;
}

struct Expected {
    int ret;
    int mds_calls;
    int data_calls;
    double local_us;
    std::string tier;
};

void TestClientRequests() {
    TempDir temp;
    NodeService node;
    LocalServer node_server(&node);
    MetadataService metadata;
    metadata.node_address = node_server.Address();
    LocalServer mds_server(&metadata);
    FuseState state;
    Check(state.mds.Init(mds_server.Address()), "initialize MDS client");
    bool slow_resolver = false;
    uint64_t resolver_ns = 0;
    state.data_nodes.SetNodeAddressResolver([&](const std::string&, std::string* address) {
        if (slow_resolver) {
            const auto begin = metrics::SteadyClock::now();
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            resolver_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(metrics::SteadyClock::now() - begin).count();
        }
        *address = node_server.Address(); return true;
    });
    state.latency = std::make_unique<metrics::LatencyRecorder>();
    std::string error;
    Check(state.latency->Prepare((temp.path / "metrics").string(), "", {20, 8, 256}, &error) && state.latency->Start(&error), error);
    std::vector<Expected> expected;
    auto run = [&](const std::function<int()>& call, int ret, const std::string& tier = "disk") {
        const int m = metadata.calls, d = node.calls;
        resolver_ns = 0;
        Check(call() == ret, "business result changed");
        expected.push_back({ret, metadata.calls - m, node.calls - d, resolver_ns / 1000.0, tier});
    };
    char buffer[8]{};
    slow_resolver = true;
    run([&] { return ReadRequest(&state, "/file", buffer, 8, 0, nullptr); }, 8);
    slow_resolver = false;
    Check(std::string(buffer, 8) == "aaaaaaaa" && node.reads == 2 && node.resolves == 1, "multi-object read path");
    metadata.fail_update = true;
    run([&] { return WriteRequest(&state, "/file", "aaaabbbb", 8, 0, nullptr); }, 8);
    Check(metadata.updates == 1 && node.writes == 2 && node.allocates == 1 && node.commits == 1, "write plan/object/commit/update path");
    metadata.fail_update = false;
    run([&] { return ReadRequest(&state, "/file", buffer, 8, 0, nullptr); }, 8);
    Check(std::string(buffer, 8) == "aaaabbbb", "read/write content regression");
    run([&] { return ReadRequest(&state, "/file", buffer, 8, 8, nullptr); }, 0);
    node.fail_read = true;
    run([&] { return ReadRequest(&state, "/file", buffer, 8, 0, nullptr); }, -EIO);
    node.fail_read = false;
    node.fail_commit = true;
    run([&] { return WriteRequest(&state, "/file", "aaaabbbb", 8, 0, nullptr); }, -EIO);
    node.fail_commit = false;
    metadata.missing = true;
    run([&] { return ReadRequest(&state, "/missing", buffer, 8, 0, nullptr); }, -ENOENT, "unknown");
    metadata.missing = false;
    run([&] { return ReadRequest(&state, "/file", buffer, 0, 0, nullptr); }, 0, "unknown");
    run([&] { return WriteRequest(&state, "/file", buffer, 0, 0, nullptr); }, 0, "unknown");
    run([&] { return ReadRequest(&state, "/file", buffer, 8, -1, nullptr); }, -EINVAL, "unknown");
    run([&] { return WriteRequest(&state, "/file", buffer, 8, -1, nullptr); }, -EINVAL, "unknown");
    metadata.optical = true;
    state.inode_cache.clear();
    state.handle_to_inode[42] = 1;
    struct fuse_file_info fi{}; fi.fh = 42;
    const int before_resolves = node.resolves;
    run([&] { return ReadRequest(&state, "/optical", buffer, 8, 0, &fi); }, 8, "optical");
    Check(metadata.attrs == 1 && node.resolves == before_resolves, "optical must Getattr as needed and bypass layout RPC");
    metadata.archived = true;
    run([&] { return WriteRequest(&state, "/optical", buffer, 8, 0, &fi); }, -EROFS, "optical");
    state.latency->Stop();
    const auto rows = ReadCsv(state.latency->FilePath());
    Check(rows.size() == expected.size() + 1 && state.latency->Stats().dropped == 0, "every completed request needs exactly one row");
    for (size_t i = 0; i < expected.size(); ++i) {
        const auto& row = rows[i + 1];
        const auto& e = expected[i];
        const double mds = std::stod(row[11]), data = std::stod(row[12]), total = std::stod(row[13]);
        Check(std::stoi(row[9]) == e.ret && row[10] == e.tier, "result or tier mismatch");
        Check(mds >= e.mds_calls * kMdsDelayUs && data >= e.data_calls * kNodeDelayUs, "RPC wait missing from cumulative timings, row " + std::to_string(i));
        Check(total + 0.002 >= mds + data + e.local_us, "resolver/local time double-counted as RPC or total too small");
        if (e.mds_calls == 0) Check(mds == 0, "no MDS access should have zero latency");
        if (e.data_calls == 0) Check(data == 0, "no data access should have zero latency");
    }
    const auto submitted = state.latency->Stats().submitted;
    state.latency.reset();
    Check(ReadRequest(&state, "/file", buffer, 0, 0, nullptr) == 0 && submitted == expected.size(), "disabled path changed");
}

void TestAddressFallback() {
    TempDir temp;
    NodeService bad, good;
    bad.fail_read = true;
    LocalServer bad_server(&bad), good_server(&good);
    DataNodeClient client;
    client.SetNodeAddressResolver([&](const std::string& node, std::string* address) {
        *address = node == "bad" ? bad_server.Address() : good_server.Address(); return true;
    });
    rpc::ReplicaLocation replica;
    replica.set_primary_node_id("bad"); replica.set_node_id("good");
    replica.set_disk_id("disk0"); replica.set_object_id("o");
    metrics::LatencyRecorder recorder;
    std::string error, output;
    Check(recorder.Prepare(temp.path, "", {}, &error) && recorder.Start(&error), error);
    metrics::RequestTraceScope trace(recorder, false, 0, 4);
    Check(client.Read(replica, 0, 4, &output, &error, trace.Context()), error);
    trace.Finish(4);
    recorder.Stop();
    const auto rows = ReadCsv(recorder.FilePath());
    Check(bad.reads == 1 && good.reads == 1, "both candidate addresses should be attempted");
    Check(std::stod(rows[1][12]) >= 2 * kNodeDelayUs && std::stod(rows[1][11]) == 0, "failed address wait omitted or misclassified");
}

void TestPrepareConfiguration() {
    TempDir temp;
    const auto mount = temp.path / "mount"; fs::create_directory(mount);
    const auto config = temp.path / "base.conf";
    Write(config, "ROOT_PATH=" + (temp.path / "root").string());
    FLAGS_io_latency_base_conf = config;
    FLAGS_io_latency_dir.clear();
    FLAGS_io_latency_enabled = true;
    std::string program = "zb_fuse_client", mount_arg = mount, foreground = "-f";
    char* args[] = {program.data(), mount_arg.data(), foreground.data()};
    std::string error;
    {
        FuseState state;
        Check(PrepareLatency(&state, 3, args, &error), error);
        Check(state.latency->Start(&error), error);
        Check(fs::path(state.latency->FilePath()).parent_path() == temp.path / "root/client/metrics", "client default base.conf path");
    }
    FLAGS_io_latency_dir = (temp.path / "override").string();
    FLAGS_io_latency_base_conf = temp.path; // invalid, but bypassed by explicit dir
    {
        FuseState state;
        Check(PrepareLatency(&state, 3, args, &error) && state.latency->Start(&error), error);
        Check(fs::path(state.latency->FilePath()).parent_path() == temp.path / "override", "explicit final directory");
    }
    FLAGS_io_latency_dir = (mount / "metrics").string();
    {
        FuseState state;
        Check(!PrepareLatency(&state, 3, args, &error), "client must reject recursive mount path");
    }
    FLAGS_io_latency_enabled = false;
    FLAGS_io_latency_dir = (temp.path / "disabled").string();
    FuseState disabled;
    Check(PrepareLatency(&disabled, 3, args, &error) && !disabled.latency && !fs::exists(FLAGS_io_latency_dir), "disabled client must not create metrics directory");
}
} // namespace

int main(int argc, char** argv) {
    FLAGS_default_object_unit_size = 4;
    FLAGS_max_retry = 0;
    try {
        if (argc == 2 && std::string(argv[1]) == "--serve-fixture") return ServeFixture();
        TestPrepareConfiguration(); TestClientRequests(); TestAddressFallback();
        std::cout << "PASS: client config, multi-object read/write, optical, early returns, failed RPC and address fallback timing\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FAIL: " << e.what() << '\n'; return 1; }
}
