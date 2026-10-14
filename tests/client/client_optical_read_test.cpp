#define ZBSTORAGE_FUSE_CLIENT_NO_MAIN
#include "client/fuse/zb_fuse_client.cpp"
#include "client/sdk/PrepareClient.h"
#include "optical_read_test_service.h"
#include <brpc/server.h>
#include <future>
#include <iostream>
#include <csignal>

using namespace zb::client;
using namespace zb::client::fuse_client;
namespace rpc = zb::rpc;
#define REQUIRE(x) do { if (!(x)) throw std::runtime_error(std::string("failed: ") + #x); } while (0)
template<class F> void Error(int code, F f) {
    try { f(); } catch (const optical::ClientError& e) { REQUIRE(e.code() == code); return; }
    throw std::runtime_error("expected ClientError");
}
class Metadata : public rpc::MdsService {
public:
    std::string address;
    std::atomic<uint64_t> next_handle{100};
    std::atomic<int> opens{0}, closes{0};
    void Attr(rpc::InodeAttr* attr, uint64_t inode = 1) {
        attr->set_inode_id(inode); attr->set_type(rpc::INODE_FILE);
        attr->set_size(OpticalReadTestService::Content(inode).size());
        attr->set_mode(0644); attr->set_version(7);
    }
    void Location(rpc::FileLocationView* view, uint64_t inode = 1) {
        Attr(view->mutable_attr(), inode);
        auto* loc = view->mutable_optical_location();
        loc->set_node_id("optical-test"); loc->set_node_address(address);
        loc->set_disk_id("disk-1"); loc->set_image_id("img-1");
    }
    void Lookup(google::protobuf::RpcController*, const rpc::LookupRequest* request,
        rpc::LookupReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        Attr(reply->mutable_attr(), request->path() == "/empty" ? 2 : 1);
        if (request->path() == "/") {
            reply->mutable_attr()->set_type(rpc::INODE_DIR); reply->mutable_attr()->set_mode(0755);
        }
    }
    void Getattr(google::protobuf::RpcController*, const rpc::GetattrRequest* request,
        rpc::GetattrReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Attr(reply->mutable_attr(), request->inode_id());
    }
    void GetFileLocation(google::protobuf::RpcController*, const rpc::GetFileLocationRequest* request,
        rpc::GetFileLocationReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); Location(reply->mutable_location(), request->inode_id());
    }
    void Open(google::protobuf::RpcController*, const rpc::OpenRequest* request,
        rpc::OpenReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done); ++opens;
        reply->set_handle_id(next_handle++); Location(reply->mutable_location(), request->path() == "/empty" ? 2 : 1);
    }
    void Close(google::protobuf::RpcController*, const rpc::CloseRequest*, rpc::CloseReply*,
        google::protobuf::Closure* done) override { brpc::ClosureGuard guard(done); ++closes; }
};
class Scheduler : public rpc::SchedulerService {
public:
    void GetClusterView(google::protobuf::RpcController*, const rpc::GetClusterViewRequest*,
        rpc::GetClusterViewReply*, google::protobuf::Closure* done) override { brpc::ClosureGuard guard(done); }
};
class Fixture {
public:
    OpticalReadTestService optical;
    Metadata mds;
    Scheduler scheduler;
    brpc::Server server;
    std::string address;
    Fixture() {
        REQUIRE(server.AddService(&optical, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
        REQUIRE(server.AddService(&mds, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
        REQUIRE(server.AddService(&scheduler, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
        REQUIRE(server.Start("127.0.0.1:0", nullptr) == 0);
        address = "127.0.0.1:" + std::to_string(server.listen_address().port);
        mds.address = address;
    }
    ~Fixture() { server.Stop(0); server.Join(); }
    optical::FileInfo Info(uint64_t inode = 1) {
        return {address, inode, "disk-1", "img-1", optical.Content(inode).size(), 7};
    }
};
PreparedFile Ready(optical::OpticalReadClient& client, const PrepareHandle& h) {
    auto p = std::make_shared<std::promise<optical::Completion>>();
    auto future = p->get_future();
    client.Watch(h, [p](const optical::Completion& completion) { p->set_value(completion); });
    REQUIRE(future.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    auto result = future.get(); REQUIRE(result.error == 0); return result.file;
}
void TestProtocol(Fixture& f) {
    optical::OpticalReadClient client;
    Error(EAGAIN, [&] { client.Open(1, 7); });
    const int subs = f.optical.subscriptions;
    auto h = client.Prepare(f.Info(), "/mount/file");
    REQUIRE(Ready(client, h).handle.value == h.value);
    REQUIRE(client.Prepare(f.Info(), "/mount/file").value == h.value);
    auto a = client.Open(1, 7), b = client.Open(1, 7);
    char data[32]{};
    metrics::IoTraceContext trace;
    int reads = f.optical.reads;
    REQUIRE(client.Read(a, 0, sizeof(data), data, &trace) == 10);
    REQUIRE(std::string(data, 10) == f.optical.Content(1));
    REQUIRE(f.optical.reads == reads + 3); REQUIRE(trace.record.data_node_ns > 0);
    REQUIRE(client.Read(a, 8, 9, data) == 2);
    REQUIRE(client.Read(a, 0, 3, data) == 3); REQUIRE(std::string(data, 3) == f.optical.Content(1).substr(0, 3));
    REQUIRE(client.Read(a, UINT64_MAX, 3, data) == 0);
    REQUIRE(client.Read(a, 0, 0, nullptr) == 0);
    f.optical.corrupt_read = true;
    Error(EIO, [&] { client.Read(a, 0, 1, data); }); f.optical.corrupt_read = false;
    client.Close(a); Error(EBADF, [&] { client.Read(a, 0, 1, data); });
    REQUIRE(client.Read(b, 0, 10, data) == 10);
    auto c = client.Open(1, 7); client.Close(c);
    client.Release(h); Error(EAGAIN, [&] { client.Open(1, 7); });
    REQUIRE(client.Read(b, 0, 10, data) == 10); client.Close(b);
    REQUIRE(f.optical.LiveArtifacts() == 0);
    auto empty = client.Prepare(f.Info(2), "/mount/empty"); Ready(client, empty);
    auto e = client.Open(2, 7); REQUIRE(client.Read(e, 0, 10, data) == 0);
    client.Close(e); client.Release(empty);
    REQUIRE(f.optical.subscriptions == subs + 1);
    auto again = client.Prepare(f.Info(), "/mount/file"); Ready(client, again); client.Release(again);
    REQUIRE(f.optical.LiveArtifacts() == 0);
}
void TestPendingAndSdk(Fixture& f) {
    optical::OpticalReadClient client({3000, 2});
    f.optical.auto_ready = false;
    std::string socket = "/tmp/zb-optical-test-" + std::to_string(getpid()) + ".sock";
    control::PrepareControlServer server(client, "/mount", socket, [&](const std::string& path) {
        return f.Info(path == "/empty" ? 2 : 1);
    });
    server.Start();
    PrepareHandle handle;
    { PrepareClient sdk(socket); handle = sdk.submit("/mount/file"); } // submitter exits
    Error(EAGAIN, [&] { client.Open(1, 7); });
    PrepareClient sdk(socket);
    Error(EINVAL, [&] { sdk.submit("/mount-other/file"); });
    std::vector<std::future<PreparedFile>> waits;
    // More waits than control workers: pending watches must not occupy workers.
    for (int i = 0; i < 8; ++i) waits.push_back(std::async(std::launch::async, [&] { return sdk.wait(handle); }));
    auto empty = sdk.submit("/mount/empty");
    f.optical.Complete(f.optical.TaskFor(1)); f.optical.Complete(f.optical.TaskFor(2));
    for (auto& wait : waits) {
        REQUIRE(wait.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
        REQUIRE(wait.get().handle.value == handle.value);
    }
    REQUIRE(sdk.wait(empty).size == 0); // late watch
    auto session = client.Open(1, 7);
    char data[10]; int reads = f.optical.reads;
    REQUIRE(client.Read(session, 0, 10, data) == 10); REQUIRE(f.optical.reads == reads + 5);
    client.Close(session); sdk.release_prepared(handle); sdk.release_prepared(empty);
    f.optical.auto_ready = true;
    auto promise = std::make_shared<std::promise<PreparedFile>>(); auto result = promise->get_future();
    auto async_handle = sdk.prepare_read("/mount/file", [promise](const PreparedFile& file) { promise->set_value(file); });
    REQUIRE(result.wait_for(std::chrono::seconds(3)) == std::future_status::ready);
    REQUIRE(result.get().handle.value == async_handle.value); sdk.release_prepared(async_handle);
    // A pending SDK watcher can be destroyed without hanging the client process.
    f.optical.auto_ready = false;
    PrepareHandle abandoned;
    { PrepareClient short_lived(socket); abandoned = short_lived.prepare_read("/mount/file", [](const PreparedFile&) {}); }
    f.optical.Complete(f.optical.TaskFor(1)); sdk.wait(abandoned); sdk.release_prepared(abandoned);
    f.optical.auto_ready = true;
    server.Stop(); REQUIRE(access(socket.c_str(), F_OK) != 0);
}
void TestIsolationAndConcurrentRelease(Fixture& f) {
    Fixture other;
    optical::OpticalReadClient client;
    auto first = client.Prepare(f.Info(3), "/mount/a");
    auto second = client.Prepare(other.Info(4), "/mount/b");
    Ready(client, first); Ready(client, second);
    auto a = client.Open(3, 7), b = client.Open(4, 7);
    client.Release(first); client.Close(a);
    char bytes[10];
    REQUIRE(client.Read(b, 0, 10, bytes) == 10);
    client.Close(b); client.Release(second);
    REQUIRE(other.optical.LiveArtifacts() == 0);
    // Presence and value zero are distinct protocol identities.
    for (auto version : {std::optional<uint64_t>{}, std::optional<uint64_t>{0}}) {
        auto info = f.Info(); info.file_version = version;
        auto h = client.Prepare(info, "/mount/version"); Ready(client, h);
        auto session = client.Open(1, version); client.Close(session); client.Release(h);
    }
    for (int i = 0; i < 8; ++i) {
        auto h = client.Prepare(f.Info(), "/mount/race"); Ready(client, h);
        auto opening = std::async(std::launch::async, [&] {
            try { return client.Open(1, 7); }
            catch (const optical::ClientError& e) { REQUIRE(e.code() == EAGAIN); return optical::OpticalReadClient::Session{}; }
        });
        client.Release(h);
        auto session = opening.get();
        if (session) {
            REQUIRE(client.Read(session, 0, sizeof(bytes), bytes) == 10);
            client.Close(session);
        }
        Error(EAGAIN, [&] { client.Open(1, 7); });
    }
    REQUIRE(f.optical.LiveArtifacts() == 0);
}
void TestMissingServerCapability() {
    struct LegacyService : rpc::OpticalNodeService {} service;
    brpc::Server server;
    REQUIRE(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
    REQUIRE(server.Start("127.0.0.1:0", nullptr) == 0);
    {
        optical::OpticalReadClient client;
        optical::FileInfo info{"127.0.0.1:" + std::to_string(server.listen_address().port),
                               1, "disk-1", "img-1", 10, std::nullopt};
        Error(EIO, [&] { client.Prepare(info, "/mount/file"); });
        // Failure to establish a subscription must also allow clean shutdown.
    }
    server.Stop(0); server.Join();
}
void TestNodeOwnedCache() {
    Fixture f;
    f.optical.auto_ready = false;
    optical::OpticalReadClient client;
    // Submit returns while the node is still preparing; no wait or release is sent.
    auto first = client.Prepare(f.Info(), "/mount/file");
    REQUIRE(client.Prepare(f.Info(), "/mount/file").value == first.value);
    REQUIRE(f.optical.preparations == 1);
    Error(EAGAIN, [&] { client.Open(1, 7); });
    f.optical.Complete(f.optical.TaskFor(1)); Ready(client, first);
    auto opened = client.Open(1, 7);
    REQUIRE(!f.optical.Expire(f.optical.TaskFor(1))); // active readers are protected
    client.Close(opened);
    opened = client.Open(1, 7); // close is independent of prepared cache lifetime
    client.Close(opened);
    REQUIRE(f.optical.releases == 0);
    REQUIRE(f.optical.Expire(f.optical.TaskFor(1))); // server-side eviction
    Error(EAGAIN, [&] { client.Open(1, 7); });
    auto second = client.Prepare(f.Info(), "/mount/file");
    REQUIRE(second.value != first.value);
    f.optical.Complete(f.optical.TaskFor(1)); Ready(client, second);
    opened = client.Open(1, 7);
    char bytes[10]; REQUIRE(client.Read(opened, 0, 10, bytes) == 10);
    REQUIRE(std::string(bytes, 10) == f.optical.Content(1));
    client.Close(opened);
    REQUIRE(f.optical.Expire(f.optical.TaskFor(1)));
    REQUIRE(f.optical.releases == 0);
}
void TestFuse(Fixture& f) {
    FuseState state;
    REQUIRE(state.mds.Init(f.address));
    state.optical_reads = std::make_unique<optical::OpticalReadClient>();
    struct fuse_file_info fi{}; fi.flags = O_RDONLY;
    int closed = f.mds.closes;
    REQUIRE(OpenRequest(&state, "/file", &fi) == -EAGAIN);
    REQUIRE(f.mds.closes == closed + 1); REQUIRE(state.handle_to_inode.empty());
    auto info = ResolveOpticalPrepare(&state, "/file");
    auto h = state.optical_reads->Prepare(info, "/mount/file"); Ready(*state.optical_reads, h);
    REQUIRE(OpenRequest(&state, "/file", &fi) == 0);
    REQUIRE(fi.fh >= 100); REQUIRE(state.optical_sessions.count(fi.fh) == 1);
    char data[20]{}; metrics::IoTraceContext trace;
    REQUIRE(FuseReadImpl(&state, "/file", data, 20, 0, &fi, &trace) == 10);
    REQUIRE(std::string(data, 10) == f.optical.Content(1));
    REQUIRE(trace.record.storage_tier == metrics::StorageTier::Optical);
    REQUIRE(trace.record.data_node_ns > 0 && trace.record.mds_ns > 0);
    REQUIRE(FuseReadImpl(&state, "/file", data, 3, -1, &fi, nullptr) == -EINVAL);
    REQUIRE(ReleaseRequest(&state, &fi) == 0);
    REQUIRE(state.optical_sessions.empty());
    REQUIRE(OpenRequest(&state, "/file", &fi) == 0); REQUIRE(ReleaseRequest(&state, &fi) == 0);
    state.optical_reads->Release(h);
    REQUIRE(OpenRequest(&state, "/file", &fi) == -EAGAIN);
    REQUIRE(state.handle_to_inode.empty());
}
int main(int argc, char** argv) {
    try {
        Fixture fixture;
        if (argc > 1 && std::string(argv[1]) == "--serve-fixture") {
            std::cout << fixture.address << std::endl;
            fixture.server.RunUntilAskedToQuit(); return 0;
        }
        TestProtocol(fixture); TestPendingAndSdk(fixture); TestIsolationAndConcurrentRelease(fixture); TestMissingServerCapability(); TestFuse(fixture); TestNodeOwnedCache();
        REQUIRE(fixture.optical.LiveArtifacts() == 0);
        std::cout << "PASS: optical protocol, SDK/control, FUSE and reference lifecycle\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
