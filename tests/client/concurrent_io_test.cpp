#include "common/concurrency/BoundedExecutor.h"
#include "client/data/ParallelIoExecutor.h"
#include "client/sdk/BatchIoClient.h"
#include "data_node/real_node/io/IOExecutor.h"
#include "io_latency_test_support.h"
#include <atomic>
#include <array>
#include <future>
#include <iostream>
using namespace latency_test;
using namespace zb;

void TestPool() {
    concurrency::BoundedExecutor pool({3, 8, 1, 12});
    std::promise<void> release; auto gate = release.get_future().share();
    std::atomic<int> running{0};
    Check(pool.Submit("a", 4, [&] { ++running; gate.wait(); }), "submit A");
    Check(pool.Submit("a", 4, [&] { ++running; gate.wait(); }), "queue same disk");
    Check(pool.Submit("b", 4, [&] { ++running; gate.wait(); }), "submit B");
    const bool parallel = WaitUntil([&] { return running == 2; });
    const bool rejected = !pool.Submit("c", 1, [] {});
    release.set_value(); pool.Stop();
    Check(parallel && rejected, "per-disk concurrency / byte budget");
    Check(pool.Stats().peak_active == 2 && pool.Stats().completed == 3, "pool accounting");
    Check(!pool.Submit("a", 0, [] {}), "submit after stop");
}
void TestParallel() {
    client::data::ParallelIoExecutor pool({4, 16, 4, 1024});
    client::metrics::IoTraceContext trace;
    std::atomic<int> active{0}, peak{0};
    std::array<int,4> values{};
    Check(pool.Run("n", {1,1,1,1}, [&](size_t i, client::metrics::IoTraceContext* c) {
        int n = ++active; int p = peak;
        while (p < n && !peak.compare_exchange_weak(p,n)) {}
        std::this_thread::sleep_for(std::chrono::milliseconds((4-i)*10));
        values[i] = i+1; c->record.data_node_ns = 10; --active;
        return i == 2 ? EIO : 0;
    }, &trace) == EIO, "parallel failure result");
    Check(active == 0 && peak > 1 && trace.record.data_node_ns == 40 && values[3] == 4,
          "failure drains tasks, private trace aggregation, concurrent execution");
}
void TestIO() {
    TempDir dir; real_node::IOExecutor io;
    std::vector<std::future<void>> jobs;
    for (int i=0; i<16; ++i) jobs.push_back(std::async(std::launch::async, [&,i] {
        uint64_t n; Check(io.Write((dir.path/"object").string(), i*4096, std::string(4096,'a'+i), &n).ok(), "write");
    }));
    for (auto& job:jobs) job.get();
    std::string data; uint64_t n;
    Check(io.Read((dir.path/"object").string(), 0, 65536, &data, &n).ok() && n==65536, "read");
    for (int i=0;i<16;++i) Check(data.substr(i*4096,4096)==std::string(4096,'a'+i), "first-create truncation");
    Check(!io.Read((dir.path/"object").string(), UINT64_MAX, 2, &data, &n).ok(), "overflow");
}
void TestBatch() {
    TempDir dir;
    Write(dir.path/"source", "hello batch"); Write(dir.path/"target", "");
    client::BatchIoClient sdk;
    using Task=client::BatchIoTask;
    std::vector<Task> tasks{
        {"write",true,(dir.path/"target").string(),0,11,(dir.path/"source").string(),{}},
        {"read",false,(dir.path/"target").string(),0,11,(dir.path/"readback").string(),{"write"}},
        {"bad",false,(dir.path/"missing").string(),0,1,(dir.path/"unused").string(),{}},
        {"skip",false,(dir.path/"target").string(),0,1,(dir.path/"skip").string(),{"bad"}}};
    std::atomic<int> callbacks{0};
    auto h=sdk.submit_batch(tasks,[&](const auto&) { ++callbacks; });
    auto r=sdk.wait_batch(h);
    Check(!r[0].error && !r[1].error && r[1].bytes==11 && r[2].error==ENOENT && r[3].skipped && callbacks==4,"batch results");
    std::ifstream in(dir.path/"readback"); std::string actual((std::istreambuf_iterator<char>(in)),{});
    Check(actual=="hello batch", "batch dependency content");
    tasks[0].after={"read"};
    bool invalid=false;try { sdk.submit_batch(tasks); } catch (const std::invalid_argument&) { invalid=true; }
    Check(invalid,"forward dependency rejected");
    auto same=sdk.wait_batch(sdk.submit_batch({{"same",false,(dir.path/"source").string(),0,11,(dir.path/"source").string(),{}}}));
    Check(same[0].error==EINVAL && fs::file_size(dir.path/"source")==11,"same-file destination must not truncate");
}
int main() {
    try { TestPool(); TestParallel(); TestIO(); TestBatch(); std::cout<<"PASS concurrent I/O core\n"; }
    catch(const std::exception& e) { std::cerr<<e.what()<<'\n'; return 1; }
}
