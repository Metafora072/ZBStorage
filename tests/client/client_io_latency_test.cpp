#include "client/metrics/IoLatency.h"
#include "io_latency_test_support.h"
#include <iostream>
#include <set>
#include <sys/stat.h>
#include <unistd.h>

using namespace zb::client::metrics;
using namespace latency_test;

void TestPaths() {
    TempDir temp;
    const auto conf = temp.path / "base.conf";
    OutputDirectory output;
    std::string error;
    Write(conf, "# comment\n ROOT_PATH= relative path/ # comment\nROOT_PATH=ignored\n");
    Check(ResolveOutputDirectory("", conf, temp.path, &output, &error), error);
    Check(fs::path(output.path) == temp.path / "relative path/client/metrics", "relative ROOT_PATH or first-key rule");
    Write(conf, "ROOT_PATH=" + (temp.path / "absolute").string());
    Check(ResolveOutputDirectory("", conf, temp.path, &output, &error), error);
    Check(output.path == (temp.path / "absolute/client/metrics").string(), "absolute ROOT_PATH");
    if (::geteuid() != 0) {
        fs::permissions(conf, fs::perms::none);
        Check(!ResolveOutputDirectory("", conf, temp.path, &output, &error), "unreadable config must fail");
        Check(ResolveOutputDirectory((temp.path / "override").string(), conf, temp.path, &output, &error), error);
        fs::permissions(conf, fs::perms::owner_read | fs::perms::owner_write);
    }
    Write(conf, "ROOT_PATH=\n");
    Check(ResolveOutputDirectory("", conf, temp.path, &output, &error), error);
    Check(output.path == (temp.path / ".demo_run/client/metrics").string(), "empty root fallback");
    fs::remove(conf);
    Check(ResolveOutputDirectory("", conf, temp.path, &output, &error), error);
    Check(output.source.find("fallback") == 0, "missing config fallback source");
    Check(ResolveOutputDirectory((temp.path / "override").string(), temp.path, temp.path, &output, &error), error);
    Check(output.path == (temp.path / "override").string(), "override must skip invalid base config and not append suffix");
    Check(!ResolveOutputDirectory("", temp.path, temp.path, &output, &error), "directory cannot be base config");
    Check(ResolveOutputDirectory("relative-override", conf, temp.path, &output, &error), error);
    Check(output.path == (fs::current_path() / "relative-override").string(), "explicit relative dir uses cwd");
    const auto mount = temp.path / "mount";
    fs::create_directory(mount);
    fs::create_directory_symlink(mount, temp.path / "alias");
    std::string normalized;
    Check(!ValidateOutputDirectory((mount / "metrics").string(), mount, &normalized, &error), "reject nested mount path");
    Check(!ValidateOutputDirectory((temp.path / "alias/metrics").string(), mount, &normalized, &error), "reject symlink into mount");
    Check(!ValidateOutputDirectory(mount, mount, &normalized, &error), "reject mount itself");
    Check(ValidateOutputDirectory((temp.path / "mount-other").string(), mount, &normalized, &error), error);
}

void TestConcurrentDrainAndPrecision() {
    TempDir temp;
    LatencyRecorder recorder;
    std::string error;
    RecorderOptions options{1000, 64, 4096};
    Check(recorder.Prepare((temp.path / "nested/metrics").string(), "", options, &error), error);
    Check(recorder.Start(&error), error);
    const auto file = recorder.FilePath();
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) threads.emplace_back([&recorder] {
        for (int i = 0; i < 300; ++i) {
            IoLatencyRecord r;
            r.request_id = recorder.NextRequestId();
            r.mds_ns = 1234; r.data_node_ns = 2345; r.total_ns = 5678;
            r.offset = -1; r.ret = -EINVAL;
            recorder.Submit(r);
        }
    });
    for (auto& t : threads) t.join();
    {
        RequestTraceScope scope(recorder, false, 0, 1);
        {
            RpcLatencyScope timing(scope.Context(), RpcTarget::Mds);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        // Destructor covers unwinding/unfinished requests, with an error result.
    }
    recorder.Stop();
    recorder.Stop();
    const auto stats = recorder.Stats();
    Check(stats.submitted == 1201 && stats.written == 1201 && stats.synced == 1201 && stats.dropped == 0 && !stats.failed, "normal close must drain and sync exactly once");
    const auto rows = ReadCsv(file);
    Check(rows.size() == 1202, "lost/duplicated rows");
    std::set<uint64_t> ids;
    for (size_t i = 1; i < rows.size(); ++i) {
        Check(ids.insert(std::stoull(rows[i][1])).second, "duplicate request id");
        if (i != rows.size() - 1) {
            Check(rows[i][11] == "1.234" && rows[i][12] == "2.345" && rows[i][13] == "5.678", "nanosecond precision changed");
            Check(rows[i][6] == "-1" && rows[i][8] == "0" && rows[i][9] == std::to_string(-EINVAL), "invalid request fields");
        }
    }
    Check(std::stod(rows.back()[11]) >= 2000 && std::stod(rows.back()[13]) >= std::stod(rows.back()[11]), "request/RPC scope timing missing");
    struct stat st{};
    Check(::stat(file.c_str(), &st) == 0 && (st.st_mode & 0777) == 0600, "file permissions");
    LatencyRecorder next;
    Check(next.Prepare((temp.path / "nested/metrics").string(), "", options, &error) && next.Start(&error), error);
    Check(next.FilePath() != file, "restart must use a new file");
    next.Stop();
    Check(ReadCsv(file).size() == 1202, "restart overwrote previous file");
}

void TestPeriodicShortWrites() {
    TempDir temp;
    std::atomic<int> writes{0}, syncs{0};
    RecorderIo io;
    io.write = [&](int fd, const void* data, size_t size) -> ssize_t {
        if (writes.fetch_add(1) == 0) { errno = EINTR; return -1; }
        return ::write(fd, data, std::min<size_t>(7, size));
    };
    io.sync = [&](int fd) {
        if (syncs.fetch_add(1) == 0) { errno = EINTR; return -1; }
        return ::fdatasync(fd);
    };
    LatencyRecorder recorder(io);
    std::string error;
    Check(recorder.Prepare(temp.path, "", {20, 32, 64}, &error) && recorder.Start(&error), error);
    IoLatencyRecord r; r.request_id = 1;
    recorder.Submit(r);
    Check(WaitUntil([&] { return recorder.Stats().synced == 1; }), "low-volume tail was not periodically persisted");
    Check(ReadCsv(recorder.FilePath()).size() == 2, "short writes lost data");
    recorder.Stop();
    Check(!recorder.Stats().failed, "EINTR must be retried");
}

void TestQueueOverflow() {
    TempDir temp;
    std::mutex gate_mu;
    std::condition_variable gate;
    bool release = false;
    std::atomic<bool> block{false}, entered{false};
    RecorderIo io;
    io.write = [&](int fd, const void* data, size_t size) -> ssize_t {
        if (block) {
            entered = true;
            std::unique_lock<std::mutex> lock(gate_mu);
            gate.wait(lock, [&] { return release; });
        }
        return ::write(fd, data, size);
    };
    LatencyRecorder recorder(io);
    std::string error;
    Check(recorder.Prepare(temp.path, "", {1000, 1, 2}, &error) && recorder.Start(&error), error);
    block = true;
    IoLatencyRecord r; r.request_id = 1;
    recorder.Submit(r);
    const bool writing = WaitUntil([&] { return entered.load(); });
    if (writing) {
        recorder.Submit(r); recorder.Submit(r); recorder.Submit(r);
    }
    { std::lock_guard<std::mutex> lock(gate_mu); release = true; }
    gate.notify_all();
    recorder.Stop();
    Check(writing, "writer never reached gate");
    const auto stats = recorder.Stats();
    Check(stats.submitted == 4 && stats.written == 3 && stats.synced == 3 && stats.dropped == 1, "queue overflow accounting/backpressure");
}

void TestFileFailures() {
    TempDir temp;
    for (bool fail_sync : {false, true}) {
        std::atomic<bool> fail{false};
        RecorderIo io;
        io.write = [&](int fd, const void* data, size_t size) -> ssize_t {
            if (fail && !fail_sync) { errno = ENOSPC; return -1; }
            return ::write(fd, data, size);
        };
        io.sync = [&](int fd) {
            if (fail && fail_sync) { errno = EIO; return -1; }
            return ::fdatasync(fd);
        };
        LatencyRecorder recorder(io);
        std::string error;
        Check(recorder.Prepare(temp.path, "", {10, 1, 8}, &error) && recorder.Start(&error), error);
        fail = true;
        IoLatencyRecord r;
        recorder.Submit(r);
        Check(WaitUntil([&] { return recorder.Stats().failed; }), "file error not detected");
        recorder.Submit(r);
        recorder.Stop();
        const auto stats = recorder.Stats();
        Check(stats.submitted == 2 && stats.synced == 0, "failed file incorrectly marked durable");
        Check(stats.dropped == (fail_sync ? 1U : 2U), "failed recorder must count dropped records");
    }
    std::string error;
    LatencyRecorder invalid;
    Check(!invalid.Prepare(temp.path, "", {0, 1, 1}, &error), "zero flush accepted");
    Check(!invalid.Prepare(temp.path, "", {1, 2, 1}, &error), "batch larger than queue accepted");
    Write(temp.path / "file", "x");
    Check(!invalid.Prepare((temp.path / "file/child").string(), "", {}, &error), "file used as directory");
}

int main() {
    try {
        TestPaths(); TestConcurrentDrainAndPrecision(); TestPeriodicShortWrites();
        TestQueueOverflow(); TestFileFailures();
        std::cout << "PASS: paths, precision, concurrent drain, periodic sync, short writes, overflow, file failures\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FAIL: " << e.what() << '\n'; return 1; }
}
