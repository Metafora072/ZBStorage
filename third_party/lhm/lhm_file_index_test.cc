#include "lhm_namespace.hh"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <unistd.h>

#include "kvthread.hh"
#include "timestamp.hh"

relaxed_atomic<mrcu_epoch_type> globalepoch(1);
relaxed_atomic<mrcu_epoch_type> active_epoch(1);
volatile bool recovering = false;
kvtimestamp_t initial_timestamp;

namespace {

using MasstreeLHM::DirectoryBlockStore;
using MasstreeLHM::LhmNamespace;
using MasstreeLHM::PathKey;
using MasstreeLHM::entry_is_directory;
using MasstreeLHM::entry_is_file;
using MasstreeLHM::namespace_entry;

void require(bool condition, const char* message) {
    if (!condition) {
        std::fprintf(stderr, "FAIL: %s\n", message);
        std::exit(1);
    }
}

std::string file_name(int i) {
    return "f" + std::to_string(i);
}

std::string path_join(const std::string& dir, const std::string& name) {
    return dir == "/" ? "/" + name : dir + "/" + name;
}

uint64_t last_hash(const std::string& path) {
    auto parsed = PathKey::parse_absolute_path(path);
    return parsed.hashes.empty() ? 0 : parsed.hashes.back();
}

uint16_t fp16(uint64_t hash) {
    hash ^= hash >> 32;
    hash ^= hash >> 16;
    return static_cast<uint16_t>(hash);
}

void small_directory_test(LhmNamespace& ns, threadinfo& ti) {
    require(ns.mkdir("/small", ti), "mkdir /small");
    for (int i = 0; i != 16; ++i) {
        require(ns.creat_file(path_join("/small", file_name(i)), ti), "small create");
    }
    for (int i = 0; i != 16; ++i) {
        namespace_entry out{};
        require(ns.lookup_file(path_join("/small", file_name(i)), out, ti), "small stat");
        require(entry_is_file(out), "small stat kind");
    }
    namespace_entry missing{};
    require(!ns.lookup_file("/small/missing", missing, ti), "small missing");
    require(ns.mkdir("/small/child", ti), "small mkdir child");
    auto entries = ns.readdir("/small", ti);
    int files = 0;
    int dirs = 0;
    for (const auto& entry : entries) {
        files += entry_is_file(entry.entry) ? 1 : 0;
        dirs += entry_is_directory(entry.entry) ? 1 : 0;
    }
    require(files == 16, "small readdir file count");
    require(dirs == 1, "small readdir child dir count");

    DirectoryBlockStore::file_index_debug_info info{};
    require(ns.debug_file_index_info("/small", info, ti), "small file index debug");
    require(info.ext_hash, "small uses bucket directory");
    require(info.global_depth == 0, "small stays single bucket");
    require(info.unique_buckets == 1, "small single bucket count");
}

void large_directory_split_test(LhmNamespace& ns, threadinfo& ti) {
    const int n = 3000;
    require(ns.mkdir("/large", ti), "mkdir /large");
    for (int i = 0; i != n; ++i) {
        require(ns.creat_file(path_join("/large", file_name(i)), ti), "large create");
    }
    for (int i = 0; i != n; ++i) {
        namespace_entry out{};
        require(ns.lookup_file(path_join("/large", file_name(i)), out, ti), "large stat all");
    }
    namespace_entry missing{};
    require(!ns.lookup_file("/large/not-present", missing, ti), "large missing");

    auto entries = ns.readdir("/large", ti);
    std::vector<std::string> names;
    for (const auto& entry : entries) {
        if (entry_is_file(entry.entry)) {
            names.push_back(entry.child_name);
        }
    }
    std::sort(names.begin(), names.end());
    require(static_cast<int>(names.size()) == n, "large readdir count");
    require(std::unique(names.begin(), names.end()) == names.end(), "large readdir unique");

    DirectoryBlockStore::file_index_debug_info info{};
    require(ns.debug_file_index_info("/large", info, ti), "large file index debug");
    require(info.ext_hash, "large converts to ext hash");
    require(info.split_count > 0, "large split count");
    require(info.max_split_moved < static_cast<uint64_t>(n), "split is local, not global");
    require(info.last_split_moved <= info.max_split_moved, "split moved accounting");
}

void mkdir_routing_test(LhmNamespace& ns, threadinfo& ti) {
    require(ns.mkdir("/route", ti), "mkdir /route");
    DirectoryBlockStore::file_index_debug_info before{};
    require(ns.debug_file_index_info("/route", before, ti), "route debug before");
    require(ns.mkdir("/route/child", ti), "mkdir /route/child");
    DirectoryBlockStore::file_index_debug_info after{};
    require(ns.debug_file_index_info("/route", after, ti), "route debug after");
    require(after.entry_count == before.entry_count, "mkdir does not touch parent file index");
    namespace_entry out{};
    require(ns.lookup_directory("/route/child", out, ti), "child in route tree");
    require(!ns.lookup_file("/route/child", out, ti), "child not file index entry");
    require(ns.creat_file("/route/child/file", ti), "create under child");
    require(ns.lookup_file("/route/child/file", out, ti), "stat under child");
}

void directory_rename_test(LhmNamespace& ns, threadinfo& ti) {
    require(ns.mkdir("/a", ti), "mkdir /a");
    require(ns.mkdir("/a/b", ti), "mkdir /a/b");
    require(ns.mkdir("/x", ti), "mkdir /x");
    require(ns.creat_file("/a/b/f", ti), "create descendant");
    DirectoryBlockStore::file_index_debug_info old_before{};
    DirectoryBlockStore::file_index_debug_info new_before{};
    require(ns.debug_file_index_info("/a", old_before, ti), "old parent before");
    require(ns.debug_file_index_info("/x", new_before, ti), "new parent before");
    require(ns.rename_path("/a/b", "/x/b", ti), "rename dir");
    namespace_entry out{};
    require(!ns.lookup_directory("/a/b", out, ti), "old dir gone");
    require(ns.lookup_directory("/x/b", out, ti), "new dir exists");
    require(ns.lookup_file("/x/b/f", out, ti), "descendant remains under moved root");
    DirectoryBlockStore::file_index_debug_info old_after{};
    DirectoryBlockStore::file_index_debug_info new_after{};
    require(ns.debug_file_index_info("/a", old_after, ti), "old parent after");
    require(ns.debug_file_index_info("/x", new_after, ti), "new parent after");
    require(old_before.entry_count == old_after.entry_count, "old parent file index unchanged");
    require(new_before.entry_count == new_after.entry_count, "new parent file index unchanged");
}

void collision_test(LhmNamespace& ns, threadinfo& ti) {
    require(ns.mkdir("/collide", ti), "mkdir /collide");
    std::unordered_map<uint16_t, std::string> seen;
    std::string a;
    std::string b;
    for (int i = 0; i != 200000 && a.empty(); ++i) {
        std::string name = "c" + std::to_string(i);
        uint16_t f = fp16(last_hash(path_join("/collide", name)));
        auto it = seen.find(f);
        if (it != seen.end() && it->second != name) {
            a = it->second;
            b = name;
            break;
        }
        seen.emplace(f, name);
    }
    require(!a.empty(), "find fp16 collision");
    require(ns.creat_file(path_join("/collide", a), ti), "collision create a");
    require(ns.creat_file(path_join("/collide", b), ti), "collision create b");
    namespace_entry out_a{};
    namespace_entry out_b{};
    require(ns.lookup_file(path_join("/collide", a), out_a, ti), "collision lookup a");
    require(ns.lookup_file(path_join("/collide", b), out_b, ti), "collision lookup b");
    require(out_a.ref.block_id != out_b.ref.block_id || out_a.ref.offset != out_b.ref.offset,
            "collision returns distinct entries");
}

void width_scalability_microbenchmark(LhmNamespace& ns, threadinfo& ti) {
    const std::vector<int> widths = {10, 100, 1000, 10000, 100000};
    for (int width : widths) {
        std::string dir = "/w" + std::to_string(width);
        require(ns.mkdir(dir, ti), "width mkdir");
        for (int i = 0; i != width; ++i) {
            require(ns.creat_file(path_join(dir, file_name(i)), ti), "width create");
        }
        int samples = std::min(width, 1000);
        auto begin = std::chrono::steady_clock::now();
        for (int i = 0; i != samples; ++i) {
            int id = static_cast<int>((static_cast<int64_t>(i) * width) / samples);
            namespace_entry out{};
            require(ns.lookup_file(path_join(dir, file_name(id)), out, ti), "width stat");
        }
        auto end = std::chrono::steady_clock::now();
        double total_us = std::chrono::duration<double, std::micro>(end - begin).count();
        DirectoryBlockStore::file_index_debug_info info{};
        require(ns.debug_file_index_info(dir, info, ti), "width debug");
        std::printf("width=%d layout=%s global_depth=%u buckets=%zu entries=%llu "
                    "avg_stat_us=%.3f max_split_moved=%llu\n",
                    width,
                    info.global_depth == 0 ? "SINGLE_BUCKET" : "MULTI_BUCKET",
                    info.global_depth,
                    info.unique_buckets,
                    static_cast<unsigned long long>(info.entry_count),
                    total_us / samples,
                    static_cast<unsigned long long>(info.max_split_moved));
    }
}

}  // namespace

int main() {
    std::string dir = "/tmp/lhm-file-index-test-" + std::to_string(getpid());
    setenv("LHM_PERSISTENCE_DIR", dir.c_str(), 1);
    setenv("LHM_DENTRY_CACHE_BLOCKS", "256", 1);
    threadinfo* ti = threadinfo::make(threadinfo::TI_MAIN, 0);
    initial_timestamp = timestamp();

    auto ns = std::make_unique<LhmNamespace>();
    ns->initialize(*ti);
    small_directory_test(*ns, *ti);
    large_directory_split_test(*ns, *ti);
    mkdir_routing_test(*ns, *ti);
    directory_rename_test(*ns, *ti);
    collision_test(*ns, *ti);
    width_scalability_microbenchmark(*ns, *ti);
    ns->destroy(*ti);
    std::puts("lhm_file_index_test: PASS");
    return 0;
}
