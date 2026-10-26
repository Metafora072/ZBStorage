#include "mds/lhm/LhmBridge.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>
#include <atomic>

// Deliberately incompatible symbols in the host. The DSO must not bind to them.
extern "C" {
uint64_t globalepoch = 0x123456789abcdef0ULL;
uint64_t active_epoch = 0xfedcba9876543210ULL;
}
static void check(bool ok, const char* label) { if (!ok) throw std::runtime_error(label); }
using Context = std::unique_ptr<zb_lhm_context, decltype(&zb_lhm_close)>;
static zb_lhm_inode lookup(zb_lhm_context* ctx, const char* path) {
    zb_lhm_inode out{}; check(zb_lhm_lookup(ctx, path, &out), path); return out;
}
static void same(const zb_lhm_inode& a, const zb_lhm_inode& b) {
    check(a.inode_id == b.inode_id && a.size == b.size && a.mode == b.mode &&
          a.type == b.type && a.ctime_ns == b.ctime_ns && a.mtime_ns == b.mtime_ns &&
          a.version == b.version, "path/ID attribute mismatch");
}
int main(int argc, char** argv) {
    try {
        check(argc == 2 && !std::filesystem::exists(argv[1]), "expected fresh work directory");
        std::filesystem::create_directories(argv[1]);
        const std::string file = std::string(argv[1]) + "/namespace.meta";
        Context ctx(zb_lhm_open(file.c_str()), zb_lhm_close);
        check(bool(ctx), "open real LHM");
        check(!zb_lhm_open(file.c_str()), "existing store must be rejected");
        auto root = lookup(ctx.get(), "/"); check(root.inode_id == 1, "root alias");
        check(zb_lhm_create(ctx.get(), "/dir", 1), "mkdir");
        check(zb_lhm_create(ctx.get(), "/dir/file", 0), "create");
        auto dir = lookup(ctx.get(), "/dir"), inode = lookup(ctx.get(), "/dir/file");
        check(inode.inode_id == 0x100000001ULL, "file encoded block/slot");
        check((dir.inode_id >> 63) == 1, "directory tag");
        for (auto item : {root, dir, inode}) {
            zb_lhm_inode by_id{};
            check(zb_lhm_read_inode(ctx.get(), item.inode_id, &by_id), "read by ID"); same(item, by_id);
            item.size = 12345; item.mode = 0600; item.ctime_ns = 111; item.mtime_ns = 222;
            check(zb_lhm_update_inode(ctx.get(), &item), "write by ID");
            auto actual = lookup(ctx.get(), item.type == 1 ? "/dir/file" : item.inode_id == 1 ? "/" : "/dir");
            item.version++; same(item, actual);
        }
        zb_lhm_inode out{};
        for (uint64_t invalid : {0ULL, 2ULL, 0x10000003fULL, 0x100000000ULL,
                                 0x7fffffff00000000ULL, 0x800000007fffffffULL}) {
            check(!zb_lhm_read_inode(ctx.get(), invalid, &out), "invalid/missing ID accepted");
        }
        auto bad = inode; bad.type = 2;
        check(!zb_lhm_update_inode(ctx.get(), &bad), "wrong type update accepted");
        check(zb_lhm_rename(ctx.get(), "/dir/file", "/dir/renamed"), "file rename");
        check(zb_lhm_rename(ctx.get(), "/dir", "/moved"), "directory rename");
        check(!zb_lhm_lookup(ctx.get(), "/dir/file", &out), "old path exists");
        auto moved = lookup(ctx.get(), "/moved/renamed");
        check(moved.inode_id == inode.inode_id, "rename changed file ID");
        check(lookup(ctx.get(), "/moved").inode_id == dir.inode_id, "rename changed directory ID");
        check(zb_lhm_read_inode(ctx.get(), moved.inode_id, &out), "renamed ID missing"); same(moved, out);
        check(zb_lhm_sync(ctx.get()), "sync");
        // Independent file reads verify that updates reached the predicted disk
        // locations, not just a bridge-side cache or a fabricated inode map.
        std::ifstream disk(file, std::ios::binary);
        auto stored_size = [&](uint64_t offset) {
            uint64_t size = 0;
            disk.seekg(static_cast<std::streamoff>(offset));
            disk.read(reinterpret_cast<char*>(&size), sizeof(size));
            check(bool(disk), "raw inode read");
            return size;
        };
        const uint64_t block = inode.inode_id >> 32;
        const uint64_t slot = static_cast<uint32_t>(inode.inode_id);
        check(stored_size((2*block-1)*16384 + 256 + slot*256 + 16) == 12345,
              "file ID does not match physical inode position");
        const uint64_t head = dir.inode_id & ~(uint64_t{1} << 63);
        check(stored_size(2*head*16384 + 128 + 16) == 12345,
              "directory ID does not match physical inode position");
        check(zb_lhm_read_inode(ctx.get(), moved.inode_id, &out), "read after sync"); same(moved, out);
        std::atomic<bool> ok{true};
        std::vector<std::thread> callers;
        for (int i=0; i<4; ++i) callers.emplace_back([&] {
            for (int n=0; n<25; ++n) {
                zb_lhm_inode result{};
                if (!zb_lhm_read_inode(ctx.get(), moved.inode_id, &result) || result.size != 12345) ok=false;
            }
        });
        for (auto& t : callers) t.join(); check(ok, "caller thread serialization");
        check(!zb_lhm_remove(ctx.get(), "/moved"), "nonempty removal accepted");
        check(zb_lhm_remove(ctx.get(), "/moved/renamed"), "file removal");
        check(!zb_lhm_read_inode(ctx.get(), inode.inode_id, &out), "deleted file ID readable");
        check(!zb_lhm_update_inode(ctx.get(), &inode), "deleted file writable");
        check(zb_lhm_remove(ctx.get(), "/moved"), "directory removal");
        check(!zb_lhm_read_inode(ctx.get(), dir.inode_id, &out), "deleted directory ID readable");
        check(!zb_lhm_update_inode(ctx.get(), &dir), "deleted directory writable");
        check(!zb_lhm_remove(ctx.get(), "/"), "root removal accepted");
        Context second(zb_lhm_open((std::string(argv[1])+"/second.meta").c_str()), zb_lhm_close);
        check(bool(second), "second context");
        check(lookup(second.get(), "/").size != 12345, "contexts share storage");
        check(globalepoch == 0x123456789abcdef0ULL && active_epoch == 0xfedcba9876543210ULL,
              "Masstree symbols interposed");
        std::cout << "PASS: real LHM ID/path reads, writes, rename, deletion, root, invalid IDs, sync, callers and symbol isolation\n";
    } catch (const std::exception& ex) { std::cerr << ex.what() << '\n'; return 1; }
}
