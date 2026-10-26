#include <filesystem>
#include <algorithm>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include "mds/lhm/LhmNamespaceStore.h"
#include "mds/storage/MetaSchema.h"
#include "mds/storage/RocksNamespaceStore.h"

using namespace zb::mds;
using namespace zb::rpc;
using Records = std::map<uint64_t, std::string>;
void Check(bool ok, const std::string& message) { if (!ok) throw std::runtime_error(message); }
InodeAttr Attr(uint64_t id, InodeType type, const std::string& name) {
    InodeAttr a;
    a.set_inode_id(id); a.set_type(type); a.set_file_name(name);
    a.set_mode(0751); a.set_uid(12345); a.set_gid(54321); a.set_size(9876543210ULL);
    a.set_atime(1234); a.set_mtime(1700000001); a.set_ctime(1700000002);
    a.set_nlink(type == INODE_DIR ? 2 : 1); a.set_object_unit_size(1ULL << 20); a.set_replica(3);
    a.set_version(123456789); a.set_file_archive_state(INODE_ARCHIVE_ARCHIVED);
    a.set_storage_tier(INODE_STORAGE_OPTICAL); a.set_disk_node_id(1ULL << 40);
    a.set_disk_id(UINT32_MAX); a.set_optical_node_id(1ULL << 45);
    a.set_optical_disk_id(1234); a.set_optical_image_id(900);
    a.set_disk_node_is_virtual(true); a.set_blksize(16384); a.set_blocks_512(987654);
    a.set_rdev_major(200); a.set_rdev_minor(300);
    return a;
}
uint64_t Create(NamespaceStore& store, const std::string& path, InodeType type) {
    std::string error;
    auto r = store.ReserveCreate(path, type, &error);
    Check(bool(r), "reserve " + path + ": " + error);
    auto a = Attr(r->inode_id(), type, path.substr(path.rfind('/') + 1));
    auto status = r->Commit(a);
    Check(status.ok(), "commit " + path + ": " + status.message);
    return a.inode_id();
}
Records Drain(NamespaceStore& store, NamespaceScanner& scanner, uint32_t limit, bool* empty = nullptr) {
    Records result;
    for (unsigned calls = 0; calls < 20000; ++calls) {
        SubtreeScanBatch batch;
        batch.entries.push_back({}); batch.done = true; // Outputs must be overwritten.
        auto status = scanner.Next(limit, &batch);
        Check(status.ok(), "Next: " + status.message);
        Check(batch.entries.size() <= limit, "batch limit");
        if (empty && batch.entries.empty() && !batch.done) *empty = true;
        for (const auto& a : batch.entries) {
            InodeAttr direct; std::string error;
            Check(store.GetInode(a.inode_id(), &direct, &error), "GetInode for scan result");
            Check(direct.SerializeAsString() == a.SerializeAsString(), "all attributes preserved");
            Check(result.emplace(a.inode_id(), a.SerializeAsString()).second, "duplicate inode");
        }
        if (batch.done) {
            Check(scanner.Next(limit, &batch).ok() && batch.done && batch.entries.empty(), "stable EOF");
            return result;
        }
    }
    throw std::runtime_error("scan did not terminate");
}
void Contract(NamespaceStore& store, const std::string& label) {
    std::string error;
    Check(store.PutInode(1, Attr(1, INODE_DIR, "/"), &error), "root attributes");
    Create(store, "/tree", INODE_DIR);
    Create(store, "/outside", INODE_FILE);
    Create(store, "/tree/empty", INODE_DIR);
    Create(store, "/tree/wide", INODE_DIR);
    // Small functional fixtures exercising Masstree internal-node splits and
    // multiple file buckets. These are not generated benchmark datasets.
    for (int i = 0; i < 300; ++i) {
        auto dir = "/tree/dir" + std::to_string(i);
        Create(store, dir, INODE_DIR);
        if (i % 11 == 0) Create(store, dir + "/nested", INODE_FILE);
    }
    for (int i = 0; i < 400; ++i)
        Create(store, "/tree/wide/" + std::to_string(i) + std::string(60, 'x'), INODE_FILE);
    std::string deep = "/tree";
    for (int i = 0; i < 40; ++i) {
        deep += "/d";
        Create(store, deep, INODE_DIR);
    }
    Create(store, deep + "/中文", INODE_FILE);
    auto removed = Create(store, "/tree/wide/deleted", INODE_FILE);
    Check(store.Remove("/tree/wide/deleted", INODE_FILE).ok(), "tombstone fixture");
    auto pending = store.ReserveCreate("/tree/pending", INODE_FILE, &error);
    Check(bool(pending), "pending fixture");

    // Independent reference: explicit Readdir + GetInode, not the scan API.
    Records all, files;
    std::vector<std::string> dirs = {"/tree"};
    while (!dirs.empty()) {
        auto path = dirs.back(); dirs.pop_back();
        std::vector<Dentry> children; bool more = false; std::string token;
        Check(store.Readdir(path, "", 0, &children, &more, &token, &error) && !more, "reference readdir");
        for (const auto& child : children) {
            InodeAttr attr;
            Check(store.GetInode(child.inode_id(), &attr, &error), "reference attr");
            all.emplace(attr.inode_id(), attr.SerializeAsString());
            if (attr.type() == INODE_DIR) dirs.push_back(path + "/" + child.name());
            else files.emplace(attr.inode_id(), attr.SerializeAsString());
        }
    }
    Check(files.size() == 429 && all.size() == 771, "reference fixture counts");
    Check(!all.count(removed) && !all.count(pending->inode_id()), "hidden entries");
    for (bool files_only : {true, false}) {
        for (uint32_t limit : {1u, 17u, 1000u}) {
            std::unique_ptr<NamespaceScanner> scanner;
            SubtreeScanOptions options{files_only, limit == 1 ? 1u : 1024u};
            Check(store.BeginSubtreeScan("//tree///", options, &scanner).ok(), "begin normalized");
            SubtreeScanBatch batch;
            Check(scanner->Next(0, &batch).code == MDS_INVALID_ARGUMENT, "reject zero page");
            Check(scanner->Next(1, nullptr).code == MDS_INVALID_ARGUMENT, "reject null output");
            bool empty = false;
            Check(Drain(store, *scanner, limit, &empty) == (files_only ? files : all), "complete subtree set");
            if (limit == 1) Check(empty, "bounded work allows empty nonterminal batch");
        }
    }
    std::unique_ptr<NamespaceScanner> scanner;
    Check(store.BeginSubtreeScan("/tree/empty", {}, &scanner).ok(), "begin empty");
    Check(Drain(store, *scanner, 3).empty(), "empty directory");
    Check(store.BeginSubtreeScan("/tree/wide", {}, &scanner).ok(), "begin nested subtree");
    Check(Drain(store, *scanner, 7).size() == 400, "subtree boundary");
    // Two paused cursors retain independent positions. Changes outside /tree
    // also force the ancestor Masstree layer to split between worker jobs.
    std::unique_ptr<NamespaceScanner> second;
    Check(store.BeginSubtreeScan("/tree", {true, 1}, &scanner).ok(), "begin interleave first");
    Check(store.BeginSubtreeScan("/tree", {true, 1}, &second).ok(), "begin interleave second");
    SubtreeScanBatch paused;
    Check(scanner->Next(1, &paused).ok() && paused.entries.empty() && !paused.done, "pause first");
    Check(second->Next(1, &paused).ok() && paused.entries.empty() && !paused.done, "pause second");
    for (int i = 0; i < 24; ++i) Create(store, "/unrelated" + std::to_string(i), INODE_DIR);
    Check(Drain(store, *scanner, 13) == files && Drain(store, *second, 7) == files, "independent cursors after outside mutations");
    Check(store.BeginSubtreeScan("/", {}, &scanner).ok(), "begin root");
    Check(Drain(store, *scanner, 31).size() == files.size() + 1, "root traversal");
    Check(store.BeginSubtreeScan("/tree", {}, &scanner).ok(), "begin early close");
    scanner.reset();
    Check(store.BeginSubtreeScan("/absent", {}, &scanner).code == MDS_NOT_FOUND && !scanner, "missing root");
    Check(store.BeginSubtreeScan("/outside", {}, &scanner).code == MDS_INVALID_ARGUMENT, "file root");
    for (const auto& path : {std::string("relative"), std::string("/tree/../outside"), std::string("/tree\0x", 7)})
        Check(store.BeginSubtreeScan(path, {}, &scanner).code == MDS_INVALID_ARGUMENT, "invalid path");
    Check(store.BeginSubtreeScan("/", {true, 0}, &scanner).code == MDS_INVALID_ARGUMENT, "invalid work budget");
    Check(store.BeginSubtreeScan("/", {}, nullptr).code == MDS_INVALID_ARGUMENT, "invalid scanner output");
    std::cout << "PASS " << label << " subtree contract: 429 files, 342 directories; pages 1/17/1000\n";
}

using Context = std::unique_ptr<zb_lhm_context, decltype(&zb_lhm_close)>;
using Scan = std::unique_ptr<zb_lhm_scan, decltype(&zb_lhm_scan_close)>;
void RawCreate(zb_lhm_context* ctx, const std::string& path) {
    uint64_t token = 0, id = 0;
    Check(zb_lhm_reserve_checked(ctx, path.c_str(), 1, &token, &id) == ZB_LHM_OK, "raw reserve");
    zb_lhm_full_inode a{};
    a.inode_id = id; a.type = 1; a.mode = 0644; a.nlink = 1;
    auto name = path.substr(path.rfind('/') + 1);
    a.name_length = name.size(); std::copy(name.begin(), name.end(), a.file_name);
    Check(zb_lhm_commit_checked(ctx, token, &a) == ZB_LHM_OK, "raw commit");
}
void NativeChecks(const std::string& file) {
    Context ctx(zb_lhm_open(file.c_str()), zb_lhm_close);
    Check(bool(ctx), "raw open");
    uint64_t slots = 0, buckets = 0;
    int files = 0;
    // Stop as soon as a split leaves aliasing slots: this specifically tests
    // unique-bucket iteration, not a balanced directory with one slot per bucket.
    do {
        RawCreate(ctx.get(), "/" + std::to_string(files++) + std::string(60, 'q'));
        Check(zb_lhm_scan_buckets_for_test(ctx.get(), "/", &slots, &buckets) == ZB_LHM_OK, "bucket info");
    } while (slots == buckets && files < 1000);
    Check(slots > buckets && buckets > 1, "aliasing hash slots exercised");
    uint64_t previous_reads = 0;
    for (uint32_t limit : {1u, 17u, 1000u}) {
        zb_lhm_scan* raw = nullptr;
        Check(zb_lhm_scan_begin(ctx.get(), "/", 1, 7, &raw) == ZB_LHM_OK, "raw begin");
        Scan scan(raw, zb_lhm_scan_close);
        std::set<uint64_t> seen;
        const auto cb = +[](void* ptr, const zb_lhm_full_inode* a) -> int {
            return static_cast<std::set<uint64_t>*>(ptr)->insert(a->inode_id).second;
        };
        zb_lhm_scan_stats before{}, after{};
        int done = 0;
        for (unsigned calls = 0; !done && calls < 10000; ++calls) {
            const auto size = seen.size();
            Check(zb_lhm_scan_next(scan.get(), limit, cb, &seen, &done) == ZB_LHM_OK, "raw next");
            Check(seen.size() - size <= limit, "raw batch bound");
            Check(zb_lhm_scan_stats_for_test(scan.get(), &after) == ZB_LHM_OK, "stats");
            Check(after.steps - before.steps <= 7, "native work bound");
            before = after;
        }
        Check(done && seen.size() == static_cast<size_t>(files), "native complete set");
        Check(after.bucket_reads == buckets && after.file_entries == static_cast<uint64_t>(files), "one read per unique bucket");
        if (previous_reads) Check(previous_reads == after.bucket_reads, "page size does not multiply bucket reads");
        previous_reads = after.bucket_reads;
        std::cout << "PASS native page=" << limit << " files=" << files << " slots=" << slots
                  << " unique_buckets=" << buckets << " bucket_reads=" << after.bucket_reads << '\n';
    }
    zb_lhm_scan* raw = nullptr;
    Check(zb_lhm_scan_begin(ctx.get(), "/", 1, 1024, &raw) == ZB_LHM_OK, "begin failed read");
    Scan failed(raw, zb_lhm_scan_close);
    Check(zb_lhm_fail_next_lookup_read_for_test(ctx.get()), "arm read failure");
    int done = 0;
    const auto ok = +[](void*, const zb_lhm_full_inode*) -> int { return 1; };
    Check(zb_lhm_scan_next(failed.get(), 10, ok, nullptr, &done) == ZB_LHM_ERROR && !done, "read failure propagated");
    Check(zb_lhm_scan_next(failed.get(), 10, ok, nullptr, &done) == ZB_LHM_ERROR, "failure terminal");
    Check(zb_lhm_scan_begin(ctx.get(), "/", 1, 1024, &raw) == ZB_LHM_OK, "new cursor after failure");
    Scan fresh(raw, zb_lhm_scan_close);
    Check(zb_lhm_scan_next(fresh.get(), 1, ok, nullptr, &done) == ZB_LHM_OK, "fresh cursor independent");
    const auto reject = +[](void*, const zb_lhm_full_inode*) -> int { return 0; };
    Check(zb_lhm_scan_next(fresh.get(), 1, reject, nullptr, &done) == ZB_LHM_ERROR, "callback error terminal");
    std::cout << "PASS native read errors and callback failure\n";
}

int main(int argc, char** argv) {
    try {
        Check(argc == 2 && !std::filesystem::exists(argv[1]), "fresh work directory required");
        std::filesystem::create_directories(argv[1]);
        std::string dir = argv[1], error;
        RocksMetaStore db;
        Check(db.Open(dir + "/rocks", &error), "rocks open");
        RocksNamespaceStore rocks(&db);
        Contract(rocks, "Rocks");
        auto lhm = std::make_unique<LhmNamespaceStore>(dir + "/lhm.meta");
        Check(lhm->ready(), "lhm open");
        Contract(*lhm, "LHM");
        std::unique_ptr<NamespaceScanner> survivor;
        Check(lhm->BeginSubtreeScan("/tree/wide", {}, &survivor).ok(), "retained context");
        lhm.reset();
        SubtreeScanBatch batch;
        Check(survivor->Next(1, &batch).ok() && batch.entries.size() == 1, "scanner keeps LHM alive");
        survivor.reset();
        NativeChecks(dir + "/native.meta");

        // Corrupt a later Rocks inode to verify partial output is discarded.
        Create(rocks, "/bad", INODE_DIR);
        Create(rocks, "/bad/a", INODE_FILE);
        auto bad = Create(rocks, "/bad/z", INODE_FILE);
        std::unique_ptr<NamespaceScanner> scanner;
        Check(rocks.BeginSubtreeScan("/bad", {}, &scanner).ok(), "begin corrupt fixture");
        Check(db.Put(InodeKey(bad), "corrupt", &error), "corrupt inode");
        Check(scanner->Next(100, &batch).code == MDS_INTERNAL_ERROR && batch.entries.empty() && !batch.done, "no partial batch on error");
        Check(scanner->Next(100, &batch).code == MDS_INTERNAL_ERROR, "generic failure terminal");
        // Readdir itself must discard its already decoded first child.
        std::vector<Dentry> entries; bool more = true; std::string token;
        Check(!rocks.Readdir("/bad", "", 0, &entries, &more, &token, &error)
            && !error.empty() && entries.empty() && !more && token.empty(), "Readdir must not skip corrupt inode");
        std::cout << "PASS scan lifecycle and partial-batch error handling\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
