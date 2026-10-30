#include "mds/lhm/LhmNamespaceStore.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>
using namespace zb::mds;
using namespace zb::rpc;
namespace fs = std::filesystem;
void Check(bool ok, const std::string& why) { if (!ok) throw std::runtime_error(why); }
using Rows = std::vector<std::pair<std::string, std::string>>;
void Write(const fs::path& path, const Rows& rows) {
    std::ofstream out(path); out << "type\tpath\n";
    for (const auto& [type, name] : rows) out << type << '\t' << name << '\n';
    Check(bool(out), "write fixture");
}
uint64_t Create(NamespaceStore& ns, const std::string& path, bool dir) {
    std::string error;
    auto r = ns.ReserveCreate(path, dir ? INODE_DIR : INODE_FILE, &error);
    Check(bool(r), "reserve " + path + error);
    InodeAttr a;
    a.set_inode_id(r->inode_id()); a.set_type(dir ? INODE_DIR : INODE_FILE);
    a.set_file_name(path.substr(path.rfind('/') + 1));
    a.set_mode(dir ? 0755 : 0644); a.set_nlink(dir ? 2 : 1); a.set_version(1);
    Check(r->Commit(a).ok(), "commit " + path);
    return a.inode_id();
}
InodeAttr Lookup(NamespaceStore& ns, const std::string& path) {
    InodeAttr a, b; uint64_t id = 0; std::string error;
    Check(ns.ResolvePath(path, &id, &a, &error), "lookup " + path + error);
    Check(ns.GetInode(id, &b, &error) && a.SerializeAsString() == b.SerializeAsString(), "id lookup " + path);
    return a;
}
std::vector<std::string> List(NamespaceStore& ns, const std::string& path) {
    std::vector<std::string> names; bool more = true; std::string token, error;
    while (more) {
        std::vector<Dentry> batch; std::string next;
        Check(ns.Readdir(path, token, 7, &batch, &more, &next, &error), "readdir " + path + error);
        for (const auto& e : batch) names.push_back(e.name());
        Check(!more || next != token, "pagination progress"); token = next;
    }
    return names;
}
void Boundary(const fs::path& base, unsigned count) {
    const auto dir = base / ("boundary_" + std::to_string(count)); fs::create_directory(dir);
    Rows rows{{"dir", "/"}};
    for (unsigned i = 0; i < count; ++i) rows.emplace_back("file", "/f" + std::to_string(i));
    Write(dir / "input.tsv", rows);
    zb_lhm_bulk_stats stats{}; std::string error;
    LhmNamespaceStore ns((dir / "store").string(), (dir / "input.tsv").string(), &stats, &error);
    Check(ns.ready(), error);
    Check(stats.files == count && stats.inode_blocks == (count + 63) / 63, "block count");
    std::map<std::string, uint64_t> ids;
    for (unsigned i = 0; i < count; ++i) {
        const std::string path = "/f" + std::to_string(i);
        auto a = Lookup(ns, path);
        const uint64_t ticket = i + 1;
        Check(a.inode_id() == (((ticket / 63 + 1) << 32) | (ticket % 63)), "packed inode address");
        ids[path] = a.inode_id();
    }
    // Cross several normal allocator chunks after bulk construction.
    for (unsigned i = 0; i < 270; ++i) Create(ns, "/new" + std::to_string(i), false);
    for (const auto& [path, id] : ids) Check(Lookup(ns, path).inode_id() == id, "allocator overwrite");
    Check(List(ns, "/").size() == count + 270 && ns.Sync(), "boundary contents");
}
void Tree(const fs::path& base) {
    Rows rows{{"dir", "/"}, {"dir", "/wide"}, {"dir", "/empty"}, {"file", "/rootfile"}};
    for (unsigned i = 0; i < 650; ++i) rows.emplace_back("file", "/wide/file" + std::to_string(i));
    for (unsigned i = 0; i < 60; ++i) rows.emplace_back("dir", "/dir" + std::to_string(i));
    std::string deep = "/dir0";
    for (unsigned i = 0; i < 20; ++i) { deep += "/child"; rows.emplace_back("dir", deep); }
    rows.emplace_back("file", deep + "/" + std::string(64, 'x'));
    // Children of earlier sibling directories arrive late (valid BFS/mixed order).
    rows.emplace_back("file", "/dir59/file");
    Write(base / "tree.tsv", rows);
    zb_lhm_bulk_stats stats{}; std::string error;
    LhmNamespaceStore ns((base / "tree.store").string(), (base / "tree.tsv").string(), &stats, &error);
    LhmNamespaceStore reference((base / "reference.store").string());
    Check(ns.ready() && reference.ready(), "tree open " + error);
    Check(stats.files == 653 && stats.directories == 83 && stats.buckets > 4, "tree statistics");
    std::set<uint64_t> all_ids;
    for (size_t i = 0; i < rows.size(); ++i) {
        const auto& [type, path] = rows[i];
        if (i) Create(reference, path, type == "dir");
        auto a = Lookup(ns, path), b = Lookup(reference, path);
        Check(all_ids.insert(a.inode_id()).second, "unique ids");
        Check(a.type() == b.type() && a.file_name() == b.file_name() && a.mode() == b.mode() &&
              a.nlink() == b.nlink() && a.size() == 0 && a.version() == 1 &&
              !a.disk_node_id() && !a.optical_disk_id(), "default attrs " + path);
    }
    for (const auto& [type, path] : rows) if (type == "dir") {
        std::set<std::string> expected;
        for (size_t i = 1; i < rows.size(); ++i) {
            const auto& child = rows[i].second;
            auto parent = child.substr(0, child.rfind('/')); if (parent.empty()) parent = "/";
            if (parent == path) expected.insert(child.substr(child.rfind('/') + 1));
        }
        auto actual = List(ns, path);
        Check(actual == std::vector<std::string>(expected.begin(), expected.end()), "manifest list coverage " + path);
        Check(actual == List(reference, path), "list equivalence " + path);
    }
    std::unique_ptr<NamespaceScanner> scan;
    SubtreeScanOptions options; options.files_only = false; options.work_budget = 13;
    Check(ns.BeginSubtreeScan("/", options, &scan).ok(), "scan begin");
    std::set<uint64_t> scanned;
    for (;;) {
        SubtreeScanBatch batch; Check(scan->Next(11, &batch).ok(), "scan next");
        for (const auto& a : batch.entries) Check(scanned.insert(a.inode_id()).second, "scan duplicate");
        if (batch.done) break;
    }
    all_ids.erase(1); Check(scanned == all_ids, "scan coverage: got " + std::to_string(scanned.size()) + " expected " + std::to_string(all_ids.size())); scan.reset();
    const auto saved = Lookup(ns, "/wide/file0").inode_id();
    Check(ns.Rename("/wide", "/moved").ok(), "directory rename");
    Check(Lookup(ns, "/moved/file0").inode_id() == saved, "stable file id");
    Check(ns.Rename("/moved/file0", "/empty/renamed").ok(), "cross parent file rename");
    auto attr = Lookup(ns, "/empty/renamed"); attr.set_size(12345); attr.set_optical_image_id(123);
    Check(ns.PutInode(saved, attr, &error), "put inode");
    Check(Lookup(ns, "/empty/renamed").size() == 12345, "updated attrs");
    Check(ns.Remove("/empty/renamed", INODE_FILE).ok() && ns.Remove("/empty", INODE_DIR).ok(), "remove imported entries");
    Create(ns, "/empty", true); Create(ns, "/empty/new", false);
    for (unsigned i = 0; i < 750; ++i) Create(ns, "/moved/new" + std::to_string(i), false);
    Check(List(ns, "/moved").size() == 1399, "post-import bucket splits");
    for (unsigned i = 1; i < 650; ++i) Lookup(ns, "/moved/file" + std::to_string(i));
    Check(ns.Sync(), "sync");
    // Existing output must never be truncated, even for a valid manifest.
    const auto size = fs::file_size(base / "tree.store");
    LhmNamespaceStore again((base / "tree.store").string(), (base / "tree.tsv").string(), &stats, &error);
    Check(!again.ready() && !error.empty() && fs::file_size(base / "tree.store") == size, "existing output protection");
}
void Invalid(const fs::path& base) {
    const std::vector<std::string> cases = {"", "path\ttype\n", "type\tpath\n", "type\tpath\nfile\t/\n",
        "type\tpath\ndir\t/\nfile\t/missing/f\n", "type\tpath\ndir\t/\ndir\t/\n",
        "type\tpath\ndir\t/\nfile\t/f\nfile\t/f/c\n", "type\tpath\ndir\t/\nfile\t//f\n",
        "type\tpath\ndir\t/\nfile\t/a/../f\n", "type\tpath\ndir\t/\nfile\trelative\n",
        "type\tpath\ndir\t/\nfile\t/f\textra\n", "type\tpath\ndir\t/\nother\t/f\n",
        "type\tpath\ndir\t/\nfile\t/" + std::string(65, 'a') + "\n"};
    for (size_t i = 0; i < cases.size(); ++i) {
        const auto input = base / ("bad" + std::to_string(i) + ".tsv");
        { std::ofstream out(input); out << cases[i]; }
        std::string error; zb_lhm_bulk_stats stats{};
        const auto output = base / ("bad" + std::to_string(i) + ".store");
        LhmNamespaceStore ns(output.string(), input.string(), &stats, &error);
        Check(!ns.ready() && !error.empty() && !fs::exists(output), "reject invalid manifest " + std::to_string(i));
    }
}
int main(int argc, char** argv) {
    try {
        Check(argc == 2, "usage: lhm_bulk_import_test <new result directory>");
        const fs::path base = argv[1]; Check(fs::create_directory(base), "result directory must be new");
        Invalid(base);
        for (unsigned n : {0, 1, 62, 63, 64, 126, 256}) Boundary(base, n);
        Tree(base);
        std::cout << "PASS bulk import: invalid manifests, inode boundaries, reference equivalence, paging, scan, mutation, splits\n";
        return 0;
    } catch (const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
