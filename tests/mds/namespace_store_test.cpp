#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "mds/storage/MetaCodec.h"
#include "mds/storage/MetaSchema.h"
#include "mds/storage/RocksNamespaceStore.h"

using namespace zb::mds;

static void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

// The explicit argument must be a fresh directory outside the source tree.
// Exercise the semantic interface and the metadata that must survive extraction.
int main(int argc, char** argv) {
    try {
        Check(argc == 2, "usage: namespace_store_test <fresh-work-dir>");
        Check(!std::filesystem::exists(argv[1]), "work directory already exists");
        std::filesystem::create_directories(argv[1]);
        RocksMetaStore db;
        std::string error;
        Check(db.Open(std::string(argv[1]) + "/db", &error), "open");
        RocksNamespaceStore rocks(&db);
        NamespaceStore& ns = rocks;
        Check(ns.AllocateInodeId(static_cast<zb::rpc::InodeType>(99), &error) == 0 &&
              !error.empty(), "invalid allocation type rejected");
        zb::rpc::InodeAttr root;
        root.set_inode_id(1);
        root.set_type(zb::rpc::INODE_DIR);
        root.set_file_name("/");
        Check(ns.PutInode(1, root, &error), "root");

        auto dir = root;
        dir.set_inode_id(ns.AllocateInodeId(zb::rpc::INODE_DIR, &error));
        Check(ns.Create("/dir", dir).ok(), "mkdir");
        auto file = root;
        file.set_inode_id(ns.AllocateInodeId(zb::rpc::INODE_FILE, &error));
        Check(dir.inode_id() == 2 && file.inode_id() == 3 && error.empty(),
              "typed allocation preserves Rocks IDs and invalid calls consume no ID");
        file.set_type(zb::rpc::INODE_FILE);
        file.set_mode(0640);
        file.set_uid(12);
        file.set_gid(34);
        file.set_storage_tier(zb::rpc::INODE_STORAGE_DISK);
        file.set_disk_node_id(17);
        file.set_disk_id(5);
        file.set_disk_node_is_virtual(true);
        Check(ns.Create("/dir/file", file).ok(), "create");
        Check(ns.Create("/dir/file", file).code == zb::rpc::MDS_ALREADY_EXISTS, "duplicate");
        Check(ns.Create("/missing/file", file).code == zb::rpc::MDS_NOT_FOUND, "missing parent");
        Check(ns.Remove("/dir", zb::rpc::INODE_DIR).code == zb::rpc::MDS_NOT_EMPTY, "nonempty");

        zb::rpc::InodeAttr attr;
        uint64_t id = 0;
        Check(ns.ResolvePath("/dir/file", &id, &attr, &error), "lookup");
        Check(id == file.inode_id() && attr.disk_node_is_virtual(), "stable ID and tier flags");
        attr.set_size(12345);
        Check(ns.PutInode(id, attr, &error), "update");
        Check(ns.Rename("/dir", "/moved").ok(), "directory rename");
        Check(ns.ResolvePath("/moved/file", &id, &attr, &error), "descendant lookup");
        Check(attr.size() == 12345 && attr.uid() == 12 && attr.disk_id() == 5, "preserved attributes");
        Check(ns.Rename("/moved/file", "/renamed").ok(), "file rename");
        std::string payload;
        UnifiedInodeRecord record;
        Check(db.Get(InodeKey(id), &payload, &error) &&
              MetaCodec::DecodeUnifiedInodeRecord(payload, &record, &error), "record decode");
        Check(record.parent_inode_id == 1 && record.file_name == "renamed" &&
              record.disk_node_id == 17 && record.disk_id == 5, "layout and location preserved");

        std::vector<zb::rpc::Dentry> entries;
        bool more = false;
        std::string token;
        Check(ns.Readdir("/", "", 1, &entries, &more, &token, &error), "page one");
        Check(entries.size() == 1 && entries[0].name() == "moved" && more && token == "moved", "page contract");
        const std::string start_after = token;
        Check(ns.Readdir("/", start_after, 1, &entries, &more, &token, &error), "page two");
        Check(entries.size() == 1 && entries[0].name() == "renamed" && !more, "page two contract");
        Check(ns.Remove("/renamed", zb::rpc::INODE_DIR).code == zb::rpc::MDS_INVALID_ARGUMENT, "type check");
        Check(ns.Remove("/renamed", zb::rpc::INODE_FILE).ok(), "unlink");
        error = "stale";
        Check(!ns.GetInode(id, &attr, &error) && error.empty(), "not found clears error");
        Check(ns.Remove("/moved", zb::rpc::INODE_DIR).ok(), "rmdir");

        // Preserve mixed hot-dentry/cold-inode routing without exposing KV to
        // NamespaceStore. GetInode stays local, avoiding recursive fallback.
        Check(db.Put(DentryKey(1, "cold"), MetaCodec::EncodeUInt64(900), &error), "cold dentry fixture");
        RocksNamespaceStore mixed(&db, [&](uint64_t inode, zb::rpc::InodeAttr* out, std::string* err) {
            if (inode != 900) return rocks.GetInode(inode, out, err);
            *out = root;
            out->set_inode_id(inode);
            return true;
        });
        Check(mixed.ResolvePath("/cold", &id, &attr, &error) && id == 900, "cold inode fallback");
        Check(mixed.Readdir("/", "", 0, &entries, &more, &token, &error) &&
              entries.size() == 1 && entries[0].inode_id() == 900, "cold readdir fallback");
        Check(!mixed.GetInode(900, &attr, &error) && error.empty(), "local GetInode boundary");

        // A malformed existing inode must not be overwritten by an attr update.
        Check(db.Put(InodeKey(901), "broken", &error), "corrupt fixture");
        root.set_inode_id(901);
        Check(!ns.PutInode(901, root, &error) && !error.empty(), "decode failure propagated");
        uint64_t canceled_id = 0;
        {
            auto creation = ns.ReserveCreate("/not-published", zb::rpc::INODE_FILE, &error);
            Check(bool(creation), "reserve through common interface");
            canceled_id = creation->inode_id();
        }
        Check(!ns.GetInode(canceled_id, &attr, &error) && error.empty(), "scoped cancellation published inode");
        auto creation = ns.ReserveCreate("/published", zb::rpc::INODE_FILE, &error);
        auto created = file;
        created.set_inode_id(creation->inode_id());
        Check(creation->Commit(created).ok(), "scoped commit");
        Check(!creation->Commit(created).ok(), "double scoped commit");
        Check(ns.ResolvePath("/published", &id, &attr, &error) && id == created.inode_id(), "scoped publication");
        std::cout << "PASS: namespace contract, location preservation, cold fallback, error propagation\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
