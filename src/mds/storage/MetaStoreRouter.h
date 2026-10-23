#pragma once

#include <string>
#include <vector>

#include "../archive_meta/ArchiveMetaStore.h"
#include "../archive_meta/ArchiveNamespaceCatalog.h"
#include "../masstree_meta/MasstreeMetaStore.h"
#include "../masstree_meta/MasstreeNamespaceCatalog.h"
#include "NamespaceStore.h"
#include "mds.pb.h"

namespace zb::mds {

class MetaStoreRouter {
public:
    MetaStoreRouter(NamespaceStore* hot_store,
                    ArchiveNamespaceCatalog* archive_catalog,
                    ArchiveMetaStore* archive_store,
                    MasstreeNamespaceCatalog* masstree_catalog,
                    MasstreeMetaStore* masstree_store, bool unified = false);

    bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool ResolvePath(const std::string& path, uint64_t* inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool Readdir(const std::string& path,
                 const std::string& start_after,
                 uint32_t limit,
                 std::vector<zb::rpc::Dentry>* entries,
                 bool* has_more,
                 std::string* next_token,
                 std::string* error) const;

private:
    bool GetArchiveInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool GetMasstreeInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool unified_ = false;
    NamespaceStore* hot_store_{};
    ArchiveNamespaceCatalog* archive_catalog_{};
    ArchiveMetaStore* archive_store_{};
    MasstreeNamespaceCatalog* masstree_catalog_{};
    MasstreeMetaStore* masstree_store_{};
};

} // namespace zb::mds
