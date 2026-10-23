#pragma once

#include <mutex>
#include <functional>
#include <utility>

#include "NamespaceStore.h"
#include "RocksMetaStore.h"

namespace zb::mds {

// Uses the original D/, I/ and next-inode encoding; does not own store.
class RocksNamespaceStore final : public NamespaceStore {
public:
    using InodeReader = std::function<bool(uint64_t, zb::rpc::InodeAttr*, std::string*)>;
    // A hot dentry may refer to an inode served by a cold route. Only path and
    // directory reads use this resolver; GetInode itself always stays hot-only.
    explicit RocksNamespaceStore(RocksMetaStore* store, InodeReader visible_inode = {})
        : store_(store), visible_inode_(std::move(visible_inode)) {}
    bool ResolvePath(const std::string&, uint64_t*, zb::rpc::InodeAttr*, std::string*) const override;
    bool GetInode(uint64_t, zb::rpc::InodeAttr*, std::string*) const override;
    bool PutInode(uint64_t, const zb::rpc::InodeAttr&, std::string*) override;
    uint64_t AllocateInodeId(zb::rpc::InodeType, std::string*) override;
    NamespaceStatus Create(const std::string&, const zb::rpc::InodeAttr&) override;
    NamespaceStatus Rename(const std::string&, const std::string&) override;
    NamespaceStatus Remove(const std::string&, zb::rpc::InodeType) override;
    bool Readdir(const std::string&, const std::string&, uint32_t,
                 std::vector<zb::rpc::Dentry>*, bool*, std::string*, std::string*) const override;

private:
    bool ResolveParent(const std::string&, uint64_t*, std::string*, std::string*) const;
    bool GetDentry(uint64_t, const std::string&, uint64_t*, std::string*) const;
    bool ReadVisibleInode(uint64_t id, zb::rpc::InodeAttr* attr, std::string* error) const {
        return visible_inode_ ? visible_inode_(id, attr, error) : GetInode(id, attr, error);
    }
    RocksMetaStore* store_;
    InodeReader visible_inode_;
    std::mutex inode_id_mu_;
};

} // namespace zb::mds
