#pragma once
#include <memory>
#include "LhmBridge.h"
#include "mds/storage/NamespaceStore.h"

namespace zb::mds {
// Full-attribute codec and scoped creation shared by the namespace adapter.
class LhmInodeStore {
public:
    explicit LhmInodeStore(const std::string& fresh_file);
    LhmInodeStore(const std::string& fresh_file, const std::string& manifest,
                  zb_lhm_bulk_stats* stats, std::string* error);
    bool ready() const { return bool(context_); }
    std::unique_ptr<NamespaceCreation> ReserveCreate(const std::string& path,
                                                    zb::rpc::InodeType type,
                                                    std::string* error);
    bool GetInode(uint64_t id, zb::rpc::InodeAttr* attr, std::string* error = nullptr) const;
    bool Lookup(const std::string& path, zb::rpc::InodeAttr* attr, std::string* error = nullptr) const;
    bool PutInode(const zb::rpc::InodeAttr& attr);
    bool Sync();
    uint64_t pending() const;
    static void DecodeAttributes(const zb_lhm_full_inode&, zb::rpc::InodeAttr*);
private:
    friend class LhmNamespaceStore;
    std::shared_ptr<zb_lhm_context> context_;
};
} // namespace zb::mds
