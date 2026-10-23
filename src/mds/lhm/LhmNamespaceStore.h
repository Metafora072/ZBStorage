#pragma once
#include "LhmInodeStore.h"
namespace zb::mds {
// Fresh-store namespace backend. All native operations use the bridge worker.
class LhmNamespaceStore final : public NamespaceStore {
public:
    explicit LhmNamespaceStore(const std::string& fresh_file) : inodes_(fresh_file) {}
    bool ready() const { return inodes_.ready(); }
    bool Sync() { return inodes_.Sync(); }
    bool ResolvePath(const std::string&,uint64_t*,zb::rpc::InodeAttr*,std::string*) const override;
    bool GetInode(uint64_t,zb::rpc::InodeAttr*,std::string*) const override;
    bool PutInode(uint64_t,const zb::rpc::InodeAttr&,std::string*) override;
    std::unique_ptr<NamespaceCreation> ReserveCreate(const std::string&,zb::rpc::InodeType,std::string*) override;
    uint64_t AllocateInodeId(zb::rpc::InodeType,std::string*) override;
    NamespaceStatus Create(const std::string&,const zb::rpc::InodeAttr&) override;
    NamespaceStatus Rename(const std::string&,const std::string&) override;
    NamespaceStatus Remove(const std::string&,zb::rpc::InodeType) override;
    bool Readdir(const std::string&,const std::string&,uint32_t,std::vector<zb::rpc::Dentry>*,bool*,std::string*,std::string*) const override;
    NamespaceStatus BeginSubtreeScan(const std::string&, const SubtreeScanOptions&,
        std::unique_ptr<NamespaceScanner>*) const override;
private:
    LhmInodeStore inodes_;
};
}
