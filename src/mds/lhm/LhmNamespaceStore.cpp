#include "LhmNamespaceStore.h"
#include "LhmStatus.h"
namespace zb::mds {
namespace {
bool HasNul(const std::string& s) { return s.find('\0')!=std::string::npos; }
class LhmScanner final : public NamespaceScanner {
    // Keep the store alive until after closing its cursor.
    std::shared_ptr<zb_lhm_context> context_;
    std::unique_ptr<zb_lhm_scan, decltype(&zb_lhm_scan_close)> scan_;
public:
    LhmScanner(std::shared_ptr<zb_lhm_context> context, zb_lhm_scan* scan)
        : context_(std::move(context)), scan_(scan, zb_lhm_scan_close) {}
    NamespaceStatus Next(uint32_t max_entries, SubtreeScanBatch* batch) override {
        if (batch) *batch = {};
        if (!batch || !max_entries) return LhmStatus(ZB_LHM_INVALID);
        SubtreeScanBatch result;
        const auto callback = +[](void* opaque, const zb_lhm_full_inode* raw) -> int {
            auto& out = *static_cast<SubtreeScanBatch*>(opaque);
            zb::rpc::InodeAttr attr;
            LhmInodeStore::DecodeAttributes(*raw, &attr);
            out.entries.push_back(std::move(attr));
            return 1;
        };
        int done = 0;
        auto status = LhmStatus(zb_lhm_scan_next(scan_.get(), max_entries, callback, &result, &done));
        if (!status.ok()) return status;
        result.done = done;
        *batch = std::move(result);
        return {};
    }
};
}
NamespaceStatus LhmNamespaceStore::BeginSubtreeScan(const std::string& path,
    const SubtreeScanOptions& options, std::unique_ptr<NamespaceScanner>* scanner) const {
    if (scanner) scanner->reset();
    if (!scanner || HasNul(path)) return LhmStatus(ZB_LHM_INVALID);
    zb_lhm_scan* raw = nullptr;
    auto status = LhmStatus(zb_lhm_scan_begin(inodes_.context_.get(), path.c_str(),
                                            options.files_only, options.work_budget, &raw));
    if (!status.ok()) return status;
    std::unique_ptr<zb_lhm_scan, decltype(&zb_lhm_scan_close)> guard(raw, zb_lhm_scan_close);
    *scanner = std::make_unique<LhmScanner>(inodes_.context_, raw);
    guard.release();
    return {};
}
bool LhmNamespaceStore::ResolvePath(const std::string& path,uint64_t* id,zb::rpc::InodeAttr* attr,std::string* error) const {
    if (!id) return LhmReadResult(ZB_LHM_INVALID,error);
    zb::rpc::InodeAttr found;
    if (!inodes_.Lookup(path,&found,error)) return false;
    *id=found.inode_id();
    if(attr) *attr=std::move(found);
    return true;
}
bool LhmNamespaceStore::GetInode(uint64_t id,zb::rpc::InodeAttr* attr,std::string* error) const {
    return inodes_.GetInode(id,attr,error);
}
bool LhmNamespaceStore::PutInode(uint64_t id,const zb::rpc::InodeAttr& attr,std::string* error) {
    if(id!=attr.inode_id()) return LhmReadResult(ZB_LHM_INVALID,error);
    return LhmReadResult(inodes_.PutInode(attr)?ZB_LHM_OK:ZB_LHM_ERROR,error);
}
std::unique_ptr<NamespaceCreation> LhmNamespaceStore::ReserveCreate(const std::string& path,zb::rpc::InodeType type,std::string* error) {
    return inodes_.ReserveCreate(path,type,error);
}
uint64_t LhmNamespaceStore::AllocateInodeId(zb::rpc::InodeType,std::string* error) {
    if(error) *error="LHM requires path-bound ReserveCreate";
    return 0;
}
NamespaceStatus LhmNamespaceStore::Create(const std::string&,const zb::rpc::InodeAttr&) {
    return {zb::rpc::MDS_INVALID_ARGUMENT,"use ReserveCreate followed by Commit for LHM"};
}
NamespaceStatus LhmNamespaceStore::Rename(const std::string& from,const std::string& to) {
    if(HasNul(from)||HasNul(to)) return LhmStatus(ZB_LHM_INVALID);
    return LhmStatus(zb_lhm_rename_checked(inodes_.context_.get(),from.c_str(),to.c_str()));
}
NamespaceStatus LhmNamespaceStore::Remove(const std::string& path,zb::rpc::InodeType type) {
    if(HasNul(path)||(type!=zb::rpc::INODE_FILE && type!=zb::rpc::INODE_DIR)) return LhmStatus(ZB_LHM_INVALID);
    return LhmStatus(zb_lhm_remove_checked(inodes_.context_.get(),path.c_str(),type==zb::rpc::INODE_FILE?1:2));
}
bool LhmNamespaceStore::Readdir(const std::string& path,const std::string& after,uint32_t limit,
    std::vector<zb::rpc::Dentry>* entries,bool* more,std::string* token,std::string* error) const {
    // Copy before clearing outputs: callers may pass next_token as start_after.
    const auto cursor=after;
    if(entries) entries->clear();
    if(more) *more=false;
    if(token) token->clear();
    if(!entries||HasNul(path)||HasNul(cursor)) return LhmReadResult(ZB_LHM_INVALID,error);
    std::vector<zb::rpc::Dentry> result;
    int has_more=0;
    const auto callback=+[](void* opaque,const char* name,uint32_t len,uint64_t id,uint8_t type)->int {
        auto& out=*static_cast<std::vector<zb::rpc::Dentry>*>(opaque);
        zb::rpc::Dentry entry;
        entry.set_name(name,len); entry.set_inode_id(id);
        entry.set_type(type==1?zb::rpc::INODE_FILE:zb::rpc::INODE_DIR);
        out.push_back(std::move(entry)); return 1;
    };
    int rc=zb_lhm_readdir_checked(inodes_.context_.get(),path.c_str(),cursor.c_str(),limit,callback,&result,&has_more);
    if(!LhmReadResult(rc,error)) return false;
    *entries=std::move(result);
    if(more) *more=has_more;
    if(token && !entries->empty()) *token=entries->back().name();
    return true;
}
}
