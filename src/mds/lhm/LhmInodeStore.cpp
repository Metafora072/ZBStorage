#include "LhmInodeStore.h"
#include "LhmStatus.h"
#include <cstring>

namespace zb::mds {
namespace {
bool Encode(const zb::rpc::InodeAttr& a, zb_lhm_full_inode* out) {
    if (a.file_name().size() > 64 || (a.type() != zb::rpc::INODE_FILE && a.type() != zb::rpc::INODE_DIR) ||
        !zb::rpc::InodeArchiveState_IsValid(a.file_archive_state()) ||
        !zb::rpc::InodeStorageTier_IsValid(a.storage_tier())) return false;
    *out = {};
    out->inode_id = a.inode_id();
    out->size = a.size();
    out->atime = a.atime();
    out->mtime = a.mtime();
    out->ctime = a.ctime();
    out->object_unit_size = a.object_unit_size();
    out->version = a.version();
    out->disk_node_id = a.disk_node_id();
    out->optical_node_id = a.optical_node_id();
    out->optical_disk_id = a.optical_disk_id();
    out->blocks_512 = a.blocks_512();
    out->mode = a.mode();
    out->uid = a.uid();
    out->gid = a.gid();
    out->nlink = a.nlink();
    out->replica = a.replica();
    out->disk_id = a.disk_id();
    out->optical_image_id = a.optical_image_id();
    out->blksize = a.blksize();
    out->rdev_major = a.rdev_major();
    out->rdev_minor = a.rdev_minor();
    out->type = a.type() == zb::rpc::INODE_FILE ? 1 : 2;
    out->archive_state = a.file_archive_state(); out->storage_tier = a.storage_tier();
    out->disk_node_is_virtual = a.disk_node_is_virtual();
    out->name_length = a.file_name().size();
    std::memcpy(out->file_name,a.file_name().data(),out->name_length);
    return true;
}
void Decode(const zb_lhm_full_inode& a, zb::rpc::InodeAttr* out) {
    out->Clear();
    out->set_inode_id(a.inode_id);
    out->set_size(a.size);
    out->set_atime(a.atime);
    out->set_mtime(a.mtime);
    out->set_ctime(a.ctime);
    out->set_object_unit_size(a.object_unit_size);
    out->set_version(a.version);
    out->set_disk_node_id(a.disk_node_id);
    out->set_optical_node_id(a.optical_node_id);
    out->set_optical_disk_id(a.optical_disk_id);
    out->set_blocks_512(a.blocks_512);
    out->set_mode(a.mode);
    out->set_uid(a.uid);
    out->set_gid(a.gid);
    out->set_nlink(a.nlink);
    out->set_replica(a.replica);
    out->set_disk_id(a.disk_id);
    out->set_optical_image_id(a.optical_image_id);
    out->set_blksize(a.blksize);
    out->set_rdev_major(a.rdev_major);
    out->set_rdev_minor(a.rdev_minor);
    out->set_type(a.type == 1 ? zb::rpc::INODE_FILE : zb::rpc::INODE_DIR);
    out->set_file_archive_state(static_cast<zb::rpc::InodeArchiveState>(a.archive_state));
    out->set_storage_tier(static_cast<zb::rpc::InodeStorageTier>(a.storage_tier));
    out->set_disk_node_is_virtual(a.disk_node_is_virtual);
    out->set_file_name(a.file_name,a.name_length);
}
class Creation final : public NamespaceCreation {
    std::shared_ptr<zb_lhm_context> context_;
    uint64_t token_, id_;
    zb::rpc::InodeType type_;
    bool finished_ = false;
public:
    Creation(std::shared_ptr<zb_lhm_context> context, uint64_t token, uint64_t id, zb::rpc::InodeType type)
        : context_(std::move(context)), token_(token), id_(id), type_(type) {}
    ~Creation() override { Cancel(); }
    uint64_t inode_id() const override { return id_; }
    NamespaceStatus Commit(const zb::rpc::InodeAttr& attr) override {
        zb_lhm_full_inode raw{};
        if (finished_ || attr.inode_id() != id_ || attr.type() != type_ || !Encode(attr,&raw))
            return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid creation attributes"};
        auto status=LhmStatus(zb_lhm_commit_checked(context_.get(),token_,&raw));
        if (!status.ok()) return status;
        finished_ = true;
        return {};
    }
    bool Cancel() override {
        if (finished_) return true;
        if (!zb_lhm_cancel(context_.get(),token_)) return false;
        finished_ = true;
        return true;
    }
};
} // namespace
void LhmInodeStore::DecodeAttributes(const zb_lhm_full_inode& raw, zb::rpc::InodeAttr* out) {
    Decode(raw, out);
}
LhmInodeStore::LhmInodeStore(const std::string& file)
    : context_(zb_lhm_open(file.c_str()),zb_lhm_close) {}
std::unique_ptr<NamespaceCreation> LhmInodeStore::ReserveCreate(
    const std::string& path, zb::rpc::InodeType type, std::string* error) {
    if (error) error->clear();
    uint64_t token=0,id=0;
    if (path.find('\0') != std::string::npos || (type != zb::rpc::INODE_FILE && type != zb::rpc::INODE_DIR)) {
        if (error) *error="invalid creation arguments";
        return nullptr;
    }
    auto status=LhmStatus(zb_lhm_reserve_checked(context_.get(),path.c_str(),type == zb::rpc::INODE_FILE ? 1 : 2,&token,&id));
    if (!status.ok()) { if(error) *error=status.message; return nullptr; }
    return std::make_unique<Creation>(context_,token,id,type);
}
bool LhmInodeStore::GetInode(uint64_t id, zb::rpc::InodeAttr* out, std::string* error) const {
    zb_lhm_full_inode raw{};
    if (!LhmReadResult(zb_lhm_read_checked(context_.get(),id,out ? &raw : nullptr),error)) return false;
    Decode(raw,out); return true;
}
bool LhmInodeStore::Lookup(const std::string& path, zb::rpc::InodeAttr* out, std::string* error) const {
    zb_lhm_full_inode raw{};
    if (path.find('\0') != std::string::npos) return LhmReadResult(ZB_LHM_INVALID,error);
    if (!LhmReadResult(zb_lhm_lookup_checked(context_.get(),path.c_str(),out ? &raw : nullptr),error)) return false;
    Decode(raw,out); return true;
}
bool LhmInodeStore::PutInode(const zb::rpc::InodeAttr& attr) {
    zb_lhm_full_inode raw{};
    return Encode(attr,&raw) && zb_lhm_update_full(context_.get(),&raw);
}
bool LhmInodeStore::Sync() { return zb_lhm_sync(context_.get()); }
uint64_t LhmInodeStore::pending() const { return zb_lhm_pending(context_.get()); }
} // namespace zb::mds
