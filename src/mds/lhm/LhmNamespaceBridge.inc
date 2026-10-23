// Included by LhmBridge.cpp to share its private worker and native context.
namespace {
template<class F> int checked(zb_lhm_context* ctx, F f) {
    if (!ctx) return ZB_LHM_ERROR;
    try { return worker().run(f); } catch (const std::invalid_argument&) { return ZB_LHM_INVALID; } catch (...) { return ZB_LHM_ERROR; }
}
bool parse_service_path(const char* path, std::string& out) {
    if (!path || path[0] != '/') return false;
    auto parsed = MasstreeLHM::PathKey::parse_absolute_path(path);
    for (const auto& name : parsed.components)
        if (name.size() > 64 || name == "." || name == "..") return false;
    out = parsed.normalized_path;
    return true;
}
int lookup_checked(zb_lhm_context* ctx, const std::string& path,
                   zb_lhm_full_inode& out, threadinfo& ti) {
    MasstreeLHM::namespace_entry entry{};
    bool storage_error=false;
    if (!ctx->ns.lookup_entry_checked(path,entry,storage_error,ti))
        return storage_error ? ZB_LHM_ERROR : ZB_LHM_MISSING;
    MasstreeLHM::inode_disk inode{};
    const uint64_t id = path=="/" ? ctx->ns.root_inode_id()
        : MasstreeLHM::entry_is_directory(entry) ? (uint64_t{1}<<63)|entry.ref.block_id
        : (uint64_t(entry.ref.block_id)<<32)|entry.ref.offset;
    if (!ctx->ns.read_inode_by_id(id,inode) || !zb::mds::lhm_codec::Decode(inode,out))
        return ZB_LHM_ERROR;
    if (out.inode_id == ctx->ns.root_inode_id()) out.inode_id = 1;
    return ZB_LHM_OK;
}
int parent_checked(zb_lhm_context* ctx, const std::string& path, threadinfo& ti) {
    auto parent = path.substr(0,path.rfind('/'));
    zb_lhm_full_inode attr{};
    int rc=lookup_checked(ctx,parent.empty()?"/":parent,attr,ti);
    return rc != ZB_LHM_OK ? rc : attr.type == 2 ? ZB_LHM_OK : ZB_LHM_INVALID;
}
}
extern "C" {
#ifdef ZB_LHM_TESTING
int zb_lhm_fail_next_lookup_read_for_test(zb_lhm_context* ctx) {
    return call(ctx,[=](threadinfo&) {ctx->ns.fail_next_lookup_read_for_test(); return true;});
}
#endif
int zb_lhm_lookup_checked(zb_lhm_context* ctx, const char* path, zb_lhm_full_inode* out) {
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string normalized;
        if (!out || !parse_service_path(path,normalized)) return ZB_LHM_INVALID;
        return lookup_checked(ctx,normalized,*out,ti);
    });
}
int zb_lhm_read_checked(zb_lhm_context* ctx, uint64_t id, zb_lhm_full_inode* out) {
    return checked(ctx,[=](threadinfo&) -> int {
        if (!out || !zb::mds::LhmInodeId::Decode(id)) return ZB_LHM_INVALID;
        if (id != 1 && id == ctx->ns.root_inode_id()) return ZB_LHM_MISSING;
        MasstreeLHM::inode_disk inode{};
        bool storage_error=false;
        if (!ctx->ns.read_inode_by_id(native_id(ctx,id),inode,&storage_error))
            return storage_error ? ZB_LHM_ERROR : ZB_LHM_MISSING;
        if (!zb::mds::lhm_codec::Decode(inode,*out)) return ZB_LHM_ERROR;
        out->inode_id=id;
        return ZB_LHM_OK;
    });
}
int zb_lhm_reserve_checked(zb_lhm_context* ctx, const char* path, uint8_t type,
                          uint64_t* token,uint64_t* id) {
    if (token) *token=0;
    if (id) *id=0;
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string normalized;
        if (!token || !id || (type!=1 && type!=2) || !parse_service_path(path,normalized) || normalized=="/") return ZB_LHM_INVALID;
        zb_lhm_full_inode existing{};
        int rc=lookup_checked(ctx,normalized,existing,ti);
        if (rc==ZB_LHM_OK || ctx->ns.path_reserved(normalized)) return ZB_LHM_EXISTS;
        if (rc!=ZB_LHM_MISSING) return rc;
        rc=parent_checked(ctx,normalized,ti);
        if (rc!=ZB_LHM_OK) return rc;
        return ctx->ns.reserve_create(normalized,type==2,*token,*id,ti) ? ZB_LHM_OK : ZB_LHM_ERROR;
    });
}
int zb_lhm_commit_checked(zb_lhm_context* ctx,uint64_t token,const zb_lhm_full_inode* attr) {
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string path;
        if (!attr || !zb::mds::lhm_codec::Valid(*attr) || !ctx->ns.reservation_path(token,path)) return ZB_LHM_INVALID;
        if (path.substr(path.rfind('/')+1)!=std::string(attr->file_name,attr->name_length)) return ZB_LHM_INVALID;
        int rc=parent_checked(ctx,path,ti);
        if (rc!=ZB_LHM_OK) return rc;
        zb_lhm_full_inode existing{};
        rc=lookup_checked(ctx,path,existing,ti);
        if (rc==ZB_LHM_OK) return ZB_LHM_EXISTS;
        if (rc!=ZB_LHM_MISSING) return rc;
        MasstreeLHM::inode_disk inode{};
        if (!zb::mds::lhm_codec::Encode(*attr,inode)) return ZB_LHM_INVALID;
        return ctx->ns.commit_create(token,inode,ti) ? ZB_LHM_OK : ZB_LHM_ERROR;
    });
}
int zb_lhm_rename_checked(zb_lhm_context* ctx,const char* from,const char* to) {
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string source,target;
        if (!parse_service_path(from,source) || !parse_service_path(to,target) || source=="/" || target=="/") return ZB_LHM_INVALID;
        zb_lhm_full_inode attr{},existing{};
        int rc=lookup_checked(ctx,source,attr,ti);
        if (rc!=ZB_LHM_OK) return rc;
        rc=lookup_checked(ctx,target,existing,ti);
        if (rc==ZB_LHM_OK || ctx->ns.path_reserved(target)) return ZB_LHM_EXISTS;
        if (rc!=ZB_LHM_MISSING) return rc;
        if (attr.type==2 && target.compare(0,source.size()+1,source+"/")==0) return ZB_LHM_INVALID;
        rc=parent_checked(ctx,target,ti);
        if (rc!=ZB_LHM_OK) return rc;
        auto name=target.substr(target.rfind('/')+1);
        attr.name_length=name.size();
        std::memset(attr.file_name,0,sizeof(attr.file_name));
        std::memcpy(attr.file_name,name.data(),name.size());
        attr.inode_id=native_id(ctx,attr.inode_id);
        MasstreeLHM::inode_disk encoded{};
        if (!zb::mds::lhm_codec::Encode(attr,encoded)) return ZB_LHM_ERROR;
        if (!ctx->ns.rename_path(source,target,ti)) return ZB_LHM_ERROR;
        // Restore application timestamps/version and update the full-attribute name.
        // Structural refs/generation from the native rename remain intact.
        return ctx->ns.replace_inode_attributes(attr.inode_id,encoded) ? ZB_LHM_OK : ZB_LHM_ERROR;
    });
}
int zb_lhm_remove_checked(zb_lhm_context* ctx,const char* path,uint8_t type) {
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string normalized;
        if ((type!=1 && type!=2) || !parse_service_path(path,normalized) || normalized=="/") return ZB_LHM_INVALID;
        zb_lhm_full_inode attr{};
        int rc=lookup_checked(ctx,normalized,attr,ti);
        if (rc!=ZB_LHM_OK) return rc;
        if (attr.type!=type) return ZB_LHM_INVALID;
        if (type==2) {
            std::vector<MasstreeLHM::readdir_record> entries;
            if (!ctx->ns.readdir_checked(normalized,entries,ti)) return ZB_LHM_ERROR;
            if (!entries.empty()) return ZB_LHM_NOT_EMPTY;
        }
        return ctx->ns.remove_path(normalized,ti) ? ZB_LHM_OK : ZB_LHM_ERROR;
    });
}
int zb_lhm_readdir_checked(zb_lhm_context* ctx,const char* path,const char* after,
                          uint32_t limit,zb_lhm_entry_callback callback,void* opaque,int* more) {
    if (more) *more=0;
    return checked(ctx,[=](threadinfo& ti) -> int {
        std::string normalized;
        if (!callback || !after || !parse_service_path(path,normalized)) return ZB_LHM_INVALID;
        zb_lhm_full_inode attr{};
        int rc=lookup_checked(ctx,normalized,attr,ti);
        if (rc!=ZB_LHM_OK) return rc;
        if (attr.type!=2) return ZB_LHM_INVALID;
        std::vector<MasstreeLHM::readdir_record> entries;
        if (!ctx->ns.readdir_checked(normalized,entries,ti)) return ZB_LHM_ERROR;
        std::sort(entries.begin(),entries.end(),[](const auto& a,const auto& b){return a.child_name<b.child_name;});
        uint32_t count=0;
        for (const auto& entry:entries) {
            if (entry.child_name<=after) continue;
            if (limit && count==limit) { if(more) *more=1; break; }
            // Read by encoded address; no per-child path traversal.
            uint64_t id=MasstreeLHM::entry_is_directory(entry.entry)
                ? (uint64_t{1}<<63)|entry.entry.ref.block_id
                : (uint64_t(entry.entry.ref.block_id)<<32)|entry.entry.ref.offset;
            MasstreeLHM::inode_disk inode{};
            zb_lhm_full_inode full{};
            if (!ctx->ns.read_inode_by_id(id,inode) || !zb::mds::lhm_codec::Decode(inode,full)) return ZB_LHM_ERROR;
            if (!callback(opaque,entry.child_name.data(),entry.child_name.size(),id,full.type)) return ZB_LHM_ERROR;
            ++count;
        }
        return ZB_LHM_OK;
    });
}
}
