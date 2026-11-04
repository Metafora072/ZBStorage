#include <charconv>
#include <set>
namespace {
uint64_t ParseNumber(const std::string& s) {
    uint64_t n=0; auto r=std::from_chars(s.data(),s.data()+s.size(),n);
    if(s.empty() || r.ec!=std::errc() || r.ptr!=s.data()+s.size())
        throw std::runtime_error("expected unsigned decimal integer");
    return n;
}
// getline would allocate an arbitrarily long malformed record before checking.
bool BoundedLine(std::istream& in,std::string& line,size_t max) {
    line.clear(); char c;
    while(in.get(c)) { if(c=='\n') return true;
        if(line.size()==max) throw std::runtime_error("record exceeds parser memory budget");
        line.push_back(c); }
    if(in.bad()) throw std::runtime_error("manifest read failure");
    return !line.empty();
}
std::vector<std::string> Columns(const std::string& line) {
    std::vector<std::string> values; size_t begin=0;
    for(;;) { auto end=line.find('\t',begin); values.push_back(line.substr(begin,end==std::string::npos?end:end-begin));
        if(end==std::string::npos) return values; begin=end+1; }
}
void SetImportField(zb_lhm_full_inode& a,const std::string& key,uint64_t n) {
#define FIELD64(name) if(key==#name) { a.name=n; return; }
#define FIELD32(name) if(key==#name) { if(n>UINT32_MAX) throw std::runtime_error("attribute overflow: "+key); a.name=n; return; }
#define FIELD8(name) if(key==#name) { if(n>UINT8_MAX) throw std::runtime_error("attribute overflow: "+key); a.name=n; return; }
    FIELD64(size) FIELD64(atime) FIELD64(mtime) FIELD64(ctime) FIELD64(object_unit_size)
    FIELD64(version) FIELD64(disk_node_id) FIELD64(optical_node_id) FIELD64(optical_disk_id) FIELD64(blocks_512)
    FIELD32(mode) FIELD32(uid) FIELD32(gid) FIELD32(nlink) FIELD32(replica) FIELD32(disk_id)
    FIELD32(optical_image_id) FIELD32(blksize) FIELD32(rdev_major) FIELD32(rdev_minor)
    FIELD8(archive_state) FIELD8(storage_tier) FIELD8(disk_node_is_virtual)
#undef FIELD64
#undef FIELD32
#undef FIELD8
    throw std::runtime_error("unsupported import attribute: "+key);
}
}
extern "C" int zb_lhm_import_manifest(zb_lhm_context* ctx,const char* target,const char* manifest,
    uint64_t budget,zb_lhm_import_result* result,char* error,size_t size) {
    Error(error,size,""); if(result) *result={};
    if(!ctx || !target || !manifest || !result || budget<(1ULL<<20)) {
        Error(error,size,"context, target, manifest, result and at least 1 MiB budget required"); return ZB_LHM_INVALID;
    }
    try { return worker().run([&](threadinfo& ti)->int {
        uint64_t line_number=0;
        try {
            const size_t max_record=std::min<uint64_t>(budget/128,64*1024);
            if(std::strlen(target)>max_record) throw std::runtime_error("target exceeds parser memory budget");
            auto mount=MasstreeLHM::PathKey::parse_absolute_path(target);
            if(mount.normalized_path!=target || mount.components.empty() || target[0]!='/')
                throw std::runtime_error("target must be a canonical non-root path");
            std::ifstream input(manifest,std::ios::binary);
            if(!input) throw std::runtime_error("cannot open manifest");
            // Up to 32 columns, each copied only a bounded number of times.
            std::string line;
            ++line_number;
            if(!BoundedLine(input,line,max_record)) throw std::runtime_error("empty manifest");
            auto columns=Columns(line);
            if(columns.size()<2 || columns.size()>32 || columns[0]!="type" || columns[1]!="path")
                throw std::runtime_error("expected type<TAB>path followed by optional attribute columns");
            std::set<std::string> unique;
            for(size_t i=0;i<columns.size();++i) {
                if(!unique.insert(columns[i]).second) throw std::runtime_error("duplicate column");
                if(i>=2) { zb_lhm_full_inode test{}; SetImportField(test,columns[i],0); }
            }
            const auto now=std::time(nullptr);
            for(;;) {
                ++line_number;
                if(!BoundedLine(input,line,max_record)) { --line_number; break; }
                auto fields=Columns(line);
                if(fields.size()!=columns.size()) throw std::runtime_error("wrong column count");
                auto& path=fields[1]; const bool dir=fields[0]=="dir";
                if(!dir && fields[0]!="file") throw std::runtime_error("invalid entry type");
                if(path.empty() || path[0]!='/' || path.find('\0')!=std::string::npos || path.find('\r')!=std::string::npos)
                    throw std::runtime_error("invalid manifest path");
                auto parsed=MasstreeLHM::PathKey::parse_absolute_path(path);
                if(parsed.normalized_path!=path) throw std::runtime_error("noncanonical manifest path");
                for(const auto& part:parsed.components)
                    if(part=="." || part==".." || part.size()>64) throw std::runtime_error("invalid path component");
                if(!result->processed_records && (path!="/" || !dir)) throw std::runtime_error("first record must be dir /");
                if(result->processed_records && path=="/") throw std::runtime_error("duplicate root");
                const std::string full=std::string(target)+(path=="/"?"":path);
                if(full.size()>max_record) throw std::runtime_error("mapped path exceeds parser memory budget");
                auto final=MasstreeLHM::PathKey::parse_absolute_path(full);
                if(final.components.size()>4096) throw std::runtime_error("path depth exceeds 4096");
                for(const auto& part:final.components) if(part.size()>64 || part=="." || part=="..")
                    throw std::runtime_error("invalid target component");
                zb_lhm_full_inode attr{};
                attr.inode_id=1; attr.type=dir?2:1; attr.mode=dir?0755:0644; attr.nlink=dir?2:1;
                attr.version=1; attr.atime=attr.mtime=attr.ctime=now;
                for(size_t i=2;i<columns.size();++i) if(!fields[i].empty()) SetImportField(attr,columns[i],ParseNumber(fields[i]));
                auto name=final.components.back(); attr.name_length=name.size(); std::memcpy(attr.file_name,name.data(),name.size());
                if(!zb::mds::lhm_codec::Valid(attr)) throw std::runtime_error("invalid inode attributes");
                uint64_t token=0,id=0;
                if(!ctx->ns.reserve_create(full,dir,token,id,ti)) throw std::runtime_error("create failed (duplicate target, missing parent or storage failure): "+full);
                attr.inode_id=id; MasstreeLHM::inode_disk inode{};
                bool ok=false;
                try { ok=zb::mds::lhm_codec::Encode(attr,inode) && ctx->ns.commit_create(token,inode,ti); }
                catch(...) { ctx->ns.cancel_create(token); throw; }
                if(!ok) { ctx->ns.cancel_create(token); throw std::runtime_error("write/publish failed: "+full); }
                if(!result->processed_records) result->root_inode_id=id;
                ++result->processed_records; if(dir) ++result->directories; else ++result->files;
                // This is one long worker job. Advance RCU between completed
                // records so deferred Masstree frees do not accumulate until EOF.
                if((result->processed_records & 63)==0) {
                    globalepoch.store(globalepoch.load()+2); active_epoch.store(globalepoch.load()); ti.rcu_quiesce();
                }
            }
            if(!result->processed_records) throw std::runtime_error("manifest has no root");
            if(!ctx->ns.sync_persistence()) throw std::runtime_error("import data sync failed");
            return ZB_LHM_OK;
        } catch(const std::exception& e) { result->error_line=line_number; Error(error,size,e.what()); return ZB_LHM_ERROR; }
    }); } catch(const std::exception& e) { Error(error,size,e.what()); return ZB_LHM_ERROR; }
}
