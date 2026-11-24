#ifndef ZB_LHM_INCREMENTAL_CHECKPOINT_HH
#define ZB_LHM_INCREMENTAL_CHECKPOINT_HH

#include "segmented_layout.hh"
#include <fcntl.h>
#include <unistd.h>

namespace MasstreeLHM {
constexpr uint64_t kIncrementalSnapshotVersion = 5;
constexpr uint64_t kIndexFileMagic = 0x315844494d484cULL;
constexpr uint64_t kMaxIndexFiles = 1000000;
constexpr uint64_t kSharedSnapshotVersion = 6;
inline bool SupportedSnapshotVersion(uint64_t v) {
    return v == kSegmentedSnapshotVersion || v == kIncrementalSnapshotVersion || v == kSharedSnapshotVersion;
}
struct IndexFile {
    uint64_t token=0, dev=0, ino=0, bytes=0, checksum=0;
    uint64_t mtime_sec=0, mtime_ns=0, ctime_sec=0, ctime_ns=0;
};
struct IndexCatalog {
    std::vector<uint64_t> roots;
    std::vector<IndexFile> files;
};
// v6 stores one immutable relative route file per template, plus one mapping
// per imported instance. All instance data pages remain independently allocated.
struct SharedTemplateInfo {
    std::string digest;
    uint64_t files=0, directories=0, inode_pages=0, bucket_pages=0;
};
struct SharedInstance {
    std::string name;
    uint64_t template_index=0, inode_first=0, bucket_first=0, head_first=0;
};
struct SharedCatalog {
    std::vector<SharedTemplateInfo> templates;
    std::vector<SharedInstance> instances;
};
inline void SharedString(SnapshotWriter& out,const std::string& s) {
    out.u64(s.size());
    for(size_t i=0;i<s.size();i+=8) {
        uint64_t n=0; for(size_t j=0;j<8 && i+j<s.size();++j) n|=uint64_t(uint8_t(s[i+j]))<<(8*j);
        out.u64(n);
    }
}
inline std::string SharedString(SnapshotReader& in) {
    const auto n=in.u64(); if(n>64) throw std::runtime_error("shared string exceeds limit");
    std::string s; s.reserve(n);
    for(uint64_t i=0;i<n;i+=8) { auto word=in.u64();
        for(uint64_t j=0;j<8 && i+j<n;++j) { s+=char(word&255); word>>=8; }
        if(word) throw std::runtime_error("noncanonical shared string padding");
    }
    return s;
}
inline void WriteSharedCatalog(SnapshotWriter& out,const SharedCatalog& c) {
    out.u64(c.templates.size());
    for(const auto& t:c.templates) { SharedString(out,t.digest); out.u64(t.files); out.u64(t.directories);
        out.u64(t.inode_pages); out.u64(t.bucket_pages); }
    out.u64(c.instances.size());
    for(const auto& b:c.instances) { SharedString(out,b.name); out.u64(b.template_index);
        out.u64(b.inode_first); out.u64(b.bucket_first); out.u64(b.head_first); }
}
inline SharedCatalog ReadSharedCatalog(SnapshotReader& in,const IndexCatalog& indexes) {
    SharedCatalog c; const auto nt=in.u64();
    if(!nt || nt!=indexes.files.size()) throw std::runtime_error("shared template count differs from index catalog");
    std::set<std::string> digests,names; std::vector<bool> referenced(nt,false);
    for(uint64_t i=0;i<nt;++i) {
        SharedTemplateInfo t; t.digest=SharedString(in); t.files=in.u64(); t.directories=in.u64();
        t.inode_pages=in.u64(); t.bucket_pages=in.u64();
        if(t.digest.size()!=64 || t.digest.find_first_not_of("0123456789abcdef")!=std::string::npos ||
           !digests.insert(t.digest).second || !t.directories || t.directories>kMaxLogicalBlock ||
           t.inode_pages>kMaxLogicalBlock || t.bucket_pages>kMaxLogicalBlock ||
           t.inode_pages!=t.files/63+(t.files%63!=0)) throw std::runtime_error("invalid shared template metadata");
        c.templates.push_back(t);
    }
    const auto nb=in.u64(); if(!nb || nb>kMaxIndexFiles || nb!=indexes.roots.size())
        throw std::runtime_error("invalid shared instance count");
    std::vector<std::pair<uint64_t,uint64_t>> ranges[3];
    for(uint64_t i=0;i<nb;++i) {
        SharedInstance b; b.name=SharedString(in); b.template_index=in.u64();
        b.inode_first=in.u64(); b.bucket_first=in.u64(); b.head_first=in.u64();
        if(b.name.empty() || b.name=="." || b.name==".." || b.name.find('/')!=std::string::npos ||
           b.name.find('\0')!=std::string::npos || !names.insert(b.name).second || b.template_index>=nt)
            throw std::runtime_error("invalid shared instance name/template");
        referenced[b.template_index]=true; const auto& t=c.templates[b.template_index];
        const uint64_t starts[]={b.inode_first,b.bucket_first,b.head_first};
        const uint64_t counts[]={t.inode_pages,t.bucket_pages,t.directories};
        for(unsigned k=0;k<3;++k) {
            if(!starts[k] || starts[k]>kMaxLogicalBlock || counts[k]>kMaxLogicalBlock-starts[k]+1)
                throw std::runtime_error("shared instance address overflow");
            if(counts[k]) ranges[k].emplace_back(starts[k],starts[k]+counts[k]);
        }
        c.instances.push_back(b);
    }
    for(auto& range:ranges) { std::sort(range.begin(),range.end());
        for(size_t i=1;i<range.size();++i) if(range[i].first<range[i-1].second)
            throw std::runtime_error("overlapping shared instance ranges"); }
    if(std::find(referenced.begin(),referenced.end(),false)!=referenced.end())
        throw std::runtime_error("unreferenced shared template");
    return c;
}
inline uint64_t IndexToken(const std::string& suffix) {
    if(suffix.size()!=6) throw std::runtime_error("invalid index token");
    uint64_t token=0;
    for(size_t i=0;i<6;++i) {
        const auto c=static_cast<unsigned char>(suffix[i]);
        if(!((c>='0'&&c<='9')||(c>='A'&&c<='Z')||(c>='a'&&c<='z')))
            throw std::runtime_error("invalid index token character");
        token |= uint64_t(c) << (8*i);
    }
    return token;
}
inline std::string IndexPath(const std::string& store, uint64_t token) {
    if(token >> 48) throw std::runtime_error("invalid index token range");
    std::string suffix;
    for(unsigned i=0;i<6;++i) suffix.push_back(char(token>>(8*i)));
    (void)IndexToken(suffix);
    return store+".index-"+suffix;
}
inline IndexFile InspectIndex(const std::string& store,uint64_t token) {
    struct stat st{};
    const auto path=IndexPath(store,token);
    if(::stat(path.c_str(),&st)||!S_ISREG(st.st_mode)||st.st_size<0)
        throw std::runtime_error("cannot stat checkpoint index: "+path);
    return {token,uint64_t(st.st_dev),uint64_t(st.st_ino),uint64_t(st.st_size),0,
            uint64_t(st.st_mtim.tv_sec),uint64_t(st.st_mtim.tv_nsec),
            uint64_t(st.st_ctim.tv_sec),uint64_t(st.st_ctim.tv_nsec)};
}
inline void ValidateIndexFiles(const std::string& store,const IndexCatalog& catalog) {
    std::set<std::pair<uint64_t,uint64_t>> identities;
    std::set<uint64_t> tokens;
    for(const auto& f:catalog.files) {
        const auto actual=InspectIndex(store,f.token);
        if(f.bytes<32 || f.bytes%8 || actual.dev!=f.dev || actual.ino!=f.ino ||
           actual.bytes!=f.bytes || actual.mtime_sec!=f.mtime_sec || actual.mtime_ns!=f.mtime_ns ||
           actual.ctime_sec!=f.ctime_sec || actual.ctime_ns!=f.ctime_ns ||
           !identities.emplace(f.dev,f.ino).second || !tokens.insert(f.token).second)
            throw std::runtime_error("checkpoint index identity/length/time mismatch: "+IndexPath(store,f.token));
    }
}
inline void WriteIndexCatalog(SnapshotWriter& out,const IndexCatalog& c) {
    out.u64(c.roots.size()); for(auto hash:c.roots) out.u64(hash);
    out.u64(c.files.size());
    for(const auto& f:c.files) for(auto n:{f.token,f.dev,f.ino,f.bytes,f.checksum,
        f.mtime_sec,f.mtime_ns,f.ctime_sec,f.ctime_ns}) out.u64(n);
}
inline IndexCatalog ReadIndexCatalog(SnapshotReader& in) {
    IndexCatalog c;
    const auto roots=in.u64();
    if(roots>kMaxIndexFiles) throw std::runtime_error("too many checkpoint root directories");
    std::set<uint64_t> seen;
    for(uint64_t i=0;i<roots;++i) {
        auto hash=in.u64();
        if(!seen.insert(hash).second) throw std::runtime_error("duplicate checkpoint root hash");
        c.roots.push_back(hash);
    }
    const auto count=in.u64();
    if(!count || count>kMaxIndexFiles) throw std::runtime_error("invalid checkpoint index count");
    std::set<uint64_t> tokens;
    for(uint64_t i=0;i<count;++i) {
        IndexFile f;
        f.token=in.u64(); f.dev=in.u64(); f.ino=in.u64(); f.bytes=in.u64(); f.checksum=in.u64();
        f.mtime_sec=in.u64(); f.mtime_ns=in.u64(); f.ctime_sec=in.u64(); f.ctime_ns=in.u64();
        (void)IndexPath("",f.token);
        if(!f.ino || f.bytes<32 || f.bytes%8 || f.mtime_ns>=1000000000 || f.ctime_ns>=1000000000 ||
           !tokens.insert(f.token).second) throw std::runtime_error("invalid checkpoint index metadata");
        c.files.push_back(f);
    }
    return c;
}
// Root file routing has the same scalar encoding as full snapshots. This
// lightweight parser is also used by recovery, without allocating any trees.
inline void SkipRootFileIndex(SnapshotReader& in) {
    for(unsigned i=0;i<5;++i) (void)in.u64();
    const auto slots=in.u64();
    if(slots>(1ULL<<20)) throw std::runtime_error("invalid root index slots");
    for(uint64_t i=0;i<slots;++i) (void)in.u64();
    const auto buckets=in.u64();
    if(buckets>(1ULL<<20)) throw std::runtime_error("invalid root index buckets");
    for(uint64_t i=0;i<buckets*5;++i) (void)in.u64();
}
inline IndexCatalog CheckpointIndexes(const std::string& snapshot) {
    SnapshotReader in(snapshot);
    if(in.u64()!=kSegmentedSnapshotMagic) throw std::runtime_error("invalid checkpoint magic");
    const auto version=in.u64();
    if(!SupportedSnapshotVersion(version)) throw std::runtime_error("unsupported checkpoint version");
    if(version==kSegmentedSnapshotVersion) return {};
    (void)ReadSegmentLayout(in);
    for(unsigned i=0;i<10;++i) (void)in.u64(); // allocation and root reference
    SkipRootFileIndex(in);
    auto result=ReadIndexCatalog(in);
    if(version==kSharedSnapshotVersion) (void)ReadSharedCatalog(in,result);
    in.finish(); return result;
}
}
#endif
