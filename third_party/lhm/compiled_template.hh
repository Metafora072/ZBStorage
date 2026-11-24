#pragma once
// Offline template v1. Pages use the checked native little-endian LHM v2 layout;
// catalog/header integers are explicitly little endian and never dump padding.
#include "directory_block_store.hh"
#include "path_key.hh"
#include <openssl/evp.h>
#include <array>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <stdexcept>

namespace MasstreeLHM::compiled {
inline constexpr uint32_t kVersion = 1;
inline constexpr uint32_t kHeadBytes = 512, kHeadsPerPage = 31;
inline constexpr const char* kLayoutFingerprint =
    "lhm2-le-b16384-i256x63-h512x31-d16000-dentry16-ref8-index16-fnv1a64-zba1-161";
inline constexpr std::array<const char*,4> kFiles =
    {{"inode.pages", "heads.pages", "bucket.pages", "routes.bin"}};
struct FileInfo { std::string name; uint64_t bytes = 0; std::string sha256; };
struct Header {
    uint64_t files = 0, directories = 0, inode_pages = 0, head_pages = 0;
    uint64_t bucket_pages = 0, source_bytes = 0;
    std::string source_sha256, layout_fingerprint = kLayoutFingerprint;
    std::array<FileInfo,4> artifacts{};
};
struct Route {
    uint64_t ordinal = 0, parent_ordinal = 0;
    std::string name;
    DirectoryBlockStore::file_index_state plan;
};
struct Bases {
    uint64_t inode_first = 0, bucket_first = 0, head_first = 0, sequence_base = 0;
    uint64_t inode_pages = 0, bucket_pages = 0, head_count = 0;
};
inline void Require(bool ok, const char* why) {
    if (!ok) throw std::runtime_error(why);
}
inline void RequirePlatform() {
    uint16_t endian = 1;
    Require(*reinterpret_cast<uint8_t*>(&endian) == 1, "compiled templates require little endian host");
    static_assert(sizeof(inode_block_image)==16384 && sizeof(directory_block_image)==16384);
    static_assert(offsetof(directory_block_image,payload)==384 && sizeof(inode_disk)==256);
    static_assert(offsetof(inode_disk,reserved)==92 && sizeof(inode_ref)==8);
}
inline void Put(std::ostream& out, uint64_t value, unsigned bytes) {
    char data[8]; for(unsigned i=0;i<bytes;++i) data[i]=char(value>>(8*i));
    out.write(data,bytes); Require(bool(out),"compiled template write failed");
}
inline uint64_t Get(std::istream& in, unsigned bytes) {
    char data[8]; in.read(data,bytes); Require(bool(in),"truncated compiled template metadata");
    uint64_t value=0; for(unsigned i=0;i<bytes;++i) value|=uint64_t(uint8_t(data[i]))<<(8*i);
    return value;
}
inline void PutString(std::ostream& out, const std::string& value, size_t limit) {
    Require(value.size()<=limit,"compiled template string exceeds limit");
    Put(out,value.size(),4); out.write(value.data(),value.size()); Require(bool(out),"template write failed");
}
inline std::string GetString(std::istream& in, size_t limit) {
    const auto n=Get(in,4); Require(n<=limit,"compiled template string exceeds limit");
    std::string value(size_t(n),'\0'); in.read(value.data(),value.size());
    Require(bool(in),"truncated compiled template string"); return value;
}
inline bool IsDigest(const std::string& digest) {
    return digest.size()==64 && digest.find_first_not_of("0123456789abcdef")==std::string::npos;
}
inline uint64_t HashComponentBytes(const void* bytes,size_t length) {
    const auto* p=static_cast<const uint8_t*>(bytes);
    uint64_t hash=14695981039346656037ULL;
    for(size_t i=0;i<length;++i) hash=(hash^p[i])*1099511628211ULL;
    return hash;
}
class Digest {
    std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)> ctx_{EVP_MD_CTX_new(),EVP_MD_CTX_free};
public:
    Digest() { Require(ctx_ && EVP_DigestInit_ex(ctx_.get(),EVP_sha256(),nullptr)==1,"SHA256 init failed"); }
    void update(const void* data,size_t n) { Require(EVP_DigestUpdate(ctx_.get(),data,n)==1,"SHA256 update failed"); }
    std::string finish() {
        unsigned char bytes[32]; unsigned n=0;
        Require(EVP_DigestFinal_ex(ctx_.get(),bytes,&n)==1 && n==32,"SHA256 final failed");
        constexpr char hex[]="0123456789abcdef"; std::string out; out.reserve(64);
        for(auto b:bytes) { out+=hex[b>>4]; out+=hex[b&15]; } return out;
    }
};
inline std::string Sha256(const std::filesystem::path& path) {
    std::ifstream input(path,std::ios::binary); Require(bool(input),"cannot open template file for hashing");
    Digest digest; std::array<char,1024*1024> data;
    while(input) { input.read(data.data(),data.size()); digest.update(data.data(),size_t(input.gcount())); }
    Require(input.eof() && !input.bad(),"template hash read failed"); return digest.finish();
}
inline void WriteHeader(const std::filesystem::path& root, const Header& h) {
    std::ofstream out(root/"header.bin",std::ios::binary|std::ios::trunc);
    Require(bool(out),"cannot create template header");
    out.write("ZBLHM-TEMPLATE\0\0\0",16); Put(out,kVersion,4);
    PutString(out,h.layout_fingerprint,256);
    for(uint64_t n:{h.files,h.directories,h.inode_pages,h.head_pages,h.bucket_pages,h.source_bytes}) Put(out,n,8);
    PutString(out,h.source_sha256,64);
    for(const auto& f:h.artifacts) { PutString(out,f.name,32); Put(out,f.bytes,8); PutString(out,f.sha256,64); }
    out.close(); Require(bool(out),"template header close failed");
}
inline Header ReadHeader(const std::filesystem::path& root) {
    RequirePlatform(); std::ifstream in(root/"header.bin",std::ios::binary);
    Require(bool(in),"cannot open compiled template header"); char magic[16]; in.read(magic,16);
    Require(in && !std::memcmp(magic,"ZBLHM-TEMPLATE\0\0\0",16),"wrong compiled template magic");
    Require(Get(in,4)==kVersion,"unsupported compiled template version");
    Header h; h.layout_fingerprint=GetString(in,256);
    Require(h.layout_fingerprint==kLayoutFingerprint,"incompatible compiled template layout/hash/codec");
    h.files=Get(in,8); h.directories=Get(in,8); h.inode_pages=Get(in,8);
    h.head_pages=Get(in,8); h.bucket_pages=Get(in,8); h.source_bytes=Get(in,8);
    h.source_sha256=GetString(in,64); Require(IsDigest(h.source_sha256),"invalid source SHA256");
    for(size_t i=0;i<h.artifacts.size();++i) {
        auto& f=h.artifacts[i]; f.name=GetString(in,32); f.bytes=Get(in,8); f.sha256=GetString(in,64);
        Require(f.name==kFiles[i] && IsDigest(f.sha256),"invalid compiled artifact descriptor");
    }
    Require(in.peek()==std::char_traits<char>::eof(),"trailing compiled header bytes");
    Require(h.directories>0 && h.directories<kCompactHeadTag,"invalid compiled directory count");
    Require(h.inode_pages==h.files/kInodesPerBlock+(h.files%kInodesPerBlock!=0),"invalid inode page count");
    Require(h.head_pages==(h.directories-1)/kHeadsPerPage+1,"invalid compact head page count");
    for(size_t i=0;i<3;++i) {
        const uint64_t pages=i==0?h.inode_pages:i==1?h.head_pages:h.bucket_pages;
        Require(pages<=UINT64_MAX/kMetadataBlockBytes && h.artifacts[i].bytes==pages*kMetadataBlockBytes,
                "invalid compiled page file length");
    }
    return h;
}
inline void VerifyFiles(const std::filesystem::path& root,const Header& h) {
    for(const auto& f:h.artifacts) {
        Require(std::filesystem::is_regular_file(root/f.name),"compiled artifact is not a regular file");
        Require(std::filesystem::file_size(root/f.name)==f.bytes,"compiled artifact length differs");
        Require(Sha256(root/f.name)==f.sha256,"compiled artifact SHA256 differs");
    }
}
inline void WriteRoute(std::ostream& out,const Route& r) {
    const auto& d=r.plan.directory;
    Put(out,r.ordinal,8); Put(out,r.parent_ordinal,8); PutString(out,r.name,64);
    Put(out,uint8_t(r.plan.state),1); Put(out,r.plan.head_block_id,8);
    Put(out,d.global_depth,2); Put(out,d.entry_count,8); Put(out,d.single_bucket,4);
    Put(out,d.slots.size(),4); Put(out,d.buckets.size(),4);
    for(auto slot:d.slots) Put(out,slot,4);
    for(const auto& b:d.buckets) {
        Put(out,b.block_id,8); Put(out,b.local_depth,2); Put(out,b.entry_count,2);
        Put(out,b.version,4); Put(out,b.flags,4);
    }
}
inline bool ReadRoute(std::istream& in,Route& r,uint64_t budget=UINT64_MAX) {
    if(in.peek()==std::char_traits<char>::eof()) { Require(!in.bad(),"route read failure"); return false; }
    r={}; r.ordinal=Get(in,8); r.parent_ordinal=Get(in,8); r.name=GetString(in,64);
    const auto state=Get(in,1); Require(state<=2,"invalid file index state");
    r.plan.state=static_cast<DirectoryBlockStore::file_index_state_kind>(state);
    r.plan.head_block_id=Get(in,8); auto& d=r.plan.directory;
    d.global_depth=Get(in,2); d.entry_count=Get(in,8); d.single_bucket=Get(in,4);
    const auto slots=Get(in,4), buckets=Get(in,4);
    Require(d.global_depth<=20 && slots<=(1U<<20) && buckets<=(1U<<20),"file index exceeds format bounds");
    Require(slots*sizeof(uint32_t)+buckets*(sizeof(DirectoryBlockStore::bucket_meta)+8)+sizeof(Route)+128<=budget,
            "route exceeds importer memory budget");
    Require(r.ordinal>0 && r.ordinal<kCompactHeadTag && r.parent_ordinal<r.ordinal &&
            r.plan.head_block_id==(kCompactHeadTag|r.ordinal),"invalid template directory reference");
    Require(!r.name.empty() && (r.ordinal==1?r.name=="/":
            r.name!="." && r.name!=".." && r.name.find('/')==std::string::npos) &&
            r.name.find('\0')==std::string::npos,"invalid template directory name");
    Require((r.ordinal==1)==(r.parent_ordinal==0),"invalid template parent ordinal");
    Require(state==0 ? slots==0 && buckets==0 && d.entry_count==0 && d.global_depth==0 :
            state==1 ? slots==0 && buckets==1 && d.global_depth==0 && d.single_bucket==0 :
            d.global_depth>0 && slots==(1U<<d.global_depth) && buckets>0,"invalid file index topology");
    d.slots.resize(size_t(slots)); for(auto& slot:d.slots) { slot=Get(in,4); Require(slot<buckets,"invalid bucket routing slot"); }
    uint64_t count=0; d.buckets.resize(size_t(buckets));
    for(auto& b:d.buckets) {
        b.block_id=Get(in,8); b.local_depth=Get(in,2); b.entry_count=Get(in,2); b.version=Get(in,4); b.flags=Get(in,4);
        Require(b.block_id>0 && b.block_id<kCompactHeadTag && b.local_depth<=d.global_depth && b.entry_count<=400,
                "invalid compiled bucket metadata"); count+=b.entry_count;
    }
    Require(count==d.entry_count && (state==0 || count>0),"file index entry count mismatch");
    if(state==2) {
        std::vector<uint32_t> first(size_t(buckets),UINT32_MAX),references(size_t(buckets),0);
        for(uint32_t slot=0;slot<slots;++slot) {
            const auto bucket=d.slots[slot],mask=(1U<<d.buckets[bucket].local_depth)-1;
            if(first[bucket]==UINT32_MAX) first[bucket]=slot;
            Require((slot&mask)==(first[bucket]&mask),"inconsistent compiled hash bucket prefix");
            ++references[bucket];
        }
        for(size_t i=0;i<buckets;++i)
            Require(references[i]==(1U<<(d.global_depth-d.buckets[i].local_depth)),
                    "incomplete compiled hash bucket routing");
    }
    return true;
}
inline uint64_t Head(uint64_t ordinal,const Bases& b) {
    Require(ordinal && ordinal<=b.head_count && b.head_first && ordinal<=kMaxLogicalBlock-b.head_first+1,"head relocation overflow");
    return kCompactHeadTag|(b.head_first+ordinal-1);
}
inline uint64_t DirInode(uint64_t ordinal,const Bases& b) { return (uint64_t(1)<<63)|Head(ordinal,b); }
inline uint64_t RelocateId(uint64_t id,const Bases& b) {
    if(!id) return 0;
    if(id>>63) {
        Require((id&kCompactHeadTag) && !(id&~((uint64_t(1)<<63)|kCompactHeadTag|kMaxLogicalBlock)),
                "invalid compiled directory inode encoding");
        return DirInode(id&kMaxLogicalBlock,b);
    }
    const uint64_t block=id>>16,slot=id&65535;
    Require(block && block<=b.inode_pages && slot<kInodesPerBlock && b.inode_first && block<=kMaxLogicalBlock-b.inode_first+1,
            "file inode relocation overflow");
    return ((b.inode_first+block-1)<<16)|slot;
}
// The codec owns bytes [0,161). The template-only marker is consumed before
// publishing any inode; explicit zero timestamp values have no marker bit.
inline void MarkDefaultTimes(inode_disk& inode,uint8_t mask) {
    inode.reserved[161]=0xc7; inode.reserved[162]=mask; inode.reserved[163]=1;
}
inline void FillDefaultTimes(inode_disk& inode,uint64_t now) {
    Require(inode.reserved[161]==0xc7 && inode.reserved[163]==1 && !(inode.reserved[162]&~7),
            "missing compiled inode timestamp marker");
    const auto mask=inode.reserved[162];
    Require(now<=UINT64_MAX/1000000000ULL,"timestamp relocation overflow");
    if(mask&1) for(unsigned j=0;j<8;++j) inode.reserved[17+j]=uint8_t(now>>(j*8));
    if(mask&2) inode.mtime_ns=now*1000000000ULL;
    if(mask&4) inode.ctime_ns=now*1000000000ULL;
    inode.reserved[161]=inode.reserved[162]=inode.reserved[163]=0;
}
inline void RelocateInode(inode_disk& inode,const Bases& b,uint64_t now) {
    const bool directory=inode.type==2;
    Require((directory || inode.type==1) && bool(inode.inode_id>>63)==directory && inode.link_count>0 &&
            inode.generation==1 && inode.reserved[0]==0x5a && inode.reserved[1]==0x42 &&
            inode.reserved[2]==0x41 && inode.reserved[3]==0x31 && inode.reserved[4]==1 &&
            inode.reserved[5]>0 && inode.reserved[5]<=64,"invalid compiled inode/attribute codec");
    Require(inode.primary_ref.block_id==(directory?(inode.inode_id&(kCompactHeadTag|kMaxLogicalBlock)):(inode.inode_id>>16)) &&
            inode.primary_ref.offset==(directory?0:(inode.inode_id&65535)),"compiled inode primary reference mismatch");
    Require(directory ? inode.parent_inode_id==0 && inode.aux_ref.block_id==inode.primary_ref.block_id && !inode.aux_ref.offset :
            (inode.parent_inode_id>>63) && !inode.aux_ref.block_id && !inode.aux_ref.offset,
            "invalid compiled inode parent or auxiliary reference");
    inode.inode_id=RelocateId(inode.inode_id,b);
    inode.parent_inode_id=RelocateId(inode.parent_inode_id,b);
    if(inode.primary_ref.block_id) {
        const auto block=inode.primary_ref.block_id;
        inode.primary_ref.block_id=(block&kCompactHeadTag)?Head(block&kMaxLogicalBlock,b):b.inode_first+block-1;
    }
    if(inode.aux_ref.block_id) {
        Require(inode.type==2 && (inode.aux_ref.block_id&kCompactHeadTag) && !inode.aux_ref.offset,
                "unsupported compiled auxiliary reference");
        inode.aux_ref.block_id=Head(inode.aux_ref.block_id&kMaxLogicalBlock,b);
    }
    FillDefaultTimes(inode,now);
}
inline void ValidateCommon(const metadata_common_block_header& c,uint64_t id,metadata_block_type type) {
    Require(c.magic==0x4c484d31 && c.layout_version==2 && c.block_type==uint16_t(type) && c.block_id==id,
            "invalid compiled page header");
    Require(c.next_block_id==0,"unexpected compiled page chain");
}
inline void RelocateInodePage(metadata_block_bytes& bytes,uint64_t page,const Bases& b,uint64_t now) {
    Require(page<b.inode_pages,"inode page outside template");
    auto& image=*reinterpret_cast<inode_block_image*>(bytes.data());
    ValidateCommon(image.common,page+1,metadata_block_type::inode_block);
    Require(!image.common.owner_inode_id && image.common.payload_bytes==kMetadataBlockBytes-kCommonBlockHeaderBytes,
            "invalid inode page ownership/payload");
    Require(image.header.slot_count==kInodesPerBlock && image.header.used_count<=kInodesPerBlock,
            "invalid compiled inode slot counts");
    Require(image.header.used_count>0 && (page+1==b.inode_pages || image.header.used_count==kInodesPerBlock),
            "compiled inode pages are not densely packed");
    image.common.block_id=b.inode_first+page; image.common.sequence=b.sequence_base+page;
    image.header.alloc_epoch=image.common.sequence; uint32_t used=0;
    for(unsigned slot=0;slot<kInodesPerBlock;++slot) {
        const bool occupied=image.header.slot_bitmap[slot/8]&(1U<<(slot%8));
        Require(occupied==(slot<image.header.used_count),"compiled inode bitmap is not densely packed");
        if(!occupied) continue;
        auto& inode=image.slots[slot]; Require(inode.type==1 && inode.inode_id==((page+1)<<16|slot),"invalid compiled file inode");
        RelocateInode(inode,b,now); ++used;
    }
    Require(used==image.header.used_count,"compiled inode bitmap mismatch");
}
inline void RelocateBucketPage(metadata_block_bytes& bytes,uint64_t page,const Bases& b) {
    Require(page<b.bucket_pages,"bucket page outside template");
    using Store=DirectoryBlockStore;
    auto& image=*reinterpret_cast<directory_block_image*>(bytes.data());
    ValidateCommon(image.common,page+1,metadata_block_type::directory_block);
    Require(image.common.owner_inode_id==image.header.dir_inode_id && (image.header.dir_inode_id>>63) &&
            !image.header.prev_block_id && !image.header.next_block_id && !image.header.flags &&
            image.header.delta_count==image.header.entry_count,"invalid compiled bucket ownership/index");
    image.common.block_id=b.bucket_first+page; image.common.sequence=b.sequence_base+b.inode_pages+page;
    image.common.owner_inode_id=RelocateId(image.common.owner_inode_id,b);
    image.header.dir_inode_id=RelocateId(image.header.dir_inode_id,b);
    Require(image.header.used_bytes<=kDirectoryBlockPayloadBytes &&
            image.header.entry_count<=kDirectoryBlockPayloadBytes/sizeof(Store::dentry_index_entry) &&
            image.header.used_bytes+image.header.entry_count*sizeof(Store::dentry_index_entry)<=kDirectoryBlockPayloadBytes,
            "invalid compiled bucket payload size");
    // Records are eight-byte aligned. Membership bits replace a binary search
    // for every index entry and are consumed to reject duplicate offsets.
    constexpr size_t kOffsetWords=(kDirectoryBlockPayloadBytes/8+63)/64;
    uint32_t offset=0,count=0; std::array<uint64_t,kOffsetWords> starts{};
    while(offset<image.header.used_bytes) {
        Store::dentry_entry_header h{};
        Require(image.header.used_bytes-offset>=sizeof(h),"truncated compiled dentry");
        std::memcpy(&h,image.payload+offset,sizeof(h));
        Require(h.kind==uint8_t(Store::dentry_kind::file) && h.flags==0 && h.ref_bytes==8 &&
                h.name_length>0 && h.name_length<=64 && h.record_bytes>=sizeof(h)+8+h.name_length &&
                h.record_bytes<=image.header.used_bytes-offset && !(h.record_bytes&7),"invalid compiled dentry");
        Require(count<400,"compiled bucket record count exceeds capacity");
        const uint32_t bit=offset/8; starts[bit/64]|=uint64_t(1)<<(bit%64);
        const auto* name=image.payload+offset+sizeof(h)+8;
        Require(!std::memchr(name,'/',h.name_length) && !std::memchr(name,0,h.name_length) &&
                HashComponentBytes(name,h.name_length)==h.component_hash,"compiled dentry name/hash mismatch");
        Store::file_ref_payload ref{}; std::memcpy(&ref,image.payload+offset+sizeof(h),sizeof(ref));
        Require(ref.inode_block_id && ref.inode_block_id<=b.inode_pages && ref.inode_slot<kInodesPerBlock &&
                ref.inode_block_id<=kMaxLogicalBlock-b.inode_first+1,"invalid compiled file reference");
        ref.inode_block_id+=b.inode_first-1; std::memcpy(image.payload+offset+sizeof(h),&ref,sizeof(ref));
        offset+=h.record_bytes; ++count;
    }
    Require(count==image.header.entry_count,"compiled bucket count mismatch");
    uint64_t last_hash=0;
    for(uint32_t i=0;i<count;++i) {
        Store::dentry_index_entry index{};
        std::memcpy(&index,image.payload+kDirectoryBlockPayloadBytes-count*sizeof(index)+i*sizeof(index),sizeof(index));
        Require(!i || index.component_hash>=last_hash,"compiled dentry index is not sorted"); last_hash=index.component_hash;
        Require(index.entry_offset<image.header.used_bytes && !(index.entry_offset&7),
                "invalid compiled dentry index offset");
        const uint32_t bit=index.entry_offset/8;
        const uint64_t mask=uint64_t(1)<<(bit%64);
        Require(starts[bit/64]&mask,"invalid or duplicate compiled dentry index offset");
        starts[bit/64]&=~mask;
        Store::dentry_entry_header h{}; std::memcpy(&h,image.payload+index.entry_offset,sizeof(h));
        Require(h.component_hash==index.component_hash && !index.flags,"compiled dentry index/hash mismatch");
    }
}
inline void RelocateHeadPage(metadata_block_bytes& bytes,uint64_t page,const Bases& b,uint64_t now,
                             const std::string& root_name) {
    Require(!root_name.empty() && root_name.size()<=64 && root_name.find('/')==std::string::npos,
            "invalid template mount name");
    Require(b.head_count && b.head_first && (b.head_first-1)%kHeadsPerPage==0,"head reservation is not page aligned");
    Require(page<(b.head_count-1)/kHeadsPerPage+1,"head page outside template");
    for(unsigned i=0;i<kHeadBytes;++i) Require(bytes[i]==0,"invalid compact head page prefix");
    for(unsigned slot=0;slot<kHeadsPerPage;++slot) {
        directory_block_image image{}; auto* record=bytes.data()+(slot+1)*kHeadBytes;
        const uint64_t ordinal=page*kHeadsPerPage+slot+1;
        std::memcpy(&image,record,kHeadBytes);
        if(ordinal>b.head_count) {
            for(unsigned i=0;i<kHeadBytes;++i) Require(record[i]==0,"unexpected compact head past declared count");
            continue;
        }
        ValidateCommon(image.common,kCompactHeadTag|ordinal,metadata_block_type::directory_block);
        Require(image.common.owner_inode_id==((uint64_t(1)<<63)|image.common.block_id) &&
                image.header.dir_inode_id==image.common.owner_inode_id &&
                image.directory_inode.inode_id==image.header.dir_inode_id &&
                !image.header.prev_block_id && !image.header.next_block_id && !image.header.entry_count &&
                !image.header.delta_count && image.header.flags==kDirectoryBlockFlagHasEmbeddedInode && image.header.used_bytes==80,
                "invalid compiled compact directory head");
        DirectoryBlockStore::dentry_entry_header self{}; std::memcpy(&self,image.payload,sizeof(self));
        Require(self.kind==uint8_t(DirectoryBlockStore::dentry_kind::self_directory) && self.record_bytes==80 &&
                !self.ref_bytes && !self.flags && self.name_length>0 && self.name_length<=64 &&
                self.name_length==image.directory_inode.reserved[5] &&
                !std::memcmp(image.payload+sizeof(self),image.directory_inode.reserved+97,self.name_length),
                "compiled directory self-name/attribute mismatch");
        Require(HashComponentBytes(image.payload+sizeof(self),self.name_length)==self.component_hash,
                "compiled directory self-name/hash mismatch");
        image.common.block_id=Head(ordinal,b); image.common.sequence=b.sequence_base+b.inode_pages+b.bucket_pages+ordinal-1;
        image.common.owner_inode_id=RelocateId(image.common.owner_inode_id,b);
        image.header.dir_inode_id=RelocateId(image.header.dir_inode_id,b);
        Require(image.directory_inode.type==2,"invalid compiled directory inode");
        RelocateInode(image.directory_inode,b,now);
        if(ordinal==1) {
            auto& inode=image.directory_inode; inode.reserved[5]=uint8_t(root_name.size());
            std::memset(inode.reserved+97,0,64); std::memcpy(inode.reserved+97,root_name.data(),root_name.size());
            self.component_hash=PathKey::hash_component(root_name); self.name_length=root_name.size();
            std::memcpy(image.payload,&self,sizeof(self)); std::memset(image.payload+sizeof(self),0,64);
            std::memcpy(image.payload+sizeof(self),root_name.data(),root_name.size());
        }
        std::memcpy(record,&image,kHeadBytes);
    }
}
inline void RelocateRoute(Route& r,const Bases& b) {
    r.plan.head_block_id=Head(r.ordinal,b);
    for(auto& bucket:r.plan.directory.buckets) {
        Require(bucket.block_id && bucket.block_id<=b.bucket_pages && b.bucket_first && bucket.block_id<=kMaxLogicalBlock-b.bucket_first+1,
                "bucket relocation overflow");
        bucket.block_id+=b.bucket_first-1;
    }
}
} // namespace MasstreeLHM::compiled
