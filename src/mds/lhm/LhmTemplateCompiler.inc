// Included after LhmStreamBridge.inc: reuse its strict attribute parser/codec.
#include "compiled_template.hh"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <cerrno>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

namespace {
namespace ctemplate = MasstreeLHM::compiled;
class TemplateSource {
    int fd_ = -1;
    std::vector<char> buffer_ = std::vector<char>(1024*1024);
    size_t begin_ = 0, end_ = 0;
    ctemplate::Digest digest_;
    std::string path_;
    struct stat before_{};
public:
    uint64_t bytes = 0;
    explicit TemplateSource(const char* path):path_(path) {
        fd_=::open(path,O_RDONLY|O_CLOEXEC);
        ctemplate::Require(fd_>=0,"cannot open template source manifest");
        if(::fstat(fd_,&before_) || !S_ISREG(before_.st_mode)) {
            ::close(fd_); fd_=-1; throw std::runtime_error("template source must be a regular file");
        }
    }
    ~TemplateSource() { if(fd_>=0) ::close(fd_); }
    bool line(std::string& out,size_t limit) {
        out.clear();
        for(;;) {
            if(begin_==end_) {
                ssize_t n; do { n=::read(fd_,buffer_.data(),buffer_.size()); } while(n<0 && errno==EINTR);
                ctemplate::Require(n>=0,"source manifest read failed");
                if(!n) return !out.empty();
                begin_=0; end_=size_t(n); bytes+=n; digest_.update(buffer_.data(),size_t(n));
            }
            const char* start=buffer_.data()+begin_;
            const char* newline=static_cast<const char*>(std::memchr(start,'\n',end_-begin_));
            const size_t n=newline?size_t(newline-start):end_-begin_;
            ctemplate::Require(n<=limit-out.size(),"source record exceeds compiler limit");
            out.append(start,n); begin_+=n;
            if(newline) { ++begin_; return true; }
        }
    }
    std::string digest() {
        struct stat descriptor{},path{};
        ctemplate::Require(::fstat(fd_,&descriptor)==0 && ::stat(path_.c_str(),&path)==0 &&
            SameManifest(before_,descriptor) && SameManifest(before_,path) && bytes==uint64_t(before_.st_size),
            "source manifest changed during compilation");
        return digest_.finish();
    }
};
class TemplateOutput {
    int fd_ = -1;
    std::vector<char> buffer_ = std::vector<char>(2*1024*1024);
    size_t used_ = 0;
    ctemplate::Digest digest_;
    std::string name_;
    uint64_t bytes_ = 0;
    void flush() {
        size_t offset=0;
        while(offset<used_) {
            ssize_t n=::write(fd_,buffer_.data()+offset,used_-offset);
            if(n<0 && errno==EINTR) continue;
            ctemplate::Require(n>0,"compiled artifact write failed (check available disk space)"); offset+=size_t(n);
        }
        used_=0;
    }
public:
    TemplateOutput(const std::filesystem::path& root,const char* name):name_(name) {
        fd_=::open((root/name).c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0644);
        ctemplate::Require(fd_>=0,"cannot create compiled artifact");
    }
    ~TemplateOutput() { if(fd_>=0) ::close(fd_); }
    void write(const void* data,size_t n) {
        digest_.update(data,n); bytes_+=n;
        auto p=static_cast<const char*>(data);
        while(n) {
            const auto amount=std::min(n,buffer_.size()-used_);
            std::memcpy(buffer_.data()+used_,p,amount); used_+=amount; p+=amount; n-=amount;
            if(used_==buffer_.size()) flush();
        }
    }
    ctemplate::FileInfo finish() {
        flush(); ctemplate::Require(::fsync(fd_)==0,"compiled artifact fsync failed");
        const auto status=::close(fd_); fd_=-1; ctemplate::Require(status==0,"compiled artifact close failed");
        return {name_,bytes_,digest_.finish()};
    }
};
void TemplateSyncPath(const std::filesystem::path& path,bool directory=false) {
    const int fd=::open(path.c_str(),O_RDONLY|O_CLOEXEC|(directory?O_DIRECTORY:0));
    ctemplate::Require(fd>=0,"cannot open compiled artifact for sync");
    const int status=::fsync(fd),saved=errno; ::close(fd); errno=saved;
    ctemplate::Require(status==0,"compiled artifact sync failed");
}
void TemplateCommon(MasstreeLHM::metadata_common_block_header& out,uint64_t id,
                    MasstreeLHM::metadata_block_type type,uint64_t owner=0) {
    out={}; out.magic=0x4c484d31; out.layout_version=2; out.block_type=uint16_t(type);
    out.block_id=id; out.sequence=id & MasstreeLHM::kMaxLogicalBlock; out.owner_inode_id=owner;
    out.payload_bytes=type==MasstreeLHM::metadata_block_type::inode_block?
        MasstreeLHM::kMetadataBlockBytes-MasstreeLHM::kCommonBlockHeaderBytes:sizeof(MasstreeLHM::directory_block_header);
}
struct TemplateDirectory {
    uint64_t parent=0;
    std::string name;
    bool files_started=false, files_finished=false;
};
// Validate paths without allocating a component/hash vector for every file.
void TemplatePath(const std::string& path) {
    ctemplate::Require(!path.empty() && path[0]=='/' && path.find('\0')==std::string::npos &&
        path.find('\r')==std::string::npos,"invalid source path");
    if(path=="/") return;
    size_t start=1,depth=0;
    for(;;) {
        const auto end=path.find('/',start),n=(end==std::string::npos?path.size():end)-start;
        ctemplate::Require(n>0 && n<=64 && !(n==1 && path[start]=='.') &&
            !(n==2 && path[start]=='.' && path[start+1]=='.'),"noncanonical source path component");
        ctemplate::Require(++depth<=4096,"source path depth exceeds 4096");
        if(end==std::string::npos) return;
        start=end+1;
    }
}
class TemplateCompiler {
    using Store=MasstreeLHM::DirectoryBlockStore;
    using Inode=MasstreeLHM::inode_disk;
    using DImage=MasstreeLHM::directory_block_image;
    uint64_t budget_, directory_memory_=0, leaf_memory_=0, current_parent_=0;
    uint64_t line_number_=0;
    std::filesystem::path root_;
    TemplateSource source_;
    TemplateOutput inodes_,heads_,buckets_,routes_;
    ctemplate::Header header_;
    std::unordered_map<std::string,uint64_t> paths_;
    std::vector<TemplateDirectory> dirs_{{}};
    std::set<std::pair<uint64_t,uint64_t>> directory_hashes_;
    std::vector<Store::entry> entries_;
    MasstreeLHM::inode_block_image inode_page_{};
    MasstreeLHM::metadata_block_bytes head_page_{};
    void memory(uint64_t extra=0) const {
        // Allow for map nodes/rehashing, vector spare capacity, and partition
        // splitting copies. All file memory is released at each parent change.
        ctemplate::Require(directory_memory_+leaf_memory_+extra+16*1024*1024<=budget_,
            "compiler memory budget exceeded: increase budget or reduce files per leaf directory");
    }
    void route(uint64_t ordinal,Store::file_index_state&& plan) {
        ctemplate::Route r; r.ordinal=ordinal; r.parent_ordinal=dirs_[ordinal].parent;
        r.name=dirs_[ordinal].name; r.plan=std::move(plan); r.plan.head_block_id=MasstreeLHM::kCompactHeadTag|ordinal;
        std::ostringstream output(std::ios::binary|std::ios::out); ctemplate::WriteRoute(output,r);
        const auto bytes=output.str(); routes_.write(bytes.data(),bytes.size());
    }
    void flush_files() {
        if(!current_parent_) return;
        auto& dir=dirs_[current_parent_]; dir.files_finished=true;
        Store::file_index_state plan; plan.head_block_id=MasstreeLHM::kCompactHeadTag|current_parent_;
        plan.directory.entry_count=entries_.size();
        // Sort once to detect duplicates; equal hashes with different names are
        // valid file entries and are resolved by the normal bucket name check.
        std::sort(entries_.begin(),entries_.end(),[](const auto& a,const auto& b) {
            return a.component_hash!=b.component_hash?a.component_hash<b.component_hash:a.name<b.name;
        });
        for(size_t i=1;i<entries_.size();++i)
            ctemplate::Require(entries_[i-1].name!=entries_[i].name,"duplicate file name in one directory");
        struct Partition { uint16_t depth; uint32_t prefix; std::vector<Store::entry> items; };
        std::vector<Partition> pending; pending.push_back({0,0,std::move(entries_)});
        std::vector<std::pair<uint16_t,uint32_t>> prefixes;
        while(!pending.empty()) {
            auto part=std::move(pending.back()); pending.pop_back();
            DImage image{};
            if(!Store::build_unallocated_file_bucket(part.items,image)) {
                ctemplate::Require(part.depth<20,"file hash bucket exceeds maximum split depth 20");
                Partition low{uint16_t(part.depth+1),part.prefix,{}},high{uint16_t(part.depth+1),part.prefix|(1U<<part.depth),{}};
                for(auto& item:part.items)
                    ((item.component_hash>>part.depth)&1?high.items:low.items).push_back(std::move(item));
                pending.push_back(std::move(high)); pending.push_back(std::move(low)); continue;
            }
            const auto block=++header_.bucket_pages;
            const auto owner=(uint64_t(1)<<63)|plan.head_block_id;
            TemplateCommon(image.common,block,MasstreeLHM::metadata_block_type::directory_block,owner);
            image.header.dir_inode_id=owner; buckets_.write(&image,sizeof(image));
            Store::bucket_meta meta{}; meta.block_id=block; meta.local_depth=part.depth;
            meta.entry_count=part.items.size(); meta.version=1;
            plan.directory.buckets.push_back(meta); prefixes.emplace_back(part.depth,part.prefix);
            plan.directory.global_depth=std::max(plan.directory.global_depth,part.depth);
            memory(plan.directory.buckets.capacity()*sizeof(meta)*3+prefixes.capacity()*16);
        }
        auto& d=plan.directory;
        if(d.global_depth==0) plan.state=Store::file_index_state_kind::single_bucket;
        else {
            plan.state=Store::file_index_state_kind::multi_bucket;
            memory((uint64_t(1)<<d.global_depth)*16+d.buckets.capacity()*sizeof(Store::bucket_meta)*3);
            d.slots.resize(size_t(1)<<d.global_depth);
            for(uint32_t i=0;i<prefixes.size();++i)
                for(uint32_t slot=prefixes[i].second;slot<d.slots.size();slot+=1U<<prefixes[i].first) d.slots[slot]=i;
        }
        route(current_parent_,std::move(plan)); current_parent_=0; leaf_memory_=0;
        std::vector<Store::entry>().swap(entries_);
    }
    void file(uint64_t parent,const std::string& name,zb_lhm_full_inode& attr,uint8_t time_mask) {
        if(parent!=current_parent_) {
            flush_files();
            ctemplate::Require(!dirs_[parent].files_finished,"file parent returned after its group finished; grouped manifest required");
            dirs_[parent].files_started=true; current_parent_=parent;
        }
        leaf_memory_+=4*(sizeof(Store::entry)+name.size()+32); memory();
        const uint64_t block=header_.files/MasstreeLHM::kInodesPerBlock+1;
        const auto slot=header_.files%MasstreeLHM::kInodesPerBlock;
        if(slot==0) {
            inode_page_={}; TemplateCommon(inode_page_.common,block,MasstreeLHM::metadata_block_type::inode_block);
            inode_page_.header.slot_count=MasstreeLHM::kInodesPerBlock; inode_page_.header.alloc_epoch=block;
        }
        attr.inode_id=(block<<16)|slot; Inode inode{};
        ctemplate::Require(zb::mds::lhm_codec::Encode(attr,inode),"invalid file attribute encoding");
        inode.parent_inode_id=(uint64_t(1)<<63)|MasstreeLHM::kCompactHeadTag|parent;
        inode.generation=1;
        inode.primary_ref=MasstreeLHM::make_inode_ref(block,slot); ctemplate::MarkDefaultTimes(inode,time_mask);
        inode_page_.slots[slot]=inode; ++inode_page_.header.used_count;
        inode_page_.header.slot_bitmap[slot/8]|=uint8_t(1U<<(slot%8));
        ++header_.files;
        if(slot+1==MasstreeLHM::kInodesPerBlock) { inodes_.write(&inode_page_,sizeof(inode_page_)); ++header_.inode_pages; }
        Store::entry entry; entry.kind=Store::dentry_kind::file; entry.name=name;
        entry.component_hash=MasstreeLHM::PathKey::hash_component(name); entry.file_ref=inode.primary_ref;
        entries_.push_back(std::move(entry));
    }
    void directory(const std::string& path,uint64_t parent,const std::string& name,
                   zb_lhm_full_inode& attr,uint8_t time_mask) {
        flush_files();
        ctemplate::Require(!parent || !dirs_[parent].files_started,
            "directory children must be declared before the parent's file group");
        ctemplate::Require(directory_hashes_.insert({parent,MasstreeLHM::PathKey::hash_component(name)}).second,
            "duplicate directory name or sibling directory hash collision");
        directory_memory_+=3*(path.size()+name.size()+192); memory();
        const uint64_t ordinal=dirs_.size(),head=MasstreeLHM::kCompactHeadTag|ordinal;
        ctemplate::Require(paths_.emplace(path,ordinal).second,"duplicate directory path");
        dirs_.push_back({parent,name,false,false}); ++header_.directories;
        attr.inode_id=(uint64_t(1)<<63)|head;
        DImage image{}; TemplateCommon(image.common,head,MasstreeLHM::metadata_block_type::directory_block,attr.inode_id);
        image.header.dir_inode_id=attr.inode_id; image.header.flags=MasstreeLHM::kDirectoryBlockFlagHasEmbeddedInode;
        image.header.base_version=1;
        ctemplate::Require(zb::mds::lhm_codec::Encode(attr,image.directory_inode),"invalid directory attribute encoding");
        image.directory_inode.primary_ref=image.directory_inode.aux_ref=MasstreeLHM::make_inode_ref(head,0);
        image.directory_inode.generation=1;
        ctemplate::MarkDefaultTimes(image.directory_inode,time_mask);
        Store::dentry_entry_header self{}; self.kind=uint8_t(Store::dentry_kind::self_directory);
        self.component_hash=MasstreeLHM::PathKey::hash_component(name); self.name_length=name.size(); self.record_bytes=80;
        std::memcpy(image.payload,&self,sizeof(self)); std::memcpy(image.payload+sizeof(self),name.data(),name.size());
        image.header.used_bytes=80;
        const auto slot=(ordinal-1)%ctemplate::kHeadsPerPage;
        if(!slot) head_page_={};
        std::memcpy(head_page_.data()+(slot+1)*ctemplate::kHeadBytes,&image,ctemplate::kHeadBytes);
        if(slot+1==ctemplate::kHeadsPerPage) { heads_.write(head_page_.data(),head_page_.size()); ++header_.head_pages; }
    }
public:
    TemplateCompiler(const char* source,const std::filesystem::path& root,uint64_t budget):
        budget_(budget),root_(root),source_(source),inodes_(root,"inode.pages"),heads_(root,"heads.pages"),
        buckets_(root,"bucket.pages"),routes_(root,"routes.bin") {}
    uint64_t line_number() const { return line_number_; }
    void run() {
        std::string line; const size_t max_record=64*1024;
        ++line_number_; ctemplate::Require(source_.line(line,max_record),"empty source manifest");
        const auto columns=Columns(line);
        ctemplate::Require(columns.size()>=2 && columns.size()<=32 && columns[0]=="type" && columns[1]=="path",
            "expected type<TAB>path followed by attribute columns");
        std::set<std::string> unique;
        for(size_t i=0;i<columns.size();++i) {
            ctemplate::Require(unique.insert(columns[i]).second,"duplicate source column");
            if(i>=2) { zb_lhm_full_inode test{}; SetImportField(test,columns[i],0); }
        }
        while(source_.line(line,max_record)) {
            ++line_number_; const auto fields=Columns(line);
            ctemplate::Require(fields.size()==columns.size(),"wrong source column count");
            const bool dir=fields[0]=="dir";
            ctemplate::Require(dir || fields[0]=="file","invalid source entry type");
            const auto& path=fields[1]; TemplatePath(path);
            if(header_.directories==0) ctemplate::Require(dir && path=="/","first source record must be dir /");
            else ctemplate::Require(path!="/","duplicate source root");
            const auto slash=path.rfind('/');
            const std::string name=path=="/"?"/":path.substr(slash+1);
            uint64_t parent=0;
            if(path!="/") {
                const auto parent_path=slash==0?std::string("/"):path.substr(0,slash);
                const auto p=paths_.find(parent_path);
                ctemplate::Require(p!=paths_.end(),"source parent directory must be declared before children"); parent=p->second;
            }
            ctemplate::Require(paths_.find(path)==paths_.end(),"duplicate source path or file/directory name conflict");
            zb_lhm_full_inode attr{}; attr.inode_id=1; attr.type=dir?2:1;
            attr.mode=dir?0755:0644; attr.nlink=dir?2:1; attr.version=1;
            attr.name_length=name.size(); std::memcpy(attr.file_name,name.data(),name.size());
            uint8_t time_mask=7;
            for(size_t i=2;i<columns.size();++i) if(!fields[i].empty()) {
                SetImportField(attr,columns[i],ParseNumber(fields[i]));
                if(columns[i]=="atime") time_mask&=~1;
                if(columns[i]=="mtime") time_mask&=~2;
                if(columns[i]=="ctime") time_mask&=~4;
            }
            ctemplate::Require(zb::mds::lhm_codec::Valid(attr),"invalid source inode attributes");
            if(dir) directory(path,parent,name,attr,time_mask); else file(parent,name,attr,time_mask);
        }
        ctemplate::Require(header_.directories>0,"source manifest has no root"); flush_files();
        if(header_.files%MasstreeLHM::kInodesPerBlock) { inodes_.write(&inode_page_,sizeof(inode_page_)); ++header_.inode_pages; }
        if(header_.directories%ctemplate::kHeadsPerPage) { heads_.write(head_page_.data(),head_page_.size()); ++header_.head_pages; }
        for(uint64_t ordinal=1;ordinal<dirs_.size();++ordinal)
            if(!dirs_[ordinal].files_started) route(ordinal,{});
        header_.source_bytes=source_.bytes; header_.source_sha256=source_.digest();
        header_.artifacts={inodes_.finish(),heads_.finish(),buckets_.finish(),routes_.finish()};
        ctemplate::WriteHeader(root_,header_); TemplateSyncPath(root_/"header.bin");
        const auto digest=ctemplate::Sha256(root_/"header.bin");
        std::ofstream summary(root_/"manifest.json",std::ios::binary);
        summary<<"{\n  \"format\": \"lhm-compiled-template-v1\",\n  \"files\": "<<header_.files
            <<",\n  \"directories\": "<<header_.directories<<",\n  \"source_sha256\": \""<<header_.source_sha256
            <<"\",\n  \"source_bytes\": "<<header_.source_bytes<<",\n  \"inode_pages\": "<<header_.inode_pages
            <<",\n  \"head_pages\": "<<header_.head_pages<<",\n  \"bucket_pages\": "<<header_.bucket_pages
            <<",\n  \"header_sha256\": \""<<digest<<"\"\n}\n";
        summary.close(); ctemplate::Require(bool(summary),"compiled manifest write failed"); TemplateSyncPath(root_/"manifest.json");
        std::ofstream checksums(root_/"SHA256SUMS",std::ios::binary);
        checksums<<digest<<"  header.bin\n";
        for(const auto& f:header_.artifacts) checksums<<f.sha256<<"  "<<f.name<<'\n';
        checksums.close(); ctemplate::Require(bool(checksums),"compiled checksum list write failed"); TemplateSyncPath(root_/"SHA256SUMS");
        std::ofstream readme(root_/"README.txt",std::ios::binary);
        readme<<"LHM compiled page template v1\n"
            <<"This is a relocatable page bundle, not a database snapshot. Import through zb_lhm_import_compiled_batch.\n"
            <<"header.bin binds the source SHA256, native layout/hash/codec fingerprint, counts, and four artifact SHA256 digests.\n"
            <<"inode.pages: 16 KiB pages, 63 file inodes/page; relative blocks start at 1.\n"
            <<"heads.pages: 16 KiB pages, first 512 bytes reserved, 31 compact directory heads/page.\n"
            <<"bucket.pages: 16 KiB file-entry buckets using FNV-1a low-bit extendible hashing.\n"
            <<"routes.bin: streaming directory catalog and file-index plans; ordinal 1 is the mountable root.\n"
            <<"Directory ordinals follow source creation order; route records can be emitted in a different order.\n"
            <<"All 26 codec attributes are retained. Missing timestamps are filled once per import batch.\n"
            <<"Compiler requires canonical paths, parent directories first, directory children before the parent file group,\n"
            <<"and contiguous files for each parent. Memory is bounded by the budget check for directory catalog + largest leaf.\n"
            <<"The source manifest and output bundle must remain unchanged while compilation/import is running.\n";
        readme.close(); ctemplate::Require(bool(readme),"compiled README write failed"); TemplateSyncPath(root_/"README.txt");
        TemplateSyncPath(root_,true);
    }
};
} // anonymous namespace

extern "C" int zb_lhm_compile_template(const char* manifest,const char* output,uint64_t budget,
                                        char* error,size_t error_size) {
    Error(error,error_size,"");
    if(!manifest || !output || !*manifest || !*output || budget<32*1024*1024) {
        Error(error,error_size,"manifest, output directory and at least 32 MiB compiler budget required"); return ZB_LHM_INVALID;
    }
    std::filesystem::path temporary;
    try {
        ctemplate::RequirePlatform(); const auto final=std::filesystem::absolute(output).lexically_normal();
        ctemplate::Require(!std::filesystem::exists(final),"compiled output already exists; refusing overwrite");
        ctemplate::Require(std::filesystem::is_directory(final.parent_path()),"compiled output parent directory does not exist");
        std::string pattern=final.string()+".tmp.XXXXXX"; std::vector<char> name(pattern.begin(),pattern.end()); name.push_back(0);
        const auto created=::mkdtemp(name.data()); ctemplate::Require(created,"cannot create private compiler staging directory");
        temporary=created;
        {
            TemplateCompiler compiler(manifest,temporary,budget);
            try { compiler.run(); }
            catch(const std::exception& e) { throw std::runtime_error("source line "+std::to_string(compiler.line_number())+": "+e.what()); }
        }
        // Atomic no-replace publication, including concurrent compiler attempts.
        ctemplate::Require(::syscall(SYS_renameat2,AT_FDCWD,temporary.c_str(),AT_FDCWD,final.c_str(),1)==0,
            "compiled template atomic publication failed (output may already exist)");
        temporary.clear(); TemplateSyncPath(final.parent_path(),true); return ZB_LHM_OK;
    } catch(const std::exception& e) {
        if(!temporary.empty()) { std::error_code ignored; std::filesystem::remove_all(temporary,ignored); }
        Error(error,error_size,e.what()); return ZB_LHM_ERROR;
    }
}
