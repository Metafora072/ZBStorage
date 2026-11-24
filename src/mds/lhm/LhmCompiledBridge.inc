// Compiled pages retain the normal on-disk LHM layout. Only offline construction
// differs: one reservation, bounded page relocation, directory-only publication.
#include "compiled_template.hh"
namespace {
namespace CT = MasstreeLHM::compiled;
using ProfileClock=std::chrono::steady_clock;
uint64_t ProfileElapsed(ProfileClock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now()-start).count();
}
struct ProfileTimer {
    uint64_t& elapsed;
    ProfileClock::time_point start=ProfileClock::now();
    explicit ProfileTimer(uint64_t& value):elapsed(value) {}
    ~ProfileTimer() { elapsed+=ProfileElapsed(start); }
};
class CompiledReader {
    std::filesystem::path path_;
    CT::FileInfo info_;
    struct stat before_{};
    std::vector<char> buffer_;
    std::ifstream input_;
    CT::Digest digest_;
    uint64_t consumed_=0;
public:
    CompiledReader(const std::filesystem::path& root,const CT::FileInfo& info)
        :path_(root/info.name),info_(info),buffer_(1<<20) {
        if(::stat(path_.c_str(),&before_) || !S_ISREG(before_.st_mode) ||
           uint64_t(before_.st_size)!=info.bytes) throw std::runtime_error("compiled artifact length/type mismatch: "+info.name);
        input_.rdbuf()->pubsetbuf(buffer_.data(),buffer_.size());
        input_.open(path_,std::ios::binary);
        if(!input_) throw std::runtime_error("cannot open compiled artifact: "+info.name);
    }
    void read(void* bytes,size_t size) {
        if(size>info_.bytes-consumed_ || !input_.read(static_cast<char*>(bytes),size))
            throw std::runtime_error("compiled artifact read failed: "+info_.name);
        digest_.update(bytes,size); consumed_+=size;
    }
    void finish() {
        struct stat after{};
        if(consumed_!=info_.bytes || input_.peek()!=std::char_traits<char>::eof() || input_.bad() ||
           digest_.finish()!=info_.sha256 || ::stat(path_.c_str(),&after) || !SameManifest(before_,after))
            throw std::runtime_error("compiled artifact changed or checksum mismatch: "+info_.name);
    }
};
struct CompiledCatalog {
    std::vector<CT::Route> routes;
    std::vector<std::vector<uint64_t>> paths;
};
CompiledCatalog ReadCompiledCatalog(const std::filesystem::path& root,const CT::Header& h,
                                    uint64_t budget,const std::string& target,bool build_paths=true) {
    // This limit covers temporary catalog objects and page buffers. The final
    // namespace index, including prior batches, has its own lifetime and size.
    const uint64_t bytes_per_directory=sizeof(CT::Route)+(build_paths?sizeof(std::vector<uint64_t>):0)+3;
    const uint64_t bitmap_bytes=h.bucket_pages/8+(h.bucket_pages%8!=0);
    if(h.directories>budget/bytes_per_directory || h.artifacts[3].bytes>budget/2 || bitmap_bytes>budget/2)
        throw std::runtime_error("compiled catalog exceeds import memory budget");
    uint64_t used=h.directories*bytes_per_directory+bitmap_bytes+(16ULL<<20);
    if(used>budget) throw std::runtime_error("compiled catalog exceeds import memory budget");
    const auto before=CT::Sha256(root/"routes.bin");
    if(before!=h.artifacts[3].sha256 || std::filesystem::file_size(root/"routes.bin")!=h.artifacts[3].bytes)
        throw std::runtime_error("compiled route catalog checksum/length mismatch");
    std::ifstream in(root/"routes.bin",std::ios::binary);
    CompiledCatalog c; c.routes.resize(h.directories); if(build_paths) c.paths.resize(h.directories);
    std::vector<bool> seen(h.directories,false), buckets(h.bucket_pages,false);
    uint64_t files=0;
    CT::Route r; uint64_t count=0;
    while(CT::ReadRoute(in,r,budget-used)) {
        if(++count>h.directories || r.ordinal>h.directories || seen[r.ordinal-1])
            throw std::runtime_error("duplicate/out-of-range compiled route");
        seen[r.ordinal-1]=true;
        used+=r.name.size()+r.plan.directory.slots.size()*4+r.plan.directory.buckets.size()*sizeof(MasstreeLHM::DirectoryBlockStore::bucket_meta);
        if(used>budget) throw std::runtime_error("compiled catalog exceeds import memory budget");
        if(r.plan.directory.entry_count>h.files-files) throw std::runtime_error("compiled route file count overflow");
        files+=r.plan.directory.entry_count;
        for(auto& b:r.plan.directory.buckets) {
            if(b.block_id>h.bucket_pages || buckets[b.block_id-1]) throw std::runtime_error("duplicate/out-of-range compiled bucket");
            buckets[b.block_id-1]=true;
        }
        c.routes[r.ordinal-1]=std::move(r);
    }
    if(count!=h.directories || files!=h.files || std::find(buckets.begin(),buckets.end(),false)!=buckets.end() ||
       CT::Sha256(root/"routes.bin")!=before) throw std::runtime_error("compiled catalog totals/checksum mismatch");
    std::vector<uint16_t> depth(h.directories);
    for(size_t i=0;i<c.routes.size();++i) {
        depth[i]=i?depth[c.routes[i].parent_ordinal-1]+1:1;
        if(depth[i]>4096) throw std::runtime_error("compiled directory exceeds maximum depth");
    }
    if(build_paths) for(size_t i=0;i<c.routes.size();++i) {
        const auto& route=c.routes[i];
        auto& path=c.paths[i];
        if(i) path=c.paths[route.parent_ordinal-1];
        path.push_back(MasstreeLHM::PathKey::hash_component(i?route.name:target.substr(1)));
        if(path.size()>4096 || path.size()*8>budget-used) throw std::runtime_error("compiled path catalog exceeds budget/depth");
        used+=path.size()*8;
    }
    return c;
}
}
extern "C" int zb_lhm_compiled_info(const char* directory,zb_lhm_import_result* result,char* error,size_t size) {
    Error(error,size,""); if(result) *result={};
    if(!directory || !*directory || !result) { Error(error,size,"template directory and result required"); return ZB_LHM_INVALID; }
    try {
        const auto h=CT::ReadHeader(directory);
        if(h.files>UINT64_MAX-h.directories) throw std::runtime_error("compiled count overflow");
        result->files=h.files; result->directories=h.directories; result->processed_records=h.files+h.directories;
        return ZB_LHM_OK;
    } catch(const std::exception& e) { Error(error,size,e.what()); return ZB_LHM_ERROR; }
}
static int ImportCompiledBatch(bool shared,const char* file,const char* target,
    const char* directory,const char* batch_id,const char* header_sha256,uint64_t budget,
    zb_lhm_import_result* result,int* already_committed,zb_lhm_compiled_profile* output_profile,
    char* error,size_t size) {
    zb_lhm_compiled_profile local_profile{};
    auto& profile=output_profile?*output_profile:local_profile;
    profile={}; ProfileTimer total_timer(profile.total_ns);
    Error(error,size,""); if(result) *result={}; if(already_committed) *already_committed=0;
    if(!file || !*file || !target || !directory || !*directory || !batch_id || !header_sha256 ||
       !result || !already_committed || budget<(32ULL<<20)) {
        Error(error,size,"file, target, compiled directory, identity, outputs and >=32 MiB budget required"); return ZB_LHM_INVALID;
    }
    BatchContext ctx; bool namespace_open=false, exclusive_lock=false;
    try {
        auto phase=ProfileClock::now();
        const std::string id=batch_id,digest=header_sha256,destination=target;
        if(!BatchIdValid(id) || !DigestValid(digest)) throw std::runtime_error("invalid compiled batch identity");
        ValidateBatchTarget(destination);
        const auto path=std::filesystem::absolute(file).lexically_normal().string();
        const auto root=std::filesystem::absolute(directory).lexically_normal();
        ctx.reset(new zb_lhm_context); LockStore(*ctx,path); exclusive_lock=true;
        BatchUndo::Recover(path); const auto previous=ReadBatchReceipt(path); CleanupBatchTemps(path);
        profile.checkpoint_validation_ns=ProfileElapsed(phase);
        if(previous.batch_id==id) {
            MasstreeLHM::SnapshotReader mode(path+".snapshot"); (void)mode.u64();
            if((mode.u64()==MasstreeLHM::kSharedSnapshotVersion)!=shared)
                throw std::runtime_error("committed batch index mode differs from requested mode");
            if(previous.target!=destination || previous.digest!=digest)
                throw std::runtime_error("committed compiled batch ID reused with different parameters");
            *result=previous.result; *already_committed=1; return ZB_LHM_OK;
        }
        phase=ProfileClock::now();
        if(CT::Sha256(root/"header.bin")!=digest) throw std::runtime_error("compiled header SHA256 differs from batch identity");
        const auto h=CT::ReadHeader(root);
        if(h.files>UINT64_MAX-h.directories) throw std::runtime_error("compiled count overflow");
        auto catalog=ReadCompiledCatalog(root,h,budget,destination,!shared);
        CompiledReader inodes(root,h.artifacts[0]), heads(root,h.artifacts[1]), buckets(root,h.artifacts[2]);
        profile.template_validation_ns=ProfileElapsed(phase);
        worker().run([&](threadinfo& ti) {
            phase=ProfileClock::now();
            namespace_open=true; ctx->ns.reopen_snapshot(path,path+".snapshot",ti,true);
            if(shared) ctx->ns.require_shared_importable();
            if(ctx->ns.shared_readonly() && !shared) throw std::runtime_error("shared-index database requires shared compiled imports");
            if(ctx->ns.append_target_exists(destination,ti)) throw std::runtime_error("compiled import target already exists");
            profile.reopen_ns=ProfileElapsed(phase); phase=ProfileClock::now();
            ctx->ns.begin_batch_undo();
            const auto reservation=ctx->ns.reserve_template_blocks(h.inode_pages,h.bucket_pages,h.directories);
            profile.reserve_ns=ProfileElapsed(phase);
            CT::Bases bases{reservation.inode_first,reservation.bucket_first,reservation.head_first,
                            reservation.sequence_base,h.inode_pages,h.bucket_pages,h.directories};
            const uint64_t now=std::time(nullptr);
            MasstreeLHM::metadata_block_bytes bytes{};
            uint64_t inode_count=0,bucket_count=0;
            // Each file has its own contiguous page range and buffered writer.
            // Keep streams sequential instead of merging the former odd/even layout.
            for(uint64_t page=0;page<h.inode_pages;++page) {
                { ProfileTimer timer(profile.read_hash_ns); inodes.read(bytes.data(),bytes.size()); }
                { ProfileTimer timer(profile.relocate_ns); CT::RelocateInodePage(bytes,page,bases,now); }
                const auto count=reinterpret_cast<const MasstreeLHM::inode_block_image*>(bytes.data())->header.used_count;
                if(count>h.files-inode_count) throw std::runtime_error("compiled inode count exceeds header");
                inode_count+=count;
                { ProfileTimer timer(profile.submit_ns); ctx->ns.write_template_block(0,page,bytes); }
            }
            for(uint64_t page=0;page<h.bucket_pages;++page) {
                { ProfileTimer timer(profile.read_hash_ns); buckets.read(bytes.data(),bytes.size()); }
                { ProfileTimer timer(profile.relocate_ns); CT::RelocateBucketPage(bytes,page,bases); }
                const auto count=reinterpret_cast<const MasstreeLHM::directory_block_image*>(bytes.data())->header.entry_count;
                if(count>h.files-bucket_count) throw std::runtime_error("compiled bucket count exceeds header");
                bucket_count+=count;
                { ProfileTimer timer(profile.submit_ns); ctx->ns.write_template_block(1,page,bytes); }
            }
            if(inode_count!=h.files || bucket_count!=h.files)
                throw std::runtime_error("compiled page counts differ from header");
            for(uint64_t page=0;page<h.head_pages;++page) {
                { ProfileTimer timer(profile.read_hash_ns); heads.read(bytes.data(),bytes.size()); }
                { ProfileTimer timer(profile.relocate_ns); CT::RelocateHeadPage(bytes,page,bases,now,destination.substr(1)); }
                { ProfileTimer timer(profile.submit_ns); ctx->ns.write_template_block(2,page,bytes); }
            }
            {
                ProfileTimer timer(profile.read_hash_ns);
                inodes.finish(); buckets.finish(); heads.finish();
                if(CT::Sha256(root/"header.bin")!=digest) throw std::runtime_error("compiled header changed during import");
            }
            { ProfileTimer timer(profile.data_sync_ns); ctx->ns.finish_template_writes(); }
            const auto io=ctx->ns.template_write_stats();
            profile.write_calls=io.write_calls; profile.write_bytes=io.write_bytes; profile.pwrite_ns=io.write_ns;
            profile.main_sync_ns=io.main_sync_ns; profile.heads_sync_ns=io.heads_sync_ns;
            profile.buckets_sync_ns=io.buckets_sync_ns; profile.writeback_calls=io.writeback_calls;
            profile.writeback_wait_ns=io.writeback_wait_ns;
            phase=ProfileClock::now();
            const std::vector<uint64_t> empty;
            if(!shared) for(size_t n=0;n<catalog.routes.size();++n) {
                auto& route=catalog.routes[n]; CT::RelocateRoute(route,bases);
                ctx->ns.install_template_directory(n?catalog.paths[route.parent_ordinal-1]:empty,
                    catalog.paths[n].back(),route.plan.head_block_id,std::move(route.plan),ti);
                if((n&63)==0) {
                    globalepoch.store(globalepoch.load()+2); active_epoch.store(globalepoch.load()); ti.rcu_quiesce();
                }
            }
            profile.directory_install_ns=ProfileElapsed(phase);
            *result={CT::DirInode(1,bases),h.files,h.directories,h.files+h.directories,0};
            phase=ProfileClock::now();
            const auto temp=path+".snapshot.batch.tmp";
            if(shared) ctx->ns.save_shared_snapshot(temp,digest,h,bases,catalog.routes,destination,ti);
            else ctx->ns.save_incremental_snapshot(temp,ti);
            SyncFile(temp); BatchFault("batch_snapshot");
            profile.snapshot_ns=ProfileElapsed(phase); phase=ProfileClock::now();
            const auto receipt=MakeBatchReceipt(BatchUndo::SnapshotFingerprint(temp),id,destination,digest,*result);
            ctx->ns.batch_undo()->PrepareCommit(temp,receipt);
            ctx->ns.destroy(ti); namespace_open=false;
            if(::rename(temp.c_str(),(path+".snapshot").c_str())) throw std::runtime_error("compiled checkpoint publication failed");
            SyncFile(std::filesystem::path(path).parent_path().string(),true); BatchFault("batch_published");
            BatchUndo::Recover(path); CleanupBatchTemps(path);
            profile.commit_ns=ProfileElapsed(phase);
        });
        return ZB_LHM_OK;
    } catch(const std::exception& e) {
        std::string message=e.what();
        if(ctx && exclusive_lock) {
            try {
                if(namespace_open) worker().run([&](threadinfo& ti) { ctx->ns.destroy(ti); namespace_open=false; });
                if(BatchUndo::Recover(ctx->path)) CleanupBatchTemps(ctx->path);
            } catch(const std::exception& cleanup) { message+="; recovery pending: "; message+=cleanup.what(); }
        }
        Error(error,size,message); return ZB_LHM_ERROR;
    }
}

extern "C" int zb_lhm_import_compiled_batch(const char* file,const char* target,
    const char* directory,const char* batch_id,const char* header_sha256,uint64_t budget,
    zb_lhm_import_result* result,int* already_committed,char* error,size_t size) {
    return zb_lhm_import_compiled_batch_profiled(file,target,directory,batch_id,header_sha256,
        budget,result,already_committed,nullptr,error,size);
}

extern "C" int zb_lhm_import_compiled_batch_profiled(const char* file,const char* target,
    const char* directory,const char* id,const char* digest,uint64_t budget,
    zb_lhm_import_result* result,int* already,zb_lhm_compiled_profile* profile,char* error,size_t size) {
    return ImportCompiledBatch(false,file,target,directory,id,digest,budget,result,already,profile,error,size);
}
extern "C" int zb_lhm_import_shared_compiled_batch_profiled(const char* file,const char* target,
    const char* directory,const char* id,const char* digest,uint64_t budget,
    zb_lhm_import_result* result,int* already,zb_lhm_compiled_profile* profile,char* error,size_t size) {
    return ImportCompiledBatch(true,file,target,directory,id,digest,budget,result,already,profile,error,size);
}

// Offline conversion reads old routing and verifies its exact correspondence to
// the compiled sources. It rewrites only routing/checkpoint files, never data pages.
extern "C" int zb_lhm_convert_shared_index(const char* file,const char* mapping,uint64_t budget,
                                           char* error,size_t size) {
    Error(error,size,"");
    if(!file || !mapping || budget<(32ULL<<20)) {Error(error,size,"store, mapping TSV and >=32 MiB required");return ZB_LHM_INVALID;}
    BatchContext ctx; bool opened=false,locked=false;
    try {
        struct Request {std::string target,directory,digest;}; std::vector<Request> requests;
        std::ifstream input(mapping); if(!input) throw std::runtime_error("cannot open conversion mapping");
        std::string line; std::set<std::string> names;
        while(std::getline(input,line)) {
            const auto a=line.find('\t'),b=a==std::string::npos?a:line.find('\t',a+1);
            if(a==std::string::npos || b==std::string::npos || line.find('\t',b+1)!=std::string::npos || requests.size()>=MasstreeLHM::kMaxIndexFiles)
                throw std::runtime_error("mapping rows must be target TAB compiled-directory TAB header-SHA256");
            Request r{line.substr(0,a),line.substr(a+1,b-a-1),line.substr(b+1)};
            ValidateBatchTarget(r.target);
            if(!DigestValid(r.digest) || !names.insert(r.target).second) throw std::runtime_error("invalid/duplicate conversion mapping");
            requests.push_back(std::move(r));
        }
        if(input.bad() || requests.empty()) throw std::runtime_error("empty/failed conversion mapping");
        const auto path=std::filesystem::absolute(file).lexically_normal().string();
        ctx.reset(new zb_lhm_context); LockStore(*ctx,path); locked=true;
        BatchUndo::Recover(path); const auto previous=ReadBatchReceipt(path); CleanupBatchTemps(path);
        worker().run([&](threadinfo& ti) {
            opened=true; ctx->ns.reopen_snapshot(path,path+".snapshot",ti,true);
            if(ctx->ns.shared_readonly()) {
                const auto& c=ctx->ns.shared_mapping_catalog();
                if(requests.size()!=c.instances.size()) throw std::runtime_error("already shared but mapping differs");
                for(const auto& r:requests) {
                    const auto found=std::find_if(c.instances.begin(),c.instances.end(),[&](const auto& b){return "/"+b.name==r.target;});
                    if(found==c.instances.end() || c.templates[found->template_index].digest!=r.digest)
                        throw std::runtime_error("already shared but mapping differs");
                }
                ctx->ns.destroy(ti); opened=false; return;
            }
            const auto old=ctx->ns.shared_index_catalog();
            if(old.files.size()!=requests.size() || old.roots.size()!=requests.size() || previous.batch_id.empty())
                throw std::runtime_error("conversion requires all v5 compiled batches and their complete mapping");
            // Preserve reviewable original checkpoint/receipt; exclusive creation.
            for(const auto* suffix:{".snapshot",".batch-receipt"}) {
                const auto source=path+suffix,dest=source+".pre-shared";
                if(std::filesystem::exists(dest)) {
                    if(CT::Sha256(source)!=CT::Sha256(dest)) throw std::runtime_error("pre-shared backup differs from conversion baseline");
                } else {
                    // Publish the backup only after the complete copy is durable.
                    // A crash while copying leaves a replaceable temporary file.
                    const auto temp=dest+".tmp";
                    std::filesystem::copy_file(source,temp,std::filesystem::copy_options::overwrite_existing);
                    SyncFile(temp); BatchFault("shared_conversion_backup");
                    if(::rename(temp.c_str(),dest.c_str())) throw std::runtime_error("cannot publish pre-shared backup");
                }
            }
            SyncFile(std::filesystem::path(path).parent_path().string(),true);
            std::map<uint64_t,MasstreeLHM::IndexFile> by_root;
            for(const auto& f:old.files) {
                MasstreeLHM::SnapshotReader in(MasstreeLHM::IndexPath(path,f.token));
                if(in.u64()!=MasstreeLHM::kIndexFileMagic || in.u64()!=1 || in.u64()!=1 || !by_root.emplace(in.u64(),f).second)
                    throw std::runtime_error("legacy indexes are not one top-level compiled batch each");
            }
            ctx->ns.begin_batch_undo();
            MasstreeLHM::IndexCatalog next; next.roots=old.roots; MasstreeLHM::SharedCatalog shared;
            // Group requests by digest to bound conversion memory to one template.
            std::map<std::string,std::vector<const Request*>> groups;
            for(const auto& r:requests) groups[r.digest].push_back(&r);
            for(const auto& group:groups) {
                const auto root=std::filesystem::absolute(group.second.front()->directory);
                if(CT::Sha256(root/"header.bin")!=group.first) throw std::runtime_error("conversion template header mismatch");
                const auto h=CT::ReadHeader(root);
                auto catalog=ReadCompiledCatalog(root,h,budget,group.second.front()->target,false);
                MasstreeLHM::directory_block_image anchor{};
                if(h.files) {
                    if(CT::Sha256(root/"bucket.pages")!=h.artifacts[2].sha256)
                        throw std::runtime_error("conversion template bucket checksum mismatch");
                    std::ifstream buckets(root/"bucket.pages",std::ios::binary);
                    if(!buckets.read(reinterpret_cast<char*>(&anchor),sizeof(anchor))) throw std::runtime_error("missing template bucket anchor");
                }
                const uint64_t id=shared.templates.size();
                shared.templates.push_back({group.first,h.files,h.directories,h.inode_pages,h.bucket_pages});
                const auto token=ctx->ns.batch_undo()->CreateIndex();
                const auto index_path=MasstreeLHM::IndexPath(path,token);
                const auto checksum=MasstreeLHM::SharedRoutes::save(index_path,catalog.routes);
                SyncFile(index_path); auto identity=MasstreeLHM::InspectIndex(path,token); identity.checksum=checksum;
                next.files.push_back(identity); BatchFault("shared_conversion_index");
                for(const auto* r:group.second) {
                    const auto found=by_root.find(MasstreeLHM::PathKey::hash_component(r->target.substr(1)));
                    if(found==by_root.end()) throw std::runtime_error("missing legacy batch index for mapping");
                    const auto bases=ctx->ns.inspect_legacy_shared_instance(found->second,r->target,h,catalog.routes,h.files?&anchor:nullptr);
                    shared.instances.push_back({r->target.substr(1),id,bases.inode_first,bases.bucket_first,bases.head_first});
                    by_root.erase(found);
                }
            }
            if(!by_root.empty()) throw std::runtime_error("unmapped legacy batch indexes");
            const auto temp=path+".snapshot.batch.tmp";
            ctx->ns.save_converted_shared_snapshot(temp,next,shared); SyncFile(temp); BatchFault("shared_conversion_snapshot");
            const auto receipt=MakeBatchReceipt(BatchUndo::SnapshotFingerprint(temp),previous.batch_id,previous.target,previous.digest,previous.result);
            ctx->ns.batch_undo()->PrepareCommit(temp,receipt);
            ctx->ns.destroy(ti); opened=false;
            if(::rename(temp.c_str(),(path+".snapshot").c_str())) throw std::runtime_error("conversion checkpoint publication failed");
            SyncFile(std::filesystem::path(path).parent_path().string(),true); BatchFault("shared_conversion_published");
            BatchUndo::Recover(path); CleanupBatchTemps(path);
        });
        return ZB_LHM_OK;
    } catch(const std::exception& e) {
        std::string message=e.what();
        if(ctx && locked) {
            try {
                if(opened) worker().run([&](threadinfo& ti){ctx->ns.destroy(ti);opened=false;});
                if(BatchUndo::Recover(ctx->path)) CleanupBatchTemps(ctx->path);
            } catch(const std::exception& e) {message+="; recovery pending: "; message+=e.what();}
        }
        Error(error,size,message);return ZB_LHM_ERROR;
    }
}
