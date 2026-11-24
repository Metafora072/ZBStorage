// Exclusive offline batch lifecycle. The existing online RPC keeps its original
// contract; only this entry point installs undo protection and commits a batch.
#include <sstream>
#include <sys/stat.h>

namespace {
using MasstreeLHM::BatchUndo;
using MasstreeLHM::BatchFault;
constexpr size_t kBatchReceiptLimit=8192;

struct BatchContextDeleter {
    void operator()(zb_lhm_context* ctx) const noexcept {
        // PersistentStore's destructor clears worker-local allocator/cache
        // handles. Destroying it on the calling thread can leave a stale TLS
        // pointer if a later batch's store reuses the same allocation address.
        try { worker().run([ctx](threadinfo&) { delete ctx; }); }
        catch(...) { std::terminate(); }
    }
};
using BatchContext=std::unique_ptr<zb_lhm_context,BatchContextDeleter>;

uint64_t ReceiptHash(const std::string& bytes) {
    uint64_t h=14695981039346656037ULL;
    for(unsigned char c:bytes) h=(h^c)*1099511628211ULL;
    return h;
}
bool BatchIdValid(const std::string& id) {
    if(id.empty() || id.size()>128) return false;
    for(unsigned char c:id)
        if(!((c>='a' && c<='z') || (c>='A' && c<='Z') ||
             (c>='0' && c<='9') || c=='-' || c=='_' || c=='.')) return false;
    return true;
}
bool DigestValid(const std::string& sha) {
    return sha.size()==64 && sha.find_first_not_of("0123456789abcdef")==std::string::npos;
}
void ValidateBatchTarget(const std::string& target) {
    auto p=MasstreeLHM::PathKey::parse_absolute_path(target);
    if(p.normalized_path!=target || p.components.size()!=1 ||
       p.components[0].size()>64 || p.components[0]=="." || p.components[0]=="..")
        throw std::runtime_error("batch target must be a new, canonical top-level directory (name <=64 bytes)");
    for(unsigned char c:target) if(c<32 || c==127)
        throw std::runtime_error("batch target contains a control character");
}
struct BatchReceipt {
    std::string bytes, batch_id, target, digest;
    zb_lhm_import_result result{};
};

// Validate the complete snapshot and every segment, including unexpected files.
// A receipt alone never certifies that a store is reopenable.
std::string CheckedSnapshot(const std::string& path) {
    auto fingerprint=BatchUndo::SnapshotFingerprint(path+".snapshot");
    MasstreeLHM::SnapshotReader in(path+".snapshot");
    if(in.u64()!=MasstreeLHM::kSegmentedSnapshotMagic || !MasstreeLHM::SupportedSnapshotVersion(in.u64()))
        throw std::runtime_error("unsupported checkpoint: snapshot version 4 or 5 required");
    const auto layout=MasstreeLHM::ReadSegmentLayout(in);
    MasstreeLHM::ValidateSegmentFiles(path,layout);
    if(!(MasstreeLHM::ScanSegmentLayout(path,layout.segment_bytes)==layout))
        throw std::runtime_error("batch checkpoint has missing or untracked segments");
    MasstreeLHM::ValidateIndexFiles(path,MasstreeLHM::CheckpointIndexes(path+".snapshot"));
    return fingerprint;
}
BatchReceipt ReadBatchReceipt(const std::string& path) {
    const auto fingerprint=CheckedSnapshot(path);
    BatchReceipt result; const auto receipt_path=path+".batch-receipt";
    struct stat st{};
    if(::stat(receipt_path.c_str(),&st)) {
        if(errno==ENOENT) return result;
        throw std::runtime_error("cannot stat batch receipt");
    }
    if(!S_ISREG(st.st_mode) || st.st_size<=0 || st.st_size>off_t(kBatchReceiptLimit))
        throw std::runtime_error("invalid batch receipt length");
    std::ifstream input(receipt_path,std::ios::binary);
    result.bytes.resize(static_cast<size_t>(st.st_size));
    if(!input.read(result.bytes.data(),result.bytes.size())) throw std::runtime_error("cannot read batch receipt");
    const auto pos=result.bytes.rfind("\nchecksum=");
    if(pos==std::string::npos || result.bytes.back()!='\n') throw std::runtime_error("invalid batch receipt trailer");
    const std::string body=result.bytes.substr(0,pos+1);
    const std::string checksum=result.bytes.substr(pos+10,result.bytes.size()-pos-11);
    if(ParseNumber(checksum)!=ReceiptHash(body)) throw std::runtime_error("batch receipt checksum mismatch");
    std::map<std::string,std::string> fields; std::istringstream lines(body); std::string line;
    while(std::getline(lines,line)) {
        const auto split=line.find('=');
        if(split==std::string::npos || !fields.emplace(line.substr(0,split),line.substr(split+1)).second)
            throw std::runtime_error("invalid batch receipt fields");
    }
    if(fields.size()!=10 || fields.at("version")!="1") throw std::runtime_error("unsupported batch receipt");
    if(fields.at("snapshot")!=fingerprint)
        throw std::runtime_error("checkpoint changed outside the batch runner; refusing stale batch receipt");
    result.batch_id=fields.at("batch_id"); result.target=fields.at("target"); result.digest=fields.at("manifest_sha256");
    if(!BatchIdValid(result.batch_id) || !DigestValid(result.digest)) throw std::runtime_error("invalid batch receipt identity");
    ValidateBatchTarget(result.target);
    result.result={ParseNumber(fields.at("root_inode_id")),ParseNumber(fields.at("files")),
        ParseNumber(fields.at("directories")),ParseNumber(fields.at("processed_records")),ParseNumber(fields.at("error_line"))};
    if(!result.result.root_inode_id || !result.result.directories || result.result.error_line ||
       result.result.files>UINT64_MAX-result.result.directories ||
       result.result.processed_records!=result.result.files+result.result.directories)
        throw std::runtime_error("invalid committed batch counts");
    return result;
}
std::string MakeBatchReceipt(const std::string& fingerprint,const std::string& id,
    const std::string& target,const std::string& digest,const zb_lhm_import_result& r) {
    std::ostringstream out;
    out<<"version=1\nsnapshot="<<fingerprint<<"\nbatch_id="<<id<<"\ntarget="<<target
       <<"\nmanifest_sha256="<<digest<<"\nroot_inode_id="<<r.root_inode_id<<"\nfiles="<<r.files
       <<"\ndirectories="<<r.directories<<"\nprocessed_records="<<r.processed_records<<"\nerror_line=0\n";
    const auto body=out.str(); return body+"checksum="+std::to_string(ReceiptHash(body))+"\n";
}
bool SameManifest(const struct stat& a,const struct stat& b) {
    return a.st_dev==b.st_dev && a.st_ino==b.st_ino && a.st_size==b.st_size &&
        a.st_mtim.tv_sec==b.st_mtim.tv_sec && a.st_mtim.tv_nsec==b.st_mtim.tv_nsec &&
        a.st_ctim.tv_sec==b.st_ctim.tv_sec && a.st_ctim.tv_nsec==b.st_ctim.tv_nsec;
}
void CleanupBatchTemps(const std::string& path) {
    bool changed=false;
    for(const char* suffix:{".snapshot.batch.tmp",".batch-receipt.tmp"}) {
        if(!::unlink((path+suffix).c_str())) changed=true;
        else if(errno!=ENOENT) throw std::runtime_error("cannot remove completed batch temporary file");
    }
    if(changed) SyncFile(std::filesystem::path(path).parent_path().string(),true);
}
}

extern "C" int zb_lhm_batch_status(const char* file,char* receipt,size_t receipt_size,char* error,size_t size) {
    Error(error,size,""); if(receipt && receipt_size) receipt[0]=0;
    if(!file || !*file || !receipt || !receipt_size) { Error(error,size,"file and receipt buffer required"); return ZB_LHM_INVALID; }
    try {
        const auto path=std::filesystem::absolute(file).lexically_normal().string();
        BatchContext ctx(new zb_lhm_context); LockStore(*ctx,path);
        if(BatchUndo::Exists(path)) throw std::runtime_error("unfinished batch; run recover before status");
        const auto r=ReadBatchReceipt(path);
        if(r.bytes.size()>=receipt_size) throw std::runtime_error("receipt buffer too small");
        std::memcpy(receipt,r.bytes.c_str(),r.bytes.size()+1);
        return ZB_LHM_OK;
    } catch(const std::exception& e) { Error(error,size,e.what()); return ZB_LHM_ERROR; }
}
extern "C" int zb_lhm_recover_batch(const char* file,char* error,size_t size) {
    Error(error,size,"");
    if(!file || !*file) { Error(error,size,"file required"); return ZB_LHM_INVALID; }
    try {
        const auto path=std::filesystem::absolute(file).lexically_normal().string();
        BatchContext ctx(new zb_lhm_context); LockStore(*ctx,path);
        BatchUndo::Recover(path); (void)ReadBatchReceipt(path); CleanupBatchTemps(path);
        return ZB_LHM_OK;
    } catch(const std::exception& e) { Error(error,size,e.what()); return ZB_LHM_ERROR; }
}
extern "C" int zb_lhm_import_batch(const char* file,const char* target,const char* manifest,
    const char* batch_id,const char* manifest_sha256,uint64_t budget,uint64_t expected_files,
    uint64_t expected_directories,zb_lhm_import_result* result,int* already_committed,char* error,size_t size) {
    Error(error,size,""); if(result) *result={}; if(already_committed) *already_committed=0;
    if(!file || !*file || !target || !manifest || !batch_id || !manifest_sha256 || !result || !already_committed || budget<(1ULL<<20)) {
        Error(error,size,"file, target, manifest, batch identity, outputs and at least 1 MiB budget required"); return ZB_LHM_INVALID;
    }
    BatchContext ctx; bool namespace_open=false, exclusive_lock=false;
    try {
        const std::string id=batch_id, digest=manifest_sha256, destination=target;
        if(!BatchIdValid(id) || !DigestValid(digest)) throw std::runtime_error("invalid batch ID or lowercase SHA256");
        ValidateBatchTarget(destination);
        const auto path=std::filesystem::absolute(file).lexically_normal().string();
        ctx.reset(new zb_lhm_context); LockStore(*ctx,path); exclusive_lock=true;
        BatchUndo::Recover(path);
        const auto previous=ReadBatchReceipt(path);
        CleanupBatchTemps(path);
        if(previous.batch_id==id) {
            if(previous.target!=destination || previous.digest!=digest ||
               (expected_files && expected_files!=previous.result.files) ||
               (expected_directories && expected_directories!=previous.result.directories))
                throw std::runtime_error("committed batch ID was reused with different parameters");
            *result=previous.result; *already_committed=1; return ZB_LHM_OK;
        }
        struct stat before{}, after{};
        if(::stat(manifest,&before) || !S_ISREG(before.st_mode)) throw std::runtime_error("manifest must be a regular file");
        worker().run([&](threadinfo& ti) {
            namespace_open=true;
            ctx->ns.reopen_snapshot(path,path+".snapshot",ti);
            ctx->ns.begin_batch_undo();
            ctx->batch_manifest_sha256=digest;
        });
        char import_error[2048]{};
        if(zb_lhm_import_manifest(ctx.get(),target,manifest,budget,result,import_error,sizeof(import_error))!=ZB_LHM_OK) {
            std::string detail=import_error;
            if(ctx->ns.batch_undo() && !ctx->ns.batch_undo()->error().empty()) detail+="; "+ctx->ns.batch_undo()->error();
            throw std::runtime_error("batch import failed: "+detail);
        }
        if(::stat(manifest,&after) || !SameManifest(before,after)) throw std::runtime_error("manifest changed during import");
        if((expected_files && result->files!=expected_files) ||
           (expected_directories && result->directories!=expected_directories))
            throw std::runtime_error("imported counts differ from expected batch counts");
        worker().run([&](threadinfo& ti) {
            const auto temp=path+".snapshot.batch.tmp";
            ctx->ns.save_snapshot(temp,ti); SyncFile(temp);
            BatchFault("batch_snapshot");
            const auto receipt=MakeBatchReceipt(BatchUndo::SnapshotFingerprint(temp),id,destination,digest,*result);
            ctx->ns.batch_undo()->PrepareCommit(temp,receipt);
            // Finish every flush while the undo guard is still attached. Once
            // the namespace is closed, no cached image can overwrite recovery.
            ctx->ns.destroy(ti); namespace_open=false;
            if(::rename(temp.c_str(),(path+".snapshot").c_str())) throw std::runtime_error("batch checkpoint publication failed");
            SyncFile(std::filesystem::path(path).parent_path().string(),true);
            BatchFault("batch_published");
            BatchUndo::Recover(path); // publish receipt, then retire the undo log
            CleanupBatchTemps(path);
        });
        return ZB_LHM_OK;
    } catch(const std::exception& e) {
        std::string message=e.what();
        if(ctx && exclusive_lock) {
            try {
                if(namespace_open) worker().run([&](threadinfo& ti) { ctx->ns.destroy(ti); namespace_open=false; });
                // Keep the original main-file flock through all rollback work.
                if(BatchUndo::Recover(ctx->path)) CleanupBatchTemps(ctx->path);
            } catch(const std::exception& cleanup) {
                message+="; recovery pending: "; message+=cleanup.what();
            }
        }
        Error(error,size,message); return ZB_LHM_ERROR;
    }
}
