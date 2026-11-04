#include "LhmBridge.h"
#include "mds/storage/LhmInodeId.h"
#include "lhm_namespace.hh"
#include "timestamp.hh"
#include "LhmAttributeCodec.h"
#include <ctime>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>

relaxed_atomic<mrcu_epoch_type> globalepoch(1);
relaxed_atomic<mrcu_epoch_type> active_epoch(1);
volatile bool recovering = false;
kvtimestamp_t initial_timestamp;

struct zb_lhm_context {
    MasstreeLHM::LhmNamespace ns;
    std::string path;
    int lock_fd=-1;
    ~zb_lhm_context() { if(lock_fd>=0) ::close(lock_fd); }
};
namespace {
void Error(char* out,size_t n,const std::string& message) {
    if(out && n) { std::strncpy(out,message.c_str(),n-1); out[n-1]=0; }
}
void SyncFile(const std::string& path, bool directory=false) {
    int fd=::open(path.c_str(),O_RDONLY|(directory?O_DIRECTORY:0));
    if(fd<0) throw std::runtime_error("open for sync failed: "+path);
    int rc=::fsync(fd); ::close(fd);
    if(rc) throw std::runtime_error("fsync failed: "+path);
}
void LockStore(zb_lhm_context& ctx,const std::string& path) {
    ctx.path=path; ctx.lock_fd=::open(path.c_str(),O_RDWR);
    if(ctx.lock_fd<0 || ::flock(ctx.lock_fd,LOCK_EX|LOCK_NB)) throw std::runtime_error("store is already open or inaccessible");
}
// One module-owned worker and upstream threadinfo for the module lifetime.
// Multiple contexts/caller threads are serialized; no per-request threadinfo leaks.
class Worker {
    std::mutex mutex_;
    std::condition_variable ready_;
    std::queue<std::function<void()>> jobs_;
    bool stop_ = false;
    threadinfo* ti_ = nullptr;
    std::thread thread_;
public:
    Worker() : thread_([this] {
        ti_ = threadinfo::make(threadinfo::TI_MAIN, 0);
        initial_timestamp = timestamp();
        for (;;) {
            std::function<void()> job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                ready_.wait(lock, [&] { return stop_ || !jobs_.empty(); });
                if (stop_ && jobs_.empty()) break;
                job = std::move(jobs_.front()); jobs_.pop();
            }
            ti_->rcu_start();
            job();
            globalepoch.store(globalepoch.load() + 2);
            active_epoch.store(globalepoch.load());
            ti_->rcu_stop();
        }
        // Upstream threadinfo has no destructor/unregister API; retain one until exit.
    }) {}
    ~Worker() {
        { std::lock_guard<std::mutex> lock(mutex_); stop_ = true; }
        ready_.notify_one(); thread_.join();
    }
    template<class F> auto run(F f) -> decltype(f(*ti_)) {
        using R = decltype(f(*ti_));
        auto task = std::make_shared<std::packaged_task<R()>>([this, f] { return f(*ti_); });
        auto result = task->get_future();
        { std::lock_guard<std::mutex> lock(mutex_); jobs_.push([task] { (*task)(); }); }
        ready_.notify_one(); return result.get();
    }
};
Worker& worker() { static Worker instance; return instance; }
void export_inode(zb_lhm_context* ctx, const MasstreeLHM::inode_disk& in, zb_lhm_inode* out) {
    *out = {};
    out->inode_id = in.inode_id == ctx->ns.root_inode_id() ? 1 : in.inode_id;
    out->size = in.size_bytes; out->mode = in.mode; out->type = in.type;
    out->ctime_ns = in.ctime_ns; out->mtime_ns = in.mtime_ns; out->version = in.stable_version;
}
uint64_t native_id(zb_lhm_context* ctx, uint64_t id) {
    return id == 1 ? ctx->ns.root_inode_id() : id;
}
template<class F> int call(zb_lhm_context* ctx, F f) {
    if (!ctx) return 0;
    try { return worker().run(f) ? 1 : 0; } catch (...) { return 0; }
}
}
extern "C" {
zb_lhm_context* zb_lhm_open(const char* file) {
    if (!file || !*file) return nullptr;
    try {
        const std::string path = std::filesystem::absolute(file).string();
        // Never truncate existing data; make the fresh-store requirement enforceable.
        const int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
        if (fd < 0) return nullptr;
        ::close(fd);
        return worker().run([path](threadinfo& ti) -> zb_lhm_context* {
            auto ctx = std::make_unique<zb_lhm_context>();
            LockStore(*ctx,path);
            ctx->ns.initialize(ti, path);
            MasstreeLHM::inode_disk root{};
            if (!ctx->ns.read_inode_by_id(ctx->ns.root_inode_id(), root)) {
                ctx->ns.destroy(ti); return nullptr;
            }
            zb_lhm_full_inode attributes{};
            attributes.inode_id = root.inode_id; attributes.type = 2;
            attributes.mode = 0755; attributes.nlink = 2; attributes.version = 1;
            attributes.atime = attributes.mtime = attributes.ctime = std::time(nullptr);
            attributes.name_length = 1; attributes.file_name[0] = '/';
            MasstreeLHM::inode_disk encoded{};
            if (!zb::mds::lhm_codec::Encode(attributes, encoded) ||
                !ctx->ns.replace_inode_attributes(root.inode_id, encoded)) {
                ctx->ns.destroy(ti); return nullptr;
            }
            return ctx.release();
        });
    } catch (...) { return nullptr; }
}
zb_lhm_context* zb_lhm_reopen(const char* file,char* error,size_t size) {
    Error(error,size,"");
    if(!file || !*file) { Error(error,size,"missing store path"); return nullptr; }
    try {
        const auto path=std::filesystem::absolute(file).string();
        return worker().run([&](threadinfo& ti)->zb_lhm_context* {
            auto ctx=std::make_unique<zb_lhm_context>(); LockStore(*ctx,path);
            // Validate before invalidation. No service operations run until the
            // clean snapshot has been removed durably.
            try {
                ctx->ns.reopen_snapshot(path,path+".snapshot",ti);
                if(::unlink((path+".snapshot").c_str())) throw std::runtime_error("cannot invalidate clean snapshot");
                SyncFile(std::filesystem::path(path).parent_path().string(),true);
                return ctx.release();
            } catch(...) { ctx->ns.destroy(ti); throw; }
        });
    } catch(const std::exception& e) { Error(error,size,e.what()); return nullptr; }
}
int zb_lhm_close_checked(zb_lhm_context* ctx,char* error,size_t size) {
    Error(error,size,""); if(!ctx) return 1;
    try { return worker().run([&](threadinfo& ti) {
        bool ok=true;
        try {
            auto temp=ctx->path+".snapshot.tmp";
            ctx->ns.save_snapshot(temp,ti); SyncFile(temp);
            if(::rename(temp.c_str(),(ctx->path+".snapshot").c_str())) throw std::runtime_error("snapshot publication failed");
            SyncFile(std::filesystem::path(ctx->path).parent_path().string(),true);
        } catch(const std::exception& e) { ok=false; Error(error,size,e.what()); ::unlink((ctx->path+".snapshot").c_str()); }
        ctx->ns.destroy(ti); delete ctx; return ok?1:0;
    }); } catch(const std::exception& e) { Error(error,size,e.what()); return 0; }
}
void zb_lhm_close(zb_lhm_context* ctx) {
    char error[512]{};
    if(!zb_lhm_close_checked(ctx,error,sizeof(error))) std::fprintf(stderr,"LHM close: %s\n",error);
}
int zb_lhm_create(zb_lhm_context* ctx, const char* path, int directory) {
    if (!path || path[0] != '/') return 0;
    return call(ctx, [=](threadinfo& ti) {
        MasstreeLHM::namespace_entry entry;
        if (ctx->ns.lookup_entry(path, entry, ti)) return false;
        return directory ? ctx->ns.mkdir(path, ti) : ctx->ns.creat_file(path, ti);
    });
}
int zb_lhm_lookup(zb_lhm_context* ctx, const char* path, zb_lhm_inode* out) {
    if (!path || path[0] != '/' || !out) return 0;
    return call(ctx, [=](threadinfo& ti) {
        MasstreeLHM::inode_disk inode{};
        if (!ctx->ns.lookup_inode(path, inode, ti)) return false;
        export_inode(ctx, inode, out); return true;
    });
}
int zb_lhm_read_inode(zb_lhm_context* ctx, uint64_t id, zb_lhm_inode* out) {
    if (!out || !zb::mds::LhmInodeId::Decode(id)) return 0;
    return call(ctx, [=](threadinfo&) {
        MasstreeLHM::inode_disk inode{};
        // The native root also has a file-shaped ID: expose only its public alias.
        if (id != 1 && id == ctx->ns.root_inode_id()) return false;
        if (!ctx->ns.read_inode_by_id(native_id(ctx, id), inode)) return false;
        export_inode(ctx, inode, out); return true;
    });
}
int zb_lhm_update_inode(zb_lhm_context* ctx, const zb_lhm_inode* attr) {
    if (!attr || !zb::mds::LhmInodeId::Decode(attr->inode_id)) return 0;
    return call(ctx, [=](threadinfo&) {
        MasstreeLHM::inode_disk inode{};
        if (attr->inode_id != 1 && attr->inode_id == ctx->ns.root_inode_id()) return false;
        uint64_t id = native_id(ctx, attr->inode_id);
        if (!ctx->ns.read_inode_by_id(id, inode) || inode.type != attr->type) return false;
        return ctx->ns.update_inode_by_id(id, attr->size, attr->mode, attr->ctime_ns, attr->mtime_ns);
    });
}
int zb_lhm_rename(zb_lhm_context* ctx, const char* from, const char* to) {
    if (!from || !to || from[0] != '/' || to[0] != '/') return 0;
    return call(ctx, [=](threadinfo& ti) { return ctx->ns.rename_path(from, to, ti); });
}
int zb_lhm_remove(zb_lhm_context* ctx, const char* path) {
    if (!path || path[0] != '/' || MasstreeLHM::PathKey::parse_absolute_path(path).components.empty()) return 0;
    return call(ctx, [=](threadinfo& ti) {
        MasstreeLHM::namespace_entry entry;
        if (!ctx->ns.lookup_entry(path, entry, ti)) return false;
        if (MasstreeLHM::entry_is_directory(entry) && !ctx->ns.readdir(path, ti).empty()) return false;
        return ctx->ns.remove_path_for_test(path, ti);
    });
}
int zb_lhm_sync(zb_lhm_context* ctx) {
    return call(ctx, [=](threadinfo&) { return ctx->ns.sync_persistence(); });
}
int zb_lhm_reserve(zb_lhm_context* ctx, const char* path, uint8_t type,
                   uint64_t* token, uint64_t* id) {
    if (token) *token = 0;
    if (id) *id = 0;
    if (!path || path[0] != '/' || !token || !id || (type != 1 && type != 2)) return 0;
    const auto parsed = MasstreeLHM::PathKey::parse_absolute_path(path);
    if (parsed.components.empty() || parsed.components.back().size() > 64) return 0;
    return call(ctx, [=](threadinfo& ti) {
        return ctx->ns.reserve_create(path, type == 2, *token, *id, ti);
    });
}
int zb_lhm_commit(zb_lhm_context* ctx, uint64_t token, const zb_lhm_full_inode* attr) {
    if (!attr || !zb::mds::lhm_codec::Valid(*attr)) return 0;
    return call(ctx, [=](threadinfo& ti) {
        std::string path;
        if (!ctx->ns.reservation_path(token, path)) return false;
        const auto expected = MasstreeLHM::PathKey::parse_absolute_path(path).components.back();
        if (expected != std::string(attr->file_name, attr->name_length)) return false;
        MasstreeLHM::inode_disk inode{};
        return zb::mds::lhm_codec::Encode(*attr, inode) && ctx->ns.commit_create(token, inode, ti);
    });
}
#ifdef ZB_LHM_TESTING
int zb_lhm_fail_next_publish_for_test(zb_lhm_context* ctx) {
    return call(ctx, [=](threadinfo&) { ctx->ns.fail_next_publish_for_test(); return true; });
}
#endif
int zb_lhm_cancel(zb_lhm_context* ctx, uint64_t token) {
    return call(ctx, [=](threadinfo&) { return ctx->ns.cancel_create(token); });
}
uint64_t zb_lhm_pending(zb_lhm_context* ctx) {
    if (!ctx) return 0;
    try { return worker().run([=](threadinfo&) { return uint64_t(ctx->ns.pending_create_count()); }); }
    catch (...) { return UINT64_MAX; }
}
int zb_lhm_read_full(zb_lhm_context* ctx, uint64_t id, zb_lhm_full_inode* out) {
    if (!out || !zb::mds::LhmInodeId::Decode(id)) return 0;
    return call(ctx, [=](threadinfo&) {
        if (id != 1 && id == ctx->ns.root_inode_id()) return false;
        MasstreeLHM::inode_disk inode{};
        if (!ctx->ns.read_inode_by_id(native_id(ctx,id),inode) || !zb::mds::lhm_codec::Decode(inode,*out)) return false;
        out->inode_id = id;
        return true;
    });
}
int zb_lhm_lookup_full(zb_lhm_context* ctx, const char* path, zb_lhm_full_inode* out) {
    if (!out || !path || path[0] != '/') return 0;
    return call(ctx, [=](threadinfo& ti) {
        MasstreeLHM::inode_disk inode{};
        if (!ctx->ns.lookup_inode(path,inode,ti) || !zb::mds::lhm_codec::Decode(inode,*out)) return false;
        if (out->inode_id == ctx->ns.root_inode_id()) out->inode_id = 1;
        return true;
    });
}
int zb_lhm_update_full(zb_lhm_context* ctx, const zb_lhm_full_inode* attr) {
    if (!attr || !zb::mds::lhm_codec::Valid(*attr) || !zb::mds::LhmInodeId::Decode(attr->inode_id)) return 0;
    return call(ctx, [=](threadinfo&) {
        if (attr->inode_id != 1 && attr->inode_id == ctx->ns.root_inode_id()) return false;
        auto native = *attr;
        native.inode_id = native_id(ctx,attr->inode_id);
        MasstreeLHM::inode_disk existing{}, inode{};
        zb_lhm_full_inode old{};
        if (!ctx->ns.read_inode_by_id(native.inode_id,existing) || !zb::mds::lhm_codec::Decode(existing,old)) return false;
        // A metadata update cannot rename the object. Rename owns the name change.
        if (old.name_length != native.name_length || std::memcmp(old.file_name,native.file_name,old.name_length)) return false;
        return zb::mds::lhm_codec::Encode(native,inode) && ctx->ns.replace_inode_attributes(native.inode_id,inode);
    });
}

}

#include "LhmNamespaceBridge.inc"
#include "LhmScanBridge.inc"

#include "LhmBulkBridge.inc"

#include "LhmStreamBridge.inc"
