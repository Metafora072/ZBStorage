#include "MultiLhmQueryService.h"
#include "LhmBridge.h"
#include "LhmInodeStore.h"
#include <brpc/closure_guard.h>
#include <rocksdb/db.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sys/stat.h>

namespace zb::mds {
namespace {
void Status(zb::rpc::MdsStatus* out,zb::rpc::MdsStatusCode code,const std::string& message) {
    out->set_code(code);out->set_message(message);
}
bool ValidId(const std::string& id) {
    return !id.empty() && id.size()<=64 && id!="." && id!=".." &&
        id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")==std::string::npos;
}
std::string Normalize(const std::string& path) {
    const std::filesystem::path p(path);
    if(!p.is_absolute()) throw std::runtime_error("database paths must be absolute");
    return p.lexically_normal().string();
}
void CheckControl(const std::string& path,const std::string& store) {
    rocksdb::Options options;options.create_if_missing=false;
    std::vector<std::string> names;
    auto status=rocksdb::DB::ListColumnFamilies(options,path,&names);
    if(!status.ok()) throw std::runtime_error("control store unavailable: "+status.ToString());
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    for(const auto& name:names) descriptors.emplace_back(name,rocksdb::ColumnFamilyOptions());
    struct ReadOnlyControl {
        rocksdb::DB* db=nullptr;
        std::vector<rocksdb::ColumnFamilyHandle*> handles;
        ~ReadOnlyControl() {for(auto* h:handles) delete h;delete db;}
    } control;
    status=rocksdb::DB::OpenForReadOnly(options,path,descriptors,&control.handles,&control.db);
    if(!status.ok()) throw std::runtime_error("cannot open control store read-only: "+status.ToString());
    std::string backend,bound;
    if(!control.db->Get(rocksdb::ReadOptions(),"SYS/NAMESPACE_BACKEND",&backend).ok() || backend!="lhm" ||
       !control.db->Get(rocksdb::ReadOptions(),"SYS/LHM_STORE_FILE",&bound).ok() || bound!=store)
        throw std::runtime_error("database requires its matching LHM control store and original configured path");
}
}
struct MultiLhmQueryService::Entry {
    std::string id,control,store,error;
    std::mutex mutex;
    zb_lhm_context* context=nullptr;
    uint64_t device=0,inode=0,templates=0,instances=0;
    ~Entry() {if(context) {char error[1024]{};zb_lhm_close_checked(context,error,sizeof(error));}}
};
MultiLhmQueryService::MultiLhmQueryService()=default;
MultiLhmQueryService::~MultiLhmQueryService() {std::string error;Close(&error);}
void MultiLhmQueryService::Unavailable(Entry& entry,const std::string& error) {
    entry.error=error;
    if(entry.context) {
        char message[1024]{};zb_lhm_close_checked(entry.context,message,sizeof(message));entry.context=nullptr;
    }
}
bool MultiLhmQueryService::Healthy(Entry& entry) {
    if(!entry.context) return false;
    struct stat st{};
    if(::stat(entry.store.c_str(),&st) || !S_ISREG(st.st_mode) ||
       uint64_t(st.st_dev)!=entry.device || uint64_t(st.st_ino)!=entry.inode) {
        Unavailable(entry,"database disappeared or changed while open; restore the disk and restart query service");
        return false;
    }
    return true;
}
bool MultiLhmQueryService::LoadCatalog(const std::string& file,std::string* error) {
    try {
        if(!databases_.empty()) throw std::runtime_error("database catalog is already loaded");
        std::ifstream input(file);
        if(!input) throw std::runtime_error("cannot open LHM_DATABASES_FILE");
        std::map<std::string,std::unique_ptr<Entry>> entries;
        std::set<std::string> paths;
        std::string line;size_t number=0;
        while(std::getline(input,line)) {
            ++number;if(!line.empty() && line.back()=='\r') line.pop_back();
            if(line.empty() || line[0]=='#') continue;
            const auto a=line.find('\t'),b=a==std::string::npos?a:line.find('\t',a+1);
            if(a==std::string::npos || b==std::string::npos || line.find('\t',b+1)!=std::string::npos)
                throw std::runtime_error("catalog line "+std::to_string(number)+": expected database_id TAB control_directory TAB namespace_file");
            auto entry=std::make_unique<Entry>();entry->id=line.substr(0,a);
            entry->control=Normalize(line.substr(a+1,b-a-1));entry->store=Normalize(line.substr(b+1));
            if(!ValidId(entry->id) || entries.count(entry->id) || !paths.insert(entry->store).second)
                throw std::runtime_error("invalid/duplicate database ID or namespace path on catalog line "+std::to_string(number));
            const auto id=entry->id;entries.emplace(id,std::move(entry));
        }
        if(input.bad() || entries.empty()) throw std::runtime_error("empty or unreadable database catalog");
        // Validate the complete catalog before opening anything. Missing mounts
        // remain entries with errors; never create replacement stores there.
        for(auto& item:entries) {
            auto& entry=*item.second;
            try {
                CheckControl(entry.control,entry.store);
                char message[2048]{};
                entry.context=zb_lhm_reopen_shared_readonly(entry.store.c_str(),message,sizeof(message));
                if(!entry.context) throw std::runtime_error(message);
                struct stat st{};
                if(::stat(entry.store.c_str(),&st)) throw std::runtime_error("database disappeared during open");
                entry.device=st.st_dev;entry.inode=st.st_ino;
                uint64_t routes=0;
                if(zb_lhm_shared_index_stats(entry.context,&entry.templates,&entry.instances,&routes)!=ZB_LHM_OK)
                    throw std::runtime_error("cannot read shared namespace statistics");
                std::cerr<<"database="<<entry.id<<" available templates="<<entry.templates<<" instances="<<entry.instances<<'\n';
            } catch(const std::exception& e) {
                Unavailable(entry,e.what());std::cerr<<"database="<<entry.id<<" unavailable: "<<entry.error<<'\n';
            }
        }
        databases_=std::move(entries);return true;
    } catch(const std::exception& e) {if(error) *error=e.what();return false;}
}
bool MultiLhmQueryService::Close(std::string* error) {
    bool ok=true;
    for(auto& item:databases_) {
        auto& entry=*item.second;std::lock_guard<std::mutex> lock(entry.mutex);
        if(entry.context) {
            char message[1024]{};
            if(!zb_lhm_close_checked(entry.context,message,sizeof(message))) {
                ok=false;if(error) *error=entry.id+": "+message;
            }
            entry.context=nullptr;
        }
    }
    return ok;
}
MultiLhmQueryService::Entry* MultiLhmQueryService::Select(const std::string& id,zb::rpc::MdsStatus* out,
                                                        std::unique_lock<std::mutex>& lock) {
    if(!ValidId(id)) {Status(out,zb::rpc::MDS_INVALID_ARGUMENT,"database_id is required and must be a configured ID");return nullptr;}
    const auto found=databases_.find(id);
    if(found==databases_.end()) {Status(out,zb::rpc::MDS_NOT_FOUND,"unknown database_id");return nullptr;}
    auto& entry=*found->second;lock=std::unique_lock<std::mutex>(entry.mutex);
    if(!Healthy(entry)) {Status(out,zb::rpc::MDS_UNAVAILABLE,entry.error);return nullptr;}
    return &entry;
}
void MultiLhmQueryService::Result(Entry& entry,int code,zb::rpc::MdsStatus* out) {
    if(code==ZB_LHM_OK) Status(out,zb::rpc::MDS_OK,"OK");
    else if(code==ZB_LHM_MISSING) Status(out,zb::rpc::MDS_NOT_FOUND,"metadata not found in selected database");
    else if(code==ZB_LHM_INVALID) Status(out,zb::rpc::MDS_INVALID_ARGUMENT,"invalid query");
    else {Unavailable(entry,"database read failed; inspect this database before restarting service");Status(out,zb::rpc::MDS_UNAVAILABLE,entry.error);}
}
void MultiLhmQueryService::Lookup(google::protobuf::RpcController*,const zb::rpc::LookupRequest* request,
                                 zb::rpc::LookupReply* reply,google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);reply->set_database_id(request->database_id());
    std::unique_lock<std::mutex> lock;auto* entry=Select(request->database_id(),reply->mutable_status(),lock);if(!entry) return;
    if(request->path().find('\0')!=std::string::npos) {Status(reply->mutable_status(),zb::rpc::MDS_INVALID_ARGUMENT,"invalid path");return;}
    zb_lhm_full_inode inode{};const auto rc=zb_lhm_lookup_checked(entry->context,request->path().c_str(),&inode);
    if(rc==ZB_LHM_OK) LhmInodeStore::DecodeAttributes(inode,reply->mutable_attr());
    Result(*entry,rc,reply->mutable_status());
}
void MultiLhmQueryService::Getattr(google::protobuf::RpcController*,const zb::rpc::GetattrRequest* request,
                                  zb::rpc::GetattrReply* reply,google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);reply->set_database_id(request->database_id());
    std::unique_lock<std::mutex> lock;auto* entry=Select(request->database_id(),reply->mutable_status(),lock);if(!entry) return;
    if(!request->inode_id()) {Status(reply->mutable_status(),zb::rpc::MDS_INVALID_ARGUMENT,"inode_id must be nonzero");return;}
    zb_lhm_full_inode inode{};const auto rc=zb_lhm_read_checked(entry->context,request->inode_id(),&inode);
    if(rc==ZB_LHM_OK) LhmInodeStore::DecodeAttributes(inode,reply->mutable_attr());
    Result(*entry,rc,reply->mutable_status());
}
void MultiLhmQueryService::Readdir(google::protobuf::RpcController*,const zb::rpc::ReaddirRequest* request,
                                  zb::rpc::ReaddirReply* reply,google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);reply->set_database_id(request->database_id());
    std::unique_lock<std::mutex> lock;auto* entry=Select(request->database_id(),reply->mutable_status(),lock);if(!entry) return;
    if(request->path().find('\0')!=std::string::npos || request->start_after().find('\0')!=std::string::npos || request->limit()>10000) {
        Status(reply->mutable_status(),zb::rpc::MDS_INVALID_ARGUMENT,"invalid path/cursor or limit exceeds 10000");return;
    }
    auto collect=[](void* opaque,const char* name,uint32_t length,uint64_t id,uint8_t type)->int {
        auto* response=static_cast<zb::rpc::ReaddirReply*>(opaque);auto* entry=response->add_entries();
        entry->set_name(name,length);entry->set_inode_id(id);entry->set_type(type==2?zb::rpc::INODE_DIR:zb::rpc::INODE_FILE);return 1;
    };
    int more=0;const auto rc=zb_lhm_readdir_checked(entry->context,request->path().c_str(),request->start_after().c_str(),
        request->limit()?request->limit():256,collect,reply,&more);
    if(rc==ZB_LHM_OK) {reply->set_has_more(more);if(more && reply->entries_size()) reply->set_next_token(reply->entries(reply->entries_size()-1).name());}
    else reply->clear_entries();
    Result(*entry,rc,reply->mutable_status());
}
void MultiLhmQueryService::ListDatabases(google::protobuf::RpcController*,const zb::rpc::ListDatabasesRequest*,
                                        zb::rpc::ListDatabasesReply* reply,google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    for(auto& item:databases_) {
        auto& entry=*item.second;std::lock_guard<std::mutex> lock(entry.mutex);
        auto* info=reply->add_databases();info->set_database_id(entry.id);info->set_available(Healthy(entry));
        info->set_error(entry.error);info->set_template_count(entry.templates);info->set_instance_count(entry.instances);
    }
    zb_lhm_shared_cache_info cache{};
    if(zb_lhm_shared_cache_stats(&cache)!=ZB_LHM_OK) {Status(reply->mutable_status(),zb::rpc::MDS_INTERNAL_ERROR,"cannot inspect route cache");return;}
    reply->set_cached_templates(cache.templates);reply->set_cached_directory_routes(cache.routes);
    reply->set_template_references(cache.references);reply->set_template_loads(cache.loads);reply->set_template_cache_hits(cache.hits);
    Status(reply->mutable_status(),zb::rpc::MDS_OK,"OK");
}
}
