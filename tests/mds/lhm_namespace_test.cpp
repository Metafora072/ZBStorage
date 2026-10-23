#include <algorithm>
#include <atomic>
#include <barrier>
#include <set>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <thread>
#include "mds/lhm/LhmNamespaceStore.h"
#include "mds/storage/RocksNamespaceStore.h"
using namespace zb::mds;
using namespace zb::rpc;
void Check(bool ok,const std::string& text) { if(!ok) throw std::runtime_error(text); }
InodeAttr Attr(uint64_t id,InodeType type,const std::string& name) {
    InodeAttr a; a.set_inode_id(id); a.set_type(type); a.set_file_name(name);
    a.set_nlink(type==INODE_DIR?2:1); a.set_mode(0750); a.set_uid(123); a.set_gid(456);
    a.set_size(789); a.set_mtime(100); a.set_ctime(200); a.set_atime(300);
    a.set_version(9); a.set_disk_node_id(1234567890123ULL); a.set_disk_id(42);
    a.set_storage_tier(INODE_STORAGE_DISK); return a;
}
uint64_t Create(NamespaceStore& ns,const std::string& path,InodeType type) {
    std::string error;
    auto r=ns.ReserveCreate(path,type,&error);
    Check(bool(r),"reserve "+path+": "+error);
    auto a=Attr(r->inode_id(),type,path.substr(path.rfind('/')+1));
    auto status=r->Commit(a); Check(status.ok(),"commit "+path+": "+status.message);
    Check(!r->Commit(a).ok(),"double commit");
    return r->inode_id();
}
void Contract(NamespaceStore& ns) {
    std::string error,token; uint64_t id=0; InodeAttr a; bool more=false;
    Check(ns.PutInode(1,Attr(1,INODE_DIR,"/"),&error),"initialize root");
    auto dir=Create(ns,"/dir",INODE_DIR); Create(ns,"/other",INODE_DIR);
    auto file=Create(ns,"/dir/file",INODE_FILE);
    Check(ns.ResolvePath("//dir//file/",&id,&a,&error) && id==file,"normalized lookup");
    Check(ns.Remove("/dir",INODE_DIR).code==MDS_NOT_EMPTY,"nonempty");
    Check(ns.Remove("/dir/file",INODE_DIR).code==MDS_INVALID_ARGUMENT,"wrong remove type");
    Check(ns.Rename("/dir/file","/other").code==MDS_ALREADY_EXISTS,"no overwrite");
    Check(ns.Rename("/absent","/new").code==MDS_NOT_FOUND,"missing rename");
    Check(ns.Rename("/dir/file","/missing/new").code==MDS_NOT_FOUND,"missing target parent");
    a.set_size(1234567); Check(ns.PutInode(file,a,&error),"update");
    auto expected=a; expected.set_file_name("renamed");
    Check(ns.Rename("/dir/file","/other/renamed").ok(),"cross-directory file rename");
    Check(ns.GetInode(file,&a,&error) && a.SerializeAsString()==expected.SerializeAsString(),"rename preserves all attributes except name");
    Check(ns.Rename("/other","/dir/moved").ok(),"move populated directory");
    Check(ns.ResolvePath("/dir/moved/renamed",&id,&a,&error) && id==file,"descendant stable ID");
    Check(!ns.ResolvePath("/other/renamed",&id,&a,&error) && error.empty(),"old path missing");
    Check(ns.GetInode(dir,&a,&error) && a.file_name()=="dir","parent identity");
    std::vector<std::string> names={"a","aa","b",std::string(64,'x'),"中文","\xff"};
    for(int n=0;n<90;++n) names.push_back("child"+std::to_string(n));
    for(const auto& name:names) Create(ns,"/dir/"+name,INODE_FILE);
    names.push_back("moved"); std::sort(names.begin(),names.end());
    std::vector<Dentry> page; std::vector<std::string> observed;
    do {
        auto cursor=token;
        Check(ns.Readdir("/dir",cursor,7,&page,&more,&token,&error),"paged readdir");
        Check(!page.empty(),"nonempty page");
        for(const auto& e:page) observed.push_back(e.name());
        Check(token==page.back().name(),"page token");
    } while(more);
    Check(observed==names,"bytewise order and complete pagination");
    Check(ns.Readdir("/dir","az",0,&page,&more,&token,&error) && !more,"unbounded after absent cursor");
    std::vector<std::string> suffix; for(const auto& n:names) if(n>"az") suffix.push_back(n);
    Check(page.size()==suffix.size(),"cursor suffix size");
    for(size_t i=0;i<page.size();++i) Check(page[i].name()==suffix[i],"cursor suffix order");
    Check(ns.Readdir("/dir",std::string(2,'\xff'),3,&page,&more,&token,&error) && page.empty() && !more && token.empty(),"end page");
    Check(!ns.Readdir("/dir/moved/renamed","",0,&page,&more,&token,&error) && !error.empty(),"readdir file");
    Check(ns.Remove("/dir/moved/renamed",INODE_FILE).ok(),"unlink");
    error="stale"; Check(!ns.GetInode(file,&a,&error) && error.empty(),"deleted ID missing");
    uint64_t moved=0; Check(ns.ResolvePath("/dir/moved",&moved,&a,&error),"moved directory");
    Check(a.file_name()=="moved","directory attribute name");
    Check(ns.Remove("/dir/moved",INODE_DIR).ok(),"empty rmdir");
    Check(!ns.GetInode(moved,&a,&error) && error.empty(),"deleted directory ID missing");
    Check(ns.Remove("/",INODE_DIR).code==MDS_INVALID_ARGUMENT,"root protected");
    uint64_t canceled=0;
    { auto r=ns.ReserveCreate("/canceled",INODE_FILE,&error); Check(bool(r),"cancel reserve"); canceled=r->inode_id(); }
    Check(!ns.GetInode(canceled,&a,&error) && error.empty(),"canceled ID hidden");
    std::cout<<"PASS shared namespace contract\n";
}
void LhmSpecific(LhmNamespaceStore& ns) {
    std::string error,token; uint64_t id=0; InodeAttr a; std::vector<Dentry> entries; bool more=false;
    Check(ns.Rename("/dir","/dir/cycle").code==MDS_INVALID_ARGUMENT,"directory cycle");
    Check(ns.Rename("/dir","/dir").code==MDS_ALREADY_EXISTS,"same path matches Rocks");
    Check(ns.Rename("/dir","relative").code==MDS_INVALID_ARGUMENT,"relative path");
    Check(ns.Rename("/dir","/../bad").code==MDS_INVALID_ARGUMENT,"dotdot path");
    Check(ns.Rename("/dir","/"+std::string(65,'x')).code==MDS_INVALID_ARGUMENT,"oversize name");
    Check(ns.Rename("/dir",std::string("/bad\0tail",9)).code==MDS_INVALID_ARGUMENT,"embedded NUL");
    Check(!ns.ReserveCreate("/dir/a/child",INODE_FILE,&error),"file parent rejected");
    Check(ns.Rename("/dir/b","/dir/a/child").code==MDS_INVALID_ARGUMENT,"rename file parent");
    auto r=ns.ReserveCreate("/pending",INODE_FILE,&error); Check(bool(r),"pending reservation");
    Check(!ns.ReserveCreate("/pending",INODE_FILE,&error),"duplicate pending");
    Check(ns.Rename("/dir/a","/pending").code==MDS_ALREADY_EXISTS,"reserved destination");
    Check(!ns.ResolvePath("/pending",&id,&a,&error) && error.empty(),"pending path hidden");
    auto wrong=Attr(r->inode_id(),INODE_DIR,"pending"); Check(r->Commit(wrong).code==MDS_INVALID_ARGUMENT,"wrong type");
    Check(r->Cancel(),"cancel");
    Create(ns,"/temporary",INODE_DIR);
    auto child=ns.ReserveCreate("/temporary/file",INODE_FILE,&error); Check(bool(child),"pending child");
    Check(ns.Remove("/temporary",INODE_DIR).ok(),"remove parent with unpublished child");
    Check(child->Commit(Attr(child->inode_id(),INODE_FILE,"file")).code==MDS_NOT_FOUND,"missing parent commit status");
    Check(child->Cancel(),"cancel orphan reservation");
    Check(ns.Readdir("/dir","",1,&entries,&more,&token,&error),"first alias page");
    auto previous=token; Check(ns.Readdir("/dir",token,1,&entries,&more,&token,&error) && token>previous,"aliased cursor output");
    Check(ns.Sync(),"sync");
    Check(ns.ResolvePath("/dir/a",&id,&a,&error),"post-sync read");
    std::cout<<"PASS LHM validation, reservation conflicts and pagination alias\n";
}
void Concurrent(LhmNamespaceStore& ns) {
    constexpr int threads=4, per_thread=25;
    std::vector<std::thread> workers;
    std::vector<uint64_t> ids(threads*per_thread);
    std::atomic<int> failures{0},winners{0};
    std::barrier barrier(threads);
    for(int t=0;t<threads;++t) workers.emplace_back([&,t] {
        try {
            for(int n=0;n<per_thread;++n) ids[t*per_thread+n]=Create(ns,"/concurrent"+std::to_string(t)+"_"+std::to_string(n),INODE_FILE);
        } catch(...) {++failures;}
        barrier.arrive_and_wait();
        std::string error;
        auto reservation=ns.ReserveCreate("/contended",INODE_FILE,&error);
        if(reservation) ++winners;
        barrier.arrive_and_wait(); // keep winning reservation alive until all callers attempted
    });
    for(auto& thread:workers) thread.join();
    Check(failures==0 && winners==1,"concurrent creation and same-path exclusion");
    Check(std::set<uint64_t>(ids.begin(),ids.end()).size()==ids.size(),"concurrent unique IDs");
    std::cout<<"PASS concurrent creation: 100 unique IDs and one same-path reservation\n";
}
void BridgeErrors(const std::string& file) {
    auto* ctx=zb_lhm_open(file.c_str()); Check(ctx!=nullptr,"error fixture open");
    // The legacy harness creates a native inode without the ZB extension.
    Check(zb_lhm_create(ctx,"/without-extension",0),"native inode fixture");
    zb_lhm_inode basic{}; zb_lhm_full_inode full{};
    Check(zb_lhm_lookup(ctx,"/without-extension",&basic),"native lookup");
    Check(zb_lhm_lookup_checked(ctx,"/without-extension",&full)==ZB_LHM_ERROR,"path decode error is not missing");
    Check(zb_lhm_read_checked(ctx,basic.inode_id,&full)==ZB_LHM_ERROR,"ID decode error is not missing");
    Check(zb_lhm_lookup_checked(ctx,"/absent",&full)==ZB_LHM_MISSING,"missing distinct from corrupt");
    Check(zb_lhm_fail_next_lookup_read_for_test(ctx),"arm lookup read failure");
    Check(zb_lhm_lookup_checked(ctx,"/absent",&full)==ZB_LHM_ERROR,"failed bucket read must not appear as missing");
    Check(zb_lhm_lookup_checked(ctx,"/absent",&full)==ZB_LHM_MISSING,"read error resets between calls");
    int more=0;
    auto callback=+[](void*,const char*,uint32_t,uint64_t,uint8_t)->int{return 1;};
    Check(zb_lhm_readdir_checked(ctx,"/","",0,callback,nullptr,&more)==ZB_LHM_ERROR,"readdir decode error");
    zb_lhm_close(ctx);
    std::cout<<"PASS checked decode errors\n";
}

int main(int argc,char** argv) {
    try {
        Check(argc==2 && !std::filesystem::exists(argv[1]),"fresh work directory required");
        std::filesystem::create_directories(argv[1]);
        RocksMetaStore db; std::string error;
        Check(db.Open(std::string(argv[1])+"/rocks",&error),"rocks open");
        RocksNamespaceStore rocks(&db); Contract(rocks);
        LhmNamespaceStore lhm(std::string(argv[1])+"/lhm.meta"); Check(lhm.ready(),"LHM open");
        Contract(lhm); LhmSpecific(lhm); Concurrent(lhm);
        BridgeErrors(std::string(argv[1])+"/errors.meta");
        return 0;
    } catch(const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
