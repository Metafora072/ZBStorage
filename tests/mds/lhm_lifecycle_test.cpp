#include "mds/lhm/LhmNamespaceStore.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <sys/wait.h>
#include <unistd.h>
using namespace zb::mds;
namespace fs=std::filesystem;
void Check(bool b,const std::string& m) { if(!b) throw std::runtime_error(m); }
zb::rpc::InodeAttr Lookup(LhmNamespaceStore& ns,const std::string& path) {
    zb::rpc::InodeAttr a; uint64_t id; std::string e;
    Check(ns.ResolvePath(path,&id,&a,&e),"lookup "+path+": "+e); return a;
}
void Fixture(const fs::path& p) {
    std::ofstream o(p); o<<"type\tpath\tsize\tmode\tuid\tmtime\toptical_image_id\toptical_disk_id\n";
    o<<"dir\t/\t0\t493\t42\t123\t0\t0\n";
    for(int i=0;i<65;++i) o<<"dir\t/d"<<i<<"\t0\t493\t42\t123\t0\t0\n";
    for(int i=0;i<600;++i) o<<"file\t/d0/f"<<i<<"\t"<<1000+i<<"\t384\t42\t123\t7\t9\n";
}
void Phase(const fs::path& dir,const std::string& phase) {
    std::string e; auto file=(dir/"store").string(); char error[512]{};
    if(phase=="reject") { auto* ctx=zb_lhm_reopen(file.c_str(),error,sizeof(error)); Check(!ctx,"expected reopen rejection"); return; }
    LhmNamespaceStore ns(file,phase!="create",&e); Check(ns.ready(),"open: "+e);
    if(phase=="dirty") _exit(0);
    if(phase=="create") {
        Fixture(dir/"manifest.tsv"); NamespaceImportResult r;
        auto st=ns.ImportManifest("/copy1",(dir/"manifest.tsv").string(),1<<20,&r);
        Check(st.ok(),"import: "+st.message); Check(r.files==600 && r.directories==66,"import counts");
        auto a=Lookup(ns,"/copy1/d0/f599"); Check(a.size()==1599 && a.uid()==42 && a.mode()==0600 && a.mtime()==123 && a.optical_image_id()==7 && a.optical_disk_id()==9,"attribute preservation");
        std::ofstream(dir/"saved_id")<<a.inode_id();
        Check(!zb_lhm_reopen(file.c_str(),error,sizeof(error)),"concurrent open rejected");
        NamespaceImportResult rejected;
        Check(!ns.ImportManifest("/budget",(dir/"manifest.tsv").string(),1024,&rejected).ok() && !rejected.processed_records,"small budget rejected");
        auto failure=(dir/"close-failure").string();
        auto* raw=zb_lhm_open(failure.c_str()); Check(raw,"create failure-close store");
        uint64_t token=0,pending_id=0; Check(zb_lhm_reserve(raw,"/pending",1,&token,&pending_id),"pending creation");
        Check(!zb_lhm_close_checked(raw,error,sizeof(error)),"close with pending reservation rejected");
        Check(!zb_lhm_reopen(failure.c_str(),error,sizeof(error)),"failed close has no valid snapshot");
        Check(ns.Sync(),"sync");
        Check(fs::file_size(file+".heads")==3*16384,"31 heads per page including root");
    } else if(phase=="append") {
        for(int i=0;i<600;++i) Check(Lookup(ns,"/copy1/d0/f"+std::to_string(i)).size()==uint64_t(1000+i),"reopened bucket routing");
        for(int i=0;i<65;++i) Check(Lookup(ns,"/copy1/d"+std::to_string(i)).uid()==42,"shared head slots preserved");
        uint64_t id; std::ifstream(dir/"saved_id")>>id;
        Check(Lookup(ns,"/copy1/d0/f599").inode_id()==id,"stable ID across process");
        Check(ns.Rename("/copy1/d0","/copy1/moved").ok(),"rename reopened directory");
        Check(ns.Remove("/copy1/moved/f0",zb::rpc::INODE_FILE).ok(),"remove reopened file");
        NamespaceImportResult r; auto st=ns.ImportManifest("/copy2",(dir/"manifest.tsv").string(),1<<20,&r);
        Check(st.ok(),"append: "+st.message);
        Check(Lookup(ns,"/copy2/d0/f599").inode_id()!=id,"append IDs distinct");
        std::ofstream bad(dir/"bad.tsv"); bad<<"type\tpath\ndir\t/\nfile\t/missing/file\n"; bad.close();
        st=ns.ImportManifest("/partial",(dir/"bad.tsv").string(),1<<20,&r);
        Check(!st.ok() && r.processed_records==1 && r.error_line==3,"partial failure reported");
        std::ofstream oversized(dir/"oversized.tsv"); oversized<<"type\tpath\ndir\t/\nfile\t/"<<std::string(9000,'x')<<"\n"; oversized.close();
        st=ns.ImportManifest("/oversized",(dir/"oversized.tsv").string(),1<<20,&r);
        Check(!st.ok() && r.processed_records==1 && r.error_line==3,"bounded long record rejected");
        std::ofstream overflow(dir/"overflow.tsv"); overflow<<"type\tpath\tmode\ndir\t/\t65536\n"; overflow.close();
        st=ns.ImportManifest("/overflow",(dir/"overflow.tsv").string(),1<<20,&r);
        Check(!st.ok() && !r.processed_records && r.error_line==2,"attribute overflow rejected before create");
    } else if(phase=="verify") {
        uint64_t id; std::ifstream(dir/"saved_id")>>id;
        Check(Lookup(ns,"/copy1/moved/f599").inode_id()==id,"rename persisted");
        Check(Lookup(ns,"/copy2/d0/f599").size()==1599,"append persisted");
        zb::rpc::InodeAttr a; uint64_t missing; Check(!ns.ResolvePath("/copy1/moved/f0",&missing,&a,&e),"delete persisted");
        std::unique_ptr<NamespaceScanner> scan; SubtreeScanOptions opt; opt.work_budget=17;
        Check(ns.BeginSubtreeScan("/",opt,&scan).ok(),"begin scan"); size_t n=0;
        for(;;) { SubtreeScanBatch b; Check(scan->Next(13,&b).ok(),"scan"); n+=b.entries.size(); if(b.done) break; }
        Check(n==1199,"scan reopened namespace"); scan.reset();
    }
    Check(ns.Close(&e),"normal close: "+e);
}
int main(int argc,char** argv) {
    try {
        Check(argc==2 || argc==3,"usage: lhm_lifecycle_test <new directory> [phase]");
        if(argc==3) { Phase(argv[1],argv[2]); return 0; }
        Check(fs::create_directory(argv[1]),"test directory must be new");
        auto run=[&](const char* phase) { pid_t pid=fork(); Check(pid>=0,"fork");
            if(!pid) { execl(argv[0],argv[0],argv[1],phase,nullptr); _exit(127); }
            int status; Check(waitpid(pid,&status,0)==pid && WIFEXITED(status) && WEXITSTATUS(status)==0,std::string("phase failed: ")+phase); };
        run("create"); run("append"); run("verify");
        auto snap=fs::path(argv[1])/"store.snapshot";
        fs::copy_file(snap,snap.string()+".backup");
        { std::fstream f(snap,std::ios::binary|std::ios::in|std::ios::out); f.seekg(-1,std::ios::end); char c=static_cast<char>(f.get() ^ 0x80); f.seekp(-1,std::ios::end); f.put(c); }
        run("reject"); fs::copy_file(snap.string()+".backup",snap,fs::copy_options::overwrite_existing);
        run("verify"); run("dirty"); run("reject");
        // The dirty phase made no writes; restore the previously verified test snapshot.
        fs::copy_file(snap.string()+".backup",snap,fs::copy_options::overwrite_existing); run("verify");
        std::cout<<"PASS: cross-process reopen, streaming append, attributes, compact heads, rename/delete, scan, corruption and unclean rejection\n";
        return 0;
    } catch(const std::exception& e) { std::cerr<<"FAIL: "<<e.what()<<'\n'; return 1; }
}
