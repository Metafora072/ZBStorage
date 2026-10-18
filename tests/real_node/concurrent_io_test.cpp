#include "data_node/real_node/service/BrpcStorageService.h"
#include <brpc/server.h>
#include <butil/endpoint.h>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace fs=std::filesystem;
using namespace zb::real_node;
void Check(bool ok,const char* text) { if (!ok) throw std::runtime_error(text); }
struct Node {
    fs::path dir;
    DiskManager disks;
    LocalPathResolver paths;
    IOExecutor io;
    StorageServiceImpl storage{&disks,&paths,&io};
    BrpcStorageService rpc{&storage};
    brpc::Server server;
    explicit Node(const fs::path& root):dir(root) {
        fs::create_directories(dir/"disk");
        Check(disks.Init("disk0:"+(dir/"disk").string()).ok(),"disk init");
        storage.SetFileMetaStoreDir((dir/"filemeta").string());
        std::string error;
        Check(storage.InitArchiveMetaStore((dir/"archive").string(),10000,20000,false,&error),"archive init");
        Check(!server.AddService(&rpc,brpc::SERVER_DOESNT_OWN_SERVICE),"add RPC");
    }
    ~Node() { server.Stop(0); server.Join(); }
    std::string Start(const std::string& address="127.0.0.1:0") {
        Check(!server.Start(address.c_str(),nullptr),"server start");
        return butil::endpoint2str(server.listen_address()).c_str();
    }
    zb::msg::Status Write(std::string object, uint64_t offset, const std::string& data) {
        zb::msg::WriteObjectRequest q;q.disk_id="disk0";q.SetArchiveObjectId(object);q.offset=offset;q.data=data;
        return storage.WriteObject(q).status;
    }
    std::string Read(const std::string& object,uint64_t size) {
        zb::msg::ReadObjectRequest q;q.disk_id="disk0";q.SetArchiveObjectId(object);q.size=size;
        auto r=storage.ReadObject(q);Check(r.status.ok(),"read object");return r.data;
    }
};
void TestObjectLock(Node& node) {
    Check(node.Write("shared",0,std::string(16384,'a')).ok(),"seed shared");
    std::vector<std::future<void>> tasks;
    for(int i=0;i<8;++i) tasks.push_back(std::async(std::launch::async,[&,i] {
        for(int k=0;k<20;++k) {
            if(i<4) Check(node.Write("shared",0,std::string(16384,'a'+i)).ok(),"concurrent write");
            else {auto r=node.Read("shared",16384);Check(r.size()==16384 && r==std::string(16384,r.front()),"torn object read");}
            Check(node.disks.Refresh().ok(),"concurrent disk refresh");
        }
    }));
    for(auto& t:tasks)t.get();
    // Same millisecond/range must still yield different txids.
    zb::msg::AllocateFileWriteRequest q;q.inode_id=5;q.disk_id="disk0";q.size=4;q.object_unit_size_hint=4;
    auto a=node.storage.AllocateFileWrite(q);auto b=node.storage.AllocateFileWrite(q);
    Check(a.status.ok() && b.status.ok() && a.txid!=b.txid,"txid uniqueness");
}
void TestReplication(Node& primary,Node& secondary) {
    const auto address=secondary.Start();
    primary.storage.ConfigureReplication("primary","g",true,true,"secondary",address,100);
    secondary.storage.ConfigureReplication("secondary","g",true,false,"primary","",100);
    std::vector<std::future<void>> tasks;
    for(int i=0;i<6;++i) tasks.push_back(std::async(std::launch::async,[&,i] {
        for(int k=0;k<5;++k) Check(primary.Write("replicated",0,std::string(4096,'a'+i)).ok(),"replicated write");
    }));
    for(auto& t:tasks)t.get();
    Check(primary.Read("replicated",4096)==secondary.Read("replicated",4096),"replication order diverged");
    secondary.server.Stop(0);secondary.server.Join();
    Check(!primary.Write("repair",0,std::string(4096,'x')).ok(),"unavailable peer must fail");
    // Different offset/length still conflicts with the same object's pending repair.
    Check(!primary.Write("repair",512,std::string(1024,'y')).ok(),"new write bypassed pending repair");
    secondary.Start(address);
    bool repaired=false;
    for(int i=0;i<100;++i) {
        auto status=primary.Write("repair",512,std::string(1024,'y'));
        if(status.ok()) {repaired=true;break;}
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    Check(repaired,"repair did not unblock newer write");
    auto expected=std::string(4096,'x');expected.replace(512,1024,1024,'y');
    Check(primary.Read("repair",4096)==expected && secondary.Read("repair",4096)==expected,"stale repair overwrote new write");
}
int main() {
    char pattern[]="/tmp/zb-real-concurrency-XXXXXX";
    const auto* directory=mkdtemp(pattern); if(!directory)return 1;
    fs::path root(directory);
    try {
        {Node primary(root/"primary"),secondary(root/"secondary");TestObjectLock(primary);TestReplication(primary,secondary);}
        fs::remove_all(root);std::cout<<"PASS real node object isolation, refresh, txids and replication repair\n";
    } catch(const std::exception& e) {std::cerr<<e.what()<<"; artifacts: "<<root<<'\n';return 1;}
}
