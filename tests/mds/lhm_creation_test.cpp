#include "mds/lhm/LhmInodeStore.h"
#include <filesystem>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>

using namespace zb::mds;
static void Check(bool ok, const char* why) { if (!ok) throw std::runtime_error(why); }
static zb::rpc::InodeAttr Attr(uint64_t id, bool directory, const std::string& name) {
    zb::rpc::InodeAttr a;
    a.set_inode_id(id); a.set_type(directory ? zb::rpc::INODE_DIR : zb::rpc::INODE_FILE);
    a.set_mode(0751); a.set_uid(12345); a.set_gid(54321); a.set_size(9876543210ULL);
    a.set_atime(UINT64_MAX); a.set_mtime(1700000001); a.set_ctime(1700000002);
    a.set_nlink(directory ? 2 : 1); a.set_object_unit_size(1ULL<<40); a.set_replica(70000);
    a.set_version(123456789); a.set_file_archive_state(zb::rpc::INODE_ARCHIVE_ARCHIVED);
    a.set_file_name(name); a.set_storage_tier(zb::rpc::INODE_STORAGE_OPTICAL);
    a.set_disk_node_id(1ULL<<40); a.set_disk_id(UINT32_MAX);
    a.set_optical_node_id(1ULL<<45); a.set_optical_disk_id(1ULL<<42); a.set_optical_image_id(900);
    a.set_disk_node_is_virtual(true); a.set_blksize(16384); a.set_blocks_512(987654);
    a.set_rdev_major(200); a.set_rdev_minor(300);
    return a;
}
static void Same(const zb::rpc::InodeAttr& a, const zb::rpc::InodeAttr& b) {
    Check(a.SerializeAsString()==b.SerializeAsString(), "complete InodeAttr roundtrip mismatch");
}
int main(int argc,char** argv) {
    try {
        Check(argc==2 && !std::filesystem::exists(argv[1]),"fresh work directory required");
        std::filesystem::create_directories(argv[1]);
        const std::string file=std::string(argv[1])+"/namespace.meta";
        LhmInodeStore store(file); Check(store.ready(),"open");
        Check(zb::rpc::InodeAttr::descriptor()->field_count()==26,"update full-field fixture when proto changes");
        std::string error;
        zb::rpc::InodeAttr out;
        Check(store.Lookup("/",&out) && out.inode_id()==1,"full root");
        Check(!store.ReserveCreate("/missing/f",zb::rpc::INODE_FILE,&error),"missing parent");
        for (bool directory : {false,true}) {
            const std::string path=directory ? "/canceled-dir" : "/canceled-file";
            uint64_t id=0;
            for(int i=0;i<25;++i) {
                auto r=store.ReserveCreate(path,directory?zb::rpc::INODE_DIR:zb::rpc::INODE_FILE,&error);
                Check(bool(r),"reserve for canceled placement");
                if(i) Check(r->inode_id()==id,"unpublished reservation space not reused");
                id=r->inode_id();
                Check(store.pending()==1 && !store.Lookup(path,&out) && !store.GetInode(id,&out),"reservation is visible");
                // Placement failure / early return: destruction cancels the reservation.
                r.reset();
                Check(store.pending()==0 && !store.Lookup(path,&out) && !store.GetInode(id,&out),"cancellation leaked");
            }
        }
        auto dir=store.ReserveCreate("/dir",zb::rpc::INODE_DIR,&error);
        Check(bool(dir),"reserve dir"); auto da=Attr(dir->inode_id(),true,"dir");
        Check(dir->Commit(da).ok(),"commit directory");
        Check(store.Lookup("/dir",&out),"directory lookup"); Same(da,out);
        const std::string name(64,'x'), path="/dir/"+name;
        auto f=store.ReserveCreate(path,zb::rpc::INODE_FILE,&error);
        Check(bool(f),"reserve file"); auto a=Attr(f->inode_id(),false,name);
        Check(!store.ReserveCreate(path,zb::rpc::INODE_FILE,&error),"duplicate reservation accepted");
        auto bad=a; bad.set_mode(65536); Check(!f->Commit(bad).ok(),"mode overflow accepted");
        bad=a; bad.set_ctime(UINT64_MAX); Check(!f->Commit(bad).ok(),"time overflow accepted");
        bad=a; bad.set_file_name("wrong"); Check(!f->Commit(bad).ok(),"wrong name accepted");
        bad=a; bad.set_type(zb::rpc::INODE_DIR); Check(!f->Commit(bad).ok(),"wrong type accepted");
        Check(!store.GetInode(a.inode_id(),&out),"failed commit exposed inode");
        Check(f->Commit(a).ok(),"commit full file");
        Check(!f->Commit(a).ok(),"double commit accepted");
        Check(store.pending()==0,"commit retained reservation");
        Check(store.Lookup(path,&out),"lookup full file"); Same(a,out);
        Check(store.GetInode(a.inode_id(),&out),"read full ID"); Same(a,out);
        a.set_size(555); a.set_uid(99); a.set_version(999); a.set_storage_tier(zb::rpc::INODE_STORAGE_DISK);
        Check(store.PutInode(a),"full attribute update");
        Check(store.Lookup(path,&out),"lookup after update"); Same(a,out);
        Check(!store.ReserveCreate(path,zb::rpc::INODE_FILE,&error),"existing path accepted");
        Check(!store.ReserveCreate("/"+std::string(65,'x'),zb::rpc::INODE_FILE,&error),"oversize name accepted");
        Check(store.Sync(),"sync");
        // Inspect the actual extended inode bytes, independent of decoder/cache.
        std::ifstream disk(file,std::ios::binary);
        const auto id=a.inode_id();
        const uint64_t offset=(2*(id>>32)-1)*16384+256+uint32_t(id)*256;
        char magic[4]{}; disk.seekg(offset+92); disk.read(magic,4);
        Check(bool(disk) && std::string(magic,4)=="ZBA1","extension missing at native inode offset");
        Check(store.GetInode(a.inode_id(),&out),"read full after sync"); Same(a,out);

        // Natural publication failure: the parent disappears between reserve/commit.
        const std::string rawfile=std::string(argv[1])+"/raw.meta";
        auto* raw=zb_lhm_open(rawfile.c_str()); Check(raw,"raw open");
        Check(zb_lhm_create(raw,"/parent",1),"raw parent");
        uint64_t token=0,rid=0;
        Check(zb_lhm_reserve(raw,"/parent/file",1,&token,&rid),"raw reserve");
        Check(zb_lhm_remove(raw,"/parent"),"remove parent during reservation");
        zb_lhm_full_inode attrs{}; attrs.inode_id=rid; attrs.type=1; attrs.nlink=1;
        attrs.name_length=4; std::memcpy(attrs.file_name,"file",4);
        Check(!zb_lhm_commit(raw,token,&attrs),"publication should reject missing parent");
        Check(zb_lhm_cancel(raw,token) && zb_lhm_pending(raw)==0,"failed publish cancellation");
        uint64_t newtoken=0,newid=0;
        Check(zb_lhm_reserve(raw,"/new",1,&newtoken,&newid) && newid==rid && newtoken!=token,"token/space lifecycle");
        Check(!zb_lhm_cancel(raw,token) && !zb_lhm_commit(raw,token,&attrs),"stale token reused");
        Check(zb_lhm_cancel(raw,newtoken),"final cancel");
        for (uint8_t type : {uint8_t(1),uint8_t(2)}) {
            const char* path=type==1 ? "/fault-file" : "/fault-dir";
            const char* name=type==1 ? "fault-file" : "fault-dir";
            Check(zb_lhm_reserve(raw,path,type,&token,&rid),"fault reserve");
            attrs={}; attrs.inode_id=rid; attrs.type=type; attrs.nlink=1;
            attrs.name_length=std::strlen(name); std::memcpy(attrs.file_name,name,attrs.name_length);
            Check(zb_lhm_fail_next_publish_for_test(raw),"arm publish failure");
            Check(!zb_lhm_commit(raw,token,&attrs),"injected publish failure ignored");
            zb_lhm_inode hidden{};
            Check(!zb_lhm_lookup(raw,path,&hidden) && !zb_lhm_read_inode(raw,rid,&hidden),"failed publication leaked inode");
            Check(zb_lhm_cancel(raw,token) && zb_lhm_pending(raw)==0,"failed publication leaked reservation");
            Check(zb_lhm_reserve(raw,path,type,&newtoken,&newid) && newid==rid,"failed publication space lost");
            Check(zb_lhm_commit(raw,newtoken,&attrs),"retry publication failed");
            zb_lhm_full_inode full{};
            Check(zb_lhm_read_full(raw,rid,&full) && full.inode_id==rid,"retry inode missing");
        }
        zb_lhm_close(raw);
        std::cout<<"PASS: full 26-field attributes, hidden reservations, RAII cancellation, publication failure, reuse, stale tokens, limits and disk layout\n";
    } catch(const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
