#include "mds/storage/RocksMetaStore.h"
#include <rocksdb/iterator.h>
#include <iostream>
#include <memory>
#include <stdexcept>
void check(bool ok,const char* message){if(!ok) throw std::runtime_error(message);}
int main(int argc,char** argv){try{
    check(argc==3,"usage: tool seed|audit|seed-inode DB");
    zb::mds::RocksMetaStore db; std::string error,value;
    check(db.Open(argv[2],&error),"open");
    std::string mode=argv[1];
    if(mode=="seed"){
        // Either old router would attempt to decode these conflicting root routes.
        check(db.Put("MTR/","invalid legacy route",&error),"seed masstree");
        check(db.Put("AN/","invalid legacy route",&error),"seed archive");
        db.DisableNamespaceAccess();
        check(!db.Get("I/1",&value,&error) && !error.empty(),"read guard");
        check(!db.Put("D/1/a","x",&error),"put guard");
        rocksdb::WriteBatch batch; batch.Put("I/1","x");
        check(!db.WriteBatch(&batch,&error),"batch guard");
    }else if(mode=="seed-inode"){
        check(db.Put("I/1","legacy",&error),"seed old inode");
    }else if(mode=="audit"){
        check(db.Get("SYS/NAMESPACE_BACKEND",&value,&error)&&value=="lhm","backend marker");
        check(db.Get("MTR/",&value,&error)&&value=="invalid legacy route","legacy route unchanged");
        check(db.Get("AN/",&value,&error)&&value=="invalid legacy route","archive route unchanged");
        std::unique_ptr<rocksdb::Iterator> it(db.db()->NewIterator(rocksdb::ReadOptions()));
        for(const char* prefix:{"I/","D/"}){
            it->Seek(prefix); check(it->status().ok(),"iterator");
            check(!it->Valid()||!it->key().starts_with(prefix),"unexpected Rocks namespace record");
        }
    }else throw std::runtime_error("unknown mode");
    std::cout<<"PASS "<<mode<<'\n'; return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
