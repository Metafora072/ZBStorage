#pragma once
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include "mds.pb.h"

namespace zb::mds {
// Query-only service: independent database contexts, shared immutable routes.
// Unimplemented mutation RPCs use protobuf's explicit "not implemented" error.
class MultiLhmQueryService final : public zb::rpc::MdsService {
public:
    MultiLhmQueryService();
    ~MultiLhmQueryService() override;
    bool LoadCatalog(const std::string& file,std::string* error);
    bool Close(std::string* error);
    void Lookup(google::protobuf::RpcController*,const zb::rpc::LookupRequest*,
                zb::rpc::LookupReply*,google::protobuf::Closure*) override;
    void Getattr(google::protobuf::RpcController*,const zb::rpc::GetattrRequest*,
                 zb::rpc::GetattrReply*,google::protobuf::Closure*) override;
    void Readdir(google::protobuf::RpcController*,const zb::rpc::ReaddirRequest*,
                 zb::rpc::ReaddirReply*,google::protobuf::Closure*) override;
    void ListDatabases(google::protobuf::RpcController*,const zb::rpc::ListDatabasesRequest*,
                       zb::rpc::ListDatabasesReply*,google::protobuf::Closure*) override;
private:
    struct Entry;
    std::map<std::string,std::unique_ptr<Entry>> databases_;
    Entry* Select(const std::string&,zb::rpc::MdsStatus*,std::unique_lock<std::mutex>&);
    static void Unavailable(Entry&,const std::string&);
    static bool Healthy(Entry&);
    static void Result(Entry&,int,zb::rpc::MdsStatus*);
};
}
