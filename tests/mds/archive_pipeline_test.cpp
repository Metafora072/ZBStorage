#include "mds/archive/SubtreeArchive.h"
#include "mds/lhm/LhmNamespaceStore.h"
#include <brpc/server.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <google/protobuf/util/json_util.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>

using namespace zb::mds;
using namespace zb::rpc;
void Check(bool ok, const std::string& why) { if (!ok) throw std::runtime_error(why); }
// Recording protocol peer only: no image generation or data transfer is mocked
// as success. Integration checks end at SendArchiveMetadata admission.
class Receiver : public OpticalNodeService {
public:
    std::ofstream log;
    std::mutex mutex;
    int calls = 0, fail_at = 0;
    Receiver(const std::string& path, int fail) : log(path), fail_at(fail) { Check(bool(log), "receiver log"); }
    void SendArchiveMetadata(google::protobuf::RpcController*, const SendArchiveMetadataRequest* request,
        ArchiveReply* reply, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        std::lock_guard<std::mutex> lock(mutex);
        std::string json;
        google::protobuf::util::JsonPrintOptions options; options.preserve_proto_field_names = true;
        google::protobuf::util::MessageToJsonString(*request, &json, options);
        log << json << '\n'; log.flush();
        ++calls;
        if (fail_at == -1) return; // Malformed success reply without status.
        if (fail_at == -2) bthread_usleep(300000); // Client deadline is 50 ms.
        reply->mutable_status()->set_code(calls == fail_at ? MDS_INTERNAL_ERROR : MDS_OK);
        if (fail_at == -3 && request->files_size()) {
            auto* admission = reply->add_admissions();
            admission->set_inode_id(request->files(0).inode_id());
            admission->mutable_status()->set_code(MDS_INTERNAL_ERROR);
        }
        if (calls == fail_at) reply->mutable_status()->set_message("injected optical rejection");
    }
};
class Capture : public ArchiveMetadataSink {
public:
    std::vector<SendArchiveMetadataRequest> requests;
    int fail_at = 0;
    NamespaceStatus Send(const SendArchiveMetadataRequest& request, bool* uncertain) override {
        requests.push_back(request);
        *uncertain = requests.size() == static_cast<size_t>(fail_at);
        if (*uncertain) return {MDS_INTERNAL_ERROR, "injected admission failure"};
        return {};
    }
};

int main(int argc, char** argv) { try {
    if (argc == 5 && std::string(argv[1]) == "--serve") {
        Receiver receiver(argv[3], std::stoi(argv[4]));
        brpc::Server server;
        Check(server.AddService(&receiver, brpc::SERVER_DOESNT_OWN_SERVICE) == 0, "add receiver");
        Check(server.Start((std::string("127.0.0.1:") + argv[2]).c_str(), nullptr) == 0, "start receiver");
        server.RunUntilAskedToQuit(); return 0;
    }
    Check(argc == 2 && !std::filesystem::exists(argv[1]), "fresh work directory required");
    std::filesystem::create_directories(argv[1]);
    LhmNamespaceStore ns(std::string(argv[1]) + "/namespace.meta"); Check(ns.ready(), "LHM open");
    std::map<uint64_t, std::string> records;
    bool record_error = false;
    FileLocationSources sources;
    sources.read_inode = [&](uint64_t id, InodeAttr* a, std::string* error) { return ns.GetInode(id, a, error); };
    sources.read_record = [&](uint64_t id, bool, std::string* value, std::string* error) {
        error->clear();
        if (record_error) { *error = "injected record I/O error"; return false; }
        auto it = records.find(id); if (it == records.end()) return false;
        *value = it->second; return true;
    };
    sources.resolve_disk = [](const std::string&, uint32_t n, std::string* out) {
        *out = "resolved-disk-" + std::to_string(n); return true;
    };
    sources.resolve_node = [](const std::string&, std::string* address, uint64_t* generation) {
        *address = "127.0.0.1:8000"; *generation = 7;
    };
    sources.resolve_optical = [](OpticalFileLocation*, bool, std::string*) { return true; };
    RecordFileLocationResolver resolver(sources);
    auto create = [&](const std::string& path, InodeType type) {
        std::string error;
        auto r = ns.ReserveCreate(path, type, &error); Check(bool(r), error);
        InodeAttr a; a.set_inode_id(r->inode_id()); a.set_type(type);
        a.set_file_name(path.substr(path.rfind('/') + 1)); a.set_mode(0755); a.set_nlink(1);
        a.set_size(123); a.set_object_unit_size(4096); a.set_replica(1); a.set_version(3);
        a.set_storage_tier(INODE_STORAGE_DISK); a.set_disk_node_id(1); a.set_disk_id(2);
        Check(r->Commit(a).ok(), "commit fixture");
        if (type == INODE_FILE) {
            DiskFileLocation loc; loc.set_node_id("opaque-source-A"); loc.set_disk_id("opaque-disk-B");
            records[a.inode_id()] = loc.SerializeAsString();
        }
        return a.inode_id();
    };
    create("/tree", INODE_DIR); create("/tree/child", INODE_DIR); create("/empty", INODE_DIR);
    const auto a = create("/tree/a", INODE_FILE);
    const auto b = create("/tree/b", INODE_FILE);
    const auto c = create("/tree/child/c", INODE_FILE);
    create("/outside", INODE_FILE);
    FileLocationView view;
    Check(resolver.Resolve(a, &view).ok() && view.disk_location().node_id() == "opaque-source-A"
          && view.disk_location().disk_id() == "opaque-disk-B"
          && view.disk_location().instance_generation() == 7 && !view.disk_location().node_address().empty(), "full location");
    const auto record = records[a]; records.erase(a);
    Check(resolver.Resolve(a, &view).ok() && view.disk_location().disk_id() == "resolved-disk-2", "legacy fallback");
    records[a] = "broken";
    Check(resolver.Resolve(a, &view).code == MDS_INTERNAL_ERROR && !view.has_attr(), "corruption must not fall back");
    records[a] = record; record_error = true;
    Check(resolver.Resolve(a, &view).code == MDS_INTERNAL_ERROR, "record I/O error"); record_error = false;
    Check(resolver.Resolve(0, &view).code == MDS_INVALID_ARGUMENT, "invalid ID");
    uint64_t missing = 0; { std::string e; auto r = ns.ReserveCreate("/gone", INODE_FILE, &e); missing = r->inode_id(); }
    Check(resolver.Resolve(missing, &view).code == MDS_NOT_FOUND, "missing inode");
    Check(resolver.Resolve(1, &view).code == MDS_INVALID_ARGUMENT, "directory rejected");
    for (uint32_t page : {1u, 2u, 100u}) {
        Capture sink; ArchiveSendProgress progress;
        auto status = SendArchiveSubtree(ns, resolver, "/tree", 55, page, 1, sink, &progress);
        Check(status.ok() && progress.acknowledged_files == 3 && !progress.last_batch_may_be_accepted, "send complete");
        std::set<uint64_t> ids;
        Check(sink.requests.back().end_of_batch() && sink.requests.back().files().empty(), "final flush marker");
        for (const auto& request : sink.requests) {
            if (request.end_of_batch()) continue;
            Check(request.batch_id() == 55 && request.files_size() > 0 && request.files_size() <= int(page), "chunk limits");
            for (const auto& f : request.files()) {
                Check(ids.insert(f.inode_id()).second && f.size() == 123 && f.object_unit_size() == 4096
                    && f.target_node_id() == "opaque-source-A" && f.target_disk_id() == "opaque-disk-B"
                    && f.operation_id().empty() && f.image_id().empty(), "archive file fields");
            }
        }
        Check(ids == std::set<uint64_t>{a,b,c}, "subtree coverage");
    }
    Capture failed_end; failed_end.fail_at = 4; ArchiveSendProgress end_progress;
    Check(!SendArchiveSubtree(ns,resolver,"/tree",57,1,1024,failed_end,&end_progress).ok()
        && failed_end.requests.size()==4 && failed_end.requests.back().end_of_batch()
        && end_progress.acknowledged_files==3 && end_progress.attempted_batches==3
        && end_progress.last_batch_may_be_accepted, "failed end marker preserves file progress without retry");
    Capture empty; ArchiveSendProgress progress;
    Check(SendArchiveSubtree(ns,resolver,"/empty",56,2,1,empty,&progress).ok() && empty.requests.empty(), "no empty RPC");
    Capture fail; fail.fail_at = 2;
    Check(!SendArchiveSubtree(ns,resolver,"/tree",57,1,1024,fail,&progress).ok()
        && fail.requests.size()==2 && progress.acknowledged_files==1 && progress.attempted_batches==2
        && progress.last_batch_may_be_accepted, "stop without retry, report partial progress");
    InodeAttr saved; std::string error; Check(ns.GetInode(a,&saved,&error), "read fixture");
    for (int variant = 0; variant < 4; ++variant) {
        auto attr = saved;
        if (variant==0) attr.set_storage_tier(INODE_STORAGE_OPTICAL);
        if (variant==1) attr.set_replica(2);
        if (variant==2) attr.set_file_archive_state(INODE_ARCHIVE_ARCHIVED);
        if (variant==3) attr.set_object_unit_size(0);
        Check(ns.PutInode(a,attr,&error), "unsupported fixture");
        Capture sink; Check(!SendArchiveSubtree(ns,resolver,"/tree",58,100,1024,sink,&progress).ok()
            && sink.requests.empty(), "invalid file discards entire current chunk");
        Check(ns.PutInode(a,saved,&error), "restore fixture");
    }
    records[a] = "broken";
    std::unique_ptr<ArchiveManifestBuilder> manifest;
    Check(ArchiveManifestBuilder::Begin(ns,resolver,"/tree",1024,&manifest).ok(), "begin invalid manifest");
    ArchiveBatch batch;
    Check(!manifest->Next(100,&batch).ok() && batch.files.empty(), "no partial manifest batch");
    records[a] = record;
    Check(!manifest->Next(100,&batch).ok(), "manifest error terminal");
    auto optical = saved; optical.set_storage_tier(INODE_STORAGE_OPTICAL);
    Check(ns.PutInode(a,optical,&error), "optical fixture");
    OpticalFileLocation ol; ol.set_node_id("optical-A"); ol.set_disk_id("disc-opaque"); ol.set_image_id("img-opaque");
    records[a] = ol.SerializeAsString();
    Check(resolver.Resolve(a,&view).ok() && view.optical_location().image_id()=="img-opaque", "opaque optical location");
    Check(ns.PutInode(a,saved,&error), "restore hot"); records[a] = record;
    // Detect a mutation between enumeration and the resolver's subsequent read.
    auto changed_sources = sources;
    changed_sources.read_inode = [&](uint64_t id, InodeAttr* attr, std::string* e) {
        if (!ns.GetInode(id,attr,e)) return false;
        attr->set_version(attr->version()+1); return true;
    };
    RecordFileLocationResolver changed(changed_sources); Capture changed_sink;
    Check(!SendArchiveSubtree(ns,changed,"/tree",59,100,1024,changed_sink,&progress).ok()
        && changed_sink.requests.empty(), "inode changed after scan");
    Check(!SendArchiveSubtree(ns,resolver,"/tree",0,100,1024,empty,&progress).ok(), "invalid batch ID");
    std::cout << "PASS location resolver, manifest coverage/filtering, bounded chunks, partial failure and no retry\n";
    return 0;
} catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; } }
