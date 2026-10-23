#include "mds/service/MdsServiceImpl.h"
#include "mds/lhm/LhmNamespaceStore.h"
#include "mds/storage/MetaSchema.h"
#include <filesystem>
#include <fstream>
#include <thread>
#include <iostream>
using namespace zb::mds;
using namespace zb::rpc;
void check(bool ok,const std::string& message){if(!ok)throw std::runtime_error(message);}
void ok(const MdsStatus& s){check(s.code()==MDS_OK,s.message());}
int main(int argc,char** argv){try{
    check(argc==2 && !std::filesystem::exists(argv[1]),"fresh work directory required");
    const std::string work=argv[1]; std::filesystem::create_directories(work);
    RocksMetaStore control; std::string error;
    check(control.Open(work+"/control",&error),error);
    check(control.Put("MTR/","invalid legacy route",&error),error);
    check(control.Put("AN/","invalid legacy route",&error),error);
    control.DisableNamespaceAccess();
    LhmNamespaceStore ns(work+"/lhm.meta");check(ns.ready(),"LHM open");
    NodeInfo node;node.node_id="node-real-01";node.address="127.0.0.1:1";node.disks.push_back({"disk-data-A"});
    NodeStateCache cache({node});ObjectAllocator allocator(&cache);
    MdsServiceImpl service(&control,&allocator,4194304,work+"/archive",work+"/templates",{},65536,
                           false,true,0.7,0,2.5,true,nullptr,nullptr,nullptr,&ns,true);
    MkdirRequest mk;MkdirReply mkr;mk.set_path("/hot");mk.set_mode(0755);service.Mkdir(nullptr,&mk,&mkr,nullptr);ok(mkr.status());
    CreateRequest cr;CreateReply crr;cr.set_path("/hot/file");cr.set_mode(0644);cr.set_replica(1);
    service.Create(nullptr,&cr,&crr,nullptr);ok(crr.status());
    auto id=crr.location().attr().inode_id();check(id>1,"LHM ID");
    LookupRequest lookup;LookupReply lookupr;lookup.set_path("/hot/file");service.Lookup(nullptr,&lookup,&lookupr,nullptr);ok(lookupr.status());
    check(lookupr.attr().inode_id()==id,"lookup ID");
    GetattrRequest get;GetattrReply getr;get.set_inode_id(id);service.Getattr(nullptr,&get,&getr,nullptr);ok(getr.status());
    GetFileLocationRequest location; GetFileLocationReply locationr;
    location.set_inode_id(id); service.GetFileLocation(nullptr,&location,&locationr,nullptr); ok(locationr.status());
    check(locationr.location().disk_location().node_id()==node.node_id &&
          locationr.location().disk_location().disk_id()=="disk-data-A", "full upstream location preserved with LHM inode");
    std::string full_location;
    check(control.Get(DiskFileLocationKey(id), &full_location, &error), "read full location fixture");
    check(control.Put(DiskFileLocationKey(id), "broken", &error), "corrupt full location fixture");
    locationr.Clear(); service.GetFileLocation(nullptr,&location,&locationr,nullptr);
    check(locationr.status().code()==MDS_INTERNAL_ERROR, "service must not fall back on corrupt location");
    check(control.Put(DiskFileLocationKey(id), full_location, &error), "restore full location fixture");
    ModelOpticalArchiveRequest optical; ModelOpticalArchiveReply opticalr;
    service.ModelOpticalArchive(nullptr,&optical,&opticalr,nullptr);
    check(opticalr.status().message().find("LHM namespace mode")!=std::string::npos,"new modeled archive guard");
    opticalr.Clear(); service.CommitPhysicalOpticalArchive(nullptr,&optical,&opticalr,nullptr);
    check(opticalr.status().message().find("LHM namespace mode")!=std::string::npos,"new physical archive guard");
    CommitModelOpticalMigrationRequest cm; CommitModelOpticalMigrationReply cmr;
    service.CommitModelOpticalMigration(nullptr,&cm,&cmr,nullptr);
    check(cmr.status().message().find("LHM namespace mode")!=std::string::npos,"new optical migration guard");
    CommitOpticalLibraryHandoffRequest handoff; CommitOpticalLibraryHandoffReply handoffr;
    service.CommitOpticalLibraryHandoff(nullptr,&handoff,&handoffr,nullptr);
    check(handoffr.status().message().find("LHM namespace mode")!=std::string::npos,"new handoff guard");
    ImportArchiveNamespaceRequest ar;ImportArchiveNamespaceReply arr;service.ImportArchiveNamespace(nullptr,&ar,&arr,nullptr);
    check(arr.status().code()==MDS_INVALID_ARGUMENT,"archive guard");
    CommitFileMigrationRequest migration;CommitFileMigrationReply migrationr;
    service.CommitFileMigration(nullptr,&migration,&migrationr,nullptr);check(migrationr.status().code()==MDS_INVALID_ARGUMENT,"migration guard");
    std::ofstream(work+"/paths.txt")<<"/folder/alpha\n/folder/beta\n";
    auto wait=[&](const std::string& id){
        for(int n=0;n<600;++n){GetMasstreeImportJobRequest r;GetMasstreeImportJobReply reply;r.set_job_id(id);
            service.GetMasstreeImportJob(nullptr,&r,&reply,nullptr);ok(reply.status());
            check(reply.job().state()!=MASSTREE_IMPORT_JOB_FAILED,reply.job().error_message());
            if(reply.job().state()==MASSTREE_IMPORT_JOB_COMPLETED)return reply.job();
            std::this_thread::sleep_for(std::chrono::milliseconds(100));}
        throw std::runtime_error("job timeout");};
    GenerateMasstreeTemplateRequest gen;GenerateMasstreeTemplateReply genr;gen.set_template_id("fixture");
    gen.set_path_list_file(work+"/paths.txt");gen.set_file_count(2);gen.set_path_list_leaf_nodes_are_files(true);
    service.GenerateMasstreeTemplate(nullptr,&gen,&genr,nullptr);ok(genr.status());wait(genr.job_id());
    ImportMasstreeNamespaceRequest imp;ImportMasstreeNamespaceReply impr;imp.set_template_id("fixture");
    imp.set_namespace_id("cold");imp.set_generation_id("g1");imp.set_path_prefix("/cold");imp.set_publish_route(true);
    service.ImportMasstreeNamespace(nullptr,&imp,&impr,nullptr);ok(impr.status());auto job=wait(impr.job_id());
    check(job.file_count()==2,"import file count");
    std::filesystem::rename(work+"/templates",work+"/source_offline");
    std::vector<std::string> pending{"/cold"};int files=0;
    while(!pending.empty()){
        auto path=pending.back();pending.pop_back();ReaddirRequest r;ReaddirReply reply;r.set_path(path);
        service.Readdir(nullptr,&r,&reply,nullptr);ok(reply.status());
        for(const auto& child:reply.entries()){
            lookup.set_path(path+"/"+child.name());lookupr.Clear();service.Lookup(nullptr,&lookup,&lookupr,nullptr);ok(lookupr.status());
            if(child.type()==INODE_DIR)pending.push_back(lookup.path());else ++files;
        }
    }
    check(files==2,"source-independent namespace");
    RenameRequest rename;RenameReply renamer;rename.set_old_path("/cold");rename.set_new_path("/cold_moved");
    service.Rename(nullptr,&rename,&renamer,nullptr);ok(renamer.status());
    lookup.set_path("/cold_moved");lookupr.Clear();service.Lookup(nullptr,&lookup,&lookupr,nullptr);ok(lookupr.status());
    check(lookupr.attr().inode_id()==job.root_inode_id(),"import root stable ID");
    // Simple archive callbacks: two images on one disc, inode fields are authoritative.
    cr.set_path("/hot/second"); crr.Clear(); service.Create(nullptr,&cr,&crr,nullptr); ok(crr.status());
    const auto second_id = crr.location().attr().inode_id();
    AllocateAvailableImageIdRequest ai; AllocateAvailableImageIdReply air;
    ai.set_node_id("optical-A"); ai.set_node_address("127.0.0.1:9200");
    service.AllocateAvailableImageId(nullptr,&ai,&air,nullptr); ok(air.status());
    const auto image1 = air.image_id();
    air.Clear(); service.AllocateAvailableImageId(nullptr,&ai,&air,nullptr); ok(air.status());
    const auto image2 = air.image_id(); check(image1 != image2,"unique image allocations");
    ReportFilesPackedToImageRequest packed; SimpleArchiveReportReply reported;
    packed.set_node_id("optical-A"); packed.set_image_id(image1); packed.add_inode_ids(id);
    service.ReportFilesPackedToImage(nullptr,&packed,&reported,nullptr); ok(reported.status());
    check(reported.updated_files()==1,"first image count");
    getr.Clear(); service.Getattr(nullptr,&get,&getr,nullptr); ok(getr.status());
    check(getr.attr().file_archive_state()==INODE_ARCHIVE_IMAGE_READY &&
          getr.attr().optical_image_id()==std::stoull(image1.substr(4)) &&
          getr.attr().optical_disk_id()==0 && getr.attr().storage_tier()==INODE_STORAGE_DISK,
          "image-ready survives LHM codec without primary-location change");
    packed.set_image_id(image2); packed.clear_inode_ids(); packed.add_inode_ids(second_id);
    service.ReportFilesPackedToImage(nullptr,&packed,&reported,nullptr); ok(reported.status());
    AllocateAvailableDiscIdRequest ad; AllocateAvailableDiscIdReply adr;
    ad.set_node_id("optical-A"); ad.set_node_address("127.0.0.1:9200");
    service.AllocateAvailableDiscId(nullptr,&ad,&adr,nullptr); ok(adr.status());
    ReportImagesBurnedToDiscRequest burned; burned.set_node_id("optical-A");
    burned.set_disc_id(adr.disc_id()); burned.add_image_ids(image1); burned.add_image_ids(image2);
    service.ReportImagesBurnedToDisc(nullptr,&burned,&reported,nullptr); ok(reported.status());
    check(reported.updated_files()==2,"two-image disc scan updates exactly both files");
    for (auto file_id : {id,second_id}) {
        InodeAttr attr; check(ns.GetInode(file_id,&attr,&error),error);
        check(attr.file_archive_state()==INODE_ARCHIVE_ARCHIVED &&
              attr.optical_disk_id()==std::stoull(adr.disc_id().substr(5)) &&
              attr.storage_tier()==INODE_STORAGE_DISK,"completed archive inode fields");
    }
    std::cout<<"PASS in-process MDS: hot namespace, conflicting routes, guards, template import, source-independent lookup and rename\n";
    return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
