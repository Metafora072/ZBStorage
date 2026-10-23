#include "LhmTemplateImporter.h"
#include "MasstreeManifest.h"
#include "MasstreeMetaStore.h"
#include <filesystem>
#include <deque>
#include <set>
#include <algorithm>
namespace zb::mds {
bool ImportTemplateIntoNamespace(NamespaceStore& target,
    const MasstreeImportService::TemplateImportRequest& req,
    MasstreeImportService::Result* result,std::string* error) {
    auto fail=[&](const std::string& text){ if(error) *error=text; return false; };
    // Existing namespace data and its external object-ID references are not migrated.
    if(req.inode_start || req.template_id.empty() || req.template_id.find('/')!=std::string::npos ||
       req.template_id=="." || req.template_id==".." ||
       (!req.template_mode.empty() && req.template_mode!="page_fast"))
        return fail("LHM imports metadata templates with fresh IDs; inode_start/legacy_records are unsupported");
    auto manifest_path=(std::filesystem::path(req.masstree_root)/"templates"/req.template_id/"template.staging"/"manifest.txt").string();
    MasstreeNamespaceManifest manifest;
    if(!MasstreeNamespaceManifest::LoadFromFile(manifest_path,&manifest,error)) return false;
    if(manifest.template_id!=req.template_id || manifest.path_prefix.rfind("/__masstree_template__/",0)!=0)
        return fail("source must be a generated metadata template, not an existing namespace");
    MasstreeNamespaceRoute route;
    route.namespace_id=manifest.namespace_id; route.generation_id=manifest.generation_id;
    route.path_prefix=manifest.path_prefix; route.manifest_path=manifest_path;
    route.root_inode_id=manifest.root_inode_id; route.inode_min=manifest.inode_min;
    route.inode_max=manifest.inode_max; route.inode_count=manifest.inode_count;
    MasstreeMetaStore source; // source reader lifetime ends with this import
    struct Item {std::string source_path,target_path; uint64_t source_id;};
    std::deque<Item> pending{{manifest.path_prefix,req.path_prefix,manifest.root_inode_id}};
    std::set<uint64_t> visited;
    *result={}; result->manifest_path=manifest_path;
    while(!pending.empty()) {
        auto item=std::move(pending.front()); pending.pop_front();
        if(!visited.insert(item.source_id).second) return fail("template contains repeated inode or cycle");
        zb::rpc::InodeAttr attr;
        if(!source.GetInode(route,item.source_id,&attr,error)) return fail("cannot read template inode");
        auto creation=target.ReserveCreate(item.target_path,attr.type(),error);
        if(!creation) return false;
        attr.set_inode_id(creation->inode_id());
        auto path=std::filesystem::path(item.target_path).lexically_normal().string();
        while(path.size()>1 && path.back()=='/') path.pop_back();
        attr.set_file_name(path.substr(path.rfind('/')+1));
        if(!attr.nlink()) attr.set_nlink(attr.type()==zb::rpc::INODE_DIR?2:1);
        auto status=creation->Commit(attr);
        if(!status.ok()) return fail(status.message);
        const auto id=creation->inode_id();
        if(!result->root_inode_id) result->root_inode_id=id;
        result->inode_min=result->inode_count?std::min(result->inode_min,id):id;
        result->inode_max=std::max(result->inode_max,id); ++result->inode_count;
        if(attr.type()==zb::rpc::INODE_FILE) {++result->file_count; continue;}
        std::string cursor;
        bool more=false;
        do {
            std::vector<zb::rpc::Dentry> entries; std::string next;
            if(!source.Readdir(route,item.source_path,cursor,1024,&entries,&more,&next,error)) return false;
            if(more && (next.empty() || next==cursor)) return fail("invalid source pagination");
            for(const auto& entry:entries) {
                if(entry.name().empty() || entry.name()=="." || entry.name()==".." || entry.name().find('/')!=std::string::npos)
                    return fail("invalid template entry name");
                pending.push_back({item.source_path+"/"+entry.name(),path+"/"+entry.name(),entry.inode_id()});
                ++result->dentry_count;
            }
            cursor=next;
        } while(more);
    }
    // min/max are descriptive only: LHM IDs are never treated as a contiguous range.
    return true;
}
}
