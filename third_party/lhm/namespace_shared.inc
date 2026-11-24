// Included inside LhmNamespace. All native page layouts and inode IDs stay intact.
bool shared_readonly() const { return shared_snapshot_; }
size_t shared_template_count() const { return shared_catalog_.templates.size(); }
size_t shared_instance_count() const { return shared_catalog_.instances.size(); }
uint64_t shared_route_count() const {
    uint64_t n=0; for(const auto& t:shared_routes_) if(t) n+=t->routes.size(); return n;
}
void validate_shared_instances() {
    shared_names_.clear(); std::vector<uint64_t> roots;
    for(size_t n=0;n<shared_catalog_.instances.size();++n) {
        const auto& b=shared_catalog_.instances[n]; const auto& t=shared_catalog_.templates[b.template_index];
        if(!shared_names_.emplace(b.name,n).second) throw std::runtime_error("duplicate shared batch root");
        roots.push_back(PathKey::hash_component(b.name));
        if(!persistent_store_.DirectoryAddressMayExist(kCompactHeadTag|(b.head_first+t.directories-1)) ||
           (t.bucket_pages && !persistent_store_.DirectoryAddressMayExist(b.bucket_first+t.bucket_pages-1)) ||
           (t.inode_pages && !persistent_store_.InodeAddressMayExist(make_inode_ref(b.inode_first+t.inode_pages-1,0))))
            throw std::runtime_error("shared mapping exceeds allocated data pages");
    }
    auto expected=snapshot_indexes_.roots; std::sort(roots.begin(),roots.end()); std::sort(expected.begin(),expected.end());
    if(roots!=expected) throw std::runtime_error("shared batch root catalog mismatch");
}
void require_shared_importable() {
    if(!shared_snapshot_ && (!snapshot_indexes_.files.empty() || !snapshot_root_hashes().empty()))
        throw std::runtime_error("shared import requires an empty or shared-index database; convert legacy data separately");
    if(!shared_snapshot_) {
        DirectoryBlockStore::file_index_debug_info info{};
        if(!directory_blocks_.debug_file_index(root_directory_file_index_id_,info) || info.entry_count)
            throw std::runtime_error("shared import requires empty root files");
    }
}
void save_shared_snapshot(const std::string& path,const std::string& digest,
                          const compiled::Header& header,const compiled::Bases& bases,
                          const std::vector<compiled::Route>& routes,const std::string& target,
                          threadinfo& ti) {
    if(!append_only_snapshot_ || !batch_undo() || !sync_persistence())
        throw std::runtime_error("shared checkpoint requires synchronized offline import");
    require_shared_importable();
    auto indexes=snapshot_indexes_; auto catalog=shared_catalog_;
    SharedTemplateInfo info{digest,header.files,header.directories,header.inode_pages,header.bucket_pages};
    size_t id=0; for(;id<catalog.templates.size();++id) if(catalog.templates[id].digest==digest) break;
    if(id==catalog.templates.size()) {
        const auto token=batch_undo()->CreateIndex();
        const auto file=IndexPath(snapshot_store_,token);
        const auto checksum=SharedRoutes::save(file,routes); BatchFault("shared_index_written");
        const int fd=::open(file.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
        if(fd<0) throw std::runtime_error("cannot open shared index for sync");
        int rc; do {rc=::fsync(fd);} while(rc && errno==EINTR); ::close(fd);
        if(rc) throw std::runtime_error("cannot sync shared index");
        auto identity=InspectIndex(snapshot_store_,token); identity.checksum=checksum;
        indexes.files.push_back(identity); catalog.templates.push_back(info); BatchFault("shared_index_synced");
    } else {
        const auto& old=catalog.templates[id];
        if(old.files!=info.files || old.directories!=info.directories || old.inode_pages!=info.inode_pages || old.bucket_pages!=info.bucket_pages)
            throw std::runtime_error("shared template identity reused with different counts");
    }
    if(catalog.instances.size()>=kMaxIndexFiles || catalog.templates.size()>kMaxIndexFiles)
        throw std::runtime_error("shared checkpoint catalog limit exceeded");
    const auto name=target.substr(1);
    for(const auto& b:catalog.instances) if(b.name==name) throw std::runtime_error("duplicate shared instance");
    indexes.roots.push_back(PathKey::hash_component(name));
    catalog.instances.push_back({name,id,bases.inode_first,bases.bucket_first,bases.head_first});
    ValidateIndexFiles(snapshot_store_,indexes);
    SnapshotWriter out(path); save_snapshot_header(out,kSharedSnapshotVersion);
    WriteIndexCatalog(out,indexes); WriteSharedCatalog(out,catalog); out.finish();
    // Validate serialized ranges/catalog before the caller prepares its commit.
    (void)CheckpointIndexes(path); BatchFault("shared_mapping_written");
}
bool shared_entry_in_range(const SharedInstance& b,const DirectoryBlockStore::entry& e) const {
    const auto& t=shared_catalog_.templates[b.template_index];
    if(e.kind!=DirectoryBlockStore::dentry_kind::file || e.file_ref.block_id<b.inode_first ||
       e.file_ref.block_id-b.inode_first>=t.inode_pages || e.file_ref.offset>=kInodesPerBlock) return false;
    return (e.file_ref.block_id-b.inode_first)*kInodesPerBlock+e.file_ref.offset<t.files;
}
bool shared_lookup(const ParsedPath& parsed,value_type& out) const {
    const auto it=shared_names_.find(parsed.components.front());
    if(it==shared_names_.end()) return false;
    const auto& b=shared_catalog_.instances[it->second]; const auto& tree=*shared_routes_[b.template_index];
    uint64_t ordinal=1;
    for(size_t i=1;i<parsed.components.size();++i) {
        const auto child=tree.child(ordinal,parsed.components[i]);
        if(child) {ordinal=child; continue;}
        if(i+1!=parsed.components.size()) return false;
        DirectoryBlockStore::entry e;
        if(!directory_blocks_.lookup_shared(tree.routes[ordinal-1].plan,b.bucket_first,
                parsed.hashes[i],parsed.components[i],e)) return false;
        if(!shared_entry_in_range(b,e)) throw std::runtime_error("shared bucket references another instance or tail padding");
        out=DirectoryBlockStore::to_namespace_entry(e); return true;
    }
    out=make_namespace_entry(entry_kind::directory,make_inode_ref(kCompactHeadTag|(b.head_first+ordinal-1),0),parsed.components.back());
    return true;
}
bool shared_readdir(const ParsedPath& parsed,std::vector<readdir_record>& result) const {
    auto add=[&](const std::string& name,const value_type& entry) {
        readdir_record r; r.child_name=name; r.entry=entry; r.full_hashes=parsed.hashes;
        r.child_component_hash=PathKey::hash_component(name); r.full_hashes.push_back(r.child_component_hash);
        result.push_back(std::move(r));
    };
    if(parsed.components.empty()) {
        for(const auto& b:shared_catalog_.instances)
            add(b.name,make_namespace_entry(entry_kind::directory,make_inode_ref(kCompactHeadTag|b.head_first,0),b.name));
        return true;
    }
    const auto it=shared_names_.find(parsed.components.front()); if(it==shared_names_.end()) return false;
    const auto& b=shared_catalog_.instances[it->second]; const auto& tree=*shared_routes_[b.template_index];
    uint64_t ordinal=1;
    for(size_t i=1;i<parsed.components.size();++i) {ordinal=tree.child(ordinal,parsed.components[i]); if(!ordinal) return false;}
    auto child=std::lower_bound(tree.children.begin(),tree.children.end(),ordinal,
        [&](uint64_t n,uint64_t parent) {return tree.routes[n-1].parent_ordinal<parent;});
    for(;child!=tree.children.end() && tree.routes[*child-1].parent_ordinal==ordinal;++child) {
        const auto& r=tree.routes[*child-1];
        add(r.name,make_namespace_entry(entry_kind::directory,make_inode_ref(kCompactHeadTag|(b.head_first+r.ordinal-1),0),r.name));
    }
    std::vector<DirectoryBlockStore::entry> files;
    if(!directory_blocks_.read_shared_entries(tree.routes[ordinal-1].plan,b.bucket_first,files)) return false;
    for(const auto& f:files) {if(!shared_entry_in_range(b,f)) return false; add(f.name,DirectoryBlockStore::to_namespace_entry(f));}
    return true;
}

const IndexCatalog& shared_index_catalog() const { return snapshot_indexes_; }
const SharedCatalog& shared_mapping_catalog() const { return shared_catalog_; }
compiled::Bases inspect_legacy_shared_instance(const IndexFile& file,const std::string& target,
    const compiled::Header& h,const std::vector<compiled::Route>& routes,
    const directory_block_image* template_anchor) const {
    SnapshotReader in(IndexPath(snapshot_store_,file.token));
    if(in.u64()!=kIndexFileMagic || in.u64()!=1) throw std::runtime_error("conversion requires v5 immutable directory indexes");
    compiled::Bases bases{}; bases.inode_pages=h.inode_pages; bases.bucket_pages=h.bucket_pages; bases.head_count=h.directories;
    std::vector<bool> seen(h.directories,false); std::vector<uint64_t> parents;
    uint64_t count=0;
    auto equal=[&](uint64_t value) {if(in.u64()!=value) throw std::runtime_error("legacy index differs from compiled template");};
    for(;;) {
        auto depth=in.u64(); if(depth==UINT64_MAX) break;
        const auto hash=in.u64(),head=in.u64(),tail=in.u64();
        if(!depth || depth>4096 || depth>parents.size()+1 || !(head&kCompactHeadTag) || head!=tail)
            throw std::runtime_error("unsupported legacy directory record");
        if(!count) {
            if(depth!=1 || hash!=PathKey::hash_component(target.substr(1)))
                throw std::runtime_error("conversion target does not match index root");
            bases.head_first=head&kMaxLogicalBlock;
        }
        const auto address=head&kMaxLogicalBlock;
        if(address<bases.head_first || address-bases.head_first>=h.directories)
            throw std::runtime_error("legacy directory outside contiguous template range");
        const auto ordinal=address-bases.head_first+1;
        if(seen[ordinal-1]) throw std::runtime_error("duplicate legacy directory");
        seen[ordinal-1]=true; const auto& r=routes[ordinal-1];
        if((depth==1?0:parents[depth-2])!=r.parent_ordinal ||
           hash!=PathKey::hash_component(ordinal==1?target.substr(1):r.name))
            throw std::runtime_error("legacy hierarchy differs from template");
        parents.resize(depth); parents.back()=ordinal; ++count;
        const auto& d=r.plan.directory;
        equal(uint64_t(r.plan.state)); equal(head); equal(d.global_depth); equal(d.entry_count); equal(d.single_bucket);
        equal(d.slots.size()); for(auto slot:d.slots) equal(slot);
        equal(d.buckets.size());
        for(const auto& b:d.buckets) {
            const auto address=in.u64();
            if(address<b.block_id) throw std::runtime_error("legacy bucket base underflow");
            const auto base=address-b.block_id+1;
            if(!bases.bucket_first) bases.bucket_first=base;
            if(base!=bases.bucket_first) throw std::runtime_error("legacy bucket ranges are not relocatable");
            equal(b.local_depth); equal(b.entry_count); equal(b.version); equal(b.flags);
        }
    }
    if(count!=h.directories || in.checksum()!=file.checksum) throw std::runtime_error("legacy index count/checksum mismatch");
    in.finish();
    if(!h.files) {bases.inode_first=1; bases.bucket_first=1; return bases;}
    if(!template_anchor || !bases.bucket_first || !template_anchor->header.entry_count)
        throw std::runtime_error("missing template address anchor");
    const auto& image=*template_anchor;
    DirectoryBlockStore::dentry_entry_header header{}; std::memcpy(&header,image.payload,sizeof(header));
    DirectoryBlockStore::file_ref_payload ref{};
    if(header.kind!=uint8_t(DirectoryBlockStore::dentry_kind::file) || header.ref_bytes!=sizeof(ref) ||
       header.flags || header.name_length==0 || header.name_length>64 ||
       header.record_bytes<sizeof(header)+sizeof(ref)+header.name_length || header.record_bytes>image.header.used_bytes)
        throw std::runtime_error("invalid template address anchor");
    std::memcpy(&ref,image.payload+sizeof(header),sizeof(ref));
    const std::string name(reinterpret_cast<const char*>(image.payload+sizeof(header)+sizeof(ref)),header.name_length);
    if(!(image.header.dir_inode_id>>63)) throw std::runtime_error("invalid template bucket owner");
    const auto owner=image.header.dir_inode_id&kMaxLogicalBlock;
    if(!owner || owner>routes.size()) throw std::runtime_error("template anchor owner out of bounds");
    DirectoryBlockStore::entry actual;
    if(!directory_blocks_.lookup_shared(routes[owner-1].plan,bases.bucket_first,header.component_hash,name,actual) ||
       actual.file_ref.block_id<ref.inode_block_id || actual.file_ref.offset!=ref.inode_slot)
        throw std::runtime_error("legacy file address anchor differs from template");
    bases.inode_first=actual.file_ref.block_id-ref.inode_block_id+1;
    return bases;
}
void save_converted_shared_snapshot(const std::string& path,const IndexCatalog& indexes,const SharedCatalog& catalog) {
    if(!append_only_snapshot_ || shared_snapshot_ || !batch_undo() || !sync_persistence())
        throw std::runtime_error("invalid shared conversion state");
    // Validate all mappings against the allocation and the old global roots.
    shared_catalog_=catalog; validate_shared_instances(); shared_catalog_={}; shared_names_.clear();
    ValidateIndexFiles(snapshot_store_,indexes);
    SnapshotWriter out(path); save_snapshot_header(out,kSharedSnapshotVersion);
    WriteIndexCatalog(out,indexes); WriteSharedCatalog(out,catalog); out.finish();
    (void)CheckpointIndexes(path);
}
