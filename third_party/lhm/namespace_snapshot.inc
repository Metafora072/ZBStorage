// Caller serializes all operations and drains reservations before checkpointing.
void save_directory_records(SnapshotWriter& out, threadinfo& ti) {
    struct Frame { bool have=false; uint64_t last=0; };
    std::vector<Frame> stack(1);
    ParsedPath location; location.normalized_path="<snapshot>";
    while (!stack.empty()) {
        const node_type* root=nullptr;
        if (!locate_directory_root(location,root,ti)) throw std::runtime_error("snapshot directory lookup failed");
        auto& frame=stack.back(); uint64_t hash=0; value_type child{}; bool found=false;
        if (!directory_index_.next_child_slot(root,frame.have,frame.last,hash,child,found))
            throw std::runtime_error("snapshot directory scan failed");
        if (!found) { stack.pop_back(); if(!stack.empty()) location.hashes.pop_back(); continue; }
        frame.have=true; frame.last=hash;
        if (!entry_is_directory(child)) throw std::runtime_error("unexpected route entry");
        location.hashes.push_back(hash);
        const node_type* child_root=nullptr; uint64_t head=0,tail=0,index=0;
        if (!locate_directory_root(location,child_root,ti) ||
            !directory_block_refs_for_root(child_root,head,tail) || !file_index_id_for_root(child_root,index))
            throw std::runtime_error("snapshot directory metadata missing");
        out.u64(location.hashes.size()); out.u64(hash); out.u64(head); out.u64(tail);
        directory_blocks_.save_file_index(index,out);
        stack.emplace_back();
    }
    out.u64(UINT64_MAX);
}
std::vector<uint64_t> snapshot_root_hashes() const {
    std::vector<uint64_t> result;
    bool have=false,found=false; uint64_t last=0,hash=0; value_type child{};
    for(;;) {
        if(!directory_index_.next_child_slot(directory_index_.root(),have,last,hash,child,found))
            throw std::runtime_error("cannot enumerate checkpoint root");
        if(!found) return result;
        if(!entry_is_directory(child)) throw std::runtime_error("unexpected root route");
        result.push_back(hash); have=true; last=hash;
    }
}
uint64_t namespace_index_hash(threadinfo& ti) {
    SnapshotWriter hash("");
    directory_blocks_.save_file_index(root_directory_file_index_id_,hash);
    save_directory_records(hash,ti);
    return hash.checksum();
}
void save_snapshot_header(SnapshotWriter& out,uint64_t version) {
    out.u64(kSegmentedSnapshotMagic); out.u64(version);
    persistent_store_.SaveAllocation(out);
    out.u64(root_persistent_ref_.block_id); out.u64(root_persistent_ref_.offset);
    out.u64(root_directory_head_block_id_); out.u64(root_directory_tail_block_id_);
    directory_blocks_.save_file_index(root_directory_file_index_id_,out);
}
void save_snapshot(const std::string& path, threadinfo& ti) {
    if(shared_snapshot_) throw std::runtime_error("shared-index database is read-only");
    if(append_only_snapshot_) throw std::runtime_error("partial append namespace requires incremental checkpoint");
    if (!pending_creates_.empty() || !sync_persistence())
        throw std::runtime_error("pending reservations or failed data sync");
    // A service has loaded all indexes. Preserve immutable files when indexes
    // match; structural changes use a complete snapshot. This scan runs during
    // service checkpoints, never between compiled batches.
    const bool unchanged=loaded_incremental_snapshot_ &&
        namespace_index_hash(ti)==loaded_directory_hash_;
    SnapshotWriter out(path);
    save_snapshot_header(out,unchanged?kIncrementalSnapshotVersion:kSegmentedSnapshotVersion);
    if(unchanged) {
        ValidateIndexFiles(snapshot_store_,snapshot_indexes_);
        WriteIndexCatalog(out,snapshot_indexes_);
    } else save_directory_records(out,ti);
    out.finish();
}
bool append_target_exists(const std::string& target, threadinfo& ti) {
    const auto parsed=PathKey::parse_absolute_path(target);
    if(parsed.hashes.size()!=1) throw std::runtime_error("append requires one top-level target");
    if(std::find(snapshot_indexes_.roots.begin(),snapshot_indexes_.roots.end(),parsed.hashes[0])!=snapshot_indexes_.roots.end())
        return true;
    value_type existing{};
    return lookup_entry(target,existing,ti);
}
void save_incremental_snapshot(const std::string& path,threadinfo& ti) {
    if(!append_only_snapshot_ || !batch_undo() || !pending_creates_.empty() || !sync_persistence())
        throw std::runtime_error("incremental checkpoint requires a synchronized offline batch");
    auto catalog=snapshot_indexes_;
    auto roots=snapshot_root_hashes();
    catalog.roots.insert(catalog.roots.end(),roots.begin(),roots.end());
    std::sort(catalog.roots.begin(),catalog.roots.end());
    if(catalog.roots.size()>kMaxIndexFiles || catalog.files.size()>=kMaxIndexFiles ||
       std::adjacent_find(catalog.roots.begin(),catalog.roots.end())!=catalog.roots.end())
        throw std::runtime_error("duplicate/excessive incremental checkpoint roots");
    const auto token=batch_undo()->CreateIndex();
    const auto index_path=IndexPath(snapshot_store_,token);
    SnapshotWriter index(index_path);
    index.u64(kIndexFileMagic); index.u64(1);
    save_directory_records(index,ti);
    const auto checksum=index.checksum(); index.finish(); BatchFault("index_written");
    const int fd=::open(index_path.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
    if(fd<0) throw std::runtime_error("cannot open new index for sync");
    int rc; do { rc=::fsync(fd); } while(rc && errno==EINTR);
    ::close(fd);
    if(rc) throw std::runtime_error("cannot sync new index");
    auto identity=InspectIndex(snapshot_store_,token); identity.checksum=checksum;
    catalog.files.push_back(identity);
    ValidateIndexFiles(snapshot_store_,catalog);
    BatchFault("index_synced");
    SnapshotWriter out(path); save_snapshot_header(out,kIncrementalSnapshotVersion);
    WriteIndexCatalog(out,catalog); out.finish();
}
void load_directory_records(SnapshotReader& in,threadinfo& ti) {
    ParsedPath parent; parent.normalized_path="<snapshot>";
    size_t previous_depth=0; uint64_t restored=0;
    for(;;) {
        auto depth=in.u64(); if(depth==UINT64_MAX) break;
        if(!depth || depth>previous_depth+1 || depth>4096) throw std::runtime_error("invalid snapshot directory depth");
        parent.hashes.resize(depth-1);
        auto hash=in.u64(); auto head=in.u64(),tail=in.u64();
        if(!persistent_store_.DirectoryAddressMayExist(head)) throw std::runtime_error("invalid directory reference");
        auto meta=make_directory_meta(head,tail); meta.file_index_id=directory_blocks_.load_file_index(head,in);
        const node_type* root=nullptr; node_type* child=nullptr;
        if(!locate_directory_root(parent,root,ti) ||
            !directory_index_.create_directory_layer(const_cast<node_type*>(root),hash,meta,child,ti))
            throw std::runtime_error("snapshot directory reconstruction failed");
        parent.hashes.push_back(hash); previous_depth=depth;
        if((++restored & 63)==0) {
            globalepoch.store(globalepoch.load()+2); active_epoch.store(globalepoch.load()); ti.rcu_quiesce();
        }
    }
}
void reopen_snapshot(const std::string& file, const std::string& snapshot, threadinfo& ti,
                     bool append_only=false) {
    shared_snapshot_=false; shared_catalog_={}; shared_routes_.clear(); shared_names_.clear();
    snapshot_indexes_={}; snapshot_store_=file; append_only_snapshot_=append_only;
    loaded_incremental_snapshot_=false;
    directory_index_.initialize(ti);
    SnapshotReader in(snapshot);
    if(in.u64()!=kSegmentedSnapshotMagic) throw std::runtime_error("invalid snapshot magic");
    const auto version=in.u64();
    if(!SupportedSnapshotVersion(version)) throw std::runtime_error("unsupported snapshot version: expected 4, 5 or 6");
    // Shared service sessions are immutable. Offline append and all writable
    // formats retain physical identity checks for their undo baselines.
    const bool portable_readonly=version==kSharedSnapshotVersion && !append_only;
    if(!persistent_store_.OpenOrCreate(file,true)) throw std::runtime_error("cannot reopen store");
    persistent_store_.LoadAllocation(in,portable_readonly);
    directory_blocks_.bind(persistent_store_);
    auto block=in.u64(), slot=in.u64();
    if(!block || block>kMaxLogicalBlock || slot>=kInodesPerBlock) throw std::runtime_error("invalid snapshot root reference");
    root_persistent_ref_=make_inode_ref(block,slot); root_persistent_ref_valid_=true;
    root_directory_head_block_id_=in.u64(); root_directory_tail_block_id_=in.u64();
    root_directory_file_index_id_=directory_blocks_.load_file_index(root_directory_head_block_id_,in);
    persistence_available_=true;
    if(version==kSegmentedSnapshotVersion) {
        // One-time conversion of an existing v4 store. An empty new store costs
        // only its root. Existing v4 directories become the first immutable index.
        load_directory_records(in,ti); in.finish();
    } else {
        snapshot_indexes_=ReadIndexCatalog(in);
        if(version==kSharedSnapshotVersion) {
            shared_catalog_=ReadSharedCatalog(in,snapshot_indexes_); shared_snapshot_=true;
        }
        in.finish(); ValidateIndexFiles(file,snapshot_indexes_,!portable_readonly);
        if(shared_snapshot_) {
            validate_shared_instances();
            if(!append_only) {
                shared_routes_.resize(shared_catalog_.templates.size());
                for(size_t i=0;i<shared_routes_.size();++i)
                    shared_routes_[i]=SharedRouteCache::Instance().Acquire(
                        IndexPath(file,snapshot_indexes_.files[i].token),
                        snapshot_indexes_.files[i],shared_catalog_.templates[i]);
            }
        } else if(!append_only) {
            for(const auto& f:snapshot_indexes_.files) {
                SnapshotReader index(IndexPath(file,f.token));
                if(index.u64()!=kIndexFileMagic || index.u64()!=1)
                    throw std::runtime_error("invalid immutable index header");
                load_directory_records(index,ti);
                if(index.checksum()!=f.checksum) throw std::runtime_error("immutable index checksum differs from checkpoint");
                index.finish();
            }
            auto roots=snapshot_root_hashes(), expected=snapshot_indexes_.roots;
            std::sort(roots.begin(),roots.end()); std::sort(expected.begin(),expected.end());
            if(roots!=expected) throw std::runtime_error("immutable index root catalog mismatch");
            loaded_directory_hash_=namespace_index_hash(ti);
            loaded_incremental_snapshot_=true;
        }
    }
    inode_disk root{};
    if(!read_inode_by_id(root_inode_id(),root)) throw std::runtime_error("snapshot root inode missing");
}
