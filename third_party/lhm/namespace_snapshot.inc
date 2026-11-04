// Caller serializes all operations and drains reservations before checkpointing.
void save_snapshot(const std::string& path, threadinfo& ti) {
    if (!pending_creates_.empty() || !sync_persistence())
        throw std::runtime_error("pending reservations or failed data sync");
    struct stat st{};
    if (::stat(persistent_store_.BackingFile().c_str(), &st)) throw std::runtime_error("stat store failed");
    SnapshotWriter out(path);
    out.u64(0x5a424c484d534e32ULL); out.u64(2);
    out.u64(st.st_dev); out.u64(st.st_ino); out.u64(st.st_size);
    persistent_store_.SaveAllocation(out);
    out.u64(root_persistent_ref_.block_id); out.u64(root_persistent_ref_.offset);
    out.u64(root_directory_head_block_id_); out.u64(root_directory_tail_block_id_);
    directory_blocks_.save_file_index(root_directory_file_index_id_, out);
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
    out.u64(UINT64_MAX); out.finish();
}
void reopen_snapshot(const std::string& file, const std::string& snapshot, threadinfo& ti) {
    directory_index_.initialize(ti);
    SnapshotReader in(snapshot);
    if(in.u64()!=0x5a424c484d534e32ULL || in.u64()!=2) throw std::runtime_error("unsupported snapshot version");
    struct stat st{};
    if(::stat(file.c_str(),&st)) throw std::runtime_error("stat store failed");
    auto dev=in.u64(), ino=in.u64(), bytes=in.u64();
    if(dev!=uint64_t(st.st_dev) || ino!=uint64_t(st.st_ino) || bytes!=uint64_t(st.st_size))
        throw std::runtime_error("snapshot/store identity mismatch");
    if(!persistent_store_.OpenOrCreate(file,true)) throw std::runtime_error("cannot reopen store");
    persistent_store_.LoadAllocation(in);
    directory_blocks_.bind(persistent_store_);
    auto block=in.u64(), slot=in.u64();
    if(!block || block>kMaxLogicalBlock || slot>=kInodesPerBlock) throw std::runtime_error("invalid snapshot root reference");
    root_persistent_ref_=make_inode_ref(block,slot); root_persistent_ref_valid_=true;
    root_directory_head_block_id_=in.u64(); root_directory_tail_block_id_=in.u64();
    root_directory_file_index_id_=directory_blocks_.load_file_index(root_directory_head_block_id_,in);
    persistence_available_=true;
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
    in.finish();
    inode_disk root{};
    if(!read_inode_by_id(root_inode_id(),root)) throw std::runtime_error("snapshot root inode missing");
}
