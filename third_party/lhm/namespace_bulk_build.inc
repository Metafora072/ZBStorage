// Private-to-ZB fresh-store construction contract: validated parent-before-child
// entries, explicit root at index 0. Nothing is exposed until this returns true.
struct bulk_entry {
    std::string path;
    size_t parent = 0;
    inode_disk attributes{};
};
struct bulk_stats { uint64_t files = 0, directories = 0, inode_blocks = 0, buckets = 0; };
bool build_fresh_namespace(std::vector<bulk_entry>& entries, bulk_stats& stats, threadinfo& ti) {
    if (entries.empty() || entries[0].path != "/" || !root_persistent_ref_valid_) return false;
    stats = {};
    std::vector<uint32_t> heads(entries.size(), 0);
    std::vector<std::vector<DirectoryBlockStore::entry>> children(entries.size());
    std::vector<inode_disk> files;
    std::vector<size_t> file_entries;
    heads[0] = root_directory_head_block_id_;
    // Allocate directory heads first, so every file has its final parent ID.
    for (size_t i = 0; i < entries.size(); ++i) {
        auto& e = entries[i];
        if (e.attributes.type != static_cast<uint8_t>(inode_type::directory)) continue;
        ++stats.directories;
        uint32_t tail = 0;
        if (i && !allocate_directory_blocks(heads[i], tail)) return false;
        auto& inode = e.attributes;
        inode.inode_id = i ? packed_directory_inode_id(heads[i]) : root_inode_id();
        inode.primary_ref = inode.aux_ref = make_inode_ref(heads[i], 0);
        inode.generation = 1;
        if (!directory_blocks_.write_directory_inode(heads[i], inode)) return false;
        if (i) {
            auto parsed = PathKey::parse_absolute_path(e.path);
            if (!directory_blocks_.write_directory_self_name(heads[i], parsed.hashes.back(),
                                                               parsed.components.back())) return false;
        } else if (!persistent_store_.WriteInode(root_persistent_ref_, inode)) return false;
    }
    // Group direct files by their parent in first-appearance order. Directory
    // input may be DFS or BFS; inode allocation never performs path lookup.
    std::vector<std::vector<size_t>> by_parent(entries.size());
    for (size_t i = 1; i < entries.size(); ++i)
        if (entries[i].attributes.type == static_cast<uint8_t>(inode_type::file))
            by_parent[entries[i].parent].push_back(i);
    for (const auto& group : by_parent) for (size_t i : group) {
        auto inode = entries[i].attributes;
        inode.parent_inode_id = entries[entries[i].parent].attributes.inode_id;
        inode.generation = 1;
        files.push_back(inode); file_entries.push_back(i);
    }
    if (!persistent_store_.WriteFreshFileInodes(files)) return false;
    stats.files = files.size();
    stats.inode_blocks = (files.size() + 1 + kInodesPerBlock - 1) / kInodesPerBlock;
    for (size_t f = 0; f < files.size(); ++f) {
        auto& e = entries[file_entries[f]];
        e.attributes = files[f];
        auto parsed = PathKey::parse_absolute_path(e.path);
        children[e.parent].push_back(DirectoryBlockStore::make_file_entry(
            parsed.hashes.back(), files[f].primary_ref, parsed.components.back()));
    }
    std::vector<DirectoryBlockStore::file_index_state> plans(entries.size());
    for (size_t i = 0; i < entries.size(); ++i) if (heads[i]) {
        if (!directory_blocks_.prepare_bulk_file_index(heads[i], children[i], plans[i])) return false;
        stats.buckets += plans[i].directory.buckets.size();
    }
    if (!sync_persistence()) return false;
    // Only now publish runtime routing tables. Re-resolve directory roots: a
    // previous Masstree insertion can replace the parent's root node.
    root_directory_file_index_id_ = directory_blocks_.install_bulk_file_index(std::move(plans[0]));
    for (size_t i = 1; i < entries.size(); ++i) if (heads[i]) {
        const node_type* parent = nullptr;
        if (!locate_directory_root(PathKey::parse_absolute_path(entries[entries[i].parent].path), parent, ti))
            return false;
        auto parsed = PathKey::parse_absolute_path(entries[i].path);
        auto meta = make_directory_meta(heads[i], heads[i]);
        meta.file_index_id = directory_blocks_.install_bulk_file_index(std::move(plans[i]));
        node_type* child = nullptr;
        if (!directory_index_.create_directory_layer(const_cast<node_type*>(parent),
                parsed.hashes.back(), meta, child, ti)) return false;
    }
    return true;
}
