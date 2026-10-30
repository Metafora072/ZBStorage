// Included in DirectoryBlockStore's public section. No live index is published
// until all bucket images have been constructed and flushed by the caller.
bool prepare_bulk_file_index(uint32_t head, const std::vector<entry>& entries,
                             file_index_state& plan) {
    plan = {};
    plan.head_block_id = head;
    if (entries.empty()) return true;
    struct partition { uint16_t depth; uint32_t prefix; std::vector<entry> items; };
    std::vector<partition> pending{{0, 0, entries}};
    std::vector<std::pair<uint16_t, uint32_t>> prefixes;
    directory_block_image owner{};
    if (!read_block(head, owner)) return false;
    while (!pending.empty()) {
        auto part = std::move(pending.back()); pending.pop_back();
        directory_block_image image{};
        bool fits = true;
        for (const auto& item : part.items) {
            uint16_t offset = 0;
            if (!append_entry_to_image(image, item, offset)) { fits = false; break; }
        }
        if (!fits) {
            if (part.depth >= kMaxExtHashGlobalDepth) return false;
            partition low{uint16_t(part.depth + 1), part.prefix, {}};
            partition high{uint16_t(part.depth + 1), part.prefix | (1U << part.depth), {}};
            for (auto& item : part.items)
                ((item.component_hash >> part.depth) & 1 ? high.items : low.items).push_back(std::move(item));
            pending.push_back(std::move(high)); pending.push_back(std::move(low));
            continue;
        }
        uint32_t block = 0;
        if (!allocate_bucket_block(owner.header.dir_inode_id, block)) return false;
        directory_block_image target{};
        if (!read_block(block, target)) return false;
        // Preserve the allocator's common header, including physical ownership.
        // The completed header carries entry counts and the sorted offset index.
        const auto common = target.common;
        target = image;
        target.common = common;
        target.header.dir_inode_id = owner.header.dir_inode_id;
        if (!write_block(block, target)) return false;
        bucket_meta meta;
        meta.block_id = block; meta.local_depth = part.depth;
        meta.entry_count = static_cast<uint16_t>(part.items.size()); meta.version = 1;
        plan.directory.buckets.push_back(meta);
        prefixes.emplace_back(part.depth, part.prefix);
        plan.directory.global_depth = std::max(plan.directory.global_depth, part.depth);
    }
    auto& d = plan.directory;
    d.entry_count = entries.size();
    if (d.global_depth == 0) {
        plan.state = file_index_state_kind::single_bucket;
    } else {
        plan.state = file_index_state_kind::multi_bucket;
        d.slots.resize(size_t(1) << d.global_depth);
        for (uint32_t i = 0; i < prefixes.size(); ++i)
            for (uint32_t slot = prefixes[i].second; slot < d.slots.size(); slot += 1U << prefixes[i].first)
                d.slots[slot] = i;
    }
    return true;
}
uint32_t install_bulk_file_index(file_index_state&& plan) {
    std::lock_guard<std::mutex> guard(file_index_mu_);
    file_indexes_.push_back(std::move(plan));
    return static_cast<uint32_t>(file_indexes_.size() - 1);
}
