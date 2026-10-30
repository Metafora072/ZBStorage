#ifndef MASSTREELHM_DIRECTORY_BLOCK_STORE_HH
#define MASSTREELHM_DIRECTORY_BLOCK_STORE_HH

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metadata_layout.hh"
#include "namespace_types.hh"
#include "persistent_store.hh"

namespace MasstreeLHM {

class DirectoryBlockStore {
  public:
    enum class dentry_kind : uint8_t {
        invalid = 0,
        directory = 1,
        file = 2,
        self_directory = 3
    };

    struct file_ref_payload {
        uint32_t inode_block_id = 0;
        uint16_t inode_slot = 0;
        uint16_t generation = 0;
    };
    static_assert(sizeof(file_ref_payload) == 8, "file ref payload must be 8B");

    struct directory_ref_payload {
        uint32_t head_block_id = 0;
    };
    static_assert(sizeof(directory_ref_payload) == 4,
                  "directory ref payload must be 4B");

    struct dentry_entry_header {
        uint64_t component_hash = 0;
        uint16_t record_bytes = 0;
        uint8_t kind = static_cast<uint8_t>(dentry_kind::invalid);
        uint8_t name_length = 0;
        uint16_t flags = 0;
        uint16_t ref_bytes = 0;
    };
    static_assert(sizeof(dentry_entry_header) == 16,
                  "dentry entry header must be 16B");

    struct dentry_index_entry {
        uint64_t component_hash = 0;
        uint16_t entry_offset = 0;
        uint16_t flags = 0;
        uint32_t reserved = 0;
    };
    static_assert(sizeof(dentry_index_entry) == 16,
                  "dentry index entry must be 16B");

    struct entry {
        dentry_kind kind = dentry_kind::invalid;
        uint64_t component_hash = 0;
        inode_ref file_ref = make_inode_ref(0, 0);
        uint32_t directory_head_block_id = 0;
        std::string name;
    };

    enum class file_index_state_kind : uint8_t {
        empty = 0,
        single_bucket = 1,
        multi_bucket = 2
    };

    struct bucket_meta {
        uint32_t block_id = 0;
        uint16_t local_depth = 0;
        uint16_t entry_count = 0;
        uint32_t version = 0;
        uint32_t flags = 0;
    };
    static_assert(sizeof(bucket_meta) == 16, "bucket metadata must stay compact");

    struct bucket_directory {
        uint16_t global_depth = 0;
        uint64_t entry_count = 0;
        uint32_t single_bucket = 0;
        std::vector<uint32_t> slots;
        std::vector<bucket_meta> buckets;
    };

    struct file_index_state {
        file_index_state_kind state = file_index_state_kind::empty;
        uint32_t head_block_id = 0;
        bucket_directory directory;
        uint64_t split_count = 0;
        uint64_t last_split_moved = 0;
        uint64_t max_split_moved = 0;
    };

    struct stats_snapshot {
        uint64_t block_reads = 0;
        uint64_t block_writes = 0;
        uint64_t cache_hits = 0;
        uint64_t cache_misses = 0;
        uint64_t cache_flushes = 0;
        uint64_t cache_evictions = 0;
        uint64_t lookups = 0;
        uint64_t inserts = 0;
        uint64_t removes = 0;
        uint64_t file_index_ext_dirs = 0;
        uint64_t file_index_splits = 0;
        uint64_t file_index_last_split_moved = 0;
        uint64_t file_index_max_split_moved = 0;
        uint64_t file_index_bucket_reads = 0;
        uint64_t file_index_max_bucket_reads_per_lookup = 0;
    };

    void bind(PersistentStore& store) {
        store_ = &store;
        stats_ = stats_snapshot{};
        reset_file_index_pool();
        configure_cache_from_env();
    }

    void reset() {
        (void) flush_all();
        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_lru_.clear();
        cache_index_.clear();
        {
            std::lock_guard<std::mutex> index_guard(file_index_mu_);
            file_indexes_.clear();
            file_indexes_.emplace_back();
        }
        store_ = nullptr;
        stats_ = stats_snapshot{};
    }

    stats_snapshot stats() const {
        return stats_;
    }

    void reset_stats() {
        stats_ = stats_snapshot{};
    }

    uint32_t create_file_index(uint32_t head_block_id = 0) {
        std::lock_guard<std::mutex> guard(file_index_mu_);
        if (file_indexes_.empty()) {
            file_indexes_.emplace_back();
        }
        file_indexes_.emplace_back();
        const uint32_t id = static_cast<uint32_t>(file_indexes_.size() - 1);
        file_indexes_[id].head_block_id = head_block_id;
        return id;
    }

    #include "directory_bulk_build.inc"

    bool flush_all() {
        std::lock_guard<std::mutex> guard(cache_mu_);
        for (cache_entry& entry : cache_lru_) {
            if (!flush_cache_entry_locked(entry)) {
                return false;
            }
        }
        return true;
    }

    // Service-only error observation: calls are serialized by the ZB worker.
    void clear_read_failure() { read_failed_ = false; }
    bool read_failed() const { return read_failed_; }
#ifdef ZB_LHM_TESTING
    void fail_next_read_for_test() { fail_next_read_ = true; }
#endif
    bool lookup(uint32_t file_index_id, uint64_t component_hash,
                const std::string& name, entry& out) {
        ++stats_.lookups;
        file_index_state* state = file_index_by_id(file_index_id);
        return state != nullptr && lookup_bucket_directory(*state, component_hash, name, out);
    }

    bool lookup(uint32_t file_index_id, uint64_t component_hash, entry& out) {
        ++stats_.lookups;
        file_index_state* state = file_index_by_id(file_index_id);
        return state != nullptr
            && lookup_bucket_directory(*state, component_hash, std::string(), out);
    }

    bool read_entries(uint32_t file_index_id, std::vector<entry>& out) {
        out.clear();
        file_index_state* state = file_index_by_id(file_index_id);
        return state != nullptr && read_bucket_directory_entries(*state, out);
    }

    // ZB streaming cursor. Only one decoded bucket is retained. Bucket metadata
    // is append-only on split; existing buckets are retained and there is no
    // bucket merge. Iterating metadata therefore visits each live bucket once,
    // unlike the aliasing extendible-hash slots. Structural mutation is excluded
    // by the scan contract. No pointer into file_index_pool_ survives a call.
    struct file_scan_cursor {
        size_t bucket = 0, position = 0;
        std::vector<entry> buffered;
        uint64_t bucket_reads = 0;
    };

    bool scan_file_step(uint32_t file_index_id, file_scan_cursor& cursor,
                        entry& out, bool& found, bool& done) {
        found = done = false;
        if (cursor.position < cursor.buffered.size()) {
            out = cursor.buffered[cursor.position++];
            found = true;
            return true;
        }
        std::vector<entry>().swap(cursor.buffered);
        cursor.position = 0;
        const auto* state = file_index_by_id(file_index_id);
        if (!state) return false;
        if (state->state == file_index_state_kind::empty
            || cursor.bucket == state->directory.buckets.size()) {
            done = true;
            return true;
        }
        if (cursor.bucket > state->directory.buckets.size()) return false;
        const auto block = state->directory.buckets[cursor.bucket++].block_id;
        if (!block) return false; // Metadata currently has no retired buckets.
        directory_block_image image{};
        if (!read_block(block, image)) return false;
        ++cursor.bucket_reads;
        // Decode strictly: malformed live entries must fail, not vanish from a scan.
        if (image.header.used_bytes > sizeof(image.payload)) return false;
        uint32_t offset = 0;
        while (offset < image.header.used_bytes) {
            if (image.header.used_bytes - offset < sizeof(dentry_entry_header)) return false;
            dentry_entry_header header{};
            std::memcpy(&header, image.payload + offset, sizeof(header));
            if (header.record_bytes < sizeof(header)
                || header.record_bytes > image.header.used_bytes - offset) return false;
            if (!(header.flags & kDentryFlagDeleted)) {
                entry item;
                if (!decode_entry_at(image, static_cast<uint16_t>(offset), item)
                    || item.kind != dentry_kind::file) return false;
                cursor.buffered.push_back(std::move(item));
            }
            offset += header.record_bytes;
        }
        return true; // Loading a bucket itself consumes one bounded scan step.
    }

    struct file_index_debug_info {
        bool found = false;
        bool ext_hash = false;
        uint32_t global_depth = 0;
        uint64_t entry_count = 0;
        uint64_t split_count = 0;
        uint64_t last_split_moved = 0;
        uint64_t max_split_moved = 0;
        std::size_t directory_slots = 0;
        std::size_t unique_buckets = 0;
    };

    bool debug_file_index(uint32_t file_index_id, file_index_debug_info& out) {
        out = file_index_debug_info{};
        file_index_state* state = file_index_by_id(file_index_id);
        if (state == nullptr) {
            return false;
        }
        out.found = true;
        out.ext_hash = state->state != file_index_state_kind::empty;
        out.global_depth = state->directory.global_depth;
        out.entry_count = state->directory.entry_count;
        out.split_count = state->split_count;
        out.last_split_moved = state->last_split_moved;
        out.max_split_moved = state->max_split_moved;
        out.directory_slots = state->state == file_index_state_kind::multi_bucket
            ? state->directory.slots.size()
            : (state->state == file_index_state_kind::single_bucket ? 1 : 0);
        std::vector<uint32_t> buckets;
        if (state->state == file_index_state_kind::single_bucket) {
            buckets.push_back(state->directory.single_bucket);
        } else if (state->state == file_index_state_kind::multi_bucket) {
            buckets = state->directory.slots;
        }
        std::sort(buckets.begin(), buckets.end());
        buckets.erase(std::unique(buckets.begin(), buckets.end()), buckets.end());
        out.unique_buckets = buckets.size();
        return true;
    }

    bool lookup_inline(uint32_t head_block_id, uint64_t component_hash,
                       const std::string& name, entry& out) {
        uint32_t block_id = head_block_id;
        while (block_id != 0) {
            directory_block_image image{};
            if (!read_block(block_id, image)) {
                return false;
            }
            dentry_index_entry idx{};
            if (find_entry_in_image(image, component_hash, name, idx, out)) {
                return true;
            }
            block_id = image.header.next_block_id;
        }
        return false;
    }

    bool read_inline_entries(uint32_t head_block_id, std::vector<entry>& out) {
        uint32_t block_id = head_block_id;
        while (block_id != 0) {
            directory_block_image image{};
            if (!read_block(block_id, image)) {
                return false;
            }
            if (!append_entries_from_block(image, out)) {
                return false;
            }
            block_id = image.header.next_block_id;
        }
        return true;
    }

    bool read_directory_inode(uint32_t head_block_id, inode_disk& out) {
        directory_block_image image{};
        if (!read_block(head_block_id, image)) {
            return false;
        }
        if ((image.header.flags & kDirectoryBlockFlagHasEmbeddedInode) == 0) {
            return false;
        }
        out = image.directory_inode;
        return true;
    }

    bool write_directory_inode(uint32_t head_block_id, const inode_disk& inode) {
        directory_block_image image{};
        if (!read_block(head_block_id, image)) {
            return false;
        }
        image.directory_inode = inode;
        image.header.dir_inode_id = inode.inode_id;
        image.header.flags |= kDirectoryBlockFlagHasEmbeddedInode;
        image.common.owner_inode_id = inode.inode_id;
        return write_block(head_block_id, image);
    }

    bool write_directory_self_name(uint32_t head_block_id, uint64_t component_hash,
                                   const std::string& name) {
        if (name.size() > kMaxEntryNameBytes) {
            return false;
        }
        directory_block_image image{};
        if (!read_block(head_block_id, image)) {
            return false;
        }
        if (!upsert_self_name(image, component_hash, name)) {
            return false;
        }
        return write_block(head_block_id, image);
    }

    bool read_directory_self_name(uint32_t head_block_id, std::string& out) {
        directory_block_image image{};
        if (!read_block(head_block_id, image)) {
            return false;
        }
        return decode_self_name(image, out);
    }

    bool insert(uint32_t file_index_id, uint32_t head_block_id, const entry& item,
                bool overwrite_existing) {
        ++stats_.inserts;
        if (store_ == nullptr || !store_->IsOpen() || item.name.empty()
            || item.name.size() > kMaxEntryNameBytes) {
            return false;
        }
        if (file_index_id == 0 || head_block_id == 0) {
            return false;
        }

        file_index_state* state = file_index_by_id(file_index_id);
        if (state == nullptr) {
            return false;
        }
        if (state->head_block_id == 0) {
            state->head_block_id = head_block_id;
        }
        if (overwrite_existing) {
            (void) remove_from_bucket_directory(*state, item.component_hash, item.name);
        }
        return insert_into_bucket_directory(*state, item);
    }

    bool remove(uint32_t file_index_id, uint64_t component_hash, const std::string& name) {
        ++stats_.removes;
        file_index_state* state = file_index_by_id(file_index_id);
        return state != nullptr && remove_from_bucket_directory(*state, component_hash, name);
    }

    bool remove(uint32_t file_index_id, uint64_t component_hash) {
        ++stats_.removes;
        file_index_state* state = file_index_by_id(file_index_id);
        return state != nullptr
            && remove_from_bucket_directory(*state, component_hash, std::string());
    }

    bool remove_inline(uint32_t head_block_id, uint64_t component_hash,
                       const std::string& name) {
        uint32_t block_id = head_block_id;
        while (block_id != 0) {
            directory_block_image image{};
            if (!read_block(block_id, image)) {
                return false;
            }
            if (remove_index(image, component_hash, name)) {
                if (image.header.entry_count > 0) {
                    --image.header.entry_count;
                }
                image.header.base_version += 1;
                if (!write_block(block_id, image)) {
                    return false;
                }
                return true;
            }
            block_id = image.header.next_block_id;
        }
        return false;
    }

    static entry make_file_entry(uint64_t component_hash, inode_ref ref,
                                 const std::string& name) {
        entry out;
        out.kind = dentry_kind::file;
        out.component_hash = component_hash;
        out.file_ref = ref;
        out.name = name;
        return out;
    }

    static entry make_directory_entry(uint64_t component_hash, uint32_t head_block_id,
                                      const std::string& name) {
        entry out;
        out.kind = dentry_kind::directory;
        out.component_hash = component_hash;
        out.directory_head_block_id = head_block_id;
        out.name = name;
        return out;
    }

    static namespace_entry to_namespace_entry(const entry& item) {
        if (item.kind == dentry_kind::directory) {
            return make_namespace_entry(entry_kind::directory,
                                        make_inode_ref(item.directory_head_block_id, 0),
                                        item.name);
        }
        if (item.kind == dentry_kind::file) {
            return make_namespace_entry(entry_kind::file, item.file_ref, item.name);
        }
        return namespace_entry{};
    }

  private:
    static constexpr uint16_t kDentryFlagDeleted = 1u;
    static constexpr uint32_t kEntryAlignment = 8;
    static constexpr std::size_t kDefaultCacheBlocks = 64;
    static constexpr uint64_t kInlineFileIndexEntryThreshold = 128;
    static constexpr uint8_t kMaxExtHashGlobalDepth = 20;
    static constexpr uint32_t kSelfNameRecordBytes =
        ((sizeof(dentry_entry_header) + kMaxEntryNameBytes + kEntryAlignment - 1)
         / kEntryAlignment) * kEntryAlignment;

    struct cache_entry {
        bool valid = false;
        bool dirty = false;
        uint32_t block_id = 0;
        uint64_t last_access = 0;
        directory_block_image image{};
    };
    using cache_list = std::list<cache_entry>;
    using cache_iterator = cache_list::iterator;

    static std::size_t ext_hash_slot(uint64_t component_hash, uint8_t global_depth) {
        if (global_depth == 0) {
            return 0;
        }
        const uint64_t mask = (uint64_t{1} << global_depth) - 1;
        return static_cast<std::size_t>(component_hash & mask);
    }

    void reset_file_index_pool() {
        std::lock_guard<std::mutex> guard(file_index_mu_);
        file_indexes_.clear();
        file_indexes_.emplace_back();
    }

    file_index_state* file_index_by_id(uint32_t file_index_id) {
        if (file_index_id == 0) {
            return nullptr;
        }
        std::lock_guard<std::mutex> guard(file_index_mu_);
        if (file_index_id >= file_indexes_.size()) {
            return nullptr;
        }
        return &file_indexes_[file_index_id];
    }

    bool allocate_bucket_block(uint64_t dir_inode_id, uint32_t& out_block_id) {
        out_block_id = 0;
        if (store_ == nullptr || !store_->IsOpen()) {
            return false;
        }
        inode_ref ref = make_inode_ref(0, 0);
        if (!store_->CreateDirectoryChain(dir_inode_id, ref)) {
            return false;
        }
        out_block_id = ref.block_id;
        return out_block_id != 0;
    }

    static void clear_bucket_image(directory_block_image& image) {
        image.header.entry_count = 0;
        image.header.used_bytes = 0;
        image.header.delta_count = 0;
        image.header.base_version += 1;
        image.header.prev_block_id = 0;
        image.header.next_block_id = 0;
        std::memset(image.payload, 0, sizeof(image.payload));
    }

    bool append_entry_to_block(uint32_t block_id, const entry& item) {
        directory_block_image image{};
        if (!read_block(block_id, image)) {
            return false;
        }
        uint16_t ignored_offset = 0;
        if (!append_entry_to_image(image, item, ignored_offset)) {
            return false;
        }
        return write_block(block_id, image);
    }

    bucket_meta* bucket_by_id(file_index_state& state, uint32_t bucket_id) {
        if (bucket_id >= state.directory.buckets.size()
            || state.directory.buckets[bucket_id].block_id == 0) {
            return nullptr;
        }
        return &state.directory.buckets[bucket_id];
    }

    const bucket_meta* bucket_by_id(const file_index_state& state, uint32_t bucket_id) const {
        if (bucket_id >= state.directory.buckets.size()
            || state.directory.buckets[bucket_id].block_id == 0) {
            return nullptr;
        }
        return &state.directory.buckets[bucket_id];
    }

    bool bucket_for_hash(const file_index_state& state, uint64_t component_hash,
                         uint32_t& bucket_id) const {
        bucket_id = 0;
        if (state.state == file_index_state_kind::empty) {
            return false;
        }
        if (state.state == file_index_state_kind::single_bucket) {
            bucket_id = state.directory.single_bucket;
            return bucket_id < state.directory.buckets.size();
        }
        const std::size_t slot = ext_hash_slot(component_hash, state.directory.global_depth);
        if (slot >= state.directory.slots.size()) {
            return false;
        }
        bucket_id = state.directory.slots[slot];
        return bucket_id < state.directory.buckets.size();
    }

    bool lookup_bucket_directory(file_index_state& state, uint64_t component_hash,
                                 const std::string& name, entry& out) {
        uint32_t bucket_id = 0;
        if (!bucket_for_hash(state, component_hash, bucket_id)) {
            return false;
        }
        const bucket_meta* meta = bucket_by_id(state, bucket_id);
        if (meta == nullptr) {
            return false;
        }
        directory_block_image bucket{};
        if (!read_block(meta->block_id, bucket)) {
            return false;
        }
        ++stats_.file_index_bucket_reads;
        stats_.file_index_max_bucket_reads_per_lookup =
            std::max<uint64_t>(stats_.file_index_max_bucket_reads_per_lookup, 1);
        dentry_index_entry idx{};
        return find_entry_in_image(bucket, component_hash, name, idx, out);
    }

    bool read_bucket_directory_entries(const file_index_state& state, std::vector<entry>& out) {
        if (state.state == file_index_state_kind::empty) {
            return true;
        }
        std::vector<uint32_t> bucket_ids;
        if (state.state == file_index_state_kind::single_bucket) {
            bucket_ids.push_back(state.directory.single_bucket);
        } else {
            bucket_ids = state.directory.slots;
            std::sort(bucket_ids.begin(), bucket_ids.end());
            bucket_ids.erase(std::unique(bucket_ids.begin(), bucket_ids.end()), bucket_ids.end());
        }
        for (uint32_t bucket_id : bucket_ids) {
            const bucket_meta* meta = bucket_by_id(state, bucket_id);
            if (meta == nullptr) {
                continue;
            }
            directory_block_image image{};
            if (!read_block(meta->block_id, image)) {
                return false;
            }
            if (!append_entries_from_block(image, out)) {
                return false;
            }
        }
        return true;
    }

    bool create_initial_bucket(file_index_state& state) {
        if (state.state != file_index_state_kind::empty || state.head_block_id == 0) {
            return false;
        }
        uint32_t bucket_block_id = 0;
        directory_block_image head{};
        if (!read_block(state.head_block_id, head)) {
            return false;
        }
        if (!allocate_bucket_block(head.header.dir_inode_id, bucket_block_id)) {
            return false;
        }
        bucket_meta meta;
        meta.block_id = bucket_block_id;
        meta.local_depth = 0;
        meta.entry_count = 0;
        meta.version = 1;
        state.directory.buckets.clear();
        state.directory.buckets.push_back(meta);
        state.directory.single_bucket = 0;
        state.directory.global_depth = 0;
        state.directory.entry_count = 0;
        state.directory.slots.clear();
        state.state = file_index_state_kind::single_bucket;
        ++stats_.file_index_ext_dirs;
        return true;
    }

    bool insert_into_bucket_directory(file_index_state& state, const entry& item) {
        if (state.state == file_index_state_kind::empty && !create_initial_bucket(state)) {
            return false;
        }
        while (true) {
            uint32_t bucket_id = 0;
            if (!bucket_for_hash(state, item.component_hash, bucket_id)) {
                return false;
            }
            bucket_meta* meta = bucket_by_id(state, bucket_id);
            if (meta == nullptr) {
                return false;
            }
            if (append_entry_to_block(meta->block_id, item)) {
                ++state.directory.entry_count;
                if (meta->entry_count < UINT16_MAX) {
                    ++meta->entry_count;
                }
                ++meta->version;
                return true;
            }
            if (!split_bucket(state, bucket_id)) {
                return false;
            }
        }
    }

    bool remove_from_bucket_directory(file_index_state& state, uint64_t component_hash,
                                      const std::string& name) {
        uint32_t bucket_id = 0;
        if (!bucket_for_hash(state, component_hash, bucket_id)) {
            return false;
        }
        bucket_meta* meta = bucket_by_id(state, bucket_id);
        if (meta == nullptr) {
            return false;
        }
        directory_block_image bucket{};
        if (!read_block(meta->block_id, bucket)) {
            return false;
        }
        if (!remove_index(bucket, component_hash, name)) {
            return false;
        }
        if (bucket.header.entry_count > 0) {
            --bucket.header.entry_count;
        }
        bucket.header.base_version += 1;
        if (!write_block(meta->block_id, bucket)) {
            return false;
        }
        if (state.directory.entry_count > 0) {
            --state.directory.entry_count;
        }
        if (meta->entry_count > 0) {
            --meta->entry_count;
        }
        ++meta->version;
        return true;
    }

    bool split_bucket(file_index_state& state, uint32_t old_bucket_id) {
        bucket_meta* old_meta = bucket_by_id(state, old_bucket_id);
        if (old_meta == nullptr) {
            return false;
        }
        const uint16_t old_depth = old_meta->local_depth;
        if (old_depth >= kMaxExtHashGlobalDepth) {
            return false;
        }

        if (old_depth == state.directory.global_depth) {
            if (state.state == file_index_state_kind::single_bucket) {
                state.directory.slots.assign(2, old_bucket_id);
                state.directory.global_depth = 1;
                state.state = file_index_state_kind::multi_bucket;
            } else {
                const std::size_t old_size = state.directory.slots.size();
                state.directory.slots.resize(old_size * 2);
                for (std::size_t i = 0; i != old_size; ++i) {
                    state.directory.slots[i + old_size] = state.directory.slots[i];
                }
                ++state.directory.global_depth;
            }
        }

        directory_block_image old_image{};
        if (!read_block(old_meta->block_id, old_image)) {
            return false;
        }
        std::vector<entry> old_entries;
        if (!append_entries_from_block(old_image, old_entries)) {
            return false;
        }

        uint32_t new_bucket_block_id = 0;
        if (!allocate_bucket_block(old_image.header.dir_inode_id, new_bucket_block_id)) {
            return false;
        }

        const uint16_t new_depth = static_cast<uint16_t>(old_depth + 1);
        old_meta->local_depth = new_depth;
        old_meta->entry_count = 0;
        ++old_meta->version;

        bucket_meta new_meta;
        new_meta.block_id = new_bucket_block_id;
        new_meta.local_depth = new_depth;
        new_meta.entry_count = 0;
        new_meta.version = 1;
        const uint32_t new_bucket_id = static_cast<uint32_t>(state.directory.buckets.size());
        state.directory.buckets.push_back(new_meta);

        for (std::size_t i = 0; i != state.directory.slots.size(); ++i) {
            if (state.directory.slots[i] != old_bucket_id) {
                continue;
            }
            if (((i >> old_depth) & 1u) != 0) {
                state.directory.slots[i] = new_bucket_id;
            }
        }

        directory_block_image new_image{};
        if (!read_block(new_bucket_block_id, new_image)) {
            return false;
        }
        clear_bucket_image(old_image);
        clear_bucket_image(new_image);

        // Extendible hashing only redistributes the old bucket. Directory
        // doubling above copies bucket pointers only; it never moves entries
        // from unrelated buckets, so split work is bounded by bucket capacity.
        for (const entry& item : old_entries) {
            const std::size_t slot = ext_hash_slot(item.component_hash,
                                                   state.directory.global_depth);
            directory_block_image& target =
                state.directory.slots[slot] == new_bucket_id ? new_image : old_image;
            uint16_t ignored_offset = 0;
            if (!append_entry_to_image(target, item, ignored_offset)) {
                return false;
            }
            bucket_meta& target_meta =
                state.directory.slots[slot] == new_bucket_id
                    ? state.directory.buckets[new_bucket_id]
                    : state.directory.buckets[old_bucket_id];
            if (target_meta.entry_count < UINT16_MAX) {
                ++target_meta.entry_count;
            }
        }

        if (!write_block(state.directory.buckets[old_bucket_id].block_id, old_image)) {
            return false;
        }
        if (!write_block(new_bucket_block_id, new_image)) {
            return false;
        }

        ++state.split_count;
        state.last_split_moved = old_entries.size();
        state.max_split_moved = std::max<uint64_t>(state.max_split_moved,
                                                   state.last_split_moved);
        ++stats_.file_index_splits;
        stats_.file_index_last_split_moved = state.last_split_moved;
        stats_.file_index_max_split_moved =
            std::max(stats_.file_index_max_split_moved, state.max_split_moved);
        return true;
    }

    static uint32_t align_up(uint32_t value, uint32_t alignment) {
        return (value + alignment - 1) & ~(alignment - 1);
    }

    static uint32_t index_count(const directory_block_image& image) {
        return image.header.delta_count;
    }

    static uint32_t index_start(const directory_block_image& image) {
        return kDirectoryBlockPayloadBytes
            - index_count(image) * sizeof(dentry_index_entry);
    }

    static std::vector<dentry_index_entry> load_indexes(const directory_block_image& image) {
        std::vector<dentry_index_entry> indexes;
        const uint32_t count = index_count(image);
        indexes.reserve(count);
        uint32_t offset = kDirectoryBlockPayloadBytes - count * sizeof(dentry_index_entry);
        for (uint32_t i = 0; i != count; ++i) {
            dentry_index_entry idx{};
            std::memcpy(&idx, image.payload + offset, sizeof(idx));
            indexes.push_back(idx);
            offset += sizeof(idx);
        }
        return indexes;
    }

    static bool store_indexes(directory_block_image& image,
                              const std::vector<dentry_index_entry>& indexes) {
        const uint32_t bytes =
            static_cast<uint32_t>(indexes.size() * sizeof(dentry_index_entry));
        if (bytes > kDirectoryBlockPayloadBytes || image.header.used_bytes
            > kDirectoryBlockPayloadBytes - bytes) {
            return false;
        }
        image.header.delta_count = static_cast<uint32_t>(indexes.size());
        uint32_t offset = kDirectoryBlockPayloadBytes - bytes;
        if (bytes != 0) {
            std::memcpy(image.payload + offset, indexes.data(), bytes);
        }
        return true;
    }

    static bool find_index(const directory_block_image& image, uint64_t component_hash,
                           dentry_index_entry& out) {
        std::vector<dentry_index_entry> indexes = load_indexes(image);
        auto it = std::lower_bound(indexes.begin(), indexes.end(), component_hash,
                                   [](const dentry_index_entry& lhs, uint64_t rhs) {
                                       return lhs.component_hash < rhs;
                                   });
        if (it == indexes.end() || it->component_hash != component_hash) {
            return false;
        }
        out = *it;
        return true;
    }

    static uint16_t fingerprint16(uint64_t component_hash) {
        uint64_t x = component_hash;
        x ^= x >> 32;
        x ^= x >> 16;
        return static_cast<uint16_t>(x);
    }

    static bool find_entry_in_image(const directory_block_image& image,
                                    uint64_t component_hash,
                                    const std::string& name,
                                    dentry_index_entry& out_index,
                                    entry& out) {
        std::vector<dentry_index_entry> indexes = load_indexes(image);
        const uint16_t target_fp = fingerprint16(component_hash);
        for (const dentry_index_entry& idx : indexes) {
            if (fingerprint16(idx.component_hash) != target_fp) {
                continue;
            }
            entry candidate;
            if (!decode_entry_at(image, idx.entry_offset, candidate)) {
                continue;
            }
            if (!name.empty() && candidate.name != name) {
                continue;
            }
            if (name.empty() && candidate.component_hash != component_hash) {
                continue;
            }
            out_index = idx;
            out = std::move(candidate);
            return true;
        }
        return false;
    }

    static bool remove_index(directory_block_image& image, uint64_t component_hash,
                             const std::string& name) {
        dentry_index_entry found{};
        entry removed;
        if (!find_entry_in_image(image, component_hash, name, found, removed)) {
            return false;
        }
        dentry_entry_header header{};
        std::memcpy(&header, image.payload + found.entry_offset, sizeof(header));
        header.flags = static_cast<uint16_t>(header.flags | kDentryFlagDeleted);
        std::memcpy(image.payload + found.entry_offset, &header, sizeof(header));
        std::vector<dentry_index_entry> indexes = load_indexes(image);
        auto it = std::find_if(indexes.begin(), indexes.end(),
                               [&](const dentry_index_entry& idx) {
                                   return idx.entry_offset == found.entry_offset;
                               });
        if (it != indexes.end()) {
            indexes.erase(it);
        }
        return store_indexes(image, indexes);
    }

    static bool append_entry_to_image(directory_block_image& image, const entry& item,
                                      uint16_t& entry_offset) {
        const uint32_t ref_bytes =
            item.kind == dentry_kind::directory ? sizeof(directory_ref_payload)
                                                : sizeof(file_ref_payload);
        const uint32_t raw_bytes = sizeof(dentry_entry_header) + ref_bytes
            + static_cast<uint32_t>(item.name.size());
        const uint32_t record_bytes = align_up(raw_bytes, kEntryAlignment);
        const uint32_t needed = record_bytes + sizeof(dentry_index_entry);
        const uint32_t idx_bytes = index_count(image) * sizeof(dentry_index_entry);
        if (image.header.used_bytes > kDirectoryBlockPayloadBytes - idx_bytes
            || needed > kDirectoryBlockPayloadBytes - idx_bytes - image.header.used_bytes) {
            return false;
        }
        if (image.header.used_bytes > UINT16_MAX) {
            return false;
        }

        const uint32_t offset = image.header.used_bytes;
        dentry_entry_header header{};
        header.component_hash = item.component_hash;
        header.record_bytes = static_cast<uint16_t>(record_bytes);
        header.kind = static_cast<uint8_t>(item.kind);
        header.name_length = static_cast<uint8_t>(item.name.size());
        header.flags = 0;
        header.ref_bytes = static_cast<uint16_t>(ref_bytes);
        std::memcpy(image.payload + offset, &header, sizeof(header));

        uint32_t cursor = offset + sizeof(header);
        if (item.kind == dentry_kind::directory) {
            directory_ref_payload ref{};
            ref.head_block_id = item.directory_head_block_id;
            std::memcpy(image.payload + cursor, &ref, sizeof(ref));
            cursor += sizeof(ref);
        } else if (item.kind == dentry_kind::file) {
            file_ref_payload ref{};
            ref.inode_block_id = item.file_ref.block_id;
            ref.inode_slot = static_cast<uint16_t>(item.file_ref.offset);
            ref.generation = 0;
            std::memcpy(image.payload + cursor, &ref, sizeof(ref));
            cursor += sizeof(ref);
        } else {
            return false;
        }
        std::memcpy(image.payload + cursor, item.name.data(), item.name.size());
        cursor += static_cast<uint32_t>(item.name.size());
        if (cursor < offset + record_bytes) {
            std::memset(image.payload + cursor, 0, offset + record_bytes - cursor);
        }

        std::vector<dentry_index_entry> indexes = load_indexes(image);
        dentry_index_entry idx{};
        idx.component_hash = item.component_hash;
        idx.entry_offset = static_cast<uint16_t>(offset);
        auto pos = std::lower_bound(indexes.begin(), indexes.end(), idx.component_hash,
                                    [](const dentry_index_entry& lhs, uint64_t rhs) {
                                        return lhs.component_hash < rhs;
                                    });
        indexes.insert(pos, idx);

        image.header.used_bytes += record_bytes;
        ++image.header.entry_count;
        image.header.base_version += 1;
        entry_offset = static_cast<uint16_t>(offset);
        return store_indexes(image, indexes);
    }

    static bool upsert_self_name(directory_block_image& image, uint64_t component_hash,
                                 const std::string& name) {
        dentry_entry_header existing{};
        if (load_self_name_header(image, existing)) {
            if (sizeof(existing) + name.size() <= existing.record_bytes) {
                existing.component_hash = component_hash;
                existing.name_length = static_cast<uint8_t>(name.size());
                std::memcpy(image.payload, &existing, sizeof(existing));
                uint32_t cursor = sizeof(existing);
                std::memcpy(image.payload + cursor, name.data(), name.size());
                cursor += static_cast<uint32_t>(name.size());
                const uint32_t end = existing.record_bytes;
                if (cursor < end) {
                    std::memset(image.payload + cursor, 0, end - cursor);
                }
                image.header.base_version += 1;
                return true;
            }
            return false;
        }

        const uint32_t record_bytes = kSelfNameRecordBytes;
        const uint32_t idx_bytes = index_count(image) * sizeof(dentry_index_entry);
        if (image.header.used_bytes != 0
            || record_bytes > kDirectoryBlockPayloadBytes - idx_bytes) {
            return false;
        }

        dentry_entry_header header{};
        header.component_hash = component_hash;
        header.record_bytes = static_cast<uint16_t>(record_bytes);
        header.kind = static_cast<uint8_t>(dentry_kind::self_directory);
        header.name_length = static_cast<uint8_t>(name.size());
        header.flags = 0;
        header.ref_bytes = 0;
        std::memcpy(image.payload, &header, sizeof(header));
        uint32_t cursor = sizeof(header);
        std::memcpy(image.payload + cursor, name.data(), name.size());
        cursor += static_cast<uint32_t>(name.size());
        if (cursor < record_bytes) {
            std::memset(image.payload + cursor, 0, record_bytes - cursor);
        }
        image.header.used_bytes += record_bytes;
        image.header.base_version += 1;
        return true;
    }

    static bool load_self_name_header(const directory_block_image& image,
                                      dentry_entry_header& header) {
        if (image.header.used_bytes < sizeof(dentry_entry_header)) {
            return false;
        }
        std::memcpy(&header, image.payload, sizeof(header));
        if (static_cast<dentry_kind>(header.kind) != dentry_kind::self_directory) {
            return false;
        }
        if (header.record_bytes == 0 || header.record_bytes > image.header.used_bytes) {
            return false;
        }
        if (header.ref_bytes != 0 || header.name_length > header.record_bytes
            || sizeof(header) + header.name_length > header.record_bytes) {
            return false;
        }
        return true;
    }

    static bool decode_self_name(const directory_block_image& image, std::string& out) {
        dentry_entry_header header{};
        if (!load_self_name_header(image, header)) {
            return false;
        }
        out.assign(reinterpret_cast<const char*>(image.payload + sizeof(header)),
                   header.name_length);
        return true;
    }

    static bool decode_entry_at(const directory_block_image& image, uint16_t offset,
                                entry& out) {
        if (offset > image.header.used_bytes
            || sizeof(dentry_entry_header) > image.header.used_bytes - offset) {
            return false;
        }
        dentry_entry_header header{};
        std::memcpy(&header, image.payload + offset, sizeof(header));
        if ((header.flags & kDentryFlagDeleted) != 0) {
            return false;
        }
        if (header.record_bytes == 0 || header.record_bytes > image.header.used_bytes - offset) {
            return false;
        }
        dentry_kind kind = static_cast<dentry_kind>(header.kind);
        uint32_t cursor = offset + sizeof(header);
        if (kind == dentry_kind::directory) {
            if (header.ref_bytes != sizeof(directory_ref_payload)) {
                return false;
            }
            directory_ref_payload ref{};
            std::memcpy(&ref, image.payload + cursor, sizeof(ref));
            cursor += sizeof(ref);
            out.directory_head_block_id = ref.head_block_id;
            out.file_ref = make_inode_ref(0, 0);
        } else if (kind == dentry_kind::file) {
            if (header.ref_bytes != sizeof(file_ref_payload)) {
                return false;
            }
            file_ref_payload ref{};
            std::memcpy(&ref, image.payload + cursor, sizeof(ref));
            cursor += sizeof(ref);
            out.file_ref = make_inode_ref(ref.inode_block_id, ref.inode_slot);
            out.directory_head_block_id = 0;
        } else {
            return false;
        }
        if (header.name_length > header.record_bytes
            || cursor + header.name_length > offset + header.record_bytes) {
            return false;
        }
        out.kind = kind;
        out.component_hash = header.component_hash;
        out.name.assign(reinterpret_cast<const char*>(image.payload + cursor),
                        header.name_length);
        return true;
    }

    static bool append_entries_from_block(const directory_block_image& image,
                                          std::vector<entry>& out) {
        uint32_t offset = 0;
        while (offset < image.header.used_bytes) {
            if (sizeof(dentry_entry_header) > image.header.used_bytes - offset) {
                return false;
            }
            dentry_entry_header header{};
            std::memcpy(&header, image.payload + offset, sizeof(header));
            if (header.record_bytes == 0
                || header.record_bytes > image.header.used_bytes - offset) {
                return false;
            }
            entry item;
            if ((header.flags & kDentryFlagDeleted) == 0
                && decode_entry_at(image, static_cast<uint16_t>(offset), item)) {
                out.push_back(std::move(item));
            }
            offset += header.record_bytes;
        }
        return true;
    }

    void configure_cache_from_env() {
        cache_capacity_ = kDefaultCacheBlocks;
        if (const char* env = std::getenv("LHM_DENTRY_CACHE_BLOCKS")) {
            char* end = nullptr;
            unsigned long value = std::strtoul(env, &end, 10);
            if (end != env) {
                cache_capacity_ = static_cast<std::size_t>(value);
            }
        }
        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_lru_.clear();
        cache_index_.clear();
        cache_index_.reserve(cache_capacity_);
        access_clock_ = 0;
    }

    bool flush_cache_entry_locked(cache_entry& entry) {
        if (!entry.valid || !entry.dirty) {
            return true;
        }
        if (store_ == nullptr || !store_->WriteDirectoryBlock(entry.block_id, entry.image)) {
            return false;
        }
        entry.dirty = false;
        ++stats_.block_writes;
        ++stats_.cache_flushes;
        return true;
    }

    cache_iterator find_cache_entry_locked(uint32_t block_id) {
        auto it = cache_index_.find(block_id);
        if (it == cache_index_.end()) {
            return cache_lru_.end();
        }
        return it->second;
    }

    cache_iterator allocate_cache_entry_locked() {
        if (cache_capacity_ == 0) {
            return cache_lru_.end();
        }
        if (cache_lru_.size() >= cache_capacity_) {
            cache_iterator victim = std::prev(cache_lru_.end());
            if (!flush_cache_entry_locked(*victim)) {
                return cache_lru_.end();
            }
            cache_index_.erase(victim->block_id);
            cache_lru_.erase(victim);
            ++stats_.cache_evictions;
        }
        cache_lru_.emplace_front();
        return cache_lru_.begin();
    }

    bool insert_cache_entry_locked(uint32_t block_id, const directory_block_image& image,
                                   bool dirty) {
        cache_iterator entry = find_cache_entry_locked(block_id);
        if (entry == cache_lru_.end()) {
            entry = allocate_cache_entry_locked();
        }
        if (entry == cache_lru_.end()) {
            return false;
        }
        entry->valid = true;
        entry->dirty = dirty;
        entry->block_id = block_id;
        entry->last_access = ++access_clock_;
        entry->image = image;
        cache_index_[block_id] = entry;
        cache_lru_.splice(cache_lru_.begin(), cache_lru_, entry);
        return true;
    }

    bool flush_cached_block(uint32_t block_id) {
        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_iterator entry = find_cache_entry_locked(block_id);
        if (entry == cache_lru_.end()) {
            return true;
        }
        return flush_cache_entry_locked(*entry);
    }

    bool drop_cached_block(uint32_t block_id, bool flush_dirty) {
        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_iterator entry = find_cache_entry_locked(block_id);
        if (entry == cache_lru_.end()) {
            return true;
        }
        if (flush_dirty && !flush_cache_entry_locked(*entry)) {
            return false;
        }
        cache_index_.erase(block_id);
        cache_lru_.erase(entry);
        return true;
    }

    bool read_failed_ = false;
#ifdef ZB_LHM_TESTING
    bool fail_next_read_ = false;
#endif
    bool read_block(uint32_t block_id, directory_block_image& out) {
#ifdef ZB_LHM_TESTING
        if (fail_next_read_) { fail_next_read_ = false; read_failed_ = true; return false; }
#endif
        if (store_ == nullptr) {
            read_failed_ = true; return false;
        }
        if (cache_capacity_ == 0) {
            if (!store_->ReadDirectoryBlock(block_id, out)) {
                read_failed_ = true; return false;
            }
            ++stats_.block_reads;
            return true;
        }

        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_iterator entry = find_cache_entry_locked(block_id);
        if (entry != cache_lru_.end()) {
            entry->last_access = ++access_clock_;
            out = entry->image;
            cache_lru_.splice(cache_lru_.begin(), cache_lru_, entry);
            ++stats_.cache_hits;
            return true;
        }
        ++stats_.cache_misses;
        directory_block_image loaded{};
        if (!store_->ReadDirectoryBlock(block_id, loaded)) {
            read_failed_ = true; return false;
        }
        ++stats_.block_reads;
        if (!insert_cache_entry_locked(block_id, loaded, false)) {
            read_failed_ = true; return false;
        }
        out = loaded;
        return true;
    }

    bool write_block(uint32_t block_id, const directory_block_image& image) {
        if (store_ == nullptr) {
            return false;
        }
        if (cache_capacity_ == 0) {
            if (!store_->WriteDirectoryBlock(block_id, image)) {
                return false;
            }
            ++stats_.block_writes;
            return true;
        }

        std::lock_guard<std::mutex> guard(cache_mu_);
        cache_iterator entry = find_cache_entry_locked(block_id);
        if (entry == cache_lru_.end()) {
            if (!insert_cache_entry_locked(block_id, image, true)) {
                return false;
            }
            return true;
        }
        entry->image = image;
        entry->dirty = true;
        entry->last_access = ++access_clock_;
        cache_lru_.splice(cache_lru_.begin(), cache_lru_, entry);
        return true;
    }

    PersistentStore* store_ = nullptr;
    stats_snapshot stats_{};
    std::size_t cache_capacity_ = kDefaultCacheBlocks;
    uint64_t access_clock_ = 0;
    cache_list cache_lru_{};
    std::unordered_map<uint32_t, cache_iterator> cache_index_{};
    std::mutex cache_mu_;
    std::mutex file_index_mu_;
    std::vector<file_index_state> file_indexes_{};
};

}  // namespace MasstreeLHM

#endif
