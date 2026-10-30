#ifndef MASSTREELHM_LHM_NAMESPACE_HH
#define MASSTREELHM_LHM_NAMESPACE_HH

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <inttypes.h>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "kvthread.hh"
#include "directory_block_store.hh"
#include "directory_meta.hh"
#include "masstree.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_directory_index.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "namespace_path.hh"
#include "namespace_types.hh"
#include "path_key.hh"
#include "persistent_store.hh"

#include <sys/stat.h>

namespace MasstreeLHM {

#if defined(__GNUC__) || defined(__clang__)
#define LHM_NAMESPACE_ALWAYS_INLINE inline __attribute__((always_inline))
#else
#define LHM_NAMESPACE_ALWAYS_INLINE inline
#endif

// LhmNamespace 是第二阶段的薄包装层：
// 负责把“路径字符串 -> PathKey -> Masstree 操作”这条链路收敛成
// 更接近文件系统语义的接口。
class LhmNamespace {
    struct PendingCreate {
        std::string path;
        uint64_t id = 0, parent_id = 0;
        inode_ref ref{};
        bool directory = false;
        uint32_t file_index_id = 0;
    };
    std::map<uint64_t, PendingCreate> pending_creates_;
    std::vector<PendingCreate> canceled_files_, canceled_directories_;
    uint64_t next_reservation_ = 1;
#ifdef ZB_LHM_TESTING
    bool fail_next_publish_ = false;
#endif
  public:
    using value_type = MasstreeDirectoryIndex::value_type;

    #include "namespace_bulk_build.inc"

    void initialize(threadinfo& ti, const std::string& backing_file = {}) {
        directory_index_.initialize(ti);
        initialize_persistence_store(backing_file);
        directory_blocks_.bind(persistent_store_);
        root_directory_file_index_id_ = directory_blocks_.create_file_index();
        rename_profile_enabled_ = std::getenv("LHM_RENAME_PROFILE") != nullptr;
        reset_rename_profile_stats();
    }

    void destroy(threadinfo& ti) {
        if (persistent_store_.IsOpen()) {
            (void) directory_blocks_.flush_all();
            (void) persistent_store_.Sync();
            persistent_store_.Close();
        }
        root_persistent_ref_ = make_inode_ref(0, 0);
        root_persistent_ref_valid_ = false;
        root_directory_head_block_id_ = 0;
        root_directory_tail_block_id_ = 0;
        root_directory_file_index_id_ = 0;
        pending_creates_.clear();
        canceled_files_.clear();
        canceled_directories_.clear();
        directory_blocks_.reset();
        directory_index_.destroy(ti);
    }

    // Native IDs remain encoded storage addresses. Callers serialize operations.
    uint64_t root_inode_id() const {
        return root_persistent_ref_valid_ ? packed_inode_id(root_persistent_ref_) : 0;
    }

    bool read_inode_by_id(uint64_t id, inode_disk& out, bool* storage_error = nullptr) const {
        if (storage_error) *storage_error = false;
        if (!persistence_available_ || id == 0) return false;
        for (const auto& pending : pending_creates_)
            if (pending.second.id == id) return false;
        bool ok = false;
        uint8_t expected_type = static_cast<uint8_t>(inode_type::file);
        if (id == root_inode_id()) {
            expected_type = static_cast<uint8_t>(inode_type::directory);
            ok = directory_blocks_.read_directory_inode(root_directory_head_block_id_, out);
        } else if (id & (uint64_t{1} << 63)) {
            const uint64_t head = id & ~(uint64_t{1} << 63);
            if (!head || head > 0x7fffffffU || !persistent_store_.DirectoryAddressMayExist(head)) return false;
            expected_type = static_cast<uint8_t>(inode_type::directory);
            ok = directory_blocks_.read_directory_inode(static_cast<uint32_t>(head), out);
        } else {
            const uint32_t block = static_cast<uint32_t>(id >> 32);
            const uint32_t slot = static_cast<uint32_t>(id);
            if (!block || block > 0x7fffffffU || slot >= kInodesPerBlock) return false;
            if (!persistent_store_.InodeAddressMayExist(make_inode_ref(block,slot))) return false;
            ok = persistent_store_.ReadInode(make_inode_ref(block, slot), out);
        }
        if (!ok && storage_error) *storage_error = true;
        return ok && out.inode_id == id && out.type == expected_type &&
               !(out.flags & (kInodeFlagDeleted | kInodeFlagReserved)) && out.link_count != 0;
    }

    // These methods are serialized by the embedding service. Tokens are scoped
    // to this instance; only unpublished slots may return to the cancellation pool.
    bool reserve_create(const std::string& path, bool directory, uint64_t& token,
                        uint64_t& id, threadinfo& ti) {
        token = id = 0;
        if (!persistence_available_ || next_reservation_ == UINT64_MAX) return false;
        const auto parsed = PathKey::parse_absolute_path(path);
        if (parsed.components.empty() || !entry_name_fits(parsed.components.back())) return false;
        value_type existing;
        if (lookup_entry(parsed.normalized_path, existing, ti)) return false;
        for (const auto& item : pending_creates_)
            if (item.second.path == parsed.normalized_path) return false;
        inode_disk parent{};
        if (!lookup_inode(parent_path(parsed.normalized_path), parent, ti) ||
            parent.type != static_cast<uint8_t>(inode_type::directory)) return false;
        PendingCreate pending;
        auto& pool = directory ? canceled_directories_ : canceled_files_;
        if (!pool.empty()) { pending = std::move(pool.back()); pool.pop_back(); }
        else if (directory) {
            uint32_t head = 0, tail = 0;
            if (!allocate_directory_blocks(head, tail)) return false;
            pending.ref = make_inode_ref(head, 0);
            pending.id = packed_directory_inode_id(head);
        } else {
            if (!persistent_store_.AllocateInodeSlot(pending.ref)) return false;
            pending.id = packed_inode_id(pending.ref);
        }
        pending.directory = directory;
        pending.path = parsed.normalized_path;
        pending.parent_id = parent.inode_id;
        inode_disk inode{};
        inode.inode_id = pending.id;
        inode.type = static_cast<uint8_t>(directory ? inode_type::directory : inode_type::file);
        inode.flags = kInodeFlagReserved;
        inode.primary_ref = pending.ref;
        if (!write_pending_inode(pending, inode)) { pool.push_back(pending); return false; }
        token = next_reservation_++;
        id = pending.id;
        pending_creates_.emplace(token, std::move(pending));
        return true;
    }

    bool cancel_create(uint64_t token) {
        auto it = pending_creates_.find(token);
        if (it == pending_creates_.end()) return false;
        inode_disk inode{};
        inode.inode_id = it->second.id;
        inode.type = static_cast<uint8_t>(it->second.directory ? inode_type::directory : inode_type::file);
        inode.flags = kInodeFlagReserved;
        if (!write_pending_inode(it->second, inode)) return false;
        auto& pool = it->second.directory ? canceled_directories_ : canceled_files_;
        pool.push_back(std::move(it->second));
        pending_creates_.erase(it);
        return true;
    }

#ifdef ZB_LHM_TESTING
    void fail_next_publish_for_test() { fail_next_publish_ = true; }
#endif
    bool commit_create(uint64_t token, const inode_disk& attributes, threadinfo& ti) {
        auto it = pending_creates_.find(token);
        if (it == pending_creates_.end()) return false;
        auto& pending = it->second;
        const auto parsed = PathKey::parse_absolute_path(pending.path);
        inode_disk parent{};
        value_type existing;
        if (attributes.inode_id != pending.id || attributes.link_count == 0 ||
            attributes.type != static_cast<uint8_t>(pending.directory ? inode_type::directory : inode_type::file) ||
            lookup_entry(pending.path, existing, ti) ||
            !lookup_inode(parent_path(pending.path), parent, ti) || parent.inode_id != pending.parent_id)
            return false;
        node_type* parent_root = nullptr;
        inode_ref parent_ref{};
        if (!resolve_parent_creation_context(parsed, parent_root, parent_ref, ti)) return false;
        inode_disk inode = attributes;
        inode.inode_id = pending.id;
        inode.parent_inode_id = pending.directory ? 0 : parent.inode_id;
        inode.primary_ref = pending.ref;
        inode.aux_ref = pending.directory ? pending.ref : make_inode_ref(0, 0);
        inode.flags = 0;
        inode.generation = 1;
        inode.tombstone_epoch = 0;
        inode.checksum = 0;
        if (!write_pending_inode(pending, inode)) return false;
#ifdef ZB_LHM_TESTING
        if (fail_next_publish_) {
            fail_next_publish_ = false;
            return false; // already written, still hidden by the pending reservation
        }
#endif
        bool published = false;
        if (pending.directory) {
            if (pending.file_index_id == 0)
                pending.file_index_id = directory_blocks_.create_file_index(pending.ref.block_id);
            if (pending.file_index_id && directory_blocks_.write_directory_self_name(
                    pending.ref.block_id, parsed.hashes.back(), parsed.components.back())) {
                auto meta = make_directory_meta(pending.ref.block_id, pending.ref.block_id);
                meta.file_index_id = pending.file_index_id;
                node_type* directory_root = nullptr;
                published = directory_index_.create_directory_layer(parent_root, parsed.hashes.back(),
                                                                     meta, directory_root, ti);
            }
        } else {
            published = create_file_on_parent_root(parsed, parent_root, pending.ref, ti);
        }
        if (!published) {
            inode.flags = kInodeFlagReserved;
            inode.link_count = 0;
            (void)write_pending_inode(pending, inode);
            return false;
        }
        pending_creates_.erase(it);
        return true;
    }

    bool replace_inode_attributes(uint64_t id, const inode_disk& attributes) {
        inode_disk current{};
        if (!read_inode_by_id(id, current) || attributes.inode_id != id ||
            attributes.type != current.type || attributes.link_count == 0) return false;
        current.size_bytes = attributes.size_bytes;
        current.ctime_ns = attributes.ctime_ns;
        current.mtime_ns = attributes.mtime_ns;
        current.mode = attributes.mode;
        current.link_count = attributes.link_count;
        current.stable_version = attributes.stable_version;
        std::memcpy(current.reserved, attributes.reserved, sizeof(current.reserved));
        current.checksum = 0;
        if (id == root_inode_id())
            return directory_blocks_.write_directory_inode(root_directory_head_block_id_, current) &&
                   persistent_store_.WriteInode(root_persistent_ref_, current);
        if (id & (uint64_t{1} << 63))
            return directory_blocks_.write_directory_inode(static_cast<uint32_t>(id), current);
        return persistent_store_.WriteInode(make_inode_ref(id >> 32, static_cast<uint32_t>(id)), current);
    }

    size_t pending_create_count() const { return pending_creates_.size(); }
    bool reservation_path(uint64_t token, std::string& path) const {
        auto it = pending_creates_.find(token);
        if (it == pending_creates_.end()) return false;
        path = it->second.path;
        return true;
    }

    bool lookup_entry_checked(const std::string& path, value_type& entry,
                              bool& storage_error, threadinfo& ti) const {
        directory_blocks_.clear_read_failure();
        const bool found = lookup_entry(path, entry, ti);
        storage_error = directory_blocks_.read_failed();
        return found && !storage_error;
    }
#ifdef ZB_LHM_TESTING
    void fail_next_lookup_read_for_test() { directory_blocks_.fail_next_read_for_test(); }
#endif

    // Service lookup must observe dirty inode caches. The older *_direct helper
    // deliberately bypasses them for I/O experiments and is not used here.
    bool lookup_inode(const std::string& path, inode_disk& out, threadinfo& ti) const {
        value_type entry;
        if (!lookup_entry(path, entry, ti)) return false;
        const bool root = PathKey::parse_absolute_path(path).components.empty();
        const uint64_t id = root ? root_inode_id() : entry_is_directory(entry)
            ? packed_directory_inode_id(entry.ref.block_id) : packed_inode_id(entry.ref);
        return read_inode_by_id(id, out);
    }

    // Only mutable attributes cross this API; identity and structural references
    // are retained from the stored inode, preventing callers from relocating it.
    bool update_inode_by_id(uint64_t id, uint64_t size, uint16_t mode,
                            uint64_t ctime_ns, uint64_t mtime_ns) {
        inode_disk inode{};
        if (!read_inode_by_id(id, inode)) return false;
        inode.size_bytes = size;
        inode.mode = mode;
        inode.ctime_ns = ctime_ns;
        inode.mtime_ns = mtime_ns;
        ++inode.stable_version;
        inode.checksum = 0;
        if (id == root_inode_id()) {
            return directory_blocks_.write_directory_inode(root_directory_head_block_id_, inode) &&
                   persistent_store_.WriteInode(root_persistent_ref_, inode);
        }
        if (id & (uint64_t{1} << 63)) {
            return directory_blocks_.write_directory_inode(static_cast<uint32_t>(id), inode);
        }
        return persistent_store_.WriteInode(
            make_inode_ref(static_cast<uint32_t>(id >> 32), static_cast<uint32_t>(id)), inode);
    }

    const std::string& persistence_backing_file() const {
        return persistent_store_.BackingFile();
    }

    PersistentStore::stats_snapshot persistence_stats() const {
        return persistent_store_.Stats();
    }

    void reset_persistence_stats() {
        persistent_store_.ResetStats();
        directory_blocks_.reset_stats();
    }

    DirectoryBlockStore::stats_snapshot directory_block_stats() const {
        return directory_blocks_.stats();
    }

    bool sync_persistence() {
        if (!persistent_store_.IsOpen()) {
            return false;
        }
        if (!directory_blocks_.flush_all()) {
            return false;
        }
        return persistent_store_.Sync();
    }

    // fast 模式用于先打通持久化主链路：
    // - 默认开启；
    // - 弱化 create/delete/rename 的存在性与类型检查；
    // - 仅用于原型阶段压缩流程开销。
    void set_fast_mode(bool enabled) {
        fast_mode_ = enabled;
    }

    bool fast_mode() const {
        return fast_mode_;
    }

    struct rename_profile_snapshot {
        uint64_t calls = 0;
        uint64_t success = 0;
        uint64_t fail = 0;
        uint64_t file_renames = 0;
        uint64_t directory_renames = 0;
        uint64_t parse_ns = 0;
        uint64_t fast_path_ns = 0;
        uint64_t old_lookup_ns = 0;
        uint64_t create_new_ns = 0;
        uint64_t remove_old_ns = 0;
        uint64_t directory_o1_ns = 0;
        uint64_t persist_ns = 0;
        uint64_t total_ns = 0;
    };

    rename_profile_snapshot rename_profile_stats() const {
        return rename_profile_;
    }

    void reset_rename_profile_stats() {
        rename_profile_ = rename_profile_snapshot{};
    }

    // L0 inode 缓冲开关：
    // - true: 启用 L0+L1 两级缓存（更适合冷启动批量写）
    // - false: 只启用 L1（更适合常规稳态测试）
    bool set_l0_cache_enabled(bool enabled) {
        l0_cache_enabled_ = enabled;
        return persistent_store_.set_l0_cache_enabled(enabled);
    }

    bool l0_cache_enabled() const {
        return l0_cache_enabled_;
    }

    // lookup_entry 返回任意类型的命名空间项，根目录 "/" 被视为隐式存在。
    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_entry(const std::string& path,
                                                  value_type& out,
                                                  threadinfo& ti) const {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        return lookup_entry_from_parsed(parsed, out, ti);
    }

    bool lookup_entry_with_inode_block_direct(const std::string& path,
                                              value_type& out,
                                              inode_block_image& inode_block,
                                              inode_disk* inode_out,
                                              threadinfo& ti) const {
        if (!lookup_entry(path, out, ti)) {
            return false;
        }

        if (entry_is_directory(out)) {
            const uint32_t head_block_id =
                path == "/" ? root_directory_head_block_id_ : out.ref.block_id;
            if (head_block_id == 0) {
                return false;
            }
            inode_disk directory_inode{};
            if (!directory_blocks_.read_directory_inode(head_block_id, directory_inode)) {
                return false;
            }
            if (inode_out != nullptr) {
                *inode_out = directory_inode;
            }
            std::memset(&inode_block, 0, sizeof(inode_block));
            return true;
        }

        inode_ref ref = out.ref;
        if (path == "/") {
            if (!root_persistent_ref_valid_) {
                return false;
            }
            ref = root_persistent_ref_;
        }
        if (!persistent_store_.ReadInodeBlockForRefDirect(ref, inode_block)) {
            return false;
        }
        if (inode_out != nullptr) {
            *inode_out = inode_block.slots[ref.offset];
            if (inode_out->inode_id != packed_inode_id(ref)) {
                return false;
            }
        }
        return true;
    }

    // lookup_file 只在目标路径存在且类型为普通文件时返回成功。
    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_file(const std::string& path,
                                                 value_type& out,
                                                 threadinfo& ti) const {
        if (!lookup_entry(path, out, ti)) {
            return false;
        }
        return entry_is_file(out);
    }

    // lookup_directory 只在目标路径存在且类型为目录时返回成功。
    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_directory(const std::string& path,
                                                      value_type& out,
                                                      threadinfo& ti) const {
        if (!lookup_entry(path, out, ti)) {
            return false;
        }
        return entry_is_directory(out);
    }

    // Load the directory's persistent head/self-name block into the bounded
    // dentry cache without scanning any children.  This is an explicit O(1)
    // navigation-metadata warmup used by rename experiments; lookup_entry()
    // alone only resolves the in-memory directory layer.
    bool prewarm_directory_head(const std::string& path, threadinfo& ti) const {
        value_type out{};
        if (!lookup_directory(path, out, ti)) {
            return false;
        }
        const uint32_t head_block_id =
            path == "/" ? root_directory_head_block_id_ : out.ref.block_id;
        if (head_block_id == 0) {
            return false;
        }
        std::string self_name;
        return directory_blocks_.read_directory_self_name(head_block_id, self_name);
    }

    // mkdir（推荐接口）：
    // 调用方只提供路径，inode 引用由命名空间层自动分配。
    bool mkdir(const std::string& path, threadinfo& ti) {
        return mkdir(path, make_inode_ref(0, 0), ti);
    }

    // mkdir 兼容接口：
    // 保留显式 ref 传参，主要用于旧测试/回放场景。
    bool mkdir(const std::string& path, inode_ref ref, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        node_type* parent_root = nullptr;
        inode_ref parent_ref = make_inode_ref(0, 0);
        if (!resolve_parent_creation_context(parsed, parent_root, parent_ref, ti)) {
            return false;
        }

        if (!create_directory_on_parent_root(parsed, parent_root, ref, ti)) {
            return false;
        }
        (void) parent_ref;
        return true;
    }

    // creat_file（推荐接口）：
    // 调用方只提供路径，inode 引用由命名空间层自动分配。
    bool creat_file(const std::string& path, threadinfo& ti) {
        return creat_file(path, make_inode_ref(0, 0), ti);
    }

    // creat_file 兼容接口：
    // 保留显式 ref 传参，主要用于旧测试/回放场景。
    bool creat_file(const std::string& path, inode_ref ref, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        node_type* parent_root = nullptr;
        inode_ref parent_ref = make_inode_ref(0, 0);
        if (!resolve_parent_creation_context(parsed, parent_root, parent_ref, ti)) {
            return false;
        }

        inode_ref persistent_ref = make_inode_ref(0, 0);
        if (!allocate_inode_ref_for_create(ref, persistent_ref)) {
            return false;
        }
        if (!create_file_on_parent_root(parsed, parent_root, persistent_ref, ti)) {
            return false;
        }
        return persist_create_success(parsed, entry_kind::file, persistent_ref, parent_ref);
    }

    // 仅用于哈希碰撞测试：强制将最后一级路径分量替换成指定哈希值，
    // 用于观察“忽略碰撞模式”下的行为。
    bool creat_file_with_forced_last_hash_for_test(const std::string& path,
                                                   uint64_t forced_hash, threadinfo& ti) {
        return creat_file_with_forced_last_hash_for_test(path, make_inode_ref(0, 0),
                                                         forced_hash, ti);
    }

    // 兼容接口：保留显式 ref 注入能力，供旧测试代码继续使用。
    bool creat_file_with_forced_last_hash_for_test(const std::string& path, inode_ref ref,
                                                   uint64_t forced_hash, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        if (parsed.normalized_path == "/" || parsed.components.empty() || parsed.hashes.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }
        parsed.hashes.back() = forced_hash;

        node_type* parent_root = nullptr;
        inode_ref parent_ref = make_inode_ref(0, 0);
        if (!resolve_parent_creation_context(parsed, parent_root, parent_ref, ti)) {
            return false;
        }

        inode_ref persistent_ref = make_inode_ref(0, 0);
        if (!allocate_inode_ref_for_create(ref, persistent_ref)) {
            return false;
        }
        if (!create_file_on_parent_root(parsed, parent_root, persistent_ref, ti)) {
            return false;
        }
        return persist_create_success(parsed, entry_kind::file, persistent_ref, parent_ref);
    }

    bool lookup_file_with_forced_last_hash_for_test(const std::string& path, value_type& out,
                                                    uint64_t forced_hash, threadinfo& ti) const {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        if (parsed.hashes.empty()) {
            return false;
        }
        parsed.hashes.back() = forced_hash;
        if (!lookup_entry_from_parsed(parsed, out, ti)) {
            return false;
        }
        return entry_is_file(out);
    }

    // readdir 只扫描“当前目录对应 layer root”这一层的孩子项。
    // 扫描顺序按 Masstree 键序从左到右；命中目录 edge 时只读取目录元数据，
    // 不下钻到子目录内部。
    std::vector<readdir_record> readdir(const std::string& path, threadinfo& ti) const {
        std::vector<readdir_record> results;
        readdir_checked(path, results, ti);
        return results;
    }
    bool path_reserved(const std::string& path) const {
        for (const auto& item : pending_creates_)
            if (item.second.path == path) return true;
        return false;
    }
    bool readdir_checked(const std::string& path, std::vector<readdir_record>& results, threadinfo& ti) const {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        results.clear();
        const node_type* directory_root = nullptr;

        if (parsed.normalized_path != "/") {
            if (!locate_directory_root(parsed, directory_root, ti)) {
                return false;
            }
            if (!directory_root->has_directory_meta()) {
                return false;
            }
            const directory_meta* meta = directory_root->directory_meta();
            if (meta == nullptr) {
                return false;
            }
        } else {
            directory_root = directory_index_.root();
        }

        directory_index_.append_directory_children(directory_root, parsed.hashes, results);
        for (readdir_record& record : results) {
            if (!entry_is_directory(record.entry)) {
                continue;
            }
            std::string child_name;
            if (directory_blocks_.read_directory_self_name(record.entry.ref.block_id,
                                                           child_name)) {
                record.child_name = child_name;
                record.entry = make_namespace_entry(entry_kind::directory, record.entry.ref,
                                                    child_name);
            } else { results.clear(); return false; }
        }

        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        if (!directory_block_refs_for_root(directory_root, head, tail)) {
            return false;
        }
        if (!file_index_id_for_root(directory_root, file_index_id)) {
            return false;
        }
        std::vector<DirectoryBlockStore::entry> entries;
        if (!directory_blocks_.read_entries(file_index_id, entries)) {
            return false;
        }
        for (const DirectoryBlockStore::entry& item : entries) {
            if (item.kind != DirectoryBlockStore::dentry_kind::file) {
                continue;
            }
            readdir_record record;
            record.full_hashes = parsed.hashes;
            record.full_hashes.push_back(item.component_hash);
            record.child_component_hash = item.component_hash;
            record.child_name = item.name;
            record.entry = DirectoryBlockStore::to_namespace_entry(item);
            results.push_back(std::move(record));
        }
        return true;
    }

    // ZB-only incremental subtree traversal. The stack contains logical handles,
    // never Masstree node pointers. File buffers are released before descending,
    // so memory is one bucket plus O(depth) frames and path hashes.
    struct subtree_scan_frame {
        uint32_t head = 0, file_index_id = 0;
        bool files_done = false, have_child = false;
        uint64_t last_child = 0;
        DirectoryBlockStore::file_scan_cursor files;
    };
    struct subtree_scan_cursor {
        ParsedPath location; // Only hashes are used after initial path resolution.
        std::vector<subtree_scan_frame> stack;
        uint64_t bucket_reads = 0, file_entries = 0, directory_entries = 0, steps = 0;
    };

    bool begin_subtree_scan(const std::string& path, subtree_scan_cursor& cursor,
                            threadinfo& ti) const {
        cursor = {};
        cursor.location = PathKey::parse_absolute_path(path);
        const node_type* root = nullptr;
        if (!locate_directory_root(cursor.location, root, ti)) return false;
        subtree_scan_frame frame;
        uint32_t tail = 0;
        if (!directory_block_refs_for_root(root, frame.head, tail)
            || !file_index_id_for_root(root, frame.file_index_id)) return false;
        cursor.stack.push_back(std::move(frame));
        // Prevent the root special-case from hiding hashes when descending '/'.
        cursor.location.normalized_path = "<scan>";
        cursor.location.components.clear();
        return true;
    }

    bool subtree_scan_step(subtree_scan_cursor& cursor, inode_disk& inode,
                            bool& found, threadinfo& ti) const {
        found = false;
        if (cursor.stack.empty()) return true;
        ++cursor.steps;
        auto& frame = cursor.stack.back();
        if (!frame.files_done) {
            DirectoryBlockStore::entry item;
            bool done = false, entry_found = false;
            const auto before = frame.files.bucket_reads;
            if (!directory_blocks_.scan_file_step(frame.file_index_id, frame.files,
                                                   item, entry_found, done)) return false;
            cursor.bucket_reads += frame.files.bucket_reads - before;
            frame.files_done = done;
            if (!entry_found) return true;
            const uint64_t id = (uint64_t(item.file_ref.block_id) << 32) | item.file_ref.offset;
            if (!read_inode_by_id(id, inode) || inode.type != 1) return false;
            ++cursor.file_entries;
            found = true;
            return true;
        }
        const node_type* root = nullptr;
        uint32_t head = 0, tail = 0;
        if (!locate_directory_root(cursor.location, root, ti)
            || !directory_block_refs_for_root(root, head, tail) || head != frame.head) return false;
        uint64_t child_hash = 0;
        value_type child{};
        bool child_found = false;
        if (!directory_index_.next_child_slot(root, frame.have_child, frame.last_child,
                                              child_hash, child, child_found)) return false;
        if (!child_found) {
            cursor.stack.pop_back();
            if (!cursor.stack.empty()) cursor.location.hashes.pop_back();
            return true;
        }
        frame.have_child = true;
        frame.last_child = child_hash;
        // The block-indexed namespace puts files only in the hash buckets.
        if (!entry_is_directory(child)) return false;
        const uint64_t id = (uint64_t{1} << 63) | child.ref.block_id;
        if (!read_inode_by_id(id, inode) || inode.type != 2) return false;
        cursor.location.hashes.push_back(child_hash);
        const node_type* child_root = nullptr;
        subtree_scan_frame next;
        if (!locate_directory_root(cursor.location, child_root, ti)
            || !directory_block_refs_for_root(child_root, next.head, tail)
            || next.head != child.ref.block_id
            || !file_index_id_for_root(child_root, next.file_index_id)) return false;
        cursor.stack.push_back(std::move(next));
        ++cursor.directory_entries;
        found = true;
        return true;
    }

    // 当前 rename 是单表原型上的“可工作语义实现”，不是最终 O(1) 版本。
    // 它通过扫描源子树、重建目标路径并逐项重插入/删除来完成：
    // - 普通文件 rename：移动一个 entry
    // - 目录 rename：递归移动整棵子树
    // 后续真正的 O(1) 目录 rename 仍需要依赖 Masstree 内核中的子树入口切换。
    bool rename_path(const std::string& old_path, const std::string& new_path, threadinfo& ti) {
        const uint64_t total_begin = rename_profile_enabled_ ? now_ns() : 0;
        const uint64_t parse_begin = rename_profile_enabled_ ? now_ns() : 0;
        ParsedPath old_parsed = PathKey::parse_absolute_path(old_path);
        ParsedPath new_parsed = PathKey::parse_absolute_path(new_path);
        if (rename_profile_enabled_) {
            ++rename_profile_.calls;
            rename_profile_.parse_ns += now_ns() - parse_begin;
        }

        if (fast_mode_) {
            value_type moved_entry;
            const uint64_t fast_begin = rename_profile_enabled_ ? now_ns() : 0;
            bool ok = rename_path_fast_from_parsed(old_parsed, new_parsed, ti, &moved_entry);
            if (rename_profile_enabled_) {
                rename_profile_.fast_path_ns += now_ns() - fast_begin;
            }
            if (!ok) {
                if (rename_profile_enabled_) {
                    ++rename_profile_.fail;
                    rename_profile_.total_ns += now_ns() - total_begin;
                }
                return false;
            }
            const uint64_t persist_begin = rename_profile_enabled_ ? now_ns() : 0;
            ok = persist_rename_success(old_parsed, new_parsed, moved_entry, ti);
            if (rename_profile_enabled_) {
                rename_profile_.persist_ns += now_ns() - persist_begin;
                ok ? ++rename_profile_.success : ++rename_profile_.fail;
                rename_profile_.total_ns += now_ns() - total_begin;
            }
            return ok;
        }

        if (old_parsed.normalized_path == "/" || new_parsed.normalized_path == "/") {
            return false;
        }
        if (old_parsed.normalized_path == new_parsed.normalized_path) {
            return true;
        }
        if (is_prefix_path(old_parsed.normalized_path, new_parsed.normalized_path)) {
            return false;
        }

        value_type source_entry;
        if (!lookup_entry_from_parsed(old_parsed, source_entry, ti)) {
            return false;
        }

        if (entry_is_directory(source_entry)) {
            if (!rename_directory_o1_from_parsed(old_parsed, new_parsed, ti)) {
                return false;
            }
            return persist_rename_success(old_parsed, new_parsed, source_entry, ti);
        }

        value_type target_entry;
        if (lookup_entry_from_parsed(new_parsed, target_entry, ti)) {
            return false;
        }

        value_type new_parent;
        if (!lookup_directory(parent_path(new_parsed.normalized_path), new_parent, ti)) {
            return false;
        }

        std::vector<subtree_record> subtree = scan_subtree(old_parsed, ti);
        if (subtree.empty()) {
            return false;
        }

        std::map<std::string, namespace_entry> source_path_map;
        for (const subtree_record& record : subtree) {
            source_path_map.emplace(hash_path_key(record.full_hashes), record.entry);
        }

        std::vector<rename_plan_item> plan;
        plan.reserve(subtree.size());
        for (const subtree_record& record : subtree) {
            std::string source_path = reconstruct_path_under_root(
                old_parsed.normalized_path, old_parsed.hashes, record.full_hashes, source_path_map);
            if (source_path.empty()) {
                return false;
            }

            std::string destination_path = rewrite_destination_path(
                source_path, old_parsed.normalized_path, new_parsed.normalized_path);
            if (destination_path.empty()) {
                return false;
            }

            ParsedPath destination_parsed = PathKey::parse_absolute_path(destination_path);
            value_type existing;
            if (lookup_entry_from_parsed(destination_parsed, existing, ti)) {
                return false;
            }

            rename_plan_item item;
            item.source_path = std::move(source_path);
            item.destination_path = std::move(destination_path);
            item.entry = record.entry;
            item.depth = destination_parsed.hashes.size();
            plan.push_back(std::move(item));
        }

        std::sort(plan.begin(), plan.end(),
                  [](const rename_plan_item& a, const rename_plan_item& b) {
                      if (a.depth != b.depth) {
                          return a.depth < b.depth;
                      }
                      return a.destination_path < b.destination_path;
                  });

        for (const rename_plan_item& item : plan) {
            if (!create_entry(item.destination_path, item.entry.kind, item.entry.ref, ti)) {
                return false;
            }
        }

        std::sort(plan.begin(), plan.end(),
                  [](const rename_plan_item& a, const rename_plan_item& b) {
                      if (a.depth != b.depth) {
                          return a.depth > b.depth;
                      }
                      return a.source_path > b.source_path;
                  });

        for (const rename_plan_item& item : plan) {
            if (!remove_entry(item.source_path, ti)) {
                return false;
            }
        }

        return persist_rename_success(old_parsed, new_parsed, source_entry, ti);
    }

    // 测试辅助：最小删除接口，复用当前命名空间层内部的 remove_entry。
    bool remove_path_for_test(const std::string& path, threadinfo& ti) {
        return remove_path(path, ti);
    }

    // Caller validates root/type/emptiness on the serialized bridge worker.
    bool remove_path(const std::string& path, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        value_type removed{};
        if (!remove_entry_from_parsed(parsed, &removed, ti)) {
            return false;
        }
        return persist_delete_success(removed, ti);
    }

    // 测试辅助：读取某个目录当前 root 的结构形态，
    // 用来验证 gc_layer() 是否真的把 root internode 收缩到了 child root。
    bool debug_directory_root_info(const std::string& path,
                                   directory_root_debug_info& out,
                                   threadinfo& ti) const {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        const node_type* root = nullptr;
        if (parsed.normalized_path == "/") {
            root = directory_index_.root();
        } else if (!locate_directory_root(parsed, root, ti)) {
            out = directory_root_debug_info{false, false, false, 0, 0, false,
                                            false, false, -1, -1};
            return false;
        }

        out.found = true;
        out.is_leaf = root->isleaf();
        out.has_meta = root->has_directory_meta();
        out.child0_is_leaf = false;
        out.child1_exists = false;
        out.child1_is_leaf = false;
        out.child0_size = -1;
        out.child1_size = -1;
        if (root->isleaf()) {
            const typename node_type::leaf_type* lf =
                static_cast<const typename node_type::leaf_type*>(root);
            out.height = 0;
            out.size = lf->size();
        } else {
            const typename node_type::internode_type* in =
                static_cast<const typename node_type::internode_type*>(root);
            out.height = in->height_;
            out.size = in->size();
            out.child0_is_leaf = in->child_[0] ? in->child_[0]->isleaf() : false;
            if (in->child_[0]) {
                out.child0_size = in->child_[0]->isleaf()
                    ? static_cast<const typename node_type::leaf_type*>(in->child_[0])->size()
                    : static_cast<const typename node_type::internode_type*>(in->child_[0])->size();
            }
            if (in->nkeys_ > 0 && in->child_[1]) {
                out.child1_exists = true;
                out.child1_is_leaf = in->child_[1]->isleaf();
                out.child1_size = in->child_[1]->isleaf()
                    ? static_cast<const typename node_type::leaf_type*>(in->child_[1])->size()
                    : static_cast<const typename node_type::internode_type*>(in->child_[1])->size();
            }
        }
        return true;
    }

    bool debug_file_index_info(const std::string& path,
                               DirectoryBlockStore::file_index_debug_info& out,
                               threadinfo& ti) const {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        const node_type* root = nullptr;
        if (parsed.normalized_path == "/") {
            root = directory_index_.root();
        } else if (!locate_directory_root(parsed, root, ti)) {
            out = DirectoryBlockStore::file_index_debug_info{};
            return false;
        }
        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        if (!directory_block_refs_for_root(root, head, tail)) {
            out = DirectoryBlockStore::file_index_debug_info{};
            return false;
        }
        if (!file_index_id_for_root(root, file_index_id)) {
            out = DirectoryBlockStore::file_index_debug_info{};
            return false;
        }
        return directory_blocks_.debug_file_index(file_index_id, out);
    }

  private:
    using node_type = MasstreeDirectoryIndex::node_type;

    struct rename_plan_item {
        std::string source_path;
        std::string destination_path;
        namespace_entry entry;
        size_t depth;
    };

    using directory_edge_lookup = MasstreeDirectoryIndex::directory_edge_lookup;
    using child_slot_lookup = MasstreeDirectoryIndex::child_slot_lookup;

    MasstreeDirectoryIndex directory_index_;
    bool fast_mode_ = true;
    bool l0_cache_enabled_ = true;
    bool rename_profile_enabled_ = false;
    rename_profile_snapshot rename_profile_;
    PersistentStore persistent_store_;
    mutable DirectoryBlockStore directory_blocks_;
    bool persistence_available_ = false;
    std::atomic<uint64_t> transient_inode_cursor_{1};
    inode_ref root_persistent_ref_ = make_inode_ref(0, 0);
    bool root_persistent_ref_valid_ = false;
    uint32_t root_directory_head_block_id_ = 0;
    uint32_t root_directory_tail_block_id_ = 0;
    uint32_t root_directory_file_index_id_ = 0;

    using single_component_key = MasstreeDirectoryIndex::single_component_key;

    static single_component_key make_single_component_key(uint64_t component_hash) {
        return MasstreeDirectoryIndex::make_single_component_key(component_hash);
    }

    static node_type* canonicalize_directory_root(node_type* root) {
        return MasstreeDirectoryIndex::canonicalize_directory_root(root);
    }

    static const node_type* canonicalize_directory_root(const node_type* root) {
        return MasstreeDirectoryIndex::canonicalize_directory_root(root);
    }

    static std::string parent_path(const std::string& normalized_path) {
        return NamespacePath::parent_path(normalized_path);
    }

    static bool is_prefix_path(const std::string& prefix, const std::string& path) {
        return NamespacePath::is_prefix_path(prefix, path);
    }

    static std::string hash_path_key(const std::vector<uint64_t>& hashes) {
        std::string key;
        key.reserve(hashes.size() * sizeof(uint64_t));
        for (uint64_t h : hashes) {
            key.append(reinterpret_cast<const char*>(&h), sizeof(h));
        }
        return key;
    }

    static std::string rewrite_destination_path(const std::string& source_path,
                                                const std::string& source_root,
                                                const std::string& destination_root) {
        if (source_path == source_root) {
            return destination_root;
        }
        if (!is_prefix_path(source_root, source_path)) {
            return std::string();
        }
        return destination_root + source_path.substr(source_root.size());
    }

    static std::string reconstruct_path_under_root(
        const std::string& root_path,
        const std::vector<uint64_t>& root_hashes,
        const std::vector<uint64_t>& full_hashes,
        const std::map<std::string, namespace_entry>& source_path_map) {
        if (full_hashes.size() < root_hashes.size()) {
            return std::string();
        }
        for (size_t i = 0; i < root_hashes.size(); ++i) {
            if (full_hashes[i] != root_hashes[i]) {
                return std::string();
            }
        }

        std::string path = root_path;
        for (size_t i = root_hashes.size(); i < full_hashes.size(); ++i) {
            std::vector<uint64_t> prefix_hashes(full_hashes.begin(), full_hashes.begin() + i + 1);
            auto it = source_path_map.find(hash_path_key(prefix_hashes));
            if (it == source_path_map.end()) {
                return std::string();
            }
            path.push_back('/');
            path += entry_name(it->second);
        }
        return path;
    }

    static constexpr const char* kPersistenceDir = "/mnt/batchtest/lhm";
    static constexpr const char* kPersistenceFile = "/mnt/batchtest/lhm/lhm_namespace.meta";
    static constexpr uint8_t kInodeFlagDeleted = 0x1;
    static constexpr uint8_t kInodeFlagReserved = 0x2;

    static uint64_t now_ns() {
        using namespace std::chrono;
        return static_cast<uint64_t>(
            duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
    }

    static uint64_t packed_inode_id(const inode_ref& ref) {
        return (static_cast<uint64_t>(ref.block_id) << 32) | static_cast<uint64_t>(ref.offset);
    }

    static uint64_t packed_directory_inode_id(uint32_t head_block_id) {
        return (uint64_t{1} << 63) | static_cast<uint64_t>(head_block_id);
    }

    static bool is_zero_inode_ref(const inode_ref& ref) {
        return ref.block_id == 0 && ref.offset == 0;
    }

    static inode_disk make_embedded_directory_inode(uint64_t inode_id,
                                                    uint32_t head_block_id) {
        inode_disk inode{};
        const uint64_t ts = now_ns();
        inode.inode_id = inode_id;
        inode.parent_inode_id = 0;
        inode.primary_ref = make_inode_ref(head_block_id, 0);
        inode.aux_ref = make_inode_ref(head_block_id, 0);
        inode.ctime_ns = ts;
        inode.mtime_ns = ts;
        inode.tombstone_epoch = 0;
        inode.stable_version = 1;
        inode.generation = 1;
        inode.link_count = 1;
        inode.mode = 0755;
        inode.type = static_cast<uint8_t>(inode_type::directory);
        inode.flags = 0;
        inode.checksum = 0;
        return inode;
    }

    bool write_pending_inode(const PendingCreate& pending, const inode_disk& inode) {
        return pending.directory ? directory_blocks_.write_directory_inode(pending.ref.block_id, inode)
                                 : persistent_store_.WriteInode(pending.ref, inode);
    }

    bool allocate_inode_ref_for_create(const inode_ref& requested_ref, inode_ref& out_ref) {
        if (persistence_available_ && persistent_store_.IsOpen()) {
            return persistent_store_.AllocateInodeSlot(out_ref);
        }
        if (!is_zero_inode_ref(requested_ref)) {
            out_ref = requested_ref;
            return true;
        }

        uint64_t ticket = transient_inode_cursor_.fetch_add(1, std::memory_order_relaxed);
        if (ticket == 0) {
            ticket = transient_inode_cursor_.fetch_add(1, std::memory_order_relaxed);
        }
        out_ref.block_id = static_cast<uint32_t>(ticket >> 32);
        out_ref.offset = static_cast<uint32_t>(ticket & 0xffffffffULL);
        return true;
    }

    void initialize_persistence_store(const std::string& explicit_file = {}) {
        persistence_available_ = false;
        transient_inode_cursor_.store(1, std::memory_order_relaxed);
        root_persistent_ref_ = make_inode_ref(0, 0);
        root_persistent_ref_valid_ = false;

        if (const char* env = std::getenv("LHM_ENABLE_L0_CACHE")) {
            if (env[0] == '0') {
                l0_cache_enabled_ = false;
            } else if (env[0] == '1') {
                l0_cache_enabled_ = true;
            }
        }

        std::string persistence_dir = kPersistenceDir;
        if (const char* env_dir = std::getenv("LHM_PERSISTENCE_DIR")) {
            if (env_dir[0] != '\0') {
                persistence_dir = env_dir;
            }
        }

        std::string persistence_file = persistence_dir + "/lhm_namespace.meta";
        if (const char* env_file = std::getenv("LHM_PERSISTENCE_FILE")) {
            if (env_file[0] != '\0') {
                persistence_file = env_file;
            }
        }

        if (!explicit_file.empty()) persistence_file = explicit_file;
        if (explicit_file.empty() && ::mkdir(persistence_dir.c_str(), 0755) != 0 && errno != EEXIST) {
            return;
        }
        if (!persistent_store_.OpenOrCreate(persistence_file)) {
            return;
        }
        if (!persistent_store_.set_l0_cache_enabled(l0_cache_enabled_)) {
            persistent_store_.Close();
            return;
        }
        persistence_available_ = ensure_root_inode_persisted();
    }

    bool ensure_root_inode_persisted() {
        if (root_persistent_ref_valid_) {
            return true;
        }

        inode_ref root_slot;
        if (!persistent_store_.AllocateInodeSlot(root_slot)) {
            return false;
        }

        inode_disk root{};
        root.inode_id = packed_inode_id(root_slot);
        root.parent_inode_id = 0;
        inode_ref root_dir_head = make_inode_ref(0, 0);
        if (!persistent_store_.CreateDirectoryChain(root.inode_id, root_dir_head)) {
            return false;
        }
        root.primary_ref = root_dir_head;
        root.aux_ref = root_dir_head;
        root_directory_head_block_id_ = root_dir_head.block_id;
        root_directory_tail_block_id_ = root_dir_head.block_id;
        root.ctime_ns = now_ns();
        root.mtime_ns = root.ctime_ns;
        root.tombstone_epoch = 0;
        root.stable_version = 1;
        root.generation = 1;
        root.link_count = 1;
        root.mode = 0755;
        root.type = static_cast<uint8_t>(inode_type::directory);
        root.flags = 0;
        root.checksum = 0;
        if (!persistent_store_.WriteDirectoryInode(root_dir_head.block_id, root)) {
            return false;
        }
        if (!persistent_store_.WriteInode(root_slot, root)) {
            return false;
        }

        root_persistent_ref_ = root_slot;
        root_persistent_ref_valid_ = true;
        return true;
    }

    bool resolve_directory_persistent_ref(const std::string& normalized_path,
                                          inode_ref& out,
                                          threadinfo& ti) const {
        if (normalized_path == "/") {
            if (!root_persistent_ref_valid_) {
                return false;
            }
            out = root_persistent_ref_;
            return true;
        }
        value_type dir_entry;
        if (!lookup_directory(normalized_path, dir_entry, ti)) {
            return false;
        }
        out = dir_entry.ref;
        return true;
    }

    bool resolve_parent_persistent_ref(const ParsedPath& parsed,
                                       inode_ref& out,
                                       threadinfo& ti) const {
        return resolve_directory_persistent_ref(parent_path(parsed.normalized_path), out, ti);
    }

    bool allocate_directory_blocks(uint32_t& head_block_id, uint32_t& tail_block_id) {
        head_block_id = 0;
        tail_block_id = 0;
        if (!persistence_available_ || !persistent_store_.IsOpen()) {
            return false;
        }
        inode_ref head_ref = make_inode_ref(0, 0);
        if (!persistent_store_.CreateDirectoryChain(0, head_ref)) {
            return false;
        }
        head_block_id = head_ref.block_id;
        tail_block_id = head_ref.block_id;
        return true;
    }

    bool directory_block_refs_for_root(node_type* root, uint32_t& head_block_id,
                                       uint32_t& tail_block_id,
                                       directory_meta** meta_out = nullptr) {
        if (meta_out != nullptr) {
            *meta_out = nullptr;
        }
        if (root == directory_index_.root()) {
            head_block_id = root_directory_head_block_id_;
            tail_block_id = root_directory_tail_block_id_;
            return head_block_id != 0 && tail_block_id != 0;
        }
        if (root == nullptr || !root->has_directory_meta()) {
            return false;
        }
        directory_meta* meta = root->directory_meta();
        if (meta == nullptr || meta->head_block_id == 0 || meta->tail_block_id == 0) {
            return false;
        }
        head_block_id = meta->head_block_id;
        tail_block_id = meta->tail_block_id;
        if (meta_out != nullptr) {
            *meta_out = meta;
        }
        return true;
    }

    bool directory_block_refs_for_root(const node_type* root, uint32_t& head_block_id,
                                       uint32_t& tail_block_id) const {
        if (root == directory_index_.root()) {
            head_block_id = root_directory_head_block_id_;
            tail_block_id = root_directory_tail_block_id_;
            return head_block_id != 0 && tail_block_id != 0;
        }
        if (root == nullptr || !root->has_directory_meta()) {
            return false;
        }
        const directory_meta* meta = root->directory_meta();
        if (meta == nullptr || meta->head_block_id == 0 || meta->tail_block_id == 0) {
            return false;
        }
        head_block_id = meta->head_block_id;
        tail_block_id = meta->tail_block_id;
        return true;
    }

    bool file_index_id_for_root(node_type* root, uint32_t& file_index_id) {
        file_index_id = 0;
        if (root == directory_index_.root()) {
            file_index_id = root_directory_file_index_id_;
            return file_index_id != 0;
        }
        if (root == nullptr || !root->has_directory_meta()) {
            return false;
        }
        directory_meta* meta = root->directory_meta();
        if (meta == nullptr || meta->file_index_id == 0) {
            return false;
        }
        file_index_id = meta->file_index_id;
        return true;
    }

    bool file_index_id_for_root(const node_type* root, uint32_t& file_index_id) const {
        file_index_id = 0;
        if (root == directory_index_.root()) {
            file_index_id = root_directory_file_index_id_;
            return file_index_id != 0;
        }
        if (root == nullptr || !root->has_directory_meta()) {
            return false;
        }
        const directory_meta* meta = root->directory_meta();
        if (meta == nullptr || meta->file_index_id == 0) {
            return false;
        }
        file_index_id = meta->file_index_id;
        return true;
    }

    bool insert_dentry_on_directory_root(node_type* directory_root,
                                         const DirectoryBlockStore::entry& item,
                                         bool overwrite_existing) {
        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        directory_meta* meta = nullptr;
        if (!directory_block_refs_for_root(directory_root, head, tail, &meta)) {
            return false;
        }
        if (!file_index_id_for_root(directory_root, file_index_id)) {
            return false;
        }
        if (!directory_blocks_.insert(file_index_id, head, item, overwrite_existing)) {
            return false;
        }
        (void) tail;
        (void) meta;
        return true;
    }

    bool remove_dentry_from_directory_root(node_type* directory_root, uint64_t component_hash,
                                           const std::string& name) {
        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        directory_meta* meta = nullptr;
        if (!directory_block_refs_for_root(directory_root, head, tail, &meta)) {
            return false;
        }
        if (!file_index_id_for_root(directory_root, file_index_id)) {
            return false;
        }
        if (!directory_blocks_.remove(file_index_id, component_hash, name)) {
            return false;
        }
        if (directory_root == directory_index_.root()) {
            return true;
        }
        return true;
    }

    bool resolve_parent_creation_context(const ParsedPath& parsed,
                                         node_type*& parent_root,
                                         inode_ref& parent_ref,
                                         threadinfo& ti) const {
        parent_root = nullptr;
        parent_ref = make_inode_ref(0, 0);
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        parent_root = const_cast<node_type*>(parent_root_const);

        if (parent.normalized_path == "/") {
            if (persistence_available_) {
                if (!root_persistent_ref_valid_) {
                    return false;
                }
                parent_ref = root_persistent_ref_;
            }
            return true;
        }

        if (!parent_root->has_directory_meta()) {
            return false;
        }
        const directory_meta* meta = parent_root->directory_meta();
        if (meta == nullptr) {
            return false;
        }
        parent_ref = make_inode_ref(meta->head_block_id, 0);
        return true;
    }

    bool create_file_on_parent_root(const ParsedPath& parsed, node_type* parent_root, inode_ref ref,
                                    threadinfo& ti) {
        (void) ti;
        DirectoryBlockStore::entry entry =
            DirectoryBlockStore::make_file_entry(parsed.hashes.back(), ref,
                                                 parsed.components.back());
        return insert_dentry_on_directory_root(parent_root, entry, fast_mode_);
    }

    bool create_directory_on_parent_root(const ParsedPath& parsed, node_type* parent_root,
                                         inode_ref ref, threadinfo& ti) {
        (void) ref;
        uint32_t head_block_id = 0;
        uint32_t tail_block_id = 0;
        if (!allocate_directory_blocks(head_block_id, tail_block_id)) {
            return false;
        }
        const uint64_t dir_inode_id = packed_directory_inode_id(head_block_id);
        inode_disk embedded_inode = make_embedded_directory_inode(dir_inode_id, head_block_id);
        if (!persistent_store_.WriteDirectoryInode(head_block_id, embedded_inode)) {
            return false;
        }
        node_type* directory_root = nullptr;
        directory_meta meta = make_directory_meta(head_block_id, tail_block_id);
        meta.file_index_id = directory_blocks_.create_file_index(head_block_id);
        if (meta.file_index_id == 0) {
            return false;
        }
        if (!directory_blocks_.write_directory_self_name(head_block_id, parsed.hashes.back(),
                                                         parsed.components.back())) {
            return false;
        }
        if (!directory_index_.create_directory_layer(parent_root, parsed.hashes.back(),
                                                     meta, directory_root, ti)) {
            return false;
        }
        return true;
    }

    bool persist_create_success(const ParsedPath& parsed, entry_kind kind, inode_ref persistent_ref,
                                const inode_ref& parent_ref) {
        if (!persistence_available_) {
            return true;
        }
        if (parsed.normalized_path == "/") {
            return true;
        }
        if (kind == entry_kind::directory) {
            return true;
        }

        inode_disk inode{};
        const uint64_t ts = now_ns();
        inode.inode_id = packed_inode_id(persistent_ref);
        inode.parent_inode_id = packed_inode_id(parent_ref);
        inode.primary_ref = persistent_ref;
        inode.aux_ref = make_inode_ref(0, 0);
        inode.ctime_ns = ts;
        inode.mtime_ns = ts;
        inode.tombstone_epoch = 0;
        inode.stable_version = 1;
        inode.generation = 1;
        inode.link_count = 1;
        inode.mode = 0644;
        inode.type = static_cast<uint8_t>(inode_type::file);
        inode.flags = 0;
        inode.checksum = 0;
        return persistent_store_.WriteInode(persistent_ref, inode);
    }

    bool persist_delete_success(const value_type& existing, threadinfo&) {
        if (!persistence_available_) {
            return true;
        }
        if (entry_is_directory(existing)) {
            inode_disk inode{};
            if (!directory_blocks_.read_directory_inode(existing.ref.block_id, inode)) return false;
            inode.link_count = 0;
            inode.flags = static_cast<uint8_t>(inode.flags | kInodeFlagDeleted);
            inode.tombstone_epoch = now_ns();
            ++inode.stable_version;
            return directory_blocks_.write_directory_inode(existing.ref.block_id, inode);
        }

        inode_disk inode{};
        if (!persistent_store_.ReadInode(existing.ref, inode)) {
            return false;
        }
        const uint64_t ts = now_ns();
        inode.mtime_ns = ts;
        inode.tombstone_epoch = ts;
        inode.primary_ref = existing.ref;
        inode.link_count = 0;
        inode.stable_version = inode.stable_version + 1;
        inode.generation = inode.generation + 1;
        inode.flags = static_cast<uint8_t>(inode.flags | kInodeFlagDeleted);
        inode.checksum = 0;
        if (!persistent_store_.WriteInode(existing.ref, inode)) {
            return false;
        }
        return true;
    }

    bool persist_rename_success(const ParsedPath& old_parsed, const ParsedPath& new_parsed,
                                const value_type& moved_entry, threadinfo& ti) {
        (void) old_parsed;
        if (!persistence_available_) {
            return true;
        }
        if (entry_is_directory(moved_entry)) {
            return true;
        }

        inode_ref parent_ref = make_inode_ref(0, 0);
        if (!resolve_parent_persistent_ref(new_parsed, parent_ref, ti)) {
            return false;
        }

        inode_disk inode{};
        if (!persistent_store_.ReadInode(moved_entry.ref, inode)) {
            return false;
        }
        inode.parent_inode_id = packed_inode_id(parent_ref);
        inode.mtime_ns = now_ns();
        inode.stable_version = inode.stable_version + 1;
        inode.generation = inode.generation + 1;
        inode.checksum = 0;
        return persistent_store_.WriteInode(moved_entry.ref, inode);
    }

    bool create_entry(const std::string& path, entry_kind kind, inode_ref ref, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        return create_entry_from_parsed(parsed, kind, ref, ti);
    }

    bool force_remove_child_slot_fast(node_type* parent_root, uint64_t child_hash,
                                      threadinfo& ti) {
        return directory_index_.force_remove_child_slot(parent_root, child_hash, ti);
    }

    bool remove_entry_fast_from_parsed(const ParsedPath& parsed, value_type* removed_out,
                                       threadinfo& ti) {
        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);

        value_type directory_entry;
        if (lookup_child_from_parent_root(parent_root, parsed.components.back(),
                                          parsed.hashes.back(), directory_entry, ti)
            && entry_is_directory(directory_entry)) {
            if (!directory_index_.remove_child_slot_fast(parent_root, parsed.hashes.back(),
                                                         removed_out, ti)) {
                return false;
            }
            return true;
        }

        DirectoryBlockStore::entry dentry;
        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        if (!directory_block_refs_for_root(parent_root, head, tail)) {
            return false;
        }
        if (!file_index_id_for_root(parent_root, file_index_id)) {
            return false;
        }
        if (!directory_blocks_.lookup(file_index_id, parsed.hashes.back(),
                                      parsed.components.back(), dentry)) {
            return false;
        }
        if (removed_out != nullptr) {
            *removed_out = DirectoryBlockStore::to_namespace_entry(dentry);
        }
        return remove_dentry_from_directory_root(parent_root, parsed.hashes.back(),
                                                parsed.components.back());
    }

    bool create_entry_fast_from_parsed(const ParsedPath& parsed, entry_kind kind, inode_ref ref,
                                       threadinfo& ti) {
        if (kind == entry_kind::directory) {
            return create_directory_fast_from_parsed(parsed, ref, ti);
        }
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);
        DirectoryBlockStore::entry entry =
            DirectoryBlockStore::make_file_entry(parsed.hashes.back(), ref,
                                                 parsed.components.back());
        return insert_dentry_on_directory_root(parent_root, entry, true);
    }

    bool create_directory_fast_from_parsed(const ParsedPath& parsed, inode_ref ref,
                                           threadinfo& ti) {
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);

        return create_directory_on_parent_root(parsed, parent_root, ref, ti);
    }

    bool rename_path_fast_from_parsed(const ParsedPath& old_parsed,
                                      const ParsedPath& new_parsed,
                                      threadinfo& ti,
                                      value_type* moved_entry_out) {
        if (old_parsed.normalized_path == "/" || new_parsed.normalized_path == "/") {
            return false;
        }
        if (old_parsed.normalized_path == new_parsed.normalized_path) {
            return true;
        }
        if (is_prefix_path(old_parsed.normalized_path, new_parsed.normalized_path)) {
            return false;
        }

        value_type source_entry;
        const uint64_t lookup_begin = rename_profile_enabled_ ? now_ns() : 0;
        if (!lookup_entry_from_parsed(old_parsed, source_entry, ti)) {
            if (rename_profile_enabled_) {
                rename_profile_.old_lookup_ns += now_ns() - lookup_begin;
            }
            return false;
        }
        if (rename_profile_enabled_) {
            rename_profile_.old_lookup_ns += now_ns() - lookup_begin;
        }
        if (moved_entry_out != nullptr) {
            *moved_entry_out = source_entry;
        }
        if (entry_is_directory(source_entry)) {
            if (rename_profile_enabled_) {
                ++rename_profile_.directory_renames;
            }
            const uint64_t dir_begin = rename_profile_enabled_ ? now_ns() : 0;
            const bool ok = rename_directory_o1_fast_from_parsed(old_parsed, new_parsed, ti);
            if (rename_profile_enabled_) {
                rename_profile_.directory_o1_ns += now_ns() - dir_begin;
            }
            return ok;
        }

        if (rename_profile_enabled_) {
            ++rename_profile_.file_renames;
        }
        const uint64_t create_begin = rename_profile_enabled_ ? now_ns() : 0;
        if (!create_entry_from_parsed(new_parsed, source_entry.kind, source_entry.ref, ti)) {
            if (rename_profile_enabled_) {
                rename_profile_.create_new_ns += now_ns() - create_begin;
            }
            return false;
        }
        if (rename_profile_enabled_) {
            rename_profile_.create_new_ns += now_ns() - create_begin;
        }
        const uint64_t remove_begin = rename_profile_enabled_ ? now_ns() : 0;
        const bool ok = remove_entry(old_parsed.normalized_path, ti);
        if (rename_profile_enabled_) {
            rename_profile_.remove_old_ns += now_ns() - remove_begin;
        }
        return ok;
    }

    bool rename_directory_o1_fast_from_parsed(const ParsedPath& old_parsed,
                                              const ParsedPath& new_parsed,
                                              threadinfo& ti) {
        if (old_parsed.normalized_path == "/" || new_parsed.normalized_path == "/") {
            return false;
        }
        if (old_parsed.normalized_path == new_parsed.normalized_path) {
            return true;
        }
        if (is_prefix_path(old_parsed.normalized_path, new_parsed.normalized_path)) {
            return false;
        }
        if (new_parsed.components.empty() || !entry_name_fits(new_parsed.components.back())) {
            return false;
        }

        ParsedPath old_parent = old_parsed;
        old_parent.normalized_path = parent_path(old_parsed.normalized_path);
        old_parent.components.pop_back();
        old_parent.hashes.pop_back();

        ParsedPath new_parent = new_parsed;
        new_parent.normalized_path = parent_path(new_parsed.normalized_path);
        new_parent.components.pop_back();
        new_parent.hashes.pop_back();

        const node_type* old_parent_root_const = nullptr;
        if (!locate_directory_root(old_parent, old_parent_root_const, ti)) {
            return false;
        }
        const node_type* new_parent_root_const = nullptr;
        if (!locate_directory_root(new_parent, new_parent_root_const, ti)) {
            return false;
        }

        directory_edge_lookup source_edge;
        if (!lookup_directory_edge_from_parent_root(const_cast<node_type*>(old_parent_root_const),
                                                   old_parsed.components.back(),
                                                   old_parsed.hashes.back(),
                                                   source_edge, ti)) {
            return false;
        }

        bool same_slot = old_parent_root_const == new_parent_root_const
            && old_parsed.hashes.back() == new_parsed.hashes.back();
        if (!same_slot) {
            if (!force_remove_child_slot_fast(const_cast<node_type*>(new_parent_root_const),
                                              new_parsed.hashes.back(), ti)) {
                return false;
            }
        }

        if (!directory_index_.remove_directory_layer(
                const_cast<node_type*>(old_parent_root_const),
                old_parsed.hashes.back(),
                source_edge.child_root,
                ti)) {
            return false;
        }

        if (!directory_index_.attach_existing_directory_layer(
                const_cast<node_type*>(new_parent_root_const),
                new_parsed.hashes.back(),
                source_edge.child_root,
                ti)) {
            return false;
        }

        if (!source_edge.child_root->has_directory_meta()) {
            return false;
        }
        directory_meta* meta = source_edge.child_root->directory_meta();
        if (meta == nullptr) {
            return false;
        }
        const uint32_t moved_head = meta->head_block_id;
        const uint32_t moved_tail = meta->tail_block_id;
        const uint16_t moved_flags = meta->flags;
        const uint32_t moved_file_index_id = meta->file_index_id;
        if (old_parsed.components.back() != new_parsed.components.back()) {
            if (!directory_blocks_.write_directory_self_name(moved_head,
                                                             new_parsed.hashes.back(),
                                                             new_parsed.components.back())) {
                return false;
            }
        }
        *meta = make_directory_meta(moved_head, moved_tail, moved_flags);
        meta->file_index_id = moved_file_index_id;

        return true;
    }

    bool rename_directory_o1_from_parsed(const ParsedPath& old_parsed,
                                         const ParsedPath& new_parsed,
                                         threadinfo& ti) {
        if (old_parsed.normalized_path == "/" || new_parsed.normalized_path == "/") {
            return false;
        }
        if (old_parsed.normalized_path == new_parsed.normalized_path) {
            return true;
        }
        if (is_prefix_path(old_parsed.normalized_path, new_parsed.normalized_path)) {
            return false;
        }
        if (new_parsed.components.empty() || !entry_name_fits(new_parsed.components.back())) {
            return false;
        }

        ParsedPath old_parent = old_parsed;
        old_parent.normalized_path = parent_path(old_parsed.normalized_path);
        old_parent.components.pop_back();
        old_parent.hashes.pop_back();

        ParsedPath new_parent = new_parsed;
        new_parent.normalized_path = parent_path(new_parsed.normalized_path);
        new_parent.components.pop_back();
        new_parent.hashes.pop_back();

        const node_type* old_parent_root_const = nullptr;
        if (!locate_directory_root(old_parent, old_parent_root_const, ti)) {
            return false;
        }
        const node_type* new_parent_root_const = nullptr;
        if (!locate_directory_root(new_parent, new_parent_root_const, ti)) {
            return false;
        }

        directory_edge_lookup source_edge;
        if (!lookup_directory_edge_from_parent_root(const_cast<node_type*>(old_parent_root_const),
                                                   old_parsed.components.back(),
                                                   old_parsed.hashes.back(),
                                                   source_edge, ti)) {
            return false;
        }

        value_type existing_target;
        if (lookup_child_from_parent_root(const_cast<node_type*>(new_parent_root_const),
                                          new_parsed.components.back(),
                                          new_parsed.hashes.back(),
                                          existing_target, ti)) {
            return false;
        }

        if (!directory_index_.remove_directory_layer(
                const_cast<node_type*>(old_parent_root_const),
                old_parsed.hashes.back(),
                source_edge.child_root,
                ti)) {
            return false;
        }

        if (!directory_index_.attach_existing_directory_layer(
                const_cast<node_type*>(new_parent_root_const),
                new_parsed.hashes.back(),
                source_edge.child_root,
                ti)) {
            return false;
        }

        if (!source_edge.child_root->has_directory_meta()) {
            return false;
        }
        directory_meta* meta = source_edge.child_root->directory_meta();
        if (meta == nullptr) {
            return false;
        }
        const uint32_t moved_head = meta->head_block_id;
        const uint32_t moved_tail = meta->tail_block_id;
        const uint16_t moved_flags = meta->flags;
        const uint32_t moved_file_index_id = meta->file_index_id;
        if (old_parsed.components.back() != new_parsed.components.back()) {
            if (!directory_blocks_.write_directory_self_name(moved_head,
                                                             new_parsed.hashes.back(),
                                                             new_parsed.components.back())) {
                return false;
            }
        }
        *meta = make_directory_meta(moved_head, moved_tail, moved_flags);
        meta->file_index_id = moved_file_index_id;

        return true;
    }

    bool remove_entry(const std::string& path, threadinfo& ti) {
        ParsedPath parsed = PathKey::parse_absolute_path(path);
        return remove_entry_from_parsed(parsed, nullptr, ti);
    }

    bool remove_entry_from_parsed(const ParsedPath& parsed, value_type* removed_out,
                                  threadinfo& ti) {
        if (parsed.normalized_path == "/" || parsed.components.empty()) {
            return false;
        }

        if (fast_mode_) {
            return remove_entry_fast_from_parsed(parsed, removed_out, ti);
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);

        value_type directory_entry;
        if (lookup_child_from_parent_root(parent_root, parsed.components.back(),
                                          parsed.hashes.back(), directory_entry, ti)
            && entry_is_directory(directory_entry)) {
            if (!directory_index_.remove_child_slot_checked(parent_root, parsed.hashes.back(),
                                                            parsed.components.back(),
                                                            removed_out, ti)) {
                return false;
            }
            return true;
        }

        DirectoryBlockStore::entry dentry;
        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        if (!directory_block_refs_for_root(parent_root, head, tail)) {
            return false;
        }
        if (!file_index_id_for_root(parent_root, file_index_id)) {
            return false;
        }
        if (!directory_blocks_.lookup(file_index_id, parsed.hashes.back(),
                                      parsed.components.back(), dentry)
            || dentry.kind != DirectoryBlockStore::dentry_kind::file) {
            return false;
        }
        if (removed_out != nullptr) {
            *removed_out = DirectoryBlockStore::to_namespace_entry(dentry);
        }
        return remove_dentry_from_directory_root(parent_root, parsed.hashes.back(),
                                                parsed.components.back());
    }

    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_entry_from_parsed(const ParsedPath& parsed,
                                                              value_type& out,
                                                              threadinfo& ti) const {
        if (parsed.normalized_path == "/") {
            out = make_namespace_entry(entry_kind::directory, make_inode_ref(0, 0), "/");
            return true;
        }
        if (parsed.components.empty() || parsed.hashes.empty()) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }

        if (lookup_child_from_parent_root(const_cast<node_type*>(parent_root_const),
                                          parsed.components.back(),
                                          parsed.hashes.back(),
                                          out, ti)) {
            return true;
        }

        uint32_t head = 0;
        uint32_t tail = 0;
        uint32_t file_index_id = 0;
        if (!directory_block_refs_for_root(parent_root_const, head, tail)) {
            return false;
        }
        if (!file_index_id_for_root(parent_root_const, file_index_id)) {
            return false;
        }
        DirectoryBlockStore::entry dentry;
        if (!directory_blocks_.lookup(file_index_id, parsed.hashes.back(),
                                      parsed.components.back(), dentry)) {
            return false;
        }
        out = DirectoryBlockStore::to_namespace_entry(dentry);
        return true;
    }

    bool create_entry_from_parsed(const ParsedPath& parsed, entry_kind kind, inode_ref ref,
                                  threadinfo& ti) {
        if (fast_mode_) {
            return create_entry_fast_from_parsed(parsed, kind, ref, ti);
        }
        if (kind == entry_kind::directory) {
            return create_directory_from_parsed(parsed, ref, ti);
        }
        if (parsed.normalized_path == "/") {
            return false;
        }
        if (parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);

        DirectoryBlockStore::entry entry =
            DirectoryBlockStore::make_file_entry(parsed.hashes.back(), ref,
                                                 parsed.components.back());
        return insert_dentry_on_directory_root(parent_root, entry, false);
    }

    bool create_directory_from_parsed(const ParsedPath& parsed, inode_ref ref, threadinfo& ti) {
        if (fast_mode_) {
            return create_directory_fast_from_parsed(parsed, ref, ti);
        }
        if (parsed.normalized_path == "/") {
            return false;
        }
        if (parsed.components.empty()) {
            return false;
        }
        if (!entry_name_fits(parsed.components.back())) {
            return false;
        }

        ParsedPath parent = parsed;
        parent.normalized_path = parent_path(parsed.normalized_path);
        parent.components.pop_back();
        parent.hashes.pop_back();

        const node_type* parent_root_const = nullptr;
        if (!locate_directory_root(parent, parent_root_const, ti)) {
            return false;
        }
        node_type* parent_root = const_cast<node_type*>(parent_root_const);

        return create_directory_on_parent_root(parsed, parent_root, ref, ti);
    }

    std::vector<subtree_record> scan_subtree(const ParsedPath& root, threadinfo& ti) const {
        return directory_index_.scan_subtree(root, ti);
    }

    // 在父目录对应的 layer root 内，用“最后一级组件哈希 + 真实名字”检查孩子是否已存在。
    // 当前目录和文件都可以复用这条局部查找路径，避免每次都从全局根重新走完整路径。
    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_child_from_parent_root(
        node_type* parent_root,
        const std::string& child_name,
        uint64_t child_hash,
        value_type& out,
        threadinfo& ti) const {
        return directory_index_.lookup_child_from_parent_root(parent_root, child_name,
                                                             child_hash, out, ti);
    }

    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_directory_edge_from_parent_root(
        node_type* parent_root,
        const std::string& child_name,
        uint64_t child_hash,
        directory_edge_lookup& out,
        threadinfo& ti) const {
        return directory_index_.lookup_directory_edge_from_parent_root(parent_root, child_name,
                                                                      child_hash, out, ti);
    }

    LHM_NAMESPACE_ALWAYS_INLINE bool lookup_child_slot_from_parent_root(
        node_type* parent_root,
        uint64_t child_hash,
        child_slot_lookup& out,
        threadinfo& ti) const {
        return directory_index_.lookup_child_slot_from_parent_root(parent_root, child_hash,
                                                                  out, ti);
    }

    LHM_NAMESPACE_ALWAYS_INLINE bool locate_directory_root(const ParsedPath& parsed,
                                                           const node_type*& out,
                                                           threadinfo& ti) const {
        return directory_index_.locate_directory_root(parsed, out, ti);
    }

};

}  // namespace MasstreeLHM

#undef LHM_NAMESPACE_ALWAYS_INLINE

namespace Masstree {

template <>
class value_print<MasstreeLHM::route_value> {
  public:
    static void print(MasstreeLHM::route_value value, FILE* f, const char* prefix,
                      int indent, Str key, kvtimestamp_t, char* suffix) {
        fprintf(f, "%s%*s%.*s = {kind=%s, ref=(block=%" PRIu32 ", offset=%" PRIu32 ")}%s\n",
                prefix, indent, "", key.len, key.s,
                MasstreeLHM::entry_kind_name(value.kind),
                value.ref.block_id, value.ref.offset, suffix);
    }
};

template <>
class value_print<MasstreeLHM::namespace_entry> {
  public:
    static void print(MasstreeLHM::namespace_entry value, FILE* f, const char* prefix,
                      int indent, Str key, kvtimestamp_t, char* suffix) {
        std::string name = MasstreeLHM::entry_name(value);
        fprintf(f, "%s%*s%.*s = {kind=%s, name=%s, ref=(block=%" PRIu32 ", offset=%" PRIu32 ")}%s\n",
                prefix, indent, "", key.len, key.s,
                MasstreeLHM::entry_kind_name(value.kind), name.c_str(),
                value.ref.block_id, value.ref.offset, suffix);
    }
};

}  // namespace Masstree

#endif
