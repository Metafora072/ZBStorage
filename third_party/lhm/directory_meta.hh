#ifndef MASSTREELHM_DIRECTORY_META_HH
#define MASSTREELHM_DIRECTORY_META_HH

#include <stdint.h>
#include <string>
#include <string_view>

namespace MasstreeLHM {

// ZB limits names to 64 bytes. Masstree stores component hashes; names live
// in disk records, including compact directory head records.
static constexpr size_t kMaxEntryNameBytes = 64;

// inode_ref 模拟未来 DMB 中的物理地址引用。
// 当前阶段仍然使用 block_id + offset 作为最小占位表示。
struct inode_ref {
    uint64_t block_id : 48;
    uint64_t offset : 16;
};

inline inode_ref make_inode_ref(uint64_t block_id, uint32_t offset) {
    inode_ref ref;
    ref.block_id = block_id;
    ref.offset = offset;
    return ref;
}

using block_id_t = uint64_t;
static constexpr uint64_t kMaxLogicalBlock = (uint64_t{1} << 47) - 1;
static constexpr uint64_t kCompactHeadTag = uint64_t{1} << 47;

struct directory_ref {
    block_id_t block_id;
};

inline directory_ref make_directory_ref(block_id_t block_id) {
    directory_ref ref;
    ref.block_id = block_id;
    return ref;
}

// directory_meta 是 Masstree 目录 route root 的内存热元数据。
// 真实目录名与目录 inode 存在该目录自己的 directory block 中；父目录
// dentry block 不再重复保存子目录 entry。
struct directory_meta {
    uint64_t head_block_id;
    uint64_t tail_block_id;
    uint16_t flags;
    uint16_t layout_version;
    // Runtime FileIndex id owned by this directory root. The index state is
    // kept in DirectoryBlockStore's segmented pool, not in a hash side table.
    uint64_t file_index_id;
};
static_assert(sizeof(directory_meta) == 32, "directory_meta must stay 32B");

inline directory_meta make_directory_meta(uint64_t head_block_id,
                                          uint64_t tail_block_id,
                                          uint16_t flags = 0) {
    directory_meta meta;
    meta.head_block_id = head_block_id;
    meta.tail_block_id = tail_block_id;
    meta.flags = flags;
    meta.layout_version = 2;
    meta.file_index_id = 0;
    return meta;
}

inline std::string directory_meta_name(const directory_meta& meta) {
    (void) meta;
    return std::string();
}

inline bool directory_meta_name_equals(const directory_meta& meta, std::string_view name) {
    (void) meta;
    (void) name;
    return true;
}

}  // namespace MasstreeLHM

#endif
