#ifndef MASSTREELHM_DIRECTORY_META_HH
#define MASSTREELHM_DIRECTORY_META_HH

#include <stdint.h>
#include <string>
#include <string_view>

namespace MasstreeLHM {

// Match common POSIX filesystem NAME_MAX semantics.  The in-memory Masstree
// route stores component hashes; names are kept in dentry blocks/API records.
static constexpr size_t kMaxEntryNameBytes = 255;

// inode_ref 模拟未来 DMB 中的物理地址引用。
// 当前阶段仍然使用 block_id + offset 作为最小占位表示。
struct inode_ref {
    uint32_t block_id;
    uint32_t offset;
};

inline inode_ref make_inode_ref(uint32_t block_id, uint32_t offset) {
    inode_ref ref;
    ref.block_id = block_id;
    ref.offset = offset;
    return ref;
}

using block_id_t = uint32_t;

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
    uint32_t head_block_id;
    uint32_t tail_block_id;
    uint16_t flags;
    uint16_t layout_version;
    // Runtime FileIndex id owned by this directory root. The index state is
    // kept in DirectoryBlockStore's compact vector, not in a hash side table.
    uint32_t file_index_id;
};
static_assert(sizeof(directory_meta) == 16, "directory_meta must stay 16B");

inline directory_meta make_directory_meta(uint32_t head_block_id,
                                          uint32_t tail_block_id,
                                          uint16_t flags = 0) {
    directory_meta meta;
    meta.head_block_id = head_block_id;
    meta.tail_block_id = tail_block_id;
    meta.flags = flags;
    meta.layout_version = 1;
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
