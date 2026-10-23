#ifndef MASSTREELHM_NAMESPACE_TYPES_HH
#define MASSTREELHM_NAMESPACE_TYPES_HH

#include <algorithm>
#include <cstring>
#include <inttypes.h>
#include <string>
#include <string_view>
#include <vector>

#include "directory_meta.hh"
#include "kvthread.hh"
#include "masstree.hh"
#include "path_key.hh"

namespace MasstreeLHM {

// entry_kind 描述同一张命名空间表中的最小记录类型。
// 当前阶段只区分目录和普通文件，为后续 rename 和 inode
// 外存指针预留扩展空间。
enum class entry_kind : uint8_t {
    invalid = 0,
    directory = 1,
    file = 2
};

struct namespace_entry {
    entry_kind kind;
    inode_ref ref;
    uint8_t name_length;
    char name[kMaxEntryNameBytes + 1];
};

// Masstree directory route values must stay small.  File and directory names
// live in dentry blocks; route slots only need the minimal payload required by
// legacy value-slot code paths.
struct route_value {
    entry_kind kind;
    inode_ref ref;
};
static_assert(sizeof(route_value) <= 16, "route value must remain compact");

// readdir_record 描述一次最小目录扫描结果。
// 当前阶段已经可以同时返回：
// 1. 完整哈希路径
// 2. 当前目录下的直接孩子哈希
// 3. 真实目录项名字
// 4. 对应 entry
struct readdir_record {
    std::vector<uint64_t> full_hashes;
    uint64_t child_component_hash;
    std::string child_name;
    namespace_entry entry;
};

struct subtree_record {
    std::vector<uint64_t> full_hashes;
    namespace_entry entry;
};

struct directory_root_debug_info {
    bool found;
    bool is_leaf;
    bool has_meta;
    uint32_t height;
    int size;
    bool child0_is_leaf;
    bool child1_exists;
    bool child1_is_leaf;
    int child0_size;
    int child1_size;
};

inline std::string entry_name(const namespace_entry& entry) {
    return std::string(entry.name, entry.name + entry.name_length);
}

inline std::string_view entry_name_view(const namespace_entry& entry) {
    return std::string_view(entry.name, entry.name_length);
}

inline bool entry_name_fits(const std::string& name) {
    return name.size() <= kMaxEntryNameBytes;
}

inline namespace_entry make_namespace_entry(entry_kind kind, inode_ref ref,
                                            const std::string& name) {
    namespace_entry entry;
    entry.kind = kind;
    entry.ref = ref;
    entry.name_length = static_cast<uint8_t>(name.size());
    memset(entry.name, 0, sizeof(entry.name));
    memcpy(entry.name, name.data(), name.size());
    return entry;
}

inline route_value make_route_value(entry_kind kind, inode_ref ref) {
    route_value value;
    value.kind = kind;
    value.ref = ref;
    return value;
}

inline route_value make_route_value(const namespace_entry& entry) {
    return make_route_value(entry.kind, entry.ref);
}

inline namespace_entry make_namespace_entry(const route_value& value,
                                            const std::string& name) {
    return make_namespace_entry(value.kind, value.ref, name);
}

inline bool entry_is_valid(const namespace_entry& entry) {
    return entry.kind != entry_kind::invalid;
}

inline bool entry_is_directory(const namespace_entry& entry) {
    return entry.kind == entry_kind::directory;
}

inline bool entry_is_file(const namespace_entry& entry) {
    return entry.kind == entry_kind::file;
}

inline bool entry_name_equals(const namespace_entry& entry, std::string_view name) {
    const size_t n = static_cast<size_t>(entry.name_length);
    return n == name.size() && (n == 0 || memcmp(entry.name, name.data(), n) == 0);
}

inline const char* entry_kind_name(entry_kind kind) {
    switch (kind) {
    case entry_kind::directory:
        return "directory";
    case entry_kind::file:
        return "file";
    default:
        return "invalid";
    }
}

// Masstree 只保存目录 route 结构；真实名字保存在 dentryBlock 中。这里的
// value_type 必须保持紧凑，避免文件名上限提高后重新放大内存索引树。
struct namespace_table_params : Masstree::nodeparams<7, 7> {
    using value_type = route_value;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = ::threadinfo;
};

}  // namespace MasstreeLHM

#endif
