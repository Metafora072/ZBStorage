#ifndef MASSTREELHM_METADATA_LAYOUT_HH
#define MASSTREELHM_METADATA_LAYOUT_HH

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "directory_meta.hh"

namespace MasstreeLHM {

// 持久化布局定稿 v1：
// 1) 元数据物理块固定 16KB
// 2) inode 固定 256B
// 3) inode 不存名字，名字只保存在目录项中
static constexpr uint32_t kMetadataBlockBytes = 16 * 1024;
static constexpr uint32_t kDirectoryBufferBytes = 16 * 1024;
static constexpr uint32_t kCommonBlockHeaderBytes = 64;
static constexpr uint32_t kInodeBytes = 256;

enum class metadata_block_type : uint16_t {
    invalid = 0,
    superblock = 1,
    checkpoint = 2,
    inode_block = 3,
    directory_block = 4,
    allocator_state = 5
};

// 每个 16KB 元数据块共享的统一头部。
struct metadata_common_block_header {
    uint32_t magic;             // 固定魔数，用于快速判定块是否合法
    uint16_t block_type;        // 元数据块类型，见 metadata_block_type
    uint16_t layout_version;    // 布局版本号，用于未来升级兼容
    uint32_t block_id;          // 当前块号（逻辑块编号）
    uint32_t payload_bytes;     // 有效载荷字节数（不含 common header）
    uint32_t checksum;          // 当前块校验和
    uint64_t sequence;          // 提交序列号/版本序号
    uint64_t owner_inode_id;    // 块所属 inode（目录块/数据块可复用）
    uint32_t next_block_id;     // 链式组织时的下一块块号（0 表示无）
    uint32_t flags;             // 通用标记位
    uint8_t reserved[16];       // 预留扩展字段，补齐到 64B
};
static_assert(sizeof(metadata_common_block_header) == kCommonBlockHeaderBytes,
              "common block header must be 64B");

// superblock / checkpoint / allocator state 先保留最小字段与扩展空间。
struct superblock_payload {
    uint32_t metadata_block_bytes;      // 元数据块大小（当前固定 16KB）
    uint32_t latest_checkpoint_block;   // 最新 checkpoint 所在块号
    uint64_t latest_checkpoint_seq;     // 最新 checkpoint 序列号
    uint64_t fs_uuid_hi;                // 文件系统 UUID 高 64 位
    uint64_t fs_uuid_lo;                // 文件系统 UUID 低 64 位
    uint32_t flags;                     // superblock 标记位
    uint8_t reserved[28];               // 预留扩展字段，补齐到 64B
};
static_assert(sizeof(superblock_payload) == 64, "superblock payload must be 64B");

struct checkpoint_payload {
    uint64_t seq;                       // checkpoint 序列号
    uint64_t committed_epoch;           // 提交 epoch（逻辑时间戳）
    inode_ref root_dir_inode;           // 根目录 inode 的持久化引用
    inode_ref allocator_state_ref;      // 分配器状态对象引用
    uint32_t flags;                     // checkpoint 标记位
    uint32_t checksum;                  // checkpoint 载荷校验和
    uint8_t reserved[24];               // 预留扩展字段，补齐到 64B
};
static_assert(sizeof(checkpoint_payload) == 64, "checkpoint payload must be 64B");

struct allocator_state_payload {
    uint32_t next_free_block;           // append-only 分配时的下一个可分配块号
    uint32_t free_list_head_block;      // 空闲链表头块号（0 表示空）
    uint64_t alloc_epoch;               // 分配器版本/epoch
    uint32_t flags;                     // 分配器标记位
    uint32_t checksum;                  // 分配器状态校验和
    uint8_t reserved[40];               // 预留扩展字段，补齐到 64B
};
static_assert(sizeof(allocator_state_payload) == 64,
              "allocator state payload must be 64B");

enum class inode_type : uint8_t {
    file = 1,
    directory = 2
};

// 固定 256B inode，不含名字字段。
// 当前目录父子关系以 Masstree route tree 为准；目录 embedded inode 不维护
// parent_inode_id，避免目录迁移引入额外持久化写。
struct inode_disk {
    uint64_t inode_id;           // inode 全局唯一标识
    uint64_t parent_inode_id;    // 文件可用父目录 inode id；目录 embedded inode 当前置 0
    uint64_t size_bytes;         // 文件大小（目录可用作逻辑统计字段）
    uint64_t ctime_ns;           // 创建/状态变更时间戳（ns）
    uint64_t mtime_ns;           // 内容/目录项变更时间戳（ns）
    inode_ref primary_ref;       // 主引用：目录通常指向 stable directory blocks
    inode_ref aux_ref;           // 辅助引用：预留给额外元数据或扩展链
    uint64_t stable_version;     // 稳定版本号（flush 后递增）
    uint64_t generation;         // 代际号（防止旧引用误用）
    uint64_t tombstone_epoch;    // 删除墓碑时间（延迟回收可用）
    uint32_t link_count;         // 链接计数（未来 hard link 可扩展）
    uint32_t checksum;           // inode 校验和
    uint16_t mode;               // 权限/模式位
    uint8_t type;                // inode_type（file/directory）
    uint8_t flags;               // inode 标记位
    uint8_t reserved[164];       // 预留扩展字段，补齐到 256B
};
static_assert(sizeof(inode_disk) == kInodeBytes, "inode size must be 256B");

// inode block 内部头：配合 slot bitmap 管理定长 inode 槽位。
struct inode_block_header {
    uint32_t slot_count;       // 当前块 inode 槽位总数（固定 63）
    uint32_t used_count;       // 已占用槽位数量
    uint32_t flags;            // inode block 标记位
    uint32_t reserved0;        // 对齐/预留
    uint64_t alloc_epoch;      // 本块最近分配 epoch
    uint8_t slot_bitmap[16];   // 槽位占用位图（63 位足够）
    uint8_t reserved[152];     // 预留扩展字段，补齐到 192B
};
static_assert(sizeof(inode_block_header) == 192, "inode block header must be 192B");

static constexpr uint32_t kInodesPerBlock = 63;
static_assert(kCommonBlockHeaderBytes + sizeof(inode_block_header)
                  + kInodesPerBlock * sizeof(inode_disk)
              == kMetadataBlockBytes,
              "inode block layout must fill one 16KB block");

enum class metadata_entry_kind : uint8_t {
    invalid = 0,
    directory = 1,
    file = 2
};

// 目录项固定头，后面紧跟 name[name_length]。
// 这里固定头按 24B 对齐，便于扫描和编码。
struct directory_entry_disk_header {
    uint64_t component_hash;  // 当前目录分量 hash（用于快速匹配）
    inode_ref ref;            // 目标 inode 的持久化引用
    uint8_t kind;             // metadata_entry_kind（file/directory）
    uint8_t name_length;      // 名字长度（后续紧跟 name 字节）
    uint16_t flags;           // 目录项标记位
    uint32_t reserved;        // 对齐/预留（固定头凑到 24B）
};
static_assert(sizeof(directory_entry_disk_header) == 24,
              "directory entry header must be 24B");

// 目录块局部头，描述本块承载的目录项载荷。
struct directory_block_header {
    uint64_t dir_inode_id;      // 所属目录 inode id
    uint32_t entry_count;       // 本块目录项数量
    uint32_t used_bytes;        // 本块 payload 已使用字节数
    uint64_t base_version;      // 目录稳定版本号（flush 生成）
    uint32_t prev_block_id;     // 前一目录块块号（链表头可为 0）
    uint32_t next_block_id;     // 下一目录块块号（尾块为 0）
    uint32_t delta_count;       // 本块由多少 delta 合并生成（统计/调试）
    uint32_t flags;             // 目录块标记位
    uint8_t reserved[24];       // 预留扩展字段，补齐到 64B
};
static_assert(sizeof(directory_block_header) == 64,
              "directory block header must be 64B");

static constexpr uint32_t kDirectoryBlockFlagHasEmbeddedInode = 1u << 0;

static constexpr uint32_t kDirectoryBlockPayloadBytes =
    kMetadataBlockBytes - kCommonBlockHeaderBytes - sizeof(directory_block_header)
    - sizeof(inode_disk);
static_assert(kDirectoryBlockPayloadBytes == 16000,
              "directory payload must be 16000B for 16KB block");

enum class directory_delta_op : uint8_t {
    insert = 1,
    erase = 2
};

// 目录缓冲中的增量项：用于合并 stable directory blocks 与内存变更。
struct directory_delta_entry {
    directory_delta_op op;      // 增量操作类型（insert/erase）
    metadata_entry_kind kind;   // 目录项类型（file/directory）
    uint16_t flags;             // 增量标记位
    uint64_t component_hash;    // 目录分量 hash
    inode_ref ref;              // 目标 inode 引用
    std::string name;           // 目录项名字（变长，仅内存态）
};

// 活跃目录对应的 16KB 写缓冲元信息。
struct directory_buffer_state {
    inode_ref dir_inode_ref;                // 当前目录 inode 引用
    uint64_t dir_inode_id;                  // 当前目录 inode id
    uint64_t base_stable_version;           // 目录缓冲基于的 stable 版本
    uint64_t last_access_epoch;             // 最近一次访问 epoch
    uint64_t last_flush_epoch;              // 最近一次 flush epoch
    uint32_t used_bytes;                    // 当前缓冲已使用字节
    uint32_t recent_read_hits;              // 近期读命中计数（热度判定）
    uint32_t recent_write_hits;             // 近期写命中计数（热度判定）
    bool dirty;                             // 是否有未落盘修改
    bool flushing;                          // 是否正在 flush
    std::vector<directory_delta_entry> deltas;  // 目录增量列表
};

// 结构体大小汇总（编译期可得）。
static constexpr std::size_t kSizeOfMetadataCommonBlockHeader =
    sizeof(metadata_common_block_header);
static constexpr std::size_t kSizeOfSuperblockPayload = sizeof(superblock_payload);
static constexpr std::size_t kSizeOfCheckpointPayload = sizeof(checkpoint_payload);
static constexpr std::size_t kSizeOfAllocatorStatePayload =
    sizeof(allocator_state_payload);
static constexpr std::size_t kSizeOfInodeDisk = sizeof(inode_disk);
static constexpr std::size_t kSizeOfInodeBlockHeader = sizeof(inode_block_header);
static constexpr std::size_t kSizeOfDirectoryEntryDiskHeader =
    sizeof(directory_entry_disk_header);
static constexpr std::size_t kSizeOfDirectoryBlockHeader =
    sizeof(directory_block_header);
static constexpr std::size_t kSizeOfDirectoryDeltaEntry =
    sizeof(directory_delta_entry);
static constexpr std::size_t kSizeOfDirectoryBufferState =
    sizeof(directory_buffer_state);

}  // namespace MasstreeLHM

#endif
