#ifndef MASSTREELHM_PERSISTENT_STORE_HH
#define MASSTREELHM_PERSISTENT_STORE_HH

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "metadata_layout.hh"

namespace MasstreeLHM {

// 一个完整的 16KB 元数据块镜像。
using metadata_block_bytes = std::array<uint8_t, kMetadataBlockBytes>;

// inode 块的内存镜像，方便一次读写与尺寸校验。
struct inode_block_image {
    metadata_common_block_header common;
    inode_block_header header;
    inode_disk slots[kInodesPerBlock];
};
static_assert(sizeof(inode_block_image) == kMetadataBlockBytes,
              "inode block image must be one 16KB metadata block");

// 目录块的内存镜像：固定头 + 目录项载荷。
struct directory_block_image {
    metadata_common_block_header common;
    directory_block_header header;
    inode_disk directory_inode;
    uint8_t payload[kDirectoryBlockPayloadBytes];
};
static_assert(sizeof(directory_block_image) == kMetadataBlockBytes,
              "directory block image must be one 16KB metadata block");

// 第一阶段最小持久化存储：
// 1) 提供 16KB 元数据块读写；
// 2) 提供顺序块分配；
// 3) 提供 inode slot 分配与 inode 读写；
// 4) 提供目录块读写。
//
// 当前不实现崩溃恢复流程，只保留布局和最小操作能力。
class PersistentStore {
  public:
    struct stats_snapshot {
        uint64_t block_read_ops = 0;
        uint64_t block_write_ops = 0;
        uint64_t bytes_read = 0;
        uint64_t bytes_written = 0;
        uint64_t sync_ops = 0;
        uint64_t fsync_ops = 0;
        uint64_t allocate_block_ops = 0;
        uint64_t l0_flush_ops = 0;
        uint64_t inode_cache_flush_ops = 0;
        // [PROFILE] Break down each 16 KiB O_DIRECT ReadBlock call so we can
        // distinguish allocator/CPU overhead from the blocking pread itself.
        uint64_t block_read_total_time_ns = 0;
        uint64_t block_read_alloc_time_ns = 0;
        uint64_t block_read_pread_time_ns = 0;
        uint64_t block_read_copy_free_time_ns = 0;
        uint64_t inode_block_read_ops = 0;
        uint64_t inode_block_read_time_ns = 0;
        uint64_t directory_block_read_ops = 0;
        uint64_t directory_block_read_time_ns = 0;
        uint64_t other_block_read_ops = 0;
        uint64_t other_block_read_time_ns = 0;
        std::array<uint64_t, 9> block_read_pread_latency_buckets{};
    };

    PersistentStore();
    ~PersistentStore();

    PersistentStore(const PersistentStore&) = delete;
    PersistentStore& operator=(const PersistentStore&) = delete;

    bool OpenOrCreate(const std::string& path);
    void Close();
    bool IsOpen() const;
    bool Sync();
    bool set_l0_cache_enabled(bool enabled);
    bool l0_cache_enabled() const;

    // 基础块读写接口（固定 16KB）。
    bool ReadBlock(uint32_t block_id, metadata_block_bytes& out) const;
    bool WriteBlock(uint32_t block_id, const metadata_block_bytes& data);

    // 顺序分配一个新块号（当前为 append-only 策略）。
    bool AllocateBlock(uint32_t& out_block_id);

    // 控制块读写（block_id=0 固定 superblock）。
    bool ReadSuperblock(superblock_payload& out) const;
    bool WriteSuperblock(const superblock_payload& payload);
    bool ReadCheckpoint(uint32_t block_id, checkpoint_payload& out) const;
    bool WriteCheckpoint(uint32_t block_id, const checkpoint_payload& payload);
    bool ReadAllocatorState(uint32_t block_id, allocator_state_payload& out) const;
    bool WriteAllocatorState(uint32_t block_id, const allocator_state_payload& payload);

    // inode 块与 inode slot 操作。
    bool ReadInodeBlock(uint32_t block_id, inode_block_image& out) const;
    bool ReadInodeBlockForRefDirect(const inode_ref& ref, inode_block_image& out) const;
    bool WriteInodeBlock(uint32_t block_id, const inode_block_image& image);
    bool AllocateInodeSlot(inode_ref& out_ref);
    // Exclusive fresh-store builder: root occupies ticket 0; assign files from 1.
    bool WriteFreshFileInodes(std::vector<inode_disk>& inodes);
    bool ReadInode(const inode_ref& ref, inode_disk& out) const;
    bool WriteInode(const inode_ref& ref, const inode_disk& inode);

    // 目录块操作。
    bool ReadDirectoryBlock(uint32_t block_id, directory_block_image& out) const;
    bool WriteDirectoryBlock(uint32_t block_id, const directory_block_image& image);
    bool ReadDirectoryInode(uint32_t head_block_id, inode_disk& out) const;
    bool WriteDirectoryInode(uint32_t head_block_id, const inode_disk& inode);
    bool CreateDirectoryChain(uint64_t dir_inode_id, inode_ref& out_head_ref);
    bool AppendDirectoryChainBlock(const inode_ref& tail_ref,
                                   uint64_t dir_inode_id,
                                   inode_ref& out_new_tail_ref);

    // Allocation bounds for checked ID reads; not an inode-ID index.
    bool InodeAddressMayExist(const inode_ref& ref) const {
        return ref.block_id && ref.offset < kInodesPerBlock &&
            (uint64_t(ref.block_id - 1) * kInodesPerBlock + ref.offset) < next_inode_ticket_.load(std::memory_order_relaxed);
    }
    bool DirectoryAddressMayExist(uint32_t block) const {
        return block && block < next_directory_block_.load(std::memory_order_relaxed);
    }
    uint32_t NextAllocBlock() const;
    const std::string& BackingFile() const;
    void ResetStats();
    stats_snapshot Stats() const;

  private:
    static constexpr uint64_t kInodeAllocChunk = 256;
    static constexpr uint32_t kL0FlushOpsThreshold = 64;
    static constexpr std::size_t kInodeCacheShardCount = 8;
    static constexpr std::size_t kInodeCacheEntriesPerShard = 64;
    static constexpr std::size_t kInodeCacheDirtyWordCount =
        (kInodeCacheEntriesPerShard + 63) / 64;

    struct inode_chunk_cache {
        const PersistentStore* owner = nullptr;
        uint64_t epoch = 0;
        uint64_t next_ticket = 0;
        uint64_t end_ticket = 0;
    };
    static thread_local inode_chunk_cache tls_inode_chunk_;

    // L0：线程局部 inode 缓冲句柄（每线程一个活动块缓冲，冷启动批量 create/delete 优先）。
    struct l0_thread_buffer {
        std::mutex mu;
        bool has_active_block = false;    // 当前是否持有活动 inode block。
        bool active_dirty = false;        // 活动块是否有未下发到 L1 的修改。
        uint32_t active_block_id = 0;     // 活动块号。
        uint32_t pending_ops = 0;         // 自上次 flush 后累计操作数。
        inode_block_image active_image{}; // 活动块镜像。
    };

    struct l0_tls_handle {
        const PersistentStore* owner = nullptr;
        uint64_t epoch = 0;
        l0_thread_buffer* buffer = nullptr;
    };
    static thread_local l0_tls_handle tls_l0_handle_;

    // inode block 缓冲条目：缓存一个 16KB inode block，并记录是否为脏块。
    struct inode_block_cache_entry {
        bool valid = false;     // 该槽位是否已装载有效 block。
        bool dirty = false;     // 该槽位是否包含未落盘修改。
        uint32_t block_id = 0;  // 当前缓存命中的 inode block_id。
        inode_block_image image{};
    };

    // 每个 shard 一把锁，控制该 shard 下 cache slot 的并发访问。
    struct inode_block_cache_shard {
        mutable std::mutex mu;
        std::array<inode_block_cache_entry, kInodeCacheEntriesPerShard> entries{};
        std::array<uint64_t, kInodeCacheDirtyWordCount> dirty_bitmap{};
    };

    bool EnsureBlockCount(uint32_t block_count);
    bool InitializeFreshStore();
    bool RecomputeNextAllocBlock();
    bool RecomputeLinearInodeCursor();
    bool RecomputeNextDirectoryBlock();
    bool AllocateBlockLocked(uint32_t& out_block_id);
    bool AllocateDirectoryBlockLocked(uint32_t& out_block_id);
    uint64_t NextSequenceLocked();
    void InitDirectoryBlockImage(directory_block_image& out,
                                 uint32_t block_id,
                                 uint64_t dir_inode_id,
                                 uint32_t prev_block_id,
                                 uint32_t next_block_id,
                                 uint64_t base_version,
                                 uint64_t sequence);
    bool EnsureInodeBlocksInitializedLocked(uint32_t target_block_id);
    bool AcquireInodeTicketRange(uint64_t& begin_ticket, uint64_t& end_ticket);
    l0_thread_buffer* GetOrCreateL0ThreadBuffer();
    l0_thread_buffer* GetL0ThreadBufferIfValid() const;
    bool FlushL0ThreadBufferLocked(l0_thread_buffer& buffer, bool keep_active_block);
    bool FlushAllL0Buffers();
    void ResetAllL0Buffers();
    bool ReadInodeBlockDirect(uint32_t block_id, inode_block_image& out) const;
    bool WriteInodeBlockDirect(uint32_t block_id, const inode_block_image& image);
    bool FlushInodeCacheEntryLocked(inode_block_cache_shard& shard, std::size_t slot_index);
    bool FlushInodeCacheShard(std::size_t shard_index);
    bool FlushInodeCache();
    void ResetInodeCache();
    static void SetShardDirtyBit(inode_block_cache_shard& shard,
                                 std::size_t slot_index,
                                 bool dirty);
    static bool IsShardDirtyBitSet(const inode_block_cache_shard& shard, std::size_t slot_index);
    static std::size_t InodeCacheShardIndex(uint32_t block_id);
    static std::size_t InodeCacheSlotIndex(uint32_t block_id);
    bool IsKnownBlockType(uint16_t block_type) const;
    static uint32_t InodeBlockIdFromTicket(uint64_t ticket);
    static uint32_t InodeSlotFromTicket(uint64_t ticket);
    static uint32_t InodePhysicalBlockId(uint32_t inode_block_id);
    static uint32_t DirectoryPhysicalBlockId(uint32_t directory_block_id);
    static uint32_t MaxLogicalInodeBlockId(uint32_t physical_block_count);
    static uint32_t MaxLogicalDirectoryBlockId(uint32_t physical_block_count);

    static bool IsSlotUsed(const inode_block_header& header, uint32_t slot);
    static void SetSlotUsed(inode_block_header& header, uint32_t slot, bool used);
    static void BuildCommonHeader(metadata_common_block_header& out,
                                  metadata_block_type block_type,
                                  uint32_t block_id,
                                  uint32_t payload_bytes,
                                  uint64_t sequence,
                                  uint64_t owner_inode_id,
                                  uint32_t next_block_id,
                                  uint32_t flags);

    int fd_;
    std::string path_;
    std::atomic<uint32_t> next_alloc_block_;
    std::atomic<uint32_t> next_directory_block_;
    uint64_t sequence_;
    std::atomic<uint64_t> next_inode_ticket_;
    std::atomic<uint32_t> highest_initialized_inode_block_;
    std::atomic<uint64_t> inode_alloc_epoch_;
    std::atomic<uint64_t> l0_epoch_;
    std::atomic<bool> l0_cache_enabled_;
    mutable std::atomic<uint64_t> stats_block_read_ops_;
    mutable std::atomic<uint64_t> stats_block_write_ops_;
    mutable std::atomic<uint64_t> stats_bytes_read_;
    mutable std::atomic<uint64_t> stats_bytes_written_;
    mutable std::atomic<uint64_t> stats_sync_ops_;
    mutable std::atomic<uint64_t> stats_fsync_ops_;
    mutable std::atomic<uint64_t> stats_allocate_block_ops_;
    mutable std::atomic<uint64_t> stats_l0_flush_ops_;
    mutable std::atomic<uint64_t> stats_inode_cache_flush_ops_;
    // [PROFILE] O_DIRECT read timing counters. Bucket upper bounds are
    // 20/40/60/80/100/150/250/500 us, followed by an overflow bucket.
    mutable std::atomic<uint64_t> stats_block_read_total_time_ns_;
    mutable std::atomic<uint64_t> stats_block_read_alloc_time_ns_;
    mutable std::atomic<uint64_t> stats_block_read_pread_time_ns_;
    mutable std::atomic<uint64_t> stats_block_read_copy_free_time_ns_;
    mutable std::atomic<uint64_t> stats_inode_block_read_ops_;
    mutable std::atomic<uint64_t> stats_inode_block_read_time_ns_;
    mutable std::atomic<uint64_t> stats_directory_block_read_ops_;
    mutable std::atomic<uint64_t> stats_directory_block_read_time_ns_;
    mutable std::atomic<uint64_t> stats_other_block_read_ops_;
    mutable std::atomic<uint64_t> stats_other_block_read_time_ns_;
    mutable std::array<std::atomic<uint64_t>, 9> stats_block_read_pread_latency_buckets_;
    mutable std::mutex inode_alloc_mu_;
    mutable std::array<inode_block_cache_shard, kInodeCacheShardCount> inode_block_cache_;
    mutable std::mutex l0_buffers_mu_;
    std::vector<l0_thread_buffer*> l0_buffers_;
};

}  // namespace MasstreeLHM

#endif
