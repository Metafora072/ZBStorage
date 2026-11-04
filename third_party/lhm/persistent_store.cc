#include "persistent_store.hh"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

namespace MasstreeLHM {

namespace {

static constexpr uint64_t kMetadataMagic = 0x4C484D31u;  // "LHM1"
static constexpr uint16_t kLayoutVersion = 2;
static constexpr std::size_t kDirectIoAlign = 4096;

// O_DIRECT 在少数平台上可能不存在；这里保证至少可编译。
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

bool alloc_aligned_block(void** out_ptr) {
    *out_ptr = nullptr;
    int rc = ::posix_memalign(out_ptr, kDirectIoAlign, kMetadataBlockBytes);
    return rc == 0 && *out_ptr != nullptr;
}

bool pread_all(int fd, void* buf, size_t bytes, off_t off) {
    uint8_t* p = reinterpret_cast<uint8_t*>(buf);
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = ::pread(fd, p + done, bytes - done, off + done);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool pwrite_all(int fd, const void* buf, size_t bytes, off_t off) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = ::pwrite(fd, p + done, bytes - done, off + done);
        if (n > 0) {
            done += static_cast<size_t>(n);
            continue;
        }
        if (n == 0) {
            return false;
        }
        if (errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

off_t block_offset(uint64_t block_id) {
    return static_cast<off_t>(block_id) * static_cast<off_t>(kMetadataBlockBytes);
}

uint64_t monotonic_raw_ns() {
    struct timespec ts {};
    ::clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL
        + static_cast<uint64_t>(ts.tv_nsec);
}

std::size_t read_latency_bucket(uint64_t duration_ns) {
    static constexpr uint64_t bounds_ns[] = {
        20'000, 40'000, 60'000, 80'000, 100'000,
        150'000, 250'000, 500'000
    };
    for (std::size_t i = 0; i < sizeof(bounds_ns) / sizeof(bounds_ns[0]); ++i) {
        if (duration_ns <= bounds_ns[i]) {
            return i;
        }
    }
    return 8;
}

}  // namespace

thread_local PersistentStore::inode_chunk_cache PersistentStore::tls_inode_chunk_{};
thread_local PersistentStore::l0_tls_handle PersistentStore::tls_l0_handle_{};

PersistentStore::PersistentStore()
    : fd_(-1), next_alloc_block_(1), next_directory_block_(1), sequence_(1),
      next_inode_ticket_(0),
      highest_initialized_inode_block_(0), inode_alloc_epoch_(1), l0_epoch_(1),
      l0_cache_enabled_(true),
      stats_block_read_ops_(0), stats_block_write_ops_(0),
      stats_bytes_read_(0), stats_bytes_written_(0), stats_sync_ops_(0),
      stats_fsync_ops_(0), stats_allocate_block_ops_(0), stats_l0_flush_ops_(0),
      stats_inode_cache_flush_ops_(0),
      stats_block_read_total_time_ns_(0), stats_block_read_alloc_time_ns_(0),
      stats_block_read_pread_time_ns_(0), stats_block_read_copy_free_time_ns_(0),
      stats_inode_block_read_ops_(0), stats_inode_block_read_time_ns_(0),
      stats_directory_block_read_ops_(0), stats_directory_block_read_time_ns_(0),
      stats_other_block_read_ops_(0), stats_other_block_read_time_ns_(0) {
    for (auto& bucket : stats_block_read_pread_latency_buckets_) {
        bucket.store(0, std::memory_order_relaxed);
    }
}

PersistentStore::~PersistentStore() {
    Close();
    // ZB constructs/uses/destroys stores on the bridge worker. A later store
    // may reuse this address; never leave TLS pointing at freed L0 buffers.
    if (tls_l0_handle_.owner == this) tls_l0_handle_ = {};
    if (tls_inode_chunk_.owner == this) tls_inode_chunk_ = {};
    std::lock_guard<std::mutex> guard(l0_buffers_mu_);
    for (l0_thread_buffer* buf : l0_buffers_) {
        delete buf;
    }
    l0_buffers_.clear();
}

bool PersistentStore::OpenOrCreate(const std::string& path, bool from_snapshot) {
    Close();
    ResetStats();
    // 当前阶段统一采用 direct I/O，绕过页缓存。
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_DIRECT, 0644);
    if (fd < 0) {
        return false;
    }

    fd_ = fd;
    path_ = path;
    next_alloc_block_.store(1, std::memory_order_relaxed);
    next_directory_block_.store(1, std::memory_order_relaxed);
    sequence_ = 1;
    next_inode_ticket_.store(0, std::memory_order_relaxed);
    highest_initialized_inode_block_.store(0, std::memory_order_relaxed);
    inode_alloc_epoch_.fetch_add(1, std::memory_order_relaxed);
    l0_epoch_.fetch_add(1, std::memory_order_relaxed);
    ResetAllL0Buffers();
    ResetInodeCache();

    struct stat st;
    if (::fstat(fd_, &st) != 0) {
        Close();
        return false;
    }

    heads_fd_=::open((path+".heads").c_str(),O_RDWR|O_DIRECT|(st.st_size==0 ? O_CREAT|O_EXCL : 0),0600);
    if(heads_fd_<0) { Close(); return false; }
    next_head_=1; head_page_=UINT64_MAX; head_dirty_=false;
    if (st.st_size == 0) {
        if (!InitializeFreshStore()) {
            Close();
            return false;
        }
        return true;
    }

    if (from_snapshot) {
        superblock_payload sb{};
        if (!ReadSuperblock(sb) || sb.metadata_block_bytes != kMetadataBlockBytes) { Close(); return false; }
        return true;
    }
    if (!RecomputeNextAllocBlock()) {
        Close();
        return false;
    }
    return true;
}

void PersistentStore::SaveAllocation(SnapshotWriter& out) const {
    out.u64(next_alloc_block_.load()); out.u64(next_directory_block_.load());
    out.u64(sequence_); out.u64(next_inode_ticket_.load());
    out.u64(highest_initialized_inode_block_.load()); out.u64(next_head_);
    struct stat st{}; if(::fstat(heads_fd_,&st)) throw std::runtime_error("head store stat failed");
    out.u64(st.st_dev); out.u64(st.st_ino); out.u64(st.st_size);
}
void PersistentStore::LoadAllocation(SnapshotReader& in) {
    auto blocks=in.u64(), dirs=in.u64(); auto seq=in.u64(), tickets=in.u64(); auto high=in.u64();
    if (!blocks || !dirs || !seq || !tickets || !high || dirs>(kMaxLogicalBlock+1) ||
        high>kMaxLogicalBlock || tickets>uint64_t(kMaxLogicalBlock)*kInodesPerBlock)
        throw std::runtime_error("invalid snapshot allocation state");
    auto heads=in.u64(); auto dev=in.u64(),ino=in.u64(),bytes=in.u64(); struct stat st{};
    if(!heads || heads>kCompactHeadTag || ::fstat(heads_fd_,&st) || dev!=uint64_t(st.st_dev) ||
       ino!=uint64_t(st.st_ino) || bytes!=uint64_t(st.st_size)) throw std::runtime_error("head store/snapshot mismatch");
    next_head_=heads;
    next_alloc_block_.store(blocks); next_directory_block_.store(dirs); sequence_=seq;
    next_inode_ticket_.store(tickets); highest_initialized_inode_block_.store(high);
}

void PersistentStore::Close() {
    if(heads_fd_>=0) { FlushHeadPage(); ::fsync(heads_fd_); ::close(heads_fd_); heads_fd_=-1; }
    head_page_=UINT64_MAX; head_dirty_=false; next_head_=1;
    if (fd_ >= 0) {
        if (l0_cache_enabled_.load(std::memory_order_relaxed)) {
            (void) FlushAllL0Buffers();
        }
        (void) FlushInodeCache();
        if (::fsync(fd_) == 0) {
            stats_fsync_ops_.fetch_add(1, std::memory_order_relaxed);
        }
        ::close(fd_);
        fd_ = -1;
    }
    l0_epoch_.fetch_add(1, std::memory_order_relaxed);
    ResetAllL0Buffers();
    ResetInodeCache();
    path_.clear();
    next_alloc_block_.store(1, std::memory_order_relaxed);
    next_directory_block_.store(1, std::memory_order_relaxed);
    sequence_ = 1;
    next_inode_ticket_.store(0, std::memory_order_relaxed);
    highest_initialized_inode_block_.store(0, std::memory_order_relaxed);
    inode_alloc_epoch_.fetch_add(1, std::memory_order_relaxed);
}

bool PersistentStore::IsOpen() const {
    return fd_ >= 0;
}

bool PersistentStore::Sync() {
    if(heads_fd_<0 || !FlushHeadPage() || ::fsync(heads_fd_)) return false;
    if (!IsOpen()) {
        return false;
    }
    if (l0_cache_enabled_.load(std::memory_order_relaxed)) {
        if (!FlushAllL0Buffers()) {
            return false;
        }
    }
    if (!FlushInodeCache()) {
        return false;
    }
    stats_sync_ops_.fetch_add(1, std::memory_order_relaxed);
    if (::fsync(fd_) != 0) {
        return false;
    }
    stats_fsync_ops_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::set_l0_cache_enabled(bool enabled) {
    const bool old = l0_cache_enabled_.exchange(enabled, std::memory_order_relaxed);
    if (old == enabled) {
        return true;
    }
    if (!enabled) {
        if (IsOpen() && !FlushAllL0Buffers()) {
            l0_cache_enabled_.store(old, std::memory_order_relaxed);
            return false;
        }
        ResetAllL0Buffers();
        l0_epoch_.fetch_add(1, std::memory_order_relaxed);
    } else {
        l0_epoch_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

bool PersistentStore::l0_cache_enabled() const {
    return l0_cache_enabled_.load(std::memory_order_relaxed);
}

bool PersistentStore::ReadBlock(uint64_t block_id, metadata_block_bytes& out) const {
    if (!IsOpen()) {
        return false;
    }
    const uint64_t total_start_ns = monotonic_raw_ns();
    void* aligned = nullptr;
    if (!alloc_aligned_block(&aligned)) {
        return false;
    }
    const uint64_t alloc_end_ns = monotonic_raw_ns();
    bool ok = pread_all(fd_, aligned, kMetadataBlockBytes, block_offset(block_id));
    const uint64_t pread_end_ns = monotonic_raw_ns();
    if (ok) {
        std::memcpy(out.data(), aligned, kMetadataBlockBytes);
        stats_block_read_ops_.fetch_add(1, std::memory_order_relaxed);
        stats_bytes_read_.fetch_add(kMetadataBlockBytes, std::memory_order_relaxed);
    }
    ::free(aligned);
    const uint64_t total_end_ns = monotonic_raw_ns();
    if (ok) {
        const uint64_t alloc_ns = alloc_end_ns - total_start_ns;
        const uint64_t pread_ns = pread_end_ns - alloc_end_ns;
        const uint64_t copy_free_ns = total_end_ns - pread_end_ns;
        const uint64_t total_ns = total_end_ns - total_start_ns;
        stats_block_read_alloc_time_ns_.fetch_add(alloc_ns, std::memory_order_relaxed);
        stats_block_read_pread_time_ns_.fetch_add(pread_ns, std::memory_order_relaxed);
        stats_block_read_copy_free_time_ns_.fetch_add(copy_free_ns,
                                                       std::memory_order_relaxed);
        stats_block_read_total_time_ns_.fetch_add(total_ns, std::memory_order_relaxed);
        stats_block_read_pread_latency_buckets_[read_latency_bucket(pread_ns)].fetch_add(
            1, std::memory_order_relaxed);

        const auto* common = reinterpret_cast<const metadata_common_block_header*>(out.data());
        if (common->block_type == static_cast<uint16_t>(metadata_block_type::inode_block)) {
            stats_inode_block_read_ops_.fetch_add(1, std::memory_order_relaxed);
            stats_inode_block_read_time_ns_.fetch_add(total_ns, std::memory_order_relaxed);
        } else if (common->block_type
                   == static_cast<uint16_t>(metadata_block_type::directory_block)) {
            stats_directory_block_read_ops_.fetch_add(1, std::memory_order_relaxed);
            stats_directory_block_read_time_ns_.fetch_add(total_ns,
                                                           std::memory_order_relaxed);
        } else {
            stats_other_block_read_ops_.fetch_add(1, std::memory_order_relaxed);
            stats_other_block_read_time_ns_.fetch_add(total_ns, std::memory_order_relaxed);
        }
    }
    return ok;
}

bool PersistentStore::WriteBlock(uint64_t block_id, const metadata_block_bytes& data) {
    if (!IsOpen()) {
        return false;
    }
    if (!EnsureBlockCount(block_id + 1)) {
        return false;
    }
    void* aligned = nullptr;
    if (!alloc_aligned_block(&aligned)) {
        return false;
    }
    std::memcpy(aligned, data.data(), kMetadataBlockBytes);
    bool ok = pwrite_all(fd_, aligned, kMetadataBlockBytes, block_offset(block_id));
    ::free(aligned);
    if (ok) {
        stats_block_write_ops_.fetch_add(1, std::memory_order_relaxed);
        stats_bytes_written_.fetch_add(kMetadataBlockBytes, std::memory_order_relaxed);
        uint64_t need = block_id + 1;
        uint64_t cur = next_alloc_block_.load(std::memory_order_relaxed);
        while (cur < need
               && !next_alloc_block_.compare_exchange_weak(cur, need,
                                                           std::memory_order_relaxed,
                                                           std::memory_order_relaxed)) {
        }
    }
    return ok;
}

bool PersistentStore::AllocateBlock(uint64_t& out_block_id) {
    if (!IsOpen()) {
        return false;
    }
    std::lock_guard<std::mutex> guard(inode_alloc_mu_);
    return AllocateBlockLocked(out_block_id);
}

bool PersistentStore::ReadSuperblock(superblock_payload& out) const {
    metadata_block_bytes block;
    if (!ReadBlock(0, block)) {
        return false;
    }
    const auto* common = reinterpret_cast<const metadata_common_block_header*>(block.data());
    if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
        || common->block_type != static_cast<uint16_t>(metadata_block_type::superblock)) {
        return false;
    }
    const auto* payload =
        reinterpret_cast<const superblock_payload*>(block.data() + kCommonBlockHeaderBytes);
    out = *payload;
    return true;
}

bool PersistentStore::WriteSuperblock(const superblock_payload& payload) {
    metadata_block_bytes block{};
    auto* common = reinterpret_cast<metadata_common_block_header*>(block.data());
    BuildCommonHeader(*common, metadata_block_type::superblock, 0,
                      sizeof(superblock_payload), sequence_++, 0, 0, 0);
    auto* p = reinterpret_cast<superblock_payload*>(block.data() + kCommonBlockHeaderBytes);
    *p = payload;
    return WriteBlock(0, block);
}

bool PersistentStore::ReadCheckpoint(uint64_t block_id, checkpoint_payload& out) const {
    metadata_block_bytes block;
    if (!ReadBlock(block_id, block)) {
        return false;
    }
    const auto* common = reinterpret_cast<const metadata_common_block_header*>(block.data());
    if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
        || common->block_type != static_cast<uint16_t>(metadata_block_type::checkpoint)) {
        return false;
    }
    const auto* payload =
        reinterpret_cast<const checkpoint_payload*>(block.data() + kCommonBlockHeaderBytes);
    out = *payload;
    return true;
}

bool PersistentStore::WriteCheckpoint(uint64_t block_id, const checkpoint_payload& payload) {
    metadata_block_bytes block{};
    auto* common = reinterpret_cast<metadata_common_block_header*>(block.data());
    BuildCommonHeader(*common, metadata_block_type::checkpoint, block_id,
                      sizeof(checkpoint_payload), sequence_++, 0, 0, 0);
    auto* p = reinterpret_cast<checkpoint_payload*>(block.data() + kCommonBlockHeaderBytes);
    *p = payload;
    return WriteBlock(block_id, block);
}

bool PersistentStore::ReadAllocatorState(uint64_t block_id, allocator_state_payload& out) const {
    metadata_block_bytes block;
    if (!ReadBlock(block_id, block)) {
        return false;
    }
    const auto* common = reinterpret_cast<const metadata_common_block_header*>(block.data());
    if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
        || common->block_type != static_cast<uint16_t>(metadata_block_type::allocator_state)) {
        return false;
    }
    const auto* payload =
        reinterpret_cast<const allocator_state_payload*>(block.data() + kCommonBlockHeaderBytes);
    out = *payload;
    return true;
}

bool PersistentStore::WriteAllocatorState(uint64_t block_id,
                                          const allocator_state_payload& payload) {
    metadata_block_bytes block{};
    auto* common = reinterpret_cast<metadata_common_block_header*>(block.data());
    BuildCommonHeader(*common, metadata_block_type::allocator_state, block_id,
                      sizeof(allocator_state_payload), sequence_++, 0, 0, 0);
    auto* p =
        reinterpret_cast<allocator_state_payload*>(block.data() + kCommonBlockHeaderBytes);
    *p = payload;
    return WriteBlock(block_id, block);
}

bool PersistentStore::ReadInodeBlock(uint64_t block_id, inode_block_image& out) const {
    if (!IsOpen()) {
        return false;
    }

    const std::size_t shard_idx = InodeCacheShardIndex(block_id);
    const std::size_t slot_idx = InodeCacheSlotIndex(block_id);
    inode_block_cache_shard& shard = inode_block_cache_[shard_idx];
    {
        std::lock_guard<std::mutex> guard(shard.mu);
        const inode_block_cache_entry& entry = shard.entries[slot_idx];
        if (entry.valid && entry.block_id == block_id) {
            out = entry.image;
            return true;
        }
    }

    return ReadInodeBlockDirect(block_id, out);
}

bool PersistentStore::ReadInodeBlockForRefDirect(const inode_ref& ref,
                                                 inode_block_image& out) const {
    if (ref.offset >= kInodesPerBlock) {
        return false;
    }
    if (!IsOpen()) {
        return false;
    }
    return ReadInodeBlockDirect(ref.block_id, out);
}

bool PersistentStore::WriteInodeBlock(uint64_t block_id, const inode_block_image& image) {
    if (!IsOpen()) {
        return false;
    }

    const std::size_t shard_idx = InodeCacheShardIndex(block_id);
    const std::size_t slot_idx = InodeCacheSlotIndex(block_id);
    inode_block_cache_shard& shard = inode_block_cache_[shard_idx];
    std::lock_guard<std::mutex> guard(shard.mu);

    inode_block_cache_entry& entry = shard.entries[slot_idx];
    if (entry.valid && entry.block_id != block_id) {
        if (!FlushInodeCacheEntryLocked(shard, slot_idx)) {
            return false;
        }
    }
    entry.valid = true;
    entry.dirty = true;
    entry.block_id = block_id;
    entry.image = image;
    SetShardDirtyBit(shard, slot_idx, true);
    return true;
}

bool PersistentStore::AllocateInodeSlot(inode_ref& out_ref) {
    if (!IsOpen()) {
        return false;
    }
    uint64_t begin = 0;
    uint64_t end = 0;
    if (!AcquireInodeTicketRange(begin, end)) {
        return false;
    }
    if (begin >= end) {
        return false;
    }
    out_ref = make_inode_ref(InodeBlockIdFromTicket(begin), InodeSlotFromTicket(begin));
    return true;
}

bool PersistentStore::ReadInode(const inode_ref& ref, inode_disk& out) const {
    if (ref.offset >= kInodesPerBlock) {
        return false;
    }
    if (!IsOpen()) {
        return false;
    }

    if (l0_cache_enabled_.load(std::memory_order_relaxed)) {
        if (l0_thread_buffer* l0 = GetL0ThreadBufferIfValid()) {
            std::lock_guard<std::mutex> l0_guard(l0->mu);
            if (l0->has_active_block && l0->active_block_id == ref.block_id) {
                out = l0->active_image.slots[ref.offset];
                return true;
            }
        }
    }

    const std::size_t shard_idx = InodeCacheShardIndex(ref.block_id);
    const std::size_t slot_idx = InodeCacheSlotIndex(ref.block_id);
    inode_block_cache_shard& shard = inode_block_cache_[shard_idx];
    {
        std::lock_guard<std::mutex> guard(shard.mu);
        const inode_block_cache_entry& entry = shard.entries[slot_idx];
        if (entry.valid && entry.block_id == ref.block_id) {
            out = entry.image.slots[ref.offset];
            return true;
        }
    }

    inode_block_image image;
    if (!ReadInodeBlockDirect(ref.block_id, image)) {
        return false;
    }
    out = image.slots[ref.offset];
    return true;
}

bool PersistentStore::WriteInode(const inode_ref& ref, const inode_disk& inode) {
    if (ref.offset >= kInodesPerBlock) {
        return false;
    }
    if (!IsOpen()) {
        return false;
    }

    if (l0_cache_enabled_.load(std::memory_order_relaxed)) {
        l0_thread_buffer* l0 = GetOrCreateL0ThreadBuffer();
        if (!l0) {
            return false;
        }
        std::lock_guard<std::mutex> l0_guard(l0->mu);

        if (!l0->has_active_block || l0->active_block_id != ref.block_id) {
            if (!FlushL0ThreadBufferLocked(*l0, false)) {
                return false;
            }

            inode_block_image loaded{};
            if (!ReadInodeBlock(ref.block_id, loaded)) {
                return false;
            }
            l0->has_active_block = true;
            l0->active_dirty = false;
            l0->active_block_id = ref.block_id;
            l0->pending_ops = 0;
            l0->active_image = loaded;
        }

        const bool was_used = IsSlotUsed(l0->active_image.header, ref.offset);
        l0->active_image.slots[ref.offset] = inode;
        SetSlotUsed(l0->active_image.header, ref.offset, true);
        if (!was_used && l0->active_image.header.used_count < kInodesPerBlock) {
            ++l0->active_image.header.used_count;
        }
        l0->active_dirty = true;
        ++l0->pending_ops;
        if (l0->pending_ops >= kL0FlushOpsThreshold
            || l0->active_image.header.used_count >= kInodesPerBlock) {
            if (!FlushL0ThreadBufferLocked(*l0, true)) {
                return false;
            }
        }
        return true;
    }

    const std::size_t shard_idx = InodeCacheShardIndex(ref.block_id);
    const std::size_t slot_idx = InodeCacheSlotIndex(ref.block_id);
    inode_block_cache_shard& shard = inode_block_cache_[shard_idx];
    std::lock_guard<std::mutex> guard(shard.mu);

    inode_block_cache_entry& entry = shard.entries[slot_idx];
    if (!entry.valid || entry.block_id != ref.block_id) {
        if (entry.valid && entry.block_id != ref.block_id) {
            if (!FlushInodeCacheEntryLocked(shard, slot_idx)) {
                return false;
            }
        }
        inode_block_image loaded{};
        if (!ReadInodeBlockDirect(ref.block_id, loaded)) {
            return false;
        }
        entry.valid = true;
        entry.dirty = false;
        entry.block_id = ref.block_id;
        entry.image = loaded;
    }

    const bool was_used = IsSlotUsed(entry.image.header, ref.offset);
    entry.image.slots[ref.offset] = inode;
    SetSlotUsed(entry.image.header, ref.offset, true);
    if (!was_used && entry.image.header.used_count < kInodesPerBlock) {
        ++entry.image.header.used_count;
    }
    entry.dirty = true;
    SetShardDirtyBit(shard, slot_idx, true);
    return true;
}

bool PersistentStore::FlushHeadPage() const {
    if(!head_dirty_) return true;
    void* aligned=nullptr; if(!alloc_aligned_block(&aligned)) return false;
    std::memcpy(aligned,head_image_.data(),kMetadataBlockBytes);
    bool ok=pwrite_all(heads_fd_,aligned,kMetadataBlockBytes,block_offset(head_page_));
    std::free(aligned); if(ok) head_dirty_=false; return ok;
}
bool PersistentStore::LoadHeadPage(uint64_t page,bool create) const {
    if(page==head_page_) return true;
    if(!FlushHeadPage()) return false;
    struct stat st{}; if(::fstat(heads_fd_,&st)) return false;
    metadata_block_bytes next{};
    if(block_offset(page)<st.st_size) {
        void* aligned=nullptr; if(!alloc_aligned_block(&aligned)) return false;
        bool ok=pread_all(heads_fd_,aligned,kMetadataBlockBytes,block_offset(page));
        if(ok) std::memcpy(next.data(),aligned,kMetadataBlockBytes);
        std::free(aligned); if(!ok) return false;
    } else if(!create || block_offset(page)!=st.st_size) return false;
    head_image_=next; head_page_=page; head_dirty_=false; return true;
}

bool PersistentStore::ReadDirectoryBlock(uint64_t block_id, directory_block_image& out) const {
    if(block_id & kCompactHeadTag) {
        const auto n=(block_id & ~kCompactHeadTag);
        if(!n || n>=next_head_ || !LoadHeadPage((n-1)/31,false)) return false;
        out={}; std::memcpy(&out,head_image_.data()+512+((n-1)%31)*512,512);
        return out.common.magic==kMetadataMagic && out.common.layout_version==kLayoutVersion && out.common.block_id==block_id;
    }
    metadata_block_bytes block;
    if (!ReadBlock(DirectoryPhysicalBlockId(block_id), block)) {
        return false;
    }
    std::memcpy(&out, block.data(), sizeof(out));
    if (out.common.magic != kMetadataMagic || out.common.layout_version != kLayoutVersion
        || out.common.block_type != static_cast<uint16_t>(metadata_block_type::directory_block)) {
        return false;
    }
    return true;
}

bool PersistentStore::WriteDirectoryBlock(uint64_t block_id,
                                          const directory_block_image& image) {
    if(block_id & kCompactHeadTag) {
        const auto n=block_id & ~kCompactHeadTag;
        if(!n || n>=next_head_ || image.header.used_bytes>128 || image.header.entry_count ||
           !LoadHeadPage((n-1)/31,true)) return false;
        std::memcpy(head_image_.data()+512+((n-1)%31)*512,&image,512); head_dirty_=true; return true;
    }
    metadata_block_bytes block{};
    std::memcpy(block.data(), &image, sizeof(image));
    return WriteBlock(DirectoryPhysicalBlockId(block_id), block);
}

bool PersistentStore::ReadDirectoryInode(uint64_t head_block_id, inode_disk& out) const {
    directory_block_image image{};
    if (!ReadDirectoryBlock(head_block_id, image)) {
        return false;
    }
    if ((image.header.flags & kDirectoryBlockFlagHasEmbeddedInode) == 0) {
        return false;
    }
    out = image.directory_inode;
    return true;
}

bool PersistentStore::WriteDirectoryInode(uint64_t head_block_id,
                                          const inode_disk& inode) {
    directory_block_image image{};
    if (!ReadDirectoryBlock(head_block_id, image)) {
        return false;
    }
    image.directory_inode = inode;
    image.header.dir_inode_id = inode.inode_id;
    image.header.flags |= kDirectoryBlockFlagHasEmbeddedInode;
    image.common.owner_inode_id = inode.inode_id;
    image.common.sequence = NextSequenceLocked();
    return WriteDirectoryBlock(head_block_id, image);
}

bool PersistentStore::CreateDirectoryChain(uint64_t dir_inode_id, inode_ref& out_head_ref, bool compact_head) {
    out_head_ref = make_inode_ref(0, 0);
    if (!IsOpen()) {
        return false;
    }

    std::lock_guard<std::mutex> guard(inode_alloc_mu_);
    uint64_t block_id = 0;
    if(compact_head) {
        if(next_head_>=kCompactHeadTag) return false;
        block_id=kCompactHeadTag|next_head_++;
    } else if (!AllocateDirectoryBlockLocked(block_id)) {
        return false;
    }

    directory_block_image image{};
    InitDirectoryBlockImage(image, block_id, dir_inode_id, 0, 0, 1, NextSequenceLocked());
    if (!WriteDirectoryBlock(block_id, image)) {
        return false;
    }

    out_head_ref = make_inode_ref(block_id, 0);
    return true;
}

bool PersistentStore::AppendDirectoryChainBlock(const inode_ref& tail_ref,
                                                uint64_t dir_inode_id,
                                                inode_ref& out_new_tail_ref) {
    out_new_tail_ref = make_inode_ref(0, 0);
    if (!IsOpen() || tail_ref.block_id == 0 || tail_ref.offset != 0) {
        return false;
    }

    std::lock_guard<std::mutex> guard(inode_alloc_mu_);

    directory_block_image tail{};
    if (!ReadDirectoryBlock(tail_ref.block_id, tail)) {
        return false;
    }
    if (tail.header.dir_inode_id != dir_inode_id) {
        return false;
    }
    if (tail.header.next_block_id != 0) {
        return false;
    }

    uint64_t new_block_id = 0;
    if (!AllocateDirectoryBlockLocked(new_block_id)) {
        return false;
    }

    directory_block_image fresh{};
    InitDirectoryBlockImage(fresh, new_block_id, dir_inode_id, tail_ref.block_id, 0,
                            tail.header.base_version, NextSequenceLocked());
    if (!WriteDirectoryBlock(new_block_id, fresh)) {
        return false;
    }

    tail.header.next_block_id = new_block_id;
    tail.common.sequence = NextSequenceLocked();
    if (!WriteDirectoryBlock(tail_ref.block_id, tail)) {
        return false;
    }

    out_new_tail_ref = make_inode_ref(new_block_id, 0);
    return true;
}

uint64_t PersistentStore::NextAllocBlock() const {
    return next_alloc_block_.load(std::memory_order_relaxed);
}

const std::string& PersistentStore::BackingFile() const {
    return path_;
}

void PersistentStore::ResetStats() {
    stats_block_read_ops_.store(0, std::memory_order_relaxed);
    stats_block_write_ops_.store(0, std::memory_order_relaxed);
    stats_bytes_read_.store(0, std::memory_order_relaxed);
    stats_bytes_written_.store(0, std::memory_order_relaxed);
    stats_sync_ops_.store(0, std::memory_order_relaxed);
    stats_fsync_ops_.store(0, std::memory_order_relaxed);
    stats_allocate_block_ops_.store(0, std::memory_order_relaxed);
    stats_l0_flush_ops_.store(0, std::memory_order_relaxed);
    stats_inode_cache_flush_ops_.store(0, std::memory_order_relaxed);
    stats_block_read_total_time_ns_.store(0, std::memory_order_relaxed);
    stats_block_read_alloc_time_ns_.store(0, std::memory_order_relaxed);
    stats_block_read_pread_time_ns_.store(0, std::memory_order_relaxed);
    stats_block_read_copy_free_time_ns_.store(0, std::memory_order_relaxed);
    stats_inode_block_read_ops_.store(0, std::memory_order_relaxed);
    stats_inode_block_read_time_ns_.store(0, std::memory_order_relaxed);
    stats_directory_block_read_ops_.store(0, std::memory_order_relaxed);
    stats_directory_block_read_time_ns_.store(0, std::memory_order_relaxed);
    stats_other_block_read_ops_.store(0, std::memory_order_relaxed);
    stats_other_block_read_time_ns_.store(0, std::memory_order_relaxed);
    for (auto& bucket : stats_block_read_pread_latency_buckets_) {
        bucket.store(0, std::memory_order_relaxed);
    }
}

PersistentStore::stats_snapshot PersistentStore::Stats() const {
    stats_snapshot out;
    out.block_read_ops = stats_block_read_ops_.load(std::memory_order_relaxed);
    out.block_write_ops = stats_block_write_ops_.load(std::memory_order_relaxed);
    out.bytes_read = stats_bytes_read_.load(std::memory_order_relaxed);
    out.bytes_written = stats_bytes_written_.load(std::memory_order_relaxed);
    out.sync_ops = stats_sync_ops_.load(std::memory_order_relaxed);
    out.fsync_ops = stats_fsync_ops_.load(std::memory_order_relaxed);
    out.allocate_block_ops = stats_allocate_block_ops_.load(std::memory_order_relaxed);
    out.l0_flush_ops = stats_l0_flush_ops_.load(std::memory_order_relaxed);
    out.inode_cache_flush_ops =
        stats_inode_cache_flush_ops_.load(std::memory_order_relaxed);
    out.block_read_total_time_ns =
        stats_block_read_total_time_ns_.load(std::memory_order_relaxed);
    out.block_read_alloc_time_ns =
        stats_block_read_alloc_time_ns_.load(std::memory_order_relaxed);
    out.block_read_pread_time_ns =
        stats_block_read_pread_time_ns_.load(std::memory_order_relaxed);
    out.block_read_copy_free_time_ns =
        stats_block_read_copy_free_time_ns_.load(std::memory_order_relaxed);
    out.inode_block_read_ops =
        stats_inode_block_read_ops_.load(std::memory_order_relaxed);
    out.inode_block_read_time_ns =
        stats_inode_block_read_time_ns_.load(std::memory_order_relaxed);
    out.directory_block_read_ops =
        stats_directory_block_read_ops_.load(std::memory_order_relaxed);
    out.directory_block_read_time_ns =
        stats_directory_block_read_time_ns_.load(std::memory_order_relaxed);
    out.other_block_read_ops =
        stats_other_block_read_ops_.load(std::memory_order_relaxed);
    out.other_block_read_time_ns =
        stats_other_block_read_time_ns_.load(std::memory_order_relaxed);
    for (std::size_t i = 0; i < out.block_read_pread_latency_buckets.size(); ++i) {
        out.block_read_pread_latency_buckets[i] =
            stats_block_read_pread_latency_buckets_[i].load(std::memory_order_relaxed);
    }
    return out;
}

bool PersistentStore::EnsureBlockCount(uint64_t block_count) {
    off_t target_size = block_offset(block_count);
    struct stat st;
    if (::fstat(fd_, &st) != 0) {
        return false;
    }
    if (st.st_size >= target_size) {
        return true;
    }
    return ::ftruncate(fd_, target_size) == 0;
}

bool PersistentStore::InitializeFreshStore() {
    if (!EnsureBlockCount(1)) {
        return false;
    }

    superblock_payload sb{};
    sb.metadata_block_bytes = kMetadataBlockBytes;
    sb.latest_checkpoint_block = 0;
    sb.latest_checkpoint_seq = 0;
    sb.fs_uuid_hi = 0;
    sb.fs_uuid_lo = 0;
    sb.flags = 0;
    if (!WriteSuperblock(sb)) {
        return false;
    }

    next_alloc_block_.store(1, std::memory_order_relaxed);
    next_directory_block_.store(1, std::memory_order_relaxed);
    sequence_ = 1;
    next_inode_ticket_.store(0, std::memory_order_relaxed);
    highest_initialized_inode_block_.store(0, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::RecomputeNextAllocBlock() {
    struct stat st;
    if (::fstat(fd_, &st) != 0) {
        return false;
    }
    if (st.st_size < 0) {
        return false;
    }
    uint64_t bytes = static_cast<uint64_t>(st.st_size);
    uint64_t blocks = (bytes + kMetadataBlockBytes - 1) / kMetadataBlockBytes;
    if (blocks == 0) {
        next_alloc_block_.store(1, std::memory_order_relaxed);
        return InitializeFreshStore();
    }
    if (blocks > ((uint64_t{1} << 48)-1)) {
        return false;
    }
    next_alloc_block_.store(static_cast<uint64_t>(blocks), std::memory_order_relaxed);

    // 确认 block0 是 superblock，否则拒绝加载。
    metadata_block_bytes block0;
    if (!ReadBlock(0, block0)) {
        return false;
    }
    const auto* common = reinterpret_cast<const metadata_common_block_header*>(block0.data());
    if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
        || common->block_type != static_cast<uint16_t>(metadata_block_type::superblock)) {
        return false;
    }
    sequence_ = common->sequence + 1;
    if (!RecomputeLinearInodeCursor()) {
        return false;
    }
    return RecomputeNextDirectoryBlock();
}

bool PersistentStore::RecomputeLinearInodeCursor() {
    uint64_t next_ticket = 0;
    uint64_t highest_inode_block = 0;

    // 启动阶段允许一次线性扫描，用于恢复“最后一个 inode block + 下一个 slot”游标。
    const uint64_t total_blocks = next_alloc_block_.load(std::memory_order_relaxed);
    const uint64_t max_inode_block = MaxLogicalInodeBlockId(total_blocks);
    for (uint64_t b = 1; b <= max_inode_block; ++b) {
        metadata_block_bytes raw;
        if (!ReadBlock(InodePhysicalBlockId(b), raw)) {
            continue;
        }
        const auto* common =
            reinterpret_cast<const metadata_common_block_header*>(raw.data());
        if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
            || common->block_type != static_cast<uint16_t>(metadata_block_type::inode_block)) {
            continue;
        }

        inode_block_image image;
        if (!ReadInodeBlock(b, image)) {
            return false;
        }
        highest_inode_block = std::max(highest_inode_block, b);
        uint64_t used = image.header.used_count;
        if (used > kInodesPerBlock) {
            used = kInodesPerBlock;
        }
        uint64_t candidate =
            static_cast<uint64_t>(b - 1) * static_cast<uint64_t>(kInodesPerBlock)
            + static_cast<uint64_t>(used);
        if (candidate > next_ticket) {
            next_ticket = candidate;
        }
    }
    next_inode_ticket_.store(next_ticket, std::memory_order_relaxed);
    highest_initialized_inode_block_.store(highest_inode_block, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::RecomputeNextDirectoryBlock() {
    uint64_t highest_directory_block = 0;
    const uint64_t total_blocks = next_alloc_block_.load(std::memory_order_relaxed);
    const uint64_t max_directory_block = MaxLogicalDirectoryBlockId(total_blocks);
    for (uint64_t b = 1; b <= max_directory_block; ++b) {
        metadata_block_bytes raw;
        if (!ReadBlock(DirectoryPhysicalBlockId(b), raw)) {
            continue;
        }
        const auto* common =
            reinterpret_cast<const metadata_common_block_header*>(raw.data());
        if (common->magic != kMetadataMagic || common->layout_version != kLayoutVersion
            || common->block_type != static_cast<uint16_t>(metadata_block_type::directory_block)) {
            continue;
        }
        highest_directory_block = std::max(highest_directory_block, b);
        if (common->sequence >= sequence_) {
            sequence_ = common->sequence + 1;
        }
    }
    next_directory_block_.store(highest_directory_block + 1, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::AllocateBlockLocked(uint64_t& out_block_id) {
    out_block_id = next_alloc_block_.load(std::memory_order_relaxed);
    next_alloc_block_.store(out_block_id + 1, std::memory_order_relaxed);
    const bool ok = EnsureBlockCount(out_block_id + 1);
    if (ok) {
        stats_allocate_block_ops_.fetch_add(1, std::memory_order_relaxed);
    }
    return ok;
}

bool PersistentStore::AllocateDirectoryBlockLocked(uint64_t& out_block_id) {
    out_block_id = next_directory_block_.load(std::memory_order_relaxed);
    if(out_block_id>kMaxLogicalBlock) return false;
    next_directory_block_.store(out_block_id + 1, std::memory_order_relaxed);
    const uint64_t physical_block_id = DirectoryPhysicalBlockId(out_block_id);
    const bool ok = EnsureBlockCount(physical_block_id + 1);
    if (ok) {
        stats_allocate_block_ops_.fetch_add(1, std::memory_order_relaxed);
    }
    return ok;
}

uint64_t PersistentStore::NextSequenceLocked() {
    return sequence_++;
}

void PersistentStore::InitDirectoryBlockImage(directory_block_image& out,
                                              uint64_t block_id,
                                              uint64_t dir_inode_id,
                                              uint64_t prev_block_id,
                                              uint64_t next_block_id,
                                              uint64_t base_version,
                                              uint64_t sequence) {
    std::memset(&out, 0, sizeof(out));
    BuildCommonHeader(out.common, metadata_block_type::directory_block, block_id,
                      sizeof(directory_block_header), sequence,
                      dir_inode_id, next_block_id, 0);
    out.header.dir_inode_id = dir_inode_id;
    out.header.entry_count = 0;
    out.header.used_bytes = 0;
    out.header.base_version = base_version;
    out.header.prev_block_id = prev_block_id;
    out.header.next_block_id = next_block_id;
    out.header.delta_count = 0;
    out.header.flags = 0;
    std::memset(out.header.reserved, 0, sizeof(out.header.reserved));
    std::memset(&out.directory_inode, 0, sizeof(out.directory_inode));
    std::memset(out.payload, 0, sizeof(out.payload));
}

bool PersistentStore::EnsureInodeBlocksInitializedLocked(uint64_t target_block_id) {
    uint64_t initialized = highest_initialized_inode_block_.load(std::memory_order_relaxed);
    while (initialized < target_block_id) {
        const uint64_t block_id = initialized + 1;
        inode_block_image fresh{};
        BuildCommonHeader(fresh.common, metadata_block_type::inode_block, block_id,
                          sizeof(inode_block_header) + kInodesPerBlock * sizeof(inode_disk),
                          NextSequenceLocked(), 0, 0, 0);
        fresh.header.slot_count = kInodesPerBlock;
        fresh.header.used_count = 0;
        fresh.header.flags = 0;
        fresh.header.reserved0 = 0;
        fresh.header.alloc_epoch = sequence_;
        std::memset(fresh.header.slot_bitmap, 0, sizeof(fresh.header.slot_bitmap));
        std::memset(fresh.slots, 0, sizeof(fresh.slots));
        if (!WriteInodeBlock(block_id, fresh)) {
            return false;
        }
        initialized = block_id;
    }
    highest_initialized_inode_block_.store(initialized, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::AcquireInodeTicketRange(uint64_t& begin_ticket, uint64_t& end_ticket) {
    begin_ticket = 0;
    end_ticket = 0;
    if (!IsOpen()) {
        return false;
    }

    inode_chunk_cache& tls = tls_inode_chunk_;
    const uint64_t epoch = inode_alloc_epoch_.load(std::memory_order_relaxed);
    if (tls.owner != this || tls.epoch != epoch || tls.next_ticket >= tls.end_ticket) {
        const uint64_t begin =
            next_inode_ticket_.fetch_add(kInodeAllocChunk, std::memory_order_relaxed);
        const uint64_t end = begin + kInodeAllocChunk;
        if (end <= begin || end > kMaxLogicalBlock*kInodesPerBlock) {
            return false;
        }

        const uint64_t target_block_id = InodeBlockIdFromTicket(end - 1);
        {
            std::lock_guard<std::mutex> guard(inode_alloc_mu_);
            if (!EnsureInodeBlocksInitializedLocked(target_block_id)) {
                return false;
            }
        }

        tls.owner = this;
        tls.epoch = epoch;
        tls.next_ticket = begin;
        tls.end_ticket = end;
    }

    begin_ticket = tls.next_ticket;
    end_ticket = tls.end_ticket;
    ++tls.next_ticket;
    return true;
}

PersistentStore::l0_thread_buffer* PersistentStore::GetOrCreateL0ThreadBuffer() {
    const uint64_t epoch = l0_epoch_.load(std::memory_order_relaxed);
    l0_tls_handle& tls = tls_l0_handle_;
    if (tls.owner == this && tls.epoch == epoch && tls.buffer != nullptr) {
        return tls.buffer;
    }

    if (tls.owner == this && tls.buffer != nullptr) {
        std::lock_guard<std::mutex> buf_guard(tls.buffer->mu);
        tls.buffer->has_active_block = false;
        tls.buffer->active_dirty = false;
        tls.buffer->active_block_id = 0;
        tls.buffer->pending_ops = 0;
        std::memset(&tls.buffer->active_image, 0, sizeof(tls.buffer->active_image));
        tls.epoch = epoch;
        return tls.buffer;
    }

    l0_thread_buffer* fresh = new l0_thread_buffer();
    {
        std::lock_guard<std::mutex> guard(l0_buffers_mu_);
        l0_buffers_.push_back(fresh);
    }
    tls.owner = this;
    tls.epoch = epoch;
    tls.buffer = fresh;
    return fresh;
}

PersistentStore::l0_thread_buffer* PersistentStore::GetL0ThreadBufferIfValid() const {
    const uint64_t epoch = l0_epoch_.load(std::memory_order_relaxed);
    l0_tls_handle& tls = tls_l0_handle_;
    if (tls.owner == this && tls.epoch == epoch && tls.buffer != nullptr) {
        return tls.buffer;
    }
    return nullptr;
}

bool PersistentStore::FlushL0ThreadBufferLocked(l0_thread_buffer& buffer, bool keep_active_block) {
    bool flushed = false;
    if (buffer.has_active_block && buffer.active_dirty) {
        if (!WriteInodeBlock(buffer.active_block_id, buffer.active_image)) {
            return false;
        }
        flushed = true;
    }
    buffer.active_dirty = false;
    buffer.pending_ops = 0;
    if (!keep_active_block) {
        buffer.has_active_block = false;
        buffer.active_block_id = 0;
        std::memset(&buffer.active_image, 0, sizeof(buffer.active_image));
    }
    if (flushed) {
        stats_l0_flush_ops_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

bool PersistentStore::FlushAllL0Buffers() {
    std::vector<l0_thread_buffer*> buffers;
    {
        std::lock_guard<std::mutex> guard(l0_buffers_mu_);
        buffers = l0_buffers_;
    }
    for (l0_thread_buffer* buffer : buffers) {
        if (!buffer) {
            continue;
        }
        std::lock_guard<std::mutex> buf_guard(buffer->mu);
        if (!FlushL0ThreadBufferLocked(*buffer, true)) {
            return false;
        }
    }
    return true;
}

void PersistentStore::ResetAllL0Buffers() {
    std::vector<l0_thread_buffer*> buffers;
    {
        std::lock_guard<std::mutex> guard(l0_buffers_mu_);
        buffers = l0_buffers_;
    }
    for (l0_thread_buffer* buffer : buffers) {
        if (!buffer) {
            continue;
        }
        std::lock_guard<std::mutex> buf_guard(buffer->mu);
        buffer->has_active_block = false;
        buffer->active_dirty = false;
        buffer->active_block_id = 0;
        buffer->pending_ops = 0;
        std::memset(&buffer->active_image, 0, sizeof(buffer->active_image));
    }
}

bool PersistentStore::ReadInodeBlockDirect(uint64_t block_id, inode_block_image& out) const {
    metadata_block_bytes block;
    if (!ReadBlock(InodePhysicalBlockId(block_id), block)) {
        return false;
    }
    std::memcpy(&out, block.data(), sizeof(out));
    if (out.common.magic != kMetadataMagic || out.common.layout_version != kLayoutVersion
        || out.common.block_type != static_cast<uint16_t>(metadata_block_type::inode_block)) {
        return false;
    }
    return true;
}

bool PersistentStore::WriteInodeBlockDirect(uint64_t block_id, const inode_block_image& image) {
    metadata_block_bytes block{};
    std::memcpy(block.data(), &image, sizeof(image));
    return WriteBlock(InodePhysicalBlockId(block_id), block);
}

bool PersistentStore::FlushInodeCacheEntryLocked(inode_block_cache_shard& shard,
                                                 std::size_t slot_index) {
    inode_block_cache_entry& entry = shard.entries[slot_index];
    if (!entry.valid || !entry.dirty) {
        SetShardDirtyBit(shard, slot_index, false);
        return true;
    }
    if (!WriteInodeBlockDirect(entry.block_id, entry.image)) {
        return false;
    }
    entry.dirty = false;
    SetShardDirtyBit(shard, slot_index, false);
    stats_inode_cache_flush_ops_.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool PersistentStore::FlushInodeCacheShard(std::size_t shard_index) {
    inode_block_cache_shard& shard = inode_block_cache_[shard_index];
    std::lock_guard<std::mutex> guard(shard.mu);
    for (std::size_t slot_index = 0; slot_index < shard.entries.size(); ++slot_index) {
        if (!IsShardDirtyBitSet(shard, slot_index)) {
            continue;
        }
        if (!FlushInodeCacheEntryLocked(shard, slot_index)) {
            return false;
        }
    }
    return true;
}

bool PersistentStore::FlushInodeCache() {
    for (std::size_t i = 0; i < inode_block_cache_.size(); ++i) {
        if (!FlushInodeCacheShard(i)) {
            return false;
        }
    }
    return true;
}

void PersistentStore::ResetInodeCache() {
    for (inode_block_cache_shard& shard : inode_block_cache_) {
        std::lock_guard<std::mutex> guard(shard.mu);
        for (inode_block_cache_entry& entry : shard.entries) {
            entry.valid = false;
            entry.dirty = false;
            entry.block_id = 0;
            std::memset(&entry.image, 0, sizeof(entry.image));
        }
        shard.dirty_bitmap.fill(0);
    }
}

void PersistentStore::SetShardDirtyBit(inode_block_cache_shard& shard,
                                       std::size_t slot_index,
                                       bool dirty) {
    const std::size_t word_index = slot_index / 64;
    const std::size_t bit_index = slot_index % 64;
    const uint64_t mask = static_cast<uint64_t>(1) << bit_index;
    if (dirty) {
        shard.dirty_bitmap[word_index] |= mask;
    } else {
        shard.dirty_bitmap[word_index] &= ~mask;
    }
}

bool PersistentStore::IsShardDirtyBitSet(const inode_block_cache_shard& shard,
                                         std::size_t slot_index) {
    const std::size_t word_index = slot_index / 64;
    const std::size_t bit_index = slot_index % 64;
    const uint64_t mask = static_cast<uint64_t>(1) << bit_index;
    return (shard.dirty_bitmap[word_index] & mask) != 0;
}

std::size_t PersistentStore::InodeCacheShardIndex(uint64_t block_id) {
    static_assert((kInodeCacheShardCount & (kInodeCacheShardCount - 1)) == 0,
                  "inode cache shard count must be power of two");
    const uint64_t mixed = block_id * 2654435761u;
    return static_cast<std::size_t>(mixed & static_cast<uint64_t>(kInodeCacheShardCount - 1));
}

std::size_t PersistentStore::InodeCacheSlotIndex(uint64_t block_id) {
    static_assert((kInodeCacheEntriesPerShard & (kInodeCacheEntriesPerShard - 1)) == 0,
                  "inode cache entries per shard must be power of two");
    const uint64_t mixed = (block_id ^ (block_id >> 16)) * 2246822519u;
    return static_cast<std::size_t>(
        (mixed >> 1) & static_cast<uint64_t>(kInodeCacheEntriesPerShard - 1));
}

uint64_t PersistentStore::InodeBlockIdFromTicket(uint64_t ticket) {
    return 1u + static_cast<uint64_t>(ticket / static_cast<uint64_t>(kInodesPerBlock));
}

uint64_t PersistentStore::InodeSlotFromTicket(uint64_t ticket) {
    return static_cast<uint64_t>(ticket % static_cast<uint64_t>(kInodesPerBlock));
}

uint64_t PersistentStore::InodePhysicalBlockId(uint64_t inode_block_id) {
    return inode_block_id * 2u - 1u;
}

uint64_t PersistentStore::DirectoryPhysicalBlockId(uint64_t directory_block_id) {
    return directory_block_id * 2u;
}

uint64_t PersistentStore::MaxLogicalInodeBlockId(uint64_t physical_block_count) {
    return physical_block_count / 2u;
}

uint64_t PersistentStore::MaxLogicalDirectoryBlockId(uint64_t physical_block_count) {
    if (physical_block_count == 0) {
        return 0;
    }
    return (physical_block_count - 1u) / 2u;
}

bool PersistentStore::IsKnownBlockType(uint16_t block_type) const {
    return block_type >= static_cast<uint16_t>(metadata_block_type::superblock)
        && block_type <= static_cast<uint16_t>(metadata_block_type::allocator_state);
}

bool PersistentStore::IsSlotUsed(const inode_block_header& header, uint64_t slot) {
    if (slot >= kInodesPerBlock) {
        return false;
    }
    uint64_t byte_index = slot / 8;
    uint64_t bit_index = slot % 8;
    return (header.slot_bitmap[byte_index] & static_cast<uint8_t>(1u << bit_index)) != 0;
}

void PersistentStore::SetSlotUsed(inode_block_header& header, uint64_t slot, bool used) {
    if (slot >= kInodesPerBlock) {
        return;
    }
    uint64_t byte_index = slot / 8;
    uint64_t bit_index = slot % 8;
    uint8_t mask = static_cast<uint8_t>(1u << bit_index);
    if (used) {
        header.slot_bitmap[byte_index] |= mask;
    } else {
        header.slot_bitmap[byte_index] &= static_cast<uint8_t>(~mask);
    }
}

void PersistentStore::BuildCommonHeader(metadata_common_block_header& out,
                                        metadata_block_type block_type,
                                        uint64_t block_id,
                                        uint64_t payload_bytes,
                                        uint64_t sequence,
                                        uint64_t owner_inode_id,
                                        uint64_t next_block_id,
                                        uint64_t flags) {
    std::memset(&out, 0, sizeof(out));
    out.magic = kMetadataMagic;
    out.block_type = static_cast<uint16_t>(block_type);
    out.layout_version = kLayoutVersion;
    out.block_id = block_id;
    out.payload_bytes = payload_bytes;
    out.checksum = 0;
    out.sequence = sequence;
    out.owner_inode_id = owner_inode_id;
    out.next_block_id = next_block_id;
    out.flags = flags;
}

// Called only by the fresh-store factory before publishing the namespace.
bool PersistentStore::WriteFreshFileInodes(std::vector<inode_disk>& inodes) {
    auto& tls = tls_inode_chunk_;
    if (!IsOpen() || tls.owner != this ||
        tls.epoch != inode_alloc_epoch_.load() || tls.next_ticket != 1 ||
        next_inode_ticket_.load() != kInodeAllocChunk ||
        inodes.size() >= uint64_t((kMaxLogicalBlock-1)) * kInodesPerBlock) return false;
    const bool l0 = l0_cache_enabled();
    if (!set_l0_cache_enabled(false)) return false;
    const uint64_t end = 1 + inodes.size();
    const uint64_t last = InodeBlockIdFromTicket(end - 1);
    const uint64_t initialized = highest_initialized_inode_block_.load();
    for (uint64_t block = 1; block <= last; ++block) {
        inode_block_image image{};
        if (block <= initialized) {
            if (!ReadInodeBlock(block, image)) return false;
        } else {
            BuildCommonHeader(image.common, metadata_block_type::inode_block, block,
                sizeof(inode_block_header) + kInodesPerBlock * sizeof(inode_disk),
                NextSequenceLocked(), 0, 0, 0);
            image.header.slot_count = kInodesPerBlock;
            image.header.alloc_epoch = sequence_;
        }
        for (uint64_t slot = (block == 1 ? 1 : 0); slot < kInodesPerBlock; ++slot) {
            const uint64_t ticket = uint64_t(block - 1) * kInodesPerBlock + slot;
            if (ticket >= end) break;
            auto& inode = inodes[ticket - 1];
            inode.primary_ref = make_inode_ref(block, slot);
            inode.inode_id = (uint64_t(block) << 16) | slot;
            image.slots[slot] = inode;
            if (!IsSlotUsed(image.header, slot)) ++image.header.used_count;
            SetSlotUsed(image.header, slot, true);
        }
        if (!WriteInodeBlock(block, image)) return false;
    }
    highest_initialized_inode_block_.store(std::max(initialized, last));
    next_inode_ticket_.store(end);
    inode_alloc_epoch_.fetch_add(1); // invalidate the root's reserved TLS chunk
    return Sync() && set_l0_cache_enabled(l0);
}

}  // namespace MasstreeLHM
