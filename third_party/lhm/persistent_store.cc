#include "persistent_store.hh"

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <filesystem>
#include <charconv>
#include <stdexcept>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

namespace MasstreeLHM {

namespace {

struct UndoWriteResult {
    BatchUndo* undo;
    bool ok=false;
    ~UndoWriteResult() { if(undo && !ok) undo->Poison(); }
};

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

bool pwrite_all(int fd, const void* buf, size_t bytes, off_t off, uint64_t* calls=nullptr) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    size_t done = 0;
    while (done < bytes) {
        if(calls) ++*calls;
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

std::string store_parent(const std::string& path) {
    const auto parent=std::filesystem::path(path).parent_path();
    return parent.empty()?std::string("."):parent.string();
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

struct PersistentStore::bulk_write_state {
    static constexpr size_t kBufferBytes = 4 * 1024 * 1024;
    struct buffer {
        uint64_t first_page = 0;
        std::vector<uint8_t> bytes;
    };
    BulkReservation reservation;
    uint64_t received[3]{};
    buffer buffers[3];
    bool failed = false;
};

struct PersistentStore::segment_state {
    int fd=-1, direct_flags=0;
    uint64_t bytes=0;
    mutable std::atomic<uint64_t> dirty_epoch{0}, synced_epoch{0};
    uint64_t next_writeback=0, pending_offset=0, pending_bytes=0;
    bool writeback_initialized=false, writeback_supported=true;
    ~segment_state() { if(fd>=0) ::close(fd); }
};

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
    Close(); ResetStats(); bulk_stats_={}; path_=path;
    try {
        int fd=::open(path.c_str(),O_RDWR|O_CREAT|O_DIRECT|O_CLOEXEC,0644);
        if(fd<0 || !AddSegment(0,0,fd)) { Close(); return false; }
        struct stat st{};
        if(::fstat(fd_,&st) || !S_ISREG(st.st_mode) || st.st_size<0) { Close(); return false; }
        const bool fresh=st.st_size==0;
        if(fresh) {
            segment_bytes_=kDefaultSegmentBytes;
#ifdef ZB_LHM_TESTING
            if(const char* value=std::getenv("ZB_LHM_TEST_SEGMENT_BYTES")) {
                uint64_t requested=0;
                const auto end=value+std::strlen(value);
                const auto parsed=std::from_chars(value,end,requested);
                if(parsed.ec!=std::errc{} || parsed.ptr!=end || !ValidSegmentBytes(requested)) {
                    Close(); return false;
                }
                segment_bytes_=requested;
            }
#endif
            // A fresh database must not adopt any leftover data segment.
            const std::filesystem::path anchor(path);
            const auto parent=anchor.has_parent_path()?anchor.parent_path():std::filesystem::path(".");
            for(const auto& item:std::filesystem::directory_iterator(parent)) {
                const auto name=item.path().filename().string();
                for(unsigned kind=0;kind<3;++kind) {
                    const auto prefix=std::filesystem::path(SegmentBasePath(path,kind)).filename().string()+".part";
                    if(name.compare(0,prefix.size(),prefix)==0) { Close(); return false; }
                }
            }
            for(unsigned kind=0;kind<3;++kind) {
                if(kind) {
                    int side=::open(SegmentPath(path,kind,0).c_str(),O_RDWR|O_CREAT|O_EXCL|O_DIRECT|O_CLOEXEC|O_NOFOLLOW,0600);
                    if(side<0 || !AddSegment(kind,0,side)) { Close(); return false; }
                    MarkFileDirty(kind);
                }
            }
            segment_directory_dirty_.store(true);
            if(!InitializeFreshStore()) { Close(); return false; }
            return true;
        }
        // Read page zero before interpreting any later address. Legacy stores
        // are rejected without creating or opening their other files.
        superblock_payload sb{};
        if(!ReadSuperblock(sb) || sb.metadata_block_bytes!=kMetadataBlockBytes ||
           !(sb.flags & kSuperblockFlagSplitFiles) || !(sb.flags & kSuperblockFlagSegmentedFiles) ||
           sb.segment_layout_version!=1 || !ValidSegmentBytes(sb.segment_bytes)) { Close(); return false; }
        segment_bytes_=sb.segment_bytes;
        const auto layout=ScanSegmentLayout(path,segment_bytes_);
        for(const auto& item:layout.segments) {
            if(item.kind==0 && item.index==0) continue;
            int side=::open(SegmentPath(path,unsigned(item.kind),item.index).c_str(),O_RDWR|O_DIRECT|O_CLOEXEC);
            if(side<0 || !AddSegment(unsigned(item.kind),item.index,side)) { Close(); return false; }
        }
        const auto opened=CurrentSegmentLayout();
        ValidateSegmentFiles(path,opened);
        if(!(opened==layout)) { Close(); return false; }
        if(from_snapshot) return true;
        if(!RecomputeNextAllocBlock()) { Close(); return false; }
        return true;
    } catch(...) { Close(); return false; }
}

void PersistentStore::SaveAllocation(SnapshotWriter& out) const {
    if (bulk_write_) throw std::runtime_error("cannot snapshot unfinished template writes");
    const auto layout=CurrentSegmentLayout();
    ValidateSegmentFiles(path_,layout);
    WriteSegmentLayout(out,layout);
    out.u64(next_alloc_block_.load()); out.u64(next_directory_block_.load());
    out.u64(sequence_); out.u64(next_inode_ticket_.load());
    out.u64(highest_initialized_inode_block_.load()); out.u64(next_head_);
}
void PersistentStore::LoadAllocation(SnapshotReader& in) {
    const auto layout=ReadSegmentLayout(in);
    if(layout.segment_bytes!=segment_bytes_) throw std::runtime_error("snapshot segment size differs from superblock");
    ValidateSegmentFiles(path_,layout);
    const auto actual=CurrentSegmentLayout();
    if(!(layout==actual)) throw std::runtime_error("snapshot segment inventory differs from store");
    auto blocks=in.u64(), dirs=in.u64(); auto seq=in.u64(), tickets=in.u64(), high=in.u64(), heads=in.u64();
    if (!blocks || !dirs || !seq || !tickets || !high || dirs>(kMaxLogicalBlock+1) ||
        high>kMaxLogicalBlock || tickets>uint64_t(kMaxLogicalBlock)*kInodesPerBlock ||
        !heads || heads>kCompactHeadTag || dirs-1>layout.logical_bytes[2]/kMetadataBlockBytes ||
        layout.logical_bytes[0]<kMetadataBlockBytes || blocks!=layout.logical_bytes[0]/kMetadataBlockBytes || high>=blocks ||
        (heads-1+30)/31!=layout.logical_bytes[1]/kMetadataBlockBytes)
        throw std::runtime_error("invalid snapshot allocation state");
    next_head_=heads;
    next_alloc_block_.store(blocks); next_directory_block_.store(dirs); sequence_=seq;
    next_inode_ticket_.store(tickets); highest_initialized_inode_block_.store(high);
    clean_snapshot_loaded_.store(true, std::memory_order_relaxed);
}

void PersistentStore::Close() {
    // Incomplete buffers contain only uncommitted new pages. Recovery owns the
    // journal and newly published segments after all cached state is discarded.
    if(bulk_write_) {
        bulk_write_.reset(); ResetAllL0Buffers(); ResetInodeCache(); head_dirty_=false;
    }
    clean_snapshot_loaded_.store(false);
    if(fd_>=0) {
        if(l0_cache_enabled_.load()) (void)FlushAllL0Buffers();
        (void)FlushInodeCache();
    }
    if(heads_fd_>=0) (void)FlushHeadPage();
    for(unsigned kind=0;kind<3;++kind) {
        if(!segments_[kind].empty()) (void)SyncFileIfDirty(kind);
        segments_[kind].clear(); logical_file_bytes_[kind].store(0);
    }
    if(segment_directory_dirty_.exchange(false) && !path_.empty()) {
        int parent=::open(store_parent(path_).c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);
        if(parent>=0) { (void)::fsync(parent); ::close(parent); }
    }
    fd_=heads_fd_=buckets_fd_=-1; buffered_mode_=false; segment_bytes_=kDefaultSegmentBytes;
    head_page_=UINT64_MAX; head_dirty_=false; next_head_=1;
    batch_undo_.reset();
    l0_epoch_.fetch_add(1); ResetAllL0Buffers(); ResetInodeCache(); path_.clear();
    next_alloc_block_.store(1); next_directory_block_.store(1); sequence_=1;
    next_inode_ticket_.store(0); highest_initialized_inode_block_.store(0);
    inode_alloc_epoch_.fetch_add(1);
}

bool PersistentStore::IsOpen() const {
    return fd_ >= 0;
}

void PersistentStore::BeginBatchUndo(bool service) {
    if (!IsOpen() || batch_undo_ || head_dirty_)
        throw std::runtime_error("batch undo requires a newly reopened clean store");
    batch_undo_ = BatchUndo::Begin(path_, highest_initialized_inode_block_.load(),
                                 next_directory_block_.load(),service);
}

PersistentStore::BulkReservation PersistentStore::ReserveTemplateBlocks(
    uint64_t inode_pages, uint64_t bucket_pages, uint64_t head_count) {
    if (!IsOpen() || heads_fd_ < 0 || buckets_fd_ < 0 || !batch_undo_ || bulk_write_ ||
        !clean_snapshot_loaded_.load(std::memory_order_relaxed))
        throw std::runtime_error("template reservation requires a clean reopened batch store");
    std::lock_guard<std::mutex> guard(inode_alloc_mu_);
    const uint64_t high = highest_initialized_inode_block_.load();
    const uint64_t tickets = next_inode_ticket_.load();
    const uint64_t bucket_first = next_directory_block_.load();
    if (high > kMaxLogicalBlock || tickets > kMaxLogicalBlock * kInodesPerBlock ||
        !bucket_first || bucket_first > kMaxLogicalBlock + 1 ||
        !next_head_ || next_head_ > kCompactHeadTag || !sequence_)
        throw std::runtime_error("invalid template allocation baseline");

    BulkReservation r;
    r.inode_first = std::max(high, (tickets + kInodesPerBlock - 1) / kInodesPerBlock) + 1;
    r.bucket_first = bucket_first;
    r.head_first = ((next_head_ - 1 + 30) / 31) * 31 + 1;
    if (inode_pages > kMaxLogicalBlock - (r.inode_first - 1) ||
        bucket_pages > kMaxLogicalBlock - (r.bucket_first - 1) ||
        !head_count || r.head_first >= kCompactHeadTag ||
        head_count > kCompactHeadTag - r.head_first)
        throw std::runtime_error("template allocation exceeds reference range");
    r.inode_pages = inode_pages;
    r.bucket_pages = bucket_pages;
    r.head_count = head_count;
    r.head_pages = (head_count + 30) / 31;
    r.sequence_base = sequence_;
    // Each count is already bounded by the 47-bit reference representation.
    const uint64_t sequence_count = inode_pages + bucket_pages + head_count;
    if (sequence_count > std::numeric_limits<uint64_t>::max() - sequence_)
        throw std::runtime_error("template sequence range exhausted");

    const uint64_t main_bytes=LogicalFileBytes(0), head_bytes=LogicalFileBytes(1), bucket_bytes=LogicalFileBytes(2);
    if(main_bytes<kMetadataBlockBytes || main_bytes%kMetadataBlockBytes || head_bytes%kMetadataBlockBytes || bucket_bytes%kMetadataBlockBytes)
        throw std::runtime_error("invalid template store file lengths");
    const uint64_t head_start_page = (r.head_first - 1) / 31;
    if(head_start_page!=head_bytes/kMetadataBlockBytes)
        throw std::runtime_error("compact head cursor/file length mismatch");
    uint64_t main_pages=main_bytes/kMetadataBlockBytes;
    if(inode_pages) main_pages=std::max(main_pages,InodePhysicalBlockId(r.inode_first+inode_pages-1)+1);
    const uint64_t bucket_pages_total=r.bucket_first-1+bucket_pages;
    if(r.bucket_first-1!=bucket_bytes/kMetadataBlockBytes)
        throw std::runtime_error("bucket cursor/file length mismatch");
    const uint64_t head_pages_total=head_start_page+r.head_pages;
    if(main_pages>UINT64_MAX/kMetadataBlockBytes || head_pages_total>UINT64_MAX/kMetadataBlockBytes || bucket_pages_total>UINT64_MAX/kMetadataBlockBytes)
        throw std::runtime_error("template logical allocation overflow");

    // Flush ordinary caches before changing the open-file description's I/O
    // mode. The caller holds the offline store lock throughout this operation.
    if (!Sync()) throw std::runtime_error("sync before template reservation failed");
    auto state = std::make_unique<bulk_write_state>();
    state->reservation = r;
    for (auto& buffer : state->buffers) buffer.bytes.reserve(bulk_write_state::kBufferBytes);
    bulk_stats_={};
    bulk_write_ = std::move(state);
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);
    try {
        if(!SetBufferedMode(true)) throw std::runtime_error("enable buffered template I/O failed");
        if(!EnsureFileBlockCount(0,main_pages) || !EnsureFileBlockCount(1,head_pages_total) ||
           !EnsureFileBlockCount(2,bucket_pages_total))
            throw std::runtime_error("extend template stores failed");
        next_alloc_block_.store(std::max(next_alloc_block_.load(), main_pages));
        next_directory_block_.store(r.bucket_first + bucket_pages);
        if (inode_pages) {
            const uint64_t last_inode = r.inode_first + inode_pages - 1;
            highest_initialized_inode_block_.store(last_inode);
            next_inode_ticket_.store(last_inode * kInodesPerBlock);
        }
        next_head_ = r.head_first + head_count;
        sequence_ += sequence_count;
        inode_alloc_epoch_.fetch_add(1, std::memory_order_relaxed);
        l0_epoch_.fetch_add(1, std::memory_order_relaxed);
        ResetAllL0Buffers();
        ResetInodeCache();
        head_page_ = UINT64_MAX;
        head_dirty_ = false;
        stats_allocate_block_ops_.fetch_add(inode_pages + bucket_pages + r.head_pages,
                                             std::memory_order_relaxed);
        BatchFault("compiled_reserved");
        return r;
    } catch (...) {
        bulk_write_->failed = true;
        throw;
    }
}

void PersistentStore::FlushTemplateBuffer(unsigned file_kind) {
    auto& buffer=bulk_write_->buffers[file_kind];
    if(buffer.bytes.empty()) return;
    const uint64_t pages=buffer.bytes.size()/kMetadataBlockBytes;
    uint64_t done=0;
    while(done<pages) {
        const uint64_t page=buffer.first_page+done, index=SegmentIndex(page);
        const uint64_t local_page=page%(segment_bytes_/kMetadataBlockBytes);
        const uint64_t count=std::min(pages-done,segment_bytes_/kMetadataBlockBytes-local_page);
        const int fd=FileDescriptor(file_kind,index);
        if(fd<0) throw std::runtime_error("template segment is missing");
        for(uint64_t i=0;i<count;++i)
            if(!batch_undo_->BeforeWrite(file_kind,fd,page+i))
                throw std::runtime_error("template undo guard failed: "+batch_undo_->error());
        const uint64_t bytes=count*kMetadataBlockBytes;
        const uint64_t start=monotonic_raw_ns();
        const bool ok=pwrite_all(fd,buffer.bytes.data()+done*kMetadataBlockBytes,size_t(bytes),SegmentOffset(page),&bulk_stats_.write_calls);
        bulk_stats_.write_ns+=monotonic_raw_ns()-start;
        MarkFileDirty(file_kind,index);
        if(!ok) throw std::runtime_error("buffered template segment write failed");
        bulk_stats_.write_bytes+=bytes;
        auto* segment=Segment(file_kind,index);
        if(!segment->writeback_initialized) {
            segment->writeback_initialized=true; segment->next_writeback=uint64_t(SegmentOffset(page));
        }
        AdvanceTemplateWriteback(file_kind,index,uint64_t(SegmentOffset(page))+bytes);
        done+=count;
    }
    stats_block_write_ops_.fetch_add(pages,std::memory_order_relaxed);
    stats_bytes_written_.fetch_add(buffer.bytes.size(),std::memory_order_relaxed);
    buffer.bytes.clear(); BatchFault("compiled_write");
}

void PersistentStore::WriteTemplateBlock(unsigned kind, uint64_t page_index,
                                          const metadata_block_bytes& data) {
    if (!bulk_write_ || bulk_write_->failed)
        throw std::runtime_error("template writer is not active");
    try {
        const auto& r = bulk_write_->reservation;
        const uint64_t limits[] = {r.inode_pages, r.bucket_pages, r.head_pages};
        if (kind > 2 || page_index >= limits[kind] || page_index != bulk_write_->received[kind])
            throw std::runtime_error("template page is outside its reserved sequence");
        auto valid_header = [&](const metadata_common_block_header& common,
                                metadata_block_type type, uint64_t logical) {
            return common.magic == kMetadataMagic && common.layout_version == kLayoutVersion &&
                common.block_type == static_cast<uint16_t>(type) && common.block_id == logical &&
                common.payload_bytes <= kMetadataBlockBytes - kCommonBlockHeaderBytes &&
                common.sequence >= r.sequence_base && common.sequence < sequence_;
        };
        if (kind < 2) {
            metadata_common_block_header common{};
            std::memcpy(&common, data.data(), sizeof(common));
            const uint64_t logical = (kind ? r.bucket_first : r.inode_first) + page_index;
            const auto type = kind ? metadata_block_type::directory_block : metadata_block_type::inode_block;
            if (!valid_header(common, type, logical))
                throw std::runtime_error("invalid relocated template page header");
            if (!kind) {
                inode_block_header header{};
                std::memcpy(&header, data.data() + kCommonBlockHeaderBytes, sizeof(header));
                if (header.slot_count != kInodesPerBlock || header.used_count > kInodesPerBlock)
                    throw std::runtime_error("invalid template inode page occupancy");
            } else {
                directory_block_header header{};
                std::memcpy(&header, data.data() + kCommonBlockHeaderBytes, sizeof(header));
                if (header.used_bytes > kDirectoryBlockPayloadBytes)
                    throw std::runtime_error("invalid template bucket payload length");
            }
        } else {
            const uint64_t records = std::min<uint64_t>(31, r.head_count - page_index * 31);
            for (uint64_t slot = 0; slot < records; ++slot) {
                metadata_common_block_header common{};
                directory_block_header header{};
                const uint8_t* record = data.data() + 512 + slot * 512;
                std::memcpy(&common, record, sizeof(common));
                std::memcpy(&header, record + kCommonBlockHeaderBytes, sizeof(header));
                const uint64_t logical = kCompactHeadTag | (r.head_first + page_index * 31 + slot);
                if (!valid_header(common, metadata_block_type::directory_block, logical) ||
                    header.entry_count || header.used_bytes > 128)
                    throw std::runtime_error("invalid relocated compact-head record");
            }
        }
        const unsigned file_kind = kind == 0 ? 0 : kind == 1 ? 2 : 1;
        const uint64_t physical = kind == 0 ? InodePhysicalBlockId(r.inode_first + page_index) :
            kind == 1 ? DirectoryPhysicalBlockId(r.bucket_first + page_index) :
            (r.head_first - 1) / 31 + page_index;
        auto& buffer = bulk_write_->buffers[file_kind];
        // Each native page kind now has an independent contiguous file; the
        // three buffers allow callers to interleave reads without small writes.
        if (!buffer.bytes.empty() &&
            (physical != buffer.first_page + buffer.bytes.size() / kMetadataBlockBytes ||
             buffer.bytes.size() == bulk_write_state::kBufferBytes))
            FlushTemplateBuffer(file_kind);
        if (buffer.bytes.empty()) buffer.first_page = physical;
        buffer.bytes.insert(buffer.bytes.end(), data.begin(), data.end());
        ++bulk_write_->received[kind];
    } catch (...) {
        bulk_write_->failed = true;
        throw;
    }
}

void PersistentStore::FinishTemplateWrites() {
    if (!bulk_write_ || bulk_write_->failed)
        throw std::runtime_error("template writer is not active");
    try {
        const auto& r = bulk_write_->reservation;
        if (bulk_write_->received[0] != r.inode_pages ||
            bulk_write_->received[1] != r.bucket_pages ||
            bulk_write_->received[2] != r.head_pages)
            throw std::runtime_error("template has unwritten reserved pages");
        for(unsigned kind=0;kind<3;++kind) FlushTemplateBuffer(kind);
        for(unsigned kind=0;kind<3;++kind) {
            const uint64_t start=monotonic_raw_ns();
            const bool ok=SyncFileIfDirty(kind);
            const uint64_t elapsed=monotonic_raw_ns()-start;
            if(kind==0) bulk_stats_.main_sync_ns+=elapsed;
            else if(kind==1) bulk_stats_.heads_sync_ns+=elapsed;
            else bulk_stats_.buckets_sync_ns+=elapsed;
            if(!ok) throw std::runtime_error("sync buffered template writes failed");
        }
        if(!SetBufferedMode(false)) throw std::runtime_error("restore direct template I/O failed");
        // New pages bypassed the ordinary caches. A subsequent normal create
        // must reload its head page and allocate a fresh TLS inode range.
        ResetAllL0Buffers();
        ResetInodeCache();
        head_page_ = UINT64_MAX;
        head_dirty_ = false;
        BatchFault("compiled_synced");
        bulk_write_.reset();
    } catch (...) {
        bulk_write_->failed = true;
        throw;
    }
}

bool PersistentStore::Sync() {
    if(bulk_write_ || !IsOpen() || heads_fd_<0 || buckets_fd_<0) return false;
    if(l0_cache_enabled_.load() && !FlushAllL0Buffers()) return false;
    if(!FlushInodeCache() || !FlushHeadPage()) return false;
    stats_sync_ops_.fetch_add(1);
    for(unsigned kind=0;kind<3;++kind) if(!SyncFileIfDirty(kind)) return false;
    if(segment_directory_dirty_.load()) {
        int parent=::open(store_parent(path_).c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);
        if(parent<0) return false;
        int rc; do { rc=::fsync(parent); } while(rc && errno==EINTR);
        ::close(parent); if(rc) return false;
        segment_directory_dirty_.store(false);
    }
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
    return ReadFileBlock(0,block_id,out);
}

bool PersistentStore::ReadFileBlock(unsigned kind,uint64_t block_id,metadata_block_bytes& out) const {
    const int data_fd=FileDescriptor(kind,SegmentIndex(block_id));
    if (data_fd<0 || block_id>=LogicalFileBytes(kind)/kMetadataBlockBytes) {
        return false;
    }
    const uint64_t total_start_ns = monotonic_raw_ns();
    void* aligned = nullptr;
    if (!alloc_aligned_block(&aligned)) {
        return false;
    }
    const uint64_t alloc_end_ns = monotonic_raw_ns();
    bool ok = pread_all(data_fd, aligned, kMetadataBlockBytes, SegmentOffset(block_id));
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
    return WriteFileBlock(0,block_id,data);
}

bool PersistentStore::WriteFileBlock(unsigned kind,uint64_t block_id,const metadata_block_bytes& data) {
    UndoWriteResult result{batch_undo_.get()};
    if(!IsOpen() || kind>2 || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);
    if (!EnsureFileBlockCount(kind,block_id + 1)) {
        return false;
    }
    const uint64_t index=SegmentIndex(block_id);
    const int data_fd=FileDescriptor(kind,index);
    if(data_fd<0 || (batch_undo_ && !batch_undo_->BeforeWrite(kind,data_fd,block_id))) return false;
    void* aligned = nullptr;
    if (!alloc_aligned_block(&aligned)) {
        return false;
    }
    std::memcpy(aligned, data.data(), kMetadataBlockBytes);
    bool ok = pwrite_all(data_fd, aligned, kMetadataBlockBytes, SegmentOffset(block_id));
    MarkFileDirty(kind,index);
    ::free(aligned);
    if (ok && batch_undo_) BatchFault("data_write");
    if (ok) {
        stats_block_write_ops_.fetch_add(1, std::memory_order_relaxed);
        stats_bytes_written_.fetch_add(kMetadataBlockBytes, std::memory_order_relaxed);
        uint64_t need = kind==0 ? block_id+1 : 0;
        uint64_t cur = next_alloc_block_.load(std::memory_order_relaxed);
        while (cur < need
               && !next_alloc_block_.compare_exchange_weak(cur, need,
                                                           std::memory_order_relaxed,
                                                           std::memory_order_relaxed)) {
        }
    }
    result.ok=ok; return ok;
}

bool PersistentStore::AllocateBlock(uint64_t& out_block_id) {
    if (!IsOpen() || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);
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
    if (bulk_write_) return false;
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
    if (bulk_write_) return false;
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
    if (bulk_write_) return false;
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
    if (!IsOpen() || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);

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
    if (!IsOpen() || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);
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
    if (!IsOpen() || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);

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
    UndoWriteResult result{batch_undo_.get()};
    if(!EnsureFileBlockCount(1,head_page_+1)) return false;
    const uint64_t index=SegmentIndex(head_page_);
    const int fd=FileDescriptor(1,index);
    if(fd<0 || (batch_undo_ && !batch_undo_->BeforeWrite(1,fd,head_page_))) return false;
    void* aligned=nullptr; if(!alloc_aligned_block(&aligned)) return false;
    std::memcpy(aligned,head_image_.data(),kMetadataBlockBytes);
    bool ok=pwrite_all(fd,aligned,kMetadataBlockBytes,SegmentOffset(head_page_));
    MarkFileDirty(1,index);
    if(ok && batch_undo_) BatchFault("data_write");
    std::free(aligned); if(ok) head_dirty_=false; result.ok=ok; return ok;
}
bool PersistentStore::LoadHeadPage(uint64_t page,bool create) const {
    if(page==head_page_) return true;
    if(!FlushHeadPage()) return false;
    const uint64_t total=LogicalFileBytes(1)/kMetadataBlockBytes;
    metadata_block_bytes next{};
    if(page<total) {
        const int fd=FileDescriptor(1,SegmentIndex(page));
        if(fd<0) return false;
        void* aligned=nullptr; if(!alloc_aligned_block(&aligned)) return false;
        bool ok=pread_all(fd,aligned,kMetadataBlockBytes,SegmentOffset(page));
        if(ok) std::memcpy(next.data(),aligned,kMetadataBlockBytes);
        std::free(aligned); if(!ok) return false;
    } else if(!create || page!=total) return false;
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
    if (!block_id || !ReadFileBlock(2,DirectoryPhysicalBlockId(block_id), block)) {
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
    if (bulk_write_) return false;
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);
    if(block_id & kCompactHeadTag) {
        const auto n=block_id & ~kCompactHeadTag;
        if(!n || n>=next_head_ || image.header.used_bytes>128 || image.header.entry_count ||
           !LoadHeadPage((n-1)/31,true)) return false;
        std::memcpy(head_image_.data()+512+((n-1)%31)*512,&image,512); head_dirty_=true; return true;
    }
    metadata_block_bytes block{};
    std::memcpy(block.data(), &image, sizeof(image));
    return block_id && WriteFileBlock(2,DirectoryPhysicalBlockId(block_id), block);
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
    if (bulk_write_) return false;
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
    if (!IsOpen() || bulk_write_) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);

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
    if (!IsOpen() || bulk_write_ || tail_ref.block_id == 0 || tail_ref.offset != 0) {
        return false;
    }
    clean_snapshot_loaded_.store(false, std::memory_order_relaxed);

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

PersistentStore::segment_state* PersistentStore::Segment(unsigned kind,uint64_t index) const {
    std::lock_guard<std::mutex> guard(segments_mu_);
    return kind<3 && index<segments_[kind].size()?segments_[kind][size_t(index)].get():nullptr;
}
int PersistentStore::FileDescriptor(unsigned kind,uint64_t index) const {
    const auto* segment=Segment(kind,index); return segment?segment->fd:-1;
}
uint64_t PersistentStore::LogicalFileBytes(unsigned kind) const {
    return kind<3?logical_file_bytes_[kind].load(std::memory_order_acquire):0;
}
uint64_t PersistentStore::SegmentIndex(uint64_t page) const {
    return page/(segment_bytes_/kMetadataBlockBytes);
}
off_t PersistentStore::SegmentOffset(uint64_t page) const {
    return off_t((page%(segment_bytes_/kMetadataBlockBytes))*kMetadataBlockBytes);
}
bool PersistentStore::AddSegment(unsigned kind,uint64_t index,int fd) {
    if(fd<0) return false;
    auto entry=std::make_unique<segment_state>(); entry->fd=fd;
    struct stat st{};
    if(kind>2 || index!=segments_[kind].size() || ::fstat(fd,&st) || !S_ISREG(st.st_mode) ||
       st.st_size<0 || st.st_size%kMetadataBlockBytes) return false;
    entry->bytes=uint64_t(st.st_size); entry->direct_flags=::fcntl(fd,F_GETFL);
    if(entry->direct_flags<0) return false;
    entry->direct_flags|=O_DIRECT;
    segments_[kind].push_back(std::move(entry));
    logical_file_bytes_[kind].store(index*segment_bytes_+uint64_t(st.st_size));
    if(!index) { if(!kind) fd_=fd; else if(kind==1) heads_fd_=fd; else buckets_fd_=fd; }
    return true;
}
SegmentLayout PersistentStore::CurrentSegmentLayout() const {
    SegmentLayout layout; layout.segment_bytes=segment_bytes_;
    std::lock_guard<std::mutex> guard(segments_mu_);
    for(unsigned kind=0;kind<3;++kind) {
        layout.logical_bytes[kind]=logical_file_bytes_[kind].load();
        for(size_t index=0;index<segments_[kind].size();++index) {
            struct stat st{};
            if(::fstat(segments_[kind][index]->fd,&st) || !S_ISREG(st.st_mode) || st.st_size<0)
                throw std::runtime_error("cannot stat data segment");
            layout.segments.push_back({kind,uint64_t(index),uint64_t(st.st_dev),uint64_t(st.st_ino),uint64_t(st.st_size)});
        }
    }
    ValidateSegmentLayout(layout); return layout;
}
bool PersistentStore::SetBufferedMode(bool enabled) {
    std::lock_guard<std::mutex> guard(segments_mu_);
    buffered_mode_=enabled;
    for(auto& group:segments_) for(auto& segment:group)
        if(::fcntl(segment->fd,F_SETFL,enabled?(segment->direct_flags&~O_DIRECT):segment->direct_flags)) return false;
    return true;
}
void PersistentStore::MarkFileDirty(unsigned kind,uint64_t index) const {
    if(auto* segment=Segment(kind,index)) segment->dirty_epoch.fetch_add(1,std::memory_order_release);
}
bool PersistentStore::SyncFileIfDirty(unsigned kind) const {
    std::lock_guard<std::mutex> guard(segments_mu_);
    if(kind>2 || segments_[kind].empty()) return false;
    for(const auto& segment:segments_[kind]) {
        const auto target=segment->dirty_epoch.load(std::memory_order_acquire);
        if(target==segment->synced_epoch.load(std::memory_order_acquire)) continue;
        int rc; do { rc=::fsync(segment->fd); } while(rc && errno==EINTR);
        if(rc) return false;
        segment->synced_epoch.store(target,std::memory_order_release);
        stats_fsync_ops_.fetch_add(1,std::memory_order_relaxed);
    }
    return true;
}
bool PersistentStore::EnsureBlockCount(uint64_t count) { return EnsureFileBlockCount(0,count); }
bool PersistentStore::EnsureFileBlockCount(unsigned kind,uint64_t count) const {
    UndoWriteResult result{batch_undo_.get()};
    if(kind>2 || count>uint64_t(std::numeric_limits<int64_t>::max())/kMetadataBlockBytes) return false;
    try {
        std::lock_guard<std::mutex> guard(segments_mu_);
        if(segments_[kind].empty()) return false;
        const uint64_t bytes=count*kMetadataBlockBytes;
        if(bytes<=logical_file_bytes_[kind].load()) { result.ok=true; return true; }
        const uint64_t needed=bytes/segment_bytes_+(bytes%segment_bytes_!=0);
        for(uint64_t index=segments_[kind].size()-1;index<needed;++index) {
            if(index==segments_[kind].size()) {
                const int flags=buffered_mode_?0:O_DIRECT;
                int fd=batch_undo_?batch_undo_->CreateSegment(kind,index,flags):
                    ::open(SegmentPath(path_,kind,index).c_str(),O_RDWR|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW|flags,0600);
                if(fd<0 || !const_cast<PersistentStore*>(this)->AddSegment(kind,index,fd)) return false;
                segment_directory_dirty_.store(true);
            }
            auto& segment=*segments_[kind][size_t(index)];
            const uint64_t wanted=std::min(segment_bytes_,bytes-index*segment_bytes_);
            if(wanted>segment.bytes) {
                int rc; do { rc=::ftruncate(segment.fd,off_t(wanted)); } while(rc && errno==EINTR);
                if(rc) return false;
                segment.bytes=wanted; segment.dirty_epoch.fetch_add(1,std::memory_order_release);
            }
            logical_file_bytes_[kind].store(index*segment_bytes_+segment.bytes,std::memory_order_release);
        }
        result.ok=true; return true;
    } catch(...) { return false; }
}

void PersistentStore::AdvanceTemplateWriteback(unsigned kind,uint64_t index,uint64_t end_offset) {
    const uint64_t window=std::min<uint64_t>(128ULL*1024*1024,segment_bytes_);
    auto* segment=Segment(kind,index);
    if(!segment) throw std::runtime_error("template writeback segment is missing");
    const int fd=segment->fd;
    auto sync_range=[&](uint64_t offset,uint64_t bytes,unsigned flags,bool wait) {
        const uint64_t start=monotonic_raw_ns();
        int rc; do { rc=::sync_file_range(fd,off_t(offset),off_t(bytes),flags); } while(rc && errno==EINTR);
        ++bulk_stats_.writeback_calls;
        if(wait) bulk_stats_.writeback_wait_ns+=monotonic_raw_ns()-start;
        if(rc && (errno==ENOSYS || errno==EOPNOTSUPP || errno==EINVAL)) { segment->writeback_supported=false; return; }
        if(rc) throw std::runtime_error("template incremental writeback failed");
    };
    while(end_offset-segment->next_writeback>=window) {
        if(segment->writeback_supported && segment->pending_bytes)
            sync_range(segment->pending_offset,segment->pending_bytes,
                SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER,true);
        segment->pending_bytes=0;
        if(segment->writeback_supported) {
            sync_range(segment->next_writeback,window,SYNC_FILE_RANGE_WRITE,false);
            if(segment->writeback_supported) { segment->pending_offset=segment->next_writeback; segment->pending_bytes=window; }
        }
        if(!segment->writeback_supported) {
            const uint64_t start=monotonic_raw_ns();
            int rc; do { rc=::fdatasync(fd); } while(rc && errno==EINTR);
            ++bulk_stats_.writeback_calls; bulk_stats_.writeback_wait_ns+=monotonic_raw_ns()-start;
            if(rc) throw std::runtime_error("template writeback fallback failed");
        }
        segment->next_writeback+=window;
    }
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
    sb.flags = kSuperblockFlagSplitFiles | kSuperblockFlagSegmentedFiles;
    sb.segment_layout_version=1; sb.segment_bytes=segment_bytes_;
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
    const uint64_t bytes=LogicalFileBytes(0);
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
    const uint64_t total_blocks=LogicalFileBytes(2)/kMetadataBlockBytes;
    const uint64_t max_directory_block = MaxLogicalDirectoryBlockId(total_blocks);
    for (uint64_t b = 1; b <= max_directory_block; ++b) {
        metadata_block_bytes raw;
        if (!ReadFileBlock(2,DirectoryPhysicalBlockId(b), raw)) {
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
    const bool ok = EnsureFileBlockCount(2,physical_block_id + 1);
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
    return inode_block_id;
}

uint64_t PersistentStore::DirectoryPhysicalBlockId(uint64_t directory_block_id) {
    return directory_block_id - 1u;
}

uint64_t PersistentStore::MaxLogicalInodeBlockId(uint64_t physical_block_count) {
    return physical_block_count ? physical_block_count-1u : 0;
}

uint64_t PersistentStore::MaxLogicalDirectoryBlockId(uint64_t physical_block_count) {
    if (physical_block_count == 0) {
        return 0;
    }
    return physical_block_count;
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
