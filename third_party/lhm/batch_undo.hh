#ifndef MASSTREELHM_BATCH_UNDO_HH
#define MASSTREELHM_BATCH_UNDO_HH

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>

#include "incremental_checkpoint.hh"

namespace MasstreeLHM {

// Offline batch / online checkpoint recovery. Callers hold the main store's flock for
// Begin/Recover and until the namespace has been destroyed. A journal is never
// removed by a destructor: only successful recovery or commit may remove it.
class BatchUndo {
public:
    static std::unique_ptr<BatchUndo> Begin(const std::string& store,
        uint64_t inode_high_water, uint64_t directory_next_block, bool service = false);
    static bool Exists(const std::string& store);
    static bool IsServiceJournal(const std::string& store);
    // Validates the entire existing v4 segmented snapshot and returns "bytes:checksum".
    static std::string SnapshotFingerprint(const std::string& snapshot);
    // true: a journal was finalized/rolled back; false: no journal existed.
    // Throws on corruption, identity mismatch or I/O error, retaining the log.
    static bool Recover(const std::string& store);

    ~BatchUndo();
    BatchUndo(const BatchUndo&) = delete;
    BatchUndo& operator=(const BatchUndo&) = delete;

    // kind 0 = main inode-file block; kind 1 = compact-head page;
    // kind 2 = file-bucket block. block is the global page within that kind;
    // data_fd is the corresponding segment descriptor.
    // The complete old image is durable before true is returned. Any failure
    // poisons this journal so a destructor cannot accidentally flush past it.
    bool BeforeWrite(unsigned kind, int data_fd, uint64_t block) noexcept;
    // Create and durably register a new append-only segment before publishing
    // its final name. Caller owns the returned fd. open_flags may be O_DIRECT.
    int CreateSegment(unsigned kind, uint64_t index, int open_flags = 0);
    // Creates an empty unique index and journals its identity before returning
    // its token. Its contents are not visible until the checkpoint references it.
    uint64_t CreateIndex();
    const std::string& error() const { return error_; }
    bool failed() const { return poisoned_; }
    void Poison() noexcept { poisoned_ = true; }
    // Caller next closes the namespace, atomically publishes new_snapshot as
    // .snapshot and fsyncs its directory, then calls Recover to finalize.
    void PrepareCommit(const std::string& new_snapshot,
                       const std::string& opaque_receipt);
    void PrepareServiceCommit(const std::string& new_snapshot);

private:
    BatchUndo() = default;
    bool IsOld(unsigned kind, uint64_t block) const;
    std::string store_;
    int fd_ = -1;
    SegmentLayout baseline_;
    uint64_t next_segment_[3]{};
    uint64_t base_bytes_ = 0, base_checksum_ = 0;
    std::unordered_set<uint64_t> seen_[3];
    bool poisoned_ = false, prepared_ = false;
    bool service_ = false;
    std::string error_;
};

// Test builds only: ZB_LHM_CRASH_AT=name[:occurrence] sends SIGKILL.
// It is a no-op in builds without ZB_LHM_TESTING.
void BatchFault(const char* name);

} // namespace MasstreeLHM
#endif
