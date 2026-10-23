#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "mds.pb.h"

namespace zb::mds {

struct NamespaceStatus {
    zb::rpc::MdsStatusCode code{zb::rpc::MDS_OK};
    std::string message{"OK"};
    bool ok() const { return code == zb::rpc::MDS_OK; }
};

struct SubtreeScanOptions {
    bool files_only = true;
    // Bounds traversal steps per Next, including empty directories. Not a time
    // deadline. A successful batch may be empty with done=false.
    uint32_t work_budget = 1024;
};

struct SubtreeScanBatch {
    std::vector<zb::rpc::InodeAttr> entries;
    bool done = false;
};

// In-memory, single-caller cursor. The selected subtree must not be structurally
// modified until destruction; ancestors must not be renamed/moved. No
// snapshot/restart guarantee; order is unspecified. Attributes are read live.
// The root itself is excluded. max_entries must be positive. Storage errors are
// terminal and discard the current batch; earlier successful batches stay valid.
class NamespaceScanner {
public:
    virtual ~NamespaceScanner() = default;
    virtual NamespaceStatus Next(uint32_t max_entries, SubtreeScanBatch* batch) = 0;
};

// Scoped creation: destruction cancels unfinished work. The backing store must
// outlive the reservation unless its implementation retains shared ownership.
class NamespaceCreation {
public:
    virtual ~NamespaceCreation() = default;
    virtual uint64_t inode_id() const = 0;
    virtual NamespaceStatus Commit(const zb::rpc::InodeAttr&) = 0;
    virtual bool Cancel() = 0;
};

// Namespace semantics (Rocks hot namespace or unified LHM namespace). No key layout or storage-engine types cross this
// boundary. Paths follow the existing ZB convention. Readdir returns immediate
// children in bytewise name order, strictly after start_after; limit=0 is unbounded.
// Read failures: missing => false + empty error; backend failure => nonempty error.
// Inode IDs are stable across rename. PutInode preserves backend structural and
// location metadata; it also supports initializing the root inode.
class NamespaceStore {
public:
    virtual ~NamespaceStore() = default;
    virtual bool ResolvePath(const std::string& path, uint64_t* inode_id,
                             zb::rpc::InodeAttr* attr, std::string* error) const = 0;
    virtual bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr,
                          std::string* error) const = 0;
    virtual bool PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr,
                          std::string* error) = 0;
    virtual std::unique_ptr<NamespaceCreation> ReserveCreate(
        const std::string& path, zb::rpc::InodeType type, std::string* error);
    // Opaque ID: callers must not assume sequential IDs or derive storage
    // locations. Type permits backends to allocate different directory/file
    // layouts. Legacy allocation remains for Rocks compatibility; services use ReserveCreate.
    virtual uint64_t AllocateInodeId(zb::rpc::InodeType type, std::string* error) = 0;
    // Legacy Rocks creation entry. Path-bound backends may reject this pair of
    // legacy calls; portable callers use ReserveCreate/Commit.
    // attr includes the reserved inode ID and the placement chosen by MDS.
    virtual NamespaceStatus Create(const std::string& path,
                                   const zb::rpc::InodeAttr& attr) = 0;
    virtual NamespaceStatus Rename(const std::string& old_path,
                                   const std::string& new_path) = 0;
    // expected_type distinguishes unlink from rmdir; nonempty dirs are rejected.
    virtual NamespaceStatus Remove(const std::string& path,
                                   zb::rpc::InodeType expected_type) = 0;
    virtual bool Readdir(const std::string& path, const std::string& start_after,
                         uint32_t limit, std::vector<zb::rpc::Dentry>* entries,
                         bool* has_more, std::string* next_token,
                         std::string* error) const = 0;
    // Default implementation uses paged Readdir + GetInode and borrows this
    // store, which must outlive the scanner. LHM overrides with native cursors.
    virtual NamespaceStatus BeginSubtreeScan(
        const std::string& root_path, const SubtreeScanOptions& options,
        std::unique_ptr<NamespaceScanner>* scanner) const;
};

// Compatibility implementation for the Rocks backend: reserving a number does
// not create a stored inode. Cancellation consumes the number without publishing.
inline std::unique_ptr<NamespaceCreation> NamespaceStore::ReserveCreate(
    const std::string& path, zb::rpc::InodeType type, std::string* error) {
    class Creation final : public NamespaceCreation {
        NamespaceStore* store_;
        std::string path_;
        zb::rpc::InodeType type_;
        uint64_t id_;
        bool finished_ = false;
    public:
        Creation(NamespaceStore* store, const std::string& path, zb::rpc::InodeType type, uint64_t id)
            : store_(store), path_(path), type_(type), id_(id) {}
        ~Creation() override { Cancel(); }
        uint64_t inode_id() const override { return id_; }
        NamespaceStatus Commit(const zb::rpc::InodeAttr& attr) override {
            if (finished_ || attr.inode_id() != id_ || attr.type() != type_)
                return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid creation reservation"};
            auto result = store_->Create(path_, attr);
            if (result.ok()) finished_ = true;
            return result;
        }
        bool Cancel() override { finished_ = true; return true; }
    };
    const auto id = AllocateInodeId(type, error);
    if (!id) return nullptr;
    return std::make_unique<Creation>(this, path, type, id);
}

} // namespace zb::mds
