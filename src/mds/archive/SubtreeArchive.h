#pragma once
#include "mds/location/FileLocationResolver.h"
#include "optical_node.pb.h"

namespace zb::mds {
struct ArchiveBatch {
    std::vector<zb::rpc::ArchiveFile> files;
    bool done = false;
};
// Borrows the namespace and resolver. The caller keeps the selected files and
// ancestor paths unchanged throughout scan/send. Errors terminate this cursor.
class ArchiveManifestBuilder {
public:
    static NamespaceStatus Begin(const NamespaceStore&, const FileLocationResolver&,
        const std::string& root, uint32_t work_budget, std::unique_ptr<ArchiveManifestBuilder>*);
    NamespaceStatus Next(uint32_t max_files, ArchiveBatch*);
private:
    ArchiveManifestBuilder(const FileLocationResolver& resolver, std::unique_ptr<NamespaceScanner> scan)
        : resolver_(resolver), scan_(std::move(scan)) {}
    const FileLocationResolver& resolver_;
    std::unique_ptr<NamespaceScanner> scan_;
    NamespaceStatus failure_;
};

class ArchiveMetadataSink {
public:
    virtual ~ArchiveMetadataSink() = default;
    // False certainty does not imply rollback. On failure an attempted request
    // may already have been accepted; callers must never retry automatically.
    virtual NamespaceStatus Send(const zb::rpc::SendArchiveMetadataRequest&, bool* may_be_accepted) = 0;
};
struct ArchiveSendProgress {
    uint64_t acknowledged_files = 0, acknowledged_batches = 0, attempted_batches = 0;
    bool last_batch_may_be_accepted = false;
};
NamespaceStatus SendArchiveSubtree(const NamespaceStore&, const FileLocationResolver&,
    const std::string& root, uint64_t batch_id, uint32_t max_files, uint32_t work_budget,
    ArchiveMetadataSink&, ArchiveSendProgress*);
// Connects to the physical OpticalNodeService protocol; max_retry is zero.
NamespaceStatus MakeOpticalArchiveSink(const std::string& address, uint32_t timeout_ms,
                                      std::unique_ptr<ArchiveMetadataSink>*);
}
