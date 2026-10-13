#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <rocksdb/write_batch.h>

#include "../storage/RocksMetaStore.h"
#include "mds.pb.h"

namespace zb::mds {

class ArchiveLeaseManager {
public:
    struct Options {
        uint64_t default_lease_ms{30000};
        uint64_t min_lease_ms{1000};
        uint64_t max_lease_ms{300000};
    };

    explicit ArchiveLeaseManager(RocksMetaStore* store);
    explicit ArchiveLeaseManager(RocksMetaStore* store, Options options);

    bool Claim(const zb::rpc::ClaimArchiveTaskRequest& request,
               zb::rpc::ClaimArchiveTaskReply* reply,
               std::string* error);
    bool Renew(const zb::rpc::RenewArchiveLeaseRequest& request,
               zb::rpc::RenewArchiveLeaseReply* reply,
               std::string* error);
    bool Commit(const zb::rpc::CommitArchiveTaskRequest& request,
                zb::rpc::CommitArchiveTaskReply* reply,
                std::string* error);
    bool ValidateModelLease(uint64_t inode_id, const std::string& node_id,
                            const std::string& disk_id, const std::string& lease_id,
                            std::string* error) const;
    bool CommitModelLeaseWithInodeCas(uint64_t inode_id, const std::string& node_id,
                                     const std::string& disk_id, const std::string& lease_id,
                                     const std::string& expected_inode_payload,
                                     rocksdb::WriteBatch* batch, bool* matched,
                                     std::string* error);

private:
    bool LoadOrInitRecordLocked(const std::string& lease_key,
                                uint64_t inode_id,
                                zb::rpc::ArchiveLeaseRecord* record,
                                std::string* error) const;
    bool PersistRecordLocked(const zb::rpc::ArchiveLeaseRecord& record, std::string* error) const;
    uint64_t NormalizeLeaseMs(uint64_t requested_lease_ms) const;
    static std::string GenerateLeaseId();
    static uint64_t NowMilliseconds();

    RocksMetaStore* store_{};
    Options options_;
    mutable std::mutex mu_;
};

} // namespace zb::mds
