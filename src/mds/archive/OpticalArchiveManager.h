#pragma once

#include <brpc/channel.h>
#include <rocksdb/write_batch.h>

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "ArchiveBatchStager.h"
#include "FileArchiveCandidateQueue.h"
#include "ArchiveLeaseManager.h"
#include "../allocator/NodeStateCache.h"
#include "../storage/RocksMetaStore.h"
#include "mds.pb.h"
#include "real_node.pb.h"

namespace zb::mds {

class OpticalArchiveManager {
public:
    struct Options {
        uint64_t archive_trigger_bytes{10ULL * 1024ULL * 1024ULL * 1024ULL};
        uint64_t archive_target_bytes{8ULL * 1024ULL * 1024ULL * 1024ULL};
        uint64_t cold_file_ttl_sec{3600};
        uint32_t max_objects_per_round{64};
        uint64_t default_object_unit_size{4ULL * 1024ULL * 1024ULL};
    };

    OpticalArchiveManager(RocksMetaStore* store,
                          NodeStateCache* cache,
                          FileArchiveCandidateQueue* candidate_queue,
                          ArchiveBatchStager* batch_stager,
                          ArchiveLeaseManager* lease_manager,
                          Options options,
                          std::shared_mutex* placement_transaction_mu = nullptr);

    // Runs one archive + eviction round. Returns true if the round completed.
    bool RunOnce(std::string* error);

private:
    bool ShouldArchiveNow(const std::vector<NodeInfo>& nodes);
    bool LoadInodeAttr(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) const;
    bool BuildDiskFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                       zb::rpc::DiskFileLocation* location,
                                       std::string* error) const;
    bool BuildOpticalFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                          uint64_t inode_id,
                                          zb::rpc::OpticalFileLocation* location,
                                          std::string* error) const;
    bool LoadDiskFileLocation(uint64_t inode_id, zb::rpc::DiskFileLocation* location, std::string* error) const;
    bool LoadOpticalFileLocation(uint64_t inode_id, zb::rpc::OpticalFileLocation* location, std::string* error) const;
    bool SaveOpticalFileLocation(uint64_t inode_id,
                                 const zb::rpc::OpticalFileLocation& location,
                                 rocksdb::WriteBatch* batch,
                                 std::string* error) const;
    bool DeleteDiskFileLocation(uint64_t inode_id, rocksdb::WriteBatch* batch, std::string* error) const;
    bool ReadObjectFromReplica(const zb::rpc::ReplicaLocation& source,
                               uint64_t read_size,
                               std::string* data,
                               std::string* error);
    bool WriteObjectToOptical(const NodeSelection& optical,
                              const std::string& object_id,
                              const std::string& op_id,
                              const std::string& data,
                              uint64_t inode_id,
                              uint32_t object_index,
                              const zb::rpc::InodeAttr* inode_attr,
                              zb::rpc::ReplicaLocation* optical_location,
                              std::string* error);
    bool DeleteDiskFile(uint64_t inode_id, const zb::rpc::DiskFileLocation& location, std::string* error);
    bool BurnSealedBatch(const NodeSelection& optical,
                         uint32_t* archived_count,
                         std::unordered_set<std::string>* touched_object_keys,
                         std::string* error);
    bool ReconcileArchiveStates(uint32_t max_records, std::string* error);
    bool HasUsableOpticalLocation(uint64_t inode_id, bool* archived, std::string* error);
    bool ClaimFileLease(const FileArchiveCandidateEntry& candidate,
                        const std::string& request_id,
                        std::string* lease_id,
                        std::string* op_id,
                        uint64_t* version,
                        std::string* error);
    bool RenewFileLease(const FileArchiveCandidateEntry& candidate,
                        const std::string& lease_id,
                        std::string* renewed_lease_id,
                        std::string* renewed_op_id,
                        uint64_t* version,
                        std::string* error);
    bool CommitFileLease(const FileArchiveCandidateEntry& candidate,
                         const std::string& lease_id,
                         const std::string& op_id,
                         bool success,
                         const std::string& error_message,
                         uint64_t* version,
                         std::string* error);
    bool ResolveObjectOwner(const std::string& object_id,
                            uint64_t* inode_id,
                            uint32_t* object_index,
                            std::string* error);
    bool UpdateSourceArchiveState(const ArchiveCandidateEntry& candidate,
                                  const std::string& archive_state,
                                  uint64_t version,
                                  std::string* error);
    bool UpdateInodeArchiveState(uint64_t inode_id,
                                 zb::rpc::InodeArchiveState archive_state,
                                 std::string* error);
    bool UpdateSourceFileArchiveState(const FileArchiveCandidateEntry& candidate,
                                      zb::rpc::FileArchiveState archive_state,
                                      uint64_t version,
                                      std::string* error);
    brpc::Channel* GetChannel(const std::string& address, std::string* error);

    RocksMetaStore* store_{};
    NodeStateCache* cache_{};
    FileArchiveCandidateQueue* candidate_queue_{};
    ArchiveBatchStager* batch_stager_{};
    ArchiveLeaseManager* lease_manager_{};
    Options options_;
    std::shared_mutex* placement_transaction_mu_{};
    bool archive_mode_{false};

    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

} // namespace zb::mds
