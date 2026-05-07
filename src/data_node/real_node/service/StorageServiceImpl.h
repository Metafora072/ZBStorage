#pragma once

#include <cstdint>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "ArchiveFileMetaStore.h"
#include <brpc/channel.h>

#include "ArchiveObjectMetaStore.h"
#include "../io/DiskManager.h"
#include "../io/IOExecutor.h"
#include "../io/LocalPathResolver.h"
#include "../../common/ObjectStore.h"
#include "../../../msg/storage_node_messages.h"

namespace zb::real_node {

struct ReplicationStatusSnapshot {
    bool replication_enabled{false};
    bool is_primary{true};
    uint64_t epoch{1};
    uint64_t applied_lsn{0};
    std::string node_id;
    std::string group_id;
    std::string peer_node_id;
    std::string peer_address;
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
};

struct ArchiveCandidateStat {
    std::string disk_id;
    std::string object_id;
    uint64_t last_access_ts_ms{0};
    uint64_t size_bytes{0};
    uint64_t checksum{0};
    double heat_score{0.0};
    std::string archive_state{"pending"};
    uint64_t version{0};
    double score{0.0};
    uint64_t read_ops{0};
    uint64_t write_ops{0};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

struct FileArchiveCandidateStat {
    uint64_t inode_id{0};
    std::string disk_id;
    uint64_t file_size{0};
    uint32_t object_count{0};
    uint64_t last_access_ts_ms{0};
    std::string archive_state{"pending"};
    uint64_t version{0};
};

class StorageServiceImpl {
public:
    StorageServiceImpl(DiskManager* disk_manager,
                       LocalPathResolver* path_resolver,
                       IOExecutor* io_executor);
    ~StorageServiceImpl();

    void ConfigureReplication(const std::string& node_id,
                              const std::string& group_id,
                              bool replication_enabled,
                              bool is_primary,
                              const std::string& peer_node_id,
                              const std::string& peer_address,
                              uint32_t replication_timeout_ms);
    void ApplySchedulerAssignment(bool is_primary,
                                  uint64_t epoch,
                                  const std::string& group_id,
                                  const std::string& primary_node_id,
                                  const std::string& primary_address,
                                  const std::string& secondary_node_id,
                                  const std::string& secondary_address);
    ReplicationStatusSnapshot GetReplicationStatus() const;

    zb::msg::WriteObjectReply WriteObject(const zb::msg::WriteObjectRequest& request);
    zb::msg::ReadObjectReply ReadObject(const zb::msg::ReadObjectRequest& request);
    zb::msg::ReadArchivedFileReply ReadArchivedFile(const zb::msg::ReadArchivedFileRequest& request);
    zb::msg::DeleteObjectReply DeleteObject(const zb::msg::DeleteObjectRequest& request);
    zb::msg::Status PutObject(const zb::data_node::ObjectWriteRequest& request);
    zb::data_node::ObjectReadResult GetObject(const zb::data_node::ObjectReadRequest& request);
    zb::msg::Status DeleteObject(const zb::data_node::ObjectDeleteRequest& request);
    zb::msg::ResetNodeDataReply ResetNodeData(const zb::msg::ResetNodeDataRequest& request);
    zb::msg::DiskReportReply GetDiskReport() const;
    zb::msg::DeleteFileMetaReply DeleteFileMeta(const zb::msg::DeleteFileMetaRequest& request);
    zb::msg::ResolveFileReadReply ResolveFileRead(const zb::msg::ResolveFileReadRequest& request) const;
    zb::msg::AllocateFileWriteReply AllocateFileWrite(const zb::msg::AllocateFileWriteRequest& request) const;
    zb::msg::CommitFileWriteReply CommitFileWrite(const zb::msg::CommitFileWriteRequest& request);
    void SetFileMetaStoreDir(const std::string& dir_path);

    bool InitArchiveMetaStore(const std::string& meta_dir,
                              size_t max_objects,
                              uint32_t snapshot_interval_ops,
                              bool wal_fsync,
                              std::string* error);
    bool FlushArchiveMetaSnapshot(std::string* error);
    zb::msg::Status UpdateArchiveState(const std::string& disk_id,
                                       const std::string& object_id,
                                       const std::string& archive_state,
                                       uint64_t version);
    zb::msg::Status UpdateFileArchiveState(uint64_t inode_id,
                                           const std::string& disk_id,
                                           FileArchiveState archive_state,
                                           uint64_t version);
    void SetArchiveTrackingMaxObjects(size_t max_objects);
    std::vector<ArchiveCandidateStat> CollectArchiveCandidates(uint32_t max_candidates, uint64_t min_age_ms) const;
    std::vector<FileArchiveCandidateStat> CollectFileArchiveCandidates(uint32_t max_candidates, uint64_t min_age_ms) const;

private:
    bool ApplyFileMetaInternal(const zb::msg::ApplyFileMetaRequest& request,
                                const std::string* txid,
                                zb::msg::ApplyFileMetaReply* reply);

    struct ReplicationRepairTask {
        std::string key;
        zb::msg::WriteObjectRequest request;
        uint64_t epoch{0};
        uint32_t attempts{0};
        uint64_t generation{0};
    };

    zb::msg::Status ReplicateWriteToSecondary(const zb::msg::WriteObjectRequest& request, uint64_t epoch);
    void EnqueueReplicationRepair(const zb::msg::WriteObjectRequest& request, uint64_t epoch);
    uint64_t BumpReplicationRepairGeneration(const zb::msg::WriteObjectRequest& request);
    static std::string BuildReplicationRepairKey(const zb::msg::WriteObjectRequest& request);
    void ReplicationRepairLoop();
    void TrackObjectAccess(const std::string& disk_id,
                           const std::string& object_id,
                           uint64_t end_offset,
                           bool is_write,
                           uint64_t checksum);
    void RemoveObjectTracking(const std::string& disk_id, const std::string& object_id);
    static uint64_t FastChecksum64(const std::string& data);
    static uint64_t NowMilliseconds();
    static std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index);
    static bool ParseStableObjectId(const std::string& object_id, uint64_t* inode_id, uint32_t* object_index);
    static void BuildObjectSlices(uint64_t inode_id,
                                  uint64_t offset,
                                  uint64_t size,
                                  uint64_t object_unit_size,
                                  const std::string& disk_id,
                                  std::vector<zb::msg::FileObjectSlice>* slices);
    bool InitFileMetaStorePath() const;
    bool LoadFileMetaStoreLocked(std::string* error) const;
    bool PersistFileMetaStoreLocked(std::string* error) const;

    DiskManager* disk_manager_{};
    LocalPathResolver* path_resolver_{};
    IOExecutor* io_executor_{};

    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    uint32_t replication_timeout_ms_{2000};

    mutable std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> peer_channels_;
    std::mutex repl_repair_mu_;
    std::condition_variable repl_repair_cv_;
    std::deque<ReplicationRepairTask> repl_repair_queue_;
    std::unordered_map<std::string, uint64_t> repl_repair_generation_;
    std::atomic<bool> stop_repl_repair_{false};
    std::thread repl_repair_thread_;

    ArchiveObjectMetaStore archive_meta_store_;
    ArchiveFileMetaStore archive_file_meta_store_;
    size_t archive_tracking_max_objects_{500000};
    mutable std::mutex file_meta_mu_;
    mutable std::unordered_map<uint64_t, zb::msg::FileMeta> file_meta_by_inode_;
    mutable std::unordered_map<uint64_t, std::string> last_commit_txid_by_inode_;
    mutable std::string file_meta_store_path_;
    mutable bool file_meta_loaded_{false};
};

} // namespace zb::real_node
