#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::real_node {

enum class FileArchiveState : uint32_t {
    kPending = 0,
    kArchiving = 1,
    kArchived = 2,
};

struct ArchiveFileMeta {
    uint64_t inode_id{0};
    std::string disk_id;
    uint64_t file_size{0};
    uint32_t object_count{0};
    uint64_t last_access_ts_ms{0};
    FileArchiveState archive_state{FileArchiveState::kPending};
    uint64_t version{0};
};

class ArchiveFileMetaStore {
public:
    bool Init(const std::string& dir_path,
              size_t max_files,
              uint32_t snapshot_interval_ops,
              std::string* error,
              bool wal_fsync = false);
    void SetMaxFiles(size_t max_files);
    void TrackFileAccess(uint64_t inode_id,
                         const std::string& disk_id,
                         uint64_t file_size,
                         uint32_t object_count,
                         bool is_write,
                         uint64_t now_ms,
                         uint64_t version);
    bool UpsertFile(uint64_t inode_id,
                    const std::string& disk_id,
                    uint64_t file_size,
                    uint32_t object_count,
                    uint64_t version,
                    bool reset_to_pending);
    bool UpdateFileArchiveState(uint64_t inode_id,
                                const std::string& disk_id,
                                FileArchiveState archive_state,
                                uint64_t version);
    void RemoveFile(uint64_t inode_id);
    std::vector<ArchiveFileMeta> CollectCandidates(uint32_t max_candidates,
                                                   uint64_t min_age_ms,
                                                   uint64_t now_ms) const;
    std::vector<ArchiveFileMeta> SnapshotMetas() const;
    bool ClearAll(std::string* error);
    bool FlushSnapshot(std::string* error);

private:
    static std::string BuildFileKey(uint64_t inode_id);
    static bool ParseMetaLine(const std::string& line, ArchiveFileMeta* meta);
    static std::string SerializeMetaLine(const ArchiveFileMeta& meta);
    static std::vector<std::string> SplitTabs(const std::string& line);
    static uint32_t Crc32(const std::string& data);
    static bool WriteUint32LE(std::ostream* out, uint32_t value);
    static bool FsyncPath(const std::string& path);
    static const char* StateToString(FileArchiveState state);
    static bool ParseState(const std::string& text, FileArchiveState* state);
    bool EnsureWalMagicLocked(std::string* error);

    bool LoadSnapshotLocked(std::string* error);
    bool ReplayWalLocked(std::string* error);
    bool AppendWalRecordLocked(const std::string& payload);
    bool MaybeSnapshotLocked();
    bool WriteSnapshotLocked(std::string* error);
    void EvictIfNeededLocked();

    mutable std::mutex mu_;
    std::unordered_map<std::string, ArchiveFileMeta> metas_;
    size_t max_files_{500000};
    uint32_t snapshot_interval_ops_{20000};
    uint32_t updates_since_snapshot_{0};
    bool wal_fsync_enabled_{false};
    std::string dir_path_;
    std::string wal_path_;
    std::string snapshot_path_;
    std::ofstream wal_out_;
    bool inited_{false};
};

} // namespace zb::real_node
