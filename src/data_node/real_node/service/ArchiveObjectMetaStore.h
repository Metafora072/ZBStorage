#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::real_node {

struct ArchiveObjectMeta {
    std::string disk_id;
    std::string object_id;
    uint64_t size_bytes{0};
    uint64_t checksum{0};
    uint64_t last_access_ts_ms{0};
    double heat_score{0.0};
    std::string archive_state{"pending"};
    uint64_t version{0};
    uint64_t read_ops{0};
    uint64_t write_ops{0};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

struct ArchiveCandidateView {
    std::string disk_id;
    std::string object_id;
    uint64_t last_access_ts_ms{0};
    uint64_t size_bytes{0};
    uint64_t checksum{0};
    double heat_score{0.0};
    double score{0.0};
    uint64_t read_ops{0};
    uint64_t write_ops{0};
    uint64_t version{0};
    std::string archive_state{"pending"};

    std::string ArchiveObjectId() const {
        return object_id;
    }

    void SetArchiveObjectId(const std::string& id) {
        object_id = id;
    }
};

class ArchiveObjectMetaStore {
public:
    bool Init(const std::string& dir_path,
              size_t max_objects,
              uint32_t snapshot_interval_ops,
              std::string* error,
              bool wal_fsync = false);
    void SetMaxObjects(size_t max_objects);
    void TrackObjectAccess(const std::string& disk_id,
                           const std::string& object_id,
                           uint64_t end_offset,
                           bool is_write,
                           uint64_t checksum,
                           uint64_t now_ms);
    void RemoveObject(const std::string& disk_id, const std::string& object_id);
    bool UpdateObjectArchiveState(const std::string& disk_id,
                                  const std::string& object_id,
                                  const std::string& archive_state,
                                  uint64_t version);
    std::vector<ArchiveCandidateView> CollectCandidates(uint32_t max_candidates,
                                                        uint64_t min_age_ms,
                                                        uint64_t now_ms) const;
    std::vector<ArchiveObjectMeta> SnapshotMetas() const;
    bool ClearAll(std::string* error);
    bool FlushSnapshot(std::string* error);

private:
    static std::string BuildObjectKey(const std::string& disk_id, const std::string& object_id);
    static bool ParseMetaLine(const std::string& line, ArchiveObjectMeta* meta);
    static std::string SerializeMetaLine(const ArchiveObjectMeta& meta);
    static std::vector<std::string> SplitTabs(const std::string& line);
    static uint32_t Crc32(const std::string& data);
    static bool WriteUint32LE(std::ostream* out, uint32_t value);
    static bool FsyncPath(const std::string& path);
    bool EnsureWalMagicLocked(std::string* error);

    bool LoadSnapshotLocked(std::string* error);
    bool ReplayWalLocked(std::string* error);
    bool AppendWalRecordLocked(const std::string& payload);
    bool MaybeSnapshotLocked();
    bool WriteSnapshotLocked(std::string* error);
    void EvictIfNeededLocked();

    mutable std::mutex mu_;
    std::unordered_map<std::string, ArchiveObjectMeta> metas_;
    size_t max_objects_{500000};
    uint32_t snapshot_interval_ops_{20000};
    uint32_t updates_since_snapshot_{0};
    bool wal_fsync_enabled_{false};
    std::string dir_path_;
    std::string wal_path_;
    std::string snapshot_path_;
    std::ofstream wal_out_;
    bool inited_{false};
};

using ArchiveObjectCandidateView = ArchiveCandidateView;

} // namespace zb::real_node
