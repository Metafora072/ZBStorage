#pragma once

#include <cstdint>
#include <string>

#include "MasstreeOpticalProfile.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {

struct MasstreeClusterStatsRecord {
    uint32_t optical_layout_version{MasstreeOpticalProfile::kLegacyMixedLayoutVersion};
    uint64_t disk_node_count{0};
    uint64_t optical_node_count{0};
    uint64_t disk_device_count{0};
    uint64_t optical_device_count{0};
    std::string total_capacity_bytes{"0"};
    std::string used_capacity_bytes{"0"};
    std::string free_capacity_bytes{"0"};
    uint64_t total_file_count{0};
    std::string total_file_bytes{"0"};
    std::string total_metadata_bytes{"0"};
    uint64_t avg_file_size_bytes{0};
    uint64_t min_file_size_bytes{0};
    uint64_t max_file_size_bytes{0};
    uint64_t total_disc_count{0};
    uint64_t used_disc_count{0};
    uint64_t unused_disc_count{0};
    uint64_t sealed_legacy_disc_count{0};
    uint64_t uniform_v2_used_disc_count{0};
    std::string allocated_file_bytes{"0"};
    MasstreeOpticalClusterCursor cursor;
};

struct MasstreeNamespaceStatsRecord {
    uint32_t optical_layout_version{MasstreeOpticalProfile::kLegacyMixedLayoutVersion};
    std::string namespace_id;
    std::string generation_id;
    uint64_t file_count{0};
    std::string total_file_bytes{"0"};
    std::string total_metadata_bytes{"0"};
    uint64_t avg_file_size_bytes{0};
    uint64_t start_global_image_id{0};
    uint64_t end_global_image_id{0};
    MasstreeOpticalClusterCursor start_cursor;
    MasstreeOpticalClusterCursor end_cursor;
};

class MasstreeStatsStore {
public:
    explicit MasstreeStatsStore(RocksMetaStore* store);

    bool LoadClusterStats(MasstreeClusterStatsRecord* record, std::string* error) const;
    bool LoadNamespaceStats(const std::string& namespace_id,
                            MasstreeNamespaceStatsRecord* record,
                            std::string* error) const;
    bool LoadNamespaceStatsRaw(const std::string& namespace_id,
                               bool* found,
                               std::string* raw_value,
                               std::string* error) const;
    bool LoadClusterStatsRaw(bool* found, std::string* raw_value, std::string* error) const;
    bool PutClusterStats(const MasstreeClusterStatsRecord& record,
                         rocksdb::WriteBatch* batch,
                         std::string* error) const;
    bool PutNamespaceStats(const MasstreeNamespaceStatsRecord& record,
                           rocksdb::WriteBatch* batch,
                           std::string* error) const;
    bool RestoreRawClusterStats(bool found,
                                const std::string& raw_value,
                                rocksdb::WriteBatch* batch,
                                std::string* error) const;
    bool RestoreRawNamespaceStats(const std::string& namespace_id,
                                  bool found,
                                  const std::string& raw_value,
                                  rocksdb::WriteBatch* batch,
                                  std::string* error) const;
    MasstreeClusterStatsRecord BuildUpdatedClusterStats(const MasstreeClusterStatsRecord& current,
                                                        uint64_t delta_file_count,
                                                        const std::string& delta_file_bytes,
                                                        uint64_t delta_metadata_bytes,
                                                        const MasstreeOpticalClusterCursor& end_cursor) const;
    MasstreeClusterStatsRecord BuildReplacedClusterStats(
        const MasstreeClusterStatsRecord& current,
        const MasstreeNamespaceStatsRecord* previous_namespace_stats,
        const MasstreeNamespaceStatsRecord& next_namespace_stats,
        const MasstreeOpticalClusterCursor& end_cursor) const;

private:
    static bool DecodeClusterStats(const std::string& payload,
                                   MasstreeClusterStatsRecord* record,
                                   std::string* error);
    static bool DecodeNamespaceStats(const std::string& payload,
                                     MasstreeNamespaceStatsRecord* record,
                                     std::string* error);
    static std::string EncodeClusterStats(const MasstreeClusterStatsRecord& record);
    static std::string EncodeNamespaceStats(const MasstreeNamespaceStatsRecord& record);
    static bool ParseU64Field(const std::string& value, uint64_t* out);
    static std::string CursorToString(const MasstreeOpticalClusterCursor& cursor);
    static bool CursorFromString(const std::string& value,
                                 MasstreeOpticalClusterCursor* cursor);

    RocksMetaStore* store_{};
};

} // namespace zb::mds
