#include "MasstreeStatsStore.h"

#include <algorithm>
#include <cctype>
#include <sstream>
#include <vector>

#include "MasstreeDecimalUtils.h"
#include "MasstreeOpticalLayoutStore.h"
#include "MasstreeOpticalLayoutTranslator.h"
#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) == 0;
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch) == 0;
    }).base(), value.end());
    return value;
}

bool UsedDiscCountForCursor(const MasstreeOpticalProfile& profile,
                            const MasstreeOpticalClusterCursor& cursor,
                            uint64_t* used_disc_count,
                            std::string* error) {
    if (!used_disc_count || !profile.IsValidCursor(cursor)) {
        if (error) {
            *error = "invalid optical allocation cursor";
        }
        return false;
    }
    const uint64_t cursor_disc_ordinal =
        static_cast<uint64_t>(cursor.node_index) * static_cast<uint64_t>(profile.disks_per_node) +
        static_cast<uint64_t>(cursor.disk_index);
    *used_disc_count = cursor_disc_ordinal +
                       ((cursor.image_index_in_disk > 0 || cursor.image_used_bytes > 0) ? 1ULL : 0ULL);
    return true;
}

MasstreeOpticalProfile ProfileForLayoutVersion(uint32_t layout_version) {
    return layout_version >= MasstreeOpticalProfile::kUniform2TbLayoutVersion
               ? MasstreeOpticalProfile::Fixed()
               : MasstreeOpticalProfile::LegacyMixed();
}

bool PopulateClusterDiscStats(RocksMetaStore* store,
                              MasstreeClusterStatsRecord* record,
                              std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree cluster stats output is null";
        }
        return false;
    }
    const MasstreeOpticalProfile profile = ProfileForLayoutVersion(record->optical_layout_version);
    const bool uses_uniform_layout =
        profile.layout_version >= MasstreeOpticalProfile::kUniform2TbLayoutVersion;
    uint64_t used_disc_count = 0;
    if (!UsedDiscCountForCursor(profile, record->cursor, &used_disc_count, error)) {
        return false;
    }
    record->optical_layout_version = profile.layout_version;
    record->used_capacity_bytes = NormalizeDecimalString(record->used_capacity_bytes);
    record->total_file_bytes = NormalizeDecimalString(record->total_file_bytes);
    record->optical_node_count = profile.optical_node_count;
    record->optical_device_count = static_cast<uint64_t>(profile.optical_node_count) *
                                   static_cast<uint64_t>(profile.disks_per_node);
    record->total_capacity_bytes = profile.TotalCapacityBytesDecimal();
    record->free_capacity_bytes =
        SubtractDecimalStrings(record->total_capacity_bytes, record->used_capacity_bytes);
    record->total_disc_count = record->optical_device_count;
    record->used_disc_count = used_disc_count;
    record->unused_disc_count = used_disc_count <= record->total_disc_count
                                    ? record->total_disc_count - used_disc_count
                                    : 0;
    record->sealed_legacy_disc_count = 0;
    record->uniform_v2_used_disc_count = uses_uniform_layout ? used_disc_count : 0;
    record->allocated_file_bytes = NormalizeDecimalString(record->total_file_bytes);

    if (uses_uniform_layout && store) {
        MasstreeOpticalLayoutStore layout_store(store);
        MasstreeOpticalLayoutTranslation translation;
        bool found = false;
        if (!layout_store.LoadCurrent(&found, &translation, error)) {
            return false;
        }
        if (found) {
            record->sealed_legacy_disc_count = translation.cutover_disc_ordinal;
            record->uniform_v2_used_disc_count =
                used_disc_count >= translation.cutover_disc_ordinal
                    ? used_disc_count - translation.cutover_disc_ordinal
                    : 0;
        }
    }
    if (record->used_disc_count > record->total_disc_count) {
        if (error) {
            *error = "masstree optical used disc count exceeds total disc count";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool NormalizeNamespaceStatsToCurrentOpticalLayout(MasstreeNamespaceStatsRecord* record,
                                                   std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree namespace stats normalize output is null";
        }
        return false;
    }
    const MasstreeOpticalProfile current_profile = MasstreeOpticalProfile::Fixed();
    if (record->optical_layout_version < MasstreeOpticalProfile::kUniform2TbLayoutVersion) {
        const MasstreeOpticalProfile legacy_profile = MasstreeOpticalProfile::LegacyMixed();
        MasstreeOpticalClusterCursor converted_start;
        MasstreeOpticalClusterCursor converted_end;
        if (!legacy_profile.ConvertCursorTo(record->start_cursor, current_profile, &converted_start) ||
            !legacy_profile.ConvertCursorTo(record->end_cursor, current_profile, &converted_end)) {
            if (error) {
                *error = "failed to convert legacy masstree optical namespace cursor";
            }
            return false;
        }
        record->start_cursor = converted_start;
        record->end_cursor = converted_end;
    }
    record->optical_layout_version = current_profile.layout_version;
    return true;
}

} // namespace

MasstreeStatsStore::MasstreeStatsStore(RocksMetaStore* store)
    : store_(store) {
}

bool MasstreeStatsStore::LoadClusterStats(MasstreeClusterStatsRecord* record, std::string* error) const {
    if (!record || !store_) {
        if (error) {
            *error = "masstree cluster stats store is unavailable";
        }
        return false;
    }

    std::string payload;
    std::string local_error;
    if (store_->Get(MasstreeClusterStatsCurrentKey(), &payload, &local_error)) {
        return DecodeClusterStats(payload, record, error) &&
               PopulateClusterDiscStats(store_, record, error);
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }

    const MasstreeOpticalProfile profile = MasstreeOpticalProfile::LegacyMixed();
    record->optical_layout_version = profile.layout_version;
    record->disk_node_count = 0;
    record->optical_node_count = profile.optical_node_count;
    record->disk_device_count = 0;
    record->optical_device_count = static_cast<uint64_t>(profile.optical_node_count) *
                                   static_cast<uint64_t>(profile.disks_per_node);
    record->total_capacity_bytes = profile.TotalCapacityBytesDecimal();
    record->used_capacity_bytes = "0";
    record->free_capacity_bytes = record->total_capacity_bytes;
    record->total_file_count = 0;
    record->total_file_bytes = "0";
    record->total_metadata_bytes = "0";
    record->avg_file_size_bytes = 0;
    record->min_file_size_bytes = profile.min_file_size_bytes;
    record->max_file_size_bytes = profile.max_file_size_bytes;
    record->cursor = MasstreeOpticalClusterCursor();
    if (!PopulateClusterDiscStats(store_, record, error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::LoadNamespaceStats(const std::string& namespace_id,
                                            MasstreeNamespaceStatsRecord* record,
                                            std::string* error) const {
    if (!record || !store_ || namespace_id.empty()) {
        if (error) {
            *error = "invalid masstree namespace stats load args";
        }
        return false;
    }
    std::string payload;
    std::string local_error;
    if (!store_->Get(MasstreeNamespaceStatsCurrentKey(namespace_id), &payload, &local_error)) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    return DecodeNamespaceStats(payload, record, error) &&
           NormalizeNamespaceStatsToCurrentOpticalLayout(record, error);
}

bool MasstreeStatsStore::LoadNamespaceStatsRaw(const std::string& namespace_id,
                                               bool* found,
                                               std::string* raw_value,
                                               std::string* error) const {
    if (!store_ || !found || !raw_value || namespace_id.empty()) {
        if (error) {
            *error = "invalid masstree namespace stats load args";
        }
        return false;
    }
    std::string local_error;
    if (store_->Get(MasstreeNamespaceStatsCurrentKey(namespace_id), raw_value, &local_error)) {
        *found = true;
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    *found = false;
    raw_value->clear();
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::LoadClusterStatsRaw(bool* found,
                                             std::string* raw_value,
                                             std::string* error) const {
    if (!store_ || !found || !raw_value) {
        if (error) {
            *error = "invalid masstree cluster stats load args";
        }
        return false;
    }
    std::string local_error;
    if (store_->Get(MasstreeClusterStatsCurrentKey(), raw_value, &local_error)) {
        *found = true;
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!local_error.empty()) {
        if (error) {
            *error = local_error;
        }
        return false;
    }
    *found = false;
    raw_value->clear();
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::PutClusterStats(const MasstreeClusterStatsRecord& record,
                                         rocksdb::WriteBatch* batch,
                                         std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "masstree cluster stats batch is null";
        }
        return false;
    }
    batch->Put(MasstreeClusterStatsCurrentKey(), EncodeClusterStats(record));
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::PutNamespaceStats(const MasstreeNamespaceStatsRecord& record,
                                           rocksdb::WriteBatch* batch,
                                           std::string* error) const {
    if (!batch || record.namespace_id.empty()) {
        if (error) {
            *error = "invalid masstree namespace stats write args";
        }
        return false;
    }
    batch->Put(MasstreeNamespaceStatsCurrentKey(record.namespace_id), EncodeNamespaceStats(record));
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::RestoreRawClusterStats(bool found,
                                                const std::string& raw_value,
                                                rocksdb::WriteBatch* batch,
                                                std::string* error) const {
    if (!batch) {
        if (error) {
            *error = "masstree cluster stats restore batch is null";
        }
        return false;
    }
    if (found) {
        batch->Put(MasstreeClusterStatsCurrentKey(), raw_value);
    } else {
        batch->Delete(MasstreeClusterStatsCurrentKey());
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::RestoreRawNamespaceStats(const std::string& namespace_id,
                                                  bool found,
                                                  const std::string& raw_value,
                                                  rocksdb::WriteBatch* batch,
                                                  std::string* error) const {
    if (!batch || namespace_id.empty()) {
        if (error) {
            *error = "invalid masstree namespace stats restore args";
        }
        return false;
    }
    const std::string key = MasstreeNamespaceStatsCurrentKey(namespace_id);
    if (found) {
        batch->Put(key, raw_value);
    } else {
        batch->Delete(key);
    }
    if (error) {
        error->clear();
    }
    return true;
}

MasstreeClusterStatsRecord MasstreeStatsStore::BuildUpdatedClusterStats(
    const MasstreeClusterStatsRecord& current,
    uint64_t delta_file_count,
    const std::string& delta_file_bytes,
    uint64_t delta_metadata_bytes,
    const MasstreeOpticalClusterCursor& end_cursor) const {
    MasstreeClusterStatsRecord updated = current;
    updated.used_capacity_bytes = AddDecimalStrings(current.used_capacity_bytes, delta_file_bytes);
    updated.free_capacity_bytes = SubtractDecimalStrings(current.total_capacity_bytes, updated.used_capacity_bytes);
    updated.total_file_count = current.total_file_count + delta_file_count;
    updated.total_file_bytes = AddDecimalStrings(current.total_file_bytes, delta_file_bytes);
    updated.total_metadata_bytes =
        AddDecimalStrings(current.total_metadata_bytes, std::to_string(delta_metadata_bytes));
    updated.avg_file_size_bytes = updated.total_file_count == 0 ? 0 :
                                  DivideDecimalStringByU64(updated.total_file_bytes, updated.total_file_count);
    updated.cursor = end_cursor;
    return updated;
}

MasstreeClusterStatsRecord MasstreeStatsStore::BuildReplacedClusterStats(
    const MasstreeClusterStatsRecord& current,
    const MasstreeNamespaceStatsRecord* previous_namespace_stats,
    const MasstreeNamespaceStatsRecord& next_namespace_stats,
    const MasstreeOpticalClusterCursor& end_cursor) const {
    const uint64_t previous_file_count = previous_namespace_stats ? previous_namespace_stats->file_count : 0;
    const std::string previous_file_bytes = previous_namespace_stats
                                                ? NormalizeDecimalString(previous_namespace_stats->total_file_bytes)
                                                : std::string("0");
    const std::string previous_metadata_bytes = previous_namespace_stats
                                                    ? NormalizeDecimalString(previous_namespace_stats->total_metadata_bytes)
                                                    : std::string("0");
    const std::string next_file_bytes = NormalizeDecimalString(next_namespace_stats.total_file_bytes);
    const std::string next_metadata_bytes = NormalizeDecimalString(next_namespace_stats.total_metadata_bytes);

    MasstreeClusterStatsRecord updated = current;
    updated.total_file_count = current.total_file_count >= previous_file_count
                                   ? (current.total_file_count - previous_file_count + next_namespace_stats.file_count)
                                   : next_namespace_stats.file_count;

    const std::string base_used_capacity =
        CompareDecimalStrings(current.used_capacity_bytes, previous_file_bytes) >= 0
            ? SubtractDecimalStrings(current.used_capacity_bytes, previous_file_bytes)
            : std::string("0");
    updated.used_capacity_bytes = AddDecimalStrings(base_used_capacity, next_file_bytes);

    const std::string base_total_file_bytes =
        CompareDecimalStrings(current.total_file_bytes, previous_file_bytes) >= 0
            ? SubtractDecimalStrings(current.total_file_bytes, previous_file_bytes)
            : std::string("0");
    updated.total_file_bytes = AddDecimalStrings(base_total_file_bytes, next_file_bytes);

    const std::string base_total_metadata_bytes =
        CompareDecimalStrings(current.total_metadata_bytes, previous_metadata_bytes) >= 0
            ? SubtractDecimalStrings(current.total_metadata_bytes, previous_metadata_bytes)
            : std::string("0");
    updated.total_metadata_bytes = AddDecimalStrings(base_total_metadata_bytes, next_metadata_bytes);

    updated.free_capacity_bytes = SubtractDecimalStrings(current.total_capacity_bytes, updated.used_capacity_bytes);
    updated.avg_file_size_bytes = updated.total_file_count == 0
                                      ? 0
                                      : DivideDecimalStringByU64(updated.total_file_bytes, updated.total_file_count);
    updated.cursor = end_cursor;
    return updated;
}

bool MasstreeStatsStore::DecodeClusterStats(const std::string& payload,
                                            MasstreeClusterStatsRecord* record,
                                            std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree cluster stats decode output is null";
        }
        return false;
    }

    MasstreeClusterStatsRecord decoded;
    const MasstreeOpticalProfile profile = MasstreeOpticalProfile::LegacyMixed();
    decoded.optical_layout_version = profile.layout_version;
    decoded.min_file_size_bytes = profile.min_file_size_bytes;
    decoded.max_file_size_bytes = profile.max_file_size_bytes;
    std::istringstream input(payload);
    std::string line;
    bool header_checked = false;
    while (std::getline(input, line)) {
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "masstree_cluster_stats_v1") {
                if (error) {
                    *error = "invalid masstree cluster stats header";
                }
                return false;
            }
            continue;
        }
        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string key = Trim(trimmed.substr(0, eq));
        const std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "disk_node_count") {
            ParseU64Field(value, &decoded.disk_node_count);
        } else if (key == "optical_layout_version") {
            uint64_t parsed_version = 0;
            ParseU64Field(value, &parsed_version);
            decoded.optical_layout_version = static_cast<uint32_t>(parsed_version);
        } else if (key == "optical_node_count") {
            ParseU64Field(value, &decoded.optical_node_count);
        } else if (key == "disk_device_count") {
            ParseU64Field(value, &decoded.disk_device_count);
        } else if (key == "optical_device_count") {
            ParseU64Field(value, &decoded.optical_device_count);
        } else if (key == "total_capacity_bytes") {
            decoded.total_capacity_bytes = NormalizeDecimalString(value);
        } else if (key == "used_capacity_bytes") {
            decoded.used_capacity_bytes = NormalizeDecimalString(value);
        } else if (key == "free_capacity_bytes") {
            decoded.free_capacity_bytes = NormalizeDecimalString(value);
        } else if (key == "total_file_count") {
            ParseU64Field(value, &decoded.total_file_count);
        } else if (key == "total_file_bytes") {
            decoded.total_file_bytes = NormalizeDecimalString(value);
        } else if (key == "total_metadata_bytes") {
            decoded.total_metadata_bytes = NormalizeDecimalString(value);
        } else if (key == "avg_file_size_bytes") {
            ParseU64Field(value, &decoded.avg_file_size_bytes);
        } else if (key == "min_file_size_bytes") {
            ParseU64Field(value, &decoded.min_file_size_bytes);
        } else if (key == "max_file_size_bytes") {
            ParseU64Field(value, &decoded.max_file_size_bytes);
        } else if (key == "cursor") {
            CursorFromString(value, &decoded.cursor);
        } else if (key == "total_disc_count") {
            ParseU64Field(value, &decoded.total_disc_count);
        } else if (key == "used_disc_count") {
            ParseU64Field(value, &decoded.used_disc_count);
        } else if (key == "unused_disc_count") {
            ParseU64Field(value, &decoded.unused_disc_count);
        } else if (key == "sealed_legacy_disc_count") {
            ParseU64Field(value, &decoded.sealed_legacy_disc_count);
        } else if (key == "uniform_v2_used_disc_count") {
            ParseU64Field(value, &decoded.uniform_v2_used_disc_count);
        } else if (key == "allocated_file_bytes") {
            decoded.allocated_file_bytes = NormalizeDecimalString(value);
        }
    }

    if (!header_checked) {
        if (error) {
            *error = "empty masstree cluster stats payload";
        }
        return false;
    }

    *record = std::move(decoded);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeStatsStore::DecodeNamespaceStats(const std::string& payload,
                                              MasstreeNamespaceStatsRecord* record,
                                              std::string* error) {
    if (!record) {
        if (error) {
            *error = "masstree namespace stats decode output is null";
        }
        return false;
    }

    MasstreeNamespaceStatsRecord decoded;
    std::istringstream input(payload);
    std::string line;
    bool header_checked = false;
    while (std::getline(input, line)) {
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "masstree_namespace_stats_v1") {
                if (error) {
                    *error = "invalid masstree namespace stats header";
                }
                return false;
            }
            continue;
        }
        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string key = Trim(trimmed.substr(0, eq));
        const std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "namespace_id") {
            decoded.namespace_id = value;
        } else if (key == "optical_layout_version") {
            uint64_t parsed_version = 0;
            ParseU64Field(value, &parsed_version);
            decoded.optical_layout_version = static_cast<uint32_t>(parsed_version);
        } else if (key == "generation_id") {
            decoded.generation_id = value;
        } else if (key == "file_count") {
            ParseU64Field(value, &decoded.file_count);
        } else if (key == "total_file_bytes") {
            decoded.total_file_bytes = NormalizeDecimalString(value);
        } else if (key == "total_metadata_bytes") {
            decoded.total_metadata_bytes = NormalizeDecimalString(value);
        } else if (key == "avg_file_size_bytes") {
            ParseU64Field(value, &decoded.avg_file_size_bytes);
        } else if (key == "start_global_image_id") {
            ParseU64Field(value, &decoded.start_global_image_id);
        } else if (key == "end_global_image_id") {
            ParseU64Field(value, &decoded.end_global_image_id);
        } else if (key == "start_cursor") {
            CursorFromString(value, &decoded.start_cursor);
        } else if (key == "end_cursor") {
            CursorFromString(value, &decoded.end_cursor);
        }
    }
    if (!header_checked) {
        if (error) {
            *error = "empty masstree namespace stats payload";
        }
        return false;
    }
    *record = std::move(decoded);
    if (error) {
        error->clear();
    }
    return true;
}

std::string MasstreeStatsStore::EncodeClusterStats(const MasstreeClusterStatsRecord& record) {
    std::ostringstream out;
    out << "masstree_cluster_stats_v1\n";
    out << "optical_layout_version=" << record.optical_layout_version << "\n";
    out << "disk_node_count=" << record.disk_node_count << "\n";
    out << "optical_node_count=" << record.optical_node_count << "\n";
    out << "disk_device_count=" << record.disk_device_count << "\n";
    out << "optical_device_count=" << record.optical_device_count << "\n";
    out << "total_capacity_bytes=" << NormalizeDecimalString(record.total_capacity_bytes) << "\n";
    out << "used_capacity_bytes=" << NormalizeDecimalString(record.used_capacity_bytes) << "\n";
    out << "free_capacity_bytes=" << NormalizeDecimalString(record.free_capacity_bytes) << "\n";
    out << "total_file_count=" << record.total_file_count << "\n";
    out << "total_file_bytes=" << NormalizeDecimalString(record.total_file_bytes) << "\n";
    out << "total_metadata_bytes=" << NormalizeDecimalString(record.total_metadata_bytes) << "\n";
    out << "avg_file_size_bytes=" << record.avg_file_size_bytes << "\n";
    out << "min_file_size_bytes=" << record.min_file_size_bytes << "\n";
    out << "max_file_size_bytes=" << record.max_file_size_bytes << "\n";
    out << "cursor=" << CursorToString(record.cursor) << "\n";
    out << "total_disc_count=" << record.total_disc_count << "\n";
    out << "used_disc_count=" << record.used_disc_count << "\n";
    out << "unused_disc_count=" << record.unused_disc_count << "\n";
    out << "sealed_legacy_disc_count=" << record.sealed_legacy_disc_count << "\n";
    out << "uniform_v2_used_disc_count=" << record.uniform_v2_used_disc_count << "\n";
    out << "allocated_file_bytes=" << NormalizeDecimalString(record.allocated_file_bytes) << "\n";
    return out.str();
}

std::string MasstreeStatsStore::EncodeNamespaceStats(const MasstreeNamespaceStatsRecord& record) {
    std::ostringstream out;
    out << "masstree_namespace_stats_v1\n";
    out << "optical_layout_version=" << record.optical_layout_version << "\n";
    out << "namespace_id=" << record.namespace_id << "\n";
    out << "generation_id=" << record.generation_id << "\n";
    out << "file_count=" << record.file_count << "\n";
    out << "total_file_bytes=" << NormalizeDecimalString(record.total_file_bytes) << "\n";
    out << "total_metadata_bytes=" << NormalizeDecimalString(record.total_metadata_bytes) << "\n";
    out << "avg_file_size_bytes=" << record.avg_file_size_bytes << "\n";
    out << "start_global_image_id=" << record.start_global_image_id << "\n";
    out << "end_global_image_id=" << record.end_global_image_id << "\n";
    out << "start_cursor=" << CursorToString(record.start_cursor) << "\n";
    out << "end_cursor=" << CursorToString(record.end_cursor) << "\n";
    return out.str();
}

bool MasstreeStatsStore::ParseU64Field(const std::string& value, uint64_t* out) {
    if (!out) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

std::string MasstreeStatsStore::CursorToString(const MasstreeOpticalClusterCursor& cursor) {
    return std::to_string(cursor.node_index) + "," +
           std::to_string(cursor.disk_index) + "," +
           std::to_string(cursor.image_index_in_disk) + "," +
           std::to_string(cursor.image_used_bytes);
}

bool MasstreeStatsStore::CursorFromString(const std::string& value,
                                          MasstreeOpticalClusterCursor* cursor) {
    if (!cursor) {
        return false;
    }
    std::istringstream stream(value);
    std::string part;
    std::vector<std::string> fields;
    while (std::getline(stream, part, ',')) {
        fields.push_back(part);
    }
    if (fields.size() != 4U) {
        return false;
    }
    try {
        cursor->node_index = static_cast<uint32_t>(std::stoul(fields[0]));
        cursor->disk_index = static_cast<uint32_t>(std::stoul(fields[1]));
        cursor->image_index_in_disk = static_cast<uint32_t>(std::stoul(fields[2]));
        cursor->image_used_bytes = static_cast<uint64_t>(std::stoull(fields[3]));
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace zb::mds
