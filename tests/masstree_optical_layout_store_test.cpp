#include "mds/masstree_meta/MasstreeDecimalUtils.h"
#include "mds/masstree_meta/MasstreeOpticalLayoutStore.h"
#include "mds/masstree_meta/MasstreeOpticalLayoutTranslator.h"
#include "mds/masstree_meta/MasstreeStatsStore.h"
#include "mds/storage/RocksMetaStore.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << message << "\n";
        return false;
    }
    return true;
}

} // namespace

int main() {
    const auto suffix = std::chrono::steady_clock::now().time_since_epoch().count();
    const std::filesystem::path test_root =
        std::filesystem::temp_directory_path() / ("zb-optical-layout-test-" + std::to_string(suffix));
    const std::filesystem::path db_path = test_root / "rocksdb";
    const std::filesystem::path snapshot_path =
        test_root / "data/mds/masstree_meta/optical_layout_translation.txt";
    std::string error;
    std::filesystem::create_directories(test_root);

    {
        zb::mds::RocksMetaStore store;
        if (!store.Open(db_path.string(), &error)) {
            std::cerr << error << "\n";
            return 1;
        }
        zb::mds::MasstreeStatsStore stats_store(&store);

        const std::string legacy_raw_stats =
            "masstree_cluster_stats_v1\n"
            "disk_node_count=0\n"
            "optical_node_count=10000\n"
            "disk_device_count=0\n"
            "optical_device_count=100000000\n"
            "total_capacity_bytes=190000000000000000000\n"
            "used_capacity_bytes=101\n"
            "free_capacity_bytes=189999999999999999899\n"
            "total_file_count=7\n"
            "total_file_bytes=101\n"
            "total_metadata_bytes=11\n"
            "avg_file_size_bytes=14\n"
            "min_file_size_bytes=500000000\n"
            "max_file_size_bytes=1500000000\n"
            "cursor=5353,4517,0,24246134027\n";
        rocksdb::WriteBatch legacy_batch;
        if (!stats_store.RestoreRawClusterStats(true, legacy_raw_stats, &legacy_batch, &error) ||
            !store.WriteBatch(&legacy_batch, &error)) {
            std::cerr << error << "\n";
            return 1;
        }
        zb::mds::MasstreeClusterStatsRecord legacy_loaded;
        if (!stats_store.LoadClusterStats(&legacy_loaded, &error) ||
            !Expect(legacy_loaded.optical_layout_version == zb::mds::MasstreeOpticalProfile::kLegacyMixedLayoutVersion,
                    "unexpected legacy layout version") ||
            !Expect(legacy_loaded.total_capacity_bytes == "190000000000000000000",
                    "unexpected legacy total capacity") ||
            !Expect(legacy_loaded.total_disc_count == 100000000, "unexpected legacy total disc count") ||
            !Expect(legacy_loaded.used_disc_count == 53534518, "unexpected legacy used disc count") ||
            !Expect(legacy_loaded.unused_disc_count == 46465482, "unexpected legacy unused disc count") ||
            !Expect(legacy_loaded.allocated_file_bytes == "101", "unexpected legacy allocated file bytes")) {
            std::cerr << error << "\n";
            return 1;
        }

        zb::mds::MasstreeClusterStatsRecord stats;
        stats.optical_layout_version = zb::mds::MasstreeOpticalProfile::kLegacyMixedLayoutVersion;
        stats.cursor = {0, 8999, 9, 1};
        stats.used_capacity_bytes = "123";
        stats.total_file_bytes = "123";

        rocksdb::WriteBatch seed_batch;
        if (!stats_store.PutClusterStats(stats, &seed_batch, &error) ||
            !store.WriteBatch(&seed_batch, &error)) {
            std::cerr << error << "\n";
            return 1;
        }

        zb::mds::MasstreeOpticalLayoutTranslation translation;
        if (!zb::mds::MasstreeOpticalLayoutTranslation::BuildForLegacyCursor(stats.cursor,
                                                                             &translation,
                                                                             &error)) {
            std::cerr << error << "\n";
            return 1;
        }
        zb::mds::MasstreeOpticalClusterCursor next;
        zb::mds::MasstreeOpticalLayoutTranslator translator(translation);
        if (!translator.NextWritableCursor(&next, &error)) {
            std::cerr << error << "\n";
            return 1;
        }
        const zb::mds::MasstreeOpticalProfile active = zb::mds::MasstreeOpticalProfile::Fixed();
        stats.optical_layout_version = active.layout_version;
        stats.cursor = next;
        stats.total_capacity_bytes = active.TotalCapacityBytesDecimal();
        stats.free_capacity_bytes =
            zb::mds::SubtractDecimalStrings(stats.total_capacity_bytes, stats.used_capacity_bytes);

        zb::mds::MasstreeOpticalLayoutStore layout_store(&store);
        rocksdb::WriteBatch migration_batch;
        if (!layout_store.PutCurrent(translation, &migration_batch, &error) ||
            !stats_store.PutClusterStats(stats, &migration_batch, &error) ||
            !store.WriteBatch(&migration_batch, &error) ||
            !layout_store.SaveSnapshot(translation, snapshot_path.string(), &error)) {
            std::cerr << error << "\n";
            return 1;
        }

        zb::mds::MasstreeClusterStatsRecord loaded_stats;
        if (!stats_store.LoadClusterStats(&loaded_stats, &error) ||
            !Expect(loaded_stats.total_capacity_bytes == "200000000000000000000",
                    "unexpected active total capacity") ||
            !Expect(loaded_stats.total_disc_count == 100000000, "unexpected total disc count") ||
            !Expect(loaded_stats.used_disc_count == 4500, "unexpected used disc count") ||
            !Expect(loaded_stats.unused_disc_count == 99995500, "unexpected unused disc count") ||
            !Expect(loaded_stats.sealed_legacy_disc_count == 4500, "unexpected sealed disc count") ||
            !Expect(loaded_stats.uniform_v2_used_disc_count == 0, "unexpected uniform used disc count") ||
            !Expect(loaded_stats.allocated_file_bytes == "123", "unexpected allocated file bytes") ||
            !Expect(std::filesystem::exists(snapshot_path), "optical layout snapshot missing")) {
            std::cerr << error << "\n";
            return 1;
        }
    }
    std::filesystem::remove_all(test_root);
    return 0;
}
