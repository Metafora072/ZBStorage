#include "mds/masstree_meta/MasstreeOpticalLayoutStore.h"
#include "mds/masstree_meta/MasstreeOpticalLayoutTranslator.h"
#include "mds/masstree_meta/MasstreeStatsStore.h"
#include "mds/masstree_meta/MasstreeDecimalUtils.h"
#include "mds/storage/RocksMetaStore.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_map>

namespace {

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  masstree_optical_layout_migrate_tool --db_path=<path>"
           " [--root_path=<path>|--snapshot_path=<path>] [--dry_run]\n";
}

std::unordered_map<std::string, std::string> ParseArgs(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> args;
    for (int i = 1; i < argc; ++i) {
        const std::string token = argv[i];
        if (token.rfind("--", 0) != 0) {
            continue;
        }
        const size_t eq = token.find('=');
        args[token.substr(2, eq == std::string::npos ? eq : eq - 2)] =
            eq == std::string::npos ? "1" : token.substr(eq + 1);
    }
    return args;
}

std::string GetArg(const std::unordered_map<std::string, std::string>& args,
                   const std::string& key) {
    const auto it = args.find(key);
    return it == args.end() ? std::string() : it->second;
}

void PrintTranslation(const zb::mds::MasstreeOpticalLayoutTranslation& translation,
                      const zb::mds::MasstreeOpticalClusterCursor& next_cursor) {
    std::cout << translation.Encode();
    std::cout << "next_cursor_node_index=" << next_cursor.node_index << "\n";
    std::cout << "next_cursor_disk_index=" << next_cursor.disk_index << "\n";
    std::cout << "next_cursor_image_index=" << next_cursor.image_index_in_disk << "\n";
    std::cout << "next_cursor_image_used_bytes=" << next_cursor.image_used_bytes << "\n";
}

} // namespace

int main(int argc, char* argv[]) {
    const auto args = ParseArgs(argc, argv);
    const std::string db_path = GetArg(args, "db_path");
    const std::string root_path = GetArg(args, "root_path");
    std::string snapshot_path = GetArg(args, "snapshot_path");
    const bool dry_run = args.find("dry_run") != args.end();
    if (snapshot_path.empty() && !root_path.empty()) {
        snapshot_path =
            (std::filesystem::path(root_path) / "data/mds/masstree_meta/optical_layout_translation.txt").string();
    }
    if (db_path.empty() || (!dry_run && snapshot_path.empty())) {
        PrintUsage();
        return 1;
    }

    std::string error;
    zb::mds::RocksMetaStore store;
    if (!store.Open(db_path, &error)) {
        std::cerr << "failed to open db: " << error << "\n";
        return 1;
    }

    zb::mds::MasstreeOpticalLayoutStore layout_store(&store);
    zb::mds::MasstreeOpticalLayoutTranslation existing;
    bool found = false;
    if (!layout_store.LoadCurrent(&found, &existing, &error)) {
        std::cerr << "failed to load optical layout: " << error << "\n";
        return 1;
    }
    if (found) {
        zb::mds::MasstreeOpticalClusterCursor next_cursor;
        zb::mds::MasstreeOpticalLayoutTranslator translator(existing);
        if (!translator.NextWritableCursor(&next_cursor, &error)) {
            std::cerr << "failed to decode existing optical layout: " << error << "\n";
            return 1;
        }
        std::cout << "status=already_migrated\n";
        PrintTranslation(existing, next_cursor);
        return 0;
    }

    zb::mds::MasstreeStatsStore stats_store(&store);
    zb::mds::MasstreeClusterStatsRecord stats;
    if (!stats_store.LoadClusterStats(&stats, &error)) {
        std::cerr << "failed to load cluster stats: " << error << "\n";
        return 1;
    }
    if (stats.optical_layout_version >= zb::mds::MasstreeOpticalProfile::kUniform2TbLayoutVersion) {
        std::cerr << "cluster stats already use uniform_2tb_v2 but layout translation is missing\n";
        return 1;
    }

    zb::mds::MasstreeOpticalLayoutTranslation translation;
    if (!zb::mds::MasstreeOpticalLayoutTranslation::BuildForLegacyCursor(stats.cursor,
                                                                         &translation,
                                                                         &error)) {
        std::cerr << "failed to build optical layout translation: " << error << "\n";
        return 1;
    }
    zb::mds::MasstreeOpticalClusterCursor next_cursor;
    zb::mds::MasstreeOpticalLayoutTranslator translator(translation);
    if (!translator.NextWritableCursor(&next_cursor, &error)) {
        std::cerr << "failed to build next optical cursor: " << error << "\n";
        return 1;
    }

    if (dry_run) {
        std::cout << "status=dry_run\n";
        PrintTranslation(translation, next_cursor);
        return 0;
    }

    const zb::mds::MasstreeOpticalProfile active = zb::mds::MasstreeOpticalProfile::Fixed();
    stats.optical_layout_version = active.layout_version;
    stats.optical_node_count = active.optical_node_count;
    stats.optical_device_count = static_cast<uint64_t>(active.optical_node_count) *
                                 static_cast<uint64_t>(active.disks_per_node);
    stats.total_capacity_bytes = active.TotalCapacityBytesDecimal();
    stats.free_capacity_bytes =
        zb::mds::SubtractDecimalStrings(stats.total_capacity_bytes, stats.used_capacity_bytes);
    stats.cursor = next_cursor;

    rocksdb::WriteBatch batch;
    if (!layout_store.PutCurrent(translation, &batch, &error) ||
        !stats_store.PutClusterStats(stats, &batch, &error) ||
        !store.WriteBatch(&batch, &error)) {
        std::cerr << "failed to commit optical layout migration: " << error << "\n";
        return 1;
    }
    if (!layout_store.SaveSnapshot(translation, snapshot_path, &error)) {
        std::cerr << "migration committed but failed to write audit snapshot: " << error << "\n";
        return 1;
    }
    std::cout << "status=migrated\n";
    PrintTranslation(translation, next_cursor);
    std::cout << "snapshot_path=" << snapshot_path << "\n";
    return 0;
}
