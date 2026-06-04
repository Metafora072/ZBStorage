#pragma once

#include <cstdint>
#include <string>

#include "MasstreeDecimalUtils.h"

namespace zb::mds {

struct MasstreeOpticalClusterCursor {
    uint32_t node_index{0};
    uint32_t disk_index{0};
    uint32_t image_index_in_disk{0};
    uint64_t image_used_bytes{0};
};

struct MasstreeOpticalProfile {
    static constexpr uint32_t kLegacyMixedLayoutVersion = 1;
    static constexpr uint32_t kUniform2TbLayoutVersion = 2;
    static constexpr const char* kLegacyMixedLayoutName = "legacy_mixed_v1";
    static constexpr const char* kUniform2TbLayoutName = "uniform_2tb_v2";

    uint32_t layout_version{kUniform2TbLayoutVersion};
    uint32_t optical_node_count{10000};
    uint32_t disks_per_node{10000};
    uint32_t small_disks_per_node{10000};
    uint32_t large_disks_per_node{0};
    uint64_t small_disk_capacity_bytes{2000000000000ULL};
    uint64_t large_disk_capacity_bytes{2000000000000ULL};
    uint32_t images_per_small_disk{20};
    uint32_t images_per_large_disk{20};
    uint64_t image_capacity_bytes{100000000000ULL};
    uint64_t min_file_size_bytes{500000000ULL};
    uint64_t max_file_size_bytes{1500000000ULL};

    static MasstreeOpticalProfile Fixed() {
        return MasstreeOpticalProfile();
    }

    static MasstreeOpticalProfile LegacyMixed() {
        MasstreeOpticalProfile profile;
        profile.layout_version = kLegacyMixedLayoutVersion;
        profile.small_disks_per_node = 9000;
        profile.large_disks_per_node = 1000;
        profile.small_disk_capacity_bytes = 1000000000000ULL;
        profile.large_disk_capacity_bytes = 10000000000000ULL;
        profile.images_per_small_disk = 10;
        profile.images_per_large_disk = 100;
        return profile;
    }

    uint32_t ImagesPerDisk(uint32_t disk_index) const {
        return disk_index < small_disks_per_node ? images_per_small_disk : images_per_large_disk;
    }

    uint64_t DiskCapacityBytes(uint32_t disk_index) const {
        return disk_index < small_disks_per_node ? small_disk_capacity_bytes : large_disk_capacity_bytes;
    }

    uint32_t ImagesPerNode() const {
        return small_disks_per_node * images_per_small_disk +
               large_disks_per_node * images_per_large_disk;
    }

    uint64_t GlobalImageId(uint32_t node_index, uint32_t disk_index, uint32_t image_index_in_disk) const {
        uint64_t image_index_in_node = 0;
        if (disk_index < small_disks_per_node) {
            image_index_in_node = static_cast<uint64_t>(disk_index) *
                                      static_cast<uint64_t>(images_per_small_disk) +
                                  image_index_in_disk;
        } else {
            const uint32_t large_disk_slot = disk_index - small_disks_per_node;
            image_index_in_node = static_cast<uint64_t>(small_disks_per_node) *
                                      static_cast<uint64_t>(images_per_small_disk) +
                                  static_cast<uint64_t>(large_disk_slot) *
                                      static_cast<uint64_t>(images_per_large_disk) +
                                  image_index_in_disk;
        }
        return static_cast<uint64_t>(node_index) * static_cast<uint64_t>(ImagesPerNode()) + image_index_in_node;
    }

    uint64_t GlobalImageId(const MasstreeOpticalClusterCursor& cursor) const {
        return GlobalImageId(cursor.node_index, cursor.disk_index, cursor.image_index_in_disk);
    }

    bool DecodeGlobalImageId(uint64_t global_image_id,
                             uint32_t* node_index,
                             uint32_t* disk_index,
                             uint32_t* image_index_in_disk) const {
        const uint64_t images_per_node = static_cast<uint64_t>(ImagesPerNode());
        if (images_per_node == 0) {
            return false;
        }
        const uint64_t parsed_node_index = global_image_id / images_per_node;
        if (parsed_node_index >= optical_node_count) {
            return false;
        }
        const uint32_t image_index_in_node = static_cast<uint32_t>(global_image_id % images_per_node);
        const uint32_t small_region_images = small_disks_per_node * images_per_small_disk;
        uint32_t parsed_disk_index = 0;
        uint32_t parsed_image_index_in_disk = 0;
        if (image_index_in_node < small_region_images) {
            parsed_disk_index = image_index_in_node / images_per_small_disk;
            parsed_image_index_in_disk = image_index_in_node % images_per_small_disk;
        } else {
            const uint32_t large_region = image_index_in_node - small_region_images;
            if (images_per_large_disk == 0) {
                return false;
            }
            parsed_disk_index = small_disks_per_node + (large_region / images_per_large_disk);
            parsed_image_index_in_disk = large_region % images_per_large_disk;
        }
        if (parsed_disk_index >= disks_per_node ||
            parsed_image_index_in_disk >= ImagesPerDisk(parsed_disk_index)) {
            return false;
        }
        if (node_index) {
            *node_index = static_cast<uint32_t>(parsed_node_index);
        }
        if (disk_index) {
            *disk_index = parsed_disk_index;
        }
        if (image_index_in_disk) {
            *image_index_in_disk = parsed_image_index_in_disk;
        }
        return true;
    }

    std::string NodeId(uint32_t node_index) const {
        return "optical-node-" + std::to_string(node_index);
    }

    std::string DiskId(uint32_t disk_index) const {
        return "disk-" + std::to_string(disk_index);
    }

    bool IsValidCursor(const MasstreeOpticalClusterCursor& cursor) const {
        if (cursor.node_index >= optical_node_count || cursor.disk_index >= disks_per_node) {
            return false;
        }
        if (cursor.image_index_in_disk >= ImagesPerDisk(cursor.disk_index)) {
            return false;
        }
        return cursor.image_used_bytes <= image_capacity_bytes;
    }

    bool ConvertCursorTo(const MasstreeOpticalClusterCursor& source_cursor,
                         const MasstreeOpticalProfile& target_profile,
                         MasstreeOpticalClusterCursor* target_cursor) const {
        if (!target_cursor || !IsValidCursor(source_cursor)) {
            return false;
        }
        const uint64_t global_image_id = GlobalImageId(source_cursor);
        if (!target_profile.DecodeGlobalImageId(global_image_id,
                                                &target_cursor->node_index,
                                                &target_cursor->disk_index,
                                                &target_cursor->image_index_in_disk)) {
            return false;
        }
        target_cursor->image_used_bytes = source_cursor.image_used_bytes;
        return true;
    }

    bool ConvertLocationTo(uint32_t source_node_index,
                           uint32_t source_disk_index,
                           uint32_t source_image_index_in_disk,
                           const MasstreeOpticalProfile& target_profile,
                           uint32_t* target_node_index,
                           uint32_t* target_disk_index,
                           uint32_t* target_image_index_in_disk) const {
        if (source_node_index >= optical_node_count ||
            source_disk_index >= disks_per_node ||
            source_image_index_in_disk >= ImagesPerDisk(source_disk_index)) {
            return false;
        }
        return target_profile.DecodeGlobalImageId(GlobalImageId(source_node_index,
                                                                source_disk_index,
                                                                source_image_index_in_disk),
                                                  target_node_index,
                                                  target_disk_index,
                                                  target_image_index_in_disk);
    }

    std::string ImageId(uint64_t global_image_id) const {
        uint32_t node_index = 0;
        uint32_t disk_index = 0;
        uint32_t image_index_in_disk = 0;
        if (!DecodeGlobalImageId(global_image_id, &node_index, &disk_index, &image_index_in_disk)) {
            return std::string();
        }
        return "img-" + std::to_string(node_index) + "-" + std::to_string(disk_index) + "-" +
               std::to_string(image_index_in_disk);
    }

    std::string TotalCapacityBytesDecimal() const {
        MasstreeDecimalAccumulator total;
        uint64_t per_node_capacity = 0;
        for (uint32_t disk_index = 0; disk_index < disks_per_node; ++disk_index) {
            per_node_capacity += DiskCapacityBytes(disk_index);
        }
        for (uint32_t node_index = 0; node_index < optical_node_count; ++node_index) {
            (void)node_index;
            total.Add(per_node_capacity);
        }
        return total.ToString();
    }
};

} // namespace zb::mds
