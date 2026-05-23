#include "sim/masstree/SimMasstreeOpticalAllocator.h"

#include <limits>

namespace zb::sim::masstree {

uint64_t SimMasstreeOpticalProfile::DiscCapacityBytes(uint32_t disk_index) const {
    return disk_index < kOneTbDiscsPerNode ? kDecimalTb : 10ULL * kDecimalTb;
}

uint64_t SimMasstreeOpticalProfile::GlobalImageId(const SimOpticalCursor& cursor) const {
    return (static_cast<uint64_t>(cursor.node_index) * kDiscsPerNode) + cursor.disk_index;
}

bool SimMasstreeOpticalProfile::Advance(SimOpticalCursor* cursor, std::string* error) const {
    if (!cursor) {
        if (error) {
            *error = "optical cursor output is null";
        }
        return false;
    }
    cursor->image_used_bytes = 0;
    cursor->image_index = 0;
    ++cursor->disk_index;
    if (cursor->disk_index >= kDiscsPerNode) {
        cursor->disk_index = 0;
        ++cursor->node_index;
    }
    if (cursor->node_index >= kOpticalNodeCount) {
        if (error) {
            *error = "sim optical capacity exhausted";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

SimMasstreeOpticalAllocator::SimMasstreeOpticalAllocator(SimOpticalCursor cursor)
    : cursor_(cursor) {
}

bool SimMasstreeOpticalAllocator::Allocate(uint64_t length_bytes,
                                           SimOpticalLocation* location,
                                           std::string* error) {
    if (!location) {
        if (error) {
            *error = "optical location output is null";
        }
        return false;
    }
    if (length_bytes == 0) {
        length_bytes = 1;
    }

    for (;;) {
        if (cursor_.node_index >= SimMasstreeOpticalProfile::kOpticalNodeCount ||
            cursor_.disk_index >= SimMasstreeOpticalProfile::kDiscsPerNode) {
            if (error) {
                *error = "invalid sim optical cursor";
            }
            return false;
        }
        const uint64_t capacity = profile_.DiscCapacityBytes(cursor_.disk_index);
        if (length_bytes > capacity) {
            if (error) {
                *error = "file is larger than one simulated optical disc";
            }
            return false;
        }
        if (cursor_.image_used_bytes + length_bytes <= capacity) {
            location->node_index = cursor_.node_index;
            location->disk_index = cursor_.disk_index;
            location->image_index = cursor_.image_index;
            location->offset_bytes = cursor_.image_used_bytes;
            location->length_bytes = length_bytes;
            cursor_.image_used_bytes += length_bytes;
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!profile_.Advance(&cursor_, error)) {
            return false;
        }
    }
}

uint64_t SimMasstreeOpticalAllocator::current_global_image_id() const {
    return profile_.GlobalImageId(cursor_);
}

} // namespace zb::sim::masstree
