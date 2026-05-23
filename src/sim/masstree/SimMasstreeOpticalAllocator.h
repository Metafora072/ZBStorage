#pragma once

#include "sim/masstree/SimMasstreeTypes.h"

#include <cstdint>
#include <string>

namespace zb::sim::masstree {

class SimMasstreeOpticalProfile {
public:
    static constexpr uint32_t kOpticalNodeCount = 10000;
    static constexpr uint32_t kDiscsPerNode = 10000;
    static constexpr uint32_t kOneTbDiscsPerNode = kDiscsPerNode / 2;
    static constexpr uint64_t kDecimalTb = 1000000000000ULL;

    uint64_t DiscCapacityBytes(uint32_t disk_index) const;
    uint64_t GlobalImageId(const SimOpticalCursor& cursor) const;
    bool Advance(SimOpticalCursor* cursor, std::string* error) const;
};

class SimMasstreeOpticalAllocator {
public:
    explicit SimMasstreeOpticalAllocator(SimOpticalCursor cursor = {});

    bool Allocate(uint64_t length_bytes, SimOpticalLocation* location, std::string* error);
    const SimOpticalCursor& cursor() const { return cursor_; }
    uint64_t current_global_image_id() const;

private:
    SimMasstreeOpticalProfile profile_;
    SimOpticalCursor cursor_;
};

} // namespace zb::sim::masstree
