#pragma once

#include <cstdint>
#include <optional>

namespace zb::mds {

// Adapter contract matching MasstreeLHM-index's packed IDs. Root is exported
// as ZB ID 1 and must be resolved through the LHM instance's root reference.
// This codec does not establish that a decoded inode exists or is still live.
class LhmInodeId {
public:
    enum class Kind { Root, File, Directory };
    struct Address {
        Kind kind;
        uint64_t block;
        uint32_t slot;
    };
    static constexpr uint64_t kRoot = 1;
    static constexpr uint64_t kDirectoryTag = uint64_t{1} << 63;
    // v2: 48-bit storage references, 16-bit file slot; bit 63 tags directories.
    // Compact directory references additionally use bit 47 within the address.
    static constexpr uint64_t kMaxBlock = (uint64_t{1} << 47) - 1;
    static constexpr uint32_t kSlotsPerBlock = 63;

    static std::optional<uint64_t> File(uint64_t block, uint32_t slot) {
        if (!block || block > kMaxBlock || slot >= kSlotsPerBlock) return std::nullopt;
        return (uint64_t{block} << 16) | slot;
    }
    static std::optional<uint64_t> Directory(uint64_t block) {
        if (!block || block >= (uint64_t{1} << 48)) return std::nullopt;
        return kDirectoryTag | block;
    }
    static std::optional<Address> Decode(uint64_t id) {
        if (id == kRoot) return Address{Kind::Root, 0, 0};
        if (id & kDirectoryTag) {
            const uint64_t block = id & ~kDirectoryTag;
            if (!block || block >= (uint64_t{1} << 48)) return std::nullopt;
            return Address{Kind::Directory, static_cast<uint64_t>(block), 0};
        }
        const auto block = static_cast<uint64_t>(id >> 16);
        const auto slot = static_cast<uint32_t>(id & 0xffffULL);
        if (!File(block, slot)) return std::nullopt;
        return Address{Kind::File, block, slot};
    }
};

} // namespace zb::mds
