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
        uint32_t block;
        uint32_t slot;
    };
    static constexpr uint64_t kRoot = 1;
    static constexpr uint64_t kDirectoryTag = uint64_t{1} << 63;
    // Both physical block conversions use uint32_t. Limit both namespaces
    // to avoid physical block overflow and overlap with the directory tag.
    static constexpr uint32_t kMaxBlock = 0x7fffffffU;
    static constexpr uint32_t kSlotsPerBlock = 63;

    static std::optional<uint64_t> File(uint32_t block, uint32_t slot) {
        if (!block || block > kMaxBlock || slot >= kSlotsPerBlock) return std::nullopt;
        return (uint64_t{block} << 32) | slot;
    }
    static std::optional<uint64_t> Directory(uint32_t block) {
        if (!block || block > kMaxBlock) return std::nullopt;
        return kDirectoryTag | block;
    }
    static std::optional<Address> Decode(uint64_t id) {
        if (id == kRoot) return Address{Kind::Root, 0, 0};
        if (id & kDirectoryTag) {
            const uint64_t block = id & ~kDirectoryTag;
            if (!block || block > kMaxBlock) return std::nullopt;
            return Address{Kind::Directory, static_cast<uint32_t>(block), 0};
        }
        const auto block = static_cast<uint32_t>(id >> 32);
        const auto slot = static_cast<uint32_t>(id);
        if (!File(block, slot)) return std::nullopt;
        return Address{Kind::File, block, slot};
    }
};

} // namespace zb::mds
