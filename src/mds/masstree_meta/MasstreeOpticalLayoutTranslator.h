#pragma once

#include <cstdint>
#include <string>

#include "MasstreeOpticalProfile.h"

namespace zb::mds {

struct MasstreeOpticalLayoutTranslation {
    std::string legacy_profile{"legacy_mixed_v1"};
    std::string active_profile{"uniform_2tb_v2"};
    uint64_t image_capacity_bytes{100000000000ULL};
    uint64_t disc_capacity_bytes{2000000000000ULL};
    uint32_t images_per_disc{20};
    uint64_t cutover_global_image_id{0};
    uint64_t cutover_disc_ordinal{0};
    bool legacy_read_compat{true};

    static bool BuildForLegacyCursor(const MasstreeOpticalClusterCursor& legacy_cursor,
                                     MasstreeOpticalLayoutTranslation* translation,
                                     std::string* error);
    bool Validate(std::string* error) const;
    std::string Encode() const;
    static bool Decode(const std::string& payload,
                       MasstreeOpticalLayoutTranslation* translation,
                       std::string* error);
};

class MasstreeOpticalLayoutTranslator {
public:
    explicit MasstreeOpticalLayoutTranslator(MasstreeOpticalLayoutTranslation translation = {});

    bool TranslateLegacyToActive(uint32_t legacy_node_index,
                                 uint32_t legacy_disc_index,
                                 uint32_t legacy_image_index,
                                 uint32_t* active_node_index,
                                 uint32_t* active_disc_index,
                                 uint32_t* active_image_index,
                                 std::string* error) const;
    bool ResolveActiveReadAlias(uint32_t active_node_index,
                                uint32_t active_disc_index,
                                uint32_t active_image_index,
                                bool* uses_legacy_alias,
                                uint32_t* physical_node_index,
                                uint32_t* physical_disc_index,
                                uint32_t* physical_image_index,
                                std::string* error) const;
    bool NextWritableCursor(MasstreeOpticalClusterCursor* cursor, std::string* error) const;

    const MasstreeOpticalLayoutTranslation& translation() const { return translation_; }

private:
    MasstreeOpticalLayoutTranslation translation_;
};

} // namespace zb::mds
