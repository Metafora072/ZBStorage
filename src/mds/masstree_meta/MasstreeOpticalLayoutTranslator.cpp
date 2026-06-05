#include "MasstreeOpticalLayoutTranslator.h"

#include <algorithm>
#include <cctype>
#include <limits>
#include <sstream>
#include <utility>

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

bool ParseU64(const std::string& value, uint64_t* parsed) {
    if (!parsed || value.empty()) {
        return false;
    }
    try {
        size_t consumed = 0;
        *parsed = std::stoull(value, &consumed);
        return consumed == value.size();
    } catch (...) {
        return false;
    }
}

bool ParseBool(const std::string& value, bool* parsed) {
    if (!parsed) {
        return false;
    }
    if (value == "true" || value == "1") {
        *parsed = true;
        return true;
    }
    if (value == "false" || value == "0") {
        *parsed = false;
        return true;
    }
    return false;
}

} // namespace

bool MasstreeOpticalLayoutTranslation::BuildForLegacyCursor(
    const MasstreeOpticalClusterCursor& legacy_cursor,
    MasstreeOpticalLayoutTranslation* translation,
    std::string* error) {
    if (!translation) {
        if (error) {
            *error = "optical layout translation output is null";
        }
        return false;
    }
    const MasstreeOpticalProfile legacy_profile = MasstreeOpticalProfile::LegacyMixed();
    if (!legacy_profile.IsValidCursor(legacy_cursor)) {
        if (error) {
            *error = "invalid legacy optical allocation cursor";
        }
        return false;
    }

    MasstreeOpticalLayoutTranslation built;
    const uint64_t last_global_image_id = legacy_profile.GlobalImageId(legacy_cursor);
    built.cutover_global_image_id =
        last_global_image_id + (legacy_cursor.image_used_bytes > 0 ? 1ULL : 0ULL);
    built.cutover_disc_ordinal =
        (built.cutover_global_image_id + built.images_per_disc - 1ULL) / built.images_per_disc;
    if (!built.Validate(error)) {
        return false;
    }
    *translation = built;
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeOpticalLayoutTranslation::Validate(std::string* error) const {
    const MasstreeOpticalProfile profile = MasstreeOpticalProfile::Fixed();
    const uint64_t total_disc_count = static_cast<uint64_t>(profile.optical_node_count) *
                                      static_cast<uint64_t>(profile.disks_per_node);
    if (legacy_profile != "legacy_mixed_v1" ||
        active_profile != "uniform_2tb_v2" ||
        image_capacity_bytes != profile.image_capacity_bytes ||
        disc_capacity_bytes != profile.DiskCapacityBytes(0) ||
        images_per_disc != profile.ImagesPerDisk(0) ||
        cutover_disc_ordinal > total_disc_count ||
        cutover_disc_ordinal !=
            (cutover_global_image_id + images_per_disc - 1ULL) / images_per_disc ||
        cutover_global_image_id > cutover_disc_ordinal * static_cast<uint64_t>(images_per_disc)) {
        if (error) {
            *error = "invalid optical layout translation record";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string MasstreeOpticalLayoutTranslation::Encode() const {
    std::ostringstream out;
    out << "optical_layout_translation_v1\n";
    out << "legacy_profile=" << legacy_profile << "\n";
    out << "active_profile=" << active_profile << "\n";
    out << "image_capacity_bytes=" << image_capacity_bytes << "\n";
    out << "disc_capacity_bytes=" << disc_capacity_bytes << "\n";
    out << "images_per_disc=" << images_per_disc << "\n";
    out << "cutover_global_image_id=" << cutover_global_image_id << "\n";
    out << "cutover_disc_ordinal=" << cutover_disc_ordinal << "\n";
    out << "legacy_read_compat=" << (legacy_read_compat ? "true" : "false") << "\n";
    return out.str();
}

bool MasstreeOpticalLayoutTranslation::Decode(const std::string& payload,
                                              MasstreeOpticalLayoutTranslation* translation,
                                              std::string* error) {
    if (!translation) {
        if (error) {
            *error = "optical layout translation output is null";
        }
        return false;
    }
    MasstreeOpticalLayoutTranslation decoded;
    std::istringstream input(payload);
    std::string line;
    bool header_checked = false;
    uint32_t seen_fields = 0;
    while (std::getline(input, line)) {
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "optical_layout_translation_v1") {
                if (error) {
                    *error = "invalid optical layout translation header";
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
        uint64_t parsed = 0;
        if (key == "legacy_profile") {
            decoded.legacy_profile = value;
            seen_fields |= 1U << 0U;
        } else if (key == "active_profile") {
            decoded.active_profile = value;
            seen_fields |= 1U << 1U;
        } else if (key == "image_capacity_bytes") {
            if (!ParseU64(value, &decoded.image_capacity_bytes)) {
                if (error) {
                    *error = "invalid image_capacity_bytes in optical layout translation";
                }
                return false;
            }
            seen_fields |= 1U << 2U;
        } else if (key == "disc_capacity_bytes") {
            if (!ParseU64(value, &decoded.disc_capacity_bytes)) {
                if (error) {
                    *error = "invalid disc_capacity_bytes in optical layout translation";
                }
                return false;
            }
            seen_fields |= 1U << 3U;
        } else if (key == "images_per_disc") {
            if (!ParseU64(value, &parsed) || parsed > std::numeric_limits<uint32_t>::max()) {
                if (error) {
                    *error = "invalid images_per_disc in optical layout translation";
                }
                return false;
            }
            decoded.images_per_disc = static_cast<uint32_t>(parsed);
            seen_fields |= 1U << 4U;
        } else if (key == "cutover_global_image_id") {
            if (!ParseU64(value, &decoded.cutover_global_image_id)) {
                if (error) {
                    *error = "invalid cutover_global_image_id in optical layout translation";
                }
                return false;
            }
            seen_fields |= 1U << 5U;
        } else if (key == "cutover_disc_ordinal") {
            if (!ParseU64(value, &decoded.cutover_disc_ordinal)) {
                if (error) {
                    *error = "invalid cutover_disc_ordinal in optical layout translation";
                }
                return false;
            }
            seen_fields |= 1U << 6U;
        } else if (key == "legacy_read_compat" && !ParseBool(value, &decoded.legacy_read_compat)) {
            if (error) {
                *error = "invalid legacy_read_compat in optical layout translation";
            }
            return false;
        } else if (key == "legacy_read_compat") {
            seen_fields |= 1U << 7U;
        }
    }
    if (!header_checked || seen_fields != 0xffU || !decoded.Validate(error)) {
        if (error && error->empty()) {
            *error = "optical layout translation record is incomplete";
        }
        return false;
    }
    *translation = decoded;
    if (error) {
        error->clear();
    }
    return true;
}

MasstreeOpticalLayoutTranslator::MasstreeOpticalLayoutTranslator(
    MasstreeOpticalLayoutTranslation translation)
    : translation_(std::move(translation)) {
}

bool MasstreeOpticalLayoutTranslator::TranslateLegacyToActive(
    uint32_t legacy_node_index,
    uint32_t legacy_disc_index,
    uint32_t legacy_image_index,
    uint32_t* active_node_index,
    uint32_t* active_disc_index,
    uint32_t* active_image_index,
    std::string* error) const {
    const MasstreeOpticalProfile legacy = MasstreeOpticalProfile::LegacyMixed();
    const MasstreeOpticalProfile active = MasstreeOpticalProfile::Fixed();
    if (!legacy.ConvertLocationTo(legacy_node_index,
                                  legacy_disc_index,
                                  legacy_image_index,
                                  active,
                                  active_node_index,
                                  active_disc_index,
                                  active_image_index)) {
        if (error) {
            *error = "failed to translate legacy optical location";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeOpticalLayoutTranslator::ResolveActiveReadAlias(
    uint32_t active_node_index,
    uint32_t active_disc_index,
    uint32_t active_image_index,
    bool* uses_legacy_alias,
    uint32_t* physical_node_index,
    uint32_t* physical_disc_index,
    uint32_t* physical_image_index,
    std::string* error) const {
    if (!uses_legacy_alias || !physical_node_index || !physical_disc_index || !physical_image_index) {
        if (error) {
            *error = "invalid optical read alias outputs";
        }
        return false;
    }
    const MasstreeOpticalProfile active = MasstreeOpticalProfile::Fixed();
    if (!active.IsValidCursor({active_node_index, active_disc_index, active_image_index, 0})) {
        if (error) {
            *error = "invalid active optical location";
        }
        return false;
    }
    const uint64_t global_image_id =
        active.GlobalImageId(active_node_index, active_disc_index, active_image_index);
    *uses_legacy_alias = translation_.legacy_read_compat &&
                         global_image_id < translation_.cutover_global_image_id;
    if (*uses_legacy_alias) {
        const MasstreeOpticalProfile legacy = MasstreeOpticalProfile::LegacyMixed();
        if (!legacy.DecodeGlobalImageId(global_image_id,
                                        physical_node_index,
                                        physical_disc_index,
                                        physical_image_index)) {
            if (error) {
                *error = "failed to resolve legacy optical read alias";
            }
            return false;
        }
    } else {
        *physical_node_index = active_node_index;
        *physical_disc_index = active_disc_index;
        *physical_image_index = active_image_index;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeOpticalLayoutTranslator::NextWritableCursor(MasstreeOpticalClusterCursor* cursor,
                                                         std::string* error) const {
    if (!cursor) {
        if (error) {
            *error = "next optical cursor output is null";
        }
        return false;
    }
    const MasstreeOpticalProfile active = MasstreeOpticalProfile::Fixed();
    const uint64_t global_image_id =
        translation_.cutover_disc_ordinal * static_cast<uint64_t>(translation_.images_per_disc);
    MasstreeOpticalClusterCursor next;
    if (!active.DecodeGlobalImageId(global_image_id,
                                    &next.node_index,
                                    &next.disk_index,
                                    &next.image_index_in_disk)) {
        if (error) {
            *error = "optical cluster has no writable disc after migration";
        }
        return false;
    }
    next.image_used_bytes = 0;
    *cursor = next;
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
