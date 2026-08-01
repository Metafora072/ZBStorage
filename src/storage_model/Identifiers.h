#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace zb::storage_model {

// Logical model version. This is not a serialized-format or protobuf version.
inline constexpr uint32_t kStorageModelVersion = 2;

using StorageNodeCompactId = uint16_t;
using OpticalLibraryId = uint32_t; // Only the low 24 bits are valid on media.
using OpticalDiscId = uint32_t;
using OpticalImageId = uint64_t;   // Only the low 40 bits are valid on media.
using OpticalSlotIndex = uint16_t;

inline constexpr OpticalLibraryId kMaxOpticalLibraryId = 0x00ffffffU;
inline constexpr OpticalImageId kMaxOpticalImageId = 0x000000ffffffffffULL;
inline constexpr OpticalDiscId kNoOpticalDisc = 0;

// The document-defined 24-byte logical file key. Serialization must write the
// integer fields explicitly; callers must not persist the native object bytes.
struct FileId {
    uint16_t user_id{0};
    uint16_t namespace_id{0};
    std::array<uint8_t, 10> directory_path_hash{};
    std::array<uint8_t, 10> file_name_hash{};

    bool operator==(const FileId& other) const {
        return user_id == other.user_id &&
               namespace_id == other.namespace_id &&
               directory_path_hash == other.directory_path_hash &&
               file_name_hash == other.file_name_hash;
    }
    bool operator!=(const FileId& other) const { return !(*this == other); }
};

bool IsValidOpticalLibraryId(OpticalLibraryId id);
bool IsValidOpticalImageId(OpticalImageId id);
bool IsZeroFileId(const FileId& id);
std::string FileIdHex(const FileId& id);

} // namespace zb::storage_model
