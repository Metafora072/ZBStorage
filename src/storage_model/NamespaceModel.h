#pragma once

#include "Identifiers.h"

#include <array>
#include <cstdint>
#include <string>
#include <vector>

namespace zb::storage_model {

inline constexpr uint32_t kCachedInodeRecordBytes = 256;
inline constexpr uint32_t kArchivedInodeRecordBytes = 512;

enum class FileLifecycleState {
    kPending = 0,
    kCached = 1,
    kArchived = 2,
};

enum class StorageTier {
    kNone = 0,
    kMagnetic = 1,
    kOptical = 2,
};

struct PosixAttributes {
    uint64_t inode_id{0};
    uint64_t parent_inode_id{0};
    uint64_t size_bytes{0};
    uint64_t atime_ms{0};
    uint64_t mtime_ms{0};
    uint64_t ctime_ms{0};
    uint64_t version{0};
    uint32_t mode{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t link_count{0};
};

// One file slice on one node. The public model intentionally has no replica
// index: the current design treats every node as one independent copy.
struct CachedFileExtent {
    std::string node_id;
    std::string disk_id;
    std::string object_id;
    uint64_t file_offset{0};
    uint64_t storage_offset{0};
    uint64_t length{0};
};

struct OpticalFileExtent {
    OpticalLibraryId library_id{0};
    OpticalDiscId disc_id{0};
    OpticalImageId image_id{0};
    uint64_t file_offset{0};
    uint64_t image_offset{0};
    uint64_t length{0};
    OpticalImageId previous_image_id{0};
    OpticalImageId next_image_id{0};
};

struct FilePlacement {
    StorageTier tier{StorageTier::kNone};
    std::vector<CachedFileExtent> cached_extents;
    std::vector<OpticalFileExtent> optical_extents;
};

// Logical inode shared by workflows. It deliberately does not promise an
// in-memory packed layout; MDS codecs own the current 256-byte disk encoding.
struct FileRecord {
    FileId file_id;
    PosixAttributes attributes;
    FileLifecycleState state{FileLifecycleState::kPending};
    FilePlacement placement;
    std::string file_name;
    std::string relative_path;
    bool path_overflow{false};
    uint64_t path_overflow_key{0};
    bool obsolete{false};
    uint64_t archive_time_ms{0};
    uint64_t erasure_group_id{0};
    uint32_t erasure_role{0};
};

struct SegmentTableEntry {
    uint64_t base_offset{0};
    uint64_t length{0};
    uint64_t file_count{0};
};

struct DirectoryMarker {
    std::array<uint8_t, 10> directory_path_hash{};
    uint64_t create_time_ms{0};
};

struct RenameRedirect {
    FileId old_file_id;
    FileId new_file_id;
    uint64_t expire_time_ms{0};
};

struct NamespaceLeaf {
    std::array<uint8_t, 10> root_prefix{};
    OpticalLibraryId library_id{0};
    uint64_t bundle_id{0};
    uint64_t file_count{0};
    uint64_t total_size_bytes{0};
    uint64_t last_access_time_ms{0};
    std::array<uint8_t, 32> checksum_sha256{};
    // Logical reserve matching the document's 512-byte record budget. A codec
    // must pack the 24-bit library ID explicitly before claiming 512 bytes.
    std::array<uint8_t, 435> reserved{};
};

struct DiscSubNamespaceEntry {
    FileId file_id;
    OpticalImageId image_id{0};
    uint64_t image_offset{0};
    uint64_t compressed_length{0};
    PosixAttributes attributes;
    std::string relative_path;
};

struct NamespaceBundleDescriptor {
    uint64_t bundle_id{0};
    std::array<uint8_t, 10> root_prefix{};
    OpticalLibraryId library_id{0};
    uint64_t file_count{0};
    uint64_t total_size_bytes{0};
    std::array<uint8_t, 32> checksum_sha256{};
    uint64_t create_time_ms{0};
    uint32_t format_version{1};
};

bool ValidateFileRecord(const FileRecord& record, std::string* error);
bool ValidateNamespaceLeaf(const NamespaceLeaf& leaf, std::string* error);

} // namespace zb::storage_model
