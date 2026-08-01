#pragma once

#include "NamespaceModel.h"

#include <cstdint>
#include <string>
#include <vector>

namespace zb::storage_model {

struct SourceSegment {
    std::string node_id;
    uint64_t start_offset{0};
    uint64_t length{0};
};

struct ChunkFileSlice {
    FileId file_id;
    uint64_t chunk_offset{0};
    uint64_t file_offset{0};
    uint64_t length{0};
};

struct TransferChunk {
    std::string chunk_id;
    SourceSegment source;
    std::vector<ChunkFileSlice> files;
    uint64_t checksum_crc32{0};
};

struct SegmentManifest {
    std::string migration_id;
    std::array<uint8_t, 10> root_prefix{};
    OpticalLibraryId target_library_id{0};
    uint64_t create_time_ms{0};
    std::vector<TransferChunk> chunks;
};

struct ArchiveLocationUpdate {
    FileId file_id;
    std::vector<OpticalFileExtent> new_extents;
    uint64_t archive_time_ms{0};
    uint64_t erasure_group_id{0};
    uint32_t erasure_role{0};
};

struct ReadOperation {
    FileId file_id;
    OpticalImageId image_id{0};
    uint64_t image_offset{0};
    uint64_t length{0};
};

struct DiscReadGroup {
    OpticalDiscId disc_id{0};
    OpticalSlotIndex slot_index{0};
    std::vector<ReadOperation> operations;
};

struct LibraryReadPlan {
    OpticalLibraryId library_id{0};
    std::vector<DiscReadGroup> disc_groups;
};

struct BatchReadPlan {
    std::string request_id;
    std::vector<LibraryReadPlan> libraries;
};

bool ValidateSegmentManifest(const SegmentManifest& manifest, std::string* error);
bool ValidateBatchReadPlan(const BatchReadPlan& plan, std::string* error);

} // namespace zb::storage_model
