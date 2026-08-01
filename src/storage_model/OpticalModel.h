#pragma once

#include "Identifiers.h"
#include "NodeModel.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace zb::storage_model {

inline constexpr uint64_t kDesignOpticalDiscCapacityBytes = 1000000000000ULL;
inline constexpr uint64_t kDesignOpticalImageCapacityBytes = 10000000000ULL;
inline constexpr uint32_t kDesignImagesPerDisc = 100;
inline constexpr uint32_t kDesignSlotsPerLibrary = 10000;
inline constexpr uint32_t kDesignDrivesPerLibrary = 10;

enum class OpticalLibraryState {
    kActive = 0,
    kReadOnly = 1,
    kRetired = 2,
};

enum class DiscState {
    kBlank = 0,
    kRecording = 1,
    kRecorded = 2,
    kDefective = 3,
};

enum class OpticalDriveType {
    kWriterOnly = 0,
    kReaderOnly = 1,
    kReadWrite = 2,
};

enum class ResourceState {
    kIdle = 0,
    kReserved = 1,
    kBusy = 2,
    kFailed = 3,
    kRemoved = 4,
};

enum class OpticalImageState {
    kBuilding = 0,
    kSealedPendingBurn = 1,
    kBurning = 2,
    kRecorded = 3,
    kVerified = 4,
    kDefective = 5,
};

enum class ErasureCodeRole {
    kData = 0,
    kParity = 1,
};

struct DiscSlot {
    OpticalSlotIndex slot_index{0};
    OpticalDiscId disc_id{kNoOpticalDisc}; // Zero means an empty slot.
};

struct DiscSlotSegment {
    OpticalDiscId id_start{0};
    OpticalDiscId id_end{0};
    OpticalSlotIndex slot_start{0};
    OpticalSlotIndex slot_end{0};
};

struct DiscSlotPoint {
    OpticalDiscId disc_id{0};
    OpticalSlotIndex slot_index{0};
};

// Authoritative one-to-one DiscID <-> local slot mapping. SegmentSnapshot()
// provides the document's compact representation without making it the mutable
// source of truth.
class DiscSlotIndex {
public:
    bool Insert(OpticalDiscId disc_id, OpticalSlotIndex slot_index, std::string* error);
    bool Erase(OpticalDiscId disc_id);
    bool GetSlot(OpticalDiscId disc_id, OpticalSlotIndex* slot_index) const;
    bool GetDisc(OpticalSlotIndex slot_index, OpticalDiscId* disc_id) const;
    bool Contains(OpticalDiscId disc_id) const;
    size_t Size() const { return disc_to_slot_.size(); }
    std::vector<DiscSlotSegment> SegmentSnapshot() const;
    std::vector<DiscSlotPoint> PointSnapshot() const;

private:
    std::map<OpticalDiscId, OpticalSlotIndex> disc_to_slot_;
    std::map<OpticalSlotIndex, OpticalDiscId> slot_to_disc_;
};

struct OpticalDisc {
    OpticalDiscId disc_id{0};
    OpticalLibraryId library_id{0};
    OpticalSlotIndex slot_index{0};
    DiscState state{DiscState::kBlank};
    uint64_t capacity_bytes{kDesignOpticalDiscCapacityBytes};
    uint64_t used_bytes{0};
    uint32_t image_count{0};
    uint64_t read_bandwidth_bytes_per_sec{100000000ULL};
    uint64_t write_bandwidth_bytes_per_sec{10000000ULL};
    uint64_t manufacture_time_ms{0};
    uint64_t recorded_time_ms{0};
    uint64_t last_scan_time_ms{0};
    uint64_t corrected_error_count{0};
    uint64_t uncorrectable_error_count{0};
};

struct OpticalDrive {
    std::string drive_id;
    OpticalDriveType type{OpticalDriveType::kReadWrite};
    ResourceState state{ResourceState::kIdle};
    OpticalDiscId loaded_disc_id{kNoOpticalDisc};
    std::string current_task_id;
    uint64_t read_bandwidth_bytes_per_sec{100000000ULL};
    uint64_t write_bandwidth_bytes_per_sec{10000000ULL};
    uint64_t available_at_ms{0};
    std::string failure_reason;
};

struct MechanicalArm {
    std::string arm_id;
    ResourceState state{ResourceState::kIdle};
    OpticalSlotIndex current_slot{0};
    std::string target_drive_id;
    OpticalDiscId carrying_disc_id{kNoOpticalDisc};
    std::string current_task_id;
    uint64_t available_at_ms{0};
    std::string failure_reason;
};

struct ImageSuperblock {
    uint32_t magic{0};
    uint16_t format_version{1};
    OpticalImageId image_id{0};
    uint64_t capacity_bytes{kDesignOpticalImageCapacityBytes};
    uint64_t create_time_ms{0};
    std::string common_path_prefix;
    uint64_t erasure_group_id{0};
    ErasureCodeRole erasure_role{ErasureCodeRole::kData};
    std::string checksum_type{"SHA-256"};
    std::string superblock_checksum_hex;
};

struct ImageRegionLayout {
    uint64_t superblock_offset{0};
    uint64_t superblock_bytes{4096};
    uint64_t data_offset{4096};
    uint64_t data_bytes{0};
    uint64_t metadata_offset{0};
    uint64_t metadata_bytes{0};
    uint64_t checksum_offset{0};
    uint64_t checksum_bytes{0};
};

struct ImageFileExtent {
    std::string object_id;
    OpticalImageId image_id{0};
    uint64_t file_offset{0};
    uint64_t image_offset{0};
    uint64_t length{0};
    OpticalImageId previous_image_id{0};
    OpticalImageId next_image_id{0};
    uint32_t data_crc32{0}; // Current ImageStore compatibility checksum.
};

struct OpticalImageDescriptor {
    ImageSuperblock superblock;
    OpticalLibraryId library_id{0};
    OpticalDiscId disc_id{0};
    OpticalImageState state{OpticalImageState::kBuilding};
    ImageRegionLayout regions;
    uint64_t file_count{0};
    std::string image_checksum_hex;
};

struct ErasureCodeProfile {
    uint32_t data_image_count{10};
    uint32_t parity_image_count{4};
    uint64_t chunk_bytes{16000000ULL};
    std::string codec{"Reed-Solomon/ISA-L"};
};

struct ErasureCodeMember {
    OpticalImageId image_id{0};
    OpticalDiscId disc_id{0};
    ErasureCodeRole role{ErasureCodeRole::kData};
    uint32_t role_index{0};
};

struct ErasureCodeGroup {
    uint64_t group_id{0};
    ErasureCodeProfile profile;
    std::vector<ErasureCodeMember> members;
    uint32_t missing_members{0};
    bool recoverable{true};
};

struct OpticalLibraryInventory {
    OpticalLibraryId library_id{0};
    std::string node_id;
    OpticalLibraryState state{OpticalLibraryState::kActive};
    uint64_t observed_at_ms{0};
    std::vector<DiscSlot> slots;
    std::vector<OpticalDisc> discs;
    std::vector<OpticalDrive> drives;
    std::vector<MechanicalArm> arms;
    std::vector<DeviceInventory> cache_devices;
    uint64_t namespace_generation{0};
    uint64_t namespace_bytes{0};
    uint64_t staging_bytes{0};
};

bool ValidateOpticalDisc(const OpticalDisc& disc, std::string* error);
bool ValidateOpticalLibraryInventory(const OpticalLibraryInventory& library, std::string* error);
bool ValidateImageDescriptor(const OpticalImageDescriptor& image, std::string* error);

} // namespace zb::storage_model
