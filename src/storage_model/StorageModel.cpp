#include "Identifiers.h"
#include "NamespaceModel.h"
#include "NodeModel.h"
#include "OpticalModel.h"
#include "WorkflowModel.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>
#include <unordered_set>

namespace zb::storage_model {

namespace {

bool Fail(const std::string& message, std::string* error) {
    if (error) {
        *error = message;
    }
    return false;
}

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs
               ? std::numeric_limits<uint64_t>::max()
               : lhs + rhs;
}

bool IsDataDevice(DeviceKind kind) {
    return kind == DeviceKind::kHdd || kind == DeviceKind::kSsd || kind == DeviceKind::kDisc;
}

} // namespace

bool IsValidOpticalLibraryId(OpticalLibraryId id) {
    return id > 0 && id <= kMaxOpticalLibraryId;
}

bool IsValidOpticalImageId(OpticalImageId id) {
    return id > 0 && id <= kMaxOpticalImageId;
}

bool IsZeroFileId(const FileId& id) {
    if (id.user_id != 0 || id.namespace_id != 0) {
        return false;
    }
    return std::all_of(id.directory_path_hash.begin(), id.directory_path_hash.end(),
                       [](uint8_t value) { return value == 0; }) &&
           std::all_of(id.file_name_hash.begin(), id.file_name_hash.end(),
                       [](uint8_t value) { return value == 0; });
}

std::string FileIdHex(const FileId& id) {
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(4) << id.user_id
        << std::setw(4) << id.namespace_id;
    for (uint8_t value : id.directory_path_hash) {
        out << std::setw(2) << static_cast<unsigned>(value);
    }
    for (uint8_t value : id.file_name_hash) {
        out << std::setw(2) << static_cast<unsigned>(value);
    }
    return out.str();
}

uint64_t DeviceBandwidthBytesPerSecond(const DeviceGroup& group, IoDirection direction) {
    const uint64_t directional = direction == IoDirection::kRead
                                     ? group.per_device_read_bandwidth_bytes_per_sec
                                     : group.per_device_write_bandwidth_bytes_per_sec;
    return directional == 0 ? group.per_device_bandwidth_bytes_per_sec : directional;
}

uint64_t DeviceUsedBytes(const DeviceInventory& device) {
    return device.capacity_bytes > device.free_bytes
               ? device.capacity_bytes - device.free_bytes
               : 0;
}

uint64_t InventoryCapacityBytes(const NodeInventorySnapshot& snapshot) {
    uint64_t total = 0;
    for (const auto& device : snapshot.devices) {
        if (IsDataDevice(device.kind)) {
            total = SaturatingAdd(total, device.capacity_bytes);
        }
    }
    return total;
}

uint64_t InventoryFreeBytes(const NodeInventorySnapshot& snapshot) {
    uint64_t total = 0;
    for (const auto& device : snapshot.devices) {
        if (IsDataDevice(device.kind)) {
            total = SaturatingAdd(total, std::min(device.free_bytes, device.capacity_bytes));
        }
    }
    return total;
}

uint64_t InventoryUsedBytes(const NodeInventorySnapshot& snapshot) {
    uint64_t total = 0;
    for (const auto& device : snapshot.devices) {
        if (IsDataDevice(device.kind)) {
            total = SaturatingAdd(total, DeviceUsedBytes(device));
        }
    }
    return total;
}

uint64_t InventoryHealthyCapacityBytes(const NodeInventorySnapshot& snapshot) {
    uint64_t total = 0;
    for (const auto& device : snapshot.devices) {
        if (device.is_healthy && IsDataDevice(device.kind)) {
            total = SaturatingAdd(total, device.capacity_bytes);
        }
    }
    return total;
}

uint64_t InventoryHealthyFreeBytes(const NodeInventorySnapshot& snapshot) {
    uint64_t total = 0;
    for (const auto& device : snapshot.devices) {
        if (device.is_healthy && IsDataDevice(device.kind)) {
            total = SaturatingAdd(total, std::min(device.free_bytes, device.capacity_bytes));
        }
    }
    return total;
}

bool ValidateNodeHardwareProfile(const NodeHardwareProfile& profile, std::string* error) {
    if (profile.node_id.empty()) {
        return Fail("node_id is empty", error);
    }
    if (profile.logical_node_count == 0) {
        return Fail("logical_node_count must be greater than zero", error);
    }
    if (profile.technology_generation == 0) {
        return Fail("technology_generation must be greater than zero", error);
    }
    if (profile.external_network_bandwidth_bytes_per_sec == 0) {
        return Fail("external network bandwidth must be greater than zero", error);
    }
    if (profile.device_groups.empty()) {
        return Fail("at least one device group is required", error);
    }
    std::unordered_set<std::string> names;
    bool has_hdd_or_ssd = false;
    bool has_ssd = false;
    bool has_disc = false;
    bool has_optical_drive = false;
    for (const auto& group : profile.device_groups) {
        if (group.name.empty() || group.device_count == 0) {
            return Fail("device group name and count are required", error);
        }
        if (!names.insert(group.name).second) {
            return Fail("duplicate device group name: " + group.name, error);
        }
        if (group.max_concurrency > group.device_count) {
            return Fail("device group concurrency exceeds device count: " + group.name, error);
        }
        if (IsDataDevice(group.kind) && group.device_capacity_bytes == 0) {
            return Fail("data device capacity must be greater than zero: " + group.name, error);
        }
        if ((group.kind == DeviceKind::kHdd || group.kind == DeviceKind::kSsd) &&
            DeviceBandwidthBytesPerSecond(group, IoDirection::kRead) == 0 &&
            DeviceBandwidthBytesPerSecond(group, IoDirection::kWrite) == 0) {
            return Fail("online device bandwidth must be greater than zero: " + group.name,
                        error);
        }
        if (group.kind == DeviceKind::kOpticalDrive &&
            (DeviceBandwidthBytesPerSecond(group, IoDirection::kRead) == 0 ||
             DeviceBandwidthBytesPerSecond(group, IoDirection::kWrite) == 0)) {
            return Fail("optical drive requires both read and write bandwidth: " + group.name,
                        error);
        }
        has_hdd_or_ssd = has_hdd_or_ssd || group.kind == DeviceKind::kHdd ||
                         group.kind == DeviceKind::kSsd;
        has_ssd = has_ssd || group.kind == DeviceKind::kSsd;
        has_disc = has_disc || group.kind == DeviceKind::kDisc;
        has_optical_drive = has_optical_drive || group.kind == DeviceKind::kOpticalDrive;
    }
    if (profile.kind == NodeKind::kStorage && !has_hdd_or_ssd) {
        return Fail("storage node requires an HDD or SSD group", error);
    }
    if (profile.kind == NodeKind::kMetadata && !has_ssd) {
        return Fail("metadata node requires an SSD group", error);
    }
    if (profile.kind == NodeKind::kOpticalLibrary &&
        (!has_hdd_or_ssd || !has_disc || !has_optical_drive)) {
        return Fail("optical library requires cache, disc, and optical-drive groups", error);
    }
    if (profile.power.peak_milliwatts < profile.power.medium_milliwatts ||
        profile.power.medium_milliwatts < profile.power.standby_milliwatts ||
        profile.power.standby_milliwatts < profile.power.off_milliwatts) {
        return Fail("power levels must satisfy peak >= medium >= standby >= off", error);
    }
    if (profile.power.peak_milliwatts == 0) {
        return Fail("peak power must be greater than zero", error);
    }
    if (profile.reliability.mean_lifetime_ms == 0 || profile.reliability.minimum_lifetime_ms == 0) {
        return Fail("lifetime mean and minimum must be greater than zero", error);
    }
    if (profile.reliability.model == ReliabilityModel::kBathtubCurve &&
        profile.reliability.wearout_start_age_ms != 0 &&
        profile.reliability.early_failure_window_ms >
            profile.reliability.wearout_start_age_ms) {
        return Fail("bathtub early-failure window exceeds wearout start age", error);
    }
    return true;
}

bool ValidateNodeInventorySnapshot(const NodeInventorySnapshot& snapshot, std::string* error) {
    if (snapshot.node_id.empty()) {
        return Fail("inventory node_id is empty", error);
    }
    std::unordered_set<std::string> ids;
    for (const auto& device : snapshot.devices) {
        if (device.disk_id.empty()) {
            return Fail("inventory disk_id is empty", error);
        }
        if (!ids.insert(device.disk_id).second) {
            return Fail("duplicate inventory disk_id: " + device.disk_id, error);
        }
        if (device.free_bytes > device.capacity_bytes) {
            return Fail("device free space exceeds capacity: " + device.disk_id, error);
        }
    }
    return true;
}

bool ValidateOpticalLibraryStatus(const OpticalLibraryStatus& status, std::string* error) {
    if (!status.observed) {
        return true;
    }
    if (status.local_disc_count > status.total_slots) {
        return Fail("optical local disc count exceeds total slots", error);
    }
    uint64_t classified = status.blank_disc_count;
    classified = SaturatingAdd(classified, status.recording_disc_count);
    classified = SaturatingAdd(classified, status.recorded_disc_count);
    classified = SaturatingAdd(classified, status.defective_disc_count);
    if (classified != status.local_disc_count) {
        return Fail("optical disc state counts do not equal local disc count", error);
    }
    if (status.staging_used_bytes > status.staging_capacity_bytes) {
        return Fail("optical staging used space exceeds capacity", error);
    }
    return true;
}

bool DiscSlotIndex::Insert(OpticalDiscId disc_id, OpticalSlotIndex slot_index, std::string* error) {
    if (disc_id == kNoOpticalDisc) {
        return Fail("disc_id zero is reserved for an empty slot", error);
    }
    if (disc_to_slot_.count(disc_id) != 0) {
        return Fail("disc_id is already assigned", error);
    }
    if (slot_to_disc_.count(slot_index) != 0) {
        return Fail("slot is already occupied", error);
    }
    disc_to_slot_[disc_id] = slot_index;
    slot_to_disc_[slot_index] = disc_id;
    return true;
}

bool DiscSlotIndex::Erase(OpticalDiscId disc_id) {
    const auto it = disc_to_slot_.find(disc_id);
    if (it == disc_to_slot_.end()) {
        return false;
    }
    slot_to_disc_.erase(it->second);
    disc_to_slot_.erase(it);
    return true;
}

bool DiscSlotIndex::GetSlot(OpticalDiscId disc_id, OpticalSlotIndex* slot_index) const {
    const auto it = disc_to_slot_.find(disc_id);
    if (it == disc_to_slot_.end()) {
        return false;
    }
    if (slot_index) {
        *slot_index = it->second;
    }
    return true;
}

bool DiscSlotIndex::GetDisc(OpticalSlotIndex slot_index, OpticalDiscId* disc_id) const {
    const auto it = slot_to_disc_.find(slot_index);
    if (it == slot_to_disc_.end()) {
        return false;
    }
    if (disc_id) {
        *disc_id = it->second;
    }
    return true;
}

bool DiscSlotIndex::Contains(OpticalDiscId disc_id) const {
    return disc_to_slot_.count(disc_id) != 0;
}

std::vector<DiscSlotSegment> DiscSlotIndex::SegmentSnapshot() const {
    std::vector<DiscSlotSegment> segments;
    for (const auto& entry : disc_to_slot_) {
        if (!segments.empty()) {
            DiscSlotSegment& tail = segments.back();
            if (tail.id_end < std::numeric_limits<OpticalDiscId>::max() &&
                tail.slot_end < std::numeric_limits<OpticalSlotIndex>::max() &&
                entry.first == tail.id_end + 1 && entry.second == tail.slot_end + 1) {
                tail.id_end = entry.first;
                tail.slot_end = entry.second;
                continue;
            }
        }
        segments.push_back({entry.first, entry.first, entry.second, entry.second});
    }
    return segments;
}

std::vector<DiscSlotPoint> DiscSlotIndex::PointSnapshot() const {
    std::vector<DiscSlotPoint> points;
    for (const auto& segment : SegmentSnapshot()) {
        if (segment.id_start == segment.id_end) {
            points.push_back({segment.id_start, segment.slot_start});
        }
    }
    return points;
}

bool ValidateOpticalDisc(const OpticalDisc& disc, std::string* error) {
    if (disc.disc_id == kNoOpticalDisc) {
        return Fail("optical disc_id is zero", error);
    }
    if (!IsValidOpticalLibraryId(disc.library_id)) {
        return Fail("optical disc library_id exceeds 24-bit range or is zero", error);
    }
    if (disc.capacity_bytes == 0 || disc.used_bytes > disc.capacity_bytes) {
        return Fail("optical disc capacity/usage is invalid", error);
    }
    if (disc.image_count > kDesignImagesPerDisc &&
        disc.capacity_bytes == kDesignOpticalDiscCapacityBytes) {
        return Fail("one design-profile disc cannot contain more than 100 images", error);
    }
    return true;
}

bool ValidateOpticalLibraryInventory(const OpticalLibraryInventory& library, std::string* error) {
    if (!IsValidOpticalLibraryId(library.library_id) || library.node_id.empty()) {
        return Fail("library requires a 24-bit id and node_id", error);
    }
    std::set<OpticalSlotIndex> slots;
    std::set<OpticalDiscId> slotted_discs;
    for (const auto& slot : library.slots) {
        if (slot.slot_index >= kDesignSlotsPerLibrary) {
            return Fail("optical slot index exceeds the design library size", error);
        }
        if (!slots.insert(slot.slot_index).second) {
            return Fail("duplicate optical slot", error);
        }
        if (slot.disc_id != kNoOpticalDisc && !slotted_discs.insert(slot.disc_id).second) {
            return Fail("one disc is assigned to multiple slots", error);
        }
    }
    std::set<OpticalDiscId> discs;
    for (const auto& disc : library.discs) {
        if (!ValidateOpticalDisc(disc, error)) {
            return false;
        }
        if (disc.library_id != library.library_id || !discs.insert(disc.disc_id).second) {
            return Fail("library contains a foreign or duplicate disc", error);
        }
        if (slotted_discs.count(disc.disc_id) == 0) {
            return Fail("library disc has no slot assignment", error);
        }
        const auto slot = std::find_if(library.slots.begin(), library.slots.end(),
                                       [&disc](const DiscSlot& item) {
                                           return item.disc_id == disc.disc_id;
                                       });
        if (slot == library.slots.end() || slot->slot_index != disc.slot_index) {
            return Fail("library disc and slot disagree on physical position", error);
        }
    }
    for (OpticalDiscId disc_id : slotted_discs) {
        if (discs.count(disc_id) == 0) {
            return Fail("occupied slot has no optical disc record", error);
        }
    }
    if (library.state == OpticalLibraryState::kRetired && !library.discs.empty()) {
        return Fail("retired optical library must contain zero local discs", error);
    }
    return true;
}

bool ValidateImageDescriptor(const OpticalImageDescriptor& image, std::string* error) {
    if (!IsValidOpticalImageId(image.superblock.image_id)) {
        return Fail("image_id exceeds 40-bit range or is zero", error);
    }
    if (!IsValidOpticalLibraryId(image.library_id) || image.disc_id == kNoOpticalDisc) {
        return Fail("image location is incomplete", error);
    }
    const uint64_t capacity = image.superblock.capacity_bytes;
    const auto& regions = image.regions;
    if (capacity == 0 || regions.superblock_bytes == 0 ||
        regions.data_offset < regions.superblock_offset + regions.superblock_bytes ||
        regions.metadata_offset < regions.data_offset + regions.data_bytes ||
        regions.checksum_offset < regions.metadata_offset + regions.metadata_bytes ||
        regions.checksum_offset + regions.checksum_bytes > capacity) {
        return Fail("image regions overlap or exceed image capacity", error);
    }
    return true;
}

bool ValidateFileRecord(const FileRecord& record, std::string* error) {
    if (IsZeroFileId(record.file_id) || record.attributes.inode_id == 0) {
        return Fail("file record requires FileId and inode_id", error);
    }
    if (record.state == FileLifecycleState::kCached) {
        if (record.placement.tier != StorageTier::kMagnetic || record.placement.cached_extents.empty()) {
            return Fail("cached file requires magnetic extents", error);
        }
    } else if (record.state == FileLifecycleState::kArchived) {
        if (record.placement.tier != StorageTier::kOptical || record.placement.optical_extents.empty()) {
            return Fail("archived file requires optical extents", error);
        }
        for (const auto& extent : record.placement.optical_extents) {
            if (!IsValidOpticalLibraryId(extent.library_id) || extent.disc_id == kNoOpticalDisc ||
                !IsValidOpticalImageId(extent.image_id) || extent.length == 0) {
                return Fail("archived file contains an invalid optical extent", error);
            }
        }
    }
    return true;
}

bool ValidateNamespaceLeaf(const NamespaceLeaf& leaf, std::string* error) {
    if (!IsValidOpticalLibraryId(leaf.library_id) || leaf.bundle_id == 0 || leaf.file_count == 0) {
        return Fail("namespace leaf requires library, bundle and files", error);
    }
    return true;
}

bool ValidateSegmentManifest(const SegmentManifest& manifest, std::string* error) {
    if (manifest.migration_id.empty() || !IsValidOpticalLibraryId(manifest.target_library_id) ||
        manifest.chunks.empty()) {
        return Fail("segment manifest requires migration id, target library and chunks", error);
    }
    std::unordered_set<std::string> ids;
    for (const auto& chunk : manifest.chunks) {
        if (chunk.chunk_id.empty() || !ids.insert(chunk.chunk_id).second ||
            chunk.source.node_id.empty() || chunk.source.length == 0 || chunk.files.empty()) {
            return Fail("segment manifest contains an invalid or duplicate chunk", error);
        }
        for (const auto& file : chunk.files) {
            if (IsZeroFileId(file.file_id) || file.length == 0 ||
                file.chunk_offset > chunk.source.length ||
                file.length > chunk.source.length - file.chunk_offset) {
                return Fail("chunk file slice is outside the source segment", error);
            }
        }
    }
    return true;
}

bool ValidateBatchReadPlan(const BatchReadPlan& plan, std::string* error) {
    if (plan.request_id.empty() || plan.libraries.empty()) {
        return Fail("batch read plan requires request id and libraries", error);
    }
    for (const auto& library : plan.libraries) {
        if (!IsValidOpticalLibraryId(library.library_id) || library.disc_groups.empty()) {
            return Fail("batch read plan contains an invalid library", error);
        }
        for (const auto& disc : library.disc_groups) {
            if (disc.disc_id == kNoOpticalDisc || disc.operations.empty()) {
                return Fail("batch read plan contains an empty disc group", error);
            }
            for (const auto& op : disc.operations) {
                if (!IsValidOpticalImageId(op.image_id) || op.length == 0) {
                    return Fail("batch read operation is invalid", error);
                }
            }
        }
    }
    return true;
}

} // namespace zb::storage_model
