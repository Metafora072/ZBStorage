#include "UnifiedInodeRecord.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "UnifiedInodeLocation.h"

namespace zb::mds {

namespace {

using namespace unified_inode_layout;

constexpr uint32_t kDefaultBlockSize = 4096;
constexpr uint64_t kBlocksUnitBytes = 512;

void WriteU16(std::string* out, size_t offset, uint16_t value) {
    std::memcpy(out->data() + offset, &value, sizeof(value));
}

void WriteU32(std::string* out, size_t offset, uint32_t value) {
    std::memcpy(out->data() + offset, &value, sizeof(value));
}

void WriteU64(std::string* out, size_t offset, uint64_t value) {
    std::memcpy(out->data() + offset, &value, sizeof(value));
}

uint16_t ReadU16(std::string_view data, size_t offset) {
    uint16_t value = 0;
    std::memcpy(&value, data.data() + offset, sizeof(value));
    return value;
}

uint32_t ReadU32(std::string_view data, size_t offset) {
    uint32_t value = 0;
    std::memcpy(&value, data.data() + offset, sizeof(value));
    return value;
}

uint64_t ReadU64(std::string_view data, size_t offset) {
    uint64_t value = 0;
    std::memcpy(&value, data.data() + offset, sizeof(value));
    return value;
}

bool IsValidInodeType(uint8_t value) {
    return value == static_cast<uint8_t>(zb::rpc::INODE_FILE) ||
           value == static_cast<uint8_t>(zb::rpc::INODE_DIR);
}

bool IsValidStorageTier(uint8_t value) {
    return value <= static_cast<uint8_t>(UnifiedStorageTier::kOptical);
}

bool IsValidLocationFormat(uint8_t value) {
    return value <= static_cast<uint8_t>(UnifiedLocationFormat::kOpticalPacked);
}

bool IsValidArchiveState(uint32_t value) {
    return value <= static_cast<uint32_t>(zb::rpc::INODE_ARCHIVE_IMAGE_READY);
}

zb::rpc::InodeStorageTier ToProtoStorageTier(uint8_t storage_tier) {
    switch (static_cast<UnifiedStorageTier>(storage_tier)) {
        case UnifiedStorageTier::kDisk:
            return zb::rpc::INODE_STORAGE_DISK;
        case UnifiedStorageTier::kOptical:
            return zb::rpc::INODE_STORAGE_OPTICAL;
        case UnifiedStorageTier::kNone:
        default:
            return zb::rpc::INODE_STORAGE_NONE;
    }
}

uint8_t ToUnifiedStorageTier(zb::rpc::InodeStorageTier storage_tier) {
    switch (storage_tier) {
        case zb::rpc::INODE_STORAGE_DISK:
            return static_cast<uint8_t>(UnifiedStorageTier::kDisk);
        case zb::rpc::INODE_STORAGE_OPTICAL:
            return static_cast<uint8_t>(UnifiedStorageTier::kOptical);
        case zb::rpc::INODE_STORAGE_NONE:
        default:
            return static_cast<uint8_t>(UnifiedStorageTier::kNone);
    }
}

uint64_t CeilDiv(uint64_t value, uint64_t divisor) {
    return divisor == 0 ? 0 : ((value + divisor - 1U) / divisor);
}

uint64_t NormalizeDirectorySize(uint8_t inode_type, uint64_t size_bytes) {
    if (inode_type == static_cast<uint8_t>(zb::rpc::INODE_DIR) && size_bytes == 0) {
        return kDefaultBlockSize;
    }
    return size_bytes;
}

uint32_t NormalizeBlockSize(uint32_t requested) {
    return requested == 0 ? kDefaultBlockSize : requested;
}

uint64_t NormalizeBlocks512(uint64_t requested, uint64_t size_bytes) {
    if (requested != 0) {
        return requested;
    }
    return CeilDiv(size_bytes, kBlocksUnitBytes);
}

bool NormalizePackedLocation(UnifiedInodeRecord* record, std::string* error) {
    if (!record) {
        if (error) {
            *error = "unified inode record is null";
        }
        return false;
    }
    record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kNone);
    record->storage_loc0 = 0;
    record->storage_loc1 = 0;
    switch (static_cast<UnifiedStorageTier>(record->storage_tier)) {
        case UnifiedStorageTier::kDisk:
            record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kDisk);
            record->storage_loc0 = record->disk_node_id;
            record->storage_loc1 = static_cast<uint64_t>(record->disk_id);
            break;
        case UnifiedStorageTier::kOptical:
            if (record->optical_disk_id > std::numeric_limits<uint32_t>::max()) {
                if (error) {
                    *error = "optical_disk_id exceeds packed v3 range";
                }
                return false;
            }
            record->location_format = static_cast<uint8_t>(UnifiedLocationFormat::kOpticalPacked);
            record->storage_loc0 = record->optical_node_id;
            record->storage_loc1 = (record->optical_disk_id << 32U) |
                                   static_cast<uint64_t>(record->optical_image_id);
            break;
        case UnifiedStorageTier::kNone:
        default:
            break;
    }
    return true;
}

void ExpandPackedLocation(UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    record->disk_node_id = 0;
    record->disk_id = 0;
    record->optical_node_id = 0;
    record->optical_disk_id = 0;
    record->optical_image_id = 0;
    switch (static_cast<UnifiedLocationFormat>(record->location_format)) {
        case UnifiedLocationFormat::kDisk:
            record->disk_node_id = record->storage_loc0;
            record->disk_id = static_cast<uint32_t>(record->storage_loc1);
            break;
        case UnifiedLocationFormat::kOpticalPacked:
            record->optical_node_id = record->storage_loc0;
            record->optical_disk_id = (record->storage_loc1 >> 32U);
            record->optical_image_id = static_cast<uint32_t>(record->storage_loc1 & 0xFFFFFFFFULL);
            break;
        case UnifiedLocationFormat::kNone:
        default:
            break;
    }
}

void FillDerivedPosixFields(UnifiedInodeRecord* record, bool normalize_dir_size) {
    if (!record) {
        return;
    }
    if (normalize_dir_size) {
        record->size_bytes = NormalizeDirectorySize(record->inode_type, record->size_bytes);
    }
    record->blksize = NormalizeBlockSize(record->blksize);
    record->blocks_512 = NormalizeBlocks512(record->blocks_512, record->size_bytes);
    record->file_name_len = static_cast<uint16_t>(record->file_name.size());
}

bool DecodeV3UnifiedInodeRecord(std::string_view data, UnifiedInodeRecord* out, std::string* error) {
    if (data.size() != kRecordSize) {
        if (error) {
            *error = "invalid unified inode size";
        }
        return false;
    }
    const uint8_t inode_type = static_cast<uint8_t>(data[kInodeTypeOffset]);
    const uint8_t storage_tier = static_cast<uint8_t>(data[kStorageTierOffset]);
    const uint8_t location_format = static_cast<uint8_t>(data[kLocationFormatOffset]);
    if (!IsValidInodeType(inode_type)) {
        if (error) {
            *error = "invalid unified inode_type";
        }
        return false;
    }
    if (!IsValidStorageTier(storage_tier)) {
        if (error) {
            *error = "invalid unified storage_tier";
        }
        return false;
    }
    if (!IsValidLocationFormat(location_format)) {
        if (error) {
            *error = "invalid unified location_format";
        }
        return false;
    }
    const uint16_t file_name_len = static_cast<uint16_t>(static_cast<uint8_t>(data[kFileNameLenOffset]));
    if (file_name_len > kFileNameSize) {
        if (error) {
            *error = "invalid unified file_name_len";
        }
        return false;
    }
    const uint32_t archive_state = ReadU32(data, kFileArchiveStateOffset);
    if (!IsValidArchiveState(archive_state)) {
        if (error) {
            *error = "invalid unified file_archive_state";
        }
        return false;
    }

    UnifiedInodeRecord record;
    record.version = kVersion;
    record.inode_type = inode_type;
    record.storage_tier = storage_tier;
    record.flags = static_cast<uint8_t>(data[kFlagsOffset]);
    record.file_name_len = file_name_len;
    record.location_format = location_format;
    record.inode_id = ReadU64(data, kInodeIdOffset);
    record.parent_inode_id = ReadU64(data, kParentInodeIdOffset);
    record.size_bytes = ReadU64(data, kSizeBytesOffset);
    record.atime = ReadU64(data, kAtimeOffset);
    record.mtime = ReadU64(data, kMtimeOffset);
    record.ctime = ReadU64(data, kCtimeOffset);
    record.version_no = ReadU64(data, kVersionNoOffset);
    record.mode = ReadU32(data, kModeOffset);
    record.uid = ReadU32(data, kUidOffset);
    record.gid = ReadU32(data, kGidOffset);
    record.nlink = ReadU32(data, kNlinkOffset);
    record.blksize = ReadU32(data, kBlockSizeOffset);
    record.file_archive_state = archive_state;
    record.blocks_512 = ReadU64(data, kBlocks512Offset);
    record.storage_loc0 = ReadU64(data, kStorageLoc0Offset);
    record.storage_loc1 = ReadU64(data, kStorageLoc1Offset);
    record.rdev_major = ReadU32(data, kRdevMajorOffset);
    record.rdev_minor = ReadU32(data, kRdevMinorOffset);
    record.object_unit_size = ReadU32(data, kObjectUnitSizeOffset);
    record.replica = ReadU16(data, kReplicaOffset);
    record.file_name.assign(data.data() + kFileNameOffset, file_name_len);
    ExpandPackedLocation(&record);
    // Tagged auxiliary archive location for a hot primary; old v3 reserved bytes are zero.
    if (record.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kDisk) &&
        ReadU32(data, kReservedOffset) == 0x31435241U) { // ARC1
        record.optical_image_id = ReadU32(data, kReservedOffset + 4);
        record.optical_disk_id = ReadU64(data, kReservedOffset + 8);
        record.optical_node_id = ReadU64(data, kReservedOffset + 16);
    }
    FillDerivedPosixFields(&record, false);
    *out = std::move(record);
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace

bool EncodeUnifiedInodeRecord(const UnifiedInodeRecord& record, std::string* out, std::string* error) {
    if (!out) {
        if (error) {
            *error = "unified inode output is null";
        }
        return false;
    }
    if (!IsValidInodeType(record.inode_type)) {
        if (error) {
            *error = "invalid inode_type";
        }
        return false;
    }
    if (!IsValidStorageTier(record.storage_tier)) {
        if (error) {
            *error = "invalid storage_tier";
        }
        return false;
    }
    if (!IsValidArchiveState(record.file_archive_state)) {
        if (error) {
            *error = "invalid file_archive_state";
        }
        return false;
    }
    if (record.file_name.size() > kFileNameSize) {
        if (error) {
            *error = "file_name exceeds 64 bytes";
        }
        return false;
    }
    if (record.object_unit_size > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "object_unit_size exceeds v3 range";
        }
        return false;
    }
    if (record.replica > std::numeric_limits<uint16_t>::max()) {
        if (error) {
            *error = "replica exceeds v3 range";
        }
        return false;
    }

    UnifiedInodeRecord normalized = record;
    normalized.version = kVersion;
    FillDerivedPosixFields(&normalized, true);
    if (!NormalizePackedLocation(&normalized, error)) {
        return false;
    }

    *out = std::string(kRecordSize, '\0');
    (*out)[kVersionOffset] = static_cast<char>(normalized.version);
    (*out)[kInodeTypeOffset] = static_cast<char>(normalized.inode_type);
    (*out)[kStorageTierOffset] = static_cast<char>(normalized.storage_tier);
    (*out)[kFlagsOffset] = static_cast<char>(normalized.flags);
    (*out)[kFileNameLenOffset] = static_cast<char>(normalized.file_name_len);
    (*out)[kLocationFormatOffset] = static_cast<char>(normalized.location_format);
    WriteU16(out, kReserved0Offset, 0);
    WriteU64(out, kInodeIdOffset, normalized.inode_id);
    WriteU64(out, kParentInodeIdOffset, normalized.parent_inode_id);
    WriteU64(out, kSizeBytesOffset, normalized.size_bytes);
    WriteU64(out, kAtimeOffset, normalized.atime);
    WriteU64(out, kMtimeOffset, normalized.mtime);
    WriteU64(out, kCtimeOffset, normalized.ctime);
    WriteU64(out, kVersionNoOffset, normalized.version_no);
    WriteU32(out, kModeOffset, normalized.mode);
    WriteU32(out, kUidOffset, normalized.uid);
    WriteU32(out, kGidOffset, normalized.gid);
    WriteU32(out, kNlinkOffset, normalized.nlink);
    WriteU32(out, kBlockSizeOffset, normalized.blksize);
    WriteU32(out, kFileArchiveStateOffset, normalized.file_archive_state);
    WriteU64(out, kBlocks512Offset, normalized.blocks_512);
    WriteU64(out, kStorageLoc0Offset, normalized.storage_loc0);
    WriteU64(out, kStorageLoc1Offset, normalized.storage_loc1);
    WriteU32(out, kRdevMajorOffset, normalized.rdev_major);
    WriteU32(out, kRdevMinorOffset, normalized.rdev_minor);
    WriteU32(out, kObjectUnitSizeOffset, static_cast<uint32_t>(normalized.object_unit_size));
    WriteU16(out, kReplicaOffset, static_cast<uint16_t>(normalized.replica));
    WriteU16(out, kReserved1Offset, 0);
    if (normalized.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kDisk) &&
        normalized.optical_image_id != 0) {
        WriteU32(out, kReservedOffset, 0x31435241U);
        WriteU32(out, kReservedOffset + 4, normalized.optical_image_id);
        WriteU64(out, kReservedOffset + 8, normalized.optical_disk_id);
        WriteU64(out, kReservedOffset + 16, normalized.optical_node_id);
    }
    if (!normalized.file_name.empty()) {
        std::memcpy(out->data() + kFileNameOffset, normalized.file_name.data(), normalized.file_name.size());
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool DecodeUnifiedInodeRecord(std::string_view data, UnifiedInodeRecord* out, std::string* error) {
    if (!out) {
        if (error) {
            *error = "unified inode output is null";
        }
        return false;
    }
    if (data.size() != kRecordSize) {
        if (error) {
            *error = "invalid unified inode size";
        }
        return false;
    }
    const uint8_t version = static_cast<uint8_t>(data[kVersionOffset]);
    if (version == kVersion) {
        return DecodeV3UnifiedInodeRecord(data, out, error);
    }
    if (error) {
        *error = "unified inode version mismatch";
    }
    return false;
}

void UnifiedInodeRecordToAttr(const UnifiedInodeRecord& record, zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    attr->Clear();
    attr->set_inode_id(record.inode_id);
    attr->set_type(static_cast<zb::rpc::InodeType>(record.inode_type));
    attr->set_mode(record.mode);
    attr->set_uid(record.uid);
    attr->set_gid(record.gid);
    attr->set_size(record.size_bytes);
    attr->set_atime(record.atime);
    attr->set_mtime(record.mtime);
    attr->set_ctime(record.ctime);
    attr->set_nlink(record.nlink);
    attr->set_blksize(record.blksize);
    attr->set_blocks_512(record.blocks_512);
    attr->set_rdev_major(record.rdev_major);
    attr->set_rdev_minor(record.rdev_minor);
    attr->set_object_unit_size(record.object_unit_size);
    attr->set_replica(record.replica);
    attr->set_version(record.version_no);
    attr->set_file_archive_state(static_cast<zb::rpc::InodeArchiveState>(record.file_archive_state));
    attr->set_file_name(record.file_name);
    attr->set_storage_tier(ToProtoStorageTier(record.storage_tier));
    attr->set_disk_node_id(0);
    attr->set_disk_id(0);
    attr->set_optical_node_id(0);
    attr->set_optical_disk_id(0);
    attr->set_optical_image_id(0);
    attr->set_disk_node_is_virtual(false);
    switch (static_cast<UnifiedStorageTier>(record.storage_tier)) {
        case UnifiedStorageTier::kDisk:
            attr->set_disk_node_id(record.disk_node_id);
            attr->set_disk_id(record.disk_id);
            attr->set_disk_node_is_virtual((record.flags & kUnifiedFlagVirtualDiskNode) != 0);
            attr->set_optical_node_id(record.optical_node_id);
            attr->set_optical_disk_id(record.optical_disk_id);
            attr->set_optical_image_id(record.optical_image_id);
            break;
        case UnifiedStorageTier::kOptical:
            attr->set_optical_node_id(record.optical_node_id);
            attr->set_optical_disk_id(record.optical_disk_id);
            attr->set_optical_image_id(record.optical_image_id);
            break;
        case UnifiedStorageTier::kNone:
        default:
            break;
    }
}

bool AttrToUnifiedInodeRecord(const zb::rpc::InodeAttr& attr,
                              uint64_t parent_inode_id,
                              UnifiedStorageTier storage_tier,
                              UnifiedInodeRecord* record,
                              std::string* error) {
    if (!record) {
        if (error) {
            *error = "unified inode output is null";
        }
        return false;
    }
    if (!IsValidInodeType(static_cast<uint8_t>(attr.type()))) {
        if (error) {
            *error = "invalid attr.type";
        }
        return false;
    }
    if (attr.file_name().size() > kFileNameSize) {
        if (error) {
            *error = "attr.file_name exceeds 64 bytes";
        }
        return false;
    }
    if (attr.object_unit_size() > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "attr.object_unit_size exceeds v3 range";
        }
        return false;
    }
    if (attr.replica() > std::numeric_limits<uint16_t>::max()) {
        if (error) {
            *error = "attr.replica exceeds v3 range";
        }
        return false;
    }
    if (attr.optical_disk_id() > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "attr.optical_disk_id exceeds packed v3 range";
        }
        return false;
    }

    UnifiedInodeRecord out;
    out.version = kVersion;
    out.inode_type = static_cast<uint8_t>(attr.type());
    out.storage_tier = attr.storage_tier() == zb::rpc::INODE_STORAGE_NONE
                           ? static_cast<uint8_t>(storage_tier)
                           : ToUnifiedStorageTier(attr.storage_tier());
    out.inode_id = attr.inode_id();
    out.parent_inode_id = parent_inode_id;
    out.size_bytes = attr.size();
    out.atime = attr.atime();
    out.mtime = attr.mtime();
    out.ctime = attr.ctime();
    out.version_no = attr.version();
    out.mode = attr.mode();
    out.uid = attr.uid();
    out.gid = attr.gid();
    out.nlink = attr.nlink();
    out.blksize = attr.blksize();
    out.file_archive_state = static_cast<uint32_t>(attr.file_archive_state());
    out.blocks_512 = attr.blocks_512();
    out.rdev_major = attr.rdev_major();
    out.rdev_minor = attr.rdev_minor();
    out.object_unit_size = attr.object_unit_size();
    out.replica = attr.replica();
    out.file_name = attr.file_name();
    out.file_name_len = static_cast<uint16_t>(out.file_name.size());
    out.disk_node_id = attr.disk_node_id();
    out.disk_id = attr.disk_id();
    out.optical_node_id = attr.optical_node_id();
    out.optical_disk_id = attr.optical_disk_id();
    out.optical_image_id = attr.optical_image_id();
    if (attr.disk_node_is_virtual()) {
        out.flags |= kUnifiedFlagVirtualDiskNode;
    }
    switch (static_cast<UnifiedStorageTier>(out.storage_tier)) {
        case UnifiedStorageTier::kDisk:
            out.optical_node_id = 0;
            out.optical_disk_id = 0;
            out.optical_image_id = 0;
            break;
        case UnifiedStorageTier::kOptical:
            out.disk_node_id = 0;
            out.disk_id = 0;
            out.flags &= ~kUnifiedFlagVirtualDiskNode;
            break;
        case UnifiedStorageTier::kNone:
        default:
            out.disk_node_id = 0;
            out.disk_id = 0;
            out.optical_node_id = 0;
            out.optical_disk_id = 0;
            out.optical_image_id = 0;
            out.flags &= ~kUnifiedFlagVirtualDiskNode;
            break;
    }
    FillDerivedPosixFields(&out, true);
    if (!NormalizePackedLocation(&out, error)) {
        return false;
    }
    *record = std::move(out);
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
