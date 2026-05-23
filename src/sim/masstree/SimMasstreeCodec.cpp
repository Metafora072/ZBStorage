#include "sim/masstree/SimMasstreeCodec.h"

#include <limits>

namespace zb::sim::masstree {

namespace {

template <typename T>
bool WritePod(std::ostream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    return static_cast<bool>(out);
}

template <typename T>
bool ReadPod(std::istream& in, T* value) {
    in.read(reinterpret_cast<char*>(value), sizeof(T));
    return static_cast<bool>(in);
}

bool WriteString(std::ostream& out, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
        return false;
    }
    const uint32_t size = static_cast<uint32_t>(value.size());
    return WritePod(out, size) && (size == 0 || static_cast<bool>(out.write(value.data(), size)));
}

bool ReadString(std::istream& in, std::string* value) {
    uint32_t size = 0;
    if (!ReadPod(in, &size)) {
        return false;
    }
    value->assign(size, '\0');
    return size == 0 || static_cast<bool>(in.read(&(*value)[0], size));
}

} // namespace

bool WriteInodeRecord(std::ostream& out, const SimInodeRecord& record, std::string* error) {
    const uint8_t type = static_cast<uint8_t>(record.type);
    const uint8_t tier = static_cast<uint8_t>(record.storage_tier);
    const bool ok =
        WritePod(out, record.inode_id) &&
        WritePod(out, record.parent_inode_id) &&
        WritePod(out, type) &&
        WritePod(out, tier) &&
        WritePod(out, record.size_bytes) &&
        WritePod(out, record.atime) &&
        WritePod(out, record.mtime) &&
        WritePod(out, record.ctime) &&
        WritePod(out, record.mode) &&
        WritePod(out, record.uid) &&
        WritePod(out, record.gid) &&
        WritePod(out, record.nlink) &&
        WriteString(out, record.file_name) &&
        WritePod(out, record.optical_node_id) &&
        WritePod(out, record.optical_disk_id) &&
        WritePod(out, record.optical_image_id) &&
        WritePod(out, record.optical_offset) &&
        WritePod(out, record.optical_length);
    if (!ok && error) {
        *error = "failed to write inode record";
    }
    return ok;
}

bool ReadInodeRecord(std::istream& in, SimInodeRecord* record, std::string* error) {
    if (!record) {
        if (error) {
            *error = "inode record output is null";
        }
        return false;
    }
    uint8_t type = 0;
    uint8_t tier = 0;
    const bool ok =
        ReadPod(in, &record->inode_id) &&
        ReadPod(in, &record->parent_inode_id) &&
        ReadPod(in, &type) &&
        ReadPod(in, &tier) &&
        ReadPod(in, &record->size_bytes) &&
        ReadPod(in, &record->atime) &&
        ReadPod(in, &record->mtime) &&
        ReadPod(in, &record->ctime) &&
        ReadPod(in, &record->mode) &&
        ReadPod(in, &record->uid) &&
        ReadPod(in, &record->gid) &&
        ReadPod(in, &record->nlink) &&
        ReadString(in, &record->file_name) &&
        ReadPod(in, &record->optical_node_id) &&
        ReadPod(in, &record->optical_disk_id) &&
        ReadPod(in, &record->optical_image_id) &&
        ReadPod(in, &record->optical_offset) &&
        ReadPod(in, &record->optical_length);
    if (!ok) {
        if (error) {
            *error = "failed to read inode record";
        }
        return false;
    }
    record->type = static_cast<SimInodeType>(type);
    record->storage_tier = static_cast<SimStorageTier>(tier);
    return true;
}

bool WriteDentryRecord(std::ostream& out, const SimDentryRecord& record, std::string* error) {
    const uint8_t type = static_cast<uint8_t>(record.type);
    const bool ok =
        WritePod(out, record.parent_inode_id) &&
        WriteString(out, record.name) &&
        WritePod(out, record.child_inode_id) &&
        WritePod(out, type);
    if (!ok && error) {
        *error = "failed to write dentry record";
    }
    return ok;
}

bool ReadDentryRecord(std::istream& in, SimDentryRecord* record, std::string* error) {
    if (!record) {
        if (error) {
            *error = "dentry record output is null";
        }
        return false;
    }
    uint8_t type = 0;
    const bool ok =
        ReadPod(in, &record->parent_inode_id) &&
        ReadString(in, &record->name) &&
        ReadPod(in, &record->child_inode_id) &&
        ReadPod(in, &type);
    if (!ok) {
        if (error) {
            *error = "failed to read dentry record";
        }
        return false;
    }
    record->type = static_cast<SimInodeType>(type);
    return true;
}

} // namespace zb::sim::masstree
