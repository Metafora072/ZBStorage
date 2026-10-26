#include "RocksNamespaceStore.h"

#include <memory>
#include <sstream>
#include <rocksdb/iterator.h>
#include "MetaCodec.h"
#include "MetaSchema.h"
#include "UnifiedInodeLocation.h"

namespace zb::mds {
namespace {
std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::istringstream stream(path);
    std::string token;
    while (std::getline(stream, token, '/')) if (!token.empty()) parts.push_back(token);
    return parts;
}
NamespaceStatus ReadFailure(const std::string& error, const char* missing) {
    return error.empty() ? NamespaceStatus{zb::rpc::MDS_NOT_FOUND, missing}
                         : NamespaceStatus{zb::rpc::MDS_INTERNAL_ERROR, error};
}
} // namespace

bool RocksNamespaceStore::GetInode(uint64_t id, zb::rpc::InodeAttr* attr,
                                    std::string* error) const {
    if (error) error->clear();
    if (!store_ || !attr || !id) {
        if (error) *error = "invalid inode lookup args";
        return false;
    }
    std::string data;
    return store_->Get(InodeKey(id), &data, error) &&
           MetaCodec::DecodeUnifiedInodeAttr(data, attr, error);
}

bool RocksNamespaceStore::GetDentry(uint64_t parent_inode,
                                const std::string& name,
                                uint64_t* inode_id,
                                std::string* error) const {
    if (error) error->clear();
    if (!store_ || !inode_id || parent_inode == 0 || name.empty()) {
        if (error) {
            *error = "invalid dentry lookup args";
        }
        return false;
    }
    std::string data;
    if (!store_->Get(DentryKey(parent_inode, name), &data, error)) {
        return false;
    }
    if (!MetaCodec::DecodeUInt64(data, inode_id)) {
        if (error) {
            *error = "invalid dentry";
        }
        return false;
    }
    return true;
}

bool RocksNamespaceStore::ResolvePath(const std::string& path,
                                  uint64_t* inode_id,
                                  zb::rpc::InodeAttr* attr,
                                  std::string* error) const {
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }

    std::vector<std::string> parts = SplitPath(path);
    uint64_t current = kRootInodeId;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return ReadVisibleInode(current, attr, error);
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    for (const auto& name : parts) {
        uint64_t next = 0;
        if (!GetDentry(current, name, &next, error)) {
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return ReadVisibleInode(current, attr, error);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool RocksNamespaceStore::ResolveParent(const std::string& path,
                                   uint64_t* parent_inode,
                                   std::string* name,
                                   std::string* error) const {
    if (!parent_inode || !name) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(path);
    if (parts.empty()) {
        if (error) {
            *error = "invalid path";
        }
        return false;
    }
    *name = parts.back();
    parts.pop_back();
    uint64_t current = kRootInodeId;
    for (const auto& part : parts) {
        std::string data;
        if (!store_->Get(DentryKey(current, part), &data, error)) {
            return false;
        }
        if (!MetaCodec::DecodeUInt64(data, &current)) {
            if (error) {
                *error = "invalid dentry";
            }
            return false;
        }
    }
    *parent_inode = current;
    return true;
}

bool RocksNamespaceStore::PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error) {
    if (error) error->clear();
    UnifiedInodeRecord record;
    std::string existing_payload;
    std::string get_error;
    if (store_->Get(InodeKey(inode_id), &existing_payload, &get_error)) {
        if (!MetaCodec::DecodeUnifiedInodeRecord(existing_payload, &record, error)) return false;
        ApplyAttrToUnifiedRecord(attr, &record);
        // A hot primary may also carry an archive copy. Keep the primary location intact.
        if (record.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kDisk)) {
            record.optical_node_id = attr.optical_node_id();
            record.optical_disk_id = attr.optical_disk_id();
            record.optical_image_id = attr.optical_image_id();
        }
    } else {
        if (!get_error.empty()) {
            if (error) *error = get_error;
            return false;
        }
        if (!AttrToUnifiedInodeRecord(attr, 0, UnifiedStorageTier::kNone, &record, error)) return false;
    }

    std::string payload;
    if (!MetaCodec::EncodeUnifiedInodeRecord(record, &payload, error)) {
        return false;
    }
    return store_->Put(InodeKey(inode_id), payload, error);
}

uint64_t RocksNamespaceStore::AllocateInodeId(zb::rpc::InodeType type, std::string* error) {
    if (type != zb::rpc::INODE_FILE && type != zb::rpc::INODE_DIR) {
        if (error) *error = "invalid inode type";
        return 0;
    }
    std::lock_guard<std::mutex> guard(inode_id_mu_);
    if (error) error->clear();
    std::string value;
    uint64_t next_id = kRootInodeId + 1;
    if (store_->Get(NextInodeKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "Invalid next inode value";
            }
            return 0;
        }
    } else if (error && !error->empty()) {
        return 0;
    }

    uint64_t allocated = next_id;
    uint64_t new_value = next_id + 1;
    if (!store_->Put(NextInodeKey(), MetaCodec::EncodeUInt64(new_value), error)) {
        return 0;
    }
    return allocated;
}

bool RocksNamespaceStore::Readdir(const std::string& path,
                                 const std::string& start_after,
                                 uint32_t limit,
                                 std::vector<zb::rpc::Dentry>* entries,
                                 bool* has_more,
                                 std::string* next_token,
                                 std::string* error) const {
    if (error) error->clear();
    uint64_t parent_inode = 0;
    zb::rpc::InodeAttr attr;
    if (!ResolvePath(path, &parent_inode, &attr, error)) return false;
    if (attr.type() != zb::rpc::INODE_DIR) {
        if (error) *error = "not a directory";
        return false;
    }
    if (!store_ || !entries || !store_->db() || parent_inode == 0) {
        if (error) {
            *error = "invalid readdir args";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    const auto fail = [&](const std::string& message) {
        entries->clear();
        if (has_more) *has_more = false;
        if (next_token) next_token->clear();
        if (error) *error = message;
        return false;
    };

    const std::string prefix = DentryPrefix(parent_inode);
    const std::string seek_key = prefix + start_after;
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(start_after.empty() ? prefix : seek_key); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        const std::string name = it->key().ToString().substr(prefix.size());
        if (!start_after.empty() && name <= start_after) {
            continue;
        }
        uint64_t child_inode = 0;
        if (!MetaCodec::DecodeUInt64(it->value().ToString(), &child_inode)) {
            return fail("invalid directory entry: " + name);
        }
        zb::rpc::InodeAttr child_attr;
        std::string inode_error;
        if (!ReadVisibleInode(child_inode, &child_attr, &inode_error)) {
            return fail(inode_error.empty() ? "directory entry inode is missing: " + name : inode_error);
        }
        zb::rpc::Dentry entry;
        entry.set_name(name);
        entry.set_inode_id(child_inode);
        entry.set_type(child_attr.type());
        entries->push_back(std::move(entry));
        if (next_token) {
            *next_token = name;
        }
        if (limit > 0 && entries->size() >= limit) {
            for (it->Next(); it->Valid(); it->Next()) {
                if (!it->key().starts_with(prefix)) {
                    break;
                }
                const std::string next_name = it->key().ToString().substr(prefix.size());
                if (next_name <= name) {
                    continue;
                }
                if (has_more) {
                    *has_more = true;
                }
                break;
            }
            if (!it->status().ok()) {
                return fail(it->status().ToString());
            }
            if (error) error->clear();
            return true;
        }
    }
    if (!it->status().ok()) {
        return fail(it->status().ToString());
    }
    if (error) error->clear();
    return true;
}

NamespaceStatus RocksNamespaceStore::Create(const std::string& path,
                                           const zb::rpc::InodeAttr& attr) {
    std::string error, name;
    uint64_t parent = 0, existing = 0;
    if (!ResolveParent(path, &parent, &name, &error)) return ReadFailure(error, "parent not found");
    if (GetDentry(parent, name, &existing, &error)) return {zb::rpc::MDS_ALREADY_EXISTS, "path already exists"};
    if (!error.empty()) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    zb::rpc::InodeAttr parent_attr;
    if (!GetInode(parent, &parent_attr, &error)) return ReadFailure(error, "parent not found");
    if (parent_attr.type() != zb::rpc::INODE_DIR) return {zb::rpc::MDS_INVALID_ARGUMENT, "parent is not a directory"};
    if (attr.inode_id() <= kRootInodeId) return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid inode ID"};

    auto named_attr = attr;
    named_attr.set_file_name(name);
    UnifiedInodeRecord record;
    std::string payload;
    if (!AttrToUnifiedInodeRecord(named_attr, parent, UnifiedStorageTier::kNone, &record, &error) ||
        !MetaCodec::EncodeUnifiedInodeRecord(record, &payload, &error)) {
        return {zb::rpc::MDS_INTERNAL_ERROR, error};
    }
    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent, name), MetaCodec::EncodeUInt64(attr.inode_id()));
    batch.Put(InodeKey(attr.inode_id()), payload);
    if (!store_->WriteBatch(&batch, &error)) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    return {};
}

NamespaceStatus RocksNamespaceStore::Rename(const std::string& old_path,
                                           const std::string& new_path) {
    std::string error, old_name, new_name;
    uint64_t old_parent = 0, new_parent = 0, inode = 0, target = 0;
    if (!ResolveParent(old_path, &old_parent, &old_name, &error)) return ReadFailure(error, "old parent not found");
    if (!ResolveParent(new_path, &new_parent, &new_name, &error)) return ReadFailure(error, "new parent not found");
    if (!GetDentry(old_parent, old_name, &inode, &error)) return ReadFailure(error, "source not found");
    if (GetDentry(new_parent, new_name, &target, &error)) return {zb::rpc::MDS_ALREADY_EXISTS, "target exists"};
    if (!error.empty()) return {zb::rpc::MDS_INTERNAL_ERROR, error};

    UnifiedInodeRecord record;
    std::string payload;
    if (!store_->Get(InodeKey(inode), &payload, &error)) return ReadFailure(error, "inode not found");
    if (!MetaCodec::DecodeUnifiedInodeRecord(payload, &record, &error)) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    record.parent_inode_id = new_parent;
    record.file_name = new_name;
    record.file_name_len = static_cast<uint16_t>(new_name.size());
    if (!MetaCodec::EncodeUnifiedInodeRecord(record, &payload, &error)) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(old_parent, old_name));
    batch.Put(DentryKey(new_parent, new_name), MetaCodec::EncodeUInt64(inode));
    batch.Put(InodeKey(inode), payload);
    if (!store_->WriteBatch(&batch, &error)) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    return {};
}

NamespaceStatus RocksNamespaceStore::Remove(const std::string& path,
                                           zb::rpc::InodeType expected_type) {
    if (SplitPath(path).empty()) return {zb::rpc::MDS_INVALID_ARGUMENT, "cannot remove root"};
    std::string error, name;
    uint64_t parent = 0, inode = 0;
    if (!ResolveParent(path, &parent, &name, &error)) return ReadFailure(error, "parent not found");
    if (!GetDentry(parent, name, &inode, &error)) return ReadFailure(error, "path not found");
    zb::rpc::InodeAttr attr;
    if (!GetInode(inode, &attr, &error)) return ReadFailure(error, "inode not found");
    if (attr.type() != expected_type) {
        return {zb::rpc::MDS_INVALID_ARGUMENT, expected_type == zb::rpc::INODE_DIR ? "not a directory" : "not a file"};
    }
    if (expected_type == zb::rpc::INODE_DIR) {
        const auto prefix = DentryPrefix(inode);
        std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
        it->Seek(prefix);
        if (!it->status().ok()) return {zb::rpc::MDS_INTERNAL_ERROR, it->status().ToString()};
        if (it->Valid() && it->key().starts_with(prefix)) return {zb::rpc::MDS_NOT_EMPTY, "directory not empty"};
    }
    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent, name));
    batch.Delete(InodeKey(inode));
    if (!store_->WriteBatch(&batch, &error)) return {zb::rpc::MDS_INTERNAL_ERROR, error};
    return {};
}

} // namespace zb::mds
