#include "ModelStateStore.h"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <limits>
#include <unordered_map>
#include <unordered_set>

namespace zb::virtual_node {
namespace {

constexpr char kIdentityKey[] = "M:identity";
constexpr char kObjectPrefix[] = "O:";
constexpr char kFilePrefix[] = "F:";

bool ParseUint64(const std::string& text, uint64_t* result) {
    if (text.empty() || !result) return false;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), *result);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

void AppendUint64(std::string* output, uint64_t value) {
    for (unsigned int i = 0; i < 8; ++i) {
        output->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
    }
}

bool TakeUint64(const std::string& input, size_t* offset, uint64_t* value) {
    if (!offset || !value || *offset > input.size() || input.size() - *offset < 8) return false;
    *value = 0;
    for (unsigned int i = 0; i < 8; ++i) {
        *value |= static_cast<uint64_t>(
            static_cast<unsigned char>(input[*offset + i])) << (8 * i);
    }
    *offset += 8;
    return true;
}

std::string Identity(const std::string& node_id,
                     std::vector<std::string> disk_ids, uint64_t capacity) {
    std::sort(disk_ids.begin(), disk_ids.end());
    std::string value = "ZB_MODEL_STATE_V1\n" + node_id + "\n" +
                        std::to_string(capacity) + "\n";
    for (const auto& id : disk_ids) value += id + "\n";
    return value;
}

bool HasPrefix(const std::string& key, const char* prefix) {
    return key.compare(0, 2, prefix) == 0;
}

} // namespace

ModelStateStore::ModelStateStore() = default;
ModelStateStore::~ModelStateStore() = default;

std::string ModelStateStore::ObjectKey(const std::string& disk_id,
                                       const std::string& object_id) {
    std::string key(kObjectPrefix);
    key += disk_id;
    key.push_back('\0');
    key += object_id;
    return key;
}

std::string ModelStateStore::FileKey(uint64_t inode_id) {
    return std::string(kFilePrefix) + std::to_string(inode_id);
}

std::string ModelStateStore::EncodeFile(const ModelFileRecord& file) {
    std::string result = "FM2";
    AppendUint64(&result, file.meta.inode_id);
    AppendUint64(&result, file.meta.file_size);
    AppendUint64(&result, file.meta.object_unit_size);
    AppendUint64(&result, file.meta.version);
    AppendUint64(&result, file.meta.mtime_sec);
    AppendUint64(&result, file.meta.update_ts_ms);
    AppendUint64(&result, file.txid.size());
    result += file.txid;
    AppendUint64(&result, file.commit_signature.size());
    result += file.commit_signature;
    return result;
}

bool ModelStateStore::DecodeFile(const std::string& data, ModelFileRecord* file) {
    if (!file || (data.compare(0, 3, "FM1") != 0 &&
                  data.compare(0, 3, "FM2") != 0)) return false;
    const bool has_signature = data.compare(0, 3, "FM2") == 0;
    size_t offset = 3;
    uint64_t txid_size = 0;
    if (!TakeUint64(data, &offset, &file->meta.inode_id) ||
        !TakeUint64(data, &offset, &file->meta.file_size) ||
        !TakeUint64(data, &offset, &file->meta.object_unit_size) ||
        !TakeUint64(data, &offset, &file->meta.version) ||
        !TakeUint64(data, &offset, &file->meta.mtime_sec) ||
        !TakeUint64(data, &offset, &file->meta.update_ts_ms) ||
        !TakeUint64(data, &offset, &txid_size) ||
        txid_size > data.size() - offset ||
        file->meta.inode_id == 0 || file->meta.object_unit_size == 0) return false;
    file->txid.assign(data.data() + offset, static_cast<size_t>(txid_size));
    offset += static_cast<size_t>(txid_size);
    file->commit_signature.clear();
    if (has_signature) {
        uint64_t signature_size = 0;
        if (!TakeUint64(data, &offset, &signature_size) ||
            signature_size != data.size() - offset) return false;
        file->commit_signature.assign(data.data() + offset,
                                      static_cast<size_t>(signature_size));
    } else if (offset != data.size()) {
        return false;
    }
    return true;
}

bool ModelStateStore::Open(const std::string& path, const std::string& node_id,
                           const std::vector<std::string>& disk_ids,
                           uint64_t disk_capacity,
                           std::vector<ModelObjectRecord>* objects,
                           std::vector<ModelFileRecord>* files,
                           std::string* error) {
    if (!objects || !files || db_) {
        if (error) *error = "invalid or repeated model state open";
        return false;
    }
    std::error_code path_error;
    const bool existed = std::filesystem::exists(path, path_error);
    if (path_error) {
        if (error) *error = "model state path: " + path_error.message();
        return false;
    }
    rocksdb::Options options;
    options.create_if_missing = true;
    options.paranoid_checks = true;
    rocksdb::DB* raw = nullptr;
    auto status = rocksdb::DB::Open(options, path, &raw);
    if (!status.ok()) {
        if (error) *error = "model state open: " + status.ToString();
        return false;
    }
    db_.reset(raw);
    const std::string expected = Identity(node_id, disk_ids, disk_capacity);
    std::string identity;
    status = db_->Get(rocksdb::ReadOptions(), kIdentityKey, &identity);
    if (status.IsNotFound() && !existed) {
        rocksdb::WriteOptions write_options;
        write_options.sync = true;
        status = db_->Put(write_options, kIdentityKey, expected);
    } else if (status.ok() && identity != expected) {
        if (error) *error = "model state identity, disks or capacity changed";
        return false;
    } else if (status.IsNotFound()) {
        if (error) *error = "existing model state has no identity marker";
        return false;
    }
    if (!status.ok()) {
        if (error) *error = "model state identity: " + status.ToString();
        return false;
    }

    std::unordered_set<std::string> known_disks(disk_ids.begin(), disk_ids.end());
    std::unordered_map<std::string, uint64_t> used;
    std::unordered_set<std::string> object_ids;
    std::vector<ModelObjectRecord> loaded_objects;
    std::vector<ModelFileRecord> loaded_files;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key == kIdentityKey) continue;
        if (HasPrefix(key, kObjectPrefix)) {
            const size_t split = key.find('\0', 2);
            if (split == std::string::npos || split == 2 || split + 1 == key.size()) {
                if (error) *error = "malformed model object key";
                return false;
            }
            ModelObjectRecord record;
            record.disk_id = key.substr(2, split - 2);
            record.object_id = key.substr(split + 1);
            if (!ParseUint64(it->value().ToString(), &record.size) ||
                known_disks.count(record.disk_id) == 0 ||
                !object_ids.insert(record.object_id).second ||
                used[record.disk_id] > disk_capacity ||
                record.size > disk_capacity - used[record.disk_id]) {
                if (error) *error = "invalid model object state or capacity";
                return false;
            }
            used[record.disk_id] += record.size;
            loaded_objects.push_back(std::move(record));
        } else if (HasPrefix(key, kFilePrefix)) {
            uint64_t inode_id = 0;
            ModelFileRecord record;
            if (!ParseUint64(key.substr(2), &inode_id) ||
                !DecodeFile(it->value().ToString(), &record) ||
                record.meta.inode_id != inode_id || FileKey(inode_id) != key) {
                if (error) *error = "invalid model file metadata";
                return false;
            }
            loaded_files.push_back(std::move(record));
        } else {
            if (error) *error = "unknown model state key";
            return false;
        }
    }
    if (!it->status().ok()) {
        if (error) *error = "model state scan: " + it->status().ToString();
        return false;
    }
    *objects = std::move(loaded_objects);
    *files = std::move(loaded_files);
    return true;
}

bool ModelStateStore::SyncWrite(const std::string& key,
                                const std::string* value, std::string* error) {
    if (!db_) {
        if (error) *error = "model state is not open";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = value ? db_->Put(options, key, *value)
                              : db_->Delete(options, key);
    if (!status.ok()) {
        if (error) *error = "model state write: " + status.ToString();
        return false;
    }
    return true;
}

bool ModelStateStore::PutObject(const std::string& disk_id,
                                const std::string& object_id,
                                uint64_t size, std::string* error) {
    const std::string value = std::to_string(size);
    return SyncWrite(ObjectKey(disk_id, object_id), &value, error);
}

bool ModelStateStore::DeleteObject(const std::string& disk_id,
                                   const std::string& object_id,
                                   std::string* error) {
    return SyncWrite(ObjectKey(disk_id, object_id), nullptr, error);
}

bool ModelStateStore::PutFile(const ModelFileRecord& file, std::string* error) {
    const std::string value = EncodeFile(file);
    return SyncWrite(FileKey(file.meta.inode_id), &value, error);
}

bool ModelStateStore::DeleteFileAndObjects(
    uint64_t inode_id,
    const std::vector<std::pair<std::string, std::string>>& objects,
    std::string* error) {
    if (!db_) {
        if (error) *error = "model state is not open";
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Delete(FileKey(inode_id));
    for (const auto& [disk_id, object_id] : objects)
        batch.Delete(ObjectKey(disk_id, object_id));
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Write(options, &batch);
    if (!status.ok()) {
        if (error) *error = "model state delete: " + status.ToString();
        return false;
    }
    return true;
}

bool ModelStateStore::Clear(bool objects, bool files, std::string* error) {
    if (!db_) {
        if (error) *error = "model state is not open";
        return false;
    }
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if ((objects && HasPrefix(key, kObjectPrefix)) ||
            (files && HasPrefix(key, kFilePrefix))) batch.Delete(key);
    }
    if (!it->status().ok()) {
        if (error) *error = "model state clear scan: " + it->status().ToString();
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Write(options, &batch);
    if (!status.ok()) {
        if (error) *error = "model state clear: " + status.ToString();
        return false;
    }
    return true;
}

} // namespace zb::virtual_node
