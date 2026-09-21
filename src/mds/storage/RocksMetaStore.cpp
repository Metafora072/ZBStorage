#include "RocksMetaStore.h"

#include <algorithm>
#include <vector>

namespace zb::mds {

namespace {

constexpr const char* kLegacyDiskFileLocationColumnFamily = "disk_file_locations";
constexpr const char* kLegacyOpticalFileLocationColumnFamily = "optical_file_locations";

bool IsLegacyLocationColumnFamily(const std::string& name) {
    return name == kLegacyDiskFileLocationColumnFamily || name == kLegacyOpticalFileLocationColumnFamily;
}

} // namespace

RocksMetaStore::~RocksMetaStore() {
    for (auto& item : column_families_) {
        delete item.second;
    }
    column_families_.clear();
    delete db_;
    db_ = nullptr;
}

bool RocksMetaStore::Open(const std::string& path, std::string* error) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    std::vector<std::string> cf_names;
    rocksdb::DBOptions db_options(options);
    rocksdb::Status list_status = rocksdb::DB::ListColumnFamilies(db_options, path, &cf_names);
    if (!list_status.ok()) {
        cf_names.clear();
        cf_names.push_back(rocksdb::kDefaultColumnFamilyName);
    }
    if (std::find(cf_names.begin(), cf_names.end(), rocksdb::kDefaultColumnFamilyName) == cf_names.end()) {
        cf_names.push_back(rocksdb::kDefaultColumnFamilyName);
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size());
    for (const auto& name : cf_names) {
        descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    }

    rocksdb::DB* db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::Status status = rocksdb::DB::Open(db_options, path, descriptors, &handles, &db);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }

    for (size_t i = 0; i < descriptors.size() && i < handles.size(); ++i) {
        if (IsLegacyLocationColumnFamily(descriptors[i].name)) {
            rocksdb::Status drop_status = db->DropColumnFamily(handles[i]);
            if (!drop_status.ok()) {
                if (error) {
                    *error = drop_status.ToString();
                }
                for (auto* handle : handles) {
                    delete handle;
                }
                delete db;
                return false;
            }
            delete handles[i];
            handles[i] = nullptr;
            continue;
        }
        column_families_[descriptors[i].name] = handles[i];
    }
    db_ = db;
    return true;
}

bool RocksMetaStore::Put(const std::string& key, const std::string& value, std::string* error) {
    std::lock_guard<std::mutex> lock(write_mu_);
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::Get(const std::string& key, std::string* value, std::string* error) const {
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, value);
    if (!status.ok()) {
        if (status.IsNotFound()) {
            return false;
        }
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::Exists(const std::string& key, std::string* error) const {
    std::string value;
    return Get(key, &value, error);
}

bool RocksMetaStore::WriteBatch(rocksdb::WriteBatch* batch, std::string* error) {
    std::lock_guard<std::mutex> lock(write_mu_);
    if (!db_) {
        if (error) {
            *error = "DB not opened";
        }
        return false;
    }
    if (!batch) {
        if (error) {
            *error = "WriteBatch is null";
        }
        return false;
    }
    rocksdb::Status status = db_->Write(rocksdb::WriteOptions(), batch);
    if (!status.ok()) {
        if (error) {
            *error = status.ToString();
        }
        return false;
    }
    return true;
}

bool RocksMetaStore::WriteBatchIfValueEquals(const std::string& key,
                                             const std::string& expected_value,
                                             rocksdb::WriteBatch* batch,
                                             bool* matched,
                                             std::string* error) {
    if (matched) *matched = false;
    std::lock_guard<std::mutex> lock(write_mu_);
    if (!db_ || !batch) {
        if (error) *error = !db_ ? "DB not opened" : "WriteBatch is null";
        return false;
    }
    std::string current;
    const rocksdb::Status read_status = db_->Get(rocksdb::ReadOptions(), key, &current);
    if (!read_status.ok()) {
        if (error) *error = read_status.ToString();
        return false;
    }
    if (current != expected_value) return true;
    const rocksdb::Status write_status = db_->Write(rocksdb::WriteOptions(), batch);
    if (!write_status.ok()) {
        if (error) *error = write_status.ToString();
        return false;
    }
    if (matched) *matched = true;
    return true;
}

} // namespace zb::mds
