#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include <string>
#include <mutex>
#include <unordered_map>

namespace zb::mds {

class RocksMetaStore {
public:
    RocksMetaStore() = default;
    ~RocksMetaStore();

    bool Open(const std::string& path, std::string* error);
    bool Put(const std::string& key, const std::string& value, std::string* error);
    bool Get(const std::string& key, std::string* value, std::string* error) const;
    bool Exists(const std::string& key, std::string* error) const;
    bool WriteBatch(rocksdb::WriteBatch* batch, std::string* error);
    bool WriteBatchIfValueEquals(const std::string& key,
                                 const std::string& expected_value,
                                 rocksdb::WriteBatch* batch,
                                 bool* matched,
                                 std::string* error);

    rocksdb::DB* db() { return db_; }
    rocksdb::DB* db() const { return db_; }

private:
    rocksdb::DB* db_{nullptr};
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle*> column_families_;
    mutable std::mutex write_mu_;
};

} // namespace zb::mds
