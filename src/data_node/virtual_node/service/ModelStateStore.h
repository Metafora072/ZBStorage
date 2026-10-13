#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "msg/storage_node_messages.h"

namespace rocksdb { class DB; }

namespace zb::virtual_node {

struct ModelObjectRecord {
    std::string disk_id;
    std::string object_id;
    uint64_t size{0};
};

struct ModelFileRecord {
    zb::msg::FileMeta meta;
    std::string txid;
    // Exact logical CommitFileWrite parameters for the last acknowledged txid.
    // Empty for records written by the older FM1 format.
    std::string commit_signature;
};

// One RocksDB WAL per logical node. Only control state is stored here; user
// payload bytes are never written. All mutations acknowledged by this store
// use sync=true, and object+file deletes use one atomic WriteBatch.
class ModelStateStore {
public:
    ModelStateStore();
    ~ModelStateStore();
    ModelStateStore(const ModelStateStore&) = delete;
    ModelStateStore& operator=(const ModelStateStore&) = delete;

    bool Open(const std::string& path, const std::string& node_id,
              const std::vector<std::string>& disk_ids, uint64_t disk_capacity,
              std::vector<ModelObjectRecord>* objects,
              std::vector<ModelFileRecord>* files, std::string* error);
    bool PutObject(const std::string& disk_id, const std::string& object_id,
                   uint64_t size, std::string* error);
    bool DeleteObject(const std::string& disk_id, const std::string& object_id,
                      std::string* error);
    bool PutFile(const ModelFileRecord& file, std::string* error);
    bool DeleteFileAndObjects(
        uint64_t inode_id,
        const std::vector<std::pair<std::string, std::string>>& objects,
        std::string* error);
    bool Clear(bool objects, bool files, std::string* error);

private:
    static std::string ObjectKey(const std::string& disk_id,
                                 const std::string& object_id);
    static std::string FileKey(uint64_t inode_id);
    static std::string EncodeFile(const ModelFileRecord& file);
    static bool DecodeFile(const std::string& data, ModelFileRecord* file);
    bool SyncWrite(const std::string& key, const std::string* value,
                   std::string* error);

    std::unique_ptr<rocksdb::DB> db_;
};

} // namespace zb::virtual_node
