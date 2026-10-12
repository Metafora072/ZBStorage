#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace rocksdb { class DB; }

namespace zb::scheduler {

struct OpticalFileRecord {
    uint64_t inode_id{0};
    uint64_t version{0};
    uint64_t size{0};
    std::string disc_id;
    std::string image_id;
    // Physical modeled allocation. OF1 records recover with media_bytes=size.
    uint64_t media_bytes{0};
};

struct OpticalTaskRecord {
    uint64_t task_id{0};
    uint64_t inode_id{0};
    uint64_t version{0};
    uint64_t size{0};
    uint64_t remaining_us{0};
    uint64_t release_epoch_ms{0};
    bool released{false};
    bool cache_hit{false};
};

enum class OpticalArchivePhase : uint8_t {
    kTransfer = 1,
    kPack = 2,
    kBurn = 3,
    kAwaitingMds = 4,
    kCommitted = 5,
    kFailed = 6,
};

// This is a locally verified manifest, not a replacement for an MDS archive
// lease or a published file location. The caller must verify source version
// and lease before submitting it to the model.
struct OpticalArchiveRecord {
    std::string operation_id;
    std::string source_lease_id;
    uint64_t inode_id{0};
    uint64_t version{0};
    uint64_t size{0};
    std::string source_node_id;
    std::string source_disk_id;
    std::string disc_id;
    std::string image_id;
    OpticalArchivePhase phase{OpticalArchivePhase::kTransfer};
    uint64_t remaining_us{0};
    uint64_t media_bytes{0};
};

// Durable control inventory only: no payload, image, or disc bytes are stored.
class OpticalStateStore {
public:
    OpticalStateStore();
    ~OpticalStateStore();
    OpticalStateStore(const OpticalStateStore&) = delete;
    OpticalStateStore& operator=(const OpticalStateStore&) = delete;

    bool Open(const std::string& path, const std::string& node_id,
              uint32_t disc_count, uint64_t disc_capacity,
              std::vector<OpticalFileRecord>* files,
              std::vector<OpticalTaskRecord>* tasks,
              std::vector<OpticalArchiveRecord>* archives,
              uint64_t* next_task_id, std::string* error);
    bool PutFile(const OpticalFileRecord& file, std::string* error);
    bool PutTask(const OpticalTaskRecord& task, uint64_t next_task_id,
                 std::string* error);
    bool UpdateTask(const OpticalTaskRecord& task, std::string* error);
    bool DeleteTask(uint64_t task_id, std::string* error);
    bool DeleteFileAndTasks(uint64_t inode_id,
                            const std::vector<uint64_t>& task_ids,
                            std::string* error);
    bool PutArchive(const OpticalArchiveRecord& archive, std::string* error);
    bool CompleteArchive(const OpticalArchiveRecord& archive,
                         const OpticalFileRecord& file, std::string* error);

private:
    std::unique_ptr<rocksdb::DB> db_;
};

} // namespace zb::scheduler
