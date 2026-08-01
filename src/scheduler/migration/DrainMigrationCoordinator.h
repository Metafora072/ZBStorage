#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../lifecycle/LifecycleManager.h"
#include "../model/ClusterState.h"
#include "../power/NodePowerManager.h"

namespace zb::scheduler {

struct MigrationObject {
    std::string disk_id;
    std::string object_id;
    uint64_t inode_id{0};
    uint32_t object_index{0};
    uint64_t size_bytes{0};
};

struct MigrationTarget {
    std::string node_id;
    std::string address;
    std::string disk_id;
};

enum class DrainMigrationState {
    kPending = 0,
    kRunning = 1,
    kRetryWait = 2,
    kCompleted = 3,
    kBlocked = 4,
};

struct DrainMigrationTask {
    std::string migration_id;
    std::string source_node_id;
    std::string source_address;
    DrainMigrationState state{DrainMigrationState::kPending};
    uint64_t created_at_ms{0};
    uint64_t updated_at_ms{0};
    uint64_t total_objects{0};
    uint64_t total_bytes{0};
    uint64_t remaining_objects{0};
    uint64_t remaining_bytes{0};
    uint64_t migrated_objects{0};
    uint64_t migrated_bytes{0};
    uint32_t retry_count{0};
    uint64_t next_retry_ms{0};
    std::string last_error;
};

class ObjectMigrationBackend {
public:
    virtual ~ObjectMigrationBackend() = default;
    virtual bool ListObjects(const std::string& source_address,
                             std::vector<MigrationObject>* objects,
                             std::string* error) = 0;
    virtual bool CopyAndVerify(const std::string& source_address,
                               const MigrationObject& object,
                               const MigrationTarget& target,
                               std::string* error) = 0;
    virtual bool CommitFileLocation(const std::string& migration_id,
                                    uint64_t inode_id,
                                    const std::string& source_node_id,
                                    const std::string& source_address,
                                    const std::string& source_disk_id,
                                    const MigrationTarget& target,
                                    std::string* error) = 0;
    virtual bool DeleteSourceObject(const std::string& source_address,
                                    const MigrationObject& object,
                                    std::string* error) = 0;
    // Final source re-enumeration closes the race between the last delete and
    // retirement. Backends may override this with an atomic CMS check.
    virtual bool VerifySourceEmpty(const std::string& source_address,
                                   std::string* error);
    virtual bool VerifyCmsNodeReferencesZero(const std::string& node_id,
                                             uint64_t* disk_references,
                                             uint64_t* optical_references,
                                             std::string* error) = 0;
};

class DrainMigrationCoordinator {
public:
    DrainMigrationCoordinator(ClusterState* cluster,
                              NodePowerManager* nodes,
                              LifecycleManager* lifecycle,
                              ObjectMigrationBackend* backend,
                              uint64_t retry_base_ms = 1000,
                              uint32_t max_retries = 20);

    bool Start(const std::string& node_id, uint64_t now_ms, std::string* error);
    void Tick(uint64_t now_ms);
    bool GetTask(const std::string& node_id, DrainMigrationTask* out) const;
    std::vector<DrainMigrationTask> ListTasks() const;

private:
    struct WorkItem {
        uint64_t inode_id{0};
        std::string source_disk_id;
        MigrationTarget target;
        std::vector<MigrationObject> objects;
        bool target_selected{false};
        bool metadata_committed{false};
        size_t cleanup_index{0};
    };
    struct InternalTask {
        DrainMigrationTask view;
        std::vector<WorkItem> work;
        size_t next_work{0};
        bool enumerated{false};
    };

    static bool ParseStableObjectId(const std::string& object_id,
                                    uint64_t* inode_id,
                                    uint32_t* object_index);
    bool EnumerateLocked(InternalTask* task, uint64_t now_ms);
    bool SelectTargetLocked(const InternalTask& task,
                            const WorkItem& work,
                            uint64_t now_ms,
                            MigrationTarget* target,
                            std::string* error) const;
    bool ProcessWorkLocked(InternalTask* task, uint64_t now_ms);
    void RetryLocked(InternalTask* task, uint64_t now_ms, const std::string& error);
    void CompleteLocked(InternalTask* task, uint64_t now_ms);

    ClusterState* cluster_{};
    NodePowerManager* nodes_{};
    LifecycleManager* lifecycle_{};
    ObjectMigrationBackend* backend_{};
    uint64_t retry_base_ms_{1000};
    uint32_t max_retries_{20};
    uint64_t next_id_{1};
    mutable std::mutex mu_;
    std::unordered_map<std::string, InternalTask> tasks_;
};

} // namespace zb::scheduler
