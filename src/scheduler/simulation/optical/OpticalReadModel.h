#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "OpticalStateStore.h"
#include "common/rpc/ModelIoFaults.h"

namespace zb::scheduler {

// 无载荷光盘模型：归档/迁移接收、共卷刻录与读任务；MDS 仍是位置权威。
struct OpticalReadConfig {
    std::string node_id;
    uint64_t instance_generation{1};
    std::string state_dir;
    uint32_t drive_count{10};
    uint32_t disc_count{10000};
    uint64_t disc_capacity_bytes{10000000000000ULL};
    uint64_t read_bytes_per_sec{100000000};
    uint64_t cache_capacity_bytes{280000000000000ULL};
    uint64_t cache_read_bytes_per_sec{1000000000};
    uint32_t cache_base_latency_us{500};
    uint32_t load_latency_ms{60000};
    uint32_t seek_latency_ms{500};
    uint32_t wake_latency_ms{0};
    uint32_t reboot_latency_ms{1000};
    uint32_t max_read_tasks{128};
    uint32_t retry_retention_ms{30000};
    uint32_t max_archive_tasks{128};
    uint64_t archive_transfer_bytes_per_sec{1000000000};
    uint64_t archive_pack_bytes_per_sec{1000000000};
    uint64_t archive_burn_bytes_per_sec{10000000};
    uint32_t archive_burn_fixed_ms{500};
    uint32_t archive_tail_wait_ms{1000};
    // 0 使用盘容量；一张模型盘封入一个卷，可容纳多个文件。
    uint64_t image_capacity_bytes{0};
    // Encoded media size = ceil(logical bytes * ratio / 1000), then rounded up
    // to ARCHIVE_ALIGNMENT_BYTES. This is a model parameter, not compression.
    uint32_t archive_compression_permille{1000};
    uint32_t archive_alignment_bytes{1};
    uint32_t io_failure_probability_ppm{0};
    uint32_t io_timeout_probability_ppm{0};
    uint32_t io_timeout_ms{1000};
    uint32_t io_fault_seed{0};
    uint32_t archive_source_lease_ms{300000};
    static OpticalReadConfig LoadFromFile(const std::string& path, std::string* error);
};

enum class OpticalModelCode { kOk, kInvalid, kNotFound, kNotReady, kBusy, kNoSpace, kIoError };

struct OpticalModelResult {
    OpticalModelCode code{OpticalModelCode::kOk};
    std::string message;
    uint64_t task_id{0};
    std::string data;
    bool cache_hit{false};
    uint64_t modeled_wait_us{0};
};

struct OpticalModelStats {
    uint32_t tasks{0};
    uint32_t pending_tasks{0};
    uint32_t occupied_discs{0};
    uint32_t recording_discs{0};
    uint32_t recorded_discs{0};
    uint32_t busy_drives{0};
    uint32_t pending_write_tasks{0};
    uint32_t cached_files{0};
    uint64_t cached_bytes{0};
    uint64_t staging_used_bytes{0};
    uint64_t reserved_disc_bytes{0};
    // Encoded modeled media allocation, not source logical file bytes.
    uint64_t stored_bytes{0};
};

// 准备任务只提供状态与身份；查询不生成业务字节，也不重复计算设备耗时。
struct OpticalPreparedInfo {
    uint64_t inode_id{0}, version{0}, size{0};
    bool ready{false};
};

class OpticalReadModel {
public:
    explicit OpticalReadModel(OpticalReadConfig config);
    const OpticalReadConfig& config() const { return config_; }
    bool InitStateStore(std::string* error);

    // Internal test/verified-commit hook. This does not claim an MDS location
    // update, payload verification, or durable archive completion.
    OpticalModelResult InstallFile(uint64_t inode_id, uint64_t version,
                                   uint64_t size, const std::string& disc_id,
                                   const std::string& image_id);
    // Internal source cleanup hook: caller must first verify authoritative
    // references and pass the exact version/location being removed.
    OpticalModelResult RemoveFile(uint64_t inode_id, uint64_t version,
                                  const std::string& disc_id,
                                  const std::string& image_id);
    OpticalModelResult RequestRead(uint64_t inode_id, const std::string& disc_id,
                                   const std::string& image_id, bool retained = false,
                                   const OpticalPreparedInfo* expected = nullptr);
    OpticalModelResult InspectPrepared(uint64_t task_id, OpticalPreparedInfo* output);
    OpticalModelResult ReleasePrepared(uint64_t task_id);
    OpticalModelResult ReadTask(uint64_t task_id, uint64_t offset, uint64_t size);
    OpticalModelResult ReadLatest(uint64_t inode_id, uint64_t offset, uint64_t size);
    OpticalModelResult SubmitVerifiedArchive(const OpticalArchiveRecord& request);
    OpticalModelResult AdvanceArchives();
    // 停机/节点故障冻结任务时间线；原子检查点保存剩余工作，不推进停机时间。
    bool SetPaused(bool paused, std::string* error);
    OpticalModelResult AcknowledgeArchive(const std::string& operation_id,
                                          const std::string& source_lease_id);
    OpticalModelResult FailArchive(const std::string& operation_id,
                                   const std::string& source_lease_id);
    bool GetArchive(const std::string& operation_id, OpticalArchiveRecord* output) const;
    OpticalModelResult ReplaceArchiveLease(const std::string& operation_id,
                                            const std::string& expected_lease,
                                            const std::string& next_lease);
    std::vector<OpticalArchiveRecord> PendingArchives() const;
    std::vector<OpticalArchiveRecord> ListArchives() const;
    // 只读导出逻辑文件清单，供排空迁移核对权威位置，不生成业务数据。
    std::vector<OpticalFileRecord> ListFiles() const;
    OpticalModelStats Stats() const;

private:
    using Clock = std::chrono::steady_clock;
    struct File {
        uint64_t version{0};
        uint64_t size{0};
        std::string disc_id;
        std::string image_id;
        uint64_t media_bytes{0};
    };
    struct Task {
        uint64_t inode_id{0};
        uint64_t version{0};
        uint64_t size{0};
        uint64_t remaining_us{0};
        uint64_t release_epoch_ms{0};
        Clock::time_point ready_at;
        Clock::time_point released_at;
        bool released{false};
        bool cache_hit{false};
        bool retained{false}; // 进程内准备/读取引用；重启后失效，不持久化会话。
    };
    struct Cache {
        uint64_t version{0};
        uint64_t size{0};
        uint64_t lru{0};
    };
    void PruneLocked(Clock::time_point now);
    void CacheReadyLocked(uint64_t inode_id, const Task& task);
    OpticalModelResult ReadTaskLocked(uint64_t task_id, uint64_t offset,
                                      uint64_t size, Clock::time_point now);

    OpticalReadConfig config_;
    zb::model::IoFaultPolicy io_faults_;
    OpticalModelResult InjectIoFault();
    mutable std::mutex mu_;
    std::unordered_map<uint64_t, File> files_;
    std::unordered_map<std::string, uint64_t> disc_used_;
    std::unordered_map<uint64_t, Task> tasks_;
    std::unordered_map<uint64_t, uint64_t> latest_task_;
    std::unordered_map<uint64_t, Cache> cache_;
    struct ArchiveJob {
        OpticalArchiveRecord record;
        Clock::time_point ready_at;
        Clock::time_point seal_deadline;
    };
    std::unordered_map<std::string, ArchiveJob> archives_;
    std::unordered_map<std::string, uint64_t> disc_reserved_;
    std::vector<Clock::time_point> drive_available_;
    Clock::time_point cache_available_{};
    Clock::time_point archive_transfer_available_{};
    Clock::time_point archive_pack_available_{};
    uint64_t stored_bytes_{0};
    uint64_t cached_bytes_{0};
    uint64_t staging_used_bytes_{0};
    uint64_t next_task_id_{1};
    uint64_t lru_clock_{0};
    std::unique_ptr<OpticalStateStore> state_store_;
    bool paused_{false};
    Clock::time_point paused_at_{};
};

} // namespace zb::scheduler
