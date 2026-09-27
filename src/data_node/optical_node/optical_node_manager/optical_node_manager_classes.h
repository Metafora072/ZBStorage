#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "volume_manager/error_codes.h"

// ============================================================================
// 以下内容来自 optical_node_manager_status.h
// ============================================================================

// 集中声明 OpticalNodeManager 用到的状态 / 阶段字符串常量。
// 字符串值需保持稳定，上层可直接做 == 比较或拼入日志 / RPC 返回。
namespace optical_node_manager {

namespace manager_status {

// Run() 尚未调用。
inline constexpr const char* kUninitialized = "UNINITIALIZED";

// Run() 已进入，正在初始化目录与组件。
inline constexpr const char* kInitializing  = "INITIALIZING";

// 启动失败；对外接口应拒绝服务，等待上层销毁或修复后重建。
inline constexpr const char* kStartFailed   = "START_FAILED";

// 所有后台线程与 cd_manager 已就绪，可对外提供服务。
inline constexpr const char* kRunning      = "RUNNING";

// 已请求停止，等待后台线程退出。
inline constexpr const char* kStopping     = "STOPPING";

// 停止流程已结束；可再次 Run()（任务队列不会重开）。
inline constexpr const char* kStopped      = "STOPPED";

}  // namespace manager_status

namespace start_failure_phase {

// 启动失败 / 运行期失败对应的 phase 字符串，仅用于 GetStatusDetail / 日志分流。
inline constexpr const char* kRunEntry      = "run_entry";
inline constexpr const char* kInitializeDir = "InitializeDir";
inline constexpr const char* kStartWorkers  = "StartBackgroundWorkers";
// 从 node_discs_meta 重建镜像定位索引（Run 内紧接 InitializeDir）。
inline constexpr const char* kRebuildDiscImageIndex = "RebuildDiscImageIndex";

// 后台线程创建失败的细分 phase。
inline constexpr const char* kThreadRead    = "StartBackgroundWorkers.thread=read";
inline constexpr const char* kThreadZip     = "StartBackgroundWorkers.thread=zip";
inline constexpr const char* kThreadBurn    = "StartBackgroundWorkers.thread=burn";
inline constexpr const char* kThreadArchive = "StartBackgroundWorkers.thread=archive";

// 运行期链路 phase：上层按字段做 == 比较时使用这些常量。
inline constexpr const char* kRunGuard          = "RunGuard";
inline constexpr const char* kReadFile          = "ReadFile";
inline constexpr const char* kReadObjectByTaskId = "ReadObjectByTaskId";
inline constexpr const char* kReadObjectByInodeId = "ReadObjectByInodeId";
inline constexpr const char* kWriteObject       = "WriteObject";
inline constexpr const char* kSendArchiveMetadata = "SendArchiveMetadata";
inline constexpr const char* kArchiveTaskProcessor = "ArchiveTaskProcessor";
inline constexpr const char* kOnCDReadComplete  = "OnCDReadComplete";
inline constexpr const char* kOnCDBurnComplete  = "OnCDBurnComplete";
inline constexpr const char* kZipTaskProcessor  = "ZipTaskProcessor";
inline constexpr const char* kCDReadTaskProcessor = "CDReadTaskProcessor";
inline constexpr const char* kCDBurnTaskProcessor = "CDBurnTaskProcessor";
inline constexpr const char* kCleanupTaskProcessor = "CleanupTaskProcessor";
// 封印待打包光盘（分配 disk_id、定稿盘元数据、提交 DISC_BURN）。
inline constexpr const char* kSealPendingDiscAndSubmitBurn = "SealPendingDiscAndSubmitBurn";

// 运行期状态快照：关机落盘 / 重启恢复 / 恢复后幂等补齐。
inline constexpr const char* kPersistRuntimeState  = "PersistRuntimeState";
inline constexpr const char* kLoadRuntimeState     = "LoadRuntimeState";
inline constexpr const char* kLoadPendingDiscMeta  = "LoadPendingDiscMeta";
inline constexpr const char* kReconcileAndReplay   = "ReconcileAndReplay";

}  // namespace start_failure_phase

}  // namespace optical_node_manager

// ============================================================================
// 以下内容来自 WR_task.h
// ============================================================================

namespace WR_task {

// 任务生命周期状态：START 等待 zip；WAITING 等底层调度；LOADING 已下发 cd_manager（仅 CD_READ）；
// READY 读产物已解压可读（仅 READ）；ZIPPED 压缩完成；CD_BURNING 刻录中（仅 CD_BURN / DISC_BURN）；
// FINISH 已消费完毕（READ 读完 / WRITE 压缩完 / CD_BURN 烧完 / DISC_BURN 刻录完 / batch 处理完 / CD_READ 加载完）；FAILED 终态。
// FINISH / FAILED 为终态，由清理线程周期性从 task_map_ 移除 FINISH 任务。
enum class WRTaskState { START, WAITING, LOADING, READY, ZIPPED, CD_BURNING, FINISH, FAILED };

inline const char* WRTaskStateToString(WRTaskState state) {
    switch (state) {
        case WRTaskState::START:   return "START";
        case WRTaskState::WAITING: return "WAITING";
        case WRTaskState::LOADING: return "LOADING";
        case WRTaskState::READY:   return "READY";
        case WRTaskState::ZIPPED:  return "ZIPPED";
        case WRTaskState::CD_BURNING: return "CD_BURNING";
        case WRTaskState::FINISH:  return "FINISH";
        case WRTaskState::FAILED:  return "FAILED";
    }
    return "UNKNOWN";
}

// 任务类型：
//   READ                 数据面读任务（客户端 RequestAsyncReadFile 发起，产物为解压后的读文件）
//   WRITE                写任务（压缩并封装进卷镜像）
//   CD_BURN              单卷刻录任务（旧链路保留）
//   DISC_BURN            光盘级刻录任务：一张光盘包含多个卷镜像，volume_ids 有效
//   READ_BATCH_BY_VOLUME 仅 volume_id 有效，镜像就绪后批量推进该卷下挂起的 READ 任务
//   CD_READ              光盘库读请求：让光盘库把某卷镜像加载回 image_dir_（不产出用户数据）
enum class WRTaskType { READ, WRITE, CD_BURN, DISC_BURN, READ_BATCH_BY_VOLUME, CD_READ };

inline const char* WRTaskTypeToString(WRTaskType type) {
    switch (type) {
        case WRTaskType::READ:                 return "READ";
        case WRTaskType::WRITE:                return "WRITE";
        case WRTaskType::CD_BURN:              return "CD_BURN";
        case WRTaskType::DISC_BURN:            return "DISC_BURN";
        case WRTaskType::READ_BATCH_BY_VOLUME: return "READ_BATCH_BY_VOLUME";
        case WRTaskType::CD_READ:              return "CD_READ";
    }
    return "UNKNOWN";
}

// 当前毫秒级时间戳（自 epoch 起的毫秒数）；只存 uint64_t，落日志时再转可读串。
inline uint64_t NowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

// 任务元数据：含 task_id、类型、disk/volume/inode 定位信息、状态与失败上下文。
class WRTask {
public:
    uint64_t task_id;
    WRTaskType type;

    std::string disk_id;
    std::string volume_id;
    std::string inode_id;
    uint64_t inode_id_num;
    WRTaskState state;
    // 最近一次状态变更时刻（epoch 毫秒；构造 / MarkTaskState / MarkTaskFailed 时写入）；
    // 与 state 同批赋值，落审计日志时再转成可读时间串。
    uint64_t state_changed_at_ms;
    std::string file_path;

    // 仅 DISC_BURN 有效：本张光盘包含的全部卷镜像 ID（顺序与光盘数据区顺序一致）。
    std::vector<std::string> volume_ids;

    // 仅 DISC_BURN 有效：待刻录光盘文件的预期字节数（封印时按布局算出）。
    // 提交刻录任务时 vdisc 尚未生成，供 cd_manager 估算刻录时长，避免回退到默认值。
    uint64_t expected_image_size_bytes{0};

    // 仅当 state==FAILED 时有意义；task_map_->Get(id) 可读到具体失败码与上下文。
    volumemanager::ErrorCode last_error_code;
    std::string last_error_detail;

    // 任务被推回 WAITING 重新入队的次数；超上限后由调用方强制置 FAILED。
    uint32_t attempt_count;

    // 与 state==FAILED 同步的冗余标志，便于快照直接读出终态。
    bool is_failed_terminal;

    // 默认构造（随后由调用方 SetXxxTask 填充载荷）：新建任务即记录状态时间戳，
    // 其余字段保持既有语义（不在此初始化）。
    WRTask() : state_changed_at_ms(NowMs()) {}

    explicit WRTask(uint64_t task_id, WRTaskType type)
        : task_id(task_id),
          type(type),
          inode_id_num(INVALID_INODE_ID),
          state(WRTaskState::START),
          state_changed_at_ms(NowMs()),
          file_path("none"),
          last_error_code(volumemanager::ErrorCode::SUCCESS),
          last_error_detail(),
          attempt_count(0),
          is_failed_terminal(false) {
    }

    // 置为失败终态并记录错误码 / 上下文；调用方负责同步写回 task_map_。
    void SetFailed(volumemanager::ErrorCode code, const std::string& detail = std::string()) {
        state = WRTaskState::FAILED;
        state_changed_at_ms = NowMs();
        last_error_code = code;
        last_error_detail = detail;
        is_failed_terminal = true;
    }

    // 自增一次重试计数；终态任务会被静默忽略。
    // 只改 attempt_count，不改 state——任务状态一律由 OpticalNodeManager::MarkTaskState /
    // MarkTaskFailed 统一写入，避免绕过终态保护。
    void IncrementAttemptCount() {
        if (is_failed_terminal) {
            return;
        }
        attempt_count++;
    }

    // 设置读任务载荷并重置运行期状态；inode_id 会同步解析为 uint64_t。
    void SetReadTask(const std::string& disk_id,
                     const std::string& volume_id,
                     const std::string& inode_id) {
        this->disk_id = disk_id;
        this->volume_id = volume_id;
        this->inode_id = inode_id;
        this->inode_id_num = ParseInodeId(inode_id);
        this->type = WRTaskType::READ;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 设置写任务载荷；disk_id/volume_id 留空，由 ZipTaskProcessor 分配。
    void SetWriteTask(const std::string& inode_id) {
        this->disk_id.clear();
        this->volume_id.clear();
        this->inode_id = inode_id;
        this->inode_id_num = ParseInodeId(inode_id);
        this->type = WRTaskType::WRITE;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 设置刻录任务载荷；不含 inode_id。
    void SetCDBurnTask(const std::string& disk_id,
                     const std::string& volume_id,
                     const std::string& file_path) {
        this->disk_id = disk_id;
        this->volume_id = volume_id;
        this->file_path = file_path;
        this->inode_id.clear();
        this->inode_id_num = INVALID_INODE_ID;
        this->type = WRTaskType::CD_BURN;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 设置光盘级刻录任务载荷：disk_id 为光盘 ID，file_path 为待刻录的光盘文件
    // （vdisc）路径，volume_ids 为本张光盘包含的全部卷镜像 ID，
    // expected_image_size_bytes 为该盘封盘后应占的字节数；不含 inode_id。
    void SetDiscBurnTask(const std::string& disk_id,
                         const std::string& file_path,
                         const std::vector<std::string>& volume_ids,
                         uint64_t expected_image_size_bytes) {
        this->disk_id = disk_id;
        this->volume_id.clear();
        this->inode_id.clear();
        this->inode_id_num = INVALID_INODE_ID;
        this->file_path = file_path;
        this->volume_ids = volume_ids;
        this->expected_image_size_bytes = expected_image_size_bytes;
        this->type = WRTaskType::DISC_BURN;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 设置按镜像批量读请求载荷；仅 volume_id 有效。
    void SetReadBatchByVolumeTask(const std::string& volume_id) {
        this->disk_id.clear();
        this->volume_id = volume_id;
        this->inode_id.clear();
        this->inode_id_num = INVALID_INODE_ID;
        this->file_path.clear();
        this->type = WRTaskType::READ_BATCH_BY_VOLUME;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 设置光盘库读请求载荷：仅 disk_id / volume_id 有效，不携带 inode。
    void SetCDReadTask(const std::string& disk_id,
                       const std::string& volume_id) {
        this->disk_id = disk_id;
        this->volume_id = volume_id;
        this->inode_id.clear();
        this->inode_id_num = INVALID_INODE_ID;
        this->file_path.clear();
        this->type = WRTaskType::CD_READ;
        state = WRTaskState::START;
        state_changed_at_ms = NowMs();
    }

    // 任务是否已到达 FINISH 终态（通用语义：READ 读完 / WRITE 压缩完 / CD_BURN / DISC_BURN 刻录完 / batch 处理完）。
    bool isTaskFinish() const {
        return (this->state == WRTaskState::FINISH);
    }

    // READ 任务读产物是否已解压就绪可读（READY）；仅对 READ 任务调用有意义。
    bool isReadTaskReady() const {
        return (this->state == WRTaskState::READY);
    }

    // 任务是否处于不可推进的终态（FINISH / FAILED）。
    bool isTaskTerminal() const {
        return state == WRTaskState::FINISH || state == WRTaskState::FAILED;
    }

    // inode_id_num 无效时的哨兵值；用 UINT64_MAX 避免与合法 0 冲突。
    static constexpr uint64_t INVALID_INODE_ID = UINT64_MAX;

    // 解析字符串 inode_id 为 uint64_t；解析失败返回 INVALID_INODE_ID。
    static uint64_t ParseInodeId(const std::string& inode_id) {
        if (inode_id.empty()) {
            return INVALID_INODE_ID;
        }
        try {
            size_t consumed = 0;
            const unsigned long long value = std::stoull(inode_id, &consumed);
            if (consumed != inode_id.size()) {
                return INVALID_INODE_ID;
            }
            return static_cast<uint64_t>(value);
        } catch (const std::exception&) {
            return INVALID_INODE_ID;
        }
    }
};

// 任务的轻量句柄：仅携带 task_id 与 type，适合在队列中传递。
class WRTaskShort {
public:
    uint64_t task_id;
    WRTaskType type;

    explicit WRTaskShort(uint64_t task_id, WRTaskType type)
        : task_id(task_id),
          type(type) {
    }

    explicit WRTaskShort(const WRTask& task)
        : task_id(task.task_id),
          type(task.type) {
    }
};

// 多生产者/多消费者安全的阻塞任务队列；Close 后停止接收并唤醒所有消费者。
class WRTaskQueue {
public:
    WRTaskQueue() = default;

    WRTaskQueue(const WRTaskQueue&) = delete;
    WRTaskQueue& operator=(const WRTaskQueue&) = delete;

    // 队尾入队；关闭后静默丢弃。
    void Push(const WRTaskShort& task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(task);
        }
        condition_variable_.notify_one();
    }

    void Push(WRTaskShort&& task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(std::move(task));
        }
        condition_variable_.notify_one();
    }

    void Push(const WRTask& task) {
        Push(WRTaskShort(task));
    }

    void Push(WRTask&& task) {
        Push(WRTaskShort(task));
    }

    // 队首入队，适合失败重试时优先处理。
    void PushFront(const WRTaskShort& task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            std::queue<WRTaskShort> new_queue;
            new_queue.push(task);
            while (!queue_.empty()) {
                new_queue.push(std::move(queue_.front()));
                queue_.pop();
            }
            queue_ = std::move(new_queue);
        }
        condition_variable_.notify_one();
    }

    void PushFront(WRTaskShort&& task) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            std::queue<WRTaskShort> new_queue;
            new_queue.push(task);
            while (!queue_.empty()) {
                new_queue.push(std::move(queue_.front()));
                queue_.pop();
            }
            queue_ = std::move(new_queue);
        }
        condition_variable_.notify_one();
    }

    void PushFront(const WRTask& task) {
        PushFront(WRTaskShort(task));
    }

    void PushFront(WRTask&& task) {
        PushFront(WRTaskShort(task));
    }

    // 阻塞出队；关闭后返回空值作为退出信号。
    std::optional<WRTaskShort> Pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_variable_.wait(lock, [this] {
            return closed_ || !queue_.empty();
        });

        if (queue_.empty()) {
            return std::nullopt;
        }

        WRTaskShort task = std::move(queue_.front());
        queue_.pop();
        return task;
    }

    // 非阻塞查看队首；空队列返回空值。
    std::optional<WRTaskShort> Peek() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return std::nullopt;
        }

        return queue_.front();
    }

    // 把当前队首移到队尾；空队列或单元素时不变，返回 false。
    bool RequeueFront() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() <= 1) {
            return false;
        }

        queue_.push(std::move(queue_.front()));
        queue_.pop();
        return true;
    }

    // 非阻塞出队，成功写入 *task 返回 true。
    bool TryPop(WRTaskShort* task) {
        if (task == nullptr) {
            return false;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }

        *task = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void Close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        condition_variable_.notify_all();
    }

    bool IsClosed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    bool Empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    std::size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    // 按 FIFO 顺序导出当前队列内容的副本，供关机快照落盘。
    // 只读语义：不移动队首、不修改队列状态；调用方负责先把队列静止（Shutdown 在
    // 全部后台线程 join 之后调用）。
    std::vector<WRTaskShort> SnapshotAll() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<WRTaskShort> out;
        out.reserve(queue_.size());
        std::queue<WRTaskShort> copy = queue_;
        while (!copy.empty()) {
            out.push_back(copy.front());
            copy.pop();
        }
        return out;
    }

    // 批量按序入队（重启恢复用）：单次加锁，保持 tasks 的给定顺序。
    // 队列已 Close 时静默丢弃，语义与 Push 一致。
    void PushAll(const std::vector<WRTaskShort>& tasks) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            for (const WRTaskShort& task : tasks) {
                queue_.push(task);
            }
        }
        condition_variable_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_variable_;
    std::queue<WRTaskShort> queue_;
    bool closed_ = false;
};

// 以 task_id 为键的线程安全任务索引表；返回副本避免外部持悬空引用。
class WRTaskMap {
public:
    WRTaskMap() = default;

    WRTaskMap(const WRTaskMap&) = delete;
    WRTaskMap& operator=(const WRTaskMap&) = delete;

    void Insert(const WRTask& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        task_map_[task.task_id] = task;
    }

    void Insert(WRTask&& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        const uint64_t task_id = task.task_id;
        task_map_[task_id] = std::move(task);
    }

    // 不存在时返回空值。
    std::optional<WRTask> Get(uint64_t task_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = task_map_.find(task_id);
        if (it == task_map_.end()) {
            return std::nullopt;
        }

        return it->second;
    }

    bool Update(const WRTask& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = task_map_.find(task.task_id);
        if (it == task_map_.end()) {
            return false;
        }

        it->second = task;
        return true;
    }

    bool Update(WRTask&& task) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = task_map_.find(task.task_id);
        if (it == task_map_.end()) {
            return false;
        }

        it->second = std::move(task);
        return true;
    }

    // 成功删除返回 true。
    bool Erase(uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return task_map_.erase(task_id) > 0;
    }

    // 收集所有 FINISH 终态任务的 task_id（供清理线程周期性移除）。
    // 返回副本，调用方随后逐个 Erase；FINISH 为终态，期间不会被其它路径改写。
    std::vector<uint64_t> CollectFinishTaskIds() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<uint64_t> ids;
        ids.reserve(task_map_.size());
        for (const auto& kv : task_map_) {
            if (kv.second.state == WRTaskState::FINISH) {
                ids.push_back(kv.first);
            }
        }
        return ids;
    }

    bool Contains(uint64_t task_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return task_map_.find(task_id) != task_map_.end();
    }

    // 导出全部任务的副本，按 task_id 升序（保证同一份输入的结果可复现），
    // 供关机快照落盘。返回副本，调用方自行筛掉终态任务。
    std::vector<WRTask> SnapshotAll() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<WRTask> out;
        out.reserve(task_map_.size());
        for (const auto& kv : task_map_) {
            out.push_back(kv.second);
        }
        std::sort(out.begin(), out.end(), [](const WRTask& lhs, const WRTask& rhs) {
            return lhs.task_id < rhs.task_id;
        });
        return out;
    }

    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        task_map_.clear();
    }

    bool Empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return task_map_.empty();
    }

    std::size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return task_map_.size();
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, WRTask> task_map_;
};

}  // namespace WR_task

// ============================================================================
// 以下内容来自 volume_task_map.h，以及归档接口的领域结构
// ============================================================================

namespace optical_node_manager {

/**
 * @brief 按 volume_id 索引 task_id 集合的线程安全映射。
 *
 * 一个 volume_id 可同时被多个 task 引用（读 / 写 / 刻录），
 * 用于"按卷批量推进"和"新读请求搭便车"判定。
 */
class VolumeTaskMap {
public:
    VolumeTaskMap() = default;
    ~VolumeTaskMap() = default;

    VolumeTaskMap(const VolumeTaskMap&) = delete;
    VolumeTaskMap& operator=(const VolumeTaskMap&) = delete;
    VolumeTaskMap(VolumeTaskMap&&) = delete;
    VolumeTaskMap& operator=(VolumeTaskMap&&) = delete;

    /**
     * @brief 将 task_id 登记到指定 volume_id 下；task_id 已存在则返回 false。
     */
    bool AddTask(const std::string& volume_id, uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto& vec = map_[volume_id];
        if (std::find(vec.begin(), vec.end(), task_id) != vec.end()) {
            return false;
        }
        vec.push_back(task_id);
        return true;
    }

    /**
     * @brief 从指定 volume_id 下移除 task_id；移除后该 volume 项自动回收。
     * @return true 表示成功移除；false 表示未找到。
     */
    bool RemoveTask(const std::string& volume_id, uint64_t task_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return false;
        }
        auto& vec = it->second;
        auto vit = std::find(vec.begin(), vec.end(), task_id);
        if (vit == vec.end()) {
            return false;
        }
        vec.erase(vit);
        if (vec.empty()) {
            map_.erase(it);
        }
        return true;
    }

    /**
     * @brief 返回指定 volume_id 下所有 task_id（拷贝）；volume 不存在返回空。
     */
    std::vector<uint64_t> GetTasks(const std::string& volume_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return {};
        }
        return it->second;
    }

    /**
     * @brief 判断指定 volume_id 下是否登记了 task_id。
     */
    bool Contains(const std::string& volume_id, uint64_t task_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        if (it == map_.end()) {
            return false;
        }
        const auto& vec = it->second;
        return std::find(vec.begin(), vec.end(), task_id) != vec.end();
    }

    /**
     * @brief 判断指定 volume_id 下是否登记了任意 task。
     *
     * 用于"搭便车"预查询：若为 true，新任务直接登记即可，
     * 由 cd_manager / 镜像回灌链路负责后续唤醒。
     */
    bool ContainsVolume(const std::string& volume_id) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = map_.find(volume_id);
        return it != map_.end() && !it->second.empty();
    }

    /**
     * @brief 返回当前管理的 volume_id 数量。
     */
    size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return map_.size();
    }

    /**
     * @brief 清空整张表（用于 Stop / Reset 流程）。
     */
    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        map_.clear();
    }

    /**
     * @brief 导出整张表的副本（volume_id → task_id 集合），供关机快照落盘。
     *
     * 返回的 vector 按 map_ 的遍历顺序排列；恢复时不依赖该顺序（每个 volume_id
     * 内部的 task_id 顺序被保留，决定批量推进的先后）。
     */
    std::vector<std::pair<std::string, std::vector<uint64_t>>> SnapshotAll() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::pair<std::string, std::vector<uint64_t>>> out;
        out.reserve(map_.size());
        for (const auto& kv : map_) {
            out.emplace_back(kv.first, kv.second);
        }
        return out;
    }

    /**
     * @brief 恢复整张表（重启恢复用）：直接覆盖当前内容。
     *
     * 与逐条 AddTask 的区别：AddTask 对已存在的 task_id 返回 false，而恢复场景下
     * 表应被快照内容整体替换，因此这里先 Clear 再批量写入。
     * 重复的 task_id（同一 volume_id 下）会被去重，空 task_id 列表的 volume 不登记。
     */
    void RestoreAll(const std::vector<std::pair<std::string, std::vector<uint64_t>>>& entries) {
        std::lock_guard<std::mutex> lock(mutex_);
        map_.clear();
        for (const auto& kv : entries) {
            if (kv.first.empty() || kv.second.empty()) {
                continue;
            }
            auto& vec = map_[kv.first];
            for (uint64_t task_id : kv.second) {
                if (std::find(vec.begin(), vec.end(), task_id) == vec.end()) {
                    vec.push_back(task_id);
                }
            }
        }
    }

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint64_t>> map_;
};

// 归档下载背压判定的一次快照：记录「为什么暂停/放行」，便于日志与排查。
// 判定为「需暂停下一批下载」当且仅当：写镜像占用达上限，或原始文件积压超阈值。
struct ArchiveBackpressureState {
    uint64_t write_images{0};         // image_dir 中当前写镜像数（含已封印、等刻录释放的）
    uint64_t write_image_limit{0};    // 写镜像数上限（MAX_WRITE_IMAGES）
    uint64_t input_pending_bytes{0};  // input/ 未压缩原始文件总量（粗略估算）
    uint64_t input_pending_limit{0};  // 原始文件积压阈值（硬编码 = 卷镜像容量 10%）
    bool write_overloaded{false};     // 写镜像数达上限
    bool input_overloaded{false};     // 原始文件积压超阈值
    bool throttled{false};            // 是否应暂停下一批下载（= 上面两者之一）
};

// MDS 下发的单个归档文件信息（对应设计文档中的 ArchiveFile）。
// 只包含从 real_node 下载文件数据所需的最小字段集；
// 目录名称、父子关系、权限和时间等权威元数据仍保存在 MDS，不在此结构中。
struct ArchiveFileInfo {
    uint64_t inode_id{0};              // 文件 inode；同时作为文件在镜像中的内部标识
    uint64_t size{0};                  // 文件总大小，用于读取和安排镜像空间
    uint64_t object_unit_size{0};      // 文件分片大小
    std::string target_node_id;        // 热数据节点 id，由光盘节点向 scheduler 查询 node_address
    std::string target_disk_id;        // 文件所在磁盘 id，用于定位文件
};

// MDS 下发的一批归档文件元数据（对应 SendArchiveMetadataRequest）。
// batch_id 仅用于后续可能的任务查询接口；光盘节点不关心批次，
// 封装打包以文件为单位，逐个从热数据节点下载文件并封装。
struct SendArchiveMetadataRequest {
    uint64_t batch_id{0};
    std::vector<ArchiveFileInfo> files;
};

// 归档请求队列：多生产者（MDS RPC 线程）/ 单消费者（archive_task_thread_）。
// 生命周期语义与 WRTaskQueue 一致：Close 后 Push 静默丢弃、Pop 返回空值作为退出信号。
class ArchiveRequestQueue {
public:
    ArchiveRequestQueue() = default;

    ArchiveRequestQueue(const ArchiveRequestQueue&) = delete;
    ArchiveRequestQueue& operator=(const ArchiveRequestQueue&) = delete;

    // 队尾入队；关闭后静默丢弃。
    void Push(const SendArchiveMetadataRequest& request) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(request);
        }
        condition_variable_.notify_one();
    }

    void Push(SendArchiveMetadataRequest&& request) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            queue_.push(std::move(request));
        }
        condition_variable_.notify_one();
    }

    // 阻塞出队；关闭后返回空值作为退出信号。
    std::optional<SendArchiveMetadataRequest> Pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_variable_.wait(lock, [this] {
            return closed_ || !queue_.empty();
        });

        if (queue_.empty()) {
            return std::nullopt;
        }

        SendArchiveMetadataRequest request = std::move(queue_.front());
        queue_.pop();
        return request;
    }

    void Close() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        condition_variable_.notify_all();
    }

    bool IsClosed() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

    std::size_t Size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    // 按 FIFO 顺序导出当前归档批次的副本，供关机快照落盘（只读语义，同 WRTaskQueue）。
    std::vector<SendArchiveMetadataRequest> SnapshotAll() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<SendArchiveMetadataRequest> out;
        out.reserve(queue_.size());
        std::queue<SendArchiveMetadataRequest> copy = queue_;
        while (!copy.empty()) {
            out.push_back(copy.front());
            copy.pop();
        }
        return out;
    }

    // 批量按序入队（重启恢复用）：单次加锁，保持 requests 的给定顺序。
    // 队列已 Close 时静默丢弃，语义与 Push 一致。
    void PushAll(const std::vector<SendArchiveMetadataRequest>& requests) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (closed_) {
                return;
            }
            for (const SendArchiveMetadataRequest& request : requests) {
                queue_.push(request);
            }
        }
        condition_variable_.notify_all();
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_variable_;
    std::queue<SendArchiveMetadataRequest> queue_;
    bool closed_ = false;
};

// ============================================================================
// 光盘打包领域结构：超级块 / 元数据区 / 镜像记录
//
// 光盘文件（vdisc）布局，三个区域均按 block_size_bytes 对齐：
//   [超级块 DISC_SUPERBLOCK_SIZE]
//   [元数据区 DiscMetaHeader + N × DiscImageEntry]
//   [数据区 卷镜像 0][卷镜像 1]...
//
// 编码约定与 volume_manager/Serializer 一致：定长字段按本机字节序 memcpy，
// 尾部保留区补零；字符串写入定长区，不足处补零（天然以 '\0' 结尾），超长截断。
// ============================================================================

// 光盘格式版本；字段布局或编码方式变化时必须递增。
constexpr uint32_t DISC_FORMAT_VERSION = 1;

// 超级块魔数标识 "ZBDC"。
constexpr uint32_t DISC_SUPERBLOCK_MAGIC = 0x5A424443u;
// 超级块固定大小（字节）。
constexpr uint32_t DISC_SUPERBLOCK_SIZE = 4096;
// 超级块中公共路径前缀字段的容量（含结尾 '\0'）。
constexpr uint32_t DISC_PATH_PREFIX_CAPACITY = 256;
// 超级块中 path_prefix 之前的定长字段总大小（字节），须与 Serialize 的写入顺序一致。
constexpr uint32_t DISC_SUPERBLOCK_FIXED_SIZE = 4 + 4 + 8 + 8 + 8 + 4 + 4 + 8 + 8 + 8 + 8;

// 元数据区魔数标识 "ZBDM"。
constexpr uint32_t DISC_META_MAGIC = 0x5A42444Du;
// 元数据区头部固定大小（字节）。
constexpr uint32_t DISC_META_HEADER_SIZE = 32;
// 元数据区中单条镜像记录的固定大小（字节）。
constexpr uint32_t DISC_IMAGE_RECORD_SIZE = 128;

// 校验算法标识：SHA-256。
constexpr uint32_t DISC_CHECKSUM_SHA256 = 1;
// SHA-256 十六进制串长度（不含结尾 '\0'）。
constexpr uint32_t DISC_SHA256_HEX_LENGTH = 64;

// node_discs_meta 中每张光盘追加块的魔数标识 "ZBDI"。
constexpr uint32_t DISC_INDEX_MAGIC = 0x5A424449u;
// node_discs_meta 追加块头部固定大小（字节）。
constexpr uint32_t DISC_INDEX_HEADER_SIZE = 32;
// 追加块头部中 disc_id 之前的定长字段总大小（字节），须与 Serialize 顺序一致。
constexpr uint32_t DISC_INDEX_HEADER_FIXED_SIZE = 4 + 4 + 4 + 4;

// 元数据区总大小（字节）：头部 + entry_count 条定长记录。
inline uint64_t DiscMetaAreaSize(uint64_t entry_count) {
    return static_cast<uint64_t>(DISC_META_HEADER_SIZE) +
           entry_count * static_cast<uint64_t>(DISC_IMAGE_RECORD_SIZE);
}

// 从定长区读取字符串：读到 '\0' 或读满 capacity 为止（对应写入时的补零语义）。
inline std::string ReadFixedString(const uint8_t* field, size_t capacity) {
    size_t len = 0;
    while (len < capacity && field[len] != '\0') {
        ++len;
    }
    return std::string(reinterpret_cast<const char*>(field), len);
}

/**
 * @brief 元数据区头部：固定 DISC_META_HEADER_SIZE 字节，后跟 entry_count 条镜像记录
 *
 * meta/pending_disc_meta 文件与光盘内的元数据区使用同一编码，
 * 因此待打包文件的内容可直接作为光盘元数据区写入。
 */
struct DiscMetaHeader {
    uint32_t magic_number{DISC_META_MAGIC};        // 魔数 "ZBDM"
    uint32_t format_version{DISC_FORMAT_VERSION};  // 格式版本
    uint32_t record_size{DISC_IMAGE_RECORD_SIZE};  // 单条记录大小（字节）
    uint32_t entry_count{0};                       // 记录条数

    // 序列化为固定 DISC_META_HEADER_SIZE 字节（尾部保留区补零）。
    void Serialize(std::vector<uint8_t>& out) const {
        out.assign(DISC_META_HEADER_SIZE, 0);
        uint8_t* p = out.data();
        std::memcpy(p, &magic_number, sizeof(magic_number));
        p += sizeof(magic_number);
        std::memcpy(p, &format_version, sizeof(format_version));
        p += sizeof(format_version);
        std::memcpy(p, &record_size, sizeof(record_size));
        p += sizeof(record_size);
        std::memcpy(p, &entry_count, sizeof(entry_count));
    }

    // 反序列化；缓冲区过短或魔数 / 格式版本 / 记录大小不符时返回 false。
    static bool Parse(const uint8_t* header, size_t header_size, DiscMetaHeader& out) {
        if (header == nullptr || header_size < DISC_META_HEADER_SIZE) {
            return false;
        }
        const uint8_t* p = header;
        std::memcpy(&out.magic_number, p, sizeof(out.magic_number));
        p += sizeof(out.magic_number);
        std::memcpy(&out.format_version, p, sizeof(out.format_version));
        p += sizeof(out.format_version);
        std::memcpy(&out.record_size, p, sizeof(out.record_size));
        p += sizeof(out.record_size);
        std::memcpy(&out.entry_count, p, sizeof(out.entry_count));

        return out.magic_number == DISC_META_MAGIC &&
               out.format_version == DISC_FORMAT_VERSION &&
               out.record_size == DISC_IMAGE_RECORD_SIZE;
    }
};

/**
 * @brief 光盘数据区中的单条卷镜像记录
 *
 * offset_in_disc 为该镜像在光盘文件中的起始偏移，读回时据此从 vdisc 中
 * 复制出所需镜像；sha256_hex 用于校验复制出的内容。
 */
struct DiscImageEntry {
    uint64_t volume_id{0};           // 卷镜像 ID（与 volume_<id>.vimg 一致）
    uint64_t image_size_bytes{0};    // vimg 实际字节数
    uint64_t aligned_size_bytes{0};  // 按光盘数据块对齐后占用的字节数
    uint64_t offset_in_disc{0};      // vimg 在光盘文件中的起始偏移
    std::string sha256_hex;          // vimg 的 SHA-256（64 字符小写十六进制）

    // 序列化为固定 DISC_IMAGE_RECORD_SIZE 字节（尾部保留区补零）。
    void Serialize(std::vector<uint8_t>& out) const {
        out.assign(DISC_IMAGE_RECORD_SIZE, 0);
        uint8_t* p = out.data();
        std::memcpy(p, &volume_id, sizeof(volume_id));
        p += sizeof(volume_id);
        std::memcpy(p, &image_size_bytes, sizeof(image_size_bytes));
        p += sizeof(image_size_bytes);
        std::memcpy(p, &aligned_size_bytes, sizeof(aligned_size_bytes));
        p += sizeof(aligned_size_bytes);
        std::memcpy(p, &offset_in_disc, sizeof(offset_in_disc));
        p += sizeof(offset_in_disc);

        const size_t copy_len = std::min<size_t>(sha256_hex.size(), DISC_SHA256_HEX_LENGTH);
        if (copy_len > 0) {
            std::memcpy(p, sha256_hex.data(), copy_len);
        }
    }

    // 反序列化；缓冲区长度不符时返回 false。
    static bool Parse(const uint8_t* record, size_t record_size, DiscImageEntry& out) {
        if (record == nullptr || record_size != DISC_IMAGE_RECORD_SIZE) {
            return false;
        }
        const uint8_t* p = record;
        std::memcpy(&out.volume_id, p, sizeof(out.volume_id));
        p += sizeof(out.volume_id);
        std::memcpy(&out.image_size_bytes, p, sizeof(out.image_size_bytes));
        p += sizeof(out.image_size_bytes);
        std::memcpy(&out.aligned_size_bytes, p, sizeof(out.aligned_size_bytes));
        p += sizeof(out.aligned_size_bytes);
        std::memcpy(&out.offset_in_disc, p, sizeof(out.offset_in_disc));
        p += sizeof(out.offset_in_disc);
        out.sha256_hex = ReadFixedString(p, DISC_SHA256_HEX_LENGTH);
        return true;
    }
};

/**
 * @brief node_discs_meta 中一张光盘的追加块头部：固定 DISC_INDEX_HEADER_SIZE 字节，
 *        后跟 entry_count 条 DiscImageEntry 记录
 *
 * node_discs_meta 为纯追加文件：每张光盘刻录完成后追加一个「头部 + 镜像记录」块，
 * 记录该盘全部镜像（volume_id → offset_in_disc / image_size_bytes / sha256），
 * 供读回时按 volume_id 定位镜像在 vdisc 中的偏移与大小。
 */
struct DiscIndexHeader {
    uint32_t magic_number{DISC_INDEX_MAGIC};        // 魔数 "ZBDI"
    uint32_t format_version{DISC_FORMAT_VERSION};   // 格式版本
    uint32_t record_size{DISC_IMAGE_RECORD_SIZE};   // 单条记录大小（字节）
    uint32_t entry_count{0};                        // 记录条数
    uint64_t disc_id{0};                            // 光盘全局 ID

    // 序列化为固定 DISC_INDEX_HEADER_SIZE 字节（尾部保留区补零）。
    void Serialize(std::vector<uint8_t>& out) const {
        out.assign(DISC_INDEX_HEADER_SIZE, 0);
        uint8_t* p = out.data();
        std::memcpy(p, &magic_number, sizeof(magic_number));
        p += sizeof(magic_number);
        std::memcpy(p, &format_version, sizeof(format_version));
        p += sizeof(format_version);
        std::memcpy(p, &record_size, sizeof(record_size));
        p += sizeof(record_size);
        std::memcpy(p, &entry_count, sizeof(entry_count));
        p += sizeof(entry_count);
        std::memcpy(p, &disc_id, sizeof(disc_id));
    }

    // 反序列化；缓冲区过短或魔数 / 格式版本 / 记录大小不符时返回 false。
    static bool Parse(const uint8_t* header, size_t header_size, DiscIndexHeader& out) {
        if (header == nullptr || header_size < DISC_INDEX_HEADER_SIZE) {
            return false;
        }
        const uint8_t* p = header;
        std::memcpy(&out.magic_number, p, sizeof(out.magic_number));
        p += sizeof(out.magic_number);
        std::memcpy(&out.format_version, p, sizeof(out.format_version));
        p += sizeof(out.format_version);
        std::memcpy(&out.record_size, p, sizeof(out.record_size));
        p += sizeof(out.record_size);
        std::memcpy(&out.entry_count, p, sizeof(out.entry_count));
        p += sizeof(out.entry_count);
        std::memcpy(&out.disc_id, p, sizeof(out.disc_id));

        return out.magic_number == DISC_INDEX_MAGIC &&
               out.format_version == DISC_FORMAT_VERSION &&
               out.record_size == DISC_IMAGE_RECORD_SIZE;
    }
};

/**
 * @brief 内存索引条目：某卷镜像在已刻录光盘中的位置
 *
 * 是 node_discs_meta 在内存中的等价视图，供读回时定位 vdisc 文件与复制区间。
 */
struct DiscImageLocation {
    std::string disk_id;             // 所属光盘 ID（光盘文件名为 disc_<disk_id>.vdisc）
    uint64_t offset_in_disc{0};      // 镜像在光盘文件中的起始偏移
    uint64_t image_size_bytes{0};    // 镜像实际字节数
    std::string sha256_hex;          // 镜像 SHA-256，供复制回 image_dir_ 后校验
};

/**
 * @brief 光盘超级块：固定 4 KB，位于光盘文件起始处
 *
 * 记录整张光盘的全局信息与三个区域的布局。path_prefix 为镜像在节点内的
 * 公共路径前缀（由上层给出），供后续校验镜像来源。
 */
struct DiscSuperblock {
    uint32_t magic_number{DISC_SUPERBLOCK_MAGIC};   // 魔数 "ZBDC"
    uint32_t format_version{DISC_FORMAT_VERSION};   // 光盘格式版本
    uint64_t disc_id{0};                            // 光盘全局 ID
    uint64_t capacity_bytes{0};                     // 光盘容量（字节）
    uint64_t created_at_ms{0};                      // 创建时间（epoch 毫秒）
    uint32_t block_size_bytes{0};                   // 数据块对齐大小（字节）
    uint32_t checksum_algo{DISC_CHECKSUM_SHA256};   // 校验算法标识
    uint64_t image_count{0};                        // 数据区镜像条数
    uint64_t metadata_offset{0};                    // 元数据区起始偏移
    uint64_t metadata_size{0};                      // 元数据区大小（字节）
    uint64_t data_offset{0};                        // 数据区起始偏移
    std::string path_prefix;                        // 镜像公共路径前缀

    // 序列化为固定 DISC_SUPERBLOCK_SIZE 字节（含 path_prefix 定长区，其余补零）。
    void Serialize(std::vector<uint8_t>& out) const {
        out.assign(DISC_SUPERBLOCK_SIZE, 0);
        uint8_t* p = out.data();
        std::memcpy(p, &magic_number, sizeof(magic_number));
        p += sizeof(magic_number);
        std::memcpy(p, &format_version, sizeof(format_version));
        p += sizeof(format_version);
        std::memcpy(p, &disc_id, sizeof(disc_id));
        p += sizeof(disc_id);
        std::memcpy(p, &capacity_bytes, sizeof(capacity_bytes));
        p += sizeof(capacity_bytes);
        std::memcpy(p, &created_at_ms, sizeof(created_at_ms));
        p += sizeof(created_at_ms);
        std::memcpy(p, &block_size_bytes, sizeof(block_size_bytes));
        p += sizeof(block_size_bytes);
        std::memcpy(p, &checksum_algo, sizeof(checksum_algo));
        p += sizeof(checksum_algo);
        std::memcpy(p, &image_count, sizeof(image_count));
        p += sizeof(image_count);
        std::memcpy(p, &metadata_offset, sizeof(metadata_offset));
        p += sizeof(metadata_offset);
        std::memcpy(p, &metadata_size, sizeof(metadata_size));
        p += sizeof(metadata_size);
        std::memcpy(p, &data_offset, sizeof(data_offset));
        p += sizeof(data_offset);

        // 公共路径前缀：定长区末尾留 '\0'，超长截断。
        const size_t copy_len =
            std::min<size_t>(path_prefix.size(), DISC_PATH_PREFIX_CAPACITY - 1);
        if (copy_len > 0) {
            std::memcpy(p, path_prefix.data(), copy_len);
        }
    }

    // 反序列化；缓冲区过短或魔数 / 格式版本不符时返回 false。
    static bool Parse(const std::vector<uint8_t>& in, DiscSuperblock& out) {
        if (in.size() < DISC_SUPERBLOCK_SIZE) {
            return false;
        }
        const uint8_t* p = in.data();
        std::memcpy(&out.magic_number, p, sizeof(out.magic_number));
        p += sizeof(out.magic_number);
        std::memcpy(&out.format_version, p, sizeof(out.format_version));
        p += sizeof(out.format_version);
        std::memcpy(&out.disc_id, p, sizeof(out.disc_id));
        p += sizeof(out.disc_id);
        std::memcpy(&out.capacity_bytes, p, sizeof(out.capacity_bytes));
        p += sizeof(out.capacity_bytes);
        std::memcpy(&out.created_at_ms, p, sizeof(out.created_at_ms));
        p += sizeof(out.created_at_ms);
        std::memcpy(&out.block_size_bytes, p, sizeof(out.block_size_bytes));
        p += sizeof(out.block_size_bytes);
        std::memcpy(&out.checksum_algo, p, sizeof(out.checksum_algo));
        p += sizeof(out.checksum_algo);
        std::memcpy(&out.image_count, p, sizeof(out.image_count));
        p += sizeof(out.image_count);
        std::memcpy(&out.metadata_offset, p, sizeof(out.metadata_offset));
        p += sizeof(out.metadata_offset);
        std::memcpy(&out.metadata_size, p, sizeof(out.metadata_size));
        p += sizeof(out.metadata_size);
        std::memcpy(&out.data_offset, p, sizeof(out.data_offset));
        p += sizeof(out.data_offset);
        out.path_prefix = ReadFixedString(p, DISC_PATH_PREFIX_CAPACITY);

        return out.magic_number == DISC_SUPERBLOCK_MAGIC &&
               out.format_version == DISC_FORMAT_VERSION;
    }
};

// ============================================================================
// 运行期状态快照：log/runtime_state_snapshot
//
// 正常关机（Shutdown）时把 task_map_、各任务队列、任务计数、可用 ID 池与打包中间态
// 写入 log/runtime_state_snapshot；进程重启（Run）时读回，继续关机前未完成的任务。
//
// 文件布局：
//   [RuntimeStateMetaHeader]                       // 定长 24B
//   [section 0][section 1]...                      // 每段 [tag u32][payload_len u64][payload]
//
// 编码约定与 volume_manager/Serializer、DiscMetaHeader 一致：定长字段按本机字节序
// memcpy，变长字符串用「u32 长度 + 字节」；解析侧全部带边界检查，截断 / 越界一律
// 返回 false（不抛异常、不读越界），由调用方按「放弃该段」处理，不阻塞启动。
//
// 段缺失即表示对应状态为空，因此新增段天然向后兼容；字段布局变化时递增
// RUNTIME_STATE_FORMAT_VERSION。
// ============================================================================

// 快照文件魔数标识 "ZBRS"。
constexpr uint32_t RUNTIME_STATE_MAGIC = 0x5A425253u;
// 快照格式版本；字段布局或编码方式变化时必须递增。
constexpr uint32_t RUNTIME_STATE_FORMAT_VERSION = 1;
// 快照文件头固定大小（字节），须与 RuntimeStateMetaHeader::Serialize 的写入顺序一致。
constexpr uint32_t RUNTIME_STATE_HEADER_SIZE = 4 + 4 + 4 + 4 + 8;
// 单个 section 头固定大小（字节）：[tag u32][payload_len u64]。
constexpr uint32_t RUNTIME_STATE_SECTION_HEADER_SIZE = 4 + 8;

// section 标识：每段承载一类相互独立的状态，便于按需扩展与容错降级。
enum class RuntimeStateSectionTag : uint32_t {
    kCounters = 1,         // next_task_id_ / next_volume_id_ / next_disk_id_
    kAvailableIds = 2,     // available_volume_ids + available_disk_ids
    kTasks = 3,            // task_map_ 中的非终态任务（WRTask blob 序列）
    kQueues = 4,           // zip / cd_read / cd_burn 三个任务队列的 FIFO 内容
    kArchiveQueue = 5,     // archive_task_queue_ + 归档下载断点
    kVolumeReadIndex = 6,  // volume_read_index_（volume_id → task_id 集合）
    kPendingFiles = 7,     // volume_manager_ 的 pending_files_ / pending_size_
    kImages = 8,           // space_manager_ 管理表全量快照（volume_id + category，按 LRU 由旧到新）
};

/**
 * @brief 快照文件头：固定 RUNTIME_STATE_HEADER_SIZE 字节，后跟 section_count 个 section
 */
struct RuntimeStateMetaHeader {
    uint32_t magic_number{RUNTIME_STATE_MAGIC};        // 魔数 "ZBRS"
    uint32_t format_version{RUNTIME_STATE_FORMAT_VERSION};  // 格式版本
    uint32_t section_count{0};                         // section 数量
    uint32_t reserved{0};                              // 保留（补零）
    uint64_t created_at_ms{0};                         // 快照生成时刻（epoch 毫秒）

    // 序列化为固定 RUNTIME_STATE_HEADER_SIZE 字节。
    void Serialize(std::vector<uint8_t>& out) const {
        out.assign(RUNTIME_STATE_HEADER_SIZE, 0);
        uint8_t* p = out.data();
        std::memcpy(p, &magic_number, sizeof(magic_number));
        p += sizeof(magic_number);
        std::memcpy(p, &format_version, sizeof(format_version));
        p += sizeof(format_version);
        std::memcpy(p, &section_count, sizeof(section_count));
        p += sizeof(section_count);
        std::memcpy(p, &reserved, sizeof(reserved));
        p += sizeof(reserved);
        std::memcpy(p, &created_at_ms, sizeof(created_at_ms));
    }

    // 反序列化；缓冲区过短或魔数 / 格式版本不符时返回 false。
    static bool Parse(const uint8_t* header, size_t header_size, RuntimeStateMetaHeader& out) {
        if (header == nullptr || header_size < RUNTIME_STATE_HEADER_SIZE) {
            return false;
        }
        const uint8_t* p = header;
        std::memcpy(&out.magic_number, p, sizeof(out.magic_number));
        p += sizeof(out.magic_number);
        std::memcpy(&out.format_version, p, sizeof(out.format_version));
        p += sizeof(out.format_version);
        std::memcpy(&out.section_count, p, sizeof(out.section_count));
        p += sizeof(out.section_count);
        std::memcpy(&out.reserved, p, sizeof(out.reserved));
        p += sizeof(out.reserved);
        std::memcpy(&out.created_at_ms, p, sizeof(out.created_at_ms));

        return out.magic_number == RUNTIME_STATE_MAGIC &&
               out.format_version == RUNTIME_STATE_FORMAT_VERSION;
    }
};

// ---- 编码原语：写入侧 ----

inline void AppendU8(std::vector<uint8_t>& out, uint8_t value) {
    out.push_back(value);
}

inline void AppendU32(std::vector<uint8_t>& out, uint32_t value) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&value);
    out.insert(out.end(), p, p + sizeof(value));
}

inline void AppendU64(std::vector<uint8_t>& out, uint64_t value) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&value);
    out.insert(out.end(), p, p + sizeof(value));
}

inline void AppendI32(std::vector<uint8_t>& out, int32_t value) {
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&value);
    out.insert(out.end(), p, p + sizeof(value));
}

// 变长字符串：[u32 长度][字节]（不写结尾 '\0'）。
inline void AppendString(std::vector<uint8_t>& out, const std::string& value) {
    AppendU32(out, static_cast<uint32_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

// 追加一个 section：[tag u32][payload_len u64][payload]
inline void AppendSection(std::vector<uint8_t>& out,
                          RuntimeStateSectionTag tag,
                          const std::vector<uint8_t>& payload) {
    AppendU32(out, static_cast<uint32_t>(tag));
    AppendU64(out, static_cast<uint64_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
}

// ---- 编码原语：读取侧 ----

/**
 * @brief 快照字节流读取游标
 *
 * 所有读取都做边界检查：数据不足 / 越界一律返回 false，不抛异常、不读越界。
 * 解析侧据此把「截断的尾部记录」判为失败并放弃，而不是崩溃。
 */
class RuntimeStateReader {
public:
    // 默认构造：空视图（remaining() == 0）。供 SubReader 的目标对象与调用方局部变量使用。
    RuntimeStateReader() = default;

    RuntimeStateReader(const uint8_t* data, size_t size)
        : data_(data), size_(size) {
    }

    bool ReadU8(uint8_t* out) {
        return ReadRaw(out, sizeof(*out));
    }

    bool ReadU32(uint32_t* out) {
        return ReadRaw(out, sizeof(*out));
    }

    bool ReadU64(uint64_t* out) {
        return ReadRaw(out, sizeof(*out));
    }

    bool ReadI32(int32_t* out) {
        return ReadRaw(out, sizeof(*out));
    }

    // 读取变长字符串；长度超出剩余字节时返回 false。
    bool ReadString(std::string* out) {
        if (out == nullptr) {
            return false;
        }
        uint32_t len = 0;
        if (!ReadU32(&len)) {
            return false;
        }
        if (static_cast<size_t>(len) > size_ - cursor_) {
            return false;
        }
        out->assign(reinterpret_cast<const char*>(data_ + cursor_), len);
        cursor_ += len;
        return true;
    }

    // 从当前位置切出长度为 len 的子读取器（用于 section payload / 单条记录）。
    bool SubReader(uint64_t len, RuntimeStateReader* out) {
        if (out == nullptr || len > static_cast<uint64_t>(size_ - cursor_)) {
            return false;
        }
        *out = RuntimeStateReader(data_ + cursor_, static_cast<size_t>(len));
        cursor_ += static_cast<size_t>(len);
        return true;
    }

    // 剩余可读字节数。
    size_t remaining() const {
        return size_ - cursor_;
    }

    // 当前未读字节的只读起始地址（不移动游标）。
    // 用于把「已按长度切出的子段」直接交给更专门的解析函数（如 ParseWRTaskBlob）。
    const uint8_t* remaining_data() const {
        return data_ + cursor_;
    }

private:
    bool ReadRaw(void* out, size_t len) {
        if (out == nullptr || len > size_ - cursor_) {
            return false;
        }
        std::memcpy(out, data_ + cursor_, len);
        cursor_ += len;
        return true;
    }

    const uint8_t* data_{nullptr};
    size_t size_{0};
    size_t cursor_{0};
};

// ---- 单条记录编解码 ----

/**
 * @brief 把单个 WRTask 编码为自描述 blob
 *
 * 故意**不**落盘 inode_id_num：解析时由 inode_id 重新推导（WRTask::ParseInodeId），
 * 避免冗余字段在恢复后与 inode_id 不一致。
 * 通用字段（state）与失败字段（last_error_code / detail）全量落盘，
 * 以便恢复后既知道任务做什么，也知道上次失败在哪一步。
 */
inline std::vector<uint8_t> SerializeWRTaskBlob(const WR_task::WRTask& task) {
    std::vector<uint8_t> out;
    out.reserve(160);
    AppendU64(out, task.task_id);
    AppendU32(out, static_cast<uint32_t>(task.type));
    AppendU32(out, static_cast<uint32_t>(task.state));
    AppendString(out, task.disk_id);
    AppendString(out, task.volume_id);
    AppendString(out, task.inode_id);
    AppendString(out, task.file_path);
    AppendU64(out, task.state_changed_at_ms);
    AppendU32(out, static_cast<uint32_t>(task.volume_ids.size()));
    for (const std::string& id : task.volume_ids) {
        AppendString(out, id);
    }
    AppendU64(out, task.expected_image_size_bytes);
    AppendI32(out, static_cast<int32_t>(task.last_error_code));
    AppendString(out, task.last_error_detail);
    AppendU32(out, task.attempt_count);
    AppendU8(out, task.is_failed_terminal ? 1 : 0);
    return out;
}

/**
 * @brief 反序列化 WRTask blob
 *
 * 任一步数据不足、尾部有多余字节、type / state 超出枚举范围都返回 false。
 * 成功时 out 的每个字段都被显式赋值（WRTask 默认构造并不初始化全部字段）。
 */
inline bool ParseWRTaskBlob(const uint8_t* data, size_t size, WR_task::WRTask& out) {
    RuntimeStateReader reader(data, size);

    uint64_t task_id = 0;
    uint32_t type = 0;
    uint32_t state = 0;
    std::string disk_id;
    std::string volume_id;
    std::string inode_id;
    std::string file_path;
    uint64_t state_changed_at_ms = 0;
    uint32_t volume_ids_count = 0;
    uint64_t expected_image_size_bytes = 0;
    int32_t last_error_code = 0;
    std::string last_error_detail;
    uint32_t attempt_count = 0;
    uint8_t is_failed_terminal = 0;

    if (!reader.ReadU64(&task_id) ||
        !reader.ReadU32(&type) ||
        !reader.ReadU32(&state) ||
        !reader.ReadString(&disk_id) ||
        !reader.ReadString(&volume_id) ||
        !reader.ReadString(&inode_id) ||
        !reader.ReadString(&file_path) ||
        !reader.ReadU64(&state_changed_at_ms) ||
        !reader.ReadU32(&volume_ids_count)) {
        return false;
    }
    if (type > static_cast<uint32_t>(WR_task::WRTaskType::CD_READ) ||
        state > static_cast<uint32_t>(WR_task::WRTaskState::FAILED)) {
        return false;
    }

    // 计数合理性校验：每条 volume_id 至少占 4 字节（u32 长度前缀）。
    // 截断数据会把后续字段的字节误读成巨大的计数，若不先按剩余字节收紧，
    // reserve 会抛 std::length_error / std::bad_alloc —— 解析必须只返回 false。
    if (volume_ids_count > reader.remaining() / 4) {
        return false;
    }

    std::vector<std::string> volume_ids;
    volume_ids.reserve(volume_ids_count);
    for (uint32_t i = 0; i < volume_ids_count; ++i) {
        std::string volume_id_item;
        if (!reader.ReadString(&volume_id_item)) {
            return false;
        }
        volume_ids.push_back(std::move(volume_id_item));
    }

    if (!reader.ReadU64(&expected_image_size_bytes) ||
        !reader.ReadI32(&last_error_code) ||
        !reader.ReadString(&last_error_detail) ||
        !reader.ReadU32(&attempt_count) ||
        !reader.ReadU8(&is_failed_terminal) ||
        reader.remaining() != 0) {
        return false;
    }

    WR_task::WRTask parsed;
    parsed.task_id = task_id;
    parsed.type = static_cast<WR_task::WRTaskType>(type);
    parsed.disk_id = std::move(disk_id);
    parsed.volume_id = std::move(volume_id);
    parsed.inode_id = std::move(inode_id);
    parsed.inode_id_num = WR_task::WRTask::ParseInodeId(parsed.inode_id);
    parsed.state = static_cast<WR_task::WRTaskState>(state);
    parsed.state_changed_at_ms = state_changed_at_ms;
    parsed.file_path = std::move(file_path);
    parsed.volume_ids = std::move(volume_ids);
    parsed.expected_image_size_bytes = expected_image_size_bytes;
    parsed.last_error_code = static_cast<volumemanager::ErrorCode>(last_error_code);
    parsed.last_error_detail = std::move(last_error_detail);
    parsed.attempt_count = attempt_count;
    parsed.is_failed_terminal = (is_failed_terminal != 0);

    out = std::move(parsed);
    return true;
}

// 编码一批归档文件元数据（对应 SendArchiveMetadataRequest）：batch_id + files 全量字段。
// target_node_id / target_disk_id 是恢复后重新下载文件的唯一定位依据，必须落盘。
inline std::vector<uint8_t> SerializeArchiveRequestBlob(const SendArchiveMetadataRequest& request) {
    std::vector<uint8_t> out;
    out.reserve(64 + request.files.size() * 80);
    AppendU64(out, request.batch_id);
    AppendU32(out, static_cast<uint32_t>(request.files.size()));
    for (const ArchiveFileInfo& file : request.files) {
        AppendU64(out, file.inode_id);
        AppendU64(out, file.size);
        AppendU64(out, file.object_unit_size);
        AppendString(out, file.target_node_id);
        AppendString(out, file.target_disk_id);
    }
    return out;
}

// 反序列化归档批次 blob；数据不足 / 尾部有多余字节返回 false。
inline bool ParseArchiveRequestBlob(const uint8_t* data,
                                    size_t size,
                                    SendArchiveMetadataRequest& out) {
    RuntimeStateReader reader(data, size);

    uint64_t batch_id = 0;
    uint32_t file_count = 0;
    if (!reader.ReadU64(&batch_id) || !reader.ReadU32(&file_count)) {
        return false;
    }

    // 计数合理性校验：每个文件至少占 3 个 u64 定长字段（24 字节）+ 两个字符串
    // 长度前缀（8 字节）= 32 字节。截断数据误读出的巨大计数必须先被拒绝，
    // 否则 reserve 会抛 std::length_error / std::bad_alloc。
    if (file_count > reader.remaining() / 32) {
        return false;
    }

    SendArchiveMetadataRequest parsed;
    parsed.batch_id = batch_id;
    parsed.files.reserve(file_count);
    for (uint32_t i = 0; i < file_count; ++i) {
        ArchiveFileInfo file;
        if (!reader.ReadU64(&file.inode_id) ||
            !reader.ReadU64(&file.size) ||
            !reader.ReadU64(&file.object_unit_size) ||
            !reader.ReadString(&file.target_node_id) ||
            !reader.ReadString(&file.target_disk_id)) {
            return false;
        }
        parsed.files.push_back(std::move(file));
    }
    if (reader.remaining() != 0) {
        return false;
    }

    out = std::move(parsed);
    return true;
}

}  // namespace optical_node_manager
