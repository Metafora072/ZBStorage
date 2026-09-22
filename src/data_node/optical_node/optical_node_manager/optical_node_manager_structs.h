#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
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

}  // namespace start_failure_phase

}  // namespace optical_node_manager

// ============================================================================
// 以下内容来自 WR_task.h
// ============================================================================

namespace WR_task {

// 任务生命周期状态：START 等待 zip；WAITING 等底层调度；LOADING 已下发 cd_manager（仅 CD_READ）；
// READY 读产物已解压可读（仅 READ）；ZIPPED 压缩完成；CD_BURNING 刻录中（仅 CD_BURN）；
// FINISH 已消费完毕（READ 读完 / WRITE 压缩完 / CD_BURN 烧完 / batch 处理完 / CD_READ 加载完）；FAILED 终态。
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
//   CD_BURN              刻录任务
//   READ_BATCH_BY_VOLUME 仅 volume_id 有效，镜像就绪后批量推进该卷下挂起的 READ 任务
//   CD_READ              光盘库读请求：让光盘库把某卷镜像加载回 image_dir_（不产出用户数据）
enum class WRTaskType { READ, WRITE, CD_BURN, READ_BATCH_BY_VOLUME, CD_READ };

inline const char* WRTaskTypeToString(WRTaskType type) {
    switch (type) {
        case WRTaskType::READ:                 return "READ";
        case WRTaskType::WRITE:                return "WRITE";
        case WRTaskType::CD_BURN:              return "CD_BURN";
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

    // 任务是否已到达 FINISH 终态（通用语义：READ 读完 / WRITE 压缩完 / CD_BURN 刻录完 / batch 处理完）。
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

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<uint64_t>> map_;
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

private:
    mutable std::mutex mutex_;
    std::condition_variable condition_variable_;
    std::queue<SendArchiveMetadataRequest> queue_;
    bool closed_ = false;
};

}  // namespace optical_node_manager
