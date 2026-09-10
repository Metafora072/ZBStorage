#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>

#include "volume_manager/error_codes.h"

namespace WR_task {

// 任务生命周期状态：START 等待 zip；WAITING 等底层调度；LOADING 已下发 cd_manager；
// LOADED 镜像就绪；READY 读产物已解压可读（仅 READ）；ZIPPED 压缩完成；BURNING 刻录中；
// FINISH 已消费完毕（READ 读完 / WRITE 压缩完 / BURN 烧完 / batch 处理完）；FAILED 终态。
// FINISH / FAILED 为终态，由清理线程周期性从 task_map_ 移除 FINISH 任务。
enum class WRTaskState { START, WAITING, LOADING, LOADED, READY, ZIPPED, BURNING, FINISH, FAILED };

inline const char* WRTaskStateToString(WRTaskState state) {
    switch (state) {
        case WRTaskState::START:   return "START";
        case WRTaskState::WAITING: return "WAITING";
        case WRTaskState::LOADING: return "LOADING";
        case WRTaskState::LOADED:  return "LOADED";
        case WRTaskState::READY:   return "READY";
        case WRTaskState::ZIPPED:  return "ZIPPED";
        case WRTaskState::BURNING: return "BURNING";
        case WRTaskState::FINISH:  return "FINISH";
        case WRTaskState::FAILED:  return "FAILED";
    }
    return "UNKNOWN";
}

// 任务类型：READ 读；WRITE 写；BURN 刻录；READ_BATCH_BY_VOLUME 仅 volume_id 有效，
// 触发该卷下挂起读任务的批量推进。
enum class WRTaskType { READ, WRITE, BURN, READ_BATCH_BY_VOLUME };

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
    std::string file_path;

    // 仅当 state==FAILED 时有意义；task_map_->Get(id) 可读到具体失败码与上下文。
    volumemanager::ErrorCode last_error_code;
    std::string last_error_detail;

    // 任务被推回 WAITING 重新入队的次数；超上限后由调用方强制置 FAILED。
    uint32_t attempt_count;

    // 与 state==FAILED 同步的冗余标志，便于快照直接读出终态。
    bool is_failed_terminal;

    WRTask() = default;

    explicit WRTask(uint64_t task_id, WRTaskType type)
        : task_id(task_id),
          type(type),
          inode_id_num(INVALID_INODE_ID),
          state(WRTaskState::START),
          file_path("none"),
          last_error_code(volumemanager::ErrorCode::SUCCESS),
          last_error_detail(),
          attempt_count(0),
          is_failed_terminal(false) {
    }

    // 置为失败终态并记录错误码 / 上下文；调用方负责同步写回 task_map_。
    void SetFailed(volumemanager::ErrorCode code, const std::string& detail = std::string()) {
        state = WRTaskState::FAILED;
        last_error_code = code;
        last_error_detail = detail;
        is_failed_terminal = true;
    }

    // 把任务从 WAITING 回退到 START 并自增 attempt_count；终态任务会被静默忽略。
    void ResetForRetry() {
        if (is_failed_terminal) {
            return;
        }
        state = WRTaskState::START;
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
    }

    // 设置写任务载荷；disk_id/volume_id 留空，由 ZipTaskProcessor 分配。
    void SetWriteTask(const std::string& inode_id) {
        this->disk_id.clear();
        this->volume_id.clear();
        this->inode_id = inode_id;
        this->inode_id_num = ParseInodeId(inode_id);
        this->type = WRTaskType::WRITE;
        state = WRTaskState::START;
    }

    // 设置刻录任务载荷；不含 inode_id。
    void SetBurnTask(const std::string& disk_id,
                     const std::string& volume_id,
                     const std::string& file_path) {
        this->disk_id = disk_id;
        this->volume_id = volume_id;
        this->file_path = file_path;
        this->inode_id.clear();
        this->inode_id_num = INVALID_INODE_ID;
        this->type = WRTaskType::BURN;
        state = WRTaskState::START;
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
    }

    // 任务是否已到达 FINISH 终态（通用语义：READ 读完 / WRITE 压缩完 / BURN 刻录完 / batch 处理完）。
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
