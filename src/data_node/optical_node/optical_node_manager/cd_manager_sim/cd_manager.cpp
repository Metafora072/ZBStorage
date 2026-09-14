#include <cd_manager_sim/cd_manager.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cd_manager_sim {
namespace {

using Clock = std::chrono::steady_clock;
using Duration = Clock::duration;
using TimePoint = Clock::time_point;

// 将以秒表示的模型参数统一转换为 steady_clock 使用的时长，
// 这样调度逻辑就可以同时复用真实等待和预测完成时间的计算。
Duration SecondsToDuration(double seconds) {
    if (seconds <= 0.0) {
        return Duration::zero();
    }
    return std::chrono::duration_cast<Duration>(std::chrono::duration<double>(seconds));
}

}  // namespace

struct CDManager::Impl {
    struct Point3D {
        double x;
        double y;
        double z;
        Point3D() : x(0.0), y(0.0), z(0.0) {}
        Point3D(double x_, double y_, double z_) : x(x_), y(y_), z(z_) {}
    };

    // 任务类型枚举
    enum class TaskType { READ, BURN };

    // ActiveTask 保存一个任务在模拟器中的完整生命周期状态：
    // 支持读任务和刻录任务，从进入队列、预测装载/操作完成，到回调发出及资源释放。
    struct ActiveTask {
        TaskType type;
        ReadRequest read_request;
        BurnRequest burn_request;
        Point3D disc_location;
        TimePoint arrival_time;
        TimePoint predicted_complete_at;  // 读完成或刻录完成时刻
        TimePoint predicted_unload_complete_at;
        bool complete_notified;  // 读/刻录完成已通知标志
        int assigned_drive_index;
        int assigned_load_arm_index;
        int assigned_unload_arm_index;
        uint64_t read_size_bytes;        // 实际读取字节数
        uint64_t burn_image_size_bytes;  // 实际待刻录镜像文件大小（字节）

        ActiveTask() : type(TaskType::READ), complete_notified(false),
                       assigned_drive_index(-1), assigned_load_arm_index(-1),
                       assigned_unload_arm_index(-1),
                       read_size_bytes(0),
                       burn_image_size_bytes(0) {}
    };

    struct ReadCompleteDeadline {
        TimePoint due_time;
        uint64_t task_id;

        ReadCompleteDeadline() : due_time(), task_id(0) {}
        ReadCompleteDeadline(TimePoint due, uint64_t id) : due_time(due), task_id(id) {}

        bool operator>(const ReadCompleteDeadline& other) const {
            if (due_time != other.due_time) {
                return due_time > other.due_time;
            }
            return task_id > other.task_id;
        }
    };

    struct BurnCompleteDeadline {
        TimePoint due_time;
        uint64_t task_id;

        BurnCompleteDeadline() : due_time(), task_id(0) {}
        BurnCompleteDeadline(TimePoint due, uint64_t id) : due_time(due), task_id(id) {}

        bool operator>(const BurnCompleteDeadline& other) const {
            if (due_time != other.due_time) {
                return due_time > other.due_time;
            }
            return task_id > other.task_id;
        }
    };

    explicit Impl(CDManagerConfig cfg)
        : config(std::move(cfg)),
          rng(std::random_device{}()),
          dist_row(0, static_cast<int>(kRackRows) - 1),
          dist_col(0, static_cast<int>(kRackCols) - 1),
          dist_depth(0, static_cast<int>(kRackDepth) - 1) {}

    CDManagerConfig config;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool running = false;
    bool stop_requested = false;
    std::thread worker;
    ReadCompleteCallback read_complete_callback;
    BurnCompleteCallback burn_complete_callback;

    std::unordered_map<uint64_t, ActiveTask> active_tasks;
    std::priority_queue<ReadCompleteDeadline,
                        std::vector<ReadCompleteDeadline>,
                        std::greater<ReadCompleteDeadline>> read_complete_heap;
    std::priority_queue<BurnCompleteDeadline,
                        std::vector<BurnCompleteDeadline>,
                        std::greater<BurnCompleteDeadline>> burn_complete_heap;
    bool schedule_dirty = false;

    std::mt19937 rng;
    std::uniform_int_distribution<int> dist_row;
    std::uniform_int_distribution<int> dist_col;
    std::uniform_int_distribution<int> dist_depth;

    static constexpr double kTimeGraspSeconds = 2.0;
    static constexpr double kTimeDriveLoadSeconds = 12.0;
    static constexpr double kTimeDriveEjectSeconds = 3.0;
    static constexpr double kTimeReadFixedSeconds = 0.5;
    static constexpr double kReadBandwidthMbPerSecond = 100.0;
    static constexpr double kArmSpeedX = 2.0;
    static constexpr double kArmSpeedY = 1.0;
    static constexpr double kArmSpeedZ = 1.0;
    static constexpr double kRackRows = 50.0;
    static constexpr double kRackCols = 200.0;
    static constexpr double kRackDepth = 10.0;
    static constexpr double kSlotWidth = 0.015;
    static constexpr double kSlotHeight = 0.15;
    static constexpr double kSlotDepth = 0.3;

    // Start/Stop 维护一个后台调度线程：
    // 线程只负责等待最早完成的任务并在到点后触发回调。
    void Start() {
        std::lock_guard<std::mutex> lock(mutex);
        if (running) {
            return;
        }

        stop_requested = false;
        running = true;
        worker = std::thread([this] {
            WorkerLoop();
        });
    }

    // 停止后台调度线程：先在锁内设置停止标志，再唤醒工作线程，
    // 最后在锁外 join，避免持锁等待线程退出造成死锁。
    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!running) {
                return;
            }
            stop_requested = true;
        }
        cv.notify_all();

        if (worker.joinable()) {
            worker.join();
        }

        std::lock_guard<std::mutex> lock(mutex);
        running = false;
    }

    // 向模拟器提交一个新的读请求。
    // 成功后任务会进入 active_tasks，并把调度标记为失效，
    // 由后台线程按最新资源占用关系统一重建所有任务的完成时刻。
    bool SubmitReadTask(const ReadRequest& request) {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running || stop_requested) {
            return false;
        }
        if (active_tasks.find(request.task_id) != active_tasks.end()) {
            return false;
        }

        ActiveTask task;
        task.type = TaskType::READ;
        task.read_request = request;
        task.disc_location = GenerateRandomDiscLocationLocked();
        // 优先使用调用方在 ReadRequest 中提供的真实读取字节数；
        // 未提供（0）时回退到 config.volume_size，保持纯模拟场景兼容。
        task.read_size_bytes = (request.read_size_bytes != 0)
                                   ? request.read_size_bytes
                                   : config.volume_size;
        task.arrival_time = Clock::now();
        active_tasks.emplace(request.task_id, std::move(task));

        schedule_dirty = true;
        cv.notify_all();
        return true;
    }

    // 向模拟器提交一个新的刻录请求。
    // 刻录任务不经过缓存，直接进行调度。
    bool SubmitBurnTask(const BurnRequest& request) {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running || stop_requested) {
            return false;
        }
        if (active_tasks.find(request.task_id) != active_tasks.end()) {
            return false;
        }

        ActiveTask task;
        task.type = TaskType::BURN;
        task.burn_request = request;
        task.disc_location = GenerateRandomDiscLocationLocked();
        // 优先使用调用方在 BurnRequest 中提供的真实镜像字节数；
        // 未提供（0）时回退到 config.burn_image_size，保持纯模拟场景兼容。
        task.burn_image_size_bytes = (request.image_size_bytes != 0)
                                         ? request.image_size_bytes
                                         : config.burn_image_size;
        task.arrival_time = Clock::now();
        active_tasks.emplace(request.task_id, std::move(task));

        schedule_dirty = true;
        cv.notify_all();
        return true;
    }

    // 注册读完成回调。回调会在后台线程判断某个任务到达预计读完成时刻后触发。
    void SetReadCompleteCallback(ReadCompleteCallback callback) {
        std::lock_guard<std::mutex> lock(mutex);
        read_complete_callback = std::move(callback);
    }

    // 注册刻录完成回调。回调会在后台线程判断某个任务到达预计刻录完成时刻后触发。
    void SetBurnCompleteCallback(BurnCompleteCallback callback) {
        std::lock_guard<std::mutex> lock(mutex);
        burn_complete_callback = std::move(callback);
    }

    // 查询指定 task_id 是否仍然处于模拟器的活动集合中。
    bool HasActiveTask(uint64_t task_id) const {
        std::lock_guard<std::mutex> lock(mutex);
        return active_tasks.find(task_id) != active_tasks.end();
    }

    // 返回当前仍在模拟生命周期内的任务数量。
    std::size_t ActiveTaskCount() const {
        std::lock_guard<std::mutex> lock(mutex);
        return active_tasks.size();
    }

    // 后台线程维护一个预测调度：
    // 它会等待下一次"读/刻录完成回调"或"卸盘完成释放"时刻到达，再推进状态。
    void WorkerLoop() {
        std::unique_lock<std::mutex> lock(mutex);
        while (true) {
            if (stop_requested) {
                break;
            }

            if (schedule_dirty) {
                RebuildScheduleLocked();
                schedule_dirty = false;
            }
            // 清理过期的截止时间
            DiscardStaleDeadlinesLocked();
            ReleaseCompletedResourcesLocked();

            const std::optional<TimePoint> next_wakeup = NextWakeupTimeLocked();
            // 当前没有任何可等待的未来事件，挂起等待新任务或停止信号。
            if (!next_wakeup.has_value()) {
                cv.wait(lock, [this] {
                    return stop_requested || schedule_dirty || !active_tasks.empty();
                });
                continue;
            }

            cv.wait_until(lock, *next_wakeup, [this, next_wakeup] {
                if (stop_requested || schedule_dirty) {
                    return true;
                }
                const std::optional<TimePoint> updated = NextWakeupTimeLocked();
                return !updated.has_value() || updated != next_wakeup;
            });

            if (stop_requested) {
                break;
            }

            if (schedule_dirty) {
                RebuildScheduleLocked();
                schedule_dirty = false;
            }
            DiscardStaleDeadlinesLocked();
            ReleaseCompletedResourcesLocked();

            const std::optional<TimePoint> current_wakeup = NextWakeupTimeLocked();
            // 即便线程已经醒来，也不代表一定到了目标时间，再次确认。
            if (current_wakeup.has_value() && *current_wakeup > Clock::now()) {
                continue;
            }

            // 检查读完成事件
            if (!read_complete_heap.empty() && read_complete_heap.top().due_time <= Clock::now()) {
                const ReadCompleteDeadline current_deadline = read_complete_heap.top();
                read_complete_heap.pop();
                NotifyReadCompleteLocked(current_deadline.task_id, lock);
            }

            // 检查刻录完成事件
            if (!burn_complete_heap.empty() && burn_complete_heap.top().due_time <= Clock::now()) {
                const BurnCompleteDeadline current_deadline = burn_complete_heap.top();
                burn_complete_heap.pop();
                NotifyBurnCompleteLocked(current_deadline.task_id, lock);
            }

            // 完成后资源释放
            ReleaseCompletedResourcesLocked();
        }

        ReleaseCompletedResourcesLocked();
    }

    // 在确认读任务已达到预测读完成时刻后，构造回调事件并发出通知。
    void NotifyReadCompleteLocked(uint64_t task_id, std::unique_lock<std::mutex>& lock) {
        auto it = active_tasks.find(task_id);
        if (it == active_tasks.end()) {
            return;
        }
        if (it->second.complete_notified) {
            return;
        }
        if (Clock::now() < it->second.predicted_complete_at) {
            return;
        }

        it->second.complete_notified = true;

        ReadCompleteEvent event;
        event.task_id = it->second.read_request.task_id;
        event.disk_id = it->second.read_request.disk_id;
        event.volume_id = it->second.read_request.volume_id;
        event.inode_id = it->second.read_request.inode_id;
        ReadCompleteCallback callback = read_complete_callback;

        lock.unlock();
        try {
            if (callback) callback(event);
        } catch (...) {
            lock.lock();
            throw;
        }
        lock.lock();
    }

    // 在确认刻录任务已达到预测刻录完成时刻后，构造回调事件并发出通知。
    void NotifyBurnCompleteLocked(uint64_t task_id, std::unique_lock<std::mutex>& lock) {
        auto it = active_tasks.find(task_id);
        if (it == active_tasks.end()) {
            return;
        }
        if (it->second.complete_notified) {
            return;
        }
        if (Clock::now() < it->second.predicted_complete_at) {
            return;
        }

        it->second.complete_notified = true;

        BurnCompleteEvent event;
        event.task_id = it->second.burn_request.task_id;
        event.disk_id = it->second.burn_request.disk_id;
        event.volume_id = it->second.burn_request.volume_id;
        event.image_path = it->second.burn_request.image_path;
        BurnCompleteCallback callback = burn_complete_callback;

        lock.unlock();
        try {
            if (callback) callback(event);
        } catch (...) {
            lock.lock();
            throw;
        }
        lock.lock();
    }

    // 从当前状态中找出下一次需要线程醒来的时刻。
    std::optional<TimePoint> NextWakeupTimeLocked() const {
        std::optional<TimePoint> next_time;

        // 检查读完成堆
        if (!read_complete_heap.empty()) {
            next_time = read_complete_heap.top().due_time;
        }

        // 检查刻录完成堆
        if (!burn_complete_heap.empty()) {
            if (!next_time.has_value() || burn_complete_heap.top().due_time < *next_time) {
                next_time = burn_complete_heap.top().due_time;
            }
        }

        // 检查待卸载任务的完成时间
        for (const auto& pair : active_tasks) {
            const ActiveTask& task = pair.second;
            if (!task.complete_notified) {
                continue;
            }
            if (!next_time.has_value() || task.predicted_unload_complete_at < *next_time) {
                next_time = task.predicted_unload_complete_at;
            }
        }
        return next_time;
    }

    // 清理那些已经完成卸盘阶段的任务。
    void ReleaseCompletedResourcesLocked() {
        const TimePoint now = Clock::now();
        for (auto it = active_tasks.begin(); it != active_tasks.end();) {
            if (it->second.complete_notified && it->second.predicted_unload_complete_at <= now) {
                it = active_tasks.erase(it);
            } else {
                ++it;
            }
        }
    }

    // 从最小堆顶开始移除已经失效的读完成事件。
    void DiscardStaleDeadlinesLocked() {
        // 清理失效的读完成事件
        while (!read_complete_heap.empty()) {
            const ReadCompleteDeadline top = read_complete_heap.top();
            const auto it = active_tasks.find(top.task_id);
            if (it == active_tasks.end() ||
                it->second.complete_notified ||
                it->second.type != TaskType::READ ||
                it->second.predicted_complete_at != top.due_time) {
                read_complete_heap.pop();
                continue;
            }
            break;
        }

        // 清理失效的刻录完成事件
        while (!burn_complete_heap.empty()) {
            const BurnCompleteDeadline top = burn_complete_heap.top();
            const auto it = active_tasks.find(top.task_id);
            if (it == active_tasks.end() ||
                it->second.complete_notified ||
                it->second.type != TaskType::BURN ||
                it->second.predicted_complete_at != top.due_time) {
                burn_complete_heap.pop();
                continue;
            }
            break;
        }
    }

    // 基于当前 active_tasks 从头重建整条预测调度时间线。
    // 调度语义贴合 ODL_simulation.cpp：
    // 1. 光驱在任务进入系统后先被占住，直到卸载完成才释放；
    // 2. 机械臂在装载完成后立即释放，不等待读盘/刻录；
    // 3. 完成后任务进入"等待卸载臂"队列，机械臂优先处理卸载；
    // 4. 读任务优先于刻录任务分配光驱。
    void RebuildScheduleLocked() {
        std::vector<uint64_t> pending_read_ids;
        std::vector<uint64_t> pending_burn_ids;
        pending_read_ids.reserve(active_tasks.size());
        pending_burn_ids.reserve(active_tasks.size());

        for (const auto& pair : active_tasks) {
            if (!pair.second.complete_notified) {
                if (pair.second.type == TaskType::READ) {
                    pending_read_ids.push_back(pair.first);
                } else {
                    pending_burn_ids.push_back(pair.first);
                }
            }
        }

        std::priority_queue<ReadCompleteDeadline,
                            std::vector<ReadCompleteDeadline>,
                            std::greater<ReadCompleteDeadline>> new_read_heap;
        std::priority_queue<BurnCompleteDeadline,
                            std::vector<BurnCompleteDeadline>,
                            std::greater<BurnCompleteDeadline>> new_burn_heap;

        if (pending_read_ids.empty() && pending_burn_ids.empty()) {
            read_complete_heap = std::move(new_read_heap);
            burn_complete_heap = std::move(new_burn_heap);
            return;
        }

        // 按到达时间排序读任务
        std::sort(pending_read_ids.begin(), pending_read_ids.end(),
                  [this](uint64_t a, uint64_t b) {
                      const auto& lhs = active_tasks.at(a);
                      const auto& rhs = active_tasks.at(b);
                      if (lhs.arrival_time != rhs.arrival_time) {
                          return lhs.arrival_time < rhs.arrival_time;
                      }
                      return a < b;
                  });

        // 按到达时间排序刻录任务
        std::sort(pending_burn_ids.begin(), pending_burn_ids.end(),
                  [this](uint64_t a, uint64_t b) {
                      const auto& lhs = active_tasks.at(a);
                      const auto& rhs = active_tasks.at(b);
                      if (lhs.arrival_time != rhs.arrival_time) {
                          return lhs.arrival_time < rhs.arrival_time;
                      }
                      return a < b;
                  });

        const int num_drives = std::max(1, config.num_drives);
        const int num_arms = std::max(1, config.num_arms);
        const TimePoint now = Clock::now();

        std::vector<TimePoint> drive_available_at(num_drives, now);
        std::vector<TimePoint> arm_available_at(num_arms, now);

        const Duration drive_load_duration = CalculateDriveLoadDuration();
        const Duration drive_eject_duration = CalculateDriveEjectDuration();

        struct PendingLoadTask {
            uint64_t task_id;
            TaskType type;
            TimePoint ready_time;
        };
        struct PendingUnloadTask {
            uint64_t task_id;
            TimePoint ready_time;
        };

        auto compare_ready_load = [](const PendingLoadTask& lhs, const PendingLoadTask& rhs) {
            if (lhs.ready_time != rhs.ready_time) {
                return lhs.ready_time > rhs.ready_time;
            }
            return lhs.task_id > rhs.task_id;
        };
        auto compare_ready_unload = [](const PendingUnloadTask& lhs, const PendingUnloadTask& rhs) {
            if (lhs.ready_time != rhs.ready_time) {
                return lhs.ready_time > rhs.ready_time;
            }
            return lhs.task_id > rhs.task_id;
        };

        std::priority_queue<PendingLoadTask,
                            std::vector<PendingLoadTask>,
                            decltype(compare_ready_load)> load_queue(compare_ready_load);
        std::priority_queue<PendingUnloadTask,
                            std::vector<PendingUnloadTask>,
                            decltype(compare_ready_unload)> unload_queue(compare_ready_unload);

        // 将读任务和刻录任务都加入装载队列
        for (uint64_t task_id : pending_read_ids) {
            load_queue.push({task_id, TaskType::READ, active_tasks.at(task_id).arrival_time});
        }
        for (uint64_t task_id : pending_burn_ids) {
            load_queue.push({task_id, TaskType::BURN, active_tasks.at(task_id).arrival_time});
        }

        auto has_pending_work = [&]() {
            return !load_queue.empty() || !unload_queue.empty();
        };

        while (has_pending_work()) {
            const std::size_t load_arm_idx = IndexOfEarliestTime(arm_available_at);
            const TimePoint next_arm_available = arm_available_at[load_arm_idx];

            // 优先处理卸载队列
            const bool can_unload = !unload_queue.empty() && unload_queue.top().ready_time <= next_arm_available;
            if (can_unload) {
                const PendingUnloadTask pending = unload_queue.top();
                unload_queue.pop();

                ActiveTask& task = active_tasks.at(pending.task_id);
                const TimePoint unload_start = std::max(next_arm_available, task.predicted_complete_at);
                const Duration unload_move = CalculateArmMoveDuration(task.disc_location);
                const TimePoint unload_complete = unload_start + drive_eject_duration + unload_move;

                task.assigned_unload_arm_index = static_cast<int>(load_arm_idx);
                task.predicted_unload_complete_at = unload_complete;
                arm_available_at[load_arm_idx] = unload_complete;
                if (task.assigned_drive_index >= 0 && task.assigned_drive_index < num_drives) {
                    drive_available_at[task.assigned_drive_index] = unload_complete;
                }
                continue;
            }

            // 装载队列为空但有卸载任务时，分配机械臂处理卸载
            if (load_queue.empty()) {
                const PendingUnloadTask pending = unload_queue.top();
                unload_queue.pop();

                ActiveTask& task = active_tasks.at(pending.task_id);
                const std::size_t unload_arm_idx = IndexOfEarliestTime(arm_available_at);
                const TimePoint arm_ready = arm_available_at[unload_arm_idx];
                const TimePoint unload_start = std::max(arm_ready, task.predicted_complete_at);
                const Duration unload_move = CalculateArmMoveDuration(task.disc_location);
                const TimePoint unload_complete = unload_start + drive_eject_duration + unload_move;

                task.assigned_unload_arm_index = static_cast<int>(unload_arm_idx);
                task.predicted_unload_complete_at = unload_complete;
                arm_available_at[unload_arm_idx] = unload_complete;
                if (task.assigned_drive_index >= 0 && task.assigned_drive_index < num_drives) {
                    drive_available_at[task.assigned_drive_index] = unload_complete;
                }
                continue;
            }

            // 处理装载队列
            const PendingLoadTask pending = load_queue.top();
            load_queue.pop();

            ActiveTask& task = active_tasks.at(pending.task_id);
            const std::size_t drive_idx = IndexOfEarliestTime(drive_available_at);
            const TimePoint drive_ready = drive_available_at[drive_idx];
            const std::size_t arm_idx = IndexOfEarliestTime(arm_available_at);
            const TimePoint arm_ready = arm_available_at[arm_idx];

            const Duration outbound_move = CalculateArmMoveDuration(task.disc_location);
            const double time_z = task.disc_location.z / kArmSpeedZ;
            const double time_xy = std::max(task.disc_location.x / kArmSpeedX,
                                            task.disc_location.y / kArmSpeedY);
            const Duration inbound_move = SecondsToDuration(time_z + time_xy);

            const TimePoint drive_allocated_at = std::max(task.arrival_time, drive_ready);
            const TimePoint load_start = std::max(drive_allocated_at, arm_ready);
            const TimePoint load_complete = load_start + outbound_move + inbound_move;

            // 根据任务类型选择不同的操作时长
            Duration operation_duration;
            if (pending.type == TaskType::READ) {
                operation_duration = CalculateReadDuration(task.read_size_bytes);
            } else {
                operation_duration = CalculateBurnDuration(task.burn_image_size_bytes);
            }

            const TimePoint complete = load_complete + drive_load_duration + operation_duration;

            task.assigned_drive_index = static_cast<int>(drive_idx);
            task.assigned_load_arm_index = static_cast<int>(arm_idx);
            task.assigned_unload_arm_index = -1;
            task.predicted_complete_at = complete;
            task.predicted_unload_complete_at = complete;

            arm_available_at[arm_idx] = load_complete;
            drive_available_at[drive_idx] = complete;

            // 根据任务类型放入对应的完成堆
            if (pending.type == TaskType::READ) {
                new_read_heap.push({complete, pending.task_id});
            } else {
                new_burn_heap.push({complete, pending.task_id});
            }
            unload_queue.push({pending.task_id, complete});
        }

        read_complete_heap = std::move(new_read_heap);
        burn_complete_heap = std::move(new_burn_heap);
    }

    // 为新任务随机生成一个盘片在库位中的三维位置。
    Point3D GenerateRandomDiscLocationLocked() {
        const int r = dist_row(rng);
        const int c = dist_col(rng);
        const int d = dist_depth(rng);
        return Point3D(c * kSlotWidth, r * kSlotHeight, d * kSlotDepth);
    }

    // 估算一次读请求的纯读取阶段耗时。
    // 入参为实际读取的字节数，而不是固定用 config.volume_size。
    Duration CalculateReadDuration(uint64_t read_size_bytes) const {
        const double read_file_size_mb = static_cast<double>(read_size_bytes) / (1024.0 * 1024.0);
        const double transfer_time = read_file_size_mb / kReadBandwidthMbPerSecond;
        return SecondsToDuration(kTimeReadFixedSeconds + transfer_time);
    }

    // 估算一次刻录请求的纯刻录阶段耗时。
    // 参照 ODL_simulation.cpp 中的 calculate_burn_time()。
    // 入参为实际待刻录镜像的字节数，而不是固定用 config.burn_image_size，
    // 否则 10MB 的小镜像也会按默认 10GB 计算，导致刻录耗时被放大数百倍。
    Duration CalculateBurnDuration(uint64_t image_size_bytes) const {
        const double burn_image_size_mb = static_cast<double>(image_size_bytes) / (1024.0 * 1024.0);
        const double transfer_time = burn_image_size_mb / config.burn_bandwidth_mbps;
        return SecondsToDuration(config.time_burn_fixed_seconds + transfer_time);
    }

    // 返回光驱执行装盘动作的固定耗时。
    Duration CalculateDriveLoadDuration() const {
        return SecondsToDuration(kTimeDriveLoadSeconds);
    }

    // 返回光驱执行退盘动作的固定耗时。
    Duration CalculateDriveEjectDuration() const {
        return SecondsToDuration(kTimeDriveEjectSeconds);
    }

    // 根据盘片所在三维坐标估算机械臂的单程移动时间。
    Duration CalculateArmMoveDuration(const Point3D& disc_location) const {
        const double time_z = disc_location.z / kArmSpeedZ;
        const double time_xy = std::max(disc_location.x / kArmSpeedX, disc_location.y / kArmSpeedY);
        return SecondsToDuration(time_z + time_xy + kTimeGraspSeconds);
    }

    // 在一组资源可用时间中找到最早空闲的那一个。
    static std::size_t IndexOfEarliestTime(const std::vector<TimePoint>& times) {
        return static_cast<std::size_t>(std::distance(
            times.begin(),
            std::min_element(times.begin(), times.end())));
    }
};

// 构造 CDManager，并创建内部实现对象。
CDManager::CDManager(CDManagerConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

// 析构时确保后台线程和内部模拟状态被安全停止并回收。
CDManager::~CDManager() {
    Stop();
}

// 启动内部实现维护的后台调度线程。
void CDManager::Start() {
    impl_->Start();
}

// 停止内部实现的后台调度线程。
void CDManager::Stop() {
    impl_->Stop();
}

// 向内部实现提交一个读请求。
bool CDManager::SubmitReadTask(const ReadRequest& request) {
    return impl_->SubmitReadTask(request);
}

// 向内部实现提交一个刻录请求。
bool CDManager::SubmitBurnTask(const BurnRequest& request) {
    return impl_->SubmitBurnTask(request);
}

// 设置读完成事件的回调函数。
void CDManager::SetReadCompleteCallback(ReadCompleteCallback callback) {
    impl_->SetReadCompleteCallback(std::move(callback));
}

// 设置刻录完成事件的回调函数。
void CDManager::SetBurnCompleteCallback(BurnCompleteCallback callback) {
    impl_->SetBurnCompleteCallback(std::move(callback));
}

// 查询某个任务是否仍在内部调度器的活动集合中。
bool CDManager::HasActiveTask(uint64_t task_id) const {
    return impl_->HasActiveTask(task_id);
}

// 返回当前处于内部模拟生命周期中的任务总数。
std::size_t CDManager::ActiveTaskCount() const {
    return impl_->ActiveTaskCount();
}

}  // namespace cd_manager_sim
