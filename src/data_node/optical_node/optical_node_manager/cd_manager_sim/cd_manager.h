#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace cd_manager_sim {

// ReadRequest 表示一个已经由上层确认“缓存未命中”的读任务输入。
// cd_manager 不负责再次判断是否命中缓存，只负责接收这类任务并进行调度。
struct ReadRequest {
    uint64_t task_id = 0;
    std::string disk_id;
    std::string volume_id;
    std::string inode_id;
    uint64_t read_size_bytes = 0;  // 实际读取字节数，由调用方填入
};

// ReadCompleteEvent 表示 cd_manager 对外通知的“读阶段完成事件”。
// 该事件在模拟读盘阶段结束时触发；此时卸盘/归位释放流程可能仍在内部继续。
struct ReadCompleteEvent {
    uint64_t task_id = 0;
    std::string disk_id;
    std::string volume_id;
    std::string inode_id;
};

// BurnRequest 表示一个刻录任务输入。
// 刻录任务不经过缓存，直接由 cd_manager 调度执行。
struct BurnRequest {
    uint64_t task_id = 0;
    std::string disk_id;
    std::string volume_id;
    std::string image_path;
    uint64_t image_size_bytes = 0;  // 实际待刻录镜像文件大小（字节），由调用方填入
};

// BurnCompleteEvent 表示 cd_manager 对外通知的"刻录阶段完成事件"。
// 刻录完成意味着数据已成功写入光盘，卸盘/归位释放流程可能仍在内部继续。
struct BurnCompleteEvent {
    uint64_t task_id = 0;
    std::string disk_id;
    std::string volume_id;
    std::string image_path;
};

// CDManagerConfig 用于集中管理 cd_manager 的基础配置。
// 配置项命名与 ODL_simulation 中的 SimConfig 保持一致，便于两边对齐。
struct CDManagerConfig {
    int num_drives = 20;
    int num_arms = 4;
    uint64_t volume_size = 1024ULL * 1024ULL * 1024ULL;
    // 刻录相关参数
    uint64_t burn_image_size = 10ULL * 1024ULL * 1024ULL * 1024ULL;  // 默认10GB镜像大小
    double burn_bandwidth_mbps = 20.0;  // 刻录带宽 MB/s
    double time_burn_fixed_seconds = 1.0;  // 刻录固定延迟
    std::string workspace_root = "/tmp/cd_manager/";
};

class CDManager {
public:
    // ReadCompleteCallback 由上层注册。
    // 当某个任务完成模拟读阶段时，cd_manager 会通过该回调通知上层继续后续流程。
    using ReadCompleteCallback = std::function<void(const ReadCompleteEvent& event)>;

    // BurnCompleteCallback 由上层注册。
    // 当某个任务完成模拟刻录阶段时，cd_manager 会通过该回调通知上层。
    using BurnCompleteCallback = std::function<void(const BurnCompleteEvent& event)>;

    explicit CDManager(CDManagerConfig config = {});
    ~CDManager();

    CDManager(const CDManager&) = delete;
    CDManager& operator=(const CDManager&) = delete;

    // Start() 启动 cd_manager 内部后台线程与调度循环。
    // 若重复调用，后续实现可选择静默忽略。
    void Start();

    // Stop() 请求后台线程退出，并等待其完成清理。
    // 关闭后不应再继续接受新任务。
    void Stop();

    // SubmitReadTask() 提交一个缓存未命中的读任务。
    // 返回 true 表示任务已被 cd_manager 接管；返回 false 表示提交失败。
    bool SubmitReadTask(const ReadRequest& request);

    // SubmitBurnTask() 提交一个刻录任务。
    // 返回 true 表示任务已被 cd_manager 接管；返回 false 表示提交失败。
    bool SubmitBurnTask(const BurnRequest& request);

    // SetReadCompleteCallback() 注册读完成回调。
    // 建议在 Start() 前完成注册，以避免运行中替换回调带来的同步复杂度。
    void SetReadCompleteCallback(ReadCompleteCallback callback);

    // SetBurnCompleteCallback() 注册刻录完成回调。
    // 建议在 Start() 前完成注册，以避免运行中替换回调带来的同步复杂度。
    void SetBurnCompleteCallback(BurnCompleteCallback callback);

    // HasActiveTask() 用于调试或上层状态核对，判断某任务当前是否仍在 cd_manager 的活跃集合中。
    bool HasActiveTask(uint64_t task_id) const;

    // ActiveTaskCount() 返回当前仍由 cd_manager 管理的活跃任务数量。
    std::size_t ActiveTaskCount() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace cd_manager_sim
