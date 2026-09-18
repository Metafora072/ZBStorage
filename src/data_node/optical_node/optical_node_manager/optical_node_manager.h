#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <queue>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cd_manager_sim/cd_manager.h>
#include <optical_node_manager_structs.h>
#include <space_manager/image_dir_manager.h>
#include <volume_manager/volume_manager.h>

// 仅用于 scheduler 出站 channel 的不透明持有，避免在头文件引入 brpc 依赖。
namespace brpc { class Channel; }

namespace optical_node_manager {

// 顶层读写管理：协调 volume_manager 缓存、cd_manager 异步光盘调度与后台工作线程。
class OpticalNodeManager {
public:
    // volume_size / size_threshold 透传给 volume_manager；同时构建 cd_manager 调度组件。
    // root_dir / capacity_in_images / available_volume_id_count 为启动所需的工作目录、
    // image_dir 镜像数上限与 volume_id 队列容量上限。
    // scheduler_addr 为 scheduler 服务地址，供归档线程查询全量节点视图（node_id→address）。
    OpticalNodeManager(uint64_t volume_size,
                       double size_threshold,
                       const std::string& root_dir,
                       uint64_t capacity_in_images,
                       uint8_t available_volume_id_count,
                       const std::string& scheduler_addr);
    ~OpticalNodeManager();

    OpticalNodeManager(const OpticalNodeManager&) = delete;
    OpticalNodeManager& operator=(const OpticalNodeManager&) = delete;

    // 提交异步读任务，立即返回 task_id；调用方通过 ReadObjectByTaskId 取结果。
    volumemanager::ErrorCode RequestAsyncReadFile(const std::string& disk_id,
                                                  const std::string& volume_id,
                                                  const std::string& inode_id,
                                                  uint64_t* task_id);

    // 按 task_id 读取已就绪（READY）读任务的 [offset, offset+read_size) 区间到 *out 末尾；
    // 服务端按 offset+read_size>=file_size 自动判定末片，末片读成功后 unlink 读产物并置 FINISH。
    // 客户端按固定 read_size 顺序调用即可，无需感知末片。
    // FAILED 返回细分错误码。
    volumemanager::ErrorCode ReadObjectByTaskId(const uint64_t& task_id,
                                                std::string* out,
                                                uint64_t offset,
                                                uint64_t read_size);

    // 按 inode_id 读取最近一次 RequestAsyncReadFile(inode_id) 产物的 [offset, offset+read_size) 区间；
    // 内部先查 inode_id→task_id 索引，再走 ReadObjectByTaskId 同一管线（is_last / unlink / Erase 一致）。
    // 索引由 RequestAsyncReadFile 写入、ReadObjectByTaskId 末片清除。
    // 未发起过对应 read 请求或已读完时返回 TASK_NOT_FOUND。
    volumemanager::ErrorCode ReadObjectByInodeId(const std::string& inode_id,
                                                std::string* out,
                                                uint64_t offset,
                                                uint64_t read_size);

    // 写一段数据到 input_file_dir_/<inode_id>.bin 的 [offset, offset+data_size) 区间；
    // 服务端按 inode_id 跟踪分片累积：累计字节 == total_size 时才创建 WRITE 任务并入压缩 / 刻录队列。
    // 客户端按固定 data_size 顺序调用即可，offset 必须严格等于已累计字节数。
    // 同 inode 多次调用串行；不同 inode 可并发（内部有互斥保护）。
    volumemanager::ErrorCode WriteObject(const std::string& inode_id,
                                         const char* data,
                                         uint64_t data_size,
                                         uint64_t offset,
                                         uint64_t total_size);

    // 接收 MDS 下发的一批归档文件信息（对应 OpticalNodeService.SendArchiveMetadata）。
    // 仅校验并入队 archive_task_queue_，立即返回；由 archive_task_thread_ 异步逐个按
    // target_node_id/target_disk_id 从热数据节点下载文件并封装到镜像，封装/刻录结果通过
    // 两次上报（ReportFilesPackedToImage / ReportImagesBurnedToDisc）异步通知 MDS，
    // 本接口不等待这些阶段完成。
    volumemanager::ErrorCode SendArchiveMetadata(const SendArchiveMetadataRequest& request);

    // 初始化目录、构造底层组件并启动后台线程；成功后状态切到 RUNNING。
    bool Run();

    // 当前运行状态字符串（manager_status::k*）。
    std::string GetStatus() const;

    // 简化判断：等价于 GetStatus() == kRunning。
    bool IsReady() const;

    // 在状态字符串后追加最近一次失败原因，仅用于排障 / 日志。
    std::string GetStatusDetail() const;

private:
    // 分配单调递增的 task_id。
    uint64_t GenerateTaskId();

    // 取用 volume_id 前补齐 available_volume_ids：元素数少于 available_volume_id_count_ 时，
    // 循环向 MDS 申请（AllocateAvailableImageIdFromMds），直到达到该数量。
    void EnsureAvailableVolumeIds();

    // 向 MDS 申请一个可用 image_id；MDS 侧尚未实现，暂时返回 0 占位。
    uint64_t AllocateAvailableImageIdFromMds();

    // 启动 cd_manager 与后台工作线程；任一失败回滚并返回 false。
    bool StartBackgroundWorkers();

    // 规范化 root_dir_，派生七个子目录并下发到 volume_manager；
    // 构造 cd_manager_ / image_dir_manager_；失败回滚已建目录与路径下发。
    bool InitializeDir();

    // 倒序删除 InitializeDir 内部本次新建的目录；非空 / 无权限时静默跳过。
    void RollbackCreatedDirs(const std::vector<std::string>& created_dirs);

    // 请求后台线程退出并完成资源回收。
    void StopBackgroundWorkers();

    // 消费 read_task_queue_，将任务提交给 cd_manager。
    void ReadTaskProcessor();

    // cd_manager 读完成回调：镜像转移 + 触发按卷批量读。
    void OnCDReadComplete(const cd_manager_sim::ReadCompleteEvent& event);

    // cd_manager 刻录完成回调：释放对应写镜像。
    void OnCDBurnComplete(const cd_manager_sim::BurnCompleteEvent& event);

    // 把 WRTask 包装为 cd_manager 的 ReadRequest 并提交。
    bool SubmitTaskToCDManager(const WR_task::WRTask& task);

    // 把 WRTask 包装为 cd_manager 的 BurnRequest 并提交（含真实文件大小）。
    bool SubmitBurnTaskToCDManager(const WR_task::WRTask& task);

    // 消费 zip_task_queue_：READ 走缓存 / MountVolume / 异步等待；WRITE 走压缩封装。
    void ZipTaskProcessor();

    // 消费 archive_task_queue_：每批次重建节点映射，逐文件从 real_node 下载并重组为
    // input_file_dir_/<inode_id>.archive，再建 WRITE 任务入 zip_task_queue_。
    void ArchiveTaskProcessor();

    // 向 scheduler 拉取全量节点视图（GetClusterView, min_generation=0）并建立
    // node_id -> node_address 临时映射；仅保留 NODE_REAL 且健康/启用的节点。
    // 成功返回 true；失败返回 false 并写 last_failure_reason_buf_。
    bool BuildNodeAddressMap(std::unordered_map<std::string, std::string>* out);

    // 从 target_node_address 指向的 real_node 下载 file 的全部分片，按绝对偏移直接写入
    // input_file_dir_/<inode_id>.archive 形成完整文件；成功后把相对文件名写入 *out_relative_path。
    // 任一步失败都会清理半成品并返回 false。
    bool DownloadArchiveFile(const ArchiveFileInfo& file,
                             const std::string& target_node_address,
                             std::string* out_relative_path);

    // 为已落盘的归档文件建 WRITE 任务并入 zip_task_queue_；
    // 队列已关闭 / 正在停止时返回 false（调用方负责清理已下载文件）。
    bool SubmitWriteTaskForArchive(uint64_t inode_id, const std::string& relative_path);

    // 封装触发后调用：扫描 temp_dir_ 剩余 temp_*.compressed，与待打包集合 diff，
    // 并从集合移除本次已打包的 inode_id。
    void ReportPackedInodes(const std::string& volume_id);

    // 消费 burn_task_queue_，将刻录任务提交给 cd_manager。
    void BurnTaskProcessor();

    // 清理线程主体：周期性扫描 task_map_，移除 FINISH 终态任务（含 inode 索引清理）。
    void CleanupTaskProcessor();

    // 更新任务状态；终态保护（FINISH/FAILED 不再回退中间态）。
    bool MarkTaskState(uint64_t task_id, WR_task::WRTaskState new_state);

    // 置 FAILED 终态并写入细分错误码 + 上下文；终态保护；同步写入 last_failure_reason_buf_。
    bool MarkTaskFailed(uint64_t task_id,
                        volumemanager::ErrorCode code,
                        const std::string& detail = "");

    // 任务 attempt_count 是否已达上限（防止重试风暴）。
    bool ShouldGiveUpRetry(uint64_t task_id) const;

private:
    volumemanager::VolumeManager volume_manager_;
    // 等待提交 cd_manager 的读任务队列。
    WR_task::WRTaskQueue* read_task_queue_;
    // 等待压缩 / 缓存查询的任务队列。
    WR_task::WRTaskQueue* zip_task_queue_;
    // 等待归档消费线程处理的任务队列（MDS 下发的批次）。
    ArchiveRequestQueue* archive_task_queue_;
    // scheduler 服务地址，构造注入；供归档线程查询节点视图。
    std::string scheduler_addr_;
    // 出站 scheduler channel：仅归档线程访问，懒初始化；RPC 失败时 reset 以便下批重建。
    std::unique_ptr<brpc::Channel> scheduler_channel_;
    // real_node 出站 channel 缓存（key = node_address）：仅归档线程访问，懒初始化。
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> data_node_channels_;
    // 等待刻录的任务队列。
    WR_task::WRTaskQueue* burn_task_queue_;
    // 任务元数据索引（按 task_id）。
    WR_task::WRTaskMap* task_map_;

    // 可用 volume_id 的 FIFO 池；容量为 available_volume_id_count_，取用时按需向 MDS 补齐。
    std::queue<uint64_t> available_volume_ids;
    uint8_t available_volume_id_count_{0};

    // 按 volume_id 索引的挂起读任务映射（线程安全）；
    // 用于"卷镜像就绪时批量推进"以及"新读请求搭便车"判定。
    VolumeTaskMap volume_read_index_{};

    // 负责缓存未命中的底层光盘读 / 刻录调度。
    std::unique_ptr<cd_manager_sim::CDManager> cd_manager_;

    // 卷镜像目录的容量管理 + LRU 淘汰；InitializeDir 阶段构造。
    std::unique_ptr<space_manager::ImageDirManager> image_dir_manager_;

    // 后台工作线程：读 / 压缩 / 刻录 / 归档。
    std::thread read_task_thread_;
    std::thread zip_task_thread_;
    std::thread burn_task_thread_;
    std::thread archive_task_thread_;
    // 清理线程：周期性移除 task_map_ 中的 FINISH 终态任务。
    std::thread cleanup_thread_;

    // 全局停止标志。
    std::atomic<bool> stop_requested_{false};
    // 异步任务 ID 单调递增计数器。
    std::atomic<uint64_t> next_task_id_{1};

    // WriteObject 分片上传中的 inode 状态：累计字节数 + 预期总大小。
    // 累计 == total_size 时创建 WRITE 任务并从 map 移除；中途失败或客户端放弃则残留。
    struct InProgressWriteState {
        uint64_t bytes_received = 0;
        uint64_t total_size = 0;
    };
    std::unordered_map<std::string, InProgressWriteState> in_progress_writes_;
    std::mutex in_progress_writes_mutex_;

    // inode_id -> 最近一次 RequestAsyncReadFile 创建的 READ 任务 id。
    // 用于 ReadObjectByInodeId 反查；同 inode 多次请求只保留最新 task_id。
    // 末片读取成功后由 ReadObjectByTaskId 清理。
    std::unordered_map<std::string, uint64_t> inode_to_read_task_id_;
    std::mutex inode_to_read_task_id_mutex_;

    // 待打包 inode_id 集合：ZipTaskProcessor 在每次 AddFileToCollect 前登记本次 inode，
    // 封装触发后扫描 temp_dir_ 剩余 temp_*.compressed，diff 出本次被打包的 inode_id 并移除。
    // 仅 ZipTaskProcessor 单线程访问，仍加锁以满足线程安全要求。
    std::unordered_set<uint64_t> pending_pack_inode_ids_;
    std::mutex pending_pack_inode_ids_mutex_;

    // InitializeDir 派生的七个子目录路径。
    std::string root_dir_;
    std::string input_file_dir_;
    std::string temp_dir_;
    std::string image_dir_;
    std::string read_dir_;
    std::string disc_sim_dir_;
    // 持久化元数据目录：用于存放本节点需要跨重启保留的信息。
    std::string meta_dir_;
    // 任务日志目录：用于存放已完成 / 进行中任务等信息。
    std::string log_dir_;

    // 构造函数给出的 image_dir 镜像数上限，InitializeDir 时下发给 image_dir_manager_。
    uint64_t capacity_in_images_{0};

    // 原子地更新 status_ 与 last_status_reason_；提供稳定的字符串生命周期。
    void SetStatus(const std::string& new_status, const std::string& reason = std::string());

    // 当前运行状态字符串（manager_status::k*）。
    mutable std::atomic<const char*> status_{manager_status::kUninitialized};
    // 进入当前状态的最近原因。
    mutable std::atomic<const char*> last_status_reason_{""};

    // 持有 SetStatus 写入的字符串，确保发布的指针不会悬空。
    mutable std::string last_failure_reason_buf_;
    mutable std::string status_buf_;

    // 副作用失败（清理残留 unlink 失败等），与主错分流。
    mutable std::string last_sidecar_failure_reason_buf_;

    // 单任务重试上限；超限后由 ShouldGiveUpRetry 强制置 FAILED。
    static constexpr uint32_t attempt_count_max_ = 32;

    // 清理线程扫描间隔：每 10 分钟移除一次 task_map_ 中的 FINISH 任务。
    static constexpr std::chrono::seconds cleanup_interval_ = std::chrono::seconds(600);
};

}  // namespace optical_node_manager
