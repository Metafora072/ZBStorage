#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <optional>
#include <queue>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <cd_manager_sim/cd_manager.h>
#include <optical_node_manager_structs.h>
#include <space_manager/space_manager.h>
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
    // disc_capacity_bytes / standard_images_per_disc / disc_block_size_bytes 为光盘有关参数
    // （单张光盘容量上限、标准镜像数基线、数据块对齐大小）。
    // max_write_images 为下载侧背压阈值：image_dir 中允许同时存在的写镜像数上限，
    // 超过即暂停下载原始文件（须大于单盘可容纳的镜像数，否则无法封盘）。
    // scheduler_addr 为 scheduler 服务地址，供归档线程查询全量节点视图（node_id→address）。
    OpticalNodeManager(uint64_t volume_size,
                       double size_threshold,
                       const std::string& root_dir,
                       uint64_t capacity_in_images,
                       uint8_t available_volume_id_count,
                       uint64_t disc_capacity_bytes,
                       uint32_t standard_images_per_disc,
                       uint64_t disc_block_size_bytes,
                       uint32_t max_write_images,
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
    // 循环本地生成（GenerateVolumeId），直到达到该数量。
    void EnsureAvailableVolumeIds();

    // 本地递增生成一个可用 volume_id（对应 MDS 的 image_id）。
    // MDS 的 AllocateAvailableImageId 尚未实现，暂由节点自行分配作为替代；
    // 本地序号与 MDS 分配的 image_id 数值空间不做隔离，接入 MDS 后需整体替换。
    uint64_t GenerateVolumeId();

    // 本地递增生成一个 disk_id（对应 MDS 的 disc_id）。
    // MDS 的 AllocateAvailableDiscId 尚未实现，且光盘库刻录细节未完善，
    // 该值当前仅作为光盘级刻录任务（DISC_BURN）与超级块的占位标识，
    // 不参与实际调度语义。任务中的 disk_id 字段取其十进制字符串形式。
    uint64_t GenerateDiskId();

    // 启动 cd_manager 与后台工作线程；任一失败回滚并返回 false。
    bool StartBackgroundWorkers();

    // 规范化 root_dir_，派生八个子目录并下发到 volume_manager；
    // 构造 cd_manager_ / space_manager_；失败回滚已建目录与路径下发。
    bool InitializeDir();

    // 倒序删除 InitializeDir 内部本次新建的目录；非空 / 无权限时静默跳过。
    void RollbackCreatedDirs(const std::vector<std::string>& created_dirs);

    // 从 meta/node_discs_meta 重建「镜像 → 光盘」定位索引 disc_image_index_：
    // 按「32B 块头 + N×128B 记录」顺序扫描追加块，恢复 volume_id → vdisc 偏移。
    // 文件不存在（首次启动）视为空索引；尾部截断 / 块头损坏时保留已解析部分，
    // 只写 sidecar 原因，不阻塞启动。
    void RebuildDiscImageIndex();

    // 请求后台线程退出并完成资源回收。
    void StopBackgroundWorkers();

    // 消费 cd_read_task_queue_，把 CD_READ 任务提交给 cd_manager。
    void CDReadTaskProcessor();

    // 构造一个 CD_READ 任务（光盘库读请求）并登记入 task_map_ / cd_read_task_queue_；
    // 仅 ZipTaskProcessor 调用（单线程），无需加锁。
    void EnqueueCDReadTask(const std::string& disk_id, const std::string& volume_id);

    // image_dir_ 下卷镜像的绝对路径：image_dir_ + "volume_<volume_id>.vimg"。
    std::string VolumeImagePath(const std::string& volume_id) const;

    // 把 volume_id 对应读镜像的 LRU 访问时间推前（命中后推迟淘汰）。
    // 解析失败 / 未登记时静默跳过（best-effort）；调用方需已持有 space_manager_ 的读锁。
    void TouchVolume(const std::string& volume_id);

    // cd_manager 读完成回调（对应 CD_READ 任务）：镜像转移 + 触发按卷批量读。
    void OnCDReadComplete(const cd_manager_sim::ReadCompleteEvent& event);

    // cd_manager 刻录完成回调：释放对应写镜像。
    void OnCDBurnComplete(const cd_manager_sim::BurnCompleteEvent& event);

    // 把 CD_READ 任务包装为 cd_manager 的 ReadRequest 并提交。
    bool SubmitCDReadTaskToCDManager(const WR_task::WRTask& task);

    // 把 WRTask 包装为 cd_manager 的 BurnRequest 并提交（含真实文件大小）。
    bool SubmitCDBurnTaskToCDManager(const WR_task::WRTask& task);

    // 消费 zip_task_queue_：READ 走缓存 / MountVolume / 异步等待；WRITE 走压缩封装。
    void ZipTaskProcessor();

    // 消费 archive_task_queue_：每批次重建节点映射，逐文件从 real_node 下载并重组为
    // input_file_dir_/<inode_id>.archive，再建 WRITE 任务入 zip_task_queue_。
    void ArchiveTaskProcessor();

    // 采集一次归档下载背压判定快照：写镜像占用（SpaceManager，内部加读锁）+
    // input/ 未压缩原始文件总量（opendir + stat 粗略估算，不加锁）。
    // 纯查询、无副作用；由归档下载批次在开始前调用，决定是否暂停。
    ArchiveBackpressureState GetArchiveBackpressureState() const;

    // 估算 input_file_dir_ 下未压缩原始文件的总字节数。
    // 粗略估算：并发写入期间 readdir 可能漏项，用于背压判断足够；目录不可读时返回 0。
    uint64_t EstimateInputPendingBytes() const;

    // 归档下载节流：若「本轮配额用尽」或「负载过载」则阻塞，直到被 Zip 压缩完成信号 /
    // Stop 唤醒；唤醒后开启新一轮（配额清零）并从断点继续。
    // 返回 false 表示应当停止（stop_requested_ 或归档队列已关闭）。
    // 仅 ArchiveTaskProcessor 单线程调用。
    bool WaitIfArchiveDownloadThrottled();

    // Zip 每次压缩完成（原始文件已被消化）后递增并唤醒归档线程，作为「流水线前进一步」的信号；
    // 由 ZipTaskProcessor 写入、ArchiveTaskProcessor 读取，访问需持有 archive_backpressure_mutex_。
    void NotifyArchiveCompressionProgress();

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
    // 从集合移除本次已打包的 inode_id，并把 volume_id → inode_ids 对应关系打印到控制台
    // （MDS 的 ReportFilesPackedToImage 上报接口未完善前的替代）。
    void ReportPackedInodes(const std::string& volume_id);

    // 第二次上报（镜像 → 光盘）：把本盘包含的全部镜像上报给 MDS。
    // MDS 的 ReportImagesBurnedToDisc 上报接口未完善前，先以控制台输出替代
    // （格式与 proto 字段对齐：disc_id + image_ids）。
    void ReportImagesBurnedToDisc(const std::string& disc_id,
                                  const std::vector<DiscImageEntry>& entries);

    // 消费 cd_burn_task_queue_，将刻录任务提交给 cd_manager。
    void CDBurnTaskProcessor();

    // 把已登记到 image_dir_ 的写镜像纳入待打包光盘：
    //   1) 计算 vimg 实大小与 SHA-256，构造 DiscImageEntry；
    //   2) 若加入后超出光盘容量，先把当前待打包集合封印为一张光盘
    //      （见 SealPendingDiscAndSubmitBurn）；
    //   3) 追加进 pending_disc_entries_，按光盘布局回填 aligned_size_bytes /
    //      offset_in_disc，并原子重写 meta/pending_disc_meta。
    // 仅 ZipTaskProcessor 调用（单线程）；失败返回对应错误码。
    volumemanager::ErrorCode AccumulatePackedImage(const std::string& image_path,
                                                   uint64_t volume_id);

    // 把 pending_disc_entries_ 按「元数据区头 + 定长记录」编码后写入
    // meta/pending_disc_meta；先写 .tmp 再 rename，避免中断后留下半截元数据。
    volumemanager::ErrorCode PersistPendingDiscMeta();

    // 把当前待打包集合封印为一张光盘：分配 disk_id、定稿该盘元数据（按布局回填
    // 偏移）并改名为 meta/disc_<disk_id>_meta，随后提交光盘级刻录任务（DISC_BURN）；
    // 成功后清空 pending_disc_entries_。
    // 注意：光盘文件（vdisc）此时**不**生成——镜像数据在刻录完成回调
    // OnCDBurnComplete 中逐个写入 vdisc 并逐个释放，见 WriteVdiscAndReleaseImages。
    // 仅 ZipTaskProcessor 调用（单线程）。
    volumemanager::ErrorCode SealPendingDiscAndSubmitBurn();

    // 把 [超级块][元数据区][镜像数据区] 拼接为一个光盘文件，边写边释放：
    // 每写入一个镜像后立即调用 RemoveWriteImage 删除 image_dir_ 内的 vimg 副本。
    // 镜像按 entries 顺序从 image_dir_ 逐个读取；先写 .tmp 再 rename。
    // 由 OnCDBurnComplete（刻录完成）调用。
    bool WriteVdiscAndReleaseImages(const std::string& vdisc_path,
                                    const std::vector<DiscImageEntry>& entries,
                                    uint64_t disc_id);

    // 把一张光盘的元数据块（DiscIndexHeader + 镜像记录）以追加方式写入
    // meta/node_discs_meta：记录该盘全部镜像的偏移 / 大小 / SHA-256，供读回时定位。
    // 刻录完成后由 OnCDBurnComplete 调用。
    bool AppendToNodeDiscsMeta(uint64_t disc_id, const std::vector<DiscImageEntry>& entries);

    // 清理线程主体：周期性扫描 task_map_，移除 FINISH 终态任务（含 inode 索引清理）。
    void CleanupTaskProcessor();

    // 把即将被清理的终态任务摘要追加写入 log_dir_/task_done_<yyyymmdd>.log；
    // 同一天追加，文件不存在时新建。写入失败仅记录 sidecar 原因，不影响清理主流程。
    void AppendTaskDoneLog(const WR_task::WRTask& task);

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
    // 等待提交 cd_manager 的光盘库读（镜像加载）任务队列，仅承载 CD_READ。
    WR_task::WRTaskQueue* cd_read_task_queue_;
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
    // 等待提交 cd_manager 的刻录任务队列，仅承载 CD_BURN。
    WR_task::WRTaskQueue* cd_burn_task_queue_;
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
    std::unique_ptr<space_manager::SpaceManager> space_manager_;

    // 后台工作线程：光盘库读 / 压缩 / 刻录 / 归档。
    std::thread cd_read_task_thread_;
    std::thread zip_task_thread_;
    std::thread cd_burn_task_thread_;
    std::thread archive_task_thread_;
    // 清理线程：周期性移除 task_map_ 中的 FINISH 终态任务。
    std::thread cleanup_thread_;

    // 全局停止标志。
    std::atomic<bool> stop_requested_{false};
    // 异步任务 ID 单调递增计数器。
    std::atomic<uint64_t> next_task_id_{1};
    // 本地 volume_id（对应 MDS image_id）单调递增计数器。
    std::atomic<uint64_t> next_volume_id_{1};
    // 本地 disk_id（对应 MDS disc_id）单调递增计数器。
    std::atomic<uint64_t> next_disk_id_{1};

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

    // 待打包光盘的镜像元数据：按登记顺序积攒，达到光盘容量后由
    // SealPendingDiscAndSubmitBurn 封印为一张光盘。
    // 仅 ZipTaskProcessor 单线程访问，仍加锁以满足线程安全要求。
    std::vector<DiscImageEntry> pending_disc_entries_;
    std::mutex pending_disc_mutex_;

    // node_discs_meta 的内存索引：volume_id → 镜像在已刻录光盘中的位置。
    // 读回时据此定位 vdisc 文件与复制区间；由 OnCDBurnComplete 在追加元数据后同步。
    std::unordered_map<uint64_t, DiscImageLocation> disc_image_index_;
    std::mutex disc_image_index_mutex_;

    // InitializeDir 派生的八个子目录路径。
    std::string root_dir_;
    std::string input_file_dir_;
    std::string temp_dir_;
    std::string image_dir_;
    std::string read_dir_;
    std::string disc_sim_dir_;
    std::string meta_dir_;
    std::string log_dir_;
    std::string write_buffer_dir_;

    // 构造函数给出的 image_dir 镜像数上限，InitializeDir 时下发给 space_manager_。
    uint64_t capacity_in_images_{0};

    // 构造函数给出的光盘打包参数：单张光盘容量上限（字节）、单张光盘标准镜像数基线、
    // 光盘数据块对齐大小（字节）。standard_images_per_disc 仅作元数据区预留基线，
    // 不作硬上限；实际容纳镜像数以 disc_capacity_bytes 为准。
    uint64_t disc_capacity_bytes_{0};
    uint32_t standard_images_per_disc_{0};
    uint64_t disc_block_size_bytes_{0};

    // 下载侧背压阈值：image_dir 中允许同时存在的写镜像数上限（含「已封印、等刻录释放」的）。
    // 由归档下载批次在开始前检查，超过即暂停下载，等 Zip 压缩 / 刻录释放降低占用。
    uint32_t max_write_images_{1};

    // 归档下载背压：Archive 线程在「本轮下载量达 volume_size」或「负载过载」时在此 CV 等待，
    // 由 Zip 每次压缩完成唤醒、Stop 时唤醒全部。
    std::mutex archive_backpressure_mutex_;
    std::condition_variable archive_backpressure_cv_;
    // 「流水线前进一步」的信号计数：Zip 每完成一次压缩递增。
    // Zip 写、Archive 读，均在 archive_backpressure_mutex_ 保护下访问。
    uint64_t zip_compress_completed_seq_{0};

    // 以下五项仅 ArchiveTaskProcessor 单线程访问，无需加锁：
    // 本轮（自上次唤醒以来）已下载的原始字节数（跨 MDS 批次累计，达 volume_size 即阻塞）。
    uint64_t archive_round_downloaded_bytes_{0};
    // 本轮已成功提交到压缩队列的文件数 + 本轮起点时的压缩进度；
    // 两者配合用于配额判定：等本轮提交的文件都被压缩完即放行（已满足则不空睡）。
    uint64_t archive_round_submitted_tasks_{0};
    uint64_t archive_round_start_seq_{0};
    // 下载断点：当前未下完的 MDS 请求与其下一个待下载文件下标；阻塞时不丢请求。
    std::optional<SendArchiveMetadataRequest> pending_archive_request_;
    size_t pending_archive_file_index_{0};

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
