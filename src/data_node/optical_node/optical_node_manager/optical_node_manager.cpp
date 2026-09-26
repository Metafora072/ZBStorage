#include "optical_node_manager.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <system_error>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <optical_node_manager_structs.h>

#include <brpc/channel.h>
#include "real_node.pb.h"
#include "scheduler.pb.h"

namespace optical_node_manager {
    volumemanager::ErrorCode ReadFileToStringMmap(const std::string& file_path,
                                                  std::string* out,
                                                  uint64_t offset,
                                                  uint64_t read_size,
                                                  bool* out_is_last);

    // 循环写直到写满 size 字节；处理短写与 EINTR。失败返回 false。
    static bool PwriteAll(int fd, const char* data, size_t size, uint64_t offset) {
        size_t written = 0;
        while (written < size) {
            const ssize_t n = ::pwrite(fd,
                                       data + written,
                                       size - written,
                                       static_cast<off_t>(offset + written));
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (n == 0) {
                return false;
            }
            written += static_cast<size_t>(n);
        }
        return true;
    }

    // 把 epoch 毫秒时间戳格式化为本地时间字符串；localtime_r / strftime 失败时返回空串。
    static std::string FormatLocalTimeMs(uint64_t epoch_ms, const char* fmt) {
        const std::time_t t = static_cast<std::time_t>(epoch_ms / 1000);
        std::tm tm_buf{};
        if (::localtime_r(&t, &tm_buf) == nullptr) {
            return std::string();
        }
        char buf[32] = {0};
        if (std::strftime(buf, sizeof(buf), fmt, &tm_buf) == 0) {
            return std::string();
        }
        return std::string(buf);
    }

    // 统一的排障详情串："phase=<phase> subphase=<subphase>"，extra 非空时追加 " <extra>"。
    // 约定 extra 由调用方拼成 "k=v k2=v2" 形式，避免各处自行拼 "phase= ... subphase= ..."
    // 造成格式漂移（last_failure_reason_buf_ / last_sidecar_failure_reason_buf_ /
    // MarkTaskFailed 的 detail 都走这里）。
    static std::string FormatFailureDetail(const char* phase,
                                  const std::string& subphase,
                                  const std::string& extra = std::string()) {
        std::string detail;
        detail.reserve(64 + extra.size());
        detail.append("phase=").append(phase);
        detail.append(" subphase=").append(subphase);
        if (!extra.empty()) {
            detail.push_back(' ');
            detail.append(extra);
        }
        return detail;
    }

    // 从 node_id -> node_address 映射解析地址；支持虚节点 id 的 "<base>-v<index>" 回退。
    static bool ResolveNodeAddressFromMap(
        const std::unordered_map<std::string, std::string>& address_map,
        const std::string& node_id,
        std::string* out_address) {
        if (out_address == nullptr) {
            return false;
        }
        auto it = address_map.find(node_id);
        if (it != address_map.end() && !it->second.empty()) {
            *out_address = it->second;
            return true;
        }
        // 虚节点回退：形如 "<base>-v<index>"，去掉后缀再查一次。
        const std::string marker = "-v";
        const size_t pos = node_id.rfind(marker);
        if (pos != std::string::npos && pos > 0) {
            it = address_map.find(node_id.substr(0, pos));
            if (it != address_map.end() && !it->second.empty()) {
                *out_address = it->second;
                return true;
            }
        }
        return false;
    }

    OpticalNodeManager::OpticalNodeManager(uint64_t volume_size,
                                           double size_threshold,
                                           const std::string& root_dir,
                                           uint64_t capacity_in_images,
                                           uint8_t available_volume_id_count,
                                           uint64_t disc_capacity_bytes,
                                           uint32_t standard_images_per_disc,
                                           const std::string& scheduler_addr)
        : volume_manager_(volume_size, size_threshold),
          cd_read_task_queue_(new WR_task::WRTaskQueue()),
          zip_task_queue_(new WR_task::WRTaskQueue()),
          archive_task_queue_(new ArchiveRequestQueue()),
          scheduler_addr_(scheduler_addr),
          cd_burn_task_queue_(new WR_task::WRTaskQueue()),
          task_map_(new WR_task::WRTaskMap()),
          available_volume_ids() {
        root_dir_ = root_dir;
        capacity_in_images_ = capacity_in_images;
        available_volume_id_count_ = available_volume_id_count;
        disc_capacity_bytes_ = disc_capacity_bytes;
        standard_images_per_disc_ = standard_images_per_disc;
    }

    // 把字符串先拷到成员变量再发布指针，避免栈上临时 string 析构导致悬空。
    void OpticalNodeManager::SetStatus(const std::string& new_status, const std::string& reason) {
        status_buf_ = new_status;
        status_.store(status_buf_.c_str(), std::memory_order_release);

        if (reason.empty()) {
            last_status_reason_.store("", std::memory_order_release);
        } else {
            last_failure_reason_buf_ = reason;
            last_status_reason_.store(last_failure_reason_buf_.c_str(), std::memory_order_release);
        }
    }

    std::string OpticalNodeManager::GetStatus() const {
        const char* current = status_.load(std::memory_order_acquire);
        return current != nullptr ? std::string(current) : std::string(manager_status::kUninitialized);
    }

    bool OpticalNodeManager::IsReady() const {
        const char* current = status_.load(std::memory_order_acquire);
        return current != nullptr && std::string(current) == manager_status::kRunning;
    }

    std::string OpticalNodeManager::GetStatusDetail() const {
        const char* current = status_.load(std::memory_order_acquire);
        const char* reason  = last_status_reason_.load(std::memory_order_acquire);

        std::string detail = (current != nullptr) ? current : manager_status::kUninitialized;
        if (reason != nullptr && reason[0] != '\0') {
            detail += " (";
            detail += reason;
            detail += ")";
        }
        // 追加副作用失败上下文（清理残留失败等），与主错分流。
        if (!last_sidecar_failure_reason_buf_.empty()) {
            detail += " sidecar=(";
            detail += last_sidecar_failure_reason_buf_;
            detail += ")";
        }
        return detail;
    }

    // 接收 MDS 下发的归档批次：仅校验并入队 archive_task_queue_，由 archive_task_thread_
    // 异步消费（下载 → 建 WRITE 任务）。本函数不等待消费结果，入队成功即返回 SUCCESS。
    volumemanager::ErrorCode OpticalNodeManager::SendArchiveMetadata(
        const SendArchiveMetadataRequest& request) {
        if (archive_task_queue_ == nullptr) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kSendArchiveMetadata, "queue_null");
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受归档请求；其它状态下消费线程未运行，入队会无人消费。
        if (GetStatus() != manager_status::kRunning) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kSendArchiveMetadata, "not_ready",
                "status=" + GetStatus());
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        if (request.files.empty()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kSendArchiveMetadata, "empty_files",
                "batch_id=" + std::to_string(request.batch_id));
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 队列已关闭（Stop 进行中）：Push 会被静默丢弃，直接拒绝而不是假装成功。
        if (archive_task_queue_->IsClosed()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kSendArchiveMetadata, "queue_closed",
                "batch_id=" + std::to_string(request.batch_id));
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        archive_task_queue_->Push(request);
        last_failure_reason_buf_.clear();
        return volumemanager::ErrorCode::SUCCESS;
    }

    bool OpticalNodeManager::Run() {
        // 状态机守卫：RUNNING 幂等成功；INITIALIZING / STOPPING 重入拒绝。
        const std::string current_status = GetStatus();
        if (current_status == manager_status::kRunning) {
            // 经 SetStatus 发布原因指针；直接改 last_failure_reason_buf_ 会让
            // 已发布的 last_status_reason_ 指向重新分配前的旧缓冲。
            SetStatus(manager_status::kRunning,
                      FormatFailureDetail(start_failure_phase::kRunGuard, "idempotent_already_running",
                          "status=" + current_status));
            return true;
        }
        if (current_status == manager_status::kInitializing ||
            current_status == manager_status::kStopping) {
            std::string status_lower = current_status;
            for (auto& c : status_lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            // 同上：重入拒绝的原因也要经 SetStatus 发布。
            SetStatus(current_status,
                      FormatFailureDetail(start_failure_phase::kRunGuard, "reentry_during_" + status_lower,
                          "status=" + current_status));
            return false;
        }
        SetStatus(manager_status::kInitializing, "Run() entered");

        if (!InitializeDir()) {
            // InitializeDir 已写子阶段到 last_failure_reason_buf_，这里拼顶层 phase。
            const std::string detailed = FormatFailureDetail(start_failure_phase::kInitializeDir, last_failure_reason_buf_);
            SetStatus(manager_status::kStartFailed, detailed);
            // 收尾：StopBackgroundWorkers 负责 joinable / cd_manager_ 守门，
            // 并保持 kStartFailed 与本次失败原因不被覆盖。
            StopBackgroundWorkers();
            return false;
        }
        if (!StartBackgroundWorkers()) {
            // StartBackgroundWorkers 已写完整结构化 KV，直接转发给 SetStatus。
            SetStatus(manager_status::kStartFailed, last_failure_reason_buf_);
            StopBackgroundWorkers();
            return false;
        }
        SetStatus(manager_status::kRunning, "Run() succeeded");
        return true;
    }

    bool OpticalNodeManager::InitializeDir() {
        if (root_dir_.empty()) {
            last_failure_reason_buf_ = "root_dir_empty";
            return false;
        }

        // 规范化 root_dir_ 并派生八个子目录。
        if (root_dir_.back() != '/') {
            root_dir_ += '/';
        }

        input_file_dir_   = root_dir_ + "input/";
        temp_dir_         = root_dir_ + "temp/";
        image_dir_        = root_dir_ + "image/";
        read_dir_         = root_dir_ + "read/";
        disc_sim_dir_     = root_dir_ + "disc_sim/";
        meta_dir_         = root_dir_ + "meta/";
        log_dir_          = root_dir_ + "log/";
        write_buffer_dir_ = root_dir_ + "write_buffer/";

        const std::string dirs[] = {
            root_dir_,
            input_file_dir_,
            temp_dir_,
            image_dir_,
            read_dir_,
            disc_sim_dir_,
            meta_dir_,
            log_dir_,
            write_buffer_dir_,
        };

        // 记录本次新创建的目录，失败回滚时只删这些。
        std::vector<std::string> created_dirs;
        created_dirs.reserve(sizeof(dirs) / sizeof(dirs[0]));

        for (const auto& dir : dirs) {
            if (dir.empty()) {
                continue;
            }
            struct stat dir_stat{};
            if (::stat(dir.c_str(), &dir_stat) == 0) {
                if (S_ISDIR(dir_stat.st_mode)) {
                    continue;
                }
                last_failure_reason_buf_ = std::string("dir_not_directory path=")
                    + dir + " errno=" + std::to_string(errno);
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
                last_failure_reason_buf_ = std::string("mkdir_failed path=")
                    + dir + " errno=" + std::to_string(errno);
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            created_dirs.push_back(dir);
        }

        // 下发子目录到 volume_manager；失败回滚路径与已建目录。
        const std::string saved_base_path        = volume_manager_.base_path_;
        const std::string saved_temp_dir         = volume_manager_.temp_dir_;
        const std::string saved_image_dir        = volume_manager_.image_dir_;
        const std::string saved_read_cache_dir   = volume_manager_.read_cache_dir_;

        auto rollback_volume_manager = [&]() {
            (void)volume_manager_.SetBasePath(saved_base_path);
            (void)volume_manager_.SetTempDir(saved_temp_dir);
            (void)volume_manager_.SetImageDir(saved_image_dir);
            (void)volume_manager_.SetReadCacheDir(saved_read_cache_dir);
        };

        if (volume_manager_.SetBasePath(input_file_dir_) != volumemanager::ErrorCode::SUCCESS) {
            last_failure_reason_buf_ = "SetBasePath_failed path=" + input_file_dir_;
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }
        if (volume_manager_.SetTempDir(temp_dir_) != volumemanager::ErrorCode::SUCCESS) {
            last_failure_reason_buf_ = "SetTempDir_failed path=" + temp_dir_;
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }
        if (volume_manager_.SetImageDir(image_dir_) != volumemanager::ErrorCode::SUCCESS) {
            last_failure_reason_buf_ = "SetImageDir_failed path=" + image_dir_;
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }
        if (volume_manager_.SetReadCacheDir(read_dir_) != volumemanager::ErrorCode::SUCCESS) {
            last_failure_reason_buf_ = "SetReadCacheDir_failed path=" + read_dir_;
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }

        // 每次 InitializeDir 入口强制重建 cd_manager_ / space_manager_：
        // 旧对象（若存在）先 Stop / reset，注入本轮 disc_sim_dir_。
        if (cd_manager_ != nullptr) {
            cd_manager_->Stop();
            cd_manager_.reset();
        }

        cd_manager_sim::CDManagerConfig config{};
        config.workspace_root = disc_sim_dir_;
        try {
            cd_manager_ = std::make_unique<cd_manager_sim::CDManager>(config);
        } catch (const std::exception& e) {
            last_failure_reason_buf_ = std::string("CDManager_ctor_failed what=") + e.what();
            cd_manager_.reset();
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        } catch (...) {
            last_failure_reason_buf_ = "CDManager_ctor_failed what=<unknown>";
            cd_manager_.reset();
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }

        if (space_manager_ != nullptr) {
            space_manager_.reset();
        }
        try {
            space_manager_ = std::make_unique<space_manager::SpaceManager>(image_dir_);
            const volumemanager::ErrorCode rebuild_ret =
                space_manager_->RebuildManagementTable();
            if (rebuild_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("SpaceManager_Rebuild_failed code=")
                    + std::to_string(static_cast<int>(rebuild_ret));
                space_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            const volumemanager::ErrorCode cap_ret =
                space_manager_->SetCapacityInImages(capacity_in_images_);
            if (cap_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("SpaceManager_SetCapacity_failed code=")
                    + std::to_string(static_cast<int>(cap_ret))
                    + " capacity=" + std::to_string(capacity_in_images_);
                space_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            // 注入模拟光盘库目录：Release / LRU 换出 / 刻录释放时 rename 到此处。
            const volumemanager::ErrorCode disc_sim_ret =
                space_manager_->SetDiscSimDir(disc_sim_dir_);
            if (disc_sim_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("SpaceManager_SetDiscSimDir_failed code=")
                    + std::to_string(static_cast<int>(disc_sim_ret))
                    + " disc_sim_dir=" + disc_sim_dir_;
                space_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
        } catch (const std::exception& e) {
            last_failure_reason_buf_ = std::string("SpaceManager_init_failed what=") + e.what();
            space_manager_.reset();
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        } catch (...) {
            last_failure_reason_buf_ = "SpaceManager_init_failed what=<unknown>";
            space_manager_.reset();
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        }

        last_failure_reason_buf_.clear();
        return true;
    }

    // 倒序删除本次 InitializeDir 新建的目录；非空 / 无权限时静默跳过。
    void OpticalNodeManager::RollbackCreatedDirs(const std::vector<std::string>& created_dirs) {
        for (auto it = created_dirs.rbegin(); it != created_dirs.rend(); ++it) {
            if (it->empty()) {
                continue;
            }
            if (::rmdir(it->c_str()) != 0) {
                // 非空 / 权限不足 / 被占用：留给上层或下次 Run() 复用。
            }
        }
    }

    bool OpticalNodeManager::StartBackgroundWorkers() {
        if (cd_manager_ == nullptr) {
            // 防御性兜底：InitializeDir 实际未成功但 Run() 仍走到这里。
            // 收尾由 Run() 统一调 StopBackgroundWorkers() 完成。
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, "cd_manager_null");
            return false;
        }

        stop_requested_.store(false, std::memory_order_relaxed);

        cd_manager_->SetReadCompleteCallback(
            [this](const cd_manager_sim::ReadCompleteEvent& event) {
                OnCDReadComplete(event);
            });

        cd_manager_->SetBurnCompleteCallback(
            [this](const cd_manager_sim::BurnCompleteEvent& event) {
                OnCDBurnComplete(event);
            });

        // cd_manager_->Start() 在所有 thread 创建之前调用；失败路径收尾由 Run() 统一 Stop。
        cd_manager_->Start();

        if (!cd_read_task_thread_.joinable()) {
            try {
                cd_read_task_thread_ = std::thread(&OpticalNodeManager::CDReadTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, start_failure_phase::kThreadRead,
                    std::string("what=") + e.what());
                return false;
            }
        }

        if (!zip_task_thread_.joinable()) {
            try {
                zip_task_thread_ = std::thread(&OpticalNodeManager::ZipTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, start_failure_phase::kThreadZip,
                    std::string("what=") + e.what());
                return false;
            }
        }

        if (!archive_task_thread_.joinable()) {
            try {
                archive_task_thread_ = std::thread(&OpticalNodeManager::ArchiveTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, start_failure_phase::kThreadArchive,
                    std::string("what=") + e.what());
                return false;
            }
        }

        if (!cd_burn_task_thread_.joinable()) {
            try {
                cd_burn_task_thread_ = std::thread(&OpticalNodeManager::CDBurnTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, start_failure_phase::kThreadBurn,
                    std::string("what=") + e.what());
                return false;
            }
        }

        if (!cleanup_thread_.joinable()) {
            try {
                cleanup_thread_ = std::thread(&OpticalNodeManager::CleanupTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kStartWorkers, "StartBackgroundWorkers.thread=cleanup",
                    std::string("what=") + e.what());
                return false;
            }
        }

        last_failure_reason_buf_.clear();
        return true;
    }

    void OpticalNodeManager::StopBackgroundWorkers() {
        stop_requested_.store(true, std::memory_order_relaxed);

        const std::string current_status = GetStatus();
        // 启动失败已通过 SetStatus(kStartFailed, 详细原因) 发布；收尾时保持该状态与原因，
        // 否则会被 kStopping / kStopped 覆盖，导致上层拿不到失败根因。
        const bool keep_start_failure = (current_status == manager_status::kStartFailed);
        if (!keep_start_failure && current_status != manager_status::kStopped) {
            SetStatus(manager_status::kStopping, "StopBackgroundWorkers entered");
        }

        // 归档消费线程是 zip_task_queue_ 的生产者，必须先于 zip_task_queue_->Close() 停止并
        // join：否则正在处理的批次会把 WRITE 任务 Push 进已关闭队列（静默丢弃），
        // 造成 task_map_ 中残留永不被消费的任务 + 已落盘文件残留。
        if (archive_task_queue_ != nullptr) {
            archive_task_queue_->Close();
        }

        if (archive_task_thread_.joinable()) {
            archive_task_thread_.join();
        }

        if (cd_read_task_queue_ != nullptr) {
            cd_read_task_queue_->Close();
        }

        if (cd_read_task_thread_.joinable()) {
            cd_read_task_thread_.join();
        }

        // 必须在 cd_manager_->Stop() 之前关闭 zip_task_queue_，OnCDReadComplete 仍在写入。
        if (zip_task_queue_ != nullptr) {
            zip_task_queue_->Close();
        }

        if (zip_task_thread_.joinable()) {
            zip_task_thread_.join();
        }

        if (cd_burn_task_queue_ != nullptr) {
            cd_burn_task_queue_->Close();
        }

        if (cd_burn_task_thread_.joinable()) {
            cd_burn_task_thread_.join();
        }

        // 清理线程仅依赖 stop_requested_ 退出，不访问队列 / cd_manager。
        if (cleanup_thread_.joinable()) {
            cleanup_thread_.join();
        }

        if (cd_manager_ != nullptr) {
            cd_manager_->Stop();
        }

        if (!keep_start_failure) {
            SetStatus(manager_status::kStopped, "StopBackgroundWorkers completed");
        }
    }

    OpticalNodeManager::~OpticalNodeManager() {
        StopBackgroundWorkers();

        if (cd_manager_ != nullptr) {
            cd_manager_.reset();
        }

        // 清理 volume_read_index_ 残留 mapping，避免外部观察到指向已 Stop task 的引用。
        volume_read_index_.Clear();

        delete cd_read_task_queue_;
        delete zip_task_queue_;
        delete archive_task_queue_;
        delete cd_burn_task_queue_;
        delete task_map_;
    }

    uint64_t OpticalNodeManager::GenerateTaskId() {
        return next_task_id_.fetch_add(1, std::memory_order_relaxed);
    }

    // 取用 volume_id 前补齐 available_volume_ids 到 available_volume_id_count_ 个。
    // 仅 ZipTaskProcessor 调用，单线程访问队列，无需加锁。
    void OpticalNodeManager::EnsureAvailableVolumeIds() {
        while (available_volume_ids.size() <
               static_cast<size_t>(available_volume_id_count_)) {
            available_volume_ids.push(GenerateVolumeId());
        }
    }

    // 本地递增生成 volume_id：MDS 的 AllocateAvailableImageId 尚未实现，先由节点自行分配。
    // TODO: 接入 MDS 后改为向 MDS 申请全局唯一 image_id。
    uint64_t OpticalNodeManager::GenerateVolumeId() {
        return next_volume_id_.fetch_add(1, std::memory_order_relaxed);
    }

    // 本地递增生成 disk_id：MDS 的 AllocateAvailableDiscId 尚未实现，先由节点自行分配。
    // TODO: 光盘库刻录细节完善 / 接入 MDS 后，改为向 MDS 申请全局唯一 disc_id。
    std::string OpticalNodeManager::GenerateDiskId() {
        return std::to_string(next_disk_id_.fetch_add(1, std::memory_order_relaxed));
    }

    volumemanager::ErrorCode OpticalNodeManager::RequestAsyncReadFile(const std::string& disk_id,
                                                          const std::string& volume_id,
                                                          const std::string& inode_id,
                                                          uint64_t* task_id) {
        if(!task_id){
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受读任务；其它状态返回 NOT_READY。
        if (GetStatus() != manager_status::kRunning) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kReadFile, "not_ready",
                "status=" + GetStatus());
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        *task_id = 0;
        const uint64_t generated_task_id = GenerateTaskId();

        WR_task::WRTask task(generated_task_id, WR_task::WRTaskType::READ);
        task.SetReadTask(disk_id, volume_id, inode_id);

        task_map_->Insert(std::move(task));
        zip_task_queue_->Push(WR_task::WRTaskShort(generated_task_id, WR_task::WRTaskType::READ));

        // 记录 inode_id -> task_id，供 ReadObjectByInodeId 反查；同 inode 多次请求只保留最新。
        {
            std::lock_guard<std::mutex> lock(inode_to_read_task_id_mutex_);
            inode_to_read_task_id_[inode_id] = generated_task_id;
        }

        *task_id = generated_task_id;
        return volumemanager::ErrorCode::READ_DELAY;
    }

    volumemanager::ErrorCode OpticalNodeManager::ReadObjectByTaskId(const uint64_t& task_id,
                                                    std::string* out,
                                                    uint64_t offset,
                                                    uint64_t read_size){
        if(!out){
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "invalid_parameter",
                "out=null task_id=" + std::to_string(task_id));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受重读；其它状态拒绝。
        if (GetStatus() != manager_status::kRunning) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "not_ready",
                "status=" + GetStatus());
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        auto task_opt = task_map_->Get(task_id);
        if (!task_opt.has_value()) {
            // 任务不存在：可能已被 Erase（消费过一次）或 task_id 传错。
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "task_not_found",
                "task_id=" + std::to_string(task_id));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_NOT_FOUND;
        }
        const WR_task::WRTask& task = *task_opt;
        if (task.type != WR_task::WRTaskType::READ) {
            // type 不匹配：把 WRITE 任务的 task_id 传给了 ReadObjectByTaskId。
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "invalid_type",
                "task_id=" + std::to_string(task_id) + " type=" + std::to_string(static_cast<int>(task.type)));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }
        // FAILED → 返回细分错误码；FINISH（已读完）→ TASK_ALREADY_FINISH；
        // READY 前的中间态 → TASK_NOT_FINISH；READY → 走 mmap 分片读取，末片置 FINISH。
        if (task.state == WR_task::WRTaskState::FAILED) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "task_failed",
                "task_id=" +
                std::to_string(task_id) +
                " code=" +
                volumemanager::GetErrorMessage(task.last_error_code) +
                " code_int=" +
                std::to_string(static_cast<int>(task.last_error_code)) +
                " attempt=" +
                std::to_string(task.attempt_count) +
                "/" +
                std::to_string(attempt_count_max_) +
                " detail=" +
                task.last_error_detail);
            last_failure_reason_buf_ = detail;
            return task.last_error_code == volumemanager::ErrorCode::SUCCESS
                ? volumemanager::ErrorCode::READ_FAILED
                : task.last_error_code;
        }
        if (task.state == WR_task::WRTaskState::FINISH) {
            // 任务已读完（读产物已 unlink），不可再读。
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "task_already_finish",
                "task_id=" + std::to_string(task_id));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_ALREADY_FINISH;
        }
        if (!task.isReadTaskReady()) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "task_not_finish",
                "task_id=" + std::to_string(task_id) + " state=" + std::to_string(static_cast<int>(task.state)));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_NOT_FINISH;
        }
        // 分片读取 [offset, offset+read_size)；helper 内部按 offset+read_size>=file_size 判定末片。
        bool is_last = false;
        volumemanager::ErrorCode ret = ReadFileToStringMmap(task.file_path, out, offset, read_size, &is_last);
        // mmap 阶段失败：不改 task_map_，FINISH 任务的 IO 失败由调用方决定下一步。
        if (ret != volumemanager::ErrorCode::SUCCESS) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "mmap_failed",
                "task_id=" +
                std::to_string(task_id) +
                " code=" +
                volumemanager::GetErrorMessage(ret) +
                " code_int=" +
                std::to_string(static_cast<int>(ret)) +
                " file_path=" +
                task.file_path);
            last_failure_reason_buf_ = detail;
            return ret;
        }
        // 服务端按 is_last 判定末片：unlink 读产物 + 置 FINISH（由清理线程稍后 Erase）；
        // 非末片保留文件供后续分片继续读。
        if (is_last) {
            if (::unlink(task.file_path.c_str()) != 0 && errno != ENOENT) {
                last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kReadObjectByTaskId, "post_success_unlink_failed",
                    std::string("code=") +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" +
                    std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " task_id=" +
                    std::to_string(task_id) +
                    " file_path=" +
                    task.file_path +
                    " errno=" +
                    std::to_string(errno));
            }
            // 读产物已消费：置 FINISH 终态，交由清理线程从 task_map_ 移除。
            MarkTaskState(task_id, WR_task::WRTaskState::FINISH);
            // inode_id -> task_id 索引同步清除（仅当仍指向本 task 时）。
            std::lock_guard<std::mutex> lock(inode_to_read_task_id_mutex_);
            auto idx_it = inode_to_read_task_id_.find(task.inode_id);
            if (idx_it != inode_to_read_task_id_.end() && idx_it->second == task_id) {
                inode_to_read_task_id_.erase(idx_it);
            }
        }
        return ret;
    }

    volumemanager::ErrorCode OpticalNodeManager::ReadObjectByInodeId(const std::string& inode_id,
                                                      std::string* out,
                                                      uint64_t offset,
                                                      uint64_t read_size) {
        // inode_id -> task_id 反查；仅多这一步，其余逻辑全部复用 ReadObjectByTaskId。
        uint64_t task_id = 0;
        {
            std::lock_guard<std::mutex> lock(inode_to_read_task_id_mutex_);
            auto idx_it = inode_to_read_task_id_.find(inode_id);
            if (idx_it == inode_to_read_task_id_.end()) {
                const std::string detail = FormatFailureDetail(start_failure_phase::kReadObjectByInodeId, "inode_not_found",
                    "inode_id=" + inode_id);
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::TASK_NOT_FOUND;
            }
            task_id = idx_it->second;
        }
        return ReadObjectByTaskId(task_id, out, offset, read_size);
    }

    volumemanager::ErrorCode OpticalNodeManager::WriteObject(
            const std::string& inode_id,
            const char* data,
            uint64_t data_size,
            uint64_t offset,
            uint64_t total_size) {
        if (!data && data_size > 0) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "invalid_parameter",
                "data=null data_size=" + std::to_string(data_size) + " inode_id=" + inode_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受写数据；其它状态拒绝。
        if (GetStatus() != manager_status::kRunning) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "not_ready",
                "status=" + GetStatus());
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        const std::string file_name = inode_id + ".bin";
        const std::string output_path = input_file_dir_ + file_name;

        std::lock_guard<std::mutex> lock(in_progress_writes_mutex_);

        // 首次见到此 inode：截断 temp file、记录 total_size。
        auto it = in_progress_writes_.find(inode_id);
        if (it == in_progress_writes_.end()) {
            if (total_size == 0) {
                // 空文件：直接创建空文件 + 任务 + 入压缩队列。
                std::ofstream trunc(output_path, std::ios::binary | std::ios::trunc);
                if (!trunc) {
                    const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "open_failed",
                        std::string("code=") +
                        volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                        " code_int=" +
                        std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                        " path=" +
                        output_path);
                    last_failure_reason_buf_ = detail;
                    return volumemanager::ErrorCode::IO_ERROR;
                }
                trunc.close();

                const uint64_t generated_task_id = GenerateTaskId();
                WR_task::WRTask task(generated_task_id, WR_task::WRTaskType::WRITE);
                task.SetWriteTask(inode_id);
                task.file_path = file_name;
                task_map_->Insert(std::move(task));
                zip_task_queue_->Push(WR_task::WRTaskShort(generated_task_id, WR_task::WRTaskType::WRITE));
                last_failure_reason_buf_.clear();
                return volumemanager::ErrorCode::SUCCESS;
            }

            std::ofstream trunc(output_path, std::ios::binary | std::ios::trunc);
            if (!trunc) {
                const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "open_failed",
                    std::string("code=") +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" +
                    std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " path=" +
                    output_path);
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::IO_ERROR;
            }
            trunc.close();

            InProgressWriteState new_state;
            new_state.total_size = total_size;
            in_progress_writes_.emplace(inode_id, new_state);
            it = in_progress_writes_.find(inode_id);
        }

        InProgressWriteState& state = it->second;

        // total_size 必须与首次调用一致；不一致视为参数错误。
        if (state.total_size != total_size) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "invalid_parameter",
                "total_size_mismatch expected=" +
                std::to_string(state.total_size) +
                " got=" +
                std::to_string(total_size) +
                " inode_id=" +
                inode_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // offset 必须严格等于已累积字节数；不接受乱序或重复写。
        if (offset != state.bytes_received) {
            const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "invalid_parameter",
                "offset_mismatch expected=" +
                std::to_string(state.bytes_received) +
                " got=" +
                std::to_string(offset) +
                " inode_id=" +
                inode_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        if (data_size > 0) {
            // in+out 打开（已截断创建）；seekp 定位到 offset。
            std::fstream fs(output_path,
                            std::ios::binary | std::ios::in | std::ios::out);
            if (!fs) {
                const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "open_failed",
                    std::string("code=") +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" +
                    std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " path=" +
                    output_path);
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::IO_ERROR;
            }
            fs.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
            if (!fs.good()) {
                fs.close();
                const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "seek_failed",
                    "offset=" + std::to_string(offset) + " path=" + output_path);
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::IO_ERROR;
            }
            fs.write(data, static_cast<std::streamsize>(data_size));
            if (!fs.good()) {
                fs.close();
                // 写失败：清状态 + 删残留文件；删失败走 sidecar。
                in_progress_writes_.erase(it);
                const std::string detail = FormatFailureDetail(start_failure_phase::kWriteObject, "write_failed",
                    std::string("code=") +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" +
                    std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " offset=" +
                    std::to_string(offset) +
                    " data_size=" +
                    std::to_string(data_size) +
                    " path=" +
                    output_path);
                last_failure_reason_buf_ = detail;
                if (::unlink(output_path.c_str()) != 0 && errno != ENOENT) {
                    last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kWriteObject, "residual_unlink_failed",
                        std::string("code=") +
                        volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                        " code_int=" +
                        std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                        " path=" +
                        output_path +
                        " errno=" +
                        std::to_string(errno));
                }
                return volumemanager::ErrorCode::IO_ERROR;
            }
            fs.close();
        }

        state.bytes_received += data_size;

        // 累计 == total_size：创建 WRITE 任务 + 入压缩队列，从 in_progress 移除。
        if (state.bytes_received >= state.total_size) {
            const uint64_t generated_task_id = GenerateTaskId();
            WR_task::WRTask task(generated_task_id, WR_task::WRTaskType::WRITE);
            task.SetWriteTask(inode_id);
            task.file_path = file_name;
            task_map_->Insert(std::move(task));
            zip_task_queue_->Push(WR_task::WRTaskShort(generated_task_id, WR_task::WRTaskType::WRITE));
            in_progress_writes_.erase(it);
        }

        last_failure_reason_buf_.clear();
        return volumemanager::ErrorCode::SUCCESS;
    }

    void OpticalNodeManager::OnCDReadComplete(const cd_manager_sim::ReadCompleteEvent& event) {
        // event.task_id 对应 CD_READ 任务（光盘库读请求）。
        // cd_manager_sim 当前未提供 success 字段，默认视为成功（TODO-08 留口）。
        auto cd_task = task_map_->Get(event.task_id);
        if (!cd_task.has_value()) {
            // 任务已被 Erase（异常清理）：只记录排查信号。
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "unknown_task_id",
                "task_id=" + std::to_string(event.task_id));
            return;
        }
        if (cd_task->type != WR_task::WRTaskType::CD_READ) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "invalid_type",
                "task_id=" + std::to_string(event.task_id) + " type=" + WR_task::WRTaskTypeToString(cd_task->type));
            return;
        }

        // 镜像从 disc_sim_dir_ 转移到 image_dir_/ 并登记到 space_manager_（扇平布局）。
        // 与 WRITE 链路 (PackVolume + MoveFrom) 形成对称；失败走 sidecar，不阻塞 CD_READ 收尾。
        if (space_manager_ != nullptr) {
            const std::string src_image_path =
                disc_sim_dir_ + "volume_" + event.volume_id + ".vimg";

            uint64_t read_volume_id = 0;
            std::string read_basename;
            const volumemanager::ErrorCode parse_ret =
                space_manager_->ParseVolumeFile(src_image_path, read_volume_id, read_basename);
            if (parse_ret == volumemanager::ErrorCode::SUCCESS) {
                const volumemanager::ErrorCode move_ret =
                    space_manager_->MoveFrom(src_image_path,
                                                 space_manager::ImageCategory::READ);
                if (move_ret != volumemanager::ErrorCode::SUCCESS) {
                    // 副作用失败：不影响 CD_READ 收尾，sidecar 记录便于运维排查。
                    last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "MoveFrom_failed",
                        std::string("code=") +
                        volumemanager::GetErrorMessage(move_ret) +
                        " code_int=" +
                        std::to_string(static_cast<int>(move_ret)) +
                        " src=" +
                        src_image_path +
                        " dst=image_dir_/" +
                        read_basename +
                        " task_id=" +
                        std::to_string(event.task_id));
                }
            } else {
                // 解析失败：兜底未来 cd_manager 落盘命名变更；sidecar 记录。
                last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "ParseVolumeFile_failed",
                    std::string("code=") +
                    volumemanager::GetErrorMessage(parse_ret) +
                    " code_int=" +
                    std::to_string(static_cast<int>(parse_ret)) +
                    " src=" +
                    src_image_path +
                    " task_id=" +
                    std::to_string(event.task_id));
            }
        } else {
            // space_manager_ 未就绪：后续 MountVolume 大概率 FILE_NOT_FOUND；仅 sidecar。
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "space_manager_null",
                "task_id=" + std::to_string(event.task_id));
        }

        // 触发"按镜像批量读"批次：处理 volume_read_index_ 中所有等待该卷的挂起读任务。
        const uint64_t batch_task_id = GenerateTaskId();
        WR_task::WRTask batch_task;
        batch_task.task_id = batch_task_id;
        batch_task.SetReadBatchByVolumeTask(event.volume_id);
        if (task_map_ != nullptr) {
            task_map_->Insert(batch_task);
        }
        zip_task_queue_->Push(
            WR_task::WRTaskShort(batch_task_id, WR_task::WRTaskType::READ_BATCH_BY_VOLUME));

        // CD_READ 一次性完成：镜像已加载回 image_dir_ 且批量读已派发，置 FINISH 等清理线程落审计日志。
        MarkTaskState(event.task_id, WR_task::WRTaskState::FINISH);
    }

    void OpticalNodeManager::CDReadTaskProcessor(){
        do{
            auto opt_task = cd_read_task_queue_->Pop();
            if (!opt_task.has_value()) {
                break;
            }
            WR_task::WRTaskShort task = *opt_task;

            if (!MarkTaskState(task.task_id, WR_task::WRTaskState::LOADING)) {
                continue;
            }

            auto full_task = task_map_->Get(task.task_id);
            if (!full_task.has_value()) {
                continue;
            }

            if (!SubmitCDReadTaskToCDManager(*full_task)) {
                // 失败根因：cd_manager 已停 / cd_manager 内部拒绝。
                if (cd_manager_ == nullptr) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   FormatFailureDetail(start_failure_phase::kCDReadTaskProcessor,
                                              "SubmitCDReadTaskToCDManager_failed",
                                              "cd_manager_null"));
                } else if (ShouldGiveUpRetry(task.task_id)) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   FormatFailureDetail(start_failure_phase::kCDReadTaskProcessor,
                                              "attempt_exceeded",
                                              "limit=" + std::to_string(attempt_count_max_) +
                                              " reason=SubmitReadTask_rejected_by_cd_manager"));
                } else {
                    // 未超限：保留 WAITING 重试，写失败痕迹但不切终态。
                    // 先提交其它字段，状态单独走 MarkTaskState（终态保护生效）。
                    auto current = task_map_->Get(task.task_id);
                    if (current.has_value()) {
                        current->IncrementAttemptCount();
                        current->last_error_code = volumemanager::ErrorCode::READ_FAILED;
                        current->last_error_detail = "SubmitReadTask_rejected_by_cd_manager";
                        task_map_->Update(std::move(*current));
                        MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                    }
                    cd_read_task_queue_->Push(
                        WR_task::WRTaskShort(task.task_id, WR_task::WRTaskType::CD_READ));
                }
            }
        } while(true);
    }

    // 构造 CD_READ 任务并登记入 task_map_ / cd_read_task_queue_。
    // 仅 ZipTaskProcessor 调用（单线程），无需额外加锁。
    void OpticalNodeManager::EnqueueCDReadTask(const std::string& disk_id,
                                               const std::string& volume_id) {
        const uint64_t cd_task_id = GenerateTaskId();
        WR_task::WRTask cd_task(cd_task_id, WR_task::WRTaskType::CD_READ);
        cd_task.SetCDReadTask(disk_id, volume_id);
        task_map_->Insert(std::move(cd_task));
        cd_read_task_queue_->Push(WR_task::WRTaskShort(cd_task_id, WR_task::WRTaskType::CD_READ));
    }

    std::string OpticalNodeManager::VolumeImagePath(const std::string& volume_id) const {
        return image_dir_ + "volume_" + volume_id + ".vimg";
    }

    void OpticalNodeManager::TouchVolume(const std::string& volume_id) {
        if (space_manager_ == nullptr) {
            return;
        }
        uint64_t parsed_volume_id = 0;
        std::string parsed_basename;
        if (space_manager_->ParseVolumeFile(VolumeImagePath(volume_id),
                                                parsed_volume_id,
                                                parsed_basename)
                == volumemanager::ErrorCode::SUCCESS) {
            space_manager_->Touch(parsed_volume_id);
        }
    }

    bool OpticalNodeManager::MarkTaskState(uint64_t task_id, WR_task::WRTaskState new_state) {
        if (task_map_ == nullptr) {
            return false;
        }

        auto current = task_map_->Get(task_id);
        if (!current.has_value()) {
            return false;
        }

        if (current->state == new_state) {
            return true;
        }

        // 终态保护：FINISH / FAILED 不再回退中间态。
        if (current->isTaskTerminal()) {
            return false;
        }

        current->state = new_state;
        current->state_changed_at_ms = WR_task::NowMs();
        if (!task_map_->Update(std::move(*current))) {
            return false;
        }
        return true;
    }

    // 置 FAILED 终态 + 写细分错误码 + 写 last_failure_reason_buf_；终态保护。
    // 副作用（删残留等）由调用方负责。
    bool OpticalNodeManager::MarkTaskFailed(uint64_t task_id,
                                              volumemanager::ErrorCode code,
                                              const std::string& detail) {
        if (task_map_ == nullptr) {
            return false;
        }

        auto current = task_map_->Get(task_id);
        if (!current.has_value()) {
            return false;
        }

        if (current->isTaskTerminal()) {
            return false;
        }

        current->SetFailed(code, detail);
        if (!task_map_->Update(std::move(*current))) {
            return false;
        }

        // 重新读 attempt_count，与 task_map_ 保持一致。
        const auto updated = task_map_->Get(task_id);
        const uint32_t attempt_for_detail = updated.has_value() ? updated->attempt_count : 0;
        std::string detail_buf;
        detail_buf.reserve(detail.size() + 96);
        detail_buf.append("task_id=").append(std::to_string(task_id)).append(" code=");
        detail_buf.append(volumemanager::GetErrorMessage(code));
        detail_buf.append(" code_int=").append(std::to_string(static_cast<int>(code)));
        detail_buf.append(" attempt=").append(std::to_string(attempt_for_detail))
                   .append("/").append(std::to_string(attempt_count_max_));
        if (!detail.empty()) {
            detail_buf.append(" detail=").append(detail);
        }
        last_failure_reason_buf_ = std::move(detail_buf);
        return true;
    }

    bool OpticalNodeManager::ShouldGiveUpRetry(uint64_t task_id) const {
        if (task_map_ == nullptr) {
            return true;
        }
        auto current = task_map_->Get(task_id);
        if (!current.has_value()) {
            return true;
        }
        return current->attempt_count >= attempt_count_max_;
    }

    bool OpticalNodeManager::SubmitCDReadTaskToCDManager(const WR_task::WRTask& task) {
        if (cd_manager_ == nullptr) {
            return false;
        }

        cd_manager_sim::ReadRequest request{
            task.task_id,
            task.disk_id,
            task.volume_id,
            task.inode_id,
            volume_manager_.volume_size_
        };

        return cd_manager_->SubmitReadTask(request);
    }

    // mmap 文件的 [offset, offset+read_size) 区间，追加到 *out 末尾；
    // 越界时 clamp 到文件剩余；out_is_last 非空时写入 offset+read_size>=file_size；
    // 不 unlink，由 ReadObjectByTaskId() 按 is_last 决定清理。
    volumemanager::ErrorCode ReadFileToStringMmap(const std::string& file_path,
                                                  std::string* out,
                                                  uint64_t offset,
                                                  uint64_t read_size,
                                                  bool* out_is_last) {
        if (!out) {
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd < 0) return volumemanager::ErrorCode::FILE_NOT_FOUND;

        struct stat st;
        if (fstat(fd, &st) < 0) {
            close(fd);
            return volumemanager::ErrorCode::IO_ERROR;
        }

        const uint64_t file_size = static_cast<uint64_t>(st.st_size);

        // 末片判定：用原始 (offset, read_size)，不能用 effective_size（clamp 后会丢失"原意读到末片"的信号）。
        // offset >= file_size 也算末片（早返回路径下也能正确标识）。
        if (out_is_last) {
            *out_is_last = (offset + read_size >= file_size);
        }

        // offset 已在文件末尾或之后：空区间。
        if (offset >= file_size) {
            close(fd);
            return volumemanager::ErrorCode::SUCCESS;
        }

        // read_size=0 不再有"读到末尾"语义：直接返回 SUCCESS（mmap(0) 无效）。
        if (read_size == 0) {
            close(fd);
            return volumemanager::ErrorCode::SUCCESS;
        }

        // 实际可读字节数：clamp 到文件剩余。
        const uint64_t file_remaining = file_size - offset;
        const uint64_t effective_size = (read_size > file_remaining)
            ? file_remaining
            : read_size;

        void* addr = mmap(nullptr, effective_size, PROT_READ, MAP_PRIVATE, fd,
                          static_cast<off_t>(offset));
        if (addr == MAP_FAILED) {
            close(fd);
            return volumemanager::ErrorCode::IO_ERROR;
        }

        out->append(static_cast<const char*>(addr), effective_size);
        munmap(addr, effective_size);
        close(fd);
        return volumemanager::ErrorCode::SUCCESS;
    }

    // 向 scheduler 拉取全量节点视图并建立 node_id -> node_address 临时映射。
    // 仅保留类型为 NODE_REAL 且健康 / 启用的节点；每批次调用一次，避免地址过期。
    bool OpticalNodeManager::BuildNodeAddressMap(
        std::unordered_map<std::string, std::string>* out) {
        if (out == nullptr) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "invalid_parameter",
                "out=null");
            return false;
        }
        out->clear();

        if (scheduler_addr_.empty()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "scheduler_addr_empty");
            return false;
        }

        // 懒初始化 channel（仅归档线程访问）；RPC 失败时 reset 供下批重建。
        if (scheduler_channel_ == nullptr) {
            auto channel = std::make_unique<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = 3000;
            options.max_retry = 0;
            if (channel->Init(scheduler_addr_.c_str(), &options) != 0) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "scheduler_channel_init_failed",
                    "addr=" + scheduler_addr_);
                return false;
            }
            scheduler_channel_ = std::move(channel);
        }

        zb::rpc::SchedulerService_Stub stub(scheduler_channel_.get());
        zb::rpc::GetClusterViewRequest request;
        request.set_min_generation(0);  // 0 表示拉取全量节点视图。
        zb::rpc::GetClusterViewReply response;
        brpc::Controller cntl;
        stub.GetClusterView(&cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            scheduler_channel_.reset();
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "GetClusterView_failed",
                "error=" + cntl.ErrorText());
            return false;
        }
        if (response.status().code() != zb::rpc::SCHED_OK) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "GetClusterView_status",
                "code=" + std::to_string(static_cast<int>(response.status().code())));
            return false;
        }

        for (const zb::rpc::NodeView& node : response.nodes()) {
            if (node.node_type() != zb::rpc::NODE_REAL) {
                continue;
            }
            if (node.health_state() == zb::rpc::NODE_HEALTH_DEAD) {
                continue;
            }
            if (node.admin_state() == zb::rpc::NODE_ADMIN_DISABLED) {
                continue;
            }
            if (node.address().empty()) {
                continue;
            }
            (*out)[node.node_id()] = node.address();
        }

        last_failure_reason_buf_.clear();
        return true;
    }

    // 从 target_node_address 指向的 real_node 下载 file 的全部分片，按绝对偏移直接写入
    // input_file_dir_/<inode_id>.archive 形成完整文件；成功后返回相对文件名。
    // 任一步失败都会清理半成品并返回 false。
    bool OpticalNodeManager::DownloadArchiveFile(const ArchiveFileInfo& file,
                                                 const std::string& target_node_address,
                                                 std::string* out_relative_path) {
        if (out_relative_path == nullptr) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "invalid_parameter",
                "out=null inode_id=" + std::to_string(file.inode_id));
            return false;
        }
        out_relative_path->clear();

        if (input_file_dir_.empty()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "input_dir_empty",
                "inode_id=" + std::to_string(file.inode_id));
            return false;
        }
        if (target_node_address.empty()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "node_address_empty",
                "inode_id=" + std::to_string(file.inode_id));
            return false;
        }

        // 取 / 懒建到目标节点的出站 channel（仅归档线程访问，无需加锁）。
        brpc::Channel* channel = nullptr;
        auto channel_it = data_node_channels_.find(target_node_address);
        if (channel_it != data_node_channels_.end() && channel_it->second != nullptr) {
            channel = channel_it->second.get();
        } else {
            auto new_channel = std::make_unique<brpc::Channel>();
            brpc::ChannelOptions options;
            options.protocol = "baidu_std";
            options.timeout_ms = 5000;
            options.max_retry = 0;
            if (new_channel->Init(target_node_address.c_str(), &options) != 0) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "data_channel_init_failed",
                    "address=" + target_node_address + " inode_id=" + std::to_string(file.inode_id));
                return false;
            }
            channel = new_channel.get();
            data_node_channels_[target_node_address] = std::move(new_channel);
        }

        zb::rpc::RealNodeService_Stub stub(channel);

        // 1. 取分片清单。
        zb::rpc::ResolveFileReadRequest resolve_req;
        resolve_req.set_inode_id(file.inode_id);
        resolve_req.set_offset(0);
        resolve_req.set_size(file.size);
        resolve_req.set_disk_id(file.target_disk_id);
        resolve_req.set_object_unit_size_hint(file.object_unit_size);
        zb::rpc::ResolveFileReadReply resolve_resp;
        brpc::Controller resolve_cntl;
        stub.ResolveFileRead(&resolve_cntl, &resolve_req, &resolve_resp, nullptr);
        if (resolve_cntl.Failed()) {
            // 连接可能已失效：丢弃缓存 channel，后续文件/批次重建。
            data_node_channels_.erase(target_node_address);
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "ResolveFileRead_failed",
                "inode_id=" +
                std::to_string(file.inode_id) +
                " address=" +
                target_node_address +
                " error=" +
                resolve_cntl.ErrorText());
            return false;
        }
        if (resolve_resp.status().code() != zb::rpc::STATUS_OK) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "ResolveFileRead_status",
                "inode_id=" +
                std::to_string(file.inode_id) +
                " code=" +
                std::to_string(static_cast<int>(resolve_resp.status().code())) +
                " message=" +
                resolve_resp.status().message());
            return false;
        }

        // 2. 校验元数据与分片布局（回复里的 object_unit_size 才是权威分片大小）。
        const zb::rpc::FileMeta& meta = resolve_resp.meta();
        const uint64_t object_unit_size = meta.object_unit_size();
        if (meta.file_size() != file.size) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "size_mismatch",
                "inode_id=" +
                std::to_string(file.inode_id) +
                " expected=" +
                std::to_string(file.size) +
                " actual=" +
                std::to_string(meta.file_size()));
            return false;
        }
        if (object_unit_size == 0 && file.size > 0) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "object_unit_size_zero",
                "inode_id=" + std::to_string(file.inode_id));
            return false;
        }

        uint64_t expected_offset = 0;
        for (const zb::rpc::FileObjectSlice& slice : resolve_resp.slices()) {
            const uint64_t slice_offset =
                static_cast<uint64_t>(slice.object_index()) * object_unit_size +
                slice.object_offset();
            if (slice_offset != expected_offset) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "slice_not_contiguous",
                    "inode_id=" +
                    std::to_string(file.inode_id) +
                    " expected_offset=" +
                    std::to_string(expected_offset) +
                    " slice_offset=" +
                    std::to_string(slice_offset));
                return false;
            }
            expected_offset += slice.length();
        }
        if (expected_offset != file.size) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "slice_total_mismatch",
                "inode_id=" +
                std::to_string(file.inode_id) +
                " expected=" +
                std::to_string(file.size) +
                " actual=" +
                std::to_string(expected_offset));
            return false;
        }

        // 3. 分片直接在磁盘重组：按绝对偏移 pwrite，不做内存拼接。
        const std::string relative_path = std::to_string(file.inode_id) + ".archive";
        const std::string local_path = input_file_dir_ + relative_path;
        const int fd = ::open(local_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "open_failed",
                "path=" + local_path + " errno=" + std::to_string(errno));
            return false;
        }

        for (const zb::rpc::FileObjectSlice& slice : resolve_resp.slices()) {
            if (stop_requested_.load(std::memory_order_relaxed)) {
                ::close(fd);
                ::unlink(local_path.c_str());
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "stopped",
                    "inode_id=" + std::to_string(file.inode_id));
                return false;
            }

            zb::rpc::ReadObjectRequest read_req;
            read_req.set_disk_id(slice.disk_id());
            read_req.set_object_id(slice.object_id());
            read_req.set_offset(slice.object_offset());
            read_req.set_size(slice.length());
            zb::rpc::ReadObjectReply read_resp;
            brpc::Controller read_cntl;
            stub.ReadObject(&read_cntl, &read_req, &read_resp, nullptr);
            if (read_cntl.Failed()) {
                ::close(fd);
                ::unlink(local_path.c_str());
                data_node_channels_.erase(target_node_address);
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "ReadObject_failed",
                    "inode_id=" +
                    std::to_string(file.inode_id) +
                    " object_id=" +
                    slice.object_id() +
                    " error=" +
                    read_cntl.ErrorText());
                return false;
            }
            if (read_resp.status().code() != zb::rpc::STATUS_OK ||
                static_cast<uint64_t>(read_resp.data().size()) != slice.length()) {
                ::close(fd);
                ::unlink(local_path.c_str());
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "ReadObject_bad_response",
                    "inode_id=" +
                    std::to_string(file.inode_id) +
                    " object_id=" +
                    slice.object_id() +
                    " expected_len=" +
                    std::to_string(slice.length()) +
                    " actual_len=" +
                    std::to_string(read_resp.data().size()));
                return false;
            }

            const uint64_t write_offset =
                static_cast<uint64_t>(slice.object_index()) * object_unit_size +
                slice.object_offset();
            if (!PwriteAll(fd,
                           read_resp.data().data(),
                           static_cast<size_t>(read_resp.data().size()),
                           write_offset)) {
                ::close(fd);
                ::unlink(local_path.c_str());
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "pwrite_failed",
                    "inode_id=" + std::to_string(file.inode_id) + " offset=" + std::to_string(write_offset));
                return false;
            }
        }

        // 4. 最终大小校验：分片连续覆盖 [0, size)，文件大小应恰为 size。
        struct stat st {};
        if (::fstat(fd, &st) != 0 || static_cast<uint64_t>(st.st_size) != file.size) {
            const uint64_t actual_size =
                (st.st_size > 0) ? static_cast<uint64_t>(st.st_size) : 0;
            ::close(fd);
            ::unlink(local_path.c_str());
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "verify_size_failed",
                "inode_id=" +
                std::to_string(file.inode_id) +
                " expected=" +
                std::to_string(file.size) +
                " actual=" +
                std::to_string(actual_size));
            return false;
        }
        ::close(fd);

        *out_relative_path = relative_path;
        last_failure_reason_buf_.clear();
        return true;
    }

    // 为已落盘的归档文件建 WRITE 任务并入 zip_task_queue_。
    // file_path 必须为相对文件名：ZipTaskProcessor 以 input_file_dir_ + file_path 定位原文件。
    bool OpticalNodeManager::SubmitWriteTaskForArchive(uint64_t inode_id,
                                                       const std::string& relative_path) {
        if (relative_path.empty()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "empty_file_path",
                "inode_id=" + std::to_string(inode_id));
            return false;
        }
        if (zip_task_queue_ == nullptr || zip_task_queue_->IsClosed()) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "zip_queue_closed",
                "inode_id=" + std::to_string(inode_id));
            return false;
        }
        if (stop_requested_.load(std::memory_order_relaxed)) {
            last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "stopping",
                "inode_id=" + std::to_string(inode_id));
            return false;
        }

        const uint64_t generated_task_id = GenerateTaskId();
        WR_task::WRTask task(generated_task_id, WR_task::WRTaskType::WRITE);
        task.SetWriteTask(std::to_string(inode_id));
        task.file_path = relative_path;
        task_map_->Insert(std::move(task));
        zip_task_queue_->Push(
            WR_task::WRTaskShort(generated_task_id, WR_task::WRTaskType::WRITE));

        last_failure_reason_buf_.clear();
        return true;
    }

    // 归档任务消费线程：从 archive_task_queue_ 取出批次，每批次重建一次节点映射，
    // 再逐文件下载（real_node 分片重组）并建 WRITE 任务入 zip_task_queue_。
    void OpticalNodeManager::ArchiveTaskProcessor() {
        while (!stop_requested_.load(std::memory_order_relaxed)) {
            auto opt_request = archive_task_queue_->Pop();
            if (!opt_request.has_value()) {
                // 队列已 Close：退出消费循环。
                break;
            }

            const SendArchiveMetadataRequest& request = *opt_request;

            // 步骤 1：每批次重建一次节点映射，避免地址过期。
            std::unordered_map<std::string, std::string> node_address_map;
            if (!BuildNodeAddressMap(&node_address_map)) {
                continue;
            }

            // 步骤 2：逐文件下载 + 建 WRITE 任务。
            for (const ArchiveFileInfo& file : request.files) {
                if (stop_requested_.load(std::memory_order_relaxed)) {
                    break;
                }

                std::string target_node_address;
                if (!ResolveNodeAddressFromMap(node_address_map,
                                               file.target_node_id,
                                               &target_node_address)) {
                    last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "node_not_found",
                        "target_node_id=" + file.target_node_id + " inode_id=" + std::to_string(file.inode_id));
                    continue;
                }

                std::string relative_path;
                if (!DownloadArchiveFile(file, target_node_address, &relative_path)) {
                    continue;
                }

                if (!SubmitWriteTaskForArchive(file.inode_id, relative_path)) {
                    // 队列已关闭 / 正在停止：清理已下载文件，避免残留，并退出本批次。
                    ::unlink((input_file_dir_ + relative_path).c_str());
                    break;
                }
            }
        }
    }

    void OpticalNodeManager::ZipTaskProcessor() {
        do {
            auto opt_task = zip_task_queue_->Pop();
            if (!opt_task.has_value()) {
                break;
            }
            WR_task::WRTaskShort task = *opt_task;

            auto full_task = task_map_->Get(task.task_id);
            if (!full_task.has_value()) {
                continue;
            }

            if (full_task->type == WR_task::WRTaskType::READ) {
                // 终态保护：避免在失败任务上继续推进状态机。
                if (full_task->isTaskTerminal()) {
                    continue;
                }
                if (full_task->inode_id_num == WR_task::WRTask::INVALID_INODE_ID) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "inode_id_invalid",
                                              "inode_id=" + full_task->inode_id));
                    continue;
                }
                const uint64_t inode_id_num = full_task->inode_id_num;

                // 搭便车预查询：volume_read_index_ 中已挂起的同卷读任务，
                // 当前任务登记后置 WAITING，等待批量唤醒。
                if (volume_read_index_.ContainsVolume(full_task->volume_id)) {
                    volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                    full_task->IncrementAttemptCount();
                    task_map_->Update(std::move(*full_task));
                    MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                    continue;
                }

                // 读互斥区间：与 SpaceManager 的写路径（LRU 换出 / MoveFrom）互斥。
                struct ReadLockGuard {
                    space_manager::SpaceManager* mgr;
                    ~ReadLockGuard() { if (mgr) mgr->ReadUnlock(); }
                };
                ReadLockGuard read_lock_guard{space_manager_.get()};
                if (space_manager_ != nullptr) {
                    space_manager_->ReadLock();
                }

                std::string output_path;
                volumemanager::ErrorCode ret = volume_manager_.ReadFile(inode_id_num, output_path);

                if (ret == volumemanager::ErrorCode::SUCCESS) {
                    TouchVolume(full_task->volume_id);
                    full_task->file_path = std::move(output_path);
                    task_map_->Update(std::move(*full_task));
                    MarkTaskState(task.task_id, WR_task::WRTaskState::READY);
                    continue;
                }

                // INODE_NOT_FOUND / VOLUME_NOT_FOUND：尝试 MountVolume。
                if (ret == volumemanager::ErrorCode::INODE_NOT_FOUND || ret == volumemanager::ErrorCode::VOLUME_NOT_FOUND) {
                    volumemanager::ErrorCode mount_ret = volume_manager_.MountVolume(full_task->volume_id);
                    if (mount_ret == volumemanager::ErrorCode::FILE_NOT_FOUND) {
                        // 卷尚未就绪 → 转异步等待；超限后强制 FAILED 防重试风暴。
                        if (ShouldGiveUpRetry(full_task->task_id)) {
                            MarkTaskFailed(full_task->task_id,
                                           volumemanager::ErrorCode::READ_FAILED,
                                           FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                      "attempt_exceeded",
                                                      "limit=" + std::to_string(attempt_count_max_) +
                                                      " volume_id=" + full_task->volume_id));
                            continue;
                        }
                        volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                        // 委托光盘库把该卷镜像加载回 image_dir_；本 READ 任务留在 WAITING，
                        // 由加载完成后的批量读任务唤醒。
                        // 注意：必须在 Update(std::move(...)) 之前取用 disk_id / volume_id。
                        EnqueueCDReadTask(full_task->disk_id, full_task->volume_id);
                        full_task->IncrementAttemptCount();
                        task_map_->Update(std::move(*full_task));
                        MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                        continue;
                    } else if (mount_ret == volumemanager::ErrorCode::SUCCESS) {
                        output_path.clear();
                        ret = volume_manager_.ReadFile(inode_id_num, output_path);
                        if (ret == volumemanager::ErrorCode::SUCCESS) {
                            TouchVolume(full_task->volume_id);
                            full_task->file_path = std::move(output_path);
                            task_map_->Update(std::move(*full_task));
                            MarkTaskState(task.task_id, WR_task::WRTaskState::READY);
                            continue;
                        }
                        // Mount 成功但 inode 缺席：真实业务错误，重试不会变 SUCCESS。
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                  "read_after_mount_failed",
                                                  "ret=" + std::to_string(static_cast<int>(ret)) +
                                                  " ret_code=" + volumemanager::GetErrorMessage(ret) +
                                                  " volume_id=" + full_task->volume_id +
                                                  " inode_id_num=" + std::to_string(inode_id_num)));
                        continue;
                    } else {
                        // 不可恢复的 Mount 错误（IO_ERROR / INVALID_VOLUME_FORMAT / INVALID_VOLUME_ID）。
                        const std::string detail = FormatFailureDetail(
                            start_failure_phase::kZipTaskProcessor, "mount_volume_failed",
                            std::string("type=") + volumemanager::GetErrorMessage(mount_ret) +
                            " code=" + std::to_string(static_cast<int>(mount_ret)) +
                            " volume_id=" + full_task->volume_id);
                        MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::READ_FAILED, detail);
                        continue;
                    }
                }

                // IO_ERROR 特判：镜像被 LRU 换出（可恢复瞬态）。
                // UnmountVolume 清残留挂载态后回 WAITING 重新挂载。
                if (ret == volumemanager::ErrorCode::IO_ERROR) {
                    if (full_task->isTaskTerminal()) {
                        continue;
                    }
                    uint64_t parsed_volume_id = 0;
                    std::string parsed_basename;
                    const std::string volume_image_path = VolumeImagePath(full_task->volume_id);
                    const volumemanager::ErrorCode parse_ret =
                        space_manager_ != nullptr
                            ? space_manager_->ParseVolumeFile(volume_image_path,
                                                                  parsed_volume_id,
                                                                  parsed_basename)
                            : volumemanager::ErrorCode::INVALID_PATH;
                    if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                  "io_error_parse_failed",
                                                  "volume_id=" + full_task->volume_id));
                        continue;
                    }
                    if (ShouldGiveUpRetry(full_task->task_id)) {
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                  "io_error_evicted",
                                                  "attempt_exceeded limit=" +
                                                      std::to_string(attempt_count_max_) +
                                                      " volume_id=" + full_task->volume_id));
                        continue;
                    }
                    // UnmountVolume 对未挂载卷返回 VOLUME_NOT_FOUND，视为幂等 no-op。
                    (void)volume_manager_.UnmountVolume(parsed_volume_id);
                    volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                    // 镜像被换出：同样委托光盘库重新加载，本 READ 任务留在 WAITING 等唤醒。
                    // 注意：必须在 Update(std::move(...)) 之前取用 disk_id / volume_id。
                    EnqueueCDReadTask(full_task->disk_id, full_task->volume_id);
                    full_task->IncrementAttemptCount();
                    task_map_->Update(std::move(*full_task));
                    MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                    continue;
                }

                // 兜底：未知读失败。
                MarkTaskFailed(full_task->task_id,
                               volumemanager::ErrorCode::READ_FAILED,
                               FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                          "read_failed",
                                          "ret=" + std::to_string(static_cast<int>(ret)) +
                                          " ret_code=" + volumemanager::GetErrorMessage(ret) +
                                          " inode_id_num=" + std::to_string(inode_id_num)));
                continue;
            } else if (full_task->type == WR_task::WRTaskType::READ_BATCH_BY_VOLUME) {
                // 按镜像批量读：镜像就绪后集中处理该卷下所有挂起读任务。
                if (full_task->isTaskTerminal()) {
                    continue;
                }
                if (full_task->volume_id.empty()) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "batch_volume_id_empty"));
                    continue;
                }

                struct ReadLockGuard {
                    space_manager::SpaceManager* mgr;
                    ~ReadLockGuard() { if (mgr) mgr->ReadUnlock(); }
                };
                ReadLockGuard read_lock_guard{space_manager_.get()};
                if (space_manager_ != nullptr) {
                    space_manager_->ReadLock();
                }

                const volumemanager::ErrorCode mount_ret =
                    volume_manager_.MountVolume(full_task->volume_id);
                if (mount_ret == volumemanager::ErrorCode::FILE_NOT_FOUND) {
                    // 镜像被 LRU 换出：派发 CD_READ 重新加载；本 batch 一次性 FINISH。
                    // 加载完成后 OnCDReadComplete 会再派发一批 batch 唤醒仍挂起的读任务。
                    EnqueueCDReadTask(std::string(), full_task->volume_id);
                    MarkTaskState(full_task->task_id, WR_task::WRTaskState::FINISH);
                    continue;
                }
                if (mount_ret != volumemanager::ErrorCode::SUCCESS &&
                    mount_ret != volumemanager::ErrorCode::INVALID_VOLUME_ID) {
                    // 不可恢复的 Mount 错误：整批 FAILED。
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "batch_mount_failed",
                                              std::string("type=") + volumemanager::GetErrorMessage(mount_ret) +
                                              " code_int=" +
                                              std::to_string(static_cast<int>(mount_ret)) +
                                              " volume_id=" + full_task->volume_id));
                    continue;
                }
                // SUCCESS / INVALID_VOLUME_ID：复用挂载态处理子任务。

                // 整卷 LRU 推前：推迟该卷的淘汰窗口。
                TouchVolume(full_task->volume_id);

                // 逐个处理挂起子任务：完成即从 read_index 移除。
                const std::vector<uint64_t> sub_task_ids =
                    volume_read_index_.GetTasks(full_task->volume_id);
                for (uint64_t sub_tid : sub_task_ids) {
                    volume_read_index_.RemoveTask(full_task->volume_id, sub_tid);
                    auto sub = task_map_->Get(sub_tid);
                    if (!sub.has_value()) {
                        continue;
                    }
                    if (sub->isTaskTerminal()) {
                        continue;
                    }
                    if (sub->inode_id_num == WR_task::WRTask::INVALID_INODE_ID) {
                        MarkTaskFailed(sub_tid,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                  "batch_sub_inode_id_invalid",
                                                  std::string("inode_id=") + sub->inode_id));
                        continue;
                    }
                    std::string output_path;
                    const volumemanager::ErrorCode sub_ret =
                        volume_manager_.ReadFile(sub->inode_id_num, output_path);
                    if (sub_ret == volumemanager::ErrorCode::SUCCESS) {
                        sub->file_path = std::move(output_path);
                        task_map_->Update(std::move(*sub));
                        MarkTaskState(sub_tid, WR_task::WRTaskState::READY);
                    } else {
                        MarkTaskFailed(sub_tid,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                                  "batch_sub_read_failed",
                                                  "ret=" + std::to_string(static_cast<int>(sub_ret)) +
                                                  " ret_code=" +
                                                  volumemanager::GetErrorMessage(sub_ret) +
                                                  " inode_id_num=" +
                                                  std::to_string(sub->inode_id_num)));
                    }
                }

                // batch 任务自身一次性 FINISH。
                MarkTaskState(full_task->task_id, WR_task::WRTaskState::FINISH);
                continue;
            } else if (full_task->type == WR_task::WRTaskType::WRITE) {
                // 写任务处理：压缩 → 删原始 → 扫描是否触发封装 → 转移 + 构造 CD_BURN 任务。
                if (full_task->isTaskTerminal()) {
                    continue;
                }
                if (full_task->inode_id_num == WR_task::WRTask::INVALID_INODE_ID) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::WRITE_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "inode_id_invalid",
                                              "inode_id=" + full_task->inode_id));
                    continue;
                }
                const uint64_t inode_id_num = full_task->inode_id_num;

                // 取用前先补齐可用 ID 队列（不足时向 MDS 申请）；随后由队首分配，空时退回 0。
                EnsureAvailableVolumeIds();
                uint64_t volume_id_num = 0;
                if (!available_volume_ids.empty()) {
                    volume_id_num = available_volume_ids.front();
                }

                // 1. 压缩原始文件；失败保留原文件供排查。
                //    压缩前先把本次 inode 登记到待打包集合，供封装触发后 diff 出本次被打包的文件。
                {
                    std::lock_guard<std::mutex> lock(pending_pack_inode_ids_mutex_);
                    pending_pack_inode_ids_.insert(inode_id_num);
                }
                volumemanager::ErrorCode add_ret = volume_manager_.AddFileToCollect(full_task->file_path, inode_id_num, volume_id_num);
                if (add_ret != volumemanager::ErrorCode::SUCCESS) {
                    // 压缩失败：未生成 temp_*.compressed，本次 inode 不会被打包，从待打包集合移除。
                    std::lock_guard<std::mutex> lock(pending_pack_inode_ids_mutex_);
                    pending_pack_inode_ids_.erase(inode_id_num);
                    const std::string keep_detail = std::string("original_file_kept path=") +
                        (full_task->file_path.empty() ? std::string("<empty>") : full_task->file_path) +
                        " add_ret=" + std::to_string(static_cast<int>(add_ret));
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::COMPRESS_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "AddFileToCollect_failed",
                                              keep_detail));
                    continue;
                }

                // 2. 压缩成功 → 删原始文件。
                const std::string original_file_path = input_file_dir_ + full_task->file_path;
                ::unlink(original_file_path.c_str());

                // 3. 状态置 FINISH（压缩完成）；状态一律走 MarkTaskState。
                MarkTaskState(task.task_id, WR_task::WRTaskState::FINISH);

                // 4. 扫描 temp_dir_ 判断是否已触发封装。
                std::string packed_volume_path;
                {
                    std::unique_ptr<DIR, decltype(&closedir)> dir(opendir(temp_dir_.c_str()), closedir);
                    if (dir) {
                        struct dirent* entry;
                        while ((entry = readdir(dir.get())) != nullptr) {
                            std::string filename(entry->d_name);
                            if (filename.size() > 5 && filename.substr(filename.size() - 5) == ".vimg") {
                                packed_volume_path = temp_dir_ + filename;
                                break;
                            }
                        }
                    }
                }

                // 5. 未触发封装 → 等待下一个任务。
                if (packed_volume_path.empty()) {
                    continue;
                }

                // 触发封装：从 available_volume_ids 移除队首。
                if (!available_volume_ids.empty()) {
                    available_volume_ids.pop();
                }

                // 5.5 从 temp_dir_ 剩余 temp_*.compressed 反推本次被打包的 inode_id，
                //     并打印镜像 → inode 对应关系（MDS 上报未完善前的控制台替代）。
                {
                    // 从 "volume_<id>.vimg" 文件名解析 volume_id。
                    const std::string basename = packed_volume_path.substr(temp_dir_.size());
                    std::string volume_id_str;
                    if (basename.size() > 6 && basename.rfind("volume_", 0) == 0 &&
                        basename.size() >= 6 + 5) {
                        volume_id_str = basename.substr(7, basename.size() - 7 - 5);  // 去掉 "volume_" 与 ".vimg"
                    }
                    ReportPackedInodes(volume_id_str);
                }

                // 6. 镜像从 temp_dir_ 转移到 image_dir_/（扇平布局）并登记管理表。
                //    失败统一 PACK_FAILED 暴露；各失败细节走 detail。
                if (space_manager_ == nullptr) {
                    const std::string detail = FormatFailureDetail(start_failure_phase::kZipTaskProcessor, "space_manager_null",
                        "ret=" +
                        std::to_string(static_cast<int>(volumemanager::ErrorCode::PACK_FAILED)) +
                        " code=" +
                        volumemanager::GetErrorMessage(volumemanager::ErrorCode::PACK_FAILED) +
                        " output_path=" +
                        packed_volume_path);
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }
                uint64_t packed_volume_id = 0;
                std::string packed_basename;
                const volumemanager::ErrorCode parse_ret =
                    space_manager_->ParseVolumeFile(packed_volume_path, packed_volume_id, packed_basename);
                if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
                    const std::string detail = FormatFailureDetail(start_failure_phase::kZipTaskProcessor, "ParseVolumeFile_failed",
                        "ret=" +
                        std::to_string(static_cast<int>(parse_ret)) +
                        " code=" +
                        volumemanager::GetErrorMessage(parse_ret) +
                        " code_int=" +
                        std::to_string(static_cast<int>(parse_ret)) +
                        " output_path=" +
                        packed_volume_path);
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }
                const volumemanager::ErrorCode move_ret =
                    space_manager_->MoveFrom(packed_volume_path,
                                                 space_manager::ImageCategory::WRITE);
                if (move_ret != volumemanager::ErrorCode::SUCCESS) {
                    const std::string detail = FormatFailureDetail(start_failure_phase::kZipTaskProcessor, "MoveFrom_failed",
                        "ret=" +
                        std::to_string(static_cast<int>(move_ret)) +
                        " code=" +
                        volumemanager::GetErrorMessage(move_ret) +
                        " code_int=" +
                        std::to_string(static_cast<int>(move_ret)) +
                        " volume_id=" +
                        std::to_string(packed_volume_id) +
                        " src=" +
                        packed_volume_path);
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }

                // 7. 用 image_dir_/ 下的新路径构造 CD_BURN 任务。
                const std::string final_volume_path = image_dir_ + packed_basename;

                // 8. 构造 CD_BURN 任务并入队；volume_id 来自实际产出，disk_id 为本地递增生成的占位标识。
                const uint64_t generated_cd_burn_task_id = GenerateTaskId();
                WR_task::WRTask new_cd_burn_task(generated_cd_burn_task_id, WR_task::WRTaskType::CD_BURN);
                const std::string packed_volume_id_str = std::to_string(packed_volume_id);
                new_cd_burn_task.SetCDBurnTask(GenerateDiskId(), packed_volume_id_str, final_volume_path);

                // 9. 入 task_map_ + cd_burn_task_queue_。
                task_map_->Insert(std::move(new_cd_burn_task));
                cd_burn_task_queue_->Push(WR_task::WRTaskShort(generated_cd_burn_task_id, WR_task::WRTaskType::CD_BURN));

                // 10. 第二次上报（镜像 → 光盘，ReportImagesBurnedToDisc）待光盘库刻录细节完善后
                //     在 OnCDBurnComplete 中实现，本轮不实现。
            }
        } while (!stop_requested_.load(std::memory_order_relaxed));
    }

    void OpticalNodeManager::ReportPackedInodes(const std::string& volume_id) {
        // 扫描 temp_dir_ 剩余 temp_<inode_id>.compressed，收集仍未打包的 inode_id。
        std::unordered_set<uint64_t> remaining_inodes;
        {
            std::unique_ptr<DIR, decltype(&closedir)> dir(opendir(temp_dir_.c_str()), closedir);
            if (dir) {
                struct dirent* entry;
                while ((entry = readdir(dir.get())) != nullptr) {
                    const std::string filename(entry->d_name);
                    // 形如 "temp_<inode_id>.compressed"。
                    const std::string prefix = "temp_";
                    const std::string suffix = ".compressed";
                    if (filename.size() > prefix.size() + suffix.size() &&
                        filename.rfind(prefix, 0) == 0 &&
                        filename.compare(filename.size() - suffix.size(), suffix.size(), suffix) == 0) {
                        const std::string id_str =
                            filename.substr(prefix.size(), filename.size() - prefix.size() - suffix.size());
                        try {
                            size_t consumed = 0;
                            const unsigned long long v = std::stoull(id_str, &consumed);
                            if (consumed == id_str.size()) {
                                remaining_inodes.insert(static_cast<uint64_t>(v));
                            }
                        } catch (const std::exception&) {
                            // 非数字 inode 文件名，忽略。
                        }
                    }
                }
            }
        }

        // 待打包集合中、但 temp_dir_ 已无对应 temp_*.compressed 的 inode = 本次被打包。
        std::vector<uint64_t> packed_inodes;
        {
            std::lock_guard<std::mutex> lock(pending_pack_inode_ids_mutex_);
            for (auto it = pending_pack_inode_ids_.begin(); it != pending_pack_inode_ids_.end();) {
                if (remaining_inodes.find(*it) == remaining_inodes.end()) {
                    packed_inodes.push_back(*it);
                    it = pending_pack_inode_ids_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        // 本次打包的镜像 → inode 对应关系打印到控制台：MDS 的
        // ReportFilesPackedToImage 上报尚未实现，先以控制台输出替代，便于单模块测试观察。
        std::cout << "[ReportFilesPackedToImage] image_id=" << volume_id
                  << " count=" << packed_inodes.size()
                  << " inode_ids=";
        for (size_t i = 0; i < packed_inodes.size(); ++i) {
            if (i > 0) {
                std::cout << ',';
            }
            std::cout << packed_inodes[i];
        }
        std::cout << std::endl;
    }

    void OpticalNodeManager::OnCDBurnComplete(const cd_manager_sim::BurnCompleteEvent& event) {
        // cd_manager_sim 当前未提供 success 字段，默认视为成功（TODO-08 留口）。
        if (!MarkTaskState(event.task_id, WR_task::WRTaskState::FINISH)) {
            auto existing = task_map_->Get(event.task_id);
            if (!existing.has_value()) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "unknown_task_id",
                    "task_id=" + std::to_string(event.task_id));
            }
            return;
        }

        // 烧录完成 → 释放对应写镜像；副作用失败不影响 FINISH 主语义。
        if (space_manager_ == nullptr) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "space_manager_null",
                "task_id=" + std::to_string(event.task_id));
            return;
        }
        if (event.image_path.empty()) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "empty_image_path",
                "task_id=" + std::to_string(event.task_id));
            return;
        }
        uint64_t burned_volume_id = 0;
        std::string basename;
        const volumemanager::ErrorCode parse_ret =
            space_manager_->ParseVolumeFile(event.image_path, burned_volume_id, basename);
        if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "ParseVolumeFile_failed",
                std::string("code=") +
                volumemanager::GetErrorMessage(parse_ret) +
                " code_int=" +
                std::to_string(static_cast<int>(parse_ret)) +
                " image_path=" +
                event.image_path +
                " task_id=" +
                std::to_string(event.task_id));
            return;
        }
        const volumemanager::ErrorCode remove_ret =
            space_manager_->RemoveWriteImage(burned_volume_id);
        if (remove_ret != volumemanager::ErrorCode::SUCCESS) {
            // 副作用失败走 sidecar，不改 CD_BURN 主语义。
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "RemoveWriteImage_failed",
                std::string("code=") +
                volumemanager::GetErrorMessage(remove_ret) +
                " code_int=" +
                std::to_string(static_cast<int>(remove_ret)) +
                " volume_id=" +
                std::to_string(burned_volume_id) +
                " image_path=" +
                event.image_path +
                " task_id=" +
                std::to_string(event.task_id));
        }
    }

    bool OpticalNodeManager::SubmitCDBurnTaskToCDManager(const WR_task::WRTask& task) {
        if (cd_manager_ == nullptr) {
            return false;
        }

        // 读取真实文件大小供 cd_manager 计算刻录时长；失败时回退到 config 默认值。
        uint64_t image_size_bytes = 0;
        struct stat st {};
        if (!task.file_path.empty() &&
            ::stat(task.file_path.c_str(), &st) == 0 && st.st_size > 0) {
            image_size_bytes = static_cast<uint64_t>(st.st_size);
        }

        cd_manager_sim::BurnRequest request{
            task.task_id,
            task.disk_id,
            task.volume_id,
            task.file_path,
            image_size_bytes
        };

        return cd_manager_->SubmitBurnTask(request);
    }

    void OpticalNodeManager::CDBurnTaskProcessor() {
        do {
            auto opt_task = cd_burn_task_queue_->Pop();
            if (!opt_task.has_value()) {
                break;
            }
            WR_task::WRTaskShort task = *opt_task;

            if (!MarkTaskState(task.task_id, WR_task::WRTaskState::CD_BURNING)) {
                continue;
            }

            auto full_task = task_map_->Get(task.task_id);
            if (!full_task.has_value()) {
                continue;
            }

            if (!SubmitCDBurnTaskToCDManager(*full_task)) {
                // 提交失败：超限 → 强制 FAILED；未超限 → 保留 WAITING 重试。
                if (ShouldGiveUpRetry(task.task_id)) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::BURN_FAILED,
                                   FormatFailureDetail(start_failure_phase::kCDBurnTaskProcessor,
                                              "attempt_exceeded",
                                              "limit=" + std::to_string(attempt_count_max_) +
                                              " volume_id=" + full_task->volume_id +
                                              " image_path=" + full_task->file_path));
                } else {
                    // 先提交其它字段，状态单独走 MarkTaskState（终态保护生效）。
                    auto current = task_map_->Get(task.task_id);
                    if (current.has_value()) {
                        current->IncrementAttemptCount();
                        current->last_error_code = volumemanager::ErrorCode::BURN_FAILED;
                        current->last_error_detail = "SubmitBurnTask_rejected_by_cd_manager volume_id=" +
                                                     full_task->volume_id +
                                                     " image_path=" + full_task->file_path;
                        task_map_->Update(std::move(*current));
                        MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                    }
                    cd_burn_task_queue_->Push(WR_task::WRTaskShort(task.task_id, WR_task::WRTaskType::CD_BURN));
                }
            }
        } while (!stop_requested_.load(std::memory_order_relaxed));
    }

    void OpticalNodeManager::AppendTaskDoneLog(const WR_task::WRTask& task) {
        if (log_dir_.empty()) {
            return;
        }

        const uint64_t now_ms = WR_task::NowMs();
        const std::string date_str = FormatLocalTimeMs(now_ms, "%Y%m%d");
        const std::string now_str = FormatLocalTimeMs(now_ms, "%Y-%m-%d %H:%M:%S");
        if (date_str.empty() || now_str.empty()) {
            return;
        }

        const std::string log_path = log_dir_ + "task_done_" + date_str + ".log";
        // ios::app：文件不存在时自动新建，已存在则追加到末尾。
        std::ofstream log(log_path, std::ios::app);
        if (!log.is_open()) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kCleanupTaskProcessor, "open_failed",
                "path=" + log_path + " errno=" + std::to_string(errno));
            return;
        }

        log << now_str
            << " task_id=" << task.task_id
            << " type=" << WR_task::WRTaskTypeToString(task.type)
            << " state=" << WR_task::WRTaskStateToString(task.state)
            << " state_changed_at="
            << FormatLocalTimeMs(task.state_changed_at_ms, "%Y-%m-%d %H:%M:%S")
            << " disk_id=" << task.disk_id
            << " volume_id=" << task.volume_id
            << " inode_id=" << task.inode_id
            << " file_path=" << task.file_path
            << " attempt_count=" << task.attempt_count
            << " error_code=" << volumemanager::GetErrorMessage(task.last_error_code)
            << '\n';
        log.flush();

        if (!log.good()) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kCleanupTaskProcessor, "write_failed",
                "path=" + log_path + " task_id=" + std::to_string(task.task_id));
        }
    }

    void OpticalNodeManager::CleanupTaskProcessor() {
        // 每 cleanup_interval_ (10 分钟) 触发一次；用 1 秒粒度睡眠以响应 stop_requested_。
        constexpr auto kTick = std::chrono::milliseconds(1000);
        auto elapsed = std::chrono::milliseconds(0);
        while (!stop_requested_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(kTick);
            elapsed += kTick;
            if (elapsed < cleanup_interval_) {
                continue;
            }
            elapsed = std::chrono::milliseconds(0);

            // 收集所有 FINISH 终态任务，逐个 Erase 并清理 inode 索引。
            const std::vector<uint64_t> finish_ids = task_map_->CollectFinishTaskIds();
            for (uint64_t finish_id : finish_ids) {
                // 读 inode_id 用于索引清理；任务可能已被并发移除，取不到则跳过。
                auto finish_task = task_map_->Get(finish_id);
                if (finish_task.has_value()) {
                    // 先落审计日志再 Erase；日志写失败不影响清理。
                    AppendTaskDoneLog(finish_task.value());

                    std::lock_guard<std::mutex> lock(inode_to_read_task_id_mutex_);
                    auto idx_it = inode_to_read_task_id_.find(finish_task->inode_id);
                    if (idx_it != inode_to_read_task_id_.end() && idx_it->second == finish_id) {
                        inode_to_read_task_id_.erase(idx_it);
                    }
                }
                task_map_->Erase(finish_id);
            }
        }
    }

}  // namespace optical_node_manager
