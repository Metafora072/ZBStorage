#include "optical_node_manager.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
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

namespace optical_node_manager {
    volumemanager::ErrorCode ReadFileToStringMmap(const std::string& file_path,
                                                  std::string* out,
                                                  uint64_t offset,
                                                  uint64_t read_size,
                                                  bool* out_is_last);

    // 返回 "[YYYY-MM-DD HH:MM:SS.mmm]" 形式的本地时间字符串，用作日志前缀。
    static std::string NowLogTimestamp() {
        using namespace std::chrono;
        const auto now = system_clock::now();
        const auto now_t = system_clock::to_time_t(now);
        const auto ms_part = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
        std::tm tm_buf{};
        localtime_r(&now_t, &tm_buf);
        char time_buf[32];
        std::strftime(time_buf, sizeof(time_buf), "%F %T", &tm_buf);
        char out[40];
        std::snprintf(out, sizeof(out), "%s.%03ld", time_buf, static_cast<long>(ms_part.count()));
        return std::string(out);
    }

    // 输出 "[ts][prefix]" 前缀到 ostream，便于统一在 std::cerr 行首附带时间戳。
    static std::ostream& LogCerrTime(std::ostream& os, const char* prefix) {
        return os << "[" << NowLogTimestamp() << "][" << prefix << "] ";
    }

    OpticalNodeManager::OpticalNodeManager(uint64_t volume_size,
                                           double size_threshold,
                                           const std::string& root_dir,
                                           uint64_t capacity_in_images,
                                           uint8_t available_volume_id_count)
        : volume_manager_(volume_size, size_threshold),
          read_task_queue_(new WR_task::WRTaskQueue()),
          zip_task_queue_(new WR_task::WRTaskQueue()),
          burn_task_queue_(new WR_task::WRTaskQueue()),
          task_map_(new WR_task::WRTaskMap()),
          available_volume_ids() {
        root_dir_ = root_dir;
        capacity_in_images_ = capacity_in_images;
        available_volume_id_count_ = available_volume_id_count;
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

    // 占位实现（临时）：仅用于解除链接期的未定义符号，真实业务（按 target_node_id /
    // target_disk_id 从热数据节点下载文件 → 封装镜像 → 两次上报）尚未落地，
    // 任何请求都直接返回错误。Step 1 落地时替换本函数。
    volumemanager::ErrorCode OpticalNodeManager::SendArchiveMetadata(
        const SendArchiveMetadataRequest& request) {
        (void)request;
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }

    bool OpticalNodeManager::Run(const std::vector<uint64_t>& initial_available_volume_ids) {
        // 状态机守卫：RUNNING 幂等成功；INITIALIZING / STOPPING 重入拒绝。
        const std::string current_status = GetStatus();
        if (current_status == manager_status::kRunning) {
            // 经 SetStatus 发布原因指针；直接改 last_failure_reason_buf_ 会让
            // 已发布的 last_status_reason_ 指向重新分配前的旧缓冲。
            SetStatus(manager_status::kRunning,
                      std::string("phase=") +
                          start_failure_phase::kRunGuard +
                          " subphase=idempotent_already_running status=" + current_status);
            return true;
        }
        if (current_status == manager_status::kInitializing ||
            current_status == manager_status::kStopping) {
            std::string status_lower = current_status;
            for (auto& c : status_lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            // 同上：重入拒绝的原因也要经 SetStatus 发布。
            SetStatus(current_status,
                      std::string("phase=") +
                          start_failure_phase::kRunGuard +
                          " subphase=reentry_during_" + status_lower +
                          " status=" + current_status);
            return false;
        }
        SetStatus(manager_status::kInitializing, "Run() entered");

        // 初始 ID 灌入：按 FIFO 顺序取前 available_volume_id_count_ 个元素，超出数量丢弃。
        const size_t push_count = std::min<size_t>(
            initial_available_volume_ids.size(),
            available_volume_id_count_);
        for (size_t i = 0; i < push_count; ++i) {
            available_volume_ids.push(initial_available_volume_ids[i]);
        }

        if (!InitializeDir()) {
            // InitializeDir 已写子阶段到 last_failure_reason_buf_，这里拼顶层 phase。
            const std::string detailed = std::string("phase=") +
                start_failure_phase::kInitializeDir +
                " subphase=" + last_failure_reason_buf_;
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

        // 规范化 root_dir_ 并派生五个子目录。
        if (root_dir_.back() != '/') {
            root_dir_ += '/';
        }

        input_file_dir_ = root_dir_ + "input/";
        temp_dir_       = root_dir_ + "temp/";
        image_dir_      = root_dir_ + "image/";
        read_dir_       = root_dir_ + "read/";
        disc_sim_dir_   = root_dir_ + "disc_sim/";

        const std::string dirs[] = {
            root_dir_,
            input_file_dir_,
            temp_dir_,
            image_dir_,
            read_dir_,
            disc_sim_dir_,
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

        // 每次 InitializeDir 入口强制重建 cd_manager_ / image_dir_manager_：
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

        if (image_dir_manager_ != nullptr) {
            image_dir_manager_.reset();
        }
        try {
            image_dir_manager_ = std::make_unique<space_manager::ImageDirManager>(image_dir_);
            const volumemanager::ErrorCode rebuild_ret =
                image_dir_manager_->RebuildManagementTable();
            if (rebuild_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("ImageDirManager_Rebuild_failed code=")
                    + std::to_string(static_cast<int>(rebuild_ret));
                image_dir_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            const volumemanager::ErrorCode cap_ret =
                image_dir_manager_->SetCapacityInImages(capacity_in_images_);
            if (cap_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("ImageDirManager_SetCapacity_failed code=")
                    + std::to_string(static_cast<int>(cap_ret))
                    + " capacity=" + std::to_string(capacity_in_images_);
                image_dir_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
            // 注入模拟光盘库目录：Release / LRU 换出 / 刻录释放时 rename 到此处。
            const volumemanager::ErrorCode disc_sim_ret =
                image_dir_manager_->SetDiscSimDir(disc_sim_dir_);
            if (disc_sim_ret != volumemanager::ErrorCode::SUCCESS) {
                last_failure_reason_buf_ = std::string("ImageDirManager_SetDiscSimDir_failed code=")
                    + std::to_string(static_cast<int>(disc_sim_ret))
                    + " disc_sim_dir=" + disc_sim_dir_;
                image_dir_manager_.reset();
                rollback_volume_manager();
                RollbackCreatedDirs(created_dirs);
                return false;
            }
        } catch (const std::exception& e) {
            last_failure_reason_buf_ = std::string("ImageDirManager_init_failed what=") + e.what();
            image_dir_manager_.reset();
            rollback_volume_manager();
            RollbackCreatedDirs(created_dirs);
            return false;
        } catch (...) {
            last_failure_reason_buf_ = "ImageDirManager_init_failed what=<unknown>";
            image_dir_manager_.reset();
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
            last_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kStartWorkers +
                " subphase=cd_manager_null";
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

        if (!read_task_thread_.joinable()) {
            try {
                read_task_thread_ = std::thread(&OpticalNodeManager::ReadTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kStartWorkers +
                    " subphase=" + start_failure_phase::kThreadRead +
                    " what=" + e.what();
                return false;
            }
        }

        if (!zip_task_thread_.joinable()) {
            try {
                zip_task_thread_ = std::thread(&OpticalNodeManager::ZipTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kStartWorkers +
                    " subphase=" + start_failure_phase::kThreadZip +
                    " what=" + e.what();
                return false;
            }
        }

        if (!burn_task_thread_.joinable()) {
            try {
                burn_task_thread_ = std::thread(&OpticalNodeManager::BurnTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kStartWorkers +
                    " subphase=" + start_failure_phase::kThreadBurn +
                    " what=" + e.what();
                return false;
            }
        }

        if (!cleanup_thread_.joinable()) {
            try {
                cleanup_thread_ = std::thread(&OpticalNodeManager::CleanupTaskProcessor, this);
            } catch (const std::system_error& e) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kStartWorkers +
                    " subphase=StartBackgroundWorkers.thread=cleanup" +
                    " what=" + e.what();
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

        if (read_task_queue_ != nullptr) {
            read_task_queue_->Close();
        }

        if (read_task_thread_.joinable()) {
            read_task_thread_.join();
        }

        // 必须在 cd_manager_->Stop() 之前关闭 zip_task_queue_，OnCDReadComplete 仍在写入。
        if (zip_task_queue_ != nullptr) {
            zip_task_queue_->Close();
        }

        if (zip_task_thread_.joinable()) {
            zip_task_thread_.join();
        }

        if (burn_task_queue_ != nullptr) {
            burn_task_queue_->Close();
        }

        if (burn_task_thread_.joinable()) {
            burn_task_thread_.join();
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

        delete read_task_queue_;
        delete zip_task_queue_;
        delete burn_task_queue_;
        delete task_map_;
    }

    uint64_t OpticalNodeManager::GenerateTaskId() {
        return next_task_id_.fetch_add(1, std::memory_order_relaxed);
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
            last_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kReadFile +
                " subphase=not_ready status=" + GetStatus();
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
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=invalid_parameter out=null task_id=" +
                std::to_string(task_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受重读；其它状态拒绝。
        if (GetStatus() != manager_status::kRunning) {
            last_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=not_ready status=" + GetStatus();
            return volumemanager::ErrorCode::MANAGER_NOT_READY;
        }

        auto task_opt = task_map_->Get(task_id);
        if (!task_opt.has_value()) {
            // 任务不存在：可能已被 Erase（消费过一次）或 task_id 传错。
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=task_not_found task_id=" +
                std::to_string(task_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_NOT_FOUND;
        }
        const WR_task::WRTask& task = *task_opt;
        if (task.type != WR_task::WRTaskType::READ) {
            // type 不匹配：把 WRITE 任务的 task_id 传给了 ReadObjectByTaskId。
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=invalid_type task_id=" +
                std::to_string(task_id) +
                " type=" + std::to_string(static_cast<int>(task.type));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }
        // FAILED → 返回细分错误码；FINISH（已读完）→ TASK_ALREADY_FINISH；
        // READY 前的中间态 → TASK_NOT_FINISH；READY → 走 mmap 分片读取，末片置 FINISH。
        if (task.state == WR_task::WRTaskState::FAILED) {
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=task_failed task_id=" +
                std::to_string(task_id) +
                " code=" + volumemanager::GetErrorMessage(task.last_error_code) +
                " code_int=" + std::to_string(static_cast<int>(task.last_error_code)) +
                " attempt=" + std::to_string(task.attempt_count) +
                "/" + std::to_string(attempt_count_max_) +
                " detail=" + task.last_error_detail;
            last_failure_reason_buf_ = detail;
            return task.last_error_code == volumemanager::ErrorCode::SUCCESS
                ? volumemanager::ErrorCode::READ_FAILED
                : task.last_error_code;
        }
        if (task.state == WR_task::WRTaskState::FINISH) {
            // 任务已读完（读产物已 unlink），不可再读。
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=task_already_finish task_id=" +
                std::to_string(task_id);
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_ALREADY_FINISH;
        }
        if (!task.isReadTaskReady()) {
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=task_not_finish task_id=" +
                std::to_string(task_id) +
                " state=" + std::to_string(static_cast<int>(task.state));
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::TASK_NOT_FINISH;
        }
        // 分片读取 [offset, offset+read_size)；helper 内部按 offset+read_size>=file_size 判定末片。
        bool is_last = false;
        volumemanager::ErrorCode ret = ReadFileToStringMmap(task.file_path, out, offset, read_size, &is_last);
        // mmap 阶段失败：不改 task_map_，FINISH 任务的 IO 失败由调用方决定下一步。
        if (ret != volumemanager::ErrorCode::SUCCESS) {
            const std::string detail = std::string("phase=") +
                start_failure_phase::kReadObjectByTaskId +
                " subphase=mmap_failed task_id=" +
                std::to_string(task_id) +
                " code=" + volumemanager::GetErrorMessage(ret) +
                " code_int=" + std::to_string(static_cast<int>(ret)) +
                " file_path=" + task.file_path;
            last_failure_reason_buf_ = detail;
            return ret;
        }
        // 服务端按 is_last 判定末片：unlink 读产物 + 置 FINISH（由清理线程稍后 Erase）；
        // 非末片保留文件供后续分片继续读。
        if (is_last) {
            if (::unlink(task.file_path.c_str()) != 0 && errno != ENOENT) {
                last_sidecar_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kReadObjectByTaskId +
                    " subphase=post_success_unlink_failed code=" +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " task_id=" + std::to_string(task_id) +
                    " file_path=" + task.file_path +
                    " errno=" + std::to_string(errno);
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
                const std::string detail = std::string("phase=") +
                    start_failure_phase::kReadObjectByInodeId +
                    " subphase=inode_not_found inode_id=" + inode_id;
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
            const std::string detail = std::string("phase=") +
                start_failure_phase::kWriteObject +
                " subphase=invalid_parameter data=null data_size=" +
                std::to_string(data_size) +
                " inode_id=" + inode_id;
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // 仅 RUNNING 接受写数据；其它状态拒绝。
        if (GetStatus() != manager_status::kRunning) {
            const std::string detail = std::string("phase=") +
                start_failure_phase::kWriteObject +
                " subphase=not_ready status=" + GetStatus();
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
                    const std::string detail = std::string("phase=") +
                        start_failure_phase::kWriteObject +
                        " subphase=open_failed code=" +
                        volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                        " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                        " path=" + output_path;
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
                const std::string detail = std::string("phase=") +
                    start_failure_phase::kWriteObject +
                    " subphase=open_failed code=" +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " path=" + output_path;
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
            const std::string detail = std::string("phase=") +
                start_failure_phase::kWriteObject +
                " subphase=invalid_parameter total_size_mismatch expected=" +
                std::to_string(state.total_size) +
                " got=" + std::to_string(total_size) +
                " inode_id=" + inode_id;
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        // offset 必须严格等于已累积字节数；不接受乱序或重复写。
        if (offset != state.bytes_received) {
            const std::string detail = std::string("phase=") +
                start_failure_phase::kWriteObject +
                " subphase=invalid_parameter offset_mismatch expected=" +
                std::to_string(state.bytes_received) +
                " got=" + std::to_string(offset) +
                " inode_id=" + inode_id;
            last_failure_reason_buf_ = detail;
            return volumemanager::ErrorCode::INVALID_PARAMETER;
        }

        if (data_size > 0) {
            // in+out 打开（已截断创建）；seekp 定位到 offset。
            std::fstream fs(output_path,
                            std::ios::binary | std::ios::in | std::ios::out);
            if (!fs) {
                const std::string detail = std::string("phase=") +
                    start_failure_phase::kWriteObject +
                    " subphase=open_failed code=" +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " path=" + output_path;
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::IO_ERROR;
            }
            fs.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
            if (!fs.good()) {
                fs.close();
                const std::string detail = std::string("phase=") +
                    start_failure_phase::kWriteObject +
                    " subphase=seek_failed offset=" +
                    std::to_string(offset) +
                    " path=" + output_path;
                last_failure_reason_buf_ = detail;
                return volumemanager::ErrorCode::IO_ERROR;
            }
            fs.write(data, static_cast<std::streamsize>(data_size));
            if (!fs.good()) {
                fs.close();
                // 写失败：清状态 + 删残留文件；删失败走 sidecar。
                in_progress_writes_.erase(it);
                const std::string detail = std::string("phase=") +
                    start_failure_phase::kWriteObject +
                    " subphase=write_failed code=" +
                    volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                    " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                    " offset=" + std::to_string(offset) +
                    " data_size=" + std::to_string(data_size) +
                    " path=" + output_path;
                last_failure_reason_buf_ = detail;
                if (::unlink(output_path.c_str()) != 0 && errno != ENOENT) {
                    last_sidecar_failure_reason_buf_ = std::string("phase=") +
                        start_failure_phase::kWriteObject +
                        " subphase=residual_unlink_failed code=" +
                        volumemanager::GetErrorMessage(volumemanager::ErrorCode::IO_ERROR) +
                        " code_int=" + std::to_string(static_cast<int>(volumemanager::ErrorCode::IO_ERROR)) +
                        " path=" + output_path +
                        " errno=" + std::to_string(errno);
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
        // cd_manager_sim 当前未提供 success 字段，默认视为成功（TODO-08 留口）。
        if (!MarkTaskState(event.task_id, WR_task::WRTaskState::LOADED)) {
            // MarkTaskState 失败：任务已被 Erase / 终态；任务不存在时记录排查信号后退出。
            auto existing = task_map_->Get(event.task_id);
            if (!existing.has_value()) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kOnCDReadComplete +
                    " subphase=unknown_task_id task_id=" +
                    std::to_string(event.task_id);
            }
            return;
        }

        // 镜像从 disc_sim_dir_ 转移到 image_dir_/ 并登记到 image_dir_manager_（扇平布局）。
        // 与 WRITE 链路 (PackVolume + MoveFrom) 形成对称；失败走 sidecar，不阻塞 LOADED 推进。
        if (image_dir_manager_ != nullptr) {
            const std::string src_image_path =
                disc_sim_dir_ + "volume_" + event.volume_id + ".vimg";

            uint64_t read_volume_id = 0;
            std::string read_basename;
            const volumemanager::ErrorCode parse_ret =
                image_dir_manager_->ParseVolumeFile(src_image_path, read_volume_id, read_basename);
            if (parse_ret == volumemanager::ErrorCode::SUCCESS) {
                const volumemanager::ErrorCode move_ret =
                    image_dir_manager_->MoveFrom(src_image_path,
                                                 space_manager::ImageCategory::READ);
                if (move_ret != volumemanager::ErrorCode::SUCCESS) {
                    // 副作用失败：不影响 LOADED 状态推进，sidecar 记录便于运维排查。
                    last_sidecar_failure_reason_buf_ = std::string("phase=") +
                        start_failure_phase::kOnCDReadComplete +
                        " subphase=MoveFrom_failed code=" +
                        volumemanager::GetErrorMessage(move_ret) +
                        " code_int=" + std::to_string(static_cast<int>(move_ret)) +
                        " src=" + src_image_path +
                        " dst=image_dir_/" + read_basename +
                        " task_id=" + std::to_string(event.task_id);
                }
            } else {
                // 解析失败：兜底未来 cd_manager 落盘命名变更；sidecar 记录。
                last_sidecar_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kOnCDReadComplete +
                    " subphase=ParseVolumeFile_failed code=" +
                    volumemanager::GetErrorMessage(parse_ret) +
                    " code_int=" + std::to_string(static_cast<int>(parse_ret)) +
                    " src=" + src_image_path +
                    " task_id=" + std::to_string(event.task_id);
            }
        } else {
            // image_dir_manager_ 未就绪：后续 MountVolume 大概率 FILE_NOT_FOUND；仅 sidecar。
            last_sidecar_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kOnCDReadComplete +
                " subphase=image_dir_manager_null task_id=" +
                std::to_string(event.task_id);
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
    }

    void OpticalNodeManager::ReadTaskProcessor(){
        do{
            auto opt_task = read_task_queue_->Pop();
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

            if (!SubmitTaskToCDManager(*full_task)) {
                // 失败根因：cd_manager 已停 / cd_manager 内部拒绝。
                if (cd_manager_ == nullptr) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   "SubmitTaskToCDManager_failed cd_manager_null");
                } else if (ShouldGiveUpRetry(task.task_id)) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   std::string("attempt_exceeded limit=") +
                                       std::to_string(attempt_count_max_) +
                                       " reason=SubmitReadTask_rejected_by_cd_manager");
                } else {
                    // 未超限：保留 WAITING 重试，写失败痕迹但不切终态。
                    auto current = task_map_->Get(task.task_id);
                    if (current.has_value()) {
                        current->ResetForRetry();
                        current->state = WR_task::WRTaskState::WAITING;
                        current->last_error_code = volumemanager::ErrorCode::READ_FAILED;
                        current->last_error_detail = "SubmitReadTask_rejected_by_cd_manager";
                        task_map_->Update(std::move(*current));
                    }
                    read_task_queue_->Push(WR_task::WRTaskShort(task.task_id, WR_task::WRTaskType::READ));
                }
            }
        } while(true);
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

        const WR_task::WRTaskState prev_state = current->state;
        current->state = new_state;
        if (!task_map_->Update(std::move(*current))) {
            return false;
        }
        // 仅在真正发生变迁时打印日志。
        LogCerrTime(std::cerr, "Info")
                  << "MarkTaskState task_id=" << task_id
                  << " state=" << WR_task::WRTaskStateToString(prev_state)
                  << "->" << WR_task::WRTaskStateToString(new_state) << std::endl;
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
        LogCerrTime(std::cerr, "Error")
                  << "MarkTaskFailed " << last_failure_reason_buf_ << std::endl;
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

    bool OpticalNodeManager::SubmitTaskToCDManager(const WR_task::WRTask& task) {
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
                                   "inode_id_invalid inode_id=" + full_task->inode_id);
                    continue;
                }
                const uint64_t inode_id_num = full_task->inode_id_num;

                // 搭便车预查询：volume_read_index_ 中已挂起的同卷读任务，
                // 当前任务登记后置 WAITING，等待批量唤醒。
                if (volume_read_index_.ContainsVolume(full_task->volume_id)) {
                    volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                    full_task->ResetForRetry();
                    full_task->state = WR_task::WRTaskState::WAITING;
                    task_map_->Update(std::move(*full_task));
                    continue;
                }

                // 读互斥区间：与 ImageDirManager 的写路径（LRU 换出 / MoveFrom）互斥。
                struct ReadLockGuard {
                    space_manager::ImageDirManager* mgr;
                    ~ReadLockGuard() { if (mgr) mgr->ReadUnlock(); }
                };
                ReadLockGuard read_lock_guard{image_dir_manager_.get()};
                if (image_dir_manager_ != nullptr) {
                    image_dir_manager_->ReadLock();
                }

                // 命中后 Touch 推迟该卷的 LRU 淘汰；解析失败静默跳过（best-effort）。
                auto touch_volume = [this](const std::string& volume_id_str) {
                    if (image_dir_manager_ == nullptr) return;
                    uint64_t parsed_volume_id = 0;
                    std::string parsed_basename;
                    const std::string volume_image_path =
                        image_dir_ + "volume_" + volume_id_str + ".vimg";
                    if (image_dir_manager_->ParseVolumeFile(volume_image_path,
                                                            parsed_volume_id,
                                                            parsed_basename)
                            == volumemanager::ErrorCode::SUCCESS) {
                        image_dir_manager_->Touch(parsed_volume_id);
                    }
                };

                std::string output_path;
                volumemanager::ErrorCode ret = volume_manager_.ReadFile(inode_id_num, output_path);

                if (ret == volumemanager::ErrorCode::SUCCESS) {
                    touch_volume(full_task->volume_id);
                    full_task->file_path = std::move(output_path);
                    full_task->state = WR_task::WRTaskState::READY;
                    task_map_->Update(std::move(*full_task));
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
                                           std::string("attempt_exceeded limit=") +
                                               std::to_string(attempt_count_max_) +
                                               " volume_id=" + full_task->volume_id);
                            continue;
                        }
                        volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                        full_task->ResetForRetry();
                        full_task->state = WR_task::WRTaskState::WAITING;
                        task_map_->Update(std::move(*full_task));
                        read_task_queue_->Push(WR_task::WRTaskShort(full_task->task_id, WR_task::WRTaskType::READ));
                        continue;
                    } else if (mount_ret == volumemanager::ErrorCode::SUCCESS) {
                        output_path.clear();
                        ret = volume_manager_.ReadFile(inode_id_num, output_path);
                        if (ret == volumemanager::ErrorCode::SUCCESS) {
                            touch_volume(full_task->volume_id);
                            full_task->file_path = std::move(output_path);
                            full_task->state = WR_task::WRTaskState::READY;
                            task_map_->Update(std::move(*full_task));
                            continue;
                        }
                        // Mount 成功但 inode 缺席：真实业务错误，重试不会变 SUCCESS。
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       std::string("subphase=read_after_mount_failed ret=") +
                                           std::to_string(static_cast<int>(ret)) +
                                           " ret_code=" + volumemanager::GetErrorMessage(ret) +
                                           " volume_id=" + full_task->volume_id +
                                           " inode_id_num=" + std::to_string(inode_id_num));
                        continue;
                    } else {
                        // 不可恢复的 Mount 错误（IO_ERROR / INVALID_VOLUME_FORMAT / INVALID_VOLUME_ID）。
                        const std::string detail =
                            std::string("subphase=mount_volume_failed type=") +
                            volumemanager::GetErrorMessage(mount_ret) +
                            " code=" + std::to_string(static_cast<int>(mount_ret)) +
                            " volume_id=" + full_task->volume_id;
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
                    const std::string volume_image_path =
                        image_dir_ + "volume_" + full_task->volume_id + ".vimg";
                    const volumemanager::ErrorCode parse_ret =
                        image_dir_manager_ != nullptr
                            ? image_dir_manager_->ParseVolumeFile(volume_image_path,
                                                                  parsed_volume_id,
                                                                  parsed_basename)
                            : volumemanager::ErrorCode::INVALID_PATH;
                    if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       "subphase=io_error_parse_failed volume_id=" +
                                           full_task->volume_id);
                        continue;
                    }
                    if (ShouldGiveUpRetry(full_task->task_id)) {
                        MarkTaskFailed(full_task->task_id,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       std::string("attempt_exceeded limit=") +
                                           std::to_string(attempt_count_max_) +
                                           " subphase=io_error_evicted" +
                                           " volume_id=" + full_task->volume_id);
                        continue;
                    }
                    // UnmountVolume 对未挂载卷返回 VOLUME_NOT_FOUND，视为幂等 no-op。
                    (void)volume_manager_.UnmountVolume(parsed_volume_id);
                    volume_read_index_.AddTask(full_task->volume_id, full_task->task_id);
                    full_task->ResetForRetry();
                    full_task->state = WR_task::WRTaskState::WAITING;
                    task_map_->Update(std::move(*full_task));
                    read_task_queue_->Push(
                        WR_task::WRTaskShort(full_task->task_id, WR_task::WRTaskType::READ));
                    continue;
                }

                // 兜底：未知读失败。
                MarkTaskFailed(full_task->task_id,
                               volumemanager::ErrorCode::READ_FAILED,
                               "subphase=read_failed ret=" +
                                   std::to_string(static_cast<int>(ret)) +
                                   " ret_code=" + volumemanager::GetErrorMessage(ret) +
                                   " inode_id_num=" + std::to_string(inode_id_num));
                continue;
            } else if (full_task->type == WR_task::WRTaskType::READ_BATCH_BY_VOLUME) {
                // 按镜像批量读：镜像就绪后集中处理该卷下所有挂起读任务。
                if (full_task->isTaskTerminal()) {
                    continue;
                }
                if (full_task->volume_id.empty()) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   "subphase=batch_volume_id_empty");
                    continue;
                }

                struct ReadLockGuard {
                    space_manager::ImageDirManager* mgr;
                    ~ReadLockGuard() { if (mgr) mgr->ReadUnlock(); }
                };
                ReadLockGuard read_lock_guard{image_dir_manager_.get()};
                if (image_dir_manager_ != nullptr) {
                    image_dir_manager_->ReadLock();
                }

                const volumemanager::ErrorCode mount_ret =
                    volume_manager_.MountVolume(full_task->volume_id);
                if (mount_ret == volumemanager::ErrorCode::FILE_NOT_FOUND) {
                    // 镜像被 LRU 换出：委托 cd_manager 重新生产；本 batch 一次性 FINISH。
                    if (cd_manager_ != nullptr) {
                        cd_manager_sim::ReadRequest req;
                        req.task_id = full_task->task_id;
                        req.disk_id.clear();
                        req.volume_id = full_task->volume_id;
                        req.inode_id.clear();
                        req.read_size_bytes = volume_manager_.volume_size_;
                        (void)cd_manager_->SubmitReadTask(req);
                    }
                    MarkTaskState(full_task->task_id, WR_task::WRTaskState::FINISH);
                    continue;
                }
                if (mount_ret != volumemanager::ErrorCode::SUCCESS &&
                    mount_ret != volumemanager::ErrorCode::INVALID_VOLUME_ID) {
                    // 不可恢复的 Mount 错误：整批 FAILED。
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::READ_FAILED,
                                   std::string("subphase=batch_mount_failed type=") +
                                       volumemanager::GetErrorMessage(mount_ret) +
                                       " code_int=" +
                                       std::to_string(static_cast<int>(mount_ret)) +
                                       " volume_id=" + full_task->volume_id);
                    continue;
                }
                // SUCCESS / INVALID_VOLUME_ID：复用挂载态处理子任务。

                // 整卷 LRU 推前：推迟该卷的淘汰窗口。
                if (image_dir_manager_ != nullptr) {
                    uint64_t parsed_volume_id = 0;
                    std::string parsed_basename;
                    const std::string volume_image_path =
                        image_dir_ + "volume_" + full_task->volume_id + ".vimg";
                    if (image_dir_manager_->ParseVolumeFile(volume_image_path,
                                                             parsed_volume_id,
                                                             parsed_basename)
                        == volumemanager::ErrorCode::SUCCESS) {
                        image_dir_manager_->Touch(parsed_volume_id);
                    }
                }

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
                                       "subphase=batch_sub_inode_id_invalid" +
                                           std::string(" inode_id=") + sub->inode_id);
                        continue;
                    }
                    std::string output_path;
                    const volumemanager::ErrorCode sub_ret =
                        volume_manager_.ReadFile(sub->inode_id_num, output_path);
                    if (sub_ret == volumemanager::ErrorCode::SUCCESS) {
                        sub->file_path = std::move(output_path);
                        sub->state = WR_task::WRTaskState::READY;
                        task_map_->Update(std::move(*sub));
                    } else {
                        MarkTaskFailed(sub_tid,
                                       volumemanager::ErrorCode::READ_FAILED,
                                       "subphase=batch_sub_read_failed ret=" +
                                           std::to_string(static_cast<int>(sub_ret)) +
                                           " ret_code=" +
                                           volumemanager::GetErrorMessage(sub_ret) +
                                           " inode_id_num=" +
                                           std::to_string(sub->inode_id_num));
                    }
                }

                // batch 任务自身一次性 FINISH。
                MarkTaskState(full_task->task_id, WR_task::WRTaskState::FINISH);
                continue;
            } else if (full_task->type == WR_task::WRTaskType::WRITE) {
                // 写任务处理：压缩 → 删原始 → 扫描是否触发封装 → 转移 + 构造 BURN 任务。
                if (full_task->isTaskTerminal()) {
                    continue;
                }
                if (full_task->inode_id_num == WR_task::WRTask::INVALID_INODE_ID) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::WRITE_FAILED,
                                   "inode_id_invalid inode_id=" + full_task->inode_id);
                    continue;
                }
                const uint64_t inode_id_num = full_task->inode_id_num;

                // volume_id 由 available_volume_ids 队首分配；空时退回 0。
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
                                   "AddFileToCollect_failed " + keep_detail);
                    continue;
                }

                // 2. 压缩成功 → 删原始文件。
                const std::string original_file_path = input_file_dir_ + full_task->file_path;
                ::unlink(original_file_path.c_str());

                // 3. 状态置 FINISH（压缩完成）。
                full_task->state = WR_task::WRTaskState::FINISH;
                task_map_->Update(std::move(*full_task));

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

                // 5.5 从 temp_dir_ 剩余 temp_*.compressed 反推本次被打包的 inode_id 并输出。
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
                if (image_dir_manager_ == nullptr) {
                    const std::string detail = std::string("phase=") +
                        start_failure_phase::kZipTaskProcessor +
                        " subphase=image_dir_manager_null ret=" +
                        std::to_string(static_cast<int>(volumemanager::ErrorCode::PACK_FAILED)) +
                        " code=" + volumemanager::GetErrorMessage(volumemanager::ErrorCode::PACK_FAILED) +
                        " output_path=" + packed_volume_path;
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }
                uint64_t packed_volume_id = 0;
                std::string packed_basename;
                const volumemanager::ErrorCode parse_ret =
                    image_dir_manager_->ParseVolumeFile(packed_volume_path, packed_volume_id, packed_basename);
                if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
                    const std::string detail = std::string("phase=") +
                        start_failure_phase::kZipTaskProcessor +
                        " subphase=ParseVolumeFile_failed ret=" +
                        std::to_string(static_cast<int>(parse_ret)) +
                        " code=" + volumemanager::GetErrorMessage(parse_ret) +
                        " code_int=" + std::to_string(static_cast<int>(parse_ret)) +
                        " output_path=" + packed_volume_path;
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }
                const volumemanager::ErrorCode move_ret =
                    image_dir_manager_->MoveFrom(packed_volume_path,
                                                 space_manager::ImageCategory::WRITE);
                if (move_ret != volumemanager::ErrorCode::SUCCESS) {
                    const std::string detail = std::string("phase=") +
                        start_failure_phase::kZipTaskProcessor +
                        " subphase=MoveFrom_failed ret=" +
                        std::to_string(static_cast<int>(move_ret)) +
                        " code=" + volumemanager::GetErrorMessage(move_ret) +
                        " code_int=" + std::to_string(static_cast<int>(move_ret)) +
                        " volume_id=" + std::to_string(packed_volume_id) +
                        " src=" + packed_volume_path;
                    MarkTaskFailed(full_task->task_id, volumemanager::ErrorCode::PACK_FAILED, detail);
                    continue;
                }

                // 7. 用 image_dir_/ 下的新路径构造 BURN 任务。
                const std::string final_volume_path = image_dir_ + packed_basename;

                // 8. 构造 BURN 任务并入队；volume_id 来自实际产出。
                const uint64_t generated_burn_task_id = GenerateTaskId();
                WR_task::WRTask new_burn_task(generated_burn_task_id, WR_task::WRTaskType::BURN);
                const std::string packed_volume_id_str = std::to_string(packed_volume_id);
                new_burn_task.SetBurnTask(std::string(), packed_volume_id_str, final_volume_path);

                // 9. 入 task_map_ + burn_task_queue_。
                task_map_->Insert(std::move(new_burn_task));
                burn_task_queue_->Push(WR_task::WRTaskShort(generated_burn_task_id, WR_task::WRTaskType::BURN));

                // 10. 上报 MDS 更新元数据（TBD）。
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

        // cerr 输出：先 volume_id，再本次打包的 inode_id 列表。
        std::cerr << "[pack] volume_id=" << volume_id << " packed_inode_ids=[";
        for (size_t i = 0; i < packed_inodes.size(); ++i) {
            if (i > 0) {
                std::cerr << ",";
            }
            std::cerr << packed_inodes[i];
        }
        std::cerr << "]" << std::endl;
    }

    void OpticalNodeManager::OnCDBurnComplete(const cd_manager_sim::BurnCompleteEvent& event) {
        // cd_manager_sim 当前未提供 success 字段，默认视为成功（TODO-08 留口）。
        if (!MarkTaskState(event.task_id, WR_task::WRTaskState::FINISH)) {
            auto existing = task_map_->Get(event.task_id);
            if (!existing.has_value()) {
                last_failure_reason_buf_ = std::string("phase=") +
                    start_failure_phase::kOnCDBurnComplete +
                    " subphase=unknown_task_id task_id=" +
                    std::to_string(event.task_id);
            }
            return;
        }

        // 烧录完成 → 释放对应写镜像；副作用失败不影响 FINISH 主语义。
        if (image_dir_manager_ == nullptr) {
            last_sidecar_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kOnCDBurnComplete +
                " subphase=image_dir_manager_null task_id=" +
                std::to_string(event.task_id);
            return;
        }
        if (event.image_path.empty()) {
            last_sidecar_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kOnCDBurnComplete +
                " subphase=empty_image_path task_id=" +
                std::to_string(event.task_id);
            return;
        }
        uint64_t burned_volume_id = 0;
        std::string basename;
        const volumemanager::ErrorCode parse_ret =
            image_dir_manager_->ParseVolumeFile(event.image_path, burned_volume_id, basename);
        if (parse_ret != volumemanager::ErrorCode::SUCCESS) {
            last_sidecar_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kOnCDBurnComplete +
                " subphase=ParseVolumeFile_failed code=" +
                volumemanager::GetErrorMessage(parse_ret) +
                " code_int=" + std::to_string(static_cast<int>(parse_ret)) +
                " image_path=" + event.image_path +
                " task_id=" + std::to_string(event.task_id);
            return;
        }
        const volumemanager::ErrorCode remove_ret =
            image_dir_manager_->RemoveWriteImage(burned_volume_id);
        if (remove_ret != volumemanager::ErrorCode::SUCCESS) {
            // 副作用失败走 sidecar，不改 BURN 主语义。
            last_sidecar_failure_reason_buf_ = std::string("phase=") +
                start_failure_phase::kOnCDBurnComplete +
                " subphase=RemoveWriteImage_failed code=" +
                volumemanager::GetErrorMessage(remove_ret) +
                " code_int=" + std::to_string(static_cast<int>(remove_ret)) +
                " volume_id=" + std::to_string(burned_volume_id) +
                " image_path=" + event.image_path +
                " task_id=" + std::to_string(event.task_id);
        }
    }

    bool OpticalNodeManager::SubmitBurnTaskToCDManager(const WR_task::WRTask& task) {
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

    void OpticalNodeManager::BurnTaskProcessor() {
        do {
            auto opt_task = burn_task_queue_->Pop();
            if (!opt_task.has_value()) {
                break;
            }
            WR_task::WRTaskShort task = *opt_task;

            if (!MarkTaskState(task.task_id, WR_task::WRTaskState::BURNING)) {
                continue;
            }

            auto full_task = task_map_->Get(task.task_id);
            if (!full_task.has_value()) {
                continue;
            }

            if (!SubmitBurnTaskToCDManager(*full_task)) {
                // 提交失败：超限 → 强制 FAILED；未超限 → 保留 WAITING 重试。
                if (ShouldGiveUpRetry(task.task_id)) {
                    MarkTaskFailed(task.task_id,
                                   volumemanager::ErrorCode::BURN_FAILED,
                                   std::string("attempt_exceeded limit=") +
                                       std::to_string(attempt_count_max_) +
                                       " volume_id=" + full_task->volume_id +
                                       " image_path=" + full_task->file_path);
                } else {
                    auto current = task_map_->Get(task.task_id);
                    if (current.has_value()) {
                        current->ResetForRetry();
                        current->state = WR_task::WRTaskState::WAITING;
                        current->last_error_code = volumemanager::ErrorCode::BURN_FAILED;
                        current->last_error_detail = "SubmitBurnTask_rejected_by_cd_manager volume_id=" +
                                                     full_task->volume_id +
                                                     " image_path=" + full_task->file_path;
                        task_map_->Update(std::move(*current));
                    }
                    burn_task_queue_->Push(WR_task::WRTaskShort(task.task_id, WR_task::WRTaskType::BURN));
                }
            }
        } while (!stop_requested_.load(std::memory_order_relaxed));
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
                    std::lock_guard<std::mutex> lock(inode_to_read_task_id_mutex_);
                    auto idx_it = inode_to_read_task_id_.find(finish_task->inode_id);
                    if (idx_it != inode_to_read_task_id_.end() && idx_it->second == finish_id) {
                        inode_to_read_task_id_.erase(idx_it);
                    }
                }
                task_map_->Erase(finish_id);
            }
            if (!finish_ids.empty()) {
                LogCerrTime(std::cerr, "Info")
                          << "CleanupTaskProcessor removed " << finish_ids.size()
                          << " FINISH task(s), remaining=" << task_map_->Size() << std::endl;
            }
        }
    }

}  // namespace optical_node_manager
