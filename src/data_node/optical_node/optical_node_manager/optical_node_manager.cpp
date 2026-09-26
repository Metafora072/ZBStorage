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
#include <space_manager/sha256.h>

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

    // 循环写直到写满 len 字节；处理短写与 EINTR。失败返回 false。
    static bool WriteAll(int fd, const char* data, size_t len) {
        size_t written = 0;
        while (written < len) {
            const ssize_t n = ::write(fd, data + written, len - written);
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

    // 顺序写入 count 个零字节（光盘各区域与镜像的对齐填充）。
    static bool WriteZeros(int fd, uint64_t count) {
        static const char kZeros[4096] = {0};
        while (count > 0) {
            const size_t chunk =
                (count < sizeof(kZeros)) ? static_cast<size_t>(count) : sizeof(kZeros);
            if (!WriteAll(fd, kZeros, chunk)) {
                return false;
            }
            count -= chunk;
        }
        return true;
    }

    // 把 src_path 的全部内容顺序写入 fd，并补零到 padded_size 字节。
    // 源文件实际大小与 expected_size 不一致（写入期间被改动）时返回 false。
    static bool CopyFileAndPadToFd(int fd,
                                   const std::string& src_path,
                                   uint64_t expected_size,
                                   uint64_t padded_size) {
        const int src_fd = ::open(src_path.c_str(), O_RDONLY);
        if (src_fd < 0) {
            return false;
        }
        std::vector<char> buffer(1u << 20);
        uint64_t copied = 0;
        bool ok = true;
        for (;;) {
            const ssize_t n = ::read(src_fd, buffer.data(), buffer.size());
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                ok = false;
                break;
            }
            if (n == 0) {
                break;
            }
            if (!WriteAll(fd, buffer.data(), static_cast<size_t>(n))) {
                ok = false;
                break;
            }
            copied += static_cast<uint64_t>(n);
        }
        ::close(src_fd);
        if (!ok || copied != expected_size || padded_size < expected_size) {
            return false;
        }
        return WriteZeros(fd, padded_size - expected_size);
    }

    // 读取元数据区编码的镜像清单：DiscMetaHeader + entry_count 条定长记录。
    // 文件不可读、大小与头部声明不一致、任一条记录不合法时返回 false 并清空输出。
    static bool ReadDiscMetaFile(const std::string& path,
                                 std::vector<DiscImageEntry>& out_entries) {
        out_entries.clear();

        struct stat st {};
        if (::stat(path.c_str(), &st) != 0 || st.st_size <= 0) {
            return false;
        }
        std::vector<uint8_t> buffer(static_cast<size_t>(st.st_size));
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            return false;
        }
        size_t read_total = 0;
        bool read_ok = true;
        while (read_total < buffer.size()) {
            const ssize_t n = ::read(fd, buffer.data() + read_total, buffer.size() - read_total);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                read_ok = false;
                break;
            }
            if (n == 0) {
                break;
            }
            read_total += static_cast<size_t>(n);
        }
        ::close(fd);
        if (!read_ok || read_total != buffer.size()) {
            return false;
        }

        DiscMetaHeader header;
        if (!DiscMetaHeader::Parse(buffer.data(), buffer.size(), header)) {
            return false;
        }
        if (DiscMetaAreaSize(header.entry_count) != buffer.size()) {
            return false;
        }

        out_entries.reserve(header.entry_count);
        for (uint32_t i = 0; i < header.entry_count; ++i) {
            DiscImageEntry entry;
            const size_t record_offset =
                DISC_META_HEADER_SIZE + static_cast<size_t>(i) * DISC_IMAGE_RECORD_SIZE;
            if (!DiscImageEntry::Parse(buffer.data() + record_offset, DISC_IMAGE_RECORD_SIZE,
                                       entry)) {
                out_entries.clear();
                return false;
            }
            out_entries.push_back(entry);
        }
        return true;
    }

    // 解析十进制无符号整数串（要求非空且全为数字）；失败返回 false。
    static bool ParseUint64Decimal(const std::string& text, uint64_t& out) {
        if (text.empty()) {
            return false;
        }
        for (char c : text) {
            if (c < '0' || c > '9') {
                return false;
            }
        }
        try {
            size_t consumed = 0;
            const unsigned long long value = std::stoull(text, &consumed);
            if (consumed != text.size()) {
                return false;
            }
            out = static_cast<uint64_t>(value);
            return true;
        } catch (const std::exception&) {
            return false;
        }
    }

    // 从光盘文件中复制出 [offset, offset+size) 区间，写入 dst_path（O_TRUNC 新建）。
    // 光盘文件缺失、越界或读写失败时返回 false 并清理半成品。
    static bool ExtractRangeFromDisc(const std::string& disc_path,
                                     const std::string& dst_path,
                                     uint64_t offset,
                                     uint64_t size) {
        if (size == 0) {
            return false;
        }
        const int src_fd = ::open(disc_path.c_str(), O_RDONLY);
        if (src_fd < 0) {
            return false;
        }
        struct stat st {};
        if (::fstat(src_fd, &st) != 0 || st.st_size <= 0 ||
            offset + size > static_cast<uint64_t>(st.st_size)) {
            // 元数据记录的区间超出光盘文件范围：不读、不写半成品。
            ::close(src_fd);
            return false;
        }

        const int dst_fd = ::open(dst_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (dst_fd < 0) {
            ::close(src_fd);
            return false;
        }

        std::vector<char> buffer(1u << 20);
        uint64_t remaining = size;
        bool ok = true;
        while (remaining > 0) {
            const size_t want =
                (remaining < buffer.size()) ? static_cast<size_t>(remaining) : buffer.size();
            const ssize_t n = ::pread(src_fd, buffer.data(), want,
                                      static_cast<off_t>(offset + (size - remaining)));
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                ok = false;
                break;
            }
            if (n == 0) {
                ok = false;
                break;
            }
            if (!WriteAll(dst_fd, buffer.data(), static_cast<size_t>(n))) {
                ok = false;
                break;
            }
            remaining -= static_cast<uint64_t>(n);
        }
        ::close(src_fd);
        if (::close(dst_fd) != 0) {
            ok = false;
        }
        if (!ok) {
            ::unlink(dst_path.c_str());
            return false;
        }
        return true;
    }

    // 把 value 向上对齐到 alignment 的整数倍；alignment 为 0 时原样返回。
    static uint64_t AlignUp(uint64_t value, uint64_t alignment) {
        if (alignment == 0) {
            return value;
        }
        const uint64_t remainder = value % alignment;
        return (remainder == 0) ? value : value + (alignment - remainder);
    }

    // 元数据区占用的字节数：按标准镜像数预留（实际条数超过基线时按实际条数），
    // 并对齐到光盘数据块。
    static uint64_t MetadataAreaBytes(uint64_t entry_count,
                                      uint32_t standard_images_per_disc,
                                      uint64_t block_size_bytes) {
        const uint64_t reserved =
            std::max<uint64_t>(entry_count, static_cast<uint64_t>(standard_images_per_disc));
        return AlignUp(DiscMetaAreaSize(reserved), block_size_bytes);
    }

    // 按 [超级块][元数据区][数据区] 布局回填每条记录的 aligned_size_bytes /
    // offset_in_disc，并返回整张光盘占用的总字节数。三个区域与每个镜像均按数据块对齐。
    static uint64_t FillDiscLayout(std::vector<DiscImageEntry>& entries,
                                   uint32_t standard_images_per_disc,
                                   uint64_t block_size_bytes) {
        uint64_t cursor = AlignUp(DISC_SUPERBLOCK_SIZE, block_size_bytes) +
                          MetadataAreaBytes(entries.size(), standard_images_per_disc, block_size_bytes);
        for (DiscImageEntry& entry : entries) {
            entry.aligned_size_bytes = AlignUp(entry.image_size_bytes, block_size_bytes);
            entry.offset_in_disc = cursor;
            cursor += entry.aligned_size_bytes;
        }
        return cursor;
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
                                           uint64_t disc_block_size_bytes,
                                           uint32_t max_write_images,
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
        disc_block_size_bytes_ = disc_block_size_bytes;
        max_write_images_ = max_write_images < 1 ? 1 : max_write_images;
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
        // 追加归档下载背压快照：下载停住时用于判断是「写镜像占满」还是「原始文件积压」，
        // 抑或只是配额节拍（两者都未过载）。
        const ArchiveBackpressureState backpressure = GetArchiveBackpressureState();
        detail += " backpressure=(throttled=";
        detail += backpressure.throttled ? "true" : "false";
        detail += " write_images=" + std::to_string(backpressure.write_images) + "/" +
                  std::to_string(backpressure.write_image_limit);
        detail += " input_pending=" + std::to_string(backpressure.input_pending_bytes) + "/" +
                  std::to_string(backpressure.input_pending_limit);
        detail += ")";
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

        // 重启后从 node_discs_meta 恢复「镜像 → 光盘」定位索引，
        // 否则已刻录光盘的读回请求无法定位 vdisc 与偏移。
        RebuildDiscImageIndex();

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

    void OpticalNodeManager::RebuildDiscImageIndex() {
        {
            std::lock_guard<std::mutex> lock(disc_image_index_mutex_);
            disc_image_index_.clear();
        }

        const std::string path = meta_dir_ + "node_discs_meta";
        struct stat st {};
        if (::stat(path.c_str(), &st) != 0) {
            // 首次启动尚无该文件：空索引即为正确状态。
            return;
        }
        if (st.st_size <= 0) {
            return;
        }

        // 全量读入后顺序扫描：文件为「32B 块头 + entry_count×128B 记录」的追加序列，
        // 规模与镜像总数成正比（每镜像 128B），一次读入可接受。
        std::vector<uint8_t> buffer(static_cast<size_t>(st.st_size));
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(
                start_failure_phase::kRebuildDiscImageIndex, "open_failed",
                "path=" + path + " errno=" + std::to_string(errno));
            return;
        }
        size_t read_total = 0;
        bool read_ok = true;
        while (read_total < buffer.size()) {
            const ssize_t n = ::read(fd, buffer.data() + read_total, buffer.size() - read_total);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                read_ok = false;
                break;
            }
            if (n == 0) {
                break;
            }
            read_total += static_cast<size_t>(n);
        }
        ::close(fd);
        if (!read_ok || read_total != buffer.size()) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(
                start_failure_phase::kRebuildDiscImageIndex, "read_failed",
                "path=" + path + " read=" + std::to_string(read_total) +
                " size=" + std::to_string(buffer.size()));
            return;
        }

        std::unordered_map<uint64_t, DiscImageLocation> rebuilt;
        size_t cursor = 0;
        size_t parsed_blocks = 0;
        bool complete = true;
        while (cursor < buffer.size()) {
            DiscIndexHeader header;
            if (!DiscIndexHeader::Parse(buffer.data() + cursor, buffer.size() - cursor, header)) {
                complete = false;
                break;
            }
            const uint64_t block_bytes =
                DISC_INDEX_HEADER_SIZE +
                static_cast<uint64_t>(header.entry_count) * DISC_IMAGE_RECORD_SIZE;
            // 块不完整（进程中断留下的残块）→ 停止扫描，保留此前已解析的块。
            if (block_bytes > buffer.size() - cursor) {
                complete = false;
                break;
            }

            const std::string disk_id = std::to_string(header.disc_id);
            bool block_ok = true;
            for (uint32_t i = 0; i < header.entry_count; ++i) {
                DiscImageEntry entry;
                const size_t record_offset =
                    cursor + DISC_INDEX_HEADER_SIZE + static_cast<size_t>(i) * DISC_IMAGE_RECORD_SIZE;
                if (!DiscImageEntry::Parse(buffer.data() + record_offset, DISC_IMAGE_RECORD_SIZE,
                                           entry)) {
                    block_ok = false;
                    break;
                }
                DiscImageLocation location;
                location.disk_id = disk_id;
                location.offset_in_disc = entry.offset_in_disc;
                location.image_size_bytes = entry.image_size_bytes;
                location.sha256_hex = entry.sha256_hex;
                rebuilt[entry.volume_id] = std::move(location);
            }
            if (!block_ok) {
                complete = false;
                break;
            }
            cursor += block_bytes;
            ++parsed_blocks;
        }

        if (!complete) {
            // 不阻塞启动：已解析的块仍可正常读回，未恢复的镜像读回时会明确失败。
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(
                start_failure_phase::kRebuildDiscImageIndex, "node_discs_meta_truncated",
                "path=" + path +
                " parsed_blocks=" + std::to_string(parsed_blocks) +
                " parsed_bytes=" + std::to_string(cursor) +
                " size=" + std::to_string(buffer.size()));
        }

        std::lock_guard<std::mutex> lock(disc_image_index_mutex_);
        disc_image_index_ = std::move(rebuilt);
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
        // 唤醒可能阻塞在背压等待上的归档线程：让它看到 stop / 队列已关闭后退出。
        // 需持背压锁 notify，避免通知落在等待方「判定谓词 → 进入睡眠」窗口内而丢失（会导致 join 卡死）。
        {
            std::lock_guard<std::mutex> lock(archive_backpressure_mutex_);
            archive_backpressure_cv_.notify_all();
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
    // 返回数值形式；落任务 / 拼文件名时取其十进制字符串。
    // TODO: 光盘库刻录细节完善 / 接入 MDS 后，改为向 MDS 申请全局唯一 disc_id。
    uint64_t OpticalNodeManager::GenerateDiskId() {
        return next_disk_id_.fetch_add(1, std::memory_order_relaxed);
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

        // 从 vdisc 中按元数据记录的偏移与大小提取该卷镜像，落到 disc_sim_dir_ 下作为
        // MoveFrom 的跳板，再 rename 进 image_dir_ 并登记（vdisc 持久保留，只读不搬移）。
        // 失败走 sidecar，不阻塞 CD_READ 收尾。
        if (space_manager_ != nullptr) {
            // 1. 按 volume_id 在 node_discs_meta 的内存索引中定位所在光盘与区间。
            uint64_t read_volume_id = 0;
            DiscImageLocation location;
            bool located = false;
            if (ParseUint64Decimal(event.volume_id, read_volume_id)) {
                std::lock_guard<std::mutex> lock(disc_image_index_mutex_);
                const auto it = disc_image_index_.find(read_volume_id);
                if (it != disc_image_index_.end()) {
                    location = it->second;
                    located = true;
                }
            }
            if (!located) {
                last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "image_location_not_found",
                    "volume_id=" + event.volume_id +
                    " task_id=" + std::to_string(event.task_id));
            } else {
                const std::string disc_path = disc_sim_dir_ + "disc_" + location.disk_id + ".vdisc";
                const std::string extracted_path =
                    disc_sim_dir_ + "volume_" + std::to_string(read_volume_id) + ".vimg";

                // 2. 从 vdisc 复制出该镜像区间（vdisc 本身不再被搬移/删除）。
                if (!ExtractRangeFromDisc(disc_path, extracted_path, location.offset_in_disc,
                                          location.image_size_bytes)) {
                    (void)::unlink(extracted_path.c_str());
                    last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "ExtractRangeFromDisc_failed",
                        "disc_path=" + disc_path +
                        " offset=" + std::to_string(location.offset_in_disc) +
                        " size=" + std::to_string(location.image_size_bytes) +
                        " volume_id=" + std::to_string(read_volume_id) +
                        " task_id=" + std::to_string(event.task_id));
                } else {
                    // 3. 校验提取内容与元数据记录的 SHA-256 一致（镜像级完整性校验）。
                    std::string extracted_sha256;
                    if (!space_manager::Sha256FileHex(extracted_path, &extracted_sha256) ||
                        extracted_sha256 != location.sha256_hex) {
                        (void)::unlink(extracted_path.c_str());
                        last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "sha256_mismatch",
                            "volume_id=" + std::to_string(read_volume_id) +
                            " expected=" + location.sha256_hex +
                            " actual=" + extracted_sha256 +
                            " disc_path=" + disc_path +
                            " task_id=" + std::to_string(event.task_id));
                    } else {
                        // 4. 跳板 rename 进 image_dir_ 并登记（复用容量 / LRU / 回滚逻辑）。
                        const volumemanager::ErrorCode move_ret =
                            space_manager_->MoveFrom(extracted_path,
                                                     space_manager::ImageCategory::READ);
                        if (move_ret != volumemanager::ErrorCode::SUCCESS) {
                            (void)::unlink(extracted_path.c_str());
                            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDReadComplete, "MoveFrom_failed",
                                std::string("code=") +
                                volumemanager::GetErrorMessage(move_ret) +
                                " code_int=" +
                                std::to_string(static_cast<int>(move_ret)) +
                                " src=" +
                                extracted_path +
                                " volume_id=" +
                                std::to_string(read_volume_id) +
                                " task_id=" +
                                std::to_string(event.task_id));
                        }
                    }
                }
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

    ArchiveBackpressureState OpticalNodeManager::GetArchiveBackpressureState() const {
        ArchiveBackpressureState state;
        state.write_image_limit = max_write_images_;
        if (space_manager_ != nullptr) {
            state.write_images = space_manager_->GetWriteImageCount();
        }

        // 硬编码：原始文件积压阈值 = 卷镜像容量的 10%，避免一次性下载太多原始文件。
        state.input_pending_limit = volume_manager_.volume_size_ / 10;
        state.input_pending_bytes = EstimateInputPendingBytes();

        state.write_overloaded =
            state.write_image_limit > 0 && state.write_images >= state.write_image_limit;
        state.input_overloaded =
            state.input_pending_limit > 0 && state.input_pending_bytes >= state.input_pending_limit;
        state.throttled = state.write_overloaded || state.input_overloaded;
        return state;
    }

    uint64_t OpticalNodeManager::EstimateInputPendingBytes() const {
        if (input_file_dir_.empty()) {
            return 0;
        }
        // 粗略估算：不加锁，并发写入期间 readdir 可能漏项，对背压判断足够。
        std::unique_ptr<DIR, decltype(&closedir)> dir(opendir(input_file_dir_.c_str()), closedir);
        if (!dir) {
            return 0;
        }
        uint64_t total = 0;
        struct dirent* entry = nullptr;
        while ((entry = readdir(dir.get())) != nullptr) {
            const std::string name(entry->d_name);
            if (name == "." || name == "..") {
                continue;
            }
            struct stat st {};
            if (::stat((input_file_dir_ + name).c_str(), &st) != 0) {
                continue;
            }
            if (S_ISREG(st.st_mode)) {
                total += static_cast<uint64_t>(st.st_size);
            }
        }
        return total;
    }

    bool OpticalNodeManager::WaitIfArchiveDownloadThrottled() {
        const uint64_t round_quota = volume_manager_.volume_size_;
        const bool round_exhausted =
            round_quota > 0 && archive_round_downloaded_bytes_ >= round_quota;
        const bool load_throttled = GetArchiveBackpressureState().throttled;
        if (!round_exhausted && !load_throttled) {
            return true;
        }

        std::unique_lock<std::mutex> lock(archive_backpressure_mutex_);

        // ① 负载过载（写镜像达上限 / 原始文件积压超阈值）：必须等负载真正降下来。
        if (load_throttled) {
            archive_backpressure_cv_.wait(lock, [this] {
                return stop_requested_.load(std::memory_order_relaxed) ||
                       archive_task_queue_->IsClosed() ||
                       !GetArchiveBackpressureState().throttled;
            });
            if (stop_requested_.load(std::memory_order_relaxed) ||
                archive_task_queue_->IsClosed()) {
                return false;
            }
        }

        // ② 本轮配额用尽：等「本轮提交的待压缩文件都被压缩完」。
        //    用「轮次内已完成的压缩次数 ≥ 本轮提交任务数」而不是「seq 发生变化」：
        //    否则若这些文件在本线程进入等待前就已压完，seq 不会再变、也不再有新文件下载
        //    → 永久挂死；而按此判定时条件已满足则直接放行（不必空睡）。
        //    注意：这里是计数是全局累计的，因此上一轮残留未压完的文件也会被计入（略偏保守）。
        if (round_exhausted) {
            archive_backpressure_cv_.wait(lock, [this] {
                if (stop_requested_.load(std::memory_order_relaxed) ||
                    archive_task_queue_->IsClosed()) {
                    return true;
                }
                // 逃生条件：压缩队列已空 → 本轮提交的压缩要么已完成、要么永远不会来
                // （例如压缩失败被置 FAILED），继续等就是睡死。
                // 放松节拍不会失控：步骤 11 的「原始文件积压阈值」仍独立限流。
                if (zip_task_queue_ == nullptr || zip_task_queue_->Size() == 0) {
                    return true;
                }
                const uint64_t progressed = zip_compress_completed_seq_ - archive_round_start_seq_;
                return progressed >= archive_round_submitted_tasks_;
            });
            if (stop_requested_.load(std::memory_order_relaxed) ||
                archive_task_queue_->IsClosed()) {
                return false;
            }
        }

        // 唤醒即开启新一轮：配额与提交计数清零，并以当前压缩进度作为新一轮基准。
        archive_round_downloaded_bytes_ = 0;
        archive_round_submitted_tasks_ = 0;
        archive_round_start_seq_ = zip_compress_completed_seq_;
        return true;
    }

    void OpticalNodeManager::NotifyArchiveCompressionProgress() {
        // 必须在持背压锁下递增计数并唤醒：等待方的「判定谓词 → 进入睡眠」是在持锁下完成的，
        // 若通知方不在持锁下 notify，通知可能恰好落进该窗口而丢失 → 永久睡死。
        std::lock_guard<std::mutex> lock(archive_backpressure_mutex_);
        ++zip_compress_completed_seq_;
        archive_backpressure_cv_.notify_all();
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

    // 归档任务消费线程（下载线程）：从 archive_task_queue_ 取 MDS 批次，重建节点映射，
    // 再逐文件下载（real_node 分片重组）并建 WRITE 任务入 zip_task_queue_。
    //
    // 数据流与背压反馈：
    //   MDS ──SendArchiveMetadata──▶ archive_task_queue_
    //     └▶ ArchiveTaskProcessor（本函数）
    //          ├─ BuildNodeAddressMap()      ← scheduler GetClusterView（每次重新开始处理时刷新）
    //          ├─ DownloadArchiveFile()      ← real_node ResolveFileRead / ReadObject
    //          │       └─▶ input_file_dir_/<inode_id>.archive
    //          └─ SubmitWriteTaskForArchive() ─▶ zip_task_queue_ + task_map_
    //                                              └▶ ZipTaskProcessor（压缩线程）
    //                                                   ├─ AddFileToCollect：压缩累加进卷镜像
    //                                                   ├─ unlink input/<inode_id>.archive
    //                                                   └─ NotifyArchiveCompressionProgress() ← 唤醒本线程
    //
    // 本线程会阻塞在两个地方，唤醒来源不同（排查「下载不动」时按此二分）：
    //   ① WaitIfArchiveDownloadThrottled 的背压 CV：
    //        - ZipTaskProcessor 每完成一次压缩（NotifyArchiveCompressionProgress）：原始文件被消化；
    //        - 刻录完成释放写镜像后（WriteVdiscAndReleaseImages 内，运行在 cd_manager 线程）：
    //          写镜像占用下降；
    //        - StopBackgroundWorkers 关队列后的 notify_all ——停机兜底。
    //      规则：凡是「能解除阻塞条件（写镜像占用下降 / 原始文件被消化）」的事件都必须 notify，
    //      漏一个就会永久睡死（曾因漏掉「刻录完成释放」而在 smoke 里挂住）。
    //      另注意：配额节拍的推进完全依赖 Zip 的压缩进度，因此「下载不动」有可能是
    //      「压缩没进展」（例如大镜像压缩耗时长、或本轮文件全都没能提交到压缩队列）。
    //   ② archive_task_queue_->Pop()：
    //        - MDS 新归档请求到达（Push 内部 notify_one）——全部请求下完后停在这里等新请求；
    //        - 队列 Close（停机）。
    void OpticalNodeManager::ArchiveTaskProcessor() {
        while (!stop_requested_.load(std::memory_order_relaxed)) {
            // 取一个 MDS 批次作为本轮起点；若上一轮阻塞在批次中间，断点仍然有效、不重复取队。
            if (!pending_archive_request_.has_value()) {
                auto opt_request = archive_task_queue_->Pop();
                if (!opt_request.has_value()) {
                    // 队列已 Close：退出消费循环。全部请求下完后也停在这里等新请求。
                    break;
                }
                pending_archive_request_ = std::move(*opt_request);
                pending_archive_file_index_ = 0;
            }

            // 步骤 1：每次（重新）开始处理时重建节点映射，避免等待期间地址过期。
            std::unordered_map<std::string, std::string> node_address_map;
            if (!BuildNodeAddressMap(&node_address_map)) {
                // 映射失败：丢弃当前批次，避免卡在同一批次上反复失败。
                pending_archive_request_.reset();
                pending_archive_file_index_ = 0;
                continue;
            }

            // 步骤 2：从断点起逐文件下载 + 建 WRITE 任务；每个文件前做一次背压检查
            // （本轮下载量达 volume_size，或写镜像/原始文件积压过载）。
            bool stop_consuming = false;
            while (pending_archive_file_index_ < pending_archive_request_->files.size()) {
                if (stop_requested_.load(std::memory_order_relaxed)) {
                    stop_consuming = true;
                    break;
                }
                if (!WaitIfArchiveDownloadThrottled()) {
                    stop_consuming = true;
                    break;
                }

                const ArchiveFileInfo& file =
                    pending_archive_request_->files[pending_archive_file_index_];

                std::string target_node_address;
                if (!ResolveNodeAddressFromMap(node_address_map,
                                               file.target_node_id,
                                               &target_node_address)) {
                    last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kArchiveTaskProcessor, "node_not_found",
                        "target_node_id=" + file.target_node_id + " inode_id=" + std::to_string(file.inode_id));
                    ++pending_archive_file_index_;
                    continue;
                }

                std::string relative_path;
                if (!DownloadArchiveFile(file, target_node_address, &relative_path)) {
                    ++pending_archive_file_index_;
                    continue;
                }

                // 本轮下载量按文件原始大小累计（与卷镜像容量的口径一致）。
                archive_round_downloaded_bytes_ += file.size;

                if (!SubmitWriteTaskForArchive(file.inode_id, relative_path)) {
                    // 队列已关闭 / 正在停止：清理已下载文件，避免残留，并退出消费循环。
                    ::unlink((input_file_dir_ + relative_path).c_str());
                    stop_consuming = true;
                    break;
                }
                ++archive_round_submitted_tasks_;
                ++pending_archive_file_index_;
            }

            if (stop_consuming) {
                break;
            }

            // 当前 MDS 批次已下完：清空断点，回到外层取下一个批次（本轮配额跨批次累计）。
            pending_archive_request_.reset();
            pending_archive_file_index_ = 0;
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

                // 背压反馈：一次压缩完成意味着流水线前进一步（原始文件已消化、写镜像占用可能下降），
                // 唤醒可能正在等待的归档下载线程。
                NotifyArchiveCompressionProgress();

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

                // 7. 把 image_dir_/ 下的写镜像纳入待打包光盘：计算 SHA-256、按容量判定是否
                //    触发封印、追加待打包集合并原子重写 meta/pending_disc_meta。
                const std::string final_volume_path = image_dir_ + packed_basename;
                const volumemanager::ErrorCode accumulate_ret =
                    AccumulatePackedImage(final_volume_path, packed_volume_id);
                if (accumulate_ret != volumemanager::ErrorCode::SUCCESS) {
                    MarkTaskFailed(full_task->task_id,
                                   volumemanager::ErrorCode::PACK_FAILED,
                                   FormatFailureDetail(start_failure_phase::kZipTaskProcessor,
                                              "AccumulatePackedImage_failed",
                                              "ret=" + std::to_string(static_cast<int>(accumulate_ret)) +
                                              " code=" + volumemanager::GetErrorMessage(accumulate_ret) +
                                              " volume_id=" + std::to_string(packed_volume_id) +
                                              " image_path=" + final_volume_path));
                    continue;
                }

                // 8. 第二次上报（镜像 → 光盘）在刻录完成回调 OnCDBurnComplete 中发出，
                //    本处不实现。
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

    void OpticalNodeManager::ReportImagesBurnedToDisc(const std::string& disc_id,
                                                      const std::vector<DiscImageEntry>& entries) {
        // 本盘的镜像 → 光盘对应关系打印到控制台：MDS 的 ReportImagesBurnedToDisc
        // 上报尚未实现，先以控制台输出替代，便于单模块测试观察。
        // 字段名与 ReportImagesBurnedToDiscRequest 对齐（disc_id + image_ids），
        // 额外带上 count 便于测试解析。
        std::cout << "[ReportImagesBurnedToDisc] disc_id=" << disc_id
                  << " count=" << entries.size()
                  << " image_ids=";
        for (size_t i = 0; i < entries.size(); ++i) {
            if (i > 0) {
                std::cout << ',';
            }
            std::cout << entries[i].volume_id;
        }
        std::cout << std::endl;
    }

    volumemanager::ErrorCode OpticalNodeManager::PersistPendingDiscMeta() {
        std::vector<uint8_t> buffer;
        {
            std::lock_guard<std::mutex> lock(pending_disc_mutex_);
            DiscMetaHeader header;
            header.entry_count = static_cast<uint32_t>(pending_disc_entries_.size());
            header.Serialize(buffer);
            for (const DiscImageEntry& entry : pending_disc_entries_) {
                std::vector<uint8_t> record;
                entry.Serialize(record);
                buffer.insert(buffer.end(), record.begin(), record.end());
            }
        }

        const std::string final_path = meta_dir_ + "pending_disc_meta";
        const std::string tmp_path = final_path + ".tmp";

        const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            return volumemanager::ErrorCode::IO_ERROR;
        }
        // 顺序写入元数据区内容。
        if (!WriteAll(fd, reinterpret_cast<const char*>(buffer.data()), buffer.size())) {
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return volumemanager::ErrorCode::IO_ERROR;
        }
        // 先落盘再 rename：避免中断后留下半截元数据。
        if (::fsync(fd) != 0) {
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return volumemanager::ErrorCode::IO_ERROR;
        }
        if (::close(fd) != 0) {
            ::unlink(tmp_path.c_str());
            return volumemanager::ErrorCode::IO_ERROR;
        }
        if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
            ::unlink(tmp_path.c_str());
            return volumemanager::ErrorCode::IO_ERROR;
        }
        return volumemanager::ErrorCode::SUCCESS;
    }

    volumemanager::ErrorCode OpticalNodeManager::SealPendingDiscAndSubmitBurn() {
        // 1. 快照待打包集合；空集合无需封印。
        std::vector<DiscImageEntry> entries;
        {
            std::lock_guard<std::mutex> lock(pending_disc_mutex_);
            if (pending_disc_entries_.empty()) {
                return volumemanager::ErrorCode::SUCCESS;
            }
            entries = pending_disc_entries_;
        }

        // 2. 定稿布局：回填每条记录的 aligned_size_bytes / offset_in_disc，
        //    返回值即整张光盘（含超级块与元数据区）应占的字节数。
        const uint64_t disc_total_bytes =
            FillDiscLayout(entries, standard_images_per_disc_, disc_block_size_bytes_);

        // 3. 分配光盘全局 ID。
        const uint64_t disc_id = GenerateDiskId();
        const std::string disk_id = std::to_string(disc_id);

        // 4. 元数据定稿：pending_disc_meta 改名为 disc_<disk_id>_meta，避免影响后续打包
        //    （新盘继续从空的 pending_disc_meta 积攒）。
        const std::string pending_meta_path = meta_dir_ + "pending_disc_meta";
        const std::string disc_meta_path = meta_dir_ + "disc_" + disk_id + "_meta";
        if (::rename(pending_meta_path.c_str(), disc_meta_path.c_str()) != 0) {
            return volumemanager::ErrorCode::IO_ERROR;
        }

        // 5. 构造光盘级刻录任务并入队；盘内全部 volume_id 与预期盘大小一并携带，
        //    供刻录完成后逐个写入 vdisc 并释放写镜像。
        //    vdisc 本步不生成：镜像数据在 OnCDBurnComplete 中边写边释放。
        const std::string vdisc_path = disc_sim_dir_ + "disc_" + disk_id + ".vdisc";
        std::vector<std::string> volume_ids;
        volume_ids.reserve(entries.size());
        for (const DiscImageEntry& entry : entries) {
            volume_ids.push_back(std::to_string(entry.volume_id));
        }
        const uint64_t disc_burn_task_id = GenerateTaskId();
        WR_task::WRTask disc_burn_task(disc_burn_task_id, WR_task::WRTaskType::DISC_BURN);
        disc_burn_task.SetDiscBurnTask(disk_id, vdisc_path, volume_ids, disc_total_bytes);
        task_map_->Insert(std::move(disc_burn_task));
        cd_burn_task_queue_->Push(
            WR_task::WRTaskShort(disc_burn_task_id, WR_task::WRTaskType::DISC_BURN));

        // 6. 清空待打包集合：本盘已封印，后续镜像从新盘从零积攒。
        {
            std::lock_guard<std::mutex> lock(pending_disc_mutex_);
            pending_disc_entries_.clear();
        }
        return volumemanager::ErrorCode::SUCCESS;
    }

    bool OpticalNodeManager::WriteVdiscAndReleaseImages(const std::string& vdisc_path,
                                                        const std::vector<DiscImageEntry>& entries,
                                                        uint64_t disc_id) {
        // 元数据区编码：与 meta/disc_<disk_id>_meta 使用同一格式（头部 + 定长记录）。
        std::vector<uint8_t> meta_area;
        DiscMetaHeader meta_header;
        meta_header.entry_count = static_cast<uint32_t>(entries.size());
        meta_header.Serialize(meta_area);
        for (const DiscImageEntry& entry : entries) {
            std::vector<uint8_t> record;
            entry.Serialize(record);
            meta_area.insert(meta_area.end(), record.begin(), record.end());
        }

        // 超级块：记录三个区域的布局、容量与镜像公共路径前缀。
        DiscSuperblock superblock;
        superblock.disc_id = disc_id;
        superblock.capacity_bytes = disc_capacity_bytes_;
        superblock.created_at_ms = WR_task::NowMs();
        superblock.block_size_bytes = static_cast<uint32_t>(disc_block_size_bytes_);
        superblock.image_count = static_cast<uint64_t>(entries.size());
        superblock.metadata_offset = AlignUp(DISC_SUPERBLOCK_SIZE, disc_block_size_bytes_);
        superblock.metadata_size =
            MetadataAreaBytes(entries.size(), standard_images_per_disc_, disc_block_size_bytes_);
        superblock.data_offset = superblock.metadata_offset + superblock.metadata_size;
        superblock.path_prefix = image_dir_;

        std::vector<uint8_t> superblock_bytes;
        superblock.Serialize(superblock_bytes);

        const std::string tmp_path = vdisc_path + ".tmp";
        const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            return false;
        }

        // 按 [超级块][元数据区][数据区] 顺序写入，区域之间补零到各自的起始偏移。
        bool ok = WriteAll(fd, reinterpret_cast<const char*>(superblock_bytes.data()),
                           superblock_bytes.size());
        if (ok) {
            ok = WriteZeros(fd, superblock.metadata_offset - superblock_bytes.size());
        }
        if (ok) {
            ok = WriteAll(fd, reinterpret_cast<const char*>(meta_area.data()), meta_area.size());
        }
        if (ok) {
            ok = WriteZeros(fd,
                            superblock.data_offset - superblock.metadata_offset - meta_area.size());
        }
        // 数据区：按元数据记录的顺序逐个写入卷镜像，各自补零到对齐长度；
        // 每写完一个立即释放其 image_dir_ 内副本（写一个释放一个），
        // 使 image_dir_ 的空间压力在刻录过程中逐步缓解。
        if (ok) {
            for (const DiscImageEntry& entry : entries) {
                const std::string image_path = VolumeImagePath(std::to_string(entry.volume_id));
                if (!CopyFileAndPadToFd(fd, image_path, entry.image_size_bytes,
                                        entry.aligned_size_bytes)) {
                    ok = false;
                    break;
                }
                const volumemanager::ErrorCode remove_ret =
                    space_manager_->RemoveWriteImage(entry.volume_id);
                if (remove_ret == volumemanager::ErrorCode::SUCCESS) {
                    // 背压反馈：释放写镜像会让「写镜像占用」下降，可能解除归档下载的等待。
                    // 本回调运行在 cd_manager 线程上，必须主动唤醒归档线程——否则若此刻
                    // 压缩已全部完成（无其它 notify 来源），等待中的归档线程会永久睡死。
                    // 且必须在持背压锁下 notify，避免通知落在等待方「判定谓词 → 进入睡眠」的窗口内。
                    std::lock_guard<std::mutex> lock(archive_backpressure_mutex_);
                    archive_backpressure_cv_.notify_all();
                } else {
                    // 镜像已写入 vdisc，释放失败只影响缓存副本残留；记录 sidecar 不中断。
                    last_sidecar_failure_reason_buf_ = FormatFailureDetail(
                        start_failure_phase::kOnCDBurnComplete, "RemoveWriteImage_failed",
                        std::string("code=") + volumemanager::GetErrorMessage(remove_ret) +
                        " code_int=" + std::to_string(static_cast<int>(remove_ret)) +
                        " volume_id=" + std::to_string(entry.volume_id) +
                        " image_path=" + image_path);
                }
            }
        }
        if (ok) {
            ok = (::fsync(fd) == 0);
        }
        if (::close(fd) != 0) {
            ok = false;
        }
        if (!ok) {
            ::unlink(tmp_path.c_str());
            return false;
        }
        // 先落盘再 rename：避免留下半截光盘文件被误当成有效光盘。
        if (::rename(tmp_path.c_str(), vdisc_path.c_str()) != 0) {
            ::unlink(tmp_path.c_str());
            return false;
        }
        return true;
    }

    bool OpticalNodeManager::AppendToNodeDiscsMeta(uint64_t disc_id,
                                                   const std::vector<DiscImageEntry>& entries) {
        DiscIndexHeader index_header;
        index_header.entry_count = static_cast<uint32_t>(entries.size());
        index_header.disc_id = disc_id;

        std::vector<uint8_t> block;
        index_header.Serialize(block);
        for (const DiscImageEntry& entry : entries) {
            std::vector<uint8_t> record;
            entry.Serialize(record);
            block.insert(block.end(), record.begin(), record.end());
        }

        const std::string path = meta_dir_ + "node_discs_meta";
        // O_APPEND：多张光盘按刻录完成顺序追加，不覆盖既有内容。
        const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            return false;
        }
        bool ok = WriteAll(fd, reinterpret_cast<const char*>(block.data()), block.size());
        if (ok) {
            ok = (::fsync(fd) == 0);
        }
        if (::close(fd) != 0) {
            ok = false;
        }
        return ok;
    }

    volumemanager::ErrorCode OpticalNodeManager::AccumulatePackedImage(
            const std::string& image_path,
            uint64_t volume_id) {
        // 1. 采集镜像实大小与 SHA-256，构造元数据记录。
        struct stat st {};
        if (image_path.empty() || ::stat(image_path.c_str(), &st) != 0 || st.st_size <= 0) {
            return volumemanager::ErrorCode::IO_ERROR;
        }
        DiscImageEntry entry;
        entry.volume_id = volume_id;
        entry.image_size_bytes = static_cast<uint64_t>(st.st_size);
        if (!space_manager::Sha256FileHex(image_path, &entry.sha256_hex)) {
            return volumemanager::ErrorCode::IO_ERROR;
        }

        // 2. 容量判定：加入本条后会超出光盘容量时，先把当前集合封印为一张光盘。
        //    空盘无论如何都要容纳第一条（单条镜像自身超容量时独占一张光盘）。
        bool need_seal = false;
        {
            std::lock_guard<std::mutex> lock(pending_disc_mutex_);
            std::vector<DiscImageEntry> candidate = pending_disc_entries_;
            candidate.push_back(entry);
            const uint64_t bytes_if_added =
                FillDiscLayout(candidate, standard_images_per_disc_, disc_block_size_bytes_);
            need_seal = !pending_disc_entries_.empty() && bytes_if_added > disc_capacity_bytes_;
        }
        if (need_seal) {
            const volumemanager::ErrorCode seal_ret = SealPendingDiscAndSubmitBurn();
            if (seal_ret != volumemanager::ErrorCode::SUCCESS) {
                return seal_ret;
            }
        }

        // 3. 追加进待打包集合，按光盘布局回填偏移后原子落盘。
        {
            std::lock_guard<std::mutex> lock(pending_disc_mutex_);
            pending_disc_entries_.push_back(entry);
            FillDiscLayout(pending_disc_entries_, standard_images_per_disc_, disc_block_size_bytes_);
        }
        return PersistPendingDiscMeta();
    }

    void OpticalNodeManager::OnCDBurnComplete(const cd_manager_sim::BurnCompleteEvent& event) {
        // cd_manager_sim 当前未提供 success 字段，默认视为成功。
        if (!MarkTaskState(event.task_id, WR_task::WRTaskState::FINISH)) {
            auto existing = task_map_->Get(event.task_id);
            if (!existing.has_value()) {
                last_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "unknown_task_id",
                    "task_id=" + std::to_string(event.task_id));
            }
            return;
        }

        // 烧录完成 → 按该盘元数据逐个释放写镜像，并把元数据追加到 node_discs_meta。
        // 以下均为副作用，失败走 sidecar，不影响 DISC_BURN 的 FINISH 主语义。
        const auto burned_task = task_map_->Get(event.task_id);
        if (!burned_task.has_value()) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "unknown_task_id",
                "task_id=" + std::to_string(event.task_id));
            return;
        }
        if (burned_task->type != WR_task::WRTaskType::DISC_BURN) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "invalid_type",
                "task_id=" + std::to_string(event.task_id) +
                " type=" + WR_task::WRTaskTypeToString(burned_task->type));
            return;
        }
        if (space_manager_ == nullptr) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "space_manager_null",
                "task_id=" + std::to_string(event.task_id));
            return;
        }

        // 1. 回读该盘元数据，拿到镜像清单及其在光盘文件中的偏移 / 大小 / SHA-256。
        //    元数据文件是封印时的定稿产物，作为释放与索引的唯一依据。
        const std::string disc_meta_path = meta_dir_ + "disc_" + burned_task->disk_id + "_meta";
        std::vector<DiscImageEntry> entries;
        if (!ReadDiscMetaFile(disc_meta_path, entries)) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "ReadDiscMetaFile_failed",
                "disk_id=" + burned_task->disk_id +
                " path=" + disc_meta_path +
                " task_id=" + std::to_string(event.task_id));
            return;
        }

        // 2. 拼接光盘文件：超级块 + 元数据区 + 逐个写入卷镜像，每写入一个即释放其
        //    image_dir_ 内副本（写一个释放一个）。此时镜像数据才真正落到 vdisc。
        const std::string vdisc_path = disc_sim_dir_ + "disc_" + burned_task->disk_id + ".vdisc";
        uint64_t disc_id_num = 0;
        (void)ParseUint64Decimal(burned_task->disk_id, disc_id_num);
        if (!WriteVdiscAndReleaseImages(vdisc_path, entries, disc_id_num)) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "WriteVdiscAndReleaseImages_failed",
                "disk_id=" + burned_task->disk_id +
                " path=" + vdisc_path +
                " task_id=" + std::to_string(event.task_id));
            return;
        }

        // 3. 元数据追加进 node_discs_meta，并同步内存索引供读回时定位。
        //    先落盘再更新索引，保证索引不会指向文件中不存在的光盘。
        if (!AppendToNodeDiscsMeta(disc_id_num, entries)) {
            last_sidecar_failure_reason_buf_ = FormatFailureDetail(start_failure_phase::kOnCDBurnComplete, "AppendToNodeDiscsMeta_failed",
                "disk_id=" + burned_task->disk_id +
                " path=" + meta_dir_ + "node_discs_meta" +
                " task_id=" + std::to_string(event.task_id));
            return;
        }
        {
            std::lock_guard<std::mutex> lock(disc_image_index_mutex_);
            for (const DiscImageEntry& entry : entries) {
                DiscImageLocation location;
                location.disk_id = burned_task->disk_id;
                location.offset_in_disc = entry.offset_in_disc;
                location.image_size_bytes = entry.image_size_bytes;
                location.sha256_hex = entry.sha256_hex;
                disc_image_index_[entry.volume_id] = std::move(location);
            }
        }

        // 4. 第二次上报（镜像 → 光盘）：本盘包含的全部镜像上报给 MDS。
        ReportImagesBurnedToDisc(burned_task->disk_id, entries);
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
        } else if (task.expected_image_size_bytes != 0) {
            // DISC_BURN 提交时 vdisc 尚未生成（镜像在刻录完成后才逐个写入）：
            // 用封印时算出的预期盘大小，避免 cd_manager 回退到 config 默认值
            // （10GB 默认值会把刻录耗时放大数百倍）。
            image_size_bytes = task.expected_image_size_bytes;
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
                                              " disk_id=" + full_task->disk_id +
                                              " image_path=" + full_task->file_path));
                } else {
                    // 先提交其它字段，状态单独走 MarkTaskState（终态保护生效）。
                    auto current = task_map_->Get(task.task_id);
                    if (current.has_value()) {
                        current->IncrementAttemptCount();
                        current->last_error_code = volumemanager::ErrorCode::BURN_FAILED;
                        current->last_error_detail = "SubmitBurnTask_rejected_by_cd_manager disk_id=" +
                                                     full_task->disk_id +
                                                     " image_path=" + full_task->file_path;
                        task_map_->Update(std::move(*current));
                        MarkTaskState(task.task_id, WR_task::WRTaskState::WAITING);
                    }
                    // 保留原任务类型（CD_BURN / DISC_BURN）重新入队。
                    cd_burn_task_queue_->Push(WR_task::WRTaskShort(task.task_id, task.type));
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
