#include "space_manager/image_dir_manager.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

namespace space_manager {

namespace {

// 文件名前缀、后缀与 PackVolume 的 output_path 约定保持一致
//    output_path = temp_dir_ + "volume_" + std::to_string(volume_id) + ".vimg"
constexpr const char* kVolumeFilePrefix = "volume_";
constexpr const char* kVolumeFileSuffix = ".vimg";

/**
 * @brief 解析形如 volume_<id>.vimg 的文件名，返回是否合法以及对应的 volume_id
 *
 * 仅校验前缀和后缀，中间部分必须是十进制 uint64。
 * 不去查文件是否存在 —— 上层在调用本工具后再做进一步校验。
 */
bool TryParseVolumeId(const std::string& basename, uint64_t& volume_id_out) {
    if (basename.size() <= std::strlen(kVolumeFilePrefix) + std::strlen(kVolumeFileSuffix)) {
        return false;
    }
    if (basename.compare(0, std::strlen(kVolumeFilePrefix), kVolumeFilePrefix) != 0) {
        return false;
    }
    const size_t suffix_begin = basename.size() - std::strlen(kVolumeFileSuffix);
    if (basename.compare(suffix_begin, std::strlen(kVolumeFileSuffix), kVolumeFileSuffix) != 0) {
        return false;
    }

    const std::string id_str =
        basename.substr(std::strlen(kVolumeFilePrefix),
                        suffix_begin - std::strlen(kVolumeFilePrefix));
    if (id_str.empty()) {
        return false;
    }
    // 全字符必须是十进制数字
    for (char c : id_str) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    try {
        size_t consumed = 0;
        unsigned long long v = std::stoull(id_str, &consumed);
        if (consumed != id_str.size()) {
            return false;
        }
        volume_id_out = static_cast<uint64_t>(v);
        return true;
    } catch (...) {
        return false;
    }
}

/**
 * @brief 从绝对路径中提取 basename
 *
 * 例如 "/tmp/dir/volume_3.vimg" -> "volume_3.vimg"
 */
std::string BasenameOf(const std::string& abs_path) {
    const size_t pos = abs_path.find_last_of('/');
    if (pos == std::string::npos) {
        return abs_path;
    }
    return abs_path.substr(pos + 1);
}

/**
 * @brief POSIX mkdir 单级目录，已存在视为成功
 */
int MkdirOne(const std::string& path) {
    if (mkdir(path.c_str(), 0755) == 0) {
        return 0;
    }
    if (errno == EEXIST) {
        return 0;
    }
    return -1;
}

/**
 * @brief 把 image_dir_path 规范化（去尾斜杠），返回规范化的父目录
 */
std::string NormalizeImageDir(const std::string& path) {
    if (path.empty()) {
        return path;
    }
    if (path.back() != '/') {
        return path;
    }
    return path.substr(0, path.size() - 1);
}

/**
 * @brief 确保 image_dir_ 单层目录存在
 *
 * 扇平布局（2026-08-15）：只保证 image_dir_ 自身存在即可，read/ write/ 子目录
 * 已废弃，不再创建。任一 mkdir 失败直接返回（构造函数不抛异常；调用方后续 IO
 * 会暴露）。
 */
void EnsureImageDir(const std::string& path) {
    if (path.empty()) {
        return;
    }
    const std::string parent = NormalizeImageDir(path);
    MkdirOne(parent);
}

}  // namespace

ImageDirManager::ImageDirManager(std::string image_dir_path)
    : image_dir_path_(std::move(image_dir_path)),
      disc_sim_dir_path_(),  // 必须由 SetDiscSimDir() 显式注入，未注入时 DeleteFileAndEntry
                             // 会兜底 unlink——见 DeleteFileAndEntry 实现。
      capacity_in_images_(0),
      capacity_set_(false) {
    EnsureImageDir(image_dir_path_);
}

ImageDirManager::~ImageDirManager() = default;

volumemanager::ErrorCode ImageDirManager::SetDiscSimDir(std::string disc_sim_dir_path) {
    // 不做"已注入"守门——上层 OpticalNodeManager::InitializeDir() 每次 Run()
    // 都会重新构造 ImageDirManager 并 SetDiscSimDir(disc_sim_dir_)，所以重复
    // 调用是预期的合法语义（同 SetCapacityInImages 的"重 Run() 重新注入"模式
    // 不同——capacity 是只能设一次的硬约束，disc_sim_dir 是每次 Run() 都重置）。
    //
    // 规范化：与 image_dir_path_ 一致，原样存储（不去尾斜杠）；内部拼路径时强制
    // 加 '/'。
    // 允许空字符串：上层如果不想用模拟光盘库兜底（让 DeleteFileAndEntry 走纯
    // unlink 兜底），可以传 ""。此时 disc_sim_dir_path_.empty() == true 会被
    // DeleteFileAndEntry 走 unlink 路径。
    disc_sim_dir_path_ = std::move(disc_sim_dir_path);
    return volumemanager::ErrorCode::SUCCESS;
}

void ImageDirManager::ReadLock() {
    mutex_.lock_shared();
}

void ImageDirManager::ReadUnlock() {
    mutex_.unlock_shared();
}

volumemanager::ErrorCode ImageDirManager::SetCapacityInImages(uint64_t capacity_in_images) {
    if (capacity_in_images == 0) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (capacity_set_) {
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }
    capacity_in_images_ = capacity_in_images;
    capacity_set_ = true;
    return volumemanager::ErrorCode::SUCCESS;
}

volumemanager::ErrorCode ImageDirManager::ParseVolumeFile(const std::string& abs_path,
                                                           uint64_t& volume_id,
                                                           std::string& basename) const {
    if (abs_path.empty()) {
        return volumemanager::ErrorCode::INVALID_PATH;
    }
    basename = BasenameOf(abs_path);
    if (!TryParseVolumeId(basename, volume_id)) {
        return volumemanager::ErrorCode::INVALID_VOLUME_FORMAT;
    }
    return volumemanager::ErrorCode::SUCCESS;
}

bool ImageDirManager::HasImage(uint64_t volume_id) const {
    return entries_.find(volume_id) != entries_.end();
}

void ImageDirManager::Touch(uint64_t volume_id) {
    auto idx_it = lru_index_.find(volume_id);
    if (idx_it == lru_index_.end()) {
        return;
    }
    // lru_index_ maps volume_id -> iterator into lru_list_
    // lru_list_ stores iterators into entries_
    lru_list_.splice(lru_list_.begin(), lru_list_, idx_it->second);
    auto entry_it = idx_it->second;  // lru_list_ iterator
    (*entry_it)->second.last_access = std::chrono::steady_clock::now();
}

void ImageDirManager::RemoveLRU(uint64_t volume_id) {
    auto idx_it = lru_index_.find(volume_id);
    if (idx_it == lru_index_.end()) {
        return;
    }
    lru_list_.erase(idx_it->second);
    lru_index_.erase(idx_it);
}

volumemanager::ErrorCode ImageDirManager::DeleteFileAndEntry(uint64_t volume_id) {
    auto it = entries_.find(volume_id);
    if (it == entries_.end()) {
        return volumemanager::ErrorCode::VOLUME_NOT_FOUND;
    }
    const std::string src_path = FullPathForEntry(it->second);
    // 提前拷贝 filename：后续 erase(it) 后迭代器失效，src_path 还可用，
    // 但 entry.filename 也直接拷贝出来更便于拼 dst 路径。
    const std::string basename = it->second.filename;

    // 先移管理表项：DeleteFileAndEntry 的调用方（RemoveSingleReadImageLocked /
    // RemoveWriteImage / MoveFrom 容量满时换出 victim）都需要 entries_ 立刻腾
    // 出位置——尤其是 MoveFrom 必须在 try-evict 后腾出槽位才能继续登记新镜像。
    // 即使后续 rename/unlink 失败，entries_ 已经没有这个条目了，对调用方来说
    // 是"已释放"，与之前"先 erase 再 unlink"的语义对齐。
    RemoveLRU(volume_id);
    entries_.erase(it);

    // **适配模拟光盘库（2026-08-15 用户决策）**：原本这里直接 unlink，模拟
    // 光盘库场景下等价于"销毁光盘"，与真实光盘库的物理行为不符。改为：
    //   1) 若 disc_sim_dir_path_ 已注入 → rename(2) 到 disc_sim_dir_/volume_<id>.vimg，
    //      等价于"把光盘放回光盘库位"。Rename 失败 → 兜底 unlink 避免文件残留，
    //      返回 IO_ERROR。
    //   2) 若 disc_sim_dir_path_ 未注入（空字符串）→ 退化为 unlink 兜底行为，
    //      与旧实现兼容（OpticalNodeManager 未注入 disc_sim_dir 的极端场景
    //      不会让 image_dir_ 内残留镜像膨胀）。
    if (!disc_sim_dir_path_.empty()) {
        std::string dst_path = disc_sim_dir_path_;
        if (!dst_path.empty() && dst_path.back() != '/') {
            dst_path.push_back('/');
        }
        dst_path += basename;

        if (rename(src_path.c_str(), dst_path.c_str()) == 0) {
            return volumemanager::ErrorCode::SUCCESS;
        }
        // rename 失败：兜底 unlink（防止 image_dir_ 内残留膨胀），并返回 IO_ERROR
        // 让上层感知。errno == ENOENT 时（极端：管理表存在但文件已被外部清理）
        // 视为 SUCCESS，避免误报。
        const int rename_errno = errno;
        if (rename_errno == ENOENT) {
            return volumemanager::ErrorCode::SUCCESS;
        }
        // 兜底 unlink
        (void)unlink(src_path.c_str());
        // 记录 rename 失败的 errno 到 last_failure_detail_ 便于排查
        // （与 MoveFrom 的 detail 风格保持一致：phase + subphase + errno）。
        last_failure_detail_ = std::string("phase=DeleteFileAndEntry subphase=rename_failed")
            + " errno=" + std::to_string(rename_errno)
            + " src=" + src_path
            + " dst=" + dst_path
            + " volume_id=" + std::to_string(volume_id);
        return volumemanager::ErrorCode::IO_ERROR;
    }

    // disc_sim_dir_path_ 未注入：保留旧 unlink 兜底行为。
    if (unlink(src_path.c_str()) != 0) {
        if (errno != ENOENT) {
            return volumemanager::ErrorCode::IO_ERROR;
        }
    }
    return volumemanager::ErrorCode::SUCCESS;
}

void ImageDirManager::InsertEntryLocked(const ImageEntry& entry) {
    // 不变量：entries_ / lru_list_ / lru_index_ 三件套同步更新。
    // entries_.emplace 在 volume_id 已存在时不会替换；但 MoveFrom / RebuildManagementTable
    // 已在调用本方法前保证 volume_id 是新的（前者通过 entries_.find 检查，后者从空表起步）。
    auto insert_ret = entries_.emplace(entry.volume_id, entry);
    lru_list_.push_front(insert_ret.first);
    lru_index_.emplace(entry.volume_id, lru_list_.begin());
}

volumemanager::ErrorCode ImageDirManager::MoveFrom(const std::string& abs_path,
                                                    ImageCategory category) {
    // 入口：上一次 MoveFrom 留下的 rollback 状态在本次入口被覆盖（语义等价于
    // 上次 MoveFrom 已经结束）。这避免上次 rollback 状态泄漏到本次。
    rollback_active_ = false;

    uint64_t volume_id = 0;
    std::string basename;
    volumemanager::ErrorCode ret = ParseVolumeFile(abs_path, volume_id, basename);
    if (ret != volumemanager::ErrorCode::SUCCESS) {
        // 区分空路径与文件名格式错。abs_path.empty() 时 ParseVolumeFile 也会
        // 返回 INVALID_PATH；这里按 abs_path 是否为空选 subphase，二者不会同时为真。
        last_failure_detail_ = std::string("phase=MoveFrom subphase=")
            + (abs_path.empty() ? "empty_abs_path"
                                 : "basename_not_volume_vimg")
            + " basename=" + basename;
        return ret;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // ④ 容量未设置
    if (!capacity_set_) {
        last_failure_detail_ = "phase=MoveFrom subphase=capacity_not_set";
        return volumemanager::ErrorCode::INVALID_PARAMETER;
    }

    // ⑤ 已存在 —— 用新码
    if (entries_.find(volume_id) != entries_.end()) {
        last_failure_detail_ = std::string("phase=MoveFrom subphase=duplicate_volume_id volume_id=")
            + std::to_string(volume_id);
        return volumemanager::ErrorCode::VOLUME_ALREADY_EXISTS;
    }

    // ⑥ + ⑦ 容量满的处理
    if (entries_.size() >= capacity_in_images_) {
        uint64_t read_count = 0;
        for (const auto& pair : entries_) {
            if (pair.second.category == ImageCategory::READ) {
                ++read_count;
            }
        }
        if (read_count == 0) {
            // ⑥ 全是 WRITE，无法为新的镜像让位
            last_failure_detail_ = std::string("phase=MoveFrom subphase=full_no_readable used=")
                + std::to_string(entries_.size())
                + " capacity=" + std::to_string(capacity_in_images_)
                + " write_count=" + std::to_string(entries_.size());
            return volumemanager::ErrorCode::VOLUME_FULL_NO_READABLE;
        }

        // ⑦ 找 LRU 末尾的 READ victim 并备份，准备 rename 失败时回滚。
        // read_count > 0 且 entries_ 已满 ⇒ WRITE 数 < capacity ⇒ 一定能找到 READ victim。
        auto victim_it = lru_list_.end();
        for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
            // *it is entries_ iterator, so we dereference once more to get the pair
            if ((*it)->second.category == ImageCategory::READ) {
                victim_it = std::prev(it.base());
                break;
            }
        }
        // victim_it is lru_list_ iterator, dereference to get entries_ iterator
        const uint64_t victim_id = (*victim_it)->first;
        rollback_entry_ = (*victim_it)->second;          // 备份完整条目（含 filename）
        rollback_active_ = true;

        ret = DeleteFileAndEntry(victim_id);          // 内部已持 unique_lock，合法
        if (ret != volumemanager::ErrorCode::SUCCESS) {
            // ⑦ unlink 失败：rollback 状态作废（被换出的 victim 已从管理表移走，
            // 文件可能仍存在；调用方后续可重试 MoveFrom）
            rollback_active_ = false;
            last_failure_detail_ = std::string("phase=MoveFrom subphase=evict_unlink_failed")
                + " volume_id=" + std::to_string(victim_id)
                + " ret=" + std::to_string(static_cast<int>(ret));
            return ret;
        }
    }

    // 拼接目标路径：image_dir_/<basename>（扇平布局，2026-08-15）
    std::string dst_path = image_dir_path_;
    if (!dst_path.empty() && dst_path.back() != '/') {
        dst_path.push_back('/');
    }
    dst_path += basename;

    // POSIX rename(2)：可能跨目录原子移动（src 通常在 temp_dir_ 或 disc_sim_dir_）。
    if (rename(abs_path.c_str(), dst_path.c_str()) != 0) {
        const int rename_errno = errno;
        // rename 失败：rollback 之前换出的 READ 镜像（仅元数据，文件已 unlink）
        if (rollback_active_) {
            InsertEntryLocked(rollback_entry_);

            // detail 里明确说被换出的文件不可恢复 —— 上层运维需要重生成。
            // evicted_file 拼 image_dir_path_ + rollback_entry_.filename 给出全路径，
            // 便于排查；末尾 evicted_file_recoverable=false 供上层做精确告警。
            last_failure_detail_ = std::string("phase=MoveFrom subphase=rename_failed")
                + " errno=" + std::to_string(rename_errno)
                + " src=" + abs_path
                + " dst=" + dst_path
                + " evicted_volume_id=" + std::to_string(rollback_entry_.volume_id)
                + " evicted_file=" + image_dir_path_ + rollback_entry_.filename
                + " evicted_file_recoverable=false";
        } else {
            last_failure_detail_ = std::string("phase=MoveFrom subphase=rename_failed")
                + " errno=" + std::to_string(rename_errno)
                + " src=" + abs_path
                + " dst=" + dst_path;
        }
        rollback_active_ = false;
        return volumemanager::ErrorCode::IO_ERROR;
    }

    // 成功路径：清空 rollback 状态 + 失败 detail，登记新条目
    rollback_active_ = false;
    last_failure_detail_.clear();

    ImageEntry entry;
    entry.volume_id = volume_id;
    entry.category = category;
    entry.filename = basename;
    entry.last_access = std::chrono::steady_clock::now();
    InsertEntryLocked(entry);
    return volumemanager::ErrorCode::SUCCESS;
}

volumemanager::ErrorCode ImageDirManager::RemoveSingleReadImage() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return RemoveSingleReadImageLocked();
}

volumemanager::ErrorCode ImageDirManager::RemoveSingleReadImageLocked() {
    // 倒序找最久未访问的 READ（链表 back 是最久未访问的）。
    auto victim_it = lru_list_.end();
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        if ((*it)->second.category == ImageCategory::READ) {
            victim_it = std::prev(it.base());
            break;
        }
    }
    if (victim_it == lru_list_.end()) {
        return volumemanager::ErrorCode::VOLUME_NOT_FOUND;
    }
    const uint64_t victim_id = (*victim_it)->first;
    // 删除走 DeleteFileAndEntry，2026-08-15 适配模拟光盘库后改为 rename 到
    // disc_sim_dir_/volume_<id>.vimg，等价于"把光盘放回库位"——详见该函数说明。
    return DeleteFileAndEntry(victim_id);
}

volumemanager::ErrorCode ImageDirManager::RemoveWriteImage(uint64_t volume_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = entries_.find(volume_id);
    if (it == entries_.end() || it->second.category != ImageCategory::WRITE) {
        return volumemanager::ErrorCode::VOLUME_NOT_FOUND;
    }
    // 走 DeleteFileAndEntry，2026-08-15 适配模拟光盘库后改为 rename 到
    // disc_sim_dir_/volume_<id>.vimg，等价于"刻完的光盘弹出到库位"——详见该函数说明。
    return DeleteFileAndEntry(volume_id);
}

volumemanager::ErrorCode ImageDirManager::GetStats(ImageDirStats& out_stats) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    out_stats.capacity_images = capacity_in_images_;
    out_stats.used_images = entries_.size();
    out_stats.read_images = 0;
    out_stats.write_images = 0;
    for (const auto& pair : entries_) {
        if (pair.second.category == ImageCategory::READ) {
            ++out_stats.read_images;
        } else {
            ++out_stats.write_images;
        }
    }
    out_stats.free_images =
        (out_stats.used_images >= out_stats.capacity_images)
            ? 0
            : (out_stats.capacity_images - out_stats.used_images);
    return volumemanager::ErrorCode::SUCCESS;
}

volumemanager::ErrorCode ImageDirManager::RebuildManagementTable() {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    entries_.clear();
    lru_list_.clear();
    lru_index_.clear();
    capacity_set_ = false;

    // 扇平布局（2026-08-15）：扫描 image_dir_ 单层目录。
    // 已知遗留（用户接受）：扇平后无法从"所在子目录"反推 category，
    // 当前把所有扫描到的镜像统一登记为 ImageCategory::READ。区分读写镜像的问题
    // 后续另说——参见头文件中 RebuildManagementTable 文档。
    const std::string flat_dir = NormalizeImageDir(image_dir_path_);
    DIR* dir = opendir(flat_dir.c_str());
    if (dir == nullptr) {
        return volumemanager::ErrorCode::IO_ERROR;
    }
    struct dirent* entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
        const std::string name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        // 跳过隐藏目录 / 临时残留（如 ".tmp" / ".stale" 之类），仅取 volume_*.vimg。
        // 当前没有别的子目录需要担心（扇平后 read/ write/ 子目录都不应存在），
        // 但若用户曾运行过旧版残留 read/write 子目录，本循环会把它们当成条目
        // ——所以这里额外跳过没有体积文件名格式的目录项。
        uint64_t volume_id = 0;
        if (!TryParseVolumeId(name, volume_id)) {
            continue;
        }
        ImageEntry img_entry;
        img_entry.volume_id = volume_id;
        img_entry.category = ImageCategory::READ;   // 见上文：扇平后统一标 READ
        img_entry.filename = name;
        img_entry.last_access = std::chrono::steady_clock::now()
                               - std::chrono::hours(1);
        InsertEntryLocked(img_entry);
    }
    closedir(dir);

    return volumemanager::ErrorCode::SUCCESS;
}

}  // namespace space_manager
