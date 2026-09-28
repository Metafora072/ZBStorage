#include "space_manager/space_manager.h"

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

SpaceManager::SpaceManager(std::string image_dir_path)
    : image_dir_path_(std::move(image_dir_path)),
      capacity_in_images_(0),
      capacity_set_(false) {
    EnsureImageDir(image_dir_path_);
}

SpaceManager::~SpaceManager() = default;

void SpaceManager::ReadLock() {
    mutex_.lock_shared();
}

void SpaceManager::ReadUnlock() {
    mutex_.unlock_shared();
}

volumemanager::ErrorCode SpaceManager::SetCapacityInImages(uint64_t capacity_in_images) {
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

volumemanager::ErrorCode SpaceManager::ParseVolumeFile(const std::string& abs_path,
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

bool SpaceManager::HasImage(uint64_t volume_id) const {
    return entries_.find(volume_id) != entries_.end();
}

void SpaceManager::Touch(uint64_t volume_id) {
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

void SpaceManager::RemoveLRU(uint64_t volume_id) {
    auto idx_it = lru_index_.find(volume_id);
    if (idx_it == lru_index_.end()) {
        return;
    }
    lru_list_.erase(idx_it->second);
    lru_index_.erase(idx_it);
}

volumemanager::ErrorCode SpaceManager::DeleteFileAndEntry(uint64_t volume_id) {
    auto it = entries_.find(volume_id);
    if (it == entries_.end()) {
        return volumemanager::ErrorCode::VOLUME_NOT_FOUND;
    }
    const std::string src_path = FullPathForEntry(it->second);

    // 先移管理表项：DeleteFileAndEntry 的调用方（RemoveSingleReadImageLocked /
    // RemoveWriteImage / MoveFrom 容量满时换出 victim）都需要 entries_ 立刻腾
    // 出位置——尤其是 MoveFrom 必须在 try-evict 后腾出槽位才能继续登记新镜像。
    // 即使后续 unlink 失败，entries_ 已经没有这个条目了，对调用方来说是"已释放"。
    RemoveLRU(volume_id);
    entries_.erase(it);

    // 光盘打包改造（2026-09-26）：镜像数据已随刻录持久化到 disc_sim_dir_ 下的
    // vdisc 光盘文件中，image_dir_ 内的 vimg 只是缓存副本，释放即直接删除，
    // 不再搬移到 disc_sim_dir_。
    if (unlink(src_path.c_str()) != 0) {
        const int unlink_errno = errno;
        if (unlink_errno == ENOENT) {
            // 管理表存在但物理文件已被外部清理：视为已释放，避免误报。
            return volumemanager::ErrorCode::SUCCESS;
        }
        last_failure_detail_ = std::string("phase=DeleteFileAndEntry subphase=unlink_failed")
            + " errno=" + std::to_string(unlink_errno)
            + " src=" + src_path
            + " volume_id=" + std::to_string(volume_id);
        return volumemanager::ErrorCode::IO_ERROR;
    }
    return volumemanager::ErrorCode::SUCCESS;
}

void SpaceManager::InsertEntryLocked(const ImageEntry& entry) {
    // 不变量：entries_ / lru_list_ / lru_index_ 三件套同步更新。
    // entries_.emplace 在 volume_id 已存在时不会替换；但 MoveFrom / RebuildManagementTable
    // 已在调用本方法前保证 volume_id 是新的（前者通过 entries_.find 检查，后者从空表起步）。
    auto insert_ret = entries_.emplace(entry.volume_id, entry);
    lru_list_.push_front(insert_ret.first);
    lru_index_.emplace(entry.volume_id, lru_list_.begin());
}

volumemanager::ErrorCode SpaceManager::MoveFrom(const std::string& abs_path,
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

volumemanager::ErrorCode SpaceManager::RemoveSingleReadImage() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    return RemoveSingleReadImageLocked();
}

volumemanager::ErrorCode SpaceManager::RemoveSingleReadImageLocked() {
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
    // 释放走 DeleteFileAndEntry：直接删除 image_dir_ 内的 vimg（镜像数据在
    // disc_sim_dir_ 的 vdisc 中持久保存）——详见该函数说明。
    return DeleteFileAndEntry(victim_id);
}

volumemanager::ErrorCode SpaceManager::RemoveWriteImage(uint64_t volume_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = entries_.find(volume_id);
    if (it == entries_.end() || it->second.category != ImageCategory::WRITE) {
        return volumemanager::ErrorCode::VOLUME_NOT_FOUND;
    }
    // 刻录完成后释放写镜像：直接删除 image_dir_ 内的 vimg——详见 DeleteFileAndEntry。
    return DeleteFileAndEntry(volume_id);
}

volumemanager::ErrorCode SpaceManager::GetStats(ImageDirStats& out_stats) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    out_stats.capacity_images = capacity_in_images_;
    out_stats.used_images = entries_.size();
    out_stats.read_images = CountEntriesByCategory(ImageCategory::READ);
    out_stats.write_images = CountEntriesByCategory(ImageCategory::WRITE);
    out_stats.free_images =
        (out_stats.used_images >= out_stats.capacity_images)
            ? 0
            : (out_stats.capacity_images - out_stats.used_images);
    return volumemanager::ErrorCode::SUCCESS;
}

uint64_t SpaceManager::GetWriteImageCount() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return CountEntriesByCategory(ImageCategory::WRITE);
}

std::vector<ImageSnapshotEntry> SpaceManager::SnapshotImageEntries() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<ImageSnapshotEntry> out;
    out.reserve(entries_.size());
    // lru_list_：front = 最近访问，back = 最久未访问。
    // 逆序遍历即「由旧到新」，与 RebuildManagementTable 的插入顺序约定一致
    // （插入方逐个 push_front，最后插入的最新，正好落在 front）。
    for (auto it = lru_list_.rbegin(); it != lru_list_.rend(); ++it) {
        const ImageEntry& entry = (*it)->second;
        ImageSnapshotEntry snapshot_entry;
        snapshot_entry.volume_id = entry.volume_id;
        snapshot_entry.category = entry.category;
        out.push_back(snapshot_entry);
    }
    return out;
}

uint64_t SpaceManager::CountEntriesByCategory(ImageCategory category) const {
    uint64_t count = 0;
    for (const auto& pair : entries_) {
        if (pair.second.category == category) {
            ++count;
        }
    }
    return count;
}

volumemanager::ErrorCode SpaceManager::RebuildManagementTable() {
    return RebuildManagementTable(std::vector<ImageSnapshotEntry>(), nullptr);
}

volumemanager::ErrorCode SpaceManager::RebuildManagementTable(
    const std::vector<ImageSnapshotEntry>& snapshot,
    RebuildReport* out_report) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    entries_.clear();
    lru_list_.clear();
    lru_index_.clear();
    capacity_set_ = false;

    if (out_report != nullptr) {
        out_report->snapshot_only.clear();
        out_report->disk_only.clear();
    }

    // ① 目录扫描 = 存在性基线（扇平布局：扫描 image_dir_ 单层目录）。
    //    只有磁盘上确实存在的镜像才可能被登记；快照里有、磁盘上没有的镜像
    //    其物理数据已丢失，不登记（避免 HasImage 说谎、MoveFrom/RemoveWriteImage
    //    对不存在的文件操作）。
    const std::string flat_dir = NormalizeImageDir(image_dir_path_);
    DIR* dir = opendir(flat_dir.c_str());
    if (dir == nullptr) {
        return volumemanager::ErrorCode::IO_ERROR;
    }
    std::unordered_map<uint64_t, std::string> on_disk;  // volume_id -> basename
    struct dirent* entry = nullptr;
    while ((entry = readdir(dir)) != nullptr) {
        const std::string name(entry->d_name);
        if (name == "." || name == "..") {
            continue;
        }
        // 跳过非 volume_<id>.vimg 项：隐藏文件 / 临时残留，以及旧版可能遗留的
        // read/ write/ 子目录项（扇平后不应存在，但不做假设）。
        uint64_t volume_id = 0;
        if (!TryParseVolumeId(name, volume_id)) {
            continue;
        }
        // 同名文件不会重复出现；emplace 保证首次（也是唯一一次）写入。
        on_disk.emplace(volume_id, name);
    }
    closedir(dir);

    // ② 按「快照由旧到新」的顺序插入：InsertEntryLocked 逐个 push_front，
    //    因此最后插入的最新镜像落在队首，恰好还原原 LRU 顺序。
    const std::chrono::steady_clock::time_point restored_at = std::chrono::steady_clock::now();
    auto insert_from_disk = [&](uint64_t volume_id, const std::string& basename,
                                ImageCategory category) {
        ImageEntry img_entry;
        img_entry.volume_id = volume_id;
        img_entry.category = category;
        img_entry.filename = basename;
        // LRU 顺序由 lru_list_ 承载（淘汰时逆序取第一个 READ），last_access 仅供参考。
        img_entry.last_access = restored_at;
        InsertEntryLocked(img_entry);
    };

    for (const ImageSnapshotEntry& snapshot_entry : snapshot) {
        // 快照内重复的 volume_id 只取首次：entries_.emplace 不会替换，
        // 若继续 push_front 会让 lru_list_ 与 entries_ 长度不一致，破坏三件套不变量。
        // 该判断必须在磁盘查找之前——首次出现已把 id 从 on_disk 中移除，
        // 若放在之后，重复项会因为「磁盘上找不到」而被误报为 snapshot_only。
        if (entries_.find(snapshot_entry.volume_id) != entries_.end()) {
            continue;
        }
        auto disk_it = on_disk.find(snapshot_entry.volume_id);
        if (disk_it == on_disk.end()) {
            // 快照有记录但磁盘已无该文件（物理镜像已被换出 / 删除）。
            if (out_report != nullptr) {
                out_report->snapshot_only.push_back(snapshot_entry.volume_id);
            }
            continue;
        }
        insert_from_disk(snapshot_entry.volume_id, disk_it->second, snapshot_entry.category);
        on_disk.erase(disk_it);
    }

    // ③ 磁盘独有（快照未记录）：无 category 依据 → READ；置于 LRU 最新端
    //    （最后插入 ⇒ 队首），避免未刻录的写镜像被立刻当作 victim 删除。
    //    按 volume_id 升序插入，保证同一份输入的结果可复现。
    std::vector<uint64_t> disk_only_ids;
    disk_only_ids.reserve(on_disk.size());
    for (const auto& kv : on_disk) {
        disk_only_ids.push_back(kv.first);
    }
    std::sort(disk_only_ids.begin(), disk_only_ids.end());
    for (uint64_t volume_id : disk_only_ids) {
        if (out_report != nullptr) {
            out_report->disk_only.push_back(volume_id);
        }
        insert_from_disk(volume_id, on_disk.at(volume_id), ImageCategory::READ);
    }

    return volumemanager::ErrorCode::SUCCESS;
}

}  // namespace space_manager
