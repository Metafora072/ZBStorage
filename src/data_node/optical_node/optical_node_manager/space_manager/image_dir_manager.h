#pragma once

#include <chrono>
#include <cstdint>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <volume_manager/error_codes.h>

/**
 * @file image_dir_manager.h
 * @brief image_dir 空间管理器
 *
 * 该模块负责管理 image_dir_ 目录下卷镜像的元信息与容量约束。
 * 镜像以"卷"为单位，按 LRU 替换；写镜像不可被替换，只能由上层刻录完毕后显式删除。
 */

namespace space_manager {

/**
 * @brief 镜像类别
 *
 * READ  : 读缓存镜像（可被 LRU 换出）
 * WRITE : 待刻录镜像（不可换出，由刻录流程显式释放）
 */
enum class ImageCategory {
    READ,
    WRITE,
};

/**
 * @brief 单个镜像条目（用于管理表）
 *
 * filename 是 basename（不带路径、不带任何子目录前缀），
 * 与 VolumeManager::PackVolume 的 output_path basename 约定保持一致。
 * 镜像在物理上全部平铺在 image_dir_/<basename>（2026-08-15 起改为扇平布局）：
 *   - 不再区分 read/ write/ 子目录；
 *   - category 仅作为语义标签（READ/WRITE），重建区分的问题后续再说。
 */
struct ImageEntry {
    uint64_t volume_id;                 // 卷镜像 ID（与文件名 volume_<id>.vimg 一致）
    ImageCategory category;             // 镜像类别（语义标签，物理上不再区分）
    std::string filename;               // 在 image_dir_ 下的文件名（basename，不带路径）
    std::chrono::steady_clock::time_point last_access;  // 最近一次访问时间（LRU 依据）
};

/**
 * @brief 镜像目录统计信息
 */
struct ImageDirStats {
    uint64_t capacity_images;           // 容量（镜像数上限）
    uint64_t used_images;               // 已占用镜像数
    uint64_t free_images;               // 剩余可用镜像数
    uint64_t read_images;               // 读镜像数（READ 类别）
    uint64_t write_images;              // 写镜像数（WRITE 类别）
};

/**
 * @brief image_dir 目录管理器
 *
 * 线程安全：所有接口内部加锁；查询类接口使用 shared_lock 并发，
 * 写入类接口使用 unique_lock 独占。
 *
 * 文件命名约定：volume_<id>.vimg，其中 <id> 为 uint64_t 类型的 volume_id。
 * 与 VolumeManager::PackVolume 的 output_path 格式保持一致：
 *     output_path = temp_dir_ + "volume_" + std::to_string(volume_id) + ".vimg"
 */
class ImageDirManager {
public:
    /**
     * @brief 构造函数
     * @param image_dir_path image_dir 目录绝对路径
     *
     * 注意：disc_sim_dir_（模拟光盘库目录）**不会**在构造函数注入。
     * 调用方必须显式调 SetDiscSimDir() 才能使用 LRU 换出 / 写镜像释放路径，
     * 否则 DeleteFileAndEntry 在拼"移出目标路径"时使用空字符串，
     * rename 必然 IO_ERROR。OpticalNodeManager::InitializeDir 流程
     * 会确保 SetDiscSimDir() 在 RebuildManagementTable 之后、首次
     * MoveFrom / RemoveSingleReadImage / RemoveWriteImage 之前调用。
     */
    explicit ImageDirManager(std::string image_dir_path);

    /**
     * @brief 析构函数
     */
    ~ImageDirManager();

    ImageDirManager(const ImageDirManager&) = delete;
    ImageDirManager& operator=(const ImageDirManager&) = delete;

    /**
     * @brief 设置模拟光盘库目录（disc_sim_dir）
     *
     * disc_sim_dir 是当前系统为适配"模拟光盘库（cd_manager_sim）"而引入的特殊目录：
     * 模拟光盘库不会真正销毁/弹出光盘——为了与"真实光盘库把光盘放回库位"的物理行为
     * 保持一致，ImageDirManager 的释放路径（RemoveSingleReadImage / RemoveWriteImage
     * / MoveFrom 容量满时换出 victim）不再是简单的 unlink，而是把镜像文件 rename(2)
     * 到 disc_sim_dir/ 下，让后续的 OnCDReadComplete 重新"从光盘库加载"时能够找到。
     *
     * **注入时机**：必须在 RebuildManagementTable() 之后、首次
     * RemoveSingleReadImage() / RemoveWriteImage() / MoveFrom() 之前调用。
     * OpticalNodeManager::InitializeDir() 会保证这个顺序。
     *
     * **重复调用**：允许重复调用（每次 Run() 路径都重新设置新路径），
     * 但**不**做"已注入"守门——上层调用方负责保证语义正确。
     *
     * @param disc_sim_dir_path disc_sim 目录绝对路径（带或不带尾斜杠均可）
     * @return volumemanager::ErrorCode
     *         SUCCESS                - 设置成功（含空字符串清空语义）
     *         INVALID_PATH           - 路径非法（当前实现兜底为空字符串）
     */
    volumemanager::ErrorCode SetDiscSimDir(std::string disc_sim_dir_path);

    /**
     * @brief 设置容量上限（镜像数）
     *
     * 系统运行期间视为固定约束。**仅允许首次调用**——重复调用返回
     * INVALID_PARAMETER（"上层保证容量不会缩小"通过本接口的"只调一次"
     * 语义来强制，避免运行时意外改写容量）。调用前应先完成
     * RebuildManagementTable()，以保证统计准确。
     *
     * @param capacity_in_images 容量（镜像数），必须 > 0
     * @return volumemanager::ErrorCode
     *         SUCCESS                - 设置成功
     *         INVALID_PARAMETER      - 容量为 0 或已设置过（重复调用）
     */
    volumemanager::ErrorCode SetCapacityInImages(uint64_t capacity_in_images);

    /**
     * @brief 从磁盘重建管理表
     *
     * 系统启动时调用一次。扫描 image_dir_ 单层目录（2026-08-15 起改为扇平布局，
     * 不再有 read/ write/ 子目录），从文件名（volume_<id>.vimg）重建管理表。
     *
     * **已知遗留（用户接受）**：扇平后无法从目录布局反推 category，当前实现把
     * 扫描到的所有镜像统一登记为 ImageCategory::READ（语义标签丢失）。区分读写
     * 镜像的问题后续另外处理（可能是 sidecar 元数据库 / mtime 启发式 / 上层
     * 注入分类表等）。WRITE 镜像若被错误标为 READ，下一次容量满时换出仍能正常
     * 执行（LRU evict 不区分读写外的语义），刻录完成后被 RemoveWriteImage 拒绝
     * (category mismatch → VOLUME_NOT_FOUND) 是已接受的临时缺陷。
     *
     * 调用前应保证 image_dir_ 已存在（构造函数已 EnsureImageDir）。
     * 若管理表已有数据，将被清空重建。
     *
     * @return volumemanager::ErrorCode
     *         SUCCESS         - 扫描成功（image_dir_ 为空也算成功）
     *         IO_ERROR        - image_dir_ 无法访问
     */
    volumemanager::ErrorCode RebuildManagementTable();

    /**
     * @brief 把外部文件移动到 image_dir_ 下，并登记到管理表
     *
     * 流程：
     *   1. 从 abs_path 中解析 volume_id（文件名必须为 volume_<id>.vimg）
     *   2. 若容量未设置（未调过 SetCapacityInImages）→ INVALID_PARAMETER
     *   3. 若管理表中已存在该 volume_id → VOLUME_ALREADY_EXISTS
     *   4. 若当前已用镜像数 == 容量：
     *      - 若没有任何 READ 镜像可换出 → VOLUME_FULL_NO_READABLE
     *      - 否则选择 LRU 末尾的 READ 镜像换出（DeleteFileAndEntry 内 unlink 文件）
     *      - 把被换出的镜像条目暂存到 rollback_entry_，供后续 rename 失败时回滚
     *   5. 调用 rename(2) 把文件移动到 image_dir_/<basename>
     *      - 若 rename 失败：把 rollback_entry_ 重新插入 entries_ / lru_list_ / lru_index_
     *        （注意：被换出的 READ 镜像文件已被 unlink，回滚仅恢复元数据，
     *         文件本身丢失，上层需自行重生成；detail 字段会记录 evicted_file=...）
     *   6. 登记到管理表，更新 LRU
     *
     * **错误码细分（与同名 enum 对应）**：
     *   - SUCCESS                - 移动成功
     *   - INVALID_PATH           - abs_path 为空
     *   - INVALID_VOLUME_FORMAT  - 文件名不符合 volume_<id>.vimg
     *   - INVALID_PARAMETER      - 容量未设置
     *   - VOLUME_ALREADY_EXISTS  - 该 volume_id 已在 image_dir_ 中
     *   - VOLUME_FULL_NO_READABLE - 容量已满且无 READ 镜像可换出
     *   - IO_ERROR               - rename 失败（含回滚）或换出时 unlink 失败
     *
     * @param abs_path 源文件绝对路径（必须在同一文件系统下）
     * @param category 镜像类别（READ / WRITE）
     * @return volumemanager::ErrorCode
     */
    volumemanager::ErrorCode MoveFrom(const std::string& abs_path,
                                      ImageCategory category);

    /**
     * @brief LRU 换出最久未访问的读镜像
     *
     * 写镜像不会被换出。若当前没有任何读镜像，返回 VOLUME_NOT_FOUND。
     *
     * **适配模拟光盘库（2026-08-15 用户决策）**：本方法不再简单地 unlink 镜像，
     * 而是把镜像文件 rename(2) 到 disc_sim_dir_/volume_<id>.vimg，
     * 等价于"把光盘放回光盘库"。这样：
     *   - 模拟 cd_manager 在后续 OnCDReadComplete 回调里（默认的源路径就是
     *     disc_sim_dir_/volume_<id>.vimg）能够再次找到该镜像；
     *   - 与"真实光盘库把光盘放回库位"的物理行为保持一致；
     *   - 模拟器下"重新读"语义得以闭环（不会因为 unlink 而丢失数据）。
     *
     * **前置条件**：调用 SetDiscSimDir() 注入 disc_sim_dir_，否则 rename 必然失败。
     *
     * @return volumemanager::ErrorCode
     *         SUCCESS             - 成功换出一个读镜像
     *         VOLUME_NOT_FOUND    - 当前没有读镜像可换出
     *         IO_ERROR            - rename 到 disc_sim_dir 失败
     */
    volumemanager::ErrorCode RemoveSingleReadImage();

    /**
     * @brief 刻录完成后释放指定写镜像
     *
     * 仅当对应 volume_id 存在且类别为 WRITE 时才会移动。
     *
     * **适配模拟光盘库（2026-08-15 用户决策）**：本方法不再简单地 unlink 镜像，
     * 而是把镜像文件 rename(2) 到 disc_sim_dir_/volume_<id>.vimg，
     * 等价于"刻完的光盘弹出到库位"。这样：
     *   - 模拟 cd_manager 的 BurnRequest.image_path 若再次指向 disc_sim_dir_，
     *     流程可被复用（虽然刻录通常是一次性的，但保留对称）；
     *   - 上层 OpticalNodeManager::OnCDBurnComplete 调本方法后,
     *     image_dir_/ 不再保留临时卷,disc_sim_dir_ 接管"已刻光盘库位"。
     *
     * **前置条件**：调用 SetDiscSimDir() 注入 disc_sim_dir_，否则 rename 必然失败。
     *
     * @param volume_id 待释放的卷镜像 ID
     * @return volumemanager::ErrorCode
     *         SUCCESS             - 移动成功
     *         VOLUME_NOT_FOUND    - 管理表中无该 volume_id 或类别不是 WRITE
     *         IO_ERROR            - rename 到 disc_sim_dir 失败
     */
    volumemanager::ErrorCode RemoveWriteImage(uint64_t volume_id);

    /**
     * @brief 获取目录统计信息
     *
     * @param out_stats 输出统计信息
     * @return volumemanager::ErrorCode
     *         SUCCESS - 始终成功
     */
    volumemanager::ErrorCode GetStats(ImageDirStats& out_stats) const;

    /**
     * @brief 获取 image_dir_ 路径
     */
    const std::string& image_dir_path() const { return image_dir_path_; }

    /**
     * @brief 获取 disc_sim_dir_ 路径（模拟光盘库目录，2026-08-15 引入）
     *
     * 注意：构造后必须显式调 SetDiscSimDir() 才会被设置，否则返回空字符串。
     * OpticalNodeManager::InitializeDir() 会保证 SetDiscSimDir() 在首次
     * RemoveSingleReadImage / RemoveWriteImage / MoveFrom 之前被调用。
     */
    const std::string& disc_sim_dir_path() const { return disc_sim_dir_path_; }

    /**
     * @brief 获取容量（镜像数）
     */
    uint64_t capacity_in_images() const { return capacity_in_images_; }

    /**
     * @brief 获取读锁（共享锁）
     *
     * 调用此方法获取读锁，允许多个线程同时持有读锁。
     * 若当前有线程持有写锁（独占锁），此方法将阻塞直到写锁释放。
     * 必须与 ReadUnlock() 成对使用。
     */
    void ReadLock();

    /**
     * @brief 释放读锁（共享锁）
     *
     * 释放一次读锁引用。若有多个线程同时持有读锁，
     * 调用一次仅释放当前线程的锁引用，其他读锁持有者不受影响。
     * 必须与 ReadLock() 成对使用。
     */
    void ReadUnlock();

    /**
     * @brief 检查指定 volume_id 的镜像是否已登记在管理表中
     *
     * 与 Touch() 共享同一组锁语义：纯查询，**不**修改任何状态，因此 const。
     *
     * **锁语义**：
     * - 调用方需持有 **读锁**（ReadLock 之后），**不要求 unique_lock**；
     * - 本方法内部 **不再加锁**，允许并发读锁下的并发调用；
     * - 由于仅做 `entries_.find(volume_id)`（unordered_map 只读路径，且所有写入
     *   都通过 unique_lock），并发 HasImage 不会破坏任何不变量。
     *
     * **语义注意**：
     * - 本方法只查 **管理表 entries_**。"存在"等价于"已被 RebuildManagementTable /
     *   MoveFrom 登记，且尚未被 RemoveSingleReadImage / RemoveWriteImage /
     *   MoveFrom 容量满换出 victim 释放"。
     * - **不**校验物理文件 `image_dir_/volume_<id>.vimg` 是否仍在——管理表与物理
     *   文件可能因外部运维误删而短暂不一致。若上层需要"管理表登记且物理存在"
     *   的强一致语义，应在 HasImage() == true 后再 stat 文件。
     *
     * **不做的事**：
     * - 不返回 `ErrorCode`，避免与"返回 false = 不存在"混淆（true/false 足够）；
     * - 不接受自定义匹配规则（如按 category 过滤）。需要时可后续新增
     *   `HasImageOfCategory(volume_id, category)`。
     *
     * @param volume_id 待查询的卷镜像 ID
     * @return true  - 管理表中存在该 volume_id
     *         false - 不存在（或已被换出 / 释放）
     */
    bool HasImage(uint64_t volume_id) const;

    /**
     * @brief 把指定 volume_id 的卷镜像访问时间设为系统当前时间
     *
     * 配合 ReadLock() / ReadUnlock() 使用：当上层完成对某卷的"读"动作
     * （例如 OpticalNodeManager 调用 VolumeManager::ReadFile / MountVolume
     * 之后），可以持读锁把该卷的 LRU 访问时间推前，避免它马上被淘汰。
     *
     * **锁语义**：
     * - 调用方需持有 **读锁**（ReadLock 之后）即可，**不要求 unique_lock**；
     * - 本方法内部 **不再加锁**，允许并发读锁下的并发调用；
     * - 由于始终把 `last_access` 设为 `steady_clock::now()`，即使多个线程
     *   并发 Touch 同一 volume_id，LRU 顺序的语义就是"最后写入者胜"，
     *   不会破坏 lru_list_ / lru_index_ 的结构不变量（splice 操作不会
     *   使 list 迭代器失效，且 splice + 写 last_access 不依赖中间状态）。
     *
     * **静默失败**：
     * - 若 volume_id 不在管理表中（未注册 / 已被换出），方法直接返回，
     *   不抛异常、不报错码。这是 LRU 的常见用法——上层 Touch 之前
     *   通常已经持有 `ReadFile / MountVolume` 的结果，命中即可推迟淘汰，
     *   未命中也无所谓。
     *
     * **不做的事**：
     * - 不修改 `category`（READ / WRITE 类别在注册时确定，本方法不变更）；
     * - 不接受自定义 `time_point` 参数；如需"伪装成更久未访问"等高级用法，
     *   留给后续 TODO 通过新增 TouchAs(time_point) 暴露。
     *
     * @param volume_id 待推迟淘汰的卷镜像 ID
     */
    void Touch(uint64_t volume_id);

    /**
     * @brief 从绝对路径解析出 volume_id 和文件名（basename）
     *
     * 仅接受文件名形如 volume_<uint64_t>.vimg 的路径。
     */
    volumemanager::ErrorCode ParseVolumeFile(const std::string& abs_path,
                                             uint64_t& volume_id,
                                             std::string& basename) const;

private:
    /**
     * @brief Touch() 的内部别名
     *
     * 与公开的 Touch() 完全等价。保留该名称用于：
     *   1. 兼容早期私有接口语义（如 DeleteFileAndEntry / 内部 MoveFrom 已持锁路径）；
     *   2. 让"内部已持锁调用"与"外部持读锁调用"在代码里通过名字区分意图。
     *
     * 调用方需持有 unique_lock 或 shared_lock（读锁即可，允许并发）。
     */
    void TouchLRU(uint64_t volume_id) { Touch(volume_id); }

    /**
     * @brief 从 LRU 中移除某 volume_id
     *
     * 调用方需持有 unique_lock。
     */
    void RemoveLRU(uint64_t volume_id);

    /**
     * @brief LRU 换出最久未访问的读镜像的内部实现
     *
     * 调用方需持有 unique_lock。被公开接口 RemoveSingleReadImage() 在已持锁路径下复用，
     * MoveFrom 不走这里（MoveFrom 走 DeleteFileAndEntry 直接释放指定 volume_id）。
     *
     * **适配模拟光盘库**：换出通过 DeleteFileAndEntry 落到 disc_sim_dir_，详见该函数说明。
     */
    volumemanager::ErrorCode RemoveSingleReadImageLocked();

    /**
     * @brief 把文件从 image_dir_ 移动到 disc_sim_dir_ 并清理管理表项
     *
     * **适配扇平布局（2026-08-15）**：`entry.filename` 就是 image_dir_/ 下的
     * basename（不再有 read/ write/ 子目录），所以 src_path = image_dir_/<basename>。
     *
     * **适配模拟光盘库（2026-08-15 用户决策）**：本方法不再简单地 unlink 文件，
     * 而是把 image_dir_/<basename> rename(2) 到 disc_sim_dir_/<basename>，
     * 等价于"把光盘放回光盘库位"。Rename 失败时优先 unlink 兜底（防止镜像残留在
     * image_dir_ 内膨胀），且即使 rename 失败，管理表项也已经移走——这是为了与
     * MoveFrom 容量满换出语义保持一致（MoveFrom 必须腾出 entries_ 槽位才能继续）。
     *
     * 调用方需持有 unique_lock。调用方需保证要释放的 volume_id 在 entries_ 中。
     *
     * **前置条件**：必须先调 SetDiscSimDir() 注入 disc_sim_dir_，否则 rename 必然失败。
     */
    volumemanager::ErrorCode DeleteFileAndEntry(uint64_t volume_id);

    /**
     * @brief 把 ImageEntry 原子地插入 entries_ + lru_list_ + lru_index_
     *
     * 这是模块唯一的 "三件套同步插入点"。MoveFrom（成功路径）与
     * RebuildManagementTable 必须通过它登记条目，避免三件套不变量被破坏。
     *
     * 不变量：
     *   - entries_.count(volume_id) == 1
     *   - lru_index_.count(volume_id) == 1
     *   - lru_list_.size() == entries_.size()
     *
     * 调用方需持有 unique_lock。**不处理容量约束**——容量校验在调用方完成。
     */
    void InsertEntryLocked(const ImageEntry& entry);

    // Private methods below

private:
    std::string image_dir_path_;                             // image_dir 绝对路径
    // disc_sim_dir_path_ 是模拟光盘库目录（适配 cd_manager_sim 引入，2026-08-15）：
    // RemoveSingleReadImage / RemoveWriteImage / MoveFrom 容量满时换出 victim 释放
    // 镜像时，会把 image_dir_/<basename> rename(2) 到本目录下，等价于
    // "把光盘放回库位"。允许为空（未注入时 DeleteFileAndEntry 的 rename 会失败
    // 并兜底 unlink，避免 image_dir_ 内残留——见 DeleteFileAndEntry 实现）。
    //
    // 注意：2026-08-15 起 image_dir_/ 内部不再划分子目录，因此 dst 也只有
    // disc_sim_dir_/volume_<id>.vimg 唯一一个位置。
    std::string disc_sim_dir_path_;
    uint64_t capacity_in_images_;                            // 容量（镜像数），0 表示尚未设置
    bool capacity_set_;                                      // 是否已设置过容量

    // 管理表：volume_id -> ImageEntry
    std::unordered_map<uint64_t, ImageEntry> entries_;

    // LRU 链表：最近访问的在 front，最久未访问的在 back。
    std::list<std::unordered_map<uint64_t, ImageEntry>::iterator> lru_list_;

    // LRU 反向索引：volume_id -> lru_list_ 中对应节点，便于 O(1) 删除。
    std::unordered_map<uint64_t,
                       std::list<std::unordered_map<uint64_t, ImageEntry>::iterator>::iterator>
        lru_index_;

    mutable std::shared_mutex mutex_;                        // 读写锁

    /**
     * @brief 把 ImageEntry 的 filename 拼成 image_dir_ 下的全路径
     *
     * 扇平布局（2026-08-15）：image_dir_/<basename>，不再有 read/ write/ 子目录。
     * 调用方负责确保 image_dir_path_ 后缀处理（缺尾 '/' 时补一个）。
     */
    std::string FullPathForEntry(const ImageEntry& entry) const {
        std::string full = image_dir_path_;
        if (!full.empty() && full.back() != '/') {
            full.push_back('/');
        }
        full += entry.filename;
        return full;
    }

    // last_failure_detail_ 记录 MoveFrom 最近一次失败的结构化 KV（subphase + kvs）。
    // 当前不暴露 public 接口：上层通过 ErrorCode 即可路由到分支，detail 仅供
    // 单元测试 / 后续接入 metrics 读取；与 OpticalNodeManager::last_failure_reason_buf_
    // 字段对齐命名风格。
    mutable std::string last_failure_detail_;

    // rollback_entry_ + rollback_active_：容量满场景下被换出 READ 镜像后，
    // 若 rename 失败，仅元数据（entries_ / lru_list_ / lru_index_）可回滚；
    // 被换出的文件本身已被 unlink，不可恢复。
    // 仅在 [换出成功] → [rename 失败] 区间内有效，rename 成功 / 下一次 MoveFrom
    // 入口时清空。
    ImageEntry rollback_entry_;
    bool rollback_active_ = false;
};

}  // namespace space_manager
