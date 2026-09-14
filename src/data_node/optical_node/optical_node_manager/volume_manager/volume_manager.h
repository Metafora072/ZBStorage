#pragma once

#include "volume_metadata.h"
#include "error_codes.h"
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @file volume_manager.h
 * @brief 卷镜像管理器主接口
 */

namespace volumemanager {

/**
 * @brief 卷镜像管理器类
 */
class VolumeManager {
public:
    /**
     * @brief 构造函数
     * @param volume_size 卷镜像大小限制（字节），默认1GB
     * @param size_threshold 触发镜像封装的大小阈值（百分比0.0-1.0），默认0.9（90%）
     */
    explicit VolumeManager(uint64_t volume_size = 1024 * 1024 * 1024,
                           double size_threshold = 0.9);

    /**
     * @brief 析构函数
     */
    ~VolumeManager();

    // 禁止拷贝构造和拷贝赋值
    VolumeManager(const VolumeManager&) = delete;
    VolumeManager& operator=(const VolumeManager&) = delete;

    /**
     * @brief 将文件纳入待封装文件集
     * @param file_path 文件相对路径
     * @return ErrorCode 操作结果
     */
    // ErrorCode AddFileToCollect(const std::string& file_path);
    ErrorCode AddFileToCollect(const std::string& file_path, uint64_t inode_id,
                               uint64_t volume_id = 0);

    /**
     * @brief 根据inode_id读取文件内容
     * @param inode_id 文件的全局唯一标识
     * @param output_path 输出中间文件的相对路径
     * @return ErrorCode 操作结果
     */
    ErrorCode ReadFile(uint64_t inode_id, std::string& output_path);

    /**
     * @brief 读取卷镜像的所有inode和目录文件数据
     * @param volume_id 卷镜像ID
     * @param output_path 输出中间文件的相对路径
     * @return ErrorCode 操作结果
     */
    ErrorCode ReadAllInodesAndDirectories(uint64_t volume_id,
                                           std::string& output_path);

    /**
     * @brief 卸载卷镜像，释放相关资源
     * @param volume_id 卷镜像ID
     * @return ErrorCode 操作结果
     */
    ErrorCode UnmountVolume(uint64_t volume_id);

    /**
     * @brief 挂载卷镜像，重建管理资源
     * @param volume_id 卷镜像ID
     * @return ErrorCode 操作结果
     */
    ErrorCode MountVolume(const std::string& volume_id);

    /**
     * @brief 设置基础路径
     * @param base_path 基础路径
     * @return ErrorCode 操作结果
     */
    ErrorCode SetBasePath(const std::string& base_path);

    /**
     * @brief 设置临时目录（AddFileToCollect 中间压缩产物 temp_<inode>.compressed、
     *                     PackVolume 输出 volume_<id>.vimg 共用此目录）
     * @param temp_dir 临时目录路径
     * @return ErrorCode 操作结果
     */
    ErrorCode SetTempDir(const std::string& temp_dir);

    /**
     * @brief 设置卷镜像检测目录（MountVolume 查找 volume_<id>.vimg 的位置）
     * @param image_dir 镜像目录路径
     * @return ErrorCode 操作结果
     */
    ErrorCode SetImageDir(const std::string& image_dir);

    /**
     * @brief 设置 ReadFile 输出目录（解压产物 file_<inode>_<name> 位置）
     * @param read_cache_dir 解压输出目录路径
     * @return ErrorCode 操作结果
     */
    ErrorCode SetReadCacheDir(const std::string& read_cache_dir);

public:
    std::string base_path_;                          // 基础路径（AddFileToCollect 源文件位置）
    std::string temp_dir_;                           // 临时目录（temp_<inode>.compressed / PackVolume 输出）
    std::string image_dir_;                          // 卷镜像检测目录（MountVolume 读取位置）
    std::string read_cache_dir_;                     // ReadFile 输出目录
    std::unordered_map<uint64_t, VolumeMetadata> mounted_volumes_;  // 已挂载的卷镜像
    std::unordered_map<uint64_t, FileMetadata> file_metadata_cache_;   // 文件元数据缓存
    std::vector<FileMetadata> pending_files_;        // 待封装文件集
    uint64_t pending_size_;                          // 待封装文件总大小
    uint64_t volume_size_;                          // 卷镜像大小限制
    double size_threshold_;                         // 触发镜像封装的大小阈值（百分比）

    /**
     * @brief 初始化 temp_dir_ / image_dir_ / read_cache_dir_ 三个目录
     * @return ErrorCode 操作结果
     */
    ErrorCode InitializeTempDir();

    /**
     * @brief 生成唯一的卷镜像ID
     * @return volume_id
     */
    uint64_t GenerateVolumeId();

    /**
     * @brief 生成唯一的inode ID
     * @return inode_id
     */
    uint64_t GenerateInodeId();

    /**
     * @brief 封装卷镜像
     * @param output_path 输出卷镜像文件路径
     * @return ErrorCode 操作结果
     */
    ErrorCode PackVolume(std::string& output_path, uint64_t volume_id = 0);

    /**
     * @brief 检查是否需要封装卷镜像
     * @return true 如果需要封装
     */
    bool ShouldPackVolume() const;
};

} // namespace volumemanager
