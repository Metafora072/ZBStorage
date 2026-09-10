#pragma once

#include <cstdint>
#include <ctime>
#include <string>

/**
 * @file volume_metadata.h
 * @brief 卷镜像和文件元数据定义
 */

namespace volumemanager {

// 魔数标识 "VIMG"
constexpr uint64_t MAGIC_NUMBER = 0x56494D47ULL;
// 当前卷镜像格式版本号
constexpr uint32_t CURRENT_VERSION = 1;
// 非法卷镜像ID
constexpr uint64_t INVALID_VOLUME_ID = 0ULL;
// 非法inode ID
constexpr uint64_t INVALID_INODE_ID = 0ULL;

/**
 * @brief 卷镜像元数据结构
 */
struct VolumeMetadata {
    uint64_t magic_number;        // 魔数标识 "VIMG" (0x56494D47)
    uint32_t version;              // 卷镜像格式版本号
    uint64_t volume_id;           // 全局唯一的卷镜像ID
    uint64_t volume_size;         // 卷镜像总大小
    uint32_t metadata_offset;     // 元数据区偏移
    uint32_t metadata_size;        // 元数据区大小
    uint32_t directory_offset;     // 目录数据区偏移
    uint32_t directory_size;       // 目录数据区大小
    uint32_t user_data_offset;     // 用户数据区偏移
    uint32_t user_data_size;       // 用户数据区大小
    uint32_t file_count;           // 文件数量

    /**
     * @brief 构造函数，初始化默认值
     */
    VolumeMetadata()
        : magic_number(MAGIC_NUMBER)
        , version(CURRENT_VERSION)
        , volume_id(INVALID_VOLUME_ID)
        , volume_size(0)
        , metadata_offset(0)
        , metadata_size(0)
        , directory_offset(0)
        , directory_size(0)
        , user_data_offset(0)
        , user_data_size(0)
        , file_count(0) {}
};

/**
 * @brief 文件元数据结构（Linux文件系统标准元数据）
 */
struct FileMetadata {
    uint64_t inode_id;               // 全局唯一的inode ID
    uint64_t file_size;              // 原始文件大小
    uint64_t compressed_size;         // 压缩后文件大小
    uint64_t volume_id;               // 所属的卷镜像ID，未封装时为非法值
    uint32_t offset_in_volume;       // 在卷镜像中的偏移量，未封装时无意义
    std::string file_name;           // 原始文件名（不含路径）
    std::string compressed_file_name;  // 压缩文件名
    uint32_t file_mode;              // 文件权限和类型信息（st_mode）
    time_t last_modified;            // 最后修改时间（st_mtime）
    bool is_directory;               // 是否为目录
    bool is_compressed;              // 是否为压缩文件

    /**
     * @brief 构造函数，初始化默认值
     */
    FileMetadata()
        : inode_id(INVALID_INODE_ID)
        , file_size(0)
        , compressed_size(0)
        , volume_id(INVALID_VOLUME_ID)
        , offset_in_volume(0)
        , file_mode(0)
        , last_modified(0)
        , is_directory(false)
        , is_compressed(false) {}
};

} // namespace volumemanager
