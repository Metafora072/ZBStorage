#pragma once

#include "volume_metadata.h"
#include "error_codes.h"
#include <string>
#include <vector>

/**
 * @file serializer.h
 * @brief 序列化/反序列化接口
 */

namespace volumemanager {

/**
 * @brief 序列化器类，用于元数据的序列化和反序列化
 */
class Serializer {
public:
    /**
     * @brief 序列化卷镜像元数据
     * @param metadata 卷镜像元数据
     * @param output 输出二进制数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode SerializeVolumeMetadata(const VolumeMetadata& metadata,
                                              std::vector<uint8_t>& output);

    /**
     * @brief 反序列化卷镜像元数据
     * @param input 输入二进制数据
     * @param metadata 输出卷镜像元数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode DeserializeVolumeMetadata(const std::vector<uint8_t>& input,
                                              VolumeMetadata& metadata);

    /**
     * @brief 序列化文件元数据
     * @param metadata 文件元数据
     * @param output 输出二进制数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode SerializeFileMetadata(const FileMetadata& metadata,
                                            std::vector<uint8_t>& output);

    /**
     * @brief 反序列化文件元数据
     * @param input 输入二进制数据
     * @param metadata 输出文件元数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode DeserializeFileMetadata(const std::vector<uint8_t>& input,
                                            FileMetadata& metadata);

    /**
     * @brief 验证卷镜像元数据
     * @param metadata 卷镜像元数据
     * @return true 如果有效，false 如果无效
     */
    static bool ValidateVolumeMetadata(const VolumeMetadata& metadata);

private:
    /**
     * @brief 写入字符串到二进制数据
     * @param str 字符串
     * @param output 输出数据
     */
    static void WriteString(const std::string& str, std::vector<uint8_t>& output);

    /**
     * @brief 从二进制数据读取字符串
     * @param input 输入数据
     * @param offset 读取偏移
     * @param str 输出字符串
     * @return ErrorCode 操作结果
     */
    static ErrorCode ReadString(const std::vector<uint8_t>& input,
                                 uint32_t& offset,
                                 std::string& str);
};

} // namespace volumemanager
