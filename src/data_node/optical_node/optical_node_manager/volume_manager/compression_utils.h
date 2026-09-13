#pragma once

#include "error_codes.h"
#include <cstdint>
#include <string>
#include <vector>

/**
 * @file compression_utils.h
 * @brief 压缩/解压缩工具类
 */

namespace volumemanager {

/**
 * @brief 压缩/解压缩工具类
 */
class CompressionUtils {
public:
    /**
     * @brief 压缩文件
     * @param input_path 输入文件路径
     * @param output_path 输出压缩文件路径
     * @return ErrorCode 操作结果
     */
    static ErrorCode CompressFile(const std::string& input_path,
                                   const std::string& output_path);

    /**
     * @brief 解压文件
     * @param input_path 输入压缩文件路径
     * @param output_path 输出解压文件路径
     * @return ErrorCode 操作结果
     */
    static ErrorCode DecompressFile(const std::string& input_path,
                                     const std::string& output_path);

    /**
     * @brief 压缩数据
     * @param input 输入数据
     * @param output 输出压缩数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode CompressData(const std::vector<uint8_t>& input,
                                   std::vector<uint8_t>& output);

    /**
     * @brief 解压数据
     * @param input 输入压缩数据
     * @param original_size 原始数据大小（用于分配解压缓冲区）
     * @param output 输出解压数据
     * @return ErrorCode 操作结果
     */
    static ErrorCode DecompressData(const std::vector<uint8_t>& input,
                                     uint64_t original_size,
                                     std::vector<uint8_t>& output);

    /**
     * @brief 获取压缩文件名
     * @param original_name 原始文件名
     * @return 压缩文件名
     */
    static std::string GetCompressedFileName(const std::string& original_name);

    /**
     * @brief 从压缩文件名获取原始文件名
     * @param compressed_name 压缩文件名
     * @return 原始文件名
     */
    static std::string GetOriginalFileName(const std::string& compressed_name);

    /**
     * @brief 检查是否为压缩文件名
     * @param filename 文件名
     * @return true 如果是压缩文件名
     */
    static bool IsCompressedFileName(const std::string& filename);

private:
    // 压缩后缀
    static constexpr const char* COMPRESSED_SUFFIX = ".compressed";
};

} // namespace volumemanager
