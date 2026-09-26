#pragma once

#include <string>

/**
 * @file sha256.h
 * @brief 自包含的流式 SHA-256 实现（不依赖 OpenSSL 等外部库）。
 *
 * 卷镜像（vimg）可达数 GiB，因此只提供"按文件流式计算"的接口，
 * 不把整文件读入内存。
 */

namespace space_manager {

/**
 * @brief 流式计算文件的 SHA-256，输出小写十六进制字符串
 *
 * 按固定大小分块读取文件并增量更新摘要，内存占用与文件大小无关。
 *
 * @param path    待计算的文件路径
 * @param out_hex 输出：64 个字符的小写十六进制摘要；失败时被清空
 * @return true  - 计算成功
 *         false - 文件无法打开或读取失败（*out_hex 被清空）
 */
bool Sha256FileHex(const std::string& path, std::string* out_hex);

}  // namespace space_manager