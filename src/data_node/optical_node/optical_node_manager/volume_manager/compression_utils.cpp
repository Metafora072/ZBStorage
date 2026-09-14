#include "compression_utils.h"
#include <zlib.h>
#include <fstream>
#include <cstring>

namespace volumemanager {

// 工具函数声明（在utils.cpp中定义）
ErrorCode ReadFileContent(const std::string& path, std::vector<uint8_t>& content);
ErrorCode WriteFileContent(const std::string& path, const std::vector<uint8_t>& content);

ErrorCode CompressionUtils::CompressData(const std::vector<uint8_t>& input,
                                         std::vector<uint8_t>& output) {
    if (input.empty()) {
        output.clear();
        return ErrorCode::SUCCESS;
    }

    // 计算压缩后的最大长度
    uLongf compressed_size = compressBound(static_cast<uLong>(input.size()));

    try {
        output.resize(compressed_size);
    } catch (const std::bad_alloc&) {
        return ErrorCode::OUT_OF_MEMORY;
    }

    // 执行压缩
    int ret = compress2(output.data(), &compressed_size,
                      input.data(), static_cast<uLong>(input.size()),
                      Z_BEST_COMPRESSION);

    if (ret != Z_OK) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    // 调整输出缓冲区大小为实际压缩大小
    output.resize(compressed_size);

    return ErrorCode::SUCCESS;
}

ErrorCode CompressionUtils::DecompressData(const std::vector<uint8_t>& input,
                                             uint64_t original_size,
                                             std::vector<uint8_t>& output) {
    if (input.empty()) {
        output.clear();
        return ErrorCode::SUCCESS;
    }

    try {
        output.resize(original_size);
    } catch (const std::bad_alloc&) {
        return ErrorCode::OUT_OF_MEMORY;
    }

    uLongf decompressed_size = static_cast<uLongf>(original_size);

    // 执行解压
    int ret = uncompress(output.data(), &decompressed_size,
                       input.data(), static_cast<uLong>(input.size()));

    if (ret != Z_OK) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    // 确保解压后的数据大小匹配预期
    if (decompressed_size != original_size) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    return ErrorCode::SUCCESS;
}

ErrorCode CompressionUtils::CompressFile(const std::string& input_path,
                                         const std::string& output_path) {
    // 读取输入文件
    std::vector<uint8_t> input_data;
    ErrorCode ret = ReadFileContent(input_path, input_data);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 压缩数据
    std::vector<uint8_t> compressed_data;
    ret = CompressData(input_data, compressed_data);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 写入压缩文件
    return WriteFileContent(output_path, compressed_data);
}

ErrorCode CompressionUtils::DecompressFile(const std::string& input_path,
                                           const std::string& output_path) {
    // 读取压缩文件
    std::vector<uint8_t> compressed_data;
    ErrorCode ret = ReadFileContent(input_path, compressed_data);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 注意：这里需要知道原始文件大小，但从压缩文件中无法直接获取
    // 实际使用时，需要从FileMetadata中获取原始大小
    // 这里提供一个简化版本，假设文件名中包含原始大小信息
    // 或者使用zlib的gzip格式（包含原始大小）

    // 由于zlib的compress/uncompress不存储原始大小，
    // 我们需要从其他地方（如FileMetadata）获取原始大小
    // 这里返回错误，需要调用DecompressData并传入原始大小

    return ErrorCode::INVALID_PARAMETER;
}

std::string CompressionUtils::GetCompressedFileName(const std::string& original_name) {
    return original_name + COMPRESSED_SUFFIX;
}

std::string CompressionUtils::GetOriginalFileName(const std::string& compressed_name) {
    size_t suffix_len = strlen(COMPRESSED_SUFFIX);
    if (compressed_name.size() > suffix_len) {
        size_t pos = compressed_name.find(COMPRESSED_SUFFIX);
        if (pos != std::string::npos &&
            pos + suffix_len == compressed_name.size()) {
            return compressed_name.substr(0, pos);
        }
    }
    return compressed_name;
}

bool CompressionUtils::IsCompressedFileName(const std::string& filename) {
    size_t suffix_len = strlen(COMPRESSED_SUFFIX);
    if (filename.size() > suffix_len) {
        size_t pos = filename.find(COMPRESSED_SUFFIX);
        return (pos != std::string::npos &&
                pos + suffix_len == filename.size());
    }
    return false;
}

} // namespace volumemanager
