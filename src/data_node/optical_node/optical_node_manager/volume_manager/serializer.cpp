#include "serializer.h"
#include <cstring>
#include <stdexcept>
#include <iostream>

namespace volumemanager {

// 卷镜像元数据固定大小：56字节
constexpr uint32_t VOLUME_METADATA_SIZE = 56;

ErrorCode Serializer::SerializeVolumeMetadata(const VolumeMetadata& metadata,
                                                std::vector<uint8_t>& output) {
    output.resize(VOLUME_METADATA_SIZE);

    uint8_t* ptr = output.data();

    // 写入魔数（8字节）
    std::memcpy(ptr, &metadata.magic_number, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 写入版本号（4字节）
    std::memcpy(ptr, &metadata.version, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入卷镜像ID（8字节）
    std::memcpy(ptr, &metadata.volume_id, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 写入卷镜像总大小（8字节）
    std::memcpy(ptr, &metadata.volume_size, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 写入元数据区偏移（4字节）
    std::memcpy(ptr, &metadata.metadata_offset, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入元数据区大小（4字节）
    std::memcpy(ptr, &metadata.metadata_size, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入目录数据区偏移（4字节）
    std::memcpy(ptr, &metadata.directory_offset, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入目录数据区大小（4字节）
    std::memcpy(ptr, &metadata.directory_size, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入用户数据区偏移（4字节）
    std::memcpy(ptr, &metadata.user_data_offset, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入用户数据区大小（4字节）
    std::memcpy(ptr, &metadata.user_data_size, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 写入文件数量（4字节）
    std::memcpy(ptr, &metadata.file_count, sizeof(uint32_t));

    return ErrorCode::SUCCESS;
}

ErrorCode Serializer::DeserializeVolumeMetadata(const std::vector<uint8_t>& input,
                                                VolumeMetadata& metadata) {
    if (input.size() < VOLUME_METADATA_SIZE) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    const uint8_t* ptr = input.data();

    // 读取魔数（8字节）
    std::memcpy(&metadata.magic_number, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 读取版本号（4字节）
    std::memcpy(&metadata.version, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取卷镜像ID（8字节）
    std::memcpy(&metadata.volume_id, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 读取卷镜像总大小（8字节）
    std::memcpy(&metadata.volume_size, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);

    // 读取元数据区偏移（4字节）
    std::memcpy(&metadata.metadata_offset, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取元数据区大小（4字节）
    std::memcpy(&metadata.metadata_size, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取目录数据区偏移（4字节）
    std::memcpy(&metadata.directory_offset, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取目录数据区大小（4字节）
    std::memcpy(&metadata.directory_size, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取用户数据区偏移（4字节）
    std::memcpy(&metadata.user_data_offset, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取用户数据区大小（4字节）
    std::memcpy(&metadata.user_data_size, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    // 读取文件数量（4字节）
    std::memcpy(&metadata.file_count, ptr, sizeof(uint32_t));

    return ErrorCode::SUCCESS;
}

void Serializer::WriteString(const std::string& str, std::vector<uint8_t>& output) {
    uint32_t length = static_cast<uint32_t>(str.size());
    output.reserve(output.size() + sizeof(uint32_t) + length);

    // 写入字符串长度
    const uint8_t* length_ptr = reinterpret_cast<const uint8_t*>(&length);
    output.insert(output.end(), length_ptr, length_ptr + sizeof(uint32_t));

    // 写入字符串内容
    if (!str.empty()) {
        output.insert(output.end(), str.begin(), str.end());
    }
}

ErrorCode Serializer::ReadString(const std::vector<uint8_t>& input,
                                   uint32_t& offset,
                                   std::string& str) {
    if (offset + sizeof(uint32_t) > input.size()) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    // 读取字符串长度
    uint32_t length;
    std::memcpy(&length, input.data() + offset, sizeof(uint32_t));
    offset += sizeof(uint32_t);

    if (offset + length > input.size()) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    // 读取字符串内容
    str.assign(reinterpret_cast<const char*>(input.data() + offset), length);
    offset += length;

    return ErrorCode::SUCCESS;
}

ErrorCode Serializer::SerializeFileMetadata(const FileMetadata& metadata,
                                              std::vector<uint8_t>& output) {
    // 固定部分：inode_id(8) + file_size(8) + compressed_size(8) + volume_id(8) +
    //                     offset_in_volume(4) + file_mode(4) + last_modified(8) +
    //           is_directory(1) + is_compressed(1) = 50字节
    uint32_t fixed_size = 8 + 8 + 8 + 8 + 4 + 4 + 8 + 1 + 1;
    output.reserve(fixed_size);
    output.clear();

    // 写入inode_id（8字节）
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&metadata.inode_id);
    output.insert(output.end(), ptr, ptr + sizeof(uint64_t));

    // 写入file_size（8字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.file_size);
    output.insert(output.end(), ptr, ptr + sizeof(uint64_t));

    // 写入compressed_size（8字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.compressed_size);
    output.insert(output.end(), ptr, ptr + sizeof(uint64_t));

    // 写入volume_id（8字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.volume_id);
    // std::cout << "序列化的volume_id" << metadata.volume_id << std::endl;
    output.insert(output.end(), ptr, ptr + sizeof(uint64_t));

    // 写入offset_in_volume（4字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.offset_in_volume);
    output.insert(output.end(), ptr, ptr + sizeof(uint32_t));

    // 写入file_mode（4字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.file_mode);
    output.insert(output.end(), ptr, ptr + sizeof(uint32_t));

    // 写入last_modified（8字节）
    ptr = reinterpret_cast<const uint8_t*>(&metadata.last_modified);
    output.insert(output.end(), ptr, ptr + sizeof(time_t));

    // 写入is_directory（1字节）
    output.push_back(metadata.is_directory ? 1 : 0);

    // 写入is_compressed（1字节）
    output.push_back(metadata.is_compressed ? 1 : 0);

    // 写入file_name（字符串）
    WriteString(metadata.file_name, output);

    // 写入compressed_file_name（字符串）
    WriteString(metadata.compressed_file_name, output);

    return ErrorCode::SUCCESS;
}

ErrorCode Serializer::DeserializeFileMetadata(const std::vector<uint8_t>& input,
                                              FileMetadata& metadata) {
    // 固定部分：50字节
    uint32_t fixed_size = 8 + 8 + 8 + 8 + 4 + 4 + 8 + 1 + 1;
    if (input.size() < fixed_size) {
        return ErrorCode::SERIALIZATION_ERROR;
    }

    uint32_t offset = 0;
    const uint8_t* ptr = input.data();

    // 读取inode_id（8字节）
    std::memcpy(&metadata.inode_id, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    offset += sizeof(uint64_t);

    // 读取file_size（8字节）
    std::memcpy(&metadata.file_size, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    offset += sizeof(uint64_t);

    // 读取compressed_size（8字节）
    std::memcpy(&metadata.compressed_size, ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    offset += sizeof(uint64_t);

    // 读取volume_id（8字节）
    std::memcpy(&metadata.volume_id, ptr, sizeof(uint64_t));
    // std::cout << "还原的volume_id" << metadata.volume_id << std::endl;
    ptr += sizeof(uint64_t);
    offset += sizeof(uint64_t);

    // 读取offset_in_volume（4字节）
    std::memcpy(&metadata.offset_in_volume, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    offset += sizeof(uint32_t);

    // 读取file_mode（4字节）
    std::memcpy(&metadata.file_mode, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);
    offset += sizeof(uint32_t);

    // 读取last_modified（8字节）
    std::memcpy(&metadata.last_modified, ptr, sizeof(time_t));
    ptr += sizeof(time_t);
    offset += sizeof(time_t);

    // 读取is_directory（1字节）
    metadata.is_directory = (input[offset] != 0);
    offset += 1;

    // 读取is_compressed（1字节）
    metadata.is_compressed = (input[offset] != 0);
    offset += 1;

    // 读取file_name（字符串）
    ErrorCode ret = ReadString(input, offset, metadata.file_name);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 读取compressed_file_name（字符串）
    return ReadString(input, offset, metadata.compressed_file_name);
}


bool Serializer::ValidateVolumeMetadata(const VolumeMetadata& metadata) {
    // 验证魔数
    if (metadata.magic_number != MAGIC_NUMBER) {
        return false;
    }

    // 验证版本号
    if (metadata.version != CURRENT_VERSION) {
        return false;
    }

    // 验证卷镜像ID
    if (metadata.volume_id == INVALID_VOLUME_ID) {
        return false;
    }

    // 验证各区域偏移和大小的合法性
    // 元数据区偏移应该大于等于56（卷镜像头大小）
    if (metadata.metadata_offset < VOLUME_METADATA_SIZE) {
        return false;
    }

    // 目录区偏移应该大于元数据区结束位置
    uint32_t metadata_end = metadata.metadata_offset + metadata.metadata_size;
    if (metadata.directory_offset < metadata_end) {
        return false;
    }

    // 用户数据区偏移应该大于目录区结束位置
    uint32_t directory_end = metadata.directory_offset + metadata.directory_size;
    if (metadata.user_data_offset < directory_end) {
        return false;
    }

    // 用户数据区结束位置应该不超过卷镜像总大小
    uint64_t user_data_end = static_cast<uint64_t>(metadata.user_data_offset) + metadata.user_data_size;
    if (user_data_end > metadata.volume_size) {
        return false;
    }

    return true;
}

} // namespace volumemanager
