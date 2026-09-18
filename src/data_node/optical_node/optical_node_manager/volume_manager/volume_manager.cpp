#include "volume_manager.h"
#include "serializer.h"
#include "compression_utils.h"
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>
#include <random>
#include <chrono>
#include <cstring>

namespace volumemanager {

// 工具函数声明
uint64_t GenerateUniqueId();
bool FileExists(const std::string& path);
uint64_t GetFileSize(const std::string& path);
bool IsDirectory(const std::string& path);
std::string GetFileName(const std::string& path);
uint32_t GetFileMode(const std::string& path);
time_t GetFileLastModified(const std::string& path);
ErrorCode CreateDirectories(const std::string& path);
ErrorCode ReadFileContent(const std::string& path, std::vector<uint8_t>& content);
ErrorCode WriteFileContent(const std::string& path, const std::vector<uint8_t>& content);
ErrorCode DeleteFile(const std::string& path);

VolumeManager::VolumeManager(uint64_t volume_size, double size_threshold)
    : base_path_("./")
    , temp_dir_("./")
    , image_dir_("./")
    , read_cache_dir_("./")
    , pending_size_(0)
    , volume_size_(volume_size)
    , size_threshold_(size_threshold) {

    // 限制阈值在合理范围内
    if (size_threshold_ <= 0.0 || size_threshold_ > 1.0) {
        size_threshold_ = 0.9;
    }

    // 初始化三个目录（temp_dir_ / image_dir_ / read_cache_dir_）
    InitializeTempDir();
}

VolumeManager::~VolumeManager() {
    // 卸载所有卷镜像
    for (auto& pair : mounted_volumes_) {
        // 清理相关的中间文件
    }
    mounted_volumes_.clear();
    file_metadata_cache_.clear();
}

ErrorCode VolumeManager::InitializeTempDir() {
    ErrorCode ret = CreateDirectories(temp_dir_);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }
    ret = CreateDirectories(image_dir_);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }
    return CreateDirectories(read_cache_dir_);
}

ErrorCode VolumeManager::SetBasePath(const std::string& base_path) {
    if (base_path.empty()) {
        return ErrorCode::INVALID_PATH;
    }
    base_path_ = base_path;

    // 确保基础路径以 / 结尾
    if (base_path_.back() != '/') {
        base_path_ += '/';
    }

    return ErrorCode::SUCCESS;
}

ErrorCode VolumeManager::SetTempDir(const std::string& temp_dir) {
    if (temp_dir.empty()) {
        return ErrorCode::INVALID_PATH;
    }
    temp_dir_ = temp_dir;

    // 确保路径以 / 结尾
    if (temp_dir_.back() != '/') {
        temp_dir_ += '/';
    }

    return CreateDirectories(temp_dir_);
}

ErrorCode VolumeManager::SetImageDir(const std::string& image_dir) {
    if (image_dir.empty()) {
        return ErrorCode::INVALID_PATH;
    }
    image_dir_ = image_dir;

    // 确保路径以 / 结尾
    if (image_dir_.back() != '/') {
        image_dir_ += '/';
    }

    return CreateDirectories(image_dir_);
}

ErrorCode VolumeManager::SetReadCacheDir(const std::string& read_cache_dir) {
    if (read_cache_dir.empty()) {
        return ErrorCode::INVALID_PATH;
    }
    read_cache_dir_ = read_cache_dir;

    // 确保路径以 / 结尾
    if (read_cache_dir_.back() != '/') {
        read_cache_dir_ += '/';
    }

    return CreateDirectories(read_cache_dir_);
}

uint64_t VolumeManager::GenerateVolumeId() {
    uint64_t id;
    do {
        id = GenerateUniqueId();
    } while (mounted_volumes_.find(id) != mounted_volumes_.end());
    return id;
}

uint64_t VolumeManager::GenerateInodeId() {
    uint64_t id;
    do {
        id = GenerateUniqueId();
    } while (file_metadata_cache_.find(id) != file_metadata_cache_.end());
    return id;
}

bool VolumeManager::ShouldPackVolume() const {
    if (pending_files_.empty()) {
        return false;
    }

    double threshold = static_cast<double>(pending_size_) / static_cast<double>(volume_size_);
    return threshold >= size_threshold_;
}

// ErrorCode VolumeManager::AddFileToCollect(const std::string& file_path) {
//     uint64_t tmp;
//     return AddFileToCollect(file_path, tmp);
// }

ErrorCode VolumeManager::AddFileToCollect(const std::string& file_path, uint64_t inode_id, uint64_t volume_id) {
    // 检查路径是否有效
    if (file_path.empty()) {
        return ErrorCode::INVALID_PATH;
    }

    // 构建完整路径
    std::string full_path = base_path_ + file_path;

    // 检查文件是否存在
    if (!FileExists(full_path)) {
        return ErrorCode::FILE_NOT_FOUND;
    }

    // 检查是否为目录
    if (IsDirectory(full_path)) {
        return ErrorCode::INVALID_PATH;
    }

    // 获取文件信息
    FileMetadata metadata;
    metadata.inode_id = inode_id;
    metadata.file_size = GetFileSize(full_path);
    metadata.volume_id = volume_id;
    metadata.offset_in_volume = 0;
    metadata.file_name = GetFileName(file_path);
    metadata.compressed_file_name = CompressionUtils::GetCompressedFileName(metadata.file_name);
    metadata.file_mode = GetFileMode(full_path);
    metadata.last_modified = GetFileLastModified(full_path);
    metadata.is_directory = false;
    metadata.is_compressed = true;

    // 压缩文件
    std::vector<uint8_t> original_content;
    ErrorCode ret = ReadFileContent(full_path, original_content);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    std::vector<uint8_t> compressed_content;
    ret = CompressionUtils::CompressData(original_content, compressed_content);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    metadata.compressed_size = compressed_content.size();

    // 保存压缩后的数据到临时文件
    std::string temp_compressed_path = temp_dir_ + "temp_" + std::to_string(metadata.inode_id) + ".compressed";
    ret = WriteFileContent(temp_compressed_path, compressed_content);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 检查单个文件是否超过卷镜像大小限制（使用压缩后大小）
    if (metadata.compressed_size > volume_size_) {
        return ErrorCode::VOLUME_FULL;
    }

    // 检查添加后是否会超过卷镜像大小限制（使用压缩后大小）
    if (pending_size_ + metadata.compressed_size > volume_size_) {
        // 先封装当前待封装文件集
        std::string volume_path;
        ErrorCode ret = PackVolume(volume_path, volume_id);
        if (ret != ErrorCode::SUCCESS) {
            return ret;
        }
    }

    // 添加到待封装文件集
    pending_files_.push_back(metadata);
    pending_size_ += metadata.compressed_size;

    // 缓存文件元数据（用于后续读取）
    file_metadata_cache_[metadata.inode_id] = metadata;

    // 检查是否需要封装卷镜像
    if (ShouldPackVolume()) {
        std::string volume_path;
        return PackVolume(volume_path, volume_id);
    }

    return ErrorCode::SUCCESS;
}

ErrorCode VolumeManager::PackVolume(std::string& output_path, uint64_t volume_id) {
    if (pending_files_.empty()) {
        return ErrorCode::INVALID_PARAMETER;
    }

    // 准备卷镜像元数据
    VolumeMetadata volume_meta;
    volume_meta.volume_id = (volume_id != 0) ? volume_id : GenerateVolumeId();
    volume_meta.volume_size = volume_size_;
    volume_meta.file_count = static_cast<uint32_t>(pending_files_.size());

    // 计算各数据区大小
    uint32_t metadata_offset = 56; // 卷镜像头固定大小56字节
    uint32_t metadata_size = 0;

    // 计算元数据区大小
    for (auto& file_meta : pending_files_) {
        std::vector<uint8_t> serialized;
        file_meta.volume_id = volume_meta.volume_id;
        Serializer::SerializeFileMetadata(file_meta, serialized);
        metadata_size += static_cast<uint32_t>(serialized.size());
    }

    uint32_t directory_offset = metadata_offset + metadata_size;
    uint32_t directory_size = 0; // 暂不处理目录数据
    uint32_t user_data_offset = directory_offset + directory_size;
    uint32_t user_data_size = 0;

    // 计算用户数据区大小并设置偏移（使用压缩后大小）
    for (auto& file_meta : pending_files_) {
        file_meta.offset_in_volume = user_data_offset + user_data_size;
        user_data_size += static_cast<uint32_t>(file_meta.compressed_size);
    }

    // 设置卷镜像元数据
    volume_meta.metadata_offset = metadata_offset;
    volume_meta.metadata_size = metadata_size;
    volume_meta.directory_offset = directory_offset;
    volume_meta.directory_size = directory_size;
    volume_meta.user_data_offset = user_data_offset;
    volume_meta.user_data_size = user_data_size;

    // 生成卷镜像文件路径
    output_path = temp_dir_ + "volume_" + std::to_string(volume_meta.volume_id) + ".vimg";

    // 打开文件进行写入
    std::ofstream file(output_path, std::ios::binary);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    // 序列化并写入卷镜像头
    std::vector<uint8_t> volume_data;
    ErrorCode ret = Serializer::SerializeVolumeMetadata(volume_meta, volume_data);
    if (ret != ErrorCode::SUCCESS) {
        file.close();
        return ret;
    }
    file.write(reinterpret_cast<const char*>(volume_data.data()), volume_data.size());

    // 写入文件元数据区
    for (const auto& file_meta : pending_files_) {
        std::vector<uint8_t> file_data;
        Serializer::SerializeFileMetadata(file_meta, file_data);
        file.write(reinterpret_cast<const char*>(file_data.data()), file_data.size());

        // 更新缓存中的文件元数据（volume_id和offset）
        if (file_metadata_cache_.find(file_meta.inode_id) != file_metadata_cache_.end()) {
            file_metadata_cache_[file_meta.inode_id].volume_id = volume_meta.volume_id;
            file_metadata_cache_[file_meta.inode_id].offset_in_volume = file_meta.offset_in_volume;
        }
    }

    // 写入目录数据区（空）
    // 写入用户数据区（使用压缩后的数据）
    for (const auto& file_meta : pending_files_) {
        // 读取临时压缩文件内容
        std::string temp_compressed_path = temp_dir_ + "temp_" + std::to_string(file_meta.inode_id) + ".compressed";
        std::vector<uint8_t> content;
        ret = ReadFileContent(temp_compressed_path, content);
        if (ret != ErrorCode::SUCCESS) {
            file.close();
            return ret;
        }

        // 写入到卷镜像
        file.write(reinterpret_cast<const char*>(content.data()), content.size());

        // 删除临时压缩文件
        DeleteFile(temp_compressed_path);
    }

    file.close();

    // 挂载新创建的卷镜像
    mounted_volumes_[volume_meta.volume_id] = volume_meta;

    // 清空待封装文件集
    pending_files_.clear();
    pending_size_ = 0;

    return ErrorCode::SUCCESS;
}

ErrorCode VolumeManager::MountVolume(const std::string& volume_id) {
    std::string volume_path = image_dir_ + "volume_" + volume_id + ".vimg";

    // 检查文件是否存在
    if (!FileExists(volume_path)) {
        return ErrorCode::FILE_NOT_FOUND;
    }

    // 打开文件
    std::ifstream file(volume_path, std::ios::binary);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    // 读取卷镜像头
    std::vector<uint8_t> header_data(56);
    file.read(reinterpret_cast<char*>(header_data.data()), 56);

    if (!file.good()) {
        file.close();
        return ErrorCode::INVALID_VOLUME_FORMAT;
    }

    // 反序列化卷镜像元数据
    VolumeMetadata volume_meta;
    ErrorCode ret = Serializer::DeserializeVolumeMetadata(header_data, volume_meta);
    if (ret != ErrorCode::SUCCESS) {
        file.close();
        return ErrorCode::INVALID_VOLUME_FORMAT;
    }

    // 验证卷镜像格式
    if (!Serializer::ValidateVolumeMetadata(volume_meta)) {
        file.close();
        return ErrorCode::INVALID_VOLUME_FORMAT;
    }

    // 检查卷镜像是否已挂载
    if (mounted_volumes_.find(volume_meta.volume_id) != mounted_volumes_.end()) {
        file.close();
        return ErrorCode::INVALID_VOLUME_ID;
    }

    // 读取文件元数据区
    file.seekg(volume_meta.metadata_offset);
    for (uint32_t i = 0; i < volume_meta.file_count; ++i) {
        // 读取文件元数据：固定部分50字节 + 两个字符串
        // 先读取固定部分：50字节
        std::vector<uint8_t> temp_data(50);
        file.read(reinterpret_cast<char*>(temp_data.data()), 50);

        if (!file.good()) {
            file.close();
            return ErrorCode::INVALID_VOLUME_FORMAT;
        }

        // 读取第一个字符串（file_name）长度（4字节）
        uint32_t str_len1;
        file.read(reinterpret_cast<char*>(&str_len1), sizeof(uint32_t));

        if (!file.good()) {
            file.close();
            return ErrorCode::INVALID_VOLUME_FORMAT;
        }

        // 读取第一个字符串内容
        std::string file_name(str_len1, '\0');
        if (str_len1 > 0) {
            file.read(&file_name[0], str_len1);
        }

        if (!file.good()) {
            file.close();
            return ErrorCode::INVALID_VOLUME_FORMAT;
        }

        // 读取第二个字符串（compressed_file_name）长度（4字节）
        uint32_t str_len2;
        file.read(reinterpret_cast<char*>(&str_len2), sizeof(uint32_t));

        if (!file.good()) {
            file.close();
            return ErrorCode::INVALID_VOLUME_FORMAT;
        }

        // 读取第二个字符串内容
        std::string compressed_file_name(str_len2, '\0');
        if (str_len2 > 0) {
            file.read(&compressed_file_name[0], str_len2);
        }

        if (!file.good()) {
            file.close();
            return ErrorCode::INVALID_VOLUME_FORMAT;
        }

        // 组合完整数据
        temp_data.insert(temp_data.end(), reinterpret_cast<const uint8_t*>(&str_len1),
                         reinterpret_cast<const uint8_t*>(&str_len1) + sizeof(uint32_t));
        temp_data.insert(temp_data.end(), file_name.begin(), file_name.end());
        temp_data.insert(temp_data.end(), reinterpret_cast<const uint8_t*>(&str_len2),
                         reinterpret_cast<const uint8_t*>(&str_len2) + sizeof(uint32_t));
        temp_data.insert(temp_data.end(), compressed_file_name.begin(), compressed_file_name.end());

        // 反序列化文件元数据
        FileMetadata file_meta;
        ret = Serializer::DeserializeFileMetadata(temp_data, file_meta);
        if (ret != ErrorCode::SUCCESS) {
            file.close();
            return ret;
        }

        // 缓存文件元数据
        file_metadata_cache_[file_meta.inode_id] = file_meta;
    }

    file.close();

    // 添加到已挂载卷镜像列表
    mounted_volumes_[volume_meta.volume_id] = volume_meta;

    return ErrorCode::SUCCESS;
}

ErrorCode VolumeManager::UnmountVolume(uint64_t volume_id) {
    // 查找卷镜像
    auto it = mounted_volumes_.find(volume_id);
    if (it == mounted_volumes_.end()) {
        return ErrorCode::VOLUME_NOT_FOUND;
    }

    const VolumeMetadata& volume_meta = it->second;

    // 从缓存中移除属于该卷镜像的文件元数据
    for (auto cache_it = file_metadata_cache_.begin();
         cache_it != file_metadata_cache_.end(); ) {
        if (cache_it->second.volume_id == volume_id) {
            cache_it = file_metadata_cache_.erase(cache_it);
        } else {
            ++cache_it;
        }
    }

    // 从已挂载列表中移除
    mounted_volumes_.erase(it);

    return ErrorCode::SUCCESS;
}

ErrorCode VolumeManager::ReadFile(uint64_t inode_id, std::string& output_path) {
    // 查找文件元数据
    auto it = file_metadata_cache_.find(inode_id);
    if (it == file_metadata_cache_.end()) {
        return ErrorCode::INODE_NOT_FOUND;
    }

    const FileMetadata& file_meta = it->second;

    // 检查文件是否属于已挂载的卷镜像
    auto volume_it = mounted_volumes_.find(file_meta.volume_id);
    if (volume_it == mounted_volumes_.end()) {
        // for (auto [k,v] : mounted_volumes_) {
        //     std::cout << "have" << k << std::endl;
        // }
        // std::cout << "no " << file_meta.volume_id << std::endl;
        return ErrorCode::VOLUME_NOT_FOUND;
    }

    const VolumeMetadata& volume_meta = volume_it->second;

    // 生成输出文件路径
    output_path = read_cache_dir_ + "file_" + std::to_string(inode_id) + "_" + file_meta.file_name;

    // 打开卷镜像文件
    std::string volume_path = image_dir_ + "volume_" + std::to_string(volume_meta.volume_id) + ".vimg";
    std::ifstream file(volume_path, std::ios::binary);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    // 定位到文件数据位置
    file.seekg(file_meta.offset_in_volume);

    // 读取压缩文件内容
    std::vector<uint8_t> compressed_content(file_meta.compressed_size);
    file.read(reinterpret_cast<char*>(compressed_content.data()), file_meta.compressed_size);

    if (!file.good()) {
        file.close();
        return ErrorCode::IO_ERROR;
    }

    file.close();

    // 解压数据
    std::vector<uint8_t> decompressed_content;
    ErrorCode ret = CompressionUtils::DecompressData(compressed_content, file_meta.file_size,
                                                      decompressed_content);
    if (ret != ErrorCode::SUCCESS) {
        return ret;
    }

    // 写入到输出文件
    return WriteFileContent(output_path, decompressed_content);
}

ErrorCode VolumeManager::ReadAllInodesAndDirectories(uint64_t volume_id,
                                                        std::string& output_path) {
    // 查找卷镜像
    auto volume_it = mounted_volumes_.find(volume_id);
    if (volume_it == mounted_volumes_.end()) {
        return ErrorCode::VOLUME_NOT_FOUND;
    }

    const VolumeMetadata& volume_meta = volume_it->second;

    // 生成输出文件路径
    output_path = read_cache_dir_ + "inodes_" + std::to_string(volume_id) + ".txt";

    // 打开输出文件
    std::ofstream file(output_path);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    // 写入卷镜像信息
    file << "卷镜像ID: " << volume_meta.volume_id << "\n";
    file << "文件数量: " << volume_meta.file_count << "\n";
    file << "卷镜像大小: " << volume_meta.volume_size << " 字节\n\n";

    // 写入所有文件元数据
    file << "文件列表:\n";
    file << "========================================\n";

    for (const auto& pair : file_metadata_cache_) {
        const FileMetadata& file_meta = pair.second;

        // 只输出属于该卷镜像的文件
        if (file_meta.volume_id == volume_id) {
            file << "Inode ID: " << file_meta.inode_id << "\n";
            file << "原始文件名: " << file_meta.file_name << "\n";
            file << "原始文件大小: " << file_meta.file_size << " 字节\n";
            if (file_meta.is_compressed) {
                file << "压缩文件名: " << file_meta.compressed_file_name << "\n";
                file << "压缩文件大小: " << file_meta.compressed_size << " 字节\n";
                double ratio = 100.0 * file_meta.compressed_size / file_meta.file_size;
                file << "压缩比: " << ratio << "%\n";
            }
            file << "文件权限: " << file_meta.file_mode << "\n";
            file << "最后修改时间: " << file_meta.last_modified << "\n";
            file << "是否为目录: " << (file_meta.is_directory ? "是" : "否") << "\n";
            file << "卷镜像内偏移: " << file_meta.offset_in_volume << "\n";
            file << "----------------------------------------\n";
        }
    }

    file.close();

    return ErrorCode::SUCCESS;
}

} // namespace volumemanager
