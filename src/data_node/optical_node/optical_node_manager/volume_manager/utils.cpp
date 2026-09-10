#include "volume_manager.h"
#include "error_codes.h"
#include "serializer.h"
#include <sys/stat.h>
#include <unistd.h>
#include <fstream>
#include <random>
#include <chrono>
#include <cstring>

namespace volumemanager {

// 工具函数：生成唯一ID（使用时间戳+随机数）
uint64_t GenerateUniqueId() {
    // 获取当前时间戳（毫秒）
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    uint64_t timestamp = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count()
    );

    // 生成随机数
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, UINT64_MAX);
    uint64_t random_num = dist(gen);

    // 组合时间戳和随机数（高32位用时间戳，低32位用随机数）
    return (timestamp << 32) | (random_num & 0xFFFFFFFFULL);
}

// 工具函数：检查文件是否存在
bool FileExists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

// 工具函数：获取文件大小
uint64_t GetFileSize(const std::string& path) {
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(buffer.st_size);
}

// 工具函数：检查是否为目录
bool IsDirectory(const std::string& path) {
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) {
        return false;
    }
    return S_ISDIR(buffer.st_mode);
}

// 工具函数：从完整路径提取文件名
std::string GetFileName(const std::string& path) {
    size_t pos = path.find_last_of("/\\");
    if (pos == std::string::npos) {
        return path;
    }
    return path.substr(pos + 1);
}

// 工具函数：获取文件权限和类型信息
uint32_t GetFileMode(const std::string& path) {
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) {
        return 0;
    }
    return static_cast<uint32_t>(buffer.st_mode);
}

// 工具函数：获取文件最后修改时间
time_t GetFileLastModified(const std::string& path) {
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) {
        return 0;
    }
    return buffer.st_mtime;
}

// 工具函数：创建目录
ErrorCode CreateDirectory(const std::string& path) {
    if (mkdir(path.c_str(), 0755) != 0) {
        // 如果目录已存在，返回成功
        if (errno == EEXIST) {
            return ErrorCode::SUCCESS;
        }
        return ErrorCode::IO_ERROR;
    }
    return ErrorCode::SUCCESS;
}

// 工具函数：递归创建目录
ErrorCode CreateDirectories(const std::string& path) {
    if (path.empty()) {
        return ErrorCode::INVALID_PATH;
    }

    // 如果目录已存在，返回成功
    if (FileExists(path) && IsDirectory(path)) {
        return ErrorCode::SUCCESS;
    }

    // 尝试创建父目录
    size_t pos = path.find_last_of("/\\");
    if (pos != std::string::npos && pos > 0) {
        std::string parent = path.substr(0, pos);
        ErrorCode ret = CreateDirectories(parent);
        if (ret != ErrorCode::SUCCESS) {
            return ret;
        }
    }

    // 创建当前目录
    return CreateDirectory(path);
}

// 工具函数：读取文件内容
ErrorCode ReadFileContent(const std::string& path, std::vector<uint8_t>& content) {
    if (!FileExists(path)) {
        return ErrorCode::FILE_NOT_FOUND;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    // 获取文件大小
    file.seekg(0, std::ios::end);
    uint64_t size = static_cast<uint64_t>(file.tellg());
    file.seekg(0, std::ios::beg);

    if (size == 0) {
        content.clear();
        return ErrorCode::SUCCESS;
    }

    // 读取文件内容
    content.resize(static_cast<size_t>(size));
    file.read(reinterpret_cast<char*>(content.data()), size);

    if (!file.good()) {
        return ErrorCode::IO_ERROR;
    }

    return ErrorCode::SUCCESS;
}

// 工具函数：写入文件内容
ErrorCode WriteFileContent(const std::string& path, const std::vector<uint8_t>& content) {
    // 确保父目录存在
    size_t pos = path.find_last_of("/\\");
    if (pos != std::string::npos) {
        std::string parent = path.substr(0, pos);
        ErrorCode ret = CreateDirectories(parent);
        if (ret != ErrorCode::SUCCESS) {
            return ret;
        }
    }

    std::ofstream file(path, std::ios::binary);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    if (!content.empty()) {
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
    }

    if (!file.good()) {
        return ErrorCode::IO_ERROR;
    }

    return ErrorCode::SUCCESS;
}

// 工具函数：追加写入文件内容
ErrorCode AppendFileContent(const std::string& path, const std::vector<uint8_t>& content) {
    std::ofstream file(path, std::ios::binary | std::ios::app);
    if (!file.is_open()) {
        return ErrorCode::IO_ERROR;
    }

    if (!content.empty()) {
        file.write(reinterpret_cast<const char*>(content.data()), content.size());
    }

    if (!file.good()) {
        return ErrorCode::IO_ERROR;
    }

    return ErrorCode::SUCCESS;
}

// 工具函数：删除文件
ErrorCode DeleteFile(const std::string& path) {
    if (unlink(path.c_str()) != 0) {
        return ErrorCode::IO_ERROR;
    }
    return ErrorCode::SUCCESS;
}

// 工具函数：删除目录（递归）
ErrorCode DeleteDirectory(const std::string& path) {
    return ErrorCode::IO_ERROR; // 暂时不实现递归删除
}

// 工具函数：打开文件进行随机访问
std::fstream OpenFileForReadWrite(const std::string& path) {
    return std::fstream(path, std::ios::binary | std::ios::in | std::ios::out);
}

// 工具函数：打开文件进行读取
std::ifstream OpenFileForRead(const std::string& path) {
    return std::ifstream(path, std::ios::binary);
}

// 工具函数：打开文件进行写入
std::ofstream OpenFileForWrite(const std::string& path) {
    return std::ofstream(path, std::ios::binary);
}

} // namespace volumemanager
