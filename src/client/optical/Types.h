#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

namespace zb::client::optical {

class ClientError : public std::runtime_error {
public:
    ClientError(int code, const std::string& message) : std::runtime_error(message), code_(code) {}
    int code() const noexcept { return code_; }
private:
    int code_;
};

struct FileInfo {
    std::string node_address;
    uint64_t inode_id{0};
    std::string disk_id;
    std::string image_id;
    uint64_t file_size{0};
    std::optional<uint64_t> file_version;
    // 共享 Scheduler 数据入口的目标身份；留空时保持旧物理节点调用方式。
    std::string node_id;
    uint64_t instance_generation{0};
};

struct PrepareHandle {
    std::string value;
};

struct PreparedFile {
    PrepareHandle handle;
    std::string path;
    uint64_t size{0};
};

struct Completion {
    PreparedFile file;
    int error{0};
    std::string message;
};

}  // namespace zb::client::optical
