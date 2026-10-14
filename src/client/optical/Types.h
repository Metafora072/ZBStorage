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
