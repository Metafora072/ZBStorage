#pragma once

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

namespace zb::handoff {
inline std::string Digest(const std::string& bytes) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
    static constexpr char hex[] = "0123456789abcdef";
    std::string result;
    for (auto c : digest) { result += hex[c >> 4]; result += hex[c & 15]; }
    return result;
}
inline bool SyncDir(const std::filesystem::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (fd < 0) return false;
    const bool ok = ::fsync(fd) == 0;
    ::close(fd);
    return ok;
}
inline bool WriteAll(int fd, const char* data, size_t size, uint64_t offset) {
    while (size) {
        const auto n = ::pwrite(fd, data, size, static_cast<off_t>(offset));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return false;
        data += n; size -= n; offset += n;
    }
    return true;
}
inline bool AtomicWrite(const std::filesystem::path& path, const std::string& bytes) {
    const auto tmp = path.string() + ".tmp";
    const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_NOFOLLOW, 0600);
    if (fd < 0) return false;
    bool ok = WriteAll(fd, bytes.data(), bytes.size(), 0) && ::fsync(fd) == 0;
    if (::close(fd) != 0) ok = false;
    if (ok) ok = ::rename(tmp.c_str(), path.c_str()) == 0 && SyncDir(path.parent_path());
    return ok;
}
inline bool Read(const std::filesystem::path& path, std::string* bytes) {
    std::error_code ec;
    if (!std::filesystem::is_regular_file(std::filesystem::symlink_status(path, ec)) ||
        ec || std::filesystem::file_size(path, ec) > 64 * 1024 * 1024 || ec) return false;
    std::ifstream in(path, std::ios::binary);
    bytes->assign(std::istreambuf_iterator<char>(in), {});
    return !in.bad();
}
// 引擎产物目前均为单层目录中的普通文件。禁止绝对路径、子目录、符号链接。
inline bool SafePath(const std::string& value) {
    const auto slash = value.find('/');
    if (slash == std::string::npos || value.find('/', slash + 1) != std::string::npos) return false;
    const auto dir = value.substr(0, slash), name = value.substr(slash + 1);
    if (name.empty() || name == "." || name == ".." || name.find('\0') != std::string::npos) return false;
    return dir == "input" || dir == "temp" || dir == "image" || dir == "read" ||
           dir == "disc_sim" || dir == "meta" ||
           (dir == "log" && name == "runtime_state_snapshot");
}
template <class Record> std::string ManifestId(Record record) {
    record.clear_manifest_id(); record.clear_phase();
    return Digest(record.SerializeAsString());
}
} // namespace zb::handoff
