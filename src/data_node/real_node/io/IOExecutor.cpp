#include "IOExecutor.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <unistd.h>

namespace zb::real_node {
namespace {
struct File { int fd; ~File() { if (fd >= 0) ::close(fd); } };
zb::msg::Status Error(const std::string& what) {
    const int code = errno;
    return code == ENOENT ? zb::msg::Status::NotFound(what + ": " + std::strerror(code))
                         : zb::msg::Status::IoError(what + ": " + std::strerror(code));
}
bool Range(uint64_t offset, uint64_t size) {
    const auto max = static_cast<uint64_t>(std::numeric_limits<off_t>::max());
    return offset <= max && size <= max - offset && size <= std::numeric_limits<size_t>::max();
}
}
zb::msg::Status IOExecutor::Write(const std::string& path, uint64_t offset,
                                 const std::string& data, uint64_t* written) {
    if (written) *written = 0;
    if (path.empty() || !Range(offset, data.size())) return zb::msg::Status::InvalidArgument("invalid write range");
    File file{::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0644)};
    if (file.fd < 0) return Error("open " + path);
    size_t pos = 0;
    while (pos < data.size()) {
        auto n = ::pwrite(file.fd, data.data() + pos,
                          std::min<size_t>(data.size() - pos, 1024 * 1024), offset + pos);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) return Error("pwrite " + path);
        if (n == 0) return zb::msg::Status::IoError("pwrite made no progress");
        pos += n; if (written) *written = pos;
    }
    int fd = file.fd; file.fd = -1;
    if (::close(fd)) return Error("close " + path);
    return zb::msg::Status::Ok();
}
zb::msg::Status IOExecutor::Read(const std::string& path, uint64_t offset, uint64_t size,
                                std::string* out, uint64_t* read) {
    if (read) *read = 0;
    if (!out || path.empty() || !Range(offset, size)) return zb::msg::Status::InvalidArgument("invalid read range");
    File file{::open(path.c_str(), O_RDONLY | O_CLOEXEC)};
    if (file.fd < 0) return Error("open " + path);
    out->assign(size, '\0');
    size_t pos = 0;
    while (pos < size) {
        auto n = ::pread(file.fd, out->data() + pos, std::min<uint64_t>(size - pos, 1024 * 1024), offset + pos);
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) { out->resize(pos); return Error("pread " + path); }
        if (n == 0) break;
        pos += n; if (read) *read = pos;
    }
    out->resize(pos);
    return zb::msg::Status::Ok();
}
zb::msg::Status IOExecutor::Delete(const std::string& path) {
    if (path.empty()) return zb::msg::Status::InvalidArgument("empty path");
    if (::unlink(path.c_str())) return Error("unlink " + path);
    return zb::msg::Status::Ok();
}
}
