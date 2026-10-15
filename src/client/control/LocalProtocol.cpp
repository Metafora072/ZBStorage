#include "client/control/LocalProtocol.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <vector>

namespace zb::client::control {
namespace {
constexpr uint32_t kMaxFrame = 1024 * 1024;
void Transfer(int fd, void* data, size_t size, bool writing) {
    auto* p = static_cast<char*>(data);
    while (size) {
        ssize_t n = writing ? send(fd, p, size, MSG_NOSIGNAL) : recv(fd, p, size, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) throw optical::ClientError(n == 0 ? ECONNRESET : errno, "local control connection closed or failed");
        p += n;
        size -= n;
    }
}
}
Socket::~Socket() { if (fd_ >= 0) close(fd_); }
void Socket::Shutdown() const { if (fd_ >= 0) shutdown(fd_, SHUT_RDWR); }
int Connect(const std::string& path) {
    sockaddr_un address{};
    address.sun_family = AF_UNIX;
    if (path.empty() || path.size() >= sizeof(address.sun_path))
        throw optical::ClientError(EINVAL, "invalid control socket path");
    std::memcpy(address.sun_path, path.c_str(), path.size() + 1);
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) throw optical::ClientError(errno, "cannot create control socket");
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        int error = errno;
        close(fd);
        throw optical::ClientError(error, "cannot connect to control socket: " + path);
    }
    return fd;
}
void SendFrame(int fd, const google::protobuf::MessageLite& message) {
    std::string bytes;
    if (!message.SerializeToString(&bytes) || bytes.size() > kMaxFrame)
        throw optical::ClientError(EMSGSIZE, "invalid local control frame");
    uint32_t length = htonl(static_cast<uint32_t>(bytes.size()));
    Transfer(fd, &length, sizeof(length), true);
    Transfer(fd, bytes.data(), bytes.size(), true);
}
void ReadFrame(int fd, google::protobuf::MessageLite* message) {
    uint32_t length;
    Transfer(fd, &length, sizeof(length), false);
    length = ntohl(length);
    if (length > kMaxFrame) throw optical::ClientError(EMSGSIZE, "local control frame too large");
    std::vector<char> data(length);
    Transfer(fd, data.data(), data.size(), false);
    if (!message->ParseFromArray(data.data(), data.size()))
        throw optical::ClientError(EINVAL, "malformed local control message");
}
} // namespace zb::client::control
