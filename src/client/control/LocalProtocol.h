#pragma once
#include "client/optical/Types.h"
#include <google/protobuf/message_lite.h>
#include <string>

namespace zb::client::control {
// Move-only descriptor. Shutdown cancels blocking I/O without early fd reuse.
class Socket {
public:
    explicit Socket(int fd = -1) : fd_(fd) {}
    ~Socket();
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;
    int fd() const { return fd_; }
    void Shutdown() const;
private:
    int fd_;
};
int Connect(const std::string& path);
void SendFrame(int fd, const google::protobuf::MessageLite& message);
void ReadFrame(int fd, google::protobuf::MessageLite* message);
} // namespace zb::client::control
