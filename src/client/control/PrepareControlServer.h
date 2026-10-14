#pragma once
#include "client/optical/OpticalReadClient.h"
#include <functional>
#include <memory>
#include <string>

namespace zb::client::control {
class PrepareControlServer {
public:
    // Receives a mount-relative path, without opening that path through FUSE.
    using Resolver = std::function<optical::FileInfo(const std::string&)>;
    PrepareControlServer(optical::OpticalReadClient& client, std::string mount,
                         std::string socket_path, Resolver resolver);
    ~PrepareControlServer();
    void Start();
    void Stop();
private:
    struct State;
    std::shared_ptr<State> state_;
};
} // namespace zb::client::control
