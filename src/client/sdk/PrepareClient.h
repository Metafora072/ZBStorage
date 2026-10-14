#pragma once
#include "client/optical/Types.h"
#include <functional>
#include <memory>

namespace zb::client {
using optical::ClientError;
using optical::PrepareHandle;
using optical::PreparedFile;

// Connects to the control socket owned by a FUSE mount. Keep this object alive
// until its callbacks finish; destroy it outside its own callbacks.
class PrepareClient {
public:
    explicit PrepareClient(std::string control_socket);
    ~PrepareClient();
    PrepareClient(const PrepareClient&) = delete;
    PrepareClient& operator=(const PrepareClient&) = delete;
    PrepareHandle prepare_read(const std::string& path,
                               std::function<void(const PreparedFile&)> on_ready,
                               std::function<void(const ClientError&)> on_error = {});
    // Legacy compatibility only; normal applications do not manage the node's prepared cache.
    void release_prepared(const PrepareHandle& handle);
    // Building blocks for separate submit/wait CLI processes. Waiting is a
    // single local subscription, never a remote optical status query.
    PrepareHandle submit(const std::string& path);
    PreparedFile wait(const PrepareHandle& handle);
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace zb::client
