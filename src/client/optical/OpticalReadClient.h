#pragma once

#include "client/optical/Types.h"
#include "client/metrics/IoLatency.h"
#include <functional>
#include <memory>

namespace zb::client::optical {

struct Options {
    int rpc_timeout_ms{3000};
    uint32_t read_chunk_bytes{0}; // 0: use the server limit.
};

// Owned by one FUSE mount. All public operations throw ClientError on failure.
// Stop must follow the end of ordinary calls and precede destruction.
class OpticalReadClient {
public:
    using ReadyCallback = std::function<void(const Completion&)>;
    struct ReadSession;
    using Session = std::shared_ptr<ReadSession>;

    explicit OpticalReadClient(Options options = {});
    ~OpticalReadClient();
    OpticalReadClient(const OpticalReadClient&) = delete;
    OpticalReadClient& operator=(const OpticalReadClient&) = delete;

    // Returns after acceptance; matching accepted/ready preparation is reused.
    PrepareHandle Prepare(const FileInfo& file, const std::string& path);
    void Watch(const PrepareHandle& handle, ReadyCallback callback);
    Session Open(uint64_t inode_id, std::optional<uint64_t> version = std::nullopt);
    size_t Read(const Session& session, uint64_t offset, size_t size, char* buffer,
                metrics::IoTraceContext* trace = nullptr);
    void Close(const Session& session);
    // Legacy explicit-release API. Normal reads rely on the node's cache retention policy.
    void Release(const PrepareHandle& handle);
    void Stop();
    const std::string& instance_id() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace zb::client::optical
