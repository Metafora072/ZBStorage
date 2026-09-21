#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace brpc { class Channel; }

namespace zb::scheduler {

class ClusterState;
class NodePowerManager;

class CmsNodeCatalogPublisher {
public:
    CmsNodeCatalogPublisher(std::string mds_address, uint32_t timeout_ms);
    ~CmsNodeCatalogPublisher();

    bool Publish(ClusterState* cluster, NodePowerManager* nodes, std::string* error);
    uint64_t last_cms_generation() const;

private:
    bool EnsureChannelLocked(std::string* error);

    std::string mds_address_;
    uint32_t timeout_ms_{2000};
    mutable std::mutex mu_;
    std::unique_ptr<brpc::Channel> channel_;
    uint64_t last_cms_generation_{0};
};

} // namespace zb::scheduler
