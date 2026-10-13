#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace brpc { class Channel; }

namespace zb::scheduler {

class ClusterState;
class NodePowerManager;

class CmsNodeCatalogPublisher {
public:
    CmsNodeCatalogPublisher(std::string mds_address, uint32_t timeout_ms);
    ~CmsNodeCatalogPublisher();

    bool Publish(ClusterState* cluster, NodePowerManager* nodes, std::string* error,
                 std::vector<std::string>* committed_retired_ids = nullptr);
    uint64_t last_cms_generation() const;

private:
    bool EnsureChannelLocked(std::string* error);
    bool ReconcileGenerationLocked(NodePowerManager* nodes, std::string* error);

    std::string mds_address_;
    uint32_t timeout_ms_{2000};
    mutable std::mutex mu_;
    std::unique_ptr<brpc::Channel> channel_;
    uint64_t last_cms_generation_{0};
    uint64_t generation_offset_{0};
    bool generation_reconciled_{false};
};

} // namespace zb::scheduler
