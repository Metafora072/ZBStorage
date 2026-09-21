#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "../config/MdsConfig.h"

namespace zb::mds {

struct NodeSelection {
    std::string node_id;
    std::string address;
    std::string disk_id;
    std::string group_id;
    uint64_t epoch{1};
    std::string secondary_node_id;
    std::string secondary_address;
    bool sync_ready{false};
    NodeType type{NodeType::kReal};
};

class NodeStateCache {
public:
    explicit NodeStateCache(std::vector<NodeInfo> nodes);

    std::vector<NodeInfo> Snapshot() const;
    void ReplaceNodes(std::vector<NodeInfo> nodes);
    bool ResolveNodeAddress(const std::string& node_id, std::string* address) const;
    bool ResolveDiskId(const std::string& node_id, uint32_t numeric_disk_id, std::string* disk_id) const;
    // Tests the current CMS-committed admission view, including virtual node
    // identifiers derived from a catalog member ("<node>-vN").
    bool IsWriteAdmitted(const std::string& node_id) const;
    std::vector<NodeSelection> PickNodes(uint32_t count);
    std::vector<NodeSelection> PickNodesByType(uint32_t count, NodeType type);

private:
    static bool IsNodeAllocatable(const NodeInfo& node, NodeType type_filter, bool strict_type_filter);
    NodeSelection NextSelectionLocked(NodeType type_filter, bool strict_type_filter);
    static std::string PickDiskLocked(NodeInfo* node, uint64_t virtual_index);
    static std::string BuildVirtualNodeId(const std::string& base_node_id, uint64_t virtual_index);
    size_t EstimateLogicalNodeCountLocked(NodeType type_filter, bool strict_type_filter) const;
    size_t EstimateWeightSumLocked(NodeType type_filter, bool strict_type_filter) const;

    mutable std::mutex mu_;
    std::vector<NodeInfo> nodes_;
    size_t next_node_index_{0};
    uint32_t repeat_remaining_{0};
};

} // namespace zb::mds
