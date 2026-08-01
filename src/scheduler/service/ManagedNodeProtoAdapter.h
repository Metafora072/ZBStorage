#pragma once

#include <string>

#include "../model/ManagedNode.h"
#include "../power/NodePowerManager.h"
#include "scheduler.pb.h"

namespace zb::scheduler {

struct NodeState;

ManagedNodeProfile DefaultProfileForLegacyNode(const std::string& node_id,
                                               zb::rpc::NodeType node_type,
                                               uint32_t logical_node_count);
bool ManagedNodeProfileFromProto(const zb::rpc::ManagedNodeSpec& input,
                                 ManagedNodeProfile* output,
                                 std::string* error);
AccessPath AccessPathFromProto(zb::rpc::ManagedAccessPath path);
bool ManagedNodeFilterFromProto(const zb::rpc::ListManagedNodesRequest& input,
                                ManagedNodeFilter* output,
                                std::string* error);
bool OpticalLibraryStatusFromProto(const zb::rpc::ManagedOpticalLibraryStatus& input,
                                   OpticalLibraryStatus* output,
                                   std::string* error);
void FillManagedNodeView(const ManagedNodeRuntime& node, zb::rpc::ManagedNodeView* output);
void FillLegacyNodeView(const NodeState& node, zb::rpc::NodeView* output);
void FillManagedSummary(const PowerAndEnergySummary& summary,
                        zb::rpc::ManagedPowerAndEnergySummary* output);

} // namespace zb::scheduler
