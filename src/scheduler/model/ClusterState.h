#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "../health/FailureDetector.h"
#include "../power/NodePowerManager.h"
#include "scheduler.pb.h"

namespace zb::scheduler {

using DiskState = zb::storage_model::DeviceInventory;

struct NodeState {
    std::string node_id;
    zb::rpc::NodeType node_type{zb::rpc::NODE_REAL};
    std::string address;
    uint32_t weight{1};
    uint32_t virtual_node_count{1};
    std::string group_id;
    zb::rpc::NodeRole role{zb::rpc::NODE_ROLE_UNKNOWN};
    uint64_t epoch{1};
    uint64_t applied_lsn{0};
    std::string peer_node_id;
    std::string peer_address;
    bool sync_ready{false};

    zb::rpc::NodeHealthState health_state{zb::rpc::NODE_HEALTH_HEALTHY};
    zb::rpc::NodeAdminState admin_state{zb::rpc::NODE_ADMIN_ENABLED};
    zb::rpc::NodePowerState power_state{zb::rpc::NODE_POWER_UNKNOWN};
    zb::rpc::NodeAdminState desired_admin_state{zb::rpc::NODE_ADMIN_ENABLED};
    zb::rpc::NodePowerState desired_power_state{zb::rpc::NODE_POWER_ON};

    uint64_t last_heartbeat_ms{0};
    uint64_t resident_data_bytes{0};
    bool optical_inventory_observed{false};
    uint32_t optical_local_disc_count{0};
    std::unordered_map<std::string, DiskState> disks;
};

struct NodeOperationState {
    std::string operation_id;
    std::string node_id;
    zb::rpc::NodeOperationType operation_type{zb::rpc::NODE_OP_START};
    zb::rpc::NodeOperationStatus status{zb::rpc::NODE_OP_PENDING};
    std::string message;
    uint64_t start_ts_ms{0};
    uint64_t finish_ts_ms{0};
};

struct HeartbeatAssignment {
    bool success{true};
    std::string error;
    uint64_t generation{0};
    std::string group_id;
    zb::rpc::NodeRole assigned_role{zb::rpc::NODE_ROLE_UNKNOWN};
    uint64_t epoch{1};
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
};

class ClusterState {
public:
    ClusterState(FailureDetector detector, NodePowerManager* nodes);

    HeartbeatAssignment ReportHeartbeat(const zb::rpc::HeartbeatRequest& request);
    uint64_t TickHealth();

    uint64_t SetNodeAdminState(const std::string& node_id,
                               zb::rpc::NodeAdminState state,
                               std::string* error);
    uint64_t SetDesiredPowerState(const std::string& node_id,
                                  zb::rpc::NodePowerState state,
                                  std::string* error);
    uint64_t SetCurrentPowerState(const std::string& node_id,
                                  zb::rpc::NodePowerState state,
                                  std::string* error);

    bool Snapshot(uint64_t min_generation,
                  uint64_t* generation,
                  std::vector<NodeState>* nodes) const;
    bool GetNode(const std::string& node_id, NodeState* node) const;

    bool CreateOperation(const std::string& node_id,
                         zb::rpc::NodeOperationType type,
                         const std::string& message,
                         NodeOperationState* out,
                         std::string* error);
    bool UpdateOperation(const std::string& operation_id,
                         zb::rpc::NodeOperationStatus status,
                         const std::string& message,
                         std::string* error);
    bool GetOperation(const std::string& operation_id, NodeOperationState* out) const;

private:
    static NodeState BuildNodeState(const ManagedNodeRuntime& node);
    static HeartbeatAssignment BuildAssignment(const ManagedNodeRuntime& node,
                                               uint64_t generation);
    static uint64_t NowMs();

    mutable std::mutex mu_;
    FailureDetector detector_;
    NodePowerManager* nodes_{nullptr};
    uint64_t next_operation_id_{1};
    std::unordered_map<std::string, NodeOperationState> operations_;
};

} // namespace zb::scheduler
