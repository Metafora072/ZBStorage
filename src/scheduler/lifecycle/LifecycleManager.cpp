#include "LifecycleManager.h"

namespace zb::scheduler {

LifecycleManager::LifecycleManager(ClusterState* state, NodeActuator* actuator)
    : state_(state), actuator_(actuator) {}

bool LifecycleManager::StartNode(const std::string& node_id,
                                 const std::string& reason,
                                 NodeOperationState* op,
                                 std::string* error) {
    return RunOperation(node_id, zb::rpc::NODE_OP_START, false, reason, op, error);
}

bool LifecycleManager::StopNode(const std::string& node_id,
                                bool force,
                                const std::string& reason,
                                NodeOperationState* op,
                                std::string* error) {
    return RunOperation(node_id, zb::rpc::NODE_OP_STOP, force, reason, op, error);
}

bool LifecycleManager::RebootNode(const std::string& node_id,
                                  const std::string& reason,
                                  NodeOperationState* op,
                                  std::string* error) {
    return RunOperation(node_id, zb::rpc::NODE_OP_REBOOT, false, reason, op, error);
}

bool LifecycleManager::RunOperation(const std::string& node_id,
                                    zb::rpc::NodeOperationType type,
                                    bool force,
                                    const std::string& reason,
                                    NodeOperationState* op,
                                    std::string* error) {
    if (!state_) {
        if (error) {
            *error = "LifecycleManager is not initialized";
        }
        return false;
    }

    NodeState node;
    if (!state_->GetNode(node_id, &node)) {
        if (error) {
            *error = "node not found: " + node_id;
        }
        return false;
    }
    if (type == zb::rpc::NODE_OP_STOP && !force &&
        (node.resident_data_bytes != 0 || node.optical_local_disc_count != 0)) {
        if (error) {
            *error = node.optical_local_disc_count != 0
                         ? "safe stop rejected: optical library still has local discs"
                         : "safe stop rejected: node still has resident data";
        }
        return false;
    }

    NodeOperationState operation;
    if (!state_->CreateOperation(node_id, type, reason, &operation, error)) {
        return false;
    }

    if (type == zb::rpc::NODE_OP_STOP) {
        state_->SetNodeAdminState(node_id, zb::rpc::NODE_ADMIN_DRAINING, nullptr);
        state_->SetDesiredPowerState(node_id, zb::rpc::NODE_POWER_OFF, nullptr);
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_STOPPING, nullptr);
    } else if (type == zb::rpc::NODE_OP_START) {
        state_->SetDesiredPowerState(node_id, zb::rpc::NODE_POWER_ON, nullptr);
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_STARTING, nullptr);
        state_->SetNodeAdminState(node_id, zb::rpc::NODE_ADMIN_ENABLED, nullptr);
    } else if (type == zb::rpc::NODE_OP_REBOOT) {
        state_->SetDesiredPowerState(node_id, zb::rpc::NODE_POWER_ON, nullptr);
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_STOPPING, nullptr);
        state_->SetNodeAdminState(node_id, zb::rpc::NODE_ADMIN_DRAINING, nullptr);
    }

    ActuatorResult result{true, "No actuator configured"};
    if (actuator_) {
        if (type == zb::rpc::NODE_OP_START) {
            result = actuator_->StartNode(node_id, node.address);
        } else if (type == zb::rpc::NODE_OP_STOP) {
            result = actuator_->StopNode(node_id, node.address, force);
        } else {
            result = actuator_->RebootNode(node_id, node.address);
        }
    }

    if (!result.success) {
        state_->SetDesiredPowerState(node_id, node.desired_power_state, nullptr);
        state_->SetCurrentPowerState(node_id, node.power_state, nullptr);
        state_->SetNodeAdminState(node_id, node.admin_state, nullptr);
        state_->UpdateOperation(operation.operation_id, zb::rpc::NODE_OP_FAILED, result.message, nullptr);
        if (op) {
            state_->GetOperation(operation.operation_id, op);
        }
        if (error) {
            *error = result.message;
        }
        return false;
    }

    if (type == zb::rpc::NODE_OP_STOP) {
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_OFF, nullptr);
        state_->SetNodeAdminState(node_id, zb::rpc::NODE_ADMIN_DISABLED, nullptr);
    } else if (type == zb::rpc::NODE_OP_START) {
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_STARTING, nullptr);
    } else {
        state_->SetCurrentPowerState(node_id, zb::rpc::NODE_POWER_STARTING, nullptr);
        state_->SetNodeAdminState(node_id, zb::rpc::NODE_ADMIN_ENABLED, nullptr);
    }

    state_->UpdateOperation(operation.operation_id, zb::rpc::NODE_OP_SUCCEEDED, result.message, nullptr);
    if (op) {
        state_->GetOperation(operation.operation_id, op);
    }
    return true;
}

} // namespace zb::scheduler
