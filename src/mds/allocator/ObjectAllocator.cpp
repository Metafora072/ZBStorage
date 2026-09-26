#include "ObjectAllocator.h"

namespace zb::mds {

ObjectAllocator::ObjectAllocator(NodeStateCache* cache, PGManager* pg_manager)
    : cache_(cache), pg_manager_(pg_manager) {}

bool ObjectAllocator::AllocateObject(uint32_t replica,
                                     const std::string& object_id,
                                     std::vector<zb::rpc::ReplicaLocation>* out) {
    if (!out || replica == 0) {
        return false;
    }

    if (!pg_manager_) {
        return false;
    }
    std::string error;
    return AllocateObjectByPg(replica, object_id, pg_manager_->CurrentEpoch(), out, &error);
}

bool ObjectAllocator::AllocateObjectByPg(uint32_t replica,
                                         const std::string& object_id,
                                         uint64_t epoch,
                                         std::vector<zb::rpc::ReplicaLocation>* out,
                                         std::string* error) {
    return AllocateObjectByPgWithType(replica,
                                      object_id,
                                      epoch,
                                      NodeType::kReal,
                                      false,
                                      out,
                                      error);
}

bool ObjectAllocator::AllocateObjectByPgWithType(uint32_t replica,
                                                 const std::string& object_id,
                                                 uint64_t epoch,
                                                 NodeType required_type,
                                                 bool strict_type,
                                                 std::vector<zb::rpc::ReplicaLocation>* out,
                                                 std::string* error) {
    if (!out || replica == 0) {
        if (error) {
            *error = "invalid allocate arguments";
        }
        return false;
    }
    if (!pg_manager_) {
        if (error) {
            *error = "pg manager is unavailable";
        }
        return false;
    }
    if (epoch == 0) {
        epoch = pg_manager_->CurrentEpoch();
    }
    if (epoch == 0) {
        if (error) {
            *error = "pg epoch is unavailable";
        }
        return false;
    }

    const uint32_t pg_id = pg_manager_->ObjectToPg(object_id, epoch);
    PgReplicaSet pg_set;
    if (!pg_manager_->ResolvePg(pg_id, epoch, &pg_set, error)) {
        return false;
    }
    if (pg_set.members.size() < replica) {
        if (error) {
            *error = "insufficient pg replicas, required=" + std::to_string(replica) +
                     ", actual=" + std::to_string(pg_set.members.size());
        }
        return false;
    }

    out->clear();
    out->reserve(replica);
    for (const auto& member : pg_set.members) {
        if (strict_type && member.node_type != required_type) {
            continue;
        }
        zb::rpc::ReplicaLocation location;
        location.set_node_id(member.node_id);
        location.set_node_address(member.node_address);
        location.set_disk_id(member.disk_id);
        location.set_object_id(object_id);
        location.set_group_id(member.group_id);
        location.set_epoch(member.epoch);
        location.set_primary_node_id(member.primary_node_id);
        location.set_primary_address(member.primary_address);
        location.set_secondary_node_id(member.secondary_node_id);
        location.set_secondary_address(member.secondary_address);
        location.set_sync_ready(member.sync_ready);
        location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        location.set_replica_state(zb::rpc::REPLICA_READY);
        out->push_back(std::move(location));
        if (out->size() >= replica) {
            break;
        }
    }
    if (!strict_type && out->size() < replica) {
        for (const auto& member : pg_set.members) {
            bool exists = false;
            for (const auto& current : *out) {
                if (current.node_id() == member.node_id && current.disk_id() == member.disk_id) {
                    exists = true;
                    break;
                }
            }
            if (exists) {
                continue;
            }
            zb::rpc::ReplicaLocation location;
            location.set_node_id(member.node_id);
            location.set_node_address(member.node_address);
            location.set_disk_id(member.disk_id);
            location.set_object_id(object_id);
            location.set_group_id(member.group_id);
            location.set_epoch(member.epoch);
            location.set_primary_node_id(member.primary_node_id);
            location.set_primary_address(member.primary_address);
            location.set_secondary_node_id(member.secondary_node_id);
            location.set_secondary_address(member.secondary_address);
            location.set_sync_ready(member.sync_ready);
            location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
            location.set_replica_state(zb::rpc::REPLICA_READY);
            out->push_back(std::move(location));
            if (out->size() >= replica) {
                break;
            }
        }
    }
    if (out->size() < replica) {
        if (error) {
            if (strict_type) {
                *error = "insufficient replicas for required node type";
            } else {
                *error = "insufficient replicas after placement filter";
            }
        }
        return false;
    }
    return true;
}

bool ObjectAllocator::AllocateObjectDirectByType(uint32_t replica,
                                                 const std::string& object_id,
                                                 NodeType required_type,
                                                 std::vector<zb::rpc::ReplicaLocation>* out,
                                                 std::string* error) {
    if (!out || replica == 0) {
        if (error) {
            *error = "invalid allocate arguments";
        }
        return false;
    }
    if (!cache_) {
        if (error) {
            *error = "node cache is unavailable";
        }
        return false;
    }

    std::vector<NodeSelection> selections = cache_->PickNodesByType(replica, required_type);
    if (selections.size() < replica) {
        if (error) {
            *error = "insufficient allocatable nodes for required node type";
        }
        return false;
    }

    out->clear();
    out->reserve(replica);
    for (const auto& selection : selections) {
        if (selection.node_id.empty() || selection.disk_id.empty()) {
            continue;
        }
        zb::rpc::ReplicaLocation location;
        location.set_node_id(selection.node_id);
        location.set_node_address(selection.address);
        location.set_disk_id(selection.disk_id);
        location.set_object_id(object_id);
        location.set_group_id(selection.group_id);
        location.set_epoch(selection.epoch);
        location.set_primary_node_id(selection.node_id);
        location.set_primary_address(selection.address);
        location.set_secondary_node_id(selection.secondary_node_id);
        location.set_secondary_address(selection.secondary_address);
        location.set_sync_ready(selection.sync_ready);
        location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        location.set_replica_state(zb::rpc::REPLICA_READY);
        out->push_back(std::move(location));
        if (out->size() >= replica) {
            break;
        }
    }
    if (out->size() < replica) {
        if (error) {
            *error = "insufficient usable direct replicas for required node type";
        }
        return false;
    }
    return true;
}

bool ObjectAllocator::AllocateObjectDirect(
    uint32_t replica,
    const std::string& object_id,
    std::vector<zb::rpc::ReplicaLocation>* out,
    std::string* error) {
    if (!out || replica == 0 || !cache_) {
        if (error) *error = "invalid direct allocation arguments";
        return false;
    }
    const auto selections = cache_->PickNodes(replica);
    if (selections.size() < replica) {
        if (error) *error = "insufficient CMS-authorized nodes for direct allocation";
        return false;
    }
    out->clear();
    out->reserve(replica);
    for (const auto& selection : selections) {
        if (selection.node_id.empty() || selection.disk_id.empty()) continue;
        zb::rpc::ReplicaLocation location;
        location.set_node_id(selection.node_id);
        location.set_node_address(selection.address);
        location.set_disk_id(selection.disk_id);
        location.set_object_id(object_id);
        location.set_group_id(selection.group_id);
        location.set_epoch(selection.epoch);
        location.set_primary_node_id(selection.node_id);
        location.set_primary_address(selection.address);
        location.set_secondary_node_id(selection.secondary_node_id);
        location.set_secondary_address(selection.secondary_address);
        location.set_sync_ready(selection.sync_ready);
        location.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        location.set_replica_state(zb::rpc::REPLICA_READY);
        out->push_back(std::move(location));
    }
    if (out->size() < replica) {
        if (error) *error = "CMS-authorized direct allocation returned incomplete replicas";
        return false;
    }
    return true;
}

bool ObjectAllocator::ResolveNodeAddress(const std::string& node_id, std::string* address) const {
    if (!cache_ || node_id.empty() || !address) {
        return false;
    }
    return cache_->ResolveNodeAddress(node_id, address);
}

bool ObjectAllocator::ResolveDiskId(const std::string& node_id,
                                    uint32_t numeric_disk_id,
                                    std::string* disk_id) const {
    if (!cache_ || node_id.empty() || numeric_disk_id == 0 || !disk_id) {
        return false;
    }
    return cache_->ResolveDiskId(node_id, numeric_disk_id, disk_id);
}

void ObjectAllocator::SetPgManager(PGManager* pg_manager) {
    pg_manager_ = pg_manager;
}

} // namespace zb::mds
