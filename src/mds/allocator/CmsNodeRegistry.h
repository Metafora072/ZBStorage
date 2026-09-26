#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "NodeStateCache.h"
#include "PGManager.h"
#include "mds.pb.h"

namespace zb::mds {

class RocksMetaStore;

// CMS-owned, in-memory authority for node membership and placement-visible
// state. Scheduler publishes proposals; only a successful commit changes the
// NodeStateCache consumed by the real I/O allocator.
class CmsNodeRegistry {
public:
    CmsNodeRegistry(NodeStateCache* cache,
                    PGManager* pg_manager,
                    RocksMetaStore* store = nullptr);

    // Restores the last CMS-committed catalog before serving requests. A
    // missing snapshot is a clean first start, not an error.
    bool Restore(std::string* error);

    bool Commit(uint64_t scheduler_generation,
                const std::vector<zb::rpc::CmsNodeCatalogEntry>& proposed,
                uint64_t* cms_generation,
                std::vector<zb::rpc::CmsNodeCompactIdAssignment>* assignments,
                std::string* error);

    std::vector<zb::rpc::CmsNodeCatalogEntry> Snapshot(uint64_t* cms_generation,
                                                       uint64_t* scheduler_generation) const;

    bool IsWriteAdmitted(const std::string& node_id) const;

private:
    static bool IsRetired(const zb::rpc::ManagedNodeView& node);
    static bool IsWriteAllocatable(const zb::rpc::ManagedNodeView& node);
    static uint32_t SchedulingWeight(const zb::rpc::ManagedNodeView& node);
    static NodeInfo BuildNodeInfo(const zb::rpc::CmsNodeCatalogEntry& entry);
    bool PersistLocked(const std::map<std::string, zb::rpc::CmsNodeCatalogEntry>& nodes,
                       uint64_t cms_generation,
                       uint64_t scheduler_generation,
                       uint32_t next_storage_compact_id,
                       uint32_t next_optical_compact_id,
                       std::string* error) const;
    void PublishPlacementLocked();

    NodeStateCache* cache_{};
    PGManager* pg_manager_{};
    RocksMetaStore* store_{};
    mutable std::mutex mu_;
    std::map<std::string, zb::rpc::CmsNodeCatalogEntry> nodes_;
    uint64_t cms_generation_{1};
    uint64_t scheduler_generation_{0};
    uint32_t next_storage_compact_id_{1};
    uint32_t next_optical_compact_id_{1};
};

} // namespace zb::mds
