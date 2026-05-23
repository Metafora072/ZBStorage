#pragma once

#include "sim/masstree/SimMasstreeTypes.h"

#include <map>
#include <memory>
#include <string>

#ifdef SIM_MASSTREE_REAL_INDEX
class MasstreeWrapper;
#endif

namespace zb::sim::masstree {

class SimMasstreeIndexRuntime {
public:
    bool Init(std::string* error);
    void Clear();

    bool PutInodePageBoundary(const std::string& namespace_id,
                              uint64_t max_inode_id,
                              uint64_t page_offset,
                              std::string* error);
    bool FindInodePageBoundary(const std::string& namespace_id,
                               uint64_t inode_id,
                               SimInodeSparseEntry* entry,
                               std::string* error) const;

    bool PutDentryPageBoundary(const std::string& namespace_id,
                               uint64_t parent_inode,
                               const std::string& max_name,
                               uint64_t page_offset,
                               std::string* error);
    bool FindDentryPageBoundary(const std::string& namespace_id,
                                uint64_t parent_inode,
                                const std::string& name,
                                SimDentrySparseEntry* entry,
                                std::string* error) const;

private:
    struct DentryBoundaryKey {
        uint64_t parent_inode{0};
        std::string name;

        bool operator<(const DentryBoundaryKey& other) const {
            if (parent_inode != other.parent_inode) {
                return parent_inode < other.parent_inode;
            }
            return name < other.name;
        }
    };

    std::map<std::string, std::map<uint64_t, uint64_t>> inode_boundaries_;
    std::map<std::string, std::map<DentryBoundaryKey, uint64_t>> dentry_boundaries_;
#ifdef SIM_MASSTREE_REAL_INDEX
    std::unique_ptr<MasstreeWrapper> inode_tree_;
    std::unique_ptr<MasstreeWrapper> dentry_tree_;
#endif
};

} // namespace zb::sim::masstree
