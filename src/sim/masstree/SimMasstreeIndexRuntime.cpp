#include "sim/masstree/SimMasstreeIndexRuntime.h"

#include <iomanip>
#include <sstream>
#include <thread>
#include <vector>

#ifdef SIM_MASSTREE_REAL_INDEX
#include "masstree_wrapper.h"
#endif

namespace zb::sim::masstree {

namespace {

std::string FixedUint64(uint64_t value) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << value;
    return oss.str();
}

std::string InodeKey(const std::string& namespace_id, uint64_t inode_id) {
    return "MTI/" + namespace_id + "/" + FixedUint64(inode_id);
}

std::string InodePrefix(const std::string& namespace_id) {
    return "MTI/" + namespace_id + "/";
}

std::string DentryKey(const std::string& namespace_id, uint64_t parent_inode, const std::string& name) {
    return "MTD/" + namespace_id + "/" + FixedUint64(parent_inode) + "/" + name;
}

std::string DentryPrefix(const std::string& namespace_id) {
    return "MTD/" + namespace_id + "/";
}

bool ParseDentryKey(const std::string& key,
                    const std::string& namespace_id,
                    uint64_t* parent_inode,
                    std::string* name) {
    const std::string prefix = DentryPrefix(namespace_id);
    if (key.rfind(prefix, 0) != 0) {
        return false;
    }
    const size_t parent_begin = prefix.size();
    const size_t parent_end = key.find('/', parent_begin);
    if (parent_end == std::string::npos) {
        return false;
    }
    *parent_inode = static_cast<uint64_t>(std::stoull(key.substr(parent_begin, parent_end - parent_begin)));
    *name = key.substr(parent_end + 1);
    return true;
}

} // namespace

bool SimMasstreeIndexRuntime::Init(std::string* error) {
#ifdef SIM_MASSTREE_REAL_INDEX
    inode_tree_ = std::make_unique<MasstreeWrapper>();
    dentry_tree_ = std::make_unique<MasstreeWrapper>();
    if (MasstreeWrapper::ti == nullptr) {
        const uint64_t tid = std::hash<std::thread::id>{}(std::this_thread::get_id());
        MasstreeWrapper::thread_init(static_cast<int>(tid & 0x7fffffff));
    }
#endif
    if (error) {
        error->clear();
    }
    return true;
}

void SimMasstreeIndexRuntime::Clear() {
    inode_boundaries_.clear();
    dentry_boundaries_.clear();
#ifdef SIM_MASSTREE_REAL_INDEX
    inode_tree_ = std::make_unique<MasstreeWrapper>();
    dentry_tree_ = std::make_unique<MasstreeWrapper>();
#endif
}

bool SimMasstreeIndexRuntime::PutInodePageBoundary(const std::string& namespace_id,
                                                   uint64_t max_inode_id,
                                                   uint64_t page_offset,
                                                   std::string* error) {
    inode_boundaries_[namespace_id][max_inode_id] = page_offset;
#ifdef SIM_MASSTREE_REAL_INDEX
    if (inode_tree_) {
        inode_tree_->insert(InodeKey(namespace_id, max_inode_id), page_offset);
    }
#endif
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeIndexRuntime::FindInodePageBoundary(const std::string& namespace_id,
                                                    uint64_t inode_id,
                                                    SimInodeSparseEntry* entry,
                                                    std::string* error) const {
    if (!entry) {
        if (error) {
            *error = "inode sparse output is null";
        }
        return false;
    }
    const auto ns_it = inode_boundaries_.find(namespace_id);
#ifdef SIM_MASSTREE_REAL_INDEX
    if (inode_tree_) {
        std::vector<std::pair<std::string, uint64_t>> hits;
        inode_tree_->scan(InodeKey(namespace_id, inode_id), 1, hits);
        if (!hits.empty()) {
            const std::string prefix = InodePrefix(namespace_id);
            if (hits.front().first.rfind(prefix, 0) == 0) {
                entry->max_inode_id = static_cast<uint64_t>(std::stoull(hits.front().first.substr(prefix.size())));
                entry->page_offset = hits.front().second;
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
    }
#endif
    if (ns_it == inode_boundaries_.end()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const auto it = ns_it->second.lower_bound(inode_id);
    if (it == ns_it->second.end()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    entry->max_inode_id = it->first;
    entry->page_offset = it->second;
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeIndexRuntime::PutDentryPageBoundary(const std::string& namespace_id,
                                                    uint64_t parent_inode,
                                                    const std::string& max_name,
                                                    uint64_t page_offset,
                                                    std::string* error) {
    dentry_boundaries_[namespace_id][DentryBoundaryKey{parent_inode, max_name}] = page_offset;
#ifdef SIM_MASSTREE_REAL_INDEX
    if (dentry_tree_) {
        dentry_tree_->insert(DentryKey(namespace_id, parent_inode, max_name), page_offset);
    }
#endif
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeIndexRuntime::FindDentryPageBoundary(const std::string& namespace_id,
                                                     uint64_t parent_inode,
                                                     const std::string& name,
                                                     SimDentrySparseEntry* entry,
                                                     std::string* error) const {
    if (!entry) {
        if (error) {
            *error = "dentry sparse output is null";
        }
        return false;
    }
    const auto ns_it = dentry_boundaries_.find(namespace_id);
#ifdef SIM_MASSTREE_REAL_INDEX
    if (dentry_tree_) {
        std::vector<std::pair<std::string, uint64_t>> hits;
        dentry_tree_->scan(DentryKey(namespace_id, parent_inode, name), 1, hits);
        if (!hits.empty()) {
            uint64_t parsed_parent = 0;
            std::string parsed_name;
            if (ParseDentryKey(hits.front().first, namespace_id, &parsed_parent, &parsed_name)) {
                entry->max_parent_inode = parsed_parent;
                entry->max_name = parsed_name;
                entry->page_offset = hits.front().second;
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
    }
#endif
    if (ns_it == dentry_boundaries_.end()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    const auto it = ns_it->second.lower_bound(DentryBoundaryKey{parent_inode, name});
    if (it == ns_it->second.end()) {
        if (error) {
            error->clear();
        }
        return false;
    }
    entry->max_parent_inode = it->first.parent_inode;
    entry->max_name = it->first.name;
    entry->page_offset = it->second;
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::sim::masstree
