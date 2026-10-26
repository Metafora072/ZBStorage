#include "MetaStoreRouter.h"

#include <vector>

namespace zb::mds {

MetaStoreRouter::MetaStoreRouter(NamespaceStore* hot_store,
                                 ArchiveNamespaceCatalog* archive_catalog,
                                 ArchiveMetaStore* archive_store,
                                 MasstreeNamespaceCatalog* masstree_catalog,
                                 MasstreeMetaStore* masstree_store, bool unified)
    : unified_(unified), hot_store_(hot_store),
      archive_catalog_(archive_catalog),
      archive_store_(archive_store),
      masstree_catalog_(masstree_catalog),
      masstree_store_(masstree_store) {
}

bool MetaStoreRouter::GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr,
                               std::string* error) const {
    if (error) error->clear();
    if (!hot_store_ || !attr || inode_id == 0) {
        if (error) *error = "invalid inode lookup args";
        return false;
    }
    if (unified_) return hot_store_->GetInode(inode_id, attr, error);
    if (hot_store_->GetInode(inode_id, attr, error)) return true;
    if (error && !error->empty()) return false;
    if (GetArchiveInode(inode_id, attr, error)) return true;
    if (error && !error->empty()) return false;
    return GetMasstreeInode(inode_id, attr, error);
}

bool MetaStoreRouter::GetArchiveInode(uint64_t inode_id,
                                      zb::rpc::InodeAttr* attr,
                                      std::string* error) const {
    if (!archive_catalog_ || !archive_store_) {
        if (error) {
            error->clear();
        }
        return false;
    }
    std::vector<ArchiveNamespaceRoute> routes;
    if (!archive_catalog_->LookupByInode(inode_id, &routes, error)) {
        return false;
    }
    if (routes.empty() && !archive_catalog_->ListRoutes(&routes, error)) {
        return false;
    }
    for (const auto& route : routes) {
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        zb::rpc::InodeAttr candidate;
        std::string local_error;
        if (archive_store_->GetInode(route, inode_id, &candidate, &local_error)) {
            *attr = std::move(candidate);
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MetaStoreRouter::GetMasstreeInode(uint64_t inode_id,
                                       zb::rpc::InodeAttr* attr,
                                       std::string* error) const {
    if (!masstree_catalog_ || !masstree_store_) {
        if (error) {
            error->clear();
        }
        return false;
    }
    std::vector<MasstreeNamespaceRoute> routes;
    if (!masstree_catalog_->LookupByInode(inode_id, &routes, error)) {
        return false;
    }
    if (routes.empty() && !masstree_catalog_->ListRoutes(&routes, error)) {
        return false;
    }
    for (const auto& route : routes) {
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        zb::rpc::InodeAttr candidate;
        std::string local_error;
        if (masstree_store_->GetInode(route, inode_id, &candidate, &local_error)) {
            *attr = std::move(candidate);
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MetaStoreRouter::ResolvePath(const std::string& path,
                                  uint64_t* inode_id,
                                  zb::rpc::InodeAttr* attr,
                                  std::string* error) const {
    if (unified_) return hot_store_->ResolvePath(path, inode_id, attr, error);
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }

    ArchiveNamespaceRoute route;
    std::string route_error;
    if (archive_catalog_ && archive_store_ &&
        archive_catalog_->LookupByPath(path, &route, &route_error)) {
        return archive_store_->ResolvePath(route, path, inode_id, attr, error);
    }
    if (!route_error.empty() && error) {
        *error = route_error;
        return false;
    }

    MasstreeNamespaceRoute masstree_route;
    route_error.clear();
    if (masstree_catalog_ && masstree_store_ &&
        masstree_catalog_->LookupByPath(path, &masstree_route, &route_error)) {
        return masstree_store_->ResolvePath(masstree_route, path, inode_id, attr, error);
    }
    if (!route_error.empty() && error) {
        *error = route_error;
        return false;
    }

    return hot_store_->ResolvePath(path, inode_id, attr, error);
}

bool MetaStoreRouter::Readdir(const std::string& path,
                              const std::string& start_after,
                              uint32_t limit,
                              std::vector<zb::rpc::Dentry>* entries,
                              bool* has_more,
                              std::string* next_token,
                              std::string* error) const {
    if (unified_) return hot_store_->Readdir(path, start_after, limit, entries, has_more, next_token, error);
    if (!entries) {
        if (error) {
            *error = "entries output is null";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    ArchiveNamespaceRoute route;
    std::string route_error;
    if (archive_catalog_ && archive_store_ &&
        archive_catalog_->LookupByPath(path, &route, &route_error)) {
        return archive_store_->Readdir(route, path, start_after, limit, entries, has_more, next_token, error);
    }
    if (!route_error.empty()) {
        if (error) {
            *error = route_error;
        }
        return false;
    }

    MasstreeNamespaceRoute masstree_route;
    route_error.clear();
    if (masstree_catalog_ && masstree_store_ &&
        masstree_catalog_->LookupByPath(path, &masstree_route, &route_error)) {
        return masstree_store_->Readdir(
            masstree_route, path, start_after, limit, entries, has_more, next_token, error);
    }
    if (!route_error.empty()) {
        if (error) {
            *error = route_error;
        }
        return false;
    }

    return hot_store_->Readdir(path, start_after, limit, entries, has_more, next_token, error);
}

} // namespace zb::mds
