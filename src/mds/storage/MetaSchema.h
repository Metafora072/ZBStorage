#pragma once

#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>

namespace zb::mds {

inline std::string InodeKey(uint64_t inode_id) {
    return "I/" + std::to_string(inode_id);
}

inline std::string DentryKey(uint64_t parent_inode, const std::string& name) {
    return "D/" + std::to_string(parent_inode) + "/" + name;
}

inline std::string DentryPrefix(uint64_t parent_inode) {
    return "D/" + std::to_string(parent_inode) + "/";
}

inline std::string FileArchiveStateKey(uint64_t inode_id) {
    return "AF/" + std::to_string(inode_id);
}

inline std::string FileArchiveStatePrefix() {
    return "AF/";
}

inline std::string PathPlacementPolicyKey(const std::string& normalized_path_prefix) {
    return "PR" + normalized_path_prefix;
}

inline std::string PathPlacementPolicyPrefix() {
    return "PR/";
}

inline std::string CmsNodeCatalogSnapshotKey() {
    return "SYS/CMS_NODE_CATALOG_V1";
}

inline std::string ArchiveNamespaceRouteKey(const std::string& normalized_path_prefix) {
    return "AN" + normalized_path_prefix;
}

inline std::string ArchiveNamespaceRoutePrefix() {
    return "AN/";
}

inline std::string ArchiveNamespaceCurrentKey(const std::string& normalized_path_prefix) {
    return "ANC" + normalized_path_prefix;
}

inline std::string ArchiveNamespaceCurrentPrefix() {
    return "ANC/";
}

constexpr uint64_t kArchiveInodeBucketSize = 1000000ULL;

inline uint64_t ArchiveInodeBucketId(uint64_t inode_id) {
    return inode_id / kArchiveInodeBucketSize;
}

inline std::string ArchiveInodeBucketComponent(uint64_t bucket_id) {
    std::ostringstream out;
    out << std::setw(20) << std::setfill('0') << bucket_id;
    return out.str();
}

inline std::string ArchiveInodeBucketKey(uint64_t bucket_id, const std::string& normalized_path_prefix) {
    return "AIB/" + ArchiveInodeBucketComponent(bucket_id) + normalized_path_prefix;
}

inline std::string ArchiveInodeBucketPrefix(uint64_t bucket_id) {
    return "AIB/" + ArchiveInodeBucketComponent(bucket_id) + "/";
}

inline std::string ArchiveRouteBucketKey(const std::string& normalized_path_prefix, uint64_t bucket_id) {
    return "AIR" + normalized_path_prefix + "/" + ArchiveInodeBucketComponent(bucket_id);
}

inline std::string ArchiveRouteBucketPrefix(const std::string& normalized_path_prefix) {
    return "AIR" + normalized_path_prefix + "/";
}

inline std::string MasstreeNumericComponent(uint64_t value) {
    std::ostringstream out;
    out << std::setw(20) << std::setfill('0') << value;
    return out.str();
}

inline std::string MasstreeNamespaceRouteKey(const std::string& normalized_path_prefix) {
    return "MTR" + normalized_path_prefix;
}

inline std::string MasstreeNamespaceRoutePrefix() {
    return "MTR/";
}

inline std::string MasstreeNamespaceCurrentKey(const std::string& normalized_path_prefix) {
    return "MTC" + normalized_path_prefix;
}

inline std::string MasstreeNamespaceCurrentPrefix() {
    return "MTC/";
}

inline std::string MasstreeNamespaceManifestKey(const std::string& namespace_id,
                                                const std::string& generation_id) {
    return "MTM/" + namespace_id + "/" + generation_id;
}

inline std::string MasstreeNamespaceManifestPrefix(const std::string& namespace_id) {
    return "MTM/" + namespace_id + "/";
}

inline std::string MasstreeInodeIndexKey(const std::string& namespace_id, uint64_t inode_id) {
    return "MTI/" + namespace_id + "/" + MasstreeNumericComponent(inode_id);
}

inline std::string MasstreeInodeIndexPrefix(const std::string& namespace_id) {
    return "MTI/" + namespace_id + "/";
}

inline std::string MasstreeDentryIndexKey(const std::string& namespace_id,
                                          uint64_t parent_inode,
                                          const std::string& name) {
    return "MTD/" + namespace_id + "/" + MasstreeNumericComponent(parent_inode) + "/" + name;
}

inline std::string MasstreeDentryIndexPrefix(const std::string& namespace_id,
                                             uint64_t parent_inode) {
    return "MTD/" + namespace_id + "/" + MasstreeNumericComponent(parent_inode) + "/";
}

constexpr uint64_t kMasstreeInodeBucketSize = 1000000ULL;

inline uint64_t MasstreeInodeBucketId(uint64_t inode_id) {
    return inode_id / kMasstreeInodeBucketSize;
}

inline std::string MasstreeInodeBucketComponent(uint64_t bucket_id) {
    return MasstreeNumericComponent(bucket_id);
}

inline std::string MasstreeInodeBucketKey(uint64_t bucket_id, const std::string& normalized_path_prefix) {
    return "MTIB/" + MasstreeInodeBucketComponent(bucket_id) + normalized_path_prefix;
}

inline std::string MasstreeInodeBucketPrefix(uint64_t bucket_id) {
    return "MTIB/" + MasstreeInodeBucketComponent(bucket_id) + "/";
}

inline std::string MasstreeRouteBucketKey(const std::string& normalized_path_prefix, uint64_t bucket_id) {
    return "MTIR" + normalized_path_prefix + "/" + MasstreeInodeBucketComponent(bucket_id);
}

inline std::string MasstreeRouteBucketPrefix(const std::string& normalized_path_prefix) {
    return "MTIR" + normalized_path_prefix + "/";
}

inline std::string MasstreeClusterStatsCurrentKey() {
    return "MTS/cluster/current";
}

inline std::string MasstreeOpticalLayoutCurrentKey() {
    return "MTS/optical/layout/current";
}

inline std::string MasstreeNamespaceStatsCurrentKey(const std::string& namespace_id) {
    return "MTS/namespace/" + namespace_id + "/current";
}

inline std::string PgViewKey(uint64_t epoch, uint32_t pg_id) {
    return "PV/" + std::to_string(epoch) + "/" + std::to_string(pg_id);
}

inline std::string PgViewPrefix(uint64_t epoch) {
    return "PV/" + std::to_string(epoch) + "/";
}

inline std::string PgViewEpochKey() {
    return "PV/current_epoch";
}

inline bool ParsePgViewKey(const std::string& key, uint64_t* epoch, uint32_t* pg_id) {
    if (!epoch || !pg_id) {
        return false;
    }
    constexpr char kPrefix[] = "PV/";
    if (key.rfind(kPrefix, 0) != 0) {
        return false;
    }
    size_t sep = key.find('/', sizeof(kPrefix) - 1);
    if (sep == std::string::npos) {
        return false;
    }
    try {
        *epoch = static_cast<uint64_t>(std::stoull(key.substr(sizeof(kPrefix) - 1, sep - (sizeof(kPrefix) - 1))));
        *pg_id = static_cast<uint32_t>(std::stoul(key.substr(sep + 1)));
        return true;
    } catch (...) {
        return false;
    }
}

inline std::string HandleKey(uint64_t handle_id) {
    return "H/" + std::to_string(handle_id);
}

inline std::string NextInodeKey() {
    return "X/next_inode";
}

inline std::string NextHandleKey() {
    return "X/next_handle";
}

constexpr uint64_t kRootInodeId = 1;

} // namespace zb::mds
