#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
#include <fuse3/fuse_lowlevel.h>
#include <gflags/gflags.h>

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <cstring>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <limits>
#include <string>
#include <sstream>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/metrics/IoLatency.h"
#include "mds.pb.h"
#include "real_node.pb.h"
#include "scheduler.pb.h"

DEFINE_string(mds, "127.0.0.1:9000", "MDS endpoint");
DEFINE_string(scheduler, "127.0.0.1:9100", "Scheduler endpoint for node-id address resolution");
DEFINE_uint32(cluster_view_refresh_ms, 2000, "Scheduler cluster-view refresh interval in ms");
DEFINE_uint32(cluster_view_ttl_ms, 10000, "Max tolerated staleness of scheduler view in ms");
DEFINE_uint32(default_replica, 1, "Default replica for create");
DEFINE_uint64(default_object_unit_size, 4194304, "Default object unit size for create");
DEFINE_int32(timeout_ms, 3000, "RPC timeout in ms");
DEFINE_int32(max_retry, 0, "RPC max retry");
DEFINE_bool(bootstrap_tier_dirs, true, "Create and configure tier-specific subdirectories on mount");
DEFINE_string(real_dir_name, "real", "Top-level directory for real-node backed POSIX files");
DEFINE_string(virtual_dir_name, "virtual", "Top-level directory for virtual-node backed POSIX files");

#ifndef ZBSTORAGE_SOURCE_ROOT
#define ZBSTORAGE_SOURCE_ROOT "."
#endif
DEFINE_bool(io_latency_enabled, false, "Record per-request client I/O latency to CSV");
DEFINE_string(io_latency_dir, "", "Final CSV directory; empty uses ROOT_PATH/client/metrics from base.conf");
DEFINE_string(io_latency_base_conf, ZBSTORAGE_SOURCE_ROOT "/config/base.conf", "Base runtime configuration");
DEFINE_uint32(io_latency_flush_ms, 1000, "CSV sync interval in milliseconds");
DEFINE_uint32(io_latency_batch_records, 1024, "Records per CSV write batch");
DEFINE_uint32(io_latency_queue_capacity, 65536, "Maximum queued latency records");

namespace zb::client::fuse_client {

using metrics::IoTraceContext;
using metrics::RpcLatencyScope;
using metrics::RpcTarget;

namespace {

constexpr const char* kReadOnlyOpticalMessage = "READ_ONLY_OPTICAL";
constexpr const char* kNoSpaceRealPolicyMessage = "NO_SPACE_REAL_POLICY";
constexpr const char* kNoSpaceVirtualPolicyMessage = "NO_SPACE_VIRTUAL_POLICY";
constexpr const char* kCrossTierRenameMessage = "CROSS_TIER_RENAME";

int StatusToErrno(const zb::rpc::MdsStatus& status) {
    if (status.message() == kNoSpaceRealPolicyMessage ||
        status.message() == kNoSpaceVirtualPolicyMessage) {
        return ENOSPC;
    }
    switch (status.code()) {
        case zb::rpc::MDS_OK:
            return 0;
        case zb::rpc::MDS_INVALID_ARGUMENT:
            if (status.message() == kReadOnlyOpticalMessage) {
                return EROFS;
            }
            if (status.message() == kCrossTierRenameMessage) {
                return EXDEV;
            }
            return EINVAL;
        case zb::rpc::MDS_NOT_FOUND:
            return ENOENT;
        case zb::rpc::MDS_ALREADY_EXISTS:
            return EEXIST;
        case zb::rpc::MDS_NOT_EMPTY:
            return ENOTEMPTY;
        case zb::rpc::MDS_STALE_EPOCH:
            return EAGAIN;
        default:
            return EIO;
    }
}

void LogMdsFailure(const char* op, const char* path, const zb::rpc::MdsStatus& status) {
    std::cerr << "[fuse] " << op
              << " failed: path=" << (path ? path : "")
              << " code=" << static_cast<int>(status.code())
              << " msg=" << status.message()
              << " errno=" << StatusToErrno(status)
              << std::endl;
}

void LogDataFailure(const char* op,
                    const char* path,
                    uint64_t inode_id,
                    const zb::rpc::ReplicaLocation& anchor,
                    const zb::rpc::Status& status) {
    int mapped_errno = EIO;
    switch (status.code()) {
        case zb::rpc::STATUS_OK:
            mapped_errno = 0;
            break;
        case zb::rpc::STATUS_INVALID_ARGUMENT:
            mapped_errno = EINVAL;
            break;
        case zb::rpc::STATUS_NOT_FOUND:
            mapped_errno = ENOENT;
            break;
        case zb::rpc::STATUS_IO_ERROR:
            mapped_errno = (status.message() == "VERSION_MISMATCH") ? EAGAIN : EIO;
            break;
        case zb::rpc::STATUS_INTERNAL_ERROR:
        default:
            mapped_errno = EIO;
            break;
    }
    std::cerr << "[fuse] " << op
              << " failed: path=" << (path ? path : "")
              << " inode_id=" << inode_id
              << " node_id=" << anchor.node_id()
              << " disk_id=" << anchor.disk_id()
              << " tier=" << static_cast<int>(anchor.storage_tier())
              << " code=" << static_cast<int>(status.code())
              << " msg=" << status.message()
              << " errno=" << mapped_errno
              << std::endl;
}

bool NormalizeMountPath(const std::string& path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::string out = path;
    std::replace(out.begin(), out.end(), '\\', '/');
    if (out.empty() || out.front() != '/') {
        out.insert(out.begin(), '/');
    }
    std::string collapsed;
    collapsed.reserve(out.size());
    bool prev_slash = false;
    for (char ch : out) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
        } else {
            prev_slash = false;
        }
        collapsed.push_back(ch);
    }
    while (collapsed.size() > 1 && collapsed.back() == '/') {
        collapsed.pop_back();
    }
    *normalized = collapsed.empty() ? "/" : collapsed;
    return true;
}

bool IsValidTierDirName(const std::string& name) {
    return !name.empty() && name.find('/') == std::string::npos && name.find('\\') == std::string::npos;
}

std::string BuildTierRootPath(const std::string& dir_name) {
    return "/" + dir_name;
}

int StatusToErrno(const zb::rpc::Status& status) {
    switch (status.code()) {
        case zb::rpc::STATUS_OK:
            return 0;
        case zb::rpc::STATUS_INVALID_ARGUMENT:
            return EINVAL;
        case zb::rpc::STATUS_NOT_FOUND:
            return ENOENT;
        case zb::rpc::STATUS_IO_ERROR:
            if (status.message() == "VERSION_MISMATCH") {
                return EAGAIN;
            }
            return EIO;
        case zb::rpc::STATUS_INTERNAL_ERROR:
        default:
            return EIO;
    }
}

void FillStat(const zb::rpc::InodeAttr& attr, struct stat* st) {
    std::memset(st, 0, sizeof(*st));
    st->st_ino = static_cast<ino_t>(attr.inode_id());
    st->st_mode = static_cast<mode_t>(attr.mode());
    if (attr.type() == zb::rpc::INODE_DIR) {
        st->st_mode |= S_IFDIR;
    } else {
        st->st_mode |= S_IFREG;
    }
    st->st_uid = static_cast<uid_t>(attr.uid());
    st->st_gid = static_cast<gid_t>(attr.gid());
    st->st_nlink = attr.nlink() ? static_cast<nlink_t>(attr.nlink()) : 1;
    st->st_size = static_cast<off_t>(attr.size());
    st->st_atim.tv_sec = static_cast<time_t>(attr.atime());
    st->st_mtim.tv_sec = static_cast<time_t>(attr.mtime());
    st->st_ctim.tv_sec = static_cast<time_t>(attr.ctime());
    st->st_blksize = static_cast<blksize_t>(attr.blksize() ? attr.blksize() : 4096U);
    st->st_blocks = static_cast<blkcnt_t>(attr.blocks_512());
    st->st_rdev = static_cast<dev_t>((static_cast<uint64_t>(attr.rdev_major()) << 32U) |
                                     static_cast<uint64_t>(attr.rdev_minor()));
}

std::string ReplicaObjectId(const zb::rpc::ReplicaLocation& replica) {
    return replica.object_id();
}

std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index);

std::string FormatRealNodeId(uint64_t numeric_id) {
    std::ostringstream oss;
    oss << "node-real-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

std::string FormatVirtualNodeId(uint64_t numeric_id) {
    return "vpool-v" + std::to_string(numeric_id);
}

std::string FormatDiskIdForTier(uint32_t numeric_id, bool is_virtual) {
    if (is_virtual) {
        return "disk" + std::to_string(numeric_id);
    }
    std::ostringstream oss;
    oss << "disk-" << std::setw(2) << std::setfill('0') << numeric_id;
    return oss.str();
}

std::string FormatOpticalNodeId(uint64_t numeric_id) {
    return "optical-node-" + std::to_string(numeric_id);
}

std::string FormatOpticalDiskId(uint64_t numeric_id) {
    return "disk-" + std::to_string(numeric_id);
}

std::string FormatOpticalImageId(uint64_t node_id, uint64_t disk_id, uint32_t image_id) {
    return "img-" + std::to_string(node_id) + "-" + std::to_string(disk_id) + "-" + std::to_string(image_id);
}

bool HasDiskLocationInAttr(const zb::rpc::InodeAttr& attr) {
    return attr.storage_tier() == zb::rpc::INODE_STORAGE_DISK && attr.disk_node_id() != 0;
}

bool HasOpticalLocationInAttr(const zb::rpc::InodeAttr& attr) {
    return attr.storage_tier() == zb::rpc::INODE_STORAGE_OPTICAL && attr.optical_node_id() != 0;
}

bool HasLegacyDiskLocation(const zb::rpc::FileLocationView& view) {
    return !view.disk_location().node_id().empty() && !view.disk_location().disk_id().empty();
}

bool HasLegacyOpticalLocation(const zb::rpc::FileLocationView& view) {
    return !view.optical_location().node_id().empty() && !view.optical_location().disk_id().empty();
}

bool IsWriteOpenFlags(uint32_t flags) {
    const int access_mode = static_cast<int>(flags) & O_ACCMODE;
    if (access_mode == O_WRONLY || access_mode == O_RDWR) {
        return true;
    }
#ifdef O_TRUNC
    if ((flags & static_cast<uint32_t>(O_TRUNC)) != 0U) {
        return true;
    }
#endif
#ifdef O_APPEND
    if ((flags & static_cast<uint32_t>(O_APPEND)) != 0U) {
        return true;
    }
#endif
    return false;
}

zb::rpc::ReplicaLocation ToReplicaLocation(const zb::rpc::FileLocationView& view,
                                           const zb::rpc::InodeAttr& attr) {
    zb::rpc::ReplicaLocation replica;
    if (attr.storage_tier() == zb::rpc::INODE_STORAGE_DISK) {
        const bool is_virtual = attr.disk_node_is_virtual();
        replica.set_node_id(is_virtual ? FormatVirtualNodeId(attr.disk_node_id())
                                       : FormatRealNodeId(attr.disk_node_id()));
        replica.set_disk_id(FormatDiskIdForTier(attr.disk_id(), is_virtual));
        replica.set_object_id(BuildStableObjectId(view.attr().inode_id(), 0));
        replica.set_size(view.attr().size());
        replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        replica.set_replica_state(zb::rpc::REPLICA_READY);
        return replica;
    }
    if (attr.storage_tier() == zb::rpc::INODE_STORAGE_OPTICAL) {
        replica.set_node_id(FormatOpticalNodeId(attr.optical_node_id()));
        replica.set_disk_id(FormatOpticalDiskId(attr.optical_disk_id()));
        replica.set_object_id(BuildStableObjectId(view.attr().inode_id(), 0));
        replica.set_size(view.attr().size());
        replica.set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
        replica.set_replica_state(zb::rpc::REPLICA_READY);
        replica.set_image_id(FormatOpticalImageId(attr.optical_node_id(),
                                                  attr.optical_disk_id(),
                                                  attr.optical_image_id()));
        return replica;
    }
    return replica;
}

zb::rpc::ReplicaLocation ToReplicaLocation(const zb::rpc::FileLocationView& view,
                                           const zb::rpc::DiskFileLocation& location) {
    zb::rpc::ReplicaLocation replica;
    replica.set_node_id(location.node_id());
    replica.set_node_address(location.node_address());
    replica.set_disk_id(location.disk_id());
    replica.set_object_id(BuildStableObjectId(view.attr().inode_id(), 0));
    replica.set_size(view.attr().size());
    replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    return replica;
}

zb::rpc::ReplicaLocation ToReplicaLocation(const zb::rpc::FileLocationView& view,
                                           const zb::rpc::OpticalFileLocation& location) {
    zb::rpc::ReplicaLocation replica;
    replica.set_node_id(location.node_id());
    replica.set_node_address(location.node_address());
    replica.set_disk_id(location.disk_id());
    replica.set_object_id(location.file_id().empty() ? BuildStableObjectId(view.attr().inode_id(), 0)
                                                     : location.file_id());
    replica.set_size(view.attr().size());
    replica.set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    replica.set_image_id(location.image_id());
    return replica;
}

bool HasOpticalLocation(const zb::rpc::FileLocationView& view) {
    return HasOpticalLocationInAttr(view.attr()) || HasLegacyOpticalLocation(view);
}

bool IsArchivedReadOnly(const zb::rpc::FileLocationView& view) {
    return view.attr().file_archive_state() == zb::rpc::INODE_ARCHIVE_ARCHIVED;
}

bool SelectLocationForIo(const zb::rpc::FileLocationView& view, zb::rpc::ReplicaLocation* anchor) {
    if (!anchor) {
        return false;
    }
    if (HasDiskLocationInAttr(view.attr()) || HasOpticalLocationInAttr(view.attr())) {
        *anchor = ToReplicaLocation(view, view.attr());
        return !anchor->node_id().empty() && !anchor->disk_id().empty();
    }
    if (HasLegacyDiskLocation(view)) {
        *anchor = ToReplicaLocation(view, view.disk_location());
        return true;
    }
    if (HasLegacyOpticalLocation(view)) {
        *anchor = ToReplicaLocation(view, view.optical_location());
        return true;
    }
    return false;
}

std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

std::string BaseNodeIdFromVirtual(const std::string& node_id) {
    const size_t pos = node_id.rfind("-v");
    if (pos == std::string::npos || pos + 2 >= node_id.size()) {
        return node_id;
    }
    for (size_t i = pos + 2; i < node_id.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(node_id[i]))) {
            return node_id;
        }
    }
    return node_id.substr(0, pos);
}

zb::rpc::ReplicaLocation BuildObjectReplicaFromAnchor(const zb::rpc::ReplicaLocation& anchor,
                                                      uint64_t inode_id,
                                                      uint32_t object_index) {
    zb::rpc::ReplicaLocation replica = anchor;
    replica.set_object_id(BuildStableObjectId(inode_id, object_index));
    if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL) {
        replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    }
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    return replica;
}

zb::rpc::ReplicaLocation BuildObjectReplicaFromAnchor(const zb::rpc::ReplicaLocation& anchor,
                                                      const std::string& object_id) {
    zb::rpc::ReplicaLocation replica = anchor;
    replica.set_object_id(object_id);
    if (replica.storage_tier() != zb::rpc::STORAGE_TIER_OPTICAL) {
        replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    }
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    return replica;
}

void BuildObjectSlices(uint64_t inode_id,
                       uint64_t file_offset,
                       uint64_t length,
                       uint64_t object_unit_size,
                       const std::string& disk_id,
                       std::vector<zb::rpc::FileObjectSlice>* slices) {
    if (!slices || object_unit_size == 0 || length == 0) {
        return;
    }
    const uint64_t start_index = file_offset / object_unit_size;
    const uint64_t end_index = (file_offset + length - 1) / object_unit_size;
    slices->reserve(slices->size() + static_cast<size_t>(end_index - start_index + 1));
    for (uint64_t index = start_index; index <= end_index; ++index) {
        const uint64_t object_begin = index * object_unit_size;
        const uint64_t object_end = object_begin + object_unit_size;
        const uint64_t slice_begin = std::max<uint64_t>(file_offset, object_begin);
        const uint64_t slice_end = std::min<uint64_t>(file_offset + length, object_end);
        if (slice_begin >= slice_end) {
            continue;
        }
        zb::rpc::FileObjectSlice slice;
        slice.set_object_index(static_cast<uint32_t>(index));
        slice.set_object_id(BuildStableObjectId(inode_id, static_cast<uint32_t>(index)));
        slice.set_disk_id(disk_id);
        slice.set_object_offset(slice_begin - object_begin);
        slice.set_length(slice_end - slice_begin);
        slices->push_back(std::move(slice));
    }
}

class MdsClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool Lookup(const char* path, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status, IoTraceContext* trace = nullptr) {
        zb::rpc::LookupRequest request;
        request.set_path(path);
        zb::rpc::LookupReply reply;
        brpc::Controller cntl;
        {
            RpcLatencyScope timing(trace, RpcTarget::Mds);
            stub_.Lookup(&cntl, &request, &reply, nullptr);
        }
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Getattr(uint64_t inode_id, zb::rpc::InodeAttr* attr, zb::rpc::MdsStatus* status, IoTraceContext* trace = nullptr) {
        zb::rpc::GetattrRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetattrReply reply;
        brpc::Controller cntl;
        {
            RpcLatencyScope timing(trace, RpcTarget::Mds);
            stub_.Getattr(&cntl, &request, &reply, nullptr);
        }
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Open(const char* path,
              uint32_t flags,
              uint64_t* handle,
              zb::rpc::FileLocationView* location,
              zb::rpc::ReplicaLocation* selected_location,
              zb::rpc::MdsStatus* status) {
        zb::rpc::OpenRequest request;
        request.set_path(path);
        request.set_flags(flags);
        zb::rpc::OpenReply reply;
        brpc::Controller cntl;
        stub_.Open(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (handle) {
            *handle = reply.handle_id();
        }
        if (location) {
            *location = reply.location();
        }
        if (selected_location) {
            if (!SelectLocationForIo(reply.location(), selected_location)) {
                if (status) {
                    status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                    status->set_message("Open returned empty file location");
                }
                return false;
            }
        }
        return true;
    }

    bool Close(uint64_t handle_id, zb::rpc::MdsStatus* status) {
        zb::rpc::CloseRequest request;
        request.set_handle_id(handle_id);
        zb::rpc::CloseReply reply;
        brpc::Controller cntl;
        stub_.Close(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Create(const char* path,
                uint32_t mode,
                uint32_t uid,
                uint32_t gid,
                zb::rpc::FileLocationView* location,
                zb::rpc::ReplicaLocation* selected_location,
                zb::rpc::MdsStatus* status) {
        zb::rpc::CreateRequest request;
        request.set_path(path);
        request.set_mode(mode);
        request.set_uid(uid);
        request.set_gid(gid);
        request.set_replica(FLAGS_default_replica);
        request.set_object_unit_size(FLAGS_default_object_unit_size);
        zb::rpc::CreateReply reply;
        brpc::Controller cntl;
        stub_.Create(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (location) {
            *location = reply.location();
        }
        if (selected_location) {
            if (!SelectLocationForIo(reply.location(), selected_location)) {
                if (status) {
                    status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                    status->set_message("Create returned empty file location");
                }
                return false;
            }
        }
        return true;
    }

    bool Mkdir(const char* path, uint32_t mode, uint32_t uid, uint32_t gid, zb::rpc::InodeAttr* attr,
               zb::rpc::MdsStatus* status) {
        zb::rpc::MkdirRequest request;
        request.set_path(path);
        request.set_mode(mode);
        request.set_uid(uid);
        request.set_gid(gid);
        zb::rpc::MkdirReply reply;
        brpc::Controller cntl;
        stub_.Mkdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool Readdir(const char* path, std::vector<zb::rpc::Dentry>* entries, zb::rpc::MdsStatus* status) {
        zb::rpc::ReaddirRequest request;
        request.set_path(path);
        zb::rpc::ReaddirReply reply;
        brpc::Controller cntl;
        stub_.Readdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (entries) {
            entries->assign(reply.entries().begin(), reply.entries().end());
        }
        return true;
    }

    bool Rename(const char* old_path, const char* new_path, zb::rpc::MdsStatus* status) {
        zb::rpc::RenameRequest request;
        request.set_old_path(old_path);
        request.set_new_path(new_path);
        zb::rpc::RenameReply reply;
        brpc::Controller cntl;
        stub_.Rename(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Unlink(const char* path, zb::rpc::MdsStatus* status) {
        zb::rpc::UnlinkRequest request;
        request.set_path(path);
        zb::rpc::UnlinkReply reply;
        brpc::Controller cntl;
        stub_.Unlink(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool Rmdir(const char* path, zb::rpc::MdsStatus* status) {
        zb::rpc::RmdirRequest request;
        request.set_path(path);
        zb::rpc::RmdirReply reply;
        brpc::Controller cntl;
        stub_.Rmdir(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (status) {
                status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                status->set_message(cntl.ErrorText());
            }
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

    bool GetFileLocationView(uint64_t inode_id,
                          zb::rpc::FileLocationView* location,
                          zb::rpc::MdsStatus* status, IoTraceContext* trace = nullptr) {
        zb::rpc::GetFileLocationRequest request;
        request.set_inode_id(inode_id);
        zb::rpc::GetFileLocationReply reply;
        brpc::Controller cntl;
        {
            RpcLatencyScope timing(trace, RpcTarget::Mds);
            stub_.GetFileLocation(&cntl, &request, &reply, nullptr);
        }
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (location) {
            *location = reply.location();
        }
        return true;
    }

    bool GetFileLocation(uint64_t inode_id,
                       zb::rpc::ReplicaLocation* anchor,
                       zb::rpc::MdsStatus* status) {
        zb::rpc::FileLocationView location;
        if (!GetFileLocationView(inode_id, &location, status)) {
            return false;
        }
        if (anchor) {
            if (!SelectLocationForIo(location, anchor)) {
                if (status) {
                    status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
                    status->set_message("GetFileLocation returned empty location");
                }
                return false;
            }
        }
        return true;
    }

    bool UpdateInodeStat(uint64_t inode_id,
                         uint64_t file_size,
                         uint64_t object_unit_size,
                         uint64_t version,
                         uint64_t mtime_sec,
                         zb::rpc::InodeAttr* attr,
                         zb::rpc::MdsStatus* status, IoTraceContext* trace = nullptr) {
        zb::rpc::UpdateInodeStatRequest request;
        request.set_inode_id(inode_id);
        request.set_file_size(file_size);
        request.set_object_unit_size(object_unit_size);
        request.set_version(version);
        request.set_mtime(mtime_sec);
        zb::rpc::UpdateInodeStatReply reply;
        brpc::Controller cntl;
        {
            RpcLatencyScope timing(trace, RpcTarget::Mds);
            stub_.UpdateInodeStat(&cntl, &request, &reply, nullptr);
        }
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        if (reply.status().code() != zb::rpc::MDS_OK) {
            return false;
        }
        if (attr) {
            *attr = reply.attr();
        }
        return true;
    }

    bool SetPathPlacementPolicy(const std::string& path_prefix,
                                zb::rpc::PathPlacementTarget target,
                                bool strict,
                                zb::rpc::MdsStatus* status) {
        zb::rpc::SetPathPlacementPolicyRequest request;
        request.mutable_policy()->set_path_prefix(path_prefix);
        request.mutable_policy()->set_target(target);
        request.mutable_policy()->set_strict(strict);
        zb::rpc::SetPathPlacementPolicyReply reply;
        brpc::Controller cntl;
        stub_.SetPathPlacementPolicy(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            SetRpcFailureStatus(cntl, status);
            return false;
        }
        if (status) {
            *status = reply.status();
        }
        return reply.status().code() == zb::rpc::MDS_OK;
    }

private:
    static void SetRpcFailureStatus(const brpc::Controller& cntl, zb::rpc::MdsStatus* status) {
        if (!status) {
            return;
        }
        status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status->set_message(cntl.ErrorText());
    }

    brpc::Channel channel_;
    zb::rpc::MdsService_Stub stub_{&channel_};
};

class SchedulerClient {
public:
    bool Init(const std::string& endpoint) {
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        return channel_.Init(endpoint.c_str(), &options) == 0;
    }

    bool GetClusterView(uint64_t min_generation,
                        std::vector<zb::rpc::NodeView>* nodes,
                        uint64_t* generation,
                        std::string* error) {
        zb::rpc::GetClusterViewRequest request;
        request.set_min_generation(min_generation);
        zb::rpc::GetClusterViewReply reply;
        brpc::Controller cntl;
        stub_.GetClusterView(&cntl, &request, &reply, nullptr);
        if (cntl.Failed()) {
            if (error) {
                *error = cntl.ErrorText();
            }
            return false;
        }
        if (reply.status().code() != zb::rpc::SCHED_OK) {
            if (error) {
                *error = reply.status().message();
            }
            return false;
        }
        if (nodes) {
            nodes->assign(reply.nodes().begin(), reply.nodes().end());
        }
        if (generation) {
            *generation = reply.generation();
        }
        return true;
    }

private:
    brpc::Channel channel_;
    zb::rpc::SchedulerService_Stub stub_{&channel_};
};

class DataNodeClient {
public:
    using NodeAddressResolver = std::function<bool(const std::string&, std::string*)>;

    void SetNodeAddressResolver(NodeAddressResolver resolver) {
        resolver_ = std::move(resolver);
    }

    bool Write(const zb::rpc::ReplicaLocation& replica, uint64_t offset, const std::string& data, std::string* error, IoTraceContext* trace = nullptr) {
        std::vector<std::string> addresses = CollectAddresses(replica, resolver_);
        if (addresses.empty()) {
            if (error) {
                *error = "No data node address in replica";
            }
            return false;
        }

        std::string last_error = "Unknown write error";
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_error = "Failed to init channel: " + address;
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::WriteObjectRequest request;
            request.set_disk_id(replica.disk_id());
            request.set_object_id(ReplicaObjectId(replica));
            request.set_offset(offset);
            request.set_data(data);
            request.set_epoch(replica.epoch());
            zb::rpc::WriteObjectReply reply;
            brpc::Controller cntl;
            {
                RpcLatencyScope timing(trace, RpcTarget::DataNode);
                stub.WriteObject(&cntl, &request, &reply, nullptr);
            }
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                return true;
            }
            last_error = reply.status().message();
        }
        if (error) {
            *error = last_error;
        }
        return false;
    }

    bool Read(const zb::rpc::ReplicaLocation& replica, uint64_t offset, uint64_t size, std::string* out,
              std::string* error, IoTraceContext* trace = nullptr) {
        std::vector<std::string> addresses = CollectAddresses(replica, resolver_);
        if (addresses.empty()) {
            if (error) {
                *error = "No data node address in replica";
            }
            return false;
        }

        std::string last_error = "Unknown read error";
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_error = "Failed to init channel: " + address;
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::ReadObjectRequest request;
            request.set_disk_id(replica.disk_id());
            request.set_object_id(ReplicaObjectId(replica));
            request.set_offset(offset);
            request.set_size(size);
            if (!replica.image_id().empty()) {
                request.set_image_id(replica.image_id());
                request.set_image_offset(replica.image_offset());
                request.set_image_length(replica.image_length());
            }
            zb::rpc::ReadObjectReply reply;
            brpc::Controller cntl;
            {
                RpcLatencyScope timing(trace, RpcTarget::DataNode);
                stub.ReadObject(&cntl, &request, &reply, nullptr);
            }
            if (cntl.Failed()) {
                last_error = cntl.ErrorText();
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                if (out) {
                    *out = reply.data();
                }
                return true;
            }
            last_error = reply.status().message();
        }
        if (error) {
            *error = last_error;
        }
        return false;
    }

    bool ResolveFileRead(const zb::rpc::ReplicaLocation& replica,
                         uint64_t inode_id,
                         uint64_t offset,
                         uint64_t size,
                         uint64_t object_unit_size_hint,
                         zb::rpc::FileMeta* meta,
                         std::vector<zb::rpc::FileObjectSlice>* slices,
                         zb::rpc::Status* status, IoTraceContext* trace = nullptr) {
        std::vector<std::string> addresses = CollectAddresses(replica, resolver_);
        if (addresses.empty()) {
            if (status) {
                status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
                status->set_message("No data node address in replica");
            }
            return false;
        }

        zb::rpc::Status last_status;
        last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
        last_status.set_message("Unknown ResolveFileRead error");
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message("Failed to init channel: " + address);
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::ResolveFileReadRequest request;
            request.set_inode_id(inode_id);
            request.set_offset(offset);
            request.set_size(size);
            request.set_disk_id(replica.disk_id());
            request.set_object_unit_size_hint(object_unit_size_hint);
            zb::rpc::ResolveFileReadReply reply;
            brpc::Controller cntl;
            {
                RpcLatencyScope timing(trace, RpcTarget::DataNode);
                stub.ResolveFileRead(&cntl, &request, &reply, nullptr);
            }
            if (cntl.Failed()) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message(cntl.ErrorText());
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                if (meta) {
                    *meta = reply.meta();
                }
                if (slices) {
                    slices->assign(reply.slices().begin(), reply.slices().end());
                }
                if (status) {
                    *status = reply.status();
                }
                return true;
            }
            last_status = reply.status();
        }
        if (status) {
            *status = last_status;
        }
        return false;
    }

    bool AllocateFileWrite(const zb::rpc::ReplicaLocation& replica,
                           uint64_t inode_id,
                           uint64_t offset,
                           uint64_t size,
                           uint64_t object_unit_size_hint,
                           zb::rpc::FileMeta* meta,
                           std::string* txid,
                           std::vector<zb::rpc::FileObjectSlice>* slices,
                           zb::rpc::Status* status, IoTraceContext* trace = nullptr) {
        std::vector<std::string> addresses = CollectAddresses(replica, resolver_);
        if (addresses.empty()) {
            if (status) {
                status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
                status->set_message("No data node address in replica");
            }
            return false;
        }

        zb::rpc::Status last_status;
        last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
        last_status.set_message("Unknown AllocateFileWrite error");
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message("Failed to init channel: " + address);
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::AllocateFileWriteRequest request;
            request.set_inode_id(inode_id);
            request.set_offset(offset);
            request.set_size(size);
            request.set_disk_id(replica.disk_id());
            request.set_object_unit_size_hint(object_unit_size_hint);
            zb::rpc::AllocateFileWriteReply reply;
            brpc::Controller cntl;
            {
                RpcLatencyScope timing(trace, RpcTarget::DataNode);
                stub.AllocateFileWrite(&cntl, &request, &reply, nullptr);
            }
            if (cntl.Failed()) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message(cntl.ErrorText());
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                if (meta) {
                    *meta = reply.meta();
                }
                if (txid) {
                    *txid = reply.txid();
                }
                if (slices) {
                    slices->assign(reply.slices().begin(), reply.slices().end());
                }
                if (status) {
                    *status = reply.status();
                }
                return true;
            }
            last_status = reply.status();
        }
        if (status) {
            *status = last_status;
        }
        return false;
    }

    bool CommitFileWrite(const zb::rpc::ReplicaLocation& replica,
                         uint64_t inode_id,
                         const std::string& txid,
                         uint64_t file_size,
                         uint64_t object_unit_size,
                         uint64_t expected_version,
                         bool allow_create,
                         uint64_t mtime_sec,
                         zb::rpc::FileMeta* meta,
                         zb::rpc::Status* status, IoTraceContext* trace = nullptr) {
        std::vector<std::string> addresses = CollectAddresses(replica, resolver_);
        if (addresses.empty()) {
            if (status) {
                status->set_code(zb::rpc::STATUS_INVALID_ARGUMENT);
                status->set_message("No data node address in replica");
            }
            return false;
        }

        zb::rpc::Status last_status;
        last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
        last_status.set_message("Unknown CommitFileWrite error");
        for (const auto& address : addresses) {
            brpc::Channel* channel = GetChannel(address);
            if (!channel) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message("Failed to init channel: " + address);
                continue;
            }
            zb::rpc::RealNodeService_Stub stub(channel);
            zb::rpc::CommitFileWriteRequest request;
            request.set_inode_id(inode_id);
            request.set_txid(txid);
            request.set_file_size(file_size);
            request.set_object_unit_size(object_unit_size);
            request.set_expected_version(expected_version);
            request.set_allow_create(allow_create);
            request.set_mtime_sec(mtime_sec);
            zb::rpc::CommitFileWriteReply reply;
            brpc::Controller cntl;
            {
                RpcLatencyScope timing(trace, RpcTarget::DataNode);
                stub.CommitFileWrite(&cntl, &request, &reply, nullptr);
            }
            if (cntl.Failed()) {
                last_status.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
                last_status.set_message(cntl.ErrorText());
                continue;
            }
            if (reply.status().code() == zb::rpc::STATUS_OK) {
                if (meta) {
                    *meta = reply.meta();
                }
                if (status) {
                    *status = reply.status();
                }
                return true;
            }
            last_status = reply.status();
        }
        if (status) {
            *status = last_status;
        }
        return false;
    }

private:
    static std::vector<std::string> CollectAddresses(const zb::rpc::ReplicaLocation& replica,
                                                     const NodeAddressResolver& resolver) {
        std::vector<std::string> addresses;
        auto push_unique = [&addresses](const std::string& address) {
            if (address.empty()) {
                return;
            }
            if (std::find(addresses.begin(), addresses.end(), address) == addresses.end()) {
                addresses.push_back(address);
            }
        };
        auto resolve_and_push = [&resolver, &push_unique](const std::string& node_id) {
            if (!resolver || node_id.empty()) {
                return;
            }
            std::string address;
            if (resolver(node_id, &address)) {
                push_unique(address);
            }
        };
        resolve_and_push(replica.primary_node_id());
        resolve_and_push(replica.node_id());
        resolve_and_push(replica.secondary_node_id());

        return addresses;
    }

    brpc::Channel* GetChannel(const std::string& address) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = channels_.find(address);
        if (it != channels_.end()) {
            return it->second.get();
        }
        auto channel = std::make_unique<brpc::Channel>();
        brpc::ChannelOptions options;
        options.protocol = "baidu_std";
        options.timeout_ms = FLAGS_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        if (channel->Init(address.c_str(), &options) != 0) {
            return nullptr;
        }
        brpc::Channel* ptr = channel.get();
        channels_[address] = std::move(channel);
        return ptr;
    }

    std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
    NodeAddressResolver resolver_;
};

struct FuseState {
    struct InodeCacheEntry {
        uint64_t inode_id{0};
        uint64_t object_unit_size{0};
        uint64_t file_size{0};
        uint64_t file_meta_version{0};
        zb::rpc::ReplicaLocation anchor;
        bool has_anchor{false};
    };

    std::unique_ptr<metrics::LatencyRecorder> latency;
    bool latency_start_failed{false};
    MdsClient mds;
    SchedulerClient scheduler;
    bool scheduler_enabled{false};
    DataNodeClient data_nodes;
    std::mutex mu;
    std::unordered_map<uint64_t, uint64_t> handle_to_inode;
    std::unordered_map<uint64_t, InodeCacheEntry> inode_cache;
    std::unordered_map<std::string, std::string> node_address_cache;
    uint64_t cluster_view_generation{0};
    uint64_t cluster_view_update_ms{0};
};

FuseState* GetState() {
    return static_cast<FuseState*>(fuse_get_context()->private_data);
}

uint64_t NowMilliseconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count());
}

bool RefreshNodeAddressCache(FuseState* state, bool force, std::string* error) {
    if (!state || !state->scheduler_enabled) {
        return false;
    }
    const uint64_t now_ms = NowMilliseconds();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        const uint64_t refresh_ms = std::max<uint64_t>(1, FLAGS_cluster_view_refresh_ms);
        if (!force && state->cluster_view_update_ms > 0 &&
            now_ms - state->cluster_view_update_ms < refresh_ms) {
            return !state->node_address_cache.empty();
        }
    }

    std::vector<zb::rpc::NodeView> nodes;
    uint64_t generation = 0;
    std::string local_error;
    if (!state->scheduler.GetClusterView(0, &nodes, &generation, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "GetClusterView failed" : local_error;
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->node_address_cache.clear();
        for (const auto& node : nodes) {
            if (node.node_id().empty() || node.address().empty()) {
                continue;
            }
            state->node_address_cache[node.node_id()] = node.address();
        }
        state->cluster_view_generation = generation;
        state->cluster_view_update_ms = now_ms;
    }
    return true;
}

bool EnsureBootstrapDirectory(FuseState* state, const std::string& path, std::string* error) {
    if (!state) {
        if (error) {
            *error = "invalid fuse state";
        }
        return false;
    }
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (state->mds.Lookup(path.c_str(), &attr, &status)) {
        if (attr.type() != zb::rpc::INODE_DIR) {
            if (error) {
                *error = "bootstrap path exists and is not a directory: " + path;
            }
            return false;
        }
        return true;
    }
    if (status.code() != zb::rpc::MDS_NOT_FOUND) {
        if (error) {
            *error = status.message().empty() ? "Lookup failed for " + path : status.message();
        }
        return false;
    }
    if (!state->mds.Mkdir(path.c_str(),
                          0755,
                          static_cast<uint32_t>(::getuid()),
                          static_cast<uint32_t>(::getgid()),
                          &attr,
                          &status)) {
        if (status.code() == zb::rpc::MDS_ALREADY_EXISTS) {
            return true;
        }
        if (error) {
            *error = status.message().empty() ? "Mkdir failed for " + path : status.message();
        }
        return false;
    }
    return true;
}

bool EnsureTierBootstrap(FuseState* state, std::string* error) {
    if (!state) {
        if (error) {
            *error = "invalid fuse state";
        }
        return false;
    }
    if (!IsValidTierDirName(FLAGS_real_dir_name) || !IsValidTierDirName(FLAGS_virtual_dir_name)) {
        if (error) {
            *error = "tier directory names must be non-empty and cannot contain path separators";
        }
        return false;
    }
    if (FLAGS_real_dir_name == FLAGS_virtual_dir_name) {
        if (error) {
            *error = "real_dir_name and virtual_dir_name must be different";
        }
        return false;
    }

    const std::string real_path = BuildTierRootPath(FLAGS_real_dir_name);
    const std::string virtual_path = BuildTierRootPath(FLAGS_virtual_dir_name);
    if (!EnsureBootstrapDirectory(state, real_path, error)) {
        return false;
    }
    if (!EnsureBootstrapDirectory(state, virtual_path, error)) {
        return false;
    }

    zb::rpc::MdsStatus status;
    if (!state->mds.SetPathPlacementPolicy(real_path,
                                           zb::rpc::PATH_PLACEMENT_REAL_ONLY,
                                           true,
                                           &status)) {
        if (error) {
            *error = status.message().empty() ? "failed to configure real tier policy" : status.message();
        }
        return false;
    }
    if (!state->mds.SetPathPlacementPolicy(virtual_path,
                                           zb::rpc::PATH_PLACEMENT_VIRTUAL_ONLY,
                                           true,
                                           &status)) {
        if (error) {
            *error = status.message().empty() ? "failed to configure virtual tier policy" : status.message();
        }
        return false;
    }
    return true;
}

bool ResolveNodeAddress(FuseState* state, const std::string& node_id, std::string* address) {
    if (!state || node_id.empty() || !address) {
        return false;
    }
    const std::string base_node_id = BaseNodeIdFromVirtual(node_id);
    const uint64_t now_ms = NowMilliseconds();
    const uint64_t ttl_ms = std::max<uint64_t>(1, FLAGS_cluster_view_ttl_ms);
    {
        std::lock_guard<std::mutex> lock(state->mu);
        auto it = state->node_address_cache.find(node_id);
        if (it == state->node_address_cache.end() && base_node_id != node_id) {
            it = state->node_address_cache.find(base_node_id);
        }
        if (it != state->node_address_cache.end() &&
            state->cluster_view_update_ms > 0 &&
            now_ms - state->cluster_view_update_ms <= ttl_ms) {
            *address = it->second;
            return true;
        }
    }

    std::string error;
    if (!RefreshNodeAddressCache(state, true, &error)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto it = state->node_address_cache.find(node_id);
    if (it == state->node_address_cache.end() && base_node_id != node_id) {
        it = state->node_address_cache.find(base_node_id);
    }
    if (it == state->node_address_cache.end()) {
        return false;
    }
    *address = it->second;
    return true;
}

bool EnsureReplicaNodeAddress(FuseState* state, zb::rpc::ReplicaLocation* replica, zb::rpc::MdsStatus* status) {
    if (!replica) {
        if (status) {
            status->set_code(zb::rpc::MDS_INVALID_ARGUMENT);
            status->set_message("replica is null");
        }
        return false;
    }
    if (!replica->node_address().empty()) {
        return true;
    }
    std::string address;
    if (!ResolveNodeAddress(state, replica->node_id(), &address) || address.empty()) {
        if (status) {
            status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            status->set_message("failed to resolve node address for anchor");
        }
        return false;
    }
    replica->set_node_address(address);
    return true;
}

void InvalidateInodeCache(FuseState* state, uint64_t inode_id) {
    if (!state || inode_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    state->inode_cache.erase(inode_id);
}

void UpdateInodeAttrCache(FuseState* state,
                          uint64_t inode_id,
                          const zb::rpc::InodeAttr& attr) {
    if (!state || inode_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto& entry = state->inode_cache[inode_id];
    entry.inode_id = inode_id;
    if (attr.object_unit_size() > 0) {
        entry.object_unit_size = attr.object_unit_size();
    }
    entry.file_size = attr.size();
}

void UpdateInodeAnchorCache(FuseState* state,
                            uint64_t inode_id,
                            const zb::rpc::ReplicaLocation& anchor) {
    if (!state || inode_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto& entry = state->inode_cache[inode_id];
    entry.inode_id = inode_id;
    entry.anchor = anchor;
    entry.has_anchor = true;
}

bool TryGetCachedAnchor(FuseState* state,
                        uint64_t inode_id,
                        zb::rpc::ReplicaLocation* anchor) {
    if (!state || inode_id == 0 || !anchor) {
        return false;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto it = state->inode_cache.find(inode_id);
    if (it == state->inode_cache.end()) {
        return false;
    }
    const auto& entry = it->second;
    if (!entry.has_anchor) {
        return false;
    }
    *anchor = entry.anchor;
    return true;
}

void UpdateCachedFileMeta(FuseState* state,
                          uint64_t inode_id,
                          uint64_t file_size,
                          uint64_t object_unit_size,
                          uint64_t version) {
    if (!state || inode_id == 0) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto& entry = state->inode_cache[inode_id];
    entry.inode_id = inode_id;
    entry.file_size = file_size;
    if (object_unit_size > 0) {
        entry.object_unit_size = object_unit_size;
    }
    entry.file_meta_version = version;
}

bool TryGetCachedInodeMeta(FuseState* state,
                           uint64_t inode_id,
                           uint64_t* file_size,
                           uint64_t* object_unit_size,
                           uint64_t* file_meta_version) {
    if (!state || inode_id == 0 || !file_size || !object_unit_size) {
        return false;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto it = state->inode_cache.find(inode_id);
    if (it == state->inode_cache.end()) {
        return false;
    }
    const auto& entry = it->second;
    if (entry.object_unit_size == 0) {
        return false;
    }
    *file_size = entry.file_size;
    *object_unit_size = entry.object_unit_size;
    if (file_meta_version) {
        *file_meta_version = entry.file_meta_version;
    }
    return true;
}

void GetCachedInodeMetaHint(FuseState* state,
                            uint64_t inode_id,
                            uint64_t* file_size,
                            uint64_t* object_unit_size) {
    if (!state || inode_id == 0 || !file_size || !object_unit_size) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    auto it = state->inode_cache.find(inode_id);
    if (it == state->inode_cache.end()) {
        return;
    }
    *file_size = it->second.file_size;
    if (it->second.object_unit_size > 0) {
        *object_unit_size = it->second.object_unit_size;
    }
}

bool GetOrFetchAnchor(FuseState* state,
                      uint64_t inode_id,
                      zb::rpc::ReplicaLocation* anchor,
                      zb::rpc::MdsStatus* status) {
    if (!state || inode_id == 0 || !anchor) {
        return false;
    }
    if (TryGetCachedAnchor(state, inode_id, anchor)) {
        return true;
    }
    if (!state->mds.GetFileLocation(inode_id, anchor, status)) {
        return false;
    }
    if (anchor->node_id().empty() || anchor->disk_id().empty()) {
        if (status) {
            status->set_code(zb::rpc::MDS_INTERNAL_ERROR);
            status->set_message("empty anchor node_id/disk_id");
        }
        return false;
    }
    if (!EnsureReplicaNodeAddress(state, anchor, status)) {
        return false;
    }
    UpdateInodeAnchorCache(state, inode_id, *anchor);
    return true;
}

bool GetOrFetchNodeFileMeta(FuseState* state,
                            uint64_t inode_id,
                            const zb::rpc::ReplicaLocation& anchor,
                            bool allow_create,
                            uint64_t hint_file_size,
                            uint64_t hint_object_unit_size,
                            uint64_t* file_size,
                            uint64_t* object_unit_size,
                            uint64_t* file_meta_version,
                            int* out_errno) {
    if (!state || inode_id == 0 || !file_size || !object_unit_size || !file_meta_version) {
        if (out_errno) {
            *out_errno = EINVAL;
        }
        return false;
    }
    if (TryGetCachedInodeMeta(state, inode_id, file_size, object_unit_size, file_meta_version)) {
        return true;
    }

    zb::rpc::Status data_status;
    zb::rpc::FileMeta meta;
    std::vector<zb::rpc::FileObjectSlice> ignored_slices;
    if (!state->data_nodes.ResolveFileRead(anchor,
                                           inode_id,
                                           0,
                                           0,
                                           hint_object_unit_size > 0 ? hint_object_unit_size : FLAGS_default_object_unit_size,
                                           &meta,
                                           &ignored_slices,
                                           &data_status)) {
        if (data_status.code() == zb::rpc::STATUS_NOT_FOUND && allow_create) {
            const uint64_t create_object_unit_size =
                hint_object_unit_size > 0 ? hint_object_unit_size : FLAGS_default_object_unit_size;
            zb::rpc::FileMeta committed;
            const std::string txid = "bootstrap-" + std::to_string(inode_id) + "-" +
                                     std::to_string(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch()).count()));
            if (!state->data_nodes.CommitFileWrite(anchor,
                                                   inode_id,
                                                   txid,
                                                   hint_file_size,
                                                   create_object_unit_size,
                                                   0,
                                                   true,
                                                   static_cast<uint64_t>(std::time(nullptr)),
                                                   &committed,
                                                   &data_status)) {
                if (out_errno) {
                    *out_errno = StatusToErrno(data_status);
                }
                return false;
            }
            meta = committed;
        } else {
            if (out_errno) {
                *out_errno = StatusToErrno(data_status);
            }
            return false;
        }
    }

    const uint64_t effective_object_unit_size =
        meta.object_unit_size() > 0 ? meta.object_unit_size()
                                    : (hint_object_unit_size > 0 ? hint_object_unit_size : FLAGS_default_object_unit_size);
    UpdateCachedFileMeta(state, inode_id, meta.file_size(), effective_object_unit_size, meta.version());
    *file_size = meta.file_size();
    *object_unit_size = effective_object_unit_size;
    *file_meta_version = meta.version();
    return true;
}

bool CommitNodeFileMeta(FuseState* state,
                        uint64_t inode_id,
                        const zb::rpc::ReplicaLocation& anchor,
                        uint64_t file_size,
                        uint64_t object_unit_size,
                        uint64_t expected_version,
                        uint64_t* committed_version,
                        int* out_errno) {
    if (!state || inode_id == 0 || object_unit_size == 0) {
        if (out_errno) {
            *out_errno = EINVAL;
        }
        return false;
    }
    zb::rpc::Status data_status;
    zb::rpc::FileMeta committed;
    const std::string txid = "meta-" + std::to_string(inode_id) + "-" +
                             std::to_string(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch()).count()));
    if (!state->data_nodes.CommitFileWrite(anchor,
                                           inode_id,
                                           txid,
                                           file_size,
                                           object_unit_size,
                                           expected_version,
                                           true,
                                           static_cast<uint64_t>(std::time(nullptr)),
                                           &committed,
                                           &data_status)) {
        if (out_errno) {
            *out_errno = StatusToErrno(data_status);
        }
        return false;
    }
    const uint64_t new_version = committed.version();
    const uint64_t committed_object_unit_size =
        committed.object_unit_size() > 0 ? committed.object_unit_size() : object_unit_size;
    UpdateCachedFileMeta(state, inode_id, committed.file_size(), committed_object_unit_size, new_version);
    zb::rpc::MdsStatus mds_status;
    zb::rpc::InodeAttr updated_attr;
    if (state->mds.UpdateInodeStat(inode_id,
                                   committed.file_size(),
                                   committed_object_unit_size,
                                   new_version,
                                   committed.mtime_sec(),
                                   &updated_attr,
                                   &mds_status)) {
        UpdateInodeAttrCache(state, inode_id, updated_attr);
    }
    if (committed_version) {
        *committed_version = new_version;
    }
    return true;
}

int FuseGetattr(const char* path, struct stat* st, struct fuse_file_info* fi) {
    (void)fi;
    auto* state = GetState();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (!state->mds.Lookup(path, &attr, &status)) {
        return -StatusToErrno(status);
    }
    if (attr.type() == zb::rpc::INODE_FILE) {
        UpdateInodeAttrCache(state, attr.inode_id(), attr);
        zb::rpc::ReplicaLocation anchor;
        if (!GetOrFetchAnchor(state, attr.inode_id(), &anchor, &status)) {
            return -StatusToErrno(status);
        }
        uint64_t file_size = attr.size();
        uint64_t object_unit_size = attr.object_unit_size() > 0
                                        ? attr.object_unit_size()
                                        : FLAGS_default_object_unit_size;
        uint64_t file_meta_version = 0;
        int node_errno = EIO;
        if (!GetOrFetchNodeFileMeta(state,
                                    attr.inode_id(),
                                    anchor,
                                    true,
                                    attr.size(),
                                    object_unit_size,
                                    &file_size,
                                    &object_unit_size,
                                    &file_meta_version,
                                    &node_errno)) {
            return -node_errno;
        }
        attr.set_size(file_size);
        if (object_unit_size > 0) {
            attr.set_object_unit_size(object_unit_size);
        }
    }
    FillStat(attr, st);
    return 0;
}

int FuseReaddir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset,
                struct fuse_file_info* fi, enum fuse_readdir_flags flags) {
    (void)offset;
    (void)fi;
    (void)flags;
    auto* state = GetState();
    std::vector<zb::rpc::Dentry> entries;
    zb::rpc::MdsStatus status;
    if (!state->mds.Readdir(path, &entries, &status)) {
        return -StatusToErrno(status);
    }
    filler(buf, ".", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", nullptr, 0, FUSE_FILL_DIR_PLUS);
    for (const auto& entry : entries) {
        struct stat st;
        std::memset(&st, 0, sizeof(st));
        st.st_ino = static_cast<ino_t>(entry.inode_id());
        st.st_mode = (entry.type() == zb::rpc::INODE_DIR) ? S_IFDIR : S_IFREG;
        filler(buf, entry.name().c_str(), &st, 0, FUSE_FILL_DIR_PLUS);
    }
    return 0;
}

int FuseOpen(const char* path, struct fuse_file_info* fi) {
    auto* state = GetState();
    zb::rpc::FileLocationView location;
    zb::rpc::ReplicaLocation open_anchor;
    zb::rpc::MdsStatus status;
    uint64_t handle_id = 0;
    if (!state->mds.Open(path, static_cast<uint32_t>(fi->flags), &handle_id, &location, &open_anchor, &status)) {
        LogMdsFailure("Open", path, status);
        return -StatusToErrno(status);
    }
    const zb::rpc::InodeAttr& attr = location.attr();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode[handle_id] = attr.inode_id();
    }
    UpdateInodeAttrCache(state, attr.inode_id(), attr);
    if (attr.type() == zb::rpc::INODE_FILE) {
        zb::rpc::ReplicaLocation anchor = open_anchor;
        if (anchor.node_id().empty() || anchor.disk_id().empty()) {
            if (!GetOrFetchAnchor(state, attr.inode_id(), &anchor, &status)) {
                LogMdsFailure("Open.GetOrFetchAnchor", path, status);
                return -StatusToErrno(status);
            }
        } else if (!EnsureReplicaNodeAddress(state, &anchor, &status)) {
            LogMdsFailure("Open.ResolveAnchorAddress", path, status);
            return -StatusToErrno(status);
        } else {
            UpdateInodeAnchorCache(state, attr.inode_id(), anchor);
        }
        if (anchor.node_id().empty() || anchor.disk_id().empty()) {
            return -StatusToErrno(status);
        }
        if (anchor.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL) {
            if (IsWriteOpenFlags(static_cast<uint32_t>(fi->flags))) {
                return -EROFS;
            }
            fi->fh = handle_id;
            return 0;
        }
        uint64_t file_size = attr.size();
        uint64_t object_unit_size = attr.object_unit_size() > 0
                                        ? attr.object_unit_size()
                                        : FLAGS_default_object_unit_size;
        uint64_t file_meta_version = 0;
        int node_errno = EIO;
        if (!GetOrFetchNodeFileMeta(state,
                                    attr.inode_id(),
                                    anchor,
                                    true,
                                    attr.size(),
                                    object_unit_size,
                                    &file_size,
                                    &object_unit_size,
                                    &file_meta_version,
                                    &node_errno)) {
            zb::rpc::Status st;
            st.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
            st.set_message(std::strerror(node_errno));
            LogDataFailure("Open.GetOrFetchNodeFileMeta", path, attr.inode_id(), anchor, st);
            return -node_errno;
        }
    }
    fi->fh = handle_id;
    return 0;
}

int FuseRelease(const char* path, struct fuse_file_info* fi) {
    (void)path;
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t handle_id = fi->fh;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode.erase(handle_id);
    }
    if (!state->mds.Close(handle_id, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseCreate(const char* path, mode_t mode, struct fuse_file_info* fi) {
    auto* state = GetState();
    struct fuse_context* ctx = fuse_get_context();
    zb::rpc::FileLocationView create_location;
    zb::rpc::ReplicaLocation create_anchor;
    zb::rpc::MdsStatus status;
    if (!state->mds.Create(path, static_cast<uint32_t>(mode), ctx->uid, ctx->gid, &create_location, &create_anchor, &status)) {
        LogMdsFailure("Create", path, status);
        return -StatusToErrno(status);
    }
    uint64_t handle_id = 0;
    zb::rpc::FileLocationView open_location;
    zb::rpc::ReplicaLocation open_anchor;
    if (!state->mds.Open(path, static_cast<uint32_t>(fi->flags), &handle_id, &open_location, &open_anchor, &status)) {
        LogMdsFailure("Create.OpenAfterCreate", path, status);
        return -StatusToErrno(status);
    }
    const zb::rpc::InodeAttr& attr = open_location.attr();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->handle_to_inode[handle_id] = attr.inode_id();
    }
    UpdateInodeAttrCache(state, attr.inode_id(), attr);
    zb::rpc::ReplicaLocation anchor = open_anchor;
    if (anchor.node_id().empty() || anchor.disk_id().empty()) {
        anchor = create_anchor;
    }
    if (anchor.node_id().empty() || anchor.disk_id().empty()) {
        if (!GetOrFetchAnchor(state, attr.inode_id(), &anchor, &status)) {
            LogMdsFailure("Create.GetOrFetchAnchor", path, status);
            return -StatusToErrno(status);
        }
    } else if (!EnsureReplicaNodeAddress(state, &anchor, &status)) {
        LogMdsFailure("Create.ResolveAnchorAddress", path, status);
        return -StatusToErrno(status);
    } else {
        UpdateInodeAnchorCache(state, attr.inode_id(), anchor);
    }
    if (anchor.node_id().empty() || anchor.disk_id().empty()) {
        return -StatusToErrno(status);
    }
    uint64_t file_size = attr.size();
    uint64_t object_unit_size = attr.object_unit_size() > 0
                                    ? attr.object_unit_size()
                                    : FLAGS_default_object_unit_size;
    uint64_t file_meta_version = 0;
    int node_errno = EIO;
    if (!GetOrFetchNodeFileMeta(state,
                                attr.inode_id(),
                                anchor,
                                true,
                                attr.size(),
                                object_unit_size,
                                &file_size,
                                &object_unit_size,
                                &file_meta_version,
                                &node_errno)) {
        zb::rpc::Status st;
        st.set_code(zb::rpc::STATUS_INTERNAL_ERROR);
        st.set_message(std::strerror(node_errno));
        LogDataFailure("Create.GetOrFetchNodeFileMeta", path, attr.inode_id(), anchor, st);
        return -node_errno;
    }
    fi->fh = handle_id;
    return 0;
}

int FuseMkdir(const char* path, mode_t mode) {
    auto* state = GetState();
    struct fuse_context* ctx = fuse_get_context();
    zb::rpc::InodeAttr attr;
    zb::rpc::MdsStatus status;
    if (!state->mds.Mkdir(path, static_cast<uint32_t>(mode), ctx->uid, ctx->gid, &attr, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseUnlink(const char* path) {
    auto* state = GetState();
    zb::rpc::InodeAttr before_attr;
    zb::rpc::MdsStatus lookup_status;
    uint64_t inode_id = 0;
    if (state->mds.Lookup(path, &before_attr, &lookup_status)) {
        inode_id = before_attr.inode_id();
    }
    zb::rpc::MdsStatus status;
    if (!state->mds.Unlink(path, &status)) {
        return -StatusToErrno(status);
    }
    if (inode_id > 0) {
        InvalidateInodeCache(state, inode_id);
    }
    return 0;
}

int FuseRmdir(const char* path) {
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    if (!state->mds.Rmdir(path, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

int FuseRename(const char* from, const char* to, unsigned int flags) {
    (void)flags;
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    if (!state->mds.Rename(from, to, &status)) {
        return -StatusToErrno(status);
    }
    return 0;
}

uint64_t ResolveInode(struct fuse_file_info* fi, const char* path, FuseState* state,
                      zb::rpc::MdsStatus* status, IoTraceContext* trace = nullptr) {
    if (fi && fi->fh != 0) {
        std::lock_guard<std::mutex> lock(state->mu);
        auto it = state->handle_to_inode.find(fi->fh);
        if (it != state->handle_to_inode.end()) {
            return it->second;
        }
    }
    zb::rpc::InodeAttr attr;
    if (!state->mds.Lookup(path, &attr, status, trace)) {
        return 0;
    }
    UpdateInodeAttrCache(state, attr.inode_id(), attr);
    return attr.inode_id();
}

int FuseReadImpl(FuseState* state, const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi, IoTraceContext* trace) {
    if (offset < 0) {
        return -EINVAL;
    }
    if (size == 0) {
        return 0;
    }
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status, trace);
    if (trace) trace->record.inode_id = inode_id;
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }
    const uint64_t request_offset = static_cast<uint64_t>(offset);
    zb::rpc::FileLocationView location;
    if (!state->mds.GetFileLocationView(inode_id, &location, &status, trace)) {
        return -StatusToErrno(status);
    }
    zb::rpc::ReplicaLocation anchor;
    if (!SelectLocationForIo(location, &anchor)) {
        status.set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status.set_message("GetFileLocation returned empty location");
        return -StatusToErrno(status);
    }
    if (!EnsureReplicaNodeAddress(state, &anchor, &status)) {
        return -StatusToErrno(status);
    }
    if (trace) trace->record.storage_tier = anchor.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL
        ? metrics::StorageTier::Optical : metrics::StorageTier::Disk;
    UpdateInodeAnchorCache(state, inode_id, anchor);

    uint64_t hint_object_unit_size = FLAGS_default_object_unit_size;
    uint64_t hint_file_size = 0;
    GetCachedInodeMetaHint(state, inode_id, &hint_file_size, &hint_object_unit_size);

    if (anchor.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL) {
        if (hint_object_unit_size == 0) {
            hint_object_unit_size = FLAGS_default_object_unit_size;
        }
        if (hint_object_unit_size == 0) {
            return -EIO;
        }
        if (hint_file_size == 0) {
            zb::rpc::InodeAttr attr;
            if (!state->mds.Getattr(inode_id, &attr, &status, trace)) {
                return -StatusToErrno(status);
            }
            hint_file_size = attr.size();
            UpdateInodeAttrCache(state, inode_id, attr);
        }
        if (request_offset >= hint_file_size) {
            return 0;
        }
        uint64_t read_size = static_cast<uint64_t>(size);
        if (read_size > hint_file_size - request_offset) {
            read_size = hint_file_size - request_offset;
        }
        std::vector<zb::rpc::FileObjectSlice> slices;
        BuildObjectSlices(inode_id,
                          request_offset,
                          read_size,
                          hint_object_unit_size,
                          anchor.disk_id(),
                          &slices);
        std::string output(static_cast<size_t>(read_size), 'x');
        uint64_t write_pos = 0;
        for (const auto& slice : slices) {
            if (slice.length() == 0) {
                continue;
            }
            std::string data;
            std::string error;
            zb::rpc::ReplicaLocation replica = BuildObjectReplicaFromAnchor(anchor, slice.object_id());
            if (!state->data_nodes.Read(replica, slice.object_offset(), slice.length(), &data, &error, trace)) {
                return -EIO;
            }
            const size_t copy_len = static_cast<size_t>(std::min<uint64_t>(slice.length(), data.size()));
            if (write_pos + copy_len > output.size()) {
                return -EIO;
            }
            std::memcpy(output.data() + write_pos, data.data(), copy_len);
            write_pos += slice.length();
        }
        std::memcpy(buf, output.data(), output.size());
        return static_cast<int>(output.size());
    }

    zb::rpc::Status data_status;
    zb::rpc::FileMeta resolved_meta;
    std::vector<zb::rpc::FileObjectSlice> slices;
    if (!state->data_nodes.ResolveFileRead(anchor,
                                           inode_id,
                                           request_offset,
                                           static_cast<uint64_t>(size),
                                           hint_object_unit_size,
                                           &resolved_meta,
                                           &slices,
                                           &data_status, trace)) {
        // Drain migration transfers objects before switching the authoritative
        // MDS location. The destination may have no file-meta acceleration entry.
        // Only recover that specific absence; never mask transport/storage errors.
        if (data_status.code() != zb::rpc::STATUS_NOT_FOUND) {
            return -StatusToErrno(data_status);
        }
        const auto& attr = location.attr();
        if (attr.object_unit_size() == 0) return -EIO;
        resolved_meta.set_inode_id(inode_id);
        resolved_meta.set_file_size(attr.size());
        resolved_meta.set_object_unit_size(attr.object_unit_size());
        resolved_meta.set_version(attr.version());
        const uint64_t remaining = request_offset < attr.size()
                                       ? attr.size() - request_offset : 0;
        slices.clear();
        BuildObjectSlices(inode_id, request_offset,
                          std::min<uint64_t>(size, remaining),
                          attr.object_unit_size(), anchor.disk_id(), &slices);
    }
    const uint64_t object_unit_size = resolved_meta.object_unit_size() > 0
                                          ? resolved_meta.object_unit_size()
                                          : hint_object_unit_size;
    UpdateCachedFileMeta(state, inode_id, resolved_meta.file_size(), object_unit_size, resolved_meta.version());
    if (request_offset >= resolved_meta.file_size()) {
        return 0;
    }
    uint64_t read_size = static_cast<uint64_t>(size);
    if (read_size > resolved_meta.file_size() - request_offset) {
        read_size = resolved_meta.file_size() - request_offset;
    }

    std::string output(read_size, '\0');
    uint64_t write_pos = 0;
    for (const auto& slice : slices) {
        if (slice.length() == 0) {
            continue;
        }
        const uint64_t read_len = slice.length();
        std::string data;
        std::string error;
        const zb::rpc::ReplicaLocation replica = BuildObjectReplicaFromAnchor(anchor, slice.object_id());
        if (!state->data_nodes.Read(replica, slice.object_offset(), read_len, &data, &error, trace)) {
            return -EIO;
        }
        const size_t copy_len = static_cast<size_t>(std::min<uint64_t>(read_len, data.size()));
        if (write_pos + copy_len > output.size()) {
            return -EIO;
        }
        std::memcpy(output.data() + write_pos, data.data(), copy_len);
        write_pos += read_len;
    }

    std::memcpy(buf, output.data(), output.size());
    return static_cast<int>(output.size());
}

int FuseWriteImpl(FuseState* state, const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi, IoTraceContext* trace) {
    if (offset < 0) {
        return -EINVAL;
    }
    if (size == 0) {
        return 0;
    }
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status, trace);
    if (trace) trace->record.inode_id = inode_id;
    if (inode_id == 0) {
        LogMdsFailure("Write.ResolveInode", path, status);
        return -StatusToErrno(status);
    }

    zb::rpc::FileLocationView location;
    if (!state->mds.GetFileLocationView(inode_id, &location, &status, trace)) {
        LogMdsFailure("Write.GetFileLocationView", path, status);
        return -StatusToErrno(status);
    }
    if (IsArchivedReadOnly(location)) {
        if (trace) trace->record.storage_tier = metrics::StorageTier::Optical;
        return -EROFS;
    }
    zb::rpc::ReplicaLocation anchor;
    if (!SelectLocationForIo(location, &anchor)) {
        status.set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status.set_message("GetFileLocation returned empty location");
        LogMdsFailure("Write.SelectLocationForIo", path, status);
        return -StatusToErrno(status);
    }
    if (!EnsureReplicaNodeAddress(state, &anchor, &status)) {
        LogMdsFailure("Write.ResolveAnchorAddress", path, status);
        return -StatusToErrno(status);
    }
    if (trace) trace->record.storage_tier = anchor.storage_tier() == zb::rpc::STORAGE_TIER_OPTICAL
        ? metrics::StorageTier::Optical : metrics::StorageTier::Disk;
    UpdateInodeAnchorCache(state, inode_id, anchor);

    uint64_t hint_object_unit_size = FLAGS_default_object_unit_size;
    uint64_t ignored_file_size = 0;
    GetCachedInodeMetaHint(state, inode_id, &ignored_file_size, &hint_object_unit_size);

    const uint64_t request_start = static_cast<uint64_t>(offset);
    if (request_start > std::numeric_limits<uint64_t>::max() - static_cast<uint64_t>(size)) {
        return -EOVERFLOW;
    }
    const uint64_t request_end = request_start + static_cast<uint64_t>(size);

    zb::rpc::Status data_status;
    zb::rpc::FileMeta plan_meta;
    std::string txid;
    std::vector<zb::rpc::FileObjectSlice> slices;
    if (!state->data_nodes.AllocateFileWrite(anchor,
                                             inode_id,
                                             request_start,
                                             static_cast<uint64_t>(size),
                                             hint_object_unit_size,
                                             &plan_meta,
                                             &txid,
                                             &slices,
                                             &data_status, trace)) {
        LogDataFailure("Write.AllocateFileWrite", path, inode_id, anchor, data_status);
        return -StatusToErrno(data_status);
    }
    const uint64_t object_unit_size =
        plan_meta.object_unit_size() > 0 ? plan_meta.object_unit_size() : hint_object_unit_size;
    if (object_unit_size == 0 || txid.empty()) {
        return -EIO;
    }

    uint64_t cursor = 0;
    for (const auto& slice : slices) {
        if (slice.length() == 0) {
            continue;
        }
        if (cursor > static_cast<uint64_t>(size) || slice.length() > static_cast<uint64_t>(size) - cursor) {
            return -EIO;
        }
        const uint64_t write_len = slice.length();
        std::string data(buf + cursor, buf + cursor + write_len);
        const zb::rpc::ReplicaLocation replica = BuildObjectReplicaFromAnchor(anchor, slice.object_id());
        std::string error;
        if (!state->data_nodes.Write(replica, slice.object_offset(), data, &error, trace)) {
            zb::rpc::Status st;
            st.set_code(zb::rpc::STATUS_IO_ERROR);
            st.set_message(error);
            LogDataFailure("Write.WriteObject", path, inode_id, replica, st);
            return -EIO;
        }
        cursor += write_len;
    }
    if (cursor != static_cast<uint64_t>(size)) {
        return -EIO;
    }

    const uint64_t new_size = std::max<uint64_t>(plan_meta.file_size(), request_end);
    zb::rpc::FileMeta committed_meta;
    if (!state->data_nodes.CommitFileWrite(anchor,
                                           inode_id,
                                           txid,
                                           new_size,
                                           object_unit_size,
                                           plan_meta.version(),
                                           true,
                                           static_cast<uint64_t>(std::time(nullptr)),
                                           &committed_meta,
                                           &data_status, trace)) {
        LogDataFailure("Write.CommitFileWrite", path, inode_id, anchor, data_status);
        return -StatusToErrno(data_status);
    }
    UpdateCachedFileMeta(state,
                         inode_id,
                         committed_meta.file_size(),
                         committed_meta.object_unit_size() > 0 ? committed_meta.object_unit_size() : object_unit_size,
                         committed_meta.version());
    zb::rpc::MdsStatus update_status;
    zb::rpc::InodeAttr updated_attr;
    if (state->mds.UpdateInodeStat(inode_id,
                                   committed_meta.file_size(),
                                   committed_meta.object_unit_size() > 0 ? committed_meta.object_unit_size()
                                                                          : object_unit_size,
                                   committed_meta.version(),
                                   committed_meta.mtime_sec(),
                                   &updated_attr,
                                   &update_status, trace)) {
        UpdateInodeAttrCache(state, inode_id, updated_attr);
    }
    return static_cast<int>(size);
}

// These entry points also let the integration test exercise the actual callback
// body and recorder without requiring a kernel FUSE mount.
int ReadRequest(FuseState* state, const char* path, char* buf, size_t size, off_t offset,
                struct fuse_file_info* fi) {
    if (!state->latency) return FuseReadImpl(state, path, buf, size, offset, fi, nullptr);
    metrics::RequestTraceScope request(*state->latency, false, offset, size);
    return request.Finish(FuseReadImpl(state, path, buf, size, offset, fi, request.Context()));
}

int WriteRequest(FuseState* state, const char* path, const char* buf, size_t size, off_t offset,
                 struct fuse_file_info* fi) {
    if (!state->latency) return FuseWriteImpl(state, path, buf, size, offset, fi, nullptr);
    metrics::RequestTraceScope request(*state->latency, true, offset, size);
    return request.Finish(FuseWriteImpl(state, path, buf, size, offset, fi, request.Context()));
}

int FuseRead(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    return ReadRequest(GetState(), path, buf, size, offset, fi);
}

int FuseWrite(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    return WriteRequest(GetState(), path, buf, size, offset, fi);
}

int FuseTruncate(const char* path, off_t size, struct fuse_file_info* fi) {
    if (size < 0) {
        return -EINVAL;
    }
    auto* state = GetState();
    zb::rpc::MdsStatus status;
    uint64_t inode_id = ResolveInode(fi, path, state, &status);
    if (inode_id == 0) {
        return -StatusToErrno(status);
    }
    zb::rpc::FileLocationView location;
    if (!state->mds.GetFileLocationView(inode_id, &location, &status)) {
        return -StatusToErrno(status);
    }
    if (IsArchivedReadOnly(location)) {
        return -EROFS;
    }
    zb::rpc::ReplicaLocation anchor;
    if (!SelectLocationForIo(location, &anchor)) {
        status.set_code(zb::rpc::MDS_INTERNAL_ERROR);
        status.set_message("GetFileLocation returned empty location");
        return -StatusToErrno(status);
    }
    if (!EnsureReplicaNodeAddress(state, &anchor, &status)) {
        return -StatusToErrno(status);
    }
    UpdateInodeAnchorCache(state, inode_id, anchor);

    uint64_t hint_file_size = static_cast<uint64_t>(size);
    uint64_t hint_object_unit_size = FLAGS_default_object_unit_size;
    GetCachedInodeMetaHint(state, inode_id, &hint_file_size, &hint_object_unit_size);
    uint64_t cached_file_size = 0;
    uint64_t object_unit_size = 0;
    uint64_t file_meta_version = 0;
    int node_errno = EIO;
    if (!GetOrFetchNodeFileMeta(state,
                                inode_id,
                                anchor,
                                true,
                                hint_file_size,
                                hint_object_unit_size,
                                &cached_file_size,
                                &object_unit_size,
                                &file_meta_version,
                                &node_errno)) {
        return -node_errno;
    }
    uint64_t committed_version = 0;
    if (!CommitNodeFileMeta(state,
                            inode_id,
                            anchor,
                            static_cast<uint64_t>(size),
                            object_unit_size,
                            file_meta_version,
                            &committed_version,
                            &node_errno)) {
        return -node_errno;
    }
    return 0;
}

bool PrepareLatency(FuseState* state, int argc, char** argv, std::string* error) {
    if (!FLAGS_io_latency_enabled) return true;
    metrics::OutputDirectory output;
    if (!metrics::ResolveOutputDirectory(FLAGS_io_latency_dir, FLAGS_io_latency_base_conf,
                                         ZBSTORAGE_SOURCE_ROOT, &output, error)) return false;
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    struct fuse_cmdline_opts opts = {};
    const int parsed = fuse_parse_cmdline(&args, &opts);
    fuse_opt_free_args(&args);
    std::string mount = opts.mountpoint ? opts.mountpoint : "";
    std::free(opts.mountpoint);
    if (parsed != 0 || mount.empty()) {
        *error = "a valid FUSE mount point is required for latency recording";
        return false;
    }
    auto recorder = std::make_unique<metrics::LatencyRecorder>();
    metrics::RecorderOptions options;
    options.flush_ms = FLAGS_io_latency_flush_ms;
    options.batch_records = FLAGS_io_latency_batch_records;
    options.queue_capacity = FLAGS_io_latency_queue_capacity;
    if (!recorder->Prepare(output.path, mount, options, error)) return false;
    std::fprintf(stderr, "[io_latency] directory=%s source=%s\n", output.path.c_str(), output.source.c_str());
    state->latency = std::move(recorder);
    return true;
}

void* FuseInit(struct fuse_conn_info*, struct fuse_config*) {
    auto* state = GetState();
    if (state->latency) {
        std::string error;
        if (!state->latency->Start(&error)) {
            state->latency_start_failed = true;
            std::fprintf(stderr, "[io_latency] startup failed: %s\n", error.c_str());
            fuse_exit(fuse_get_context()->fuse);
        }
    }
    return state;
}

void FuseDestroy(void* data) {
    auto* state = static_cast<FuseState*>(data);
    if (state->latency) state->latency->Stop();
}

} // namespace

} // namespace zb::client::fuse_client

#ifndef ZBSTORAGE_FUSE_CLIENT_NO_MAIN
int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    zb::client::fuse_client::FuseState state;
    std::string latency_error;
    if (!zb::client::fuse_client::PrepareLatency(&state, argc, argv, &latency_error)) {
        std::fprintf(stderr, "[io_latency] startup failed: %s\n", latency_error.c_str());
        return 1;
    }
    if (!state.mds.Init(FLAGS_mds)) {
        std::fprintf(stderr, "Failed to connect to MDS %s\n", FLAGS_mds.c_str());
        return 1;
    }
    if (FLAGS_scheduler.empty()) {
        std::fprintf(stderr, "scheduler endpoint is required\n");
        return 1;
    }
    if (!state.scheduler.Init(FLAGS_scheduler)) {
        std::fprintf(stderr, "Failed to connect to scheduler %s\n", FLAGS_scheduler.c_str());
        return 1;
    }
    state.scheduler_enabled = true;
    std::string refresh_error;
    if (!zb::client::fuse_client::RefreshNodeAddressCache(&state, true, &refresh_error)) {
        std::fprintf(stderr, "Failed to fetch scheduler cluster view: %s\n", refresh_error.c_str());
        return 1;
    }
    state.data_nodes.SetNodeAddressResolver(
        [&state](const std::string& node_id, std::string* address) {
            return zb::client::fuse_client::ResolveNodeAddress(&state, node_id, address);
        });
    if (FLAGS_bootstrap_tier_dirs) {
        std::string bootstrap_error;
        if (!zb::client::fuse_client::EnsureTierBootstrap(&state, &bootstrap_error)) {
            std::fprintf(stderr, "Failed to bootstrap tier directories: %s\n", bootstrap_error.c_str());
            return 1;
        }
    }

    static struct fuse_operations ops = {};
    ops.init = zb::client::fuse_client::FuseInit;
    ops.destroy = zb::client::fuse_client::FuseDestroy;
    ops.getattr = zb::client::fuse_client::FuseGetattr;
    ops.readdir = zb::client::fuse_client::FuseReaddir;
    ops.open = zb::client::fuse_client::FuseOpen;
    ops.release = zb::client::fuse_client::FuseRelease;
    ops.create = zb::client::fuse_client::FuseCreate;
    ops.mkdir = zb::client::fuse_client::FuseMkdir;
    ops.unlink = zb::client::fuse_client::FuseUnlink;
    ops.rmdir = zb::client::fuse_client::FuseRmdir;
    ops.rename = zb::client::fuse_client::FuseRename;
    ops.read = zb::client::fuse_client::FuseRead;
    ops.write = zb::client::fuse_client::FuseWrite;
    ops.truncate = zb::client::fuse_client::FuseTruncate;

    const int result = fuse_main(argc, argv, &ops, &state);
    if (state.latency) state.latency->Stop();
    return state.latency_start_failed ? 1 : result;
}

#endif // ZBSTORAGE_FUSE_CLIENT_NO_MAIN
