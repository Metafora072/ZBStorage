#include "FileLocationResolver.h"
#include <limits>
#include "mds/storage/UnifiedInodeLocation.h"

namespace zb::mds {
namespace {
NamespaceStatus Error(const std::string& error) {
    return {zb::rpc::MDS_INTERNAL_ERROR, error};
}
}
NamespaceStatus RecordFileLocationResolver::Resolve(uint64_t id, zb::rpc::FileLocationView* out) const {
    if (out) out->Clear();
    if (!id || !out) return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid location query"};
    if (!sources_.read_inode) return Error("inode reader is not configured");
    zb::rpc::InodeAttr attr;
    std::string error;
    if (!sources_.read_inode(id, &attr, &error))
        return error.empty() ? NamespaceStatus{zb::rpc::MDS_NOT_FOUND, "inode not found"} : Error(error);
    if (attr.inode_id() != id) return Error("location inode identity mismatch");
    return ResolveKnownInode(attr, out);
}
NamespaceStatus RecordFileLocationResolver::ResolveKnownInode(
    const zb::rpc::InodeAttr& attr, zb::rpc::FileLocationView* out) const {
    if (out) out->Clear();
    if (!out || !attr.inode_id() || attr.type() != zb::rpc::INODE_FILE)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "location requires a file inode"};
    const bool optical = attr.storage_tier() == zb::rpc::INODE_STORAGE_OPTICAL;
    if (!optical && attr.storage_tier() != zb::rpc::INODE_STORAGE_DISK)
        return {zb::rpc::MDS_NOT_FOUND, "file has no storage location"};
    if (!sources_.read_record) return Error("location record reader is not configured");
    std::string record, error;
    const bool full = sources_.read_record(attr.inode_id(), optical, &record, &error);
    if (!full && !error.empty()) return Error(error);
    zb::rpc::FileLocationView result;
    *result.mutable_attr() = attr;
    if (optical) {
        auto* location = result.mutable_optical_location();
        if (full) {
            if (!location->ParseFromString(record) || location->node_id().empty() || location->disk_id().empty())
                return Error("corrupt full optical location");
        } else {
            if (!PopulateOpticalFileLocationFromInodeAttr(attr, attr.inode_id(), location))
                return {zb::rpc::MDS_NOT_FOUND, "optical location missing"};
            if (sources_.resolve_disk && attr.optical_disk_id() <= std::numeric_limits<uint32_t>::max()) {
                std::string disk;
                if (sources_.resolve_disk(location->node_id(), static_cast<uint32_t>(attr.optical_disk_id()), &disk)
                    && !disk.empty()) location->set_disk_id(disk);
            }
        }
        if (sources_.resolve_optical && !sources_.resolve_optical(location, !full, &error)) return Error(error);
        std::string address = location->node_address();
        uint64_t generation = location->instance_generation();
        if (sources_.resolve_node) sources_.resolve_node(location->node_id(), &address, &generation);
        location->set_node_address(address); location->set_instance_generation(generation);
    } else {
        auto* location = result.mutable_disk_location();
        if (full) {
            if (!location->ParseFromString(record) || location->node_id().empty() || location->disk_id().empty())
                return Error("corrupt full disk location");
        } else {
            if (!PopulateDiskFileLocationFromInodeAttr(attr, location))
                return {zb::rpc::MDS_NOT_FOUND, "disk location missing"};
            if (sources_.resolve_disk) {
                std::string disk;
                if (sources_.resolve_disk(location->node_id(), attr.disk_id(), &disk) && !disk.empty())
                    location->set_disk_id(disk);
            }
        }
        std::string address = location->node_address();
        uint64_t generation = location->instance_generation();
        if (sources_.resolve_node) sources_.resolve_node(location->node_id(), &address, &generation);
        location->set_node_address(address); location->set_instance_generation(generation);
    }
    *out = std::move(result);
    return {};
}
}
