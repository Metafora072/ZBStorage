#pragma once
#include "LhmBridge.h"
#include "metadata_layout.hh"
#include <cstring>
#include <limits>

namespace zb::mds::lhm_codec {
// Explicit little-endian v1 encoding; never serialize compiler struct padding.
// 161 of the native inode's 164 reserved bytes are used.
constexpr size_t kExtensionBytes = 161;
static_assert(sizeof(MasstreeLHM::inode_disk::reserved) >= kExtensionBytes);
inline bool Valid(const zb_lhm_full_inode& a) {
    constexpr uint64_t max_seconds = std::numeric_limits<uint64_t>::max()/1000000000ULL;
    return a.inode_id && (a.type == 1 || a.type == 2) && a.nlink &&
           a.mode <= UINT16_MAX && a.mtime <= max_seconds && a.ctime <= max_seconds &&
           a.name_length <= 64 && a.storage_tier <= 2 && a.archive_state <= 3 &&
           a.disk_node_is_virtual <= 1;
}
inline bool Encode(const zb_lhm_full_inode& a, MasstreeLHM::inode_disk& out) {
    if (!Valid(a)) return false;
    out = {};
    out.inode_id=a.inode_id; out.type=a.type; out.mode=a.mode; out.link_count=a.nlink;
    out.size_bytes=a.size; out.ctime_ns=a.ctime*1000000000ULL; out.mtime_ns=a.mtime*1000000000ULL;
    out.stable_version=a.version;
    auto* p=out.reserved;
    auto put=[&](uint64_t value, unsigned bytes) { for(unsigned i=0;i<bytes;++i) *p++=value>>(8*i); };
    put(0x3141425a,4); // ZBA1
    put(1,1); put(a.name_length,1); put(a.disk_node_is_virtual,1);
    put(a.storage_tier,1); put(a.archive_state,1);
    put(a.uid,4); put(a.gid,4); put(a.atime,8); put(a.object_unit_size,8); put(a.replica,4);
    put(a.disk_node_id,8); put(a.disk_id,4);
    put(a.optical_node_id,8); put(a.optical_disk_id,8); put(a.optical_image_id,4);
    put(a.blksize,4); put(a.blocks_512,8); put(a.rdev_major,4); put(a.rdev_minor,4);
    put(a.version,8);
    std::memcpy(p,a.file_name,a.name_length); p+=64;
    return size_t(p-out.reserved)==kExtensionBytes;
}
inline bool Decode(const MasstreeLHM::inode_disk& in, zb_lhm_full_inode& a) {
    const auto* p=in.reserved;
    auto get=[&](unsigned bytes) { uint64_t v=0; for(unsigned i=0;i<bytes;++i) v|=uint64_t(*p++)<<(8*i); return v; };
    if(get(4)!=0x3141425a || get(1)!=1) return false;
    a={}; a.name_length=get(1); a.disk_node_is_virtual=get(1);
    a.storage_tier=get(1); a.archive_state=get(1);
    a.uid=get(4); a.gid=get(4); a.atime=get(8); a.object_unit_size=get(8); a.replica=get(4);
    a.disk_node_id=get(8); a.disk_id=get(4);
    a.optical_node_id=get(8); a.optical_disk_id=get(8); a.optical_image_id=get(4);
    a.blksize=get(4); a.blocks_512=get(8); a.rdev_major=get(4); a.rdev_minor=get(4);
    a.version=get(8);
    std::memcpy(a.file_name,p,64);
    a.inode_id=in.inode_id; a.type=in.type; a.mode=in.mode; a.nlink=in.link_count;
    a.size=in.size_bytes; a.ctime=in.ctime_ns/1000000000ULL; a.mtime=in.mtime_ns/1000000000ULL;
    return Valid(a);
}
} // namespace zb::mds::lhm_codec
