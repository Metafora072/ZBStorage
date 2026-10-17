#include "MdsServiceImpl.h"

#include <brpc/controller.h>
#include <set>
#include "optical_node.pb.h"
#include "common/rpc/HandoffFiles.h"
#include "real_node.pb.h"
#include "../storage/UnifiedInodeLocation.h"

namespace zb::mds {
bool MdsServiceImpl::VerifyPhysicalArchive(const zb::rpc::ModelOpticalArchiveRequest& request, std::string* error) const {
    // 当前真实卷格式没有文件版本。仅允许已排空磁盘源完成此确认，并
    // 在持久写栅栏下读取源实际字节，与介质解压摘要比较；不能凭大小猜版本。
    zb::rpc::CmsNodeCatalogEntry source;
    if (!node_registry_->GetCommittedNode(request.expected_source().node_id(), &source) ||
        source.managed_view().lifecycle() != zb::rpc::MANAGED_LIFECYCLE_DRAINING) {
        *error = "physical archive completion requires an already draining disk source"; return false;
    }
    brpc::Channel disk_channel; brpc::ChannelOptions disk_options; disk_options.timeout_ms = 5000; disk_options.max_retry = 0;
    if (disk_channel.Init(source.managed_view().service_address().c_str(), &disk_options) != 0) { *error = "disk source unavailable"; return false; }
    zb::rpc::RealNodeService_Stub disk(&disk_channel);
    zb::rpc::BeginNodeDrainRequest fence; fence.set_node_id(request.expected_source().node_id());
    zb::rpc::BeginNodeDrainReply fenced; brpc::Controller gate;
    disk.BeginNodeDrain(&gate, &fence, &fenced, nullptr);
    if (gate.Failed() || !fenced.has_status() || fenced.status().code() != zb::rpc::STATUS_OK || !fenced.write_admission_closed()) {
        *error = "disk source write fence not confirmed"; return false;
    }
    zb::rpc::ResolveFileReadRequest metadata; metadata.set_inode_id(request.inode_id());
    metadata.set_disk_id(request.expected_source().disk_id()); metadata.set_size(request.size());
    zb::rpc::ResolveFileReadReply resolved; brpc::Controller lookup;
    disk.ResolveFileRead(&lookup, &metadata, &resolved, nullptr);
    if (lookup.Failed() || !resolved.has_status() || resolved.status().code() != zb::rpc::STATUS_OK ||
        resolved.meta().version() != request.source_version() || resolved.meta().file_size() != request.size()) {
        *error = "fenced source version/size differs from archive manifest"; return false;
    }
    SHA256_CTX hash; SHA256_Init(&hash);
    uint64_t total = 0;
    for (const auto& slice : resolved.slices()) {
        if (resolved.meta().object_unit_size() == 0 ||
            slice.object_index() > UINT64_MAX / resolved.meta().object_unit_size() ||
            slice.object_index() * resolved.meta().object_unit_size() + slice.object_offset() != total ||
            slice.length() > request.size() - total) { *error = "source read plan not contiguous"; return false; }
        for (uint64_t offset = 0; offset < slice.length();) {
            zb::rpc::ReadObjectRequest read; read.set_disk_id(slice.disk_id()); read.set_object_id(slice.object_id());
            read.set_offset(slice.object_offset() + offset); read.set_size(std::min<uint64_t>(1048576, slice.length() - offset));
            zb::rpc::ReadObjectReply bytes; brpc::Controller reader;
            disk.ReadObject(&reader, &read, &bytes, nullptr);
            if (reader.Failed() || !bytes.has_status() || bytes.status().code() != zb::rpc::STATUS_OK ||
                bytes.data().size() != read.size()) { *error = "cannot checksum complete fenced source"; return false; }
            SHA256_Update(&hash, bytes.data().data(), bytes.data().size()); offset += bytes.data().size();
        }
        total += slice.length();
    }
    if (total != request.size()) { *error = "source read plan incomplete"; return false; }
    unsigned char digest[SHA256_DIGEST_LENGTH]; SHA256_Final(digest, &hash);
    static constexpr char hex[] = "0123456789abcdef"; std::string expected_sha;
    for (auto c : digest) { expected_sha += hex[c >> 4]; expected_sha += hex[c & 15]; }
    brpc::Channel channel; brpc::ChannelOptions options; options.timeout_ms = 5000; options.max_retry = 0;
    if (channel.Init(request.target().node_address().c_str(), &options) != 0) { *error = "cannot reach physical archive"; return false; }
    zb::rpc::OpticalNodeService_Stub stub(&channel);
    zb::rpc::GetArchiveImageFilesRequest query;
    query.set_node_id(request.target().node_id()); query.set_instance_generation(request.target().instance_generation());
    query.set_disk_id(request.target().disk_id()); query.set_image_id(request.target().image_id()); query.set_limit(1024);
    query.set_digest_inode_id(request.inode_id());
    for (;;) {
        brpc::Controller controller; zb::rpc::GetArchiveImageFilesReply reply;
        stub.GetArchiveImageFiles(&controller, &query, &reply, nullptr);
        if (controller.Failed() || !reply.has_status() || reply.status().code() != zb::rpc::OPTICAL_OK || reply.modeled()) {
            *error = "real archive media manifest unavailable"; return false;
        }
        for (const auto& file : reply.files()) {
            if (file.inode_id() == request.inode_id()) {
                if (file.size() == request.size() && file.payload_sha256() == expected_sha) return true;
                *error = "real archive payload/size differs from fenced source"; return false;
            }
        }
        if (!reply.has_more()) { *error = "inode absent from completed real archive"; return false; }
        if (reply.next_offset() <= query.offset() || reply.snapshot_token().empty()) {
            *error = "real archive manifest pagination invalid"; return false;
        }
        query.set_offset(reply.next_offset()); query.set_snapshot_token(reply.snapshot_token());
    }
}
namespace {
std::string Key(const std::string& node) { return "OLH/" + node; }
std::string Canonical(zb::rpc::OpticalLibraryHandoffRecord r) {
    r.clear_phase(); return r.SerializeAsString();
}
}

void MdsServiceImpl::CommitOpticalLibraryHandoff(google::protobuf::RpcController*,
    const zb::rpc::CommitOpticalLibraryHandoffRequest* request,
    zb::rpc::CommitOpticalLibraryHandoffReply* response, google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    const auto fail = [&](zb::rpc::MdsStatusCode code, const std::string& msg) {
        FillStatus(response->mutable_status(), code, msg);
    };
    if (!store_ || !node_registry_ || !request || !request->has_record()) {
        fail(zb::rpc::MDS_INVALID_ARGUMENT, "handoff record required"); return;
    }
    const auto& r = request->record();
    if (r.operation_id().empty() || r.operation_id().size() > 128 || r.source().node_id().empty() ||
        r.target().node_id().empty() || r.source().node_id() == r.target().node_id() ||
        !r.source().instance_generation() || !r.target().instance_generation() ||
        r.manifest_id() != zb::handoff::ManifestId(r) || r.files_size() == 0) {
        fail(zb::rpc::MDS_INVALID_ARGUMENT, "invalid handoff identity or manifest"); return;
    }
    const auto key = Key(r.source().node_id()), bytes = Canonical(r);
    std::string old, error;
    if (!request->commit()) {
        // 已提交映射不可删除/覆盖。只读查询不拿位置写锁，避免源释放持有
        // 本地栅栏时，与提交事务回查源收据形成跨服务锁环。
        if (store_->Get(key, &old, &error)) {
            if (old != bytes) { fail(zb::rpc::MDS_STALE_EPOCH, "handoff manifest differs from committed mapping"); return; }
            response->set_committed(true);
        } else if (!error.empty()) { fail(zb::rpc::MDS_INTERNAL_ERROR, error); return; }
        fail(zb::rpc::MDS_OK, "queried"); return;
    }
    std::unique_lock<zb::rpc_support::FiberSharedMutex> placement(placement_transaction_mu_);
    std::lock_guard<bthread::Mutex> migration(file_migration_mu_);
    if (store_->Get(key, &old, &error)) {
        if (old != bytes) { fail(zb::rpc::MDS_STALE_EPOCH, "source already handed to a different manifest"); return; }
        response->set_committed(true); fail(zb::rpc::MDS_OK, "already committed"); return;
    }
    if (!error.empty()) { fail(zb::rpc::MDS_INTERNAL_ERROR, error); return; }
    if (!request->commit()) { fail(zb::rpc::MDS_OK, "not committed"); return; }
    for (const auto* endpoint : {&r.source(), &r.target()}) {
        zb::rpc::CmsNodeCatalogEntry entry;
        if (!node_registry_->GetCommittedNode(endpoint->node_id(), &entry)) {
            fail(zb::rpc::MDS_STALE_EPOCH, "endpoint absent from CMS"); return;
        }
        const auto& view = entry.managed_view();
        const bool target = endpoint == &r.target();
        if (view.spec().kind() != zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY ||
            view.spec().execution_mode() != zb::rpc::MANAGED_EXECUTION_PHYSICAL ||
            view.spec().instance_generation() != endpoint->instance_generation() ||
            view.service_address() != endpoint->node_address() ||
            (target ? (view.lifecycle() != zb::rpc::MANAGED_LIFECYCLE_WORKING ||
                       view.health() != zb::rpc::MANAGED_HEALTH_HEALTHY ||
                       view.administrative_state() != zb::rpc::MANAGED_ADMIN_ENABLED ||
                       !view.readiness().metadata_ready() || !view.readiness().inventory_ready())
                    : (view.lifecycle() != zb::rpc::MANAGED_LIFECYCLE_DRAINING &&
                       view.lifecycle() != zb::rpc::MANAGED_LIFECYCLE_EXITING))) {
            fail(zb::rpc::MDS_STALE_EPOCH, "endpoint generation/address/lifecycle not CMS admitted"); return;
        }
        brpc::Channel channel; brpc::ChannelOptions opts; opts.timeout_ms = 5000; opts.max_retry = 0;
        if (channel.Init(endpoint->node_address().c_str(), &opts) != 0) {
            fail(zb::rpc::MDS_INTERNAL_ERROR, "cannot verify endpoint receipt"); return;
        }
        zb::rpc::OpticalNodeService_Stub stub(&channel); brpc::Controller controller;
        zb::rpc::OpticalLibraryHandoffRequest query;
        query.set_node_id(endpoint->node_id()); query.set_instance_generation(endpoint->instance_generation());
        zb::rpc::OpticalLibraryHandoffReply receipt;
        stub.ManageLibraryHandoff(&controller, &query, &receipt, nullptr);
        if (controller.Failed() || !receipt.has_status() || receipt.status().code() != zb::rpc::OPTICAL_OK ||
            Canonical(receipt.record()) != bytes || receipt.record().phase() != (target ? "ACTIVE" : "EXPORTED")) {
            fail(zb::rpc::MDS_STALE_EPOCH, "source freeze or durable target receipt not confirmed"); return;
        }
    }
    // 不展开重写冷目录：整库保留介质编号，统一解析层应用权威节点交接映射。
    // 冷目录与普通 inode 都经过同一解析；映射与精确清单同步落盘后才允许源隔离。
    zb::rpc::OpticalFileLocation next = r.target();
    if (!ResolveOpticalHandoff(&next, &error) || next.node_id() != r.target().node_id()) {
        fail(zb::rpc::MDS_STALE_EPOCH, "target already relocated or relocation cycle"); return;
    }
    rocksdb::WriteBatch batch; batch.Put(key, bytes);
    rocksdb::WriteOptions options; options.sync = true;
    const auto status = store_->db()->Write(options, &batch);
    if (!status.ok()) { fail(zb::rpc::MDS_INTERNAL_ERROR, status.ToString()); return; }
    response->set_committed(true); fail(zb::rpc::MDS_OK, "committed");
}

bool MdsServiceImpl::ResolveOpticalHandoff(zb::rpc::OpticalFileLocation* location, std::string* error) const {
    std::set<std::string> visited;
    for (unsigned depth = 0; depth < 64; ++depth) {
        if (!visited.insert(location->node_id()).second) {
            if (error) *error = "optical handoff cycle"; return false;
        }
        std::string bytes, local_error;
        if (!store_->Get(Key(location->node_id()), &bytes, &local_error)) {
            uint64_t generation = 0;
            if (location->instance_generation() != 0 && node_registry_ &&
                node_registry_->GetInstanceGeneration(location->node_id(), &generation) &&
                generation != location->instance_generation()) {
                if (error) *error = "optical location generation no longer owns this media"; return false;
            }
            if (error) *error = local_error;
            return local_error.empty();
        }
        zb::rpc::OpticalLibraryHandoffRecord r;
        if (!r.ParseFromString(bytes) || r.source().node_id() != location->node_id() ||
            r.manifest_id() != zb::handoff::ManifestId(r) ||
            (location->instance_generation() && location->instance_generation() != r.source().instance_generation())) {
            if (error) *error = "corrupt or stale-generation optical handoff"; return false;
        }
        location->set_node_id(r.target().node_id());
        location->set_node_address(r.target().node_address());
        location->set_instance_generation(r.target().instance_generation());
    }
    if (error) *error = "optical handoff chain too deep";
    return false;
}

void MdsServiceImpl::ResolvePackedOpticalNode(zb::rpc::OpticalFileLocation* location) const {
    if (!node_registry_) return;
    uint64_t compact = 0;
    if (!TryParseTrailingUint64(location->node_id(), &compact)) return;
    for (const auto& entry : node_registry_->Snapshot(nullptr, nullptr)) {
        const auto& view = entry.managed_view();
        if (view.spec().kind() == zb::rpc::MANAGED_KIND_OPTICAL_LIBRARY && view.compact_id() == compact) {
            location->set_node_id(view.spec().node_id());
            location->set_instance_generation(view.spec().instance_generation());
            return; // 包含 retired 墓碑；随后按持久交接映射解析当前所有者。
        }
    }
}

bool MdsServiceImpl::CountColdNodeReferences(const std::string& node, uint64_t* disk_count,
                                             uint64_t* optical_count, std::string* error) const {
    // 退役验证不能只扫描 RocksDB I/。沿已有只读目录入口遍历冷命名空间，
    // 不依赖内部库存格式，不修改其导入/页面/布局算法；任何解码失败均保留源。
    auto scan = [&](const auto& routes, const auto& reader, auto locate) {
        for (const auto& route : routes) {
            std::vector<std::string> paths{route.path_prefix};
            std::set<uint64_t> seen_dirs;
            while (!paths.empty()) {
                const auto path = paths.back(); paths.pop_back();
                std::string token;
                bool more = false;
                do {
                    std::vector<zb::rpc::Dentry> entries; std::string next;
                    if (!reader.Readdir(route, path, token, 1024, &entries, &more, &next, error)) return false;
                    for (const auto& entry : entries) {
                        if (entry.name() == "." || entry.name() == "..") continue;
                        if (entry.type() == zb::rpc::INODE_DIR) {
                            if (seen_dirs.insert(entry.inode_id()).second)
                                paths.push_back(path + (path.back() == '/' ? "" : "/") + entry.name());
                        } else if (entry.type() == zb::rpc::INODE_FILE) {
                            std::string payload, local_error;
                            if (store_->Get(InodeKey(entry.inode_id()), &payload, &local_error)) continue;
                            if (!local_error.empty()) { if (error) *error = local_error; return false; }
                            zb::rpc::OpticalFileLocation location;
                            zb::rpc::InodeAttr attr;
                            if (!reader.GetInode(route, entry.inode_id(), &attr, error)) return false;
                            if (attr.storage_tier() == zb::rpc::INODE_STORAGE_DISK) {
                                zb::rpc::DiskFileLocation disk;
                                if (!BuildDiskFileLocationFromAttr(attr, &disk, error)) return false;
                                if (disk.node_id() == node) ++*disk_count;
                            } else if (attr.storage_tier() == zb::rpc::INODE_STORAGE_OPTICAL) {
                                if (!locate(route, entry.inode_id(), &location)) return false;
                                if (location.node_id() == node) ++*optical_count;
                            } else { if (error) *error = "cold file has no verifiable storage tier"; return false; }
                        }
                    }
                    if (more && (next.empty() || next == token)) { if (error) *error = "cold readdir made no progress"; return false; }
                    token = std::move(next);
                } while (more);
            }
        }
        return true;
    };
    std::vector<MasstreeNamespaceRoute> masstree;
    if (!masstree_namespace_catalog_.ListRoutes(&masstree, error) ||
        !scan(masstree, masstree_meta_store_, [&](const auto& route, uint64_t inode, auto* out) {
            if (!masstree_meta_store_.GetOpticalFileLocation(route, inode, out, error)) return false;
            ResolvePackedOpticalNode(out); return ResolveOpticalHandoff(out, error);
        })) return false;
    std::vector<ArchiveNamespaceRoute> archive;
    if (!archive_namespace_catalog_.ListRoutes(&archive, error)) return false;
    return scan(archive, archive_meta_store_, [&](const auto& route, uint64_t inode, auto* out) {
        zb::rpc::InodeAttr attr;
        return archive_meta_store_.GetInode(route, inode, &attr, error) &&
               BuildOpticalFileLocationFromAttr(attr, inode, out, error);
    });
}
} // namespace zb::mds
