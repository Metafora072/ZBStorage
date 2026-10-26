#include "MdsServiceImpl.h"
#include <brpc/closure_guard.h>
#include <limits>
#include <unordered_set>

namespace zb::mds {
void MdsServiceImpl::AllocateAvailableImageId(google::protobuf::RpcController*,
    const zb::rpc::AllocateAvailableImageIdRequest* r, zb::rpc::AllocateAvailableImageIdReply* out,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    out->Clear();
    std::lock_guard<std::mutex> lock(simple_archive_mutex_);
    if (!r || r->node_id().empty() || r->node_address().empty()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "node id/address required"); return;
    }
    // Existing inode codec stores a uint32 image id. Never silently truncate.
    if (next_simple_archive_id_ > std::numeric_limits<uint32_t>::max()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive ID range exhausted"); return;
    }
    const auto id = next_simple_archive_id_++;
    const auto key = "img-" + std::to_string(id);
    simple_images_.emplace(key, SimpleArchiveAllocation{id, r->node_id()});
    out->set_image_id(key); FillStatus(out->mutable_status(), zb::rpc::MDS_OK, "OK");
}
void MdsServiceImpl::AllocateAvailableDiscId(google::protobuf::RpcController*,
    const zb::rpc::AllocateAvailableDiscIdRequest* r, zb::rpc::AllocateAvailableDiscIdReply* out,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    out->Clear();
    std::lock_guard<std::mutex> lock(simple_archive_mutex_);
    if (!r || r->node_id().empty() || r->node_address().empty()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "node id/address required"); return;
    }
    if (next_simple_archive_id_ > std::numeric_limits<uint32_t>::max()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive ID range exhausted"); return;
    }
    const auto id = next_simple_archive_id_++;
    const auto key = "disc-" + std::to_string(id);
    simple_discs_.emplace(key, SimpleArchiveAllocation{id, r->node_id()});
    out->set_disc_id(key); FillStatus(out->mutable_status(), zb::rpc::MDS_OK, "OK");
}
void MdsServiceImpl::ReportFilesPackedToImage(google::protobuf::RpcController*,
    const zb::rpc::ReportFilesPackedToImageRequest* r, zb::rpc::SimpleArchiveReportReply* out,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    out->Clear();
    std::lock_guard<std::mutex> lock(simple_archive_mutex_);
    if (!r || !namespace_store_ || r->inode_ids().empty()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "files/store required"); return;
    }
    const auto image = simple_images_.find(r->image_id());
    if (image == simple_images_.end() || image->second.node_id != r->node_id()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "image not allocated to node"); return;
    }
    for (const auto id : r->inode_ids()) {
        zb::rpc::InodeAttr attr; std::string error;
        if (!namespace_store_->GetInode(id, &attr, &error)) {
            FillStatus(out->mutable_status(), error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "inode missing" : error); return;
        }
        if (attr.type() != zb::rpc::INODE_FILE) {
            FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "expected file inode"); return;
        }
        attr.set_optical_image_id(image->second.id);
        attr.set_optical_disk_id(0);
        attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_IMAGE_READY);
        if (!namespace_store_->PutInode(id, attr, &error)) {
            FillStatus(out->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error); return;
        }
        out->set_updated_files(out->updated_files() + 1);
    }
    FillStatus(out->mutable_status(), zb::rpc::MDS_OK, "OK");
}
void MdsServiceImpl::ReportImagesBurnedToDisc(google::protobuf::RpcController*,
    const zb::rpc::ReportImagesBurnedToDiscRequest* r, zb::rpc::SimpleArchiveReportReply* out,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    out->Clear();
    std::lock_guard<std::mutex> lock(simple_archive_mutex_);
    if (!r || !namespace_store_ || r->image_ids().empty()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "images/store required"); return;
    }
    const auto disc = simple_discs_.find(r->disc_id());
    if (disc == simple_discs_.end() || disc->second.node_id != r->node_id()) {
        FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "disc not allocated to node"); return;
    }
    std::unordered_set<uint64_t> images;
    for (const auto& key : r->image_ids()) {
        const auto it = simple_images_.find(key);
        if (it == simple_images_.end() || it->second.node_id != r->node_id()) {
            FillStatus(out->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "image not allocated to node"); return;
        }
        images.insert(it->second.id);
    }
    // Deliberate O(namespace) baseline until a reverse index is designed.
    std::unique_ptr<NamespaceScanner> scan;
    auto status = namespace_store_->BeginSubtreeScan("/", {true, 1024}, &scan);
    while (status.ok()) {
        SubtreeScanBatch batch; status = scan->Next(256, &batch);
        if (!status.ok()) break;
        for (auto attr : batch.entries) {
            if (!images.count(attr.optical_image_id())) continue;
            attr.set_optical_disk_id(disc->second.id);
            attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_ARCHIVED);
            std::string error;
            if (!namespace_store_->PutInode(attr.inode_id(), attr, &error)) {
                status = {zb::rpc::MDS_INTERNAL_ERROR, error}; break;
            }
            out->set_updated_files(out->updated_files() + 1);
        }
        if (!status.ok() || batch.done) break;
    }
    FillStatus(out->mutable_status(), status.code, status.message);
}
}
