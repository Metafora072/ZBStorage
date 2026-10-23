#include "SubtreeArchive.h"
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <set>

namespace zb::mds {
namespace {
// List input adapts to the same bounded manifest/validation path as directory scans.
class InodeListScanner final : public NamespaceScanner {
public:
    InodeListScanner(const NamespaceStore& store, std::vector<uint64_t> ids)
        : store_(store), ids_(std::move(ids)) {}
    NamespaceStatus Next(uint32_t limit, SubtreeScanBatch* out) override {
        *out = {};
        for (uint32_t n = 0; n < limit && position_ < ids_.size(); ++n) {
            zb::rpc::InodeAttr attr; std::string error;
            if (!store_.GetInode(ids_[position_], &attr, &error)) {
                *out = {};
                return {error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                        error.empty() ? "archive inode missing: " + std::to_string(ids_[position_]) : error};
            }
            out->entries.push_back(std::move(attr)); ++position_;
        }
        out->done = position_ == ids_.size();
        return {};
    }
private:
    const NamespaceStore& store_;
    std::vector<uint64_t> ids_;
    size_t position_ = 0;
};
}
NamespaceStatus ArchiveManifestBuilder::BeginInodes(const NamespaceStore& store,
    const FileLocationResolver& resolver, const std::vector<uint64_t>& ids,
    std::unique_ptr<ArchiveManifestBuilder>* out) {
    if (!out) return {zb::rpc::MDS_INVALID_ARGUMENT, "manifest output required"};
    out->reset();
    if (ids.empty()) return {zb::rpc::MDS_INVALID_ARGUMENT, "nonempty inode list required"};
    std::set<uint64_t> seen; std::vector<uint64_t> unique;
    for (auto id : ids) {
        if (!id) return {zb::rpc::MDS_INVALID_ARGUMENT, "zero inode ID"};
        if (seen.insert(id).second) unique.push_back(id);
    }
    out->reset(new ArchiveManifestBuilder(resolver,
        std::make_unique<InodeListScanner>(store, std::move(unique))));
    return {};
}

NamespaceStatus ArchiveManifestBuilder::Begin(const NamespaceStore& store,
    const FileLocationResolver& resolver, const std::string& root, uint32_t budget,
    std::unique_ptr<ArchiveManifestBuilder>* out) {
    if (!out) return {zb::rpc::MDS_INVALID_ARGUMENT, "manifest output required"};
    out->reset();
    std::unique_ptr<NamespaceScanner> scan;
    auto status = store.BeginSubtreeScan(root, {true, budget}, &scan);
    if (!status.ok()) return status;
    out->reset(new ArchiveManifestBuilder(resolver, std::move(scan)));
    return {};
}
NamespaceStatus ArchiveManifestBuilder::Next(uint32_t max_files, ArchiveBatch* out) {
    if (out) *out = {};
    if (!out || !max_files || max_files > 4096)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "max_files must be 1..4096"};
    if (!failure_.ok()) return failure_;
    SubtreeScanBatch batch;
    auto status = scan_->Next(max_files, &batch);
    if (!status.ok()) return failure_ = status;
    ArchiveBatch result;
    for (const auto& attr : batch.entries) {
        auto fail = [&](NamespaceStatus error) {
            error.message = "inode " + std::to_string(attr.inode_id()) + ": " + error.message;
            return failure_ = error;
        };
        // The old enum is only an exclusion guard, not the new archive state machine.
        if (attr.type() != zb::rpc::INODE_FILE || attr.storage_tier() != zb::rpc::INODE_STORAGE_DISK
            || attr.replica() != 1 || attr.file_archive_state() != zb::rpc::INODE_ARCHIVE_PENDING)
            return fail({zb::rpc::MDS_INVALID_ARGUMENT, "requires a pending, single-replica hot file"});
        if (attr.size() && !attr.object_unit_size())
            return fail({zb::rpc::MDS_INVALID_ARGUMENT, "nonempty file has zero object_unit_size"});
        zb::rpc::FileLocationView location;
        status = resolver_.Resolve(attr.inode_id(), &location);
        if (!status.ok()) return fail(status);
        if (location.attr().SerializeAsString() != attr.SerializeAsString())
            return fail({zb::rpc::MDS_INVALID_ARGUMENT, "inode changed during archive scan"});
        if (!location.has_disk_location() || location.disk_location().node_id().empty()
            || location.disk_location().disk_id().empty())
            return fail({zb::rpc::MDS_NOT_FOUND, "resolved disk location is missing"});
        zb::rpc::ArchiveFile file;
        file.set_inode_id(attr.inode_id()); file.set_size(attr.size());
        file.set_object_unit_size(attr.object_unit_size());
        file.set_target_node_id(location.disk_location().node_id());
        file.set_target_disk_id(location.disk_location().disk_id());
        result.files.push_back(std::move(file));
    }
    result.done = batch.done;
    *out = std::move(result);
    return {};
}

namespace {
NamespaceStatus SendManifest(std::unique_ptr<ArchiveManifestBuilder> manifest,
    uint64_t batch_id, uint32_t max_files, ArchiveMetadataSink& sink, ArchiveSendProgress* progress) {
    NamespaceStatus status;
    for (;;) {
        ArchiveBatch batch;
        status = manifest->Next(max_files, &batch);
        if (!status.ok()) return status;
        if (!batch.files.empty()) {
            zb::rpc::SendArchiveMetadataRequest request;
            request.set_batch_id(batch_id);
            for (auto& file : batch.files) *request.add_files() = std::move(file);
            if (request.ByteSizeLong() > 4 * 1024 * 1024)
                return {zb::rpc::MDS_INVALID_ARGUMENT, "archive metadata chunk exceeds 4 MiB"};
            ++progress->attempted_batches;
            status = sink.Send(request, &progress->last_batch_may_be_accepted);
            if (!status.ok()) return status;
            ++progress->acknowledged_batches;
            progress->acknowledged_files += request.files_size();
            progress->last_batch_may_be_accepted = false;
        }
        if (batch.done) {
            if (progress->acknowledged_files) {
                zb::rpc::SendArchiveMetadataRequest end;
                end.set_batch_id(batch_id); end.set_end_of_batch(true);
                return sink.Send(end, &progress->last_batch_may_be_accepted);
            }
            return {};
        }
    }
}
} // namespace
NamespaceStatus SendArchiveSubtree(const NamespaceStore& store, const FileLocationResolver& resolver,
    const std::string& root, uint64_t batch_id, uint32_t max_files, uint32_t budget,
    ArchiveMetadataSink& sink, ArchiveSendProgress* progress) {
    if (progress) *progress = {};
    if (!progress || !batch_id || !max_files || max_files > 4096 || !budget || budget > 65536)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid archive batch id or limits"};
    std::unique_ptr<ArchiveManifestBuilder> manifest;
    auto status = ArchiveManifestBuilder::Begin(store, resolver, root, budget, &manifest);
    if (!status.ok()) return status;
    return SendManifest(std::move(manifest), batch_id, max_files, sink, progress);
}
NamespaceStatus SendArchiveInodes(const NamespaceStore& store, const FileLocationResolver& resolver,
    const std::vector<uint64_t>& ids, uint64_t batch_id, uint32_t max_files,
    ArchiveMetadataSink& sink, ArchiveSendProgress* progress) {
    if (progress) *progress = {};
    if (!progress || !batch_id || !max_files || max_files > 4096)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid archive batch id or limits"};
    std::unique_ptr<ArchiveManifestBuilder> manifest;
    auto status = ArchiveManifestBuilder::BeginInodes(store, resolver, ids, &manifest);
    if (!status.ok()) return status;
    return SendManifest(std::move(manifest), batch_id, max_files, sink, progress);
}
namespace {
class BrpcArchiveSink final : public ArchiveMetadataSink {
public:
    brpc::Channel channel;
    NamespaceStatus Send(const zb::rpc::SendArchiveMetadataRequest& request, bool* uncertain) override {
        *uncertain = true;
        zb::rpc::OpticalNodeService_Stub stub(&channel);
        brpc::Controller controller;
        zb::rpc::ArchiveReply reply;
        stub.SendArchiveMetadata(&controller, &request, &reply, nullptr);
        if (controller.Failed()) return {zb::rpc::MDS_INTERNAL_ERROR, controller.ErrorText()};
        if (!reply.has_status()) return {zb::rpc::MDS_INTERNAL_ERROR, "optical reply has no status"};
        if (reply.status().code() != zb::rpc::MDS_OK)
            return {reply.status().code(), reply.status().message()};
        if (reply.admissions_size()) {
            std::set<uint64_t> expected;
            for (const auto& file : request.files()) expected.insert(file.inode_id());
            for (const auto& admission : reply.admissions()) {
                if (!admission.has_status() || admission.status().code() != zb::rpc::MDS_OK
                    || !expected.erase(admission.inode_id()))
                    return {zb::rpc::MDS_INTERNAL_ERROR, "invalid or failed file admission"};
            }
            if (!expected.empty()) return {zb::rpc::MDS_INTERNAL_ERROR, "incomplete file admissions"};
        }
        *uncertain = false;
        return {};
    }
};
}
NamespaceStatus MakeOpticalArchiveSink(const std::string& address, uint32_t timeout,
    std::unique_ptr<ArchiveMetadataSink>* out) {
    if (out) out->reset();
    if (!out || address.empty() || address.find('\0') != std::string::npos || !timeout || timeout > 60000)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "invalid optical address or timeout (1..60000 ms)"};
    auto sink = std::make_unique<BrpcArchiveSink>();
    brpc::ChannelOptions options;
    options.timeout_ms = timeout; options.connect_timeout_ms = timeout; options.max_retry = 0;
    if (sink->channel.Init(address.c_str(), &options) != 0)
        return {zb::rpc::MDS_INVALID_ARGUMENT, "cannot initialize optical channel"};
    *out = std::move(sink);
    return {};
}
}
