#include "MdsServiceImpl.h"
#include "mds/archive/SubtreeArchive.h"
#include <brpc/closure_guard.h>

namespace zb::mds {
void MdsServiceImpl::SendSubtreeArchive(google::protobuf::RpcController*,
    const zb::rpc::SendSubtreeArchiveRequest* request, zb::rpc::SendSubtreeArchiveReply* response,
    google::protobuf::Closure* done) {
    brpc::ClosureGuard guard(done);
    if (!response) return;
    response->Clear();
    if (!request || !store_ || !namespace_store_ || !location_resolver_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "archive request/store required");
        return;
    }
    if (request->root_path().empty() == request->inode_ids().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT,
                   "provide exactly one of root_path or nonempty inode_ids");
        return;
    }
    std::unique_ptr<ArchiveMetadataSink> sink;
    auto status = MakeOpticalArchiveSink(request->optical_node_address(),
        request->rpc_timeout_ms() ? request->rpc_timeout_ms() : 5000, &sink);
    ArchiveSendProgress progress;
    if (status.ok()) {
        const auto max_files = request->max_files_per_batch() ? request->max_files_per_batch() : 256;
        if (!request->inode_ids().empty()) {
            const std::vector<uint64_t> ids(request->inode_ids().begin(), request->inode_ids().end());
            status = SendArchiveInodes(*namespace_store_, *location_resolver_, ids,
                                      request->batch_id(), max_files, *sink, &progress);
        } else {
            status = SendArchiveSubtree(*namespace_store_, *location_resolver_, request->root_path(),
                request->batch_id(), max_files,
                request->scan_work_budget() ? request->scan_work_budget() : 1024, *sink, &progress);
        }
    }
    response->set_acknowledged_files(progress.acknowledged_files);
    response->set_acknowledged_batches(progress.acknowledged_batches);
    response->set_attempted_batches(progress.attempted_batches);
    response->set_last_batch_may_be_accepted(progress.last_batch_may_be_accepted);
    FillStatus(response->mutable_status(), status.code,
        status.ok() ? "metadata accepted; image/disc completion is not implied" : status.message);
}
}
