#include "CmsNodeCatalogPublisher.h"

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <unordered_map>
#include <limits>
#include <utility>

#include "../model/ClusterState.h"
#include "../power/NodePowerManager.h"
#include "../service/ManagedNodeProtoAdapter.h"
#include "mds.pb.h"

namespace zb::scheduler {

CmsNodeCatalogPublisher::CmsNodeCatalogPublisher(std::string mds_address,
                                                 uint32_t timeout_ms)
    : mds_address_(std::move(mds_address)), timeout_ms_(timeout_ms == 0 ? 2000 : timeout_ms) {}

CmsNodeCatalogPublisher::~CmsNodeCatalogPublisher() = default;

bool CmsNodeCatalogPublisher::EnsureChannelLocked(std::string* error) {
    if (channel_) return true;
    if (mds_address_.empty()) {
        if (error) *error = "CMS address is empty";
        return false;
    }
    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = static_cast<int>(timeout_ms_);
    options.max_retry = 0;
    if (channel->Init(mds_address_.c_str(), &options) != 0) {
        if (error) *error = "failed to initialize CMS channel: " + mds_address_;
        return false;
    }
    channel_ = std::move(channel);
    return true;
}

bool CmsNodeCatalogPublisher::ReconcileGenerationLocked(NodePowerManager* nodes,
                                                         std::string* error) {
    if (generation_reconciled_) return true;
    zb::rpc::GetCmsNodeCatalogRequest request;
    zb::rpc::GetCmsNodeCatalogReply response;
    brpc::Controller controller;
    zb::rpc::MdsService_Stub stub(channel_.get());
    stub.GetCmsNodeCatalog(&controller, &request, &response, nullptr);
    if (controller.Failed()) {
        if (error) *error = "CMS catalog read RPC failed: " + controller.ErrorText();
        channel_.reset();
        return false;
    }
    if (response.status().code() != zb::rpc::MDS_OK) {
        if (error) *error = "CMS rejected catalog read: " + response.status().message();
        return false;
    }
    std::unordered_map<std::string, ManagedNodeRuntime> local;
    for (const auto& node : nodes->ListNodes()) local.emplace(node.profile.node_id, node);
    for (const auto& entry : response.nodes()) {
        if (!entry.has_managed_view() || !entry.managed_view().has_spec()) {
            if (error) *error = "CMS catalog contains an invalid managed node";
            return false;
        }
        const auto& committed = entry.managed_view();
        const auto& id = committed.spec().node_id();
        auto found = local.find(id);
        if (found == local.end()) {
            if (error) *error = "Scheduler snapshot omits CMS member: " + id;
            return false;
        }
        zb::rpc::ManagedNodeView current;
        FillManagedNodeView(found->second, &current);
        const auto old_instance = committed.spec().instance_generation();
        const auto new_instance = current.spec().instance_generation();
        if (committed.spec().kind() != current.spec().kind() ||
            (old_instance == 0 ? 1 : old_instance) !=
                (new_instance == 0 ? 1 : new_instance) ||
            current.lifecycle() < committed.lifecycle()) {
            if (error) *error = "Scheduler snapshot conflicts with CMS member: " + id;
            return false;
        }
    }
    const uint64_t source = nodes->generation();
    const uint64_t committed = response.scheduler_generation();
    if (source <= committed) {
        if (committed == std::numeric_limits<uint64_t>::max()) {
            if (error) *error = "CMS scheduler generation exhausted";
            return false;
        }
        generation_offset_ = committed - source + 1;
    }
    last_cms_generation_ = response.cms_generation();
    generation_reconciled_ = true;
    return true;
}

bool CmsNodeCatalogPublisher::Publish(ClusterState* cluster,
                                      NodePowerManager* nodes,
                                      std::string* error,
                                      std::vector<std::string>* committed_retired_ids) {
    if (committed_retired_ids) committed_retired_ids->clear();
    if (!cluster || !nodes) {
        if (error) *error = "CMS catalog publisher is missing Scheduler state";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!EnsureChannelLocked(error)) return false;
    if (!ReconcileGenerationLocked(nodes, error)) return false;

    // A first CMS commit may assign compact IDs and increment the local
    // generation. Publish once more so the committed CMS view contains them.
    for (int attempt = 0; attempt < 2; ++attempt) {
        uint64_t placement_generation = 0;
        std::vector<NodeState> placement_nodes;
        cluster->Snapshot(0, &placement_generation, &placement_nodes);
        std::unordered_map<std::string, NodeState> placement_by_id;
        for (auto& node : placement_nodes) placement_by_id.emplace(node.node_id, std::move(node));

        const uint64_t source_generation = nodes->generation();
        zb::rpc::CommitCmsNodeCatalogRequest request;
        if (source_generation > std::numeric_limits<uint64_t>::max() - generation_offset_) {
            if (error) *error = "Scheduler catalog generation exhausted";
            return false;
        }
        request.set_scheduler_generation(source_generation + generation_offset_);
        std::vector<std::string> retired_in_request;
        for (const auto& node : nodes->ListNodes()) {
            auto* entry = request.add_nodes();
            FillManagedNodeView(node, entry->mutable_managed_view());
            if (node.lifecycle == LifecycleState::kRetired)
                retired_in_request.push_back(node.profile.node_id);
            auto found = placement_by_id.find(node.profile.node_id);
            if (found != placement_by_id.end()) {
                FillLegacyNodeView(found->second, entry->mutable_placement_view());
            }
        }

        zb::rpc::CommitCmsNodeCatalogReply response;
        brpc::Controller controller;
        zb::rpc::MdsService_Stub stub(channel_.get());
        stub.CommitCmsNodeCatalog(&controller, &request, &response, nullptr);
        if (controller.Failed()) {
            if (error) *error = "CMS catalog commit RPC failed: " + controller.ErrorText();
            channel_.reset();
            return false;
        }
        if (response.status().code() != zb::rpc::MDS_OK) {
            if (error) *error = "CMS rejected node catalog: " + response.status().message();
            return false;
        }
        last_cms_generation_ = response.cms_generation();
        for (const auto& assignment : response.assignments()) {
            std::string assignment_error;
            if (!nodes->AssignCmsCompactId(assignment.node_id(), assignment.compact_id(),
                                           &assignment_error)) {
                if (error) *error = "failed to apply CMS compact id: " + assignment_error;
                return false;
            }
        }
        if (committed_retired_ids) *committed_retired_ids = std::move(retired_in_request);
        if (nodes->generation() == source_generation) return true;
    }
    // The second commit is already a valid CMS snapshot. A concurrent
    // heartbeat may advance Scheduler again while this RPC is in flight; the
    // next periodic publish carries that newer generation.
    return true;
}

uint64_t CmsNodeCatalogPublisher::last_cms_generation() const {
    std::lock_guard<std::mutex> lock(mu_);
    return last_cms_generation_;
}

} // namespace zb::scheduler
