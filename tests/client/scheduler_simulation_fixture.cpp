// External archive caller for the client demo. Uses only public RPCs: no data
// writes, database edits or model service implementations live in this process.
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <google/protobuf/util/json_util.h>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include "mds.pb.h"
#include "optical_node.pb.h"
#include "common/rpc/LogicalNodeRoute.h"

namespace rpc = zb::rpc;
using Clock = std::chrono::steady_clock;
void Require(bool ok, const std::string& message) {
    if (!ok) throw std::runtime_error(message);
}
void Connect(brpc::Channel& channel, const std::string& address) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std"; options.timeout_ms = 5000; options.max_retry = 0;
    Require(channel.Init(address.c_str(), &options) == 0, "channel: " + address);
}
template<class Reply> void Check(const brpc::Controller& c, const Reply& reply, const char* name) {
    Require(!c.Failed(), std::string(name) + ": " + c.ErrorText());
    Require(reply.has_status() && reply.status().code() == rpc::MDS_OK,
            std::string(name) + ": " + reply.status().message());
}
rpc::FileLocationView Location(rpc::MdsService_Stub& mds, uint64_t inode) {
    rpc::GetFileLocationRequest request; request.set_inode_id(inode);
    rpc::GetFileLocationReply reply; brpc::Controller c;
    mds.GetFileLocation(&c, &request, &reply, nullptr); Check(c, reply, "GetFileLocation");
    return reply.location();
}
int main(int argc, char** argv) {
    if (argc != 5 && argc != 6) {
        std::cerr << "Usage: scheduler_simulation_fixture MDS PATH OPTICAL_NODE DISC_ID [WAIT_MS]\n";
        return 2;
    }
    try {
        const uint64_t wait_ms = argc == 6 ? std::stoull(argv[5]) : 60000;
        Require(wait_ms > 0 && wait_ms <= 3600000, "WAIT_MS must be 1..3600000");
        brpc::Channel mds_channel; Connect(mds_channel, argv[1]); rpc::MdsService_Stub mds(&mds_channel);
        rpc::LookupRequest lookup; lookup.set_path(argv[2]);
        rpc::LookupReply found; brpc::Controller lookup_c;
        mds.Lookup(&lookup_c, &lookup, &found, nullptr); Check(lookup_c, found, "Lookup");
        const auto inode = found.attr().inode_id();
        auto location = Location(mds, inode);
        Require(location.has_disk_location() && location.attr().storage_tier() == rpc::INODE_STORAGE_DISK,
                "archive requires an existing disk file written through FUSE");
        Require(location.attr().size() > 0, "demo archive requires a nonempty file");
        const auto source = location.disk_location();
        rpc::GetCmsNodeCatalogRequest catalog_request; rpc::GetCmsNodeCatalogReply catalog;
        brpc::Controller catalog_c;
        mds.GetCmsNodeCatalog(&catalog_c, &catalog_request, &catalog, nullptr); Check(catalog_c, catalog, "CMS catalog");
        const rpc::CmsNodeCatalogEntry* target = nullptr;
        bool model_source = false;
        for (const auto& node : catalog.nodes()) {
            const auto& spec = node.managed_view().spec();
            if (spec.node_id() == argv[3]) target = &node;
            if (spec.node_id() == source.node_id() && spec.execution_mode() == rpc::MANAGED_EXECUTION_VIRTUAL &&
                spec.instance_generation() == source.instance_generation()) model_source = true;
        }
        Require(model_source, "source is not an admitted model disk with the expected generation");
        Require(target && target->placement_view().node_type() == rpc::NODE_OPTICAL &&
                target->managed_view().spec().execution_mode() == rpc::MANAGED_EXECUTION_VIRTUAL,
                "target must be a CMS-published model optical node (physical archive is not supported by this fixture)");
        const auto generation = target->managed_view().spec().instance_generation();
        Require(generation > 0 && !target->placement_view().address().empty(), "invalid target route");
        const std::string operation = "client-demo-" + std::to_string(inode) + "-" + std::to_string(getpid()) + "-" +
            std::to_string(Clock::now().time_since_epoch().count());
        rpc::ClaimArchiveTaskRequest claim;
        claim.set_inode_id(inode); claim.set_node_id(source.node_id()); claim.set_disk_id(source.disk_id());
        claim.set_requested_lease_ms(300000); claim.set_request_id(operation);
        rpc::ClaimArchiveTaskReply lease; brpc::Controller claim_c;
        mds.ClaimArchiveTask(&claim_c, &claim, &lease, nullptr); Check(claim_c, lease, "ClaimArchiveTask");
        Require(lease.granted(), "archive lease was not granted");
        brpc::Channel optical_channel; Connect(optical_channel, target->placement_view().address());
        rpc::OpticalNodeService_Stub optical(&optical_channel);
        rpc::SendArchiveMetadataRequest archive; archive.set_batch_id(inode);
        auto* file = archive.add_files();
        file->set_inode_id(inode); file->set_size(location.attr().size());
        file->set_object_unit_size(location.attr().object_unit_size()); file->set_target_node_id(argv[3]);
        file->set_operation_id(operation); file->set_source_lease_id(lease.record().lease_id());
        file->set_source_node_id(source.node_id()); file->set_source_disk_id(source.disk_id());
        file->set_source_version(location.attr().version()); file->set_disc_id(argv[4]);
        file->set_image_id(operation);
        rpc::ArchiveReply accepted; brpc::Controller archive_c;
        Require(zb::rpc_route::SetLogicalNodeRoute(&archive_c, argv[3], generation), "invalid logical route");
        optical.SendArchiveMetadata(&archive_c, &archive, &accepted, nullptr); Check(archive_c, accepted, "SendArchiveMetadata");
        Require(accepted.admissions_size() == 1 && accepted.admissions(0).status().code() == rpc::MDS_OK,
                "file archive admission failed");
        std::cerr << "archive accepted: inode=" << inode << " source=" << source.node_id()
                  << " target=" << argv[3] << " generation=" << generation << '\n';
        const auto deadline = Clock::now() + std::chrono::milliseconds(wait_ms);
        // Poll only the archive/MDS position transition. Optical read readiness
        // uses the production push stream and is never polled by this fixture.
        do {
            location = Location(mds, inode);
            if (location.attr().storage_tier() == rpc::INODE_STORAGE_OPTICAL && location.has_optical_location() &&
                location.optical_location().node_id() == argv[3] &&
                location.optical_location().instance_generation() == generation) {
                std::string json;
                google::protobuf::util::JsonPrintOptions options; options.preserve_proto_field_names = true;
                Require(google::protobuf::util::MessageToJsonString(location, &json, options).ok(), "serialize location");
                std::cout << json << '\n'; return 0;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } while (Clock::now() < deadline);
        throw std::runtime_error("archive position not published before deadline; inspect Scheduler/MDS logs");
    } catch (const std::exception& e) {
        std::cerr << "archive fixture: " << e.what() << '\n'; return 1;
    }
}
