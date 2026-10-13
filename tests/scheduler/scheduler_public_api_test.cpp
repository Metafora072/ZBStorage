#include "scheduler/api/SchedulerClient.h"
#include "scheduler/health/FailureDetector.h"
#include "scheduler/model/ClusterState.h"
#include "scheduler/power/NodePowerManager.h"
#include "scheduler/service/SchedulerServiceImpl.h"

#include <brpc/server.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

namespace {

int PickLoopbackPort() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    const bool bound = bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
    socklen_t length = sizeof(address);
    const bool measured = bound &&
        getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) == 0;
    close(fd);
    return measured ? ntohs(address.sin_port) : -1;
}

bool Check(bool condition, const std::string& message) {
    if (!condition) std::cerr << "FAIL: " << message << '\n';
    return condition;
}

} // namespace

int main() {
    zb::scheduler::FailureDetector detector(6000, 15000);
    zb::scheduler::NodePowerManager manager;
    zb::scheduler::ClusterState cluster(detector, &manager);
    zb::scheduler::SchedulerServiceImpl service(&cluster, nullptr, &manager);
    brpc::Server server;
    const int port = PickLoopbackPort();
    if (!Check(port > 0 && server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0 &&
                   server.Start(port, nullptr) == 0,
               "start real Scheduler BRPC service")) return 1;

    zb::scheduler::api::SchedulerClient client("127.0.0.1:" + std::to_string(port), 1000);
    std::string error;
    if (!Check(client.Connect(&error), "connect: " + error)) return 2;

    for (const auto* id : {"integration-storage-1", "integration-storage-2"}) {
        zb::rpc::RegisterManagedNodeRequest request;
        auto* spec = request.mutable_spec();
        spec->set_node_id(id);
        spec->set_kind(zb::rpc::MANAGED_KIND_STORAGE);
        spec->set_execution_mode(zb::rpc::MANAGED_EXECUTION_SIMULATED);
        spec->set_logical_node_count(1);
        spec->set_participates_in_placement_set(true);
        spec->set_participates_in_placement(false);
        request.set_join_time_ms(1000);
        request.set_activate_immediately(true);
        zb::rpc::ManagedNodeReply response;
        if (!Check(client.RegisterManagedNode(request, &response, &error),
                   std::string("register ") + id + ": " + error) ||
            !Check(response.node().spec().node_id() == id, "registered identity")) return 3;
    }

    zb::rpc::ListManagedNodesRequest filter;
    filter.set_include_retired(true);
    filter.set_limit(1);
    std::vector<zb::rpc::ManagedNodeView> nodes;
    uint64_t generation = 0;
    if (!Check(client.ListAllManagedNodes(filter, &nodes, &generation, &error),
               "paged list: " + error) ||
        !Check(nodes.size() == 2 && generation != 0, "paged list has both nodes")) return 4;

    zb::rpc::GetManagedNodeRequest get;
    get.set_node_id("integration-storage-1");
    zb::rpc::ManagedNodeReply node;
    if (!Check(client.GetManagedNode(get, &node, &error), "get node: " + error) ||
        !Check(node.node().lifecycle() == zb::rpc::MANAGED_LIFECYCLE_WORKING,
               "managed node state")) return 5;

    get.set_node_id("not-present");
    if (!Check(!client.GetManagedNode(get, &node, &error) &&
                   node.status().code() == zb::rpc::SCHED_NOT_FOUND &&
                   error.find("GetManagedNode") != std::string::npos,
               "application error is not mistaken for transport success")) return 6;
    if (!Check(!client.GetManagedNode(get, nullptr, &error) &&
                   error.find("response is null") != std::string::npos,
               "null output is rejected")) return 7;

    server.Stop(0);
    server.Join();
    get.set_node_id("integration-storage-1");
    if (!Check(!client.GetManagedNode(get, &node, &error) &&
                   error.find("transport failure") != std::string::npos,
               "transport failure is distinct from Scheduler status")) return 8;
    std::cout << "Scheduler public client BRPC contract: PASS\n";
    return 0;
}
