#include "scheduler/health/FailureDetector.h"
#include "scheduler/lifecycle/LifecycleManager.h"
#include "scheduler/migration/DrainMigrationCoordinator.h"

#include <iostream>

namespace {

bool Expect(bool condition, const std::string& message) {
    if (!condition) std::cerr << "FAIL: " << message << '\n';
    return condition;
}

class FakeActuator : public zb::scheduler::NodeActuator {
public:
    zb::scheduler::ActuatorResult StartNode(const std::string&, const std::string&) override {
        return {true, "started"};
    }
    zb::scheduler::ActuatorResult StopNode(const std::string&, const std::string&, bool) override {
        stopped = true;
        return {true, "stopped"};
    }
    zb::scheduler::ActuatorResult RebootNode(const std::string&, const std::string&) override {
        return {true, "rebooted"};
    }
    bool stopped{false};
};

class FakeBackend : public zb::scheduler::ObjectMigrationBackend {
public:
    bool ListObjects(const std::string&, std::vector<zb::scheduler::MigrationObject>* out,
                     std::string*) override {
        ++list_calls;
        if (deletes >= 2 && !retain_after_delete) {
            out->clear();
        } else if (deletes >= 2) {
            *out = {{"disk-a", "obj-99-0", 0, 0, 1}};
        } else {
            *out = {{"disk-a", "obj-42-0", 0, 0, 4},
                    {"disk-a", "obj-42-1", 0, 0, 6}};
        }
        return true;
    }
    bool CopyAndVerify(const std::string&, const zb::scheduler::MigrationObject&,
                       const zb::scheduler::MigrationTarget&, std::string* error) override {
        if (fail_copy) {
            if (error) *error = "injected copy failure";
            return false;
        }
        ++copies;
        return true;
    }
    bool CommitFileLocation(const std::string&, uint64_t inode_id, const std::string&,
                            const std::string&, const std::string&,
                            const zb::scheduler::MigrationTarget&, std::string*) override {
        committed_inode = inode_id;
        return true;
    }
    bool VerifyCmsNodeReferencesZero(const std::string&,
                                     uint64_t* disk_references,
                                     uint64_t* optical_references,
                                     std::string* error) override {
        if (disk_references) *disk_references = cms_references;
        if (optical_references) *optical_references = 0;
        if (cms_references != 0) {
            if (error) *error = "injected CMS reference";
            return false;
        }
        return true;
    }
    bool DeleteSourceObject(const std::string&, const zb::scheduler::MigrationObject&,
                            std::string*) override {
        ++deletes;
        return true;
    }
    bool fail_copy{false};
    bool retain_after_delete{false};
    uint64_t cms_references{0};
    uint32_t list_calls{0};
    uint32_t copies{0};
    uint32_t deletes{0};
    uint64_t committed_inode{0};
};

void Heartbeat(zb::scheduler::ClusterState* cluster, const std::string& id,
               const std::string& address, const std::string& disk) {
    zb::rpc::HeartbeatRequest request;
    request.set_node_id(id);
    request.set_node_type(zb::rpc::NODE_REAL);
    request.set_address(address);
    request.set_report_ts_ms(1);
    request.set_readiness_reported(true);
    request.set_initialization_complete(true);
    request.set_metadata_ready(true);
    auto* report = request.add_disks();
    report->set_disk_id(disk);
    report->set_capacity_bytes(1000);
    report->set_free_bytes(900);
    report->set_is_healthy(true);
    cluster->ReportHeartbeat(request);
}

bool RunSuccessCase() {
    using namespace zb::scheduler;
    NodePowerManager nodes;
    ClusterState cluster(FailureDetector(10000, 20000), &nodes);
    Heartbeat(&cluster, "source", "source:1", "disk-a");
    Heartbeat(&cluster, "target", "target:1", "disk-b");
    std::string error;
    nodes.SetResidentDataBytes("source", 10, nullptr);
    nodes.BeginRemoveNode("source", 2, nullptr);
    FakeActuator actuator;
    LifecycleManager lifecycle(&cluster, &actuator);
    FakeBackend backend;
    DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend, 1, 3);
    if (!Expect(coordinator.Start("source", 2, &error), error)) return false;
    coordinator.Tick(3);  // enumerate
    coordinator.Tick(4);  // copy, commit, cleanup, stop, retire
    ManagedNodeRuntime source;
    DrainMigrationTask task;
    nodes.GetNode("source", &source);
    coordinator.GetTask("source", &task);
    return Expect(backend.copies == 2, "all objects copied and verified") &&
           Expect(backend.committed_inode == 42, "MDS committed after copy") &&
           Expect(backend.deletes == 2, "source objects deleted after commit") &&
           Expect(backend.list_calls == 2, "source is re-enumerated before retirement") &&
           Expect(actuator.stopped, "physical lifecycle stopped after drain") &&
           Expect(source.lifecycle == LifecycleState::kRetired, "source retired automatically") &&
           Expect(task.state == DrainMigrationState::kCompleted, "migration completed");
}

bool RunFinalVerificationFailureCase() {
    using namespace zb::scheduler;
    NodePowerManager nodes;
    ClusterState cluster(FailureDetector(10000, 20000), &nodes);
    Heartbeat(&cluster, "source-v", "source-v:1", "disk-a");
    Heartbeat(&cluster, "target-v", "target-v:1", "disk-b");
    nodes.BeginRemoveNode("source-v", 2, nullptr);
    FakeActuator actuator;
    LifecycleManager lifecycle(&cluster, &actuator);
    FakeBackend backend;
    backend.retain_after_delete = true;
    DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend, 10, 3);
    std::string error;
    coordinator.Start("source-v", 2, &error);
    coordinator.Tick(3);
    coordinator.Tick(4);
    DrainMigrationTask task;
    ManagedNodeRuntime source;
    coordinator.GetTask("source-v", &task);
    nodes.GetNode("source-v", &source);
    return Expect(task.state == DrainMigrationState::kRetryWait,
                  "late source object blocks retirement") &&
           Expect(source.lifecycle != LifecycleState::kRetired,
                  "unverified source stays registered") &&
           Expect(!actuator.stopped, "source is not stopped before empty verification");
}

bool RunFailureCase() {
    using namespace zb::scheduler;
    NodePowerManager nodes;
    ClusterState cluster(FailureDetector(10000, 20000), &nodes);
    Heartbeat(&cluster, "source-f", "source-f:1", "disk-a");
    Heartbeat(&cluster, "target-f", "target-f:1", "disk-b");
    std::string error;
    nodes.BeginRemoveNode("source-f", 2, nullptr);
    FakeActuator actuator;
    LifecycleManager lifecycle(&cluster, &actuator);
    FakeBackend backend;
    backend.fail_copy = true;
    DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend, 10, 3);
    coordinator.Start("source-f", 2, &error);
    coordinator.Tick(3);
    coordinator.Tick(4);
    DrainMigrationTask task;
    coordinator.GetTask("source-f", &task);
    return Expect(task.state == DrainMigrationState::kRetryWait, "copy failure enters retry wait") &&
           Expect(backend.committed_inode == 0, "metadata not changed after copy failure") &&
           Expect(backend.deletes == 0, "source retained after copy failure") &&
           Expect(!actuator.stopped, "failed drain does not stop source");
}

bool RunCmsReferenceBarrierCase() {
    using namespace zb::scheduler;
    NodePowerManager nodes;
    ClusterState cluster(FailureDetector(10000, 20000), &nodes);
    Heartbeat(&cluster, "source-cms", "source-cms:1", "disk-a");
    Heartbeat(&cluster, "target-cms", "target-cms:1", "disk-b");
    nodes.BeginRemoveNode("source-cms", 2, nullptr);
    FakeActuator actuator;
    LifecycleManager lifecycle(&cluster, &actuator);
    FakeBackend backend;
    backend.cms_references = 1;
    DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend, 10, 3);
    std::string error;
    coordinator.Start("source-cms", 2, &error);
    coordinator.Tick(3);
    coordinator.Tick(4);
    DrainMigrationTask task;
    ManagedNodeRuntime source;
    coordinator.GetTask("source-cms", &task);
    nodes.GetNode("source-cms", &source);
    return Expect(task.state == DrainMigrationState::kRetryWait,
                  "CMS inode reference blocks retirement after source storage is empty") &&
           Expect(source.lifecycle != LifecycleState::kRetired,
                  "CMS-referenced node remains registered") &&
           Expect(!actuator.stopped, "CMS-referenced node remains powered");
}

} // namespace

int main() {
    return RunSuccessCase() && RunFailureCase() && RunFinalVerificationFailureCase() &&
                   RunCmsReferenceBarrierCase()
               ? 0
               : 1;
}
