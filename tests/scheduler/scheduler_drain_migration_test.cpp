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
        if (empty_source) { out->clear(); return true; }
        if (!invalid_object_id.empty()) {
            *out = {{"disk-a", invalid_object_id, 0, 0, 4}};
        } else if (deletes >= 2 && !retain_after_delete) {
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
    bool empty_source{false};
    std::string invalid_object_id;
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

bool RunMalformedObjectIdCases() {
    using namespace zb::scheduler;
    for (const std::string id : {"obj-42junk-0", "obj-42-0junk", "obj-+42-0",
                                "obj- 42-0", "obj-42-+0", "obj-42--1", "obj-0-0",
                                "obj-18446744073709551616-0", "obj-42-4294967296",
                                "obj--0", "obj-42-", "other-42-0"}) {
        NodePowerManager nodes;
        ClusterState cluster(FailureDetector(10000, 20000), &nodes);
        Heartbeat(&cluster, "source", "source:1", "disk-a");
        Heartbeat(&cluster, "target", "target:1", "disk-b");
        nodes.BeginRemoveNode("source", 2, nullptr);
        FakeActuator actuator;
        LifecycleManager lifecycle(&cluster, &actuator);
        FakeBackend backend;
        backend.invalid_object_id = id;
        DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend);
        std::string error;
        if (!Expect(coordinator.Start("source", 2, &error), error)) return false;
        coordinator.Tick(3);
        coordinator.Tick(4);
        DrainMigrationTask task;
        if (!Expect(coordinator.GetTask("source", &task), "migration task exists") ||
            !Expect(task.state == DrainMigrationState::kBlocked, "reject malformed ID: " + id) ||
            !Expect(backend.copies == 0 && backend.committed_inode == 0 && backend.deletes == 0,
                    "malformed ID cannot mutate either data or CMS metadata") ||
            !Expect(!actuator.stopped, "malformed source is not stopped")) return false;
    }
    return true;
}

bool RunEmptyLogicalDrainCase() {
    using namespace zb::scheduler;
    NodePowerManager nodes;
    ClusterState cluster(FailureDetector(10000, 20000), &nodes);
    auto profile = DefaultStorageNodeProfile("logical-empty");
    profile.execution_mode = ExecutionMode::kVirtual;
    std::string error;
    if (!Expect(nodes.RegisterNode(profile, 1, 1, &error), error)) return false;
    Heartbeat(&cluster, "logical-empty", "shared:1", "disk-a");
    nodes.SetResidentDataBytes("logical-empty", 0, nullptr);
    FakeActuator actuator;
    LifecycleManager lifecycle(&cluster, &actuator);
    FakeBackend backend;
    backend.empty_source = true;
    DrainMigrationCoordinator coordinator(&cluster, &nodes, &lifecycle, &backend);
    bool empty = false;
    coordinator.SetLogicalSourceEmptyVerifier(
        [&](const std::string&, std::string* why) {
            if (!empty && why) *why = "model still has objects";
            return empty;
        });
    if (!Expect(!coordinator.CanStart("logical-empty", &error),
                "logical source needs a write admission gate")) return false;
    ManagedNodeRuntime source;
    nodes.GetNode("logical-empty", &source);
    if (!Expect(source.lifecycle != LifecycleState::kDraining,
                "failed admission does not mutate lifecycle")) return false;
    coordinator.SetLogicalEmptyDrainBegin(
        [&](const std::string& id, uint64_t now_ms, std::string* why) {
            return nodes.BeginRemoveNode(id, now_ms, why);
        });
    empty = true;
    if (!Expect(coordinator.CanStart("logical-empty", &error), error) ||
        !Expect(coordinator.BeginManagedDrain("logical-empty", 2, &error), error) ||
        !Expect(coordinator.Start("logical-empty", 2, &error), error)) return false;
    empty = false;
    coordinator.Tick(3); // enumerate
    DrainMigrationTask task;
    coordinator.GetTask("logical-empty", &task);
    if (!Expect(task.state == DrainMigrationState::kRetryWait,
                "late logical data prevents retirement")) return false;
    empty = true;
    coordinator.Tick(task.next_retry_ms);
    coordinator.GetTask("logical-empty", &task);
    nodes.GetNode("logical-empty", &source);
    return Expect(task.state == DrainMigrationState::kCompleted,
                  "verified empty logical source completes drain") &&
           Expect(source.lifecycle == LifecycleState::kRetired,
                  "empty logical source retires") &&
           Expect(backend.list_calls == 1 && backend.copies == 0 && backend.deletes == 0,
                  "empty logical source enumerated without object moves") &&
           Expect(!actuator.stopped, "logical source never runs physical stop");
}

} // namespace

int main() {
    return RunSuccessCase() && RunFailureCase() && RunFinalVerificationFailureCase() &&
                   RunCmsReferenceBarrierCase() && RunMalformedObjectIdCases() &&
                   RunEmptyLogicalDrainCase()
               ? 0
               : 1;
}
