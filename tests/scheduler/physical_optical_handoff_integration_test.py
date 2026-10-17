#!/usr/bin/env python3
"""手动真实系统闭环：外存实际字节、光盘文件化后端、MDS 权威交接和源退役。

不注册 CTest；不是机械光驱/硬件功耗验收。全部服务、数据库与 PID 为本次私有实例。
"""
import argparse
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time

from optical_reporting_integration_test import raw, stop, optical_config, ready
from python.fuse_workloads import FuseMount, FuseFiles
from python.support import LocalCluster, Rpc, ROOT, wait_for
from real_system_smoke import check_root


SOURCE, TARGET = "physical-optical-source", "physical-optical-target"


def read_archived(cluster, output, archived, files):
    with FuseMount(cluster, output, optical_reads=True) as mount:
        ctl = [str(cluster.build / "zbctl"), "--socket", str(output / "prepare.sock"), "stage"]
        for _, name in archived:
            path = mount / "real/physical-handoff" / name
            handle = subprocess.check_output([*ctl, "submit", str(path)], text=True, timeout=10).strip()
            subprocess.run([*ctl, "wait", handle], check=True, timeout=30)
            assert path.read_bytes() == files.payload(name, 0, 4096)
            subprocess.run([*ctl, "release", handle], check=True, timeout=10)


class HandoffCluster(LocalCluster):
    def start(self, name, binary, config):
        config = dict(config)
        if name == "scheduler":
            config["STOP_CMD_TEMPLATE"] = " ".join(map(shlex.quote, [
                sys.executable, str(Path(__file__).resolve()), "--stop-owned",
                str(self.output)])) + " {node_id}"
        result = super().start(name, binary, config)
        if name in (SOURCE, TARGET):
            (self.output / (name + ".pid")).write_text(str(self.processes[-1].pid))
        return result


def stop_owned(path, node):
    if node not in (SOURCE, TARGET):
        raise RuntimeError("fixture actuator only permits its optical source")
    pid = int((Path(path) / (node + ".pid")).read_text())
    cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
    expected = str(Path(path) / "config" / (node + ".conf")).encode()
    if b"optical_node_server" not in cmdline or expected not in cmdline:
        raise RuntimeError("refusing to signal a process not owned by this fixture")
    os.kill(pid, signal.SIGINT)
    def stopped():
        status = Path(f"/proc/{pid}/stat")
        return not status.exists() or status.read_text().split(") ", 1)[1].startswith("Z")
    wait_for(stopped, timeout=20)


def run(build, external_root):
    check_root(Path(external_root))
    output = Path(tempfile.mkdtemp(prefix="zb_physical_optical_handoff_", dir=external_root))
    print(f"私有外存目录（不提交）: {output}", flush=True)
    with HandoffCluster(output, build, node_count=1) as cluster:
        scheduler = Rpc(cluster.scheduler, "SchedulerService")
        config = dict(optical_config(cluster, SOURCE), MDS_ADDRESS=cluster.mds,
                      VOLUME_SIZE_BYTES=1024, SIZE_THRESHOLD=0.001, CAPACITY_IN_IMAGES=8,
                      MAX_WRITE_IMAGES=4, DISC_CAPACITY_BYTES=8192, STANDARD_IMAGES_PER_DISC=1,
                      DISC_BLOCK_SIZE_BYTES=4096, BURN_FIXED_SECONDS=0.001,
                      DRIVE_LOAD_SECONDS=0.001, DRIVE_EJECT_SECONDS=0.001, GRASP_SECONDS=0.001,
                      ARM_SPEED_X=1000000, ARM_SPEED_Y=1000000, ARM_SPEED_Z=1000000)
        src = cluster.start(SOURCE, "optical_node_server", config)
        source_process = cluster.processes[-1]
        destination = dict(config, NODE_ID=TARGET, ARCHIVE_ROOT=output / TARGET)
        dst = cluster.start(TARGET, "optical_node_server", destination)
        wait_for(lambda: ready(scheduler, SOURCE) and ready(scheduler, TARGET))
        batch, by_inode = [], {}
        with FuseMount(cluster, output / "create-mount") as mount:
            files = FuseFiles(cluster.mds, "/real/physical-handoff", mount)
            files.mkdir(files.namespace)
            for name in ("a", "b", "c"):
                files.create(name, 4096)
                item, _ = files.write(name)
                files.read(name)
                batch.append({"inode_id": item["inode_id"], "size": 4096, "object_unit_size": 4194304,
                              "target_node_id": item["node_id"], "target_disk_id": item["disk_id"]})
                by_inode[str(item["inode_id"])] = name
        accepted = raw(src, "OpticalNodeService", "SendArchiveMetadata", batch_id=1, files=batch)
        assert accepted["status"].get("code", "MDS_OK") == "MDS_OK"
        identity = dict(node_id=SOURCE, instance_generation=7)
        def images():
            return raw(src, "OpticalNodeService", "ListArchiveImages", **identity).get("images", [])
        completed = wait_for(lambda: images() if len(images()) >= 2 else None)
        scheduler.call("RemoveManagedNode", node_id=batch[0]["target_node_id"], request_ts_ms=time.time_ns() // 1000000)
        wait_for(lambda: next((e for e in files.mds.call("GetCmsNodeCatalog").get("nodes", [])
                 if e["managed_view"]["spec"]["node_id"] == batch[0]["target_node_id"] and
                 e["managed_view"].get("lifecycle") == "MANAGED_LIFECYCLE_DRAINING"), None))
        archived = []
        for image in completed:
            contents = raw(src, "OpticalNodeService", "GetArchiveImageFiles", **identity,
                           disk_id=image["disk_id"], image_id=image["image_id"])["files"]
            for entry in contents:
                inode = entry["inode_id"]
                current = files.mds.call("GetFileLocation", inode_id=inode)["location"]
                lease = files.mds.call("ClaimArchiveTask", inode_id=inode,
                    node_id=current["disk_location"]["node_id"], disk_id=current["disk_location"]["disk_id"],
                    requested_lease_ms=300000)
                assert lease.get("granted")
                commit = dict(operation_id="real-archive-" + str(inode), source_lease_id=lease["record"]["lease_id"],
                    inode_id=inode, source_version=current["attr"]["version"], size=4096,
                    expected_source=current["disk_location"], target=dict(identity, node_address=src,
                    disk_id=image["disk_id"], image_id=image["image_id"]), commit=True)
                bad = raw(cluster.mds, "MdsService", "CommitPhysicalOpticalArchive",
                          **dict(commit, source_version=int(current["attr"]["version"]) + 1))
                assert bad["status"]["code"] == "MDS_STALE_EPOCH"
                files.mds.call("CommitPhysicalOpticalArchive", **commit)
                files.mds.call("CommitPhysicalOpticalArchive", **commit)  # 精确幂等
                archived.append((inode, by_inode[str(inode)]))
        assert len(archived) >= 2
        read_archived(cluster, output / "before-handoff", archived, files)
        print("真实归档实际载荷摘要/版本/租约 CAS，原路径 FUSE 回读: PASS", flush=True)
        # 通过 MDS 既有正式导入发布真实目录的冷元数据视图，不直接修改数据库。
        files.mds.call("ImportArchiveNamespace", path_prefix=files.namespace,
                       namespace_id="real-handoff", generation_id="g1", publish_route=True)
        assert int(files.mds.call("VerifyNodeReferences", node_id=SOURCE).get("optical_inode_references", 0)) >= len(archived)

        record = {"operation_id": "real-library-handoff", "source": dict(identity, node_address=src),
                  "target": {"node_id": TARGET, "instance_generation": 7, "node_address": dst}}
        def handoff(endpoint, node, action, **body):
            return raw(endpoint, "OpticalNodeService", "ManageLibraryHandoff", node_id=node,
                       instance_generation=7, action=action, **body)
        def frozen():
            reply = handoff(src, SOURCE, "HANDOFF_FREEZE", record=record)
            return reply.get("record") if reply["status"].get("code", "OPTICAL_OK") == "OPTICAL_OK" else None
        record = wait_for(frozen)
        assert record["phase"] == "EXPORTED" and any(f["path"].startswith("disc_sim/") for f in record["files"])
        denied = handoff(src, SOURCE, "HANDOFF_RELEASE", record=record)
        assert denied["status"]["code"] != "OPTICAL_OK"
        assert (output / SOURCE / "meta/node_discs_meta").exists()
        reply = handoff(dst, TARGET, "HANDOFF_ACCEPT", record=record)
        assert reply["status"].get("code", "OPTICAL_OK") == "OPTICAL_OK"
        f = next(f for f in record["files"] if int(f.get("size", 0)) > 1)
        chunk = handoff(src, SOURCE, "HANDOFF_READ", record=record, path=f["path"], chunk_size=1)
        handoff(dst, TARGET, "HANDOFF_WRITE", record=record, path=f["path"], data=chunk["data"])
        # 强杀接收者和冻结源；两个实例都必须恢复管理收据，不可启动空库覆盖数据。
        cluster.processes[-1].kill(); cluster.processes[-1].wait(timeout=10)
        cluster.start(TARGET, "optical_node_server", destination)
        prefix = handoff(dst, TARGET, "HANDOFF_QUERY", path=f["path"])
        assert int(prefix.get("received_bytes", 0)) == 1
        source_process.kill(); source_process.wait(timeout=10)
        cluster.start(SOURCE, "optical_node_server", config)
        source_process = cluster.processes[-1]
        assert handoff(src, SOURCE, "HANDOFF_QUERY")["record"]["manifest_id"] == record["manifest_id"]
        print("未提交禁止源清理，源/目标强杀重启恢复收据与续传前缀: PASS", flush=True)

        scheduler.call("RemoveManagedNode", node_id=SOURCE, request_ts_ms=time.time_ns() // 1000000)
        cluster.processes[0].kill(); cluster.processes[0].wait(timeout=10)
        scheduler.close()  # 显式丢弃被强杀服务的旧 HTTP 会话；不隐式重试变更 RPC。
        scheduler_config = dict(line.split("=", 1) for line in
                                (output / "config/scheduler.conf").read_text().splitlines() if "=" in line)
        cluster.start("scheduler", "scheduler_server", scheduler_config)
        wait_for(lambda: scheduler.call("GetManagedNode", node_id=SOURCE)["node"].get("lifecycle") ==
                 "MANAGED_LIFECYCLE_RETIRED", timeout=120)
        source_process.wait(timeout=15)
        assert source_process.returncode == 0
        assert files.mds.call("VerifyNodeReferences", node_id=SOURCE).get("zero_references")
        def migration_reported():
            node = scheduler.call("GetManagedNode", node_id=TARGET)["node"]
            return any(s.get("access_path") == "MANAGED_ACCESS_MIGRATION" and
                       int(s.get("write_bytes", 0)) > 0 for s in node.get("metrics_streams", []))
        wait_for(migration_reported)
        for inode, _ in archived:
            assert files.mds.call("GetFileLocation", inode_id=inode)["location"]["optical_location"]["node_id"] == TARGET
        assert not (output / SOURCE / "meta").exists()
        assert (output / SOURCE / ".handoff-retired/meta/node_discs_meta").is_file()
        read_archived(cluster, output / "after-handoff", archived, files)
        print("Scheduler 分块实传→目标重建→MDS 位置切换→源隔离/零引用/真实进程停止/退役→FUSE 回读: PASS", flush=True)
        mds_process = cluster.processes[1]
        stop(mds_process)
        files.mds.close()
        mds_config = dict(line.split("=", 1) for line in
                          (output / "config/mds.conf").read_text().splitlines() if "=" in line)
        cluster.start("mds", "mds_server", mds_config)
        target_process = next(p for p in reversed(cluster.processes) if p.poll() is None and
                              str(output / "config" / (TARGET + ".conf")) in " ".join(p.args))
        stop(target_process)
        cluster.start(TARGET, "optical_node_server", destination)
        wait_for(lambda: ready(scheduler, TARGET))
        read_archived(cluster, output / "restart-handoff", archived, files)
        for inode, _ in archived:
            assert files.mds.call("GetFileLocation", inode_id=inode)["location"]["optical_location"]["node_id"] == TARGET
        print("Scheduler 强杀续传、MDS 权威映射与接收库重启后原路径读取: PASS", flush=True)
        successor = TARGET + "-next"
        cluster.start(successor, "optical_node_server", dict(destination, NODE_ID=successor,
                      ARCHIVE_ROOT=output / successor))
        wait_for(lambda: ready(scheduler, successor))
        scheduler.call("RemoveManagedNode", node_id=TARGET, request_ts_ms=time.time_ns() // 1000000)
        wait_for(lambda: scheduler.call("GetManagedNode", node_id=TARGET)["node"].get("lifecycle") ==
                 "MANAGED_LIFECYCLE_RETIRED", timeout=90)
        assert files.mds.call("VerifyNodeReferences", node_id=TARGET).get("zero_references")
        for inode, _ in archived:
            assert files.mds.call("GetFileLocation", inode_id=inode)["location"]["optical_location"]["node_id"] == successor
        read_archived(cluster, output / "second-handoff", archived, files)
        print("接收库再次退役、自动兼容目标选择、两跳 MDS 权威位置与原路径读取: PASS", flush=True)
        files.close(); scheduler.close()


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--stop-owned":
        stop_owned(sys.argv[2], sys.argv[3])
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--build", default=str(ROOT / "build-release"))
        parser.add_argument("--external-root", default="/mnt/md0")
        args = parser.parse_args()
        run(args.build, args.external_root)
