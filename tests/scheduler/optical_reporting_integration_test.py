#!/usr/bin/env python3
"""手动启动真实 Scheduler/MDS/光盘服务，验证上报契约；不验收内部刻录或物理功耗。"""
import argparse
import http.client
import json
from pathlib import Path
import signal
import socket
import subprocess
import time

from python.support import LocalCluster, ROOT, Rpc, result_dir, wait_for
from python.workloads import Files


def raw(endpoint, service, method, **request):
    connection = http.client.HTTPConnection(endpoint, timeout=10)
    try:
        connection.request("POST", f"/zb.rpc.{service}/{method}", json.dumps(request),
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        assert response.status == 200
        return json.loads(response.read())
    finally:
        connection.close()


def stop(process):
    if process.poll() is None:
        process.send_signal(signal.SIGINT)
        process.wait(timeout=15)
        assert process.returncode == 0


def optical_config(cluster, node_id="reporting-optical"):
    return {"NODE_ID": node_id, "NODE_ADDRESS": "", "SCHEDULER_ADDR": cluster.scheduler,
            "HEARTBEAT_INTERVAL_MS": 100, "ARCHIVE_ROOT": cluster.output / node_id,
            "VOLUME_SIZE_BYTES": 10485760, "CAPACITY_IN_IMAGES": 4, "AVAILABLE_ID_COUNT": 1,
            "DRIVE_COUNT": 3, "TOTAL_DISC_COUNT": 10, "INSTANCE_GENERATION": 7,
            "POWER_PEAK_MILLIWATTS": 777000}


def ready(rpc, node_id="reporting-optical"):
    node = rpc.call("GetManagedNode", node_id=node_id)["node"]
    return node if node.get("lifecycle") == "MANAGED_LIFECYCLE_WORKING" and (
        node.get("optical_library", {}).get("telemetry_version") == 1) else None


def cms_gate(cluster, admitted):
    # 私有空数据库中的不存在 inode：只经过 CMS 准入判断与一次查找，绝不提交位置变更。
    reply = raw(cluster.mds, "MdsService", "CommitFileMigration", migration_id="admission-only",
                inode_id=900000000000, expected_source={"node_id": "absent", "disk_id": "absent"},
                target={"node_id": "reporting-optical", "disk_id": "disc"})
    expected = "MDS_NOT_FOUND" if admitted else "MDS_STALE_EPOCH"
    return reply["status"].get("code") == expected


def receive_and_restore(build):
    with LocalCluster(result_dir("optical-reporting"), build, node_count=1) as cluster:
        rpc = Rpc(cluster.scheduler, "SchedulerService")
        cluster.start("reporting-optical", "optical_node_server", optical_config(cluster))
        optical_process = cluster.processes[-1]
        node = wait_for(lambda: ready(rpc))
        status = node["optical_library"]
        assert all(status[key] for key in ("inventory_valid", "staging_valid", "background_valid"))
        assert not status.get("physical_health_observed", False)
        assert int(node["spec"]["instance_generation"]) == 7
        assert int(node["spec"]["power"]["peak_milliwatts"]) == 777000
        assert len(node["inventory_devices"]) == 5
        assert {d["health_source"] for d in node["inventory_devices"]} == {"software", "model"}
        assert not node["spec"].get("participates_in_placement", False)
        wait_for(lambda: cms_gate(cluster, True))
        duplicate = raw(cluster.scheduler, "SchedulerService", "RegisterManagedNode", spec=node["spec"])
        assert duplicate["status"]["code"] == "SCHED_ALREADY_EXISTS" and duplicate.get("node")
        rejected = raw(cluster.scheduler, "SchedulerService", "ReportHeartbeat",
                       node_id="reporting-optical", node_type="NODE_OPTICAL", address="127.0.0.1:1",
                       instance_generation=8, require_registered=True)
        assert rejected["status"]["code"] == "SCHED_INVALID_ARGUMENT"
        for method, body in (
                ("ReportHeartbeat", {"node_type": "NODE_OPTICAL", "address": "127.0.0.1:1"}),
                ("ReportNodeMetrics", {"window_start_ms": 1, "window_end_ms": 2}),
                ("ReportOpticalLibraryStatus", {"instance_generation": 7,
                    "status": {"observed": True, "observed_at_ms": time.time_ns() // 1000000}})):
            reply = raw(cluster.scheduler, "SchedulerService", method,
                        node_id="reporting-optical", **body)
            assert reply["status"]["code"] == "SCHED_INVALID_ARGUMENT"
        print("真实光盘注册、采集来源、CMS 独立归档准入与代际拒绝: PASS", flush=True)

        # 真数据节点写入，再由真光盘服务下载；只核验上报，不验收后续刻录/位置提交。
        files = Files(cluster.mds, "/reporting-input")
        optical = Rpc(node["service_address"], "OpticalNodeService")
        try:
            files.mkdir("/reporting-input")
            files.create("input", 4096)
            item, _ = files.write("input")
            assert item["node_id"] != "reporting-optical"  # 普通文件放置仍排除光盘层。
            optical.call("SendArchiveMetadata", batch_id=1, files=[{
                "inode_id": item["inode_id"], "size": 4096, "object_unit_size": 4194304,
                "target_node_id": item["node_id"], "target_disk_id": item["disk_id"]}])
            wait_for(lambda: int(rpc.call("GetManagedNode", node_id="reporting-optical")["node"].get(
                "accumulated_write_bytes", 0)) == 4096)
            print("真实磁盘文件下载产生光盘路径指标、普通放置仍排除光盘: PASS", flush=True)
        finally:
            optical.close()
            files.close()

        # 使用真实 Scheduler RPC 的独立受管身份，隔离可重复的窗口输入与时间推进。
        rpc.call("RegisterManagedNode", spec={"node_id": "metrics-optical", "instance_generation": 9,
                 "kind": "MANAGED_KIND_OPTICAL_LIBRARY", "execution_mode": "MANAGED_EXECUTION_SIMULATED"},
                 join_time_ms=1000, activate_immediately=True)
        cache = dict(node_id="metrics-optical", instance_generation=9, window_start_ms=1000,
                     window_end_ms=1100, read_operations=1, read_bytes=4096, reporter_epoch=101,
                     report_sequence=1, access_path="MANAGED_ACCESS_OPTICAL_CACHE_HIT")
        disc = dict(cache, window_end_ms=1200, reporter_epoch=202,
                    access_path="MANAGED_ACCESS_OPTICAL_DISC")
        for report in (cache, disc, cache):
            rpc.call("ReportNodeMetrics", **report)
        assert int(rpc.call("GetManagedNode", node_id="metrics-optical")["node"]["access_operation_count"]) == 2
        rpc.call("ReportNodeMetrics", **dict(cache, window_end_ms=1150, report_sequence=2))
        rpc.call("ReportNodeMetrics", **dict(cache, window_end_ms=1150, reporter_epoch=303))
        rpc.call("ReportNodeMetrics", **dict(cache, window_end_ms=1150, report_sequence=2))
        assert int(rpc.call("GetManagedNode", node_id="metrics-optical")["node"]["access_operation_count"]) == 4
        rpc.call("ReportOpticalLibraryStatus", node_id="metrics-optical", instance_generation=9,
                 status={"observed": True, "observed_at_ms": 1200, "valid_until_ms": 1000000,
                         "telemetry_version": 1, "inventory_valid": True, "staging_valid": True,
                         "background_valid": True, "pending_archive_batches": 1})
        rpc.call("AdvanceSimulationTime", target_time_us=100000000)
        assert rpc.call("GetManagedNode", node_id="metrics-optical")["node"]["power_level"] == "MANAGED_POWER_MEDIUM"

        stop(optical_process)
        node = rpc.call("GetManagedNode", node_id="reporting-optical")["node"]
        invalid = dict(node["optical_library"], observed=False, inventory_valid=False,
                       staging_valid=False, background_valid=False, accepts_archive_writes=False,
                       observed_at_ms=time.time_ns() // 1000000, valid_until_ms=time.time_ns() // 1000000 + 10000,
                       collection_error="integration: sampling unavailable")
        rpc.call("ReportOpticalLibraryStatus", node_id="reporting-optical", instance_generation=7, status=invalid)
        rpc.call("ReportHeartbeat", node_id="reporting-optical", node_type="NODE_OPTICAL",
                 address=node["service_address"], instance_generation=7, require_registered=True,
                 inventory_unavailable=True, readiness_reported=True, initialization_complete=True,
                 metadata_ready=True)
        retained = rpc.call("GetManagedNode", node_id="reporting-optical")["node"]
        assert retained["inventory_devices"] == node["inventory_devices"]
        assert not retained["readiness"].get("inventory_ready", False)
        wait_for(lambda: cms_gate(cluster, False))
        print("双路窗口重试、归档在途忙闲、采集失败保留库存及 CMS 拒绝: PASS", flush=True)

        scheduler_config = dict(line.split("=", 1) for line in
                                (cluster.output / "config/scheduler.conf").read_text().splitlines())
        stop(cluster.processes[0])
        rpc.close()
        cluster.start("scheduler", "scheduler_server", scheduler_config)
        rpc = Rpc(cluster.scheduler, "SchedulerService")
        restored = wait_for(lambda: rpc.call("GetManagedNode", node_id="metrics-optical")["node"])
        assert len(restored["metrics_streams"]) == 3
        rpc.call("ReportNodeMetrics", **dict(cache, window_end_ms=1150, report_sequence=2))
        assert int(rpc.call("GetManagedNode", node_id="metrics-optical")["node"]["access_operation_count"]) == 4
        assert rpc.call("GetManagedNode", node_id="reporting-optical")["node"]["optical_library"]["collection_error"] == invalid["collection_error"]
        # 仅破坏本测试自己的恢复输入，验证引擎降级运行不能冒充元数据就绪。
        snapshot = cluster.output / "reporting-optical/log/runtime_state_snapshot"
        assert snapshot.is_file()
        snapshot.write_bytes(b"invalid integration snapshot")
        cluster.start("reporting-optical", "optical_node_server", optical_config(cluster))
        wait_for(lambda: "recovery incomplete" in rpc.call("GetManagedNode", node_id="reporting-optical")[
            "node"]["optical_library"].get("collection_error", ""))
        wait_for(lambda: not rpc.call("GetManagedNode", node_id="reporting-optical")["node"]["readiness"].get("metadata_ready", False))
        wait_for(lambda: cms_gate(cluster, False))
        bad = dict(optical_config(cluster), TOTAL_DISC_COUNT=0, NODE_ID="zero-disc")
        try:
            cluster.start("zero-disc", "optical_node_server", bad)
            raise AssertionError("zero-disc configuration silently accepted")
        except ValueError as error:
            assert "TOTAL_DISC_COUNT=0 is unsupported" in str(error)
        rpc.close()
        print("真实 Scheduler 重启后的质量信息及分路去重恢复: PASS", flush=True)
        print("恢复异常如实上报、零盘配置不伪报 1000 片: PASS", flush=True)


def delayed_registration(build):
    cluster = LocalCluster(result_dir("optical-registration-retry"), build, node_count=0)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    cluster.ports["scheduler"] = port
    cluster.scheduler = f"127.0.0.1:{port}"
    try:
        cluster.start("reporting-optical", "optical_node_server", optical_config(cluster))
        # 等待当前进程已经历注册失败；不依赖固定 sleep 或外部部署。
        log = cluster.output / "reporting-optical.log"
        wait_for(lambda: subprocess.run(["rg", "-q", "registration unconfirmed", str(log)],
                                        stdout=subprocess.DEVNULL).returncode == 0)
        with cluster:
            rpc = Rpc(cluster.scheduler, "SchedulerService")
            node = wait_for(lambda: ready(rpc))
            assert int(node["spec"]["instance_generation"]) == 7
            assert int(node["spec"]["power"]["peak_milliwatts"]) == 777000
            assert next(g for g in node["spec"]["device_groups"] if g["name"] == "optical-drive")["device_count"] == 3
            conflict = dict(optical_config(cluster), INSTANCE_GENERATION=8,
                            ARCHIVE_ROOT=cluster.output / "conflict-private")
            cluster.start("conflict", "optical_node_server", conflict)
            wait_for(lambda: subprocess.run(["rg", "-q", "operator reconciliation required",
                     str(cluster.output / "conflict.log")], stdout=subprocess.DEVNULL).returncode == 0)
            assert rpc.call("GetManagedNode", node_id="reporting-optical")["node"]["service_address"] == node["service_address"]
            rpc.close()
            print("Scheduler 延迟启动后完整属性重报、不退化默认模板: PASS", flush=True)
            print("冲突代际的真实光盘进程不能覆盖已有地址/属性: PASS", flush=True)
    finally:
        cluster.__exit__(None, None, None)


def archive_inventory(build, external_root=None):
    """真实进程/真实文件；只验证受托的信息查询契约，不代替 MDS 提交验收。"""
    if external_root:
        import tempfile
        output = Path(tempfile.mkdtemp(prefix="zb_archive_inventory_", dir=external_root))
    else:
        output = result_dir("archive-inventory")
    with LocalCluster(output, build, node_count=1) as cluster:
        config = dict(optical_config(cluster), VOLUME_SIZE_BYTES=1024, SIZE_THRESHOLD=0.001,
                      CAPACITY_IN_IMAGES=8, MAX_WRITE_IMAGES=4, DISC_CAPACITY_BYTES=8192,
                      STANDARD_IMAGES_PER_DISC=1, DISC_BLOCK_SIZE_BYTES=4096,
                      BURN_FIXED_SECONDS=0.001, DRIVE_LOAD_SECONDS=0.001,
                      DRIVE_EJECT_SECONDS=0.001, GRASP_SECONDS=0.001,
                      ARM_SPEED_X=1000000, ARM_SPEED_Y=1000000, ARM_SPEED_Z=1000000)
        endpoint = cluster.start("reporting-optical", "optical_node_server", config)
        rpc = Rpc(cluster.scheduler, "SchedulerService")
        wait_for(lambda: ready(rpc))
        query = dict(node_id="reporting-optical", instance_generation=7)
        def images():
            return raw(endpoint, "OpticalNodeService", "ListArchiveImages", **query)
        empty = images()
        assert empty["status"].get("code", "OPTICAL_OK") == "OPTICAL_OK" and not empty.get("images")
        assert not empty.get("modeled", False)
        files = Files(cluster.mds, "/archive-inventory")
        try:
            files.mkdir("/archive-inventory")
            batch = []
            for name in ("a", "b", "c"):
                files.create(name, 4096)
                item, _ = files.write(name)
                batch.append({"inode_id": item["inode_id"], "size": 4096,
                              "object_unit_size": 4194304, "target_node_id": item["node_id"],
                              "target_disk_id": item["disk_id"]})
            # 使用已有容量触发封盘，不改光盘负责人尾批/封盘业务。
            accepted = raw(endpoint, "OpticalNodeService", "SendArchiveMetadata", batch_id=1, files=batch)
            assert accepted["status"].get("code", "MDS_OK") == "MDS_OK"
            wait_for(lambda: len(images().get("images", [])) >= 2)
            page = raw(endpoint, "OpticalNodeService", "ListArchiveImages", **query, limit=1)
            assert page.get("has_more") and len(page["images"]) == 1
            image = page["images"][0]
            manifest = raw(endpoint, "OpticalNodeService", "GetArchiveImageFiles", **query,
                           image_id=image["image_id"], disk_id=image["disk_id"], limit=1)
            assert manifest["status"].get("code", "OPTICAL_OK") == "OPTICAL_OK"
            assert int(manifest["files"][0]["size"]) == 4096
            assert not manifest["files"][0].get("has_file_version", False)
            inode = manifest["files"][0]["inode_id"]
            probe = Path(build).resolve() / "scheduler_model_closure_probe"
            subprocess.run([str(probe), "--archive-inventory", endpoint, "reporting-optical",
                            "7", "real", str(inode)], check=True, timeout=30)
            before = images()
            stop(cluster.processes[-1])
            cluster.start("reporting-optical", "optical_node_server", config)
            wait_for(lambda: ready(rpc) and images()["status"].get("code", "OPTICAL_OK") == "OPTICAL_OK")
            assert images()["snapshot_token"] == before["snapshot_token"]
            subprocess.run([str(probe), "--archive-inventory", endpoint, "reporting-optical",
                            "7", "real", str(inode)], check=True, timeout=30)
            print("真实磁盘下载/光盘文件化刻录→MDS 清单 RPC/SDK→重启重建查询: PASS", flush=True)
            print(f"本地结果（不提交）: {output}", flush=True)
        finally:
            files.close()
            rpc.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", default=str(ROOT / "build-release"))
    parser.add_argument("--inventory-only", action="store_true", help="只运行 MDS 归档清单接口系统闭环")
    parser.add_argument("--external-root", help="该闭环的私有真实数据目录父路径")
    args = parser.parse_args()
    if not args.inventory_only:
        receive_and_restore(args.build)
        delayed_registration(args.build)
    archive_inventory(args.build, args.external_root)
