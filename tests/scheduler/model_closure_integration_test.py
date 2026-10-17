#!/usr/bin/env python3
"""Exercise two non-default-generation disk models and one optical model with real MDS."""

import argparse
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_port(process, port):
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"server exited early: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} did not open")


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def scheduler_rpc(port, method, **payload):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/zb.rpc.SchedulerService/{method}",
        json.dumps(payload).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=2) as reply:
        result = json.load(reply)
    if result.get("status", {}).get("code", 0) not in (0, "SCHED_OK"):
        raise AssertionError(f"{method}: {result}")
    return result


def managed_node(port, node_id):
    return scheduler_rpc(port, "GetManagedNode", node_id=node_id).get("node", {})


def verify_fuse_read(root, client_path, ctl_path, mds_port, control_port):
    if not pathlib.Path("/dev/fuse").exists() or not shutil.which("fusermount3"):
        print("SKIP: FUSE mount unavailable; RPC closure remains verified")
        return
    mount = root / "fuse-mount"
    mount.mkdir()
    log_path = root / "fuse.log"
    with log_path.open("wb") as client_log:
        client = subprocess.Popen(
            [client_path, f"--mds=127.0.0.1:{mds_port}",
             f"--scheduler=127.0.0.1:{control_port}",
             "--optical_read_enabled=true", f"--prepare_control_socket={root / 'prepare.sock'}",
             "--bootstrap_tier_dirs=false", "--default_object_unit_size=40",
             "--", str(mount), "-f", "-o", "attr_timeout=0,entry_timeout=0"],
            stdout=client_log, stderr=client_log,
        )
        try:
            deadline = time.monotonic() + 10
            while not os.path.ismount(mount) and client.poll() is None and time.monotonic() < deadline:
                time.sleep(0.05)
            if not os.path.ismount(mount):
                raise AssertionError("FUSE mount did not start: " +
                                     log_path.read_text(errors="replace")[-2000:])
            handle = subprocess.check_output(
                [ctl_path, "--socket", str(root / "prepare.sock"), "stage", "submit",
                 str(mount / "virtual" / "model-closure")], text=True, timeout=10).strip()
            subprocess.run([ctl_path, "--socket", str(root / "prepare.sock"), "stage", "wait", handle],
                           check=True, timeout=15)
            subprocess.run(
                [sys.executable, "-c",
                 "import pathlib,sys; assert pathlib.Path(sys.argv[1]).read_bytes() == b'x' * 40",
                 str(mount / "virtual" / "model-closure")],
                check=True, timeout=20,
            )
            print("real FUSE -> MDS -> restored optical model read: PASS")
        finally:
            if os.path.ismount(mount):
                subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=10)
            stop(client)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", required=True)
    parser.add_argument("--mds", required=True)
    parser.add_argument("--probe", required=True)
    parser.add_argument("--client", required=True)
    parser.add_argument("--ctl", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zb-model-closure-") as root_text:
        root = pathlib.Path(root_text)
        ports = set()
        while len(ports) < 3:
            ports.add(free_port())
        control_port, data_port, mds_port = sorted(ports)
        for name, generation in (("model-disk-a", 7), ("model-disk-b", 8)):
            (root / f"{name}.conf").write_text(
                f"NODE_ID={name}\nINSTANCE_GENERATION={generation}\n"
                "DISK_IDS=disk-01\nARCHIVE_META_DIR=control-" + name + "\n"
                "DISK_CAPACITY_BYTES=1000000\nREAD_BYTES_PER_SEC=10000000\n"
                "WRITE_BYTES_PER_SEC=10000000\nREPLICATION_ENABLED=false\n",
                encoding="utf-8",
            )
        (root / "optical.conf").write_text(
            "NODE_ID=model-optical\nINSTANCE_GENERATION=9\nSTATE_DIR=optical-state\n"
            "DRIVE_COUNT=1\nDISC_COUNT=2\nDISC_CAPACITY_BYTES=1000\n"
            "DISC_READ_BYTES_PER_SEC=1000000\nCACHE_CAPACITY_BYTES=1000\n"
            "CACHE_READ_BYTES_PER_SEC=1000000\nLOAD_LATENCY_MS=10\n"
            "SEEK_LATENCY_MS=1\nARCHIVE_TRANSFER_BYTES_PER_SEC=1000000\n"
            "ARCHIVE_PACK_BYTES_PER_SEC=1000000\n"
            "ARCHIVE_BURN_BYTES_PER_SEC=1000000\nARCHIVE_BURN_FIXED_MS=5\nARCHIVE_SOURCE_LEASE_MS=1000\n",
            encoding="utf-8",
        )
        (root / "optical-target.conf").write_text(
            (root / "optical.conf").read_text().replace("NODE_ID=model-optical\n", "NODE_ID=model-optical-target\n")
            .replace("INSTANCE_GENERATION=9", "INSTANCE_GENERATION=10")
            .replace("STATE_DIR=optical-state", "STATE_DIR=optical-target-state")
            .replace("DISC_COUNT=2", "DISC_COUNT=1")
            .replace("DISC_CAPACITY_BYTES=1000", "DISC_CAPACITY_BYTES=80")
            .replace("ARCHIVE_BURN_FIXED_MS=5", "ARCHIVE_BURN_FIXED_MS=1500"), encoding="utf-8")
        scheduler_config = root / "scheduler.conf"
        scheduler_config.write_text(
            "SIMULATION_DISK_CONFIGS=model-disk-a.conf,model-disk-b.conf\n"
            "SIMULATION_OPTICAL_CONFIGS=optical.conf,optical-target.conf\n"
            f"SIMULATION_DATA_PORT={data_port}\n"
            f"SIMULATION_DATA_ADDRESS=127.0.0.1:{data_port}\n"
            f"MDS_ADDRESS=127.0.0.1:{mds_port}\n"
            f"MANAGED_STATE_SNAPSHOT_PATH={root / 'managed.pb'}\n"
            "CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS=100\nTICK_INTERVAL_MS=100\n",
            encoding="utf-8",
        )
        mds_config = root / "mds.conf"
        mds_config.write_text(
            f"MDS_DB_PATH={root / 'mds-db'}\n"
            f"SCHEDULER_ADDR=127.0.0.1:{control_port}\n"
            "MANAGED_NODE_ID=closure-mds\nOBJECT_UNIT_SIZE=40\nREPLICA=1\n"
            "STRICT_TIER_BYPASS_PG=true\nENABLE_OPTICAL_ARCHIVE=false\n"
            f"ARCHIVE_META_ROOT={root / 'archive'}\n"
            f"MASSTREE_ROOT={root / 'masstree'}\n",
            encoding="utf-8",
        )
        scheduler = mds = None
        try:
            with (root / "scheduler.log").open("wb") as scheduler_log, \
                 (root / "mds.log").open("wb") as mds_log:
                scheduler = subprocess.Popen(
                    [args.scheduler, f"--config={scheduler_config}",
                     f"--port={control_port}", "--bthread_concurrency=4"],
                    stdout=scheduler_log, stderr=scheduler_log,
                )
                wait_port(scheduler, control_port)
                wait_port(scheduler, data_port)
                mds = subprocess.Popen(
                    [args.mds, f"--config={mds_config}", f"--port={mds_port}",
                     "--bthread_concurrency=4"],
                    stdout=mds_log, stderr=mds_log,
                )
                wait_port(mds, mds_port)
                result = subprocess.run(
                    [args.probe, f"127.0.0.1:{mds_port}",
                     f"127.0.0.1:{control_port}", f"127.0.0.1:{data_port}",
                     "model-disk-a", "model-disk-b", "model-optical"],
                    capture_output=True, text=True, timeout=40,
                )
                if result.returncode != 0:
                    raise AssertionError(result.stdout + result.stderr)
                print(result.stdout, end="")
                inode_rows = [row for row in result.stdout.splitlines()
                              if row.startswith("INODE=")]
                if len(inode_rows) != 1:
                    raise AssertionError("probe did not report the committed inode")
                inode = inode_rows[0].split("=", 1)[1]
                scheduler_rpc(control_port, "StopNode", node_id="model-optical-target", force=True)
                scheduler_rpc(control_port, "RemoveManagedNode", node_id="model-optical")
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    task = scheduler_rpc(control_port, "GetDrainMigration",
                                         node_id="model-optical").get("task", {})
                    if "no admitted model optical target" in task.get("last_error", ""):
                        source_state = managed_node(control_port, "model-optical")
                        if int(source_state.get("resident_data_bytes", 0)) == 0:
                            raise AssertionError("source cleared without migration target")
                        break
                    time.sleep(0.05)
                else:
                    raise AssertionError("missing explicit no-target migration error")
                scheduler_rpc(control_port, "StartNode", node_id="model-optical-target")
                print("missing optical migration target blocks and preserves source inventory: PASS")
                drained = subprocess.Popen(
                    [args.probe, "--drain-optical", f"127.0.0.1:{mds_port}",
                     f"127.0.0.1:{control_port}", f"127.0.0.1:{data_port}", inode,
                     "model-optical", "model-optical-target"], stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True)
                try:
                    deadline = time.monotonic() + 15
                    while time.monotonic() < deadline:
                        state = managed_node(control_port, "model-optical-target")
                        if int(state.get("optical_library", {}).get("pending_write_tasks", 0)):
                            break
                        if drained.poll() is not None:
                            raise AssertionError("optical drain ended before crash injection")
                        time.sleep(0.02)
                    else:
                        raise AssertionError("no observable in-flight optical migration")
                    scheduler_rpc(control_port, "StopNode", node_id="model-optical-target", force=True)
                    time.sleep(2)  # 大于模型刻录固定时延，关闭期间仍不得结束后台任务。
                    stopped_state = managed_node(control_port, "model-optical-target")
                    if (not int(stopped_state.get("optical_library", {}).get("pending_write_tasks", 0)) or
                            stopped_state.get("power_level", "MANAGED_POWER_OFF") not in (0, "MANAGED_POWER_OFF")):
                        raise AssertionError("stopped optical tasks advanced or power was raised")
                    scheduler_rpc(control_port, "StartNode", node_id="model-optical-target")
                    print("forced logical stop checkpoints tasks and freezes modeled progress: PASS")
                    scheduler.kill()
                    scheduler.wait(timeout=8)
                    scheduler = subprocess.Popen(
                        [args.scheduler, f"--config={scheduler_config}",
                         f"--port={control_port}", "--bthread_concurrency=4"],
                        stdout=scheduler_log, stderr=scheduler_log)
                    wait_port(scheduler, control_port)
                    wait_port(scheduler, data_port)
                    scheduler_rpc(control_port, "StartNode", node_id="model-optical-target")
                    stdout, stderr = drained.communicate(timeout=40)
                    if drained.returncode != 0:
                        raise AssertionError(stdout + stderr)
                    print(stdout, end="")
                    print("SIGKILL during optical transfer/burn, durable outbox recovery: PASS")
                    started = time.monotonic()
                    reboot = scheduler_rpc(control_port, "RebootNode", node_id="model-optical-target")
                    reboot_operation = reboot["operation"]["operation_id"]
                    if time.monotonic() - started < 0.95:
                        raise AssertionError("configured 1000 ms logical reboot latency was skipped")
                    rebooted_state = managed_node(control_port, "model-optical-target")
                    if rebooted_state.get("administrative_state") not in (1, "MANAGED_ADMIN_ENABLED"):
                        raise AssertionError(f"successful reboot left node disabled: {rebooted_state}")
                    print("logical reboot duration and CMS re-admission: PASS")
                finally:
                    stop(drained)
                stop(mds)
                scheduler.kill()  # 生命周期成功应答已持久化，立即强杀不能恢复成禁用中间态。
                scheduler.wait(timeout=8)
                mds = scheduler = None
                scheduler = subprocess.Popen(
                    [args.scheduler, f"--config={scheduler_config}",
                     f"--port={control_port}", "--bthread_concurrency=4"],
                    stdout=scheduler_log, stderr=scheduler_log,
                )
                wait_port(scheduler, control_port)
                wait_port(scheduler, data_port)
                recovered_operation = scheduler_rpc(control_port, "GetOperationStatus", operation_id=reboot_operation)
                if recovered_operation["operation"]["status"] not in (2, "NODE_OP_SUCCEEDED"):
                    raise AssertionError(f"successful lifecycle operation was not restored: {recovered_operation}")
                mds = subprocess.Popen(
                    [args.mds, f"--config={mds_config}", f"--port={mds_port}",
                     "--bthread_concurrency=4"],
                    stdout=mds_log, stderr=mds_log,
                )
                wait_port(mds, mds_port)
                restored = subprocess.run(
                    [args.probe, "--verify-restored", f"127.0.0.1:{mds_port}",
                     f"127.0.0.1:{control_port}", f"127.0.0.1:{data_port}",
                     inode, "model-optical-target"],
                    capture_output=True, text=True, timeout=40,
                )
                if restored.returncode != 0:
                    raise AssertionError(restored.stdout + restored.stderr)
                print(restored.stdout, end="")
                print("full-disc read-only completion and lifecycle ACK followed by SIGKILL recovery: PASS")
                for node_id, label in (("model-disk-b", "MANAGED_ACCESS_MIGRATION"),
                                       ("model-optical-target", "MANAGED_ACCESS_MIGRATION"),
                                       ("model-optical", "MANAGED_ACCESS_ARCHIVE")):
                    streams = managed_node(control_port, node_id).get("metrics_streams", [])
                    total = sum(int(stream.get("write_bytes", 0)) for stream in streams
                                if stream.get("access_path") == label)
                    if total < 40:
                        raise AssertionError(f"missing classified workload counters: {node_id} {streams}")
                subprocess.run(
                    [args.probe, "--archive-inventory", f"127.0.0.1:{data_port}",
                     "model-optical-target", "10", "model", inode], check=True, timeout=30)
                extra_inode = next(row.split("=", 1)[1] for row in result.stdout.splitlines()
                                   if row.startswith("EXTRA_INODE="))
                subprocess.run(
                    [args.probe, "--verify-restored", f"127.0.0.1:{mds_port}",
                     f"127.0.0.1:{control_port}", f"127.0.0.1:{data_port}",
                     extra_inode, "model-optical-target"], check=True, timeout=40)
                verify_fuse_read(root, args.client, args.ctl, mds_port, control_port)
                for mode in ("FAILURE", "TIMEOUT"):
                    stop(scheduler)
                    for filename in ("model-disk-a.conf", "model-disk-b.conf", "optical.conf", "optical-target.conf"):
                        path = root / filename
                        base = "\n".join(line for line in path.read_text().splitlines()
                                         if not line.startswith(("IO_FAILURE_", "IO_TIMEOUT_")))
                        path.write_text(base + "\nIO_TIMEOUT_MS=50\n" +
                            f"IO_FAILURE_PROBABILITY_PPM={1000000 if mode == 'FAILURE' else 0}\n" +
                            f"IO_TIMEOUT_PROBABILITY_PPM={1000000 if mode == 'TIMEOUT' else 0}\n")
                    scheduler = subprocess.Popen(
                        [args.scheduler, f"--config={scheduler_config}", f"--port={control_port}",
                         "--bthread_concurrency=4"], stdout=scheduler_log, stderr=scheduler_log)
                    wait_port(scheduler, control_port)
                    wait_port(scheduler, data_port)
                    subprocess.run([args.probe, "--model-faults", f"127.0.0.1:{mds_port}",
                        f"127.0.0.1:{data_port}", "model-disk-b", "8", "model-optical-target", "10",
                        inode, f"MODEL_IO_INJECTED_{mode}"], check=True, timeout=40)
                (root / "hot.conf").write_text(
                    "NODE_ID=hot-api-disk\nINSTANCE_GENERATION=11\nDISK_IDS=disk-01\n"
                    "ARCHIVE_META_DIR=hot-api-state\nDISK_CAPACITY_BYTES=100000\nREPLICATION_ENABLED=false\n"
                    "READ_BYTES_PER_SEC=6\nIO_QUEUE_LIMIT=4\n")
                scheduler_config.write_text(scheduler_config.read_text().replace(
                    "SIMULATION_DISK_CONFIGS=model-disk-a.conf,model-disk-b.conf",
                    "SIMULATION_DISK_CONFIGS=model-disk-a.conf,model-disk-b.conf,hot.conf"))
                reload = scheduler_rpc(control_port, "ReloadModelConfigs")
                if not reload.get("accepted"):
                    raise AssertionError(f"model reload was not accepted: {reload}")
                subprocess.run([args.probe, "--hot-model-check", f"127.0.0.1:{data_port}",
                                "hot-api-disk", "11", "write", f"127.0.0.1:{control_port}"], check=True, timeout=40)
                stop(scheduler)
                scheduler = subprocess.Popen([args.scheduler, f"--config={scheduler_config}",
                    f"--port={control_port}", "--bthread_concurrency=4"], stdout=scheduler_log, stderr=scheduler_log)
                wait_port(scheduler, control_port)
                wait_port(scheduler, data_port)
                subprocess.run([args.probe, "--hot-model-check", f"127.0.0.1:{data_port}",
                                "hot-api-disk", "11", "read-drain", f"127.0.0.1:{control_port}"], check=True, timeout=40)
                deadline = time.monotonic() + 15
                while managed_node(control_port, "hot-api-disk").get("lifecycle") not in (5, "MANAGED_LIFECYCLE_RETIRED"):
                    if time.monotonic() >= deadline:
                        raise AssertionError("hot-added model did not finish CMS-confirmed retirement")
                    time.sleep(0.1)
                print("hot-add RPC, bounded concurrent IO/management isolation, recovery and final retirement: PASS")
        except BaseException:
            for filename in ("scheduler.log", "mds.log"):
                path = root / filename
                if path.exists():
                    print(f"--- {filename} ---\n{path.read_text(errors='replace')[-10000:]}")
            raise
        finally:
            stop(mds)
            stop(scheduler)


if __name__ == "__main__":
    main()
