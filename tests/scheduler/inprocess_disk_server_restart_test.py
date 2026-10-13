#!/usr/bin/env python3
"""Verify CMS-gated model IO and durable state after a Scheduler SIGKILL."""

import argparse
import pathlib
import signal
import socket
import subprocess
import tempfile
import time


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for_port(process, port, timeout=12):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"service exited before listening: {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise AssertionError(f"service did not listen on {port}")


def probe(binary, address, action, free_bytes, node_id="restart-disk"):
    return subprocess.run(
        [str(binary), address, node_id, action, str(free_bytes)],
        capture_output=True, text=True, timeout=5,
    )


def expect_probe(binary, address, action, free_bytes, timeout=15,
                 node_id="restart-disk"):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = probe(binary, address, action, free_bytes, node_id)
        if last.returncode == 0:
            return
        time.sleep(0.1)
    raise AssertionError(
        f"model {action} did not succeed: {last.stdout if last else ''} "
        f"{last.stderr if last else ''}"
    )


def start(binary, config, port, log):
    process = subprocess.Popen(
        [str(binary), f"--config={config}", f"--port={port}",
         "--bthread_concurrency=4"], stdout=log, stderr=log,
    )
    try:
        wait_for_port(process, port)
    except BaseException:
        stop(process, force=True)
        raise
    return process


def stop(process, force=False):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGKILL if force else signal.SIGINT)
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", type=pathlib.Path, required=True)
    parser.add_argument("--mds", type=pathlib.Path, required=True)
    parser.add_argument("--probe", type=pathlib.Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="zb-scheduler-restart-") as root_text:
        root = pathlib.Path(root_text)
        ports = set()
        while len(ports) < 3:
            ports.add(free_port())
        control_port, data_port, cms_port = sorted(ports)
        (root / "disk.conf").write_text(
            "NODE_ID=restart-disk\nDISK_IDS=disk-01\nARCHIVE_META_DIR=control\n"
            "DISK_CAPACITY_BYTES=1000000\nREAD_BYTES_PER_SEC=1000000\n"
            "WRITE_BYTES_PER_SEC=1000000\nREPLICATION_ENABLED=false\n",
            encoding="utf-8",
        )
        (root / "optical.conf").write_text(
            "NODE_ID=restart-optical\nSTATE_DIR=optical-control\n"
            "DRIVE_COUNT=1\nDISC_COUNT=3\nDISC_CAPACITY_BYTES=1000\n"
            "DISC_READ_BYTES_PER_SEC=1000\nCACHE_CAPACITY_BYTES=1000\n"
            "CACHE_READ_BYTES_PER_SEC=1000\nLOAD_LATENCY_MS=10\n",
            encoding="utf-8",
        )
        scheduler_config = root / "scheduler.conf"
        scheduler_config.write_text(
            "SIMULATION_DISK_CONFIGS=disk.conf\n"
            "SIMULATION_OPTICAL_CONFIGS=optical.conf\n"
            f"SIMULATION_DATA_PORT={data_port}\n"
            f"SIMULATION_DATA_ADDRESS=127.0.0.1:{data_port}\n"
            f"MDS_ADDRESS=127.0.0.1:{cms_port}\n"
            f"MANAGED_STATE_SNAPSHOT_PATH={root / 'managed.pb'}\n"
            "CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS=100\nTICK_INTERVAL_MS=500\n",
            encoding="utf-8",
        )
        (root / "mds").mkdir()
        mds_config = root / "mds.conf"
        mds_config.write_text(
            f"MDS_DB_PATH={root / 'mds' / 'db'}\n"
            f"SCHEDULER_ADDR=127.0.0.1:{control_port}\n"
            "MANAGED_NODE_ID=restart-mds\nOBJECT_UNIT_SIZE=4194304\nREPLICA=1\n"
            "STRICT_TIER_BYPASS_PG=true\nENABLE_OPTICAL_ARCHIVE=false\n"
            f"ARCHIVE_META_ROOT={root / 'mds' / 'archive'}\n"
            f"MASSTREE_ROOT={root / 'mds' / 'masstree'}\n",
            encoding="utf-8",
        )
        data_address = f"127.0.0.1:{data_port}"
        control_address = f"127.0.0.1:{control_port}"
        scheduler = mds = None
        with (root / "scheduler.log").open("ab") as scheduler_log, \
             (root / "mds.log").open("ab") as mds_log:
            try:
                scheduler = start(args.scheduler, scheduler_config, control_port, scheduler_log)
                wait_for_port(scheduler, data_port)
                expect_probe(args.probe, data_address, "blocked", 1000000)
                expect_probe(args.probe, control_address, "optical_status", 4000,
                             node_id="restart-optical")
                result = subprocess.run(
                    [str(args.probe), data_address, "restart-optical", "optical_blocked", "0"],
                    capture_output=True, text=True, timeout=5,
                )
                assert result.returncode == 0, result.stdout + result.stderr

                mds = start(args.mds, mds_config, cms_port, mds_log)
                expect_probe(args.probe, data_address, "report", 1000000)
                expect_probe(args.probe, data_address, "write", 1000000)
                expect_probe(args.probe, data_address, "commit", 1000000)
                expect_probe(args.probe, data_address, "report", 999960)
                assert (root / "managed.pb").exists(), "managed snapshot missing"
                # A populated model now admits a drain and migrates through
                # MDS; this fixture has no corresponding inode, so it does
                # not initiate retirement here.
                expect_probe(args.probe, control_address, "model_working", 0)
                expect_probe(args.probe, data_address, "read", 999960)

                (root / "new-disk.conf").write_text(
                    "NODE_ID=hot-added-disk\nDISK_IDS=disk-01\n"
                    "ARCHIVE_META_DIR=new-control\nDISK_CAPACITY_BYTES=1000000\n"
                    "READ_BYTES_PER_SEC=1000000\nWRITE_BYTES_PER_SEC=1000000\n"
                    "REPLICATION_ENABLED=false\n", encoding="utf-8",
                )
                scheduler_config.write_text(
                    scheduler_config.read_text(encoding="utf-8").replace(
                        "SIMULATION_DISK_CONFIGS=disk.conf\n",
                        "SIMULATION_DISK_CONFIGS=disk.conf,new-disk.conf\n",
                    ), encoding="utf-8",
                )
                scheduler.send_signal(signal.SIGHUP)
                expect_probe(args.probe, data_address, "report", 1000000,
                             node_id="hot-added-disk")

                stop(mds)
                mds = None
                stop(scheduler, force=True)
                scheduler = start(args.scheduler, scheduler_config, control_port, scheduler_log)
                wait_for_port(scheduler, data_port)
                # The old CMS location exists, but a new process must earn a
                # fresh catalog commit before serving it again.
                expect_probe(args.probe, data_address, "blocked", 999960)
                expect_probe(args.probe, control_address, "optical_status", 4000,
                             node_id="restart-optical")
                mds = start(args.mds, mds_config, cms_port, mds_log)
                expect_probe(args.probe, data_address, "report", 999960)
                expect_probe(args.probe, data_address, "report", 1000000,
                             node_id="hot-added-disk")
                expect_probe(args.probe, control_address, "remove_model", 0,
                             node_id="hot-added-disk")
                expect_probe(args.probe, control_address, "model_retired", 0,
                             node_id="hot-added-disk")
                expect_probe(args.probe, data_address, "missing", 0,
                             node_id="hot-added-disk")
                expect_probe(args.probe, control_address, "start_rejected", 0,
                             node_id="hot-added-disk")
                expect_probe(args.probe, data_address, "read", 999960)
                expect_probe(args.probe, data_address, "retry", 999960)
                expect_probe(args.probe, data_address, "conflict", 999960)
                expect_probe(args.probe, control_address, "stop_force", 0)
                expect_probe(args.probe, data_address, "blocked", 999960)
                time.sleep(1.1)
                expect_probe(args.probe, data_address, "blocked", 999960)
                expect_probe(args.probe, control_address, "start", 0)
                expect_probe(args.probe, data_address, "report", 999960)
                expect_probe(args.probe, control_address, "reboot", 0)
                expect_probe(args.probe, data_address, "blocked", 999960)
                expect_probe(args.probe, data_address, "report", 999960)
                expect_probe(args.probe, control_address, "remove_model", 0,
                             node_id="restart-optical")
                expect_probe(args.probe, control_address, "model_retired", 0,
                             node_id="restart-optical")
                expect_probe(args.probe, data_address, "optical_missing", 0,
                             node_id="restart-optical")
                expect_probe(args.probe, control_address, "start_rejected", 0,
                             node_id="restart-optical")
                stop(scheduler)
                scheduler = start(args.scheduler, scheduler_config, control_port, scheduler_log)
                wait_for_port(scheduler, data_port)
                expect_probe(args.probe, control_address, "model_retired", 0,
                             node_id="restart-optical")
                expect_probe(args.probe, data_address, "optical_missing", 0,
                             node_id="restart-optical")
                expect_probe(args.probe, data_address, "missing", 0,
                             node_id="hot-added-disk")
                expect_probe(args.probe, data_address, "report", 999960)
                stop(scheduler)
                scheduler = None
                print("Scheduler CMS gate, logical lifecycle, retirement and recovery: PASS")
            except BaseException:
                for name in ("scheduler.log", "mds.log"):
                    path = root / name
                    if path.exists():
                        print(f"--- {name} ---\n{path.read_text(errors='replace')[-5000:]}")
                raise
            finally:
                stop(scheduler, force=True)
                stop(mds)


if __name__ == "__main__":
    main()
