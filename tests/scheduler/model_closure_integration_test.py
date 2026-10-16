#!/usr/bin/env python3
"""Exercise two non-default-generation disk models and one optical model with real MDS."""

import argparse
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time


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
            "ARCHIVE_BURN_BYTES_PER_SEC=1000000\nARCHIVE_BURN_FIXED_MS=5\n",
            encoding="utf-8",
        )
        scheduler_config = root / "scheduler.conf"
        scheduler_config.write_text(
            "SIMULATION_DISK_CONFIGS=model-disk-a.conf,model-disk-b.conf\n"
            "SIMULATION_OPTICAL_CONFIGS=optical.conf\n"
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
                stop(mds)
                stop(scheduler)
                mds = scheduler = None
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
                restored = subprocess.run(
                    [args.probe, "--verify-restored", f"127.0.0.1:{mds_port}",
                     f"127.0.0.1:{control_port}", f"127.0.0.1:{data_port}",
                     inode, "model-optical"],
                    capture_output=True, text=True, timeout=40,
                )
                if restored.returncode != 0:
                    raise AssertionError(restored.stdout + restored.stderr)
                print(restored.stdout, end="")
                verify_fuse_read(root, args.client, args.ctl, mds_port, control_port)
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
