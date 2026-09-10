#!/usr/bin/env python3
"""Exercise the built client through a real FUSE mount against local test RPCs."""
import argparse
import csv
import json
import os
from pathlib import Path
import re
import select
import shutil
import subprocess
import sys
import tempfile
import time

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "tests/client/results"


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            raise RuntimeError("test process failed to shut down normally")
        return True
    return False


def validate_shutdown(returncode, sigterm_sent, log_text, row_count):
    # libfuse 3.10 maps a nonzero session-loop result (including SIGTERM)
    # to fuse_main status 7; 3.14 maps it to 8 (lib/helper.c).
    # Only tolerate these codes when this test deliberately sent SIGTERM.
    expected_exit = (0, 7, 8) if sigterm_sent else (0,)
    if returncode not in expected_exit:
        raise RuntimeError(f"client failed on shutdown (status={returncode}):\n" + log_text)
    summaries = re.findall(
        r"^\[io_latency\] submitted=(\d+) written=(\d+) synced=(\d+) dropped=(\d+) failed=(\d+)$",
        log_text, re.MULTILINE)
    expected_summary = (row_count, row_count, row_count, 0, 0)
    if not summaries or tuple(map(int, summaries[-1])) != expected_summary:
        raise RuntimeError("client did not persist all CSV rows on shutdown:\n" + log_text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True)
    parser.add_argument("--fixture", required=True)
    args = parser.parse_args()
    if not Path("/dev/fuse").exists() or not shutil.which("fusermount3"):
        print("SKIP: /dev/fuse or fusermount3 unavailable")
        return 77
    RESULTS.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mount-", dir=RESULTS) as tmp:
        root = Path(tmp)
        with (root / "fixture.log").open("w") as fixture_log:
            fixture = subprocess.Popen([args.fixture, "--serve-fixture"], stdout=subprocess.PIPE,
                                       stderr=fixture_log, text=True)
            try:
                if not select.select([fixture.stdout], [], [], 10)[0]:
                    raise RuntimeError("RPC fixture did not start")
                endpoints = json.loads(fixture.stdout.readline())
                for explicit in (False, True):
                    mount = root / ("mount-explicit" if explicit else "mount-default")
                    mount.mkdir()
                    conf = root / "base.conf"
                    conf.write_text(f"ROOT_PATH={root}/configured-root\n")
                    output = root / "custom output" if explicit else root / "configured-root/client/metrics"
                    flags = [f"--io_latency_dir={output}", f"--io_latency_base_conf={root}"] if explicit else [f"--io_latency_base_conf={conf}"]
                    log_path = root / f"client-{explicit}.log"
                    with log_path.open("w") as client_log:
                        client = subprocess.Popen([
                            args.client, f"--mds={endpoints['mds']}", f"--scheduler={endpoints['scheduler']}",
                            "--bootstrap_tier_dirs=false", "--default_object_unit_size=4",
                            "--io_latency_enabled=true", "--io_latency_flush_ms=60000", *flags,
                            "--", str(mount), "-f", "-o", "attr_timeout=0,entry_timeout=0",
                        ], stdout=client_log, stderr=subprocess.STDOUT)
                        try:
                            deadline = time.monotonic() + 10
                            while not os.path.ismount(mount) and client.poll() is None and time.monotonic() < deadline:
                                time.sleep(0.05)
                            if not os.path.ismount(mount):
                                text = log_path.read_text()
                                if any(reason in text.lower() for reason in ("operation not permitted", "permission denied", "device not found")):
                                    print("SKIP: kernel denied FUSE mount\n" + text)
                                    return 77
                                raise RuntimeError("client failed to mount:\n" + text)
                            subprocess.run([sys.executable, "-c", '''import os, sys
fd = os.open(sys.argv[1], os.O_RDWR)
try:
    # The first read on a fresh mount must reach FUSE; the read after writing
    # may be served by the kernel cache and is only a content check.
    assert len(os.pread(fd, 8, 0)) == 8
    assert os.pwrite(fd, b"aaaabbbb", 0) == 8
    assert os.pread(fd, 8, 0) == b"aaaabbbb"
finally:
    os.close(fd)
''', str(mount / "file")], check=True, timeout=15)
                            if explicit:
                                sigterm_sent = stop(client)
                            else:
                                subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=10)
                                client.wait(timeout=10)
                                sigterm_sent = False
                            files = list(output.glob("io_latency_*.csv"))
                            assert len(files) == 1, files
                            with files[0].open(newline="") as stream:
                                rows = list(csv.DictReader(stream))
                            assert {row["op"] for row in rows} == {"read", "write"}, rows
                            for row in rows:
                                assert int(row["ret"]) == 8, row
                                assert float(row["mds_us"]) > 0 and float(row["data_node_us"]) > 0, row
                                assert float(row["total_us"]) >= float(row["mds_us"]) + float(row["data_node_us"]), row
                            validate_shutdown(client.returncode, sigterm_sent, log_path.read_text(), len(rows))
                            if explicit:
                                assert not (output / "client/metrics").exists()
                            print(f"PASS: real FUSE {'explicit' if explicit else 'base.conf'} directory, read/write and shutdown sync ({len(rows)} rows)")
                        finally:
                            stop(client)
                            if os.path.ismount(mount):
                                subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=10)
            finally:
                stop(fixture)
                fixture.stdout.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
