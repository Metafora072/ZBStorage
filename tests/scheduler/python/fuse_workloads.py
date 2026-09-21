"""POSIX workload adapter: all mutations and payload IO pass through mounted FUSE."""
import csv
import json
import os
from pathlib import Path
import re
import subprocess

from .support import file_hash, save_json, wait_for
from .workloads import Files


class FuseFiles(Files):
    def __init__(self, mds, namespace, mount):
        super().__init__(mds, namespace)
        self.mount = Path(mount)
        if not os.path.ismount(self.mount):
            raise ValueError("real FUSE mount required, not a plain local directory")

    def physical_path(self, logical):
        if not logical.startswith("/real/") or ".." in Path(logical).parts:
            raise ValueError("test path must stay under /real")
        return self.mount / logical.lstrip("/")

    def mkdir(self, path):
        self.physical_path(path).mkdir(mode=0o755)

    def create(self, path, size):
        logical = self.namespace + "/" + path
        fd = os.open(self.physical_path(logical), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        os.close(fd)
        inode = self.mds.call("Lookup", path=logical)["attr"]["inode_id"]
        location = self.mds.call("GetFileLocation", inode_id=inode)["location"]
        return self.accept_created(location, path, size)

    def write(self, path, offset=0, length=None):
        item = self.locate(path)
        length = item["target_size"] if length is None else length
        data = self.payload(path, offset, length)
        fd = os.open(self.physical_path(item["path"]), os.O_WRONLY | os.O_DIRECT)
        try:
            if os.pwrite(fd, data, offset) != length:
                raise RuntimeError("short FUSE write")
        finally:
            os.close(fd)
        item["size"] = max(item["size"], offset + length)
        self.locate(path)  # independently observe the client's MDS commit
        return item, length

    def read(self, path, offset=0, length=None):
        item = self.locate(path)
        length = item["size"] if length is None else length
        if offset < 0 or length <= 0 or offset + length > item["size"]:
            raise ValueError("read outside prepared file")
        fd = os.open(self.physical_path(item["path"]), os.O_RDONLY | os.O_DIRECT)
        try:
            data = os.pread(fd, length, offset)
        finally:
            os.close(fd)
        if data != self.payload(path, offset, length):
            raise RuntimeError(f"FUSE byte mismatch: {path} at {offset}")
        return item, length

    def delete(self, path):
        item = self.locate(path)
        os.unlink(self.physical_path(item["path"]))
        del self.files[path]
        return item, 0


class FuseMount:
    """Own mount/process only. Workload O_DIRECT bypasses FUSE cache, not backend OS cache."""
    def __init__(self, cluster, output):
        self.cluster, self.output = cluster, Path(output)
        self.mount = self.output / "mnt"
        self.process = None

    def __enter__(self):
        self.output.mkdir(parents=True, exist_ok=True)
        self.mount.mkdir(exist_ok=True)
        if os.path.ismount(self.mount):
            raise ValueError("refusing to reuse an existing mount")
        command = [str(self.cluster.build / "zb_fuse_client"), f"--mds={self.cluster.mds}",
                   f"--scheduler={self.cluster.scheduler}", "--timeout_ms=30000",
                   "--bootstrap_tier_dirs=true", "--io_latency_enabled=true",
                   f"--io_latency_dir={self.output / 'metrics'}", "--io_latency_flush_ms=100",
                   "--", str(self.mount), "-f", "-s", "-o", "attr_timeout=0,entry_timeout=0"]
        save_json(self.output / "command.json", {"argv": command,
                  "fuse_workers": 1, "reason": "serial T03 baseline; multi-callback file-size commit race tracked separately",
                  "binary_sha256": file_hash(self.cluster.build / "zb_fuse_client")})
        self.log = (self.output / "fuse.log").open("wb")
        try:
            self.process = subprocess.Popen(command, stdout=self.log, stderr=subprocess.STDOUT)
            def ready():
                if self.process.poll() is not None:
                    raise ValueError("FUSE exited; see fuse.log")
                return os.path.ismount(self.mount) and (self.mount / "real").is_dir()
            wait_for(ready)
            return self.mount
        except BaseException as exc:
            self.__exit__(type(exc), None, None)
            raise

    def __exit__(self, kind, *args):
        try:
            try:
                if os.path.ismount(self.mount):
                    subprocess.run(["fusermount3", "-u", str(self.mount)], check=True, timeout=15)
            finally:
                # Unmount failure must not skip stopping our own client process.
                if self.process is not None:
                    try:
                        self.process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        self.process.terminate()
                        try:
                            self.process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            self.process.kill()
                            self.process.wait()
            if kind is None and self.process is not None and self.process.returncode != 0:
                raise RuntimeError("FUSE did not exit cleanly after unmount")
        finally:
            self.log.close()
        if kind is None:
            rows = []
            for path in (self.output / "metrics").glob("io_latency_*.csv"):
                with path.open(newline="") as source:
                    rows.extend(csv.DictReader(source))
            match = re.findall(r"\[io_latency\] submitted=(\d+) written=(\d+) synced=(\d+) dropped=(\d+) failed=(\d+)",
                               (self.output / "fuse.log").read_text())
            if not match or tuple(map(int, match[-1])) != (len(rows), len(rows), len(rows), 0, 0):
                raise RuntimeError("client metrics lost records; see fuse.log")
            if not rows or any(int(r["ret"]) < 0 for r in rows):
                raise RuntimeError("missing or failed FUSE IO records")
            save_json(self.output / "metrics_validation.json", {"status": "PASS", "rows": len(rows),
                      "read_rows": sum(r["op"] == "read" for r in rows),
                      "write_rows": sum(r["op"] == "write" for r in rows), "dropped": 0})


def verify_phase_rpc(batch, fuse_output):
    """Cross-check measured workload bytes against actual client MDS/data RPC traces."""
    batch, fuse_output = Path(batch), Path(fuse_output)
    summary = json.loads((batch / "summary.json").read_text())
    rows = []
    for path in (fuse_output / "metrics").glob("io_latency_*.csv"):
        with path.open(newline="") as source:
            rows.extend(r for r in csv.DictReader(source)
                        if summary["start_us"] <= int(r["start_time_unix_us"]) <= summary["end_us"])
    totals = {}
    for op in ("read", "write"):
        selected = [r for r in rows if r["op"] == op]
        expected = sum(d[op + "_bytes"] for d in summary["distribution"])
        actual = sum(int(r["returned_bytes"]) for r in selected)
        if actual != expected or any(int(r["ret"]) < 0 or float(r["mds_us"]) <= 0 or
                                     float(r["data_node_us"]) <= 0 for r in selected):
            raise RuntimeError(f"{batch.name}: actual {op} RPC bytes/time do not match workload: {actual} != {expected}")
        totals[op] = {"bytes": actual, "client_requests": len(selected)}
    save_json(batch / "fuse_rpc_validation.json", {"status": "PASS", "totals": totals})
