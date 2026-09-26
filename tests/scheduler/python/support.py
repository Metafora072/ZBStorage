"""T03 HTTP/protobuf RPC support and isolated service fixture (stdlib only)."""
import http.client
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import time

SUITE = Path(__file__).resolve().parents[1]
ROOT = SUITE.parents[1]
RESULTS = SUITE / "results"


def result_dir(label):
    import tempfile
    RESULTS.mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=time.strftime("%Y%m%dT%H%M%S-") + label + "-", dir=RESULTS))


def save_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + "\n")


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Rpc:
    """One connection per caller/thread. Never retry mutating RPCs implicitly."""
    def __init__(self, endpoint, service):
        self.endpoint, self.service = endpoint, service
        self.connection = http.client.HTTPConnection(endpoint, timeout=30)

    def call(self, method, **request):
        self.connection.request("POST", f"/zb.rpc.{self.service}/{method}",
                                json.dumps(request), {"Content-Type": "application/json"})
        response = self.connection.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"{self.endpoint} {method}: HTTP {response.status}: {body[:500]!r}")
        result = json.loads(body)
        if "status" not in result:
            raise RuntimeError(f"{method}: missing protobuf status: {result}")
        code = result["status"].get("code", 0)
        if code not in (0, "SCHED_OK", "MDS_OK", "STATUS_OK"):
            raise RuntimeError(f"{method}: {result['status']}")
        return result

    def close(self):
        self.connection.close()


def list_nodes(rpc):
    nodes, offset = [], 0
    while True:
        reply = rpc.call("ListManagedNodes", include_retired=True, offset=offset, limit=1000)
        nodes.extend(reply.get("nodes", []))
        if not reply.get("has_more", False):
            return nodes
        following = int(reply["next_offset"])
        if following <= offset:
            raise RuntimeError("invalid managed-node pagination")
        offset = following


def wait_for(check, timeout=45):
    deadline, last = time.monotonic() + timeout, None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, RuntimeError, http.client.HTTPException) as error:
            last = error
        time.sleep(0.1)
    raise RuntimeError(f"readiness timeout: {last}")


class LocalCluster:
    """Private ports/DBs; stops only subprocesses this fixture started."""
    def __init__(self, output, build, node_count=2, simulated=False, optical=False):
        self.output, self.build = Path(output), Path(build).resolve()
        self.node_count, self.simulated, self.optical = node_count, simulated, optical
        self.processes, self.logs = [], []
        self.ports = {}

    def start(self, name, binary, config):
        executable = self.build / binary
        if not executable.is_file():
            raise RuntimeError(f"build target first: {executable}")
        if name not in self.ports:
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                self.ports[name] = reservation.getsockname()[1]
        port = self.ports[name]
        config = dict(config)
        if "NODE_ADDRESS" in config:
            config["NODE_ADDRESS"] = f"127.0.0.1:{port}"
        path = self.output / "config" / (name + ".conf")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(f"{k}={v}\n" for k, v in config.items()))
        log = (self.output / (name + ".log")).open("ab")
        self.logs.append(log)
        process = subprocess.Popen([str(executable), f"--config={path}", f"--port={port}",
                                    "--bthread_concurrency=4"], stdout=log, stderr=log)
        self.processes.append(process)
        def listening():
            if process.poll() is not None:
                raise ValueError(f"{name} exited: see {name}.log")
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        wait_for(listening)
        return f"127.0.0.1:{port}"

    def __enter__(self):
        try:
            self.output.mkdir(parents=True, exist_ok=True)
            # Choose MDS's endpoint before Scheduler starts; retain it on restart.
            if "mds" not in self.ports:
                with socket.socket() as s:
                    s.bind(("127.0.0.1", 0))
                    self.ports["mds"] = s.getsockname()[1]
            self.scheduler = self.start("scheduler", "scheduler_server", {
                "MDS_ADDRESS": f"127.0.0.1:{self.ports['mds']}", "TICK_INTERVAL_MS": 100,
                "SUSPECT_TIMEOUT_MS": 6000, "DEAD_TIMEOUT_MS": 15000,
                "POWER_STANDBY_AFTER_MS": 100 if self.simulated else 60000,
                "POWER_OFF_AFTER_MS": 86400000, "POWER_MINIMUM_RESIDENCY_MS": 0,
                "POWER_MIN_ONLINE_STORAGE_NODES": 0, "POWER_ACTUATION_ENABLED": "false",
                "MANAGED_STATE_SNAPSHOT_PATH": self.output / "scheduler.pb"})
            (self.output / "mds").mkdir(parents=True, exist_ok=True)
            config = {"MDS_DB_PATH": self.output / "mds/db", "SCHEDULER_ADDR": self.scheduler,
                      "MANAGED_NODE_ID": "t03-mds", "OBJECT_UNIT_SIZE": 4194304, "REPLICA": 1,
                      "STRICT_TIER_BYPASS_PG": "true", "ENABLE_OPTICAL_ARCHIVE": "false",
                      "ARCHIVE_META_ROOT": self.output / "mds/archive",
                      "MASSTREE_ROOT": self.output / "mds/masstree"}
            self.mds = self.start("mds", "mds_server", config)
            self.nodes = []
            if not self.simulated:
                for i in range(self.node_count):
                    node_id = f"node-real-{i + 1:02d}"
                    address = self.start(node_id, "real_node_server", {
                        "NODE_ID": node_id, "NODE_ADDRESS": "", "SCHEDULER_ADDR": self.scheduler,
                        "MDS_ADDR": self.mds, "HEARTBEAT_INTERVAL_MS": 200,
                        "DISK_BASE_DIR": self.output / node_id / "disks", "DISK_COUNT": 2,
                        "DISK_CAPACITY_BYTES": 1073741824, "REPLICATION_ENABLED": "false"})
                    self.nodes.append({"node_id": node_id, "address": address})
                cms = Rpc(self.mds, "MdsService")
                def ready():
                    entries = cms.call("GetCmsNodeCatalog").get("nodes", [])
                    has_metadata = any(e["managed_view"]["spec"].get("kind") == "MANAGED_KIND_METADATA"
                                       for e in entries)
                    return has_metadata and sum(e["managed_view"]["spec"]["node_id"] in {n["node_id"] for n in self.nodes} and
                               e["managed_view"].get("lifecycle") == "MANAGED_LIFECYCLE_WORKING"
                               for e in entries) == self.node_count
                try:
                    wait_for(ready)
                finally:
                    cms.close()
            if self.optical:
                (self.output / "optical").mkdir(parents=True, exist_ok=True)
                self.optical_address = self.start("optical", "optical_node_server", {
                    "NODE_ID": "t03-optical", "NODE_ADDRESS": "", "SCHEDULER_ADDR": self.scheduler,
                    "HEARTBEAT_INTERVAL_MS": 200, "ARCHIVE_ROOT": self.output / "optical/archive",
                    "VOLUME_SIZE_BYTES": 10485760, "CAPACITY_IN_IMAGES": 2,
                    "AVAILABLE_VOLUME_ID_COUNT": 1})
            binaries = ["scheduler_server", "mds_server"]
            if not self.simulated:
                binaries.append("real_node_server")
            if self.optical:
                binaries.append("optical_node_server")
            save_json(self.output / "environment.json", {
                "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "git_diff_stat": subprocess.check_output(["git", "diff", "--stat"], cwd=ROOT, text=True),
                "binary_sha256": {name: file_hash(self.build / name) for name in binaries},
                "test_source_sha256": {str(p.relative_to(SUITE)): file_hash(p)
                                       for p in sorted([*SUITE.glob("*.py"), *(SUITE / "python").glob("*.py")])
                                       if not p.name.startswith("test_") and p.name != "closure.py"},
                "host": socket.gethostname(), "scheduler": self.scheduler, "mds": self.mds,
                "nodes": self.nodes, "physical_power_actuation": False,
                "capacity_source": "configured test disk quota, not hardware inventory",
                "deployment": "multiple processes on one host; not multiple measured servers"})
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *args):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in self.logs:
            log.close()
        self.processes.clear()
        self.logs.clear()
