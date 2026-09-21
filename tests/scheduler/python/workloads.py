"""T03 batches use CMS placement and real node byte-preserving RPCs."""
import base64
import csv
import hashlib
import json
from pathlib import Path
import random
import re
import threading
import time

from .energy import summarize_run
from .support import Rpc, list_nodes, save_json


class Files:
    def __init__(self, mds, namespace):
        self.mds = Rpc(mds, "MdsService")
        self.namespace, self.files, self.clients = namespace, {}, {}

    def client(self, address):
        if address not in self.clients:
            self.clients[address] = Rpc(address, "RealNodeService")
        return self.clients[address]

    def create(self, path, size):
        location = self.mds.call("Create", path=self.namespace + "/" + path, mode=420,
                                 replica=1, object_unit_size=4194304)["location"]
        return self.accept_created(location, path, size)

    def mkdir(self, path):
        self.mds.call("Mkdir", path=path, mode=493)

    def accept_created(self, location, path, size):
        item = dict(location["disk_location"])
        item.update(inode_id=int(location["attr"]["inode_id"]), size=0, target_size=size,
                    path=self.namespace + "/" + path)
        # Cross-check the actual returned location against the committed CMS catalog.
        catalog = self.mds.call("GetCmsNodeCatalog")
        authorized = [e["managed_view"] for e in catalog.get("nodes", [])
                      if e["managed_view"]["spec"]["node_id"] == item["node_id"]]
        if not authorized or authorized[0].get("lifecycle") != "MANAGED_LIFECYCLE_WORKING":
            raise RuntimeError("allocation is not in the committed CMS working catalog")
        view = authorized[0]
        if (view.get("administrative_state") != "MANAGED_ADMIN_ENABLED" or
                view.get("service_mode") != "MANAGED_SERVICE_READ_WRITE" or
                not view.get("readiness", {}).get("inventory_ready")):
            raise RuntimeError("CMS assigned a target without write admission/readiness")
        if view["spec"].get("execution_mode") != "MANAGED_EXECUTION_PHYSICAL":
            raise RuntimeError("real byte validation requires physical storage, not virtual/simulated nodes")
        self.files[path] = item
        return item

    @staticmethod
    def payload(path, offset, length):
        # Distinct deterministic binary bytes per file; offsets preserve random I/O semantics.
        pattern = hashlib.sha256(path.encode()).digest()
        start = offset % len(pattern)
        return (pattern * ((start + length + 31) // 32))[start:start + length]

    def locate(self, path):
        item = self.files[path]
        location = self.mds.call("GetFileLocation", inode_id=item["inode_id"])["location"]
        if (int(location["attr"].get("size", 0)) != item["size"] or
                int(location["attr"].get("object_unit_size", 0)) != 4194304):
            raise RuntimeError("authoritative MDS file size/geometry differs from workload manifest")
        item.update(location["disk_location"])
        return item

    def write(self, path, offset=0, length=None):
        item = self.locate(path)
        node = self.client(item["node_address"])
        length = item["target_size"] if length is None else length
        allocation = node.call("AllocateFileWrite", inode_id=item["inode_id"], disk_id=item["disk_id"],
                               offset=offset, size=length, object_unit_size_hint=4194304)
        position = offset
        for part in allocation.get("slices", []):
            size = int(part["length"])
            data = self.payload(path, position, size)
            reply = node.call("WriteObject", disk_id=item["disk_id"], object_id=part["object_id"],
                              offset=int(part.get("object_offset", 0)), data=base64.b64encode(data).decode())
            if int(reply.get("bytes", 0)) != size:
                raise RuntimeError("short object write")
            position += size
        if position != offset + length:
            raise RuntimeError("incomplete write slice coverage")
        size = max(item["size"], offset + length)
        committed = node.call("CommitFileWrite", inode_id=item["inode_id"], txid=allocation["txid"],
                              file_size=size, object_unit_size=4194304,
                              expected_version=int(allocation.get("meta", {}).get("version", 0)),
                              allow_create=True, mtime_sec=int(time.time()))["meta"]
        self.mds.call("UpdateInodeStat", inode_id=item["inode_id"], file_size=size,
                      object_unit_size=4194304, version=int(committed.get("version", 0)),
                      mtime=int(committed.get("mtime_sec", 0)))
        item["size"] = size
        return item, length

    def read(self, path, offset=0, length=None):
        item = self.locate(path)
        length = item["size"] if length is None else length
        if offset < 0 or length <= 0 or offset + length > item["size"]:
            raise ValueError("read outside prepared file")
        node = self.client(item["node_address"])
        resolution = node.call("ResolveFileRead", inode_id=item["inode_id"], disk_id=item["disk_id"],
                               offset=offset, size=length, object_unit_size_hint=4194304)
        position = offset
        for part in resolution.get("slices", []):
            size = int(part["length"])
            reply = node.call("ReadObject", disk_id=item["disk_id"], object_id=part["object_id"],
                              offset=int(part.get("object_offset", 0)), size=size)
            data = base64.b64decode(reply.get("data", ""), validate=True)
            if data != self.payload(path, position, size):
                raise RuntimeError(f"byte mismatch: {path} at {position}")
            position += size
        if position != offset + length:
            raise RuntimeError("incomplete read slice coverage")
        return item, length

    def delete(self, path):
        item = self.locate(path)
        self.client(item["node_address"]).call("DeleteFileMeta", inode_id=item["inode_id"],
                                               disk_id=item["disk_id"], purge_objects=True)
        self.mds.call("Unlink", path=item["path"])
        del self.files[path]
        return item, 0

    def close(self):
        self.mds.close()
        for client in self.clients.values():
            client.close()


class Sampler:
    def __init__(self, scheduler, output, interval):
        self.rpc, self.interval = Rpc(scheduler, "SchedulerService"), interval
        self.file = Path(output).open("w")
        self.stop = threading.Event()
        self.error = None

    def sample(self):
        begin = time.time_ns() // 1000
        nodes = list_nodes(self.rpc)
        self.file.write(json.dumps({"timestamp_us": begin, "received_us": time.time_ns() // 1000,
                                    "nodes": nodes}) + "\n")
        self.file.flush()

    def __enter__(self):
        try:
            self.sample()
        except BaseException:
            self.file.close()
            self.rpc.close()
            raise
        def loop():
            try:
                while not self.stop.wait(self.interval):
                    self.sample()
            except Exception as exc:
                self.error = exc
        self.thread = threading.Thread(target=loop)
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()
        try:
            self.sample()
        finally:
            self.file.close()
            self.rpc.close()
        if self.error:
            raise RuntimeError(f"power sampling failed: {self.error}")


def load_trace(path):
    """T04 CSV interchange: timestamp_us,operation_type,logical_path,offset,size_bytes.

    Only the T03 read/write subset is accepted. T02 owns general filesystem replay.
    """
    requests, sizes, previous = [], {}, -1
    with open(path, newline="") as source:
        for row in csv.DictReader(source):
            operation = row["operation_type"]
            name = row["logical_path"].lstrip("/")
            timestamp, offset, size = int(row["timestamp_us"]), int(row["offset"]), int(row["size_bytes"])
            if not name or ".." in Path(name).parts or operation not in ("read", "write"):
                raise ValueError("T03 CSV supports safe relative file paths and read/write only")
            if timestamp < 0 or timestamp < previous or offset < 0 or size <= 0:
                raise ValueError("invalid trace timestamp/range")
            previous = timestamp
            sizes[name] = max(sizes.get(name, 0), offset + size)
            requests.append((operation, name, offset, size, timestamp))
    if not requests:
        raise ValueError("empty trace")
    return requests, sizes


def run_batch(output, scheduler, mds, workload, files=10000, min_size=4096, max_size=4096,
              seed=20260914, sample_interval=0.1, trace=None, reuse_run=None, mount=None):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    if files <= 0 or min_size <= 0 or max_size < min_size or sample_interval <= 0:
        raise ValueError("invalid workload scale or sampling interval")
    rng = random.Random(seed)
    sizes = {f"f{i:08d}": rng.randint(min_size, max_size) for i in range(files)}
    reused = None
    if reuse_run:
        if workload not in ("sequential_read", "random_rw", "typical_trace") or trace:
            raise ValueError("reuse requires read/random/typical workload without external trace")
        previous = Path(reuse_run)
        if json.loads((previous / "summary.json").read_text())["status"] != "PASS":
            raise ValueError("cannot reuse a failed write run")
        reused = json.loads((previous / "manifest.json").read_text())
        old_parameters = json.loads((previous / "parameters.json").read_text())
        if old_parameters["mds"] != mds:
            raise ValueError("reused files must belong to this MDS")
        sizes = {f["path"].removeprefix(old_parameters["namespace"] + "/"): f["size"] for f in reused}
        if not sizes or any(size <= 0 for size in sizes.values()):
            raise ValueError("reuse requires nonempty prepared files")
        files = len(sizes)
    requests = []
    if trace:
        requests, sizes = load_trace(trace)
        (output / "input_trace.csv").write_bytes(Path(trace).read_bytes())
    elif workload in ("sequential_write", "sequential_read"):
        op = "write" if workload == "sequential_write" else "read"
        for name, size in sizes.items():
            if workload == "sequential_write":
                requests.append(("create", name, 0, size, 0))
            requests.append((op, name, 0, size, 0))
    elif workload == "random_rw":
        names = list(sizes)
        for _ in range(files * 2):
            name = rng.choice(names)
            length = min(4096, sizes[name])
            requests.append((rng.choice(["read", "write"]), name,
                             rng.randint(0, sizes[name] - length), length, 0))
    elif workload == "typical_trace":
        # Each ten operations: create 10%, write 20%, read 60%, delete 10%.
        names = list(sizes)
        for i in range(max(1, (files + 9) // 10)):
            extra = f"created{i}"
            requests.append(("create", extra, 0, min_size, 0))
            requests.append(("write", extra, 0, min_size, 0))
            for _ in range(6):
                name = rng.choice(names)
                requests.append(("read", name, 0, sizes[name], 0))
            name = rng.choice(names)
            requests.extend([("write", name, 0, sizes[name], 0), ("delete", extra, 0, 0, 0)])
    else:
        raise ValueError(f"unknown workload {workload}")
    namespace = "/t03_" + output.parent.name.replace("-", "_") + "_" + output.name
    if mount:
        namespace = "/real" + namespace
    if reused is not None:
        namespace = old_parameters["namespace"]
    if mount:
        from .fuse_workloads import FuseFiles
        store = FuseFiles(mds, namespace, mount)
    else:
        store = Files(mds, namespace)
    parameters = {"workload": workload, "files": len(sizes), "min_size": min(sizes.values()),
                  "max_size": max(sizes.values()), "mean_size": sum(sizes.values()) / len(sizes),
                  "size_distribution": "external trace extents" if trace else "uniform integer (fixed if min=max)",
                  "seed": seed, "concurrency": 1, "repetitions": 1, "warmup": "none",
                  "expected_operations": len(requests), "namespace": namespace, "mds": mds,
                  "scheduler": scheduler, "sample_interval_s": sample_interval,
                  "max_sample_gap_us": max(5_000_000, int(sample_interval * 5e6)),
                  "power_source": "scheduler_model", "io_source": "fuse_posix" if mount else "real_rpc",
                  "mount": str(mount) if mount else None,
                  "reuse_run": str(reuse_run) if reuse_run else None,
                  "trace_scheduling": "serial; scheduled timestamps honored; no concurrency claim",
                  "write_validation": "read back all surviving files after timed interval"}
    save_json(output / "parameters.json", parameters)
    save_json(output / "requests.json", requests)
    try:
        entries = store.mds.call("GetCmsNodeCatalog").get("nodes", [])
        if not any(e["managed_view"]["spec"].get("kind") == "MANAGED_KIND_METADATA" for e in entries):
            raise ValueError("MDS managed metrics are required to account for all participating servers")
        for entry in entries:
            node = entry["managed_view"]
            if (node["spec"].get("kind") == "MANAGED_KIND_STORAGE" and
                    node["spec"].get("execution_mode") == "MANAGED_EXECUTION_PHYSICAL" and
                    node.get("lifecycle") == "MANAGED_LIFECYCLE_WORKING"):
                node_id = node["spec"]["node_id"]
                match = re.fullmatch(r"node-real-(\d+)", node_id)
                if not match or int(match[1]) <= 0 or node_id != f"node-real-{int(match[1]):02d}":
                    raise ValueError(f"MDS compact-location codec requires canonical positive node ID, got {node_id}")
        if reused is None:
            store.mkdir(namespace)
        directories = set()
        prep_begin = time.time_ns() // 1000
        for name, size in sizes.items():
            if reused is not None or workload == "sequential_write":
                continue
            parent = Path(name).parent
            for part in reversed([parent, *parent.parents]):
                if str(part) != "." and str(part) not in directories:
                    store.mkdir(namespace + "/" + str(part))
                    directories.add(str(part))
            store.create(name, size)
            store.write(name)
        if reused is not None:
            store.files = {f["path"].removeprefix(namespace + "/"): f for f in reused}
        save_json(output / "preparation.json", {"start_us": prep_begin, "end_us": time.time_ns() // 1000,
                                                "initial_files": list(store.files.values())})
        with (output / "operations.jsonl").open("w") as log, Sampler(scheduler, output / "power.jsonl", sample_interval):
            origin = time.monotonic_ns()
            for sequence, (op, name, offset, length, scheduled) in enumerate(requests):
                delay = origin + scheduled * 1000 - time.monotonic_ns()
                if delay > 0:
                    time.sleep(delay / 1e9)
                record = {"sequence": sequence, "path": name, "op": op, "offset": offset,
                          "requested_bytes": length, "bytes": 0, "scheduled_us": scheduled,
                          "start_us": time.time_ns() // 1000, "start_monotonic_ns": time.monotonic_ns(),
                          "ok": False}
                try:
                    if op == "create":
                        item, count = store.create(name, length), 0
                    elif op == "delete":
                        item, count = store.delete(name)
                    else:
                        item, count = getattr(store, op)(name, offset, length)
                    record.update(ok=True, node_id=item["node_id"], bytes=count)
                except Exception as exc:
                    record["error"] = str(exc)
                finally:
                    record.update(end_us=time.time_ns() // 1000, end_monotonic_ns=time.monotonic_ns())
                    log.write(json.dumps(record) + "\n")
                    log.flush()
                if not record["ok"]:
                    break
        save_json(output / "manifest.json", list(store.files.values()))
        # Outside the timed interval, verify all writes and publish T05 handoff locations.
        for name in store.files:
            if store.files[name]["size"]:
                store.read(name)
        save_json(output / "validation.json", {"status": "PASS", "files_checked": len(store.files),
                                                "method": "full deterministic byte readback"})
        summary = summarize_run(output)
        save_json(output / "summary.json", summary)
        save_json(output / "handoff.json", {"task": "T03 -> T05/T02", "mds": mds,
                  "read_write_service": "zb.rpc.RealNodeService", "data_retained": True,
                  "power_records": str(output / "power.jsonl"), "power_source": "scheduler_model",
                  "files": [{**item, "sha256": hashlib.sha256(store.payload(name, 0, item["size"])).hexdigest()}
                            for name, item in store.files.items()]})
        if summary["status"] != "PASS":
            raise RuntimeError("batch failed; see summary.json")
        return summary
    except BaseException as exc:
        save_json(output / "failure.json", {"error": str(exc), "status": "FAIL"})
        save_json(output / "summary.json", {"status": "FAIL", "error": str(exc)})
        raise
    finally:
        store.close()
