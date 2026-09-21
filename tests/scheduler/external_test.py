#!/usr/bin/env python3
"""T03 isolated external-filesystem FUSE experiment; never uses another running cluster."""
import argparse
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from python.fuse_workloads import FuseFiles, FuseMount, verify_phase_rpc
from python.meter_closure import run_meter_closure
from python.support import ROOT, LocalCluster, Rpc, file_hash, list_nodes, result_dir, save_json, wait_for
from python.workloads import run_batch
from python.hardware_power import (DEFAULT_SENSOR, HardwareSampler, finish_hardware_reports,
                            idle_window, run_hardware_workload)


def filesystem(path):
    raw = subprocess.check_output(["findmnt", "-J", "-T", str(path), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"], text=True)
    return json.loads(raw)["filesystems"][0]


def catalog(cluster):
    rpc = Rpc(cluster.mds, "MdsService")
    try:
        return rpc.call("GetCmsNodeCatalog")
    finally:
        rpc.close()


def verify_backend_objects(cluster_directory, handoff_path, output):
    """Independent file-system check: CMS locations map to real external object files."""
    handoff = json.loads(Path(handoff_path).read_text())
    indexes, counts = {}, {}
    for item in handoff["files"]:
        node_id = item["node_id"]
        root = Path(cluster_directory) / node_id / "disks"
        if node_id not in indexes:
            objects = {}
            for path in root.rglob("obj-*"):
                if path.is_file():
                    if path.name in objects:
                        raise RuntimeError(f"duplicate backend object: {path.name}")
                    objects[path.name] = path
            indexes[node_id] = objects
            counts[node_id] = {"files": 0, "bytes": 0, "objects": 0}
        digest = hashlib.sha256()
        remaining, index = item["size"], 0
        while remaining:
            path = indexes[node_id][f"obj-{item['inode_id']}-{index}"]
            if path.relative_to(root).parts[0] != item["disk_id"]:
                raise RuntimeError("backend disk differs from authoritative placement")
            data = path.read_bytes()
            length = min(remaining, 4194304)
            if len(data) != length:
                raise RuntimeError("backend object length mismatch")
            digest.update(data)
            remaining -= length
            index += 1
        if digest.hexdigest() != item["sha256"]:
            raise RuntimeError("backend object SHA-256 differs from FUSE handoff")
        counts[node_id]["files"] += 1
        counts[node_id]["bytes"] += item["size"]
        counts[node_id]["objects"] += index
    report = {"status": "PASS", "filesystem": filesystem(cluster_directory), "nodes": counts,
              "files": len(handoff["files"]), "method": "independent backend object length, placement and SHA-256"}
    save_json(output, report)
    return report


def migration(cluster, output):
    output.mkdir()
    with FuseMount(cluster, output / "client") as mount:
        store = FuseFiles(cluster.mds, "/real/t03_external_migration", mount)
        scheduler = Rpc(cluster.scheduler, "SchedulerService")
        cms = Rpc(cluster.mds, "MdsService")
        try:
            store.mkdir(store.namespace)
            item = store.create("cross_object.bin", 5 * 1024 * 1024 + 17)
            store.write("cross_object.bin")
            store.read("cross_object.bin")
            source = dict(item)
            scheduler.call("RemoveManagedNode", node_id=source["node_id"], request_ts_ms=time.time_ns() // 1_000_000)
            def retired():
                return next((n for n in list_nodes(scheduler) if n["spec"]["node_id"] == source["node_id"]
                             and n["lifecycle"] == "MANAGED_LIFECYCLE_RETIRED"), None)
            state = wait_for(retired, timeout=90)
            store.read("cross_object.bin")
            target = dict(item)
            if target["node_id"] == source["node_id"]:
                raise RuntimeError("migration did not change CMS authoritative location")
            node = Rpc(source["node_address"], "RealNodeService")
            try:
                objects = node.call("ListObjects", max_objects=100)
            finally:
                node.close()
            refs = cms.call("VerifyNodeReferences", node_id=source["node_id"])
            if int(objects.get("total_objects", 0)) != 0 or not refs.get("zero_references"):
                raise RuntimeError("source not empty / CMS still references source")
            fresh = store.create("post_retire.bin", 8192)
            if fresh["node_id"] == source["node_id"]:
                raise RuntimeError("retired source received a new file")
            store.write("post_retire.bin")
            store.read("post_retire.bin")
            save_json(output / "summary.json", {"status": "PASS", "source": source, "target": target,
                      "cms_references": refs, "retired_state": state, "new_file": fresh,
                      "io_source": "mounted_fuse", "byte_validation": "PASS", "physical_power_actuation": False})
        finally:
            store.close()
            scheduler.close()
            cms.close()


def health_recovery(cluster, output):
    """Stop only an owned node process; verify real timeout, exclusion and restart."""
    output.mkdir()
    started = time.time_ns() // 1000
    target = 'node-real-02'
    rpc = Rpc(cluster.scheduler, 'SchedulerService')
    config_path = cluster.output / 'config' / (target + '.conf')
    process = next(p for p in cluster.processes if '--config=' + str(config_path) in p.args)
    def view(health, authoritative=False):
        nodes = ([e['managed_view'] for e in catalog(cluster)['nodes']] if authoritative else list_nodes(rpc))
        return next((n for n in nodes if n['spec']['node_id'] == target and n.get('health') == health), None)
    with FuseMount(cluster, output / 'client') as mount:
        store = FuseFiles(cluster.mds, '/real/t03_health_recovery', mount)
        try:
            before = wait_for(lambda: view('MANAGED_HEALTH_HEALTHY', True))
            store.mkdir(store.namespace)
            original = []
            for i in range(20):
                name = f'original_{i}'
                original.append(dict(store.create(name, 8192)))
                store.write(name)
            if not any(f['node_id'] == target for f in original):
                raise RuntimeError('health fixture has no file on restart target')
            save_json(output / 'original_files.json', original)
            process.terminate()
            process.wait(timeout=10)
            suspect = wait_for(lambda: view('MANAGED_HEALTH_SUSPECT'), timeout=15)
            save_json(output / 'suspect.json', suspect)
            failed = wait_for(lambda: view('MANAGED_HEALTH_FAILED', True), timeout=25)
            save_json(output / 'cms_failed.json', failed)
            for i in range(10):
                name = f'during_failure_{i}'
                item = store.create(name, 8192)
                if item['node_id'] == target:
                    raise RuntimeError('CMS placed new file on failed node')
                store.write(name)
                store.read(name)
            config = dict(line.split('=', 1) for line in config_path.read_text().splitlines() if line)
            cluster.start(target, 'real_node_server', config)
            recovered = wait_for(lambda: view('MANAGED_HEALTH_HEALTHY', True))
            if before['compact_id'] != recovered['compact_id']:
                raise RuntimeError('recovered node compact ID changed')
            for i in range(20):
                store.read(f'original_{i}')
            save_json(output / 'summary.json', {
                'status': 'PASS', 'target': target, 'stopped_pid': process.pid,
                'suspect': suspect, 'cms_failed': failed, 'cms_recovered': recovered,
                'preserved_files_read_back': 20, 'writes_while_failed': 10,
                'scope': 'owned process graceful stop and heartbeat timeout; not disk failure or replica failover'})
        finally:
            store.close()
            rpc.close()
    save_json(output / 'window.json', {'start_us': started, 'end_us': time.time_ns() // 1000})


def run(args, output):
    reports = {}
    cluster = LocalCluster(output / "batch_cluster", args.build, node_count=2)
    with cluster:
        save_json(output / "catalog_before.json", catalog(cluster))
        with FuseMount(cluster, output / "batch_client") as mount:
            for workload in ("sequential_write", "sequential_read", "random_rw", "typical_trace"):
                directory = output / workload
                reports[workload] = run_batch(directory, cluster.scheduler, cluster.mds, workload,
                    files=args.files, min_size=4096, max_size=4096, mount=mount,
                    reuse_run=None if workload == "sequential_write" else output / "sequential_write")
                print(f"{workload}: PASS", flush=True)
            if args.trace:
                reports["external_trace"] = run_batch(output / "external_trace", cluster.scheduler, cluster.mds,
                    "typical_trace", trace=args.trace, mount=mount)
                save_json(output / "trace_source.json", {"path": str(Path(args.trace).resolve()),
                          "sha256": file_hash(args.trace), "use": "copied read-only; remapped to private test namespace"})
                print("external_trace: PASS", flush=True)
        for name in reports:
            verify_phase_rpc(output / name, output / "batch_client")
        before = catalog(cluster)
        save_json(output / "catalog_before_restart.json", before)
        save_json(output / "environment_before_restart.json", json.loads((cluster.output / "environment.json").read_text()))
    print("batch services stopped; restarting retained databases and objects", flush=True)
    # Same ports, configs and data; fresh server processes, fresh FUSE client.
    with cluster:
        with FuseMount(cluster, output / "restart_client") as mount:
            reports["restart_read"] = run_batch(output / "restart_read", cluster.scheduler, cluster.mds,
                "sequential_read", reuse_run=output / "sequential_write", mount=mount)
        verify_phase_rpc(output / "restart_read", output / "restart_client")
        after = catalog(cluster)
        save_json(output / "catalog_after_restart.json", after)
        old_manifest = json.loads((output / "sequential_write/manifest.json").read_text())
        new_manifest = json.loads((output / "restart_read/manifest.json").read_text())
        if old_manifest != new_manifest:
            raise RuntimeError("restart changed file inode, size or placement")
        save_json(output / "restart_validation.json", {"status": "PASS", "files": len(new_manifest),
                  "method": "graceful service restart, new FUSE mount, complete readback; not a power-loss test"})
        def identities(snapshot):
            return {e["managed_view"]["spec"]["node_id"]: e["managed_view"].get("compact_id")
                    for e in snapshot["nodes"]}
        if identities(before) != identities(after) or not all(identities(after).values()):
            raise RuntimeError("CMS compact IDs changed across restart")
        save_json(output / "cms_identity_validation.json", {"status": "PASS", "compact_ids": identities(after)})
        verify_backend_objects(cluster.output, output / "sequential_write/handoff.json",
                               output / "backend_validation.json")
    print("restart_read: PASS", flush=True)
    # Keep drain migration bounded, without moving the entire benchmark dataset.
    with LocalCluster(output / "migration_cluster", args.build, node_count=2) as migration_cluster:
        start = time.time_ns() // 1000
        migration(migration_cluster, output / "migration")
        save_json(output / "migration/window.json", {"start_us": start, "end_us": time.time_ns() // 1000})
    print("FUSE drain migration: PASS", flush=True)
    with LocalCluster(output / 'health_cluster', args.build, node_count=2) as health_cluster:
        health_recovery(health_cluster, output / 'health_recovery')
    print('real heartbeat timeout, CMS exclusion and node recovery: PASS', flush=True)
    with LocalCluster(output / "meter_cluster", args.build, node_count=1) as meter_cluster:
        with FuseMount(meter_cluster, output / "meter_client") as mount:
            if args.hardware_power:
                meter = run_hardware_workload(meter_cluster, output / "meter", args.meter_files,
                                             mount, args.idle_seconds)
            else:
                meter = run_meter_closure(meter_cluster, output / "meter", args.meter_files, mount=mount)
        for phase in ("write", "read"):
            verify_phase_rpc(output / "meter" / phase, output / "meter_client")
    print("FUSE same-file closure: PASS (" + meter["source"] + ")", flush=True)
    from run import simulation
    with LocalCluster(output / "simulation_cluster", args.build, simulated=True) as simulated:
        sim = simulation(simulated, output / "simulation")
    summary = {"status": "PENDING_ANALYSIS" if args.hardware_power else "PASS", "filesystem": filesystem(output), "workloads": {
        name: {key: r[key] for key in ("status", "completed_operations", "final_files", "final_bytes",
                                     "total_execution_s", "model_energy_j")} for name, r in reports.items()},
        "meter": {"status": meter["status"], "source": meter["source"], "files": meter["files"]},
        "simulation": {"status": sim["status"], "energy_j": sim["energy_j"]},
        "migration": "PASS", "restart": "PASS", "health_recovery": "PASS", "hardware_energy_status": "NOT_MEASURED",
        "deployment": "one physical host, private current-version services on external RAID, loopback RPC",
        "scope": "FUSE + CMS/MDS/RocksDB + Scheduler + real nodes; excludes optical archive/replication/physical power actuation",
        "cache": "O_DIRECT on workload FUSE file opens; backend OS/device caches not dropped",
        "concurrency": "one workload request and one FUSE callback worker (-s); not concurrent-write acceptance",
        "data_origin": "deterministic generated payload; optional existing T04 trace, no shared database reuse"}
    return summary


def execute(args, output, index):
    """Publish final success only after service cleanup and meter analysis succeed."""
    save_json(output / "summary.json", {"status": "RUNNING"})
    try:
        context = (HardwareSampler(output / "hardware_power", args.power_sensor, args.raid_device)
                   if args.hardware_power else nullcontext())
        with context as sampler:
            if sampler:
                save_json(output / "baseline_before/window.json", idle_window(args.idle_seconds))
            summary = run(args, output)
            if sampler:
                save_json(output / "baseline_after/window.json", idle_window(args.idle_seconds))
        if sampler:
            summary = finish_hardware_reports(output, sampler, summary)
        save_json(output / "summary.json", summary)
        save_json(index / "summary.json", summary)
        print(f"Software closed loop: {summary['status']}; "
              f"hardware energy: {summary['hardware_energy_status']}; "
              f"capacity/power target: {summary.get('energy_target_status', 'NOT_MEASURED')}; "
              f"results: {output}", flush=True)
        return 0
    except BaseException as exc:
        failure = {"status": "FAIL", "error": repr(exc), "external_results": str(output)}
        save_json(output / "failure.json", failure)
        save_json(output / "summary.json", failure)
        save_json(index / "summary.json", failure)
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-root", default="/mnt/md0", type=Path)
    parser.add_argument("--build", default=str(ROOT / "build-release"))
    parser.add_argument("--files", default=10000, type=int)
    parser.add_argument("--meter-files", default=1000, type=int)
    parser.add_argument("--trace", type=Path)
    parser.add_argument("--hardware-power", action="store_true", help="read live ACPI host meter; never falls back to simulation")
    parser.add_argument("--power-sensor", type=Path, default=DEFAULT_SENSOR)
    parser.add_argument("--raid-device", default="md0", help="Linux MD device supplying the external filesystem")
    parser.add_argument("--idle-seconds", default=30.0, type=float)
    args = parser.parse_args()
    backing = filesystem(args.external_root)
    if (backing["target"] == "/" or backing["fstype"].startswith("fuse") or
            not args.external_root.is_dir() or shutil.disk_usage(args.external_root).free < 2 * 1024**3):
        raise ValueError("dedicated external filesystem with at least 2 GiB free required")
    if args.files <= 0 or args.meter_files <= 0:
        raise ValueError("positive file counts required")
    if args.trace and not args.trace.is_file():
        raise ValueError("trace input missing")
    if args.idle_seconds <= 0 or args.idle_seconds > 60:
        raise ValueError("idle window must be > 0 and <= 60 seconds")
    if args.hardware_power and backing["source"] != "/dev/" + args.raid_device:
        raise ValueError("observed array must be the workload filesystem's actual backing device")
    output = Path(tempfile.mkdtemp(prefix="zbstorage_t03_fuse_" + time.strftime("%Y%m%dT%H%M%S_"), dir=args.external_root))
    index = result_dir("external-index")
    save_json(index / "external_run.json", {"path": str(output), "filesystem": backing})
    save_json(output / "parameters.json", {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()})
    print(f"External results: {output}\nRepository index: {index}", flush=True)
    return execute(args, output, index)


if __name__ == "__main__":
    raise SystemExit(main())
