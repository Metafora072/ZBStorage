#!/usr/bin/env python3
"""Executable T03 task suite. See README.md for hardware and scope boundaries."""
import argparse
import json
from pathlib import Path
import sys
import time

from python.energy import meter_summary, summarize_run, capacity_power_ratio
from python.support import ROOT, Rpc, LocalCluster, list_nodes, result_dir, save_json, wait_for
from python.workloads import run_batch

WORKLOADS = ("sequential_write", "sequential_read", "random_rw", "typical_trace")


def simulation(cluster, output):
    """Use real CMS placement, simulated hardware, and actual Scheduler power policy."""
    rpc, cms = Rpc(cluster.scheduler, "SchedulerService"), Rpc(cluster.mds, "MdsService")
    start = 1000
    ids = [f"node-real-{i + 1:02d}" for i in range(10)]
    for node_id in ids:
        rpc.call("RegisterManagedNode", spec={"node_id": node_id, "kind": "MANAGED_KIND_STORAGE",
                 "execution_mode": "MANAGED_EXECUTION_SIMULATED", "logical_node_count": 1,
                 "participates_in_placement": True, "participates_in_placement_set": True,
                 "external_network_bandwidth_bytes_per_sec": 1_000_000_000,
                 "device_groups": [{"name": "t03-disks", "kind": "MANAGED_DEVICE_HDD",
                                    "device_capacity_bytes": 1_100_000_000_000_000, "device_count": 1,
                                    "max_concurrency": 1, "per_device_bandwidth_bytes_per_sec": 1_000_000_000}],
                 "power": {"peak_milliwatts": 1_000_000, "medium_milliwatts": 1_000_000,
                           "standby_milliwatts": 10_000, "off_milliwatts": 0}},
                 join_time_ms=start, lifetime_seed=42, activate_immediately=True)
        rpc.call("ReportHeartbeat", node_id=node_id, node_type="NODE_REAL", address="127.0.0.1:1",
                 report_ts_ms=start, readiness_reported=True, initialization_complete=True,
                 metadata_ready=True, disks=[{"disk_id": "disk0", "capacity_bytes": 1_100_000_000_000_000,
                 "free_bytes": 1_100_000_000_000_000, "is_healthy": True,
                 "device_kind": "MANAGED_DEVICE_HDD"}])
    wait_for(lambda: sum(e["managed_view"]["spec"]["node_id"] in ids
                         for e in cms.call("GetCmsNodeCatalog").get("nodes", [])) == 10)
    catalog = cms.call("GetCmsNodeCatalog")
    save_json(output / "cms_before.json", catalog)
    # Scheduler proposal -> committed CMS -> MDS allocator selects the sole target.
    location = cms.call("Create", path="/t03_one_node_workload", mode=420,
                        replica=1, object_unit_size=4194304)["location"]
    selected = location["disk_location"]["node_id"]
    if selected not in ids:
        raise RuntimeError("CMS selected unexpected target")
    rpc.call("AdvanceSimulationTime", target_time_us=1_100_000)
    def active(t):
        rpc.call("ReportNodeAccessEvent", node_id=selected, event_time_us=t * 1000,
                 operation="MANAGED_OPERATION_READ", bytes=100_000_000, queue_depth=8)
    def snapshot(t):
        nodes = [n for n in list_nodes(rpc) if n["spec"]["node_id"] in ids]
        if len(nodes) != 10:
            raise RuntimeError("simulated node count differs from task")
        for n in nodes:
            expected = "MANAGED_POWER_PEAK" if n["spec"]["node_id"] == selected else "MANAGED_POWER_STANDBY"
            if n["power_level"] != expected or int(n["capacity_bytes"]) != 1_100_000_000_000_000:
                raise RuntimeError(f"node state/capacity mismatch: {n['spec']['node_id']}")
        return {"simulation_time_ms": t, "nodes": nodes}
    active(1100)
    first = snapshot(1100)
    history = [first]
    for timestamp in range(1200, 11200, 100):
        rpc.call("AdvanceSimulationTime", target_time_us=timestamp * 1000)
        active(timestamp)
        history.append(snapshot(timestamp))
    save_json(output / "simulation_states.json", history)
    before = {n["spec"]["node_id"]: int(n.get("energy_microjoules", 0)) for n in first["nodes"]}
    details = []
    for n in history[-1]["nodes"]:
        node_id = n["spec"]["node_id"]
        energy = (int(n.get("energy_microjoules", 0)) - before[node_id]) / 1e6
        expected = 10000 if node_id == selected else 100
        if abs(energy - expected) > 1e-6:
            raise RuntimeError(f"energy integral mismatch: {node_id}: {energy} != {expected}")
        details.append({"node_id": node_id, "power_w": expected / 10, "energy_j": energy,
                        "state": n["power_level"], "capacity_bytes": int(n["capacity_bytes"])})
    summary = {"task": "T03-3", "status": "PASS", "source": "scheduler_simulation",
               "allocation": location, "selected_node": selected, "nodes": details,
               "duration_s": 10, "capacity_pb": 11, "active_power_kw": 1,
               "idle_total_power_kw": 0.09, "cluster_power_kw": 1.09,
               "energy_j": sum(n["energy_j"] for n in details),
               "capacity_power_pb_per_kw": capacity_power_ratio(11 * 10**15, 1090),
               "meets_10_pb_per_kw": True,
               "scope": "10 simulated storage nodes only; physical CMS/Scheduler host overhead excluded",
               "io": "one CMS-selected file; simulated accesses, no physical data I/O"}
    save_json(output / "summary.json", summary)
    rpc.close()
    cms.close()
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    local = sub.add_parser("local", help="isolated Scheduler + CMS + real nodes; four T03 workloads")
    local.add_argument("--build", default=str(ROOT / "build-release"))
    local.add_argument("--nodes", type=int, default=2)
    local.add_argument("--optical-check", action="store_true")
    sim = sub.add_parser("simulation", help="exact T03-3 10-node scenario")
    sim.add_argument("--build", default=str(ROOT / "build-release"))
    mock_meter = sub.add_parser("meter-closure", help="real same-file I/O with explicitly synthetic power meter inputs")
    mock_meter.add_argument("--build", default=str(ROOT / "build-release"))
    mock_meter.add_argument("--files", type=int, default=128)
    batch = sub.add_parser("batch", help="use an existing cluster; preserve data for T05")
    batch.add_argument("--scheduler", required=True)
    batch.add_argument("--mds", required=True)
    for p in (local, batch):
        p.add_argument("--files", type=int, default=10000)
        p.add_argument("--min-size", type=int, default=4096)
        p.add_argument("--max-size", type=int, default=4096)
        p.add_argument("--seed", type=int, default=20260914)
        p.add_argument("--sample-interval", type=float, default=0.1)
        p.add_argument("--workload", choices=(*WORKLOADS, "all"), default="all")
        p.add_argument("--trace", help="T04 generated_trace CSV; requires --workload typical_trace")
        p.add_argument("--reuse-run", type=Path, help="sequential read the same files from a successful write run")
    summary = sub.add_parser("summarize", help="recompute batch summary from raw records")
    summary.add_argument("directory", type=Path)
    meter = sub.add_parser("meter", help="integrate physical power meter CSV for one workload phase")
    meter.add_argument("--samples", required=True, type=Path)
    meter.add_argument("--inventory", required=True, type=Path)
    meter.add_argument("--start-us", type=int, required=True)
    meter.add_argument("--end-us", type=int, required=True)
    meter.add_argument("--max-gap-us", type=int, default=5_000_000)
    meter.add_argument("--simulated", action="store_true",
                       help="require simulated_inventory and label results as simulated_meter, never hardware measurements")
    args = parser.parse_args()
    if args.command == "summarize":
        report = summarize_run(args.directory)
        save_json(args.directory / "summary.json", report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["status"] == "PASS" else 1
    output = result_dir(args.command)
    print(f"Results: {output}", flush=True)
    try:
        if args.command == "meter":
            report = meter_summary(args.samples, json.loads(args.inventory.read_text()),
                                   args.start_us, args.end_us, args.max_gap_us, simulated=args.simulated)
            (output / "meter.csv").write_bytes(args.samples.read_bytes())
            (output / "inventory.json").write_bytes(args.inventory.read_bytes())
            save_json(output / "summary.json", report)
        elif args.command == "meter-closure":
            from python.meter_closure import run_meter_closure
            with LocalCluster(output, args.build, node_count=1) as cluster:
                run_meter_closure(cluster, output, args.files)
        elif args.command == "simulation":
            with LocalCluster(output, args.build, simulated=True) as cluster:
                report = simulation(cluster, output)
        else:
            if args.trace and args.workload != "typical_trace":
                raise ValueError("external trace requires --workload typical_trace")
            if args.reuse_run and (args.command != "batch" or args.workload != "sequential_read"):
                raise ValueError("reuse-run requires batch --workload sequential_read on the same cluster")
            def batches(scheduler, mds):
                reports = {}
                for workload in WORKLOADS if args.workload == "all" else [args.workload]:
                    reports[workload] = run_batch(output / workload, scheduler, mds, workload,
                        args.files, args.min_size, args.max_size, args.seed, args.sample_interval, args.trace, args.reuse_run)
                    print(f"{workload}: {reports[workload]['status']}", flush=True)
                return reports
            if args.command == "local":
                if args.nodes < 1:
                    raise ValueError("at least one real node required")
                with LocalCluster(output, args.build, args.nodes, optical=args.optical_check) as cluster:
                    report = batches(cluster.scheduler, cluster.mds)
                    if args.optical_check:
                        rpc = Rpc(cluster.scheduler, "SchedulerService")
                        node = wait_for(lambda: next((n for n in list_nodes(rpc)
                            if n["spec"]["node_id"] == "t03-optical"), None))
                        if node["lifecycle"] != "MANAGED_LIFECYCLE_JOINING" or node.get("readiness", {}).get("inventory_ready"):
                            raise RuntimeError("optical node without inventory must not be admitted")
                        save_json(output / "optical_boundary.json", {"status": "PASS", "node": node,
                            "external_dependency": "T05 inventory/task telemetry and T02/MDS optical data-plane integration"})
                        rpc.close()
            else:
                report = batches(args.scheduler, args.mds)
            save_json(output / "index.json", {"task": "T03-1", "status": "PASS", "workloads": list(report),
                      "data_lifecycle": "retained; local command stops its services, batch keeps external services running"})
        print(f"PASS: {output}", flush=True)
        return 0
    except Exception as exc:
        save_json(output / "failure.json", {"status": "FAIL", "error": str(exc)})
        print(f"FAIL: {exc}; see {output}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
