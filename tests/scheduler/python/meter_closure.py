"""T03-2 real I/O plus synthetic meter replay; never a hardware measurement."""
import csv
import json
import math
from pathlib import Path
import subprocess
import sys
import time

from .energy import meter_summary
from .support import SUITE, save_json
from .workloads import run_batch


def synthetic_samples(path, node_id, start, end, first_w, last_w):
    """Post-run linear fixture on actual phase timestamps, with bracketing samples."""
    if end <= start:
        raise ValueError("positive phase duration required")
    points = [(start - 100_000, first_w)]
    for timestamp in range(start, end, 100_000):
        watts = first_w + (last_w - first_w) * (timestamp - start) / (end - start)
        points.append((timestamp, watts))
    points.extend([(end, last_w), (end + 100_000, last_w)])
    with path.open("w", newline="") as target:
        writer = csv.writer(target)
        writer.writerow(("timestamp_us", "node_id", "power_w"))
        writer.writerows((timestamp, node_id, watts) for timestamp, watts in points)


def run_meter_closure(cluster, output, files, mount=None):
    output = Path(output)
    if files < 1:
        raise ValueError("positive file count required")
    idle_start = time.time_ns() // 1000
    time.sleep(0.2)
    idle_end = time.time_ns() // 1000
    write = run_batch(output / "write", cluster.scheduler, cluster.mds, "sequential_write",
                      files=files, min_size=4096, max_size=65536, mount=mount)
    read = run_batch(output / "read", cluster.scheduler, cluster.mds, "sequential_read",
                     reuse_run=output / "write", mount=mount)
    written_files = json.loads((output / "write/handoff.json").read_text())["files"]
    read_files = json.loads((output / "read/handoff.json").read_text())["files"]
    if written_files != read_files or len(written_files) != files:
        raise RuntimeError("read phase did not preserve the exact file set, contents and placement")
    if {item["node_id"] for item in written_files} != {"node-real-01"}:
        raise RuntimeError("T03-2 requires exactly one target storage node")
    phases = {"idle": {"start_us": idle_start, "end_us": idle_end}, "write": write, "read": read}
    # This capacity is a reference fixture, NOT the physical host capacity or IO quota.
    inventory = {"source": "simulated_inventory", "nodes": [{
        "node_id": "node-real-01", "meter_id": "synthetic-meter-01",
        "disk_groups": [{"count": 25, "capacity_bytes": 48 * 10**12}]}]}
    save_json(output / "inventory.json", inventory)
    curves = {"idle": (80, 80), "write": (100, 140), "read": (100, 120),
              "over_target_power": (160, 160)}
    reports = {}
    for phase, (first_w, last_w) in curves.items():
        window = phases["write" if phase == "over_target_power" else phase]
        start, end = window["start_us"], window["end_us"]
        directory = output / phase
        directory.mkdir(exist_ok=True)
        synthetic_samples(directory / "meter.csv", "node-real-01", start, end, first_w, last_w)
        fixture = {"source": "simulated_meter", "generation": "post-run linear synthetic fixture, not live sensor acquisition",
                   "start_us": start, "end_us": end, "first_power_w": first_w, "last_power_w": last_w,
                   "capacity_source": "task reference 25 x 48 TB, not observed hardware",
                   "io_window": "write" if phase == "over_target_power" else phase,
                   "scope": "single storage node only; excludes MDS/Scheduler host overhead"}
        save_json(directory / "meter_fixture.json", fixture)
        # Exercise the public command, including JSON/CSV parsing and persisted output.
        command = [sys.executable, str(SUITE / "run.py"), "meter", "--simulated",
                   "--samples", str(directory / "meter.csv"), "--inventory", str(output / "inventory.json"),
                   "--start-us", str(start), "--end-us", str(end)]
        executed = subprocess.run(command, text=True, capture_output=True, timeout=30)
        (directory / "meter_cli.log").write_text(executed.stdout + executed.stderr)
        if executed.returncode:
            raise RuntimeError(f"meter CLI failed: see {directory / 'meter_cli.log'}")
        cli_output = next(Path(line.removeprefix("Results: ")) for line in executed.stdout.splitlines()
                          if line.startswith("Results: "))
        actual = json.loads((cli_output / "summary.json").read_text())
        recomputed = meter_summary(directory / "meter.csv", inventory, start, end, simulated=True)
        if actual != recomputed:
            raise RuntimeError("meter result cannot be reproduced from raw inputs")
        expected_w = (first_w + last_w) / 2
        expected_j = expected_w * (end - start) / 1e6
        expected_ratio = 1.2 / (expected_w / 1000)
        for key, expected in (("average_power_w", expected_w), ("energy_j", expected_j),
                              ("capacity_power_pb_per_kw", expected_ratio)):
            if not math.isclose(actual[key], expected, rel_tol=1e-10, abs_tol=1e-9):
                raise RuntimeError(f"{phase}: {key} differs from independent analytic integral")
        if (actual["source"] != "simulated_meter" or actual["hardware_energy_status"] != "NOT_MEASURED"
                or actual["meets_10_pb_per_kw"] != (phase != "over_target_power")):
            raise RuntimeError(f"{phase}: incorrect provenance or threshold judgment")
        reports[phase] = {**actual, "cli_results": str(cli_output), "software_check": "PASS"}
        save_json(directory / "meter_summary.json", reports[phase])
    summary = {"task": "T03-2", "status": "PASS", "software_closed_loop": "PASS",
               "source": "fuse_posix_with_simulated_meter" if mount else "real_rpc_with_simulated_meter",
               "hardware_energy_status": "NOT_MEASURED",
               "files": files, "bytes": write["final_bytes"], "same_file_readback": "PASS",
               "write_operations": write["completed_operations"], "read_operations": read["completed_operations"],
               "phases": reports, "inventory": inventory,
               "simulation_boundary": "power and hardware capacity are synthetic; IO and phase durations are real",
               "handoff": {"write": str(output / "write/handoff.json"), "read": str(output / "read/handoff.json")},
               "negative_control": "160 W must yield 7.5 PB/kW and meets_10_pb_per_kw=false"}
    save_json(output / "summary.json", summary)
    return summary
