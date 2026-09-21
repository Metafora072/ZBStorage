"""T03 power integration. Decimal PB/kW; no measured/model data mixing."""
import csv
import math
from collections import defaultdict


def integrate(samples, start_us, end_us, max_gap_us=5_000_000):
    """Trapezoidal integration, clipped by linear interpolation at both boundaries."""
    if end_us <= start_us or max_gap_us <= 0:
        raise ValueError("positive measurement duration and max gap required")
    if len(samples) < 2:
        raise ValueError("at least two samples per meter/node required")
    previous = None
    for timestamp, watts in samples:
        if not math.isfinite(watts) or watts < 0:
            raise ValueError("power must be finite and nonnegative")
        if previous is not None and timestamp <= previous:
            raise ValueError("timestamps must be strictly increasing; duplicates are ambiguous")
        previous = timestamp
    if samples[0][0] > start_us or samples[-1][0] < end_us:
        raise ValueError("samples must bracket the entire workload interval")
    segments = []
    for (t0, p0), (t1, p1) in zip(samples, samples[1:]):
        left, right = max(t0, start_us), min(t1, end_us)
        if right <= left:
            continue
        if t1 - t0 > max_gap_us:
            raise ValueError("power sample gap exceeds configured limit")
        a = p0 + (p1 - p0) * (left - t0) / (t1 - t0)
        b = p0 + (p1 - p0) * (right - t0) / (t1 - t0)
        segments.append((a + b) / 2 * (right - left) / 1_000_000)
    return math.fsum(segments)


def capacity_power_ratio(capacity_bytes, average_watts):
    if capacity_bytes < 0 or not math.isfinite(average_watts) or average_watts < 0:
        raise ValueError("invalid capacity/power")
    return capacity_bytes / 1e15 / (average_watts / 1000) if average_watts else None


def meets_target(ratio):
    # Allow only floating-point integration roundoff, not a measurement margin.
    return None if ratio is None else ratio >= 10 or math.isclose(ratio, 10, rel_tol=1e-12)


def meter_summary(samples_path, inventory, start_us, end_us, max_gap_us=5_000_000, *, simulated=False):
    """One analysis path; synthetic inputs require explicit opt-in and provenance."""
    required_source = "simulated_inventory" if simulated else "hardware_inventory"
    if inventory.get("source") != required_source:
        raise ValueError(f"meter report requires source={required_source}")
    source = "simulated_meter" if simulated else "meter_measurement"
    nodes = inventory.get("nodes", [])
    ids = [n["node_id"] for n in nodes]
    meters = [n["meter_id"] for n in nodes]
    if not nodes or len(set(ids)) != len(ids) or len(set(meters)) != len(meters):
        raise ValueError("unique physical node and meter IDs required; do not count one host twice")
    grouped = defaultdict(list)
    with open(samples_path, newline="") as csv_input:
        for row in csv.DictReader(csv_input):
            grouped[row["node_id"]].append((int(row["timestamp_us"]), float(row["power_w"])))
    if set(grouped) != set(ids):
        raise ValueError("meter node set must match the hardware inventory exactly")
    results = []
    duration = (end_us - start_us) / 1e6
    for node in nodes:
        capacity = 0
        for group in node["disk_groups"]:
            count, size = group["count"], group["capacity_bytes"]
            if type(count) is not int or type(size) is not int or count <= 0 or size <= 0:
                raise ValueError("disk count/capacity must be positive integers")
            capacity += count * size
        joules = integrate(grouped[node["node_id"]], start_us, end_us, max_gap_us)
        watts = joules / duration
        ratio = capacity_power_ratio(capacity, watts)
        results.append({**node, "source": source, "capacity_bytes": capacity, "energy_j": joules,
                        "energy_wh": joules / 3600, "average_power_w": watts,
                        "capacity_power_pb_per_kw": ratio,
                        "meets_10_pb_per_kw": meets_target(ratio)})
    watts = sum(n["average_power_w"] for n in results)
    ratio = capacity_power_ratio(sum(n["capacity_bytes"] for n in results), watts)
    return {"source": source, "inventory_source": required_source,
            "hardware_energy_status": "NOT_MEASURED" if simulated else "METER_DATA_PROVIDED",
            "start_us": start_us, "end_us": end_us,
            "duration_s": duration, "integration": "trapezoidal, clipped, no extrapolation",
            "max_gap_us": max_gap_us, "nodes": results, "average_power_w": watts,
            "energy_j": sum(n["energy_j"] for n in results),
            "capacity_power_pb_per_kw": ratio,
            "meets_10_pb_per_kw": meets_target(ratio)}


def summarize_run(directory):
    """Recompute batch summaries from append-only operation and snapshot files."""
    import json
    from pathlib import Path
    directory = Path(directory)
    parameters = json.loads((directory / "parameters.json").read_text())
    rows = [json.loads(line) for line in (directory / "operations.jsonl").read_text().splitlines()]
    samples = [json.loads(line) for line in (directory / "power.jsonl").read_text().splitlines()]
    if not rows or not samples:
        raise ValueError("missing operations or power samples")
    start, end = rows[0]["start_us"], rows[-1]["end_us"]
    by_node, snapshot_nodes = defaultdict(list), defaultdict(list)
    for row in rows:
        by_node[row.get("node_id", "unassigned")].append(row)
    for snapshot in samples:
        for node in snapshot["nodes"]:
            # T03 storage batches include storage and MDS. Optical overhead is
            # supplied by T05 to T02's combined experiment, not guessed here.
            if node["spec"].get("kind") == "MANAGED_KIND_OPTICAL_LIBRARY":
                continue
            if node["spec"].get("execution_mode") in ("MANAGED_EXECUTION_SIMULATED", "MANAGED_EXECUTION_VIRTUAL"):
                continue
            snapshot_nodes[node["spec"]["node_id"]].append((snapshot["timestamp_us"], node))
    powers, warnings = [], []
    if not snapshot_nodes:
        warnings.append("no participating physical node power samples")
    used_nodes = {row["node_id"] for row in rows if row["ok"]}
    missing_nodes = used_nodes - snapshot_nodes.keys()
    if missing_nodes:
        warnings.append("missing workload node power samples: " + ", ".join(sorted(missing_nodes)))
    for node_id, points in snapshot_nodes.items():
        def power(node):
            level = node.get("power_level", "MANAGED_POWER_UNKNOWN")
            field = {"MANAGED_POWER_OFF": "off_milliwatts", "MANAGED_POWER_STANDBY": "standby_milliwatts",
                     "MANAGED_POWER_MEDIUM": "medium_milliwatts", "MANAGED_POWER_PEAK": "peak_milliwatts"}.get(level)
            if field is None:
                raise ValueError(f"unknown power level: {level}")
            return int(node["spec"]["power"].get(field, 0)) / 1000
        try:
            joules = integrate([(t, power(n)) for t, n in points], start, end,
                               parameters["max_sample_gap_us"])
            error = None
        except ValueError as exc:
            joules, error = None, str(exc)
            warnings.append(f"{node_id}: {error}")
        powers.append({"node_id": node_id, "source": "scheduler_model",
                       "energy_j": joules, "average_power_w": None if joules is None else joules / ((end-start)/1e6),
                       "integration_error": error,
                       "counter_delta_j": (int(points[-1][1].get("energy_microjoules", 0)) -
                                           int(points[0][1].get("energy_microjoules", 0))) / 1e6,
                       "counter_window_us": [points[0][0], points[-1][0]]})
    manifest = json.loads((directory / "manifest.json").read_text())
    resident = defaultdict(lambda: {"files": 0, "bytes": 0})
    for item in manifest:
        resident[item["node_id"]]["files"] += 1
        resident[item["node_id"]]["bytes"] += item["size"]
    distribution = []
    for node_id in sorted(set(by_node) | set(resident) | set(snapshot_nodes)):
        ops = by_node[node_id]
        distribution.append({"node_id": node_id, "request_count": len(ops),
                             "successful_requests": sum(r["ok"] for r in ops),
                             "unique_files_accessed": len({r["path"] for r in ops if r["ok"]}),
                             "read_bytes": sum(r["bytes"] for r in ops if r["ok"] and r["op"] == "read"),
                             "write_bytes": sum(r["bytes"] for r in ops if r["ok"] and r["op"] == "write"),
                             "resident_files": resident[node_id]["files"], "resident_bytes": resident[node_id]["bytes"]})
    complete = (len(rows) == parameters["expected_operations"] and all(r["ok"] for r in rows) and
                len(manifest) == parameters.get("files", len(manifest)))
    validation_path = directory / "validation.json"
    verified = validation_path.exists() and json.loads(validation_path.read_text()).get("status") == "PASS"
    return {"task": "T03-1", "parameters": parameters, "start_us": start, "end_us": end,
            "total_execution_s": (rows[-1]["end_monotonic_ns"] - rows[0]["start_monotonic_ns"]) / 1e9,
            "completed_operations": len(rows), "failures": sum(not r["ok"] for r in rows),
            "status": "PASS" if complete and verified and not warnings else "FAIL", "distribution": distribution,
            "byte_validation": "PASS" if verified else "NOT_VERIFIED",
            "node_power": powers, "model_energy_j": None if warnings else sum(p["energy_j"] for p in powers),
            "model_average_power_w": None if warnings else sum(p["average_power_w"] for p in powers),
            "final_files": len(manifest), "final_bytes": sum(item["size"] for item in manifest),
            "power_scope": "sampled physical managed storage and metadata nodes; excludes optical/virtual/simulated",
            "power_warnings": warnings, "hardware_energy_status": "NOT_MEASURED",
            "timing_boundary": "first workload request to CMS/MDS through final completion; preparation excluded",
            "counter_note": "Scheduler counters use their own update times; counter delta is not the clipped workload integral"}
