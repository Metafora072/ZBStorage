#!/usr/bin/env python3
"""Small real-I/O closure for Scheduler/node management on a private external directory."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from python.fuse_workloads import FuseFiles, FuseMount
from python.support import LocalCluster, Rpc, save_json, wait_for


FILE_SIZE = 5 * 1024 * 1024 + 17  # crosses the 4 MiB object boundary


def check_root(root):
    if not root.is_dir() or not Path("/dev/fuse").exists() or not shutil.which("fusermount3"):
        raise RuntimeError("external directory and real FUSE device are required")
    mount = json.loads(subprocess.check_output(
        ["findmnt", "-J", "-T", str(root), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS"],
        text=True))["filesystems"][0]
    if mount["target"] == "/" or mount["fstype"].startswith("fuse") or \
            "rw" not in mount["options"].split(",") or not os.access(root, os.W_OK):
        raise RuntimeError("a writable non-root external filesystem is required")
    if shutil.disk_usage(root).free < 256 * 1024 * 1024:
        raise RuntimeError("external filesystem needs at least 256 MiB free")
    return mount


def check_backend(cluster_dir, item):
    disk = cluster_dir / item["node_id"] / "disks" / item["disk_id"]
    digest = hashlib.sha256()
    remaining = FILE_SIZE
    index = 0
    while remaining:
        matches = list(disk.rglob(f"obj-{item['inode_id']}-{index}"))
        if len(matches) != 1 or not matches[0].is_file():
            raise RuntimeError("backend object missing or duplicated")
        data = matches[0].read_bytes()
        expected_length = min(remaining, 4 * 1024 * 1024)
        if len(data) != expected_length:
            raise RuntimeError("backend object length differs from MDS file geometry")
        digest.update(data)
        remaining -= expected_length
        index += 1
    expected = hashlib.sha256(FuseFiles.payload("payload.bin", 0, FILE_SIZE)).hexdigest()
    if digest.hexdigest() != expected:
        raise RuntimeError("external backend bytes differ from FUSE-written payload")
    return digest.hexdigest()


def run(output, build):
    cluster_dir = output / "cluster"
    cluster = LocalCluster(cluster_dir, build, node_count=2)
    with cluster:
        with FuseMount(cluster, output / "first_mount") as mount:
            files = FuseFiles(cluster.mds, "/real/scheduler_real_smoke", mount)
            try:
                files.mkdir(files.namespace)
                files.create("payload.bin", FILE_SIZE)
                files.write("payload.bin")
                files.read("payload.bin")
                before = dict(files.locate("payload.bin"))
                digest = check_backend(cluster_dir, before)
                before_catalog_generation = int(
                    files.mds.call("GetCmsNodeCatalog")["scheduler_generation"])
            finally:
                files.close()

    # Fresh Scheduler, MDS, real nodes and FUSE mount reopen the same durable data.
    with cluster:
        with FuseMount(cluster, output / "restart_mount") as mount:
            files = FuseFiles(cluster.mds, "/real/scheduler_real_smoke", mount)
            files.files["payload.bin"] = dict(before)
            scheduler = Rpc(cluster.scheduler, "SchedulerService")
            mds = Rpc(cluster.mds, "MdsService")
            try:
                wait_for(lambda: int(mds.call("GetCmsNodeCatalog")
                    ["scheduler_generation"]) > before_catalog_generation)
                files.read("payload.bin")
                scheduler.call("RemoveManagedNode", node_id=before["node_id"],
                               request_ts_ms=time.time_ns() // 1_000_000)
                wait_for(lambda: next((entry for entry in
                    mds.call("GetCmsNodeCatalog").get("nodes", [])
                    if entry["managed_view"]["spec"]["node_id"] == before["node_id"] and
                    entry["managed_view"]["lifecycle"] == "MANAGED_LIFECYCLE_RETIRED"), None),
                    timeout=90)
                files.read("payload.bin")
                after = dict(files.locate("payload.bin"))
                if after["node_id"] == before["node_id"]:
                    raise RuntimeError("MDS did not switch the authoritative file location")
                refs = mds.call("VerifyNodeReferences", node_id=before["node_id"])
                if not refs.get("zero_references"):
                    raise RuntimeError("retired source still has MDS file references")
                source = Rpc(before["node_address"], "RealNodeService")
                try:
                    if int(source.call("ListObjects", max_objects=100).get("total_objects", 0)):
                        raise RuntimeError("retired source still has disk objects")
                finally:
                    source.close()
                check_backend(cluster_dir, after)
                files.create("after_retirement.bin", 4096)
                files.write("after_retirement.bin")
                files.read("after_retirement.bin")
                if files.locate("after_retirement.bin")["node_id"] == before["node_id"]:
                    raise RuntimeError("CMS allocated a new file to the retired source")
            finally:
                files.close()
                scheduler.close()
                mds.close()
    return {"status": "PASS", "source": before["node_id"], "target": after["node_id"],
            "inode_id": before["inode_id"], "bytes_verified": FILE_SIZE,
            "sha256": digest, "restart": "PASS", "migration": "PASS",
            "cms_generation_continuity": "PASS",
            "cms_zero_references": True, "post_retirement_allocation": "PASS",
            "data_plane": "real_node_server files on external filesystem via real FUSE",
            "hardware_power": "NOT_MEASURED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-root", type=Path, default=Path("/mnt/md0"))
    parser.add_argument("--build", type=Path, default=Path(__file__).resolve().parents[2] / "build")
    args = parser.parse_args()
    backing = check_root(args.external_root)
    output = Path(tempfile.mkdtemp(prefix="zbstorage_scheduler_real_", dir=args.external_root))
    print(f"External result directory: {output}", flush=True)
    try:
        result = run(output, args.build)
        result["filesystem"] = backing
        save_json(output / "summary.json", result)
        print("PASS: real FUSE write/read, restart, migration and retirement", flush=True)
    except BaseException as exc:
        save_json(output / "summary.json", {"status": "FAIL", "error": str(exc),
                                           "filesystem": backing})
        raise


if __name__ == "__main__":
    main()
