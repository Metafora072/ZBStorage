#!/usr/bin/env python3
"""Small API regression fixture; starts real MDS/data-node processes, no benchmark.

All generated files stay in the explicitly supplied work directory. Run this
same fixture against the original and refactored binaries on fresh directories.
"""
import argparse
import json
import pathlib
import socket
import subprocess
import time
import urllib.request


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", type=pathlib.Path, required=True)
    parser.add_argument("--work-dir", type=pathlib.Path, required=True)
    parser.add_argument("--backend", choices=["rocksdb", "lhm"], default="rocksdb")
    parser.add_argument("--exercise-archive", action="store_true",
                        help="verify MDS -> recording optical RPC peer (metadata admission only)")
    parser.add_argument("--exercise-simple-archive", action="store_true",
                        help="real optical engine -> MDS inode callbacks with tiny fixture source")
    args = parser.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    build = args.build.resolve()
    mds_port, node_port = free_port(), free_port()
    node_config = work / "real.conf"
    node_config.write_text(
        f"DISK_BASE_DIR={work}/disks\nDISK_COUNT=1\n"
        f"NODE_ID=node-real-01\nNODE_ADDRESS=127.0.0.1:{node_port}\n"
        "REPLICATION_ENABLED=false\n")
    mds_config = work / "mds.conf"
    mds_config.write_text(
        f"MDS_DB_PATH={work}/db\n"
        f"NODES=node-real-01@127.0.0.1:{node_port}\n"
        "DISKS=node-real-01:disk-01\nREPLICA=1\nPG_COUNT=16\n"
        "ENABLE_OPTICAL_ARCHIVE=false\nMASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\n")
    if args.backend == "lhm":
        with mds_config.open("a") as config:
            config.write(f"NAMESPACE_BACKEND=lhm\nLHM_STORE_FILE={work}/lhm.meta\nMASSTREE_ROOT={work}/templates\n")
        subprocess.run([str(build / "src/mds/lhm/lhm_control_store_tool"), "seed", str(work / "db")], check=True)
    processes, logs, records = [], [], []
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def rpc(method, request, expected="MDS_OK"):
        req = urllib.request.Request(
            f"http://127.0.0.1:{mds_port}/zb.rpc.MdsService/{method}",
            data=json.dumps(request).encode(), headers={"Content-Type": "application/json"})
        with opener.open(req, timeout=10) as response:
            result = json.load(response)
        code = result.get("status", {}).get("code", "MDS_OK")
        codes = {0: "MDS_OK", 1: "MDS_INVALID_ARGUMENT", 2: "MDS_NOT_FOUND",
                 3: "MDS_ALREADY_EXISTS", 4: "MDS_NOT_EMPTY", 5: "MDS_INTERNAL_ERROR",
                 6: "MDS_STALE_EPOCH"}
        code = codes.get(code, code)
        records.append({"method": method, "request": request, "response": result})
        require(code == expected, f"{method}: expected {expected}, got {result}")
        return result

    try:
        for name, config, port in [("real_node_server", node_config, node_port),
                                   ("mds_server", mds_config, mds_port)]:
            log = (work / f"{name}.log").open("w")
            logs.append(log)
            process = subprocess.Popen([str(build / name), f"--config={config}",
                                        f"--port={port}"], cwd=work, stdout=log, stderr=log)
            processes.append(process)
            deadline = time.monotonic() + 30
            while True:
                require(process.poll() is None, f"{name} exited; see {log.name}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    require(time.monotonic() < deadline, f"{name} startup timeout")
                    time.sleep(0.1)

        # Upstream routes cleanup RPCs by logical node + incarnation. Register a
        # physical node through the real CMS API instead of bypassing that gate.
        rpc("CommitCmsNodeCatalog", {"scheduler_generation": 1, "nodes": [{
            "placement_view": {"node_id": "node-real-01", "node_type": "NODE_REAL",
                               "address": f"127.0.0.1:{node_port}"},
            "managed_view": {
                "spec": {"node_id": "node-real-01", "instance_generation": 1,
                         "kind": "MANAGED_KIND_STORAGE", "execution_mode": "MANAGED_EXECUTION_PHYSICAL",
                         "participates_in_placement": True},
                "service_address": f"127.0.0.1:{node_port}",
                "lifecycle": "MANAGED_LIFECYCLE_WORKING", "health": "MANAGED_HEALTH_HEALTHY",
                "administrative_state": "MANAGED_ADMIN_ENABLED", "service_mode": "MANAGED_SERVICE_READ_WRITE",
                "power_level": "MANAGED_POWER_PEAK", "actuated_power_level": "MANAGED_POWER_PEAK",
                "inventory_devices": [{"device_id": "disk-01", "kind": "MANAGED_DEVICE_HDD",
                                       "capacity_bytes": 1048576, "free_bytes": 1048576, "is_healthy": True}]
            }
        }]})
        root = rpc("Lookup", {"path": "/"})["attr"]
        require(int(root["inode_id"]) == 1, "root ID")
        rpc("Getattr", {"inode_id": 1})
        rpc("Lookup", {"path": "/rpc_fixture"}, "MDS_NOT_FOUND")
        for path in ["/rpc_fixture", "/rpc_fixture/dir"]:
            rpc("Mkdir", {"path": path, "mode": 493, "uid": 12, "gid": 34})
        rpc("Mkdir", {"path": "/rpc_fixture/dir"}, "MDS_ALREADY_EXISTS")
        empty = rpc("Readdir", {"path": "/rpc_fixture/dir", "limit": 1})
        require(not empty.get("entries"), "new directory must be empty")
        ids = {}
        for name in ["b", "a"]:
            response = rpc("Create", {"path": f"/rpc_fixture/dir/{name}",
                                     "mode": 420, "uid": 12, "gid": 34, "replica": 1})
            ids[name] = int(response["location"]["attr"]["inode_id"])
        require(ids["a"] != ids["b"], "unique inode IDs")
        rpc("Create", {"path": "/rpc_fixture/dir/a"}, "MDS_ALREADY_EXISTS")
        first = rpc("Readdir", {"path": "/rpc_fixture/dir", "limit": 1})
        require([e["name"] for e in first["entries"]] == ["a"], "first page order")
        require(first.get("has_more") is True, "first page has_more")
        second = rpc("Readdir", {"path": "/rpc_fixture/dir", "limit": 1,
                                 "start_after": first["next_token"]})
        require([e["name"] for e in second["entries"]] == ["b"], "second page order")
        require(not second.get("has_more"), "last page has_more")
        rpc("UpdateInodeStat", {"inode_id": ids["a"], "file_size": 12345,
                                 "object_unit_size": 4194304, "version": 2})
        attr = rpc("Getattr", {"inode_id": ids["a"]})["attr"]
        require(int(attr["size"]) == 12345 and attr["uid"] == 12 and attr["gid"] == 34,
                "updated size and preserved ownership")
        location = rpc("GetFileLocation", {"inode_id": ids["a"]})["location"]
        require(bool(location.get("disk_location")), "file placement preserved")
        if args.exercise_archive:
            def start_peer(name, fail_at):
                port = free_port()
                capture = work / f"{name}.jsonl"
                log = (work / f"{name}.log").open("w")
                logs.append(log)
                process = subprocess.Popen([str(build / "src/mds/lhm/archive_pipeline_test"),
                    "--serve", str(port), str(capture), str(fail_at)], cwd=work, stdout=log, stderr=log)
                processes.append(process)
                deadline = time.monotonic() + 30
                while True:
                    require(process.poll() is None, f"archive peer exited: {name}")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                            break
                    except OSError:
                        require(time.monotonic() < deadline, "archive peer startup timeout")
                        time.sleep(0.1)
                return f"127.0.0.1:{port}", capture

            address, capture = start_peer("optical_capture", 0)
            request = {"root_path": "/rpc_fixture", "optical_node_address": address,
                       "batch_id": 101, "max_files_per_batch": 1, "scan_work_budget": 1,
                       "rpc_timeout_ms": 1000}
            sent = rpc("SendSubtreeArchive", request)
            require(int(sent["acknowledged_files"]) == 2 and int(sent["acknowledged_batches"]) == 2,
                    "archive acknowledgment counts")
            chunks = [json.loads(line) for line in capture.read_text().splitlines()]
            require(len(chunks) == 3 and chunks[-1].get("end_of_batch") and not chunks[-1].get("files"),
                    "final marker follows all file chunks")
            chunks = chunks[:-1]
            require(len(chunks) == 2 and all(int(c["batch_id"]) == 101 and len(c["files"]) == 1 for c in chunks),
                    "archive real RPC chunk shape")
            files = {int(c["files"][0]["inode_id"]): c["files"][0] for c in chunks}
            require(set(files) == set(ids.values()), "archive subtree coverage")
            for inode, file in files.items():
                require(file["target_node_id"] == location["disk_location"]["node_id"] and
                        file["target_disk_id"] == location["disk_location"]["disk_id"] and
                        int(file["object_unit_size"]) == 4194304, "archive real file location")
                require(int(file.get("size", 0)) == (12345 if inode == ids["a"] else 0), "archive file size")
                require(not file.get("image_id") and not file.get("source_lease_id"), "minimal physical protocol")
            rpc("SendSubtreeArchive", dict(request, batch_id=0), "MDS_INVALID_ARGUMENT")
            rpc("SendSubtreeArchive", dict(request, root_path="/absent"), "MDS_NOT_FOUND")
            rpc("Mkdir", {"path": "/archive_empty", "mode": 493})
            empty_sent = rpc("SendSubtreeArchive", dict(request, root_path="/archive_empty", batch_id=102))
            require(int(empty_sent.get("attempted_batches", 0)) == 0, "empty subtree sends no RPC")
            rpc("Rmdir", {"path": "/archive_empty"})
            require(len(capture.read_text().splitlines()) == 3, "invalid/empty scans send no chunks")
            failure_address, failure_capture = start_peer("optical_failure", 2)
            failed = rpc("SendSubtreeArchive", dict(request, optical_node_address=failure_address, batch_id=103),
                         "MDS_INTERNAL_ERROR")
            require(int(failed["acknowledged_files"]) == 1 and int(failed["attempted_batches"]) == 2
                    and failed["last_batch_may_be_accepted"], "partial archive progress on failure")
            require(len(failure_capture.read_text().splitlines()) == 2, "failed optical batch is not retried")
            for name, mode in [("optical_no_status", -1), ("optical_timeout", -2), ("optical_admission_error", -3)]:
                bad_address, bad_capture = start_peer(name, mode)
                rejected = rpc("SendSubtreeArchive", dict(request, optical_node_address=bad_address,
                    batch_id=103 - mode, rpc_timeout_ms=50), "MDS_INTERNAL_ERROR")
                require(int(rejected.get("acknowledged_files", 0)) == 0 and
                        int(rejected["attempted_batches"]) == 1 and rejected["last_batch_may_be_accepted"],
                        "malformed reply/timeout/file rejection keeps acceptance uncertain")
                require(len(bad_capture.read_text().splitlines()) == 1, "failed batch is not retried")
        if args.exercise_simple_archive:
            port = free_port()
            log = (work / "simple_optical.log").open("w"); logs.append(log)
            process = subprocess.Popen([str(build / "simple_archive_optical_peer"), str(port),
                f"127.0.0.1:{mds_port}", str(work / "optical"), str(ids["a"])],
                cwd=work, stdout=log, stderr=log)
            processes.append(process)
            deadline = time.monotonic() + 30
            while True:
                require(process.poll() is None, "simple optical peer exited")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2): break
                except OSError:
                    require(time.monotonic() < deadline, "simple optical startup timeout")
                    time.sleep(0.1)
            sent = rpc("SendSubtreeArchive", {"root_path": "/rpc_fixture",
                "optical_node_address": f"127.0.0.1:{port}", "batch_id": 301,
                "max_files_per_batch": 1, "scan_work_budget": 1})
            require(int(sent["acknowledged_files"]) == 2, "simple archive submission")
            deadline = time.monotonic() + 45
            while True:
                attrs = [rpc("Getattr", {"inode_id": inode})["attr"] for inode in ids.values()]
                if all(a.get("file_archive_state") in [2, "INODE_ARCHIVE_ARCHIVED"] for a in attrs): break
                require(time.monotonic() < deadline, "small archive did not complete; inspect simple_optical.log")
                time.sleep(0.2)
            image_ids = {int(a.get("optical_image_id", 0)) for a in attrs}
            disc_ids = {int(a.get("optical_disk_id", 0)) for a in attrs}
            require(len(image_ids) == len(disc_ids) == 1 and 0 not in image_ids | disc_ids,
                    "both files share allocated image/disc IDs")
            require(image_ids != disc_ids, "shared MDS allocation sequence")
            require(all(a.get("storage_tier", "INODE_STORAGE_DISK") in [1, "INODE_STORAGE_DISK"] for a in attrs),
                    "archive callbacks preserve hot primary location")
            # Real vdisc output proves the marker flushed an underfilled image AND disc.
            require(list((work / "optical").rglob("*.vdisc")), "physical engine produced a vdisc")
            rpc("AllocateAvailableImageId", {}, "MDS_INVALID_ARGUMENT")
            rpc("ReportFilesPackedToImage", {"node_id": "optical-simple", "image_id": "img-0",
                "inode_ids": [ids["a"]]}, "MDS_INVALID_ARGUMENT")
        rpc("Rename", {"old_path": "/rpc_fixture/dir/a", "new_path": "/rpc_fixture/dir/b"},
            "MDS_ALREADY_EXISTS")
        rpc("Rmdir", {"path": "/rpc_fixture/dir"}, "MDS_NOT_EMPTY")
        rpc("Rename", {"old_path": "/rpc_fixture/dir", "new_path": "/rpc_fixture/moved"})
        rpc("Lookup", {"path": "/rpc_fixture/dir/a"}, "MDS_NOT_FOUND")
        moved = rpc("Lookup", {"path": "/rpc_fixture/moved/a"})["attr"]
        require(int(moved["inode_id"]) == ids["a"] and int(moved["size"]) == 12345,
                "subtree rename preserves identity and attributes")
        rpc("Rename", {"old_path": "/rpc_fixture/moved/a", "new_path": "/rpc_fixture/z"})
        require(int(rpc("Lookup", {"path": "/rpc_fixture/z"})["attr"]["inode_id"]) == ids["a"],
                "cross-directory file rename preserves identity")
        for path in ["/rpc_fixture/z", "/rpc_fixture/moved/b"]:
            rpc("Unlink", {"path": path})
        rpc("Getattr", {"inode_id": ids["a"]}, "MDS_NOT_FOUND")
        rpc("Rmdir", {"path": "/rpc_fixture/moved"})
        rpc("Rmdir", {"path": "/rpc_fixture"})
        rpc("Lookup", {"path": "/rpc_fixture"}, "MDS_NOT_FOUND")
        if args.backend == "lhm":
            for method in ["ImportArchiveNamespace", "ReportArchiveCandidates", "ReportFileArchiveCandidates",
                           "ClaimArchiveTask", "RenewArchiveLease", "CommitArchiveTask", "CommitFileMigration",
                           "VerifyNodeReferences", "GetRandomMasstreeFileAttr", "GetRandomMasstreeLookupPaths",
                           "GetMasstreeClusterStats", "GetMasstreeNamespaceStats"]:
                reply = rpc(method, {}, "MDS_INVALID_ARGUMENT")
                require("not supported" in reply["status"]["message"], f"explicit refusal: {method}")
            # Small functional fixture, not an experiment/benchmark dataset.
            paths = work / "template_paths.txt"
            paths.write_text("/folder/alpha\n/folder/beta\n")
            def wait_job(job_id):
                deadline = time.monotonic() + 60
                while True:
                    job = rpc("GetMasstreeImportJob", {"job_id": job_id})["job"]
                    state = job.get("state", 0)
                    require(state not in [3, "MASSTREE_IMPORT_JOB_FAILED"], f"import failed: {job}")
                    if state in [2, "MASSTREE_IMPORT_JOB_COMPLETED"]:
                        return job
                    require(time.monotonic() < deadline, "import timeout")
                    time.sleep(0.1)
            template = rpc("GenerateMasstreeTemplate", {"template_id": "fixture", "path_list_file": str(paths),
                           "path_list_leaf_nodes_are_files": True, "file_count": 2})
            wait_job(template["job_id"])
            imported = rpc("ImportMasstreeNamespace", {"template_id": "fixture", "namespace_id": "cold",
                           "generation_id": "g1", "path_prefix": "/cold", "publish_route": True})
            job = wait_job(imported["job_id"])
            root = rpc("Lookup", {"path": "/cold"})["attr"]
            require(int(root["inode_id"]) == int(job["root_inode_id"]), "imported root ID")
            require(int(root["inode_id"]) & (1 << 63), "LHM directory address ID")
            # Locate the source's exact relative layout through ordinary namespace RPCs.
            found_files = []
            todo = ["/cold"]
            while todo:
                parent = todo.pop()
                for entry in rpc("Readdir", {"path": parent}).get("entries", []):
                    path = parent + "/" + entry["name"]
                    attr = rpc("Lookup", {"path": path})["attr"]
                    if attr.get("type", 0) in [1, "INODE_DIR"]:
                        todo.append(path)
                    else:
                        found_files.append((path, int(attr["inode_id"])))
                        rpc("Getattr", {"inode_id": int(attr["inode_id"])})
            require(len(found_files) == 2, "template file count")
            # Once imported, source pages are unnecessary for live queries.
            (work / "templates").rename(work / "templates_offline")
            for path, inode in found_files:
                require(int(rpc("Lookup", {"path": path})["attr"]["inode_id"]) == inode, "source-independent lookup")
            rpc("Rename", {"old_path": "/cold", "new_path": "/cold_moved"})
            path, inode = found_files[0]
            moved = path.replace("/cold/", "/cold_moved/", 1)
            require(int(rpc("Lookup", {"path": moved})["attr"]["inode_id"]) == inode, "imported rename ID")
            rpc("Unlink", {"path": moved})
            rpc("Lookup", {"path": moved}, "MDS_NOT_FOUND")
            rpc("ImportMasstreeNamespace", {"template_id": "fixture", "namespace_id": "bad", "generation_id": "g",
                "path_prefix": "/bad", "publish_route": True, "inode_start": 100}, "MDS_INVALID_ARGUMENT")
        (work / "result.json").write_text(json.dumps({"passed": True, "rpc_count": len(records),
                                                     "records": records}, indent=2))
        print(f"PASS: {len(records)} RPC checks; {work / 'result.json'}")
    except Exception as error:
        (work / "result.json").write_text(json.dumps({"passed": False, "error": str(error),
                                                     "records": records}, indent=2))
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        for log in logs:
            log.close()
    if args.backend == "lhm":
        subprocess.run([str(build / "src/mds/lhm/lhm_control_store_tool"), "audit", str(work / "db")], check=True)


if __name__ == "__main__":
    main()
