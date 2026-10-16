#!/usr/bin/env python3
"""真实 Scheduler/MDS/磁盘 RPC：排空门禁、节点重启、迁移版本接续。"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import time

from python.support import LocalCluster, Rpc, wait_for
from python.workloads import Files


def rejected(call, message):
    try:
        call()
    except RuntimeError as error:
        if message not in str(error):
            raise
    else:
        raise AssertionError("request should have been rejected: " + message)


def run(build):
    with tempfile.TemporaryDirectory(prefix="zb-real-drain-") as directory:
        root = Path(directory)
        with LocalCluster(root, build, node_count=1) as cluster:
            files = Files(cluster.mds, "/real")
            scheduler = Rpc(cluster.scheduler, "SchedulerService")
            mds = Rpc(cluster.mds, "MdsService")
            try:
                files.mkdir("/real")
                item = files.create("drain.bin", 5 * 1024 * 1024 + 17)
                files.write("drain.bin")
                files.write("drain.bin", offset=0, length=64)
                item = dict(item)  # 保留源身份，Files.locate 会更新内部清单中的位置。
                old = mds.call("GetFileLocation", inode_id=item["inode_id"])["location"]
                old_version = int(old["attr"]["version"])
                assert old_version >= 2
                source = files.client(item["node_address"])
                completed = []
                def writer():
                    rpc = Rpc(item["node_address"], "RealNodeService")
                    data = base64.b64encode(files.payload("drain.bin", 0, 65536)).decode()
                    try:
                        for _ in range(1000):
                            try:
                                rpc.call("WriteObject", disk_id=item["disk_id"],
                                         object_id=f"obj-{item['inode_id']}-0", data=data)
                                completed.append(1)
                            except RuntimeError as error:
                                if "NODE_DRAINING" not in str(error):
                                    raise
                                return
                        raise AssertionError("writer never observed drain fence")
                    finally:
                        rpc.close()
                with ThreadPoolExecutor(max_workers=4) as pool:
                    writes = [pool.submit(writer) for _ in range(4)]
                    wait_for(lambda: len(completed) >= 4, timeout=5)
                    scheduler.call("RemoveManagedNode", node_id=item["node_id"],
                                   request_ts_ms=time.time_ns() // 1_000_000)
                    for write in writes:
                        write.result(timeout=10)

                def check_fence():
                    rejected(lambda: source.call("WriteObject", disk_id=item["disk_id"],
                        object_id=f"obj-{item['inode_id']}-0", data=base64.b64encode(b"bad").decode()),
                        "NODE_DRAINING")
                    rejected(lambda: source.call("WriteObject", disk_id=item["disk_id"],
                        object_id=f"obj-{item['inode_id']}-0", is_replication=True,
                        data=base64.b64encode(b"bad").decode()), "NODE_DRAINING")
                    rejected(lambda: source.call("AllocateFileWrite", inode_id=item["inode_id"],
                        disk_id=item["disk_id"], size=1), "NODE_DRAINING")
                    rejected(lambda: source.call("CommitFileWrite", inode_id=item["inode_id"],
                        txid="late-write", file_size=item["size"], object_unit_size=4194304,
                        allow_create=True), "NODE_DRAINING")
                    files.read("drain.bin")
                check_fence()

                # 没有第二个节点时迁移必须阻塞；杀掉源进程后门禁仍应保留。
                process = cluster.processes[-1]
                process.kill()
                process.wait(timeout=10)
                source.close()
                def start_node(node_id):
                    return cluster.start(node_id, "real_node_server", {
                        "NODE_ID": node_id, "NODE_ADDRESS": "", "SCHEDULER_ADDR": cluster.scheduler,
                        "MDS_ADDR": cluster.mds, "HEARTBEAT_INTERVAL_MS": 100,
                        "DISK_BASE_DIR": root / node_id / "disks", "DISK_COUNT": 2,
                        "DISK_CAPACITY_BYTES": 1073741824, "REPLICATION_ENABLED": "false"})
                start_node(item["node_id"])
                check_fence()
                start_node("node-real-02")
                wait_for(lambda: scheduler.call("GetManagedNode", node_id=item["node_id"])
                         ["node"].get("lifecycle") == "MANAGED_LIFECYCLE_RETIRED", timeout=45)
                files.read("drain.bin")
                after = mds.call("GetFileLocation", inode_id=item["inode_id"])["location"]
                assert after["disk_location"]["node_id"] != item["node_id"]
                target = files.client(after["disk_location"]["node_address"])
                metadata = target.call("ResolveFileRead", inode_id=item["inode_id"], size=0)["meta"]
                assert int(metadata["version"]) == old_version
                assert int(metadata["file_size"]) == item["size"]
                assert mds.call("VerifyNodeReferences", node_id=item["node_id"])["zero_references"]
                assert int(source.call("ListObjects").get("total_objects", 0)) == 0
                rejected(lambda: source.call("ResolveFileRead", inode_id=item["inode_id"]), "file meta not found")
                files.write("drain.bin", offset=17, length=4096)
                files.read("drain.bin")
                final = mds.call("GetFileLocation", inode_id=item["inode_id"])["location"]
                assert int(final["attr"]["version"]) == old_version + 1
                print("PASS: real write fence, crash recovery, migration, metadata version and subsequent write")
            finally:
                files.close()
                scheduler.close()
                mds.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    run(parser.parse_args().build)
