#!/usr/bin/env python3
"""Manual full-chain check: production FUSE/MDS/Scheduler, physical and modeled disks."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import random
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import traceback

from scheduler_simulation_demo import Environment, REPO, field, require, rpc, save_json, stop

NODES = {'full-real-a': 1, 'full-real-b': 1, 'full-model-a': 7, 'full-model-b': 8}
TARGETS = ('scheduler_server', 'mds_server', 'real_node_server', 'zb_fuse_client')


def config(path, values):
    path.write_text(''.join(f'{key}={value}\n' for key, value in values.items()))


class FullEnvironment(Environment):
    def __init__(self, args, root):
        super().__init__(args, root)
        # Reserve every endpoint simultaneously, including both real nodes.
        sockets = [socket.socket() for _ in range(5)]
        try:
            for sock in sockets:
                sock.bind(('127.0.0.1', 0))
            ports = [sock.getsockname()[1] for sock in sockets]
        finally:
            for sock in sockets:
                sock.close()
        self.control_port, self.data_port, self.mds_port, *real_ports = ports
        self.scheduler = f'127.0.0.1:{self.control_port}'
        self.data = f'127.0.0.1:{self.data_port}'
        self.mds = f'127.0.0.1:{self.mds_port}'
        self.real_ports = dict(zip(('full-real-a', 'full-real-b'), real_ports))

    def configure(self):
        self.mount.mkdir()
        for name in ('full-model-a', 'full-model-b'):
            config(self.root / f'{name}.conf', dict(
                NODE_ID=name, INSTANCE_GENERATION=NODES[name], DISK_IDS='disk-01,disk-02',
                ARCHIVE_META_DIR=f'{name}-state', DISK_CAPACITY_BYTES=67108864,
                READ_BYTES_PER_SEC=8388608, WRITE_BYTES_PER_SEC=8388608,
                READ_BASE_LATENCY_MS=2, WRITE_BASE_LATENCY_MS=2, JITTER_MS=0,
                MAX_CONCURRENT_IO=4, IO_QUEUE_LIMIT=128, REPLICATION_ENABLED='false'))
        config(self.root / 'scheduler.conf', dict(
            SIMULATION_DISK_CONFIGS='full-model-a.conf,full-model-b.conf',
            SIMULATION_DATA_PORT=self.data_port, SIMULATION_DATA_ADDRESS=self.data,
            MDS_ADDRESS=self.mds, MANAGED_STATE_SNAPSHOT_PATH=self.root / 'managed.pb',
            CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS=100, TICK_INTERVAL_MS=100,
            POWER_ACTUATION_ENABLED='false'))
        config(self.root / 'mds.conf', dict(
            MDS_DB_PATH=self.root / 'mds-db', SCHEDULER_ADDR=self.scheduler,
            MANAGED_NODE_ID='full-chain-mds', OBJECT_UNIT_SIZE=16384, REPLICA=1,
            STRICT_TIER_BYPASS_PG='true', ENABLE_OPTICAL_ARCHIVE='false',
            ARCHIVE_META_ROOT=self.root / 'archive', MASSTREE_ROOT=self.root / 'masstree'))
        for name, port in self.real_ports.items():
            config(self.root / f'{name}.conf', dict(
                NODE_ID=name, NODE_ADDRESS=f'127.0.0.1:{port}',
                SCHEDULER_ADDR=self.scheduler, MDS_ADDR=self.mds, HEARTBEAT_INTERVAL_MS=100,
                DISK_BASE_DIR=self.root / name / 'disks', DISK_COUNT=2,
                DISK_CAPACITY_BYTES=67108864, ARCHIVE_META_DIR=self.root / name / 'metadata',
                REPLICATION_ENABLED='false', IO_WORKER_THREADS=8, DISK_MAX_INFLIGHT=4,
                IO_QUEUE_DEPTH=256))

    def catalog_ready(self):
        try:
            reply = rpc(self.mds, 'MdsService', 'GetCmsNodeCatalog')
        except (OSError, ValueError, RuntimeError):
            return False
        views = {field(field(entry, 'managed_view', {}).get('spec', {}), 'node_id'):
                 field(entry, 'managed_view', {}) for entry in reply.get('nodes', [])}
        for name, generation in NODES.items():
            view = views.get(name, {})
            if (int(field(view.get('spec', {}), 'instance_generation', 0)) != generation or
                view.get('lifecycle') not in (2, 'MANAGED_LIFECYCLE_WORKING') or
                not all(field(view.get('readiness', {}), key, False) for key in
                        ('observed', 'initialization_complete', 'inventory_ready', 'metadata_ready'))):
                return False
        save_json(self.root / 'cms-ready.json', reply)
        return True

    def start(self):
        self.configure()
        for name, binary, port in [('scheduler', 'scheduler_server', self.control_port),
                                   ('mds', 'mds_server', self.mds_port),
                                   *[(name, 'real_node_server', port) for name, port in self.real_ports.items()]]:
            self.launch(name, [self.binary(binary), f'--config={self.root / (name + ".conf")}',
                               f'--port={port}', '--bthread_concurrency=8'])
        self.wait(self.catalog_ready, timeout=45)
        self.launch('client', [self.binary('zb_fuse_client'), f'--mds={self.mds}',
            f'--scheduler={self.scheduler}', '--default_object_unit_size=16384',
            '--client_direct_io=true', '--timeout_ms=30000', '--disk_io_workers=8',
            '--disk_io_per_node=2', '--data_route_log=true', '--io_latency_enabled=true',
            f'--io_latency_dir={self.root / "metrics"}', '--', self.mount, '-f', '-o',
            'attr_timeout=0,entry_timeout=0,negative_timeout=0'])
        self.wait(lambda: os.path.ismount(self.mount) and
                  all((self.mount / tier).is_dir() for tier in ('real', 'virtual')))
        save_json(self.root / 'environment.json', dict(
            processes={name: process.pid for name, process in self.processes.items()},
            nodes=NODES, mds=self.mds, scheduler=self.scheduler, model_data=self.data,
            real_addresses={name: f'127.0.0.1:{port}' for name, port in self.real_ports.items()},
            mount=str(self.mount), replicas=1, direct_io=True,
            binaries={name: hashlib.sha256(self.binary(name).read_bytes()).hexdigest() for name in TARGETS}))

    def close(self):
        try:
            super().close()
        finally:
            for name in self.real_ports:
                stop(self.processes.get(name))


def write(path, payload):
    fd = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    try:
        offset = 0
        while offset < len(payload):
            count = os.pwrite(fd, payload[offset:offset + 65536], offset)
            require(count > 0, f'zero write: {path}')
            offset += count
    finally:
        os.close(fd)


def verify(env):
    print('1/4：同时在 /real 和 /virtual 并发创建并写入文件……', flush=True)
    files = [(tier, f'parallel-{i}.bin', random.Random(1000 + i).randbytes(196731 + i))
             for tier in ('real', 'virtual') for i in range(12)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as workers:
        list(workers.map(lambda item: write(env.mount / item[0] / item[1], item[2]), files))
    print('2/4：检查正式 MDS 文件位置、并发读回、真实数据 SHA-256 和底层对象……', flush=True)
    placement, inodes = [], set()
    for tier, name, payload in files:
        location = env.location(f'/{tier}/{name}')
        attr, disk = location['attr'], field(location, 'disk_location', {})
        node, generation = field(disk, 'node_id'), int(field(disk, 'instance_generation', 0))
        inode = int(field(attr, 'inode_id'))
        require(inode not in inodes, f'MDS assigned duplicate inode={inode} to {tier}/{name}')
        inodes.add(inode)
        prefix = 'full-real-' if tier == 'real' else 'full-model-'
        require(node in NODES and node.startswith(prefix) and generation == NODES[node],
                f'incorrect placement for {tier}/{name}: {location}')
        require(int(field(attr, 'size', 0)) == len(payload), f'MDS size mismatch: {tier}/{name}')
        expected = payload if tier == 'real' else b'x' * len(payload)
        row = dict(path=f'{tier}/{name}', inode=inode, node=node, generation=generation,
                   size=len(payload), expected_sha256=hashlib.sha256(expected).hexdigest())
        if tier == 'real':
            # Read-only evidence: data was written by the production node, never by this fixture.
            disk_path = env.root / node / 'disks' / field(disk, 'disk_id')
            backend = bytearray()
            for index in range((len(payload) + 16383) // 16384):
                matches = list(disk_path.rglob(f'obj-{inode}-{index}'))
                require(len(matches) == 1, f'backend object missing/duplicated: {row}, index={index}')
                backend.extend(matches[0].read_bytes())
            require(backend == payload, f'backend content mismatch: {row}')
            row['backend_sha256'] = hashlib.sha256(backend).hexdigest()
        placement.append(row)
    save_json(env.root / 'placement.json', placement)
    require({row['node'] for row in placement} == set(NODES), 'not all four nodes received files')
    def read(item):
        tier, name, payload = item
        expected = payload if tier == 'real' else b'x' * len(payload)
        actual = (env.mount / tier / name).read_bytes()
        require(actual == expected, f'{tier}/{name}: content/size mismatch ({len(actual)} vs {len(expected)})')
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as workers:
        list(workers.map(read, files))

    print('3/4：验证随机读、跨对象读、EOF、同客户端并发写、截断及删除……', flush=True)
    unlink_results = {}
    for tier in ('real', 'virtual'):
        path = env.mount / tier / 'parallel-0.bin'
        payload = next(data for kind, name, data in files if kind == tier and name == path.name)
        expected = payload if tier == 'real' else b'x' * len(payload)
        fd = os.open(path, os.O_RDONLY)
        try:
            for offset, count in [(3, 17), (16380, 100), (len(payload) - 7, 100), (len(payload), 100)]:
                require(os.pread(fd, count, offset) == expected[offset:offset + count], f'{tier}: random/EOF read')
        finally:
            os.close(fd)
        shared = env.mount / tier / 'shared.bin'
        shared.touch()
        def region(index):
            fd = os.open(shared, os.O_WRONLY)
            try:
                require(os.pwrite(fd, bytes([65 + index]) * 16384, index * 16384) == 16384,
                        f'{tier}: incomplete shared write')
            finally:
                os.close(fd)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
            list(workers.map(region, range(8)))
        expected = b''.join(bytes([65 + i]) * 16384 for i in range(8)) if tier == 'real' else b'x' * 131072
        require(shared.read_bytes() == expected, f'{tier}: shared write content mismatch')
        os.truncate(shared, 19000)
        require(shared.read_bytes() == expected[:19000], f'{tier}: truncate content mismatch')
        location = env.location(f'/{tier}/shared.bin')
        require(int(field(location['attr'], 'size', 0)) == 19000, f'{tier}: truncate not published to MDS')
        empty = env.mount / tier / 'empty.bin'
        empty.touch()
        require(empty.read_bytes() == b'', f'{tier}: empty file')
        try:
            empty.unlink()
            require(not empty.exists() and 'empty.bin' not in os.listdir(empty.parent), f'{tier}: unlink failed')
            unlink_results[tier] = {'status': 'PASS'}
        except OSError as error:
            unlink_results[tier] = {'status': 'FAIL', 'posix_error': str(error)}
            # A separate, empty diagnostic file exposes the public MDS error.
            # This is not a retry or a substitute for the failed POSIX operation.
            probe = empty.with_name('unlink-diagnostic.bin')
            probe.touch()
            try:
                reply = rpc(env.mds, 'MdsService', 'Unlink', path=f'/{tier}/{probe.name}')
                unlink_results[tier]['mds_probe'] = reply
            except RuntimeError as diagnostic:
                unlink_results[tier]['mds_probe_error'] = str(diagnostic)
            print(f'删除检查失败，继续其余检查：{unlink_results[tier]}', flush=True)
        save_json(env.root / 'unlink-results.json', unlink_results)

    print('4/4：核对四个节点的真实 RPC 路由与 Scheduler 读写计量……', flush=True)
    text = (env.root / 'client.log').read_text(errors='replace')
    for node, generation in NODES.items():
        for operation in ('ReadObject', 'WriteObject', 'AllocateFileWrite', 'CommitFileWrite'):
            require(re.search(rf'\[data_route\] op={operation} node={node} generation={generation} .*errno=0', text),
                    f'missing successful production RPC evidence: {node}/{operation}')
    require('[file_lock] release failed' not in text, 'real node lock release failed')
    snapshots = {}
    def reported():
        for node in NODES:
            snapshots[node] = rpc(env.scheduler, 'SchedulerService', 'GetManagedNode', node_id=node)['node']
        return all(sum(int(field(stream, key, 0)) for stream in field(view, 'metrics_streams', [])) > 0
                   for view in snapshots.values() for key in ('read_bytes', 'write_bytes'))
    env.wait(reported)
    save_json(env.root / 'managed-after.json', snapshots)
    return dict(status='PASS' if all(r['status'] == 'PASS' for r in unlink_results.values()) else 'FAIL',
                read_write_status={'real': 'PASS', 'virtual': 'PASS'}, unlink=unlink_results,
                deployment='production FUSE + MDS + Scheduler + 2 real nodes + 2 in-process model disks',
                parallel_files=24, placement=placement, posix_checks=['create', 'concurrent write/read', 'random read',
                'cross-object read', 'EOF', 'same-client concurrent write', 'truncate', 'empty file'],
                real_data='byte-for-byte + SHA-256 + backend objects', model_data='generated x bytes + size + route',
                scheduler_metrics='all four nodes reported reads and writes',
                limits='single host, one FUSE client, replica=1; no crash/restart/failover or multi-client consistency test')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=REPO / 'build')
    parser.add_argument('--no-build', action='store_true')
    args = parser.parse_args()
    args.build_dir = args.build_dir.resolve()
    require(Path('/dev/fuse').exists() and shutil.which('fusermount3'), 'real FUSE mount required')
    results = Path(__file__).resolve().parent / 'results'
    results.mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='disk-full-chain-', dir=results))
    print(f'结果目录：{root}', flush=True)
    env = None
    def interrupted(signum, frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if not args.no_build:
            with (root / 'build.log').open('w') as log:
                subprocess.run(['cmake', '--build', str(args.build_dir), '-j4', '--target', *TARGETS],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
        env = FullEnvironment(args, root)
        print('启动正式模块，等待四个磁盘节点经 Scheduler 发布到 MDS……', flush=True)
        env.start()
        result = verify(env)
        save_json(root / 'result.json', result)
        print(f'{result["status"]}：真实和仿真磁盘读写均通过；删除检查：{result["unlink"]}', flush=True)
        return 0 if result['status'] == 'PASS' else 1
    except BaseException as error:
        save_json(root / 'result.json', {'status': 'FAIL', 'error': repr(error)})
        print(f'FAIL: {error}; 日志：{root}', flush=True)
        traceback.print_exc()
        return 130 if isinstance(error, KeyboardInterrupt) else 1
    finally:
        if env:
            env.close()


if __name__ == '__main__':
    raise SystemExit(main())
