#!/usr/bin/env python3
"""Production FUSE + production real_node_server; only MDS/Scheduler are external mocks."""
import argparse
import concurrent.futures
import hashlib
import errno
import json
import os
from pathlib import Path
import random
import re
import select
import shutil
import signal
import socket
import subprocess
import tempfile
import time


def require(value, message):
    if not value:
        raise RuntimeError(message)


def wait_for(predicate, processes, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        require(all(p.poll() is None for p in processes), 'child process exited; inspect logs')
        if predicate():
            return
        time.sleep(.05)
    raise TimeoutError('startup timed out')


def stop(p, sig=signal.SIGTERM):
    if p is not None and p.poll() is None:
        p.send_signal(sig)
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()


def run(args, root, result):
    mount = root / 'mount'
    mount.mkdir()
    mount2 = root / 'mount2'
    mount2.mkdir()
    disk = root / 'disk'
    disk.mkdir()
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    address = f'127.0.0.1:{port}'
    env = node = client = client2 = None
    logs = [(root / f'{name}.log').open('w') for name in ('external', 'node', 'client', 'client2')]
    try:
        env = subprocess.Popen([str(args.environment), address], stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE, stderr=logs[0], text=True, bufsize=1, start_new_session=True)
        require(select.select([env.stdout], [], [], 20)[0], 'mock start timed out')
        line = env.stdout.readline().strip()
        require(line.startswith('READY '), 'mock start failed')
        mock = line.split()[1]
        config = root / 'real.conf'
        config.write_text(f'ZB_DISKS=disk0:{disk}\nNODE_ID=disk-test\nNODE_ADDRESS={address}\n'
                          f'SCHEDULER_ADDR={mock}\nREPLICATION_ENABLED=false\n'
                          f'ARCHIVE_META_DIR={root / "metadata"}\nHEARTBEAT_INTERVAL_MS=100\n'
                          'IO_WORKER_THREADS=8\nDISK_MAX_INFLIGHT=4\nIO_QUEUE_DEPTH=256\n')
        node = subprocess.Popen([str(args.node), f'--config={config}', f'--port={port}'],
                                stdout=logs[1], stderr=subprocess.STDOUT, start_new_session=True)
        def listening():
            try:
                with socket.create_connection(('127.0.0.1', port), .2):
                    return True
            except OSError:
                return False
        wait_for(listening, [env, node])
        client = subprocess.Popen([str(args.client), f'--mds={mock}', f'--scheduler={mock}',
                                   '--bootstrap_tier_dirs=false', '--timeout_ms=30000', '--disk_io_workers=8', '--disk_io_per_node=4',
                                   '--client_direct_io=true', '--io_latency_enabled=true', f'--io_latency_dir={root / "metrics"}',
                                   '--', str(mount), '-f', '-o',
                                   'attr_timeout=0,entry_timeout=0,negative_timeout=0'],
                                  stdout=logs[2], stderr=subprocess.STDOUT, start_new_session=True)
        wait_for(lambda: os.path.ismount(mount), [env, node, client])
        require(len(os.listdir(mount)) == 16, 'directory metadata mismatch')
        # Real data is generated only on the input side. No writes into the node's private paths.
        sources = {}
        rows = []
        for i in range(8):
            data = random.Random(1000 + i).randbytes(512 * 1024 + 123 + i)
            source = root / f'source{i}.bin'
            source.write_bytes(data)
            sources[i] = data
            rows.append(f'w{i}\twrite\t{mount / f"file{i}.bin"}\t0\t{len(data)}\t{source}\t-')
        for i, data in sources.items():
            rows.append(f'r{i}\tread\t{mount / f"file{i}.bin"}\t0\t{len(data)}\t{root / f"out{i}.bin"}\tw{i}')
        manifest = root / 'tasks.tsv'
        manifest.write_text('\n'.join(rows) + '\n')
        start = time.monotonic()
        completed = subprocess.run([str(args.batch), str(manifest), '8'], capture_output=True, text=True, timeout=120)
        (root / 'batch.log').write_text(completed.stdout + completed.stderr)
        require(completed.returncode == 0, 'batch failed; inspect batch.log')
        result['batch_seconds'] = round(time.monotonic() - start, 3)
        result['sha256'] = {}
        for i, data in sources.items():
            actual = (root / f'out{i}.bin').read_bytes()
            require(actual == data, f'content mismatch file{i}')
            result['sha256'][f'file{i}'] = hashlib.sha256(actual).hexdigest()
        # Concurrent nonoverlapping writes to ONE inode, through the SAME FUSE process.
        def write_region(i):
            fd = os.open(mount / 'file8.bin', os.O_WRONLY)
            try:
                payload = bytes([i + 1]) * 65536
                require(os.pwrite(fd, payload, i * len(payload)) == len(payload), 'same-file write failed')
            finally:
                os.close(fd)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(write_region, range(8)))
        expected = b''.join(bytes([i + 1]) * 65536 for i in range(8))
        require((mount / 'file8.bin').read_bytes() == expected, 'same-file serialization/content mismatch')
        require(os.stat(mount / 'file8.bin').st_size == len(expected), 'file size lost during concurrent writes')
        fd = os.open(mount / 'file0.bin', os.O_RDONLY)
        try:
            for offset, size in [(3, 17), (16380, 100), (len(sources[0]) - 7, 30), (len(sources[0]), 50)]:
                require(os.pread(fd, size, offset) == sources[0][offset:offset + size], 'offset/EOF read mismatch')
        finally:
            os.close(fd)
        # Truncate is an existing client callback and must share the file operation lock.
        os.truncate(mount / 'file8.bin', 4096)
        require((mount / 'file8.bin').read_bytes() == expected[:4096], 'truncate/read failed')
        # A second independent production FUSE process, sharing the actual node.
        client2 = subprocess.Popen([str(args.client), f'--mds={mock}', f'--scheduler={mock}',
                                    '--bootstrap_tier_dirs=false', '--client_direct_io=true',
                                    '--timeout_ms=30000', '--', str(mount2), '-f', '-o',
                                    'attr_timeout=0,entry_timeout=0,negative_timeout=0'],
                                   stdout=logs[3], stderr=subprocess.STDOUT, start_new_session=True)
        wait_for(lambda: os.path.ismount(mount2), [env, node, client, client2])

        def control(command, expected):
            env.stdin.write(command + '\n'); env.stdin.flush()
            require(select.select([env.stdout], [], [], 15)[0], 'control response timeout')
            require(env.stdout.readline().strip() == expected, f'control failed: {command}')

        def pwrite_at(path, data, offset=0):
            fd = os.open(path, os.O_WRONLY)
            try:
                return os.pwrite(fd, data, offset)
            finally:
                os.close(fd)

        shared1, shared2 = mount / 'file10.bin', mount2 / 'file10.bin'
        require(pwrite_at(shared1, b'original') == 8, 'seed write failed')
        # Populate B's old metadata cache, then let A commit a newer version.
        fd = os.open(shared2, os.O_RDWR)
        os.close(fd)
        control('HOLD 12', 'HOLD_OK')  # file10 -> inode12
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            pending = executor.submit(pwrite_at, shared1, b'A' * 65536)
            try:
                # Pause ONLY the external MDS reply; production client/node have
                # no injected hooks. A must still hold the node lock at this point.
                control('WAIT_HELD', 'HELD')
                for label, action in [
                    ('write', lambda: pwrite_at(shared2, b'B' * 65536)),
                    ('truncate', lambda: os.truncate(shared2, 1)),
                ]:
                    try:
                        action()
                    except OSError as error:
                        require(error.errno == errno.EAGAIN, f'{label}: expected EAGAIN, got {error}')
                    else:
                        raise RuntimeError(f'{label}: competing client bypassed file lock')
                control('CHECK_LOCK 12', 'LOCK_CHECK_OK')
                require(pwrite_at(mount2 / 'file11.bin', b'independent') == 11,
                        'unrelated file was blocked by file lock')
            finally:
                control('RESUME', 'RESUMED')
            require(pending.result(timeout=15) == 65536, 'owner write did not complete')
        require(shared2.read_bytes() == b'A' * 65536, 'rejected writer changed data')
        # B succeeds after A releases; must plan against the latest node version.
        require(pwrite_at(shared2, b'B' * 65536, 65536) == 65536, 'retry after release failed')
        # A now has a stale version; truncate must refetch it while holding its lock.
        os.truncate(shared1, 65536 + 17)
        require(shared1.read_bytes() == b'A' * 65536 + b'B' * 17, 'fresh-version truncate failed')
        # Reacquire from B, proving truncate also released the lock.
        require(pwrite_at(shared2, b'C', 0) == 1, 'lock leaked after truncate')
        require(shared2.read_bytes() == b'C' + b'A' * 65535 + b'B' * 17,
                'cross-client final content mismatch')
        result['cross_client_lock'] = dict(write_conflict='EAGAIN', truncate_conflict='EAGAIN',
            invalid_token_rejected=True, other_file_progress=True, retry_after_release=True,
            truncate_uses_fresh_version=True, content_verified=True)
        subprocess.run(['fusermount3', '-u', str(mount2)], check=True, timeout=15)
        require(client2.wait(timeout=20) == 0, 'second FUSE exit failed')
        subprocess.run(['fusermount3', '-u', str(mount)], check=True, timeout=15)
        require(client.wait(timeout=20) == 0, 'FUSE exit failed')
        env.stdin.write('STATS\n'); env.stdin.flush()
        require(select.select([env.stdout], [], [], 10)[0], 'stats timeout')
        stats = env.stdout.readline().split()
        require(stats[0] == 'STATS', 'stats response')
        opened, closed, live, updates, heartbeat = map(int, stats[1:])
        require(opened == closed and live == 0 and updates > 0 and heartbeat > 0, 'external call/handle checks failed')
        result['external_calls'] = dict(opened=opened, closed=closed, live=live, updates=updates, heartbeat=heartbeat)
        stop(node, signal.SIGINT)
        require(node.returncode == 0, 'node exit failed')
        for label, filename, prefix in [('node', 'node.log', 'disk_io'), ('client', 'client.log', 'client_io')]:
            text = (root / filename).read_text(errors='replace')
            m = re.search(r'\[' + prefix + r'\] peak_active=(\d+) completed=(\d+) rejected=(\d+)', text)
            require(m is not None and int(m[1]) > 1, f'{label} has no evidence of concurrent operations')
            result[label + '_io'] = dict(peak_active=int(m[1]), completed=int(m[2]), rejected=int(m[3]))
        env.stdin.write('QUIT\n'); env.stdin.flush()
        require(env.wait(timeout=15) == 0, 'environment exit failed')
    finally:
        if env and env.poll() is None:
            try:
                env.stdin.write('RESUME\n'); env.stdin.flush()
            except BrokenPipeError:
                pass
        if os.path.ismount(mount2):
            subprocess.run(['fusermount3', '-uz', str(mount2)], timeout=15, check=False)
        stop(client2)
        if os.path.ismount(mount):
            subprocess.run(['fusermount3', '-uz', str(mount)], timeout=15, check=False)
        stop(client)
        stop(node, signal.SIGINT)
        stop(env)
        for log in logs:
            log.close()
        if env:
            env.stdin.close(); env.stdout.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('environment', 'node', 'client', 'batch'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    if not Path('/dev/fuse').exists() or not shutil.which('fusermount3'):
        print('SKIP: FUSE unavailable')
        return 77
    for name in ('environment', 'node', 'client', 'batch'):
        setattr(args, name, getattr(args, name).resolve())
    results = Path(__file__).resolve().parent / 'results'
    results.mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='disk-concurrent-', dir=results))
    print(f'Results: {root}', flush=True)
    result = {'status': 'FAIL'}
    try:
        run(args, root, result)
        result['status'] = 'PASS'
        print(json.dumps(result, indent=2), flush=True)
        return 0
    except Exception as e:
        result['error'] = str(e)
        print(f'FAIL: {e}; logs: {root}', flush=True)
        return 1
    finally:
        (root / 'result.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
