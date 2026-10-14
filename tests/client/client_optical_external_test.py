#!/usr/bin/env python3
"""Black-box test: external dependencies + production optical node + production FUSE.

Only the current archive completion logs supplement public interfaces. This script
never creates/edits the optical node's internal images, indices, or runtime state.
"""
import argparse
import errno
import hashlib
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
import zlib

HERE = Path(__file__).resolve().parent
MIB = 1024 * 1024
UNIT = 256 * 1024


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def wait_for(predicate, timeout, message, processes=()):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            require(process.poll() is None, f'process {process.args[0]} exited: {process.returncode}')
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError(message)


def stop(process, graceful_signal=signal.SIGTERM):
    if process and process.poll() is None:
        process.send_signal(graceful_signal)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


class Environment:
    def __init__(self, executable, corpus, optical, log):
        self.process = subprocess.Popen([str(executable), str(corpus), optical],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=log, text=True, bufsize=1, start_new_session=True)
        try:
            ready = self.receive()
            require(ready.startswith('READY '), ready)
            self.address = ready.split()[1]
        except Exception:
            stop(self.process)
            self.process.stdin.close()
            self.process.stdout.close()
            raise

    def receive(self):
        if not select.select([self.process.stdout], [], [], 15)[0]:
            raise TimeoutError('external service control response timed out')
        line = self.process.stdout.readline().strip()
        require(bool(line) and not line.startswith('ERROR'), f'external service: {line}')
        return line

    def command(self, command):
        self.process.stdin.write(command + '\n')
        self.process.stdin.flush()
        return self.receive()

    def close(self):
        try:
            if self.process.poll() is None:
                self.command('QUIT')
                self.process.wait(timeout=15)
        finally:
            stop(self.process)
            self.process.stdin.close()
            self.process.stdout.close()


def generate_corpus(root, demo_text=False):
    expected = root / 'expected'
    expected.mkdir()
    # First image: empty + small + large; second image: another; third: seal trigger.
    # Each ~9.25 MiB incompressible large file crosses the 10 MiB * 0.9 pack threshold.
    # A 12 MiB disc holds one image, so the third image seals the second disc.
    greeting = (b'Hello from the optical node!\n'
                b'This file was archived, prepared asynchronously, and read through FUSE.\n')
    small = (1001, 'hello.txt', len(greeting)) if demo_text else (1001, 'small.bin', 123)
    specs = [(1000, 'empty.bin', 0), small,
             (1002, 'large.bin', 9 * MIB + UNIT + 123),
             (1003, 'another.bin', 9 * MIB + UNIT + 17),
             (1004, 'tail.bin', 9 * MIB + UNIT + 31)]
    files, rows, groups, pending, group = {}, [], [], 0, []
    for inode, name, size in specs:
        data = greeting if demo_text and inode == 1001 else random.Random(20261014 + inode).randbytes(size)
        source = expected / name
        source.write_bytes(data)
        compressed = len(zlib.compress(data, 9)) if data else 0
        require(compressed < 10 * MIB, 'test file exceeds volume capacity')
        if pending + compressed > 10 * MIB:
            groups.append(group)
            pending, group = 0, []
        pending += compressed
        group.append(inode)
        if pending >= 9 * MIB:
            groups.append(group)
            pending, group = 0, []
        files[inode] = {'name': name, 'size': size, 'sha256': hashlib.sha256(data).hexdigest(),
                        'source': str(source)}
        rows.append(f'{inode}\t{size}\t/{name}\t{source}')
    require(not group and groups == [[1000, 1001, 1002], [1003], [1004]], 'unexpected compression/pack plan')
    manifest = root / 'corpus.tsv'
    manifest.write_text('\n'.join(rows) + '\n')
    (root / 'corpus.json').write_text(json.dumps(files, indent=2) + '\n')
    return manifest, files, groups


def reports(path):
    text = path.read_text(errors='replace')
    packed, burned = {}, {}
    for pattern, target, field in [
        (r'\[ReportFilesPackedToImage\] image_id=(\d+) count=(\d+) inode_ids=([\d,]+)', packed, 'inode'),
        (r'\[ReportImagesBurnedToDisc\] disc_id=(\d+) count=(\d+) image_ids=([\d,]+)', burned, 'image')]:
        for match in re.finditer(pattern, text):
            key, count = int(match[1]), int(match[2])
            values = [int(v) for v in match[3].split(',')]
            require(len(values) == count and len(set(values)) == count, f'invalid {field} report')
            require(key not in target or target[key] == values, 'conflicting archive reports')
            target[key] = values
    return packed, burned


def run(args, root, result, on_ready=None):
    corpus, files, groups = generate_corpus(root, demo_text=getattr(args, 'demo', False))
    mount = root / 'mount'
    mount.mkdir()
    control = root / 'control.sock'
    require(len(str(control).encode()) < 104, 'work-dir too long for Unix socket')
    archive = root / 'archive'  # Created and populated ONLY by optical_node_server.
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    optical_address = f'127.0.0.1:{port}'
    env = node = client = None
    node_log_path, client_log_path = root / 'optical.log', root / 'client.log'
    with (root / 'external.log').open('w') as env_log, node_log_path.open('w') as node_log, \
            client_log_path.open('w') as client_log:
        try:
            env = Environment(args.environment, corpus, optical_address, env_log)
            config = root / 'optical.conf'
            config.write_text('\n'.join([
                'NODE_ID=optical-test', f'NODE_ADDRESS={optical_address}',
                'NODE_ROLE=PRIMARY', 'GROUP_ID=optical-test', 'REPLICATION_ENABLED=false',
                f'SCHEDULER_ADDR={env.address}', f'ARCHIVE_ROOT={archive}',
                f'VOLUME_SIZE_BYTES={10 * MIB}', 'SIZE_THRESHOLD=0.9',
                f'DISC_CAPACITY_BYTES={12 * MIB}', 'STANDARD_IMAGES_PER_DISC=1',
                'DISC_BLOCK_SIZE_BYTES=2048', 'CAPACITY_IN_IMAGES=5',
                'MAX_WRITE_IMAGES=3', 'AVAILABLE_ID_COUNT=5', 'HEARTBEAT_INTERVAL_MS=1000', '']))
            node = subprocess.Popen([str(args.optical), f'--config={config}', f'--port={port}'],
                                    stdout=node_log, stderr=subprocess.STDOUT, start_new_session=True)

            def listening():
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        return True
                except OSError:
                    return False

            wait_for(listening, 20, 'optical server did not listen', [node, env.process])
            require(env.command('ARCHIVE') == 'OK ARCHIVE', 'archive submission failed')
            print('归档已提交，等待正式节点打包、封盘和刻录（使用节点原有设备时延）。', flush=True)

            def archive_complete():
                packed, burned = reports(node_log_path)
                return (packed, burned) if len(packed) == 3 and len(burned) == 2 else None

            packed, burned = wait_for(archive_complete, args.timeout, 'archive reports timed out', [node, env.process])
            require(sorted(sorted(v) for v in packed.values()) == groups, 'actual packing differs from corpus')
            locations = {}
            for disc, images in burned.items():
                require(len(images) == 1, 'test disc must contain exactly one image')
                require((archive / 'disc_sim' / f'disc_{disc}.vdisc').is_file(), 'reported disc missing')
                for image in images:
                    require(not (archive / 'image' / f'volume_{image}.vimg').exists(), 'cold-read image still cached')
                    for inode in packed[image]:
                        require(inode not in locations, 'inode archived twice')
                        locations[inode] = {'disc': disc, 'image': image}
                        require(env.command(f'PUBLISH {inode} {disc} {image}') == 'OK PUBLISH', 'publish failed')
            require(set(locations) == {1000, 1001, 1002, 1003}, 'unexpected published files')
            (root / 'locations.json').write_text(json.dumps(locations, indent=2) + '\n')
            result['archive'] = {'packed_images': len(packed), 'burned_discs': len(burned), 'locations': locations}
            print('归档完成，模拟 MDS 已发布实际光盘位置；启动原有 FUSE 客户端。', flush=True)

            client = subprocess.Popen([
                str(args.client), f'--mds={env.address}', f'--scheduler={env.address}',
                '--bootstrap_tier_dirs=false', '--optical_read_enabled=true',
                '--optical_read_chunk_bytes=65536', f'--prepare_control_socket={control}',
                '--', str(mount), '-f', '-o', 'ro,attr_timeout=0,entry_timeout=0,negative_timeout=0',
            ], stdout=client_log, stderr=subprocess.STDOUT, start_new_session=True)
            wait_for(lambda: os.path.ismount(mount) and control.exists(), 20, 'FUSE mount failed', [client, node, env.process])
            require(set(os.listdir(mount)) == {files[i]['name'] for i in locations}, 'directory contents wrong')
            try:
                os.stat(mount / 'missing.bin')
                raise RuntimeError('nonexistent path returned metadata')
            except FileNotFoundError:
                pass

            # The manual demonstration keeps exactly the same external services and production
            # processes, but leaves preparation and POSIX reads to the operator's other terminal.
            if on_ready is not None:
                on_ready(args, root, files, locations, env, node, client, result)
                return

            def ctl(operation, value, timeout=15):
                return subprocess.check_output([str(args.ctl), '--socket', str(control), 'stage', operation, str(value)],
                                               text=True, timeout=timeout).strip()

            def unprepared(path):
                try:
                    fd = os.open(path, os.O_RDONLY)
                except OSError as exc:
                    require(exc.errno == errno.EAGAIN, f'expected EAGAIN, got {exc}')
                else:
                    os.close(fd)
                    raise RuntimeError('unprepared open unexpectedly succeeded')

            handles = {}
            def check_file(inode, cold, lifecycle=False):
                info = files[inode]
                path = mount / info['name']
                expected = Path(info['source']).read_bytes()
                if inode not in handles:
                    unprepared(path)
                start = time.monotonic()
                handle = ctl('submit', path)
                accepted = time.monotonic()
                if inode in handles:
                    require(handle == handles[inode], 'repeated preparation was not reused')
                handles[inode] = handle
                if cold:
                    require(accepted - start < 5, 'prepare submission blocked on optical loading')
                    unprepared(path)  # Must still be pending after the submission RPC returns.
                require(ctl('wait', handle, args.timeout) == str(path), 'ready path mismatch')
                ready = time.monotonic()
                require((archive / 'image' / f"volume_{locations[inode]['image']}.vimg").is_file(), 'CD_READ did not load image')
                fd1 = os.open(path, os.O_RDONLY)
                fd2 = -1
                try:
                    # Sequential read all bytes; no test data is injected into the FUSE mount.
                    actual = bytearray()
                    while True:
                        block = os.read(fd1, MIB)
                        if not block:
                            break
                        actual.extend(block)
                    require(actual == expected, f'full content mismatch: {path}')
                    require(hashlib.sha256(actual).hexdigest() == info['sha256'], 'hash mismatch')
                    for offset, size in [(0, 0), (3, 91), (65533, 12345),
                                         (max(0, len(expected) - 7), 32), (len(expected), 10),
                                         (len(expected) + 123, 10), (0, MIB)]:
                        require(os.pread(fd1, size, offset) == expected[offset:offset + size], 'random/EOF reread mismatch')
                    if lifecycle:
                        fd2 = os.open(path, os.O_RDONLY)
                        os.close(fd1)
                        fd1 = os.open(path, os.O_RDONLY)  # close does not release preparation.
                        require(os.pread(fd1, 777, 7) == expected[7:784], 'reopened reader invalidated')
                        require(os.pread(fd2, 888, 19) == expected[19:907], 'second reader invalidated')
                finally:
                    if fd1 >= 0:
                        os.close(fd1)
                    if fd2 >= 0:
                        os.close(fd2)
                # All readers closed; the node still owns the prepared cache. No release RPC.
                with path.open('rb') as reopened:
                    require(reopened.read(123) == expected[:123], 'close incorrectly revoked preparation')
                result['reads'].append({'inode': inode, 'cold': cold, 'size': len(expected),
                                        'submit_ms': round((accepted - start) * 1000, 2),
                                        'prepare_ms': round((ready - start) * 1000, 2),
                                        'sha256': info['sha256'], 'lifecycle': lifecycle})
                print(f"PASS {info['name']} cold={cold} bytes={len(expected)} prepare={ready-start:.2f}s", flush=True)

            check_file(1002, True, lifecycle=True)
            check_file(1002, False)  # reuse preparation after closing all readers
            check_file(1001, False)  # same image, different inode
            check_file(1000, False)  # empty file
            check_file(1003, True)  # another image/disc
            subprocess.run(['fusermount3', '-u', str(mount)], check=True, timeout=15)
            require(client.wait(timeout=20) == 0, 'client exit failed')
            require(not control.exists(), 'client left control socket')
            stats = json.loads(env.command('STATS').removeprefix('STATS '))
            require(stats['live_handles'] == 0 and stats['opened'] == stats['close'], 'MDS handle leak')
            require(stats['resolve'] == len(files), 'missing archive file resolution')
            require(stats['read_object'] == sum((f['size'] + UNIT - 1) // UNIT for f in files.values()), 'archive shard count mismatch')
            # This FUSE implementation resolves stat through Lookup rather than Getattr RPC.
            for key in ['lookup', 'location', 'readdir', 'cluster_view', 'heartbeat']:
                require(stats.get(key, 0) > 0, f'no external {key} interaction')
            result['external_calls'] = stats
            log = client_log_path.read_text(errors='replace')
            for phase in ['accepted', 'ready', 'opened', 'closed']:
                require(f'[optical] {phase} ' in log, f'missing client {phase} evidence')
            require('[optical] released ' not in log, 'client unexpectedly released node-owned cache')
            # All notifications came from the real optical service; env registers no optical service.
            result['notification_events'] = len(re.findall(r'\[optical\] ready request=', log))
            require(result['notification_events'] == len(handles), 'notification count mismatch')
            result['cache_management'] = 'node TTL; no explicit prepared-cache release from client'
            node.send_signal(signal.SIGINT)  # brpc RunUntilAskedToQuit handles SIGINT by default.
            node_exit = node.wait(timeout=30)
            require(node_exit == 0, f'optical node shutdown failed: exit={node_exit}')
            env.close()
            env = None
        finally:
            if os.path.ismount(mount):
                unmount = subprocess.run(['fusermount3', '-u', str(mount)], timeout=15, check=False)
                if unmount.returncode:
                    # A manual demo terminal may still have its cwd/open files in this mount.
                    # Detach only our own mount before stopping the services it depends on.
                    print('挂载仍被占用，正在分离本次演示挂载。', flush=True)
                    subprocess.run(['fusermount3', '-uz', str(mount)], timeout=15, check=True)
            stop(client)
            stop(node, signal.SIGINT)
            if env:
                env.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--environment', type=Path, required=True)
    parser.add_argument('--optical', type=Path, required=True)
    parser.add_argument('--client', type=Path, required=True)
    parser.add_argument('--ctl', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, help='new empty run directory; default tests/client/results/')
    parser.add_argument('--timeout', type=int, default=240, help='archive or notification deadline, seconds')
    args = parser.parse_args()
    if not Path('/dev/fuse').exists() or not shutil.which('fusermount3'):
        print('SKIP: /dev/fuse or fusermount3 unavailable')
        return 77
    for name in ['environment', 'optical', 'client', 'ctl']:
        value = getattr(args, name).resolve()
        require(value.is_file() and os.access(value, os.X_OK), f'missing executable: {value}')
        setattr(args, name, value)
    require(args.timeout > 0, 'timeout must be positive')
    if args.work_dir:
        root = args.work_dir.resolve()
        root.mkdir(parents=True, exist_ok=True)
        require(not any(root.iterdir()), 'work-dir must be empty')
    else:
        results = HERE / 'results'
        results.mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix='optical-external-', dir=results))
    print(f'结果目录: {root}', flush=True)
    result = {'status': 'FAIL', 'reads': [], 'work_dir': str(root)}
    start = time.monotonic()
    try:
        run(args, root, result)
        result['status'] = 'PASS'
        print('PASS: external dependencies + production optical node + production FUSE/POSIX', flush=True)
        return 0
    except Exception as exc:
        result['error'] = str(exc)
        print(f'FAIL: {exc}\n日志保留在 {root}', flush=True)
        return 1
    finally:
        result['elapsed_seconds'] = round(time.monotonic() - start, 2)
        (root / 'result.json').write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
