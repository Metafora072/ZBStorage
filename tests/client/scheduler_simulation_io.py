#!/usr/bin/env python3
"""Manual POSIX verification against the production client and Scheduler models."""
import concurrent.futures
import errno
import os
from pathlib import Path
import re
import sys
import time

from scheduler_simulation_demo import NODES, field, require, save_json


def expect_errno(action, expected, description):
    try:
        value = action()
    except OSError as error:
        require(error.errno == expected, f'{description}: expected errno={expected}, got {error}')
    else:
        if isinstance(value, int):
            os.close(value)
        raise RuntimeError(f'{description}: unexpectedly succeeded')


def write_file(path, size, fill):
    # Deliberately do NOT write 'x': a model read is not an original-content check.
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        offset = 0
        while offset < size:
            chunk = bytes([fill]) * min(65536, size - offset)
            written = os.pwrite(fd, chunk, offset)
            require(written > 0, 'zero-length write')
            offset += written
    finally:
        os.close(fd)
    return size


def verify(env):
    directory = env.mount / 'virtual'
    result = {'status': 'PASS', 'processes': 'production FUSE + MDS + Scheduler',
              'payload_semantics': 'generated model bytes, not preserved application data'}
    print('1/4：通过 POSIX 并发写入、读取多个模型磁盘文件……', flush=True)
    start = time.monotonic()
    paths = [directory / f'parallel-{i}.bin' for i in range(12)]
    sizes = [196608 + 123 + i for i in range(len(paths))]
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as workers:
        list(workers.map(lambda item: write_file(item[1], sizes[item[0]], 65 + item[0]), enumerate(paths)))
    placement = []
    for path, size in zip(paths, sizes):
        location = env.location('/virtual/' + path.name)
        disk = field(location, 'disk_location', {})
        node = field(disk, 'node_id')
        generation = int(field(disk, 'instance_generation', 0))
        require(node in ('client-model-a', 'client-model-b') and generation == NODES[node],
                f'invalid file route: {location}')
        require(int(field(location['attr'], 'size', 0)) == size, 'MDS file size mismatch')
        placement.append({'path': path.name, 'node': node, 'generation': generation, 'size': size})
    require(len({p['node'] for p in placement}) == 2, 'placement did not cover both disk models')
    save_json(env.root / 'placement.json', placement)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as workers:
        outputs = list(workers.map(Path.read_bytes, paths))
    for path, content, size in zip(paths, outputs, sizes):
        require(content == b'x' * size,
                f'{path.name}: expected {size} generated x bytes, got {len(content)} bytes; '
                f'pattern_matches={content == b"x" * len(content)} '
                '(not original-content verification)')
    fd = os.open(paths[0], os.O_RDONLY)
    try:
        require(os.pread(fd, 100, 16380) == b'x' * 100, 'cross-object read failed')
        require(os.pread(fd, 100, sizes[0] - 3) == b'xxx', 'short EOF read failed')
        require(os.pread(fd, 100, sizes[0]) == b'', 'EOF read failed')
    finally:
        os.close(fd)
    shared = directory / 'same-client.bin'
    write_file(shared, 1, 65)
    def write_region(index):
        fd = os.open(shared, os.O_WRONLY)
        try:
            require(os.pwrite(fd, bytes([65 + index]) * 16384, index * 16384) == 16384,
                    'same-client write incomplete')
        finally:
            os.close(fd)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as workers:
        list(workers.map(write_region, range(8)))
    require(shared.read_bytes() == b'x' * (8 * 16384), 'same-client final size mismatch')
    os.truncate(shared, 19000)
    require(shared.read_bytes() == b'x' * 19000, 'model truncate/EOF failed')
    result['disk'] = {'placement': placement, 'parallel_files': len(paths),
                      'cross_object_and_eof': True, 'same_client_writes_and_truncate': True,
                      'elapsed_seconds': round(time.monotonic() - start, 3)}

    print('2/4：经正式 FUSE 创建文件，再由外部 fixture 调用公开归档接口……', flush=True)
    optical = directory / 'hello.txt'
    write_file(optical, 131077, 72)
    published = env.archive('/virtual/hello.txt')
    save_json(env.root / 'optical-location.json', published)
    target = field(published, 'optical_location', {})
    require(field(target, 'node_id') == 'client-model-optical' and
            int(field(target, 'instance_generation', 0)) == 9, 'optical route mismatch')
    expect_errno(lambda: os.open(optical, os.O_RDONLY), errno.EAGAIN, 'unprepared optical open')
    print('3/4：非阻塞提交准备，等待服务端通知，然后通过 POSIX 读取……', flush=True)
    start = time.monotonic()
    handle = env.ctl('submit', optical)
    submit_seconds = time.monotonic() - start
    # With >=1 s loading this checks submission did not wait for preparation.
    require(submit_seconds < env.args.prepare_ms / 1000, 'submit waited for modeled preparation')
    expect_errno(lambda: os.open(optical, os.O_RDONLY), errno.EAGAIN, 'pending optical open')
    env.ctl('wait', handle, timeout=env.args.prepare_ms / 1000 + 15)
    ready_seconds = time.monotonic() - start
    require(ready_seconds >= env.args.prepare_ms / 1000 * .8, 'cold model read skipped configured delay')
    require(optical.read_bytes() == b'x' * 131077, 'prepared POSIX read failed')
    fd = os.open(optical, os.O_RDONLY)
    try:
        require(os.pread(fd, 31, 131070) == b'x' * 7, 'optical EOF read failed')
        require(os.pread(fd, 31, 131077) == b'', 'optical EOF failed')
    finally:
        os.close(fd)
    # No user release is needed between successive opens.
    require(optical.read_bytes() == b'x' * 131077, 'repeated optical open/read failed')
    expect_errno(lambda: os.open(optical, os.O_WRONLY), errno.EROFS, 'optical write open')
    result['optical'] = {'handle': handle, 'submit_seconds': round(submit_seconds, 3),
                         'ready_seconds': round(ready_seconds, 3), 'unprepared_errno': 'EAGAIN',
                         'pending_errno': 'EAGAIN', 'read_size': 131077, 'repeat_read_without_release': True}

    print('4/4：核对实际逻辑路由、通知日志和 Scheduler 节点计量……', flush=True)
    text = (env.root / 'client.log').read_text(errors='replace')
    for name in ('client-model-a', 'client-model-b'):
        for operation in ('WriteObject', 'ReadObject'):
            pattern = rf'\[data_route\] op={operation} node={name} generation={NODES[name]} .*errno=0'
            require(re.search(pattern, text), f'no production RPC evidence for {operation}/{name}')
    ready_rows = [row for row in text.splitlines() if '[optical] ready ' in row and f'request={handle} ' in row]
    require(ready_rows, 'no pushed ReadReady evidence in client log')
    require('node_id=client-model-optical generation=9' in text, 'optical logical route missing')
    require('[file_lock] release failed' not in text, 'unexpected lock release failure')
    snapshots = {}
    def reported():
        snapshots.update(env.snapshot())
        for name in NODES:
            streams = field(snapshots[name], 'metrics_streams', [])
            read_bytes = sum(int(field(s, 'read_bytes', 0)) for s in streams)
            write_bytes = sum(int(field(s, 'write_bytes', 0)) for s in streams)
            if read_bytes == 0 or (name != 'client-model-optical' and write_bytes == 0):
                return False
        return True
    env.wait(reported)
    save_json(env.root / 'managed-after.json', snapshots)
    result['evidence'] = {'disk_routes': True, 'read_ready_notification': ready_rows,
                          'node_metrics_reported': True, 'direct_io': True}
    save_json(env.root / 'placement.json', placement)
    return result


if __name__ == '__main__':
    # Same lifecycle/cleanup as the interactive demo; no CTest registration.
    from scheduler_simulation_demo import main
    sys.argv.insert(1, '--check')
    raise SystemExit(main())
