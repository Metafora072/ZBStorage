#!/usr/bin/env python3
"""Manually verify a completed synthetic import using an isolated MDS and restart."""
import argparse
import http.client
import json
from pathlib import Path
import socket
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--build-dir', type=Path, default=REPO / 'build')
    args = parser.parse_args()
    root = args.root.resolve()
    build = args.build_dir.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    state = json.loads((root / 'bulk_state.json').read_text())
    plan = state['plan']
    assert sum(v['file_count'] for v in state['verified'].values()) == plan['total_files']
    config = output / 'mds.conf'
    config.write_text(f'MDS_DB_PATH={root}/db\nMASSTREE_ROOT={root}/masstree_meta\n'
                      f'ARCHIVE_META_ROOT={root}/archive_meta\nSCHEDULER_ADDR=\n'
                      'NODES=trial@127.0.0.1:1,type=REAL,weight=1\nDISKS=trial:disk-01\n'
                      'REPLICA=1\nENABLE_OPTICAL_ARCHIVE=false\nMASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\n')
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]

    def rpc(method, **request):
        conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
        try:
            conn.request('POST', '/zb.rpc.MdsService/' + method, json.dumps(request),
                         {'Content-Type': 'application/json'})
            response = conn.getresponse()
            data = response.read()
            assert response.status == 200, data
            data = json.loads(data)
            assert data['status'].get('code', 0) in (0, 'MDS_OK'), data
            return data
        finally:
            conn.close()

    samples = {}
    rounds = []
    for iteration in range(2):
        with (output / f'mds-{iteration}.log').open('w') as log:
            server = subprocess.Popen([str(build / 'mds_server'), f'--config={config}', f'--port={port}'],
                                      stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 45
                while True:
                    assert server.poll() is None, f'MDS exited; inspect {log.name}'
                    try:
                        stats = rpc('GetMasstreeClusterStats')
                        break
                    except (OSError, http.client.HTTPException):
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
                assert int(stats['total_file_count']) == plan['total_files']
                assert stats['optical_layout_version'] == 'uniform_2tb_v2'
                # Offline writers must not open a database served by MDS.
                locked = subprocess.run([str(build / 'masstree_meta_build_tool'),
                                         f'--db_path={root}/db', '--inspect'], capture_output=True, text=True)
                assert locked.returncode != 0 and 'LOCK' in locked.stderr, locked.stderr
                looked_up = 0
                for ns, receipt in sorted(state['verified'].items()):
                    count = receipt['file_count']
                    namespace = rpc('GetMasstreeNamespaceStats', namespace_id=ns)
                    assert int(namespace['file_count']) == count
                    for index in sorted({0, count // 2, count - 1, min(count - 1, 2048)}):
                        leaf = index // plan['files_per_leaf']
                        level, slot = divmod(leaf, plan['subdirs_per_dir'])
                        directory = f'/masstree_demo/{ns}/d{level:06d}/s{slot:06d}'
                        name = f'f{index:09d}'
                        path = directory + '/' + name
                        attr = rpc('Lookup', path=path)['attr']
                        assert attr['file_name'] == name
                        assert 500000000 <= int(attr['size']) <= 1500000000
                        assert attr['storage_tier'] in (2, 'INODE_STORAGE_OPTICAL')
                        assert 0 <= int(attr.get('optical_image_id', 0)) < 20
                        assert 0 <= int(attr.get('optical_disk_id', 0)) < 10000
                        assert rpc('Getattr', inode_id=attr['inode_id'])['attr'] == attr
                        listing = rpc('Readdir', path=directory, limit=1, start_after=f'f{index-1:09d}' if index else '')
                        assert listing['entries'][0]['name'] == name
                        assert int(listing['entries'][0]['inode_id']) == int(attr['inode_id'])
                        # Ignore access-time changes when comparing across restart.
                        stable = {k: v for k, v in attr.items() if k != 'atime'}
                        if iteration:
                            assert samples[path] == stable
                        else:
                            samples[path] = stable
                        looked_up += 1
                rounds.append(dict(restart=iteration, file_count=int(stats['total_file_count']),
                                   sampled_paths=looked_up, offline_lock_checked=True))
            finally:
                if server.poll() is None:
                    server.terminate()
                    try:
                        server.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        server.kill()
                        server.wait()
    (output / 'summary.json').write_text(json.dumps({'passed': rounds}, indent=2) + '\n')
    print(json.dumps({'passed': rounds}, indent=2))


if __name__ == '__main__':
    main()
