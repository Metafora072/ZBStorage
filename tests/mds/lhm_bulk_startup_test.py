#!/usr/bin/env python3
"""Start MDS from a supplied type/path TSV; verify the imported tree via RPC.

Use the small tree.tsv retained by lhm_bulk_import_test. All outputs go to the
explicit result directory. No data-node service or file-content data is created.
"""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import time
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--build', type=Path, required=True)
    p.add_argument('--manifest', type=Path, required=True)
    p.add_argument('--work-dir', type=Path, required=True)
    args = p.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    rows = [line.split('\t') for line in args.manifest.read_text().splitlines()[1:]]
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    config = work / 'mds.conf'
    config.write_text(
        f'MDS_DB_PATH={work}/control\nNAMESPACE_BACKEND=lhm\n'
        f'LHM_STORE_FILE={work}/namespace.meta\nLHM_BULK_MANIFEST={args.manifest.resolve()}\n'
        f'MASSTREE_ROOT={work}/templates\nENABLE_OPTICAL_ARCHIVE=false\n'
        'MASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\n'
        'NODES=node-real-01@127.0.0.1:1\nDISKS=node-real-01:disk-01\nREPLICA=1\nPG_COUNT=16\n')
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    records = []

    def rpc(method, body):
        request = urllib.request.Request(f'http://127.0.0.1:{port}/zb.rpc.MdsService/{method}',
            data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
        with opener.open(request, timeout=10) as response:
            result = json.load(response)
        records.append({'method': method, 'request': body, 'response': result})
        assert result.get('status', {}).get('code', 0) in (0, 'MDS_OK'), result
        return result

    with (work / 'mds.log').open('w') as log:
        process = subprocess.Popen([str(args.build.resolve() / 'mds_server'),
            f'--config={config}', f'--port={port}'], cwd=work, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 30
            while True:
                assert process.poll() is None, 'MDS exited: see mds.log'
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline, 'MDS startup timeout'
                    time.sleep(0.1)
            for _, path in rows:
                rpc('Lookup', {'path': path})
            for kind, path in rows:
                if kind != 'dir':
                    continue
                expected = sorted(Path(child).name for _, child in rows[1:] if str(Path(child).parent) == path)
                actual, token = [], ''
                while True:
                    result = rpc('Readdir', {'path': path, 'limit': 17, 'start_after': token})
                    actual.extend(e['name'] for e in result.get('entries', []))
                    if not result.get('has_more', False):
                        break
                    token = result['next_token']
                assert actual == expected, (path, len(actual), len(expected))
            rpc('Mkdir', {'path': '/bulk_rpc_created'})
            rpc('Rename', {'old_path': '/bulk_rpc_created', 'new_path': '/bulk_rpc_moved'})
            rpc('Rmdir', {'path': '/bulk_rpc_moved'})
            print(f'PASS bulk MDS startup and {len(records)} RPC checks')
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait()
            (work / 'rpc.json').write_text(json.dumps(records, indent=2))


if __name__ == '__main__':
    main()
