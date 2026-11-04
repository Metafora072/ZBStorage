#!/usr/bin/env python3
"""Normal-close/reopen and streaming append over the real MDS RPC surface.

Input: existing functional fixture manifest. No namespace dataset generation.
"""
import argparse
import json
import pathlib
import socket
import subprocess
import time
import urllib.request


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--build', type=pathlib.Path, required=True)
    p.add_argument('--work-dir', type=pathlib.Path, required=True)
    p.add_argument('--manifest', type=pathlib.Path, required=True)
    args = p.parse_args()
    work = args.work_dir.resolve()
    work.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    evidence = []

    def rpc(method, body):
        req = urllib.request.Request(
            f'http://127.0.0.1:{port}/zb.rpc.MdsService/{method}',
            data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
        with opener.open(req, timeout=60) as response:
            result = json.load(response)
        evidence.append({'method': method, 'request': body, 'response': result})
        assert result.get('status', {}).get('code', 0) in (0, 'MDS_OK'), result
        return result

    def serve(mode, action):
        config = work / 'mds.conf'
        config.write_text(
            f'MDS_DB_PATH={work}/control\nNAMESPACE_BACKEND=lhm\n'
            f'LHM_STORE_FILE={work}/namespace.meta\nLHM_STORE_MODE={mode}\n'
            f'MASSTREE_ROOT={work}/templates\n'
            'ENABLE_OPTICAL_ARCHIVE=false\nMASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\n'
            'NODES=node-real-01@127.0.0.1:9100\nDISKS=node-real-01:disk-01\n'
            'REPLICA=1\nPG_COUNT=16\n')
        with (work / f'{mode}.log').open('w') as log:
            proc = subprocess.Popen([str(args.build.resolve() / 'mds_server'),
                                     f'--config={config}', f'--port={port}'],
                                    cwd=work, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while True:
                    assert proc.poll() is None, f'MDS exited: {log.name}'
                    try:
                        with socket.create_connection(('127.0.0.1', port), timeout=.2):
                            break
                    except OSError:
                        assert time.monotonic() < deadline, 'startup timeout'
                        time.sleep(.1)
                action()
            finally:
                proc.terminate()
                try:
                    code = proc.wait(timeout=60)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise
            assert code == 0, f'normal close failed: {code}; {log.name}'
            assert (work / 'namespace.meta.snapshot').is_file()

    saved = {}

    def create():
        result = rpc('ImportNamespace', {'target_path': '/first',
            'manifest_path': str(args.manifest.resolve()), 'memory_budget_bytes': str(1 << 20)})
        assert int(result['files']) == 600
        attr = rpc('Lookup', {'path': '/first/d0/f599'})['attr']
        assert int(attr['size']) == 1599 and int(attr['uid']) == 42
        saved.update(attr)

    def reopen():
        attr = rpc('Getattr', {'inode_id': saved['inode_id']})['attr']
        assert attr == saved
        rpc('ImportNamespace', {'target_path': '/second',
            'manifest_path': str(args.manifest.resolve()), 'memory_budget_bytes': str(1 << 20)})
        new = rpc('Lookup', {'path': '/second/d0/f599'})['attr']
        assert new['inode_id'] != saved['inode_id'] and int(new['size']) == 1599
        page = rpc('Readdir', {'path': '/', 'limit': 1})
        assert page.get('has_more')
        rpc('Rename', {'old_path': '/first', 'new_path': '/moved'})

    serve('create', create)
    serve('reopen', reopen)
    (work / 'rpc.json').write_text(json.dumps(evidence, ensure_ascii=False, indent=2))
    print('PASS: real MDS streaming RPC, normal close, matching-control reopen, stable attributes/IDs, append, paging and rename')


if __name__ == '__main__':
    main()
