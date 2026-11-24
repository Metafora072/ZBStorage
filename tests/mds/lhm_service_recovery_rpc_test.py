#!/usr/bin/env python3
"""Exercise checkpoints and SIGKILL recovery through an isolated real MDS server."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--library', type=Path, help='defaults to the bridge under the server build directory')
    parser.add_argument('--work-dir', type=Path, required=True)
    args = parser.parse_args()
    work = args.work_dir.resolve(); work.mkdir(parents=True, exist_ok=False)
    server = work/'mds_server'; shutil.copy2(args.server.resolve(), server)
    library = work/'libzb_lhm_bridge.so'
    source_library = args.library or args.server.resolve().parent/'src/mds/lhm/libzb_lhm_bridge.so'
    shutil.copy2(source_library, library)
    environment = os.environ.copy(); environment['LD_LIBRARY_PATH'] = str(work)
    environment.pop('ZB_LHM_CRASH_AT', None)
    environment.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
    evidence = []; process = None; log = None; count = 0
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0)); port = sock.getsockname()[1]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    store = work/'namespace.meta'

    def start(mode='reopen', interval=0):
        nonlocal process, log, count
        count += 1
        config = work/'mds.conf'
        config.write_text(f'MDS_DB_PATH={work}/control\nNAMESPACE_BACKEND=lhm\n'
            f'LHM_STORE_FILE={store}\nLHM_STORE_MODE={mode}\n'
            f'LHM_CHECKPOINT_INTERVAL_SECONDS={interval}\nMASSTREE_ROOT={work}/templates\n'
            'ENABLE_OPTICAL_ARCHIVE=false\nMASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\n'
            'NODES=node-real-01@127.0.0.1:9100\nDISKS=node-real-01:disk-01\nREPLICA=1\nPG_COUNT=16\n')
        log = (work/f'server-{count}.log').open('w')
        process = subprocess.Popen([str(server), f'--config={config}', f'--port={port}'],
                                   cwd=work, env=environment, stdout=log, stderr=log)
        until = time.monotonic()+30
        while True:
            assert process.poll() is None, Path(log.name).read_text()
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.1): break
            except OSError:
                assert time.monotonic() < until, 'startup timeout'
                time.sleep(.1)
        assert Path(str(store)+'.snapshot').is_file()
        assert Path(str(store)+'.undo').is_file()
        assert str(library) in Path(f'/proc/{process.pid}/maps').read_text(), 'wrong bridge loaded'

    def stop(crash=False):
        nonlocal process
        if crash: process.kill()
        else: process.terminate()
        rc = process.wait(timeout=60); log.close(); process = None
        assert rc == (-signal.SIGKILL if crash else 0), rc

    def rpc(method, body, missing=False):
        req = urllib.request.Request(f'http://127.0.0.1:{port}/zb.rpc.MdsService/{method}',
            data=json.dumps(body).encode(), headers={'Content-Type':'application/json'})
        with opener.open(req, timeout=30) as response: result = json.load(response)
        evidence.append(dict(method=method, request=body, response=result))
        code = result.get('status', {}).get('code', 0)
        assert code in ((2, 'MDS_NOT_FOUND') if missing else (0, 'MDS_OK')), result
        return result

    def checkpoint(manual=True):
        before = Path(log.name).read_text().count('LHM checkpoint completed')
        if manual: process.send_signal(signal.SIGUSR1)
        until = time.monotonic()+30
        while Path(log.name).read_text().count('LHM checkpoint completed') <= before:
            assert process.poll() is None, Path(log.name).read_text()
            assert time.monotonic() < until, 'checkpoint timeout'
            time.sleep(.05)

    try:
        start('create')
        rpc('Mkdir', dict(path='/kept', mode=493))
        rpc('Create', dict(path='/kept/file', mode=420, replica=1))
        attr = rpc('Lookup', dict(path='/kept/file'))['attr']; identifier = attr['inode_id']
        checkpoint()
        rpc('UpdateInodeStat', dict(inode_id=identifier, file_size='12345', version='2', mtime='123'))
        rpc('Rename', dict(old_path='/kept/file', new_path='/kept/renamed'))
        rpc('Mkdir', dict(path='/lost', mode=493))
        stop(crash=True)
        start()
        assert rpc('Lookup', dict(path='/kept/file'))['attr'] == attr
        rpc('Lookup', dict(path='/kept/renamed'), missing=True)
        rpc('Lookup', dict(path='/lost'), missing=True)
        rpc('Rename', dict(old_path='/kept/file', new_path='/kept/renamed'))
        checkpoint(); stop(crash=True)
        start(interval=1)
        assert rpc('Lookup', dict(path='/kept/renamed'))['attr']['inode_id'] == identifier
        rpc('Lookup', dict(path='/kept/file'), missing=True)
        rpc('Mkdir', dict(path='/periodic', mode=493)); checkpoint(manual=False)
        stop(crash=True)
        start()
        rpc('Lookup', dict(path='/periodic'))
        rpc('Rmdir', dict(path='/periodic')); stop(crash=True)
        start()
        rpc('Lookup', dict(path='/periodic')); stop()
        start()
        rpc('Mkdir', dict(path='/failed-window', mode=493))
        temporary = Path(str(store)+'.snapshot.tmp'); temporary.mkdir()
        process.send_signal(signal.SIGUSR1)
        assert process.wait(timeout=30) == 1, 'checkpoint failure must stop MDS with failure status'
        log.close(); process = None; temporary.rmdir()
        start()
        rpc('Lookup', dict(path='/failed-window'), missing=True)
        rpc('Lookup', dict(path='/periodic')); stop()
        (work/'result.json').write_text(json.dumps(dict(
            passed=['manual checkpoint', 'periodic checkpoint', 'automatic SIGKILL recovery',
                    'stable committed inode and attributes', 'rollback create/rename/update/rmdir',
                    'checkpoint failure stops MDS and preserves baseline', 'normal close'],
            rpc_count=len(evidence), server_sha256=hashlib.sha256(server.read_bytes()).hexdigest(),
            library_sha256=hashlib.sha256(library.read_bytes()).hexdigest(), rpc=evidence), indent=2))
        print(f'PASS: {len(evidence)} RPC checks; {work}')
    finally:
        if process is not None:
            process.kill(); process.wait(timeout=30); log.close()


if __name__ == '__main__':
    main()
