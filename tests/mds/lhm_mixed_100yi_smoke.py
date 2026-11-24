#!/usr/bin/env python3
"""Manual small-scale test of the mixed-plan runner; never touches real templates.

Uses seven tiny templates and 105 files, a real MDS and the production bridge.
Run: python3 tests/mds/lhm_mixed_100yi_smoke.py
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))
import import_lhm_mixed_100yi as mixed
import import_lhm_batches as bulk


def small_args(root):
    args = mixed.parser().parse_args(['run', '--template-root', str(root / 'templates'),
        '--work-dir', str(root / 'work'), '--library', str(REPO / 'build/src/mds/lhm/libzb_lhm_bridge.so'),
        '--mds-server', str(REPO / 'build/mds_server'), '--memory-budget-mib', '32',
        '--server-memory-budget-mib', '32', '--min-free-gib', '0'])

    args.shared_template_index = (root / 'shared-template-index').exists()
    return args


def run_child(root, phase):
    args = small_args(root)
    work = args.work_dir
    work.mkdir(exist_ok=True)
    with mixed.lock(work / 'job.lock'):
        specs = mixed.source_spec(args.template_root, each=17, server_files=3)
        plan, bridge, runtime = mixed.prepare(args, work, specs)
        mixed.checked_plan(work)
        mixed.init_database(work, plan, runtime)
        if phase == 'lost-log':
            write = bulk.AuditLog.write
            def fail_after_commit(self, event, **fields):
                if event == 'batch_committed' and fields['index'] == 4:
                    os._exit(73)  # Durable DB commit, no corresponding audit-log entry.
                write(self, event, **fields)
            bulk.AuditLog.write = fail_after_commit
        elif phase == 'failed-child':
            def fail_before_batch(*unused):
                raise RuntimeError('simulated child failure before commit')
            mixed.execute = fail_before_batch
        mixed.run_batches(args, work, plan, bridge, runtime)


def query_all(root, plan):
    work = root / 'work'
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def rpc(method, body):
        req = urllib.request.Request(f'http://127.0.0.1:{port}/zb.rpc.MdsService/{method}',
            data=json.dumps(body).encode(), headers={'Content-Type': 'application/json'})
        with opener.open(req, timeout=10) as response:
            result = json.load(response)
        assert result.get('status', {}).get('code', 0) in (0, 'MDS_OK'), result
        return result
    with (root / 'query.log').open('w') as log:
        proc = mixed.launch([work / 'runtime/mds_server', f'--config={work / "mds.conf"}',
                             f'--port={port}'], work / 'runtime', stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 30
            while True:
                assert proc.poll() is None, (root / 'query.log').read_text()
                try:
                    listing = rpc('Readdir', dict(path='/', limit=1000))
                    break
                except OSError:
                    assert time.monotonic() < deadline
                    time.sleep(.1)
            assert {e['name'] for e in listing['entries']} == {b['target'][1:] for b in plan['batches']}
            ids = set()
            files = 0
            for b in plan['batches']:
                source = Path(b['template']).parent / 'manifest.tsv'
                for line in source.read_text().splitlines()[1:]:
                    kind, relative, size, blocks, blksize, replica = line.split('\t')
                    path = b['target'] + ('' if relative == '/' else relative)
                    attr = rpc('Lookup', dict(path=path))['attr']
                    inode = int(attr['inode_id'])
                    assert inode not in ids
                    ids.add(inode)
                    if kind == 'file':
                        files += 1
                        assert int(attr['size']) == int(size)
                        assert int(attr['blocks_512']) == int(blocks)
                        assert int(attr['blksize']) == int(blksize)
                        assert int(attr['replica']) == int(replica)
                    assert rpc('Getattr', dict(inode_id=inode))['attr'] == attr
            assert files == 105
            return dict(files=files, unique_inodes=len(ids))
        finally:
            proc.terminate()
            assert proc.wait(timeout=30) == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--child', choices=['lost-log', 'failed-child', 'resume'])
    parser.add_argument('--root', type=Path)
    parser.add_argument('--shared-template-index', action='store_true')
    a = parser.parse_args()
    if a.child:
        run_child(a.root, a.child)
        return
    root = Path(tempfile.mkdtemp(prefix='lhm-mixed-100yi-smoke-'))
    print(f'Test artifacts: {root}', flush=True)
    if a.shared_template_index:
        (root / 'shared-template-index').touch()
    templates = root / 'templates'
    templates.mkdir()
    bridge = bulk.Bridge(REPO / 'build/src/mds/lhm/libzb_lhm_bridge.so')
    for name, count in zip(('Server',) + mixed.NAMES, (3, 5, 6, 7, 8, 9, 10)):
        p = templates / name
        p.mkdir()
        lines = ['type\tpath\tsize\tblocks_512\tblksize\treplica',
                 'dir\t/\t0\t0\t4096\t1', 'dir\t/a\t0\t0\t4096\t1', 'dir\t/b\t0\t0\t4096\t1']
        for i in range(count):
            size = 500000000 + i
            lines.append(f'file\t/{"a" if i < count//2 else "b"}/file-{i}.dat\t{size}\t{(size+511)//512}\t4096\t1')
        (p / 'manifest.tsv').write_text('\n'.join(lines) + '\n')
        mixed.save(p / 'template.json', dict(file_count=count, directory_count_including_root=3))
        bridge.compile_template(p / 'manifest.tsv', p / 'compiled-lhm-v1', 32 << 20)
    commands = [sys.executable, __file__, '--root', str(root), '--child']
    with (root / 'run.log').open('w') as log:
        assert subprocess.run(commands + ['lost-log'], stdout=log, stderr=log).returncode == 73
        plan = mixed.checked_plan(root / 'work')
        store = root / 'work/db/namespace.meta'
        receipt = bridge.status(store)
        assert mixed.next_batch(receipt, plan) == 4
        events = [json.loads(x) for x in (root / 'work/batches.jsonl').read_text().splitlines()]
        assert len([x for x in events if x['event'] == 'batch_committed']) == 3
        assert subprocess.run(commands + ['failed-child'], stdout=log, stderr=log).returncode != 0
        assert bridge.status(store) == receipt
        assert subprocess.run(commands + ['resume'], stdout=log, stderr=log).returncode == 0
        done_receipt = bridge.status(store)
        snapshot = bulk.manifest_digest(Path(str(store) + '.snapshot'))[0]
        assert subprocess.run(commands + ['resume'], stdout=log, stderr=log).returncode == 0
        assert bridge.status(store) == done_receipt
        assert bulk.manifest_digest(Path(str(store) + '.snapshot'))[0] == snapshot
    assert mixed.next_batch(done_receipt, plan) == len(plan['batches'])
    assert mixed.read_json(root / 'work/progress.json')['completed_files'] == 105
    foreign = dict(done_receipt, target='/foreign')
    try:
        mixed.next_batch(foreign, plan)
        raise AssertionError('accepted foreign receipt')
    except RuntimeError:
        pass
    # Verify all source bytes, including bytes beyond the chosen prefix.
    bad = root / 'bad.tsv'
    good = templates / 'WeixingYingxiang/manifest.tsv'
    bad.write_bytes(good.read_bytes() + b'corrupt\n')
    try:
        mixed.prefix_manifest(bad, root / 'bad-prefix.tsv', 1, bulk.manifest_digest(good)[0])
        raise AssertionError('accepted changed source suffix')
    except RuntimeError:
        pass
    evidence = query_all(root, plan)
    assert bridge.status(store) == done_receipt
    evidence.update(shared_template_index=a.shared_template_index, status='passed', batches=len(plan['batches']),
        recovery='lost audit entry after durable commit; failed child; resume; repeated completion no-op',
        rejected='foreign receipt; corrupted source suffix')
    mixed.save(root / 'verification.json', evidence)
    print(json.dumps(evidence, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
