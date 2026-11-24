#!/usr/bin/env python3
"""Sample uniformly from a completed mixed import; query serially over native brpc.

Creates sparse manifest indexes once, reuses original prefixes for tail batches,
and checks every returned file attribute against its source manifest. No parallel
queries, retries, filesystem cache dropping, or namespace writes are performed.
"""
import argparse
import bisect
import collections
import csv
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import import_lhm_batches as bulk
import import_lhm_mixed_100yi as mixed


def read(path):
    return json.loads(path.read_text())


def write(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    tmp.replace(path)


def event(out, state, **kw):
    value = dict(time=datetime.datetime.now(datetime.timezone.utc).isoformat(), state=state, **kw)
    write(out / 'progress.json', value)
    print(json.dumps(value, ensure_ascii=False), flush=True)


def identity(path):
    s = path.stat()
    return [s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns]


def indexes(args, plan):
    cache = args.work / 'query-indexes'
    cache.mkdir(exist_ok=True)
    sources = {}
    for b in plan['batches']:
        if b['name'] in sources:
            continue
        compiled = Path(b['template'])
        meta = read(compiled / 'manifest.json')
        source = compiled.parent / ('compiled-input.tsv' if b['name'] == 'Server' else 'manifest.tsv')
        assert meta['header_sha256'] == b['digest'] and meta['files'] == b['files']
        key = dict(source=str(source), identity=identity(source), sha256=meta['source_sha256'], stride=4096)
        idx = cache / (b['name'] + '.tsv')
        stamp = cache / (b['name'] + '.json')
        if not stamp.exists() or read(stamp).get('key') != key or not idx.exists():
            event(args.out, 'building_index', template=b['name'], bytes=source.stat().st_size)
            temporary = idx.with_suffix('.preparing')
            result = subprocess.run([str(args.client), '--index_source=' + str(source),
                                     '--index_output=' + str(temporary), '--stride=4096'],
                                    check=True, text=True, capture_output=True)
            info = json.loads(result.stdout)
            assert info['files'] == b['files'] and info['sha256'] == meta['source_sha256'], info
            assert identity(source) == key['identity'], 'source changed while indexing'
            temporary.replace(idx)
            write(stamp, dict(key=key, info=info, index_sha256=hashlib.sha256(idx.read_bytes()).hexdigest()))
        saved = read(stamp)
        assert hashlib.sha256(idx.read_bytes()).hexdigest() == saved['index_sha256']
        anchors = [tuple(map(int, line.split())) for line in idx.read_text().splitlines()]
        assert len(anchors) == (b['files'] + 4095) // 4096
        assert all(ordinal == i * 4096 for i, (ordinal, offset) in enumerate(anchors))
        sources[b['name']] = dict(source=source, anchors=anchors, key=key, files=b['files'])
    for b in plan['batches']:
        src = sources[b['name']]
        assert b['files'] <= src['files']
        if Path(b['template']).parent != src['source'].parent:
            tail = read(Path(b['template']).parent / 'template.json')
            binding = tail['binding']
            assert binding['rule'] == 'manifest-prefix-through-Nth-file-v1'
            assert binding['files'] == b['files'] and binding['source'] == str(src['source'])
            assert binding['source_sha256'] == src['key']['sha256']
    return sources


def sample(args, plan, sources):
    counts = []; total = 0
    for b in plan['batches']:
        total += b['files']; counts.append(total)
    assert total == plan['total_files'] and 0 < args.count <= total
    numbers = random.Random(args.seed).sample(range(total), args.count)
    rows = []
    for number in numbers:
        batch_index = bisect.bisect_right(counts, number)
        b = plan['batches'][batch_index]
        ordinal = number - (counts[batch_index - 1] if batch_index else 0)
        rows.append(dict(global_file_number=number, batch_index=batch_index,
                         template=b['name'], target=b['target'], ordinal=ordinal))
    # Group nearby ordinals for efficient input reads; keep random query order.
    for name, src in sources.items():
        groups = collections.defaultdict(list)
        for row in rows:
            if row['template'] == name:
                groups[row['ordinal'] // 4096].append(row)
        with src['source'].open('rb') as f:
            for anchor, wanted in sorted(groups.items()):
                ordinal, offset = src['anchors'][anchor]
                f.seek(offset)
                by_ordinal = collections.defaultdict(list)
                for row in wanted:
                    by_ordinal[row['ordinal']].append(row)
                end = max(by_ordinal)
                while ordinal <= end:
                    raw = f.readline()
                    if not raw:
                        raise RuntimeError('manifest ended before sampled ordinal')
                    if not raw.startswith(b'file\t'):
                        continue
                    if ordinal in by_ordinal:
                        kind, relative, size, blocks, blksize, replica = raw.rstrip(b'\n').decode('utf-8').split('\t')
                        for row in by_ordinal[ordinal]:
                            row['path'] = row['target'] + relative
                            row['expected'] = dict(size=int(size), blocks_512=int(blocks),
                                                   blksize=int(blksize), replica=int(replica))
                    ordinal += 1
        assert identity(src['source']) == src['key']['identity']
    assert len({r['path'] for r in rows}) == args.count
    (args.out / 'samples.jsonl').write_text(''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in rows))
    (args.out / 'paths.txt').write_text(''.join(r['path'] + '\n' for r in rows))
    return rows


def stats(values):
    ordered = sorted(values)
    if not ordered:
        return dict(count=0)
    def percentile(p):
        return ordered[max(0, math.ceil(len(ordered) * p) - 1)] / 1e6
    return dict(count=len(values), mean_ms=sum(values) / len(values) / 1e6,
                median_ms=(ordered[(len(ordered)-1)//2] + ordered[len(ordered)//2]) / 2e6,
                p95_ms=percentile(.95), p99_ms=percentile(.99),
                min_ms=ordered[0] / 1e6, max_ms=ordered[-1] / 1e6)


def summarize(args, samples, client_rc):
    results = [json.loads(line) for line in (args.out / 'responses.jsonl').read_text().splitlines()]
    if len(results) != len(samples):
        raise RuntimeError(f'incomplete client output: {len(results)}/{len(samples)}, rc={client_rc}')
    times = collections.defaultdict(list); errors = []; ids = set()
    with (args.out / 'queries.csv').open('w', newline='') as f:
        writer = csv.writer(f); writer.writerow(['sequence', 'template', 'path', 'inode_id', 'rpc_ok', 'attributes_ok', 'elapsed_ms', 'error'])
        for sample, result in zip(samples, results):
            assert sample['path'] == result['path']
            attr = result['response'].get('attr', {})
            correct = result['ok'] and attr.get('type') == 'INODE_FILE'
            for key, expected in sample['expected'].items():
                correct = correct and int(attr.get(key, -1)) == expected
            inode = int(attr.get('inode_id', 0))
            correct = correct and inode > 0 and inode not in ids
            ids.add(inode)
            if correct:
                times['all'].append(result['elapsed_ns']); times[sample['template']].append(result['elapsed_ns'])
            else:
                errors.append(dict(sample=sample, result=result))
            writer.writerow([result['sequence'], sample['template'], sample['path'], str(inode),
                             result['ok'], correct, result['elapsed_ns'] / 1e6, result['rpc_error']])
    write(args.out / 'errors.json', errors)
    summary = dict(seed=args.seed, requested=args.count, responses=len(results),
                   success=args.count-len(errors), failures=len(errors), client_exit_code=client_rc,
                   distribution=dict(collections.Counter(s['template'] for s in samples)),
                   latency={name: stats(v) for name, v in times.items()},
                   scope='native brpc synchronous Lookup round trip; one outstanding request; no retry',
                   cache='root/transport warmed; sampled files not prequeried; OS cache not cleared',
                   latency_population='successful responses with matching template attributes and unique inode IDs',
                   percentile_method='nearest rank; median uses average of middle two values')
    write(args.out / 'summary.json', summary)
    return summary


def run(args):
    args.out.mkdir(parents=True, exist_ok=False)
    write(args.out / 'runner.json', dict(pid=os.getpid(), seed=args.seed, count=args.count))
    with mixed.lock(args.work / 'job.lock'):
        plan = mixed.checked_plan(args.work)
        receipt = bulk.parse_receipt((args.work / 'db/namespace.meta.batch-receipt').read_text())
        assert mixed.next_batch(receipt, plan) == len(plan['batches']), 'import is not complete'
        event(args.out, 'preparing', files=plan['total_files'], count=args.count, seed=args.seed)
        sources = indexes(args, plan)
        samples = sample(args, plan, sources)
        event(args.out, 'samples_ready', distribution=dict(collections.Counter(r['template'] for r in samples)))
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
        runtime = args.work / 'runtime'
        start = time.monotonic()
        with (args.out / 'mds.log').open('w') as log:
            # The helper ties the child lifetime to this controller via PDEATHSIG.
            proc = mixed.launch([runtime / 'mds_server', '--config=' + str(args.work / 'mds.conf'),
                                 '--port=' + str(port)], runtime, stdout=log, stderr=log)
            write(args.out / 'service.json', dict(pid=proc.pid, port=port, config=str(args.work / 'mds.conf')))
            try:
                event(args.out, 'starting_mds', pid=proc.pid, port=port)
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                deadline = time.monotonic() + 1800
                while True:
                    if proc.poll() is not None:
                        raise RuntimeError(f'MDS exited {proc.returncode}; see mds.log')
                    try:
                        req = urllib.request.Request(f'http://127.0.0.1:{port}/zb.rpc.MdsService/Lookup',
                              data=b'{"path":"/"}', headers={'Content-Type': 'application/json'})
                        with opener.open(req, timeout=5) as response:
                            reply = json.load(response)
                        if reply.get('status', {}).get('code', 0) not in (0, 'MDS_OK'):
                            raise RuntimeError(str(reply))
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise RuntimeError('MDS readiness timeout')
                        time.sleep(1)
                startup = time.monotonic() - start
                event(args.out, 'querying', startup_seconds=startup, count=args.count)
                before = time.monotonic()
                with (args.out / 'client.log').open('w') as client_log:
                    client = subprocess.run([str(args.client), '--server=127.0.0.1:' + str(port),
                                             '--paths_file=' + str(args.out / 'paths.txt'),
                                             '--output=' + str(args.out / 'responses.jsonl')],
                                            stdout=client_log, stderr=client_log)
                summary = summarize(args, samples, client.returncode)
                summary.update(mds_startup_seconds=startup, client_wall_seconds=time.monotonic()-before,
                               plan_id=plan['plan_id'], files_in_database=plan['total_files'])
                write(args.out / 'summary.json', summary)
            finally:
                event(args.out, 'closing_mds', pid=proc.pid)
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                # Normal close may publish a full directory snapshot. Do not kill it on a short timeout.
                code = proc.wait()
                write(args.out / 'service-exit.json', dict(returncode=code))
            if code != 0:
                raise RuntimeError(f'MDS close failed: {code}')
            event(args.out, 'complete', success=summary['success'], failures=summary['failures'])
            if summary['failures'] or client.returncode:
                raise RuntimeError('queries completed with failures; inspect errors.json')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work', type=Path, default=Path('/mnt/md0/Projects/ZBStorage/data/mds/mixed-100yi'))
    p.add_argument('--client', type=Path, default=Path(__file__).resolve().parents[1] / 'build/mds_query')
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--count', type=int, default=10000)
    p.add_argument('--seed', type=int, default=20261110)
    args = p.parse_args()
    args.work = args.work.resolve(); args.out = args.out.resolve(); args.client = args.client.resolve()
    try:
        run(args)
    except BaseException as exc:
        if args.out.exists():
            event(args.out, 'failed', error=str(exc))
        raise
