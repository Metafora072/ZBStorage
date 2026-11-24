#!/usr/bin/env python3
"""Prepare and import exactly 10 billion files from seven LHM templates.

plan is read-only. prepare builds six prefix templates; run initializes one
isolated MDS database and imports a single immutable, recoverable mixed plan.
"""
import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

import import_lhm_batches as bulk

NAMES = ('WeixingYingxiang', 'DuocengYaogan', 'ShixuYaogan',
         'TijianShuju', 'Monkey', 'ZnPhoto')
EACH_FILES = 1_650_000_000
SERVER_FILES = 100_000_000
SCRIPT = Path(__file__).resolve()
HELD_FDS = []
require = bulk.require


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def identity(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def read_json(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('wb') as stream:
        stream.write(canonical(value) + b'\n')
        stream.flush()
        os.fsync(stream.fileno())
    temp.replace(path)
    bulk.sync_directory(path.parent)


@contextlib.contextmanager
def lock(path):
    import fcntl
    with Path(path).open('a+b') as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f'another runner or child process holds {path}')
        HELD_FDS.append(stream.fileno())
        try:
            yield
        finally:
            HELD_FDS.remove(stream.fileno())


CHILD_GUARD = '''import ctypes, os, signal, sys
if ctypes.CDLL(None).prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
    os._exit(125)
if os.getppid() != int(sys.argv[1]):
    os.kill(os.getpid(), signal.SIGTERM)
os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
'''


def launch(command, runtime, **kwargs):
    env = os.environ.copy()
    env['LD_LIBRARY_PATH'] = str(runtime)
    env.pop('ZB_LHM_CRASH_AT', None)
    env.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
    # Use an exec guard instead of preexec_fn: the loaded native bridge may
    # already have worker threads. Locks survive until the child has exited.
    guarded = [sys.executable, '-c', CHILD_GUARD, str(os.getpid())] + list(map(str, command))
    return subprocess.Popen(guarded, env=env, stdin=subprocess.DEVNULL,
                            pass_fds=tuple(HELD_FDS), **kwargs)


def execute(command, runtime):
    proc = launch(command, runtime)
    try:
        code = proc.wait()
    except BaseException:
        proc.terminate()
        proc.wait()
        raise
    require(code == 0, f'child exited {code}; restart the same command after resolving the cause')


def source_spec(root, each=EACH_FILES, server_files=SERVER_FILES):
    """Pure planning; smaller quotas are used only by the manual test driver."""
    specs = []
    for name in ('Server',) + NAMES:
        directory = root / name
        config = read_json(directory / 'template.json')
        count = config['file_count']
        require(isinstance(count, int) and count > 0, f'invalid file count: {name}')
        if name == 'Server':
            require(count == server_files, 'Server must contain exactly the requested 100 million files')
            full, tail = 1, 0
        else:
            full, tail = divmod(each, count)
        specs.append(dict(name=name, directory=str(directory), files=count,
                          directories=config['directory_count_including_root'],
                          full_copies=full, tail_files=tail,
                          quota=server_files if name == 'Server' else each))
    return specs


def print_plan(specs):
    print('Template               files/copy    full copies       tail files       total files')
    for s in specs:
        print(f"{s['name']:22} {s['files']:13,} {s['full_copies']:14} {s['tail_files']:16,} {s['quota']:17,}")
    print(f"Total files: {sum(s['quota'] for s in specs):,}; batches: "
          f"{sum(s['full_copies'] + bool(s['tail_files']) for s in specs)}")


def pin_runtime(args, work):
    runtime = work / 'runtime'
    runtime.mkdir(exist_ok=True)
    sibling = SCRIPT.parent
    build = sibling.parent / 'build'
    default_lib = sibling / 'libzb_lhm_bridge.so'
    default_mds = sibling / 'mds_server'
    sources = {
        SCRIPT.name: SCRIPT,
        'import_lhm_batches.py': Path(bulk.__file__).resolve(),
        'libzb_lhm_bridge.so': args.library or (default_lib if default_lib.is_file() else build / 'src/mds/lhm/libzb_lhm_bridge.so'),
        'mds_server': args.mds_server or (default_mds if default_mds.is_file() else build / 'mds_server'),
    }
    hashes = {}
    for name, source in sources.items():
        source = source.resolve(strict=True)
        digest, _ = bulk.manifest_digest(source)
        dest = runtime / name
        if dest.exists():
            require(bulk.manifest_digest(dest)[0] == digest,
                    f'runtime changed: {name}; resume using {runtime / SCRIPT.name} and its pinned library/server')
        else:
            temp = runtime / (name + '.tmp')
            shutil.copy2(source, temp)
            require(bulk.manifest_digest(temp)[0] == digest, f'copy changed: {source}')
            with temp.open('rb') as stream:
                os.fsync(stream.fileno())
            temp.replace(dest)
            bulk.sync_directory(runtime)
        hashes[name] = digest
    return runtime, hashes


def compiled_descriptor(directory, bridge, files, directories=None):
    info = bridge.compiled_info(directory)
    require(info['files'] == files, f'compiled file count differs: {directory}')
    require(directories is None or info['directories'] == directories,
            f'compiled directory count differs: {directory}')
    return dict(template=str(directory), files=info['files'], directories=info['directories'],
                digest=bulk.manifest_digest(directory / 'header.bin')[0])


def prefix_manifest(source, output, count, expected_digest):
    """Copy a topological grouped manifest prefix, then verify the entire source.

    Retain every directory encountered before the last selected file. A prefix
    of these six grouped manifests preserves all parent declarations and name
    uniqueness. File attributes are copied verbatim, never regenerated.
    """
    before = bulk.file_identity(source)
    full_hash, prefix_hash = hashlib.sha256(), hashlib.sha256()
    files = directories = 0
    with source.open('rb', buffering=8 << 20) as inp, output.open('xb', buffering=8 << 20) as out:
        header = inp.readline()
        require(header == b'type\tpath\tsize\tblocks_512\tblksize\treplica\n', 'unexpected TSV header')
        out.write(header)
        full_hash.update(header)
        prefix_hash.update(header)
        while files < count:
            line = inp.readline()
            require(line and line.endswith(b'\n'), 'manifest ends before requested tail count')
            kind = line.split(b'\t', 1)[0]
            require(kind in (b'file', b'dir'), 'unexpected record in prefix')
            files += kind == b'file'
            directories += kind == b'dir'
            out.write(line)
            full_hash.update(line)
            prefix_hash.update(line)
        for block in iter(lambda: inp.read(8 << 20), b''):
            full_hash.update(block)
        require(full_hash.hexdigest() == expected_digest, f'source SHA256 mismatch: {source}')
        require(bulk.file_identity(source) == before, f'source changed: {source}')
        out.flush()
        os.fsync(out.fileno())
    return dict(files=files, directories=directories, manifest_sha256=prefix_hash.hexdigest(),
                manifest_bytes=output.stat().st_size, source_sha256=full_hash.hexdigest())


def prepare_tail(spec, bridge, runtime, args):
    parent = Path(spec['directory'])
    variants = parent / 'variants'
    variants.mkdir(exist_ok=True)
    bulk.sync_directory(parent)
    directory = variants / f"mixed-100yi-prefix-{spec['tail_files']}"
    published = read_json(parent / 'compiled-lhm-v1/manifest.json')['source_sha256']
    binding = dict(source=str(parent / 'manifest.tsv'), source_sha256=published,
                   files=spec['tail_files'], rule='manifest-prefix-through-Nth-file-v1')
    with lock(variants / (directory.name + '.lock')):
        if not directory.exists():
            staging = variants / (directory.name + '.preparing')
            if staging.exists():
                require(not staging.is_symlink(), 'unsafe staging symlink')
                # This exact reserved directory belongs to this deterministic variant.
                shutil.rmtree(staging)
            staging.mkdir()
            free = shutil.disk_usage(variants).free
            needed = (parent / 'manifest.tsv').stat().st_size
            needed += sum(p.stat().st_size for p in (parent / 'compiled-lhm-v1').iterdir() if p.is_file())
            require(free > needed + args.min_free_gib * (1 << 30), 'insufficient free space to prepare tail')
            print(f"Preparing {spec['name']} prefix: {spec['tail_files']:,} files", flush=True)
            metadata = prefix_manifest(parent / 'manifest.tsv', staging / 'manifest.tsv',
                                       spec['tail_files'], published)
            save(staging / 'template.json', dict(binding=binding, **metadata))
            (staging / 'README.md').write_text(
                f"# {spec['name']} 百亿混合任务尾批\n\n"
                f"保留原清单前 {metadata['files']:,} 个文件及此前声明的目录，共 {metadata['directories']:,} 个目录。"
                "路径、文件名和属性原样复制；最后的副本、叶子目录可以不完整，配对文件可能在截断边界处不完整。"
                "原始模板保留不变。manifest.tsv 用于编译，compiled-lhm-v1 用于批次导入。\n")
            with (staging / 'README.md').open('rb') as stream:
                os.fsync(stream.fileno())
            bulk.sync_directory(staging)
            staging.rename(directory)
            bulk.sync_directory(variants)
        metadata = read_json(directory / 'template.json')
        require(metadata['binding'] == binding, f'tail belongs to another source: {directory}')
        output = directory / 'compiled-lhm-v1'
        if not output.exists():
            require(bulk.manifest_digest(directory / 'manifest.tsv')[0] == metadata['manifest_sha256'],
                    'tail TSV changed before compilation')
            for stale in directory.glob('compiled-lhm-v1.tmp.*'):
                require(stale.is_dir() and not stale.is_symlink(), 'unexpected compiler temporary path')
                shutil.rmtree(stale)
            execute([sys.executable, runtime / 'import_lhm_batches.py', 'compile',
                     '--manifest', directory / 'manifest.tsv', '--output', output,
                     '--library', runtime / 'libzb_lhm_bridge.so',
                     '--memory-budget-mib', args.memory_budget_mib], runtime)
        compiled = read_json(output / 'manifest.json')
        require(compiled['source_sha256'] == metadata['manifest_sha256'], 'tail compiled from a different TSV')
        return compiled_descriptor(output, bridge, metadata['files'], metadata['directories'])


def prepare(args, work, specs):
    runtime, hashes = pin_runtime(args, work)
    request = dict(format=1, store=str(work / 'db/namespace.meta'), specs=specs, runtime=hashes)
    if getattr(args, 'shared_template_index', False):
        request['index_mode'] = 'shared-readonly-v6'
    request_path = work / 'request.json'
    if request_path.exists():
        require(read_json(request_path) == request, 'work directory belongs to a different request/runtime')
    else:
        require(not (work / 'db').exists(), 'refusing to adopt an existing database')
        save(request_path, request)
    if not (work / 'progress.json').exists():
        save(work / 'progress.json', dict(state='preparing', completed_batches=0, completed_files=0,
             total_files=sum(s['quota'] for s in specs), pid=os.getpid()))
    bridge = bulk.Bridge(runtime / 'libzb_lhm_bridge.so')
    batches = []
    for spec in specs:
        full = compiled_descriptor(Path(spec['directory']) / 'compiled-lhm-v1', bridge,
                                   spec['files'], spec['directories'])
        for copy in range(1, spec['full_copies'] + 1):
            batches.append(dict(name=spec['name'], target=f"/{spec['name']}-{copy:06d}", **full))
        if spec['tail_files']:
            tail = prepare_tail(spec, bridge, runtime, args)
            batches.append(dict(name=spec['name'], target=f"/{spec['name']}-{spec['full_copies']+1:06d}", **tail))
    plan = dict(request=request, batches=batches, total_files=sum(b['files'] for b in batches),
                total_directories=1 + sum(b['directories'] for b in batches))
    require(plan['total_files'] == sum(s['quota'] for s in specs), 'incorrect quota allocation')
    plan['plan_id'] = identity(plan)
    for index, batch in enumerate(batches):
        batch['batch_id'] = bulk.batch_key(plan['plan_id'], index + 1)
    path = work / 'plan.json'
    if path.exists():
        require(read_json(path) == plan, 'immutable plan changed; use the original inputs')
    else:
        save(path, plan)
    if read_json(work / 'progress.json')['state'] == 'preparing':
        save(work / 'progress.json', dict(state='prepared', completed_batches=0,
             completed_files=0, total_files=plan['total_files'], plan_id=plan['plan_id']))
    return plan, bridge, runtime


def checked_plan(work):
    plan = read_json(work / 'plan.json')
    original = dict(plan)
    plan_id = original.pop('plan_id')
    original['batches'] = [{k: v for k, v in b.items() if k != 'batch_id'} for b in plan['batches']]
    require(identity(original) == plan_id, 'plan checksum mismatch')
    for i, b in enumerate(plan['batches']):
        require(b['batch_id'] == bulk.batch_key(plan_id, i + 1), 'invalid batch identity')
    require(plan['request']['store'] == str(work / 'db/namespace.meta'), 'database location changed')
    for name, digest in plan['request']['runtime'].items():
        require(bulk.manifest_digest(work / 'runtime' / name)[0] == digest, f'pinned runtime changed: {name}')
    return plan


def next_batch(receipt, plan):
    if receipt is None:
        return 0
    for index, b in enumerate(plan['batches']):
        if receipt['batch_id'] != b['batch_id']:
            continue
        require(receipt['target'] == b['target'] and receipt['manifest_sha256'] == b['digest']
                and receipt['files'] == b['files'] and receipt['directories'] == b['directories']
                and receipt['processed_records'] == b['files'] + b['directories'],
                'checkpoint receipt differs from this mixed plan')
        return index + 1
    raise RuntimeError('database receipt belongs to another plan; refusing to infer progress from logs')


def config_text(db, mode):
    return (f'MDS_DB_PATH={db}/control\nNAMESPACE_BACKEND=lhm\nLHM_STORE_FILE={db}/namespace.meta\n'
            f'LHM_STORE_MODE={mode}\nLHM_CHECKPOINT_INTERVAL_SECONDS=0\n'
            f'MASSTREE_ROOT={db}/control_templates\nENABLE_OPTICAL_ARCHIVE=false\n'
            'MASSTREE_PRELOAD_ALL_SPARSE_ON_START=false\nNODES=node-real-01@127.0.0.1:9100\n'
            'DISKS=node-real-01:disk-01\nREPLICA=1\nPG_COUNT=16\n')


def init_database(work, plan, runtime):
    db = work / 'db'
    require(not db.is_symlink(), 'database directory must not be a symlink')
    owner = {'plan_id': plan['plan_id']}
    if (db / '.mixed-import-owner.json').exists():
        require(read_json(db / '.mixed-import-owner.json') == {'plan_id': plan['plan_id']},
                'database ownership marker differs from plan')
    else:
        if db.exists():
            require(read_json(db / '.initialization-owner.json') == owner,
                    'refusing to adopt an unowned database')
            require(not (db / 'namespace.meta.batch-receipt').exists()
                    and not list(db.glob('namespace.meta.index-*')),
                    'incomplete initialization contains imported data; refusing to replace it')
            abandoned = Path(tempfile.mkdtemp(prefix='failed-init-', dir=work))
            abandoned.rmdir()
            db.rename(abandoned)
            bulk.sync_directory(work)
        # Publish an owned EMPTY directory first. MDS creates the real control
        # store at its final path: its absolute LHM path binding must never move.
        stage = Path(tempfile.mkdtemp(prefix='init-', dir=work))
        save(stage / '.initialization-owner.json', owner)
        stage.rename(db)
        bulk.sync_directory(work)
        config = work / 'initialization.conf'
        config.write_text(config_text(db, 'create'))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        with (work / 'initialization.log').open('ab') as log:
            proc = launch([runtime / 'mds_server', f'--config={config}', f'--port={port}'],
                          runtime, stdout=log, stderr=log)
            try:
                opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                deadline = time.monotonic() + 120
                while True:
                    require(proc.poll() is None, f'MDS initialization failed; see {work / "initialization.log"}')
                    try:
                        req = urllib.request.Request(f'http://127.0.0.1:{port}/zb.rpc.MdsService/Readdir',
                            data=b'{"path":"/","limit":1}', headers={'Content-Type': 'application/json'})
                        with opener.open(req, timeout=1) as response:
                            result = json.load(response)
                        require(result.get('status', {}).get('code', 0) in (0, 'MDS_OK')
                                and not result.get('entries') and not result.get('has_more'),
                                'initialized namespace is not empty')
                        break
                    except (OSError, TimeoutError):
                        require(time.monotonic() < deadline, f'MDS initialization timed out: {db}')
                        time.sleep(.2)
            finally:
                if proc.poll() is None:
                    proc.terminate()
                try:
                    code = proc.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    raise RuntimeError(f'MDS failed to close; incomplete initialization retained: {db}')
            require(code == 0, 'MDS initialization did not close cleanly')
        bridge = bulk.Bridge(runtime / 'libzb_lhm_bridge.so')
        require(bridge.status(db / 'namespace.meta') is None, 'initial database has a batch receipt')
        save(db / '.mixed-import-owner.json', owner)
    (work / 'mds.conf').write_text(config_text(db, 'reopen'))


def batch_estimate(batch):
    p = Path(batch['template'])
    return sum((p / name).stat().st_size for name in ('inode.pages', 'heads.pages', 'bucket.pages')) + 4 * (p / 'routes.bin').stat().st_size


def worker(args, work):
    plan = checked_plan(work)
    args.shared_template_index = plan['request'].get('index_mode') == 'shared-readonly-v6'
    bridge = bulk.Bridge(work / 'runtime/libzb_lhm_bridge.so')
    store = work / 'db/namespace.meta'
    index = args.index
    require(index is not None and 0 <= index < len(plan['batches']), 'invalid worker batch index')
    batch = plan['batches'][index]
    # Never permit an out-of-order child invocation.
    require(next_batch(bridge.status(store), plan) in (index, index + 1), 'worker is not the next batch')
    current = compiled_descriptor(Path(batch['template']), bridge, batch['files'], batch['directories'])
    require(current['digest'] == batch['digest'], 'compiled template changed')
    result, already = bridge.import_compiled_batch(store, batch['target'], Path(batch['template']),
                                                  batch['batch_id'], batch['digest'], args)
    require(result['files'] == batch['files'] and result['directories'] == batch['directories'],
            'committed counts differ from plan')
    save(work / 'last-batch-result.json', dict(index=index, already_committed=already, **result))


def run_batches(args, work, plan, bridge, runtime):
    store = work / 'db/namespace.meta'
    log = bulk.AuditLog(work / 'batches.jsonl')
    with bulk.RunLock(store) as store_lock:
        HELD_FDS.append(store_lock.stream.fileno())
        try:
            bridge.recover(store)
            start = next_batch(bridge.status(store), plan)
            done = sum(b['files'] for b in plan['batches'][:start])
            remaining = sum(batch_estimate(b) for b in plan['batches'][start:])
            if start < len(plan['batches']):
                require(shutil.disk_usage(store.parent).free >= remaining + args.min_free_gib * (1 << 30),
                        f'insufficient space for remaining plan: estimate {remaining/(1<<30):.1f} GiB plus reserve')
            for index in range(start, len(plan['batches'])):
                batch = plan['batches'][index]
                required = batch_estimate(batch) + 2 * Path(str(store) + '.snapshot').stat().st_size
                require(shutil.disk_usage(store.parent).free >= required + args.min_free_gib * (1 << 30),
                        'insufficient space for next batch plus reserve')
                save(work / 'progress.json', dict(state='running', completed_batches=index,
                     completed_files=done, total_files=plan['total_files'], next_target=batch['target'], pid=os.getpid()))
                log.write('batch_start', index=index + 1, target=batch['target'], files=batch['files'])
                started = time.monotonic()
                budget = args.server_memory_budget_mib if batch['name'] == 'Server' else args.memory_budget_mib
                execute([sys.executable, runtime / SCRIPT.name, '_batch', '--work-dir', work,
                         '--index', index, '--memory-budget-mib', budget], runtime)
                require(next_batch(bridge.status(store), plan) == index + 1, 'missing durable batch receipt')
                done += batch['files']
                log.write('batch_committed', index=index + 1, target=batch['target'], completed_files=done,
                          elapsed_seconds=round(time.monotonic() - started, 3))
            require(done == plan['total_files'], 'final total differs from requested quota')
            save(work / 'progress.json', dict(state='complete', completed_batches=len(plan['batches']),
                 completed_files=done, total_files=done, plan_id=plan['plan_id']))
            log.write('complete', files=done, batches=len(plan['batches']))
        finally:
            HELD_FDS.remove(store_lock.stream.fileno())


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['plan', 'prepare', 'run', 'status', '_batch'])
    p.add_argument('--template-root', type=Path, default=Path('/mnt/md0/Projects/ZBStorage/templates'))
    p.add_argument('--work-dir', type=Path, default=Path('/mnt/md0/Projects/ZBStorage/data/mds/mixed-100yi'))
    p.add_argument('--library', type=Path)
    p.add_argument('--mds-server', type=Path)
    p.add_argument('--shared-template-index', action='store_true', help='create a read-only database with shared template routing')
    p.add_argument('--memory-budget-mib', type=bulk.positive, default=2048)
    p.add_argument('--server-memory-budget-mib', type=bulk.positive, default=65536)
    p.add_argument('--min-free-gib', type=bulk.nonnegative_float, default=200)
    p.add_argument('--index', type=int, help=argparse.SUPPRESS)
    return p


def main():
    args = parser().parse_args()
    os.environ.pop('ZB_LHM_CRASH_AT', None)
    os.environ.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
    work = args.work_dir.resolve()
    require(args.memory_budget_mib >= 32 and args.server_memory_budget_mib >= 32, 'budgets must be >=32 MiB')
    require(max(args.memory_budget_mib, args.server_memory_budget_mib) <= ((1 << 64) - 1) >> 20,
            'memory budget is too large')
    if args.command == '_batch':
        worker(args, work)
        return
    if args.command == 'status':
        value = read_json(work / 'progress.json') if (work / 'progress.json').exists() else {'state': 'not_started'}
        active = False
        if work.exists():
            try:
                with lock(work / 'job.lock'):
                    pass
            except RuntimeError:
                active = True
        print(json.dumps(dict(runner_active=active, saved_progress=value,
              note='progress is advisory; run recovers and reads the authoritative database receipt'), ensure_ascii=False, indent=2))
        return
    specs = source_spec(args.template_root.resolve())
    print_plan(specs)
    if args.command == 'plan':
        return
    work.mkdir(parents=True, exist_ok=True)
    bulk.sync_directory(work.parent)
    with lock(work / 'job.lock'):
        plan, bridge, runtime = prepare(args, work, specs)
        checked_plan(work)
        if args.command == 'prepare':
            print(f'Ready: {work / "plan.json"}. Database import has not started.', flush=True)
            return
        init_database(work, plan, runtime)
        run_batches(args, work, plan, bridge, runtime)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted; rerun the same command to recover the current batch.', file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(1)
