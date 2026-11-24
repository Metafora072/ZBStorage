#!/usr/bin/env python3
"""Manual service Undo tests. Each worker is a new process; fixtures are isolated."""
import argparse
import ctypes as C
import json
import os
from pathlib import Path
import resource
import shutil
import signal
import subprocess
import sys
import tempfile

from lhm_batch_recovery_test import Inode, require, segment_files, digest, assert_snapshot_files


def worker(req):
    lib = C.CDLL(req['library'])
    ptr, text, buf = C.c_void_p, C.c_char_p, C.POINTER(C.c_char)
    for name, args, ret in [
        ('open', [text], ptr), ('reopen', [text, buf, C.c_size_t], ptr),
        ('close_checked', [ptr, buf, C.c_size_t], C.c_int),
        ('checkpoint', [ptr, buf, C.c_size_t], C.c_int),
        ('create', [ptr, text, C.c_int], C.c_int),
        ('lookup_checked', [ptr, text, C.POINTER(Inode)], C.c_int),
        ('update_full', [ptr, C.POINTER(Inode)], C.c_int),
        ('rename', [ptr, text, text], C.c_int), ('remove', [ptr, text], C.c_int),
        ('reserve_checked', [ptr, text, C.c_uint8, C.POINTER(C.c_uint64), C.POINTER(C.c_uint64)], C.c_int),
        ('commit_checked', [ptr, C.c_uint64, C.POINTER(Inode)], C.c_int),
        ('cancel', [ptr, C.c_uint64], C.c_int), ('sync', [ptr], C.c_int),
    ]:
        f = getattr(lib, 'zb_lhm_' + name); f.argtypes = args; f.restype = ret
    error = C.create_string_buffer(4096)
    path = req['store']; encoded = path.encode()
    if req.get('early_fault'):
        os.environ['ZB_LHM_CRASH_AT'] = req['early_fault']
    ctx = lib.zb_lhm_open(encoded) if req['action'] == 'init' else lib.zb_lhm_reopen(encoded, error, len(error))
    if not ctx:
        return dict(opened=False, error=error.value.decode())
    require(Path(path+'.snapshot').exists(), 'open removed checkpoint')
    require(Path(path+'.undo').exists(), 'open omitted undo')

    def checkpoint():
        require(lib.zb_lhm_checkpoint(ctx, error, len(error)) == 0, error.value.decode())

    def create(p, directory=0):
        token, identifier = C.c_uint64(), C.c_uint64()
        kind = 2 if directory else 1
        require(lib.zb_lhm_reserve_checked(ctx, p.encode(), kind, C.byref(token), C.byref(identifier)) == 0, 'reserve: '+p)
        attr = Inode(); attr.inode_id = identifier.value; attr.type = kind
        attr.mode = 493 if directory else 384; attr.nlink = 2 if directory else 1
        attr.version = 1; attr.file_name = p.rsplit('/', 1)[-1].encode(); attr.name_length = len(attr.file_name)
        require(lib.zb_lhm_commit_checked(ctx, token.value, C.byref(attr)) == 0, 'commit: '+p)

    def lookup(p):
        inode = Inode(); rc = lib.zb_lhm_lookup_checked(ctx, p.encode(), C.byref(inode))
        return dict(rc=rc, id=inode.inode_id, size=inode.size)

    action = req['action']
    if action == 'init':
        create('/d', 1); create('/d/original'); create('/victim')
    elif action == 'peek':
        values = {p: lookup(p) for p in req['paths']}
        require(lib.zb_lhm_close_checked(ctx, error, len(error)), error.value.decode())
        return dict(opened=True, values=values)
    elif action == 'busy':
        token, inode = C.c_uint64(), C.c_uint64()
        require(lib.zb_lhm_reserve_checked(ctx, b'/pending', 1, C.byref(token), C.byref(inode)) == 0, 'reserve')
        require(lib.zb_lhm_checkpoint(ctx, error, len(error)) == 6, 'checkpoint accepted pending create')
        require(lib.zb_lhm_cancel(ctx, token.value), 'cancel')
        create('/after-busy'); checkpoint()
    elif action == 'checkpoint_error':
        create('/uncommitted')
        Path(path+'.snapshot.tmp').mkdir()
        require(lib.zb_lhm_checkpoint(ctx, error, len(error)) == 5, 'expected checkpoint error')
        require(not lib.zb_lhm_create(ctx, b'/after-failure', 0), 'failed session still writable')
        Path(path+'.snapshot.tmp').rmdir()
        require(not lib.zb_lhm_close_checked(ctx, error, len(error)), 'failed close falsely succeeded')
        return dict(opened=True)
    elif action in ('write_error', 'data_write_error'):
        inode = Inode()
        require(lib.zb_lhm_lookup_checked(ctx, b'/d/original', C.byref(inode)) == 0, 'lookup')
        inode.size = 777
        signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        limits = resource.getrlimit(resource.RLIMIT_FSIZE)
        resource.setrlimit(resource.RLIMIT_FSIZE, (1024 if action == 'write_error' else 20000, limits[1]))
        lib.zb_lhm_update_full(ctx, C.byref(inode))
        require(not lib.zb_lhm_sync(ctx), 'write failure not propagated')
        resource.setrlimit(resource.RLIMIT_FSIZE, limits)
        require(not lib.zb_lhm_create(ctx, b'/after-failure', 0), 'write failure not sticky')
        require(not lib.zb_lhm_close_checked(ctx, error, len(error)), 'failed close falsely succeeded')
        return dict(opened=True)
    elif action == 'edit':
        if req.get('first_checkpoint'):
            create('/committed'); checkpoint()
        if req.get('fault'):
            os.environ['ZB_LHM_CRASH_AT'] = req['fault']
        inode = Inode()
        require(lib.zb_lhm_lookup_checked(ctx, b'/d/original', C.byref(inode)) == 0, 'lookup')
        inode.size = 98765
        require(lib.zb_lhm_update_full(ctx, C.byref(inode)), 'update')
        if not req.get('attribute_only'):
            require(lib.zb_lhm_rename(ctx, b'/d/original', b'/d/renamed'), 'rename')
            require(lib.zb_lhm_remove(ctx, b'/victim'), 'remove')
            create('/uncommitted')
            for i in range(req.get('directories', 0)):
                create(f'/new-{i}', 1)
                for j in range(3):
                    create(f'/new-{i}/f{j}')
        require(lib.zb_lhm_sync(ctx), 'sync')
        if req.get('checkpoint'):
            checkpoint()
        if req.get('crash'):
            os.kill(os.getpid(), signal.SIGKILL)
    require(lib.zb_lhm_close_checked(ctx, error, len(error)), error.value.decode())
    return dict(opened=True)


def main():
    if '--worker' in sys.argv:
        print(json.dumps(worker(json.load(sys.stdin)))); return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    work = args.output_dir or Path(tempfile.mkdtemp(prefix='lhm-service-recovery-'))
    work.mkdir(exist_ok=True, parents=True)
    library = work/'bridge-under-test.so'; shutil.copy2(args.library.resolve(), library)
    passed = []

    def call(store, action, expect_crash=False, expect_open_failure=False, **kwargs):
        env = os.environ.copy(); env.pop('ZB_LHM_CRASH_AT', None)
        env['ZB_LHM_TEST_SEGMENT_BYTES'] = str(8 * 16384)
        proc = subprocess.run([sys.executable, __file__, '--worker'], input=json.dumps(dict(
            library=str(library.resolve()), store=str(store), action=action, **kwargs)),
            text=True, capture_output=True, env=env, timeout=90)
        if expect_crash:
            require(proc.returncode == -signal.SIGKILL, f'no SIGKILL: {proc.returncode} {proc.stdout} {proc.stderr}')
            return
        require(proc.returncode == 0, proc.stderr + proc.stdout)
        result = json.loads(proc.stdout)
        require(result['opened'] != expect_open_failure, result.get('error', 'unexpected open success'))
        return result

    def fixture(name):
        parent = work/name; parent.mkdir(); store = parent/'namespace.meta'
        call(store, 'init'); return store

    def state(store):
        return {s: digest(p.read_bytes()) for s, p in segment_files(store).items()}

    paths = ['/d/original', '/d/renamed', '/victim', '/uncommitted', '/committed']
    def check(store, committed=False):
        values = call(store, 'peek', paths=paths)['values']
        require(values['/d/original']['rc'] == (1 if committed else 0), 'original mismatch')
        require(values['/d/renamed']['rc'] == (0 if committed else 1), 'rename mismatch')
        require(values['/victim']['rc'] == (1 if committed else 0), 'delete mismatch')
        require(values['/uncommitted']['rc'] == (0 if committed else 1), 'create mismatch')
        if committed:
            require(values['/d/renamed']['size'] == 98765, 'attribute mismatch')
        else:
            require(values['/d/original']['size'] == 0, 'attribute rollback failed')
        assert_snapshot_files(store)
        return values

    def passed_case(name):
        passed.append(name); print('PASS: '+name, flush=True)

    for fault in ['service_started', 'undo_record', 'data_write', 'service_snapshot', 'service_prepared',
                  'service_published', 'service_retired']:
        store = fixture(fault); before = state(store)
        call(store, 'edit', expect_crash=True, checkpoint=True,
             **({'early_fault': fault} if fault == 'service_started' else {'fault': fault}))
        committed = fault in ('service_published', 'service_retired')
        check(store, committed)
        if not committed:
            require(state(store) == before, 'rollback data differs from baseline')
        passed_case(fault)

    store = fixture('multiple-windows')
    call(store, 'edit', expect_crash=True, first_checkpoint=True, **{'crash': True})
    require(check(store)['/committed']['rc'] == 0, 'previous window lost')
    passed_case('previous checkpoint preserved, current window rolled back')

    for action in ['busy', 'checkpoint_error', 'write_error', 'data_write_error']:
        store = fixture(action); before = state(store)
        call(store, action); check(store)
        if action != 'busy':
            require(state(store) == before, 'failed session altered baseline')
        passed_case(action)

    store = fixture('attribute-only'); old = Path(str(store)+'.snapshot').read_bytes()
    call(store, 'edit', attribute_only=True, checkpoint=True)
    require(Path(str(store)+'.snapshot').read_bytes() != old, 'attribute checkpoint fingerprint reused')
    require(call(store, 'peek', paths=['/d/original'])['values']['/d/original']['size'] == 98765, 'attribute lost')
    passed_case('attribute-only checkpoint has a distinct generation')

    for recovery_fault in ['undo_restore', 'undo_segment_removed']:
        store = fixture(recovery_fault); before = state(store)
        call(store, 'edit', expect_crash=True, directories=80, **{'crash': True})
        require(len(segment_files(store)) > len(before), 'fixture did not add segments')
        call(store, 'peek', expect_crash=True, paths=paths, early_fault=recovery_fault)
        check(store); require(state(store) == before, 'interrupted recovery changed baseline')
        passed_case('retry interrupted '+recovery_fault+' across segments')

    store = fixture('damaged-service-journal')
    call(store, 'edit', expect_crash=True, crash=True)
    journal = Path(str(store)+'.undo'); data = bytearray(journal.read_bytes()); data[32] ^= 1
    journal.write_bytes(data); before = state(store)
    call(store, 'peek', paths=paths, expect_open_failure=True)
    require(state(store) == before and journal.read_bytes() == data, 'damaged journal changed on refusal')
    passed_case('corrupt service journal refuses startup without modifying data')

    # Exercise service rollback and structural checkpoint on a real v5 import
    # baseline; the offline receipt must survive rollback and become stale after
    # a service commit, including an attribute-only commit.
    from lhm_compiled_import_test import worker as compiled_worker
    for attribute_only in (False, True):
        store = fixture('incremental-'+str(attribute_only))
        manifest = store.parent/'tiny.tsv'; template = store.parent/'compiled'
        manifest.write_text('type\tpath\tsize\tmtime\ndir\t/\t0\t123\nfile\t/f\t42\t123\n')
        base = dict(library=str(library.resolve()), store=str(store))
        require(compiled_worker(dict(base, action='compile', manifest=str(manifest), template=str(template)))['rc'] == 0, 'compile')
        require(compiled_worker(dict(base, action='compiled_import', target='/imported', batch_id='one',
            template=str(template), header_sha256=digest((template/'header.bin').read_bytes())))['rc'] == 0, 'import')
        receipt = Path(str(store)+'.batch-receipt').read_bytes()
        before = state(store)
        call(store, 'edit', expect_crash=True, crash=True, attribute_only=attribute_only)
        check(store); require(state(store) == before, 'v5 rollback changed data')
        require(compiled_worker(dict(base, action='status'))['rc'] == 0, 'rollback invalidated import receipt')
        call(store, 'edit', checkpoint=True, attribute_only=attribute_only)
        require(call(store, 'peek', paths=['/imported/f'])['values']['/imported/f']['size'] == 42, 'imported data lost')
        require(Path(str(store)+'.batch-receipt').read_bytes() == receipt, 'service rewrote offline receipt')
        require(compiled_worker(dict(base, action='status'))['rc'] != 0, 'service commit accepted stale offline receipt')
        passed_case('incremental import, service rollback/commit and receipt isolation; attribute-only='+str(attribute_only))

    (work/'result.json').write_text(json.dumps(dict(passed=passed, library_sha256=digest(library.read_bytes())), indent=2))
    print(f'PASS: {len(passed)} service recovery checks; {work}')


if __name__ == '__main__':
    main()
