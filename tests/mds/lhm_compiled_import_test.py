#!/usr/bin/env python3
"""Compiled LHM template correctness, append, CLI and crash recovery checks.

Run manually against a BUILD_TESTING=ON library. --basic-only also supports a
production library. Every database and compiled bundle is an isolated fixture.
"""

import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

from lhm_batch_recovery_test import Inode, ImportResult, Suite as RecoverySuite
from lhm_batch_recovery_test import OK, MISSING, digest, require, worker as legacy_worker


def worker(request):
    action = request['action']
    if action not in ('compile', 'compiled_import', 'info', 'peek', 'many_compiled', 'normal_create'):
        return legacy_worker(request)
    lib = C.CDLL(request['library'])
    text, buffer, size = C.c_char_p, C.POINTER(C.c_char), C.c_size_t
    bindings = {
        'zb_lhm_compile_template': ([text, text, C.c_uint64, buffer, size], C.c_int),
        'zb_lhm_compiled_info': ([text, C.POINTER(ImportResult), buffer, size], C.c_int),
        'zb_lhm_import_compiled_batch': ([text, text, text, text, text, C.c_uint64,
            C.POINTER(ImportResult), C.POINTER(C.c_int), buffer, size], C.c_int),
        'zb_lhm_reopen': ([text, buffer, size], C.c_void_p),
        'zb_lhm_close_checked': ([C.c_void_p, buffer, size], C.c_int),
        'zb_lhm_lookup_checked': ([C.c_void_p, text, C.POINTER(Inode)], C.c_int),
        'zb_lhm_reserve_checked': ([C.c_void_p, text, C.c_uint8,
            C.POINTER(C.c_uint64), C.POINTER(C.c_uint64)], C.c_int),
        'zb_lhm_commit_checked': ([C.c_void_p, C.c_uint64, C.POINTER(Inode)], C.c_int),
    }
    for name, (arguments, returns) in bindings.items():
        function = getattr(lib, name)
        function.argtypes, function.restype = arguments, returns
    error = C.create_string_buffer(8192)
    if action == 'normal_create':
        context = lib.zb_lhm_reopen(os.fsencode(request['store']), error, len(error))
        require(context, 'normal reopen failed: ' + error.value.decode())
        identifiers = []
        for kind, path in request['entries']:
            token, identifier = C.c_uint64(), C.c_uint64()
            inode = Inode()
            inode.type = 2 if kind == 'dir' else 1
            require(lib.zb_lhm_reserve_checked(context, path.encode(), inode.type,
                C.byref(token), C.byref(identifier)) == OK, 'normal reserve failed: ' + path)
            inode.inode_id, inode.size, inode.mode = identifier.value, 735, 493 if inode.type == 2 else 384
            inode.nlink, inode.version = 2 if inode.type == 2 else 1, 1
            inode.atime = inode.mtime = inode.ctime = 321
            inode.file_name = path.rsplit('/', 1)[-1].encode()
            inode.name_length = len(inode.file_name)
            require(lib.zb_lhm_commit_checked(context, token.value, C.byref(inode)) == OK,
                    'normal commit failed: ' + path)
            identifiers.append(identifier.value)
        require(lib.zb_lhm_close_checked(context, error, len(error)),
                'normal close failed: ' + error.value.decode())
        return dict(rc=OK, identifiers=identifiers)
    if action == 'compile':
        rc = lib.zb_lhm_compile_template(os.fsencode(request['manifest']),
            os.fsencode(request['template']), request.get('budget', 64 << 20), error, len(error))
        return dict(rc=rc, error=error.value.decode())
    if action == 'info':
        result = ImportResult()
        rc = lib.zb_lhm_compiled_info(os.fsencode(request['template']), C.byref(result), error, len(error))
        return dict(rc=rc, error=error.value.decode(),
                    result={name: getattr(result, name) for name, _ in result._fields_})
    if action == 'compiled_import':
        result, already = ImportResult(), C.c_int(-1)
        rc = lib.zb_lhm_import_compiled_batch(os.fsencode(request['store']), request['target'].encode(),
            os.fsencode(request['template']), request['batch_id'].encode(), request['header_sha256'].encode(),
            request.get('budget', 32 << 20), C.byref(result), C.byref(already), error, len(error))
        return dict(rc=rc, error=error.value.decode(), already=already.value,
                    result={name: getattr(result, name) for name, _ in result._fields_})
    if action == 'peek':
        context = lib.zb_lhm_reopen(os.fsencode(request['store']), error, len(error))
        if not context:
            return dict(opened=False, error=error.value.decode())
        values = {}
        for path in request.get('paths', []):
            inode = Inode()
            rc = lib.zb_lhm_lookup_checked(context, path.encode(), C.byref(inode))
            values[path] = dict(rc=rc)
            if rc == OK:
                for name, _ in inode._fields_:
                    value = getattr(inode, name)
                    values[path][name] = value.decode('utf-8') if isinstance(value, bytes) else value
        require(lib.zb_lhm_close_checked(context, error, len(error)),
                'inspection close failed: ' + error.value.decode())
        return dict(opened=True, values=values)
    previous = dict(request['baseline'])
    identifiers = {value['inode_id'] for value in previous.values()}
    for index in range(request['batches']):
        target = f'/copy-{index:03d}'
        batch = dict(request, action='compiled_import', target=target, batch_id=f'compiled-{index:03d}')
        imported = worker(batch)
        require(imported['rc'] == OK and imported['already'] == 0,
                'same-process append failed: ' + imported['error'])
        require(worker(batch)['already'] == 1, 'same-process repeat was not idempotent')
        new_paths = [target + suffix for suffix in request['suffixes']]
        snapshot = Path(request['store'] + '.snapshot').read_bytes()
        peek = worker(dict(request, action='peek', paths=list(previous) + new_paths))
        require(peek['opened'], 'same-process reopen failed')
        values = peek['values']
        require(all(values[path] == value for path, value in previous.items()),
                'same-process append changed old inode attributes')
        for path in new_paths:
            value = values[path]
            require(value['rc'] == OK, 'same-process new path missing: ' + path)
            require(value['inode_id'] not in identifiers, 'allocator reused an inode ID: ' + path)
            identifiers.add(value['inode_id'])
        require(Path(request['store'] + '.snapshot').read_bytes() == snapshot,
                'inspection changed snapshot fingerprint')
        previous = values
    return dict(rc=OK, batches=request['batches'], unique_inodes=len(identifiers))


class Suite(RecoverySuite):
    def call(self, store, action, *, fault=None, expect_crash=False, **arguments):
        request = dict(library=self.library, store=str(store), action=action, **arguments)
        environment = os.environ.copy()
        environment.pop('ZB_LHM_CRASH_AT', None)
        environment.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
        if arguments.get('segment_bytes') is not None:
            environment['ZB_LHM_TEST_SEGMENT_BYTES'] = str(arguments['segment_bytes'])
        if fault:
            environment['ZB_LHM_CRASH_AT'] = fault
        result = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker'],
            input=json.dumps(request), text=True, capture_output=True, env=environment, timeout=120)
        if expect_crash:
            require(result.returncode == -signal.SIGKILL,
                    f'{action} at {fault} did not SIGKILL: {result.returncode}; '
                    f'{result.stdout} {result.stderr}')
            return None
        require(result.returncode == 0, f'{action}: {result.stdout} {result.stderr}')
        return json.loads(result.stdout)

    def fixture(self, name):
        store, batch, baseline, checkpoint = super().fixture(name)
        template = store.parent / 'compiled'
        result = self.call(store, 'compile', manifest=batch['manifest'], template=str(template))
        require(result['rc'] == OK, 'compile failed: ' + result['error'])
        batch.update(template=str(template), header_sha256=digest((template / 'header.bin').read_bytes()))
        info = self.call(store, 'info', template=str(template))
        require(info['rc'] == OK and info['result']['files'] == 402
                and info['result']['directories'] == 71, 'compiled info counts wrong')
        return store, batch, baseline, checkpoint

    def ok_import(self, store, batch):
        result = self.call(store, 'compiled_import', **batch)
        require(result['rc'] == OK, 'compiled import failed: ' + result['error'])
        require(result['result']['files'] == batch['files']
                and result['result']['directories'] == batch['directories'], 'compiled counts wrong')
        return result

    def basic(self):
        store, batch, baseline, _ = self.fixture('basic')
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        old = self.capture_checkpoint(store)
        require(self.ok_import(store, batch)['already'] == 1, 'compiled retry not idempotent')
        require(self.capture_checkpoint(store) == old, 'compiled retry changed store')
        for change in ({'target': '/different'}, {'header_sha256': '0' * 64}):
            result = self.call(store, 'compiled_import', **dict(batch, **change))
            require(result['rc'] != OK, 'changed parameters accepted for existing batch ID')
            require(self.capture_checkpoint(store) == old, 'rejected batch changed store')
        previous = self.peek(store, list(baseline) + ['/batch', '/batch/d0/f399'])
        self.ok_import(store, dict(batch, target='/second', batch_id='second'))
        self.imported(store, dict(batch, target='/second'), previous)
        values = self.peek(store, ['/batch', '/second'])
        require(values['/batch']['file_name'] == 'batch' and values['/second']['file_name'] == 'second',
                'template root name was not relocated')
        self.passed_case('compiled append, old inode preservation, root relocation and idempotence')

    def same_process(self):
        store, batch, baseline, _ = self.fixture('same-process')
        suffixes = [''] + [f'/d{i}' for i in range(70)] + [f'/d0/f{i}' for i in range(400)]
        suffixes += ['/d0/遥感影像_α.bin', '/d69/last']
        result = self.call(store, 'many_compiled', **batch, baseline=baseline, batches=12, suffixes=suffixes)
        require(result['batches'] == 12, 'same-process batches incomplete')
        self.passed_case('12 same-process page appends preserve every prior path and inode ID')

    def corrupt_bundle(self):
        store, batch, baseline, checkpoint = self.fixture('bad-bundle')
        for filename in ('inode.pages', 'heads.pages', 'bucket.pages', 'routes.bin', 'header.bin'):
            path = Path(batch['template']) / filename
            data = path.read_bytes()
            require(data, 'expected nonempty fixture file: ' + filename)
            damaged = bytearray(data)
            damaged[len(damaged) // 2] ^= 0x40
            path.write_bytes(damaged)
            result = self.call(store, 'compiled_import', **batch)
            require(result['rc'] != OK, 'corrupted bundle was accepted: ' + filename)
            require(self.call(store, 'recover')['rc'] == OK, 'corrupt bundle rollback failed')
            self.checkpoint_restored(store, baseline, checkpoint)
            path.write_bytes(data)
        self.ok_import(store, batch)
        self.passed_case('corrupted pages, routes and header cannot commit partial data')

    def malformed_bucket_structure(self):
        store, batch, baseline, checkpoint = self.fixture('bucket-structure')
        root = Path(batch['template'])
        page_file, header_file = root / 'bucket.pages', root / 'header.bin'
        pages, header = page_file.read_bytes(), header_file.read_bytes()
        old_digest = digest(pages).encode()
        require(header.count(old_digest) == 1, 'bucket digest missing or ambiguous in template header')
        count, = struct.unpack_from('<I', pages, 64 + 8)
        require(count >= 2, 'fixture needs at least two entries in first bucket')
        index_start = 16384 - count * 16
        mutations = {}
        broken = bytearray(pages)
        broken[index_start + 16:index_start + 32] = broken[index_start:index_start + 16]
        mutations['duplicate-index'] = broken
        broken = bytearray(pages)
        struct.pack_into('<H', broken, index_start + 8, 65535)
        mutations['outside-index'] = broken
        broken = bytearray(pages)
        struct.pack_into('<H', broken, index_start + 8, 1)
        mutations['unaligned-index'] = broken
        broken = bytearray(pages)
        length, = struct.unpack_from('<H', broken, 384 + 8)
        struct.pack_into('<H', broken, 384 + 8, length - 1)
        mutations['unaligned-record'] = broken
        broken = bytearray(pages); broken[384 + 16 + 8] = ord('/')
        mutations['invalid-name'] = broken
        for name, broken in mutations.items():
            # Rebind both digests so this tests structural validation, rather
            # than merely relying on the normal corrupted-file SHA rejection.
            changed_header = header.replace(old_digest, digest(broken).encode())
            page_file.write_bytes(broken); header_file.write_bytes(changed_header)
            invalid = dict(batch, header_sha256=digest(changed_header))
            result = self.call(store, 'compiled_import', **invalid)
            require(result['rc'] != OK, 'invalid bucket structure accepted: ' + name)
            require(self.call(store, 'recover')['rc'] == OK, 'malformed bucket rollback failed')
            self.checkpoint_restored(store, baseline, checkpoint)
        page_file.write_bytes(pages); header_file.write_bytes(header)
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('bucket bitmap validation rejects duplicate, unaligned and out-of-range records/indexes')

    def compiler_inputs(self):
        directory = self.directory / 'compiler-inputs'
        directory.mkdir()
        manifest = directory / 'input.tsv'
        invalid = {
            'duplicate': 'dir\t/\ndir\t/a\nfile\t/a/x\nfile\t/a/x\n',
            'missing-parent': 'dir\t/\nfile\t/missing/x\n',
            'ungrouped': 'dir\t/\ndir\t/a\ndir\t/b\nfile\t/a/x\nfile\t/b/y\nfile\t/a/z\n',
            'duplicate-directory': 'dir\t/\ndir\t/a\ndir\t/a\n',
            'directory-file-same-name': 'dir\t/\ndir\t/a\nfile\t/a\n',
            'directory-after-parent-files': 'dir\t/\nfile\t/x\ndir\t/a\n',
        }
        for name, body in invalid.items():
            manifest.write_text('type\tpath\n' + body)
            template = directory / name
            result = self.call(directory / 'unused', 'compile', manifest=str(manifest), template=str(template))
            require(result['rc'] != OK and not template.exists(),
                    'invalid compiler input published a bundle: ' + name)
        self.passed_case('compiler rejects duplicate, missing-parent and ungrouped file input atomically')

    def attributes(self):
        directory = self.directory / 'attributes'
        directory.mkdir()
        store, manifest, template = directory / 'store', directory / 'input.tsv', directory / 'compiled'
        self.call(store, 'initialize', entries=[])
        fields = dict(size=1234567, atime=0, mtime=0, ctime=789, object_unit_size=987,
            version=6, disk_node_id=17, optical_node_id=18, optical_disk_id=19, blocks_512=321,
            mode=384, uid=41, gid=42, nlink=3, replica=2, disk_id=22, optical_image_id=23,
            blksize=4096, rdev_major=8, rdev_minor=9, archive_state=1, storage_tier=2,
            disk_node_is_virtual=1)
        columns = list(fields)
        suffix = '\t' + '\t'.join(str(fields[name]) for name in columns)
        manifest.write_text('type\tpath\t' + '\t'.join(columns) + '\n'
            + 'dir\t/' + suffix + '\nfile\t/遥感.bin' + suffix)  # no final newline
        compiled = self.call(store, 'compile', manifest=str(manifest), template=str(template))
        require(compiled['rc'] == OK, 'attribute compile failed: ' + compiled['error'])
        batch = dict(target='/attrs', template=str(template), batch_id='attrs',
                     header_sha256=digest((template / 'header.bin').read_bytes()), files=1, directories=1)
        self.ok_import(store, batch)
        values = self.peek(store, ['/attrs', '/attrs/遥感.bin'])
        for path in values:
            require(values[path]['rc'] == OK, 'attribute path missing')
            for name, expected in fields.items():
                require(values[path][name] == expected, f'attribute {name} changed for {path}: {values[path][name]}')
        manifest.write_text('type\tpath\ndir\t/\nfile\t/now\n')
        template = directory / 'default-times'
        require(self.call(store, 'compile', manifest=str(manifest), template=str(template))['rc'] == OK,
                'default attribute compile failed')
        time.sleep(1.05)  # Import defaults must differ from the compilation time.
        before = int(time.time())
        self.ok_import(store, dict(batch, target='/defaults', batch_id='defaults', template=str(template),
            header_sha256=digest((template / 'header.bin').read_bytes())))
        after = int(time.time())
        inode = self.peek(store, ['/defaults/now'])['/defaults/now']
        require(all(before <= inode[name] <= after for name in ('atime', 'mtime', 'ctime')),
                'default timestamps were not set at import time')
        manifest.write_text('type\tpath\tatime\tmtime\tctime\ndir\t/\t\t0\t\nfile\t/mixed\t\t0\t\n')
        template = directory / 'mixed-times'
        require(self.call(store, 'compile', manifest=str(manifest), template=str(template))['rc'] == OK,
                'mixed explicit/default timestamp compile failed')
        time.sleep(1.05)
        before = int(time.time())
        self.ok_import(store, dict(batch, target='/mixed', batch_id='mixed', template=str(template),
            header_sha256=digest((template / 'header.bin').read_bytes())))
        after = int(time.time())
        inode = self.peek(store, ['/mixed/mixed'])['/mixed/mixed']
        require(inode['mtime'] == 0 and all(before <= inode[name] <= after for name in ('atime', 'ctime')),
                'mixed explicit zero and default timestamp semantics changed')
        self.passed_case('all optional inode attributes, explicit zero time, default time and UTF-8 preserved')

    def crash(self, point, committed=False):
        store, batch, baseline, _ = self.fixture('crash-' + point.replace(':', '-'))
        # Crash the second append so all three files contain earlier committed
        # data and each of their allocation cursors must survive rollback.
        self.ok_import(store, dict(batch, target='/previous', batch_id='previous'))
        baseline = self.peek(store, list(baseline) + ['/previous', '/previous/d0/f0', '/previous/d0/f399'])
        checkpoint = self.capture_checkpoint(store)
        old_receipt = self.call(store, 'status')['receipt']
        self.call(store, 'compiled_import', fault=point, expect_crash=True, **batch)
        if Path(str(store) + '.undo').exists():
            require(not self.call(store, 'peek')['opened'], 'ordinary reopen bypassed pending undo')
        recovered = self.call(store, 'recover')
        require(recovered['rc'] == OK, 'compiled crash recovery failed: ' + recovered['error'])
        require(not Path(str(store) + '.undo').exists(), 'recovery left active undo')
        if committed:
            self.imported(store, batch, baseline)
            require(self.ok_import(store, batch)['already'] == 1, 'published batch imported twice')
        else:
            self.checkpoint_restored(store, baseline, checkpoint)
            require(self.call(store, 'status')['receipt'] == old_receipt, 'rollback lost preceding receipt')
            require(self.ok_import(store, batch)['already'] == 0, 'rolled-back batch marked committed')
            self.imported(store, batch, baseline)
        self.passed_case('compiled crash ' + point + (' preserves commit' if committed else ' restores prior batch'))

    def cli(self, faults=False):
        directory = self.directory / 'cli'
        directory.mkdir()
        script = Path(__file__).resolve().parents[2] / 'scripts/import_lhm_batches.py'
        manifest, template, store = directory / 'input.tsv', directory / 'compiled', directory / 'store'
        manifest.write_text('type\tpath\tsize\ndir\t/\t0\ndir\t/d\t0\nfile\t/d/x\t72\n')
        def run(*args, ok=True, fault=None):
            environment = os.environ.copy()
            environment.pop('ZB_LHM_CRASH_AT', None)
            environment.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
            if fault:
                environment['ZB_LHM_CRASH_AT'] = fault
            result = subprocess.run([sys.executable, str(script), *map(str, args), '--library', self.library],
                                    capture_output=True, text=True, timeout=120, env=environment)
            if fault:
                require(result.returncode == -signal.SIGKILL, 'CLI fault did not SIGKILL: ' + result.stderr)
                return result
            require((result.returncode == 0) == ok, 'CLI unexpected result: ' + result.stdout + result.stderr)
            return result
        run('compile', '--manifest', manifest, '--output', template)
        run('init', '--store', store)
        command = ('import', '--store', store, '--compiled-template', template, '--prefix', '/cli',
                   '--copies', '3', '--min-free-gib', '0')
        if faults:
            run(*command, '--copies', '1')
            run(*command, fault='compiled_write:3')
            require(Path(str(store) + '.undo').exists(), 'CLI interrupted batch has no recovery journal')
        run(*command)
        before = self.capture_checkpoint(store)
        run(*command)
        require(self.capture_checkpoint(store) == before, 'CLI resume duplicated committed pages')
        run(*command, '--expected-files', '99', ok=False)
        require(self.capture_checkpoint(store) == before, 'wrong explicit count changed store')
        values = self.peek(store, [f'/cli-{i:06d}/d/x' for i in range(1, 4)])
        require(all(v['rc'] == OK and v['size'] == 72 for v in values.values()), 'CLI copies missing')
        self.passed_case('CLI compile, automatic counts, three copies, resume and wrong-count refusal'
                         + (' including interrupted second batch' if faults else ''))

    def empty_and_normal_create(self):
        store, batch, baseline, _ = self.fixture('empty-and-normal')
        self.ok_import(store, batch)
        old_paths = list(baseline) + ['/batch'] + [f'/batch/d{i}' for i in range(70)]
        old_paths += [f'/batch/d0/f{i}' for i in range(400)] + ['/batch/d0/遥感影像_α.bin', '/batch/d69/last']
        original = self.peek(store, old_paths)
        manifest, template = store.parent / 'empty.tsv', store.parent / 'empty-compiled'
        manifest.write_text('type\tpath\ndir\t/\n')
        result = self.call(store, 'compile', manifest=str(manifest), template=str(template))
        require(result['rc'] == OK, 'empty template compile failed: ' + result['error'])
        self.ok_import(store, dict(target='/empty', batch_id='empty', template=str(template), files=0,
            directories=1, header_sha256=digest((template / 'header.bin').read_bytes())))
        previous = self.peek(store, old_paths + ['/empty'])
        require(all(previous[path] == value for path, value in original.items()),
                'empty template append changed old entries')
        require(previous['/empty']['type'] == 2 and previous['/empty']['file_name'] == 'empty',
                'empty template root attributes wrong')
        entries = [('dir', '/normal')] + [('dir', f'/normal/d{i}') for i in range(70)]
        entries += [('file', f'/normal/d0/f{i}') for i in range(140)]
        result = self.call(store, 'normal_create', entries=entries)
        identifiers = result['identifiers']
        require(len(set(identifiers)) == len(identifiers), 'normal create reused new IDs')
        require(not set(identifiers).intersection(value['inode_id'] for value in previous.values()),
                'normal allocator overlapped compiled reservation')
        values = self.peek(store, list(previous) + [path for _, path in entries])
        require(all(values[path] == value for path, value in previous.items()),
                'normal create changed compiled or older entries')
        for _, path in entries:
            require(values[path]['rc'] == OK and values[path]['size'] == 735,
                    'normal create not readable after compiled import: ' + path)
        self.passed_case('empty template and normal create after compiled append preserve allocator ranges')


def main():
    if sys.argv[1:] == ['--worker']:
        print(json.dumps(worker(json.load(sys.stdin))), flush=True)
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--basic-only', action='store_true')
    args = parser.parse_args()
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=False)
        directory = args.output_dir.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix='lhm-compiled-import-'))
    suite = Suite(args.library, directory)
    print('fixtures: ' + str(directory), flush=True)
    suite.basic()
    suite.same_process()
    suite.attributes()
    suite.corrupt_bundle()
    suite.malformed_bucket_structure()
    suite.compiler_inputs()
    suite.cli(faults=not args.basic_only)
    suite.empty_and_normal_create()
    suite.bucket_identity()
    if not args.basic_only:
        for point in ('compiled_reserved', 'compiled_write:3', 'compiled_synced', 'batch_snapshot', 'undo_commit'):
            suite.crash(point)
        for point in ('batch_published', 'undo_receipt', 'undo_removed'):
            suite.crash(point, committed=True)
    (directory / 'results.json').write_text(json.dumps(dict(passed=suite.passed,
        library_sha256=suite.library_sha256), indent=2) + '\n')
    print(f'PASS {len(suite.passed)} compiled import scenarios', flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('FAIL: ' + str(error), file=sys.stderr)
        sys.exit(1)
