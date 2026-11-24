#!/usr/bin/env python3
"""Small-segment LHM append, query, identity and crash-recovery checks.

Run manually against BUILD_TESTING=ON. Fixtures use persisted 8 MiB segments;
the production default is 512 MiB. No existing database is touched.
"""

import argparse
import ctypes as C
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import sys
import tempfile

from lhm_batch_recovery_test import (BLOCK, Inode, OK, MISSING, assert_snapshot_files,
    digest, fnv, parse_segment_layout, parse_undo_header, require, segment_files,
    segment_suffix, undo_frame)
from lhm_compiled_import_test import Suite as CompiledSuite, worker as compiled_worker


SEGMENT_BYTES = 8 << 20
SEGMENT_PAGES = SEGMENT_BYTES // BLOCK


def same_checkpoint(before, after):
    # Ordinary reopen consumes and normal close republishes the snapshot. Its
    # bytes must remain identical; its filesystem inode may legitimately change.
    return (set(before) == set(after) and
            all(before[name] == after[name] for name in before if name != '.snapshot') and
            before['.snapshot']['data'] == after['.snapshot']['data'])


def worker(request):
    if request['action'] != 'queries':
        return compiled_worker(request)
    lib = C.CDLL(request['library'])
    text, pointer, size = C.c_char_p, C.c_void_p, C.c_size_t
    callback_type = C.CFUNCTYPE(C.c_int, pointer, text, C.c_uint32, C.c_uint64, C.c_uint8)
    bindings = {
        'zb_lhm_reopen': ([text, text, size], pointer),
        'zb_lhm_close_checked': ([pointer, text, size], C.c_int),
        'zb_lhm_lookup_checked': ([pointer, text, C.POINTER(Inode)], C.c_int),
        'zb_lhm_read_checked': ([pointer, C.c_uint64, C.POINTER(Inode)], C.c_int),
        'zb_lhm_readdir_checked': ([pointer, text, text, C.c_uint32, callback_type,
                                  pointer, C.POINTER(C.c_int)], C.c_int),
    }
    for name, (args, result) in bindings.items():
        getattr(lib, name).argtypes, getattr(lib, name).restype = args, result
    error = C.create_string_buffer(8192)
    context = lib.zb_lhm_reopen(os.fsencode(request['store']), error, len(error))
    require(context, 'query reopen failed: ' + error.value.decode())
    identifiers = set()
    try:
        for expected in request['paths']:
            attr, by_id = Inode(), Inode()
            path = expected['path']
            require(lib.zb_lhm_lookup_checked(context, path.encode(), C.byref(attr)) == OK,
                    'path query failed: ' + path)
            require(lib.zb_lhm_read_checked(context, attr.inode_id, C.byref(by_id)) == OK,
                    'inode query failed: ' + path)
            for name, _ in Inode._fields_:
                require(getattr(attr, name) == getattr(by_id, name),
                        'path/inode query differ: ' + path + ' ' + name)
            for name, value in expected.items():
                if name != 'path':
                    require(getattr(attr, name) == value, 'query attribute differs: ' + path + ' ' + name)
            require(attr.inode_id not in identifiers, 'duplicate inode among queried paths: ' + path)
            identifiers.add(attr.inode_id)
        listed = []
        @callback_type
        def collect(_, name, length, inode, kind):
            listed.append((C.string_at(name, length).decode(), inode, kind))
            return 1
        for directory, names in request.get('listings', {}).items():
            result = []
            after = b''
            while True:
                listed.clear(); more = C.c_int()
                require(lib.zb_lhm_readdir_checked(context, directory.encode(), after, 17,
                            collect, None, C.byref(more)) == OK, 'readdir failed: ' + directory)
                result += [row[0] for row in listed]
                if not more.value:
                    break
                require(listed and listed[-1][0].encode() != after, 'readdir cursor did not advance')
                after = listed[-1][0].encode()
            require(len(result) == len(set(result)) and set(result) == set(names),
                    'readdir lost or duplicated entries: ' + directory)
    finally:
        require(lib.zb_lhm_close_checked(context, error, len(error)),
                'query close failed: ' + error.value.decode())
    return dict(rc=OK, queried=len(identifiers), listed_directories=len(request.get('listings', {})))


class Suite(CompiledSuite):
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
            input=json.dumps(request), text=True, capture_output=True, env=environment, timeout=180)
        if expect_crash:
            require(result.returncode == -signal.SIGKILL,
                    f'{action} at {fault} did not SIGKILL: {result.returncode}; '
                    f'{result.stdout} {result.stderr}')
            return None
        require(result.returncode == 0, f'{action}: {result.stdout} {result.stderr}')
        return json.loads(result.stdout)

    def prepare(self):
        directory = self.directory / 'template'
        directory.mkdir()
        manifest, template = directory / 'input.tsv', directory / 'compiled'
        with manifest.open('w') as stream:
            stream.write('type\tpath\tsize\tmtime\n' + 'dir\t/\t0\t123\n')
            for index in range(6000):
                stream.write(f'dir\t/d{index:05d}\t0\t123\n')
            for directory_index in range(500):
                for file_index in range(60):
                    ordinal = directory_index * 60 + file_index
                    stream.write(f'file\t/d{directory_index:05d}/f{file_index:03d}\t{1000+ordinal}\t123\n')
        result = self.call(directory / 'unused', 'compile', manifest=str(manifest), template=str(template))
        require(result['rc'] == OK, 'segment template compile failed: ' + result['error'])
        require((template / 'inode.pages').stat().st_size == 477 * BLOCK, 'unexpected inode page count')
        require((template / 'bucket.pages').stat().st_size == 500 * BLOCK, 'unexpected bucket page count')
        require((template / 'heads.pages').stat().st_size == 194 * BLOCK, 'unexpected head page count')
        self.batch = dict(target='/batch', batch_id='batch', template=str(template),
                          header_sha256=digest((template / 'header.bin').read_bytes()),
                          files=30000, directories=6001)

    def fresh(self, name, segment_bytes=SEGMENT_BYTES):
        directory = self.directory / name
        directory.mkdir()
        store = directory / 'store'
        self.call(store, 'initialize', entries=[], segment_bytes=segment_bytes)
        return store

    def append(self, store, number, **extra):
        batch = dict(self.batch, target=f'/copy-{number}', batch_id=f'copy-{number}')
        return self.ok_import(store, dict(batch, **extra))

    def paths(self, number, first_page):
        prefix = f'/copy-{number}'
        result = [dict(path=prefix, type=2, size=0)]
        # Directory head pages cross a different boundary than file inode pages.
        for index in range(6000):
            result.append(dict(path=f'{prefix}/d{index:05d}', type=2, size=0))
        ordinals = {0, 1, 59, 60, 29998, 29999}
        for boundary in range((first_page // SEGMENT_PAGES + 1) * SEGMENT_PAGES,
                              first_page + 477, SEGMENT_PAGES):
            edge = (boundary - first_page) * 63
            ordinals.update(n for n in range(edge - 3, edge + 4) if 0 <= n < 30000)
        for ordinal in sorted(ordinals):
            block, slot = first_page + ordinal // 63, ordinal % 63
            result.append(dict(path=f'{prefix}/d{ordinal//60:05d}/f{ordinal%60:03d}',
                               type=1, size=1000+ordinal, mtime=123, inode_id=(block << 16) | slot))
        return result

    def assert_preserved(self, store, checkpoint, receipt=None):
        require(same_checkpoint(checkpoint, self.capture_checkpoint(store)),
                'rollback did not restore exactly the prior segment set and bytes')
        assert_snapshot_files(store)
        require(not Path(str(store) + '.undo').exists(), 'recovery left undo journal')
        if receipt is not None:
            status = self.call(store, 'status')
            require(status['rc'] == OK and status['receipt'] == receipt, 'rollback changed earlier receipt')

    def default_and_persistence(self):
        default = self.fresh('default', segment_bytes=None)
        require(assert_snapshot_files(default)['segment_bytes'] == 512 << 20,
                'new store default segment size is not 512 MiB')
        legacy = self.fresh('legacy-1tib', segment_bytes=1 << 40)
        require(self.call(legacy, 'peek', paths=['/'])['opened'],
                '512 MiB default prevented reopening persisted 1 TiB store')
        require(self.call(legacy, 'compiled_import', **self.batch)['rc'] == OK,
                'cannot append to persisted 1 TiB store')
        require(assert_snapshot_files(legacy)['segment_bytes'] == 1 << 40,
                'append changed legacy segment size')
        require(self.call(legacy, 'peek', paths=['/batch/d00000/f000'])['opened'],
                'cannot reopen legacy store after append')
        store = self.fresh('persisted-size')
        original = self.capture_checkpoint(store)
        require(self.call(store, 'peek', paths=['/'], segment_bytes=16 << 20)['opened'],
                'changed creation environment prevented persisted-size reopen')
        require(self.call(store, 'peek', paths=['/'])['opened'], 'unset creation environment prevented reopen')
        require(assert_snapshot_files(store)['segment_bytes'] == SEGMENT_BYTES,
                'reopen changed the persisted segment size')
        require(same_checkpoint(original, self.capture_checkpoint(store)), 'reopen changed persisted store')
        snapshot = Path(str(store) + '.snapshot')
        saved = snapshot.read_bytes()
        wrong = bytearray(saved)
        struct.pack_into('<Q', wrong, 8, 3)
        struct.pack_into('<Q', wrong, len(wrong)-8, fnv(wrong[:-8]))
        snapshot.write_bytes(wrong)
        require(self.call(store, 'status')['rc'] != OK and not self.call(store, 'peek')['opened'],
                'old snapshot version was accepted')
        snapshot.write_bytes(saved)
        self.assert_preserved(store, original)
        self.passed_case('512 MiB default, persisted 1 TiB reopen/append, small-segment persistence and old snapshot refusal')

    def append_and_query(self):
        store = self.fresh('append-and-query')
        query_paths = []
        old_max = 0
        for number in range(1, 4):
            before = assert_snapshot_files(store)
            first_page = max(before['allocation']['inode_high'],
                             (before['allocation']['tickets'] + 62) // 63) + 1
            self.append(store, number)
            after = assert_snapshot_files(store)
            require(first_page > old_max, 'compiled reservation reused old inode pages')
            old_max = after['allocation']['inode_high']
            query_paths += self.paths(number, first_page)
        layout = assert_snapshot_files(store)
        for kind in range(3):
            require(any(s['kind'] == kind and s['index'] > 0 for s in layout['segments']),
                    'fixture did not cross a segment for kind ' + str(kind))
        require(all(s['bytes'] <= SEGMENT_BYTES for s in layout['segments']), 'segment exceeded capacity')
        checkpoint = self.capture_checkpoint(store)
        result = self.call(store, 'queries', paths=query_paths,
                           listings={'/': ['copy-1', 'copy-2', 'copy-3'],
                                     '/copy-3/d00499': [f'f{i:03d}' for i in range(60)]})
        require(result['queried'] == len(query_paths), 'cross-segment query coverage incomplete')
        require(same_checkpoint(checkpoint, self.capture_checkpoint(store)), 'read-only queries modified segments')
        # Ordinary creates must continue from compiled ranges even after all
        # three logical files acquired additional physical segments.
        new = [('dir', '/ordinary'), ('file', '/ordinary/new'), ('file', '/copy-3/d00499/new')]
        created = self.call(store, 'normal_create', entries=new)
        require(len(set(created['identifiers'])) == len(new), 'ordinary create reused an ID')
        self.call(store, 'queries', paths=query_paths + [dict(path=p, size=735) for _, p in new])
        self.passed_case('three appends cross inode/bucket/head segments; path, inode and readdir agree')
        self.good_store = store

    def segment_identities(self):
        store = self.fresh('identities')
        for number in range(1, 4):
            self.append(store, number)
        checkpoint = self.capture_checkpoint(store)
        for kind in range(3):
            path = Path(str(store) + segment_suffix(kind, 1))
            saved = path.with_name(f'saved-segment-{kind}')
            old = path.read_bytes()
            path.rename(saved)
            try:
                require(self.call(store, 'status')['rc'] != OK, 'status accepted missing segment')
                require(not self.call(store, 'peek')['opened'], 'reopen accepted missing segment')
                require(not path.exists(), 'reopen recreated missing segment')
                path.write_bytes(old)
                require(self.call(store, 'status')['rc'] != OK and not self.call(store, 'peek')['opened'],
                        'byte-identical replaced segment accepted')
                require(path.read_bytes() == old, 'rejected reopen altered replacement segment')
            finally:
                if path.exists():
                    path.unlink()
                saved.rename(path)
        extra = Path(str(store) + '.buckets.part000099')
        extra.write_bytes(b'')
        try:
            require(self.call(store, 'status')['rc'] != OK and not self.call(store, 'peek')['opened'],
                    'untracked extra segment accepted')
            require(extra.exists(), 'rejected open deleted untracked segment')
        finally:
            extra.unlink()
        self.assert_preserved(store, checkpoint)
        self.passed_case('missing, replaced and unexpected segments fail closed without changes')

    def ordinary_create_at_boundary(self):
        store = self.fresh('ordinary-boundary')
        manifest, template = store.parent / 'input.tsv', store.parent / 'compiled'
        # Fresh roots reserve inode pages 1..5 and compact-head page 0.
        # 506 inode pages and 511 head pages finish exactly at 8 MiB. The
        # 511 one-bucket leaves leave one bucket page before the same boundary.
        with manifest.open('w') as stream:
            stream.write('type\tpath\tsize\n' + 'dir\t/\t0\n')
            for index in range(15840):
                stream.write(f'dir\t/d{index:05d}\t0\n')
            ordinal = 0
            for directory in range(511):
                for file_index in range(62 + (directory < 196)):
                    stream.write(f'file\t/d{directory:05d}/f{file_index:03d}\t{ordinal+1000}\n')
                    ordinal += 1
        require(ordinal == 506 * 63, 'ordinary-boundary fixture count wrong')
        result = self.call(store, 'compile', manifest=str(manifest), template=str(template))
        require(result['rc'] == OK, 'ordinary-boundary compile failed: ' + result['error'])
        self.ok_import(store, dict(target='/before', batch_id='before', template=str(template),
            header_sha256=digest((template / 'header.bin').read_bytes()), files=ordinal, directories=15841))
        before = assert_snapshot_files(store)
        require(before['logical_bytes'] == (SEGMENT_BYTES, SEGMENT_BYTES, SEGMENT_BYTES-BLOCK),
                'ordinary-boundary fixture did not reach expected logical limits')
        entries = [('dir', '/ordinary'), ('file', '/before/d01000/a'), ('file', '/before/d01001/b')]
        self.call(store, 'normal_create', entries=entries)
        after = assert_snapshot_files(store)
        require(all(any(entry['kind'] == kind and entry['index'] == 1
                        for entry in after['segments']) for kind in range(3)),
                'ordinary writes did not create all three later segments')
        self.call(store, 'queries', paths=[dict(path=path, size=735) for _, path in entries] + [
            dict(path='/before/d00000/f000', size=1000),
            dict(path='/before/d00510/f061', size=1000+ordinal-1),
            dict(path='/before/d15839', size=0)],
            listings={'/before/d01000': ['a'], '/before/d01001': ['b']})
        self.passed_case('ordinary creates cross inode, bucket and directory-head segment boundaries')

    def one_batch_many_segments(self):
        store = self.fresh('one-batch-many-segments')
        checkpoint = self.capture_checkpoint(store)
        manifest, template = store.parent / 'input.tsv', store.parent / 'compiled'
        with manifest.open('w') as stream:
            stream.write('type\tpath\tsize\n' + 'dir\t/\t0\n')
            for directory in range(1000):
                stream.write(f'dir\t/d{directory:04d}\t0\n')
            for directory in range(1000):
                for file_index in range(100):
                    ordinal = directory * 100 + file_index
                    stream.write(f'file\t/d{directory:04d}/f{file_index:03d}\t{ordinal+1}\n')
        result = self.call(store, 'compile', manifest=str(manifest), template=str(template))
        require(result['rc'] == OK, 'multi-segment compile failed: ' + result['error'])
        batch = dict(target='/large', batch_id='large', template=str(template),
                     header_sha256=digest((template / 'header.bin').read_bytes()),
                     files=100000, directories=1001)
        self.call(store, 'compiled_import', fault='compiled_synced', expect_crash=True, **batch)
        require(len([name for name in segment_files(store) if '.part' in name]) >= 4,
                'one batch did not reserve and write several later segments')
        require(self.call(store, 'recover')['rc'] == OK, 'multi-segment first batch rollback failed')
        self.assert_preserved(store, checkpoint)
        self.ok_import(store, batch)
        layout = assert_snapshot_files(store)
        require(len([s for s in layout['segments'] if s['kind'] == 0]) == 4,
                '100000-file fixture did not span four inode segments')
        ordinals = {0, 99999}
        for boundary in (512, 1024, 1536):
            ordinal = (boundary-6)*63
            ordinals.update(range(ordinal-2, ordinal+3))
        self.call(store, 'queries', paths=[
            dict(path=f'/large/d{ordinal//100:04d}/f{ordinal%100:03d}', size=ordinal+1,
                 inode_id=((6+ordinal//63) << 16) | (ordinal%63)) for ordinal in sorted(ordinals)])
        self.passed_case('one batch spans four inode segments and rolls back every new segment')

    def crash_second(self, fault, committed=False):
        store = self.fresh('crash-' + fault.replace(':', '-'))
        self.append(store, 1)
        checkpoint = self.capture_checkpoint(store)
        receipt = self.call(store, 'status')['receipt']
        batch = dict(self.batch, target='/copy-2', batch_id='copy-2')
        self.call(store, 'compiled_import', fault=fault, expect_crash=True, **batch)
        require(not self.call(store, 'peek')['opened'], 'ordinary reopen bypassed interrupted batch')
        recovered = self.call(store, 'recover')
        require(recovered['rc'] == OK, 'segmented crash recovery failed: ' + recovered['error'])
        if committed:
            require(self.ok_import(store, batch)['already'] == 1, 'published segmented batch duplicated')
            assert_snapshot_files(store)
        else:
            self.assert_preserved(store, checkpoint, receipt)
            require(self.ok_import(store, batch)['already'] == 0, 'rolled-back batch considered committed')
        self.call(store, 'queries', paths=[dict(path='/copy-1/d00000/f000', size=1000),
                                          dict(path='/copy-2/d00499/f059', size=30999)])
        self.passed_case('segmented crash ' + fault + (' preserves commit' if committed else ' restores earlier batch'))

    def interrupted_removal(self):
        store = self.fresh('interrupted-removal')
        self.append(store, 1)
        checkpoint = self.capture_checkpoint(store)
        receipt = self.call(store, 'status')['receipt']
        batch = dict(self.batch, target='/copy-2', batch_id='copy-2')
        self.call(store, 'compiled_import', fault='compiled_synced', expect_crash=True, **batch)
        new_suffixes = set(segment_files(store)) - (set(checkpoint) - {'.snapshot'})
        require(len(new_suffixes) >= 2, 'fixture did not create multiple new segments')
        self.call(store, 'recover', fault='undo_segment_removed', expect_crash=True)
        require(Path(str(store) + '.undo').exists(), 'interrupted deletion lost undo')
        require(self.call(store, 'recover')['rc'] == OK, 'recovery could not resume segment deletion')
        self.assert_preserved(store, checkpoint, receipt)
        self.ok_import(store, batch)
        self.passed_case('recovery resumes after deleting only some newly created segments')

    def new_segment_identity(self):
        store = self.fresh('new-segment-identity')
        self.append(store, 1)
        checkpoint = self.capture_checkpoint(store)
        batch = dict(self.batch, target='/copy-2', batch_id='copy-2')
        self.call(store, 'compiled_import', fault='compiled_synced', expect_crash=True, **batch)
        new_suffixes = set(segment_files(store)) - (set(checkpoint) - {'.snapshot'})
        suffix = sorted(new_suffixes)[0]
        path = Path(str(store) + suffix)
        saved = path.with_name('saved-new-segment')
        content = path.read_bytes()
        path.rename(saved)
        try:
            path.write_bytes(content)
            before = self.capture_checkpoint(store)
            require(self.call(store, 'recover')['rc'] != OK, 'recovery accepted replaced new segment')
            require(self.capture_checkpoint(store) == before and path.exists(),
                    'failed identity check restored/deleted data before validation')
        finally:
            path.unlink(); saved.rename(path)
        require(self.call(store, 'recover')['rc'] == OK, 'recovery rejected restored new-segment identity')
        self.assert_preserved(store, checkpoint)
        self.passed_case('recovery refuses to delete a replaced new segment')

    def old_page_in_later_segment(self):
        store = self.fresh('later-segment-undo')
        for number in (1, 2):
            self.append(store, number)
        checkpoint = self.capture_checkpoint(store)
        batch = dict(self.batch, target='/copy-3', batch_id='copy-3')
        self.call(store, 'compiled_import', fault='undo_begin', expect_crash=True, **batch)
        journal = Path(str(store) + '.undo')
        header = journal.read_bytes()
        _, header_bytes = parse_undo_header(header)
        require(header_bytes == len(header), 'undo_begin did not stop after complete header')
        original = checkpoint['.buckets.part000001']['data'][:BLOCK]
        frame = undo_frame(2, SEGMENT_PAGES, original)
        with journal.open('ab') as stream:
            stream.write(frame); stream.flush(); os.fsync(stream.fileno())
        path = Path(str(store) + '.buckets.part000001')
        damaged = bytearray(original); damaged[80] ^= 0x20
        with path.open('r+b') as stream:
            stream.write(damaged); stream.flush(); os.fsync(stream.fileno())
        self.call(store, 'recover', fault='undo_restore_bucket', expect_crash=True)
        require(self.call(store, 'recover')['rc'] == OK, 'later-segment old page restore failed')
        self.assert_preserved(store, checkpoint)
        self.passed_case('global undo page addresses restore an old page inside a later segment')


def main():
    if sys.argv[1:] == ['--worker']:
        print(json.dumps(worker(json.load(sys.stdin))), flush=True)
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=False)
        directory = args.output_dir.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix='lhm-segmented-import-'))
    suite = Suite(args.library, directory)
    print('fixtures: ' + str(directory), flush=True)
    suite.prepare()
    suite.default_and_persistence()
    suite.append_and_query()
    suite.segment_identities()
    suite.ordinary_create_at_boundary()
    suite.one_batch_many_segments()
    for point in ('segment_temp_created', 'segment_recorded', 'segment_published',
                  'compiled_reserved', 'compiled_write:3', 'compiled_synced', 'batch_snapshot', 'undo_commit'):
        suite.crash_second(point)
    for point in ('batch_published', 'undo_receipt'):
        suite.crash_second(point, committed=True)
    suite.interrupted_removal()
    suite.new_segment_identity()
    suite.old_page_in_later_segment()
    (directory / 'results.json').write_text(json.dumps(dict(passed=suite.passed,
        library_sha256=suite.library_sha256, segment_bytes=SEGMENT_BYTES), indent=2) + '\n')
    print(f'PASS {len(suite.passed)} segmented import scenarios', flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print('FAIL: ' + str(error), file=sys.stderr)
        sys.exit(1)
