#!/usr/bin/env python3
"""Cross-process batch recovery checks against a ZB_LHM_TESTING bridge.

Run manually: python3 tests/mds/lhm_batch_recovery_test.py \
    --library /absolute/path/libzb_lhm_bridge.so

Workers start in new interpreters: the bridge owns a worker thread and must
never be inherited by fork. One worker also exercises many batches in the same
process. Fixtures are small, isolated databases.
"""

import argparse
import ctypes as C
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import struct
import subprocess
import sys
import tempfile


BLOCK = 16384
DATA_SUFFIXES = ('', '.heads', '.buckets')
OK = 0
MISSING = 1


class Inode(C.Structure):
    _fields_ = [(name, C.c_uint64) for name in (
        'inode_id', 'size', 'atime', 'mtime', 'ctime', 'object_unit_size',
        'version', 'disk_node_id', 'optical_node_id', 'optical_disk_id',
        'blocks_512')] + [(name, C.c_uint32) for name in (
        'mode', 'uid', 'gid', 'nlink', 'replica', 'disk_id', 'optical_image_id',
        'blksize', 'rdev_major', 'rdev_minor')] + [(name, C.c_uint8) for name in (
        'type', 'archive_state', 'storage_tier', 'disk_node_is_virtual',
        'name_length')] + [('file_name', C.c_char * 64)]


class ImportResult(C.Structure):
    _fields_ = [(name, C.c_uint64) for name in (
        'root_inode_id', 'files', 'directories', 'processed_records', 'error_line')]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def fnv(data):
    result = 14695981039346656037
    for value in data:
        result = ((result ^ value) * 1099511628211) & ((1 << 64) - 1)
    return result


def undo_frame(kind, block, previous):
    require(len(previous) == BLOCK, 'undo fixture requires one complete old page')
    body = struct.pack('<QQQQ', 1, kind, block, len(previous)) + previous
    return body + struct.pack('<Q', fnv(body))


def segment_files(store):
    """Return all canonical data segments, including unpublished extra segments."""
    expression = re.compile(re.escape(store.name) + r'(?:\.heads|\.buckets)?(?:\.part[0-9]{6,})?')
    return {path.name[len(store.name):]: path for path in store.parent.iterdir()
            if expression.fullmatch(path.name)}


def parse_segment_layout(data, offset):
    require(offset + 40 <= len(data), 'truncated segment layout')
    segment_bytes, main, heads, buckets, count = struct.unpack_from('<5Q', data, offset)
    offset += 40
    require(segment_bytes >= BLOCK and segment_bytes % BLOCK == 0, 'invalid segment size')
    require(count <= (len(data) - offset) // 40, 'truncated segment identities')
    identities = []
    for _ in range(count):
        kind, index, device, inode, length = struct.unpack_from('<5Q', data, offset)
        offset += 40
        require(kind < 3 and length <= segment_bytes, 'invalid segment identity')
        identities.append(dict(kind=kind, index=index, dev=device, ino=inode, bytes=length))
    return dict(segment_bytes=segment_bytes, logical_bytes=(main, heads, buckets),
                segments=identities), offset


def segment_suffix(kind, index):
    return DATA_SUFFIXES[kind] + (f'.part{index:06d}' if index else '')


def parse_undo_header(data):
    require(len(data) >= 14 * 8, 'truncated undo-v3 header')
    magic, version, block_bytes, header_bytes = struct.unpack_from('<4Q', data)
    require(version in (3, 4) and block_bytes == BLOCK, 'invalid undo header version/size')
    require(header_bytes <= len(data) and header_bytes % 8 == 0,
            'invalid undo-v3 header length')
    require(struct.unpack_from('<Q', data, header_bytes - 8)[0] == fnv(data[:header_bytes - 8]),
            'invalid undo-v3 header checksum')
    layout, end = parse_segment_layout(data[:header_bytes], 8 * 8)
    require(end + 8 == header_bytes, 'undo-v3 layout does not end at checksum')
    return layout, header_bytes


def assert_snapshot_files(store):
    snapshot = Path(str(store) + '.snapshot').read_bytes()
    require(len(snapshot) >= 24 * 8 and len(snapshot) % 8 == 0, 'invalid snapshot size')
    magic, version = struct.unpack_from('<2Q', snapshot)
    require(magic == 0x5a424c484d534e32 and version in (4, 5),
            'tests require segmented snapshot version 4 or 5')
    require(struct.unpack_from('<Q', snapshot, len(snapshot) - 8)[0] == fnv(snapshot[:-8]),
            'checkpoint checksum invalid')
    layout, offset = parse_segment_layout(snapshot, 16)
    expected = set()
    lengths = [0, 0, 0]
    indices = [[], [], []]
    for entry in layout['segments']:
        suffix = segment_suffix(entry['kind'], entry['index'])
        require(suffix not in expected, 'duplicate snapshot segment')
        expected.add(suffix)
        stat = Path(str(store) + suffix).stat()
        require((entry['dev'], entry['ino'], entry['bytes']) == (stat.st_dev, stat.st_ino, stat.st_size),
                'snapshot data-file identity/length mismatch: ' + (suffix or 'main'))
        lengths[entry['kind']] += entry['bytes']
        indices[entry['kind']].append(entry['index'])
    require(set(segment_files(store)) == expected, 'unexpected or missing data segments')
    require(tuple(lengths) == layout['logical_bytes'], 'logical length differs from segment lengths')
    for sequence in indices:
        require(sorted(sequence) == list(range(len(sequence))), 'segment sequence has a gap')
    fields = struct.unpack_from('<6Q', snapshot, offset)
    layout['allocation'] = dict(zip(('next_alloc', 'next_bucket', 'sequence', 'tickets',
                                     'inode_high', 'next_head'), fields))
    layout['indexes'] = checkpoint_indexes(store)
    return layout


def checkpoint_indexes(store):
    data = Path(str(store) + '.snapshot').read_bytes()
    if struct.unpack_from('<Q', data, 8)[0] != 5:
        return []
    _, offset = parse_segment_layout(data, 16)
    offset += 10 * 8
    slots = struct.unpack_from('<Q', data, offset + 5 * 8)[0]
    offset += (6 + slots) * 8
    buckets = struct.unpack_from('<Q', data, offset)[0]
    offset += (1 + buckets * 5) * 8
    roots = struct.unpack_from('<Q', data, offset)[0]
    offset += (1 + roots) * 8
    count = struct.unpack_from('<Q', data, offset)[0]; offset += 8
    result = []
    for _ in range(count):
        token, dev, ino, length, checksum, ms, mn, cs, cn = struct.unpack_from('<9Q', data, offset)
        offset += 72
        suffix = '.index-' + token.to_bytes(8, 'little')[:6].decode('ascii')
        path = Path(str(store) + suffix); stat = path.stat(); payload = path.read_bytes()
        require((dev, ino, length, ms*10**9+mn, cs*10**9+cn) ==
                (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns),
                'immutable index identity/length/timestamps differ')
        require(fnv(payload[:-8]) == checksum == struct.unpack_from('<Q', payload, len(payload)-8)[0],
                'immutable index checksum differs')
        result.append(dict(path=str(path), suffix=suffix, bytes=length, checksum=checksum))
    require(offset + 8 == len(data), 'incremental checkpoint trailing data')
    return result


def worker(request):
    lib = C.CDLL(request['library'])
    text = C.c_char_p
    pointer = C.c_void_p
    buffer = C.POINTER(C.c_char)
    bindings = {
        'zb_lhm_open': ([text], pointer),
        'zb_lhm_reopen': ([text, buffer, C.c_size_t], pointer),
        'zb_lhm_close_checked': ([pointer, buffer, C.c_size_t], C.c_int),
        'zb_lhm_reserve_checked': ([pointer, text, C.c_uint8,
            C.POINTER(C.c_uint64), C.POINTER(C.c_uint64)], C.c_int),
        'zb_lhm_commit_checked': ([pointer, C.c_uint64, C.POINTER(Inode)], C.c_int),
        'zb_lhm_lookup_checked': ([pointer, text, C.POINTER(Inode)], C.c_int),
        'zb_lhm_import_batch': ([text, text, text, text, text, C.c_uint64,
            C.c_uint64, C.c_uint64, C.POINTER(ImportResult), C.POINTER(C.c_int),
            buffer, C.c_size_t], C.c_int),
        'zb_lhm_recover_batch': ([text, buffer, C.c_size_t], C.c_int),
        'zb_lhm_batch_status': ([text, buffer, C.c_size_t, buffer, C.c_size_t], C.c_int),
    }
    for name, (arguments, returns) in bindings.items():
        function = getattr(lib, name)
        function.argtypes = arguments
        function.restype = returns
    error = C.create_string_buffer(8192)
    store = os.fsencode(request['store'])
    action = request['action']
    if action == 'initialize':
        context = lib.zb_lhm_open(store)
        require(context, 'fresh store open failed')
        for kind, path in request['entries']:
            token, identifier = C.c_uint64(), C.c_uint64()
            directory = kind == 'dir'
            require(lib.zb_lhm_reserve_checked(context, path.encode(), 2 if directory else 1,
                    C.byref(token), C.byref(identifier)) == OK, 'fixture reserve failed: ' + path)
            inode = Inode()
            inode.inode_id = identifier.value
            inode.type = 2 if directory else 1
            inode.mode = 493 if directory else 384
            inode.nlink = 2 if directory else 1
            inode.version = 1
            inode.mtime = inode.atime = inode.ctime = 123
            name = path.rsplit('/', 1)[-1].encode()
            inode.name_length = len(name)
            inode.file_name = name
            require(lib.zb_lhm_commit_checked(context, token.value, C.byref(inode)) == OK,
                    'fixture commit failed: ' + path)
        require(lib.zb_lhm_close_checked(context, error, len(error)),
                'fixture close failed: ' + error.value.decode())
        return {'rc': OK}
    if action == 'peek':
        context = lib.zb_lhm_reopen(store, error, len(error))
        if not context:
            return {'opened': False, 'error': error.value.decode()}
        values = {}
        for path in request.get('paths', []):
            inode = Inode()
            rc = lib.zb_lhm_lookup_checked(context, path.encode(), C.byref(inode))
            values[path] = {'rc': rc}
            if rc == OK:
                values[path].update({name: getattr(inode, name) for name in (
                    'inode_id', 'size', 'type', 'mode', 'uid', 'gid', 'nlink',
                    'version', 'mtime', 'optical_image_id', 'optical_disk_id')})
        require(lib.zb_lhm_close_checked(context, error, len(error)),
                'read-only close failed: ' + error.value.decode())
        return {'opened': True, 'values': values}
    if action == 'import':
        result = ImportResult()
        already = C.c_int(-1)
        rc = lib.zb_lhm_import_batch(store, request['target'].encode(),
            os.fsencode(request['manifest']), request['batch_id'].encode(),
            request['manifest_sha256'].encode(), request.get('budget', 1 << 20),
            request['files'], request['directories'], C.byref(result),
            C.byref(already), error, len(error))
        return {'rc': rc, 'error': error.value.decode(), 'already': already.value,
                'result': {name: getattr(result, name) for name, _ in result._fields_}}
    if action == 'recover':
        rc = lib.zb_lhm_recover_batch(store, error, len(error))
        return {'rc': rc, 'error': error.value.decode()}
    if action == 'status':
        receipt = C.create_string_buffer(65536)
        rc = lib.zb_lhm_batch_status(store, receipt, len(receipt), error, len(error))
        return {'rc': rc, 'error': error.value.decode(), 'receipt': receipt.value.decode()}
    if action == 'many_batches':
        previous = dict(request['baseline'])
        identifiers = {value['inode_id'] for value in previous.values()}
        for number in range(request['batches']):
            target = f'/repeat-{number:03d}'
            batch_id = f'same-process-{number:03d}'
            result, already = ImportResult(), C.c_int(-1)
            arguments = (store, target.encode(), os.fsencode(request['manifest']),
                batch_id.encode(), request['manifest_sha256'].encode(), 1 << 20,
                3, 2, C.byref(result), C.byref(already), error, len(error))
            require(lib.zb_lhm_import_batch(*arguments) == OK and already.value == 0,
                    f'same-process batch {number} failed: ' + error.value.decode())
            require(result.files == 3 and result.directories == 2, 'same-process counts wrong')
            receipt = C.create_string_buffer(65536)
            require(lib.zb_lhm_batch_status(store, receipt, len(receipt), error, len(error)) == OK
                    and batch_id.encode() in receipt.value, 'same-process status failed')
            require(lib.zb_lhm_recover_batch(store, error, len(error)) == OK,
                    'same-process clean recovery failed: ' + error.value.decode())
            require(lib.zb_lhm_import_batch(*arguments) == OK and already.value == 1,
                    'same-process idempotent retry failed: ' + error.value.decode())
            snapshot = Path(request['store'] + '.snapshot').read_bytes()
            context = lib.zb_lhm_reopen(store, error, len(error))
            require(context, 'same-process inspection reopen failed: ' + error.value.decode())
            new_paths = {target: (2, 0), target + '/d': (2, 0),
                         target + '/d/f0': (1, 10), target + '/d/f1': (1, 11),
                         target + '/d/遥感.bin': (1, 12)}
            current = {}
            for path in list(previous) + list(new_paths):
                inode = Inode()
                require(lib.zb_lhm_lookup_checked(context, path.encode(), C.byref(inode)) == OK,
                        'same-process lookup failed: ' + path)
                value = {'rc': OK, **{name: getattr(inode, name) for name in (
                    'inode_id', 'size', 'type', 'mode', 'uid', 'gid', 'nlink',
                    'version', 'mtime', 'optical_image_id', 'optical_disk_id')}}
                if path in previous:
                    require(value == previous[path], 'same-process old inode changed: ' + path)
                else:
                    require((inode.type, inode.size) == new_paths[path],
                            'same-process new attributes wrong: ' + path)
                    require(inode.inode_id not in identifiers,
                            'same-process allocator reused an existing inode ID')
                    identifiers.add(inode.inode_id)
                current[path] = value
            require(lib.zb_lhm_close_checked(context, error, len(error)),
                    'same-process inspection close failed: ' + error.value.decode())
            require(Path(request['store'] + '.snapshot').read_bytes() == snapshot,
                    'same-process inspection changed checkpoint fingerprint')
            previous = current
        return {'rc': OK, 'batches': request['batches'], 'unique_inodes': len(identifiers)}
    raise AssertionError('unknown worker action: ' + action)


class Suite:
    def __init__(self, library, directory):
        # Pin one completed build for every child, even if another terminal
        # rebuilds the original shared library while these scenarios run.
        library_copy = directory / 'bridge-under-test.so'
        shutil.copy2(library.resolve(), library_copy)
        self.library = str(library_copy)
        self.library_sha256 = digest(library_copy.read_bytes())
        self.directory = directory
        self.passed = []

    def call(self, store, action, *, fault=None, expect_crash=False, **arguments):
        request = dict(library=self.library, store=str(store), action=action, **arguments)
        environment = os.environ.copy()
        environment.pop('ZB_LHM_CRASH_AT', None)
        environment.pop('ZB_LHM_TEST_SEGMENT_BYTES', None)
        if arguments.get('segment_bytes') is not None:
            environment['ZB_LHM_TEST_SEGMENT_BYTES'] = str(arguments['segment_bytes'])
        if fault:
            environment['ZB_LHM_CRASH_AT'] = fault
        process = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker'],
            input=json.dumps(request), text=True, capture_output=True,
            env=environment, timeout=90)
        if expect_crash:
            require(process.returncode == -signal.SIGKILL,
                    f'{action} at {fault} did not SIGKILL: {process.returncode}; '
                    f'{process.stdout} {process.stderr}')
            return None
        require(process.returncode == 0,
                f'{action} subprocess failed: {process.stdout} {process.stderr}')
        try:
            return json.loads(process.stdout)
        except ValueError as exc:
            raise AssertionError(f'{action}: invalid worker result {process.stdout!r}') from exc

    def peek(self, store, paths):
        snapshot = Path(str(store) + '.snapshot')
        saved = snapshot.read_bytes()
        before = {suffix: path.read_bytes() for suffix, path in segment_files(store).items()}
        result = self.call(store, 'peek', paths=paths)
        require(result['opened'], 'peek reopen failed: ' + result.get('error', ''))
        after = {suffix: path.read_bytes() for suffix, path in segment_files(store).items()}
        require(before == after, 'read-only peek changed backing data')
        require(snapshot.read_bytes() == saved,
                'read-only reopen/close changed checkpoint fingerprint')
        return result['values']

    def fixture(self, name):
        directory = self.directory / name
        directory.mkdir()
        store = directory / 'store'
        entries = [('dir', '/baseline')]
        entries += [('dir', f'/baseline/d{i}') for i in range(7)]
        entries += [('file', f'/baseline/f{i}') for i in range(130)]
        self.call(store, 'initialize', entries=entries)
        paths = ['/'] + [path for _, path in entries]
        baseline = self.peek(store, paths)
        require(all(value['rc'] == OK for value in baseline.values()), 'invalid fixture inode')
        manifest = directory / 'input.tsv'
        rows = ['type\tpath\tsize\tuid\tmode\tmtime', 'dir\t/\t0\t42\t493\t123']
        rows += [f'dir\t/d{i}\t0\t42\t493\t123' for i in range(70)]
        rows += [f'file\t/d0/f{i}\t{1000+i}\t42\t384\t123' for i in range(400)]
        rows += ['file\t/d0/遥感影像_α.bin\t888\t42\t384\t123']
        rows += ['file\t/d69/last\t777\t42\t384\t123']
        manifest.write_text('\n'.join(rows) + '\n')
        batch = dict(target='/batch', manifest=str(manifest), batch_id='batch-001',
                     manifest_sha256=digest(manifest.read_bytes()), files=402, directories=71)
        return store, batch, baseline, self.capture_checkpoint(store)

    @staticmethod
    def capture_checkpoint(store):
        files = {}
        paths = segment_files(store)
        paths['.snapshot'] = Path(str(store) + '.snapshot')
        for suffix, path in paths.items():
            stat = path.stat()
            data = path.read_bytes()
            files[suffix] = dict(identity=(stat.st_dev, stat.st_ino), size=stat.st_size,
                                 data=data)
        return files

    def checkpoint_restored(self, store, baseline, checkpoint, absent='/batch'):
        require(not Path(str(store) + '.snapshot.batch.tmp').exists(),
                'successful rollback left an obsolete temporary snapshot')
        actual_files = segment_files(store)
        require(set(actual_files) == set(checkpoint) - {'.snapshot'},
                'rollback retained an extra segment or removed a committed segment')
        for suffix, path in actual_files.items():
            stat = path.stat()
            expected = checkpoint[suffix]
            require((stat.st_dev, stat.st_ino) == expected['identity'], 'file identity changed')
            require(stat.st_size == expected['size'], f'{suffix or "main"} length not restored')
            actual = path.read_bytes()
            require(actual == expected['data'], 'old data pages changed: ' + (suffix or 'main'))
        require(Path(str(store) + '.snapshot').read_bytes() == checkpoint['.snapshot']['data'],
                'previous snapshot not restored')
        assert_snapshot_files(store)
        values = self.peek(store, list(baseline) + [absent, absent + '/d0/f0'])
        require(all(values[path] == value for path, value in baseline.items()),
                'previously committed inode or attributes changed')
        require(values[absent]['rc'] == MISSING and values[absent + '/d0/f0']['rc'] == MISSING,
                'uncommitted batch is still visible')

    def imported(self, store, batch, baseline):
        assert_snapshot_files(store)
        require(not Path(str(store) + '.snapshot.batch.tmp').exists(),
                'successful commit left an obsolete temporary snapshot')
        imported_paths = [batch['target'] + f'/d0/f{i}' for i in range(400)]
        imported_paths.append(batch['target'] + '/d0/遥感影像_α.bin')
        imported_paths.append(batch['target'] + '/d69/last')
        values = self.peek(store, list(baseline) + imported_paths)
        require(all(values[path] == value for path, value in baseline.items()),
                'old inode changed after successful import')
        for index in range(400):
            value = values[batch['target'] + f'/d0/f{index}']
            require(value['rc'] == OK and value['size'] == 1000 + index and value['uid'] == 42
                    and value['mode'] == 384 and value['mtime'] == 123,
                    'imported attributes or bucket routing wrong')
        require(values[batch['target'] + '/d69/last']['size'] == 777,
                'late file under earlier directory missing')
        require(values[batch['target'] + '/d0/遥感影像_α.bin']['size'] == 888,
                'UTF-8 filename missing or attributes changed')
        ids = [values[path]['inode_id'] for path in imported_paths]
        require(len(set(ids)) == len(ids), 'new file inode IDs collide')

    def ok_import(self, store, batch):
        result = self.call(store, 'import', **batch)
        require(result['rc'] == OK, 'import failed: ' + result['error'])
        require(result['result']['files'] == batch['files']
                and result['result']['directories'] == batch['directories'],
                'import result counts differ')
        return result

    def passed_case(self, name):
        self.passed.append(name)
        print('PASS ' + name, flush=True)

    def basic(self):
        store, batch, baseline, _ = self.fixture('basic')
        result = self.ok_import(store, batch)
        require(result['already'] == 0, 'fresh import reported already committed')
        self.imported(store, batch, baseline)
        before = self.capture_checkpoint(store)
        repeated = self.ok_import(store, batch)
        require(repeated['already'] == 1, 'identical retry was not idempotent')
        require(self.capture_checkpoint(store) == before, 'identical retry changed database')
        for changes in ({'target': '/different'}, {'files': batch['files'] + 1},
                        {'manifest_sha256': '0' * 64}):
            invalid = self.call(store, 'import', **dict(batch, **changes))
            require(invalid['rc'] != OK, 'changed parameters accepted for committed batch')
            require(self.capture_checkpoint(store) == before, 'rejected retry changed database')
        status = self.call(store, 'status')
        require(status['rc'] == OK and batch['batch_id'] in status['receipt'],
                'committed receipt missing')
        previous = self.peek(store, list(baseline) + ['/batch', '/batch/d0/f399'])
        second = dict(batch, target='/second', batch_id='batch-002')
        self.ok_import(store, second)
        self.imported(store, second, previous)
        require(self.ok_import(store, second)['already'] == 1,
                'latest batch retry failed after checkpoint advancement')
        self.passed_case('normal commit, stable old inodes, idempotence and parameter binding')

    def invalid_inputs(self):
        store, batch, baseline, checkpoint = self.fixture('invalid')
        manifest = Path(batch['manifest'])
        original = manifest.read_text()
        manifest.write_text(original + 'file\t/missing/child\t9\t42\t384\t123\n')
        malformed = dict(batch, manifest_sha256=digest(manifest.read_bytes()),
                         files=batch['files'] + 1)
        result = self.call(store, 'import', **malformed)
        require(result['rc'] != OK, 'missing-parent manifest accepted')
        recover = self.call(store, 'recover')
        require(recover['rc'] == OK, 'failed import cannot be rolled back: ' + recover['error'])
        self.checkpoint_restored(store, baseline, checkpoint)
        manifest.write_text(original)
        mismatch = self.call(store, 'import', **dict(batch, files=batch['files'] + 1))
        require(mismatch['rc'] != OK, 'wrong expected counts committed')
        require(self.call(store, 'recover')['rc'] == OK, 'count mismatch rollback failed')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('malformed input and wrong counts discard entire partial batch')

    def digest_and_no_newline(self):
        store, batch, baseline, checkpoint = self.fixture('manifest-bytes')
        mismatch = self.call(store, 'import', **dict(batch, manifest_sha256='0' * 64))
        require(mismatch['rc'] != OK, 'incorrect manifest SHA256 accepted')
        require(self.call(store, 'recover')['rc'] == OK, 'SHA mismatch rollback failed')
        self.checkpoint_restored(store, baseline, checkpoint)
        manifest = Path(batch['manifest'])
        manifest.write_bytes(manifest.read_bytes().rstrip(b'\n'))
        batch['manifest_sha256'] = digest(manifest.read_bytes())
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('SHA256 mismatch rolls back; UTF-8 and final line without newline succeed')

    def locked_store(self):
        store, batch, baseline, checkpoint = self.fixture('exclusive-lock')
        self.call(store, 'import', fault='data_write:12', expect_crash=True, **batch)
        journal = Path(str(store) + '.undo')
        old_journal = journal.read_bytes()
        before = self.capture_checkpoint(store)
        with store.open('r+b') as owner:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            require(self.call(store, 'import', **batch)['rc'] != OK,
                    'import ignored another process holding the store lock')
            require(self.call(store, 'recover')['rc'] != OK,
                    'recovery ignored another process holding the store lock')
            require(journal.read_bytes() == old_journal and self.capture_checkpoint(store) == before,
                    'failed lock acquisition changed database or journal')
        require(self.call(store, 'recover')['rc'] == OK, 'recovery failed after lock release')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('another process lock prevents import and recovery from changing pending undo')

    def previous_receipt(self):
        store, batch, baseline, _ = self.fixture('previous-receipt')
        self.ok_import(store, batch)
        previous = self.peek(store, list(baseline) + ['/batch', '/batch/d0/f399'])
        checkpoint = self.capture_checkpoint(store)
        receipt = self.call(store, 'status')['receipt']
        following = dict(batch, target='/following', batch_id='batch-002')
        self.call(store, 'import', fault='data_write:12', expect_crash=True, **following)
        require(self.call(store, 'recover')['rc'] == OK, 'later batch rollback failed')
        self.checkpoint_restored(store, previous, checkpoint, absent='/following')
        status = self.call(store, 'status')
        require(status['rc'] == OK and status['receipt'] == receipt,
                'rollback lost the preceding committed receipt')
        require(self.ok_import(store, batch)['already'] == 1,
                'previous successful batch stopped being idempotent after rollback')
        self.ok_import(store, following)
        self.imported(store, following, previous)
        self.passed_case('later batch crash preserves earlier committed data and receipt')

    def same_process(self):
        store, _, baseline, _ = self.fixture('same-process')
        manifest = store.parent / 'small.tsv'
        manifest.write_text('type\tpath\tsize\ndir\t/\t0\ndir\t/d\t0\n'
                            'file\t/d/f0\t10\nfile\t/d/f1\t11\nfile\t/d/遥感.bin\t12\n')
        result = self.call(store, 'many_batches', baseline=baseline, batches=30,
                           manifest=str(manifest), manifest_sha256=digest(manifest.read_bytes()))
        require(result['rc'] == OK and result['batches'] == 30,
                'same-process batches incomplete')
        self.passed_case('30 batches in one process preserve old data and globally unique inode IDs')

    def crash(self, point, committed=False):
        store, batch, baseline, checkpoint = self.fixture('crash-' + point.replace(':', '-'))
        self.call(store, 'import', fault=point, expect_crash=True, **batch)
        if Path(str(store) + '.undo').exists():
            rejected = self.call(store, 'peek')
            require(not rejected['opened'], 'ordinary reopen bypassed pending undo')
        result = self.call(store, 'recover')
        require(result['rc'] == OK, 'crash recovery failed: ' + result['error'])
        require(not Path(str(store) + '.undo').exists(), 'recovery left active undo')
        if committed:
            self.imported(store, batch, baseline)
            require(self.ok_import(store, batch)['already'] == 1,
                    'committed crash was imported a second time')
        else:
            self.checkpoint_restored(store, baseline, checkpoint)
            require(self.ok_import(store, batch)['already'] == 0,
                    'rolled-back batch marked committed')
            self.imported(store, batch, baseline)
        self.passed_case('crash ' + point + (' preserves commit' if committed else ' rolls back'))

    def recovery_crash(self, point):
        store, batch, baseline, checkpoint = self.fixture('recovery-' + point)
        self.call(store, 'import', fault='data_write:12', expect_crash=True, **batch)
        self.call(store, 'recover', fault=point, expect_crash=True)
        require(self.call(store, 'recover')['rc'] == OK, 'second recovery failed')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('recovery interruption ' + point + ' is repeatable')

    def corrupt_log(self):
        store, batch, baseline, checkpoint = self.fixture('corrupt-log')
        self.call(store, 'import', fault='data_write:12', expect_crash=True, **batch)
        path = Path(str(store) + '.undo')
        good = path.read_bytes()
        _, header_bytes = parse_undo_header(good)
        require(len(good) > header_bytes + 32 + BLOCK, 'fixture did not journal an old block')
        damaged = bytearray(good)
        damaged[header_bytes + 32 + 31] ^= 0x80
        path.write_bytes(damaged)
        dirty = self.capture_checkpoint(store)
        result = self.call(store, 'recover')
        require(result['rc'] != OK and path.exists(), 'bad undo checksum did not fail closed')
        require(dirty == self.capture_checkpoint(store),
                'corrupt undo modified data before validation')
        path.write_bytes(good)
        require(self.call(store, 'recover')['rc'] == OK, 'valid undo recovery failed')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.passed_case('corrupt complete undo record fails closed without modifying data')

    def incomplete_tail(self):
        store, batch, baseline, checkpoint = self.fixture('incomplete-tail')
        self.call(store, 'import', fault='undo_begin', expect_crash=True, **batch)
        path = Path(str(store) + '.undo')
        # A crash during append can leave a partial last record. No data write
        # may have depended on this record: BeforeWrite must first fsync it.
        with path.open('ab') as stream:
            stream.write(struct.pack('<QQQQ', 1, 0, 0, BLOCK) + b'partial old image')
        require(self.call(store, 'recover')['rc'] == OK, 'incomplete uncommitted tail not tolerated')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.passed_case('torn final journal append is ignored before any dependent data write')

    def truncated_header(self):
        store, batch, baseline, checkpoint = self.fixture('truncated-header')
        self.call(store, 'import', fault='undo_begin', expect_crash=True, **batch)
        path = Path(str(store) + '.undo')
        good = path.read_bytes()
        path.write_bytes(good[:-8])
        require(self.call(store, 'recover')['rc'] != OK and path.exists(),
                'truncated published journal header did not fail closed')
        path.write_bytes(good)
        require(self.call(store, 'recover')['rc'] == OK, 'intact header cannot recover')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.passed_case('truncated published journal header fails closed')

    def bucket_identity(self):
        store, _, baseline, checkpoint = self.fixture('bucket-identity')
        assert_snapshot_files(store)
        bucket = Path(str(store) + '.buckets')
        saved = Path(str(store) + '.buckets.saved')
        original = bucket.read_bytes()
        bucket.rename(saved)
        try:
            # A missing or byte-identical replacement is not the checkpoint's
            # file. Ordinary reopen/status must not create or accept either.
            require(self.call(store, 'status')['rc'] != OK, 'missing bucket file accepted by status')
            require(not self.call(store, 'peek')['opened'], 'missing bucket file accepted by reopen')
            require(not bucket.exists(), 'ordinary reopen created a missing bucket file')
            bucket.write_bytes(original)
            require(self.call(store, 'status')['rc'] != OK, 'replacement bucket identity accepted')
            require(not self.call(store, 'peek')['opened'], 'reopen accepted replacement bucket identity')
            require(bucket.read_bytes() == original, 'rejected reopen modified replacement bucket file')
        finally:
            if bucket.exists():
                bucket.unlink()
            saved.rename(bucket)
        self.checkpoint_restored(store, baseline, checkpoint, absent='/absent')
        self.passed_case('snapshot binds the third file identity and rejects missing/replaced buckets')

    def bucket_undo_records(self):
        store, batch, baseline, checkpoint = self.fixture('bucket-undo')
        self.call(store, 'import', fault='undo_begin', expect_crash=True, **batch)
        journal = Path(str(store) + '.undo')
        header = journal.read_bytes()
        layout, header_bytes = parse_undo_header(header)
        require(len(header) == header_bytes, 'unexpected trailing undo-v3 header bytes')
        bucket = Path(str(store) + '.buckets')
        stat = bucket.stat()
        identity = next(value for value in layout['segments'] if value['kind'] == 2 and value['index'] == 0)
        require((identity['dev'], identity['ino'], identity['bytes']) == (stat.st_dev, stat.st_ino, stat.st_size),
                'undo header does not bind original bucket file')
        old_page = checkpoint['.buckets']['data'][:BLOCK]
        require(any(old_page), 'fixture has no committed bucket to restore')
        # The production append allocates fresh bucket pages. Construct one
        # durable old-bucket frame to exercise the journal's separate kind-2
        # restoration path, including an interrupted recovery and bad CRC.
        good = header + undo_frame(2, 0, old_page)
        with journal.open('wb') as out:
            out.write(good); out.flush(); os.fsync(out.fileno())
        altered = bytearray(old_page); altered[80] ^= 0x40
        with bucket.open('r+b') as out:
            out.write(altered); out.seek(0, os.SEEK_END); out.write(b'x' * (2 * BLOCK))
            out.flush(); os.fsync(out.fileno())
        dirty = self.capture_checkpoint(store)
        damaged = bytearray(good); damaged[header_bytes + 32 + 57] ^= 0x20
        journal.write_bytes(damaged)
        require(self.call(store, 'recover')['rc'] != OK and journal.exists(),
                'corrupt kind-2 record was accepted')
        require(self.capture_checkpoint(store) == dirty, 'bad kind-2 CRC modified backing data')
        journal.write_bytes(good)
        saved = Path(str(store) + '.buckets.saved')
        bucket.rename(saved)
        try:
            bucket.write_bytes(dirty['.buckets']['data'])
            rejected = self.capture_checkpoint(store)
            require(self.call(store, 'recover')['rc'] != OK and journal.read_bytes() == good,
                    'undo recovery accepted a different bucket-file identity')
            require(self.capture_checkpoint(store) == rejected, 'identity refusal modified data')
        finally:
            bucket.unlink(); saved.rename(bucket)
        self.call(store, 'recover', fault='undo_restore_bucket', expect_crash=True)
        require(self.call(store, 'recover')['rc'] == OK, 'bucket recovery could not resume')
        self.checkpoint_restored(store, baseline, checkpoint)
        self.ok_import(store, batch)
        self.imported(store, batch, baseline)
        self.passed_case('bucket undo validates identity/CRC and resumes interrupted old-page restore')


def main():
    if sys.argv[1:] == ['--worker']:
        print(json.dumps(worker(json.load(sys.stdin))), flush=True)
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path,
                        help='new directory for fixtures; default keeps a temporary result directory')
    parser.add_argument('--basic-only', action='store_true',
                        help='run input/commit tests without fault injection')
    args = parser.parse_args()
    require(args.library.is_file(), 'bridge library does not exist')
    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=False)
        directory = args.output_dir.resolve()
    else:
        directory = Path(tempfile.mkdtemp(prefix='lhm-batch-recovery-'))
    suite = Suite(args.library, directory)
    print('fixtures: ' + str(directory), flush=True)
    suite.basic()
    suite.invalid_inputs()
    suite.digest_and_no_newline()
    suite.same_process()
    suite.bucket_identity()
    if not args.basic_only:
        for point in ('undo_begin', 'undo_record', 'data_write:12', 'import_record:100',
                      'batch_snapshot', 'undo_commit'):
            suite.crash(point)
        for point in ('batch_published', 'undo_receipt', 'undo_removed'):
            suite.crash(point, committed=True)
        for point in ('undo_restore', 'undo_restored'):
            suite.recovery_crash(point)
        suite.corrupt_log()
        suite.incomplete_tail()
        suite.truncated_header()
        suite.locked_store()
        suite.previous_receipt()
        suite.bucket_undo_records()
    (directory / 'results.json').write_text(json.dumps({
        'passed': suite.passed, 'library_sha256': suite.library_sha256}, indent=2) + '\n')
    print(f'PASS {len(suite.passed)} batch recovery scenarios', flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(f'FAIL: {error}', file=sys.stderr)
        sys.exit(1)
