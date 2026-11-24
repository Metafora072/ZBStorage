#!/usr/bin/env python3
"""Compile page templates and import copies into a stopped LHM store.

The library checkpoint receipt is authoritative. The JSONL log is an audit trail,
not a restart cursor. This entry point does not call the ordinary MDS import RPC.
"""

import argparse
import ctypes
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import time


class ImportResult(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        'root_inode_id', 'files', 'directories', 'processed_records', 'error_line')]


class CompiledProfile(ctypes.Structure):
    STAGES = ('checkpoint_validation', 'template_validation', 'reopen', 'reserve',
              'read_hash', 'relocate', 'submit', 'data_sync', 'directory_install',
              'snapshot', 'commit', 'total')
    _fields_ = [(name + '_ns', ctypes.c_uint64) for name in STAGES] + [
        (name, ctypes.c_uint64) for name in ('write_calls', 'write_bytes', 'pwrite_ns',
            'main_sync_ns', 'heads_sync_ns', 'buckets_sync_ns', 'writeback_calls', 'writeback_wait_ns')]

    def as_dict(self):
        return dict(stage_seconds={name: round(getattr(self, name + '_ns') / 1e9, 6) for name in self.STAGES},
                    bulk_io=dict(write_calls=self.write_calls, write_bytes=self.write_bytes,
                                 average_write_bytes=round(self.write_bytes / self.write_calls, 1) if self.write_calls else 0,
                                 writeback_calls=self.writeback_calls,
                                 **{name: round(getattr(self, name + '_ns') / 1e9, 6) for name in
                                    ('pwrite', 'main_sync', 'heads_sync', 'buckets_sync', 'writeback_wait')}))


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def positive(value):
    number = int(value)
    if number <= 0 or number > (1 << 64) - 1:
        raise argparse.ArgumentTypeError('must be an integer in 1..2^64-1')
    return number


def nonnegative_float(value):
    number = float(value)
    if not 0 <= number < float('inf'):
        raise argparse.ArgumentTypeError('must be a finite nonnegative number')
    return number


def file_identity(path):
    stat = path.stat()
    return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns)


def segment_paths(store):
    """Include reserved segment names, even malformed ones, in file protection."""
    for suffix in ('', '.heads', '.buckets'):
        base = Path(str(store) + suffix)
        yield from base.parent.glob(base.name + '.part*')
    yield from store.parent.glob(store.name + '.index-*')


def reserved_segment_name(store, candidate):
    if candidate.parent != store.parent:
        return False
    prefixes = [store.name + suffix + '.part' for suffix in ('', '.heads', '.buckets')]
    prefixes.append(store.name + '.segment-new-')
    prefixes.append(store.name + '.index-')
    return any(candidate.name.startswith(prefix) for prefix in prefixes)


def manifest_digest(path):
    before = file_identity(path)
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while True:
            block = stream.read(8 << 20)
            if not block:
                break
            digest.update(block)
    require(file_identity(path) == before, 'manifest changed while computing SHA256')
    return digest.hexdigest(), before


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def library_path(requested):
    if requested:
        path = requested.resolve()
        require(path.is_file(), f'bridge library does not exist: {path}')
        return path
    build = Path(__file__).resolve().parents[1] / 'build'
    for path in (build / 'src/mds/lhm/libzb_lhm_bridge.so',
                 build / 'libzb_lhm_bridge.so', build / 'lib/libzb_lhm_bridge.so'):
        if path.is_file():
            return path
    raise RuntimeError('cannot find libzb_lhm_bridge.so; pass --library with the built library path')


def parse_receipt(text):
    if not text.strip():
        return None
    fields = {}
    for line in text.splitlines():
        key, separator, value = line.partition('=')
        require(separator and key and key not in fields, 'invalid checkpoint receipt')
        fields[key] = value
    for key in ('batch_id', 'target', 'manifest_sha256', 'root_inode_id',
                'files', 'directories', 'processed_records'):
        require(key in fields, f'checkpoint receipt is missing {key}')
    for key in ('root_inode_id', 'files', 'directories', 'processed_records'):
        require(fields[key].isdigit(), f'checkpoint receipt has invalid {key}')
        fields[key] = int(fields[key])
    return fields


class Bridge:
    def __init__(self, path):
        self.path = path
        self.lib = ctypes.CDLL(str(path))
        char = ctypes.c_char_p
        size = ctypes.c_size_t
        u64 = ctypes.c_uint64
        try:
            self.lib.zb_lhm_import_batch.argtypes = [
                char, char, char, char, char, u64, u64, u64,
                ctypes.POINTER(ImportResult), ctypes.POINTER(ctypes.c_int), char, size]
            self.lib.zb_lhm_import_batch.restype = ctypes.c_int
            self.lib.zb_lhm_recover_batch.argtypes = [char, char, size]
            self.lib.zb_lhm_recover_batch.restype = ctypes.c_int
            self.lib.zb_lhm_batch_status.argtypes = [char, char, size, char, size]
            self.lib.zb_lhm_batch_status.restype = ctypes.c_int
            self.lib.zb_lhm_open.argtypes = [char]
            self.lib.zb_lhm_open.restype = ctypes.c_void_p
            self.lib.zb_lhm_close_checked.argtypes = [ctypes.c_void_p, char, size]
            self.lib.zb_lhm_close_checked.restype = ctypes.c_int
        except AttributeError as error:
            raise RuntimeError(f'{path} does not contain the batch recovery API; rebuild zb_lhm_bridge') from error

    def init(self, store):
        context = self.lib.zb_lhm_open(os.fsencode(store))
        require(context, f'cannot create fresh LHM store: {store}')
        error = ctypes.create_string_buffer(8192)
        require(self.lib.zb_lhm_close_checked(context, error, len(error)) == 1,
                'initial checkpoint failed: ' + error.value.decode('utf-8', 'replace'))

    def recover(self, store):
        error = ctypes.create_string_buffer(8192)
        code = self.lib.zb_lhm_recover_batch(os.fsencode(store), error, len(error))
        require(code == 0, 'recovery failed: ' + error.value.decode('utf-8', 'replace'))

    def status(self, store):
        error = ctypes.create_string_buffer(8192)
        receipt = ctypes.create_string_buffer(65536)
        code = self.lib.zb_lhm_batch_status(os.fsencode(store), receipt, len(receipt), error, len(error))
        require(code == 0, 'status failed: ' + error.value.decode('utf-8', 'replace'))
        return parse_receipt(receipt.value.decode('utf-8', 'strict'))

    def import_batch(self, store, target, manifest, batch_id, digest, args):
        result = ImportResult()
        already = ctypes.c_int()
        error = ctypes.create_string_buffer(8192)
        code = self.lib.zb_lhm_import_batch(
            os.fsencode(store), target.encode(), os.fsencode(manifest), batch_id.encode(), digest.encode(),
            args.memory_budget_mib << 20, args.expected_files or 0, args.expected_directories or 0,
            ctypes.byref(result), ctypes.byref(already), error, len(error))
        require(code == 0, f'batch {target} failed at manifest line {result.error_line}: '
                + error.value.decode('utf-8', 'replace'))
        return {name: getattr(result, name) for name, _ in ImportResult._fields_}, bool(already.value)

    def compiled_api(self):
        # Legacy recovery commands remain usable with a library pinned before
        # the compiled-template API was introduced.
        char, size, u64 = ctypes.c_char_p, ctypes.c_size_t, ctypes.c_uint64
        try:
            self.lib.zb_lhm_compile_template.argtypes = [char, char, u64, char, size]
            self.lib.zb_lhm_compile_template.restype = ctypes.c_int
            self.lib.zb_lhm_compiled_info.argtypes = [char, ctypes.POINTER(ImportResult), char, size]
            self.lib.zb_lhm_compiled_info.restype = ctypes.c_int
            self.lib.zb_lhm_import_compiled_batch.argtypes = [
                char, char, char, char, char, u64, ctypes.POINTER(ImportResult),
                ctypes.POINTER(ctypes.c_int), char, size]
            self.lib.zb_lhm_import_compiled_batch.restype = ctypes.c_int
            if hasattr(self.lib, 'zb_lhm_import_compiled_batch_profiled'):
                self.lib.zb_lhm_import_compiled_batch_profiled.argtypes = [
                    char, char, char, char, char, u64, ctypes.POINTER(ImportResult),
                    ctypes.POINTER(ctypes.c_int), ctypes.POINTER(CompiledProfile), char, size]
                self.lib.zb_lhm_import_compiled_batch_profiled.restype = ctypes.c_int
        except AttributeError as error:
            raise RuntimeError(f'{self.path} lacks the compiled-template API; rebuild zb_lhm_bridge') from error

    def compile_template(self, manifest, output, budget):
        self.compiled_api()
        error = ctypes.create_string_buffer(8192)
        code = self.lib.zb_lhm_compile_template(
            os.fsencode(manifest), os.fsencode(output), budget, error, len(error))
        require(code == 0, 'template compilation failed: ' + error.value.decode('utf-8', 'replace'))

    def compiled_info(self, template):
        self.compiled_api()
        result, error = ImportResult(), ctypes.create_string_buffer(8192)
        code = self.lib.zb_lhm_compiled_info(os.fsencode(template), ctypes.byref(result), error, len(error))
        require(code == 0, 'invalid compiled template: ' + error.value.decode('utf-8', 'replace'))
        return {name: getattr(result, name) for name, _ in ImportResult._fields_}

    def import_compiled_batch(self, store, target, template, batch_id, digest, args):
        self.compiled_api()
        result, already, error = ImportResult(), ctypes.c_int(), ctypes.create_string_buffer(8192)
        parameters = [os.fsencode(store), target.encode(), os.fsencode(template), batch_id.encode(), digest.encode(),
                      args.memory_budget_mib << 20, ctypes.byref(result), ctypes.byref(already)]
        profile = CompiledProfile()
        shared = getattr(args, 'shared_template_index', False)
        symbol = 'zb_lhm_import_shared_compiled_batch_profiled' if shared else 'zb_lhm_import_compiled_batch_profiled'
        require(not shared or hasattr(self.lib, symbol), 'library lacks shared-template API; rebuild zb_lhm_bridge')
        profiled = hasattr(self.lib, symbol)
        if profiled:
            function = getattr(self.lib, symbol)
            function.argtypes = [ctypes.c_char_p] * 5 + [ctypes.c_uint64, ctypes.POINTER(ImportResult),
                ctypes.POINTER(ctypes.c_int), ctypes.POINTER(CompiledProfile), ctypes.c_char_p, ctypes.c_size_t]
            function.restype = ctypes.c_int
            code = function(
                *parameters, ctypes.byref(profile), error, len(error))
        else:
            code = self.lib.zb_lhm_import_compiled_batch(*parameters, error, len(error))
        require(code == 0, f'compiled batch {target} failed: ' + error.value.decode('utf-8', 'replace'))
        fields = {name: getattr(result, name) for name, _ in ImportResult._fields_}
        if profiled:
            fields['profile'] = profile.as_dict()
        return fields, bool(already.value)


class RunLock:
    def __init__(self, store, shared=False):
        self.path = Path(str(store) + '.batch-runner.lock')
        self.shared = shared
        self.stream = None

    def __enter__(self):
        self.stream = self.path.open('a+b')
        try:
            fcntl.flock(self.stream.fileno(), (fcntl.LOCK_SH if self.shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.stream.close()
            raise RuntimeError(f'another batch runner is using this store: {self.path}') from error
        return self

    def __exit__(self, *unused):
        self.stream.close()


class AuditLog:
    def __init__(self, path):
        self.path = path

    def write(self, event, **values):
        record = dict(time=datetime.datetime.now(datetime.timezone.utc).isoformat(), event=event, **values)
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        existed = self.path.exists()
        with self.path.open('a', encoding='utf-8') as stream:
            stream.write(line + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        if not existed:
            sync_directory(self.path.parent)
        print(line, flush=True)


def plan_key(store, digest, args):
    # copies is deliberately excluded so a completed series can be extended.
    plan = dict(format_version=1, store=str(store), manifest_sha256=digest,
                prefix=args.prefix, expected_files=args.expected_files or 0,
                expected_directories=args.expected_directories or 0)
    if getattr(args, 'shared_template_index', False):
        plan['index_mode'] = 'shared-readonly-v6'
    encoded = json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()


def batch_key(plan, index):
    return hashlib.sha256(f'{plan}:{index}'.encode()).hexdigest()


def next_index(receipt, plan, digest, args):
    if receipt is None:
        return 1
    match = re.fullmatch(re.escape(args.prefix) + r'-(\d{6,})', receipt['target'])
    if match:
        index = int(match.group(1))
        require(index > 0 and receipt['target'] == f'{args.prefix}-{index:06d}',
                'checkpoint target is not a canonical batch name')
        require(receipt['manifest_sha256'] == digest and receipt['batch_id'] == batch_key(plan, index),
                'this prefix belongs to a different import plan; keep the original manifest and count arguments')
        return index + 1
    require(args.start_new_series,
            f"latest checkpoint belongs to {receipt['target']}; use its original import command to resume, "
            'or use a different --prefix with --start-new-series')
    return 1


def run_import(args, bridge, store):
    compiled = args.compiled_template is not None
    require(not args.shared_template_index or compiled, '--shared-template-index requires --compiled-template')
    source = (args.compiled_template if compiled else args.manifest).resolve(strict=True)
    manifest = source / 'header.bin' if compiled else source
    require(manifest.is_file(), f'input is not a regular file: {manifest}')
    require(re.fullmatch(r'/[A-Za-z0-9][A-Za-z0-9_.-]*', args.prefix),
            '--prefix must be one absolute ASCII directory name, for example /Server')
    require(len(f'{args.prefix[1:]}-{args.copies:06d}'.encode()) <= 64,
            'batch directory name exceeds the LHM 64-byte component limit')
    require(args.memory_budget_mib <= ((1 << 64) - 1) >> 20, '--memory-budget-mib is too large')
    log = AuditLog(args.log.resolve() if args.log else Path(str(store) + '.batch-log.jsonl'))
    require(log.path.parent.is_dir(), f'log directory does not exist: {log.path.parent}')
    protected = [manifest, bridge.path, store] + [Path(str(store) + suffix) for suffix in (
        '.heads', '.buckets', '.snapshot', '.snapshot.tmp', '.snapshot.batch.tmp', '.undo', '.undo.init',
        '.batch-receipt', '.batch-receipt.tmp', '.batch-runner.lock')]
    protected.extend(segment_paths(store))
    require(not reserved_segment_name(store, log.path),
            'audit log must not use a reserved segment or segment temporary filename')
    if compiled:
        protected.extend(source.iterdir())
    for path in protected:
        same_file = log.path.exists() and path.exists() and os.path.samefile(log.path, path)
        require(log.path != path.resolve() and not same_file,
                f'audit log must be separate from input and database files: {log.path}')
    # Recovery precedes status: a crash may have published the new snapshot but
    # not its receipt. The C API completes that publication before we choose N+1.
    bridge.recover(store)
    print(f'Computing input SHA256: {manifest}', flush=True)
    digest, identity = manifest_digest(manifest)
    if compiled:
        info = bridge.compiled_info(source)
        require(file_identity(manifest) == identity, 'compiled header changed while reading counts')
        for attribute, field in (('expected_files', 'files'), ('expected_directories', 'directories')):
            supplied = getattr(args, attribute)
            require(supplied is None or supplied == info[field],
                    f'--{attribute.replace("_", "-")} differs from compiled header ({info[field]})')
            setattr(args, attribute, info[field])
    plan = plan_key(store, digest, args)
    receipt = bridge.status(store)
    start = next_index(receipt, plan, digest, args)
    log.write('plan', store=str(store), manifest=str(manifest), manifest_sha256=digest,
              input_mode='compiled' if compiled else 'manifest',
              prefix=args.prefix, copies=args.copies, first_pending=start, plan=plan)
    if start > args.copies:
        log.write('complete', completed_copies=start - 1, requested_copies=args.copies)
        return
    for index in range(start, args.copies + 1):
        require(file_identity(manifest) == identity, 'manifest changed since SHA256 validation')
        free = shutil.disk_usage(store.parent).free
        estimated_write = 0
        if compiled:
            # Physical new pages plus room for a new accumulated snapshot and
            # temporary publication. Sparse logical EOF is not disk usage.
            estimated_write = sum((source / name).stat().st_size for name in (
                'inode.pages', 'heads.pages', 'bucket.pages'))
            estimated_write += 2 * Path(str(store) + '.snapshot').stat().st_size
            estimated_write += 4 * (source / 'routes.bin').stat().st_size
        require(free >= estimated_write + args.min_free_gib * (1 << 30),
                f'free space is {free / (1 << 30):.2f} GiB; batch estimate '
                f'{estimated_write / (1 << 30):.2f} GiB plus --min-free-gib={args.min_free_gib} required')
        target = f'{args.prefix}-{index:06d}'
        batch_id = batch_key(plan, index)
        started = time.monotonic()
        log.write('batch_start', index=index, target=target, batch_id=batch_id,
                  free_bytes=free, estimated_write_bytes=estimated_write)
        try:
            if compiled:
                result, already = bridge.import_compiled_batch(store, target, source, batch_id, digest, args)
            else:
                result, already = bridge.import_batch(store, target, manifest, batch_id, digest, args)
            require(file_identity(manifest) == identity, 'manifest changed during import; inspect the committed receipt')
            # A successful C API return already includes durable receipt
            # publication. Avoid another full snapshot scan after every batch.
            log.write('batch_committed', index=index, target=target, batch_id=batch_id,
                      elapsed_seconds=round(time.monotonic() - started, 3),
                      already_committed=already, **result)
        except BaseException as error:
            try:
                log.write('batch_interrupted', index=index, target=target, error=str(error),
                          elapsed_seconds=round(time.monotonic() - started, 3))
            except OSError as log_error:
                print(f'Cannot append failure log: {log_error}; original failure: {error}', file=sys.stderr)
            raise
    log.write('complete', completed_copies=args.copies, requested_copies=args.copies)


def parser():
    command = argparse.ArgumentParser(description=__doc__)
    commands = command.add_subparsers(dest='command', required=True)
    compile_command = commands.add_parser('compile', help='compile one grouped TSV manifest into reusable LHM pages')
    compile_command.add_argument('--manifest', required=True, type=Path)
    compile_command.add_argument('--output', required=True, type=Path, help='new compiled-template directory')
    compile_command.add_argument('--library', type=Path)
    compile_command.add_argument('--memory-budget-mib', type=positive, default=256,
                                 help='compiler working budget; not a whole-process RSS cap')
    for name, help_text in (
            ('init', 'create a fresh standalone LHM store and its empty checkpoint'),
            ('status', 'show the last committed batch receipt'),
            ('recover', 'recover an interrupted batch without importing a new one'),
            ('import', 'import or resume a series of manifest copies')):
        sub = commands.add_parser(name, help=help_text)
        sub.add_argument('--store', required=True, type=Path, help='absolute LHM_STORE_FILE path')
        sub.add_argument('--library', type=Path, help='path to the rebuilt libzb_lhm_bridge.so')
        if name == 'import':
            inputs = sub.add_mutually_exclusive_group(required=True)
            inputs.add_argument('--manifest', type=Path)
            inputs.add_argument('--compiled-template', type=Path, help='directory produced by compile')
            sub.add_argument('--shared-template-index', action='store_true', help='v6 shared index; namespace becomes read-only; fresh/shared store required')
            sub.add_argument('--copies', type=positive, default=1, help='total desired copies, including completed copies')
            sub.add_argument('--prefix', required=True, help='new top-level name prefix, for example /Server')
            sub.add_argument('--expected-files', type=positive, help='required file count per copy')
            sub.add_argument('--expected-directories', type=positive, help='required directory count per copy, including template root')
            sub.add_argument('--memory-budget-mib', type=positive, default=256,
                             help='parser or compiled import working budget; not a whole-process RSS cap')
            sub.add_argument('--min-free-gib', type=nonnegative_float, default=1, help='stop before a batch below this free-space threshold')
            sub.add_argument('--start-new-series', action='store_true', help='allow a new prefix after a different series')
            sub.add_argument('--log', type=Path, help='append-only JSONL audit log (parent directory must exist)')
    return command


def main():
    args = parser().parse_args()
    if args.command == 'compile':
        require(args.memory_budget_mib <= ((1 << 64) - 1) >> 20, '--memory-budget-mib is too large')
        manifest, output = args.manifest.resolve(strict=True), args.output.resolve()
        require(manifest.is_file(), f'manifest is not a regular file: {manifest}')
        require(output.parent.is_dir(), f'output parent directory does not exist: {output.parent}')
        require(not os.path.lexists(output), f'compiled output already exists: {output}')
        bridge = Bridge(library_path(args.library))
        started = time.monotonic()
        bridge.compile_template(manifest, output, args.memory_budget_mib << 20)
        digest, _ = manifest_digest(output / 'header.bin')
        print(json.dumps(dict(event='template_compiled', template=str(output), header_sha256=digest,
                              elapsed_seconds=round(time.monotonic() - started, 3),
                              **bridge.compiled_info(output)), ensure_ascii=False), flush=True)
        return 0
    store = args.store.resolve()
    require(store.parent.is_dir(), f'store parent directory does not exist: {store.parent}')
    bridge = Bridge(library_path(args.library))
    # Fresh creation uses the service API, which takes its own shared runner
    # lock. O_EXCL on the main file arbitrates concurrent initializers; either
    # shared lock still excludes offline import for the entire initialization.
    with RunLock(store, shared=args.command == 'init'):
        if args.command == 'init':
            require(not list(segment_paths(store)), f'initialization requires no existing segments: {store}')
            for suffix in ('', '.heads', '.buckets', '.snapshot', '.snapshot.tmp', '.snapshot.batch.tmp',
                           '.undo', '.undo.init', '.batch-receipt', '.batch-receipt.tmp'):
                require(not os.path.lexists(str(store) + suffix), f'initialization requires a fresh store: {store}{suffix}')
            bridge.init(store)
            print(json.dumps(dict(event='initialized', store=str(store),
                                  scope='LHM only; no MDS RocksDB control store was created')), flush=True)
        else:
            require(store.is_file(), f'LHM store does not exist: {store}')
            if args.command == 'import':
                run_import(args, bridge, store)
            else:
                if args.command == 'recover':
                    bridge.recover(store)
                print(json.dumps(dict(store=str(store), last_committed=bridge.status(store)),
                                 ensure_ascii=False, indent=2), flush=True)
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print('Interrupted. Restart the same import command to recover and resume.', file=sys.stderr)
        sys.exit(130)
    except (RuntimeError, OSError, ValueError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        sys.exit(1)
