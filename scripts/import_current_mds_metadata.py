#!/usr/bin/env python3
"""Offline, resumable metadata import into a dedicated current-layout DB."""

import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, data):
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w') as out:
        json.dump(data, out, indent=2, sort_keys=True)
        out.write('\n')
        out.flush()
        os.fsync(out.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def fields(text):
    return dict(line.split('=', 1) for line in text.splitlines() if '=' in line)


def positive(value):
    n = int(value)
    if n <= 0:
        raise argparse.ArgumentTypeError('must be positive')
    return n


class ImportTask:
    def __init__(self, args):
        self.args = args
        self.root = args.root.resolve()
        self.meta = self.root / 'masstree_meta'
        self.state_path = self.root / 'bulk_state.json'
        self.lock = None
        self.tools = {name: args.build_dir.resolve() / name for name in (
            'mds_server', 'masstree_meta_generate_tool', 'masstree_meta_import_tool',
            'masstree_meta_build_tool')}

    def save(self):
        atomic_json(self.state_path, self.state)

    def command(self, name, options, log_name=None):
        cmd = [str(self.tools[name])] + [f'--{k}={v}' for k, v in options.items()]
        log = (self.root / 'logs' / log_name).open('a') if log_name else None
        try:
            child = subprocess.Popen(cmd, stdout=log or subprocess.PIPE,
                                     stderr=subprocess.STDOUT if log else subprocess.PIPE,
                                     text=True, start_new_session=True,
                                     pass_fds=(self.lock.fileno(),))
            try:
                stdout, stderr = child.communicate()
            except BaseException:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                raise
            require(child.returncode == 0,
                    f'{name} failed ({child.returncode}); ' +
                    (f'see {log.name}' if log else (stderr or stdout or 'no output')))
            return stdout
        finally:
            if log:
                log.close()

    def batch(self, index):
        count = min(self.args.batch_files, self.args.total_files - index * self.args.batch_files)
        template_id = self.args.template_id or f'template-{count}'
        if self.args.template_id and count != self.args.batch_files:
            template_id += f'-tail-{count}'
        actual = int(self.summaries[str(count)]['actual_file_count']) if self.summaries else count
        return dict(namespace_id=f'{self.args.prefix}-{index + 1:06d}',
                    generation_id=f'gen-{index + 1:06d}', file_count=actual,
                    target_file_count=count, template_id=template_id)

    def source_options(self):
        if self.args.source_mode == 'synthetic':
            return dict(source_mode='synthetic')
        return dict(source_mode='path_list', path_list_file=self.args.path_list_file.resolve(),
                    repeat_dir_prefix=self.args.repeat_dir_prefix,
                    path_list_leaf_nodes_are_files=int(self.args.path_list_leaf_nodes_are_files))

    def prepare_workload(self, plan):
        self.summaries = {}
        self.expected_files = self.args.total_files
        if self.args.template_id:
            require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', self.args.template_id), 'invalid template ID')
            plan['template_id'] = self.args.template_id
        if self.args.source_mode == 'synthetic':
            require(not self.args.path_list_file and not self.args.expected_fingerprint and
                    not self.args.path_list_leaf_nodes_are_files, 'path_list options require --source-mode=path_list')
            return
        require(self.args.path_list_file and self.args.path_list_file.is_file(), 'path list file required')
        require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', self.args.repeat_dir_prefix), 'invalid repeat directory prefix')
        source_sha = digest(self.args.path_list_file)
        full, tail = divmod(self.args.total_files, self.args.batch_files)
        counts = ({self.args.batch_files} if full else set()) | ({tail} if tail else set())
        for count in sorted(counts):
            summary = fields(self.command('masstree_meta_generate_tool',
                                         dict(self.source_options(), summarize=1, file_count=count)))
            require(int(summary['actual_file_count']) > 0, 'empty path list workload')
            require(not self.args.expected_fingerprint or summary['fingerprint'] == self.args.expected_fingerprint,
                    'path list structure differs from expected fingerprint')
            self.summaries[str(count)] = summary
            print(f"path_list target={count} actual={summary['actual_file_count']} "
                  f"directories={summary['dir_count']} copies={summary['repeat_count']} "
                  f"fingerprint={summary['fingerprint']}", flush=True)
        require(digest(self.args.path_list_file) == source_sha, 'path list changed during preflight')
        self.expected_files = sum((full if count == self.args.batch_files else 1) *
                                  int(self.summaries[str(count)]['actual_file_count']) for count in counts)
        plan.update(source_mode='path_list', path_list_file=str(self.args.path_list_file.resolve()),
                    path_list_sha256=source_sha, repeat_dir_prefix=self.args.repeat_dir_prefix,
                    path_list_leaf_nodes_are_files=self.args.path_list_leaf_nodes_are_files,
                    summaries=self.summaries, expected_files=self.expected_files)

    def manifest_path(self, batch):
        return (self.meta / 'namespaces' / batch['namespace_id'] / 'generations' /
                batch['generation_id'] / 'manifest.txt')

    def load_manifest(self, path, count):
        require(path.is_file(), f'missing manifest: {path}')
        m = fields(path.read_text())
        require(m.get('optical_layout_version') == 'uniform_2tb_v2', f'wrong layout: {path}')
        require(int(m['file_count']) == count and m['source_mode'] == self.args.source_mode,
                f'wrong workload: {path}')
        if self.summaries:
            summary = self.summaries.get(m['target_file_count'])
            require(summary is not None, f'unexpected target count: {path}')
            for manifest_key, summary_key in (
                    ('file_count', 'actual_file_count'), ('inode_count', 'inode_count'),
                    ('dentry_count', 'dentry_count'), ('dir_count', 'dir_count'),
                    ('template_base_file_count', 'base_file_count'), ('template_base_dir_count', 'base_dir_count'),
                    ('template_repeat_count', 'repeat_count'), ('path_list_fingerprint', 'fingerprint'),
                    ('repeat_dir_prefix', 'repeat_dir_prefix')):
                require(m[manifest_key] == summary[summary_key], f'path list workload changed: {path}: {manifest_key}')
        require(int(m['max_files_per_leaf_dir']) == self.args.files_per_leaf and
                int(m['max_subdirs_per_dir']) == self.args.subdirs_per_dir,
                f'directory layout changed: {path}')
        for key in ('inode_pages_path', 'dentry_pages_path', 'inode_sparse_index_path',
                    'dentry_sparse_index_path', 'optical_layout_path'):
            file = Path(m[key])
            require(file.resolve().parent == path.parent.resolve() and file.is_file() and
                    file.stat().st_size > 0, f'missing or external page/index: {file}')
        return m

    def inspect(self):
        return json.loads(self.command('masstree_meta_build_tool',
                                      dict(db_path=self.root / 'db', inspect=1)))

    def audit(self):
        for template_id, sha in self.state['templates'].items():
            path = self.meta / 'templates' / template_id / 'template.staging' / 'manifest.txt'
            require(path.is_file() and digest(path) == sha, f'template changed or missing: {template_id}')
        snapshot = self.inspect()
        require(snapshot['optical_layout_version'] == 2, 'database is not uniform_2tb_v2')
        routes = snapshot['routes']
        by_ns = {r['namespace_id']: r for r in routes}
        require(len(by_ns) == len(routes), 'duplicate namespace routes')
        expected = [self.batch(i) for i in range(len(routes))]
        require(len(routes) <= self.batch_count and set(by_ns) == {b['namespace_id'] for b in expected},
                'unexpected namespace or gap in published batches')
        stats = snapshot['namespace_stats']
        require(set(stats) == {f"MTS/namespace/{b['namespace_id']}/current" for b in expected},
                'namespace stats/routes disagree; inspect interrupted publication before retrying')
        total_files = total_bytes = metadata_bytes = 0
        end_cursor = '0,0,0,0'
        last_inode = 1
        verified = {}
        for batch in expected:
            ns = batch['namespace_id']
            route = by_ns[ns]
            path = self.manifest_path(batch)
            m = self.load_manifest(path, batch['file_count'])
            s = fields(stats[f'MTS/namespace/{ns}/current'])
            require(route['generation_id'] == m['generation_id'] == s['generation_id'] == batch['generation_id']
                    and m['namespace_id'] == s['namespace_id'] == ns
                    and route['manifest_path'] == str(path)
                    and route['path_prefix'] == m['path_prefix'] == '/masstree_demo/' + ns
                    and m['template_id'] == batch['template_id'], f'publication mismatch: {ns}')
            require(int(s['file_count']) == batch['file_count'] and s['optical_layout_version'] == '2',
                    f'statistics mismatch: {ns}')
            require(int(m['inode_min']) == route['inode_min'] > last_inode and
                    int(m['inode_max']) == route['inode_max'], f'overlapping inode range: {ns}')
            last_inode = route['inode_max']
            cursor = lambda which: ','.join(m[f'{which}_cursor_{part}'] for part in
                                            ('node_index', 'disk_index', 'image_index', 'image_used_bytes'))
            require(s['start_cursor'] == cursor('start') == end_cursor and
                    s['end_cursor'] == cursor('end'), f'allocation cursor mismatch: {ns}')
            end_cursor = s['end_cursor']
            inode_bytes = Path(m['inode_pages_path']).stat().st_size
            require(s['total_file_bytes'] == m['total_file_bytes'] and
                    int(s['total_metadata_bytes']) == inode_bytes, f'byte statistics mismatch: {ns}')
            sha = digest(path)
            old = self.state['verified'].get(ns)
            require(old is None or old['manifest_sha256'] == sha, f'verified manifest changed: {ns}')
            verified[ns] = dict(file_count=batch['file_count'], manifest_sha256=sha)
            total_files += batch['file_count']
            total_bytes += int(s['total_file_bytes'])
            metadata_bytes += inode_bytes
        require(snapshot['file_count'] == total_files and int(snapshot['total_file_bytes']) == total_bytes and
                int(snapshot['total_metadata_bytes']) == metadata_bytes and snapshot['cursor'] == end_cursor,
                'cluster statistics do not match published batches; refusing automatic replay')
        require(set(self.state['verified']) <= set(verified), 'a previously verified namespace disappeared')
        self.state['verified'] = verified
        self.save()
        return len(routes), total_files

    def space(self, remaining_files, template_files=0):
        # Initial conservative budget. Samples can raise it; never reduce it below the configured estimate.
        estimate = self.args.bytes_per_file
        for summary in self.summaries.values():
            estimate = max(estimate, math.ceil(1.25 * (256 * int(summary['inode_count']) +
                           96 * int(summary['dentry_count'])) / int(summary['actual_file_count'])))
        for ns in self.state['verified']:
            index = int(ns.rsplit('-', 1)[1]) - 1
            directory = self.manifest_path(self.batch(index)).parent
            size = sum(p.stat().st_size for p in directory.iterdir() if p.is_file())
            estimate = max(estimate, math.ceil(1.25 * size / self.state['verified'][ns]['file_count']))
        needed = (remaining_files + 2 * template_files + self.args.batch_files) * estimate + self.args.reserve_bytes
        available = shutil.disk_usage(self.root).free
        require(available >= needed, f'insufficient space: available={available}, required={needed} bytes')
        print(f'space available={available} required={needed} bytes', flush=True)

    def remove_owned(self, path):
        require(path.resolve().is_relative_to(self.meta) and not path.is_symlink(),
                f'refusing cleanup outside this task: {path}')
        if path.exists():
            shutil.rmtree(path)

    def template(self, batch, remaining):
        template_id = batch['template_id']
        directory = self.meta / 'templates' / template_id
        path = directory / 'template.staging' / 'manifest.txt'
        receipt = self.state['templates'].get(template_id)
        if receipt:
            self.load_manifest(path, batch['file_count'])
            require(digest(path) == receipt, f'template changed: {template_id}')
            return
        phase = dict(kind='template', template_id=template_id)
        if directory.exists():
            require(self.state['phase'] == phase, f'untracked template exists: {directory}')
            # Only an unpublished template generated by this fixed task may be retried.
            require(not any(self.batch(int(ns.rsplit('-', 1)[1]) - 1)['template_id'] == template_id
                            for ns in self.state['verified']), 'published namespace references incomplete template')
            self.remove_owned(directory)
        self.space(remaining, batch['file_count'])
        self.state['phase'] = phase
        self.save()
        if self.summaries:
            require(digest(self.args.path_list_file) == self.state['plan']['path_list_sha256'],
                    'path list changed before generation')
        self.command('masstree_meta_generate_tool', dict(self.source_options(),
            output_root=self.meta / 'templates', namespace_id=template_id, generation_id='template',
            path_prefix='/__masstree_template__/' + template_id, inode_start=1,
            file_count=batch['target_file_count'], max_files_per_leaf_dir=self.args.files_per_leaf,
            max_subdirs_per_dir=self.args.subdirs_per_dir), template_id + '.generate.log')
        if self.summaries:
            require(digest(self.args.path_list_file) == self.state['plan']['path_list_sha256'],
                    'path list changed during generation')
        self.command('masstree_meta_import_tool', dict(manifest_path=path, verify_inode_samples=128,
                                                      verify_dentry_samples=128), template_id + '.pages.log')
        self.load_manifest(path, batch['file_count'])
        self.state['templates'][template_id] = digest(path)
        self.state['phase'] = None
        self.save()

    def run(self):
        require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]*', self.args.prefix), 'invalid namespace prefix')
        require(self.args.reserve_bytes >= 0 and self.args.max_batches >= 0, 'negative limit')
        require(self.args.files_per_leaf < 2**32 and self.args.subdirs_per_dir < 2**32 and
                self.args.total_files < 2**63 and self.args.batch_files < 2**63, 'count exceeds supported range')
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / '.bulk-import.lock').open('a')
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for name in ('db', 'masstree_meta', 'bulk_state.json', 'logs'):
            require(not (self.root / name).is_symlink(), f'symlink not allowed: {name}')
        for tool in self.tools.values():
            require(tool.is_file() and os.access(tool, os.X_OK), f'build required: {tool}')
        plan = dict(schema=1, root=str(self.root), total_files=self.args.total_files,
                    batch_files=self.args.batch_files, prefix=self.args.prefix,
                    files_per_leaf=self.args.files_per_leaf, subdirs_per_dir=self.args.subdirs_per_dir,
                    binary_sha256={name: digest(path) for name, path in self.tools.items()})
        # Refuse foreign roots before even reading a potentially large source tree.
        require(self.state_path.exists() or set(p.name for p in self.root.iterdir()) == {'.bulk-import.lock'},
                'new task requires an empty root; refusing to adopt an existing DB')
        self.prepare_workload(plan)
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text())
            require(self.state['plan'] == plan, 'task parameters or binaries changed; use a new root')
        else:
            require(set(p.name for p in self.root.iterdir()) == {'.bulk-import.lock'},
                    'new task requires an empty root; refusing to adopt an existing DB')
            self.state = dict(plan=plan, phase=None, templates={}, verified={})
            self.save()
        (self.root / 'logs').mkdir(exist_ok=True)
        (self.meta / 'templates').mkdir(parents=True, exist_ok=True)
        self.batch_count = (self.args.total_files + self.args.batch_files - 1) // self.args.batch_files
        completed, total = self.audit()
        imported = 0
        while completed < self.batch_count and (not self.args.max_batches or imported < self.args.max_batches):
            batch = self.batch(completed)
            self.space(self.expected_files - total)
            self.template(batch, self.expected_files - total)
            phase = dict(kind='import', namespace_id=batch['namespace_id'])
            staging = self.meta / 'staging' / batch['namespace_id'] / (batch['generation_id'] + '.staging')
            require(not self.manifest_path(batch).parent.exists(), 'unpublished final generation exists; inspect it first')
            if staging.exists():
                require(self.state['phase'] == phase, 'untracked staging directory; inspect it first')
                self.remove_owned(staging)
            self.state['phase'] = phase
            self.save()
            started = time.monotonic()
            print(f"importing {batch['namespace_id']} files={batch['file_count']}", flush=True)
            self.command('masstree_meta_build_tool', dict(
                db_path=self.root / 'db', masstree_root=self.meta,
                namespace_id=batch['namespace_id'], generation_id=batch['generation_id'],
                path_prefix='/masstree_demo/' + batch['namespace_id'], template_id=batch['template_id'],
                template_mode='page_fast', verify_inode_samples=128, verify_dentry_samples=128,
                publish_route=1), batch['namespace_id'] + '.import.log')
            previous = completed
            completed, total = self.audit()
            require(completed == previous + 1, 'import did not publish exactly one batch')
            self.state['phase'] = None
            self.save()
            imported += 1
            print(f'verified batches={completed}/{self.batch_count} files={total} '
                  f'elapsed_seconds={time.monotonic()-started:.2f}', flush=True)
        if completed == self.batch_count:
            self.state['phase'] = None
            self.save()
        print(f"{'COMPLETE' if completed == self.batch_count else 'BATCH_LIMIT_REACHED'} "
              f'verified_files={total} target_files={self.args.total_files} expected_files={self.expected_files}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True, help='new dedicated data root, separate from the retained old DB')
    parser.add_argument('--build-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'build')
    parser.add_argument('--total-files', type=positive, required=True)
    parser.add_argument('--batch-files', type=positive, default=100000000)
    parser.add_argument('--prefix', default='current-ns')
    parser.add_argument('--source-mode', choices=('synthetic', 'path_list'), default='synthetic')
    parser.add_argument('--path-list-file', type=Path)
    parser.add_argument('--repeat-dir-prefix', default='copy')
    parser.add_argument('--path-list-leaf-nodes-are-files', action='store_true',
                        help='treat extensionless leaves as files; leave unset to match the old workload')
    parser.add_argument('--template-id', help='stable template name, also used by deterministic file size generation')
    parser.add_argument('--expected-fingerprint', help='reject a path tree that differs from this structural fingerprint')
    parser.add_argument('--files-per-leaf', type=positive, default=2048)
    parser.add_argument('--subdirs-per-dir', type=positive, default=256)
    parser.add_argument('--bytes-per-file', type=positive, default=512, help='initial capacity budget, raised by measured batches')
    parser.add_argument('--reserve-bytes', type=int, default=1024**4, help='space kept free in addition to the remaining workload')
    parser.add_argument('--max-batches', type=int, default=0, help='new batches this invocation; 0 means all remaining')
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    task = ImportTask(args)
    try:
        task.run()
    except KeyboardInterrupt:
        print('interrupted; rerun the identical task to inspect and resume', file=sys.stderr)
        return 130
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        print(f'import stopped: {error}', file=sys.stderr)
        return 1
    finally:
        if task.lock:
            task.lock.close()
    return 0


if __name__ == '__main__':
    sys.exit(main())
