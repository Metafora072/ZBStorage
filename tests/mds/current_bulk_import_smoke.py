#!/usr/bin/env python3
"""Manual integration checks; requires the four built current-layout MDS tools."""
import argparse
import json
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new directory outside the source tree')
    parser.add_argument('--build-dir', type=Path, default=REPO / 'build')
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    driver = REPO / 'scripts/import_current_mds_metadata.py'
    checks = []

    def run(root, *extra, ok=True, message=None, total=1001):
        command = [sys.executable, str(driver), f'--build-dir={args.build_dir}',
                   f'--root={root}', f'--total-files={total}', '--batch-files=600',
                   '--reserve-bytes=0', *extra]
        result = subprocess.run(command, text=True, capture_output=True)
        with (output / 'commands.log').open('a') as log:
            log.write(json.dumps(command) + '\n' + result.stdout + result.stderr)
        assert (result.returncode == 0) == ok, result.stdout + result.stderr
        if message:
            assert message in result.stdout + result.stderr, result.stdout + result.stderr
        return result

    root = output / 'dataset'
    run(root, '--max-batches=1', message='BATCH_LIMIT_REACHED verified_files=600')
    state_path = root / 'bulk_state.json'
    state = json.loads(state_path.read_text())
    assert len(state['verified']) == 1
    first = root / 'masstree_meta/namespaces/current-ns-000001/generations/gen-000001/manifest.txt'
    before = first.read_bytes(), first.stat().st_mtime_ns
    checks.append('fresh database uses current layout; first-batch limit')

    # Published data exists, but the controller died before recording success.
    state['verified'] = {}
    state['phase'] = {'kind': 'import', 'namespace_id': 'current-ns-000001'}
    state_path.write_text(json.dumps(state))
    run(root, message='COMPLETE verified_files=1001')
    assert (first.read_bytes(), first.stat().st_mtime_ns) == before
    state = json.loads(state_path.read_text())
    assert sorted(v['file_count'] for v in state['verified'].values()) == [401, 600]
    checks.append('lost completion receipt recovered without replay; exact remainder batch')
    result = run(root, message='COMPLETE verified_files=1001')
    assert 'importing ' not in result.stdout
    checks.append('completed task is idempotent')

    run(root, ok=False, total=1002, message='task parameters or binaries changed')
    checks.append('changed task rejected')
    original = first.read_bytes()
    try:
        first.write_bytes(original.replace(b'file_count=600\n', b'file_count=599\n'))
        run(root, ok=False, message='wrong workload')
    finally:
        first.write_bytes(original)
    checks.append('damaged publication rejected')

    limited = output / 'space-limited'
    run(limited, '--reserve-bytes=999999999999999999', ok=False, message='insufficient space')
    assert not list((limited / 'masstree_meta/templates').iterdir())
    run(limited, message='COMPLETE verified_files=1001')
    checks.append('space check precedes generation; retry after budget correction')

    foreign = output / 'foreign'
    foreign.mkdir()
    (foreign / 'do-not-touch').write_text('existing data')
    run(foreign, ok=False, message='new task requires an empty root')
    assert (foreign / 'do-not-touch').read_text() == 'existing data'
    checks.append('existing unowned data root rejected')

    # Stop before generation, then simulate an interrupted task-owned template.
    partial = output / 'partial'
    run(partial, '--reserve-bytes=999999999999999999', ok=False, message='insufficient space')
    partial_state = partial / 'bulk_state.json'
    state = json.loads(partial_state.read_text())
    state['phase'] = {'kind': 'template', 'template_id': 'template-600'}
    partial_state.write_text(json.dumps(state))
    interrupted = partial / 'masstree_meta/templates/template-600/template.staging'
    interrupted.mkdir(parents=True)
    (interrupted / 'inode.records').write_bytes(b'incomplete')
    run(partial, message='COMPLETE verified_files=1001')
    checks.append('owned incomplete template regenerated')

    # The historical rule treats extensionless leaves as directories. Whole-tree
    # replication rounds each batch up, including the final partial batch.
    source = output / 'tree.txt'
    source.write_text('a/x.txt\na/y.bin\na/empty\nb/z.dat\nb/w.log\ne.dat\nf.dat\ng.dat\n')
    path_root = output / 'path-list'
    options = ('--source-mode=path_list', f'--path-list-file={source}', '--template-id=old-template')
    run(path_root, *options, '--max-batches=1', message='verified_files=602')
    run(path_root, *options, message='COMPLETE verified_files=1008')
    pstate = json.loads((path_root / 'bulk_state.json').read_text())
    assert pstate['plan']['expected_files'] == 1008
    assert pstate['plan']['summaries']['600']['base_dir_count'] == '3'
    assert sorted(v['file_count'] for v in pstate['verified'].values()) == [406, 602]
    result = run(path_root, *options, message='COMPLETE verified_files=1008')
    assert 'importing ' not in result.stdout
    checks.append('old path-list leaf rule; whole-tree rounding; remainder and idempotent resume')
    source.write_text(source.read_text() + 'h.dat\n')
    run(path_root, *options, ok=False, message='task parameters or binaries changed')
    checks.append('changed path source rejected')
    run(output / 'bad-fingerprint', *options, '--expected-fingerprint=wrong', ok=False,
        message='structure differs from expected fingerprint')
    checks.append('historical structure fingerprint checked before generation')
    (output / 'summary.json').write_text(json.dumps({'passed': checks}, indent=2) + '\n')
    print(json.dumps({'passed': checks}, indent=2))


if __name__ == '__main__':
    main()
