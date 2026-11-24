#!/usr/bin/env python3
"""Manual incremental-index append/recovery checks; isolated temporary stores only."""
import argparse
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
import resource

from lhm_batch_recovery_test import (OK, assert_snapshot_files, checkpoint_indexes,
                                     digest, require, fnv, parse_segment_layout)
from lhm_compiled_import_test import Suite as CompiledSuite, worker as compiled_worker


def worker(request):
    if request['action'] != 'many_incremental':
        return compiled_worker(request)
    timings = []; identifiers = set()
    for number in range(1, request['batches']+1):
        started = time.monotonic()
        result = compiled_worker(dict(request, action='compiled_import',
            target=f'/scale-{number:04d}', batch_id=f'scale-{number:04d}'))
        require(result['rc'] == OK and not result['already'], result['error'])
        identifier = result['result']['root_inode_id']
        require(identifier not in identifiers, 'duplicate root inode across batches')
        identifiers.add(identifier); timings.append(time.monotonic()-started)
    paths = [f'/scale-{number:04d}/f' for number in range(1, request['batches']+1)]
    snapshot = Path(request['store']+'.snapshot').read_bytes()
    result = compiled_worker(dict(request, action='peek', paths=paths))
    require(result['opened'], result.get('error', 'final reopen failed'))
    require(all(v['rc'] == OK and v['size'] == 123 for v in result['values'].values()),
            'final multi-batch query mismatch')
    require(len({v['inode_id'] for v in result['values'].values()}) == len(paths), 'duplicate file inodes')
    require(Path(request['store']+'.snapshot').read_bytes() == snapshot, 'read-only close changed checkpoint')
    return dict(batches=request['batches'], queried_files=len(paths),
        first_100_seconds=sum(timings[:100]), last_100_seconds=sum(timings[-100:]),
        total_seconds=sum(timings), peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)


def index_state(store):
    result = {}
    for entry in checkpoint_indexes(store):
        p = Path(entry['path']); s = p.stat()
        result[p.name] = (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns,
                          s.st_ctime_ns, digest(p.read_bytes()))
    return result


class Suite(CompiledSuite):
    def fresh_committed(self, name):
        store, batch, baseline, _ = self.fixture(name)
        self.ok_import(store, batch)
        baseline = self.peek(store, list(baseline) + ['/batch', '/batch/d0/f0', '/batch/d69/last'])
        require(len(checkpoint_indexes(store)) == 1, 'first append did not publish one index')
        return store, dict(batch, target='/second', batch_id='second'), baseline

    def append_without_history_reads(self):
        store, batch, baseline = self.fresh_committed('append-no-history-read')
        before = index_state(store)
        snapshot_size = Path(str(store)+'.snapshot').stat().st_size
        request = dict(library=self.library, store=str(store), action='compiled_import', **batch)
        require(shutil.which('strace'), 'strace required to verify actual historical-index I/O')
        trace = store.parent/'append-open.trace'
        result = subprocess.run(['strace', '-f', '-e', 'trace=openat', '-o', str(trace),
            sys.executable, str(Path(__file__).resolve()), '--worker'],
            input=json.dumps(request), text=True, capture_output=True, timeout=180)
        require(result.returncode == 0, result.stderr)
        response = json.loads(result.stdout)
        require(response['rc'] == OK, response['error'])
        calls = trace.read_text()
        require(all(name not in calls for name in before), 'append opened a historical index file')
        after = index_state(store)
        require(all(after[name] == state for name, state in before.items()), 'old index was modified')
        require(len(after) == 2, 'second index missing')
        require(Path(str(store)+'.snapshot').stat().st_size - snapshot_size == 80,
                'checkpoint did not grow by exactly one root and one index descriptor')
        self.imported(store, batch, baseline)
        duplicate = self.call(store, 'compiled_import', **dict(batch, target='/batch', batch_id='collision'))
        require(duplicate['rc'] != OK, 'existing historical target accepted')
        self.passed_case('append opens no historical index; old bytes/identities/times unchanged; small checkpoint growth')

    def crashes(self):
        for fault, committed in [('index_created', False), ('index_registered', False),
            ('index_written', False), ('index_synced', False), ('batch_snapshot', False),
            ('undo_commit', False), ('batch_published', True), ('undo_receipt', True), ('undo_removed', True)]:
            store, batch, baseline = self.fresh_committed('crash-'+fault)
            before = index_state(store); checkpoint = self.capture_checkpoint(store)
            self.call(store, 'compiled_import', fault=fault, expect_crash=True, **batch)
            result = self.call(store, 'recover')
            require(result['rc'] == OK, 'recovery failed: '+result['error'])
            after = index_state(store)
            require(all(after[name] == state for name, state in before.items()), 'recovery changed old index')
            if committed:
                require(len(after) == 2, 'published index lost')
                self.imported(store, batch, baseline)
                require(self.ok_import(store, batch)['already'] == 1, 'published retry duplicated batch')
            else:
                self.checkpoint_restored(store, baseline, checkpoint, absent='/second')
                require(after == before, 'rollback retained new index in checkpoint')
                extras = [p for p in store.parent.glob(store.name+'.index-*') if p.name not in before]
                require((fault == 'index_created' and len(extras) == 1 and extras[0].stat().st_size == 0)
                        or (fault != 'index_created' and not extras), 'rollback left a registered index')
                self.ok_import(store, batch); self.imported(store, batch, baseline)
            self.passed_case('incremental crash '+fault+(' preserves commit' if committed else ' rolls back and retries'))

    def interrupted_cleanup(self):
        store, batch, baseline = self.fresh_committed('interrupted-index-cleanup')
        checkpoint = self.capture_checkpoint(store); before = index_state(store)
        self.call(store, 'compiled_import', fault='index_written', expect_crash=True, **batch)
        self.call(store, 'recover', fault='undo_index_removed', expect_crash=True)
        require(self.call(store, 'recover')['rc'] == OK, 'second cleanup failed')
        require(index_state(store) == before, 'cleanup changed old indexes')
        self.checkpoint_restored(store, baseline, checkpoint, absent='/second')
        self.ok_import(store, batch)
        self.passed_case('index rollback interrupted after unlink resumes without touching previous indexes')

    def replaced_uncommitted(self):
        store, batch, baseline = self.fresh_committed('replaced-uncommitted-index')
        known = index_state(store)
        self.call(store, 'compiled_import', fault='index_written', expect_crash=True, **batch)
        new = [p for p in store.parent.glob(store.name+'.index-*') if p.name not in known]
        require(len(new) == 1, 'expected exactly one uncommitted index')
        original = new[0].with_name('saved-index'); new[0].rename(original)
        new[0].write_bytes(b'unrelated file')
        before = self.capture_checkpoint(store)
        require(self.call(store, 'recover')['rc'] != OK, 'replaced new index was removed')
        require(new[0].read_bytes() == b'unrelated file' and self.capture_checkpoint(store) == before,
                'identity refusal changed data')
        new[0].unlink(); original.rename(new[0])
        require(self.call(store, 'recover')['rc'] == OK, 'restored registered identity cannot recover')
        self.passed_case('recovery refuses replaced uncommitted index before modifying any data')

    def damaged_history(self):
        for mode in ('missing', 'replaced', 'modified'):
            store, batch, baseline = self.fresh_committed('history-'+mode)
            p = Path(checkpoint_indexes(store)[0]['path'])
            if mode == 'missing': p.unlink()
            elif mode == 'replaced':
                contents = p.read_bytes(); p.rename(p.with_name('saved-index')); p.write_bytes(contents)
            else:
                with p.open('r+b') as stream:
                    stream.seek(24); b = stream.read(1); stream.seek(24); stream.write(bytes([b[0]^1]))
            before = self.capture_checkpoint(store)
            require(self.call(store, 'status')['rc'] != OK, 'status accepted '+mode+' index')
            require(self.call(store, 'compiled_import', **batch)['rc'] != OK, 'append accepted '+mode+' index')
            require(not self.call(store, 'peek', paths=['/batch'])['opened'], 'reopen accepted '+mode+' index')
            require(self.capture_checkpoint(store) == before, 'rejected history changed checkpoint/data')
            self.passed_case(mode+' historical index rejected by status, append and reopen')

    def mixed_import(self):
        store, batch, baseline = self.fresh_committed('mixed-tsv-compiled')
        legacy = dict(batch, target='/tsv', batch_id='tsv')
        require(self.call(store, 'import', **legacy)['rc'] == OK, 'TSV append after incremental import failed')
        require(struct.unpack_from('<Q', Path(str(store)+'.snapshot').read_bytes(), 8)[0] == 4,
                'TSV append did not publish a full snapshot')
        baseline = self.peek(store, list(baseline)+['/tsv', '/tsv/d0/f0'])
        self.ok_import(store, batch); self.imported(store, batch, baseline)
        require(len(checkpoint_indexes(store)) == 1, 'v4 conversion retained obsolete index references')
        self.passed_case('TSV append can load incremental indexes; next compiled append converts full checkpoint once')

    def content_checksum(self):
        store, batch, baseline = self.fresh_committed('index-content-checksum')
        p=Path(checkpoint_indexes(store)[0]['path'])
        data=bytearray(p.read_bytes()); data[-8] ^= 1; p.write_bytes(data)
        # Rebind only timestamps to bypass the cheap stat check and exercise
        # actual index-content verification during ordinary reopen.
        snap=Path(str(store)+'.snapshot'); data=bytearray(snap.read_bytes())
        _,offset=parse_segment_layout(data,16); offset+=10*8
        slots=struct.unpack_from('<Q',data,offset+5*8)[0]; offset+=(6+slots)*8
        buckets=struct.unpack_from('<Q',data,offset)[0]; offset+=(1+5*buckets)*8
        roots=struct.unpack_from('<Q',data,offset)[0]; offset+=(1+roots)*8+8
        st=p.stat()
        struct.pack_into('<4Q',data,offset+5*8,st.st_mtime_ns//10**9,st.st_mtime_ns%10**9,
                         st.st_ctime_ns//10**9,st.st_ctime_ns%10**9)
        struct.pack_into('<Q',data,len(data)-8,fnv(data[:-8])); snap.write_bytes(data)
        before=self.capture_checkpoint(store)
        result=self.call(store,'peek',paths=['/batch'])
        require(not result['opened'] and 'checksum' in result['error'], 'damaged index trailer accepted')
        require(self.capture_checkpoint(store)==before, 'failed content verification invalidated checkpoint')
        self.passed_case('MDS reopen checks index contents even when catalog timestamps match')

    def reserved_log(self):
        store,batch,baseline=self.fresh_committed('reserved-index-log')
        before=index_state(store)
        log=Path(str(store)+'.index-ABC123')
        command=[sys.executable,str(Path(__file__).resolve().parents[2]/'scripts/import_lhm_batches.py'),
            'import','--store',str(store),'--library',self.library,'--compiled-template',batch['template'],
            '--prefix','/reserved','--log',str(log)]
        result=subprocess.run(command,text=True,capture_output=True,timeout=30)
        require(result.returncode!=0 and 'reserved' in result.stderr and not log.exists(),
                'audit log was allowed to occupy an index filename')
        require(index_state(store)==before, 'rejected log modified existing indexes')
        self.passed_case('runner rejects audit logs using reserved future index filenames')

    def thousand_batches(self):
        directory = self.directory/'thousand-batches'; directory.mkdir()
        store=directory/'store'; manifest=directory/'input.tsv'; template=directory/'compiled'
        self.call(store, 'initialize', entries=[])
        manifest.write_text('type\tpath\tsize\tmtime\ndir\t/\t0\t123\nfile\t/f\t123\t123\n')
        require(self.call(store, 'compile', manifest=str(manifest), template=str(template))['rc'] == OK,
                'tiny scale template compilation failed')
        request = dict(action='many_incremental', library=self.library, store=str(store),
            template=str(template), header_sha256=digest((template/'header.bin').read_bytes()), batches=1000)
        proc = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker'],
            input=json.dumps(request), text=True, capture_output=True, timeout=300)
        require(proc.returncode == 0, proc.stdout+proc.stderr)
        result=json.loads(proc.stdout)
        indexes=checkpoint_indexes(store)
        require(len(indexes) == 1000 and len({v['bytes'] for v in indexes}) == 1,
                'index files grew with historical batches')
        result.update(checkpoint_bytes=Path(str(store)+'.snapshot').stat().st_size,
                      index_bytes=sum(v['bytes'] for v in indexes), index_files=len(indexes))
        require(result['checkpoint_bytes'] < 100000, '1000-batch checkpoint is not compact')
        (directory/'result.json').write_text(json.dumps(result, indent=2)+'\n')
        self.passed_case('1000 tiny batches use independent fixed-size indexes; all file/root IDs and final queries pass')

    def run(self):
        self.append_without_history_reads()
        self.crashes()
        self.interrupted_cleanup()
        self.replaced_uncommitted()
        self.damaged_history()
        self.mixed_import()
        self.content_checksum()
        self.reserved_log()
        self.thousand_batches()
        (self.directory/'results.json').write_text(json.dumps(dict(passed=self.passed,
            library_sha256=digest(Path(self.library).read_bytes())), indent=2)+'\n')
        print(f'PASS {len(self.passed)} incremental checkpoint scenarios', flush=True)


def main():
    if '--worker' in sys.argv:
        print(json.dumps(worker(json.load(sys.stdin)), ensure_ascii=False)); return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--output-dir', type=Path)
    args = parser.parse_args()
    directory = args.output_dir or Path(tempfile.mkdtemp(prefix='lhm-incremental-checkpoint-'))
    directory.mkdir(parents=True, exist_ok=True)
    print('fixtures:', directory, flush=True)
    Suite(args.library.resolve(), directory).run()


if __name__ == '__main__':
    main()
