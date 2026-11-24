#!/usr/bin/env python3
"""Convert a stopped v5 mixed compiled-import database to shared read-only indexes.

Default: inspect the plan and print the proposed conversion. --apply executes the
atomic conversion. Data pages/inode IDs are preserved; old index files and
.pre-shared checkpoint/receipt copies are retained. Use a newly rebuilt MDS
and library afterward: the old pinned runtime cannot read v6.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import tempfile

import import_lhm_batches as bulk
import import_lhm_mixed_100yi as mixed


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--work-dir',type=Path,required=True)
    p.add_argument('--library',type=Path,default=Path(__file__).resolve().parents[1]/'build/src/mds/lhm/libzb_lhm_bridge.so')
    p.add_argument('--memory-budget-mib',type=bulk.positive,default=65536)
    p.add_argument('--apply',action='store_true')
    a=p.parse_args();work=a.work_dir.resolve();store=work/'db/namespace.meta'
    with mixed.lock(work/'job.lock'),bulk.RunLock(store):
        plan=mixed.checked_plan(work)
        bridge=bulk.Bridge(a.library.resolve())
        if a.apply:bridge.recover(store)
        receipt=bridge.status(store)
        bulk.require(mixed.next_batch(receipt,plan)==len(plan['batches']),'mixed import must be complete before conversion')
        rows=[]
        for b in plan['batches']:
            directory=Path(b['template'])
            bulk.require(hashlib.sha256((directory/'header.bin').read_bytes()).hexdigest()==b['digest'],'compiled template header changed')
            fields=[b['target'],str(directory),b['digest']]
            bulk.require(all('\t' not in x and '\n' not in x and '\r' not in x for x in fields),'mapping field contains delimiter')
            rows.append('\t'.join(fields))
        print(json.dumps(dict(action='convert' if a.apply else 'plan',store=str(store),batches=len(rows),
                             unique_templates=len({b['digest'] for b in plan['batches']}),files=plan['total_files'],
                             namespace_mode_after_conversion='read-only',data_pages_rewritten=False),ensure_ascii=False,indent=2),flush=True)
        if not a.apply:return
        with tempfile.NamedTemporaryFile(mode='w',prefix='shared-conversion-',suffix='.tsv',dir=work) as mapping:
            mapping.write('\n'.join(rows)+'\n');mapping.flush();os.fsync(mapping.fileno())
            f=bridge.lib.zb_lhm_convert_shared_index
            f.argtypes=[ctypes.c_char_p,ctypes.c_char_p,ctypes.c_uint64,ctypes.c_char_p,ctypes.c_size_t];f.restype=ctypes.c_int
            error=ctypes.create_string_buffer(8192)
            rc=f(os.fsencode(store),os.fsencode(mapping.name),a.memory_budget_mib<<20,error,len(error))
            bulk.require(rc==0,'conversion failed: '+error.value.decode())
        print('Conversion committed. Start with the rebuilt MDS/library and the existing mds.conf; the pinned runtime was not replaced.',flush=True)


if __name__=='__main__':main()
