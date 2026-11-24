#!/usr/bin/env python3
"""Manual shared-index v6 tests, isolated small stores only. No CTest registration."""
import argparse
import ctypes as C
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
import import_lhm_batches as bulk
from lhm_batch_recovery_test import Inode, ImportResult
from lhm_compiled_import_test import worker as legacy


def worker(r):
    if r['action'] not in ('shared_import', 'query', 'convert'):
        return legacy(r)
    if r['action'] == 'convert':
        lib=C.CDLL(r['library']);f=lib.zb_lhm_convert_shared_index
        f.argtypes=[C.c_char_p,C.c_char_p,C.c_uint64,C.c_char_p,C.c_size_t];f.restype=C.c_int
        error=C.create_string_buffer(8192)
        rc=f(os.fsencode(r['store']),os.fsencode(r['mapping']),64<<20,error,len(error))
        return dict(rc=rc,error=error.value.decode())
    if r['action'] == 'shared_import':
        args = argparse.Namespace(memory_budget_mib=64, shared_template_index=True)
        bridge = bulk.Bridge(Path(r['library']))
        try:
            result, already = bridge.import_compiled_batch(Path(r['store']), r['target'], Path(r['template']),
                                                           r['batch_id'], r['header_sha256'], args)
            return dict(rc=0, already=already, result=result)
        except RuntimeError as e:
            return dict(rc=5, error=str(e))
    lib = C.CDLL(r['library']); ptr=C.c_void_p; u64=C.c_uint64; text=C.c_char_p
    bindings = {
        'zb_lhm_reopen': ([text,text,C.c_size_t],ptr),
        'zb_lhm_close_checked': ([ptr,text,C.c_size_t],C.c_int),
        'zb_lhm_checkpoint': ([ptr,text,C.c_size_t],C.c_int),
        'zb_lhm_lookup_checked': ([ptr,text,C.POINTER(Inode)],C.c_int),
        'zb_lhm_read_checked': ([ptr,u64,C.POINTER(Inode)],C.c_int),
        'zb_lhm_shared_index_stats': ([ptr,C.POINTER(u64),C.POINTER(u64),C.POINTER(u64)],C.c_int),
        'zb_lhm_reserve_checked': ([ptr,text,C.c_uint8,C.POINTER(u64),C.POINTER(u64)],C.c_int),
        'zb_lhm_remove_checked': ([ptr,text,C.c_uint8],C.c_int),
        'zb_lhm_rename_checked': ([ptr,text,text],C.c_int),
    }
    callback=C.CFUNCTYPE(C.c_int,ptr,text,C.c_uint32,u64,C.c_uint8)
    bindings['zb_lhm_readdir_checked']=([ptr,text,text,C.c_uint32,callback,ptr,C.POINTER(C.c_int)],C.c_int)
    for n,(a,ret) in bindings.items():getattr(lib,n).argtypes=a;getattr(lib,n).restype=ret
    error=C.create_string_buffer(8192);ctx=lib.zb_lhm_reopen(os.fsencode(r['store']),error,len(error))
    if not ctx:return dict(opened=False,error=error.value.decode())
    values={};t,b,d=u64(),u64(),u64()
    assert lib.zb_lhm_shared_index_stats(ctx,C.byref(t),C.byref(b),C.byref(d))==0
    for path in r.get('paths',[]):
        x=Inode();rc=lib.zb_lhm_lookup_checked(ctx,path.encode(),C.byref(x));values[path]=dict(rc=rc)
        if rc==0:
            y=Inode();assert lib.zb_lhm_read_checked(ctx,x.inode_id,C.byref(y))==0
            assert bytes(x)==bytes(y)
            values[path].update(inode=x.inode_id,size=x.size,name=bytes(x.file_name).decode(),type=x.type)
    listings={}
    for path in r.get('listings',[]):
        result=[]
        @callback
        def collect(opaque,name,size,inode,kind):result.append([C.string_at(name,size).decode(),inode,kind]);return 1
        more=C.c_int()
        rc=lib.zb_lhm_readdir_checked(ctx,path.encode(),b'',0,collect,None,C.byref(more))
        listings[path]=dict(rc=rc,entries=result)
    if r.get('mutations'):
        token,identifier=u64(),u64()
        assert lib.zb_lhm_reserve_checked(ctx,b'/outside',1,C.byref(token),C.byref(identifier))==7
        assert lib.zb_lhm_remove_checked(ctx,b'/one/a/f0000',1)==7
        assert lib.zb_lhm_rename_checked(ctx,b'/one/a',b'/renamed')==7
    if r.get('checkpoint'):
        assert lib.zb_lhm_checkpoint(ctx,error,len(error))==0,error.value
        assert not Path(str(r['store'])+'.undo').exists()
    assert lib.zb_lhm_close_checked(ctx,error,len(error))==1,error.value
    return dict(opened=True,templates=t.value,instances=b.value,routes=d.value,values=values,listings=listings)


def main(args):
    root=Path(tempfile.mkdtemp(prefix='lhm-shared-index-'));checks=[]
    def call(action,store, fault=None, crash=False, **kw):
        env=os.environ.copy();env.pop('ZB_LHM_CRASH_AT',None);env.pop('ZB_LHM_TEST_SEGMENT_BYTES',None)
        if not args.basic_only:env['ZB_LHM_TEST_SEGMENT_BYTES']=str(65536)
        if fault:env['ZB_LHM_CRASH_AT']=fault
        r=dict(action=action,store=str(store),library=str(args.library.resolve()),**kw)
        p=subprocess.run([sys.executable,__file__,'--worker'],input=json.dumps(r),text=True,capture_output=True,env=env,timeout=120)
        if crash:
            assert p.returncode==-signal.SIGKILL,(p.returncode,p.stdout,p.stderr);return
        assert p.returncode==0,(p.stdout,p.stderr)
        return json.loads(p.stdout)
    full=root/'full.tsv';tail=root/'tail.tsv'
    lines=['type\tpath\tsize\tblocks_512\tblksize\treplica','dir\t/\t0\t0\t4096\t1','dir\t/a\t0\t0\t4096\t1']
    lines += [f'file\t/a/f{i:04d}\t{i+100}\t1\t4096\t1' for i in range(400)]
    lines += ['dir\t/empty\t0\t0\t4096\t1','dir\t/deep\t0\t0\t4096\t1','file\t/deep/end\t999\t2\t4096\t1']
    full.write_text('\n'.join(lines)+'\n');tail.write_text('\n'.join(lines[:3+77])+'\n')
    templates={}
    for name,source in [('full',full),('tail',tail)]:
        directory=root/name;assert call('compile',root/'unused',manifest=str(source),template=str(directory))['rc']==0
        templates[name]=dict(template=str(directory),header_sha256=hashlib.sha256((directory/'header.bin').read_bytes()).hexdigest())
    def fresh(name):
        p=root/name/'meta';p.parent.mkdir();assert call('initialize',p,entries=[])['rc']==0;return p
    def imp(store,target,which='full',**kw):return call('shared_import',store,target=target,batch_id=target[1:],**templates[which],**kw)
    store=fresh('basic');assert imp(store,'/one')['rc']==0
    index=list(store.parent.glob('meta.index-*'));assert len(index)==1
    first_bytes=index[0].read_bytes()
    for target in ['/two','/three']:assert imp(store,target)['rc']==0
    assert len(list(store.parent.glob('meta.index-*')))==1 and index[0].read_bytes()==first_bytes
    assert imp(store,'/tail','tail')['rc']==0
    paths=[f'/{name}/a/f{i:04d}' for name in ['one','two','three'] for i in range(400)]
    paths+=['/one/deep/end','/two/deep/end','/three/deep/end','/tail/a/f0076','/tail/a/f0077','/tail/deep/end','/missing/a/f0000']
    checkpoint=Path(str(store)+'.snapshot').read_bytes()
    q=call('query',store,paths=paths,listings=['/','/one','/one/a','/tail/a','/one/empty'],mutations=True)
    assert q['opened'],q
    assert (q['templates'],q['instances'],q['routes'])==(2,4,6),q
    found=[x for x in q['values'].values() if x['rc']==0]
    assert len(found)==1204 and len({x['inode'] for x in found})==1204
    assert all(q['values'][x]['rc']==1 for x in paths[-3:])
    assert len(q['listings']['/']['entries'])==4 and len(q['listings']['/one/a']['entries'])==400
    assert len(q['listings']['/tail/a']['entries'])==77 and q['listings']['/one/empty']['entries']==[]
    assert Path(str(store)+'.snapshot').read_bytes()==checkpoint
    assert imp(store,'/tail','tail')['already']
    assert call('compiled_import',store,target='/tail',batch_id='tail',**templates['tail'])['rc']!=0
    checks.append('shared routes reused; 1204 real inode reads; unique IDs; tail and negative lookup; Readdir; mutations rejected; idempotence')
    # Removing the source bundles must not break a committed database.
    for name in templates:Path(templates[name]['template']).rename(root/(name+'-hidden'))
    assert call('query',store,paths=['/one/a/f0399'])['values']['/one/a/f0399']['size']==499
    for name in templates:(root/(name+'-hidden')).rename(Path(templates[name]['template']))
    checks.append('reopen independent of external template bundles')
    # Copying changes inode/ctime; --migration-root additionally changes device.
    # Immutable shared queries must not need checkpoint rebinding.
    with tempfile.TemporaryDirectory(prefix='lhm-portable-',dir=args.migration_root) as destination:
        moved_dir=Path(destination)/'shared'
        shutil.copytree(store.parent,moved_dir)
        moved=moved_dir/store.name
        assert (moved.stat().st_dev,moved.stat().st_ino)!=(store.stat().st_dev,store.stat().st_ino)
        if args.migration_root:
            assert moved.stat().st_dev!=store.stat().st_dev, '--migration-root must be on a different filesystem'
        def contents(directory):
            return {p.name:hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in directory.glob('meta*') if p.is_file()}
        original=contents(moved_dir)
        actual=call('query',moved,paths=paths,listings=['/','/tail/a'],mutations=True,checkpoint=True)
        assert actual['opened'] and actual['values']==q['values'],actual
        assert contents(moved_dir)==original, 'read-only migration modified persistent files'
        assert not Path(str(moved)+'.undo').exists()
        assert imp(moved,'/extra')['rc']!=0
        assert contents(moved_dir)==original
        checks.append('relocated shared query: inode IDs/attributes preserved; no checkpoint rewrite or undo; relocated append rejected')
        for mode in ('index-checksum','missing-segment','short-segment','extra-segment','aliased-segment'):
            bad_dir=Path(destination)/mode;shutil.copytree(moved_dir,bad_dir);bad_store=bad_dir/store.name
            if mode=='index-checksum':
                p=next(bad_dir.glob('meta.index-*'))
                with p.open('r+b') as stream:
                    stream.seek(-1,2); byte=stream.read(1);stream.seek(-1,2);stream.write(bytes([byte[0]^1]))
            elif mode=='missing-segment':Path(str(bad_store)+'.buckets').unlink()
            elif mode=='short-segment':
                p=Path(str(bad_store)+'.buckets')
                with p.open('r+b') as stream:stream.truncate(p.stat().st_size-16384)
            elif mode=='extra-segment':Path(str(bad_store)+'.part999999').write_bytes(bytes(16384))
            else:
                p=Path(str(bad_store)+'.heads');p.unlink();os.link(bad_store,p)
            before_bad=contents(bad_dir)
            assert not call('query',bad_store,paths=['/one/a/f0000'])['opened'],mode
            assert contents(bad_dir)==before_bad,mode
        checks.append('portable query rejects corrupt indexes, missing/truncated/extra/aliased segments without modifying data')
        if not args.basic_only:
            call('query',moved,fault='service_started',crash=True)
            assert not Path(str(moved)+'.undo').exists()
            assert call('query',moved,paths=['/one/a/f0000'])['values']['/one/a/f0000']==q['values']['/one/a/f0000']
            assert contents(moved_dir)==original
            checks.append('killed portable shared query reopens without undo or checkpoint changes')
            pending=fresh('pending-migration');assert imp(pending,'/base')['rc']==0
            imp(pending,'/new','tail',fault='batch_snapshot',crash=True)
            copied_dir=Path(destination)/'pending';shutil.copytree(pending.parent,copied_dir);copied=copied_dir/pending.name
            before_pending=contents(copied_dir)
            assert not call('query',copied,paths=['/base/a/f0000'])['opened']
            assert call('recover',copied)['rc']!=0
            assert contents(copied_dir)==before_pending
            assert call('recover',pending)['rc']==0
            checks.append('relocated pending undo rejected without restoring/deleting files; original still recovers')
    if not args.basic_only:
        for phase in ['shared_index_written','shared_index_synced','shared_mapping_written','batch_snapshot','undo_commit','batch_published']:
            crashstore=fresh('crash-'+phase);assert imp(crashstore,'/base')['rc']==0
            baseline=call('query',crashstore,paths=['/base/a/f0000'])['values']
            imp(crashstore,'/new','tail',fault=phase,crash=True)
            assert call('recover',crashstore)['rc']==0
            seen=call('query',crashstore,paths=['/base/a/f0000','/new/a/f0000'])
            assert seen['values']['/base/a/f0000']==baseline['/base/a/f0000']
            assert seen['values']['/new/a/f0000']['rc']==(0 if phase=='batch_published' else 1)
            assert imp(crashstore,'/new','tail')['rc']==0
            final=call('query',crashstore,paths=['/new/a/f0076','/new/a/f0077'])
            assert final['values']['/new/a/f0076']['rc']==0 and final['values']['/new/a/f0077']['rc']==1
            checks.append('crash recovery: '+phase)
        for phase in ['shared_mapping_written','batch_published']:
            repeated=fresh('reuse-crash-'+phase);assert imp(repeated,'/base')['rc']==0
            imp(repeated,'/new',fault=phase,crash=True)
            assert call('recover',repeated)['rc']==0
            assert imp(repeated,'/new')['rc']==0
            q=call('query',repeated,paths=['/base/a/f0000','/new/a/f0000'])
            assert (q['templates'],q['instances'],q['routes'])==(1,2,4),q
            assert q['values']['/base/a/f0000']['inode']!=q['values']['/new/a/f0000']['inode']
            assert len(list(repeated.parent.glob('meta.index-*')))==1
            checks.append('reused-template crash recovery: '+phase)
    def conversion_fixture(name):
        old=fresh(name);entries=[]
        for target,which in [('/legacy-one','full'),('/legacy-two','full'),('/legacy-tail','tail')]:
            result=call('compiled_import',old,target=target,batch_id=target[1:],**templates[which])
            assert result['rc']==0,result
            t=templates[which];entries.append(target+'\t'+t['template']+'\t'+t['header_sha256'])
        mapping=old.parent/'mapping.tsv';mapping.write_text('\n'.join(reversed(entries))+'\n')
        return old,mapping
    def data_hashes(old):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in old.parent.glob('meta*')
                if p.name=='meta' or p.name.startswith(('meta.heads','meta.buckets','meta.part'))}
    old,mapping=conversion_fixture('conversion')
    conversion_paths=['/legacy-one/a/f0000','/legacy-two/a/f0399','/legacy-tail/a/f0076','/legacy-tail/a/f0077']
    before=call('query',old,paths=conversion_paths)['values'];pages=data_hashes(old)
    converted=call('convert',old,mapping=str(mapping));assert converted['rc']==0,converted
    after=call('query',old,paths=conversion_paths)
    assert after['values']==before and after['templates']==2 and after['routes']==6,after
    assert data_hashes(old)==pages, 'conversion rewrote data pages'
    assert Path(str(old)+'.snapshot.pre-shared').is_file()
    assert call('convert',old,mapping=str(mapping))['rc']==0
    checks.append('v5 conversion: all sampled inode IDs/attributes and all data page bytes preserved; idempotent retry')
    if not args.basic_only:
        for phase in ['shared_conversion_backup','shared_conversion_index','shared_conversion_snapshot','undo_commit','shared_conversion_published']:
            old,mapping=conversion_fixture('conversion-crash-'+phase);pages=data_hashes(old)
            call('convert',old,mapping=str(mapping),fault=phase,crash=True)
            assert call('recover',old)['rc']==0
            result=call('convert',old,mapping=str(mapping));assert result['rc']==0,result
            assert data_hashes(old)==pages
            q=call('query',old,paths=conversion_paths);assert q['templates']==2 and q['instances']==3,q
            checks.append('conversion crash recovery: '+phase)
    # Actual metadata must come from independent data pages, not template attrs.
    data_store=fresh('actual-inode-read');assert imp(data_store,'/one')['rc']==0
    q=call('query',data_store,paths=['/one/a/f0000']);identifier=q['values']['/one/a/f0000']['inode']
    block,slot=identifier>>16,identifier&65535
    with Path(str(data_store)+'.snapshot').open('rb') as snapshot:
        import struct
        snapshot.seek(16);segment_bytes=struct.unpack('<Q',snapshot.read(8))[0]
    segment=block*16384//segment_bytes
    path=Path(str(data_store)+(f'.part{segment:06d}' if segment else ''))
    offset=(block*16384)%segment_bytes+256+slot*256+16
    import struct
    with path.open('r+b') as f:
        f.seek(offset);original=f.read(8);f.seek(offset);f.write(struct.pack('<Q',123456));f.flush();os.fsync(f.fileno())
    q=call('query',data_store,paths=['/one/a/f0000']);assert q['values']['/one/a/f0000']['size']==123456,q
    with path.open('r+b') as f:f.seek(offset);f.write(original);f.flush();os.fsync(f.fileno())
    checks.append('query reads physical inode attributes, not template values')
    bad, badmap=conversion_fixture('conversion-mismatch');baseline=Path(str(bad)+'.snapshot').read_bytes();pages=data_hashes(bad)
    badmap.write_text(badmap.read_text().replace(templates['full']['header_sha256'],templates['tail']['header_sha256']).replace(templates['full']['template'],templates['tail']['template']))
    assert call('convert',bad,mapping=str(badmap))['rc']!=0
    assert Path(str(bad)+'.snapshot').read_bytes()==baseline and data_hashes(bad)==pages
    checks.append('conversion rejects mismatched templates and retains old checkpoint/data')
    empty=root/'empty.tsv';empty.write_text('type\tpath\tsize\tblocks_512\tblksize\treplica\ndir\t/\t0\t0\t4096\t1\ndir\t/empty\t0\t0\t4096\t1\n')
    emptydir=root/'empty-template';assert call('compile',root/'unused',manifest=str(empty),template=str(emptydir))['rc']==0
    es=fresh('empty-template-store')
    ei=dict(template=str(emptydir),header_sha256=hashlib.sha256((emptydir/'header.bin').read_bytes()).hexdigest())
    assert call('shared_import',es,target='/empty',batch_id='empty',**ei)['rc']==0
    q=call('query',es,paths=['/empty/empty'],listings=['/empty/empty']);assert q['values']['/empty/empty']['type']==2 and not q['listings']['/empty/empty']['entries']
    checks.append('empty-file template with directory-only routes')
    result=dict(root=str(root),checks=checks,basic_only=args.basic_only)
    (root/'verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--library',type=Path);p.add_argument('--basic-only',action='store_true');p.add_argument('--worker',action='store_true');p.add_argument('--migration-root',type=Path,help='optional writable directory on a different filesystem supporting O_DIRECT');args=p.parse_args()
    if args.worker:print(json.dumps(worker(json.load(sys.stdin))))
    else:main(args)
