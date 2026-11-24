#!/usr/bin/env python3
"""Manual isolated multi-database RPC/cache/failure checks; no CTest registration.

python3 tests/mds/lhm_multi_query_test.py --server build/mds_server \
  --library build/src/mds/lhm/libzb_lhm_bridge.so --client build/mds_query
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
import import_lhm_batches as bulk
import import_lhm_mixed_100yi as mixed

def code(reply):
    value=reply.get('status',{}).get('code',0)
    return {'MDS_OK':0,'MDS_INVALID_ARGUMENT':1,'MDS_NOT_FOUND':2,'MDS_UNAVAILABLE':7}.get(value,value)

class Service:
    def __init__(self,config,runtime,log,method='ListDatabases',body=None):
        self.runtime=runtime
        with socket.socket() as s:s.bind(('127.0.0.1',0));self.port=s.getsockname()[1]
        self.log=log.open('ab')
        self.proc=mixed.launch([runtime/'mds_server','--config='+str(config),'--port='+str(self.port)],runtime,stdout=self.log,stderr=self.log)
        try:
            for _ in range(300):
                assert self.proc.poll() is None,log.read_text()[-4000:]
                try:self.rpc(method,body or {});break
                except (OSError,TimeoutError):time.sleep(.1)
            else:raise RuntimeError('MDS readiness timeout')
        except BaseException:self.close();raise
    def rpc(self,method,body):
        request=urllib.request.Request(f'http://127.0.0.1:{self.port}/zb.rpc.MdsService/{method}',
            data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
        opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request,timeout=15) as response:return json.load(response)
    def close(self):
        if self.proc.poll() is None:self.proc.terminate()
        try:rc=self.proc.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self.proc.kill();self.proc.wait();raise
        finally:self.log.close()
        assert rc==0,rc

def main(args):
    work=Path(tempfile.mkdtemp(prefix='lhm-multi-query-'));runtime=work/'runtime';runtime.mkdir()
    shutil.copy2(args.server.resolve(),runtime/'mds_server')
    shutil.copy2(args.library.resolve(),runtime/'libzb_lhm_bridge.so')
    shutil.copy2(args.client.resolve(),runtime/'mds_query')
    bridge=bulk.Bridge(runtime/'libzb_lhm_bridge.so');checks=[];databases={}
    def template(name,count):
        source=work/(name+'.tsv');compiled=work/(name+'-compiled')
        source.write_text('type\tpath\tsize\tblocks_512\tblksize\treplica\ndir\t/\t0\t0\t4096\t1\ndir\t/a\t0\t0\t4096\t1\n'+
                          ''.join(f'file\t/a/f{i:04d}\t{i+100}\t1\t4096\t1\n' for i in range(count)))
        bridge.compile_template(source,compiled,64<<20)
        return compiled,hashlib.sha256((compiled/'header.bin').read_bytes()).hexdigest()
    full=template('full',400);tail=template('tail',77)
    def fresh(name,which=full,copies=1,shared=True):
        directory=work/name;directory.mkdir()
        mixed.init_database(directory,{'plan_id':hashlib.sha256(name.encode()).hexdigest()},runtime)
        store=directory/'db/namespace.meta';databases[name]=directory
        for i in range(copies if shared else 0):
            result,_=bridge.import_compiled_batch(store,'/one' if i==0 else '/two',which[0],name+str(i),which[1],
                argparse.Namespace(memory_budget_mib=64,shared_template_index=True))
            assert result['files']==(400 if which==full else 77)
        return store
    a=fresh('a',copies=2);b=fresh('b');c=fresh('c',tail);d=fresh('d');f=fresh('f',shared=False);g=fresh('g')
    # The same physical inode number in two independent stores must select
    # different actual data, despite their shared immutable route structure.
    # A short single-store RPC gives the file ID without relying on page offsets.
    single=Service(databases['b']/'mds.conf',runtime,work/'single-before.log','Lookup',{'path':'/one/a/f0000'})
    try:
        value=single.rpc('Lookup',{'path':'/one/a/f0000'});assert code(value)==0,value
        inode=int(value['attr']['inode_id'])
    finally:single.close()
    block,slot=inode>>16,inode&65535
    page_bytes=struct.unpack_from('<Q',Path(str(b)+'.snapshot').read_bytes(),16)[0]
    segment=block*16384//page_bytes
    inode_file=Path(str(b)+(f'.part{segment:06d}' if segment else ''))
    with inode_file.open('r+b') as stream:
        stream.seek(block*16384%page_bytes+256+slot*256+16);stream.write(struct.pack('<Q',987654));stream.flush();os.fsync(stream.fileno())
    corrupt=next(d.parent.glob('namespace.meta.index-*'))
    with corrupt.open('r+b') as stream:
        stream.seek(-1,2);byte=stream.read(1);stream.seek(-1,2);stream.write(bytes([byte[0]^1]))
    Path(str(g)+'.undo').write_bytes(b'pending-journal-must-not-be-recovered-by-query-mode')
    def snapshot_hashes(directory):
        return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (directory/'db').glob('namespace.meta*') if p.is_file()}
    before={name:snapshot_hashes(directory) for name,directory in databases.items()}
    missing=work/'e-missing';catalog=work/'databases.tsv'
    rows=[f'{name}\t{directory}/db/control\t{directory}/db/namespace.meta' for name,directory in databases.items()]
    rows.append(f'e\t{missing}/db/control\t{missing}/db/namespace.meta')
    catalog.write_text('\n'.join(rows)+'\n')
    config=work/'multi.conf';config.write_text('NAMESPACE_BACKEND=lhm_multi_readonly\nLHM_DATABASES_FILE='+str(catalog)+'\n')
    service=Service(config,runtime,work/'multi.log')
    def lookup(database,path='/one/a/f0000'):return service.rpc('Lookup',{'database_id':database,'path':path})
    try:
        status=service.rpc('ListDatabases',{});assert code(status)==0,status
        states={x['database_id']:x for x in status['databases']}
        assert {name for name,x in states.items() if x.get('available')}=={'a','b','c'},status
        assert int(status['cached_templates'])==2 and int(status['template_references'])==3,status
        assert int(status['template_loads'])==2 and int(status['template_cache_hits'])==1,status
        assert not missing.exists()
        checks.append('two matching databases share one route object; distinct tail template is separate; corrupt/missing/writable/pending stores isolated')
        qa,qb=lookup('a'),lookup('b');assert code(qa)==code(qb)==0
        assert int(qa['attr']['inode_id'])==int(qb['attr']['inode_id'])==inode
        assert int(qa['attr']['size'])==100 and int(qb['attr']['size'])==987654
        for name,reply in [('a',qa),('b',qb)]:
            assert reply['database_id']==name
            attr=service.rpc('Getattr',{'database_id':name,'inode_id':inode})
            assert attr['attr']==reply['attr'] and attr['database_id']==name
        assert code(lookup('a','/two/a/f0399'))==0
        assert code(lookup('b','/two/a/f0399'))==2
        assert code(lookup('c','/one/a/f0076'))==0 and code(lookup('c','/one/a/f0077'))==2
        checks.append('database_id routes identical path/inode to independent physical attributes and instance/tail boundaries')
        assert code(service.rpc('Lookup',{'path':'/one/a/f0000'}))==1
        assert code(service.rpc('Getattr',{'inode_id':inode}))==1
        assert code(service.rpc('Readdir',{'path':'/'}))==1
        assert code(lookup('unknown'))==2
        assert code(lookup('e'))==code(lookup('d'))==code(lookup('f'))==code(lookup('g'))==7
        assert code(lookup('a','relative'))==1
        assert code(lookup('a','/one/a/f0000\0suffix'))==1
        assert code(service.rpc('Getattr',{'database_id':'a','inode_id':0}))==1
        assert code(service.rpc('Readdir',{'database_id':'a','path':'/','limit':10001}))==1
        names=[];cursor=''
        while True:
            reply=service.rpc('Readdir',{'database_id':'b','path':'/one/a','limit':17,'start_after':cursor})
            assert code(reply)==0 and reply['database_id']=='b',reply
            names.extend(x['name'] for x in reply.get('entries',[]))
            if not reply.get('has_more'):break
            assert reply['next_token']!=cursor;cursor=reply['next_token']
        assert names==[f'f{i:04d}' for i in range(400)]
        try:
            mutation=service.rpc('Mkdir',{'path':'/forbidden','mode':493})
            assert code(mutation)!=0,mutation
        except urllib.error.HTTPError as error:assert error.code>=400
        assert code(lookup('a','/forbidden'))==2
        checks.append('explicit ID required; unknown/offline/invalid queries distinguished; pagination complete; mutations unsupported')
        env=dict(os.environ,LD_LIBRARY_PATH=str(runtime))
        for flags in [['--list_databases'],['--database_id=b','--path=/one/a/f0000']]:
            result=subprocess.run([str(runtime/'mds_query'),f'--server=127.0.0.1:{service.port}']+flags,env=env,text=True,capture_output=True)
            assert result.returncode==0,(result.stdout,result.stderr)
            output=json.loads(result.stdout)
            if '--list_databases' in flags:assert int(output['cached_templates'])==2
            else:assert output['ok'] and output['response']['database_id']=='b' and int(output['response']['attr']['size'])==987654
        checks.append('native serial query client selects database and lists shared cache status')
        databases['a'].rename(work/'a-offline')
        assert code(lookup('a'))==7 and code(lookup('b'))==0
        status=service.rpc('ListDatabases',{});assert int(status['cached_templates'])==2 and int(status['template_references'])==2,status
        databases['c'].rename(work/'c-offline')
        status=service.rpc('ListDatabases',{});assert int(status['cached_templates'])==1 and int(status['template_references'])==1,status
        assert code(lookup('b'))==0
        checks.append('runtime loss of two databases leaves survivor queryable; last-reference close releases unique route memory')
    finally:
        service.close()
        for name in ('a','c'):
            if (work/(name+'-offline')).exists():(work/(name+'-offline')).rename(databases[name])
    for name,directory in databases.items():assert snapshot_hashes(directory)==before[name],name
    # Each disk remains independently usable by the existing single-db service.
    single=Service(databases['b']/'mds.conf',runtime,work/'single-after.log','Lookup',{'path':'/one/a/f0000'})
    try:
        reply=single.rpc('Lookup',{'path':'/one/a/f0000'});assert code(reply)==0 and int(reply['attr']['size'])==987654
        assert code(single.rpc('Lookup',{'database_id':'a','path':'/one/a/f0000'}))==1
        assert code(single.rpc('Getattr',{'database_id':'a','inode_id':inode}))==1
        assert code(single.rpc('Readdir',{'database_id':'a','path':'/'}))==1
    finally:single.close()
    checks.append('standalone disk still opens; single-db service rejects accidental multi-db selectors')
    service=Service(config,runtime,work/'multi-restart.log')
    try:
        status=service.rpc('ListDatabases',{});assert int(status['cached_templates'])==2 and int(status['template_references'])==3
        assert code(lookup('a'))==code(lookup('b'))==0
    finally:service.close()
    checks.append('service restart reloads returned disks and shares routes again')
    offline_catalog=work/'offline.tsv';offline_catalog.write_text(rows[-1]+'\n')
    offline_config=work/'offline.conf';offline_config.write_text('NAMESPACE_BACKEND=lhm_multi_readonly\nLHM_DATABASES_FILE='+str(offline_catalog)+'\n')
    service=Service(offline_config,runtime,work/'all-offline.log')
    try:
        status=service.rpc('ListDatabases',{});assert code(status)==0 and not status['databases'][0].get('available')
        assert int(status.get('cached_templates',0))==0 and not missing.exists()
    finally:service.close()
    checks.append('all disks absent still exposes diagnostic service without creating databases')
    invalid=work/'invalid.tsv';invalid.write_text(rows[0]+'\n'+rows[0]+'\n')
    invalid_config=work/'invalid.conf';invalid_config.write_text('NAMESPACE_BACKEND=lhm_multi_readonly\nLHM_DATABASES_FILE='+str(invalid)+'\n')
    result=subprocess.run([str(runtime/'mds_server'),'--config='+str(invalid_config),'--port=0'],env=dict(os.environ,LD_LIBRARY_PATH=str(runtime)),text=True,capture_output=True,timeout=30)
    assert result.returncode!=0 and 'duplicate' in result.stderr,result.stderr
    checks.append('duplicate catalog identities rejected before opening databases')
    report=dict(work=str(work),checks=checks)
    (work/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server',type=Path,required=True);parser.add_argument('--library',type=Path,required=True);parser.add_argument('--client',type=Path,required=True)
    main(parser.parse_args())
