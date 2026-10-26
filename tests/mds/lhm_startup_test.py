#!/usr/bin/env python3
"""Startup rejection checks; all cases exit before binding a port."""
import argparse
import pathlib
import subprocess
import json
p=argparse.ArgumentParser()
p.add_argument('--build',type=pathlib.Path,required=True)
p.add_argument('--work-dir',type=pathlib.Path,required=True)
p.add_argument('--used-control',type=pathlib.Path,required=True)
a=p.parse_args()
a.work_dir.mkdir(parents=True,exist_ok=False)
base=a.work_dir.resolve(); results={}
def reject(name,config,expected):
    path=base/(name+'.conf');path.write_text(config+'NODES=node-real-01@127.0.0.1:1\nDISKS=node-real-01:disk-01\n')
    result=subprocess.run([str(a.build/'mds_server'),'--config='+str(path)],capture_output=True,text=True,timeout=10)
    (base/(name+'.log')).write_text(result.stdout+result.stderr)
    assert result.returncode!=0 and expected in result.stderr,(name,result.returncode,result.stderr)
    results[name]=True
reject('unknown',f'MDS_DB_PATH={base}/unknown_db\nNAMESPACE_BACKEND=bad\n','NAMESPACE_BACKEND must')
reject('archive',f'MDS_DB_PATH={base}/archive_db\nNAMESPACE_BACKEND=lhm\nLHM_STORE_FILE={base}/archive.meta\nENABLE_OPTICAL_ARCHIVE=true\n','ENABLE_OPTICAL_ARCHIVE=false')
reject('missing_file',f'MDS_DB_PATH={base}/missing_db\nNAMESPACE_BACKEND=lhm\n','LHM_STORE_FILE')
for backend in ['lhm','rocksdb']:
    reject('used_'+backend,f'MDS_DB_PATH={a.used_control}\nNAMESPACE_BACKEND={backend}\nLHM_STORE_FILE={base}/new.meta\n','cannot be reopened or switched')
existing=base/'existing.meta';existing.write_bytes(b'preserve existing data')
reject('existing_file',f'MDS_DB_PATH={base}/existing_db\nNAMESPACE_BACKEND=lhm\nLHM_STORE_FILE={existing}\n','must be a new file')
assert existing.read_bytes()==b'preserve existing data'
legacy=base/'legacy_db'
subprocess.run([str(a.build/'src/mds/lhm/lhm_control_store_tool'),'seed-inode',str(legacy)],check=True)
reject('legacy_namespace',f'MDS_DB_PATH={legacy}\nNAMESPACE_BACKEND=lhm\nLHM_STORE_FILE={base}/legacy.meta\n','cannot be migrated implicitly')
(base/'summary.json').write_text(json.dumps(results,indent=2))
print('PASS',len(results),'startup rejection cases')
