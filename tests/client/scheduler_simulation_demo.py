#!/usr/bin/env python3
"""Manual production FUSE/MDS/Scheduler model demo; no mock data services."""
import argparse
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
TARGETS = ('scheduler_server', 'mds_server', 'zb_fuse_client', 'zbctl', 'scheduler_simulation_fixture')
NODES = {'client-model-a': 7, 'client-model-b': 8, 'client-model-optical': 9}


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def field(obj, name, default=None):
    parts = name.split('_')
    camel = parts[0] + ''.join(p.title() for p in parts[1:])
    return obj.get(name, obj.get(camel, default))


def rpc(address, service, method, **payload):
    request = urllib.request.Request(f'http://{address}/zb.rpc.{service}/{method}',
                                     json.dumps(payload).encode(), {'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, timeout=5) as response:
        result = json.load(response)
    require('status' in result and result['status'].get('code', 0) in (0, 'MDS_OK', 'SCHED_OK'),
            f'{method}: {result}')
    return result


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


class Environment:
    def __init__(self, args, root):
        self.args, self.root = args, root
        self.mount = root / 'mount'
        self.control_socket = root / 'prepare.sock'
        self.processes, self.logs = {}, []
        # Reserve all three simultaneously so they cannot repeat within this run.
        with socket.socket() as a, socket.socket() as b, socket.socket() as c:
            for sock in (a, b, c):
                sock.bind(('127.0.0.1', 0))
            self.control_port, self.data_port, self.mds_port = [s.getsockname()[1] for s in (a, b, c)]
        self.scheduler = f'127.0.0.1:{self.control_port}'
        self.data = f'127.0.0.1:{self.data_port}'
        self.mds = f'127.0.0.1:{self.mds_port}'

    def binary(self, name):
        return self.args.build_dir / name

    def ensure_alive(self):
        for name, process in self.processes.items():
            require(process.poll() is None, f'{name} exited ({process.returncode}); inspect {self.root}/{name}.log')

    def wait(self, predicate, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.ensure_alive()
            if predicate():
                return
            time.sleep(.1)
        raise TimeoutError(f'environment readiness timed out; logs: {self.root}')

    def launch(self, name, arguments):
        log = (self.root / f'{name}.log').open('w')
        self.logs.append(log)
        self.processes[name] = subprocess.Popen([str(x) for x in arguments], stdout=log,
                                               stderr=subprocess.STDOUT, start_new_session=True)

    def configure(self):
        self.mount.mkdir()
        for name, generation in list(NODES.items())[:2]:
            (self.root / f'{name}.conf').write_text(
                f'NODE_ID={name}\nINSTANCE_GENERATION={generation}\nDISK_IDS=disk-01,disk-02\n'
                f'ARCHIVE_META_DIR={name}-state\nDISK_CAPACITY_BYTES=67108864\n'
                'READ_BYTES_PER_SEC=8388608\nWRITE_BYTES_PER_SEC=8388608\n'
                'READ_BASE_LATENCY_MS=2\nWRITE_BASE_LATENCY_MS=2\nJITTER_MS=0\n'
                'MAX_CONCURRENT_IO=4\nIO_QUEUE_LIMIT=128\nREPLICATION_ENABLED=false\n')
        (self.root / 'optical.conf').write_text(
            'NODE_ID=client-model-optical\nINSTANCE_GENERATION=9\nSTATE_DIR=optical-state\n'
            'DRIVE_COUNT=1\nDISC_COUNT=2\nDISC_CAPACITY_BYTES=67108864\n'
            'DISC_READ_BYTES_PER_SEC=8388608\nCACHE_CAPACITY_BYTES=8388608\n'
            'CACHE_READ_BYTES_PER_SEC=33554432\n'
            f'LOAD_LATENCY_MS={self.args.prepare_ms}\nSEEK_LATENCY_MS=20\n'
            'ARCHIVE_TRANSFER_BYTES_PER_SEC=8388608\nARCHIVE_PACK_BYTES_PER_SEC=8388608\n'
            'ARCHIVE_BURN_BYTES_PER_SEC=8388608\nARCHIVE_BURN_FIXED_MS=100\n'
            'ARCHIVE_TAIL_WAIT_MS=100\nTASK_RETENTION_MS=300000\n')
        (self.root / 'scheduler.conf').write_text(
            'SIMULATION_DISK_CONFIGS=client-model-a.conf,client-model-b.conf\n'
            'SIMULATION_OPTICAL_CONFIGS=optical.conf\n'
            f'SIMULATION_DATA_PORT={self.data_port}\nSIMULATION_DATA_ADDRESS={self.data}\n'
            f'MDS_ADDRESS={self.mds}\nMANAGED_STATE_SNAPSHOT_PATH={self.root / "managed.pb"}\n'
            'CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS=100\nTICK_INTERVAL_MS=100\n')
        (self.root / 'mds.conf').write_text(
            f'MDS_DB_PATH={self.root / "mds-db"}\nSCHEDULER_ADDR={self.scheduler}\n'
            'MANAGED_NODE_ID=client-demo-mds\nOBJECT_UNIT_SIZE=16384\nREPLICA=1\n'
            'STRICT_TIER_BYPASS_PG=true\nENABLE_OPTICAL_ARCHIVE=false\n'
            f'ARCHIVE_META_ROOT={self.root / "archive"}\nMASSTREE_ROOT={self.root / "masstree"}\n')

    def catalog_ready(self):
        try:
            reply = rpc(self.mds, 'MdsService', 'GetCmsNodeCatalog')
        except (OSError, ValueError, RuntimeError):
            return False
        nodes = {}
        for node in reply.get('nodes', []):
            view = field(node, 'managed_view', {})
            spec = view.get('spec', {})
            nodes[field(spec, 'node_id', '')] = view
        for name, generation in NODES.items():
            view = nodes.get(name, {})
            ready = view.get('readiness', {})
            if (int(field(view.get('spec', {}), 'instance_generation', 0)) != generation or
                view.get('lifecycle') not in (2, 'MANAGED_LIFECYCLE_WORKING') or
                not all(field(ready, flag, False) for flag in
                        ('observed', 'initialization_complete', 'inventory_ready', 'metadata_ready'))):
                return False
        save_json(self.root / 'cms-ready.json', reply)
        return True

    def start(self):
        self.configure()
        self.launch('scheduler', [self.binary('scheduler_server'), f'--config={self.root / "scheduler.conf"}',
                                  f'--port={self.control_port}', '--bthread_concurrency=8'])
        self.launch('mds', [self.binary('mds_server'), f'--config={self.root / "mds.conf"}',
                            f'--port={self.mds_port}', '--bthread_concurrency=8'])
        self.wait(self.catalog_ready)
        self.launch('client', [self.binary('zb_fuse_client'), f'--mds={self.mds}', f'--scheduler={self.scheduler}',
            '--optical_read_enabled=true', f'--prepare_control_socket={self.control_socket}',
            '--default_object_unit_size=16384', '--client_direct_io=true', '--timeout_ms=30000',
            '--disk_io_workers=8', '--disk_io_per_node=2', '--data_route_log=true',
            '--io_latency_enabled=true', f'--io_latency_dir={self.root / "metrics"}',
            '--', self.mount, '-f', '-o', 'attr_timeout=0,entry_timeout=0,negative_timeout=0'])
        self.wait(lambda: os.path.ismount(self.mount) and self.control_socket.exists())
        self.wait(lambda: (self.mount / 'virtual').is_dir())
        save_json(self.root / 'environment.json', dict(mount=str(self.mount), mds=self.mds,
            scheduler=self.scheduler, data=self.data, nodes=NODES,
            processes={name: p.pid for name, p in self.processes.items()}, prepare_ms=self.args.prepare_ms))

    def location(self, path):
        lookup = rpc(self.mds, 'MdsService', 'Lookup', path=path)
        inode = field(lookup['attr'], 'inode_id')
        return rpc(self.mds, 'MdsService', 'GetFileLocation', inode_id=str(inode))['location']

    def archive(self, path):
        completed = subprocess.run([str(self.binary('scheduler_simulation_fixture')), self.mds, path,
            'client-model-optical', 'model-disc-1', '60000'], capture_output=True, text=True, timeout=70)
        with (self.root / 'archive-fixture.log').open('a') as log:
            log.write(completed.stdout + completed.stderr)
        require(completed.returncode == 0, 'archive failed; inspect archive-fixture.log')
        return json.loads(completed.stdout)

    def ctl(self, command, argument, timeout=15):
        return subprocess.check_output([str(self.binary('zbctl')), '--socket', str(self.control_socket),
                                        'stage', command, str(argument)], text=True, timeout=timeout).strip()

    def snapshot(self):
        return {name: rpc(self.scheduler, 'SchedulerService', 'GetManagedNode', node_id=name)['node']
                for name in NODES}

    def close(self):
        try:
            if os.path.ismount(self.mount):
                unmounted = subprocess.run(['fusermount3', '-u', str(self.mount)], capture_output=True, timeout=10)
                if unmounted.returncode:
                    subprocess.run(['fusermount3', '-uz', str(self.mount)], check=True, timeout=10)
        finally:
            for name in ('client', 'mds', 'scheduler'):
                stop(self.processes.get(name))
            for log in self.logs:
                log.close()


def write_shell_environment(env):
    values = {'ZB_DEMO_RUN': env.root, 'ZB_DEMO_MOUNT': env.mount,
              'ZB_DEMO_SOCKET': env.control_socket, 'ZB_DEMO_CTL': env.binary('zbctl')}
    lines = ['# source in Bash; commands below operate the production FUSE mount.']
    lines += [f'export {key}={shlex.quote(str(value))}' for key, value in values.items()]
    lines += [r'''zb_prepare() {
    if [[ $# != 1 ]]; then echo '用法：zb_prepare 文件路径' >&2; return 2; fi
    local target="$1" handle
    [[ "$target" = /* ]] || target="$PWD/$target"
    handle=$("$ZB_DEMO_CTL" --socket "$ZB_DEMO_SOCKET" stage submit "$target") || return
    echo "准备请求已受理，任务：$handle"
    echo '已返回；收到启动终端的就绪通知后，再用 cat 读取。'
}''']
    (env.root / 'demo-env.sh').write_text('\n'.join(lines) + '\n')


def demonstrate(env):
    directory = env.mount / 'virtual'
    (directory / 'disk.bin').write_bytes(b'D' * 65536)
    # Real FUSE writes establish the source object and metadata; fixture only archives it.
    (directory / 'hello.txt').write_bytes(b'client optical model demonstration\n')
    print('正在通过公开接口归档 hello.txt 到仿真光盘……', flush=True)
    location = env.archive('/virtual/hello.txt')
    save_json(env.root / 'optical-location.json', location)
    write_shell_environment(env)
    save_json(env.root / 'result.json', {'status': 'READY', 'mode': 'interactive', 'model_payload': 'generated x bytes'})
    commands = f'''source {shlex.quote(str(env.root / 'demo-env.sh'))}
cd "$ZB_DEMO_MOUNT/virtual"
ls -lh
cat disk.bin | wc -c
cat hello.txt          # 准备前返回 EAGAIN
zb_prepare hello.txt  # 立即返回
# 启动终端显示“收到节点就绪通知”后：
cat hello.txt
'''
    (env.root / 'commands.txt').write_text(commands)
    print(f'\n演示已就绪：正式 Scheduler、MDS 和 FUSE；3 个模型实例均在 Scheduler 内。\n'
          f'控制端口={env.scheduler}；数据端口={env.data}\n'
          '模型不保存原始业务内容，cat 显示生成的 x 字符是预期结果。\n'
          '在另一个 Bash 终端执行：\n\n' + commands +
          '\n返回本终端按 Ctrl+C，自动卸载并停止本次进程。', flush=True)
    with (env.root / 'client.log').open() as log:
        log.seek(0, os.SEEK_END)
        while True:
            env.ensure_alive()
            for line in log.readlines():
                if '[optical] ready ' in line:
                    print('收到节点就绪通知，可以再次 cat 文件。 ' + line.strip(), flush=True)
                elif '[optical] accepted ' in line:
                    print('客户端已提交异步准备。 ' + line.strip(), flush=True)
            time.sleep(.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=REPO / 'build')
    parser.add_argument('--no-build', action='store_true', help='Use existing binaries')
    parser.add_argument('--check', action='store_true', help='Run manual end-to-end verification, then clean up')
    parser.add_argument('--prepare-ms', type=int, default=5000, help='Modeled optical loading delay; default 5000 ms')
    args = parser.parse_args()
    require(1000 <= args.prepare_ms <= 3600000, '--prepare-ms must be 1000..3600000')
    args.build_dir = args.build_dir.resolve()
    require(Path('/dev/fuse').exists() and shutil.which('fusermount3'), 'FUSE is unavailable; this demo requires a real mount')
    results = HERE / 'results'
    results.mkdir(exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='scheduler-model-', dir=results))
    print(f'运行目录：{root}', flush=True)
    env = None
    def interrupted(signum, frame):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        raise KeyboardInterrupt
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if not args.no_build:
            print(f'构建所需目标，输出保存到 {root / "build.log"}', flush=True)
            with (root / 'build.log').open('w') as log:
                # Reconfigure because manual fixture is EXCLUDE_FROM_ALL and may be new.
                subprocess.run(['cmake', '-S', str(REPO), '-B', str(args.build_dir)], check=True,
                               stdout=log, stderr=subprocess.STDOUT)
                subprocess.run(['cmake', '--build', str(args.build_dir), '-j4', '--target', *TARGETS],
                               check=True, stdout=log, stderr=subprocess.STDOUT)
        for target in TARGETS:
            require((args.build_dir / target).is_file(), f'missing binary: {target}')
        env = Environment(args, root)
        print('启动正式模块，等待库存就绪及 CMS 发布……', flush=True)
        env.start()
        if args.check:
            from scheduler_simulation_io import verify
            result = verify(env)
            save_json(root / 'result.json', result)
            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        else:
            demonstrate(env)
        return 0
    except KeyboardInterrupt:
        print('\n停止演示，清理本次挂载和进程。', flush=True)
        return 130 if args.check else 0
    except Exception as error:
        save_json(root / 'result.json', {'status': 'FAIL', 'error': str(error)})
        print(f'FAIL: {error}\n日志：{root}', flush=True)
        return 1
    finally:
        if env:
            env.close()


if __name__ == '__main__':
    raise SystemExit(main())
