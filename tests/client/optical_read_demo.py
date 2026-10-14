#!/usr/bin/env python3
"""One-command, persistent FUSE optical-read demonstration using external mocks only."""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import time

from client_optical_external_test import HERE, require, run

REPO = HERE.parent.parent


class DemoStop(Exception):
    pass


def request_stop(signum, frame):
    # Children have separate sessions. Only this supervisor receives terminal Ctrl+C;
    # its finally block unmounts FUSE before shutting down either dependent service.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    raise DemoStop()


def save_state(root, state):
    pending = root / 'demo.json.tmp'
    pending.write_text(json.dumps(state, ensure_ascii=False, indent=2) + '\n')
    pending.replace(root / 'demo.json')


def write_shell_environment(args, root):
    variables = {
        'ZB_DEMO_RUN': root,
        'ZB_DEMO_MOUNT': root / 'mount',
        'ZB_DEMO_EXPECTED': root / 'expected',
        'ZB_DEMO_SOCKET': root / 'control.sock',
        'ZB_DEMO_CTL': args.ctl,
    }
    lines = ['# Source this file in the terminal used for manual POSIX operations.']
    lines.extend(f'export {name}={shlex.quote(str(value))}' for name, value in variables.items())
    lines.append('source ' + shlex.quote(str(HERE / 'optical_demo_commands.sh')))
    (root / 'demo-env.sh').write_text('\n'.join(lines) + '\n')


COMMANDS = r'''cd "$ZB_DEMO_MOUNT"
ls -lh
cat hello.txt          # 尚未准备，预期返回 EAGAIN
zb_prepare hello.txt   # 仅提交，立即返回，不阻塞终端

# 在启动终端看到“收到节点就绪通知”后执行：
cat hello.txt
# 可以多次 cat，无需释放；缓存由光盘节点内部管理。
'''


def hold_for_demo(args, root, files, locations, env, node, client, result):
    write_shell_environment(args, root)
    source = f'source {shlex.quote(str(root / "demo-env.sh"))}'
    (root / 'commands.txt').write_text(source + '\n\n' + COMMANDS)
    result.update({
        'status': 'READY', 'mount': str(root / 'mount'), 'socket': str(root / 'control.sock'),
        'mock_address': env.address, 'client_pid': client.pid, 'optical_pid': node.pid,
        'environment_pid': env.process.pid,
    })
    save_state(root, result)
    print('\n演示环境已就绪，文件尚未准备或预读。', flush=True)
    print(f'模拟服务 PID={env.process.pid}；光盘节点 PID={node.pid}；FUSE PID={client.pid}', flush=True)
    print('文件位置（来自正式节点的归档报告）：', flush=True)
    for inode in sorted(locations):
        loc, file = locations[inode], files[inode]
        print(f"  {file['name']:<14} {file['size']:>9} bytes  光盘={loc['disc']} 镜像={loc['image']} inode={inode}", flush=True)
    print('\n在另一个 Bash 终端先执行：\n\n  ' + source, flush=True)
    print('\n然后按下面的 POSIX 流程操作；zb_prepare 仅提交，完成通知在本终端展示。\n', flush=True)
    print(COMMANDS, flush=True)
    print('本终端显示客户端事件。演示结束后回到本终端按 Ctrl+C，自动卸载并停止各进程。', flush=True)
    print('若文件已准备过，再次准备可能命中缓存；another.bin 在另一张光盘，可演示另一次冷读。', flush=True)

    accepted = {}
    ready_requests = set()
    last_tick = time.monotonic()
    # Follow existing client output, not optical status RPCs. No prepare request is sent here.
    with (root / 'client.log').open() as log:
        try:
            while True:
                for proc in (env.process, node, client):
                    require(proc.poll() is None, f'演示进程提前退出: {proc.args[0]} exit={proc.returncode}')
                require(os.path.ismount(root / 'mount'), 'FUSE 挂载已被外部卸载')
                for line in log.readlines():
                    event = re.search(r'\[optical\] (accepted|ready|opened|closed|released) request=(\S+)', line)
                    if not event:
                        continue
                    phase, request = event.groups()
                    if phase == 'accepted':
                        if request not in ready_requests:
                            accepted[request] = time.monotonic()
                        print(f'[提交已返回] {line.strip()}', flush=True)
                    elif phase == 'ready':
                        accepted.pop(request, None)
                        ready_requests.add(request)
                        elapsed = re.search(r'prepare_us=(\d+)', line)
                        seconds = f'，准备耗时 {int(elapsed[1]) / 1e6:.3f} 秒' if elapsed else ''
                        print(f'[收到节点就绪通知{seconds}] {line.strip()}', flush=True)
                    else:
                        label = {'opened': 'POSIX 打开成功', 'closed': '读取会话已关闭',
                                 'released': '准备结果已释放'}[phase]
                        print(f'[{label}] {line.strip()}', flush=True)
                now = time.monotonic()
                if accepted and now - last_tick >= 5:
                    for request, started in accepted.items():
                        if now - started >= 5:
                            print(f'[等待主动通知] {request} 已等待 {now-started:.0f}s（本地计时，无状态轮询）', flush=True)
                    last_tick = now
                time.sleep(0.2)
        except DemoStop:
            print('\n正在卸载 FUSE 并停止演示环境……', flush=True)
            result['external_calls'] = json.loads(env.command('STATS').removeprefix('STATS '))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=REPO / 'build')
    parser.add_argument('--skip-build', action='store_true', help='use existing executables without invoking CMake')
    parser.add_argument('--jobs', type=int, default=4)
    parser.add_argument('--work-dir', type=Path, help='new empty directory; default tests/client/results/')
    parser.add_argument('--timeout', type=int, default=240, help='archive startup deadline, seconds')
    args = parser.parse_args()
    require(args.jobs > 0 and args.timeout > 0, 'jobs 和 timeout 必须为正数')
    if not Path('/dev/fuse').exists() or not shutil.which('fusermount3'):
        print('无法启动：需要 /dev/fuse、fusermount3 和 FUSE 挂载权限。')
        return 1
    if args.work_dir:
        root = args.work_dir.resolve()
        root.mkdir(parents=True, exist_ok=True)
        require(not any(root.iterdir()), 'work-dir 必须为空，避免覆盖已有演示数据')
    else:
        results = HERE / 'results'
        results.mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix='optical-demo-', dir=results))
    require(len(str(root / 'control.sock').encode()) < 104, 'work-dir 路径过长，无法创建 Unix socket')
    print(f'演示目录：{root}', flush=True)
    args.build_dir = args.build_dir.resolve()
    args.environment = args.build_dir / 'optical_external_services'
    args.optical = args.build_dir / 'optical_node_server'
    args.client = args.build_dir / 'zb_fuse_client'
    args.ctl = args.build_dir / 'zbctl'
    args.demo = True
    state = {'status': 'STARTING', 'supervisor_pid': os.getpid(), 'work_dir': str(root),
             'reads': [], 'mode': 'manual_demo'}
    started = time.monotonic()
    save_state(root, state)
    previous = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if not args.skip_build:
            print(f'正在增量构建，输出保存到 {root / "build.log"}', flush=True)
            with (root / 'build.log').open('w') as log:
                subprocess.run(['cmake', '-S', str(REPO), '-B', str(args.build_dir), '-DBUILD_TESTING=ON'],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
                subprocess.run(['cmake', '--build', str(args.build_dir), '-j', str(args.jobs), '--target',
                                'optical_external_services', 'optical_node_server', 'zb_fuse_client', 'zbctl'],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
        for executable in (args.environment, args.optical, args.client, args.ctl):
            require(executable.is_file() and os.access(executable, os.X_OK), f'缺少可执行文件: {executable}')
        run(args, root, state, on_ready=hold_for_demo)
        require(not os.path.ismount(root / 'mount'), '演示结束后挂载仍存在')
        state['status'] = 'STOPPED'
        print(f'演示已停止。日志、原始文件和命令保留在 {root}', flush=True)
        return 0
    except DemoStop:
        state['status'] = 'INTERRUPTED'
        print('启动已取消，已执行环境清理。', flush=True)
        return 130
    except Exception as exc:
        state.update(status='FAILED', error=str(exc))
        print(f'演示失败：{exc}\n日志目录：{root}', flush=True)
        return 1
    finally:
        state['elapsed_seconds'] = round(time.monotonic() - started, 2)
        save_state(root, state)
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == '__main__':
    raise SystemExit(main())
