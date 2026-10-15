#!/usr/bin/env python3
"""Real FUSE + zbctl optical preparation against a protocol fixture, no media simulation claim."""
import argparse
import errno
import os
from pathlib import Path
import select
import shutil
import subprocess
import tempfile
import time


def stop(process):
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--client', required=True)
    parser.add_argument('--fixture', required=True)
    parser.add_argument('--ctl', required=True)
    args = parser.parse_args()
    if not Path('/dev/fuse').exists() or not shutil.which('fusermount3'):
        print('SKIP: FUSE unavailable')
        return 77
    with tempfile.TemporaryDirectory(prefix='zb-optical-mount-') as tmp:
        root = Path(tmp)
        mount = root / 'mount'
        mount.mkdir()
        socket = root / 'control.sock'
        client = fixture = None
        with (root / 'fixture.log').open('w') as fixture_log, (root / 'client.log').open('w') as client_log:
            try:
                fixture = subprocess.Popen([args.fixture, '--serve-fixture'], stdout=subprocess.PIPE,
                                           stderr=fixture_log, text=True)
                if not select.select([fixture.stdout], [], [], 10)[0]:
                    raise RuntimeError('fixture did not start')
                address = fixture.stdout.readline().strip()
                client = subprocess.Popen([
                    args.client, f'--mds={address}', f'--scheduler={address}',
                    '--bootstrap_tier_dirs=false', '--optical_read_enabled=true',
                    f'--prepare_control_socket={socket}', '--', str(mount), '-f',
                    '-o', 'attr_timeout=0,entry_timeout=0',
                ], stdout=client_log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 10
                while not os.path.ismount(mount) and client.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                if not os.path.ismount(mount):
                    log = (root / 'client.log').read_text()
                    if any(s in log.lower() for s in ('operation not permitted', 'permission denied', 'device not found')):
                        print('SKIP: kernel denied mount\n' + log)
                        return 77
                    raise RuntimeError('mount failed:\n' + log)

                def ctl(op, value):
                    return subprocess.check_output([args.ctl, '--socket', str(socket), 'stage', op, str(value)],
                                                   text=True, timeout=10).strip()

                def expect_unprepared():
                    try:
                        fd = os.open(mount / 'file', os.O_RDONLY)
                    except OSError as exc:
                        assert exc.errno == errno.EAGAIN, exc
                    else:
                        os.close(fd)
                        raise AssertionError('unprepared open succeeded')

                expect_unprepared()
                handle = ctl('submit', mount / 'file')  # This process exits before wait starts.
                assert ctl('wait', handle) == str(mount / 'file')
                expected = b'ab\0cdefghi'
                fd1 = os.open(mount / 'file', os.O_RDONLY)
                fd2 = os.open(mount / 'file', os.O_RDONLY)
                try:
                    assert os.pread(fd1, 8, 8) == expected[8:]
                    assert os.pread(fd1, 32, 0) == expected
                    os.close(fd1)
                    fd1 = -1
                    assert os.pread(fd2, 5, 0) == expected[:5]
                    ctl('release', handle)
                    expect_unprepared()
                    assert os.pread(fd2, 32, 0) == expected
                finally:
                    if fd1 >= 0:
                        os.close(fd1)
                    os.close(fd2)
                handle = ctl('submit', mount / 'file')
                ctl('wait', handle)
                assert subprocess.check_output(['cat', str(mount / 'file')], timeout=10) == expected
                ctl('release', handle)
                empty = ctl('submit', mount / 'empty')
                ctl('wait', empty)
                assert (mount / 'empty').read_bytes() == b''
                ctl('release', empty)
                subprocess.run(['fusermount3', '-u', str(mount)], check=True, timeout=10)
                assert client.wait(timeout=10) == 0
                assert not socket.exists()
                print('PASS: real FUSE mount, separate zbctl processes, POSIX reads and release lifecycle')
                return 0
            except Exception:
                print((root / 'client.log').read_text())
                raise
            finally:
                if os.path.ismount(mount):
                    subprocess.run(['fusermount3', '-u', str(mount)], timeout=10)
                stop(client)
                stop(fixture)
                if fixture and fixture.stdout:
                    fixture.stdout.close()


if __name__ == '__main__':
    raise SystemExit(main())
