#!/usr/bin/env python3
"""Check real launch/render/stop scripts with isolated files and owned processes."""
import os
from pathlib import Path
import runpy
import shlex
import subprocess
import sys
import tempfile
import time
import unittest

REPO = Path(__file__).resolve().parents[2]
RESULTS = REPO / "tests/client/results"
validate_shutdown = runpy.run_path(str(Path(__file__).with_name("client_io_latency_mount_test.py")))["validate_shutdown"]


class MountShutdownTest(unittest.TestCase):
    summary = "[io_latency] submitted=2 written=2 synced=2 dropped=0 failed=0\n"

    def test_libfuse_sigterm_exit_codes(self):
        for code in (0, 7, 8):
            with self.subTest(code=code):
                validate_shutdown(code, True, self.summary, 2)

    def test_normal_unmount_requires_zero(self):
        validate_shutdown(0, False, self.summary, 2)
        for code in (7, 8):
            with self.subTest(code=code), self.assertRaises(RuntimeError):
                validate_shutdown(code, False, self.summary, 2)

    def test_crashes_and_startup_failures_still_fail(self):
        for code in (-15, -9, -11, 1, 3, 4, 6):
            with self.subTest(code=code), self.assertRaises(RuntimeError):
                validate_shutdown(code, True, self.summary, 2)

    def test_incomplete_recording_still_fails(self):
        bad_logs = (
            "",
            self.summary.replace("written=2", "written=1"),
            self.summary.replace("synced=2", "synced=1"),
            self.summary.replace("submitted=2", "submitted=3"),
            self.summary.replace("dropped=0", "dropped=1"),
            self.summary.replace("failed=0", "failed=1"),
        )
        for log_text in bad_logs:
            with self.subTest(log=log_text), self.assertRaises(RuntimeError):
                validate_shutdown(8, True, log_text, 2)
        with self.assertRaises(RuntimeError):
            validate_shutdown(8, True, self.summary, 1)


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        RESULTS.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix="workflow-", dir=RESULTS)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.env = dict(os.environ)
        for key in list(self.env):
            if key.startswith("IO_LATENCY_") or key in {"CONFIG_BASE_FILE", "ROLE_DIR", "ENV_PATH", "BUILD_DIR", "LOG_DIR", "PID_DIR"}:
                self.env.pop(key)

    def helper(self, **overrides):
        env = {**self.env, **overrides}
        result = subprocess.run([
            "bash", "-c", 'source "$1"; prepare_io_latency_args "$2"; printf "%s\\0" "${IO_LATENCY_ARGS[@]}"',
            "test", str(REPO / "scripts/common_demo_env.sh"), str(self.root),
        ], env=env, capture_output=True, check=True)
        return result.stdout.decode().rstrip("\0").split("\0")

    def test_default_root_ignores_other_runtime_overrides(self):
        config = self.root / "config/base.conf"
        config.parent.mkdir()
        config.write_text("# comment\nROOT_PATH=runtime with spaces\n")
        args = self.helper(IO_LATENCY_ENABLED="true", RUN_DIR="/wrong/run", DEMO_ROOT="/wrong/demo", CLIENT_ROOT="/wrong/client")
        self.assertIn(f"--io_latency_dir={self.root}/runtime with spaces/client/metrics", args)
        config.unlink()
        args = self.helper(IO_LATENCY_ENABLED="true")
        self.assertIn(f"--io_latency_dir={self.root}/.demo_run/client/metrics", args)

    def test_explicit_directory_skips_config(self):
        directory = str(self.root / "custom $(must_not_execute) results")
        args = self.helper(IO_LATENCY_ENABLED="true", IO_LATENCY_DIR=directory, CONFIG_BASE_FILE=str(self.root))
        self.assertIn(f"--io_latency_dir={directory}", args)
        self.assertFalse(any(directory + "/client/metrics" in arg for arg in args))
        self.assertEqual(self.helper(IO_LATENCY_ENABLED="false", CONFIG_BASE_FILE=str(self.root)), ["--io_latency_enabled=false"])
        self.assertFalse(Path(directory).exists())

    def test_render_quoted_directory(self):
        config = self.root / "cluster.env"
        config.write_text("MDS_HOST=127.0.0.1\nDATA_HOST=127.0.0.1\n")
        output = self.root / "rendered"
        directory = str(self.root / 'space $(echo bad) `echo bad` "quotes"')
        subprocess.run(["bash", str(REPO / "scripts/deploy/render_multi_host_configs.sh"), str(config), str(output)],
                       env={**self.env, "IO_LATENCY_DIR": directory}, check=True, capture_output=True)
        client_env = output / "client/client.env"
        self.assertNotIn("@IO_LATENCY_", client_env.read_text())
        result = subprocess.run(["bash", "-c", 'source "$1"; printf "%s" "$IO_LATENCY_DIR"', "test", str(client_env)],
                                env=self.env, check=True, capture_output=True, text=True)
        self.assertEqual(result.stdout, directory)

    def test_launcher_preserves_explicit_environment(self):
        build = self.root / "bin"
        build.mkdir()
        fake = build / "zb_fuse_client"
        fake.write_text('#!/usr/bin/env bash\nprintf "%s\\0" "$@" > "$ARG_CAPTURE"\n')
        fake.chmod(0o755)
        config = self.root / "client.env"
        config.write_text("\n".join(f"{key}={shlex.quote(value)}" for key, value in {
            "MDS_ADDR": "127.0.0.1:9000", "SCHEDULER_ADDR": "127.0.0.1:9100",
            "RUN_DIR": str(self.root / "run"), "MOUNT_POINT": str(self.root / "mnt"),
            "IO_LATENCY_ENABLED": "false", "IO_LATENCY_DIR": "", "IO_LATENCY_FLUSH_MS": "1000",
        }.items()))
        capture = self.root / "args"
        directory = str(self.root / "explicit metrics")
        subprocess.run(["bash", str(REPO / "scripts/deploy/start_client_fuse.sh")], check=True, capture_output=True,
                       env={**self.env, "ENV_PATH": str(config), "BUILD_DIR": str(build), "ROLE_DIR": str(self.root),
                            "ARG_CAPTURE": str(capture), "IO_LATENCY_ENABLED": "true", "IO_LATENCY_DIR": directory,
                            "IO_LATENCY_FLUSH_MS": "37", "CONFIG_BASE_FILE": str(self.root)})
        for _ in range(100):
            if capture.exists() and capture.stat().st_size:
                break
            time.sleep(0.01)
        args = capture.read_bytes().decode().rstrip("\0").split("\0")
        self.assertIn("--io_latency_enabled=true", args)
        self.assertIn("--io_latency_flush_ms=37", args)
        self.assertEqual([a for a in args if a.startswith("--io_latency_dir=")], [f"--io_latency_dir={directory}"])

    def test_stop_waits_for_flush(self):
        marker = self.root / "flushed"
        code = '''import signal, sys, time
from pathlib import Path
def stop(*args):
    time.sleep(1.3)
    Path(sys.argv[1]).write_text("flushed")
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
print("ready", flush=True)
time.sleep(30)
'''
        child = subprocess.Popen([sys.executable, "-c", code, str(marker)], stdout=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), "ready")
            pid_dir = self.root / "pids"
            pid_dir.mkdir()
            (pid_dir / "fuse.pid").write_text(str(child.pid))
            subprocess.run(["bash", str(REPO / "scripts/deploy/stop_client_fuse.sh")], check=True, capture_output=True,
                           env={**self.env, "PID_DIR": str(pid_dir), "IO_LATENCY_STOP_TIMEOUT_SEC": "5"}, timeout=10)
            self.assertEqual(child.wait(timeout=2), 0)
            self.assertEqual(marker.read_text(), "flushed")
            self.assertFalse((pid_dir / "fuse.pid").exists())
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()


if __name__ == "__main__":
    unittest.main()
