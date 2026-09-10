#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"
ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/client}"
PID_DIR="${PID_DIR:-${ROLE_DIR}/pids}"

source "${ROOT_DIR}/scripts/common_demo_env.sh"
client_pid="$(read_pid_file "${PID_DIR}" "fuse" || true)"
if [[ -n "${client_pid}" ]]; then
  stop_fuse_client_pid "${client_pid}"
  multi_host_log "[STOP] fuse pid=${client_pid}"
fi
rm -f "${PID_DIR}/fuse.pid"
