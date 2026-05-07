#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"
ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/data}"
PID_DIR="${PID_DIR:-${ROLE_DIR}/pids}"

stop_by_pid_file "${PID_DIR}" "optical_node"
stop_by_pid_file "${PID_DIR}" "virtual_node"
stop_by_pid_file "${PID_DIR}" "real_node"
