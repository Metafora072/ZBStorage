#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"

ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/mds}"
CONFIG_PATH="${CONFIG_PATH:-${ROLE_DIR}/scheduler.conf}"
ENV_PATH="${ENV_PATH:-${ROLE_DIR}/mds.env}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
LOG_DIR="${LOG_DIR:-${ROLE_DIR}/logs}"
PID_DIR="${PID_DIR:-${ROLE_DIR}/pids}"

[[ -f "${ENV_PATH}" ]] && load_multi_host_env "${ENV_PATH}"
SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
SCHEDULER_HOST="${SCHEDULER_HOST:-${MDS_HOST:-127.0.0.1}}"

require_multi_host_bin "${BUILD_DIR}/scheduler_server"
start_with_log scheduler "${SCHEDULER_HOST}" "${SCHEDULER_PORT}" "${LOG_DIR}" "${PID_DIR}" \
  "${BUILD_DIR}/scheduler_server" --config="${CONFIG_PATH}" --port="${SCHEDULER_PORT}"
