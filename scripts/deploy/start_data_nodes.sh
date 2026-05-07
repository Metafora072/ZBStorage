#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"

ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/data}"
ENV_PATH="${ENV_PATH:-${ROLE_DIR}/data.env}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
LOG_DIR="${LOG_DIR:-${ROLE_DIR}/logs}"
PID_DIR="${PID_DIR:-${ROLE_DIR}/pids}"

load_multi_host_env "${ENV_PATH}"

DATA_HOST="${DATA_HOST:-127.0.0.1}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"
OPTICAL_PORT="${OPTICAL_PORT:-39080}"
ENABLE_OPTICAL_NODE="${ENABLE_OPTICAL_NODE:-false}"

require_multi_host_bin "${BUILD_DIR}/real_node_server"
require_multi_host_bin "${BUILD_DIR}/virtual_node_server"

start_with_log real_node "${DATA_HOST}" "${REAL_PORT}" "${LOG_DIR}" "${PID_DIR}" \
  "${BUILD_DIR}/real_node_server" --config="${ROLE_DIR}/real_node.conf" --port="${REAL_PORT}"

start_with_log virtual_node "${DATA_HOST}" "${VIRTUAL_PORT}" "${LOG_DIR}" "${PID_DIR}" \
  "${BUILD_DIR}/virtual_node_server" --config="${ROLE_DIR}/virtual_node.conf" --port="${VIRTUAL_PORT}"

if [[ "${ENABLE_OPTICAL_NODE}" == "true" ]]; then
  require_multi_host_bin "${BUILD_DIR}/optical_node_server"
  start_with_log optical_node "${DATA_HOST}" "${OPTICAL_PORT}" "${LOG_DIR}" "${PID_DIR}" \
    "${BUILD_DIR}/optical_node_server" --config="${ROLE_DIR}/optical_node.conf" --port="${OPTICAL_PORT}"
fi
