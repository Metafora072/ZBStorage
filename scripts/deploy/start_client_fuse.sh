#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"

ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/client}"
ENV_PATH="${ENV_PATH:-${ROLE_DIR}/client.env}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
LOG_DIR="${LOG_DIR:-${ROLE_DIR}/logs}"
PID_DIR="${PID_DIR:-${ROLE_DIR}/pids}"

# Preserve caller overrides when client.env supplies defaults.
io_latency_overrides=()
for io_latency_key in IO_LATENCY_ENABLED IO_LATENCY_DIR IO_LATENCY_FLUSH_MS IO_LATENCY_BATCH_RECORDS IO_LATENCY_QUEUE_CAPACITY; do
  if [[ -v "${io_latency_key}" ]]; then
    io_latency_overrides+=("${io_latency_key}=${!io_latency_key}")
  fi
done
load_multi_host_env "${ENV_PATH}"
for io_latency_override in "${io_latency_overrides[@]}"; do
  printf -v "${io_latency_override%%=*}" '%s' "${io_latency_override#*=}"
done
source "${ROOT_DIR}/scripts/common_demo_env.sh"
prepare_io_latency_args "${ROOT_DIR}"
require_multi_host_bin "${BUILD_DIR}/zb_fuse_client"

mkdir -p "${RUN_DIR}" "${MOUNT_POINT}" "${LOG_DIR}" "${PID_DIR}"

"${BUILD_DIR}/zb_fuse_client" \
  "${IO_LATENCY_ARGS[@]}" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --bootstrap_tier_dirs=true \
  --real_dir_name="${REAL_DIR_NAME:-real}" \
  --virtual_dir_name="${VIRTUAL_DIR_NAME:-virtual}" \
  -- "${MOUNT_POINT}" -f > "${LOG_DIR}/fuse.log" 2>&1 &

write_pid_file "${PID_DIR}" "fuse" "$!"
multi_host_log "[OK] fuse started mount=${MOUNT_POINT}"
