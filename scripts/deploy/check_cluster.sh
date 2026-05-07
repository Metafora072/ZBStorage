#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/client}"
ENV_PATH="${ENV_PATH:-${ROLE_DIR}/client.env}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"

set -a
# shellcheck disable=SC1090
source "${ENV_PATH}"
set +a

if command -v nc >/dev/null 2>&1; then
  echo "Checking MDS ${MDS_ADDR}"
  nc -z "${MDS_ADDR%:*}" "${MDS_ADDR##*:}"
  echo "Checking Scheduler ${SCHEDULER_ADDR}"
  nc -z "${SCHEDULER_ADDR%:*}" "${SCHEDULER_ADDR##*:}"
fi

exec "${BUILD_DIR}/system_demo_tool" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --mount_point="${MOUNT_POINT}" \
  --real_dir="${REAL_DIR_NAME:-real}" \
  --virtual_dir="${VIRTUAL_DIR_NAME:-virtual}" \
  --scenario=health
