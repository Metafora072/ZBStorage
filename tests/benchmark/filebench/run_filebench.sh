#!/usr/bin/env bash
set -euo pipefail

FILEBENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${FILEBENCH_DIR}/../../.." && pwd)"
ROOT_PATH="$(sed -n 's/^ROOT_PATH=//p' "${ROOT_DIR}/config/base.conf" | head -n 1)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${FILEBENCH_DIR}/log/${RUN_ID}"
TEST_DIR="${ROOT_PATH}/mnt/virtual/filebench/${RUN_ID}"
DATA_DIR="${TEST_DIR}/zb_data"
CONFIG="${FILEBENCH_DIR}/run_filebench.conf"
OUTPUT="${LOG_DIR}/filebench.txt"

mkdir -p "${LOG_DIR}" "${DATA_DIR}"
cp "${CONFIG}" "${LOG_DIR}/run_filebench.conf"

(
  cd "${TEST_DIR}"
  setarch "$(uname -m)" -R filebench -f "${CONFIG}"
) > "${OUTPUT}" 2>&1

echo "${LOG_DIR}"
