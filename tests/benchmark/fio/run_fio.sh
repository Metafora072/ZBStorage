#!/usr/bin/env bash
set -euo pipefail

FIO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${FIO_DIR}/../../.." && pwd)"
ROOT_PATH="$(sed -n 's/^ROOT_PATH=//p' "${ROOT_DIR}/config/base.conf" | head -n 1)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
LOG_DIR="${FIO_DIR}/log/${RUN_ID}"
FILE_LIST="${LOG_DIR}/files.txt"
IOLOG="${LOG_DIR}/first16k.iolog"
OUTPUT="${LOG_DIR}/fio.txt"
source "${FIO_DIR}/config/run_fio.conf"
TEST_DIR="${ROOT_PATH}/mnt/${DATA_DIR}"

mkdir -p "${LOG_DIR}"
find "${TEST_DIR}" -maxdepth 1 -type f | sort > "${FILE_LIST}"

echo "fio version 2 iolog" > "${IOLOG}"
while IFS= read -r file; do
  printf '%s add\n%s open\n%s read 0 %s\n%s close\n' \
    "${file}" "${file}" "${file}" "${BLOCK_SIZE}" "${file}" >> "${IOLOG}"
done < "${FILE_LIST}"

fio "${FIO_OPTIONS[@]}"

echo "${LOG_DIR}"
