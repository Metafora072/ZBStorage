#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
TOOL="${RANDOM_READ_6B_TOOL:-${BUILD_DIR}/random_read_6b_files}"

if [[ ! -x "${TOOL}" ]]; then
  echo "Missing executable: ${TOOL}" >&2
  echo "Build it first:" >&2
  echo "  cmake --build ${BUILD_DIR} --target random_read_6b_files -j\$(nproc)" >&2
  exit 1
fi

exec "${TOOL}" "$@"
