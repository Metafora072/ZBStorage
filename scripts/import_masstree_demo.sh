#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${DEMO_ROOT}/mnt}"
NAMESPACE_PREFIX="${NAMESPACE_PREFIX:-demo-ns}"
TEMPLATE_ID="${MASSTREE_TEMPLATE_ID:-}"
TEMPLATE_MODE="${MASSTREE_TEMPLATE_MODE:-page_fast}"
VERIFY_INODE_SAMPLES="${MASSTREE_VERIFY_INODE_SAMPLES:-32}"
VERIFY_DENTRY_SAMPLES="${MASSTREE_VERIFY_DENTRY_SAMPLES:-32}"

NAMESPACE_COUNT="${1:-}"

next_namespace_start() {
  local prefix="$1"
  local namespaces_dir="${DEMO_ROOT}/data/mds/masstree_meta/namespaces"
  local max_seq=0
  local name suffix seq
  if [[ -d "${namespaces_dir}" ]]; then
    while IFS= read -r name; do
      suffix="${name#${prefix}-}"
      if [[ "${suffix}" == "${name}" ]]; then
        continue
      fi
      if [[ "${suffix}" =~ ^[0-9]+$ ]]; then
        seq=$((10#${suffix}))
        if (( seq > max_seq )); then
          max_seq="${seq}"
        fi
      fi
    done < <(find "${namespaces_dir}" -maxdepth 1 -mindepth 1 -type d -printf '%f\n' 2>/dev/null)
  fi
  echo $((max_seq + 1))
}

if [[ -z "${NAMESPACE_COUNT}" ]]; then
  echo "Usage: $0 <namespace_count>"
  exit 1
fi

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool"
  exit 1
fi

if ! [[ "${NAMESPACE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${NAMESPACE_COUNT}" -le 0 ]]; then
  echo "namespace_count must be a positive integer"
  exit 1
fi

START_SEQUENCE="$(next_namespace_start "${NAMESPACE_PREFIX}")"

for ((i=1; i<=NAMESPACE_COUNT; ++i)); do
  NAMESPACE_ID="$(printf "%s-%06d" "${NAMESPACE_PREFIX}" "$((START_SEQUENCE + i - 1))")"
  if [[ -n "${TEMPLATE_ID}" ]]; then
    echo "==== importing namespace ${i}/${NAMESPACE_COUNT}: ${NAMESPACE_ID} (template=${TEMPLATE_ID}) ===="
  else
    echo "==== importing namespace ${i}/${NAMESPACE_COUNT}: ${NAMESPACE_ID} (template=auto) ===="
  fi

  CMD=(
    "${BUILD_DIR}/system_demo_tool"
    --mds="${MDS_ADDR}" \
    --scheduler="${SCHEDULER_ADDR}"
    --mount_point="${MOUNT_POINT}"
    --scenario=masstree_import
    --masstree_namespace_id="${NAMESPACE_ID}"
    --masstree_template_mode="${TEMPLATE_MODE}"
    --masstree_verify_inode_samples="${VERIFY_INODE_SAMPLES}"
    --masstree_verify_dentry_samples="${VERIFY_DENTRY_SAMPLES}"
  )
  if [[ -n "${TEMPLATE_ID}" ]]; then
    CMD+=(--masstree_template_id="${TEMPLATE_ID}")
  fi
  "${CMD[@]}"
done
