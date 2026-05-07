#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${DEMO_ROOT}/mnt}"
TEMPLATE_ID="${1:-${MASSTREE_TEMPLATE_ID:-}}"
PATH_LIST_FILE="${2:-${MASSTREE_PATH_LIST_FILE:-}}"
REPEAT_DIR_PREFIX="${3:-${MASSTREE_REPEAT_DIR_PREFIX:-copy}}"
LEAF_NODES_ARE_FILES="${4:-${MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES:-false}}"
VERIFY_INODE_SAMPLES="${MASSTREE_VERIFY_INODE_SAMPLES:-32}"
VERIFY_DENTRY_SAMPLES="${MASSTREE_VERIFY_DENTRY_SAMPLES:-32}"

if [[ -z "${TEMPLATE_ID}" || -z "${PATH_LIST_FILE}" ]]; then
  echo "Usage: $0 <template_id> <path_list_file> [repeat_dir_prefix] [leaf_nodes_are_files]"
  echo "Environment:"
  echo "  BUILD_DIR, MDS_ADDR, SCHEDULER_ADDR, MOUNT_POINT"
  echo "  MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES"
  echo "  MASSTREE_VERIFY_INODE_SAMPLES, MASSTREE_VERIFY_DENTRY_SAMPLES"
  exit 1
fi

if [[ ! -f "${PATH_LIST_FILE}" ]]; then
  echo "Missing path list file: ${PATH_LIST_FILE}"
  exit 1
fi

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool"
  exit 1
fi

exec "${BUILD_DIR}/system_demo_tool" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --mount_point="${MOUNT_POINT}" \
  --scenario=masstree_template \
  --masstree_template_id="${TEMPLATE_ID}" \
  --masstree_path_list_file="${PATH_LIST_FILE}" \
  --masstree_repeat_dir_prefix="${REPEAT_DIR_PREFIX}" \
  --masstree_path_list_leaf_nodes_are_files="${LEAF_NODES_ARE_FILES}" \
  --masstree_verify_inode_samples="${VERIFY_INODE_SAMPLES}" \
  --masstree_verify_dentry_samples="${VERIFY_DENTRY_SAMPLES}"
