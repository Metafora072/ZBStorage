#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
RUN_DIR="${RUN_DIR:-${DEMO_ROOT}}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
LOG_DIR="${RUN_DIR}/logs"
MOUNT_POINT="${MOUNT_POINT:-${RUN_DIR}/mnt}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
TEMPLATE_ID="${1:-${MASSTREE_TEMPLATE_ID:-template-pathlist-100m-demo}}"
PATH_LIST_FILE="${2:-${MASSTREE_PATH_LIST_FILE:-}}"
REPEAT_DIR_PREFIX="${MASSTREE_REPEAT_DIR_PREFIX:-copy}"
LEAF_NODES_ARE_FILES="${MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES:-false}"
TEMPLATE_MODE="${MASSTREE_TEMPLATE_MODE:-page_fast}"
SHOWCASE_LOG="${LOG_DIR}/demo_showcase_$(date '+%Y%m%d_%H%M%S').log"

mkdir -p "${LOG_DIR}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool" >&2
  exit 1
fi

cleanup() {
  log "[INFO] stopping demo stack"
  RUN_DIR="${RUN_DIR}" BUILD_DIR="${BUILD_DIR}" bash "${ROOT_DIR}/scripts/start_demo_stack.sh" stop || true
}

trap cleanup EXIT

log "[INFO] starting demo stack"
RUN_DIR="${RUN_DIR}" BUILD_DIR="${BUILD_DIR}" bash "${ROOT_DIR}/scripts/start_demo_stack.sh" start

CMD=(
  "${BUILD_DIR}/system_demo_tool"
  --mds="${MDS_ADDR}"
  --scheduler="${SCHEDULER_ADDR}"
  --mount_point="${MOUNT_POINT}"
  --scenario=all
  --masstree_template_id="${TEMPLATE_ID}"
  --masstree_template_mode="${TEMPLATE_MODE}"
)

if [[ -n "${PATH_LIST_FILE}" ]]; then
  CMD+=(
    --masstree_path_list_file="${PATH_LIST_FILE}"
    --masstree_repeat_dir_prefix="${REPEAT_DIR_PREFIX}"
    --masstree_path_list_leaf_nodes_are_files="${LEAF_NODES_ARE_FILES}"
  )
fi

log "[INFO] running demo showcase"
"${CMD[@]}" | tee "${SHOWCASE_LOG}"

log "[OK] demo showcase finished, log=${SHOWCASE_LOG}"
