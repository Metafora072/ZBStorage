#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
RUN_DIR="${RUN_DIR:-${DEMO_ROOT}}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
LOG_DIR="${RUN_DIR}/logs"

NAMESPACE_COUNT="${1:-1000}"
TEMPLATE_ID="${MASSTREE_TEMPLATE_ID:-}"
TEMPLATE_MODE="${MASSTREE_TEMPLATE_MODE:-page_fast}"
NAMESPACE_PREFIX="${NAMESPACE_PREFIX:-demo-ns}"
PATH_LIST_FILE="${MASSTREE_PATH_LIST_FILE:-}"
REPEAT_DIR_PREFIX="${MASSTREE_REPEAT_DIR_PREFIX:-copy}"
LEAF_NODES_ARE_FILES="${MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES:-false}"

mkdir -p "${LOG_DIR}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

if ! [[ "${NAMESPACE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${NAMESPACE_COUNT}" -le 0 ]]; then
  echo "namespace_count must be a positive integer" >&2
  exit 1
fi

if [[ ! -x "${BUILD_DIR}/system_demo_tool" ]]; then
  echo "Missing executable: ${BUILD_DIR}/system_demo_tool" >&2
  exit 1
fi

IMPORT_LOG="${LOG_DIR}/import_masstree_${NAMESPACE_COUNT}ns_$(date '+%Y%m%d_%H%M%S').log"
STATUS=0

cleanup() {
  local stop_status=0
  log "[INFO] stopping demo stack"
  if ! RUN_DIR="${RUN_DIR}" BUILD_DIR="${BUILD_DIR}" bash "${ROOT_DIR}/scripts/start_demo_stack.sh" stop; then
    stop_status=$?
    log "[WARN] failed to stop demo stack cleanly (status=${stop_status})"
  fi
  if [[ "${STATUS}" -ne 0 ]]; then
    log "[ERROR] one-click import failed, see log: ${IMPORT_LOG}"
  else
    log "[OK] one-click import completed, see log: ${IMPORT_LOG}"
  fi
}

trap cleanup EXIT

log "[INFO] run_dir=${RUN_DIR}"
log "[INFO] build_dir=${BUILD_DIR}"
log "[INFO] namespace_count=${NAMESPACE_COUNT}"
if [[ -n "${TEMPLATE_ID}" ]]; then
  log "[INFO] template_id=${TEMPLATE_ID}"
else
  log "[INFO] template_id=random"
fi
log "[INFO] template_mode=${TEMPLATE_MODE}"
if [[ -n "${PATH_LIST_FILE}" ]]; then
  log "[INFO] path_list_file=${PATH_LIST_FILE}"
fi
log "[INFO] import_log=${IMPORT_LOG}"

log "[INFO] starting demo stack"
RUN_DIR="${RUN_DIR}" BUILD_DIR="${BUILD_DIR}" bash "${ROOT_DIR}/scripts/start_demo_stack.sh" start

if [[ -n "${PATH_LIST_FILE}" ]]; then
  if [[ -z "${TEMPLATE_ID}" ]]; then
    echo "MASSTREE_TEMPLATE_ID is required when MASSTREE_PATH_LIST_FILE is set" >&2
    STATUS=1
    exit "${STATUS}"
  fi
  log "[INFO] generating template from txt path list"
  if RUN_DIR="${RUN_DIR}" \
     BUILD_DIR="${BUILD_DIR}" \
     MOUNT_POINT="${RUN_DIR}/mnt" \
     MASSTREE_TEMPLATE_ID="${TEMPLATE_ID}" \
     MASSTREE_PATH_LIST_FILE="${PATH_LIST_FILE}" \
     MASSTREE_REPEAT_DIR_PREFIX="${REPEAT_DIR_PREFIX}" \
     MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES="${LEAF_NODES_ARE_FILES}" \
     bash "${ROOT_DIR}/scripts/generate_masstree_template.sh" \
     "${TEMPLATE_ID}" "${PATH_LIST_FILE}" "${REPEAT_DIR_PREFIX}" "${LEAF_NODES_ARE_FILES}" | tee "${IMPORT_LOG}"; then
    :
  else
    STATUS=${PIPESTATUS[0]:-1}
    exit "${STATUS}"
  fi
fi

log "[INFO] importing ${NAMESPACE_COUNT} namespaces (~${NAMESPACE_COUNT} * 1e8 files)"
if ! RUN_DIR="${RUN_DIR}" \
     BUILD_DIR="${BUILD_DIR}" \
     MOUNT_POINT="${RUN_DIR}/mnt" \
     NAMESPACE_PREFIX="${NAMESPACE_PREFIX}" \
     MASSTREE_TEMPLATE_ID="${TEMPLATE_ID}" \
     MASSTREE_TEMPLATE_MODE="${TEMPLATE_MODE}" \
     bash "${ROOT_DIR}/scripts/import_masstree_demo.sh" "${NAMESPACE_COUNT}" | tee -a "${IMPORT_LOG}"; then
  STATUS=$?
  exit "${STATUS}"
fi

STATUS=0
