#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
RUN_DIR="${RUN_DIR:-${DEMO_ROOT}}"
MDS_ADDR="${MDS_ADDR:-127.0.0.1:9000}"
SCHEDULER_ADDR="${SCHEDULER_ADDR:-127.0.0.1:9100}"
MOUNT_POINT="${MOUNT_POINT:-${RUN_DIR}/mnt}"
MASSTREE_ROOT="${MASSTREE_ROOT:-${RUN_DIR}/data/mds/masstree_meta}"
LOG_DIR="${RUN_DIR}/logs"

FILE_COUNT="${1:-}"
NAMESPACE_ID="${2:-random-ns-$(date '+%Y%m%d_%H%M%S')}"
GENERATION_ID="${3:-gen-$(date '+%Y%m%d_%H%M%S')}"
TEMPLATE_ID="${TEMPLATE_ID:-template-${NAMESPACE_ID}-${FILE_COUNT}}"
PATH_PREFIX="${PATH_PREFIX:-/masstree_demo/${NAMESPACE_ID}}"
TEMPLATE_MODE="${TEMPLATE_MODE:-page_fast}"
MAX_FILES_PER_LEAF_DIR="${MAX_FILES_PER_LEAF_DIR:-2048}"
MAX_SUBDIRS_PER_DIR="${MAX_SUBDIRS_PER_DIR:-256}"
VERIFY_INODE_SAMPLES="${MASSTREE_VERIFY_INODE_SAMPLES:-32}"
VERIFY_DENTRY_SAMPLES="${MASSTREE_VERIFY_DENTRY_SAMPLES:-32}"
OVERWRITE_TEMPLATE="${OVERWRITE_TEMPLATE:-false}"

usage() {
  cat >&2 <<EOF
Usage:
  $0 <file_count> [namespace_id] [generation_id]

Environment:
  BUILD_DIR                         Default: ${ROOT_DIR}/build
  RUN_DIR                           Default: value from config/base.conf or .demo_run
  MASSTREE_ROOT                     Default: <RUN_DIR>/data/mds/masstree_meta
  MDS_ADDR                          Default: 127.0.0.1:9000
  SCHEDULER_ADDR                    Default: 127.0.0.1:9100
  MOUNT_POINT                       Default: <RUN_DIR>/mnt
  TEMPLATE_ID                       Default: template-<namespace_id>-<file_count>
  PATH_PREFIX                       Default: /masstree_demo/<namespace_id>
  TEMPLATE_MODE                     Default: page_fast
  MAX_FILES_PER_LEAF_DIR            Default: 2048
  MAX_SUBDIRS_PER_DIR               Default: 256
  MASSTREE_VERIFY_INODE_SAMPLES     Default: 32
  MASSTREE_VERIFY_DENTRY_SAMPLES    Default: 32
  OVERWRITE_TEMPLATE=true|false     Default: false
EOF
}

if [[ -z "${FILE_COUNT}" ]]; then
  usage
  exit 1
fi

if ! [[ "${FILE_COUNT}" =~ ^[0-9]+$ ]] || [[ "${FILE_COUNT}" -le 0 ]]; then
  echo "file_count must be a positive integer: ${FILE_COUNT}" >&2
  exit 1
fi

if ! [[ "${MAX_FILES_PER_LEAF_DIR}" =~ ^[0-9]+$ ]] || [[ "${MAX_FILES_PER_LEAF_DIR}" -le 0 ]]; then
  echo "MAX_FILES_PER_LEAF_DIR must be a positive integer" >&2
  exit 1
fi

if ! [[ "${MAX_SUBDIRS_PER_DIR}" =~ ^[0-9]+$ ]] || [[ "${MAX_SUBDIRS_PER_DIR}" -le 0 ]]; then
  echo "MAX_SUBDIRS_PER_DIR must be a positive integer" >&2
  exit 1
fi

GENERATOR="${BUILD_DIR}/masstree_meta_generate_tool"
DEMO_TOOL="${BUILD_DIR}/system_demo_tool"
if [[ ! -x "${GENERATOR}" ]]; then
  echo "Missing executable: ${GENERATOR}" >&2
  echo "Build it first: cmake --build ${BUILD_DIR} --target masstree_meta_generate_tool -j\$(nproc)" >&2
  exit 1
fi
if [[ ! -x "${DEMO_TOOL}" ]]; then
  echo "Missing executable: ${DEMO_TOOL}" >&2
  echo "Build it first: cmake --build ${BUILD_DIR} --target system_demo_tool -j\$(nproc)" >&2
  exit 1
fi

TEMPLATE_DIR="${MASSTREE_ROOT}/templates/${TEMPLATE_ID}"
MANIFEST_PATH="${TEMPLATE_DIR}/template.staging/manifest.txt"
mkdir -p "${LOG_DIR}" "${MASSTREE_ROOT}/templates"

if [[ -e "${TEMPLATE_DIR}" ]]; then
  if [[ "${OVERWRITE_TEMPLATE}" == "true" || "${OVERWRITE_TEMPLATE}" == "1" ]]; then
    rm -rf -- "${TEMPLATE_DIR}"
  else
    echo "template already exists: ${TEMPLATE_DIR}" >&2
    echo "Use a different TEMPLATE_ID or set OVERWRITE_TEMPLATE=true" >&2
    exit 1
  fi
fi

LOG_FILE="${LOG_DIR}/random_masstree_import_${NAMESPACE_ID}_$(date '+%Y%m%d_%H%M%S').log"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log "file_count=${FILE_COUNT}"
log "namespace_id=${NAMESPACE_ID}"
log "generation_id=${GENERATION_ID}"
log "template_id=${TEMPLATE_ID}"
log "path_prefix=${PATH_PREFIX}"
log "masstree_root=${MASSTREE_ROOT}"
log "manifest_path=${MANIFEST_PATH}"
log "max_files_per_leaf_dir=${MAX_FILES_PER_LEAF_DIR}"
log "max_subdirs_per_dir=${MAX_SUBDIRS_PER_DIR}"

log "generating synthetic multi-level Masstree metadata template"
"${GENERATOR}" \
  --output_root="${MASSTREE_ROOT}/templates" \
  --namespace_id="${TEMPLATE_ID}" \
  --generation_id=template \
  --path_prefix="/__masstree_template__/${TEMPLATE_ID}" \
  --inode_start=1 \
  --source_mode=synthetic \
  --file_count="${FILE_COUNT}" \
  --max_files_per_leaf_dir="${MAX_FILES_PER_LEAF_DIR}" \
  --max_subdirs_per_dir="${MAX_SUBDIRS_PER_DIR}" \
  2>&1 | tee -a "${LOG_FILE}"

if [[ ! -f "${MANIFEST_PATH}" ]]; then
  echo "generated manifest not found: ${MANIFEST_PATH}" >&2
  exit 1
fi

log "importing generated template through system_demo_tool real import"
"${DEMO_TOOL}" \
  --mds="${MDS_ADDR}" \
  --scheduler="${SCHEDULER_ADDR}" \
  --mount_point="${MOUNT_POINT}" \
  --scenario=masstree_import \
  --masstree_import_mode=real \
  --masstree_namespace_id="${NAMESPACE_ID}" \
  --masstree_generation_id="${GENERATION_ID}" \
  --masstree_path_prefix="${PATH_PREFIX}" \
  --masstree_template_id="${TEMPLATE_ID}" \
  --masstree_template_mode="${TEMPLATE_MODE}" \
  --masstree_verify_inode_samples="${VERIFY_INODE_SAMPLES}" \
  --masstree_verify_dentry_samples="${VERIFY_DENTRY_SAMPLES}" \
  2>&1 | tee -a "${LOG_FILE}"

log "done"
log "log_file=${LOG_FILE}"
