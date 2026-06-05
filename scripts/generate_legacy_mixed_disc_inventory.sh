#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"

BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
RUN_DIR="${RUN_DIR:-${DEMO_ROOT}}"
INVENTORY_DIR="${INVENTORY_DIR:-${RUN_DIR}/data/optical_discs/inventory}"
GENERATOR="${GENERATOR:-${BUILD_DIR}/generate_legacy_mixed_discs}"

SWITCH_INVENTORY=true
BACKUP_DELTA=true
FORCE=false

usage() {
  cat <<EOF
Usage: $0 [--no-switch] [--keep-delta] [--force]

Generate the legacy_mixed_v1 optical disc inventory matching current demo stats:
  10000 nodes
  per node: 9000 x 1TB discs + 1000 x 10TB discs
  total: 100,000,000 discs, 190 EB

Environment overrides:
  BUILD_DIR       Default: ${ROOT_DIR}/build
  RUN_DIR         Default: ROOT_PATH from config/base.conf
  INVENTORY_DIR   Default: <RUN_DIR>/data/optical_discs/inventory
  GENERATOR       Default: <BUILD_DIR>/generate_legacy_mixed_discs

Options:
  --no-switch     Generate <INVENTORY_DIR>.new only; do not replace current inventory.
  --keep-delta    Do not move catalog_delta.tsv aside during switch.
  --force         Remove an existing <INVENTORY_DIR>.new before generating.
  -h, --help      Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-switch)
      SWITCH_INVENTORY=false
      ;;
    --keep-delta)
      BACKUP_DELTA=false
      ;;
    --force)
      FORCE=true
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

if [[ ! -x "${GENERATOR}" ]]; then
  log "[INFO] missing generator, building target generate_legacy_mixed_discs"
  cmake --build "${BUILD_DIR}" --target generate_legacy_mixed_discs -j"$(nproc)"
fi
if [[ ! -x "${GENERATOR}" ]]; then
  echo "[ERROR] missing executable: ${GENERATOR}" >&2
  exit 1
fi

NEW_DIR="${INVENTORY_DIR}.new"
PARENT_DIR="$(dirname "${INVENTORY_DIR}")"
DELTA_PATH="${PARENT_DIR}/catalog_delta.tsv"

if [[ -e "${NEW_DIR}" ]]; then
  if [[ "${FORCE}" == true ]]; then
    log "[INFO] removing existing staging inventory: ${NEW_DIR}"
    rm -rf "${NEW_DIR}"
  else
    echo "[ERROR] staging inventory already exists: ${NEW_DIR}" >&2
    echo "        Re-run with --force after confirming it is safe to remove." >&2
    exit 1
  fi
fi

mkdir -p "${PARENT_DIR}"

log "[INFO] run_dir=${RUN_DIR}"
log "[INFO] inventory=${INVENTORY_DIR}"
log "[INFO] staging=${NEW_DIR}"
log "[INFO] generator=${GENERATOR}"
log "[INFO] generating legacy_mixed_v1 inventory; this writes about 7.5 GiB"

"${GENERATOR}" "${NEW_DIR}"

batch_count="$(find "${NEW_DIR}" -maxdepth 1 -type f -name 'disc_batch_*.bin' | wc -l | tr -d ' ')"
if [[ "${batch_count}" != "1000" ]]; then
  echo "[ERROR] expected 1000 disc_batch files, got ${batch_count}" >&2
  exit 1
fi

first_batch="${NEW_DIR}/disc_batch_0.bin"
last_batch="${NEW_DIR}/disc_batch_999.bin"
if [[ ! -s "${first_batch}" || ! -s "${last_batch}" ]]; then
  echo "[ERROR] generated inventory is incomplete: missing/non-empty check failed" >&2
  exit 1
fi

log "[OK] generated ${batch_count} batch files under ${NEW_DIR}"

if [[ "${SWITCH_INVENTORY}" != true ]]; then
  log "[OK] --no-switch requested; leaving staging inventory in place"
  exit 0
fi

stamp="$(date +%Y%m%d_%H%M%S)"
if [[ -e "${INVENTORY_DIR}" ]]; then
  backup="${INVENTORY_DIR}.bak.${stamp}"
  log "[INFO] backing up existing inventory to ${backup}"
  mv "${INVENTORY_DIR}" "${backup}"
fi

if [[ "${BACKUP_DELTA}" == true && -e "${DELTA_PATH}" ]]; then
  delta_backup="${DELTA_PATH}.bak.${stamp}"
  log "[INFO] backing up existing catalog delta to ${delta_backup}"
  mv "${DELTA_PATH}" "${delta_backup}"
fi

log "[INFO] activating inventory"
mv "${NEW_DIR}" "${INVENTORY_DIR}"

log "[OK] legacy_mixed_v1 inventory is active: ${INVENTORY_DIR}"
log "[INFO] verify with:"
log "       printf '6 op=inventory_stats\\nq\\n' | ${BUILD_DIR}/system_demo_tool --mount_point='${RUN_DIR}/mnt' --scenario=interactive"
