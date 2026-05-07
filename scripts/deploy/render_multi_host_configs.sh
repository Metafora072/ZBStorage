#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "${ROOT_DIR}/scripts/deploy/common_multi_host.sh"

ENV_FILE="${1:-${ROOT_DIR}/deploy/multi_host/cluster.env}"
OUTPUT_DIR="${2:-${ROOT_DIR}/deploy/multi_host/rendered}"
TEMPLATE_DIR="${ROOT_DIR}/deploy/multi_host/templates"

load_multi_host_env "${ENV_FILE}"

set -a

MDS_PORT="${MDS_PORT:-9000}"
SCHEDULER_HOST="${SCHEDULER_HOST:-${MDS_HOST:-${DATA_HOST:-127.0.0.1}}}"
SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"
OPTICAL_PORT="${OPTICAL_PORT:-39080}"

REAL_NODE_ID="${REAL_NODE_ID:-node-real-01}"
REAL_NODE_GROUP_ID="${REAL_NODE_GROUP_ID:-${REAL_NODE_ID}}"
REAL_NODE_WEIGHT="${REAL_NODE_WEIGHT:-1}"
REAL_DISK_COUNT="${REAL_DISK_COUNT:-24}"

VIRTUAL_NODE_ID="${VIRTUAL_NODE_ID:-vpool}"
VIRTUAL_NODE_GROUP_ID="${VIRTUAL_NODE_GROUP_ID:-${VIRTUAL_NODE_ID}}"
VIRTUAL_NODE_WEIGHT="${VIRTUAL_NODE_WEIGHT:-8}"
VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-99}"
VIRTUAL_DISK_COUNT="${VIRTUAL_DISK_COUNT:-24}"

ENABLE_OPTICAL_NODE="${ENABLE_OPTICAL_NODE:-false}"
OPTICAL_NODE_ID="${OPTICAL_NODE_ID:-optical-01}"
OPTICAL_NODE_GROUP_ID="${OPTICAL_NODE_GROUP_ID:-${OPTICAL_NODE_ID}}"
OPTICAL_NODE_WEIGHT="${OPTICAL_NODE_WEIGHT:-2}"
OPTICAL_DISKS="${OPTICAL_DISKS:-disc0,disc1}"

ONLINE_DISK_CAPACITY_BYTES="${ONLINE_DISK_CAPACITY_BYTES:-2000000000000}"
OBJECT_UNIT_SIZE="${OBJECT_UNIT_SIZE:-4194304}"
REPLICA="${REPLICA:-1}"
STRICT_TIER_BYPASS_PG="${STRICT_TIER_BYPASS_PG:-true}"

MDS_ROOT="${MDS_ROOT:-/data/zb/mds}"
SCHEDULER_ROOT="${SCHEDULER_ROOT:-/data/zb/scheduler}"
DATA_ROOT="${DATA_ROOT:-/data/zb/data_nodes}"
CLIENT_ROOT="${CLIENT_ROOT:-/data/zb/client}"
REAL_DIR_NAME="${REAL_DIR_NAME:-real}"
VIRTUAL_DIR_NAME="${VIRTUAL_DIR_NAME:-virtual}"

MASSTREE_PRELOAD_ALL_SPARSE_ON_START="${MASSTREE_PRELOAD_ALL_SPARSE_ON_START:-true}"
MASSTREE_PRELOAD_MEMORY_UTILIZATION_LIMIT="${MASSTREE_PRELOAD_MEMORY_UTILIZATION_LIMIT:-0.70}"
MASSTREE_PRELOAD_MEMORY_RESERVE_BYTES="${MASSTREE_PRELOAD_MEMORY_RESERVE_BYTES:-137438953472}"
MASSTREE_PRELOAD_ESTIMATE_MULTIPLIER="${MASSTREE_PRELOAD_ESTIMATE_MULTIPLIER:-2.5}"
MASSTREE_PRELOAD_BACKGROUND="${MASSTREE_PRELOAD_BACKGROUND:-true}"

REAL_DISKS="$(build_real_disk_list "${REAL_DISK_COUNT}")"
VIRTUAL_DISKS="$(build_virtual_disk_list "${VIRTUAL_DISK_COUNT}")"

set +a

mkdir -p "${OUTPUT_DIR}/mds" "${OUTPUT_DIR}/data" "${OUTPUT_DIR}/client"

render_template() {
  local input="$1"
  local output="$2"
  python - "$input" "$output" <<'PY'
from pathlib import Path
import os
import sys

src = Path(sys.argv[1]).read_text(encoding="utf-8")
for key, value in os.environ.items():
    src = src.replace(f"@{key}@", value)
Path(sys.argv[2]).write_text(src, encoding="utf-8")
PY
}

render_template "${TEMPLATE_DIR}/scheduler.conf.tpl" "${OUTPUT_DIR}/mds/scheduler.conf"
render_template "${TEMPLATE_DIR}/real_node.conf.tpl" "${OUTPUT_DIR}/data/real_node.conf"
render_template "${TEMPLATE_DIR}/virtual_node.conf.tpl" "${OUTPUT_DIR}/data/virtual_node.conf"
render_template "${TEMPLATE_DIR}/mds.conf.tpl" "${OUTPUT_DIR}/mds/mds.conf"
render_template "${TEMPLATE_DIR}/client.env.tpl" "${OUTPUT_DIR}/client/client.env"
if [[ "${ENABLE_OPTICAL_NODE}" == "true" ]]; then
  render_template "${TEMPLATE_DIR}/optical_node.conf.tpl" "${OUTPUT_DIR}/data/optical_node.conf"
fi

cat > "${OUTPUT_DIR}/mds/mds.env" <<EOF
MDS_HOST=${MDS_HOST}
MDS_PORT=${MDS_PORT}
MDS_ROOT=${MDS_ROOT}
SCHEDULER_HOST=${SCHEDULER_HOST}
SCHEDULER_PORT=${SCHEDULER_PORT}
SCHEDULER_ROOT=${SCHEDULER_ROOT}
EOF

cat > "${OUTPUT_DIR}/data/data.env" <<EOF
DATA_HOST=${DATA_HOST}
SCHEDULER_HOST=${SCHEDULER_HOST}
SCHEDULER_PORT=${SCHEDULER_PORT}
REAL_PORT=${REAL_PORT}
VIRTUAL_PORT=${VIRTUAL_PORT}
OPTICAL_PORT=${OPTICAL_PORT}
ENABLE_OPTICAL_NODE=${ENABLE_OPTICAL_NODE}
EOF

multi_host_log "[OK] rendered multi-host configs to ${OUTPUT_DIR}"
