#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/common_demo_env.sh"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/build}"
DEMO_ROOT="${DEMO_ROOT:-$(resolve_demo_root "${ROOT_DIR}")}"
RUN_DIR="${RUN_DIR:-${DEMO_ROOT}}"
CFG_DIR="${RUN_DIR}/config"
LOG_DIR="${RUN_DIR}/logs"
PID_DIR="${RUN_DIR}/pids"
DATA_DIR="${RUN_DIR}/data"
MOUNT_POINT="${MOUNT_POINT:-${RUN_DIR}/mnt}"

SCHEDULER_PORT="${SCHEDULER_PORT:-9100}"
MDS_PORT="${MDS_PORT:-9000}"
REAL_PORT="${REAL_PORT:-19080}"
VIRTUAL_PORT="${VIRTUAL_PORT:-29080}"

REAL_NODE_ID="${REAL_NODE_ID:-node-real-01}"
VIRTUAL_NODE_ID="${VIRTUAL_NODE_ID:-vpool}"
REAL_DIR_NAME="${REAL_DIR_NAME:-real}"
VIRTUAL_DIR_NAME="${VIRTUAL_DIR_NAME:-virtual}"
REAL_DISK_COUNT="${REAL_DISK_COUNT:-24}"
VIRTUAL_NODE_COUNT="${VIRTUAL_NODE_COUNT:-99}"
VIRTUAL_DISK_COUNT="${VIRTUAL_DISK_COUNT:-24}"
ONLINE_DISK_CAPACITY_BYTES="${ONLINE_DISK_CAPACITY_BYTES:-2000000000000}"
MDS_STRICT_TIER_BYPASS_PG="${MDS_STRICT_TIER_BYPASS_PG:-true}"

MODE="${1:-start}"

mkdir -p "${CFG_DIR}" "${LOG_DIR}" "${PID_DIR}" "${DATA_DIR}" "${MOUNT_POINT}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

build_real_disk_list() {
  local count="$1"
  local width=2
  if (( count >= 100 )); then
    width=3
  fi
  local result=""
  local i
  for ((i=1; i<=count; ++i)); do
    local disk
    disk="$(printf "disk-%0${width}d" "${i}")"
    if [[ -n "${result}" ]]; then
      result+=","
    fi
    result+="${disk}"
  done
  echo "${result}"
}

build_virtual_disk_list() {
  local count="$1"
  local result=""
  local i
  for ((i=0; i<count; ++i)); do
    if [[ -n "${result}" ]]; then
      result+=","
    fi
    result+="disk${i}"
  done
  echo "${result}"
}

require_bin() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    log "[ERROR] missing executable: ${path}"
    exit 1
  fi
}

wait_port() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-20}"
  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if command -v nc >/dev/null 2>&1; then
      if nc -z "${host}" "${port}" >/dev/null 2>&1; then
        return 0
      fi
    elif (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1; then
      exec 3<&-
      exec 3>&-
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= timeout_sec )); then
      return 1
    fi
    sleep 1
  done
}

port_in_use() {
  local host="$1"
  local port="$2"
  if command -v nc >/dev/null 2>&1; then
    nc -z "${host}" "${port}" >/dev/null 2>&1
    return $?
  fi
  (exec 3<>"/dev/tcp/${host}/${port}") >/dev/null 2>&1
}

describe_port_owner() {
  local port="$1"
  if command -v ss >/dev/null 2>&1; then
    ss -lntp "sport = :${port}" 2>/dev/null || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN 2>/dev/null || true
  elif command -v netstat >/dev/null 2>&1; then
    netstat -lntp 2>/dev/null | grep ":${port} " || true
  fi
}

ensure_port_free() {
  local name="$1"
  local port="$2"
  if port_in_use "127.0.0.1" "${port}"; then
    log "[ERROR] ${name} port ${port} is already in use. Stop the old demo stack first."
    describe_port_owner "${port}"
    log "[HINT] run: bash scripts/start_demo_stack.sh stop"
    log "[HINT] if pid files are stale/missing, check: ss -lntp | egrep '9000|9100|19080|29080'"
    exit 1
  fi
}

ensure_all_ports_free() {
  ensure_port_free "scheduler" "${SCHEDULER_PORT}"
  ensure_port_free "mds" "${MDS_PORT}"
  ensure_port_free "real_node" "${REAL_PORT}"
  ensure_port_free "virtual_node" "${VIRTUAL_PORT}"
}

write_pid() {
  local name="$1"
  local pid="$2"
  echo "${pid}" > "${PID_DIR}/${name}.pid"
}

read_pid() {
  local name="$1"
  local file="${PID_DIR}/${name}.pid"
  if [[ -f "${file}" ]]; then
    cat "${file}"
  fi
}

is_pid_alive() {
  local pid="$1"
  [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1
}

stop_by_name() {
  local name="$1"
  local pid
  pid="$(read_pid "${name}" || true)"
  if [[ -n "${pid}" ]] && is_pid_alive "${pid}"; then
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 1
    if is_pid_alive "${pid}"; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
    log "[STOP] ${name} pid=${pid}"
  fi
  rm -f "${PID_DIR}/${name}.pid"
}

render_configs() {
  mkdir -p "${DATA_DIR}/real/disks" "${DATA_DIR}/virtual/meta" "${DATA_DIR}/mds"
  local real_disks
  local virtual_disks
  real_disks="$(build_real_disk_list "${REAL_DISK_COUNT}")"
  virtual_disks="$(build_virtual_disk_list "${VIRTUAL_DISK_COUNT}")"

  cat > "${CFG_DIR}/scheduler.conf" <<EOF
SUSPECT_TIMEOUT_MS=6000
DEAD_TIMEOUT_MS=15000
TICK_INTERVAL_MS=1000
CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS=5000
CLUSTER_VIEW_SNAPSHOT_PATH=${LOG_DIR}/scheduler_cluster_view.txt
EOF

  cat > "${CFG_DIR}/real_node.conf" <<EOF
DISK_BASE_DIR=${DATA_DIR}/real/disks
DISK_COUNT=${REAL_DISK_COUNT}
DISK_CAPACITY_BYTES=${ONLINE_DISK_CAPACITY_BYTES}
REPLICATION_ENABLED=false
NODE_ID=${REAL_NODE_ID}
NODE_ADDRESS=127.0.0.1:${REAL_PORT}
GROUP_ID=${REAL_NODE_ID}
NODE_ROLE=PRIMARY
NODE_WEIGHT=1
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=2000
EOF

  cat > "${CFG_DIR}/virtual_node.conf" <<EOF
NODE_ID=${VIRTUAL_NODE_ID}
NODE_ADDRESS=127.0.0.1:${VIRTUAL_PORT}
GROUP_ID=${VIRTUAL_NODE_ID}
NODE_ROLE=PRIMARY
REPLICATION_ENABLED=false
NODE_WEIGHT=8
VIRTUAL_NODE_COUNT=${VIRTUAL_NODE_COUNT}
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
MDS_ADDR=127.0.0.1:${MDS_PORT}
HEARTBEAT_INTERVAL_MS=2000
ALLOW_DYNAMIC_DISKS=true
DISKS=${virtual_disks}
READ_MBPS=800
WRITE_MBPS=600
READ_BASE_LATENCY_MS=2
WRITE_BASE_LATENCY_MS=3
JITTER_MS=1
DISK_CAPACITY_BYTES=${ONLINE_DISK_CAPACITY_BYTES}
MOUNT_POINT_PREFIX=${DATA_DIR}/virtual/mount
ARCHIVE_META_DIR=${DATA_DIR}/virtual/meta
ARCHIVE_META_SNAPSHOT_INTERVAL_OPS=20000
ARCHIVE_META_WAL_FSYNC=false
EOF

  cat > "${CFG_DIR}/mds.conf" <<EOF
MDS_DB_PATH=${DATA_DIR}/mds/db
SCHEDULER_ADDR=127.0.0.1:${SCHEDULER_PORT}
SCHEDULER_REFRESH_MS=2000
STRICT_TIER_BYPASS_PG=${MDS_STRICT_TIER_BYPASS_PG}
OBJECT_UNIT_SIZE=4194304
REPLICA=1
ENABLE_OPTICAL_ARCHIVE=false
ARCHIVE_TRIGGER_BYTES=10737418240
ARCHIVE_TARGET_BYTES=8589934592
COLD_FILE_TTL_SEC=3600
ARCHIVE_SCAN_INTERVAL_MS=5000
ARCHIVE_MAX_OBJECTS_PER_ROUND=64
ARCHIVE_META_ROOT=${DATA_DIR}/mds/archive_meta
MASSTREE_ROOT=${DATA_DIR}/mds/masstree_meta
NODES=${REAL_NODE_ID}@127.0.0.1:${REAL_PORT},type=REAL,weight=1;${VIRTUAL_NODE_ID}@127.0.0.1:${VIRTUAL_PORT},type=VIRTUAL,weight=8,virtual_node_count=${VIRTUAL_NODE_COUNT}
DISKS=${REAL_NODE_ID}:${real_disks};${VIRTUAL_NODE_ID}:${virtual_disks}
EOF
}

start_component() {
  local name="$1"
  local port="$2"
  shift 2
  local log_file="${LOG_DIR}/${name}.log"
  ensure_port_free "${name}" "${port}"
  "$@" >"${log_file}" 2>&1 &
  local pid="$!"
  write_pid "${name}" "${pid}"

  local start_ts
  start_ts="$(date +%s)"
  while true; do
    if ! is_pid_alive "${pid}"; then
      log "[ERROR] ${name} exited before becoming ready. See log: ${log_file}"
      tail -n 20 "${log_file}" || true
      exit 1
    fi
    if port_in_use "127.0.0.1" "${port}"; then
      log "[OK] ${name} started on :${port}"
      return 0
    fi
    if (( "$(date +%s)" - start_ts >= 20 )); then
      break
    fi
    sleep 1
  done

  if ! wait_port "127.0.0.1" "${port}" 1; then
    log "[ERROR] ${name} did not become ready on port ${port}"
    exit 1
  fi
  log "[OK] ${name} started on :${port}"
}

start_all() {
  require_bin "${BUILD_DIR}/scheduler_server"
  require_bin "${BUILD_DIR}/real_node_server"
  require_bin "${BUILD_DIR}/virtual_node_server"
  require_bin "${BUILD_DIR}/mds_server"
  require_bin "${BUILD_DIR}/zb_fuse_client"

  ensure_all_ports_free
  render_configs

  start_component scheduler "${SCHEDULER_PORT}" \
    "${BUILD_DIR}/scheduler_server" --config="${CFG_DIR}/scheduler.conf" --port="${SCHEDULER_PORT}"

  start_component real_node "${REAL_PORT}" \
    "${BUILD_DIR}/real_node_server" --config="${CFG_DIR}/real_node.conf" --port="${REAL_PORT}"

  start_component virtual_node "${VIRTUAL_PORT}" \
    "${BUILD_DIR}/virtual_node_server" --config="${CFG_DIR}/virtual_node.conf" --port="${VIRTUAL_PORT}"

  start_component mds "${MDS_PORT}" \
    "${BUILD_DIR}/mds_server" --config="${CFG_DIR}/mds.conf" --port="${MDS_PORT}"

  local fuse_log="${LOG_DIR}/fuse.log"
  "${BUILD_DIR}/zb_fuse_client" \
    --mds="127.0.0.1:${MDS_PORT}" \
    --scheduler="127.0.0.1:${SCHEDULER_PORT}" \
    --timeout_ms=30000 \
    --bootstrap_tier_dirs=true \
    --real_dir_name="${REAL_DIR_NAME}" \
    --virtual_dir_name="${VIRTUAL_DIR_NAME}" \
    -- "${MOUNT_POINT}" -f >"${fuse_log}" 2>&1 &
  write_pid "fuse" "$!"
  sleep 3
  log "[OK] fuse client started mount=${MOUNT_POINT}"
  log "[INFO] logs=${LOG_DIR}"
  log "[INFO] online topology: real_nodes=1 virtual_nodes=${VIRTUAL_NODE_COUNT} real_disks_per_node=${REAL_DISK_COUNT} virtual_disks_per_pool=${VIRTUAL_DISK_COUNT} disk_capacity_bytes=${ONLINE_DISK_CAPACITY_BYTES}"
}

stop_all() {
  if command -v fusermount3 >/dev/null 2>&1; then
    fusermount3 -u "${MOUNT_POINT}" >/dev/null 2>&1 || true
  elif command -v fusermount >/dev/null 2>&1; then
    fusermount -u "${MOUNT_POINT}" >/dev/null 2>&1 || true
  else
    umount "${MOUNT_POINT}" >/dev/null 2>&1 || true
  fi
  stop_by_name "fuse"
  stop_by_name "mds"
  stop_by_name "virtual_node"
  stop_by_name "real_node"
  stop_by_name "scheduler"
}

status_all() {
  local names=("scheduler" "real_node" "virtual_node" "mds" "fuse")
  local name pid
  for name in "${names[@]}"; do
    pid="$(read_pid "${name}" || true)"
    if [[ -n "${pid}" ]] && is_pid_alive "${pid}"; then
      echo "${name}: RUNNING pid=${pid}"
    else
      echo "${name}: STOPPED"
    fi
  done
}

case "${MODE}" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    start_all
    ;;
  status)
    status_all
    ;;
  *)
    echo "Usage: $0 [start|stop|restart|status]"
    exit 1
    ;;
esac
