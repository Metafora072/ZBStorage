#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

multi_host_log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

load_multi_host_env() {
  local env_file="$1"
  if [[ ! -f "${env_file}" ]]; then
    echo "Missing env file: ${env_file}" >&2
    exit 1
  fi
  set -a
  # shellcheck disable=SC1090
  source "${env_file}"
  set +a
}

require_multi_host_bin() {
  local path="$1"
  if [[ ! -x "${path}" ]]; then
    echo "Missing executable: ${path}" >&2
    exit 1
  fi
}

wait_multi_host_port() {
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
  printf '%s\n' "${result}"
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
  printf '%s\n' "${result}"
}

write_pid_file() {
  local pid_dir="$1"
  local name="$2"
  local pid="$3"
  mkdir -p "${pid_dir}"
  echo "${pid}" > "${pid_dir}/${name}.pid"
}

read_pid_file() {
  local pid_dir="$1"
  local name="$2"
  local file="${pid_dir}/${name}.pid"
  if [[ -f "${file}" ]]; then
    cat "${file}"
  fi
}

stop_by_pid_file() {
  local pid_dir="$1"
  local name="$2"
  local pid
  pid="$(read_pid_file "${pid_dir}" "${name}" || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" >/dev/null 2>&1; then
    kill "${pid}" >/dev/null 2>&1 || true
    sleep 1
    if kill -0 "${pid}" >/dev/null 2>&1; then
      kill -9 "${pid}" >/dev/null 2>&1 || true
    fi
    multi_host_log "[STOP] ${name} pid=${pid}"
  fi
  rm -f "${pid_dir}/${name}.pid"
}

start_with_log() {
  local name="$1"
  local host="$2"
  local port="$3"
  local log_dir="$4"
  local pid_dir="$5"
  shift 5
  mkdir -p "${log_dir}" "${pid_dir}"
  "$@" > "${log_dir}/${name}.log" 2>&1 &
  write_pid_file "${pid_dir}" "${name}" "$!"
  if ! wait_multi_host_port "${host}" "${port}" 30; then
    echo "${name} did not become ready on ${host}:${port}" >&2
    exit 1
  fi
  multi_host_log "[OK] ${name} started on ${host}:${port}"
}
