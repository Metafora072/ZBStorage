#!/usr/bin/env bash
set -euo pipefail

resolve_demo_root() {
  local root_dir="$1"
  local config_file="${CONFIG_BASE_FILE:-${root_dir}/config/base.conf}"
  local configured_root=""
  local raw_line=""

  if [[ -f "${config_file}" ]]; then
    while IFS= read -r raw_line || [[ -n "${raw_line}" ]]; do
      local line="${raw_line%%#*}"
      line="$(printf '%s' "${line}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
      [[ -z "${line}" ]] && continue
      if [[ "${line}" == ROOT_PATH=* ]]; then
        configured_root="${line#ROOT_PATH=}"
        configured_root="$(printf '%s' "${configured_root}" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
        break
      fi
    done < "${config_file}"
  fi

  if [[ -n "${configured_root}" ]]; then
    if [[ "${configured_root}" != /* ]]; then
      configured_root="${root_dir}/${configured_root}"
    fi
    printf '%s\n' "${configured_root}"
  else
    printf '%s\n' "${root_dir}/.demo_run"
  fi
}

# Shared by both FUSE launchers. Do not use RUN_DIR/DEMO_ROOT/CLIENT_ROOT here.
prepare_io_latency_args() {
  local root_dir="$1"
  IO_LATENCY_ARGS=("--io_latency_enabled=${IO_LATENCY_ENABLED:-false}")
  case "${IO_LATENCY_ENABLED:-false}" in
    false|0) return 0 ;;
    true|1) ;;
    *) echo "Invalid IO_LATENCY_ENABLED: ${IO_LATENCY_ENABLED}" >&2; return 1 ;;
  esac
  local directory="${IO_LATENCY_DIR:-}"
  if [[ -z "${directory}" ]]; then
    local config_file="${CONFIG_BASE_FILE:-${root_dir}/config/base.conf}"
    if [[ -e "${config_file}" && ( ! -f "${config_file}" || ! -r "${config_file}" ) ]]; then
      echo "Cannot read base config: ${config_file}" >&2
      return 1
    fi
    local configured_root
    configured_root="$(resolve_demo_root "${root_dir}")" || return 1
    directory="${configured_root%/}/client/metrics"
    echo "[io_latency] directory=${directory} source=${config_file} (missing/empty ROOT_PATH uses .demo_run)" >&2
  else
    echo "[io_latency] directory=${directory} source=explicit" >&2
  fi
  IO_LATENCY_ARGS+=(
    "--io_latency_dir=${directory}"
    "--io_latency_flush_ms=${IO_LATENCY_FLUSH_MS:-1000}"
    "--io_latency_batch_records=${IO_LATENCY_BATCH_RECORDS:-1024}"
    "--io_latency_queue_capacity=${IO_LATENCY_QUEUE_CAPACITY:-65536}"
  )
}

stop_fuse_client_pid() {
  local client_pid="$1"
  local timeout_sec="${IO_LATENCY_STOP_TIMEOUT_SEC:-30}"
  if [[ ! "${timeout_sec}" =~ ^[1-9][0-9]*$ ]]; then
    echo "IO_LATENCY_STOP_TIMEOUT_SEC must be a positive integer" >&2
    return 1
  fi
  [[ "${client_pid}" =~ ^[1-9][0-9]*$ ]] || return 1
  kill -0 "${client_pid}" 2>/dev/null || return 0
  kill -TERM "${client_pid}" 2>/dev/null || return 0
  local tick state
  for ((tick=0; tick<timeout_sec*10; ++tick)); do
    kill -0 "${client_pid}" 2>/dev/null || return 0
    state="$(ps -o stat= -p "${client_pid}" 2>/dev/null || true)"
    [[ -z "${state}" || "${state}" == Z* ]] && return 0
    sleep 0.1
  done
  echo "[io_latency] FUSE shutdown timed out; forcing exit, CSV may be incomplete (pid=${client_pid})" >&2
  kill -KILL "${client_pid}" 2>/dev/null || true
}
