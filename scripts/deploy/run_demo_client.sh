#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ROLE_DIR="${ROLE_DIR:-${ROOT_DIR}/deploy/multi_host/rendered/client}"
ENV_PATH="${ENV_PATH:-${ROLE_DIR}/client.env}"

set -a
# shellcheck disable=SC1090
source "${ENV_PATH}"
set +a

exec bash "${ROOT_DIR}/scripts/run_system_demo.sh" "$@"
