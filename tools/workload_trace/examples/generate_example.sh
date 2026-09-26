#!/usr/bin/env bash
set -euo pipefail

trace_example_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
trace_output=${1:-/tmp/zbstorage-workload.txt}

python3 "${trace_example_dir}/../generate_trace.py" \
  --num-file-ops 5000 \
  --num-dir-ops 1000 \
  --top-dirs 4 \
  --subdirs-per-top 2 \
  --files-per-dir 10 \
  --rate 800 \
  --dist exponential \
  --hotset-fraction 0.1 \
  --hotset-probability 0.8 \
  --seed 123 \
  --output "${trace_output}"

echo "trace written to ${trace_output}"
