#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/mnt/md0/Projects/cgy/ZBStorage"
CONFIG="$PROJECT_ROOT/tools/time_simulator/config/example_full.yaml"
BIN="$PROJECT_ROOT/build-time-simulator/zbstorage_time_simulator"
OUT_ROOT="$PROJECT_ROOT/time_sim_output"
FULL_DIR="$OUT_ROOT/full"
MATRIX_DIR="$OUT_ROOT/matrix_v3"

RATES=(100 1000 5000)
DISTS=(fixed poisson negative_exponential)

cd "$PROJECT_ROOT"

if [[ ! -x "$BIN" ]]; then
  echo "[ERROR] 找不到可执行文件: $BIN"
  exit 1
fi

BACKUP="${CONFIG}.matrix_v3_backup"
cp "$CONFIG" "$BACKUP"
restore_config() {
  if [[ -f "$BACKUP" ]]; then
    mv -f "$BACKUP" "$CONFIG"
    echo "[INFO] 已恢复原始 example_full.yaml"
  fi
}
trap restore_config EXIT

rm -rf "$MATRIX_DIR"
mkdir -p "$MATRIX_DIR"

SUMMARY="$MATRIX_DIR/matrix_results_v3.csv"
echo "distribution,request_rate_ops_per_sec,operation_count,success_count,failed_count,written_bytes,read_bytes,average_latency_ms,p95_latency_ms,p99_latency_ms,average_queue_wait_ms,p95_queue_wait_ms,p99_queue_wait_ms,average_response_time_ms,p95_response_time_ms,p99_response_time_ms" > "$SUMMARY"

for dist in "${DISTS[@]}"; do
  for rate in "${RATES[@]}"; do
    case_name="${dist}_${rate}"
    case_dir="$MATRIX_DIR/$case_name"

    echo
    echo "===== RUN $case_name ====="

    sed -i -E "s/^([[:space:]]*request_rate_ops_per_sec:)[[:space:]]*.*/\\1 $rate/" "$CONFIG"
    sed -i -E "s/^([[:space:]]*interval_distribution:)[[:space:]]*.*/\\1 $dist/" "$CONFIG"

    rm -rf "$FULL_DIR"
    mkdir -p "$case_dir"

    "$BIN" --config "$CONFIG" 2>&1 | tee "$case_dir/run.log"
    cp -a "$FULL_DIR/." "$case_dir/"

    last_line="$(tail -n 1 "$FULL_DIR/step_summary.csv")"
    rest="${last_line#*,}"
    echo "$dist,$rate,$rest" >> "$SUMMARY"

    echo "[OK] $case_name -> $case_dir"
  done
done

echo
echo "[DONE] 9组V3测试全部完成"
echo "结果目录: $MATRIX_DIR"
echo "汇总文件: $SUMMARY"
echo
echo "建议查看:"
echo "column -s, -t < $SUMMARY"
