#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/mnt/md0/Projects/cgy/ZBStorage"
CONFIG="$PROJECT_ROOT/tools/time_simulator/config/example_full.yaml"
BIN="$PROJECT_ROOT/build-time-simulator/zbstorage_time_simulator"
OUT_ROOT="$PROJECT_ROOT/time_sim_output"
FULL_DIR="$OUT_ROOT/full"
RATIO_DIR="$OUT_ROOT/ratio_v3"

# 固定请求强度和间隔分布，只改变读写比例
RATE=1000
DIST="fixed"

# read_ratio write_ratio name
CASES=(
  "0.20 0.80 read20_write80"
  "0.50 0.50 read50_write50"
  "0.80 0.20 read80_write20"
)

cd "$PROJECT_ROOT"

if [[ ! -x "$BIN" ]]; then
  echo "[ERROR] 找不到可执行文件: $BIN"
  exit 1
fi

BACKUP="${CONFIG}.ratio_backup"
cp "$CONFIG" "$BACKUP"

restore_config() {
  if [[ -f "$BACKUP" ]]; then
    mv -f "$BACKUP" "$CONFIG"
    echo "[INFO] 已恢复原始 example_full.yaml"
  fi
}
trap restore_config EXIT

rm -rf "$RATIO_DIR"
mkdir -p "$RATIO_DIR"

SUMMARY="$RATIO_DIR/ratio_results.csv"
echo "case,read_ratio,write_ratio,generated_read,generated_write,operation_count,success_count,failed_count,written_bytes,read_bytes,average_latency_ms,average_queue_wait_ms,average_response_time_ms" > "$SUMMARY"

# 固定请求强度与分布
sed -i -E \
  "s/^([[:space:]]*request_rate_ops_per_sec:)[[:space:]]*.*/\1 $RATE/" \
  "$CONFIG"

sed -i -E \
  "s/^([[:space:]]*interval_distribution:)[[:space:]]*.*/\1 $DIST/" \
  "$CONFIG"

for item in "${CASES[@]}"; do
  read -r read_ratio write_ratio case_name <<< "$item"

  echo
  echo "============================================================"
  echo "[RUN] $case_name  read=$read_ratio write=$write_ratio"
  echo "============================================================"

  sed -i -E \
    "s/^([[:space:]]*read_ratio:)[[:space:]]*.*/\1 $read_ratio/" \
    "$CONFIG"

  sed -i -E \
    "s/^([[:space:]]*write_ratio:)[[:space:]]*.*/\1 $write_ratio/" \
    "$CONFIG"

  rm -rf "$FULL_DIR"
  case_dir="$RATIO_DIR/$case_name"
  mkdir -p "$case_dir"

  "$BIN" --config "$CONFIG" 2>&1 | tee "$case_dir/run.log"

  cp -a "$FULL_DIR/." "$case_dir/"

  trace="$FULL_DIR/generated_trace_step_0001.csv"
  summary="$FULL_DIR/step_summary.csv"

  generated_read=$(awk -F',' 'NR>1 && $4=="read"{n++} END{print n+0}' "$trace")
  generated_write=$(awk -F',' 'NR>1 && $4=="write"{n++} END{print n+0}' "$trace")

  # 读取 summary 字段（按表头名）
  values=$(python3 - "$summary" <<'PY'
import csv, sys
with open(sys.argv[1], newline="") as f:
    r = next(csv.DictReader(f))
keys = [
    "operation_count",
    "success_count",
    "failed_count",
    "written_bytes",
    "read_bytes",
    "average_latency_ms",
    "average_queue_wait_ms",
    "average_response_time_ms",
]
print(",".join(r[k] for k in keys))
PY
)

  echo "$case_name,$read_ratio,$write_ratio,$generated_read,$generated_write,$values" >> "$SUMMARY"

  echo "[OK] generated_read=$generated_read generated_write=$generated_write"
done

echo
echo "============================================================"
echo "[DONE] 3组读写比例测试全部完成"
echo "结果目录: $RATIO_DIR"
echo "汇总文件: $SUMMARY"
echo "============================================================"
