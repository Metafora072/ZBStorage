#!/usr/bin/env python3

import csv
import os
import sys
from collections import defaultdict


def read_rows(path):
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def group_by_thread(rows, metric_key):
    by_thread = defaultdict(dict)
    for row in rows:
        thread = int(row["threads"])
        by_thread[thread][row["operation"]] = float(row[metric_key])
    return by_thread


def draw_grouped_bar(rows, metric_key, title, ylabel, output_path):
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise SystemExit(
            "需要 matplotlib 才能画图，请先安装后再运行 plot_benchmark.py"
        ) from exc

    operations = ["stat", "create", "delete", "ls"]
    grouped = group_by_thread(rows, metric_key)
    threads = sorted(grouped.keys())
    if not threads:
        raise SystemExit("CSV 中没有可绘制的数据")

    x = list(range(len(operations)))
    width = 0.8 / max(len(threads), 1)

    fig, ax = plt.subplots(figsize=(10, 5))
    for idx, thread in enumerate(threads):
        offsets = [value - 0.4 + width / 2 + idx * width for value in x]
        values = [grouped[thread].get(op, 0.0) for op in operations]
        ax.bar(offsets, values, width=width, label=f"{thread} threads")

    ax.set_xticks(x)
    ax.set_xticklabels(operations)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path, dpi=150)
    plt.close(fig)


def main(argv):
    if len(argv) < 2 or len(argv) > 3:
        raise SystemExit(
            "用法: python3 plot_benchmark.py <summary.csv> [output_prefix]"
        )

    summary_csv = argv[1]
    output_prefix = argv[2] if len(argv) == 3 else os.path.splitext(summary_csv)[0]
    rows = read_rows(summary_csv)

    draw_grouped_bar(
        rows,
        "throughput_ops_per_sec",
        "LHM Namespace Benchmark Throughput",
        "ops/s",
        output_prefix + "_throughput.png",
    )
    draw_grouped_bar(
        rows,
        "avg_latency_us",
        "LHM Namespace Benchmark Average Latency",
        "us",
        output_prefix + "_avg_latency.png",
    )


if __name__ == "__main__":
    main(sys.argv)
