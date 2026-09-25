#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""为光存储节点归档集成测试生成语料（仅使用 Python 标准库）。

本脚本模拟"真实节点(real node)"磁盘上待归档的文件布局，并基于 zlib 逐字节
复刻被测节点的打包行为，产出"期望打包计划"供测试断言。

被测节点侧的打包逻辑（src/data_node/optical_node/optical_node_manager/
volume_manager/volume_manager.cpp::AddFileToCollect）：
    1. compress2(..., Z_BEST_COMPRESSION) 压缩文件 —— 与 python zlib.compress(data, 9) 逐字节一致；
    2. 单文件压缩后 > 卷大小 -> VOLUME_FULL；
    3. pending + 压缩后大小 > 卷大小 -> 先打包当前分组（溢出预打包分支）；
    4. 累加 pending，若 pending / volume_size >= size_threshold -> 打包并清零。
"""

import argparse
import os
import random
import sys
import zlib

# 被测节点构造参数：100 MiB 卷 + 0.9 阈值
VOLUME_SIZE = 100 * 1024 * 1024
SIZE_THRESHOLD = 0.9

# 跳过测试的约定退出码（ctest SKIP_RETURN_CODE）
SKIP_RETURN_CODE = 77


def is_jpeg(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".jpg") or lower.endswith(".jpeg")


def collect_jpegs(input_dir: str):
    """收集并按文件名排序所有 jpg/jpeg。"""
    if not os.path.isdir(input_dir):
        return []
    names = [n for n in os.listdir(input_dir) if is_jpeg(n)]
    names.sort()
    blobs = []
    for n in names:
        path = os.path.join(input_dir, n)
        if os.path.isfile(path):
            with open(path, "rb") as fp:
                blobs.append(fp.read())
    return blobs


def build_file_content(jpg_blobs, target_size: int, file_seed: int) -> bytes:
    """按确定性但依赖数据顺序的方式拼接 jpg 得到目标大小的文件内容。

    关键点：同一个归档文件内**不复用同一张 jpg**（用 seed+f 洗牌后依次各取一次，
    不足时用 seed+f+1000*k 重新洗牌继续）。因为 jpg 本身已压缩，deflate 的匹配
    窗口只有 32 KiB，只要单个文件内不出现相隔 32 KiB 以内的重复字节块，压缩率就
    稳定在 ~1.0；若在同一位置重复使用同一张 jpg，重复块会落入窗口内被 deflate
    匹配，压缩率骤降，文件就永远填不满卷，整个打包计划随之漂移。
    """
    out = bytearray()
    k = 0
    while len(out) < target_size:
        # 第一次用 seed+f，后续每次重新洗牌用 seed+f+1000*k（k>=1）
        rnd = random.Random(file_seed + 1000 * k)
        order = list(range(len(jpg_blobs)))
        rnd.shuffle(order)
        for idx in order:
            if len(out) >= target_size:
                break
            out.extend(jpg_blobs[idx])
        k += 1
    return bytes(out[:target_size])


def predict_plan_and_write(args, jpg_blobs, write_corpus: bool):
    """逐个文件复刻 AddFileToCollect 的判断顺序，产出计划与统计。

    返回 (plan, raw_total, comp_total, single_ratio)：
      plan        每个元素是一卷的 inode 列表（按打包顺序）
      raw_total   原始总字节
      comp_total  压缩后总字节
      single_ratio 首个文件的实测压缩率
    """
    file_size = args.file_size_mb * 1024 * 1024
    unit = args.object_unit_mb * 1024 * 1024
    total_files = args.volumes * args.files_per_volume
    object_dir = os.path.join(args.out_dir, "real-1", "disk0") if write_corpus else None
    if write_corpus:
        os.makedirs(object_dir, exist_ok=True)
        manifest_lines = []

    pending = 0
    group = []
    plan = []
    raw_total = 0
    comp_total = 0
    single_ratio = None

    for f in range(total_files):
        inode_id = args.inode_base + f
        content = build_file_content(jpg_blobs, file_size, args.seed + f)

        # 与本节点侧完全一致：zlib 级别 9 == compress2(Z_BEST_COMPRESSION)
        compressed = zlib.compress(content, 9)
        c = len(compressed)
        raw_total += len(content)
        comp_total += c
        if single_ratio is None:
            single_ratio = c / float(len(content))

        if c > VOLUME_SIZE:
            print(
                "[error] inode %d 压缩后 %d 字节超过卷大小 %d 字节"
                % (inode_id, c, VOLUME_SIZE),
                file=sys.stderr,
            )
            sys.exit(1)

        if write_corpus:
            # 切分为 object_unit_mb 大小的分片，末片为余数
            object_count = (len(content) + unit - 1) // unit
            for i in range(object_count):
                obj_path = os.path.join(
                    object_dir, "obj-%d-%d.dat" % (inode_id, i)
                )
                with open(obj_path, "wb") as fp:
                    fp.write(content[i * unit:min(len(content), (i + 1) * unit)])
            volume_index = f // args.files_per_volume
            manifest_lines.append(
                "\t".join(
                    [
                        str(inode_id),
                        str(len(content)),
                        str(unit),
                        str(object_count),
                        "real-1",
                        "disk0",
                        str(volume_index),
                        os.path.abspath(object_dir),
                    ]
                )
            )

        # ---- 以下顺序严格复刻 AddFileToCollect ----
        if pending + c > VOLUME_SIZE:
            # 溢出预打包分支：先把当前分组打包
            plan.append(group)
            group = []
            pending = 0
        group.append(inode_id)
        pending += c
        if pending / float(VOLUME_SIZE) >= SIZE_THRESHOLD:
            plan.append(group)
            group = []
            pending = 0

    # 正常情况下末尾分组应恰好清空；若仍有残留则保留，让后续一致性检查报错
    if group:
        plan.append(group)

    if write_corpus:
        with open(os.path.join(args.out_dir, "manifest.tsv"), "w",
                  encoding="utf-8") as fp:
            fp.write("\n".join(manifest_lines) + "\n")
        with open(os.path.join(args.out_dir, "expected_pack_plan.tsv"), "w",
                  encoding="utf-8") as fp:
            lines = []
            for vi, g in enumerate(plan):
                lines.append(
                    "%d\t%d\t%s" % (vi, len(g), ",".join(str(x) for x in g))
                )
            fp.write("\n".join(lines) + "\n")

    return plan, raw_total, comp_total, single_ratio


def check_plan(plan, args, ratio):
    """校验计划是否为 volumes 卷、每卷 files_per_volume 个文件。"""
    ok = len(plan) == args.volumes and all(
        len(g) == args.files_per_volume for g in plan
    )
    if not ok:
        print(
            "[error] 预测打包计划与参数不一致（参数已漂移）：期望 %d 卷、每卷 %d 个文件，"
            "实际得到 %d 卷，各卷大小=%s，实测压缩率=%.6f"
            % (
                args.volumes,
                args.files_per_volume,
                len(plan),
                [len(g) for g in plan],
                ratio,
            ),
            file=sys.stderr,
        )
        sys.exit(1)


def main():
    default_input = os.environ.get("ZBSTORAGE_OPTICAL_CORPUS") or os.path.expanduser(
        "~/WR/testWrite"
    )

    parser = argparse.ArgumentParser(
        description="生成光存储节点归档集成测试语料（模拟 real node 磁盘）"
    )
    parser.add_argument("--input-dir", default=default_input,
                        help="jpg/jpeg 语料来源目录")
    parser.add_argument("--out-dir", default=None,
                        help="输出目录（--selftest 时可省略）")
    parser.add_argument("--volumes", type=int, default=6, help="卷数量")
    parser.add_argument("--files-per-volume", type=int, default=12,
                        help="每卷文件数")
    parser.add_argument("--file-size-mb", type=int, default=8,
                        help="每个归档文件大小(MiB)")
    parser.add_argument("--object-unit-mb", type=int, default=1,
                        help="对象分片大小(MiB)")
    parser.add_argument("--inode-base", type=int, default=1000,
                        help="起始 inode 号")
    parser.add_argument("--seed", type=int, default=20260925,
                        help="确定性洗牌种子")
    parser.add_argument("--selftest", action="store_true",
                        help="只打印压缩率与预测分组，不落盘")
    args = parser.parse_args()

    if not args.selftest and not args.out_dir:
        parser.error("--out-dir 为必填（除 --selftest 外）")

    jpg_blobs = collect_jpegs(args.input_dir)
    if not jpg_blobs:
        print(
            "[skip] 语料目录不可用或没有 jpg/jpeg：%s（可通过环境变量 "
            "ZBSTORAGE_OPTICAL_CORPUS 或 --input-dir 指定）" % args.input_dir,
            file=sys.stderr,
        )
        sys.exit(SKIP_RETURN_CODE)

    trigger = int(VOLUME_SIZE * SIZE_THRESHOLD)
    print("[info] 语料目录=%s，jpg 数量=%d" % (args.input_dir, len(jpg_blobs)))
    print(
        "[info] volume_size=%d 阈值=%.2f 触发线=%d 字节；单文件=%d 字节"
        % (VOLUME_SIZE, SIZE_THRESHOLD, trigger, args.file_size_mb * 1024 * 1024)
    )

    plan, raw_total, comp_total, single_ratio = predict_plan_and_write(
        args, jpg_blobs, write_corpus=not args.selftest
    )
    ratio = comp_total / float(raw_total)
    check_plan(plan, args, ratio)

    print("[info] 单文件实测压缩率=%.6f" % single_ratio)
    print(
        "[info] 合计原始=%d 字节 压缩后=%d 字节 实测压缩率=%.6f"
        % (raw_total, comp_total, ratio)
    )
    print("[info] 预测打包计划：共 %d 卷，每卷文件数=%s"
          % (len(plan), [len(g) for g in plan]))
    for vi, g in enumerate(plan):
        print("       volume %d: count=%d inodes=%s"
              % (vi, len(g), ",".join(str(x) for x in g)))

    if args.selftest:
        print("[selftest] 未写入任何文件，退出。")
        return

    with open(os.path.join(args.out_dir, "corpus_meta.txt"), "w",
              encoding="utf-8") as fp:
        fp.write("input_dir\t%s\n" % os.path.abspath(args.input_dir))
        fp.write("jpg_count\t%d\n" % len(jpg_blobs))
        fp.write("raw_total_bytes\t%d\n" % raw_total)
        fp.write("compressed_total_bytes\t%d\n" % comp_total)
        fp.write("measured_ratio\t%.6f\n" % ratio)
        fp.write("volumes\t%d\n" % args.volumes)
        fp.write("files_per_volume\t%d\n" % args.files_per_volume)
        fp.write("file_size_bytes\t%d\n" % (args.file_size_mb * 1024 * 1024))
        fp.write("object_unit_bytes\t%d\n" % (args.object_unit_mb * 1024 * 1024))
        fp.write("volume_size_bytes\t%d\n" % VOLUME_SIZE)
        fp.write("size_threshold\t%.2f\n" % SIZE_THRESHOLD)
        fp.write("trigger_bytes\t%d\n" % trigger)
        fp.write("expected_volume_file_counts\t%s\n"
                 % ",".join(str(len(g)) for g in plan))
    print("[info] 语料已写入：%s" % os.path.abspath(args.out_dir))


if __name__ == "__main__":
    main()