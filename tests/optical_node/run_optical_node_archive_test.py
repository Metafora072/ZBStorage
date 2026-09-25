#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""光存储节点归档集成测试的 ctest 驱动脚本（仅使用 Python 标准库）。

职责：
    1. 准备语料（调用同目录的 prepare_archive_corpus.py）；
    2. 运行 C++ 测试二进制（由 --driver 指定），把 stdout+stderr 合并落盘为 output.log；
    3. 回显日志末尾的「测试点汇总」区块，并按需清理工作目录。

场景（--scenario）：
    smoke  1 卷 12 文件，走完 归档→压缩→封装→打包汇报→刻录释放→读；墙钟约 1 分钟。
    full   6 卷 72 文件，在 smoke 基础上覆盖 image_dir 容量 5 的 LRU 淘汰与 CD_READ 重载；
           墙钟 6~10 分钟（光盘仿真时长不可压缩，产线未提供测试旋钮）。

语料来源：--input-dir，其次环境变量 ZBSTORAGE_OPTICAL_CORPUS，缺省 ~/WR/testWrite。
工作目录：默认 tests/optical_node/results/<scenario>-<时间戳>/（见 tests/README.md 结果保存约定），
        其中 corpus/ 为脚本产出的语料、archive/ 为光节点工作目录、
        output.log 为本次运行的完整日志（按测试点记录目标 / 期望 / 验证方法 / 逐项检查结论）。

产物保留约定（实测占用：smoke 约 192MB、full 约 1.2GB）：
    默认（成功）          删除 corpus/ 与 archive/，保留 output.log 作为可追溯的测试记录；
    失败                  全部保留以便排查；
    --keep                无论成败全部保留。

退出码约定：语料缺失 -> 77（ctest SKIP_RETURN_CODE 视为跳过）；其余非零原样透传。
"""

import argparse
import datetime
import os
import shutil
import subprocess
import sys

# 语料/跳过约定
SKIP_RETURN_CODE = 77
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
LOG_NAME = "output.log"


def resolve_corpus_src(input_dir):
    """语料来源目录：优先命令行，其次环境变量，最后 ~/WR/testWrite。"""
    if input_dir:
        return os.path.expanduser(input_dir)
    return os.environ.get("ZBSTORAGE_OPTICAL_CORPUS") or os.path.expanduser(
        "~/WR/testWrite"
    )


def default_work_dir(scenario):
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    return os.path.join(REPO_ROOT, "tests", "optical_node", "results",
                        "%s-%s" % (scenario, ts))


def run_tee(cmd, log, cwd=None):
    """运行子进程：stdout/stderr 合并后同时转发到终端与日志文件，返回退出码。"""
    print("[run] %s" % " ".join(cmd), flush=True)
    log.write("[run] %s\n" % " ".join(cmd))
    log.flush()
    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    for line in proc.stdout:
        sys.stdout.write(line)
        log.write(line)
    proc.stdout.close()
    rc = proc.wait()
    log.flush()
    return rc


def print_case_summary(log_path):
    """回显日志里的「测试点汇总」区块（到该区块结束为止），便于 ctest 直接看到结论。"""
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as fp:
            lines = fp.read().splitlines()
    except OSError:
        return
    start = None
    for index, line in enumerate(lines):
        if line.strip() == "测试点汇总":
            start = max(index - 1, 0)
    if start is None:
        return
    # 汇总区块以一条全 '=' 分隔线收尾，之后的 brpc 退出日志与最终结论另行打印。
    end = len(lines)
    for index in range(start + 2, len(lines)):
        stripped = lines[index].strip()
        if stripped and set(stripped) == {"="}:
            end = index + 1
            break
    print("-" * 60)
    for line in lines[start:end]:
        print(line)


def main():
    parser = argparse.ArgumentParser(description="光存储节点归档集成测试驱动")
    parser.add_argument("--driver", required=True, help="C++ 测试二进制路径")
    parser.add_argument("--scenario", required=True, choices=["smoke", "full"],
                        help="测试场景")
    parser.add_argument("--work-dir", default=None, help="工作目录")
    parser.add_argument("--input-dir", default=None, help="语料来源目录")
    parser.add_argument("--keep", action="store_true",
                        help="保留全部产物（默认成功后只保留 output.log）")
    args = parser.parse_args()

    corpus_src = resolve_corpus_src(args.input_dir)
    if not os.path.isdir(corpus_src):
        print("[skip] 语料目录不存在：%s（可通过 ZBSTORAGE_OPTICAL_CORPUS "
              "或 --input-dir 指定）" % corpus_src, file=sys.stderr)
        sys.exit(SKIP_RETURN_CODE)

    work_dir = args.work_dir or default_work_dir(args.scenario)
    work_dir = os.path.abspath(work_dir)
    os.makedirs(work_dir, exist_ok=True)
    print("[info] 场景=%s 语料来源=%s 工作目录=%s"
          % (args.scenario, corpus_src, work_dir))

    corpus_out = os.path.join(work_dir, "corpus")
    volumes = 1 if args.scenario == "smoke" else 6
    log_path = os.path.join(work_dir, LOG_NAME)

    with open(log_path, "w", encoding="utf-8") as log:
        log.write("=" * 96 + "\n")
        log.write("optical_node 归档集成测试运行日志\n")
        log.write("开始时间: %s\n" % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        log.write("场景: %s（%d 卷）\n" % (args.scenario, volumes))
        log.write("语料来源: %s\n" % corpus_src)
        log.write("工作目录: %s\n" % work_dir)
        log.write("=" * 96 + "\n")
        log.flush()

        # 步骤 1：生成语料
        prepare = os.path.join(SCRIPT_DIR, "prepare_archive_corpus.py")
        prep_cmd = [
            sys.executable, prepare,
            "--input-dir", corpus_src,
            "--out-dir", corpus_out,
            "--volumes", str(volumes),
        ]
        prep_rc = run_tee(prep_cmd, log)
        if prep_rc != 0:
            print("[error] 语料生成失败，退出码=%d" % prep_rc, file=sys.stderr)
            print("[info] 工作目录保留用于排查：%s" % work_dir, file=sys.stderr)
            sys.exit(prep_rc)

        # 步骤 2：运行 C++ 测试二进制（日志同时落盘）
        driver_cmd = [
            args.driver,
            "--corpus", corpus_out,
            "--work-dir", work_dir,
            "--scenario", args.scenario,
        ]
        rc = run_tee(driver_cmd, log, cwd=work_dir)

    # 步骤 3：回显测试点汇总
    print_case_summary(log_path)

    # 步骤 4：结果摘要与产物清理
    print("=" * 60)
    print("[summary] scenario=%s exit_code=%d" % (args.scenario, rc))
    print("[summary] 完整日志：%s" % log_path)
    if rc == 0 and not args.keep:
        for heavy in ("corpus", "archive"):
            shutil.rmtree(os.path.join(work_dir, heavy), ignore_errors=True)
        print("[summary] 已清理大体积产物 corpus/ 与 archive/，保留日志与工作目录：%s"
              % work_dir)
    elif rc == 0:
        print("[summary] 已保留全部产物（--keep）：%s" % work_dir)
    else:
        print("[summary] 测试失败，全部产物保留在 %s（corpus/ 与节点产物）" % work_dir)

    sys.exit(rc)


if __name__ == "__main__":
    main()