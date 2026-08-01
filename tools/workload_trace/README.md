# ZBStorage workload trace 工具

`generate_trace.py` 生成可复现、带微秒时间戳的文件系统操作序列。正式工具保留导师版本的操作码和主要参数别名，同时修正目录层级、无效操作、参数校验和到达分布问题。

## 输出格式

```text
timestamp_us op path [offset size]
```

操作码：

- `f_c/f_w/f_r/f_d`：创建、写、读、删除文件。
- `d_c/d_l/d_d`：创建、列出、删除目录。

以 `#` 开头的是版本和生成参数。路径使用真正的两层结构，例如 `/D1/D1_1/f23`。

## 快速使用

```bash
python3 tools/workload_trace/generate_trace.py \
  --num-file-ops 5000 \
  --num-dir-ops 1000 \
  --top-dirs 4 \
  --subdirs-per-top 2 \
  --files-per-dir 10 \
  --rate 800 \
  --dist exponential \
  --seed 123 \
  --output /path/to/run-root/traces/workload.txt
```

也兼容导师示例中的 `--Nfop/--Ndop/--M/--i/--k` 和 `--*-prob` 参数名。

## 状态语义

- `--initial-state=empty`：默认。工具先输出不计入请求数量的目录 bootstrap，再生成状态合法的文件和目录操作。
- `--initial-state=populated`：假定全部目录和文件在 trace 开始前已经存在，适合已有测试数据集。
- 生成过程中维护目录和文件状态，不会读取不存在的文件、越界读取或删除非空目录。
- 操作权重是目标分布；如果某个操作在当前状态下不可执行，会在其他当前合法操作中按权重重新选择。

## 负载特征

- `--dist=constant` 使用固定间隔。
- `--dist=exponential` 使用指数到达间隔，对应泊松到达过程。
- `--dist=poisson` 仅作为兼容别名，会提示使用 exponential。
- `--hotset-fraction` 和 `--hotset-probability` 控制热点访问。
- `--io-size-dist` 支持 `uniform`、`fixed` 和 `lognormal`。

生成的大 trace、日志和实验输出应放在运行根目录，不提交仓库。仓库中只保留工具、说明和必要的小型测试样例。

## 自测

```bash
python3 -m unittest tools/workload_trace/tests/test_generate_trace.py
```

## 回放到 Scheduler

`scheduler_trace_replay` 将路径级 trace 转成统一的节点访问事件。每个文件系统操作会产生一个元数据事件；`f_r/f_w` 还会按稳定路径哈希映射到数据节点。它只驱动 Scheduler 的性能、功率和能耗模型，不会修改真实文件。

```bash
./build/scheduler_trace_replay \
  --scheduler=127.0.0.1:9100 \
  --trace=/path/to/workload.txt \
  --data_nodes=sim-data-1,sim-data-2 \
  --metadata_node=sim-mds \
  --auto_register=true \
  --advance_after_ms=600000
```

回放结束会把 Scheduler 的模拟时间推进到最后一条事件；`--advance_after_ms` 可继续增加空闲尾段，用于观察节点进入 STANDBY/OFF。Scheduler 的墙钟 tick 会跳过 simulated node，避免快速回放与真实时间互相污染。

先验证输入和映射而不连接 Scheduler：

```bash
./build/scheduler_trace_replay --trace=/path/to/workload.txt \
  --data_nodes=sim-data-1,sim-data-2 --metadata_node=sim-mds --dry_run
```

导师提供的原始脚本、示例和 Word 说明保存在 `docs/references/workload-trace/2026-07-30/`，不作为正式运行入口。
