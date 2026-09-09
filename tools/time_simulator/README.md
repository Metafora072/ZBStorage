# ZBStorage 时间驱动负载仿真器

该目录实现独立的时间驱动负载生成、串行 Trace 播放和结果统计工具。

当前 full/FUSE 阶段已支持：

- YAML 配置读取与合法性检查；
- 精确控制 Trace 总请求数；
- 精确控制 read/write 比例；
- 配置请求强度（ops/s）；
- 配置请求间隔分布：fixed / poisson / negative_exponential；
- 生成逻辑请求时间戳；
- 严格串行播放 Trace；
- 通过 ZBStorage FUSE 真实执行 read/write；
- 输出 generated_trace、operations、step_summary 三类 CSV。

## 编译

在 ZBStorage 项目根目录执行：

```bash
cmake -S tools/time_simulator -B build-time-simulator
cmake --build build-time-simulator -j4
ctest --test-dir build-time-simulator --output-on-failure
```

## 串行 Trace 配置（V2）

`config/example_full.yaml` 中：

```yaml
trace:
  enabled: true
  total_operations: 100
  read_ratio: 0.70
  write_ratio: 0.30
  file_count: 10
  io_size_bytes: 262144

  # 请求强度：平均每秒逻辑请求数量
  request_rate_ops_per_sec: 1000

  # fixed / poisson / negative_exponential
  interval_distribution: fixed
```

### request_rate_ops_per_sec

表示请求强度，即每秒平均生成多少条逻辑请求。

例如：

```text
100 ops/s   -> 平均间隔约 10000 us
1000 ops/s  -> 平均间隔约 1000 us
5000 ops/s  -> 平均间隔约 200 us
```

平均间隔计算：

```text
mean_interval_us = 1,000,000 / request_rate_ops_per_sec
```

### interval_distribution

#### fixed

固定间隔。每条请求之间使用相同的逻辑间隔。

例如 `request_rate_ops_per_sec: 1000`：

```text
0 us
1000 us
2000 us
3000 us
...
```

#### poisson

按照测试案例文档“请求间隔分布包含泊松分布”的字面要求，V2 将请求间隔建模为离散微秒随机变量：

```text
interval_us ~ Poisson(mean_interval_us)
```

因此请求时间仍单调递增，但相邻请求间隔会在平均间隔附近波动。

#### negative_exponential

负指数间隔：

```text
interval_us ~ Exponential(lambda)
lambda = request_rate_ops_per_sec / 1,000,000
```

其平均间隔同样为：

```text
1,000,000 / request_rate_ops_per_sec
```

这种模式的间隔波动明显，会同时出现很短和较长的请求间隔。

## 输出文件

运行：

```bash
./build-time-simulator/zbstorage_time_simulator \
  --config tools/time_simulator/config/example_full.yaml
```

结果目录：

```text
time_sim_output/full/
├── generated_trace_step_0001.csv
├── operations_step_0001.csv
└── step_summary.csv
```

### generated_trace_step_0001.csv

字段：

```text
request_id
timestamp_us
inter_arrival_us
operation_type
logical_path
offset
size_bytes
```

其中：

- `timestamp_us`：本条请求的逻辑到达时间；
- `inter_arrival_us`：与上一条请求的逻辑时间间隔；
- 第一条请求的 `inter_arrival_us=0`。

### operations_step_0001.csv

记录真实 FUSE 执行结果，并包含：

```text
scheduled_time_us
start_time_us
finish_time_us
latency_ms
status
```

Trace 仍然严格串行执行，因此应满足：

```text
next.start_time_us >= previous.finish_time_us
```

注意：逻辑 Trace 可以以高于系统实际处理速度的强度到达；由于当前要求串行播放，请求不会并发执行，而会出现逻辑到达时间早于实际开始时间的排队现象。

## V1 配置兼容

旧配置：

```yaml
request_interval_us: 1000
```

仍然可以读取。程序会自动换算为：

```text
request_rate_ops_per_sec = 1,000,000 / request_interval_us
```

新配置建议统一使用 `request_rate_ops_per_sec`。

## 自动测试

当前 smoke test 覆盖：

- 配置解析；
- 非法读写比例拒绝；
- 精确 70/30 读写请求生成；
- fixed 间隔时间戳；
- poisson 间隔平均值；
- negative_exponential 间隔平均值及波动；
- 固定随机种子可复现；
- V1 `request_interval_us` 配置兼容；
- 非法间隔分布拒绝。

## Trace V3: queue wait and response time

The serial trace player already honors each request's logical `timestamp_us`:
when the executor is early it sleeps until the scheduled time; when the previous
request runs past the next arrival, the next request starts immediately after the
previous one completes.

V3 records two additional fields in `operations_step_XXXX.csv`:

- `queue_wait_us = max(0, start_time_us - scheduled_time_us)`
- `response_time_us = finish_time_us - scheduled_time_us`

`step_summary.csv` also adds average/P95/P99 queue-wait and response-time
metrics. These metrics make request-rate experiments meaningful: low request
rates should have near-zero queue wait, while rates above the serial service
capacity should accumulate queueing delay.
