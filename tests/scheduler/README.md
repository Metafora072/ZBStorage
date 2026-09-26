# T03：Scheduler、磁电节点和能耗测试

依据根目录 `测试任务书.pdf` 第 4–5 页（T03）及第 10–13 页的原测试案例。负责人：丁森凡。

本目录交付可直接运行的批量文件处理、节点分布、磁电节点能耗数据处理、10 节点仿真能效及外存真实闭环测试。内部开发回归用例仅保留本地，不作为交付依赖。T02 的通用 FUSE Trace 播放、T04 的长期仿真、T05 的光盘归档和机械调度不由本目录重新实现。

## 目录组织

```text
tests/scheduler/
├── README.md
├── run.py                 # 常规测试、仿真、电表分析入口
├── external_test.py       # 外存真实 FUSE 负载入口
├── python/                # 两个入口必需的支撑模块（包）
├── fixtures/              # 随代码提交的固定测试输入
└── results/               # 本地运行结果、索引和历史报告，不提交
```

以上为提交目录。开发机上的 `cpp/`、`python/test_*.py`、`python/closure.py` 均不提交，也不参与默认构建；使用者无需这些文件即可运行本文命令。原内部 `run.py closure` 命令不再公开，迁移、零引用与退役后的分配验证统一使用 `external_test.py` 的真实 FUSE 链路。

Python 内部模块通过包相对导入复用，不直接运行内部文件。`fixtures/time_simulator_trace.csv` 是可直接传给 `--trace` 的最小 T04 格式示例，不是实验结果。

`python/support.py` 固定以本测试目录定位结果和样例，不依赖调用者当前目录。`environment.json` 中的 `test_source_sha256` 以本目录为基准记录公开入口及支撑模块的相对路径，不包含未交付的内部回归文件，便于复核实际测试版本。

已知跨模块缺陷统一记录在 [docs/issues](../../docs/issues/fuse-concurrent-write-20260915.md)。原有 `REPORT-*.md` 已移到本地 `results/reports/`；历史结果保持原样，不因本次目录整理重写。

## 任务与入口

| 任务 | 实现 | 验收输出 |
| --- | --- | --- |
| T03-1 顺序读、顺序写、随机读写、典型 Trace | `run.py local/batch`、`python/workloads.py` | 请求记录、真实读写校验、文件清单、逐节点文件/字节分布、耗时、模型功率和能耗 |
| T03-2 实际磁电节点能耗 | `batch` 写入与复用同一文件集读取；`run.py meter`；`meter-closure` 模拟电表闭环 | 清单、电表 CSV、窗口能耗、平均功率、PB/kW、达标判断，明确实测/模拟来源 |
| T03-3 10 节点仿真 | `run.py simulation` | CMS 实际选择目标、10 节点逐时状态、1.09 kW、10.091743 PB/kW、积分校验 |
| 外存真实闭环 | `external_test.py` | 挂载 FUSE 的真实读写、重启恢复、排空迁移、CMS 零引用、退役后不再分配、心跳故障恢复 |

## 构建及快速验证

依赖项目已有的 CMake、C++ 工具链、bundled RocksDB/brpc、FUSE 开发库及 Python 3.9+。Python 工具只用标准库，不需要额外安装 SDK。HTTP RPC 使用 brpc 自带 protobuf JSON 接口，`bytes` 按 base64 传输。

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build-release -j4
ctest --test-dir build-release --output-on-failure

python3 tests/scheduler/run.py local --files 12 --optical-check
python3 tests/scheduler/run.py simulation
python3 tests/scheduler/run.py meter-closure --files 1000
```

`local` 启动独立端口的 Scheduler、MDS/CMS 和默认两个真实存储节点进程；`--optical-check` 额外启动新版光盘节点并验证库存未就绪时保持 JOINING。所有数据库、配置及节点数据均在本次结果目录中。结束时仅停止本工具启动的进程，文件保留。该环境是单服务器上的多进程功能实验，不是多台物理服务器的硬件性能验收。

CTest 只为公开任务入口注册 `scheduler_t03_local_smoke`、`scheduler_t03_simulation` 和 `scheduler_t03_meter_closure`，不依赖内部回归文件。真实 FUSE 挂载及硬件传感器测试需按文末说明显式运行 `external_test.py`，不会在默认 CTest 中自动启动。

## T03-1 批量文件测试

```bash
# 1 万文件，固定 4 KiB，四类负载分别运行，保留全部结果。
python3 tests/scheduler/run.py local --files 10000 --optical-check

# 真实多机环境：使用既有服务端点，测试结束后服务保持运行。
python3 tests/scheduler/run.py batch \
  --scheduler 127.0.0.1:9100 --mds 127.0.0.1:9000 \
  --files 10000 --min-size 4096 --max-size 65536 --workload all
```

默认：固定种子 `20260914`、并发 1、重复 1 次、无额外预热、功率采样间隔 100 ms。大小在 `[min-size,max-size]` 中按整数均匀分布；两者相同即固定大小。结果记录实际平均值与上下界。跨机性能测试需自行设置规模和重复次数，保留每次独立结果。

四类负载的含义：

- `sequential_write`：按文件顺序创建并写满，每个文件的 Create 和 Write 均在计时内；共 `2 × files` 条逻辑操作。
- `sequential_read`：预先创建并写满文件，在计时内依次读取整文件；预置不计入读取耗时。
- `random_rw`：预置文件后执行 `2 × files` 次随机读写，读写等概率；随机选择文件及合法偏移，每次最多 4 KiB。
- `typical_trace`：每 10 条操作为创建 1、写 2、读 6、删除 1。总操作数为 `10 × ceil(files/10)`；读写对象始终存在，不将无效随机请求算作成功。

计时从第一条测试操作向 CMS/MDS 提交开始，到最后一条逻辑操作完成结束。CMS 是分配权威：MDS Create 的返回目标必须出现在 CMS 已提交的 Working 目录；写入使用 AllocateFileWrite → WriteObject → CommitFileWrite → UpdateInodeStat；读取重新查询位置，经 ResolveFileRead → ReadObject 并逐字节检查。没有新增 Scheduler 直接绕过 CMS 放置的接口。

全文件读回校验放在计时区间后，单独记录 `validation.json`。输入 payload 是每文件独立的确定性二进制模式，支持任意偏移验证；结果提供 SHA-256，不能只凭“返回长度相同”通过。失败保留现场并以非零状态退出。

当前 MDS 的紧凑位置编码仍要求真实节点采用 `node-real-01` 等正整数尾号规范，磁盘使用节点上报的 ID。独立环境遵循这个约束；现有环境如使用任意字符串节点名，应由 MDS 负责人先完善字符串/紧凑 ID 映射。CMS 分配的 compact_id 与名字尾号不能混为一谈。

### 与 T04 新 Trace 的适配

```bash
python3 tests/scheduler/run.py batch \
  --scheduler 127.0.0.1:9100 --mds 127.0.0.1:9000 \
  --workload typical_trace --trace /path/to/generated_trace_step_0001.csv
```

接受 `tools/time_simulator` 的 `timestamp_us,operation_type,logical_path,offset,size_bytes` CSV；额外列忽略。按逻辑时间串行播放，落后时立即继续。读写路径映射到唯一测试命名空间，按最大访问范围预置文件；负偏移、逆序时间、越界路径、空 Trace 和非读写操作拒绝。此入口验证 T03 的数据节点负载，不承担 T02 的通用目录/文件系统 Trace 播放。

### 数据流与结果字段

每次运行输出到 `tests/scheduler/results/<时间>-<场景>-<唯一后缀>/`，不覆盖旧结果。

| 文件 | 字段及用途 |
| --- | --- |
| `environment.json`、`config/*.conf` | 代码提交、工作树差异摘要、主机、端口、服务配置；说明本地容量为配置配额、物理电源执行关闭 |
| `parameters.json`、`requests.json` | 文件规模/大小分布、种子、并发、预热、完整操作计划、采样周期和来源 |
| `preparation.json` | 预置区间和初始文件集，区分测试内耗时与预置开销 |
| `operations.jsonl` | 每请求序号、文件、操作、偏移、字节数、目标节点、墙钟起止微秒、单调时钟起止、成功/失败及错误 |
| `power.jsonl` | 定期完整 ManagedNodeView，含采样发起/返回时间、节点状态、功率档位、模型累计能耗、实际设备清单 |
| `manifest.json` | 最终仍存在的文件及 inode、节点、磁盘、服务地址、字节数 |
| `validation.json` | 全文件读回校验结果与覆盖数量 |
| `summary.json` | 完成数、失败数、总耗时、各节点请求数/唯一访问文件数/读写字节数/最终驻留文件数和字节数、功率与能耗 |
| `handoff.json` | 交给 T05/T02 的源文件地址、inode、分片参数、完整大小、SHA-256、功率记录位置和数据来源 |
| `failure.json` | 失败原因，不能用正常退出的部分步骤掩盖整个测试失败 |

“请求数”“访问的不同文件数”“最终驻留文件数”分别统计。一个文件反复读取不会增加驻留文件数；删除会减少驻留数量。每请求记录可按时间区间重算分布。所有已采样的存储节点及 MDS 都进入模型功耗求和，包括没有接收文件的节点；T05 的光盘能耗由组合实验单独汇入。

```bash
python3 tests/scheduler/run.py summarize tests/scheduler/results/<run>/sequential_write
```

汇总只读取原始数据，不重新访问服务器。功率使用采样档位对应的配置功率，插值并裁剪到同一负载起止时间后积分；采样必须覆盖整个区间，超出默认 5 秒的样本空洞会使测试失败。单调时钟用于总耗时，墙钟用于和外部电表对时。集群节点或跨机时钟有跳变的实验应重新同步后运行。

## T03-2 实际功耗仪测试

需要实际磁电设备清单和功率采样记录。可以导入外部功耗仪 CSV，也可以使用本文末尾新增的服务器 ACPI 真实传感器入口；二者计量边界和校准条件不同。没有真实记录时只能输出 `NOT_MEASURED`，不会宣称硬件达到 10 PB/kW。

1. 在单个待测物理存储节点的环境中，确认其空载电表连续采样；保存空载起止时间。功耗仪须覆盖完整创建/写入及读取区间，测试主机和电表统一 Unix 微秒时间。
2. 用 `batch --workload sequential_write` 运行创建写入，保留程序打印的结果目录。
3. 对同一 MDS 运行 `batch --workload sequential_read --reuse-run <上次结果>/sequential_write`，读取同一批文件。复用清单必须来自成功运行，数据不能被其他测试修改。
4. 对空载、写入、读取三个区间分别运行 `meter`。写/读起止时间直接取对应 `summary.json` 的 `start_us/end_us`。

功耗仪 CSV 必须采用：

```text
timestamp_us,node_id,power_w
```

每个节点时间严格递增，功率为非负有限值。同一时刻不同节点可交错记录；同一节点重复时间拒绝。每个物理电表只计一次，多个测试进程共用一台主机不能分别标记成多个物理电表。

硬件清单 JSON 的格式如下；数值必须替换为实际设备信息，此处为格式示例：

```json
{
  "source": "hardware_inventory",
  "nodes": [{
    "node_id": "node-real-01",
    "meter_id": "设备实际电表标识",
    "disk_groups": [{"count": 25, "capacity_bytes": 48000000000000}]
  }]
}
```

```bash
python3 tests/scheduler/run.py meter \
  --samples /path/to/meter.csv --inventory /path/to/hardware.json \
  --start-us 1789370000000000 --end-us 1789370060000000 \
  --max-gap-us 5000000
```

处理器拒绝缺失节点、重复电表、未覆盖区间、过大采样间隔、非数值功率及非法容量。它不补造缺失电表数据，也不把 Scheduler 功率档位当作实测读数。

单位和公式：

- `1 PB = 10^15 bytes`，`1 kW = 1000 W`。
- 节点容量 = 各组 `磁盘数 × 单盘容量` 之和。
- `energy_j = ∫ power_w dt_s`；`energy_wh = energy_j / 3600`。
- 平均功率 = 区间能耗 / 区间秒数。
- `PB/kW = (容量字节数 / 10^15) / (平均瓦数 / 1000)`。
- 零功率时容量功率比为 `null`，不输出 Infinity 或虚假的达标结果。

任务书参考值重新计算：`28×48 TB / 134 W ≈ 10.02985 PB/kW`；`25×48 TB / 120 W = 10 PB/kW`，不是 11.2；`25×40 TB / 100 W = 10 PB/kW`。

### 无电表时的软件闭环验收

```bash
python3 tests/scheduler/run.py meter-closure --files 1000
```

`python/meter_closure.py` 启动一个真实存储节点进程及 Scheduler、CMS/MDS，依次执行短空载窗口、批量创建写入、同一文件集顺序读回。默认 128 个文件，命令可调整；文件大小为 4096–65536 bytes 均匀整数分布，固定种子 20260914、并发 1、重复 1 次。读写结束后逐文件校验字节内容，比较两阶段的完整交接清单（含 inode、位置、大小及 SHA-256）。

随后按实际阶段起止时间生成**离线模拟电表 CSV**，不是实时采集器：空载恒定 80 W，写入从 100 W 线性上升至 140 W，读取从 100 W 上升至 120 W。采样间隔不超过 100 ms，并在区间两侧补充样本用于裁剪。参考容量为 `25×48 TB=1.2 PB`，只是模拟清单，不是测试主机的真实容量或数据目录配额。

每个 CSV 都经过正式 `run.py meter --simulated` 命令解析、积分、保存结果，再从原始文件重算，并与独立解析公式 `E=(起始功率+结束功率)/2×持续时间` 比较。另对写入区间回放恒定 160 W 的负对照，要求得到 7.5 PB/kW 和 `meets_10_pb_per_kw=false`；此处软件校验通过不表示能效达标。

模拟输入必须同时使用 `--simulated` 和清单 `source=simulated_inventory`，输出强制为 `source=simulated_meter`、`hardware_energy_status=NOT_MEASURED`。默认实测入口拒绝模拟清单，模拟入口也拒绝硬件清单，避免两种口径混用。仅靠软件不能鉴别用户错误标注的 CSV 是否确实来自硬件，真实验收仍需保留电表来源证据。

输出目录含 `inventory.json`、`write/`、`read/`、`idle/`、`over_target_power/`；各阶段保存 `meter.csv`、`meter_fixture.json`、`meter_cli.log`、`meter_summary.json`。正式分析入口生成的独立结果目录由汇总中的 `cli_results` 链接。顶层 `summary.json` 给出软件闭环结果、同文件校验结果、阶段能效及模拟边界。

这条链路计量对象仅为单个存储节点，不把同主机运行的 CMS/Scheduler 重复计为另一台物理设备。阈值判断仅容忍 `1e-12` 相对浮点舍入误差，不代表仪器误差裕量。接上硬件后替换清单和功耗仪 CSV，仍用相同的分析器，不需要改动积分与达标判定代码。

该模拟入口可以验收“T03 三项软件测试闭环（第二项使用模拟电表）”；不能将它写成“真实磁电设备能效已经实测达标”。真实传感器入口见文末；历史实验报告和运行结果仅保留在本地，不随代码提交。

## T03-3 指定仿真

`simulation` 启动独立 Scheduler 和 CMS/MDS，注册 10 个 simulated 存储节点，每个 1.1 PB，峰值 1000 W、待机 10 W。通过 MDS Create 调用正式 CMS 放置选出唯一目标；只向该目标提交模拟访问，其余节点自动进入待机。

在稳定状态采集 10 秒逻辑时间，每 100 ms 保存全部节点并检查档位和容量，比较每个节点累计能耗增量与独立公式。工作节点应为 10000 J，其他每个节点为 100 J，总计 10900 J。

```text
总容量 = 10 × 1.1 PB = 11 PB
总功率 = 1 kW + 9 × 0.01 kW = 1.09 kW
容量功率比 = 11 / 1.09 = 10.091743119... PB/kW
```

该场景使用模拟 IO，不调用假的物理服务地址。任务要求的 10 节点能效分母仅包含这 10 个仿真存储节点；承载 Scheduler/CMS 的真实主机功耗不计入该公式，也不据此宣称硬件实测达标。

## 新版适配与协作边界

- 内部 C++、Python 及 Trace 生成器回归用例留在开发机，不随本次代码交付；其他模块已在主分支上的测试不受影响。
- 客户端恢复迁移后 `ResolveFileRead=NOT_FOUND` 的对象读取回退；使用本次 MDS 位置回复的权威文件大小/分片大小，裁剪 EOF，不吞掉 IO/网络错误。公开外存入口覆盖跨对象文件迁移前后的实际读回。
- 新光盘引擎编译恢复，心跳保留引擎就绪信息；没有库存时保持 JOINING。删除旧 ImageStore 的库存和“待写任务恒为零”逻辑，等待 T05 提供线程安全的真实库存和异步任务快照。
- MDS 的旧归档写 RPC、T02 的光盘异步读适配、T05 的镜像 ID 与打包/刻录完成回报仍属于对应负责人。T03 的光盘边界测试不把这些场景标为成功。
- T02 光磁组合的测试入口仍由 T02 维护；本目录提供磁盘侧文件位置、运行区间、电表分析结果。T05 的归档/恢复可复用 `batch` 保留的数据和 `handoff.json`。
- 物理电源操作默认关闭。模型功率/模型能耗、真实 RPC 耗时、功耗仪实测在报告中分别标记。

原始输出、运行数据库、日志和实验结果报告不提交 Git；本次运行的结果以 `results/` 索引和对应外存目录为准。1 万文件测试不能证明 100 亿文件、1 ZB 或 1 亿张光盘的完整规模验收，最终集成结论由 T01 汇总。

## 外存与完整 FUSE 入口（2026-09-15 补充）

```bash
python3 tests/scheduler/external_test.py \
  --external-root /mnt/md0 --files 10000 --meter-files 1000 \
  --trace /mnt/md0/Projects/cgy/ZBStorage/time_sim_output/full/generated_trace_step_0001.csv
```

先用 `--files 12 --meter-files 12` 做小规模预检。依赖 `/dev/fuse`、`fusermount3`、`findmnt` 和外存写权限；不可挂载时直接失败，不退化成普通本地目录 IO。

`external_test.py` 在外存创建唯一的私有目录，全部数据库、对象、FUSE 挂载、原始 Trace/请求记录和客户端日志保存于其中。仓库 `tests/scheduler/results/*-external-index-*/` 保存外存路径和汇总索引。不会读取现有集群数据库、覆盖他人目录或占用旧集群端口。`--trace` 只读复制其他模块的请求文件，重映射到本次独立命名空间；payload 是本工具生成，不冒充他人的原始业务内容。

执行顺序为：

1. 当前版本 Scheduler + CMS/MDS/RocksDB + 两个真实节点 + 真实 FUSE 挂载；四类文件负载均使用 `open/pwrite/pread/unlink/mkdir`，辅助 RPC 只观察位置、准入、统计，不代替文件写读。
2. 四类负载复用顺序写创建的同一文件集，分别计时和输出；后续场景不重复预置万文件。这与早先各场景独立预置的实验不同，不能直接比较耗时。已有数据和后端缓存保留。
3. 读写均用 `O_DIRECT` 打开 FUSE 文件；客户端日志必须证明对应读写字节数进入 MDS 和存储节点 RPC，且退出时无指标丢失。这里只绕过 FUSE 文件数据缓存，不清空外存后端的系统或设备缓存。
4. 可选回放其他模块的读写 Trace CSV；然后停止本次服务，用相同端口及持久化目录重新启动并挂载，完整读回万文件，验证 inode/大小/位置不变。此为正常停启恢复，不是断电持久性测试。
5. 独立的小型双节点集群验证 5 MiB + 17 bytes 跨对象文件的 FUSE 写读、排空迁移、源对象清空、CMS 零引用、退休后不再分配。
6. 独立单真实节点以 FUSE 跑空载、1000 文件写/同集读，复用模拟电表分析；最后独立运行 T03-3 十节点仿真。

每个“真实节点”仍是同一服务器上的服务进程，两个目录盘共享同一 RAID；配置的每盘 1 GiB 是测试配额，不是物理硬件容量。光盘归档、真实副本、多主机网络、硬件关机不在此入口范围。能耗仍分别标记 Scheduler 模型与模拟电表，不输出硬件实测达标结论。

当前串行基线显式使用 FUSE `-s`（单回调工作线程）。真实多线程预检发现大文件分片提交可能回退文件大小，详见 [并发写缺陷记录](../../docs/issues/fuse-concurrent-write-20260915.md)。这项限制是已知未修复问题，不能由串行测试通过推出并发场景通过。

外存运行结果和环境通过本次 `results/*-external-index-*/external_run.json` 查找，不依赖仓库中提交实验结果。

## 外存负载 + 真实整机功率（2026-09-21 新增）

```bash
python3 tests/scheduler/external_test.py \
  --hardware-power --external-root /mnt/md0 \
  --files 10000 --meter-files 10000 --idle-seconds 30 \
  --trace /mnt/md0/Projects/cgy/ZBStorage/time_sim_output/full/generated_trace_step_0001.csv
```

`--hardware-power` 使用 `python/hardware_power.py`，在全部负载前开始连续采样，所有服务结束和最后基线完成后停止。默认只读 `/sys/devices/LNXSYSTM:00/LNXSYBUS:00/ACPI000D:00/power1_average`，单位微瓦，转为瓦；每秒采样，固件自身平均周期也是 1 秒。传感器不可读、采样线程失败、时间跳变超过 100 ms、窗口缺少两侧覆盖或样本间隔超过 5 秒都拒绝出具正常报告，不退回模拟值。

当前机器为单台服务器：MDS、Scheduler 和所有真实节点进程共用一个物理计量对象，只计算一次整机功耗。用户确认测试阵列磁盘由该服务器电源供电；公共插排上的其他设备不会因共用插排而进入服务器内部传感器。若改用插排总电表，则必须排除其他设备用电。传感器固件自报 `Intel(R) Node Manager / Meter measures total domain`；尚未独立验证 AC/DC 计量边界和校准误差，不能称为校准墙插电表。

容量通过实际 `/sys/class/block/md0/slaves` 成员及扇区数读取，只计测试阵列，不套用任务书示例或节点配置配额。必须确认外存挂载源是指定阵列、阵列没有降级且成员数正确。输出同时提供原始磁盘容量与 RAID 块设备容量两种 PB/kW，分母均为实测整机平均功率；其他主机磁盘不计入容量，但其功耗以及其他服务活动可能包含在分母中，因此这是已声明范围的“测试阵列容量 / 整机功率”，不是独立存储服务的功耗归因。

硬件模式下单节点实验用真实 FUSE 运行无测试请求窗口、创建写入、同文件集顺序读取，不调用 `synthetic_samples`，也不使用 25×48 TB 的模拟容量。前后基线各默认 30 秒，单节点服务就绪后另采 30 秒；它们不是整台共享服务器的绝对空载。不会直接用基线相减声称 ZB 独占能耗。短于 10 秒的阶段标记 `SHORT_WINDOW`，不可按完整长窗口同等精度解读。

外存入口还新增真实节点心跳恢复闭环：只停止本测试启动的一个节点进程，观察 Suspect → Failed、CMS 排除新写入，然后按相同配置/端口/数据重新启动，检查 CMS 健康恢复、compact ID 保持和 20 个原文件读回。期间向健康节点执行 10 个文件写读。此场景不是断电、坏盘或副本故障转移。

新增结果：

| 文件 | 含义 |
| --- | --- |
| `hardware_power/sensor.json` | 传感器路径、型号、固件自报精度/平均周期、计量边界限制 |
| `hardware_power/inventory.json` | 物理阵列成员/原始容量/RAID 容量、供电确认来源 |
| `hardware_power/meter.csv` | Unix 微秒、单调时钟、读取耗时、原始微瓦和瓦；同一物理计量对象 |
| `hardware_power/acquisition.json` | 样本数、读数变化数、最小/最大功率、时钟检查 |
| `<阶段>/hardware_energy.json` | 相同负载窗口的实测瓦/焦耳/Wh、原始容量与 RAID 容量 PB/kW、阈值判断 |
| `baseline_before/window.json`、`baseline_after/window.json` | 无本次测试请求的前后窗口 |
| `health_recovery/*.json` | 心跳状态、CMS 失败/恢复视图及真实文件回读结果 |

原有各负载 `summary.json` 中的 `model_energy_j` 仍为 Scheduler 模型值，不会被实测替换或与实测相加。顶层 `status` 判断软件流程；`hardware_energy_status=HARDWARE_SENSOR_MEASURED` 表示取得真实传感器记录；`energy_target_status` 独立判断实际配置是否达到 10 PB/kW。软件 `PASS` 与指标 `FAIL` 可以同时出现。十节点仿真仍单独用 11 PB / 1.09 kW 计算，不能将真实宿主机功耗代入仿真公式。

硬件分析完成前，单节点报告为 `PENDING_ANALYSIS`，顶层为 `RUNNING`；只有负载、服务清理、采样收尾及全部必需窗口分析都成功后才发布顶层 `PASS`。任一步失败，外存顶层和仓库索引统一为 `FAIL`。CLI 分别显示软件闭环、硬件测量和容量功率比目标，不把三者合并成一个笼统的通过结论。

## 审查后的错误处理约定

- 配置、对象 ID 和分页 token 要求完整解析，拒绝非法值，不通过异常捕获后补默认值继续运行。
- CMS compact ID 只在完整提案持久化成功后发布；失败提案/失败写库不消耗权威 ID，恢复时一次性校正下一个 ID，无需每次分配重复扫描全部节点。
- 真实/虚拟节点枚举错误和 CMS inode 解码错误必须向上传播，不能返回部分列表或把未知引用当作零引用。
- 无效功率策略明确失败；CMS 权威、迁移数据校验、零引用门禁、物理执行开关等必要保护保持不变。
- 模型功率数据缺失、实际 IO 节点未进入功率采样或必需硬件阶段缺失时测试失败，不输出看似正常的零能耗。
- Trace 生成器遵守零权重约束并拒绝非有限输入；零权重操作不会为了凑满请求数而被偷偷执行。生成器的内部单元测试仅保留本地。
