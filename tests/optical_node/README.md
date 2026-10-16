# optical_node 集成测试

进程内起真光节点服务 + 假 real_node / 假 scheduler，通过 brpc 走完
**归档下发（四种批次形态）→ 下载侧背压节流 → 256KiB 分片下载重组 → 压缩 → 卷镜像封装 → 打包汇报
→ 按容量积攒 → 封印为光盘 → CD_BURN 刻录 → 物化 vdisc → 逐个释放写镜像 → 刻录汇报
→ 数据面读（新 6 接口：SubscribeReadEvents → PrepareRead → ReadReady 通知 → AcquireReadSession
→ ReadPreparedFile 区间读 → ReleaseReadSession / ReleasePreparedRead）/ CD_READ 重载 → 缓存命中
→ 读会话与引用生命周期（多次独立打开 / EOF 与随机重读 / 两级释放）→ LRU 淘汰与重载
→ 正常关机落盘（log/runtime_state_snapshot）→ 同 root 重启恢复 → 在途准备任务继续推进到可读
→ 节点上报（以真 server 子进程验证注册 / 心跳+设备清单 / 光盘库状态）**
全链路，断言只依赖文件系统产物、RPC 返回码、通知 Stream 消息与控制台汇报行，不使用固定 sleep。

ctest 只注册 **一个综合用例**（`optical_node`）：一次运行覆盖以上全部环节，含末尾的「节点上报」测试点
（`C1`）——它 fork/exec 真 `optical_node_server` 子进程，复用本用例的假 scheduler 作为上报终点。

## 一、前置条件

- 已配置好构建环境（brpc / protobuf / gflags / zlib），且 `include(CTest)` 生效（顶层 `CMakeLists.txt` 已具备）。
- **无需任何外部语料**：驱动脚本每次都用 `prepare_archive_corpus.py` 在进程内合成语料。
  语料不再是 jpg，而是**确定性合成的高熵文件**（随机字节主体 + 少量可压缩填充），由 `--target-ratio`
  把 zlib level 9 的实测压缩比稳定控制在 **0.97** 附近——既避免高压缩率导致镜像填不满，
  又不再依赖 `~/WR/testWrite` 之类的 jpg 目录。

## 二、测试规模（固定参数）

| 参数 | 值 |
| --- | --- |
| `volume_size_bytes` | 10 MiB（10485760） |
| `size_threshold` | 0.9（打包触发线 9437184 字节） |
| `disc_capacity_bytes` | 100 MiB（104857600） |
| `standard_images_per_disc` | 10（100 MiB 盘恰好容纳 10 个 ≈9.7 MiB 的镜像） |
| `disc_block_size_bytes` | 2048 |
| 每卷文件数 / 单文件大小 | 10 / 1 MiB（实测压缩率 0.97，10 个文件恰好越过触发线） |
| 对象分片大小 | 256 KiB（4 片/文件，覆盖下载侧多分片重组） |
| `available_id_count`（volume/disk 共用） | 5 |
| `capacity_in_images` / `max_write_images` | 14 / 12（小容量档位：写链路必然触发背压，读入若干已刻录卷后必然满载触发淘汰） |
| 卷数 | 56（5 张完整光盘 + 尾盘） |
| inode 起始号 | 1000（56 卷 × 10 文件 = 1000~1559） |
| 读会话单次读取上限（max_read_bytes） | 4 MiB |

容量约束的不变式：**单盘镜像数(10) < `MAX_WRITE_IMAGES`(12) < `CAPACITY_IN_IMAGES`(14)**。
下限侧违反会让节点永远攒不满一张盘（不封印 → 不释放 → 下载活锁）；上限侧违反会让背压来不及生效、
先撞上 `VOLUME_FULL_NO_READABLE`。`A0` 会断言这条不变式。

单一综合场景（卷数 > 单盘镜像数才会封印出第一张盘）：

| 场景 | 卷数 | 档位（容量 / 写镜像上限） | 覆盖内容 | 墙钟（实测） |
| --- | --- | --- | --- | --- |
| `optical_node` | 56 | 14 / 12 | 写链路与四种批次形态、打包/刻录汇报、5 张光盘共存与尾盘、下载背压的触发与恢复、新 6 接口读链路（订阅 → 准备 → ReadReady → 建会话 → 区间读 → 两级释放）与 CD_READ 重载、缓存命中、读会话与引用生命周期（多次独立打开 / EOF 与随机重读）、`image/` 满载后的 LRU 淘汰与重载、正常关机落盘与重启恢复（在途准备任务继续推进到可读） | 约 9 分钟（533s） |

墙钟几乎全部来自光盘库仿真的固定时长（装盘 12s、退盘 3s、攫取 2s、读 0.5s+100MB/s、刻录 1s+20MB/s），
产线未提供测试旋钮，**不可压缩**；单次 CD_READ 约 20~32s，故 12 次 A3 读回加 A5/A8 的重载就占 6 分钟以上。

## 三、构建

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j8 --target optical_node_archive_integration_test
```

## 四、运行

### 方式 A：ctest

> **临时屏蔽**：综合用例 `optical_node` 在 `CMakeLists.txt` 中被临时加上 `DISABLED TRUE`，`ctest` 默认不执行它，
> `ctest -R '^optical_node$'` 也会因该用例被禁用而选不到。只想单跑这个综合用例 → 用**方式 B**；
> 要恢复随 `ctest` 一起跑 → 删掉那处 `DISABLED TRUE`（及其注释）并重新构建。

```bash
# optical_node 相关的全部用例（codec 单测会执行；综合用例处于禁用态，被跳过）
ctest --test-dir build -L optical_node -j1 --output-on-failure

# 综合用例（精确匹配用例名；需先移除 DISABLED 属性才会执行）
ctest --test-dir build -R '^optical_node$' --output-on-failure
```

用例注册参数：

| 用例 | TIMEOUT | SKIP_RETURN_CODE | LABELS |
| --- | --- | --- | --- |
| `optical_node` | 3600s | 77 | `optical_node;slow` |
| `optical_node_runtime_state_codec` | 60s | — | `optical_node;unit` |

ctest 通过 `$<TARGET_FILE:optical_node_server>` 把真 server 二进制路径经驱动脚本传给综合用例
（其中的 `C1` 上报测试点会 fork/exec 它），并与之建立构建依赖，无需手工准备。

### 方式 B：python 驱动脚本（推荐；备语料 → 跑二进制 → 汇总）

`optical_node` ctest 用例本质就是执行本脚本，因此**直接调用它即可单跑综合用例，不受上面的 ctest 禁用影响**：

```bash
python3 tests/optical_node/run_optical_node_archive_test.py \
  --driver "$PWD/build/optical_node_archive_integration_test" \
  --server-bin "$PWD/build/optical_node_server" \
  --keep
```

常用可选项：

| 参数 | 说明 |
| --- | --- |
| `--driver` | C++ 测试二进制路径，**必须是绝对路径**（脚本会把工作目录切到工作目录下再执行） |
| `--server-bin` | 真 `optical_node_server` 二进制路径（绝对路径）；`C1` 节点上报测试点会 fork/exec 它，不传则打印 `[SKIP] C1` |
| `--work-dir` | 工作目录，默认 `tests/optical_node/results/optical_node-<时间戳>` |
| `--keep` | 成功时也保留 `corpus/`、`archive/` 与 `reporting/`（默认成功后只保留 `output.log`，失败则全部保留） |

卷数固定为 56（脚本内置），无需也无法通过参数改变。脚本不再需要 jpg 语料目录，也没有「语料缺失 → 跳过」分支，
会先在 `<work>/corpus/` 内合成语料再运行二进制。

### 方式 C：直接跑二进制（自行生成语料）

```bash
python3 tests/optical_node/prepare_archive_corpus.py \
  --out-dir tests/optical_node/results/corpus-manual \
  --volumes 56

./build/optical_node_archive_integration_test \
  --corpus tests/optical_node/results/corpus-manual \
  --work-dir tests/optical_node/results/manual
```

语料脚本参数：`--volumes`（卷数）、`--files-per-volume`（默认 10）、`--file-size-mb`（默认 1）、
`--object-unit-kib`（默认 256）、`--inode-base`（默认 1000）、`--seed`（默认 20260925，确定性随机）、
`--target-ratio`（目标压缩比，默认 0.97）。
加 `--selftest` 只打印实测压缩率与预测打包分组、不落盘：

```bash
python3 tests/optical_node/prepare_archive_corpus.py --selftest
```

语料参数必须与 C++ 侧固定常量一致（卷 10 MiB、阈值 0.9、每卷 10 个 1 MiB 文件），
`check_plan` 会在计划为 `volumes × files_per_volume` 之外的形态时直接报「参数已漂移」退出。

## 五、产物与结果查看

工作目录布局（`<work>` 默认为 `tests/optical_node/results/optical_node-<时间戳>/`）：

```text
<work>/
├── output.log                    # 本次运行的完整日志（按测试点记录目标/期望/方法/逐项检查结论）
├── corpus/                       # 脚本生成的语料（模拟 real_node 磁盘）
│   ├── manifest.tsv              # inode_id / file_size / object_unit_size / object_count / node_id / disk_id / volume_index / object_dir
│   ├── expected_pack_plan.tsv    # volume_index / count / inode_ids —— 由 zlib9 精确预测的打包分组，断言真值
│   ├── corpus_meta.txt           # 压缩率与各尺寸参数
│   └── real-1/disk0/obj-<inode>-<index>.dat   # 每个文件 4 个 256KiB 分片
├── archive/                      # 光节点工作目录
│   ├── input/  temp/             # 归档中间文件（最终应为空）
│   ├── image/                    # 卷镜像平铺（容量 14）：volume_<id>.vimg ≈9.7MiB
│   ├── disc_sim/                 # 光盘库模拟区（已物化光盘本体）：disc_<id>.vdisc ≈97MiB
│   ├── read/                     # 数据面读产物（读完应被清理）
│   ├── meta/                     # pending_disc_meta（未封盘集合）、node_discs_meta（已刻录索引，持久）；
│   │                             # disc_<disk_id>_meta 为**临时**中间态，刻录 + 索引完成后即被删除
│   └── log/                      # 任务日志 + 关机快照 runtime_state_snapshot(.loaded)
└── reporting/                    # 节点上报测试点（C1）子进程：optical_node.conf、server.log、archive/（成功默认清理）
```

成功时的标志性输出：

- 每卷一条 `[node] [ReportFilesPackedToImage] image_id=<id> count=10 inode_ids=...`（inode 集合应与 `expected_pack_plan.tsv` 对应行一致）。
- 每张盘一条 `[node] [ReportImagesBurnedToDisc] disc_id=<id> count=10 image_ids=...`（该盘刻录完成、写镜像已逐个释放）。
- 末尾 `[PASS] optical_node 归档集成测试通过`，退出码 0。
- `archive/disc_sim/disc_<id>.vdisc` 约 100MiB；`input/`、`temp/` 最终为空；`meta/` 无残留 `disc_<id>_meta`。

结果目录按 `tests/README.md` 约定落在各模块的 `results/` 下（该目录已被 Git 忽略）。产物保留策略：

| 情况 | 保留内容 |
| --- | --- |
| 成功（默认） | 只保留 `output.log`，删除 `corpus/`、`archive/` 与 `reporting/`（合计实测约 1.5GB；`reporting/` 为 C1 子进程产物，很小） |
| 失败 | 全部保留，便于按日志与目录快照定位问题 |
| `--keep` | 无论成败全部保留 |

`output.log` 由驱动脚本把测试进程的 stdout/stderr 合并落盘（同时仍在终端流式输出），
逐测试点记录目标、期望、验证方法与每项检查的实际值；运行结束时会自动回显其中的「测试点汇总」区块。
直接运行二进制（方式 C）不会写该文件，需要时自行 `> out.log 2>&1`。

## 六、测试点与断言覆盖面

日志中的 `[CASE]` 编号即测试点编号：多卷场景下同一测试点按 `A1.1 … A1.56` 递增，
汇总表里每个编号单独一行（各自记录检查数与失败数），目标清单则按种类只列一次。

| 编号 | 测试点 | 测试目标 | 主要断言 |
| --- | --- | --- | --- |
| `A0` | 环境与配置就绪 | 被测光节点与全部仿真对手方、语料分组真值、档位参数可信，后续断言才有前提 | 分组非空、档位满足「单盘镜像数 < 背压上限 < 容量」、`IsArchiveEngineReady` 为真、`SubscribeReadEvents` 建立通知通道并返回 >0 的 `session_id`、三个 brpc server 均已监听、archive 各子目录存在 |
| `A1.i` | 第 i 卷：归档下发 → 压缩封装 → 打包汇报 | 写链路端到端：按 256KiB 分片下载 → 压缩累加 → 达阈值封装 → 上报 → 写镜像留在 `image/` 等刻录释放；批次切分（形态写在标题里）不应改变卷分组 | 汇报 `count` 与 inode 集合 == zlib9 预测分组、`volume_id` 全局唯一、`image/` 出现该写镜像且体积 ∈ (0, 10MiB] |
| `A2` | 下载完整性与多分片重组 | 节点没有漏读或重复读 real_node 的对象，且逐片取回、按绝对偏移重组 | `ResolveFileRead` 次数 == 文件数、`ReadObject` 次数 == 文件数 × 4、分片大小均为 256KiB、每文件 4 片 |
| `A3.i` | 第 i 卷：数据面读（新 6 接口）+ CD_READ 重载 | `PrepareRead` 立即可受理并返回 `task_id`；准备完成后节点经通知 Stream 主动推送 `ReadReady`；客户端据 `task_id` 建立读会话、按文件绝对偏移区间读取，镜像被 CD_READ 从 vdisc 复制回 `image/` 并读通 | `PrepareRead` 返回 `OPTICAL_OK`+`task_id`、收到 `request_id/task_id/inode_id/file_size` 匹配的 `ReadReady`、`AcquireReadSession` 返回 `max_read_bytes=4MiB` 与正确 `file_size`、读回字节与语料逐字节一致、镜像回到 `image/`、vdisc 数量不变（只复制不搬移）、`image/` 镜像数 ≤ 容量 |
| `A4.i` | 第 i 卷：缓存命中 | 同卷第二次读不重复走光盘装载（省掉一次 20s+ 的 CD_READ） | 「`PrepareRead` → `ReadReady`」耗时 < 3000ms（实测 1~2ms）、`image/` 与 `disc_sim/` 快照前后完全不变、数据仍与语料一致 |
| `A9` | 读会话与引用生命周期 | 同一 READY 任务可多次独立 `AcquireReadSession`（各自持读取引用）；EOF 只表示本次到达文件末尾（不关会话、不释放缓存）；支持随机读/重复读/先读尾再读头；超上限的单次请求被拒；`ReleaseReadSession` 只减读取引用（close 后可再 open），`ReleasePreparedRead` 才撤销新打开资格 | 两次 `AcquireReadSession` 得到不同 `read_handle`、两个句柄都能读对；尾区间 `eof=true`、`offset≥file_size` 返回 0 字节且 `eof=true`、`size=0` 返回 0 字节且 `eof=false`；单次 `size>max_read_bytes` 返回 `OPTICAL_INVALID_ARGUMENT`；关一个句柄另一个仍可读；全关后再 `Acquire` 仍成功；`ReleasePreparedRead` 后再 `Acquire` 返回 `OPTICAL_NOT_FOUND` |
| `A5` | LRU 淘汰与重载 | `image/` 满 `capacity_in_images` 后读新卷应换出一个 READ 镜像，且被换出的卷仍可重载读通——淘汰只影响缓存层级，不影响数据可用性 | 满载后读新卷仍能读通、`image/` 恒为 14、`disc_sim/` 不变、刚加载的卷保留、victim 恰为读前读后差集且 ≠ 刚加载的卷、victim 重载读通并回到 `image/` |
| `A6` | 归档中间态清理 | 流程结束不残留中间产物 | `input/` 无 `.archive`、`log/` 目录存在 |
| `A7` | 光盘封印与刻录（跨卷汇总） | 镜像按容量积攒，装不下下一个时才把当前集合封印成一张光盘（提交 `DISC_BURN`，刻录完成后物化 vdisc 并逐个释放 `image/` 中的写镜像、上报）；未装满的尾盘继续留在 pending | 写阶段 `image/` 峰值 < 容量（背压生效、未触发 `VOLUME_FULL`）、每盘 `count` == `image_ids` 数量、**`node_discs_meta` 追加块数 == 汇报盘数**、**`meta/` 无残留 `disc_<id>_meta`**、vdisc 存在且 ≤ 盘容量、`disc_sim` vdisc 数 == 已刻录盘数、已刻录镜像恰为打包顺序的前缀、尾盘镜像仍在 `image/` |
| `A8` | 正常关机落盘与重启恢复（在途准备任务继续推进） | `Shutdown()` 把运行期内存态落到 `log/runtime_state_snapshot`；同 root 重启后读回并让关机前未完成的准备任务继续推进到可读——节点重启不丢在途工作（通知会话 / 读会话表为进程内易失态，不落盘） | 关机快照生成且非空、状态切 `STOPPED`；重启后 `RUNNING` 且原因含 `runtime state restored`、快照被消费为 `.loaded`、`image/` 与 `disc_sim/` 前后完全一致、`node_discs_meta` 块数与字节数不变、`meta/` 无残留 `disc_<id>_meta`；**在途任务用原 `task_id` 经 `AcquireReadSession` 轮询到 `OPTICAL_OK` 后仍能读回完整数据**（字节与语料一致），且不补发 `ReadReady` |
| `B1` | 批次形态覆盖（下发切分不改变卷分组） | 批次边界（单文件小批次 / 跨卷边界批次 / 大小混合批次）不应改变卷分组、不应遗漏或重复文件；顺带覆盖「多个 MDS 批次累计成一轮下载」（一轮配额 = 卷镜像容量） | 四种形态均出现、单文件批次 ≥10、跨卷边界批次 ≥2、第 1 卷由 10 个单文件批次累计而成、汇报数 == 卷数、全部 inode 无遗漏无重复 |
| `B2` | 背压触发与恢复 | 写镜像数达 `MAX_WRITE_IMAGES` 时暂停下载、不把 `image/` 堆到硬上限；刻录完成释放写镜像后应唤醒被阻塞的下载线程（不能睡死） | 采样到 `throttled=true`、写镜像数确实堆到上限、过冲 ≤1 个镜像、触发原因覆盖「写镜像达上限」、随后采样到 `throttled=false`（写镜像占用回落） |

### 节点上报测试点（`C1`，并入综合用例）

节点上报（启动期注册、运行期心跳与光盘库状态）由 `optical_node_server.cpp` 的启动逻辑触发，进程内的综合
场景（直接构造 `OpticalStorageServiceImpl`、不走 `main()`）触发不到。因此把该测试点并入综合用例：在用例
末尾**`fork/exec` 启动真 `optical_node_server` 子进程**（生成临时配置把 `SCHEDULER_ADDR` 指向本用例既有的
假 scheduler、`--port=0` 由内核分配端口），轮询假 scheduler 的调用计数并对捕获的 brpc 请求逐字段断言，
结束后对子进程发 `SIGINT` 验证优雅退出（brpc 默认只对 SIGINT 注册优雅退出处理；SIGTERM 需
`--graceful_quit_on_sigterm=true`）。子进程配置与 stdout/stderr 落盘到 `<work>/reporting/`（成功默认随 `corpus/`、`archive/` 一并清理）。

| 编号 | 测试点 | 测试目标 | 主要断言 |
| --- | --- | --- | --- |
| `C1` | 调度器上报链路（注册 / 心跳+设备清单 / 光盘库状态） | 光盘节点启动后应完成一次性注册并周期上报心跳（含设备清单）与光盘库状态；无数据面 I/O 时不应上报指标 | 注册恰好 1 次、`kind=OPTICAL_LIBRARY`/`execution_mode=PHYSICAL`/不参与放置/`activate_immediately=false`、3 个设备组容量与数量一致、功率四级非递增且峰值 >0、光盘延迟 >0；心跳 `node_type=NODE_OPTICAL`、就绪标志为真、含 1 个 `hdd-cache`(容量=容量上限×卷大小) + 1 个 `disc`(容量=盘容量×盘数) + `DRIVE_COUNT` 个 `optical-drive`；库状态 `observed=true`、`local_disc_count=TOTAL_DISC_COUNT`、`total_slots=SLOT_COUNT`、空库 `blank=local` 且 `recorded=recording=defective=0`、分类守恒、`staging_capacity_bytes=容量上限×卷大小`；`ReportNodeMetrics` 调用次数为 0；`SIGINT` 后子进程 `exit=0` |

子进程二进制路径由 ctest 经驱动脚本自动传入（`--server-bin`）；手工直跑二进制时若不传该参数，则打印
`[SKIP] C1` 并跳过该测试点（其余测试点不受影响）。断言只依赖假 scheduler 捕获的 brpc 请求与子进程退出码，
不使用固定 sleep。

日志的构造方式（每项检查都记录实际值，便于回溯时序特征）。以下是实跑日志的原文摘录
（仅把绝对路径缩写成 `.../archive/…`、把过长的文件清单用 `…` 折叠）：

```text
[CASE] A0  环境与配置就绪
    [PASS] 被测光节点 Run() 成功且状态为 RUNNING
    [PASS] 三个 brpc server 均已监听（光节点 / 假 real_node / 假 scheduler）
  [STEP] 客户端 SubscribeReadEvents 建立准备完成通知通道（附带 brpc Stream）
    [PASS] SubscribeReadEvents 成功且返回有效 session_id（=1）
[CASE-END] A0 result=PASS checks=6 failed=0

[CASE] A3.1  第 1/12 卷：数据面读（新 6 接口）+ CD_READ 重载
  [STEP] PrepareRead volume_1 inode=1009（镜像在光盘库，预期触发一次 CD_READ 装载）
    [PASS] 读回数据与语料逐字节一致（inode=1009，共 1048576 字节，准备耗时 20177ms，总耗时 20180ms）
    [PASS] AcquireReadSession 返回 max_read_bytes=4MiB（actual=4194304 expected=4194304）
    [PASS] AcquireReadSession 返回 file_size 与语料一致（actual=1048576 expected=1048576）
    [PASS] CD_READ 后镜像回到 image_dir（volume_1）
    [PASS] 光盘文件持久保留（读回只复制区间，不搬移/删除 vdisc）（actual=5 expected=5）
    [PASS] image_dir 镜像数不超过容量上限（当前 8 / 14）
[CASE-END] A3.1 result=PASS checks=6 failed=0

[CASE] A4.1  第 1/12 卷：缓存命中（镜像已在 image_dir，不应再次触发 CD_READ）
  [STEP] PrepareRead 同卷第二个 inode=1008，读前快照 image=[volume_1.vimg,…] disc_sim=[disc_1.vdisc,…]
    [PASS] 缓存命中读回数据与语料一致（inode=1008）
    [PASS] 缓存命中「PrepareRead → ReadReady」耗时 < 3000ms（实测 1ms，远小于一次 CD 装载）
    [PASS] 缓存命中前后 image_dir / disc_sim 内容不变
[CASE-END] A4.1 result=PASS checks=3 failed=0

[CASE] A8  正常关机落盘与重启恢复（在途准备任务继续推进）
    [PASS] 重启时确实从快照恢复了运行期状态（状态原因含 runtime state restored）
    [PASS] 快照已被消费为 runtime_state_snapshot.loaded（不会重复重放）
    [PASS] node_discs_meta 追加块数在重启前后一致（恢复未重复追加）（actual=5 expected=5）
  [STEP] 重启后重新 SubscribeReadEvents（通知会话不落盘，需重新建立）
  [STEP] 以原 task_id=566 轮询就绪并读回：验证在途任务在重启后被恢复并推进到可读
    [PASS] 恢复任务就绪（AcquireReadSession 返回 OPTICAL_OK，task_id=566） —— 已在 31878ms 内满足
    [PASS] 在途准备任务在重启后仍能恢复并读回完整数据、与语料逐字节一致（inode=1129，准备耗时 31878ms）
    [PASS] 重启后未对恢复任务补发 ReadReady（恢复任务通知目标 session_id 不落盘，为 0）
    [PASS] 重启后完成读回的卷镜像落在 image_dir（volume_13）
[CASE-END] A8 result=PASS checks=16 failed=0

[CASE] A9  读会话与引用生命周期（多次独立打开 / EOF 与随机重读 / 两级释放）
  [STEP] PrepareRead volume_12 inode=1119（镜像已在 image_dir，准备应快速完成）
    [PASS] 收到匹配的 ReadReady（task_id=617）
    [PASS] 同一 READY 任务两次 AcquireReadSession 得到两个不同 read_handle（=26/27）
    [PASS] 先读尾部区间成功且 eof=true（offset=1044480）
    [PASS] 再读头部区间成功且 eof=false（随机/逆向读取不依赖隐式游标）
    [PASS] 重复读同一区间返回一致字节（EOF 不释放会话、不改变引用）
    [PASS] offset≥file_size 返回 0 字节且 eof=true
    [PASS] size=0 返回 0 字节且 eof=false
    [PASS] 单次 size>max_read_bytes 返回 OPTICAL_INVALID_ARGUMENT（size=4194305）
    [PASS] 关闭全部读会话后再次 AcquireReadSession 仍成功（read_handle=28，准备引用尚在）
    [PASS] ReleasePreparedRead 后再 AcquireReadSession 被拒（不再允许新会话）
[CASE-END] A9 result=PASS checks=19 failed=0
```

`B1` / `B2`（写链路的下发切分与下载背压）的观测事实：

```text
[CASE] B1  批次形态覆盖（下发切分不改变卷分组）
  [STEP] 批次形态分布：大小混合批次×4，整卷单批次×50，跨卷边界批次×3，逐文件小批次×10；
         批次总数=67（单文件批次=11，跨越卷边界的批次=3）
    [PASS] 四种批次形态均被覆盖（逐文件小批次 / 整卷单批次 / 跨卷边界批次 / 大小混合批次）
    [PASS] 单文件批次至少 10 个（覆盖多个 MDS 批次累计成一轮下载）
    [PASS] 存在跨越卷边界的批次（覆盖一个批次的文件分属两张卷镜像）
    [PASS] 第 1 卷由 10 个单文件批次累计而成（小批次未各自成卷）
    [PASS] 打包汇报数与预测卷数一致（actual=56 expected=56）
    [PASS] 批次切分未造成文件遗漏或重复（每个文件恰好归属一个卷镜像）
[CASE-END] B1 result=PASS checks=6 failed=0

[CASE] B2  背压触发与恢复（写镜像达上限 → 暂停下载 → 刻录释放唤醒）
  [STEP] 写阶段背压采样：采样 67 次；最近一次[throttled=false write_images=6/12 input_pending=0/1048576]；
         峰值 write_images=12/12；峰值 input_pending=1048576 字节；曾出现 throttled/写镜像过载/原始积压过载=Y/Y/Y
    [PASS] 下载确因负载过载被暂停过（采样到 throttled=true）
    [PASS] 写镜像数确实堆到过背压上限：12
    [PASS] 触发原因覆盖「写镜像数达上限」
    [PASS] 过冲不超过 1 个卷镜像（背压判定在「下一个文件之前」）
  [STEP] 背压原因观测：写镜像过载=有，原始文件积压过载=有（阈值=1048576 字节 = 卷镜像容量 10%）
  [STEP] 等待背压解除（依赖刻录完成释放写镜像后的唤醒，未唤醒即睡死）
    [PASS] 背压解除（快照 throttled=false，写镜像占用回落） —— 已在 0ms 内满足
    [PASS] 背压已解除且下载线程被唤醒（最近一次快照 write_images=6）
[CASE-END] B2 result=PASS checks=6 failed=0
```

`A5`（LRU 淘汰）逐项记录"淘汰前快照 → 读第 50 卷 → 淘汰后快照 → victim 重载"的观测事实：

```text
[CASE] A5  LRU 淘汰与重载（image_dir 满载后读入新卷）
  [STEP] 读前快照 .../archive/image/ => [volume_10.vimg(10176826) … volume_9.vimg(10176818) ]；.../archive/disc_sim/ => [disc_1.vdisc(101791744) … disc_5.vdisc(101791744) ]
    [PASS] 读满后 image_dir 镜像数量等于 capacity_in_images（actual=14 expected=14）
    [PASS] 待读入的卷已从 image_dir 释放（volume_50，读回必须走 CD_READ）
  [STEP] 读 volume_50 inode=1499（image_dir 已满，预期淘汰一个 READ 受害者）
    [PASS] 满载后读入新卷仍可读通（字节与语料一致）
  [STEP] 读后快照 image=[volume_10.vimg,…,volume_50.vimg,…] disc_sim=[disc_1.vdisc,…,disc_5.vdisc]
    [PASS] 淘汰后 image_dir 镜像数量保持不变（actual=14 expected=14）
    [PASS] 淘汰只删缓存副本，disc_sim 中的 vdisc 不变
    [PASS] 刚加载的卷保留在 image_dir（volume_50）
    [PASS] 恰好淘汰一个 READ 受害者（原 image_dir 中的镜像）（actual=1 expected=1）
    [PASS] 被淘汰的不是刚加载的卷（victim=5，新卷=50）
  [STEP] 被淘汰的镜像 volume_5，重新读以验证可从 vdisc 重新提取
    [PASS] 被淘汰卷重载后可读通（字节与语料一致）
    [PASS] 重载后被淘汰卷回到 image_dir（volume_5）
    [PASS] 重载后 vdisc 仍持久保留（actual=5 expected=5）
[CASE-END] A5 result=PASS checks=11 failed=0
```

`C1`（节点上报，真 server 子进程）的关键观测事实：

```text
[CASE] C1  调度器上报链路（注册 / 心跳+设备清单 / 光盘库状态）
  [STEP] 写临时配置（SCHEDULER_ADDR=127.0.0.1:37931）并启动真 server 子进程（--server-bin=.../optical_node_server）
    [PASS] RegisterManagedNode 到达假 scheduler —— 已在 20ms 内满足
    [PASS] ReportHeartbeat 到达假 scheduler —— 已在 0ms 内满足
    [PASS] ReportOpticalLibraryStatus 到达假 scheduler —— 已在 0ms 内满足
    [PASS] RegisterManagedNode 恰好发送一次（成功后不再重复注册）（actual=1 expected=1）
    [PASS] 注册 kind=MANAGED_KIND_OPTICAL_LIBRARY
    [PASS] 注册携带 3 个设备组（hdd-cache / disc / optical-drive）（actual=3 expected=3）
    [PASS] 心跳身份正确（node_id / node_type=NODE_OPTICAL）
    [PASS] 心跳含 DRIVE_COUNT 个 optical-drive 设备条目（actual=20 expected=20）
    [PASS] 库状态 local_disc_count = TOTAL_DISC_COUNT（actual=1000 expected=1000）
    [PASS] 空库 blank_disc_count = local（全部空白盘）（actual=1000 expected=1000）
    [PASS] 无数据面 I/O 时不上报 ReportNodeMetrics（actual=0 expected=0）
  [STEP] 对子进程发 SIGINT，验证优雅退出
    [PASS] SIGINT 后子进程正常退出（exit=0）
[CASE-END] C1 result=PASS checks=35 failed=0
```

实测汇总（机器负载不同会有差异；总耗时指 C++ 二进制内部计时，不含语料生成）：

| 用例 | 测试点 | 检查 | 失败 | 总耗时 |
| --- | --- | --- | --- | --- |
| `optical_node`（56 卷） | 90（A0 + A1×56 + A2 + A7 + A8 + A3×12 + A4×12 + A9 + A5 + A6 + B1 + B2 + C1） | 569 | 0 | 509s |

> `C1`（节点上报，末尾以真 server 子进程覆盖）未提供 `--server-bin` 时会被跳过（打印 `[SKIP] C1`），
> 此时测试点数为 89、检查数为 534。ctest 已自动传入该参数。

> 实测覆盖了「`disc_<id>_meta` 刻录完成后删除」：A7 断言 `node_discs_meta` 追加块数 == 汇报盘数（5）且
> `meta/` 无残留 `disc_<id>_meta`，A8 断言重启后仍无残留、索引块数与字节数均不变。

- `[CASE]` / `测试目标` / `期望` / `验证方法`：说明这一项为什么测、判定条件、靠什么观测。
- `[STEP]`：流程步骤（下发批次、轮询等待、目录快照等），不计入检查数。
- `[PASS]` / `[FAIL]`：单项检查结论；失败时额外附「失败详情」，超时等待还会打印目录快照。
- `[CASE-END]`：该测试点的检查数与失败数；`[FATAL]` 表示前置条件不满足、测试提前终止，
  此时汇总表对应行标记为 `ABORT`。
- 日志末尾的「测试点汇总」给出编号 / 结果 / 检查数 / 失败数、合计，以及各测试点的目标清单。

## 七、已知缺口

- `log/task_done_yyyymmdd.log` 在测试期内**不会生成**：清理线程间隔为 600s（`cleanup_interval_`），
  测试只会断言 `log/` 目录存在，并打印一条 `[TODO]` 说明。
- `DISC_BURN` 任务的终态没有查询 RPC，只能靠 `[ReportImagesBurnedToDisc]`、`meta/node_discs_meta`
  的追加块数、`disc_sim/` 与 `image/` 的文件系统事实间接断言（`meta/disc_<id>_meta` 在完成后即被删除，
  不能再当见证）。
- 两次 MDS 上报（`ReportFilesPackedToImage` / `ReportImagesBurnedToDisc`）尚未实现，
  当前以控制台输出替代，测试靠抓 stdout 校验。
- `volume_id`（= MDS 的 image_id）与 `disk_id`（= MDS 的 disc_id）由节点本地递增生成，
  MDS 的 `AllocateAvailableImageId` / `AllocateAvailableDiscId` 未实现。
- 重启恢复只覆盖**正常关机**（`Shutdown()`），且只由信号路径触发：`Shutdown()` 未通过 brpc 暴露，
  进程内 `Shutdown()` → 再 `Run()` 也不支持（队列 `Close()` 后不重开）；测试是「同进程内重建一个新实例」，
  并非复用同一实例再 `Run()`。
  未被持久化的运行态：`WriteObject` 分片上传中间态（客户端需重传）、`cd_manager_sim` 的活跃任务
  （刻录进度丢弃，重启后从零重演）、下载轮次配额，以及**通知会话表与读会话表**（进程内易失态）。
  故重启后需重新 `SubscribeReadEvents`；恢复任务的通知目标 `session_id` 不落盘，不会补发 `ReadReady`
  （A8 以「轮询 `AcquireReadSession` 得到 `OPTICAL_OK`」作为就绪判据）。`kill -9` / 掉电没有快照，
  退化为「无快照启动」。
- 新读接口语义尚未完全覆盖：`PrepareRead` 的可选 `file_version` 只走了「未提供」分支
  （测试不传版本，`has_file_version=false`），未验证携带版本时 `ReadReady` 的回传；
  也未断言「同一 `request_id` 恰好收到一次」`ReadReady`（重复通知未验证）。
- 节点上报测试点（`C1`，并入综合用例）以真 server 子进程 + 假 scheduler 覆盖注册/心跳/光盘库状态，但**不覆盖**
  数据面 I/O 指标上报（`ReportNodeMetrics`）：子进程无归档/读 I/O，`ReportCollectedMetrics` 因无活动跳过，
  用例只断言「无 I/O 时不上报」；指标内容仍由综合用例侧的两个采集器记账间接保证。
  同样未覆盖注册的传输层失败重试/服务端拒绝分支、心跳的 `SCHED_ALREADY_EXISTS` 幂等路径
  （假 scheduler 恒返回 `SCHED_OK`），以及 `WarnIfProfileMismatch` 的「不一致告警」分支
  （假 scheduler 回显 `spec`，走一致分支）。
- `prepare_archive_corpus.py` 的 `--volumes` 默认值仍是 26，与测试用的 56 不一致（驱动脚本每次都显式传参，
  只有手工调用该脚本时才需注意）。

## 八、排障

| 现象 | 处理 |
| --- | --- |
| A3/A4/A9 报「未在超时内收到 ReadReady」 | 先确认 A0 的 `SubscribeReadEvents` 返回了 >0 的 `session_id`；再确认 `PrepareRead` 用的就是该 `session_id`、等待的 `request_id` 与提交的一致；节点侧确认准备完成回调已在 `Run()` 之前注册（否则后台线程启动后回调缺失会丢通知） |
| 脚本报 `No such file or directory: '<driver>'` | `--driver` 给了相对路径；改用绝对路径，或用 ctest 运行 |
| 跑满 TIMEOUT | 机器负载高导致 CD 仿真叠加；确认串行执行（不要 `ctest -j` 并发），并留足磁盘（语料 + 5 张 vdisc + `image/` 峰值约 1.5GB） |
| 断言失败 | 工作目录已自动保留：看 `[ReportFilesPackedToImage]` / `[ReportImagesBurnedToDisc]` 上报行、`GetArchiveStatusDetail()` 输出（含 `backpressure=(throttled=… write_images=x/y input_pending=a/b)` 快照）与 `image/`、`disc_sim/`、`meta/`、`log/` 目录快照 |
| 写阶段出现 `VOLUME_FULL_NO_READABLE` | 档位不变式被破坏（背压上限 ≥ `capacity_in_images`）；按「单盘镜像数 < 背压上限 < 容量」调整 |
| 下载停住不动 | 看 `backpressure` 快照：`throttled=true` 且 `write_images` 达上限 → 等刻录释放；`throttled=false` 则是本轮配额节拍（等压缩进度），并非卡死 |
| A8 报「快照未被消费」或「状态原因不含 runtime state restored」 | 检查 `archive/log/`：正常应是 `runtime_state_snapshot` 已改名为 `.loaded`；若残留 `runtime_state_snapshot`，说明加载失败（`GetArchiveStatusDetail()` 的 sidecar 段会给出失败原因） |
| A7 报「meta/ 有残留 disc_<id>_meta」 | 该盘已封印但未完成「刻录 + 索引」：看 `[ReportImagesBurnedToDisc]` 是否缺该 disc_id、`meta/node_discs_meta` 块数是否少了、`disc_sim/` 是否有对应 `.vdisc`（或半截 `.tmp`） |
| 想复盘某次运行 | 日志在 `tests/optical_node/results/optical_node-<时间戳>/output.log`；`ctest` 方式下按运行时间挑最新目录 |
| `expected_pack_plan.tsv` 与实际上报的 inode 集合不符 | python 与 libz 版本差异导致压缩率漂移；用 `--selftest` 对比实测 ratio（目标 0.97，实测约 0.9704） |