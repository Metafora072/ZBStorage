# optical_node 集成测试

进程内起真光节点服务 + 假 real_node / 假 scheduler，通过 brpc 走完
**归档下发（四种批次形态）→ 下载侧背压节流 → 256KiB 分片下载重组 → 压缩 → 卷镜像封装 → 打包汇报
→ 按容量积攒 → 封印为光盘（vdisc）→ CD_BURN 刻录 → 逐个释放写镜像 → 刻录汇报
→ 数据面读 / CD_READ 重载 → 缓存命中 → LRU 淘汰与重载（evict 档位）**
全链路，断言只依赖文件系统产物、RPC 返回码与控制台汇报行，不使用固定 sleep。

## 一、前置条件

- 已配置好构建环境（brpc / protobuf / gflags / zlib），且 `include(CTest)` 生效（顶层 `CMakeLists.txt` 已具备）。
- 一份 jpg 语料目录，默认 `~/WR/testWrite`（实测 13 张、合计 30MB）。
  jpg 是**难压缩**数据，用来避免高压缩率导致镜像填不满；脚本保证同一归档文件内不复用同一张 jpg。
  可用 `--input-dir` 或环境变量 `ZBSTORAGE_OPTICAL_CORPUS` 指定其他目录。

## 二、测试规模（固定参数）

| 参数 | 值 |
| --- | --- |
| `volume_size_bytes` | 10 MiB（10485760） |
| `size_threshold` | 0.9（打包触发线 9437184 字节） |
| `disc_capacity_bytes` | 100 MiB（104857600） |
| `standard_images_per_disc` | 10（100 MiB 盘恰好容纳 10 个 ≈10.1 MiB 的镜像） |
| `disc_block_size_bytes` | 2048 |
| 每卷文件数 / 单文件大小 | 10 / 1 MiB（实测压缩率 0.9837，10 个文件恰好越过触发线） |
| 对象分片大小 | 256 KiB（4 片/文件，覆盖下载侧多分片重组） |
| `available_volume_id_count` | 5 |
| `capacity_in_images` / `max_write_images` | normal 档（smoke / full）= 100 / 30；evict 档 = 14 / 12 |
| inode 起始号 | 1000（smoke 1000~1139、evict 1000~1259、full 1000~1559） |
| 数据面读分片 | 4 MiB |

容量约束的不变式：**单盘镜像数(10) < `MAX_WRITE_IMAGES` < `CAPACITY_IN_IMAGES`**。
下限侧违反会让节点永远攒不满一张盘（不封印 → 不释放 → 下载活锁）；上限侧违反会让背压来不及生效、
先撞上 `VOLUME_FULL_NO_READABLE`。`A0` 会断言这条不变式。

三档场景（卷数 > 单盘镜像数才会封印出第一张盘）：

| 场景 | 卷数 | 档位（容量 / 写镜像上限） | 覆盖内容 | 墙钟 |
| --- | --- | --- | --- | --- |
| `smoke` | 14 | normal（100 / 30） | 全链路跑通：四种批次形态、打包/刻录汇报、封盘刻录、逐个释放、CD_READ 重载与缓存命中 | 约 1 分钟 |
| `full` | 56 | normal（100 / 30） | smoke 全部 + 5 张光盘共存与尾盘、批次形态覆盖统计、下载背压的触发与恢复 | 约 7 分钟 |
| `evict` | 26 | evict（14 / 12） | smoke 全部 + 小容量档位下的 `image/` 满载、LRU 淘汰与被淘汰卷重载读通 | 约 7 分钟 |

墙钟几乎全部来自光盘库仿真的固定时长（装盘 12s、退盘 3s、攫取 2s、读 0.5s+100MB/s、刻录 1s+20MB/s），
产线未提供测试旋钮，**不可压缩**；单次 CD_READ 约 20~22s，故 12 次读回就占 4 分钟以上。

## 三、构建

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j8 --target optical_node_archive_integration_test
```

## 四、运行

### 方式 A：ctest（推荐）

```bash
# smoke
ctest --test-dir build -R optical_node_archive_smoke --output-on-failure

# full
ctest --test-dir build -R optical_node_archive_full --output-on-failure

# evict
ctest --test-dir build -R optical_node_archive_evict --output-on-failure

# 三档一起串行跑（务必串行，避免多实例叠加 CD 墙钟与磁盘）
ctest --test-dir build -L optical_node -j1 --output-on-failure
```

三个用例的注册参数：

| 用例 | TIMEOUT | SKIP_RETURN_CODE | LABELS |
| --- | --- | --- | --- |
| `optical_node_archive_smoke` | 300s | 未设置 | `optical_node;slow` |
| `optical_node_archive_full` | 1800s | 77 | `optical_node;slow` |
| `optical_node_archive_evict` | 1500s | 77 | `optical_node;slow` |

### 方式 B：python 驱动脚本（备语料 → 跑二进制 → 汇总）

```bash
python3 tests/optical_node/run_optical_node_archive_test.py \
  --driver "$PWD/build/optical_node_archive_integration_test" \
  --scenario smoke            # 或 full / evict
```

常用可选项：

| 参数 | 说明 |
| --- | --- |
| `--driver` | C++ 测试二进制路径，**必须是绝对路径**（脚本会把工作目录切到工作目录下再执行） |
| `--scenario` | `smoke` / `full` / `evict`（决定卷数与 C++ 侧档位） |
| `--work-dir` | 工作目录，默认 `tests/optical_node/results/<scenario>-<时间戳>` |
| `--input-dir` | jpg 语料来源目录，默认取 `ZBSTORAGE_OPTICAL_CORPUS`，再缺省 `~/WR/testWrite` |
| `--keep` | 成功时也保留 `corpus/` 与 `archive/`（默认成功后只保留 `output.log`，失败则全部保留） |

### 方式 C：直接跑二进制（需自备语料）

```bash
python3 tests/optical_node/prepare_archive_corpus.py \
  --input-dir ~/WR/testWrite \
  --out-dir tests/optical_node/results/corpus-smoke \
  --volumes 14

./build/optical_node_archive_integration_test \
  --corpus tests/optical_node/results/corpus-smoke \
  --work-dir tests/optical_node/results/manual-smoke \
  --scenario smoke
```

语料脚本参数：`--volumes`（卷数）、`--files-per-volume`（默认 10）、`--file-size-mb`（默认 1）、
`--object-unit-kib`（默认 256）、`--inode-base`（默认 1000）、`--seed`（默认 20260925，确定性洗牌）。
加 `--selftest` 只打印实测压缩率与预测打包分组、不落盘：

```bash
python3 tests/optical_node/prepare_archive_corpus.py --selftest
```

语料参数必须与 C++ 侧固定常量一致（卷 10 MiB、阈值 0.9、每卷 10 个 1 MiB 文件），
`check_plan` 会在计划为 `volumes × files_per_volume` 之外的形态时直接报「参数已漂移」退出。

## 五、产物与结果查看

工作目录布局（`<work>` 默认为 `tests/optical_node/results/<scenario>-<时间戳>/`）：

```text
<work>/
├── output.log                    # 本次运行的完整日志（按测试点记录目标/期望/方法/逐项检查结论）
├── corpus/                       # 脚本生成的语料（模拟 real_node 磁盘）
│   ├── manifest.tsv              # inode_id / file_size / object_unit_size / object_count / node_id / disk_id / volume_index / object_dir
│   ├── expected_pack_plan.tsv    # volume_index / count / inode_ids —— 由 zlib9 精确预测的打包分组，断言真值
│   ├── corpus_meta.txt           # 压缩率与各尺寸参数
│   └── real-1/disk0/obj-<inode>-<index>.dat   # 每个文件 4 个 256KiB 分片
└── archive/                      # 光节点工作目录
    ├── input/  temp/  write_buffer/   # 归档中间文件（最终应为空）
    ├── image/                    # 卷镜像平铺（容量 100，evict 档位 14）：volume_<id>.vimg ≈10.1MiB
    ├── disc_sim/                 # 光盘库模拟区（已刻录光盘本体）：disc_<id>.vdisc ≈100MiB
    ├── read/                     # 数据面读产物（读完应被清理）
    ├── meta/                     # 光盘元数据：pending_disc_meta、disc_<disk_id>_meta、node_discs_meta
    └── log/                      # 任务日志目录
```

成功时的标志性输出：

- 每卷一条 `[node] [ReportFilesPackedToImage] image_id=<id> count=10 inode_ids=...`（inode 集合应与 `expected_pack_plan.tsv` 对应行一致）。
- 每张盘一条 `[node] [ReportImagesBurnedToDisc] disc_id=<id> count=10 image_ids=...`（该盘刻录完成、写镜像已逐个释放）。
- 末尾 `[PASS] optical_node 归档集成测试通过（scenario=...）`，退出码 0。
- `archive/disc_sim/disc_<id>.vdisc` 约 100MiB；`input/`、`temp/` 最终为空。

结果目录按 `tests/README.md` 约定落在各模块的 `results/` 下（该目录已被 Git 忽略）。产物保留策略：

| 情况 | 保留内容 |
| --- | --- |
| 成功（默认） | 只保留 `output.log`，删除 `corpus/` 与 `archive/`（两者合计实测：smoke 约 0.5GB、evict 约 1GB、full 约 1.5GB） |
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
| `A0` | 环境与配置就绪 | 被测光节点与全部仿真对手方、语料分组真值、档位参数可信，后续断言才有前提 | 分组非空、档位满足「单盘镜像数 < 背压上限 < 容量」、`IsArchiveEngineReady` 为真、三个 brpc server 均已监听、archive 各子目录存在 |
| `A1.i` | 第 i 卷：归档下发 → 压缩封装 → 打包汇报 | 写链路端到端：按 256KiB 分片下载 → 压缩累加 → 达阈值封装 → 上报 → 写镜像留在 `image/` 等刻录释放；批次切分（形态写在标题里）不应改变卷分组 | 汇报 `count` 与 inode 集合 == zlib9 预测分组、`volume_id` 全局唯一、`image/` 出现该写镜像且体积 ∈ (0, 10MiB] |
| `A2` | 下载完整性与多分片重组 | 节点没有漏读或重复读 real_node 的对象，且逐片取回、按绝对偏移重组 | `ResolveFileRead` 次数 == 文件数、`ReadObject` 次数 == 文件数 × 4、分片大小均为 256KiB、每文件 4 片 |
| `A3.i` | 第 i 卷：数据面读 + CD_READ 重载 | 镜像已刻录（不在 `image/`）时读请求触发 CD_READ，按 `node_discs_meta` 记录的偏移从 vdisc 复制回 `image/` 并读通，末片后任务进 FINISH | 读回字节与语料逐字节一致、镜像回到 `image/`、vdisc 数量不变（只复制不搬移）、`ReadObjectByInodeId` 返回 `MDS_NOT_FOUND`、`image/` 镜像数 ≤ 容量 |
| `A4.i` | 第 i 卷：缓存命中 | 同卷第二次读不重复走光盘装载（省掉一次 20s+ 的 CD_READ） | 耗时 < 3000ms、`image/` 与 `disc_sim/` 快照前后完全不变、数据仍与语料一致 |
| `A5` | LRU 淘汰与重载（**仅 evict 档位**） | `image/` 满 `capacity_in_images` 后读新卷应换出一个 READ 镜像，且被换出的卷仍可重载读通——淘汰只影响缓存层级，不影响数据可用性 | 满载后读新卷仍能读通、`image/` 恒为 14、`disc_sim/` 不变、刚加载的卷保留、victim 恰为读前读后差集且 ≠ 刚加载的卷、victim 重载读通并回到 `image/` |
| `A6` | 归档中间态清理 | 流程结束不残留中间产物 | `input/` 无 `.archive`、`log/` 目录存在 |
| `A7` | 光盘封印与刻录（跨卷汇总） | 镜像按容量积攒，装不下下一个时才把当前集合封印成一张光盘（写 vdisc + 提交 `CD_BURN`），刻录完成后逐个释放 `image/` 中的写镜像并上报；未装满的尾盘继续留在 pending | 写阶段 `image/` 峰值 < 容量（背压生效、未触发 `VOLUME_FULL`）、每盘 `count` == `image_ids` 数量、已封印盘数 == 汇报盘数、vdisc 存在且 ≤ 盘容量、`disc_sim` vdisc 数 == 已刻录盘数、已刻录镜像恰为打包顺序的前缀、尾盘镜像仍在 `image/` |
| `B1` | 批次形态覆盖（下发切分不改变卷分组） | 批次边界（单文件小批次 / 跨卷边界批次 / 大小混合批次）不应改变卷分组、不应遗漏或重复文件；顺带覆盖「多个 MDS 批次累计成一轮下载」（一轮配额 = 卷镜像容量） | 四种形态均出现、单文件批次 ≥10、跨卷边界批次 ≥2、第 1 卷由 10 个单文件批次累计而成、汇报数 == 卷数、全部 inode 无遗漏无重复 |
| `B2` | 背压触发与恢复（**仅 full / evict 档位**） | 写镜像数达 `MAX_WRITE_IMAGES` 时暂停下载、不把 `image/` 堆到硬上限；刻录完成释放写镜像后应唤醒被阻塞的下载线程（不能睡死） | 采样到 `throttled=true`、写镜像数确实堆到上限、过冲 ≤1 个镜像、触发原因覆盖「写镜像达上限」、随后采样到 `throttled=false`（写镜像占用回落） |

日志的构造方式（每项检查都记录实际值，便于回溯时序特征）。以下是实跑日志的原文摘录
（仅把绝对路径缩写成 `.../archive/…`、把过长的文件清单用 `…` 折叠）：

```text
[CASE] A3.1  第 1/12 卷：数据面读 + CD_READ 重载
  [STEP] 读 volume_1 inode=1009（镜像在光盘库，预期触发一次 CD_READ 装载）
    [PASS] 读任务就绪（inode=1009，镜像已从光盘库装载完成） —— 已在 20254ms 内满足
    [PASS] 读回数据与语料逐字节一致（inode=1009，共 1048576 字节，耗时 20254ms）
    [PASS] 末片读完后任务进入 FINISH（ReadObjectByInodeId 返回 MDS_NOT_FOUND，inode=1009）
    [PASS] CD_READ 后镜像回到 image_dir（volume_1）
    [PASS] 光盘文件持久保留（读回只复制区间，不搬移/删除 vdisc）（actual=5 expected=5）
    [PASS] image_dir 镜像数不超过容量上限（当前 7 / 100）
[CASE-END] A3.1 result=PASS checks=6 failed=0
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
  [STEP] 写阶段背压采样：采样 67 次；最近一次[throttled=false write_images=26/30 input_pending=0/1048576]；
         峰值 write_images=30/30；峰值 input_pending=1048576 字节；曾出现 throttled/写镜像过载/原始积压过载=Y/Y/Y
    [PASS] 下载确因负载过载被暂停过（采样到 throttled=true）
    [PASS] 写镜像数确实堆到过背压上限：30
    [PASS] 触发原因覆盖「写镜像数达上限」
    [PASS] 过冲不超过 1 个卷镜像（背压判定在「下一个文件之前」）
  [STEP] 背压原因观测：写镜像过载=有，原始文件积压过载=有（阈值=1048576 字节 = 卷镜像容量 10%）
  [STEP] 等待背压解除（依赖刻录完成释放写镜像后的唤醒，未唤醒即睡死）
    [PASS] 背压解除（快照 throttled=false，写镜像占用回落） —— 已在 0ms 内满足
    [PASS] 背压已解除且下载线程被唤醒（最近一次快照 write_images=26）
[CASE-END] B2 result=PASS checks=6 failed=0
```

`A5`（LRU 淘汰，evict 档位）逐项记录"淘汰前快照 → 读第 20 卷 → 淘汰后快照 → victim 重载"的观测事实：

```text
[CASE] A5  LRU 淘汰与重载（image_dir 满载后读入新卷）
  [STEP] 读前快照 .../archive/image/ => [volume_10.vimg(10269000) … volume_9.vimg(10288600) ]；.../disc_sim/ => [disc_1.vdisc(103112704) disc_2.vdisc(103127040) ]
    [PASS] 读满后 image_dir 镜像数量等于 capacity_in_images（actual=14 expected=14）
    [PASS] 待读入的卷已从 image_dir 释放（volume_20，读回必须走 CD_READ）
  [STEP] 读 volume_20 inode=1199（image_dir 已满，预期淘汰一个 READ 受害者）
    [PASS] 读任务就绪（inode=1199，镜像已从光盘库装载完成） —— 已在 20587ms 内满足
    [PASS] 满载后读入新卷仍可读通（字节与语料一致）
  [STEP] 读后快照 image=[volume_10.vimg,volume_11.vimg,…,volume_20.vimg,…] disc_sim=[disc_1.vdisc,disc_2.vdisc]
    [PASS] 淘汰后 image_dir 镜像数量保持不变（actual=14 expected=14）
    [PASS] 淘汰只删缓存副本，disc_sim 中的 vdisc 不变
    [PASS] 刚加载的卷保留在 image_dir（volume_20）
    [PASS] 恰好淘汰一个 READ 受害者（原 image_dir 中的镜像）（actual=1 expected=1）
    [PASS] 被淘汰的不是刚加载的卷（victim=5，新卷=20）
  [STEP] 被淘汰的镜像 volume_5，重新读以验证可从 vdisc 重新提取
    [PASS] 读任务就绪（inode=1049，镜像已从光盘库装载完成） —— 已在 22341ms 内满足
    [PASS] 被淘汰卷重载后可读通（字节与语料一致）
    [PASS] 重载后被淘汰卷回到 image_dir（volume_5）
    [PASS] 重载后 vdisc 仍持久保留（actual=2 expected=2）
[CASE-END] A5 result=PASS checks=13 failed=0
```

实测汇总（供横向对比，机器负载不同会有差异；总耗时指 C++ 二进制内部计时，不含语料生成）：

| 场景 | 测试点 | 检查 | 失败 | 总耗时 |
| --- | --- | --- | --- | --- |
| `smoke` | 21（A0 + A1×14 + A2/A3/A4/A6/A7/B1） | 122 | 0 | 62s |
| `evict` | 57（A0 + A1×26 + A2/A3×12/A4×12/A5/A6/A7/B1/B2） | 325 | 0 | 415s |
| `full` | 86（A0 + A1×56 + A2/A3×12/A4×12/A6/A7/B1/B2） | 498 | 0 | 388s |

- `[CASE]` / `测试目标` / `期望` / `验证方法`：说明这一项为什么测、判定条件、靠什么观测。
- `[STEP]`：流程步骤（下发批次、轮询等待、目录快照等），不计入检查数。
- `[PASS]` / `[FAIL]`：单项检查结论；失败时额外附「失败详情」，超时等待还会打印目录快照。
- `[CASE-END]`：该测试点的检查数与失败数；`[FATAL]` 表示前置条件不满足、测试提前终止，
  此时汇总表对应行标记为 `ABORT`。
- 日志末尾的「测试点汇总」给出编号 / 结果 / 检查数 / 失败数、合计，以及各测试点的目标清单。

## 七、已知缺口

- `log/task_done_yyyymmdd.log` 在测试期内**不会生成**：清理线程间隔为 600s（`cleanup_interval_`），
  测试只会断言 `log/` 目录存在，并打印一条 `[TODO]` 说明。
- `CD_BURN` 任务的终态没有查询 RPC，只能靠 `meta/disc_<id>_meta`、`[ReportImagesBurnedToDisc]`
  与 `disc_sim/` 的文件系统事实间接断言。
- 两次 MDS 上报（`ReportFilesPackedToImage` / `ReportImagesBurnedToDisc`）尚未实现，
  当前以控制台输出替代，测试靠抓 stdout 校验。
- `volume_id`（= MDS 的 image_id）与 `disk_id`（= MDS 的 disc_id）由节点本地递增生成，
  MDS 的 `AllocateAvailableImageId` / `AllocateAvailableDiscId` 未实现。
- 运行态不持久化：任务表与四个队列都在内存中，重启只从 `meta/node_discs_meta` 重建
  「镜像 → 光盘」定位索引，未完成的归档 / 刻录 / 读任务不恢复。测试不覆盖重启恢复。
- `smoke` 用例未设置 `SKIP_RETURN_CODE`：语料目录缺失时会直接失败，而不是像 `full` / `evict` 那样 Skipped。

## 八、排障

| 现象 | 处理 |
| --- | --- |
| ctest 直接显示 `Skipped` | 语料目录不存在（退出码 77，仅 `full` / `evict` 注册了该跳过码）。用 `--input-dir` 或 `ZBSTORAGE_OPTICAL_CORPUS` 指定 jpg 目录 |
| 脚本报 `No such file or directory: '<driver>'` | `--driver` 给了相对路径；改用绝对路径，或用 ctest 运行 |
| 跑满 TIMEOUT | 机器负载高导致 CD 仿真叠加；确认串行执行（不要 `ctest -j` 并发这几档），并留足磁盘（full 峰值约 1.5GB） |
| 断言失败 | 工作目录已自动保留：看 `[ReportFilesPackedToImage]` / `[ReportImagesBurnedToDisc]` 上报行、`GetArchiveStatusDetail()` 输出（含 `backpressure=(throttled=… write_images=x/y input_pending=a/b)` 快照）与 `image/`、`disc_sim/`、`meta/` 目录快照 |
| 写阶段出现 `VOLUME_FULL_NO_READABLE` | 档位不变式被破坏（背压上限 ≥ `capacity_in_images`）；按「单盘镜像数 < 背压上限 < 容量」调整 |
| 下载停住不动 | 看 `backpressure` 快照：`throttled=true` 且 `write_images` 达上限 → 等刻录释放；`throttled=false` 则是本轮配额节拍（等压缩进度），并非卡死 |
| 想复盘某次运行 | 日志在 `tests/optical_node/results/<scenario>-<时间戳>/output.log`；`ctest` 方式下按运行时间挑最新目录 |
| `expected_pack_plan.tsv` 与实际上报的 inode 集合不符 | python 与 libz 版本差异导致压缩率漂移；用 `--selftest` 对比实测 ratio（正常约 0.9837，单文件约 0.9785） |