# optical_node 集成测试

进程内起真光节点服务 + 假 real_node / 假 scheduler，通过 brpc 走完
**归档下载 → 压缩 → 卷镜像封装 → 打包汇报 → CD_BURN → 释放到光盘库 → 数据面读 / CD_READ 重载 → LRU 淘汰**
全链路，断言只依赖文件系统产物、RPC 返回码与控制台汇报行，不使用固定 sleep。

## 一、前置条件

- 已配置好构建环境（brpc / protobuf / gflags / zlib），且 `include(CTest)` 生效（顶层 `CMakeLists.txt` 已具备）。
- 一份 jpg 语料目录，默认 `~/WR/testWrite`（实测 13 张、合计 30MB）。
  jpg 是**难压缩**数据，用来避免高压缩率导致镜像填不满；脚本保证同一归档文件内不复用同一张 jpg。
  可用 `--input-dir` 或环境变量 `ZBSTORAGE_OPTICAL_CORPUS` 指定其他目录。

## 二、测试规模（固定参数）

| 参数 | 值 |
| --- | --- |
| `volume_size_bytes` | 100 MiB（104857600） |
| `size_threshold` | 0.9（打包触发线 94371840 字节） |
| `capacity_in_images` | 5 |
| `available_volume_id_count` | 5 |
| 每卷文件数 / 单文件大小 | 12 / 8 MiB |
| 对象分片大小 | 1 MiB（8 片/文件） |
| inode 起始号 | 1000（smoke 用 1000~1011，full 用 1000~1071） |
| 数据面读分片 | 4 MiB |

两档场景：

| 场景 | 卷数 | 覆盖内容 | 墙钟 |
| --- | --- | --- | --- |
| `smoke` | 1 | 归档→压缩→封装→打包汇报→刻录释放→读（含一次 CD_READ 重载与缓存命中） | 约 1 分钟 |
| `full` | 6 | smoke 全部 + `image_dir` 容量 5 的 LRU 淘汰、被淘汰卷重载读通 | 约 6~10 分钟 |

`full` 的墙钟来自光盘库仿真的固定时长（装盘 12s、退盘 3s、攫取 2s、读 0.5s+100MB/s、刻录 1s+20MB/s），
产线未提供测试旋钮，**不可压缩**。

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

# 两档一起串行跑（full 务必串行，避免多实例叠加 CD 墙钟与磁盘）
ctest --test-dir build -L optical_node -j1 --output-on-failure
```

两个用例的注册参数：`optical_node_archive_smoke` TIMEOUT 300s；`optical_node_archive_full` TIMEOUT 1500s、
`SKIP_RETURN_CODE 77`；两者 LABELS 均为 `optical_node;slow`。

### 方式 B：python 驱动脚本（备语料 → 跑二进制 → 汇总）

```bash
python3 tests/optical_node/run_optical_node_archive_test.py \
  --driver "$PWD/build/optical_node_archive_integration_test" \
  --scenario smoke            # 或 full
```

常用可选项：

| 参数 | 说明 |
| --- | --- |
| `--driver` | C++ 测试二进制路径，**必须是绝对路径**（脚本会把工作目录切到工作目录下再执行） |
| `--scenario` | `smoke` 或 `full` |
| `--work-dir` | 工作目录，默认 `tests/optical_node/results/<scenario>-<时间戳>` |
| `--input-dir` | jpg 语料来源目录，默认取 `ZBSTORAGE_OPTICAL_CORPUS`，再缺省 `~/WR/testWrite` |
| `--keep` | 成功时也保留 `corpus/` 与 `archive/`（默认成功后只保留 `output.log`，失败则全部保留） |

### 方式 C：直接跑二进制（需自备语料）

```bash
python3 tests/optical_node/prepare_archive_corpus.py \
  --input-dir ~/WR/testWrite \
  --out-dir tests/optical_node/results/corpus-smoke \
  --volumes 1

./build/optical_node_archive_integration_test \
  --corpus tests/optical_node/results/corpus-smoke \
  --work-dir tests/optical_node/results/manual-smoke \
  --scenario smoke
```

语料脚本参数：`--volumes`（卷数）、`--files-per-volume`（默认 12）、`--file-size-mb`（默认 8）、
`--object-unit-mb`（默认 1）、`--inode-base`（默认 1000）、`--seed`（默认 20260925，确定性洗牌）。
加 `--selftest` 只打印实测压缩率与预测打包分组、不落盘：

```bash
python3 tests/optical_node/prepare_archive_corpus.py --selftest
```

## 五、产物与结果查看

工作目录布局（`<work>` 默认为 `tests/optical_node/results/<scenario>-<时间戳>/`）：

```text
<work>/
├── output.log                    # 本次运行的完整日志（按测试点记录目标/期望/方法/逐项检查结论）
├── corpus/                       # 脚本生成的语料（模拟 real_node 磁盘）
│   ├── manifest.tsv              # inode_id / file_size / object_unit_size / object_count / node_id / disk_id / volume_index / object_dir
│   ├── expected_pack_plan.tsv    # volume_index / count / inode_ids —— 由 zlib9 精确预测的打包分组，断言真值
│   ├── corpus_meta.txt           # 压缩率与各尺寸参数
│   └── real-1/disk0/obj-<inode>-<index>.dat
└── archive/                      # 光节点工作目录
    ├── input/  temp/             # 归档中间文件（最终应为空）
    ├── image/                    # image_dir（WRITE/READ 镜像，容量 5）
    ├── disc_sim/                 # 光盘库模拟区（已刻录的卷镜像）
    ├── read/                     # 数据面读产物（读完应被清理）
    ├── meta/  log/               # 元数据与日志
    └── ...
```

成功时的标志性输出：

- 每卷一条 `[node] [ReportFilesPackedToImage] image_id=<id> count=12 inode_ids=...`（inode 集合应与 `expected_pack_plan.tsv` 对应行一致）。
- 末尾 `[PASS] optical_node 归档集成测试通过（scenario=...）`，退出码 0。
- `archive/disc_sim/volume_<id>.vimg` 约 100MB；`input/`、`temp/` 最终为空。

结果目录按 `tests/README.md` 约定落在各模块的 `results/` 下（该目录已被 Git 忽略）。产物保留策略：

| 情况 | 保留内容 |
| --- | --- |
| 成功（默认） | 只保留 `output.log`，删除 `corpus/` 与 `archive/`（两者合计实测：smoke 约 192MB、full 约 1.2GB） |
| 失败 | 全部保留，便于按日志与目录快照定位问题 |
| `--keep` | 无论成败全部保留 |

`output.log` 由驱动脚本把测试进程的 stdout/stderr 合并落盘（同时仍在终端流式输出），
逐测试点记录目标、期望、验证方法与每项检查的实际值；运行结束时会自动回显其中的「测试点汇总」区块。
直接运行二进制（方式 C）不会写该文件，需要时自行 `> out.log 2>&1`。

## 六、测试点与断言覆盖面

日志中的 `[CASE]` 编号即测试点编号：多卷场景下同一测试点按 `A1.1 … A1.6` 递增，
汇总表里每个编号单独一行（各自记录检查数与失败数），目标清单则按种类只列一次。

| 编号 | 测试点 | 测试目标 | 主要断言 |
| --- | --- | --- | --- |
| `A0` | 环境与配置就绪 | 被测光节点与全部仿真对手方、语料分组真值、工作目录齐备，后续断言才有可信前提 | 分组非空、`IsArchiveEngineReady` 为真、三个 brpc server 均已监听、archive 各子目录存在 |
| `A1.i` | 第 i 卷：归档下发 → 压缩封装 → 打包汇报 → 刻录释放 | 写链路端到端：按分片下载 → 压缩累加 → 达阈值封装 → 上报 → CD_BURN 把镜像释放到光盘库 | 汇报 `count` 与 inode 集合 == zlib9 预测分组、`volume_id` 全局唯一、镜像仅出现在 `disc_sim/` 且 ∈ (0,100MiB]、`image/` 无残留 |
| `A2` | 下载完整性（跨卷汇总计数） | 节点没有漏读或重复读 real_node 的对象 | `ResolveFileRead` 调用次数 == 文件数、`ReadObject` 调用次数 == 对象总数 |
| `A3.i` | 第 i 卷：数据面读 + CD_READ 重载 | 镜像已刻录（不在 `image/`）时读请求触发 CD_READ 重载并读通，末片后任务进 FINISH | 读回字节与语料逐字节一致、镜像回到 `image/` 且从 `disc_sim/` 移除、`image/` 中 READ 镜像数递增、`ReadObjectByInodeId` 返回 `MDS_NOT_FOUND` |
| `A4.i` | 第 i 卷：缓存命中 | 同卷第二次读不重复走光盘装载（省掉一次 20~30s 的 CD_READ） | 耗时 < 3000ms、`image/` 与 `disc_sim/` 快照前后完全不变、数据仍与语料一致 |
| `A5` | LRU 淘汰与重载（仅 full） | `image/` 满 `capacity_in_images` 后读新卷应换出一个 READ 镜像，且被换出的卷仍可重载读通——淘汰只影响缓存层级，不影响数据可用性 | 满后读新卷仍能读通、`image/` 恒为 5、`disc_sim/` 恰新增 1 个、刚加载的卷保留、victim ≠ 刚加载的卷、victim 重载读通并回到 `image/` |
| `A6` | 归档中间态清理 | 流程结束不残留中间产物 | `input/` 无 `.archive`、`log/` 目录存在 |

日志的构造方式（每项检查都记录实际值，便于回溯时序特征）。以下是 `full` 实跑日志的原文摘录：

```text
[CASE] A3.1  第 1/5 卷：数据面读 + CD_READ 重载
    测试目标: 验证读链路在镜像已刻录（不在 image_dir）时的行为：读请求必须触发 CD_READ …
    期望: 读回的 8MiB 与语料原始 jpg 数据逐字节一致；镜像回到 image_dir 且从 disc_sim 移除；…
    验证方法: RequestAsyncReadFile 取 task_id，按 4MiB 分片轮询读并拼接后与本地语料比对；…
  [STEP] 读 volume_1 inode=1011（镜像在光盘库，预期触发一次 CD_READ 装载）
    [PASS] 读任务就绪（inode=1011，镜像已从光盘库装载完成） —— 已在 23633ms 内满足
    [PASS] 读回数据与语料逐字节一致（inode=1011，共 8388608 字节，耗时 23654ms）
    [PASS] 末片读完后任务进入 FINISH（ReadObjectByInodeId 返回 MDS_NOT_FOUND，inode=1011）
    [PASS] CD_READ 后镜像回到 image_dir（volume_1）
    [PASS] CD_READ 后 disc_sim 不再保留该镜像（volume_1）
    [PASS] image_dir 中 READ 镜像数量随已读卷数递增（actual=1 expected=1）
[CASE-END] A3.1 result=PASS checks=6 failed=0
```

`A5`（LRU 淘汰）同样逐项记录"淘汰前快照 → 读第 6 卷 → 淘汰后快照 → victim 重载"的观测事实：

```text
[CASE] A5  LRU 淘汰与重载（image_dir 满 capacity_in_images 后读新卷）
  [STEP] 读前快照 image/ => [volume_1.vimg(99873166) volume_2.vimg(99931457) … volume_5.vimg(100038731)]；
         disc_sim/ => [volume_6.vimg(99920530)]
    [PASS] 读满后 image_dir 镜像数量等于 capacity_in_images（actual=5 expected=5）
  [STEP] 读 volume_6 inode=1071（image_dir 已满，预期淘汰一个 READ 受害者）
    [PASS] 读任务就绪（inode=1071，镜像已从光盘库装载完成） —— 已在 19431ms 内满足
    [PASS] 满载后读入新卷仍可读通（字节与语料一致）
  [STEP] 读后快照 image=[volume_2,volume_3,volume_4,volume_5,volume_6] disc_sim=[volume_1]
    [PASS] 淘汰后 image_dir 镜像数量保持不变（actual=5 expected=5）
    [PASS] 淘汰后 disc_sim 恰好新增一个被换出的镜像（actual=1 expected=1）
    [PASS] 刚加载的卷保留在 image_dir（volume_6）
    [PASS] 被淘汰的不是刚加载的卷（victim=1，新卷=6）
  [STEP] 被淘汰的镜像 volume_1，重新读以验证可重载
    [PASS] 读任务就绪（inode=1011，镜像已从光盘库装载完成） —— 已在 34138ms 内满足
    [PASS] 被淘汰卷重载后可读通（字节与语料一致）
    [PASS] 重载后被淘汰卷回到 image_dir（volume_1）
[CASE-END] A5 result=PASS checks=10 failed=0
```

实测汇总（供横向对比，机器负载不同会有差异）：`smoke` 6 个测试点 / 26 项检查 / 墙钟约 1 分钟；
`full` 20 个测试点 / 116 项检查 / 墙钟约 7 分钟，失败均为 0。

- `[CASE]` / `测试目标` / `期望` / `验证方法`：说明这一项为什么测、判定条件、靠什么观测。
- `[STEP]`：流程步骤（下发批次、轮询等待、目录快照等），不计入检查数。
- `[PASS]` / `[FAIL]`：单项检查结论；失败时额外附「失败详情」，超时等待还会打印目录快照。
- `[CASE-END]`：该测试点的检查数与失败数；`[FATAL]` 表示前置条件不满足、测试提前终止，
  此时汇总表对应行标记为 `ABORT`。
- 日志末尾的「测试点汇总」给出编号 / 结果 / 检查数 / 失败数、合计，以及各测试点的目标清单。

## 七、已知缺口

- `log/task_done_yyyymmdd.log` 在测试期内**不会生成**：清理线程间隔为 600s（`cleanup_interval_`），
  测试只会断言 `log/` 目录存在，并打印一条 `[TODO]` 说明。
- `CD_BURN` 任务的终态没有查询 RPC，只能靠 `image/` → `disc_sim/` 的文件系统事实间接断言。
- 刻录汇报（`ReportImagesBurnedToDisc`）尚未实现（`disk_id` 目前是占位符），因此只断言镜像打包汇报行。

## 八、排障

| 现象 | 处理 |
| --- | --- |
| ctest 直接显示 `Skipped` | 语料目录不存在（退出码 77）。用 `--input-dir` 或 `ZBSTORAGE_OPTICAL_CORPUS` 指定 jpg 目录 |
| 脚本报 `No such file or directory: '<driver>'` | `--driver` 给了相对路径；改用绝对路径，或用 ctest 运行 |
| full 跑满 TIMEOUT | 机器负载高导致 CD 仿真叠加；确认串行执行（不要 `ctest -j` 并发这两档） |
| 断言失败 | 工作目录已自动保留：看 `[ReportFilesPackedToImage]` 上报行、`GetArchiveStatusDetail()` 输出与 `image/`、`disc_sim/` 目录快照 |
| 想复盘某次运行 | 日志在 `tests/optical_node/results/<scenario>-<时间戳>/output.log`；`ctest` 方式下按运行时间挑最新目录 |
| `expected_pack_plan.tsv` 与实际上报的 inode 集合不符 | python 与 libz 版本差异导致压缩率漂移；用 `--selftest` 对比实测 ratio（正常应约 0.992） |