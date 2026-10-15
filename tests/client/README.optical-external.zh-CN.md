# 客户端与正式光盘节点的外部模拟联调

本测试启动三个独立进程：原有 `zb_fuse_client`、原有 `optical_node_server`、新增
`optical_external_services`。不向客户端或光盘节点添加测试分支，不调用其私有实现，
不直接生成或修改光盘节点内部的镜像、光盘文件、索引和快照。

## 组成与边界

| 文件/程序 | 职责 |
| --- | --- |
| `optical_external_services.cpp` | 同一个 brpc 服务注册模拟 MDS、Scheduler 和磁盘数据服务；通过正式归档 RPC 向光盘节点下发任务 |
| `client_optical_external_test.py` | 生成原始语料、启动各进程、读取归档报告、发布元数据、通过 `zbctl` 和 POSIX 运行测试、保存结果 |
| `zb_fuse_client`、`zbctl` | 被测的现有客户端及异步准备工具 |
| `optical_node_server` | 被测的现有光盘节点，真实下载/压缩/封装/读取文件，设备动作使用其现有 `cd_manager_sim` |

模拟服务不注册 `OpticalNodeService`。客户端的准备、读取和通知始终直接连接正式光盘节点。
原始文件由外部程序生成，光盘镜像由节点通过归档流程自行生成。这是软件数据通路联调，
不代表接入实体光驱或机械光盘库。

当前节点尚未向 MDS 发出打包/刻录完成 RPC，只打印
`[ReportFilesPackedToImage]`、`[ReportImagesBurnedToDisc]`。
脚本解析这两类现有输出，取得实际的 inode→image→disc 映射后才发布模拟 MDS 元数据。
这是唯一使用日志补充业务接口的地方。文件准备就绪依赖正式 `ReadReadyEvent` 推送，
没有轮询光盘准备状态。脚本只读观察节点产物，验证冷读前镜像不在缓存、准备后镜像加载成功。

## 构建与运行

在仓库根目录执行：

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j4 --target optical_external_services optical_node_server zb_fuse_client zbctl
ctest --test-dir build -R '^client_optical_external_test$' --output-on-failure
```

直接运行可实时查看阶段进度：

```bash
python3 tests/client/client_optical_external_test.py \
  --environment build/optical_external_services \
  --optical build/optical_node_server \
  --client build/zb_fuse_client \
  --ctl build/zbctl
```

需要 `/dev/fuse`、`fusermount3` 以及挂载权限。缺少设备或工具时以 77 跳过；实际挂载失败会保留日志并报错。
脚本默认在 `tests/client/results/optical-external-<随机串>/` 创建独立目录，不覆盖历史结果。
可用 `--work-dir tests/client/results/<新目录>` 指定空目录；路径需足够短以容纳 Unix socket。
`--timeout 240` 设置归档/通知等待上限（秒），不是固定等待时长。
CTest 总超时为 900 秒。设备使用原有装盘、刻录、读取时延，测试通常需要数分钟。

## 数据与执行流程

1. 生成固定 seed 的二进制数据，保存原始文件及 SHA-256。
2. 启动外部模拟服务，它提供磁盘对象分片接口，分片大小为 256 KiB。
3. 启动正式光盘节点，Scheduler 地址指向模拟服务。
4. 模拟服务调用 `SendArchiveMetadata`。节点通过 `GetClusterView` 查询源节点，
   再通过 `ResolveFileRead`、`ReadObject` 拉取原始数据。
5. 节点按正常流程生成三卷镜像。配置为 10 MiB 卷、0.9 打包阈值、12 MiB 光盘，
   每盘容纳一卷；第三卷触发第二盘封印。`MAX_WRITE_IMAGES=3`，`CAPACITY_IN_IMAGES=5`。
6. 等待两张光盘完成刻录，按实际报告向模拟 MDS 发布前两卷中的四个文件。
   第三卷是封盘触发数据，留在节点待封盘集合中，不对客户端发布。
7. 启动正式 FUSE 客户端，`--mds` 和 `--scheduler` 均指向模拟服务，
   `--optical_read_enabled=true`、`--bootstrap_tier_dirs=false`；挂载为只读。
8. 通过现有 `zbctl stage submit/wait` 和 `open/read/pread/close` 完成读取验证；不调用准备结果释放接口。
9. 卸载客户端，核对模拟 MDS 句柄已全部关闭，正常停止光盘节点和模拟服务。

| 文件 | 内容/用途 |
| --- | --- |
| `empty.bin` | 0 字节，空文件链路 |
| `small.bin` | 123 字节，非对齐小文件 |
| `large.bin` | 9 MiB + 256 KiB + 123 字节，完整大文件及读取生命周期 |
| `another.bin` | 9 MiB + 256 KiB + 17 字节，另一张光盘/另一卷定位 |
| `tail.bin` | 仅用于触发封盘，不发布到模拟 MDS |

首个大文件与另一个卷均验证冷读。大文件关闭后复用准备结果，以及同卷的小文件和空文件，
验证缓存读取。客户端设置现有 `--optical_read_chunk_bytes=65536`，让 FUSE 大请求经过客户端分段逻辑。
这不等于单独测试服务端超上限请求的错误码。

## 验收项目

- 源数据经正式归档接口进入光盘节点，下载分片数与原始文件大小一致。
- 冷读提交返回后立即打开得到 `EAGAIN`；准备完成通过节点推送通知。
- 每个文件完整读取逐字节一致，SHA-256 与生成时的原文件哈希一致。
- 非对齐随机读取、零长度读取、跨 EOF 短读、EOF 后重新读取。
- 同一个准备结果多次打开，关闭后可以再打开。
- 关闭所有读取会话后仍可重新打开，准备结果保留由节点自行决定。
- 对有效准备重复提交复用原句柄，不发送新的准备请求。
- 根目录列表正确，不存在的路径返回不存在。
- 元数据查询、节点查询、心跳、归档分片请求均确实发生。
- 元数据成功分配的打开句柄数与关闭数相等，未释放句柄数为零。
- 客户端日志中的就绪通知数与不同准备任务数相等，且不存在客户端主动释放准备结果的记录。

## 模拟接口与控制通道

| 模块 | 对外接口 |
| --- | --- |
| MDS | `Lookup`、`Getattr`、`GetFileLocation`、`Open`、`Close`、`Readdir` |
| Scheduler | `GetClusterView`、`ReportHeartbeat` |
| 磁盘数据节点 | `ResolveFileRead`、`ReadObject` |
| 归档调用方 | 调用正式节点 `SendArchiveMetadata` |

模拟服务仅维护根目录和清单中的只读文件；文件位置发布之前不可见。它不是完整 MDS 实现。
`Getattr` 接口已提供，但当前客户端的 POSIX stat 走 `Lookup`，因此不会要求该 RPC 必须被调用。

模拟程序也可独立启动：

```bash
./build/optical_external_services /absolute/path/corpus.tsv 127.0.0.1:39080 127.0.0.1:39081
```

`corpus.tsv` 每行是 `inode<TAB>size<TAB>/name<TAB>原始文件绝对路径`；可参考自动测试生成的文件。
stdout 首行输出 `READY <监听地址>`，stdin 接受：

| 命令 | 作用 |
| --- | --- |
| `ARCHIVE` | 按 inode 升序下发一次归档批次，成功返回 `OK ARCHIVE` |
| `PUBLISH <inode> <disc> <image>` | 用节点报告中的真实编号发布该文件位置，返回 `OK PUBLISH` |
| `STATS` | 返回 JSON 调用计数与当前未关闭句柄数 |
| `QUIT` | 正常停止模拟服务 |

stdin/stdout 是外部测试程序之间的控制通道，不属于客户端或光盘节点协议。
手动运行时，节点 `SCHEDULER_ADDR`、客户端 `--mds/--scheduler` 指向模拟服务监听地址；
正式节点端口应与第二个参数一致。客户端其他参数和配置可参考自动测试生成的 `optical.conf` 及脚本。

## 结果与排障

结果目录包含：

```text
result.json       # PASS/FAIL、每次读取大小/哈希/耗时、模拟接口调用计数
corpus.tsv/json   # 原始测试文件清单
locations.json   # 由正式节点归档报告得到的位置
expected/         # 原始数据，仅供模拟磁盘服务和校验
archive/          # 正式光盘节点自己创建的工作目录
optical.conf      # 本次节点配置
external.log      # 外部模拟服务日志
optical.log       # 正式光盘节点日志及归档报告
client.log        # 正式客户端请求、通知和读取会话日志
```

成功和失败都保留结果，便于双方复查。失败不会为了通过测试而修改节点产物。
本测试不覆盖故障恢复、网络异常、通知重发或实际物理设备；发现正式实现不符合读协议时，应根据日志单独修复。

当前节点沿用 READY 后 2 小时有效期、每 10 分钟扫描的自动回收策略。新的读取工作流不依赖显式准备释放。客户端和节点的缓存处理修正属于正常功能逻辑，不是测试分支。
