# Scheduler 进程内仿真：当前实现边界

本目录落实仓库根目录的 [Scheduler 开发设计与实施方案](../../../Scheduler开发设计与实施方案.md)。多个逻辑磁盘实例共用一个 Scheduler 进程的数据入口，复用 `RealNodeService` 的数据方法；光盘模型在同一数据端口提供 `OpticalNodeService` 的归档下发、六个准备/读会话接口和三个旧读接口。每个请求通过 BRPC attachment 指定逻辑节点和代际，包括请求体为空的 `GetDiskReport`；缺失、错误、过期目标会被拒绝。FUSE 的磁盘和新光盘前台调用均传递该 attachment；真实节点兼容忽略。

正式光盘读链路也支持 `SubscribeReadEvents → PrepareRead → ReadReadyEvent → AcquireReadSession → ReadPreparedFile → ReleaseReadSession`，并保留上述旧读接口。共享入口用节点身份限定通知会话、本地任务和读句柄；客户端将 MDS 返回的节点 ID/实例代际交给同一套正式光盘调用代码。每次打开产生独立读句柄，EOF 不释放会话；`ReleasePreparedRead` 禁止新打开，已有句柄继续有效，最后一个引用释放后回收任务。

另外实现与真实光盘相同的 `ListArchiveImages / GetArchiveImageFiles` 供 MDS 查询已完成逻辑介质及 inode/大小/版本。此类只读控制查询在请求体自带节点身份和非零代际，不依赖 attachment，不触发唤醒或推进模型。返回 `modeled=true`，分页令牌变化时重新查询；不能作为清理真实源数据的证明。查询协议和 C++ 客户端见 [API 文档](../api/README.md#mds-归档清单接口)。

新协议使用单个共享推进线程发送就绪事件，不为每节点/任务创建线程；会话、准备任务和读句柄都有预算上限。就绪且无读取引用的准备结果按 `TASK_RETENTION_MS` 自动回收；活跃引用保护任务不被旧协议末片/超时规则提前释放。通知流和读句柄不持久化，流断开时清理所属引用，重启后客户端须重新订阅与准备；文件、盘片和归档控制状态仍按原规则恢复。活跃读会话进入节点管理快照，参与忙碌与休眠判断。

## 配置

磁盘与光盘均可配置 `IO_FAILURE_PROBABILITY_PPM=0`、`IO_TIMEOUT_PROBABILITY_PPM=0`、`IO_TIMEOUT_MS=1000`；两概率和不得超过 1000000，超时为 1..3600000ms。磁盘以 `JITTER_SEED`、光盘以 `IO_FAULT_SEED` 固定请求受理序列。磁盘注入在模型 IO 返回成功之前，失败不会提交对象/容量；光盘在读准备/归档受理点注入，不改变已有介质。默认关闭，不影响正常部署。错误消息分别为 `MODEL_IO_INJECTED_FAILURE / MODEL_IO_INJECTED_TIMEOUT`，超时等待挂起 bthread，不持有节点模型锁。

模型归档源租约可设 `ARCHIVE_SOURCE_LEASE_MS=300000`（1000..3600000ms，MDS 仍可按自己的策略裁剪）。长期停机后只在租约已过期、源归属/大小/版本仍与 MDS 和本地一致时重新申领；新租约与原受理租约分别持久化于 OA5，兼容读取 OA1..OA4。源变更或其他有效租约存在时保持阻塞、保留源。没有直接修改 MDS 数据库。

`GetManagedNode.metrics_streams` 按前台 DEFAULT、后台 ARCHIVE、排空 MIGRATION 分别记录累计读写操作数和字节数，并保留退休报告者历史。模型光盘后台字节为编码完成量，不是实际业务网络字节；活动/忙碌与能耗仍由模型任务和原窗口决定。完成通知不会因轮询重复记账，恢复后的旧完成任务不会作为新事件重新计入。

在 Scheduler 的 `KEY=VALUE` 配置中增加：

```text
SIMULATION_DISK_CONFIGS=sim-a.conf,sim-b.conf
SIMULATION_OPTICAL_CONFIGS=opt-a.conf,opt-b.conf
SIMULATION_DATA_PORT=29100
SIMULATION_DATA_ADDRESS=127.0.0.1:29100
```

配置文件路径相对于 Scheduler 配置文件所在目录解析。每个 `sim-*.conf` 使用已有虚拟磁盘配置格式，至少指定唯一 `NODE_ID`、`DISK_IDS`、`ARCHIVE_META_DIR`、`DISK_CAPACITY_BYTES`、读写带宽。`ARCHIVE_META_DIR` 相对于本节点配置文件所在目录解析，首次启动须为空；后续可以重开该模型创建的控制目录。不同节点不能共享。真实环境应把 `SIMULATION_DATA_ADDRESS` 设置为客户端和 MDS 可访问的地址，并与监听端口一致。

```text
NODE_ID=sim-a
DISK_IDS=disk-01
INSTANCE_GENERATION=7
ARCHIVE_META_DIR=sim-a-control
DISK_CAPACITY_BYTES=1000000000
READ_BYTES_PER_SEC=200000000
WRITE_BYTES_PER_SEC=200000000
READ_BASE_LATENCY_MS=1
WRITE_BASE_LATENCY_MS=1
JITTER_MS=0
JITTER_SEED=1234
MAX_CONCURRENT_IO=5
IO_QUEUE_LIMIT=64
WAKE_LATENCY_MS=0
REBOOT_LATENCY_MS=1000
REPLICATION_ENABLED=false
```

`MAX_CONCURRENT_IO` 限制同时占用模型设备的请求数；进程内实例将其取与配置磁盘数的较小值。额外等待请求最多 `IO_QUEUE_LIMIT` 个；超出返回 `MODEL_IO_QUEUE_FULL` 且不修改对象容量。读写使用同一节点级传输时间线，方向带宽分别由 `READ/WRITE_BYTES_PER_SEC` 指定，总传输速率不会按请求数倍增。固定延迟可以在不同设备槽位间重叠，具体入队顺序仍受真实 RPC 到达顺序影响。`JITTER_SEED` 可固定抖动序列，0 则由 `NODE_ID` 稳定推导；相同请求到达顺序才有相同模型决策。EOF 读只按实际返回字节计传输。当前采用保守串行传输预算，不是连续公平分流；等待使用 bthread 挂起，不持续占用 BRPC worker。跨等待持有的模型准入锁使用 bthread 读写锁，避免 pthread 锁随协程迁移；请求自身仍占用会话/队列预算，不能宣称已经验证无限压力下的资源隔离。

文件读写计划每次至多生成 4096 个对象分片；对象序号须小于 `UINT32_MAX`，提交的文件对象总数也须可由 `uint32` 表示。超限请求返回参数错误，且不会分配巨大分片数组或提交截断后的对象映射。调用方可将大范围拆为多次计划；这不是对文件总大小施加 4096 个对象的限制。

光盘模型配置独立，示例：

```text
NODE_ID=opt-a
INSTANCE_GENERATION=9
STATE_DIR=opt-a-control
DRIVE_COUNT=10
DISC_COUNT=10000
DISC_CAPACITY_BYTES=10000000000000
DISC_READ_BYTES_PER_SEC=100000000
CACHE_CAPACITY_BYTES=280000000000000
CACHE_READ_BYTES_PER_SEC=1000000000
CACHE_BASE_LATENCY_US=500
LOAD_LATENCY_MS=60000
SEEK_LATENCY_MS=500
WAKE_LATENCY_MS=0
MAX_READ_TASKS=128
TASK_RETENTION_MS=30000
MAX_ARCHIVE_TASKS=128
ARCHIVE_TRANSFER_BYTES_PER_SEC=1000000000
ARCHIVE_PACK_BYTES_PER_SEC=1000000000
ARCHIVE_BURN_BYTES_PER_SEC=10000000
ARCHIVE_BURN_FIXED_MS=500
ARCHIVE_TAIL_WAIT_MS=1000
IMAGE_CAPACITY_BYTES=10000000000000
REBOOT_LATENCY_MS=1000
ARCHIVE_COMPRESSION_PERMILLE=1000
ARCHIVE_ALIGNMENT_BYTES=1
```

光盘实例在恢复本地库存并经 CMS 确认后注册为 `Working`，通过正式 `OpticalNodeService` 提供归档和读取。读取核心支持独立盘片容量、有限光驱、装载/寻道/传输时延、有界任务、按字节计费的 LRU 缓存、任务未就绪反馈、分片/EOF 和末片重试窗口；缓存命中按缓存带宽和固定延迟计时。`SendArchiveMetadata` 现要求一项完整清单，包含不可复用的操作 ID、MDS 源租约、源节点/磁盘/版本及目标盘片/镜像。光盘服务先向 MDS 验证 CMS、租约和 inode，再核对源模型库存；受理不等于 MDS 已发布，客户端必须以后续位置查询为准。

`STATE_DIR` 为每个进程内光盘实例必填的独立控制目录，相对于光盘配置文件解析。文件→盘片/镜像位置、盘片占用和异步读任务（含单调递增任务号、释放/重试状态）通过 RocksDB 同步持久化，启动时校验节点身份、盘片数量/容量、逐盘占用及任务对文件版本的引用；版本或位置不符的清理请求拒绝。未完成读任务在重启后保守地重新等待上次持久记录的模型剩余时间，宿主停机期间不自动推进；已完成读任务在保留窗口内可重试，旧任务号不会被新任务误用。缓存和设备时间线不持久化，恢复时清空缓存并保守占用资源；旧的无状态目录不会被自动当成空库存接管。

模型内部现有 `SubmitVerifiedArchive` / `AdvanceArchives` / `AcknowledgeArchive` / `FailArchive` 与 `GetArchive`：调用方必须先验证源版本、位置和租约，提供不可复用的操作 ID。受理后同步保存清单和暂存/盘片预留；模型按传输→打包→刻录的配置带宽与刻录固定时延推进，使用独立传输/打包时间线及共享光驱。刻录完成时以一次 RocksDB 原子批次写入本地文件位置和 `AwaitingMds` 待回报记录；此状态仍计入待写任务、阻止休眠/清理，不等于 MDS 已发布。只有可信调用方确认 MDS 提交后才能调用 `AcknowledgeArchive`，操作 ID、阶段和失败墓碑可在重启后查询，未完成阶段按持久剩余时间保守恢复。失败会释放容量预留；模型按可配置压缩比与对齐估算介质字节，不生成下载数据、镜像或光盘业务文件。

正式模型归档路径在传输/打包/刻录阶段续租，达到 `AwaitingMds` 后使用 `ModelOpticalArchive` 提交：MDS 将光盘位置、inode 归档状态、操作幂等记录及租约删除写入同一条件批次；光盘节点随后清理源模型对象并确认本地 outbox。重启后重放未完成 outbox；MDS 已提交但响应丢失时按相同操作 ID 幂等返回。`SendArchiveMetadata` 支持 1–4096 个文件并返回逐文件受理结果，按文件操作 ID 幂等；批次不是跨文件原子事务，失败前已受理的项继续执行，相同清单可重试。相同盘片/镜像的文件共卷，占用之和不超过 `IMAGE_CAPACITY_BYTES`（0 使用盘容量），封卷后禁止追加；满卷或达到 `ARCHIVE_TAIL_WAIT_MS`（默认 1000，范围 0–3600000）后，待本卷全部传输/打包完成，使用一个光驱执行一次共卷刻录。同卷阶段转换、逻辑文件和待回报记录原子落盘；MDS 仍逐文件提交。`ARCHIVE_COMPRESSION_PERMILLE` 与 `ARCHIVE_ALIGNMENT_BYTES` 只估算介质占用，不执行真实压缩。一文件不能跨盘；本实现不声称覆盖多卷共享盘片的真实设备格式。**这是模型数据位置闭环，不是物理刻录或内容保真证明。**

Scheduler 在本进程内初始化各实例，向既有节点管理器直接上报库存、就绪和指标，再经原有 CMS 提案流程发布。光盘心跳把已恢复的盘片总容量/占用与 HDD 读缓存容量/占用作为两个聚合设备上报，同时报告空白/已记录盘片数、未完成读任务数和忙碌光驱数。盘片分类只是**本地模型状态**，MDS 位置只在提交成功后可见。磁盘和光盘数据 RPC 在每次进程启动时默认关闭：只有本轮库存上报成功、随后 CMS 目录提交成功，才开放该节点的数据入口；CMS 不可达或仅有旧管理快照时返回 `MODEL_CATALOG_NOT_CONFIRMED`。后台发布与管理快照也要等当前进程的数据入口启动及首次库存上报完成后才开始。这是启动门禁，不代表已经完成 MDS 的逐文件版本/位置对账。正式入口不需要启动 `virtual_node_server`。现有独立程序暂时保留作旧流程回归。

## 路由契约与现阶段边界

`src/common/rpc/LogicalNodeRoute.h` 提供 `SetLogicalNodeRoute()` / `GetLogicalNodeRoute()`。attachment 格式版本为 `ZBSIM1`，字段为逻辑节点 ID 和非零实例代际。`INSTANCE_GENERATION` 在模型配置中指定（旧配置默认为 1），经 Scheduler 管理视图、CMS、MDS 文件位置传到客户端路由；旧代际请求被数据入口拒绝。它与硬件技术代际是两个字段。已有管理快照与当前配置代际不一致时拒绝复用，不能仅改配置冒充一次无损节点替换。

`SIMULATION_DATA_PORT` 和 `SIMULATION_DATA_ADDRESS` 可在初始节点列表为空时先配置并启动数据入口。新增实例时，添加独立节点配置文件，把路径追加到 Scheduler 配置的相应列表，然后向 Scheduler 进程发送 `SIGHUP`。定时线程读取新增路径，逐实例加载控制状态、注册节点、上报库存并等待 CMS 发布；数据入口在 CMS 成功确认前仍关闭。数据地址和端口不能热改；已有路径的配置修改不会热应用。删除路径不会自动退役实例，仍须通过节点管理排空及 CMS 零引用确认。已退役 ID 是持久墓碑，不可热添加成新实例；重启时保留原配置用于恢复校验。热添加按实例处理，若其中一个新配置失败，其余成功实例不会回滚。尚无跨节点完整事务预留。

退役后，Scheduler 等待一次**确实包含该 Retired 节点的 CMS 目录提交成功**，才从共享数据服务中释放磁盘/光盘实例与报告配置。新请求到旧逻辑路由会返回“节点不存在”；已取得实例引用的在途 RPC 可安全结束，光盘指标对象也随 RPC 持有期延寿。管理节点墓碑、宿主身份和本地持久控制目录保留；对旧 ID 的 Start/Stop/Reboot 不会落入物理 Shell 命令。此释放仅回收进程内运行对象，不删除控制目录，也不等于允许重新使用旧 ID。

两类逻辑节点均可配置 `WAKE_LATENCY_MS`（0–3600000）；自动休眠后的数据请求共享同一唤醒截止时间，以 bthread 挂起等待，不持有全局路由锁；磁盘库存查询不会唤醒。管理禁用的节点必须显式启动，不能由业务请求绕过。唤醒按中档功率计能耗，尚未单独建模启动峰值。`REBOOT_LATENCY_MS`（默认 1000，范围 0–3600000）控制逻辑重启的关闭时间；控制 RPC 同步等待该时延，调用方应配置足够超时，不能因传输超时盲目重试。相同节点的生命周期操作互斥，操作状态和业务准入须分别查询。

进程内磁盘在每个 `ARCHIVE_META_DIR/model_state` 使用独立 RocksDB，同步保存对象长度、文件元数据、最后提交 txid，原子删除文件及对象。恢复校验身份、磁盘集合、容量和库存，配置漂移或损坏拒绝启动；已有管理快照还须匹配实例代际。只保存控制状态，**不保存用户内容**。归档热度/候选 WAL 不与文件提交构成原子事务，不是完整 MDS 事务恢复承诺；旧非空目录没有 `model_state` 标记时拒绝自动迁移。独立旧入口仍使用原存储。逻辑启停/重启不执行物理 Shell；停机/重启撤销准入并在实例排他门禁内收敛已进入的数据 RPC。内部迁移也持有实例/准入引用，目标停机或排空时不能绕过门禁导入数据，恢复后仍须新库存与 CMS 确认。

模型磁盘迁移的读/写服务时间复用源/目标各自的前台共享 IO 预算，不生成或传输等规模载荷。迁移执行使用单独一个后台线程，库存、健康、功率和 CMS 更新不等待复制完成；进度查询读取最近发布的步骤快照，不持有网络等待锁。单线程迁移尚不意味着不同排空任务之间有并行公平性保证。读写字节进入节点计量，应用/迁移/模型流量的完整跨模块分类仍需进一步对齐。

配置 `MANAGED_STATE_SNAPSHOT_PATH` 时，启停/重启、管理状态变更、排空请求和外部指标 ACK 在成功应答前同步保存管理状态和操作记录；临时文件 fsync、原子改名及目录 fsync 受同一写锁保护。写入失败须先查询，不能盲目重试控制命令。成功/失败操作及 ID 序列随快照恢复，在途操作恢复为中断失败，不自动重放。没有配置路径时仍是显式的易失管理状态。

模型磁盘现在把最后一次文件提交的参数签名与 txid 一起同步持久化：同一 txid 只有文件大小、对象单位、期望版本、创建许可和显式修改时间均相同时才返回已提交结果；否则返回 `TXID_PARAMETER_MISMATCH`。旧 `FM1` 文件记录仍可读取，但因缺少签名，不能盲目确认其旧 txid 的重试；新的提交写为 `FM2`。这仍只是单 inode 最后一次提交的幂等窗口，不等于完整事务日志。

空模型光盘库在无已记录盘、后台任务、缓存占用且 CMS 引用为零时可退役，空白槽位不算待迁数据。有数据时按源镜像分组选择健康、同为无载荷模型且有容量的目标，保留编码后大小，经传输/打包/刻录模型持久接收，使用 `CommitModelOpticalMigration` 做 inode 版本、源位置及两端代际的条件切换，再清理源文件与缓存。活跃准备/读引用保护源记录。无目标时保留源并报告重试/阻塞原因；目标 outbox 保存固定操作 ID，强杀恢复后继续提交和清理。源清空后等待完整空库存上报、CMS 零引用核验及 Retired 目录确认，才释放实例。实体盘片迁移不由此模型代替；跨 Scheduler 模型迁移仍不支持。

光盘模型的管理停机和节点故障冻结未完成归档与读取的时间线，原子检查点保存剩余阶段及尾批时间，恢复时平移设备时间线；关闭期间不推进刻录、不发送 MDS 完成提交。客户端通知连接/读句柄仍遵守原来的恢复契约。源租约过期后按权威位置/大小/版本和本地源核对，再重新申领；新租约先落本地记录，再重试原条件提交。源变更或被有效租约占用则阻塞并保留源。

排空控制器支持有数据的模型磁盘源：退役 RPC 先关闭新写入准入、等待服务层在途写 RPC，并在同一排他区间进入 `Draining`；后台枚举持久模型对象，选择另一个有空间的模型节点，导入相同对象 ID/长度与文件版本，在 MDS 对 inode 版本和源位置做条件切换后删除源副本。最后必须再次确认本地为空、CMS 文件引用为零，才标记 `Retired`。生成式读数据不被当成真实字节复制；源对象若没有完整、可追溯的模型元数据则排空失败关闭，不能误删。跨 Scheduler 进程的模型迁移、预加载种子对象的保真迁移，以及物理光盘实际刻录均不在这一闭环内。

## 系统集成验证

`scheduler_inprocess_disk_server_restart_test` 启动真实 Scheduler 和 MDS，验证 CMS 不可达时的服务门禁、CMS 确认后的数据 RPC、写对象及提交文件后 `SIGKILL`、管理快照与模型控制目录重开、重启后重新确认 CMS 才可读、剩余容量正确，以及同一 txid 重试不会增加文件版本。

`scheduler_model_closure_integration_test` 启动真实 Scheduler、MDS 和数据入口，验证非默认实例代际、模型磁盘间迁移与退役、源租约、模型光盘写入/MDS 提交及光盘读回。完成归档后重启 Scheduler 与 MDS，验证 CMS 重新确认、MDS 完整位置恢复；有 FUSE 设备时实际挂载正式客户端，通过 `zbctl stage submit/wait` 准备恢复后的光盘模型文件并进行 POSIX 读取。

上述用例还覆盖两个文件共卷、无迁移目标时保留源、带数据模型光盘库迁移退役、强制逻辑停机冻结任务、迁移中 `SIGKILL` 恢复、逻辑重启时延及第二文件读回。这些数据是生成式数据，不等同于实体光盘 IO；未覆盖刻录中途 Scheduler/MDS 同时崩溃或真实物理光盘故障。真实数据链路见 [Scheduler 系统测试](../../../tests/scheduler/README.md)。

单元/组件测试不作为交付内容。本模块系统测试仅手动运行，不注册到全仓 CTest；保留的 C++ probe 是独立服务进程集成测试的 RPC 驱动，以 `EXCLUDE_FROM_ALL` 排除在默认构建之外。

```text
cmake --build build --target scheduler_inprocess_disk_route_probe scheduler_model_closure_probe scheduler_server mds_server zb_fuse_client zbctl
python3 tests/scheduler/inprocess_disk_server_restart_test.py --scheduler build/scheduler_server --mds build/mds_server --probe build/scheduler_inprocess_disk_route_probe
python3 tests/scheduler/model_closure_integration_test.py --scheduler build/scheduler_server --mds build/mds_server --probe build/scheduler_model_closure_probe --ctl build/zbctl --client build/zb_fuse_client
```
