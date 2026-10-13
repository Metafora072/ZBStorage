# Scheduler 进程内仿真：当前实现边界

本目录落实仓库根目录的 [Scheduler 开发设计与实施方案](../../../Scheduler开发设计与实施方案.md)。多个逻辑磁盘实例共用一个 Scheduler 进程的数据入口，继续使用 `RealNodeService` 的 13 个方法；光盘读取模型通过同一数据端口的正式 `OpticalNodeService` 四个 RPC 暴露。每个请求通过 BRPC attachment 指定逻辑节点和代际，包括请求体为空的 `GetDiskReport`；缺失、错误、过期目标会被拒绝。FUSE 客户端当前的磁盘前台调用已设置该 attachment。旧真实磁盘服务忽略它。

## 配置

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
REPLICATION_ENABLED=false
```

`MAX_CONCURRENT_IO` 限制同时占用模型设备的请求数；进程内实例将其取与配置磁盘数的较小值。额外等待请求最多 `IO_QUEUE_LIMIT` 个；超出返回 `MODEL_IO_QUEUE_FULL` 且不修改对象容量。读写使用同一节点级传输时间线，方向带宽分别由 `READ/WRITE_BYTES_PER_SEC` 指定，总传输速率不会按请求数倍增。固定延迟可以在不同设备槽位间重叠，具体入队顺序仍受真实 RPC 到达顺序影响。`JITTER_SEED` 可固定抖动序列，0 则由 `NODE_ID` 稳定推导；相同请求到达顺序才有相同模型决策。EOF 读只按实际返回字节计传输。当前采用可解释的保守串行传输预算，不是连续公平分流；请求仍会等待到模型完成，长 IO 的 BRPC 线程/协程资源隔离需继续加强。

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
ARCHIVE_COMPRESSION_PERMILLE=1000
ARCHIVE_ALIGNMENT_BYTES=1
```

光盘实例在恢复本地库存并经 CMS 确认后注册为 `Working`，通过正式 `OpticalNodeService` 提供归档和读取。读取核心支持独立盘片容量、有限光驱、装载/寻道/传输时延、有界任务、按字节计费的 LRU 缓存、任务未就绪反馈、分片/EOF 和末片重试窗口；缓存命中按缓存带宽和固定延迟计时。`SendArchiveMetadata` 现要求一项完整清单，包含不可复用的操作 ID、MDS 源租约、源节点/磁盘/版本及目标盘片/镜像。光盘服务先向 MDS 验证 CMS、租约和 inode，再核对源模型库存；受理不等于 MDS 已发布，客户端必须以后续位置查询为准。

`STATE_DIR` 为每个进程内光盘实例必填的独立控制目录，相对于光盘配置文件解析。文件→盘片/镜像位置、盘片占用和异步读任务（含单调递增任务号、释放/重试状态）通过 RocksDB 同步持久化，启动时校验节点身份、盘片数量/容量、逐盘占用及任务对文件版本的引用；版本或位置不符的清理请求拒绝。未完成读任务在重启后保守地重新等待上次持久记录的模型剩余时间，宿主停机期间不自动推进；已完成读任务在保留窗口内可重试，旧任务号不会被新任务误用。缓存和设备时间线不持久化，恢复时清空缓存并保守占用资源；旧的无状态目录不会被自动当成空库存接管。

模型内部现有 `SubmitVerifiedArchive` / `AdvanceArchives` / `AcknowledgeArchive` / `FailArchive` 与 `GetArchive`：调用方必须先验证源版本、位置和租约，提供不可复用的操作 ID。受理后同步保存清单和暂存/盘片预留；模型按传输→打包→刻录的配置带宽与刻录固定时延推进，使用独立传输/打包时间线及共享光驱。刻录完成时以一次 RocksDB 原子批次写入本地文件位置和 `AwaitingMds` 待回报记录；此状态仍计入待写任务、阻止休眠/清理，不等于 MDS 已发布。只有可信调用方确认 MDS 提交后才能调用 `AcknowledgeArchive`，操作 ID、阶段和失败墓碑可在重启后查询，未完成阶段按持久剩余时间保守恢复。失败会释放容量预留；模型按可配置压缩比与对齐估算介质字节，不生成下载数据、镜像或光盘业务文件。

正式模型归档路径在传输/打包/刻录阶段续租，达到 `AwaitingMds` 后使用 `ModelOpticalArchive` 提交：MDS 将光盘位置、inode 归档状态、操作幂等记录及租约删除写入同一条件批次；光盘节点随后清理源模型对象并确认本地 outbox。重启后会重放未完成 outbox；MDS 已提交但响应丢失时按相同操作 ID 幂等返回。目前每个内部任务仅对应一个不跨盘文件和一张未录过的专用盘；`ARCHIVE_COMPRESSION_PERMILLE`（1–1000）和 `ARCHIVE_ALIGNMENT_BYTES`（1–1048576）只计算编码后的逻辑介质占用及刻录时长，暂存仍按源文件大小计量，不执行真实压缩。**这里是光盘模型的正式元数据/数据位置闭环，不是物理光驱刻录或实体盘片验证**；多文件打包与真实压缩仍未实现。

Scheduler 在本进程内初始化各实例，向既有节点管理器直接上报库存、就绪和指标，再经原有 CMS 提案流程发布。光盘心跳把已恢复的盘片总容量/占用与 HDD 读缓存容量/占用作为两个聚合设备上报，同时报告空白/已记录盘片数、未完成读任务数和忙碌光驱数。盘片分类只是**本地模型状态**，MDS 位置只在提交成功后可见。磁盘和光盘数据 RPC 在每次进程启动时默认关闭：只有本轮库存上报成功、随后 CMS 目录提交成功，才开放该节点的数据入口；CMS 不可达或仅有旧管理快照时返回 `MODEL_CATALOG_NOT_CONFIRMED`。后台发布与管理快照也要等当前进程的数据入口启动及首次库存上报完成后才开始。这是启动门禁，不代表已经完成 MDS 的逐文件版本/位置对账。正式入口不需要启动 `virtual_node_server`。现有独立程序暂时保留作旧流程回归。

## 路由契约与现阶段边界

`src/common/rpc/LogicalNodeRoute.h` 提供 `SetLogicalNodeRoute()` / `GetLogicalNodeRoute()`。attachment 格式版本为 `ZBSIM1`，字段为逻辑节点 ID 和非零实例代际。`INSTANCE_GENERATION` 在模型配置中指定（旧配置默认为 1），经 Scheduler 管理视图、CMS、MDS 文件位置传到客户端路由；旧代际请求被数据入口拒绝。它与硬件技术代际是两个字段。已有管理快照与当前配置代际不一致时拒绝复用，不能仅改配置冒充一次无损节点替换。

`SIMULATION_DATA_PORT` 和 `SIMULATION_DATA_ADDRESS` 可在初始节点列表为空时先配置并启动数据入口。新增实例时，添加独立节点配置文件，把路径追加到 Scheduler 配置的相应列表，然后向 Scheduler 进程发送 `SIGHUP`。定时线程读取新增路径，逐实例加载控制状态、注册节点、上报库存并等待 CMS 发布；数据入口在 CMS 成功确认前仍关闭。数据地址和端口不能热改；已有路径的配置修改不会热应用。删除路径不会自动退役实例，仍须通过节点管理排空及 CMS 零引用确认。已退役 ID 是持久墓碑，不可热添加成新实例；重启时保留原配置用于恢复校验。热添加按实例处理，若其中一个新配置失败，其余成功实例不会回滚。尚无跨节点完整事务预留。

退役后，Scheduler 等待一次**确实包含该 Retired 节点的 CMS 目录提交成功**，才从共享数据服务中释放磁盘/光盘实例与报告配置。新请求到旧逻辑路由会返回“节点不存在”；已取得实例引用的在途 RPC 可安全结束，光盘指标对象也随 RPC 持有期延寿。管理节点墓碑、宿主身份和本地持久控制目录保留；对旧 ID 的 Start/Stop/Reboot 不会落入物理 Shell 命令。此释放仅回收进程内运行对象，不删除控制目录，也不等于允许重新使用旧 ID。

两类逻辑节点均可配置 `WAKE_LATENCY_MS`（0–3600000）；休眠/关闭后的数据请求共享同一节点的唤醒截止时间，在截止前等待，其他逻辑节点的路由锁不被其占用；磁盘库存查询不会唤醒。唤醒阶段从请求开始按中档功率计入模型能耗，尚未单独建模启动峰值；长等待仍占用数据 RPC 的执行资源，需要后续异步化。

进程内磁盘在每个 `ARCHIVE_META_DIR/model_state` 使用独立 RocksDB，同步写入对象长度、文件元数据、最后提交 txid，并用单次原子批处理删除文件及对应对象。重启时校验节点 ID、磁盘 ID 集合、每盘容量以及已用容量，再从对象记录重新计算库存；配置漂移或损坏会拒绝启动。Scheduler 管理快照中已有的同名模型节点须与当前硬件配置和实例代际相符才会复用。只存模型控制状态，**不保存真实文件内容**：读请求仍生成模型字节。归档热度/候选跟踪仍是独立 WAL，不与上述业务提交构成原子事务；因此这不是完整的 MDS 事务恢复或真实数据耐久承诺。旧版非空控制目录没有 `model_state` 标记时拒绝自动迁移。独立 `virtual_node_server` 仍使用原有文件元数据存储。管理 `StartNode`/`StopNode`/`RebootNode` 已对进程内节点执行逻辑功率与管理状态切换，不调用物理 Shell 命令；停机/重启会撤销数据准入，须再次完成库存上报及 CMS 发布。当前尚未模拟重启过程时长、在途请求收敛或归档任务的暂停恢复，不能把管理操作返回成功解释为这些条件已完成。

模型磁盘现在把最后一次文件提交的参数签名与 txid 一起同步持久化：同一 txid 只有文件大小、对象单位、期望版本、创建许可和显式修改时间均相同时才返回已提交结果；否则返回 `TXID_PARAMETER_MISMATCH`。旧 `FM1` 文件记录仍可读取，但因缺少签名，不能盲目确认其旧 txid 的重试；新的提交写为 `FM2`。这仍只是单 inode 最后一次提交的幂等窗口，不等于完整事务日志。

空的逻辑光盘库退役时，配置中的空白盘槽位不再被误认为必须迁出的数据：若无已记录/刻录中盘片、后台任务、缓存占用且 CMS 文件引用为零，节点管理器允许完成退役。重启时同名配置只校验并保留已退役墓碑，不重新发布库存或准入；首次成功发布 Retired 目录后释放运行对象。被管理禁用的光盘库不会因后台任务状态上报自行升到中档功率。真实光盘库仍要求实体盘片清空；有数据的逻辑光盘库仍必须先实现逻辑迁移和权威位置切换，不能以此空库规则绕过。

排空控制器支持有数据的模型磁盘源：退役 RPC 先关闭新写入准入、等待服务层在途写 RPC，并在同一排他区间进入 `Draining`；后台枚举持久模型对象，选择另一个有空间的模型节点，导入相同对象 ID/长度与文件版本，在 MDS 对 inode 版本和源位置做条件切换后删除源副本。最后必须再次确认本地为空、CMS 文件引用为零，才标记 `Retired`。生成式读数据不被当成真实字节复制；源对象若没有完整、可追溯的模型元数据则排空失败关闭，不能误删。跨 Scheduler 进程的模型迁移、预加载种子对象的保真迁移，以及物理光盘实际刻录均不在这一闭环内。

## 已覆盖的回归

`scheduler_inprocess_disk_test` 使用真正的本机 BRPC 端口验证双实例共享入口、相同局部对象/设备 ID 隔离、空请求路由、旧代际拒绝、管理禁用、覆盖写容量、空间不足、EOF 及有界共享 IO；还验证销毁并重建实例后的对象/容量/文件版本/txid 恢复、文件与对象删除、配置漂移和旧控制目录拒绝。`scheduler_inprocess_disk_server_restart_test` 启动真实 Scheduler 和 MDS，验证 CMS 不可达时的服务门禁、CMS 确认后的数据 RPC、写对象及提交文件后 `SIGKILL`、管理快照与模型控制目录重开、重启后重新确认 CMS 才可读、剩余容量正确，以及同一 txid 重试不会增加文件版本。`scheduler_inprocess_optical_test` 验证正式光盘 RPC 路由、双实例局部任务 ID 隔离、受理与就绪区分、读回/EOF、缓存命中和任务预算。`scheduler_model_closure_integration_test` 启动真实 Scheduler、MDS 和数据入口，验证非默认实例代际、模型磁盘间迁移与退役、源租约、模型光盘写入/MDS 提交及光盘读回。运行：

本轮新增同一 txid 但提交大小/期望版本不一致的本地及异常重启后拒绝断言。

后续边界回归又增加了 4096/4097 分片、对象序号上界、`uint64` 末端偏移和超大文件对象数拒绝断言。

光盘组件回归现也覆盖 RocksDB 控制目录重开：文件/盘片占用、未完成读任务、已释放任务重试与任务号单调性；还检查配置几何漂移拒绝、旧版本禁止删除、删除后盘片槽位复用、管理库存与读指标。内部归档测试覆盖幂等清单、暂存背压、盘片预留、读缓存淘汰、读/刻录共享光驱、阶段重建、刻录后原子库存/outbox、错误租约拒绝、MDS 确认前禁止清理、确认/失败墓碑重启。全链路测试在完成模型归档后重启 Scheduler 与 MDS，验证 CMS 重新确认、MDS 完整位置恢复及光盘模型读回；有 FUSE 设备时还实际挂载客户端，从恢复后的 MDS/光盘模型读取文件。尚未注入刻录中途的双进程崩溃或真实物理光盘故障。

```text
cmake --build build --target scheduler_node_model_test scheduler_snapshot_test scheduler_inprocess_disk_test scheduler_inprocess_disk_route_probe scheduler_inprocess_optical_test scheduler_model_closure_probe scheduler_server mds_server zb_fuse_client
ctest --test-dir build -R '^scheduler_(node_model_test|snapshot_test|inprocess_(disk|optical).*test|model_closure_integration_test)$' --output-on-failure
```
