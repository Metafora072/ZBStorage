# Scheduler 统一节点管理、性能与功耗调度设计

## 1. 文档定位

本文是 ZBStorage Scheduler 节点管理与功耗调度的权威详细设计。设计依据包括：

- `ZB后续工作计划20260730.md` 中“依据节点访问情况调整节点状态”和“节点增加、删除”的任务。
- 导师补充的存储节点、元数据节点、光盘库节点属性与默认参数。
- 当前仓库已有的心跳、健康检测、主备切换和节点启停实现。
- 长期演进仿真对统一时间、寿命、失效、容量、性能和能耗统计的要求。

旧版设计只描述 10000 个 optical node 的三态功耗模拟。新版将范围扩展为统一管理存储节点、元数据节点和光盘库节点，并把节点属性、访问负载、性能模型、四级功耗、能耗、可靠性和生命周期放入同一套模型。

本文同时描述目标架构和截至2026-09-03已落地的实现。第4节给出代码现状；可调实验参数仍保留显式配置，不以默认值代替实验记录。

## 2. 建设目标

- 统一描述三类节点的硬件、容量、带宽、固定延迟、功耗、可靠性和寿命。
- 管理节点从加入、工作、排空、失效到退出的完整生命周期。
- 根据实时访问指标或仿真访问事件，在峰值、中值、待机、关闭四个功耗等级间调度。
- 计算单节点、节点类型和集群维度的瞬时功率与累计能耗。
- 向 MDS 提供稳定的可分配资源视图，节点状态变化时不破坏现有数据位置。
- 同时支持真实时间运行和时间驱动仿真，保证相同策略可用于真实实验和长期演进实验。
- 支持批量加入、批量退出、设备换代、节点寿命到期和随机失效等宏观事件。
- 提供可分页查询、快照恢复和前端展示所需的统计接口。

## 3. 非目标与边界

- Scheduler 不解析 POSIX 路径，也不直接执行文件读写；文件 trace 必须先由执行器或仿真器转换为节点访问事件。
- Scheduler 负责编排对象排空；实际字节读写由 RealNodeService 完成，文件位置的原子切换由 MDS 完成。
- 第一阶段不要求建立精确到控制器、总线和单块盘排队的硬件模拟器。
- 可靠性模型第一阶段服务于实验，不宣称等价于真实设备失效率预测。
- 未确认的导师参数必须保留为显式配置和待确认项，不能在代码中静默猜测。
- 日志、快照、trace 和实验结果写入运行目录，不写入源码仓库。

## 4. 当前实施状态与剩余缺口

当前 Scheduler 已具备：

- `ReportHeartbeat` 自动发现 real、virtual pool 和 optical node。
- `GetClusterView` 向 MDS 返回节点、磁盘、健康、管理和电源状态。
- `HEALTHY/SUSPECT/DEAD` 心跳故障检测。
- `ENABLED/DRAINING/DISABLED` 管理状态。
- `ON/OFF/STARTING/STOPPING` 电源与启停过渡状态。
- 旧协议继续返回单节点形式的group/role/epoch兼容字段；公共节点模型不再维护主备组。
- `StartNode/StopNode/RebootNode` 以及可选 Shell actuator。
- 周期性 tick 和文本集群视图快照。
- `ManagedNode` 三类默认模板、多设备组容量、带宽、延迟和寿命计算。
- `NodePowerManager` 的JOINING/WORKING/DRAINING/EXITING/RETIRED、派生列表、四级功耗和能耗积分。
- 注册/代际替换、单节点查询、组合筛选、指标/事件、光盘库状态/只读和排空管理RPC。
- 旧节点首次心跳直接写入统一节点目录，退休tombstone拒绝旧心跳复活。
- 功耗阈值、空闲时间和驻留数据关机保护配置。
- 独立模型测试和 Scheduler 服务层集成测试。
- Real、Virtual、Optical 数据节点在 RPC 入口聚合指标并随心跳周期上报；MDS 注册为 metadata node 并上报元数据 RPC 指标。
- 指标上报携带 producer epoch 和 sequence，响应丢失后的原窗口可原样重试且不会重复计数。
- trace replay 将路径操作稳定映射为元数据事件和数据节点访问事件。
- `AdvanceSimulationTime` 显式推进 simulated node；服务器墙钟 tick 跳过模拟节点，避免未来 trace 时间与真实时间交叉积分。
- DRAINING 通过剩余对象、剩余字节和活动请求三项进度确认后进入 EXITING，不能直接普通退休。
- managed catalog v2使用Protobuf快照、同目录临时文件原子替换和启动恢复，包含身份、设备清单、写入统计和时序历史。

本阶段新增并已完成：

- Real/Virtual 节点的实际对象枚举接口；真实节点扫描对象目录而不是依赖有上限的热度索引，Virtual 节点同时枚举普通对象和稀疏预加载对象。
- `DrainMigrationCoordinator`：按稳定对象 ID 解析 inode、按文件成组迁移、选择健康且容量充足的目标磁盘。
- 分块读取、目标写入和逐块回读字节校验；全部对象校验后才调用 MDS。
- MDS `CommitFileMigration`：校验来源位置，并通过 RocksDB value compare-and-write 原子替换统一 inode 中的文件位置。
- MDS提交成功后清理源对象；所有工作项完成后再次枚举源端，确认零对象才上报归零、Stop并进入RETIRED。
- 元数据节点必须零引用，光盘库必须已观测且零本地光盘，Physical节点必须确认断电；`force`不能绕过这些条件。
- 自动 OFF 的最低在线节点、在线总容量和在线可用容量水位。
- 物理功耗执行器：策略功耗与实际执行功耗分离，默认关闭；只有显式启用且 wake/standby/off 三个模板齐全才执行。
- 正态寿命、浴盆曲线FIT、显式MTBF/MTTR、确定性失效采样、心跳故障来源、自动/手工修复和故障注入RPC。
- 批量注册、批量退出、批量访问事件、长期模拟时间分步推进和功率/能耗时序查询 RPC。

本组节点管理/Scheduler闭环已经完成。下列事项属于相邻模块或实验编排，不作为本组未完成项：

- 指标生产者已接入主要数据 I/O 和 MDS 文件系统 RPC；更细的光盘 HDD cache hit/DISC miss 分路统计仍待下沉到 ImageStore。
- MDS 已作为 managed metadata node 注册和上报指标，并用其RocksDB所在文件系统的实际容量/可用量作为SSD清单观测值。
- `ClusterState`已收敛为统一目录的兼容投影，不再有第二份心跳节点事实表。
- simulated node已可按任意步长快速推进并输出时序；采购批次和场景输入由实验驱动器编排，Scheduler只执行换代约束。
- virtual pool的进程与逻辑规模已由`execution_mode`和`logical_node_count`区分；若未来要求逐逻辑节点独立生命周期，应由Virtual DataNode/仿真方案另行定义身份与心跳契约。
- 旧`src/sim`把10000解释成光盘节点数量的问题属于旧仿真场景口径；当前节点模型明确为每个光盘库默认10000张DISC。

## 5. 总体架构

```text
真实节点心跳/指标 ───────┐
MDS 元数据指标 ─────────┤
trace/长期仿真适配器 ───┤
管理端加入/删除请求 ─────┤
                         v
+----------------------------------------------------------------+
|                         Scheduler                              |
|                                                                |
|  NodeCatalog          单一节点事实表、配置、代际和 tombstone    |
|  NodeLifecycleManager 加入、工作、排空、失效、退出               |
|  MetricsAggregator    窗口化访问指标与最近访问时间               |
|  PerformanceModel     容量、有效带宽、延迟和服务能力             |
|  PowerPolicyEngine    四级功耗状态决策与防抖                     |
|  EnergyAccounting     功率汇总与能耗积分                         |
|  ReliabilityManager   寿命采样、计划退出和随机失效               |
|  CmsCatalogPublisher  向CMS提交带generation的完整节点目录提案    |
|  SnapshotStore        快照、恢复和实验采样                       |
+----------------------------------------------------------------+
             |                         |                    |
             v                         v                    v
  CMS成员/紧凑ID/准入权威        管理/功耗查询视图          前端/实验结果
             |
             v
  MDS NodeStateCache/PG/真实I/O放置
```

所有模块共享一个可注入的 `Clock`。真实系统使用墙钟，仿真系统使用可推进的模拟时钟。

当前代码没有引入全局 Clock 对象，而是在 `TickRealTime` 与 `AdvanceSimulatedTime` 两条入口隔离时间域；长期模拟 RPC 在模拟域内按配置步长推进。该实现避免墙钟污染未来 trace，后续若加入离散事件队列可再抽象 Clock，不影响现有接口。

## 6. 统一节点模型

### 6.1 节点类型

统一支持以下节点类型：

- `STORAGE`：真实或仿真的数据存储节点，底层为 HDD/SSD 设备组。
- `METADATA`：MDS 所使用的元数据存储节点，底层主要为 SSD。
- `OPTICAL_LIBRARY`：带 HDD 缓存、光盘库存和光驱的光盘库节点。

当前 `NODE_REAL` 和 `NODE_VIRTUAL_POOL` 是部署形态，不应继续同时承担硬件类型含义。目标模型应分开：

- `node_kind`：STORAGE、METADATA、OPTICAL_LIBRARY。
- `execution_mode`：PHYSICAL、VIRTUAL、SIMULATED。
- `logical_node_count`：一个 virtual pool 代表的逻辑节点数量。
- `participates_in_placement`：是否进入 MDS 可分配资源视图。

### 6.2 硬件与性能属性

每个节点记录：

- `node_id`、紧凑ID、服务地址、节点类型、执行形态和技术代际。
- 一个或多个设备组，每组包含设备类型、单设备容量、数量、单设备带宽和最大并发数。
- 外部网络带宽。
- 内部总存储带宽或其计算参数。
- 固定文件访问延迟。
- 四级功率配置。
- 可靠性参数和随机种子。
- 加入时间、寿命、计划退出时间、实际退出时间。
- 当前健康、生命周期、功耗和启停过渡状态。
- 最近访问时间、窗口化负载指标和累计能耗。

设备容量统一按设备组求和：

```text
node_capacity_bytes = Σ(device_capacity_bytes × device_count)
```

设备组允许出现多对“容量 × 数量”，例如同一节点同时配置 `28 TB × 20` 和 `20 TB × 20`。

### 6.3 状态维度必须正交

不能再用一个枚举同时表达节点是否健康、是否允许分配、是否正在关机和功耗高低。每个节点包含六个正交维度：

1. 健康状态：`HEALTHY / SUSPECT / FAILED`。
2. 生命周期状态：`JOINING / WORKING / DRAINING / EXITING / RETIRED`。
3. 功耗等级：`PEAK / MEDIUM / STANDBY / OFF`。
4. 启停过渡状态：`STABLE / STARTING / STOPPING / REBOOTING`。
5. 运维状态：`ENABLED / DRAINING / DISABLED`。
6. 服务模式：`READ_WRITE / READ_ONLY`。

节点可分配条件由这些维度组合得到，而不是由某一个字段决定：

```text
allocatable = health == HEALTHY
           && lifecycle == WORKING
           && administrative_state == ENABLED
           && service_mode == READ_WRITE
           && power in {PEAK, MEDIUM}
           && transition == STABLE
           && has_healthy_device
```

待机节点可被唤醒，但在完成唤醒前不直接进入新请求分配集合。关闭节点必须先启动。

### 6.4 单一事实表与派生列表

导师要求的列表定义如下：

- 加入节点列表：`lifecycle == JOINING`。
- 工作节点列表：`lifecycle == WORKING && health != FAILED`。
- 失效节点列表：`health == FAILED`，且尚未完成恢复或退休。

这些列表必须由统一的节点事实表动态查询或索引得到，不能维护三份相互独立的节点副本。这样状态迁移只修改一个节点记录，不会出现同一节点同时位于多个冲突列表的问题。

## 7. 三类节点默认配置

所有容量和带宽在内部统一转换为字节及字节每秒；展示层保留 TB、PB、MB/s、GB/s。默认采用十进制单位，除非实验配置明确指定二进制单位。

### 7.1 存储节点

| 属性 | 默认值 |
|---|---:|
| 设备组 | HDD `18 TB × 24` |
| 总容量 | `432 TB` |
| 外部网络带宽 | `10 GB/s` |
| 默认并发磁盘数 | `5` |
| 单磁盘带宽 | `200 MB/s` |
| 内部总存储带宽 | `1 GB/s` |
| 固定文件访问延迟 | `5 ms` |
| PEAK 功率 | `1000 W` |
| MEDIUM 功率 | `300 W` |
| STANDBY 功率 | `100 W` |
| OFF 功率 | `0 W` |
| 平均寿命 | `5 年` |

默认单请求无排队服务时间：

```text
B_effective = min(10 GB/s, 5 × 200 MB/s) = 1 GB/s
L_storage(size) = 5 ms + size / B_effective
```

### 7.2 元数据存储节点

| 属性 | 默认值 |
|---|---:|
| 设备组 | SSD `4 TB × 16` |
| 总容量 | `64 TB` |
| 外部网络带宽 | `10 GB/s` |
| 默认并发 SSD 数 | `10` |
| 单 SSD 带宽 | `5 GB/s` |
| 理论内部总带宽 | `50 GB/s` |
| 有效带宽上限 | `10 GB/s`，受外部网络限制 |
| 固定文件访问延迟 | `0.5 ms` |
| PEAK 功率 | `500 W` |
| MEDIUM 功率 | `200 W` |
| STANDBY 功率 | `50 W` |
| OFF 功率 | `0 W` |
| 平均寿命 | `5 年` |

默认单请求无排队服务时间：

```text
B_effective = min(10 GB/s, 10 × 5 GB/s) = 10 GB/s
L_metadata(size) = 0.5 ms + size / B_effective
```

### 7.3 光盘库节点

| 属性 | 默认值 |
|---|---:|
| HDD 缓存设备组 | HDD `28 TB × 10` |
| HDD 缓存总容量 | `280 TB` |
| 单张 DISC 容量 | `1 TB` |
| DISC 数量 | `10000` |
| 光盘总容量 | `10 PB` |
| 外部网络带宽 | `10 GB/s` |
| 光驱数量 | `10` |
| 单光驱读取/写入带宽 | `100 MB/s / 10 MB/s` |
| 光驱并发数 | `10` |
| 光驱聚合读取/写入带宽 | `1 GB/s / 100 MB/s` |
| HDD 命中固定延迟 | `0.5 ms` |
| 机械取盘延迟 | `60 s` |
| 光驱寻道延迟 | `0.5 s` |
| PEAK 功率 | `400 W` |
| MEDIUM 功率 | `200 W` |
| STANDBY 功率 | `50 W` |
| OFF 功率 | `0 W` |
| 平均寿命 | `10 年` |

光盘库必须区分两条访问路径：

```text
HDD cache hit:
L_hit(size) = 0.5 ms + size / min(B_external, B_hdd_internal)

DISC miss:
L_miss(size) = 60 s + 0.5 s + size / min(B_external, 10 × 100 MB/s)
```

最新方案将HDD缓存和光驱分开建模：

- `hdd_internal_bandwidth_bytes_per_sec`
- `disc_drive_read/write_bandwidth_bytes_per_sec`
- `concurrent_disc_drives`

当前默认HDD缓存单盘200 MB/s、并发10，光驱读取100 MB/s、写入10 MB/s；所有数值均可由设备组覆盖。

## 8. 性能模型

### 8.1 有效带宽

对普通设备组：

```text
B_internal = Σ(active_device_count_i × per_device_bandwidth_i)
B_effective = min(B_external, B_internal)
```

其中 `active_device_count_i` 不超过设备数量和该组并发上限。多个设备组的并发分配由配置或策略决定。

### 8.2 请求响应时间

完整响应时间由四部分组成：

```text
L_response = L_queue + L_power_transition + L_fixed + size / B_effective
```

- `L_queue`：节点已有请求造成的排队时间。
- `L_power_transition`：待机唤醒、开机或光盘机械装载时间。
- `L_fixed`：节点类型对应的固定访问延迟。
- `size / B_effective`：数据传输服务时间。

导师给出的“平均文件延迟”公式对应无排队且节点已可服务的基础值。批量负载和高并发实验必须加入排队时间，否则无法合理计算平均延迟和 P99。

### 8.3 批量操作

批量操作不逐文件简单累加固定延迟。模型应使用：

- 批量总字节数。
- 可并发设备或光驱数量。
- 有效聚合带宽。
- 每批次固定开销。
- 队列中已有工作量。

批量操作的完成时间至少为：

```text
T_batch ≈ queue_work_bytes / B_effective
        + batch_fixed_overhead
        + batch_bytes / B_effective
```

## 9. 访问指标输入

### 9.1 统一访问报告

功耗策略不能只依赖心跳是否存在。每个统计窗口至少上报：

- 窗口开始和结束时间。
- 读请求数、写请求数和其他请求数。
- 读取字节、写入字节。
- 活跃请求数和最大队列深度。
- 设备忙碌时间或利用率。
- 最近一次访问时间。
- 可选的平均延迟和 P99 延迟。

Scheduler 将原始计数转换为 IOPS、吞吐率、并发度和空闲时间，作为功耗策略输入。

### 9.2 真实系统来源

- Real/Virtual/Optical 数据节点周期性随心跳或独立 RPC 上报窗口指标。
- MDS 上报元数据请求窗口指标。
- 同一用户请求只能选择一个权威统计点，避免 FUSE、MDS 和数据节点重复计数。
- 推荐数据节点负责数据 I/O 指标，MDS 负责元数据指标；Scheduler 只聚合，不采集路径级 trace。

### 9.3 仿真来源

文件 trace 先经过 workload executor：

```text
file operation
  -> 元数据定位/放置
  -> 一个或多个 NodeAccessEvent
  -> Scheduler MetricsAggregator
```

标准事件至少包含：模拟时间、节点 ID、请求类型、字节数、是否批量、是否 HDD 命中、关联 ID。这样导师提供的 trace、自研 trace 和长期宏观 trace 都能使用同一功耗与性能模型。

## 10. 四级功耗模型

### 10.1 功耗等级

- `PEAK`：高吞吐、高并发或队列积压状态。
- `MEDIUM`：正常工作但未达到高负载阈值。
- `STANDBY`：近期无访问，保留快速恢复能力。
- `OFF`：长期空闲或已退出服务，功率为零。

`STARTING/STOPPING/REBOOTING` 是启停过程，不是第五种功耗等级。过渡期间功率可以通过配置映射到 PEAK 或 MEDIUM。

### 10.2 基础策略

每类节点分别配置阈值和最短驻留时间。建议第一版采用确定性阈值策略：

```text
高利用率、队列超过阈值              -> PEAK
有活动请求但未达到峰值阈值          -> MEDIUM
空闲超过 standby_after              -> STANDBY
继续空闲超过 off_after 且允许关闭    -> OFF
STANDBY/OFF 收到请求                 -> 启动或唤醒 -> MEDIUM/PEAK
```

为避免状态在阈值附近频繁抖动，必须支持：

- 升级与降级采用不同阈值。
- 每个等级的最短驻留时间。
- 冷却时间和最近状态变更时间。
- 容量安全水位和最小在线节点数。
- 有数据但未完成排空的节点禁止直接 OFF。

### 10.3 功率与能耗

内部功率统一用毫瓦整数，能耗用焦耳或毫焦耳累计：

```text
energy_joules += power_watts × elapsed_seconds
```

每次时间推进或功耗等级变化前，先按旧功率积分到当前时间，再切换状态。统计输出包括：

- 当前总功率和各类节点功率。
- 各功耗等级节点数量。
- 累计能耗以及按节点类型、节点和时间区间的分解。
- 状态转换次数、唤醒次数和关闭次数。

## 11. 可靠性、寿命与退出

### 11.1 生命周期时间

每个节点记录：

- `join_time`：加入系统时间。
- `lifetime`：抽样或显式配置的寿命。
- `planned_exit_time = join_time + lifetime`。
- `actual_exit_time`：真正完成退出的时间。

存储节点和元数据节点寿命均值为 5 年，光盘库节点寿命均值为 10 年。导师只给出了正态分布均值，没有给出标准差。标准差必须配置，抽样结果必须截断为正数，并记录随机种子以保证实验可重复。

### 11.2 可靠性参数

“可靠性”不能只存一个无单位浮点数。配置和接口至少要明确采用以下哪一种：

- 年失效率。
- MTBF/MTTR。
- 给定时间区间的存活概率。
- 可用度。

在导师确认前，模型保留结构化可靠性配置，不提供未经定义的默认数值。

### 11.3 失效处理

节点可能因心跳超时、故障注入或可靠性抽样进入 `FAILED`：

1. 立即从新分配候选中移除。
2. 加入派生的失效节点列表。
3. 触发现有主备 failover 或恢复流程。
4. 记录失败时间、原因和受影响容量。
5. 根据策略选择修复后重新加入，或转入退出流程。

## 12. 节点加入与删除

### 12.1 加入流程

```text
Add/Register request
  -> 校验 node_id、类型、地址、设备组和时间参数
  -> JOINING
  -> 启动或等待首次心跳
  -> 健康检查和能力确认
  -> WORKING
  -> 加入 MDS 可分配视图
```

支持单节点和批量加入。首次心跳隐式注册作为兼容模式保留，但正式实验建议使用显式注册，以便在节点启动前生成寿命、计划退出时间和硬件档案。

### 12.2 删除流程

```text
Remove request
  -> DRAINING，停止新分配
  -> 请求 MDS/数据迁移模块迁出现有数据
  -> 等待剩余对象和活动请求归零
  -> EXITING，执行 StopNode
  -> RETIRED，记录 actual_exit_time 和 tombstone
```

强制删除只用于已失效或实验故障注入场景，必须返回可能丢失的数据和容量影响。退休节点保留 tombstone，避免旧进程心跳导致同 ID 被无意重新创建。

### 12.3 Virtual Pool 语义

必须区分：

- 增删承载 virtual pool 的进程。
- 调整 pool 内逻辑节点数量。
- 在长期仿真中增删独立模拟节点。

调整 `logical_node_count` 会改变 PG 放置空间，必须产生新的拓扑 generation，并由 MDS 触发受控迁移，不能仅修改一个整数。

## 13. 时间模型

系统提供两种运行方式：

- `REAL_TIME`：来自真实节点心跳和系统时间。
- `SIMULATED_TIME`：由仿真器显式推进，可以在短时间内模拟多年。

核心模型不得直接散落调用 `system_clock::now()`，而应依赖统一 `Clock`。模拟时间必须单调递增；迟到事件不允许回退节点的最近访问时间和能耗积分时间。

长期演进事件包括：批量写入、批量读取、随机背景读取、节点加入、节点寿命到期、故障、节点退出和设备换代。

## 14. 对外接口边界

接口名称在实现阶段可调整，但能力应覆盖：

- `RegisterNode` / `BatchRegisterNodes`
- `RemoveNode` / `BatchRemoveNodes`
- `GetNode` / `ListNodes`
- `ReportNodeMetrics`
- `ReportNodeAccessEvent`，主要用于仿真和测试
- `GetPowerAndEnergyView`
- `GetLifecycleView`
- `AdvanceSimulationTime`，仅仿真模式启用
- 现有 `ReportHeartbeat`、`GetClusterView`、`StartNode/StopNode/RebootNode`

列表接口必须支持按节点类型、生命周期、健康、功耗等级过滤并分页。`GetClusterView` 保持面向 MDS 的精简可分配视图，不一次返回所有模拟细节。

## 15. 持久化与一致性

- 节点目录、配置、生命周期、功耗、最近指标、累计能耗和随机种子进入版本化快照。
- 快照采用临时文件加原子 rename，并支持启动恢复。
- 管理操作带 request ID，重复请求幂等。
- 每次可观察状态变化递增 generation。
- 节点指标使用单调 sequence 或窗口结束时间去重，旧窗口不能覆盖新状态。
- 运行快照存放在 `SCHEDULER_ROOT` 下，不提交 Git。

## 16. 配置组织

配置分为四层：

1. 全局策略：时钟模式、tick、快照、最小在线容量。
2. 节点类型模板：三类节点的默认硬件、延迟、功率、寿命和可靠性。
3. 单节点覆盖：特殊设备组、地址、加入时间和计划退出时间。
4. 实验场景：随机种子、批量节点数量、访问 trace 和故障注入。

不建议继续把所有设备组压进简单的单值 `KEY=VALUE`。可以保留旧配置兼容层，但节点模板和多设备组应使用结构化配置文件或可重复配置项。

## 17. 观测与实验输出

至少输出以下时间序列和摘要：

- 各类节点加入、工作、失效、排空、退休数量。
- PEAK、MEDIUM、STANDBY、OFF 节点数量。
- 总容量、可用容量和不可用容量。
- 请求数、吞吐率、平均延迟和 P99 延迟。
- 当前功率和累计能耗，按节点类型分解。
- 节点故障、唤醒、关闭、迁移和换代次数。
- 给定配置和访问强度下的服务能力与排队情况。

所有曲线必须带时间单位、容量单位、带宽单位和能耗单位，并记录配置版本及随机种子。

## 18. 当前代码组织

- `src/msg/scheduler.proto`：扩展节点类型、生命周期、四级功耗、设备组、指标、加入/删除和查询消息。
- `src/scheduler/model/`：统一 `ManagedNode`、设备组、状态和统计结构。
- `src/storage_model/`：与进程无关的公共节点、设备、命名空间、光盘和工作流结构。
- `src/scheduler/model/`：默认模板、性能计算及旧集群视图适配。
- `src/scheduler/power/`：NodeRecord事实表、组合索引、功耗策略、能耗和可靠性事件。
- `src/scheduler/lifecycle/`：复用现有 actuator，扩展加入、排空和退休流程。
- `src/scheduler/persistence/`：版本化快照保存与恢复。
- `src/scheduler/cms/`：Scheduler目录提案发布、CMS提交与紧凑ID回写。
- `src/scheduler/migration/`：排空迁移、复制校验、MDS切换和CMS零引用门禁。
- `src/scheduler/service/`：RPC 参数校验、分页查询和兼容视图。
- `src/scheduler/server/`：模块装配、tick 和快照调度。

`FailureDetector`阈值和 `GetClusterView` 线协议继续保留给旧查询/诊断调用方，但MDS放置不再把它作为事实源。健康状态与节点资产都存入NodeRecord；`ClusterState`不再维护独立目录。公共模型不包含主备组，一个节点按独立资源记录处理。

## 19. 测试计划

### 19.1 模型单元测试

- 多设备组容量与有效带宽计算。
- 三类节点默认延迟公式。
- 光盘 HDD 命中与 DISC miss 两条路径。
- 四级功率映射和分段能耗积分。
- 寿命正态抽样的可重复性和正值截断。
- 乱序访问事件不回退时间。

### 19.2 状态机测试

- JOINING 到 WORKING。
- WORKING 到 DRAINING、EXITING、RETIRED。
- HEALTHY/SUSPECT/FAILED 与主备切换。
- MEDIUM/PEAK 到 STANDBY/OFF 及唤醒。
- 未排空节点禁止正常删除。
- tombstone 阻止旧心跳自动复活。

### 19.3 集成与回归测试

- 保持现有 `GetClusterView` 查询兼容，同时验证MDS只消费CMS提交目录。
- CMS目录快照、紧凑ID和分配游标在MDS RocksDB重启后保持不变。
- Create/迁移/归档位置提交与CMS目录关闭、零引用扫描不存在退役竞态。
- Real、Virtual、Optical、MDS 指标上报。
- 批量加入和批量退出后的 PG generation 变化。
- 快照后重启恢复功耗、能耗和生命周期。
- 同一 workload 在真实时间和模拟时间下产生一致的策略结果。

### 19.4 规模测试

- 10000 个托管模拟节点的 tick 耗时和内存占用；该数字是 Scheduler 压力测试规模，不代表 DISC 数量。
- 1k、10k、100k access event/s 下的 CPU 与队列积压。
- 多年仿真中的事件数量、快照大小和可重复性。

## 20. 分阶段实施

1. **已完成主体**：统一数据模型、默认节点模板、性能公式、寿命抽样和纯计算单元测试；当前以真实/模拟时间双入口实现所需时间隔离，全局Clock类只是可选内部重构。
2. **已完成第二版**：显式加入、派生列表、可核验排空、退休、tombstone 和 managed catalog 快照恢复。
3. **已完成第二版**：窗口指标 RPC、幂等重试、四级功耗策略与能耗统计。
4. **已完成主要链路**：MDS、Real、Virtual 和 Optical 自动指标上报；光盘访问分路精度待完善。
5. **已完成当前闭环**：trace adapter、单事件RPC、模拟节点墙钟隔离、显式时间推进及正态/浴盆可靠性失效事件。
6. **已完成当前闭环**：v2 managed快照恢复、批量RPC、代际替换、长期演进和前端所需节点/时序视图。
7. **已完成节点目录收敛**：心跳、旧集群视图、托管API、健康检测、能耗与快照共用NodeRecord。
8. **已完成CMS权威闭环**：目录提案、不可变紧凑ID、RocksDB恢复、MDS真实放置、并发退役门禁和零引用复核。

旧RPC继续可运行，但其group/role字段仅作单节点兼容投影；新功能以公共NodeRecord为准。

## 21. 实验配置口径

1. 最新设计按每库10000张、单盘1 TB、机械取盘60秒、寻道0.5秒实现。
2. 默认标准差为存储/元数据1年、光盘库2年，最小寿命1年；实验可覆盖并必须记录。
3. HDD缓存、光驱读和光驱写分别建模；默认聚合上限依次为2 GB/s、1 GB/s、100 MB/s。
4. 功率和容量按NodeRecord汇总，不自动乘以virtual pool的`logical_node_count`。
5. 默认禁止有驻留数据节点自动OFF；各类最低在线数和存储容量水位由部署配置明确。
