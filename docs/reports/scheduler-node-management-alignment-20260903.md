# ZBStorage Scheduler 与节点管理实现及跨模块对齐报告

> 汇报日期：2026-09-04
> 代码与实验基线：2026-09-03 当前仓库
> 权威需求基线：`docs/ZBStorage设计方案报告20260816.docx`，并结合导师后续给出的三类节点属性说明
> 本报告目的：说明 Scheduler/节点管理组已经完成什么、如何对应权威设计、使用了哪些公共数据结构，以及剩余事项由谁负责。

## 1. 汇报结论

当前 Scheduler 与节点管理的软件责任范围已经形成完整闭环：

```text
节点注册与初始化
  -> readiness + 心跳/设备清单
  -> Scheduler统一NodeRecord
  -> 健康、生命周期、功率、能耗、可靠性决策
  -> Scheduler向CMS提交节点目录提案
  -> CMS权威提交成员、紧凑ID和写入准入
  -> MDS依据CMS目录执行真实I/O放置
  -> 节点排空、复制校验、inode切换、源清理
  -> CMS确认零引用
  -> 节点断电并进入Retired tombstone
```

已经完成并验证的核心能力包括：

- 存储节点、元数据节点、光盘库节点的统一属性模型；
- 加入、工作、排空、退出、退休、故障和恢复状态管理；
- 四级功率、策略能耗、物理执行能耗和长期可靠性模型；
- CMS权威节点目录、不可变紧凑ID、目录持久化与恢复；
- 基于实际容量、健康、readiness、功率和负载的真实I/O调度；
- 真实存储节点排空迁移以及CMS零引用安全退役；
- 光盘库聚合状态、只读降级门禁、暂存区80%背压和零光盘退役门禁；
- Scheduler快照恢复、CMS目录恢复、Trace/长期模拟以及完整多进程实验。

需要准确区分：当前通过的是 Scheduler、节点管理、CMS准入、MDS放置和真实磁电节点I/O闭环；光盘库内部机械臂/光驱调度、物理换盘、固定镜像格式、纠删码以及完整光盘数据面由其他模块负责，当前尚不能宣称整个 Optical 数据面完成。

当前进度应按责任边界表达，而不建议用一个混合百分比：

| 范围 | 进度判断 |
|---|---|
| 本组节点管理与Scheduler核心代码 | 已完成 |
| 为真实放置/退役所需的CMS、MDS边界 | 已完成并通过闭环实验 |
| 真实磁电数据面联调 | 已完成并通过内容一致性、迁移和重启验证 |
| 物理电源与多主机部署验收 | 代码就绪，等待可控硬件环境 |
| 完整Optical归档、读取和物理迁盘闭环 | 未完成，主要依赖Optical、CMS/MDS其他模块 |
| 极限规模与长期稳定性验收 | 基础仿真已通过，仍需按统一实验口径补充压力测试 |

## 2. 责任边界

### 2.1 Scheduler 与节点管理组负责

| 职责 | 本组应提供的结果 | 当前状态 |
|---|---|---|
| 公共节点属性 | 三类节点统一身份、硬件、容量、带宽、延迟、功率、可靠性结构 | 已完成 |
| 节点目录 | 注册、readiness、心跳、设备清单、唯一运行记录 | 已完成 |
| 状态列表 | 工作、加入、失效、排空、退休列表从同一事实表派生 | 已完成 |
| 生命周期 | Joining、Working、Draining、Exiting、Retired及安全门禁 | 已完成 |
| 健康管理 | Healthy、Suspect、Failed、心跳超时、注入故障和修复 | 已完成 |
| 调度准入 | 健康、运维、服务模式、功率、readiness、设备健康联合过滤 | 已完成 |
| 调度权重 | 内外带宽、功率档位、利用率、队列深度综合权重 | 已完成 |
| 功率与能耗 | Peak/Medium/Standby/Off、状态防抖、双口径能耗积分 | 已完成 |
| 可靠性 | 正态寿命、浴盆曲线FIT、MTBF/MTTR、代际替换 | 已完成 |
| 排空编排 | 枚举、复制、回读校验、MDS位置切换、源清理、进度与重试 | 已完成 |
| CMS协作 | 发布完整目录提案，接受CMS分配的紧凑ID和提交结果 | 已完成 |
| 安全退役 | 停止新写、零对象、零inode引用、断电确认、Retired tombstone | 已完成 |
| 仿真与Trace | 单事件、批量事件、模拟时钟、长期推进、时序结果 | 已完成 |
| 状态恢复 | Scheduler managed快照和CMS目录/ID恢复 | 已完成 |

### 2.2 为闭环补充、但权威仍属于CMS/MDS的边界代码

权威文档规定CMS是全局命名空间、inode和存储实体注册的权威。因此本组为打通调度闭环补充了最小CMS/MDS边界实现，但没有接管CMS和MDS的全部业务：

当前仓库尚未拆出独立CMS服务进程，`CmsNodeRegistry` 由 `mds_server` 装配，作为现阶段CMS权威边界运行。它与“Scheduler只提案、CMS决定、MDS消费”的职责关系一致；未来CMS组拆分独立服务时，应迁移该Registry及持久化逻辑，而不是把权威退回Scheduler。

- CMS接收或拒绝Scheduler的完整目录提案；
- CMS分配存储/元数据16 bit紧凑ID和光盘库24 bit紧凑ID；
- CMS提交后才更新MDS的 `NodeStateCache` 和PG；
- CMS扫描现行统一inode，验证待退役节点的磁盘/光盘引用为0；
- CMS目录、generation和ID分配游标写入MDS RocksDB，用于进程重启恢复；
- MDS的Create、迁移位置提交、光盘归档位置提交与CMS退役校验共享事务门禁。

这些代码实现的是Scheduler与CMS/MDS之间的合同。CMS命名空间、Masstree、元数据上下沉、归档业务策略等仍由对应负责人维护。

### 2.3 明确不属于本组的功能

| 功能 | 责任模块 |
|---|---|
| POSIX路径、目录、rename/unlink及完整inode业务 | Client/FUSE、MDS |
| RealNode对象实际读写与磁盘文件格式 | Real DataNode |
| VirtualNode是否保存真实payload | Virtual DataNode/仿真负责人 |
| DiscID到SlotID明细映射和物理盘库存 | Optical Controller |
| 光驱读写队列、空闲光驱池、机械臂最短路径和预取流水线 | Optical Controller |
| 光盘物理迁移及新库槽位重建 | Optical运维/Controller |
| 固定10 GB镜像、Disc-SubNamespace、校验和和镜像链 | Optical ImageStore |
| `(10+4)`纠删码编码、恢复和缺失镜像处理 | Optical/EC模块 |
| DL-SubNameSpace、`.smb`、NS-leaf下沉与上浮 | CMS与Optical元数据模块 |
| CMS集群高可用、一致性协议和全局命名空间扩展 | CMS负责人 |

## 3. 与权威设计文档的逐项对应

### 3.1 CMS是权威所有者

权威文档要求：

- CMS是全局命名空间、inode、用户/命名空间字典和存储实体注册表的权威；
- 存储节点注册表使用16 bit NodeID；
- 光盘库注册表使用24 bit LibraryID；
- 客户端和数据节点不能绕过CMS形成另一套矛盾事实。

当前对应实现：

- Scheduler只维护节点的运行观测和策略状态，只能向CMS提交提案；
- CMS校验提案generation、成员完整性、重复成员和Retired复活；
- 紧凑ID只由CMS分配，分配后不可变；
- MDS不再轮询Scheduler旧 `GetClusterView` 直接覆盖放置缓存；
- 只有CMS提交成功才改变MDS写入准入；
- `GetClusterView` 只作为兼容查询接口保留，不是MDS事实源。

权威文档描述节点注册表可常驻CMS内存。当前实现仍以CMS内存目录提供运行期权威，同时额外在RocksDB保存恢复快照。这是为了保证CMS重启后紧凑ID不因Scheduler发布顺序发生变化，属于工程可靠性增强，不改变运行期权威关系。

对应代码：

- [`CmsNodeCatalogPublisher`](../../src/scheduler/cms/CmsNodeCatalogPublisher.cpp)
- [`CmsNodeRegistry`](../../src/mds/allocator/CmsNodeRegistry.cpp)
- [`CmsNodeCatalogSnapshot`](../../src/msg/mds.proto)

### 3.2 节点生命周期

权威文档的存储节点状态为：

```text
Online -> Maintenance -> Offline -> Retired
```

代码为了表示初始化和排空进度，将其细化为：

| 权威文档状态 | 当前代码状态 | 含义 |
|---|---|---|
| 部署/初始化过程 | `Joining` | 服务、清单或元数据尚未全部ready |
| `Online` | `Working` | 可以参与服务和调度 |
| `Maintenance` | `Draining` | 已关闭新写，正在排空数据 |
| `Offline` | `Exiting` | 数据与引用已清空，正在停止服务/断电 |
| `Retired` | `Retired` | 终态，保留tombstone和历史统计 |

除生命周期外，还使用三个正交维度避免状态含义混杂：

- `HealthState`：Healthy、Suspect、Failed；
- `AdministrativeState`：Enabled、Draining、Disabled；
- `NodeServiceMode`：ReadWrite、ReadOnly。

工作、加入、失效、排空、退休列表不单独维护，而是从唯一 `NodeRecord` 按状态筛选。这样满足权威文档的列表要求，同时避免多份列表产生不一致。

### 3.3 存储节点安全退役

权威文档要求：节点退出前迁移缓存文件，且只有CMS确认不存在任何inode指向该节点后，才能Retired并断电。

当前流程为：

1. `RemoveManagedNode` 将节点置为Draining；
2. Scheduler发布新目录，CMS先关闭该节点的新写准入；
3. Scheduler从真实节点枚举对象并按inode组织；
4. 选择仍被CMS准入的物理目标，待机目标先唤醒；
5. 分块复制并从目标回读逐字节校验；
6. MDS使用比较并交换方式原子切换inode位置；
7. 位置提交成功后才删除源对象；
8. 再次枚举源节点，确认不存在遗漏对象；
9. CMS扫描统一inode，确认磁盘引用和光盘引用均为0；
10. 物理节点确认Off后进入Retired，并保留tombstone。

代码还增加了并发保护：普通Create、迁移位置提交和归档位置提交持有共享门禁；CMS目录关闭和最终引用扫描持有独占门禁。因此不会在“扫描为零”和“进入Retired”之间重新产生指向旧节点的inode。

对应代码：

- [`DrainMigrationCoordinator`](../../src/scheduler/migration/DrainMigrationCoordinator.cpp)
- [`BrpcObjectMigrationBackend`](../../src/scheduler/migration/BrpcObjectMigrationBackend.cpp)
- [`MdsServiceImpl`](../../src/mds/service/MdsServiceImpl.cpp)

### 3.4 三类节点默认属性

当前默认值以2026-08-16权威文档为主，并使用导师后续属性说明补齐元数据节点模型。

| 属性 | 存储节点 | 元数据节点 | 光盘库节点 |
|---|---:|---:|---:|
| 默认设备 | 24×18 TB HDD | 16×4 TB SSD | 10×28 TB HDD缓存、10,000×1 TB DISC、10光驱 |
| 外部网络 | 10 GB/s | 10 GB/s | 10 GB/s |
| 内部并发 | 5块HDD | 10块SSD | 10光驱；缓存与光驱分别建模 |
| 单设备带宽 | HDD 200 MB/s | SSD 5 GB/s | 读100 MB/s、写10 MB/s |
| 固定延迟 | 5 ms | 0.5 ms | 缓存命中0.5 ms；机械取盘60 s、寻道0.5 s |
| 四级功率 | 1000/300/100/0 W | 500/200/50/0 W | 400/200/50/0 W |
| 平均寿命 | 5年 | 5年 | 10年 |

说明：导师较早补充曾给出存储节点 `28 TB×40`，2026-08-16权威文档给出 `18 TB×24=432 TB`。当前按最新权威文档采用后者，设备组仍支持配置多组设备和覆盖默认值。

### 3.5 功率、性能和能耗

权威文档要求存储节点使用Peak、工作中值、Standby、Off四级功率，光盘库使用Peak、Partial、Idle、Off，并采用功率—性能线性模型。

代码统一命名为：

| 公共枚举 | 存储/元数据语义 | 光盘库语义 |
|---|---|---|
| `Peak` | 峰值 | Peak |
| `Medium` | 中值 | Partial |
| `Standby` | 待机 | Idle |
| `Off` | 关闭 | Off |

有效服务带宽取内部并发聚合带宽和外部网络带宽的较小值，再按功率档位进行线性缩放。功耗等级与Start/Stop/Reboot过渡状态分离。

能耗使用整数单位精确积分：

```text
energy_microjoules += power_milliwatts × elapsed_milliseconds
```

系统分别保存：

- `power_level` / `energy_microjoules`：策略模型口径；
- `actuated_power_level` / `actuated_energy_microjoules`：物理执行确认口径。

因此在未开启真实关机命令时，实验不会把“策略希望关机”误写成“硬件已经实现节能”。

### 3.6 光盘库只读、背压和退役

权威文档要求：

- 光盘库经历Active → ReadOnly → Retired；
- 所有非空槽位均为Recorded且写任务为0时才能ReadOnly；
- L2暂存缓存达到约80%时，CMS停止向该库分配新归档；
- 光盘库退役前必须物理迁移本地光盘，最终满足零本地光盘；
- DiscID→SlotID由光盘库本地维护。

当前本组已完成：

- 接收槽位总数和Blank/Recording/Recorded/Defective聚合计数；
- 接收 `pending_write_tasks`、staging容量和使用量；
- “全部Recorded且待写为0”的自动ReadOnly门禁；
- CMS 80% staging背压；
- ReadOnly库不再接收新写；
- 零本地光盘退役门禁。

当前本组没有实现且不应在Scheduler内部实现：逐盘DiscID→SlotID明细、光驱内部队列、机械臂动作和物理光盘迁移。Scheduler只管理库级准入、状态和流程门禁。

## 4. 当前公共数据结构

### 4.1 公共结构的定位

公共结构位于 [`src/storage_model`](../../src/storage_model/README.md)，特点是：

- 不依赖Scheduler、MDS、DataNode、brpc、RocksDB和Protobuf；
- 表达跨模块共享的领域语义，不直接承诺磁盘二进制布局；
- Scheduler直接使用节点公共结构；
- Scheduler兼容层继续通过 `ManagedNode.h` 和 `NodePowerManager.h` 导出旧名称；
- 跨进程通过 `ManagedNodeProtoAdapter` 与 `scheduler.proto`/`mds.proto` 转换；
- MDS、DataNode和Optical模块不需要直接包含公共C++头文件，避免干扰其他组并行开发。

### 4.2 `Identifiers.h`

| 结构/类型 | 关键字段或约束 | 当前使用情况 |
|---|---|---|
| `FileId` | 2 B UserID + 2 B NamespaceID + 10 B目录哈希 + 10 B文件名哈希 | 公共目标模型；当前MDS仍使用现有inode格式 |
| `StorageNodeCompactId` | 16 bit | CMS节点ID语义 |
| `OpticalLibraryId` | 低24 bit有效 | CMS光盘库ID语义 |
| `OpticalDiscId` | 32 bit，0为空盘 | Optical公共模型 |
| `OpticalImageId` | 低40 bit有效 | 镜像公共模型 |
| `OpticalSlotIndex` | 16 bit | 每库10,000槽位 |

### 4.3 `NodeModel.h`：本组已经实际接入的核心结构

#### `NodeHardwareProfile`

描述注册/配置阶段的规划属性：

- `node_id`、`kind`、`execution_mode`；
- `logical_node_count`、`technology_generation`；
- `participates_in_placement`；
- 外部网络带宽、固定访问延迟、光盘取盘/寻道延迟；
- `device_groups`；
- `power`；
- `reliability`。

#### `DeviceGroup`

描述一组同类规划设备：设备类型、单设备容量、数量、最大并发、通用带宽、读带宽和写带宽。规划值与心跳实际值严格分离。

#### `NodeIdentity`

保存稳定字符串ID、CMS紧凑ID、节点类型、执行模式和服务地址。字符串ID继续作为RPC和运维主键；紧凑ID用于权威目录和未来介质编码。

#### `DeviceInventory` / `NodeInventorySnapshot`

保存心跳看到的实际设备：

- device/disk ID和类型；
- 总容量、空闲容量、健康状态；
- 实际或配置读写带宽；
- 累计写入量和观测时间；
- 整体清单generation。

Scheduler容量水位、MDS写入准入和调度权重使用实际清单，不用默认模板覆盖部署值。

#### `NodeReadinessStatus`

包含：

- `initialization_complete`；
- `inventory_ready`；
- `metadata_ready`；
- 观测时间和说明。

三项全部为真后节点才可以进入Working。普通心跳只证明“进程存在”，不能代替初始化就绪。

#### `OpticalLibraryStatus`

保存Scheduler需要的库级聚合信息：槽位总数、本地盘数、四种光盘状态计数、staging容量/使用量、待写任务数和观测时间。详细光盘明细不放入Scheduler记录。

#### `NodeRecord`

`NodeRecord` 是Scheduler唯一完整节点事实记录，组合：

| 字段组 | 内容 | 主要使用方 |
|---|---|---|
| 规划与身份 | profile、identity、inventory、readiness | 注册、心跳、CMS目录 |
| 状态 | lifecycle、health、administrative_state、service_mode | 列表、准入、退役 |
| 功率 | power_level、actuated_power_level、transition | 功率策略、执行器 |
| 生命周期时间 | 加入、抽样寿命、计划/实际退出时间 | 长期演进、成本统计 |
| 负载 | heartbeat、访问、指标序列、利用率、队列、请求数 | 健康与功率决策 |
| 能耗 | 策略/物理能耗及最后积分时间 | 汇总、时序输出 |
| 容量与写入 | resident bytes、累计写入量 | 关机和寿命策略 |
| 排空 | 剩余对象、字节、活动请求 | 迁移和退休门禁 |
| 可靠性 | 失效来源、原因、随机种子、下次失效、修复时间 | 仿真与故障恢复 |
| 光盘库 | `OpticalLibraryStatus` | ReadOnly、背压、退役 |

#### `NodePowerSummary` / `NodePowerSample`

提供节点类型/状态数量、四级功率数量、总功率、累计能耗、容量、累计写入和转换次数，供实验、前端和汇报使用。

### 4.4 `OpticalModel.h`：公共定义已完成，尚未全面接入Optical数据面

已经定义：

- `OpticalLibraryState`、`DiscState`、`OpticalDriveType`、`ResourceState`；
- `DiscSlot`、`DiscSlotIndex`及连续段/孤立点快照；
- `OpticalDisc`、`OpticalDrive`、`MechanicalArm`；
- `OpticalLibraryInventory`；
- `ImageSuperblock`、`ImageRegionLayout`、`ImageFileExtent`、`OpticalImageDescriptor`；
- `ErasureCodeProfile/Member/Group`。

其中槽位唯一性、光盘归属、容量和Retired零本地光盘等校验已有公共实现。但当前OpticalNode尚未把其内部ImageStore、机械臂和光驱调度全面迁移到这些结构，因此这些类型不能被汇报为完整数据面已经落地。

### 4.5 `NamespaceModel.h`：与CMS/MDS后续工作对齐的公共定义

已经定义 `FileRecord`、POSIX属性、磁电/光盘extent、`NamespaceLeaf`、`DiscSubNamespaceEntry`、`NamespaceBundleDescriptor`、目录标记和Rename重定向。

当前Scheduler只使用其中的位置语义参与退役引用判断，不拥有命名空间。MDS现有 `UnifiedInodeRecord` 和256 B格式继续有效；是否迁移到完整公共 `FileRecord` 由CMS/MDS负责人决定。

### 4.6 `WorkflowModel.h`：归档和批量读的公共合同

已经定义：

- `SourceSegment`、`ChunkFileSlice`、`TransferChunk`；
- `SegmentManifest`；
- `ArchiveLocationUpdate`；
- `ReadOperation`、`DiscReadGroup`、`LibraryReadPlan`、`BatchReadPlan`。

这些结构用于未来跨模块归档/读取RPC对齐。当前Scheduler真实排空使用现有对象RPC与MDS位置提交协议，不代表完整光盘归档流水线已经切换到 `SegmentManifest`。

### 4.7 公共结构当前接入矩阵

| 公共结构 | Scheduler | CMS/MDS | DataNode/Optical | 结论 |
|---|---|---|---|---|
| NodeHardwareProfile/NodeRecord | 直接使用 | Protobuf消费目录投影 | Protobuf上报 | 已实际接入 |
| DeviceInventory | 直接使用 | 转为NodeInfo/DiskInfo | 心跳生产 | 已实际接入 |
| NodeReadinessStatus | 直接使用 | 参与CMS准入 | 节点启动后上报 | 已实际接入 |
| OpticalLibraryStatus | 直接使用 | CMS执行80%背压 | Optical聚合上报 | 已实际接入 |
| NodePowerSummary/Sample | 直接生成 | 不需要 | 不需要 | 已实际接入 |
| DiscSlotIndex/OpticalDisc | 只消费聚合结果 | 只保存库级身份 | 尚未全面迁移 | 公共定义完成，数据面待接入 |
| Image/ErasureCode结构 | 不负责内部执行 | 位置合同待扩展 | 尚未全面迁移 | 公共定义完成，外部依赖 |
| FileRecord/NamespaceLeaf | 不拥有 | 现有格式继续使用 | 局部元数据待接入 | 公共定义完成，CMS/MDS后续 |
| SegmentManifest/BatchReadPlan | 不负责盘内调度 | RPC尚未统一 | 归档/读流水线待接入 | 公共定义完成，跨组后续 |

## 5. 当前代码数据流

### 5.1 节点加入

```text
DataNode启动RPC服务
  -> RegisterManagedNode/首次兼容注册
  -> 上报设备清单心跳
  -> ReportManagedNodeReadiness
  -> NodePowerManager更新NodeRecord
  -> readiness完整后Joining -> Working
  -> Scheduler提交CMS目录
  -> CMS批准后进入MDS候选集
```

### 5.2 调度与真实I/O

```text
Client/FUSE发起Create
  -> MDS读取CMS已提交NodeStateCache
  -> 过滤Working + Healthy + Enabled + ReadWrite + Ready + 已唤醒
  -> 检查健康且有空闲容量的设备
  -> 按有效带宽、功率、利用率、队列深度加权
  -> 返回真实node/address/disk
  -> Client直写DataNode
```

元数据节点虽然由Scheduler统一管理和统计，但 `participates_in_placement=false`，不会被错误选作用户数据节点。

### 5.3 指标、功率和能耗

```text
Real/Virtual/Optical/MDS聚合操作指标
  -> ReportNodeMetrics（reporter epoch + sequence幂等）
  -> NodeRecord更新利用率、队列、请求、字节和最近访问
  -> PowerPolicyEngine选择Peak/Medium/Standby/Off
  -> 先按旧功率积分能耗，再切换档位
  -> 可选PowerActuationController执行物理命令
  -> Summary/History输出
```

### 5.4 排空退役

```text
RemoveManagedNode
  -> Draining并由CMS关闭新写
  -> DrainMigrationCoordinator复制与校验
  -> MDS原子切换位置
  -> 删除源对象并二次枚举
  -> CMS零inode引用校验
  -> Exiting + 物理Off确认
  -> Retired tombstone
```

## 6. 已完成实现清单

### 6.1 节点属性与管理

- 三类节点默认模板和自定义多设备组；
- 字符串稳定ID、CMS紧凑ID和技术代际；
- Physical、Virtual、Simulated执行模式；
- 显式注册、批量注册和代际替换；
- 服务地址、设备清单和readiness；
- 分页/组合查询、单节点详情和派生列表；
- 退休tombstone阻止同ID旧进程复活；
- Scheduler v2状态快照和恢复。

### 6.2 健康、生命周期和可靠性

- 心跳超时的Healthy/Suspect/Failed转换；
- Joining/Working/Draining/Exiting/Retired；
- 注入故障、手工修复和自动修复；
- 正态寿命抽样和最小寿命截断；
- 浴盆曲线早期/稳定/磨损FIT；
- MTBF/MTTR、确定性随机种子和故障计数；
- 到寿命后的受控排空退出；
- 前任节点和technology generation替换链。

### 6.3 功率和能耗

- Peak/Medium/Standby/Off四档；
- 启停过渡与功耗档位分离；
- 利用率、队列、指标新鲜度和空闲时间策略；
- 最小驻留时间防抖；
- 有驻留数据默认禁止自动Off；
- 最低在线节点数、容量和空闲容量水位；
- 策略功率/物理执行功率双口径；
- 功率执行失败记录和退避重试；
- 节点、类型、集群和时间序列汇总。

### 6.4 CMS权威调度

- 完整目录提案和generation；
- 成员遗漏、重复、陈旧版本和Retired复活检查；
- 16/24 bit紧凑ID分配；
- CMS目录和分配游标RocksDB恢复；
- 只有CMS提交才能更新MDS候选；
- 实际清单容量/健康过滤；
- 读写带宽、外网、功率和负载加权；
- Metadata不参与用户数据放置；
- Optical staging 80%背压；
- 退役节点不再接收新文件。

### 6.5 排空和安全退役

- 真实对象枚举和按inode分组；
- 分块复制、目标回读校验和重试；
- 只选择具有真实字节保持语义的物理迁移目标；
- 目标处于Standby/Off时请求唤醒；
- MDS原子位置切换后才删除源；
- 源端二次枚举；
- CMS磁盘/光盘inode零引用复核；
- 普通存储、元数据和光盘库差异化退役门禁；
- `force`不能绕过零数据、零光盘和断电确认。

## 7. 测试与实验结果

### 7.1 构建与自动测试

- 仓库内RocksDB已完整构建；
- 全项目构建成功；
- 项目CTest共10项，10/10通过；
- Trace Python测试3/3通过；
- `git diff --check`通过。

测试覆盖公共模型、节点状态机、能耗、指标幂等、快照恢复、managed RPC、排空迁移、CMS目录/ID/持久化、加权放置和光盘80%背压。

### 7.2 多进程闭环实验

实验目录：

```text
/mnt/md0/zbstorage_cms_scheduler_20260903_final_qs5nxS
```

启动组件：Scheduler、MDS、两个RealNode、VirtualNode、OpticalNode和FUSE。

核心联机冒烟7/7通过：

- Scheduler集群视图；
- CMS权威目录；
- CMS调度到真实节点后的精确对象写读；
- MDS命名空间操作；
- RealNode精确对象I/O；
- VirtualNode约定的仿真I/O语义；
- Optical精确payload项按已知外部缺口单独标记，没有混入核心通过结论。

### 7.3 排空实验证据

12 MiB随机文件迁移前后SHA-256一致：

```text
eec0dff194c0414ccd2c1499b2689dfe92bdf9f5ac1dc78151ebcc6a9a0dfb2b
```

- 3个对象、12,582,912字节迁移完成；
- 旧节点对象枚举为空；
- 旧节点CMS引用为0；
- 新节点CMS引用为2；
- 旧节点为Retired/Off；
- 退休后新文件只分配到第二节点。

退休后新写5 MiB文件的写读SHA-256一致：

```text
67466727ea528f7c37be6155164a246adaac4c84071d4f6d950d53533cdb158d
```

完整重启两次后文件哈希、退休状态和紧凑ID保持不变：

```text
metadata=1
real-01=2
real-02=3
virtual=4
optical=1（独立24-bit空间）
```

实验进程已正常停止，数据、配置和日志保留在实验目录。

## 8. 尚未完成事项及原因

### 8.1 本组代码已准备好，但仍需环境验收

| 事项 | 当前情况 | 后续需要 |
|---|---|---|
| 真实物理开机/待机/关机 | 执行器、命令模板、确认状态和失败重试已实现；正式实验为避免共享服务器关机而关闭物理执行 | 在可控硬件环境配置命令并验收功率仪/带外管理结果 |
| 多主机网络故障 | 单机多进程、真实外存和独立端口已验证 | 在部署环境测试断网、延迟、节点失联和恢复 |
| 极限规模压力 | 已完成普通Trace、长期模拟和99逻辑虚拟节点实验 | 根据实验资源补充万级节点tick、100k event/s和长时间稳定性测试 |
| 并发退役压力 | 已有事务门禁和功能验证 | 可补充高并发Create/迁移/退役竞态压力测试 |

这些是部署/规模验收项，不是当前核心状态机或调度链路缺少实现。

### 8.2 因其他模块尚未完成而不能闭环的事项

#### Optical精确数据读取

现有 `ImageStore::ReadObject()` 在 `SIMULATE_IO=false` 时仍返回等长的合成字符 `x`。因此Optical通用对象接口目前不能证明写入payload与读出payload逐字节一致。

影响：

- 不能将Optical精确内容一致性记为全系统测试通过；
- 不影响Scheduler对Optical节点的注册、功率、健康、聚合状态和CMS背压；
- 修复责任属于Optical数据面/ImageStore。

#### 光盘库内部控制

Scheduler已经有 `OpticalLibraryStatus` 和门禁，但以下信息尚无完整生产方：

- 精确DiscID→SlotID映射；
- WriterOnly/ReaderOnly/ReadWrite光驱实时资源状态；
- 读写队列、空闲池和机械臂任务；
- 物理光盘搬移完成确认；
- DL-SubNameSpace迁移结果。

在Optical Controller提供这些接口前，Scheduler不能替代控制器猜测盘内状态，也不能独立完成光盘库物理退休。

#### 固定镜像、纠删码和元数据下沉

公共结构已定义，但当前ImageStore、CMS/MDS和OpticalNode没有全部切换到：

- 固定10 GB镜像格式；
- 完整Image superblock和Disc-SubNamespace；
- `(10+4)` Reed-Solomon纠删组；
- `.smb`、DL-SubNameSpace、NS-leaf下沉/上浮；
- 统一 `SegmentManifest` 和 `BatchReadPlan` RPC。

这些属于Optical/CMS/MDS的联合功能。Scheduler未来只需要消费任务量、节点准入、队列和完成状态，不应承担镜像格式或命名空间实现。

#### 新inode格式的退役引用枚举

CMS零引用验证目前扫描现行 `I/` UnifiedInodeRecord，并同时检查磁盘和光盘位置。若CMS/MDS负责人新增另一种inode存储格式或独立位置索引，需要同步提供统一引用枚举接口；否则Scheduler无法知道新格式中的隐藏引用。

### 8.3 明确属于本组、但不是功能阻塞的后续优化

- 将真实时间和模拟时间的双入口进一步抽象成统一Clock类；当前时间隔离功能已经实现，此项是内部结构优化；
- 增加更高强度的目录发布、心跳和退役并发基准；
- 根据前端团队需要补充节点功率、容量和可靠性时序的展示适配；底层查询数据已经具备；
- 在确定VirtualNode产品语义后，决定虚拟节点能否成为迁移目标；当前为保证数据安全，迁移明确排除只返回合成payload的VirtualNode。

## 9. 明日跨组对齐需要确认的问题

### 9.1 与CMS/MDS组确认

1. 继续确认CMS是节点成员、紧凑ID和写准入的唯一权威；`GetClusterView`只保留兼容查询。
2. 新inode格式或新位置表必须接入统一“按node_id枚举引用”接口。
3. 是否采用公共 `FileRecord/NamespaceLeaf`，以及现有256 B inode的迁移计划。
4. 归档完成后inode位置更新、L1源释放和DL-SubNameSpace写入的事务边界由谁实现。
5. CMS多副本/主从环境下节点目录快照和ID分配如何进入其正式一致性方案。

### 9.2 与Optical组确认

1. `SIMULATE_IO=false` 是否明确要求返回真实payload；若是，应修复ImageStore读取。
2. DiscID→SlotID、光驱、机械臂和队列状态通过什么RPC向上汇总。
3. 物理迁盘完成、零本地光盘和DL-SubNameSpace迁移的确认接口。
4. ReadOnly是否保持不可逆，若允许维护切回Active，需要明确状态机和权限。
5. 固定10 GB镜像、纠删码、Disc-SubNamespace和批量读写计划的落地顺序。

### 9.3 与Client/FUSE和DataNode组确认

1. RealNode继续保证对象写后读逐字节一致，并保持稳定对象ID规则。
2. VirtualNode是纯性能仿真还是POSIX一致存储；当前Scheduler按纯仿真处理。
3. 指标统计的唯一来源：数据I/O由DataNode统计，元数据操作由MDS统计，避免Client重复计数。
4. 数据节点readiness应在RPC服务已监听、设备清单完成后上报。

### 9.4 与实验/前端组确认

1. 功率展示必须区分策略功率和物理已执行功率。
2. 容量和功率按一条物理/虚拟/模拟 `NodeRecord` 汇总，是否需要额外按 `logical_node_count` 展开必须由实验口径明确。
3. 正式曲线记录随机种子、时间域、单位和配置版本。
4. 极限规模测试采用多少节点、事件率、持续时间和外存资源。

## 10. 建议汇报口径

建议明天使用下面的表述：

> 我们负责的不是光盘库内部机械臂调度，也不是CMS命名空间本身；我们负责的是统一节点模型、节点生命周期、健康、功率能耗、可靠性、放置准入和安全退役。当前这部分已经通过CMS权威目录接到MDS真实放置，并用两个真实节点完成了写入、排空、inode切换、零引用、退休、退休后重新写入和重启恢复闭环。公共节点结构已经由Scheduler直接使用，其他模块通过Protobuf对接。尚未闭环的主要是Optical数据面、物理换盘、固定镜像/纠删码和元数据下沉，这些需要对应模块提供生产者和执行接口；本组已准备好库级状态、准入和退役门禁。

## 11. 主要代码和文档入口

| 内容 | 入口 |
|---|---|
| 公共结构总说明 | [`src/storage_model/README.md`](../../src/storage_model/README.md) |
| 公共节点结构 | [`src/storage_model/NodeModel.h`](../../src/storage_model/NodeModel.h) |
| 光盘公共结构 | [`src/storage_model/OpticalModel.h`](../../src/storage_model/OpticalModel.h) |
| 命名空间公共结构 | [`src/storage_model/NamespaceModel.h`](../../src/storage_model/NamespaceModel.h) |
| 归档/读取工作流结构 | [`src/storage_model/WorkflowModel.h`](../../src/storage_model/WorkflowModel.h) |
| Scheduler详细设计 | [`docs/design/scheduler-power-management.md`](../design/scheduler-power-management.md) |
| Scheduler模块说明 | [`src/scheduler/README.md`](../../src/scheduler/README.md) |
| 节点事实表与功率策略 | [`src/scheduler/power/NodePowerManager.cpp`](../../src/scheduler/power/NodePowerManager.cpp) |
| Scheduler RPC | [`src/scheduler/service/SchedulerServiceImpl.cpp`](../../src/scheduler/service/SchedulerServiceImpl.cpp) |
| CMS目录发布 | [`src/scheduler/cms/CmsNodeCatalogPublisher.cpp`](../../src/scheduler/cms/CmsNodeCatalogPublisher.cpp) |
| 排空迁移 | [`src/scheduler/migration/DrainMigrationCoordinator.cpp`](../../src/scheduler/migration/DrainMigrationCoordinator.cpp) |
| CMS权威目录 | [`src/mds/allocator/CmsNodeRegistry.cpp`](../../src/mds/allocator/CmsNodeRegistry.cpp) |
| MDS放置与退役事务门禁 | [`src/mds/service/MdsServiceImpl.cpp`](../../src/mds/service/MdsServiceImpl.cpp) |
| 闭环测试代码与复现说明（结果留本地） | [`tests/scheduler/README.md`](../../tests/scheduler/README.md) |
