# ZBStorage 公共数据模型

本文说明 `src/storage_model` 中的公共数据结构、字段单位、结构关系，以及它们在 ZBStorage 工作流中的产生方和消费方。

模型以《ZBStorage 设计方案报告 2026-08-16》和本文的最新修订为设计基线，同时保留当前代码已经具备的能力：字符串节点 ID、虚拟/模拟节点、对象切片、心跳实际磁盘清单、四级功率、累计能耗、排空迁移、光盘镜像 extent、CRC32、批量归档和批量读取。

## 1. 定位与边界

本目录是与具体进程无关的领域模型层：

- 不依赖 Scheduler、MDS、DataNode、brpc、RocksDB 或 Protobuf；
- 类型使用 `zb::storage_model` 命名空间；
- `kStorageModelVersion` 表示 C++ 字段语义版本，不是磁盘格式版本；
- 头文件描述逻辑结构，不承诺 C++ 对象的原始内存布局可持久化；
- 跨进程仍使用 `src/msg/*.proto`，磁盘数据仍由各模块的 codec 负责。

目录内容：

| 文件 | 内容 |
|---|---|
| `StorageModel.h` | 可选的总入口头文件 |
| `Identifiers.h` | FileID 与存储实体紧凑 ID |
| `NodeModel.h` | 节点规划属性、实际清单、运行状态、功率和可靠性 |
| `OpticalModel.h` | 光盘库、槽位、光盘、光驱、机械臂、镜像和纠删组 |
| `NamespaceModel.h` | inode 逻辑模型、冷热位置、NS-leaf 和命名空间条目 |
| `WorkflowModel.h` | 批量归档分段、chunk、位置更新和批量读计划 |
| `StorageModel.cpp` | 公共校验、容量汇总、ID格式化和槽位索引实现 |

现有 Scheduler 已经直接复用 `NodeModel.h`：

```text
ManagedNodeKind       -> storage_model::NodeKind
ManagedNodeProfile    -> storage_model::NodeHardwareProfile
ManagedNodeRuntime    -> storage_model::NodeRecord
DiskState             -> storage_model::DeviceInventory
```

因此这些节点类型不是一套无人使用的平行定义。原有 Scheduler API 名称保留，以降低调用方迁移成本。跨进程链路由
`ManagedNodeProtoAdapter` 投影为 Protobuf：DataNode/MDS 上报观测值，Scheduler 维护完整 `NodeRecord`，CMS 审核并提交目录后，MDS
放置缓存才发生变化。

## 2. 单位约定

| 后缀 | 单位 |
|---|---|
| `_bytes` | 字节；硬件设计容量使用十进制，1 TB = 10^12 B |
| `_bytes_per_sec` | 十进制字节/秒 |
| `_us` | 微秒 |
| `_ms` | 毫秒 |
| `_milliwatts` | 毫瓦 |
| `_microjoules` | 微焦耳 |
| `_fit` | 每 10^9 设备工作小时的预计故障数 |

节点运行时间使用执行域时间：物理/虚拟节点使用墙上时间，模拟节点使用仿真时间。

## 3. 标识符

### 3.1 `FileId`

`FileId` 对应设计文档的 24 B 复合标识：

| 字段 | 大小 | 产生方 | 使用位置 |
|---|---:|---|---|
| `user_id` | 2 B | CMS 用户字典 | 用户段表、全局索引 |
| `namespace_id` | 2 B | CMS 命名空间字典 | 段内前缀索引 |
| `directory_path_hash` | 10 B | CMS Create/Lookup | 同目录范围定位、NS-leaf根前缀 |
| `file_name_hash` | 10 B | CMS Create/Lookup | 同目录内文件点查询 |

`FileIdHex()` 生成固定 48 个十六进制字符，便于日志、trace 和文本工具使用。它不是正式二进制 codec。

### 3.2 存储实体 ID

| 类型 | C++承载类型 | 有效位宽 | 说明 |
|---|---|---:|---|
| `StorageNodeCompactId` | `uint16_t` | 16 | 设计中的紧凑 NodeID；当前运行主 ID 仍是字符串 |
| `OpticalLibraryId` | `uint32_t` | 24 | `IsValidOpticalLibraryId()` 校验高8位为空 |
| `OpticalDiscId` | `uint32_t` | 32 | 0保留为空槽 |
| `OpticalImageId` | `uint64_t` | 40 | `IsValidOpticalImageId()` 校验高24位为空 |
| `OpticalSlotIndex` | `uint16_t` | 16 | 光盘库内局部槽位号 |

使用较宽的原生整数承载24/40 bit ID，是为了避免不可移植的位域和未对齐访问。真正写盘时必须由 codec 显式写3 B或5 B。

## 4. 节点数据结构

### 4.1 `NodeIdentity`

稳定身份与拓扑字段：

| 字段 | 含义 | 工作流 |
|---|---|---|
| `node_id` | 当前代码使用的稳定字符串 ID | 配置、心跳、Scheduler和RPC |
| `compact_id` | CMS分配的紧凑 ID，0表示未分配；存储/元数据16 bit，光盘库24 bit | CMS目录、未来 inode/介质编码 |
| `kind` | Storage、Metadata、OpticalLibrary | 注册、筛选、最小在线水位 |
| `execution_mode` | Physical、Virtual、Simulated | 时间域、功率执行方式 |
| `service_address` | 节点服务端点 | 节点寻址和管理操作 |

按照最新设计，公共节点身份不包含 `group_id` 和 `role`：当前不在这套模型中描述分布式主备关系，一个节点按一个独立副本处理。旧 Scheduler 心跳协议中仍存在的主备字段属于既有兼容逻辑，本次不迁入公共模型，也不改动其运行行为。

### 4.2 `DeviceGroup`

`DeviceGroup` 表示规划硬件的一批同类设备，而不是心跳发现的一块实际磁盘。

| 字段 | 含义 |
|---|---|
| `name` | 节点内唯一设备组名 |
| `kind` | HDD、SSD、Disc、OpticalDrive |
| `device_capacity_bytes` | 单设备规划容量；光驱为0 |
| `device_count` | 设备数量 |
| `per_device_bandwidth_bytes_per_sec` | 兼容旧配置的通用带宽 |
| `max_concurrency` | 聚合带宽可并行使用的设备数，0表示全部 |
| `per_device_read_bandwidth_bytes_per_sec` | 单设备读取带宽，0时回退到通用带宽 |
| `per_device_write_bandwidth_bytes_per_sec` | 单设备写入带宽，0时回退到通用带宽 |

分离读写带宽是为了表达光驱约100 MB/s读取、10 MB/s刻录的非对称能力，同时兼容原先只有一个带宽字段的 Scheduler 快照和 RPC。

设备组只描述规划值，不保存实时已用空间；每台设备的实时空间在 `DeviceInventory` 中记录和计算。

### 4.3 `NodeHardwareProfile`

节点注册时的规划/配置值：

| 字段组 | 字段 | 使用位置 |
|---|---|---|
| 身份 | `node_id`、`kind`、`execution_mode` | Scheduler注册和筛选 |
| 逻辑规模 | `logical_node_count` | 一个虚拟节点进程代表多个逻辑节点 |
| 技术代际 | `technology_generation` | 百年推演中的硬件代际；替换节点必须递增 |
| 放置开关 | `participates_in_placement` | Scheduler判断节点是否可参与放置 |
| 网络 | `external_network_bandwidth_bytes_per_sec` | 有效吞吐取内外带宽最小值 |
| 延迟 | `fixed_access_latency_us` | HDD/SSD及光盘缓存命中 |
| 光盘延迟 | `optical_load_latency_us`、`optical_seek_latency_us` | 光盘未命中读取 |
| 硬件 | `device_groups` | 规划容量和聚合带宽 |
| 功率 | `power` | 功率策略与能耗积分 |
| 可靠性 | `reliability` | 寿命采样、失效和修复事件 |

当前默认设计参数：

- 磁电节点：24×18 TB HDD，并发5，内部总带宽1 GB/s；峰值/中值/待机/关闭功率为1000/300/100/0 W；
- 元数据节点：16×4 TB SSD，并发10，内部总带宽50 GB/s；功率为500/200/50/0 W；
- 光盘库：10×28 TB HDD缓存作为现有实现补充，10,000×1 TB光盘，10个光驱，读取100 MB/s、写入10 MB/s；功率为400/200/50/0 W；
- 磁电/元数据寿命均值5年、默认标准差1年；光盘库均值10年、默认标准差2年。

默认寿命标准差属于代码缺省值，可以通过配置覆盖；设计文档只规定了均值，没有规定标准差。

### 4.4 `PowerProfile` 与 `ReliabilityProfile`

`PowerProfile` 包含 `peak/medium/standby/off_milliwatts`。Scheduler 根据访问利用率、队列深度和空闲时间选择档位，并按下式累计能耗：

```text
energy_microjoules += power_milliwatts × elapsed_milliseconds
```

`ReliabilityProfile` 字段：

| 字段 | 含义 |
|---|---|
| `mean_lifetime_ms`、`lifetime_stddev_ms` | 正态寿命采样参数 |
| `minimum_lifetime_ms` | 防止采样出非物理寿命 |
| `mean_time_between_failures_ms` | 当前指数随机失效调度 |
| `mean_repair_time_ms` | 当前自动修复调度 |
| `model` | NormalLifetime或BathtubCurve |
| `early_failure_window_ms` | 浴盆曲线早期失效阶段长度 |
| `wearout_start_age_ms` | 磨损阶段起点 |
| 三个`*_failure_rate_fit` | 早期、稳定期和磨损期故障率 |

当前 Scheduler 同时支持两种失效调度：NormalLifetime 使用可配置 MTBF 的指数间隔，BathtubCurve 根据节点年龄选择早期、稳定期或磨损期 FIT，再转换为该阶段的失效间隔。随机种子、故障次数、下一次故障和修复期限都进入快照，实验可重复。

### 4.5 `DeviceInventory` 与 `NodeInventorySnapshot`

这两个结构表示心跳观测值：

| 字段 | 含义 |
|---|---|
| `disk_id` | 实际磁盘/SSD/光盘设备 ID；沿用当前心跳字段名 |
| `kind` | 设备类型 |
| `capacity_bytes`、`free_bytes` | 心跳容量和剩余容量 |
| `DeviceUsedBytes(device)` | 单设备已用空间，按 `max(capacity_bytes-free_bytes, 0)` 实时派生 |
| `is_healthy` | 实际健康状态 |
| 读写带宽 | 实测或配置带宽 |
| `accumulated_write_bytes` | 寿命与成本统计 |
| `last_update_ms` | 最近一次设备观测时间 |
| snapshot `generation` | 防止旧清单覆盖新清单 |
| snapshot `observed_at_ms` | 整次观测时间 |

`InventoryCapacityBytes()`、`InventoryFreeBytes()` 和 `InventoryUsedBytes()` 分别汇总全部数据设备的总容量、空闲空间和已用空间；`InventoryHealthyCapacityBytes()`与`InventoryHealthyFreeBytes()`只统计健康设备，用于放置和关机安全水位。Scheduler 的 `NodeDiskView.used_bytes` 与文本快照也使用同一计算规则，因此不额外保存一个可能与 `capacity_bytes/free_bytes` 冲突的可写字段。

`NodeHardwareProfile` 是“计划装什么”，`NodeInventorySnapshot` 是“心跳看到什么”。二者必须并存：不能用默认硬件模板覆盖部署中的实际容量，也不能用一次临时心跳永久覆盖规划配置。

### 4.6 `NodeRecord`

`OpticalLibraryStatus` 是节点管理所需的光盘库聚合状态：是否已观测、槽位总数、本地光盘总数、Blank/Recording/Recorded/Defective分类计数、暂存区容量/用量、待处理写任务数及观测时间。详细 DiscID→Slot 映射仍属于 `OpticalModel.h`。Scheduler使用聚合状态判断只读降级和安全退役；CMS在暂存区使用率达到80%时停止向该库分配新归档。

`NodeRecord` 是 Scheduler 当前唯一的完整管理记录：

- `profile`、`identity`、`inventory`：规划属性、稳定身份/服务地址和实际设备清单；
- `lifecycle/health/administrative_state/service_mode`：生命周期、健康、运维开关和读写能力；
- `power_level/actuated_power_level/transition`：策略功率、物理执行结果和启停过程；
- 加入、寿命、计划退出、实际退出时间；
- 最近访问、指标序列和指标报告者 epoch；
- 服务地址、清单和控制端点是否就绪的 `readiness`；只有全部就绪才能从Joining进入Working；
- 功率状态进入时间、策略累计能耗和物理执行累计能耗；
- 驻留字节、利用率、队列、访问次数和累计写入量；
- 唤醒、待机、关机和代际替换计数，以及替换节点的前任ID；
- 排空剩余对象/字节/活动请求；
- 故障来源/原因、下次故障、修复时间和累计故障次数；
- 光盘库聚合状态。

该结构出现在节点注册、指标上报、功率策略、排空迁移、可靠性事件、状态查询和Scheduler快照中。

`NodeCatalogSnapshot` v2 保存格式版本、generation、全部 `NodeRecord` 和时序历史；加载器仍接受v1并补齐缺省身份/清单字段。`NodePowerSummary`/`NodePowerSample` 除生命周期和功率档位外，还汇总节点类型、规划/实测容量、可用容量、累计写入和功率转换次数。工作、加入、失败和退休列表均由 `NodeRecord` 状态筛选得到，不再额外保存一套容易失真的成员表。

## 5. 光盘库数据结构

### 5.1 `DiscSlot` 与 `DiscSlotIndex`

`DiscSlot` 只表达物理占用：

```text
slot_index -> disc_id
```

光盘容量、已用空间和状态放在 `OpticalDisc`，避免空槽位携带一个没有归属的DiscState。

`DiscSlotIndex` 同时维护：

```text
DiscID -> SlotIndex
SlotIndex -> DiscID
```

插入时拒绝重复DiscID和重复槽位。`SegmentSnapshot()` 将连续的DiscID/SlotID压缩成 `DiscSlotSegment`；`PointSnapshot()` 返回未形成连续段的孤立项。它用于：

- CMS返回归档位置后的物理寻址；
- 机械臂取盘；
- 光盘插入、移除和跨库迁移；
- 按槽位排序批量读请求。

### 5.2 `OpticalDisc`

字段覆盖身份、物理位置、状态、容量、镜像计数、读写带宽、制造/刻录/巡检时间及可纠正/不可纠正错误数。

状态为：

```text
Blank -> Recording -> Recorded
  \          |          /
   +------> Defective
```

`ValidateOpticalDisc()` 检查ID、24 bit库ID、容量/使用量以及设计规格下最多100个镜像。

### 5.3 `OpticalDrive` 与 `MechanicalArm`

光驱记录类型、资源状态、当前盘、当前任务、读写带宽、预计可用时间和故障原因。类型包括WriterOnly、ReaderOnly和ReadWrite。

机械臂记录当前位置、目标光驱、正在携带的光盘、当前任务和预计可用时间。它们共同服务于写队列、读队列、空闲光驱池及批量读预取流水线。

### 5.4 `OpticalLibraryInventory`

汇总一座库的：

- 24 bit `library_id`与当前字符串`node_id`；
- Active/ReadOnly/Retired状态；
- 槽位、光盘、光驱、机械臂；
- HDD/SSD缓存设备；
- DL-SubNameSpace generation与字节数；
- 刻录暂存数据量。

校验规则包括槽位唯一、光盘唯一、光盘必须有槽位、光盘必须属于当前库，以及Retired库必须零本地光盘。

## 6. 光盘镜像与纠删码

### 6.1 `ImageSuperblock`

逻辑字段包括magic、格式版本、40 bit ImageID、10 GB容量、创建时间、公共路径前缀、纠删组与角色、校验算法和Superblock校验值。

### 6.2 `ImageRegionLayout`

描述四个区域的偏移和长度：

```text
Superblock -> Data Region -> Metadata Region -> Checksum Region
```

校验函数保证区域有序、不重叠且不超过镜像容量。该结构描述逻辑布局，不替代当前 `ImageStore::ImageRecordHeader`。现有镜像记录仍使用CRC32和可变镜像大小；未来切换固定10 GB格式时需要单独的版本化codec和迁移工具。

### 6.3 `ImageFileExtent`

保留当前ImageStore所需的object ID、文件偏移、镜像偏移、长度和CRC32，并增加设计要求的前后镜像链，用于大文件跨镜像分割。

### 6.4 `ErasureCodeGroup`

默认 `(n=10, k=4)`、16 MB chunk、Reed-Solomon/ISA-L。成员记录ImageID、DiscID、数据/校验角色和角色下标；组状态记录缺失成员数及是否可恢复。

## 7. 文件与命名空间结构

### 7.1 `FileRecord`

它是跨工作流使用的逻辑inode，不是磁盘packed结构：

- `file_id`：设计24 B文件标识；
- `attributes`：当前MDS需要的inode/parent、大小、时间、版本、权限和所有者；
- `state`：Pending、Cached、Archived；
- `placement`：磁电extent或光盘extent；
- 文件名、相对路径和长路径溢出引用；
- `obsolete`：WORM文件的逻辑废弃标志；
- 归档时间和纠删角色。

`CachedFileExtent` 描述一个节点上的文件切片，包含节点、磁盘、对象、文件偏移、存储偏移和长度。按照最新设计，它不包含 `replica_index`；同一文件的多个 extent 表示切片或跨介质分段，而不是公共模型中的主备副本。

`OpticalFileExtent` 包含LibraryID、DiscID、ImageID、文件/镜像偏移、长度和跨镜像链。

当前 `UnifiedInodeRecord` 仍使用256 B v3磁盘格式。公共模型保留 `kCachedInodeRecordBytes=256` 和 `kArchivedInodeRecordBytes=512` 作为目标预算，但不能直接用 `sizeof(FileRecord)` 写盘。

### 7.2 `NamespaceLeaf`

字段对应设计的512 B NS-leaf：根前缀、LibraryID、bundle ID、文件数、总字节数、最近访问时间、SHA-256和预留区。

因为C++中LibraryID以4 B承载而介质要求3 B，只有显式codec打包后才能保证512 B，不能对该结构使用`reinterpret_cast`持久化。

### 7.3 其他命名空间结构

- `SegmentTableEntry`：CMS用户段的base offset、长度和文件数；
- `DirectoryMarker`：表示空目录仍然存在；
- `RenameRedirect`：旧FileID到新FileID的短TTL重定向；
- `DiscSubNamespaceEntry`：镜像内FileID到ImageID/偏移及局部属性；
- `NamespaceBundleDescriptor`：下沉 `.smb` 包的ID、根前缀、库、规模、校验和与格式版本。

## 8. 跨模块数据流

本节同时描述已接入的节点控制闭环和仍属于后续模块的目标数据流。当前实际接入范围以第9节为准。

### 8.0 节点注册、放置与退役（已接入）

```text
DataNode/MDS readiness + heartbeat
  -> Scheduler NodeRecord（规划值 + 观测值 + 生命周期 + 功率/可靠性）
  -> Scheduler提交带generation的完整目录提案
  -> CMS校验成员/tombstone并分配紧凑ID
  -> CMS提交后更新MDS NodeStateCache/PG
  -> MDS只从CMS已提交、可写且已唤醒的节点中放置真实文件

RemoveManagedNode
  -> CMS目录先关闭源节点新写准入
  -> Scheduler枚举、复制、回读校验、MDS原子切换、删除源对象
  -> MDS/CMS独占扫描统一inode，确认磁盘和光盘引用均为0
  -> 物理断电确认 -> Retired tombstone
```

### 8.1 写入热层

```text
Client path
  -> CMS生成FileId和Pending FileRecord
  -> 放置模块从NodeRecord + NodeInventorySnapshot派生候选节点
  -> DataNode写对象并返回CachedFileExtent
  -> CMS把FileRecord更新为Cached
```

### 8.2 批量归档

```text
Cached FileRecord集合
  -> 按node_id和物理offset形成SourceSegment
  -> TransferChunk + ChunkFileSlice
  -> SegmentManifest发送到目标OpticalLibrary
  -> HDD staging
  -> OpticalImageDescriptor/ImageFileExtent
  -> ErasureCodeGroup
  -> DiscSlotIndex分配物理盘槽
  -> ArchiveLocationUpdate批量提交CMS
  -> FileRecord变为Archived
  -> CMS确认后释放磁电源数据
```

`ValidateSegmentManifest()` 检查迁移ID、目标库、chunk ID唯一、源段有效及每个文件切片没有越界。

### 8.3 冷数据读取

```text
FileId
  -> CMS读取FileRecord/NamespaceLeaf
  -> 得到OpticalFileExtent
  -> 按LibraryID、DiscID、ImageID和offset分组
  -> BatchReadPlan
  -> DiscSlotIndex定位槽位
  -> MechanicalArm装盘
  -> OpticalDrive读取
  -> 按ImageFileExtent重组文件
```

`BatchReadPlan` 的层级为LibraryReadPlan -> DiscReadGroup -> ReadOperation，直接对应设计文档的 `{lib_id: [(disc_id, slot, [read_ops])]}`。

### 8.4 元数据下沉/上浮

```text
CMS FileRecord子树
  -> NamespaceBundleDescriptor + .smb
  -> DL-SubNameSpace
  -> CMS细粒度记录替换为NamespaceLeaf

NamespaceLeaf
  -> 按library_id/bundle_id取回.smb
  -> 校验SHA-256
  -> 恢复FileRecord和索引
```

## 9. 当前接入状态

| 结构 | 当前接入情况 |
|---|---|
| 节点枚举、设备组、功率、可靠性、硬件profile | Scheduler已直接复用 |
| `NodeRecord` | NodePowerManager是唯一事实表；ClusterState只投影旧线协议视图 |
| 方向性带宽、线性功率性能和浴盆参数 | 已接入Scheduler计算、Protobuf与测试 |
| `DeviceInventory` | 心跳写入NodeRecord清单；Scheduler经CMS目录投影为MDS `DiskInfo`，MDS不自行接受Scheduler旧视图 |
| `NodeInventorySnapshot` | 已接入心跳、容量安全水位、查询和v2快照 |
| `NodeReadinessStatus` | 已接入显式就绪上报；服务、清单和控制端点全部就绪后才允许Working |
| `OpticalLibraryStatus` | 已接入自动ReadOnly、待写任务门禁、80%暂存背压与零本地光盘退役门禁 |
| `FileRecord` | 已定义；现有`UnifiedInodeRecord`继续作为磁盘格式 |
| 光盘库/槽位/光驱/机械臂 | 已定义和校验；当前OpticalNode尚待迁移 |
| 镜像与纠删组 | 已定义目标逻辑模型；当前ImageStore格式保持不变 |
| SegmentManifest/BatchReadPlan | 已定义和校验；后续RPC可直接据此设计消息 |

当前 Scheduler 直接使用公共C++节点结构；DataNode、MDS和CMS通过同语义Protobuf适配器接入，不复制第二套权威节点目录。CMS是成员、紧凑ID和放置准入的权威，Scheduler是节点运行观测与策略提案方，MDS只消费CMS提交结果。现有RocksDB键、256 B inode和光盘镜像文件没有因为公共C++结构而改变。

`FileRecord`、固定10 GB镜像、纠删组、完整DiscID→Slot索引、机械臂/光驱内部队列和DL-SubNameSpace仍是目标公共模型，当前数据面尚未全面迁移到这些类型。

## 10. 使用规则

1. 新增公共节点语义时优先修改本目录和Protobuf适配器；不得在Scheduler、CMS、MDS各自再造含义冲突的节点事实表。
2. 规划硬件使用 `NodeHardwareProfile`，心跳实际值使用 `NodeInventorySnapshot`，不得互相覆盖。
3. 公共逻辑结构不得直接按原始内存写盘；固定24/40 bit字段必须由codec显式编码。
4. 新增字段应保持明确单位后缀，并同步Protobuf或磁盘codec版本。
5. 文件位置与节点属性分离；节点结构不保存具体文件列表。
6. 光盘状态属于 `OpticalDisc`，槽位只保存物理占用。
7. `Retired`节点保留tombstone；`Retired`光盘库必须零本地光盘。
8. 无论`force`取值为何，退役都不能绕过零驻留数据、光盘库零本地光盘和物理节点已确认断电三项安全条件。
9. 修改结构或校验逻辑后运行 `storage_model_test`、`scheduler_node_model_test`、`scheduler_snapshot_test`和Scheduler服务测试。
10. 紧凑ID只由CMS分配且分配后不可变；Scheduler与DataNode不得自行生成。
