# ManagedNode 与 NodePowerManager 公共接口说明

> 权威公共结构：`src/storage_model/`
> Scheduler兼容入口：`src/scheduler/model/ManagedNode.h`、`src/scheduler/power/NodePowerManager.h`
> 文档状态：当前实现说明与后续公共属性接口约束
> 更新日期：2026-09-03

## 1. 接口定位

公共数据结构现已迁移到 `src/storage_model`。这两个 Scheduler 头文件保留原有名称并重新导出公共类型，共同构成 Scheduler 的兼容入口：

- `ManagedNode.h` 描述节点是什么，包括节点类型、部署形态、设备组、容量、带宽、延迟、功率档位和可靠性参数。这部分以静态或低频变更属性为主。
- `NodePowerManager.h` 描述节点现在处于什么状态，包括生命周期、健康状态、功耗状态、访问负载、累计能耗、排空进度和失效恢复状态。这部分是 Scheduler 管理的运行态。

未来其他模块若需要读取统一节点属性，应优先依赖这套模型，不应再单独定义一份存储节点、元数据节点或光盘库节点结构。跨进程调用仍以 `src/msg/scheduler.proto` 为线协议，不能直接传输 C++ 结构体的内存布局。

当前公共属性模型版本为：

```cpp
kManagedNodeAttributeModelVersion == 2
```

该版本表示 C++ 字段语义，不等同于 Protobuf 消息版本或持久化快照版本。

## 2. 单位约定

接口不使用没有单位的容量、带宽、功率和时间字段。字段后缀就是单位契约。

| 后缀或字段 | 单位/语义 |
|---|---|
| `_bytes` | 字节，硬件模板采用十进制换算，`1 TB = 10^12 bytes` |
| `_bytes_per_sec` | 字节/秒，采用十进制带宽 |
| `_us` | 微秒 |
| `_ms` | 毫秒 |
| `_milliwatts` | 毫瓦，`1000 W = 1,000,000 mW` |
| `energy_microjoules` | 微焦耳 |
| `last_utilization` | `[0, 1]` 的无量纲比例 |
| `event_time_us` | trace 事件时间，微秒精度 |

能耗计算使用下面的精确单位恒等式，不需要浮点换算：

```text
energy_microjoules += policy_power_milliwatts × elapsed_milliseconds
actuated_energy_microjoules += actuated_power_milliwatts × elapsed_milliseconds
```

所有累加和乘法均采用饱和运算，数值超过 `uint64_t` 上限时停在上限，不发生无符号回绕。

## 3. `ManagedNode.h`：节点静态属性

### 3.1 正交状态枚举

节点属性被拆成彼此独立的维度，不能用一个状态替代另一个状态：

| 类型 | 取值 | 含义 |
|---|---|---|
| `ManagedNodeKind` | `STORAGE`、`METADATA`、`OPTICAL_LIBRARY` | 节点提供的存储能力 |
| `ExecutionMode` | `PHYSICAL`、`VIRTUAL`、`SIMULATED` | 节点如何运行、使用哪个时间域 |
| `DeviceKind` | `HDD`、`SSD`、`DISC`、`OPTICAL_DRIVE` | 设备组类型 |
| `AccessPath` | `DEFAULT`、`OPTICAL_CACHE_HIT`、`OPTICAL_DISC` | 性能计算所走的数据路径 |
| `LifecycleState` | `JOINING`、`WORKING`、`DRAINING`、`EXITING`、`RETIRED` | 节点管理生命周期 |
| `ManagedHealthState` | `HEALTHY`、`SUSPECT`、`FAILED` | 节点健康维度 |
| `PowerLevel` | `OFF`、`STANDBY`、`MEDIUM`、`PEAK` | 策略期望功耗档位 |
| `TransitionState` | `STABLE`、`STARTING`、`STOPPING`、`REBOOTING` | 物理启停过程 |
| `AdministrativeState` | `ENABLED`、`DRAINING`、`DISABLED` | 运维准入维度 |
| `NodeServiceMode` | `READ_WRITE`、`READ_ONLY` | 光盘库写入能力 |
| `FailureSource` | `NONE`、`HEARTBEAT_TIMEOUT`、`RELIABILITY_MODEL`、`INJECTED` | 故障来源 |

头文件提供所有枚举的 `*Name()` 函数，日志、CSV 和调试输出应使用这些函数，避免各模块自行维护字符串映射。未知枚举值统一返回 `UNKNOWN`。

`IsServingLifecycle()` 对 `WORKING` 和 `DRAINING` 返回真：前者可承接正常工作，后者仍需服务存量访问和迁移，但不代表二者都可接受新放置。`IsTerminalLifecycle()` 当前仅对 `RETIRED` 返回真。

### 3.2 `DeviceGroup`

一个节点可配置多个设备组，用来表达不同容量、数量或性能的设备批次。

| 字段 | 说明 |
|---|---|
| `name` | 节点内部的人类可读设备组名，建议在同一节点内唯一 |
| `kind` | 设备类型 |
| `device_capacity_bytes` | 单设备容量；光驱本身通常为 0 |
| `device_count` | 设备数量，必须大于 0 |
| `per_device_bandwidth_bytes_per_sec` | 单设备带宽；0 表示未知或该组不直接承载数据路径 |
| `max_concurrency` | 最大并发设备数；0 表示使用该组全部设备 |
| `per_device_read_bandwidth_bytes_per_sec` | 读取带宽；0时回退通用带宽 |
| `per_device_write_bandwidth_bytes_per_sec` | 写入带宽；0时回退通用带宽 |

节点总容量为除 `OPTICAL_DRIVE` 外所有设备组容量之和：

```text
capacity = Σ(device_capacity_bytes × device_count)
```

内部聚合带宽为匹配访问路径的设备组并发带宽之和：

```text
internal_bandwidth = Σ(per_device_bandwidth × min(device_count, max_concurrency))
```

其中 `max_concurrency == 0` 时使用 `device_count`。

### 3.3 `PowerProfile`

`PowerProfile` 给出四个功耗档位的毫瓦数。合法配置必须满足：

```text
peak >= medium >= standby >= off
```

`PowerMilliwatts(profile, level)` 是档位到实际功率的统一换算入口。

### 3.4 `ReliabilityProfile`

| 字段 | 说明 |
|---|---|
| `mean_lifetime_ms` | 节点寿命均值 |
| `lifetime_stddev_ms` | 寿命标准差；0 表示使用确定性寿命 |
| `minimum_lifetime_ms` | 采样寿命下界 |
| `mean_time_between_failures_ms` | 平均失效间隔；0 表示不启用随机失效 |
| `mean_repair_time_ms` | 平均修复时间；0 表示不自动修复 |
| `model`及浴盆参数 | NormalLifetime或按早期/稳定/磨损阶段FIT采样 |

`SampleLifetimeMs()` 使用调用方提供的种子进行可复现采样。标准差为 0 时，结果为 `max(mean, minimum)`；非 0 时使用正态分布并应用最小寿命下界。

### 3.5 `ManagedNodeProfile`

`ManagedNodeProfile` 是一个节点的静态描述，也是未来公用节点属性接口的核心对象。

| 字段组 | 主要字段 | 说明 |
|---|---|---|
| 身份 | `node_id` | Scheduler 稳定标识；退休后保留 tombstone，不复用 |
| 分类 | `kind`、`execution_mode` | 存储能力与执行方式分离 |
| 逻辑规模 | `logical_node_count` | 一个进程代表的逻辑节点数，必须大于 0 |
| 技术代际 | `technology_generation` | 节点替换和长期技术演进；从1开始 |
| 放置能力 | `participates_in_placement` | 是否允许进入放置候选集合；不是最终可分配条件的全部 |
| 外部性能 | `external_network_bandwidth_bytes_per_sec` | 节点外部网络上限 |
| 固定延迟 | `fixed_access_latency_us` | 普通访问或光盘 HDD 命中的固定延迟 |
| 光盘延迟 | `optical_load_latency_us`、`optical_seek_latency_us` | 机械取盘与光驱寻道延迟 |
| 设备 | `device_groups` | 一个或多个设备组 |
| 功率 | `power` | 四级功耗模板 |
| 可靠性 | `reliability` | 寿命、MTBF 和 MTTR 参数 |

`ValidateManagedNodeProfile()` 负责公共的结构合法性校验。调用注册接口前应先校验，错误通过可选的 `std::string* error` 返回。

### 3.6 性能估算

`EstimatePerformance()` 返回 `PerformanceEstimate`：

```text
effective_bandwidth = min(external_network_bandwidth, internal_bandwidth)
service_latency = fixed/path_latency
                + queue_delay
                + power_transition_delay
                + ceil(request_size / effective_bandwidth)
```

光盘未命中路径额外包含机械取盘和寻道延迟。有效带宽为 0 时，`valid == false`，调用方必须检查 `error`，不能把 0 带宽当成瞬时完成。

### 3.7 三类默认模板

| 节点类型 | 设备配置 | 有效默认带宽 | 固定/路径延迟 | 功率（峰/中/待机/关） | 寿命 |
|---|---|---:|---:|---:|---:|
| Storage | `18 TB HDD × 24`，并发 5，单盘 `200 MB/s` | `1 GB/s` | `5 ms` | `1000/300/100/0 W` | 5 年 |
| Metadata | `4 TB SSD × 16`，并发 10，单盘 `5 GB/s` | 外网限制为 `10 GB/s` | `0.5 ms` | `500/200/50/0 W` | 5 年 |
| Optical | `28 TB HDD × 10`、`1 TB DISC × 10000`、10 光驱，读 `100 MB/s`、写 `10 MB/s` | 光盘读路径 `1 GB/s` | `60 s + 0.5 s` | `400/200/50/0 W` | 10 年 |

默认模板只是生成配置的便利函数，不是实际硬件探测结果。实际容量、空闲空间、设备健康、方向性带宽和累计写入由心跳写入 `NodeRecord::inventory`；默认 `18 TB × 24` 不能被解释为机器实时容量。

## 4. `NodePowerManager.h`：节点运行态与管理接口

### 4.1 输入模型

`NodeAccessMetrics` 用于真实节点按窗口上报聚合指标。`reporter_epoch + report_sequence` 用于识别同一生产者的重复上报，时间窗口和计数器使用毫秒与字节。

`NodeAccessEvent` 用于 trace 或仿真逐事件输入。事件时间使用微秒，在进入当前功耗管理器时换算为毫秒。读写方向会选择不同的设备带宽，写事件同时累加节点写入量。

`PowerPolicy` 定义：

- 进入 `PEAK` 的利用率和队列深度阈值；
- 指标新鲜期；
- 进入 `STANDBY`、`OFF` 的空闲时间；
- 功耗状态最短驻留时间；
- 有驻留数据时是否允许关机；
- 各类节点最低在线数量；
- Storage 最低在线总容量和最低在线可用容量；
- 内存时序历史最大样本数。

`ValidatePowerPolicy()` 是公共校验入口。至少保证利用率位于 `[0, 1]`，且 `off_after_ms > standby_after_ms`。为兼容现有构造语义，向 `NodePowerManager` 传入非法策略时会回退到默认策略；配置加载器应先显式校验并把错误报告给操作者。

### 4.2 `ManagedNodeRuntime`

该结构将 `ManagedNodeProfile profile` 与 Scheduler 拥有的动态状态组合在一起。

| 字段组 | 代表字段 | 说明 |
|---|---|---|
| 身份与资产 | `identity`、`inventory` | 地址、紧凑ID和心跳实测设备清单 |
| 状态 | `lifecycle`、`health`、`administrative_state`、`service_mode` | 正交的生命周期、健康、运维和读写维度 |
| 功率 | `power_level`、`transition` | 策略功率与转换过程 |
| 执行反馈 | `actuated_power_level`、`last_power_actuation_*` | 物理执行结果，和策略期望值分开 |
| 生命周期 | `join_time_ms`、`sampled_lifetime_ms`、`planned_exit_time_ms`、`actual_exit_time_ms` | 加入、寿命和退出时间 |
| 访问运行态 | `last_access_ms`、`last_metrics_*`、`last_utilization`、`last_queue_depth` | 最近负载视图 |
| 能耗 | `last_*energy_update_ms`、`energy_microjoules`、`actuated_energy_microjoules` | 策略与物理执行双口径分段积分 |
| 数据 | `resident_data_bytes`、`access_operation_count`、`accumulated_write_bytes` | 驻留数据、操作数与累计写入 |
| 排空 | `drain_remaining_*`、`drain_active_requests`、`drain_last_report_ms` | 退出前迁移进度 |
| 可靠性 | `reliability_seed`、`next_failure_time_ms`、`repair_due_time_ms`、`failure_count/source` | 可复现失效与修复状态 |
| 光盘库 | `optical_library` | 槽位/光盘分类聚合、只读和退役门禁 |
| 演进统计 | `wake/standby/off/replacement_count`、`predecessor_node_id` | 能耗动作与代际替换链 |

除 trace 的 `event_time_us` 外，运行态时间均为毫秒。Physical/Virtual 节点使用墙钟域，Simulated 节点使用模拟时间域，两种时间不能混用。

调用方应把 `ManagedNodeRuntime` 当作只读快照；不要直接修改其成员并期待管理器内部状态同步。状态变更必须通过 `NodePowerManager` 方法完成。

### 4.3 生命周期入口

主要操作顺序为：

```text
RegisterNode -> JOINING
ActivateNode -> WORKING
BeginRemoveNode -> DRAINING
ReportDrainProgress(全部为 0) -> EXITING
FinalizeRemoveNode -> RETIRED
```

`RETIRED` 是终态且保留 node ID tombstone。退役的强制安全条件不能被 `force` 绕过：节点驻留数据必须为0，光盘库还必须有一次有效库存观测且本地光盘数为0，Physical节点必须收到已断电执行确认。`force`只允许在这些条件满足时跳过常规生命周期阶段。

`RegisterReplacementNode()` 仅接受已退休、类型相同且技术代际更高的新节点，并在新旧记录间保存前任关系和替换计数。

失效与生命周期独立：`MarkNodeFailed()` 把健康状态置为 `FAILED` 并强制逻辑功耗为 `OFF`；`RepairNode()` 恢复健康并重新安排下一次随机失效。

### 4.4 功耗决策与能耗积分

当前决策顺序为：

1. 失效、加入中、退出中或已退休节点选择 `OFF`；
2. 达到关闭阈值且满足驻留数据、最低在线节点和容量水位时选择 `OFF`；
3. 达到待机阈值时选择 `STANDBY`；
4. 新鲜指标超过利用率或队列阈值时选择 `PEAK`；
5. 其他工作状态选择 `MEDIUM`。

功耗降低还受 `minimum_residency_ms` 防抖约束。物理节点的 `power_level/energy_microjoules` 是策略口径，`actuated_power_level/actuated_energy_microjoules` 是执行器反馈口径；二者不一致时不能宣称物理设备已完成切换或已经节能。

时间推进会在指标过期、待机、关闭和最短驻留边界处分段积分。因此一次从 0 跳到 10 分钟，与逐毫秒推进得到相同的空闲功耗积分，不会把整段时间都按起始档位计算。

### 4.5 查询接口

- `GetNode()`：按 ID 获取一个拷贝。
- `ListNodes()`：返回全部节点，包括退休 tombstone。
- `ListNodes(ManagedNodeFilter)`：统一组合过滤。
- `ListByLifecycle()`、`ListFailedNodes()`：兼容原调用方的便捷封装。
- `Summary()`：节点类型/状态数量、功率能耗、总/空闲/已用/可用容量、累计写入和转换次数；可用容量只统计满足放置状态的健康设备空闲空间，不可用容量是其余空闲空间，因此两者之和等于总空闲空间。
- `MetricsHistory()`：按时间范围读取内存时序样本。
- `policy()`：获取当前生效策略的拷贝。

`ManagedNodeFilter` 的可选字段使用 AND 关系，可同时按节点类型、执行形态、生命周期、健康和功耗过滤。`include_retired=false` 会排除终态节点；若同时指定 `lifecycle=RETIRED`，结果自然为空。

所有列表和快照都按 `node_id` 字典序稳定排序。这保证测试、日志和未来分页接口不依赖 `unordered_map` 的随机遍历顺序。

### 4.6 快照与并发

`NodePowerManager` 内部用互斥锁保护节点表、代际和历史。所有查询返回值都是拷贝，锁释放后可安全读取，但不会随管理器继续变化。

`NodePowerManagerSnapshot` 当前格式版本为：

```cpp
kNodePowerManagerSnapshotFormatVersion == 2
```

恢复时接受v1和v2，拒绝未知的未来版本，并校验节点属性、实际清单、光盘库聚合状态和node ID唯一性。v2同时恢复时序历史；物理节点的能耗时间基准重置到恢复时刻，避免把Scheduler停机时间误算为设备工作能耗。磁盘持久化由`ManagedNodeSnapshotStore`和对应Protobuf格式负责。

## 5. 推荐调用方式

```cpp
using namespace zb::scheduler;

ManagedNodeProfile profile = DefaultStorageNodeProfile("storage-01");
profile.execution_mode = ExecutionMode::kSimulated;

std::string error;
if (!ValidateManagedNodeProfile(profile, &error)) {
    // 报告配置错误
}

PowerPolicy policy;
if (!ValidatePowerPolicy(policy, &error)) {
    // 不要用无效配置启动
}

NodePowerManager manager(policy);
manager.RegisterNode(profile, /*join_time_ms=*/1000, /*seed=*/42, &error);
manager.ActivateNode(profile.node_id, /*now_ms=*/1000, &error);

ManagedNodeFilter filter;
filter.kind = ManagedNodeKind::kStorage;
filter.lifecycle = LifecycleState::kWorking;
filter.health = ManagedHealthState::kHealthy;
filter.include_retired = false;
for (const ManagedNodeRuntime& node : manager.ListNodes(filter)) {
    const uint64_t capacity = CalculateCapacityBytes(node.profile);
    const uint64_t power = PowerMilliwatts(node.profile.power, node.power_level);
    // 使用只读快照；状态修改继续调用 manager 的方法。
}
```

## 6. 与仓库其他模型的边界

| 模型 | 负责内容 | 不应承担的内容 |
|---|---|---|
| `ManagedNodeProfile/Runtime` | 统一属性、地址/资产清单、策略状态、能耗、寿命、排空与可靠性 | RPC 编码和旧主备线协议 |
| `ClusterState::NodeState` | 从NodeRecord生成旧Heartbeat/GetClusterView兼容投影 | 不再拥有独立节点事实表 |
| `scheduler.proto` | 跨进程兼容的线协议 | C++ 进程内对象所有权 |
| `ManagedNodeSnapshotStore` | 版本化持久化和恢复 | 定义节点属性业务语义 |

`ManagedNodeRuntime` 是唯一节点事实记录；`ClusterState::NodeState` 只在旧RPC边界临时生成。旧协议的`group_id/role`仍按单节点主副本兼容值返回，不进入公共模型。

## 7. 公共接口演进规则

为了让节点属性成为稳定公用接口，后续修改应遵守：

1. 新增物理量必须把单位写入字段名，禁止新增 `capacity`、`latency`、`power` 这类无单位数值。
2. 新增枚举值只能追加，不能重排已有值；同步更新名称函数、Protobuf 映射和测试。
3. 静态配置放入 `ManagedNodeProfile`，运行期观测或状态放入 `ManagedNodeRuntime`，不要混放。
4. 派生量优先通过函数计算，避免在多个结构中缓存同一事实并产生不一致。
5. 不承诺 C++ struct 的二进制 ABI 或内存布局；跨组件持久化必须使用版本化 Protobuf。
6. 修改字段语义时递增 `kManagedNodeAttributeModelVersion`；修改快照兼容性时独立递增快照版本并提供迁移方案。
7. 公共查询保持确定性排序；增加过滤条件时保持现有默认行为兼容。
8. 策略状态和物理执行状态继续分离，实验报告必须说明使用哪一个口径。

## 8. Scheduler RPC接入

`scheduler.proto` 已覆盖完整节点视图和管理入口：注册/代际替换、单节点查询、组合筛选、心跳资产清单、指标与trace事件、光盘库聚合状态、手动只读切换、排空进度、故障注入/修复、长期模拟和时序历史。旧Heartbeat/GetClusterView继续可用，但只作为兼容投影。

仍需由部署或实验配置明确的是`logical_node_count`的统计口径：当前容量和功率按一条NodeRecord汇总，不自动乘以逻辑节点数。

## 9. 验证位置

核心回归测试位于 `tests/scheduler_node_model_test.cpp`，覆盖：

- 三类默认容量、带宽和延迟；
- 读写非对称带宽和功率—性能线性缩放；
- 多设备组聚合；
- 生命周期、四级功耗和驻留数据保护；
- 统一过滤、稳定排序和枚举名称；
- 策略公共校验；
- 快照、失效/修复和安全水位；
- 心跳清单、超时故障、光盘库只读/零盘退休和代际替换；
- 源端二次枚举后才允许完成排空退休；
- 长时间步进跨越多个功耗阈值时的精确分段能耗。

推荐验证命令：

```bash
cmake --build build --target scheduler_node_model_test scheduler_snapshot_test scheduler_managed_service_test scheduler_drain_migration_test -j2
ctest --test-dir build -R 'scheduler_(node_model|snapshot|managed_service|drain_migration)_test' --output-on-failure
```
