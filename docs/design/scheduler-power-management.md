# Scheduler 光盘节点功耗管理设计

## 背景

ZBStorage 当前已经有 Scheduler 控制面，用来接收 real node、virtual node、optical node 心跳，维护全局节点视图，并向 MDS/FUSE 提供 `GetClusterView`。冷数据归档和 optical 访问链路中，元数据会记录 optical node、disk、image 等位置，后续访问可以定位到对应光盘节点。

现有 Scheduler 的 power 字段主要服务真实节点生命周期控制，表达 ON/OFF/STARTING/STOPPING，并由心跳和 `StartNode/StopNode/RebootNode` 驱动。新功能希望在 Scheduler 内部增加一个轻量的光盘节点功耗模拟器：配置 10000 个仿真光盘节点，用数组直接表示节点状态；节点处于关机、待机、开机三种状态时功耗不同；Scheduler 实时统计总功耗和各状态节点数量；请求到达时节点变为开机，空闲后按时间阈值逐级降为待机、关机。

该功能和基于 KV/索引的元数据管理关系在于：MDS/FUSE 根据元数据中的 optical location 访问冷数据时，可以把 optical node access 事件上报给 Scheduler，Scheduler 据此统计光盘阵列在不同访问模式下的能耗行为，为论文或系统实验提供控制面指标。

## 目标

- 在 Scheduler 内存中创建固定大小的 `std::vector` 或等价数组，默认 10000 个元素，每个元素表示一个仿真 optical node。
- 每个仿真节点支持三态：`OFF`、`STANDBY`、`ON`。
- 每次 optical node 收到访问请求后，从 `OFF/STANDBY/ON` 统一转为 `ON`，刷新最近请求时间。
- 后台 tick 周期检查状态：
  - `ON` 节点超过较短空闲阈值后转为 `STANDBY`。
  - `STANDBY` 节点超过较长空闲阈值后转为 `OFF`。
- Scheduler 维护并返回实时统计：
  - 总节点数。
  - `OFF/STANDBY/ON` 三类节点数量。
  - 总功耗，建议内部使用 milliwatts 整数避免浮点误差。
  - 最近更新时间和 generation。
- 提供 RPC 让测试工具、MDS、optical node 或 FUSE 客户端报告某个 optical node 的访问事件。
- 提供 RPC 查询功耗摘要，可按需分页返回节点明细。
- 保持功能可测试、可配置，不引入持久化依赖。

## 非目标

- 不在第一版模拟真实光驱启动延迟、托盘机械动作、读写速度变化或预热失败。
- 不改变现有 MDS 分配和 `ClusterState` 的健康过滤规则。
- 不让 10000 个仿真 optical node 进入现有 `GetClusterView` 节点列表，避免污染真实心跳视图。
- 不做跨 Scheduler 重启的状态恢复。重启后按配置重新初始化。
- 不要求第一版在真实读写路径中强制阻塞等待节点开机完成，请求触发后状态立即变为 `ON`。

## 范围与代码位置

该功能属于主系统 `src/` 下的 Scheduler 控制面增强。

建议新增和修改位置：

- 新增 `src/scheduler/power/OpticalPowerManager.h`
- 新增 `src/scheduler/power/OpticalPowerManager.cpp`
- 修改 `src/scheduler/config/SchedulerConfig.h`
- 修改 `src/scheduler/config/SchedulerConfig.cpp`
- 修改 `src/msg/scheduler.proto`
- 修改 `src/scheduler/service/SchedulerServiceImpl.h`
- 修改 `src/scheduler/service/SchedulerServiceImpl.cpp`
- 修改 `src/scheduler/server/scheduler_server.cpp`
- 修改 `CMakeLists.txt`
- 修改 `deploy/multi_host/templates/scheduler.conf.tpl`
- 可选修改 `src/scheduler/README.md`
- 新增 `tests/scheduler_power_manager_test.cpp`
- 可选新增 `src/client/test/scheduler_power_test.cpp` 作为 RPC 手工测试工具

运行时快照、日志和实验结果不能写入仓库目录，必须放在运行根目录下，例如现有 `SCHEDULER_ROOT` 对应的 `logs/` 子目录。

## 当前仓库上下文

已验证事实：

- `src/msg/scheduler.proto` 已定义 `NodeType`，其中包括 `NODE_OPTICAL`，也已定义 `NodePowerState`，但没有 standby 状态。
- `src/msg/scheduler.proto` 已有 `GetClusterView`、`ReportHeartbeat`、`StartNode`、`StopNode`、`RebootNode` 等 Scheduler RPC。
- `src/scheduler/model/ClusterState.h` 的 `NodeState` 保存真实上报节点的 `power_state` 和 `desired_power_state`。
- `src/scheduler/model/ClusterState.cpp` 中 `ReportHeartbeat` 会把心跳节点设为 `NODE_POWER_ON`，`TickHealth` 根据 heartbeat 超时和 desired power 更新现有节点状态。
- `src/scheduler/server/scheduler_server.cpp` 已有 tick 线程，每 `TICK_INTERVAL_MS` 调用 `state.TickHealth()`，也已有 cluster view snapshot 线程。
- `src/scheduler/config/SchedulerConfig.*` 当前使用简单 `KEY=VALUE` 配置解析，已有 `SUSPECT_TIMEOUT_MS`、`DEAD_TIMEOUT_MS`、`TICK_INTERVAL_MS`、`CLUSTER_VIEW_SNAPSHOT_PATH` 等配置。
- `deploy/multi_host/templates/scheduler.conf.tpl` 当前写入 Scheduler 快照到 `@SCHEDULER_ROOT@/logs/scheduler_cluster_view.txt`。
- `src/sim/SimCluster.h` 默认 optical node 数量是 10000，`src/sim/SimCluster.cpp` 生成 `optical-node-<index>` 格式的 optical node id。
- `src/sim/readme.md` 说明仿真模型默认 `OPTICAL_NODE_COUNT=10000`。
- `src/data_node/optical_node/server/optical_node_server.cpp` 的 optical node 会以 `NODE_OPTICAL` 向 Scheduler 上报心跳。
- `src/mds/storage/UnifiedInodeLocation.h` 使用 `optical-node-<numeric_id>` 格式表达 optical node id。
- `src/mds/archive/OpticalArchiveManager.cpp` 在归档写入 optical node 时已经知道目标 optical node、address、disk、image。
- `src/client/fuse/zb_fuse_client.cpp` 通过 Scheduler `GetClusterView` 解析节点地址，并对 optical location 发起数据节点读写。

设计假设：

- 用户需求中的“10000 个仿真的光盘节点”按当前仓库命名理解为 10000 个 optical node，而不是每个 optical node 下的 10000 张 disc。
- 初始状态默认设为 `OFF`，也允许配置为 `STANDBY` 或 `ON`。
- 功耗模型只用于统计，不改变现有读写请求成功与否。
- `OFF -> ON`、`STANDBY -> ON` 第一版为瞬时状态转换，后续可扩展启动延迟。

## 方案架构

新增 `OpticalPowerManager`，作为 Scheduler 内部独立子模块。它不继承 `ClusterState`，也不把仿真节点加入 `nodes_`，避免把 10000 个仿真状态和真实心跳、主从 failover、生命周期控制混在一起。

组件职责：

- `OpticalPowerManager`
  - 持有 `std::vector<OpticalPowerNode>`，数组下标就是 optical node index。
  - 解析和格式化 `optical-node-<index>`。
  - 处理访问事件 `RecordAccess`。
  - 在 `Tick` 中执行空闲状态转换。
  - 维护功耗摘要 `OpticalPowerSummary`。
  - 提供快照接口给 RPC 和文件 snapshot 使用。
- `SchedulerServiceImpl`
  - 新增 `ReportOpticalNodeAccess` RPC，用于上报某个 optical node 收到请求。
  - 新增 `GetOpticalPowerView` RPC，用于查询功耗统计和可选节点明细。
  - 保持现有 `GetClusterView` 行为不变。
- `scheduler_server`
  - 构造 `OpticalPowerManager` 并注入 `SchedulerServiceImpl`。
  - 在现有 tick 线程中调用 `power_manager.Tick(now_ms)`。
  - 可选新增 power snapshot 线程或复用 cluster snapshot 间隔输出文本快照。
- 调用方
  - 第一阶段可由 `scheduler_power_test` 或集成测试直接调用 `ReportOpticalNodeAccess`。
  - 后续可在 MDS archive manager、FUSE optical read/write 或 optical node service 中按实际访问上报。

状态机：

```text
OFF      -- request --> ON
STANDBY  -- request --> ON
ON       -- idle >= ON_IDLE_TO_STANDBY_MS --> STANDBY
STANDBY  -- idle >= STANDBY_IDLE_TO_OFF_MS --> OFF
ON       -- request --> ON
```

时间语义：

- `last_request_ms` 表示最近一次访问请求时间。
- `ON -> STANDBY` 使用 `now_ms - last_request_ms >= on_idle_to_standby_ms`。
- `STANDBY -> OFF` 使用 `now_ms - last_request_ms >= standby_idle_to_off_ms`。
- 要求 `standby_idle_to_off_ms > on_idle_to_standby_ms`。如果配置不满足，加载配置时报错或自动修正为更大的值，建议实现时报错。
- 请求携带 `request_ts_ms` 时使用该时间；为避免旧时间戳回退，实际写入 `last_request_ms = max(last_request_ms, request_ts_ms_or_now)`。

统计策略：

- 10000 个节点规模很小，`Tick` 里 O(N) 扫描数组是可接受且容易验证的。
- `RecordAccess` 对单个节点做增量状态转换，并刷新摘要。
- 为减少错误，摘要可在每次 `Tick` 后全量重算，也可在 `RecordAccess` 中对单节点增量更新。
- 推荐实现：内部提供 `RecomputeSummaryLocked(now_ms)`，第一版在 `Tick` 和初始化后全量重算；`RecordAccess` 更新单节点后调用一次局部 `AdjustSummaryForTransition`，单测再覆盖全量重算一致性。

## 数据模型与接口

### C++ 数据结构

建议在 `src/scheduler/power/OpticalPowerManager.h` 中定义：

```cpp
namespace zb::scheduler {

enum class OpticalPowerState {
    kOff = 0,
    kStandby = 1,
    kOn = 2,
};

struct OpticalPowerConfig {
    bool enabled{true};
    uint32_t node_count{10000};
    std::string node_id_prefix{"optical-node-"};
    OpticalPowerState initial_state{OpticalPowerState::kOff};
    uint64_t on_idle_to_standby_ms{10 * 60 * 1000};
    uint64_t standby_idle_to_off_ms{60 * 60 * 1000};
    uint64_t off_power_mw{0};
    uint64_t standby_power_mw{5000};
    uint64_t on_power_mw{35000};
};

struct OpticalPowerNode {
    uint32_t index{0};
    std::string node_id;
    OpticalPowerState state{OpticalPowerState::kOff};
    uint64_t last_request_ms{0};
    uint64_t state_enter_ms{0};
    uint64_t request_count{0};
};

struct OpticalPowerSummary {
    uint64_t generation{1};
    uint32_t total_count{0};
    uint32_t off_count{0};
    uint32_t standby_count{0};
    uint32_t on_count{0};
    uint64_t total_power_mw{0};
    uint64_t last_update_ms{0};
};

class OpticalPowerManager {
public:
    bool Init(const OpticalPowerConfig& config, std::string* error);
    uint64_t Tick(uint64_t now_ms);
    bool RecordAccess(const std::string& node_id,
                      uint32_t node_index,
                      uint64_t request_ts_ms,
                      OpticalPowerNode* out,
                      OpticalPowerSummary* summary,
                      std::string* error);
    bool Snapshot(bool include_nodes,
                  uint32_t offset,
                  uint32_t limit,
                  OpticalPowerSummary* summary,
                  std::vector<OpticalPowerNode>* nodes,
                  std::string* error) const;
};

} // namespace zb::scheduler
```

`RecordAccess` 支持两种定位方式：

- `node_id` 非空时解析 `optical-node-<index>`。
- `node_id` 为空时使用 `node_index`。

如果两者都提供且不一致，返回 `SCHED_INVALID_ARGUMENT`。

### Proto 接口

建议在 `src/msg/scheduler.proto` 新增独立 enum 和 message，不直接复用现有 `NodePowerState`，避免影响真实节点生命周期语义。

```proto
enum OpticalPowerState {
  OPTICAL_POWER_UNKNOWN = 0;
  OPTICAL_POWER_OFF = 1;
  OPTICAL_POWER_STANDBY = 2;
  OPTICAL_POWER_ON = 3;
}

message OpticalPowerNodeView {
  string node_id = 1;
  uint32 node_index = 2;
  OpticalPowerState power_state = 3;
  uint64 last_request_ms = 4;
  uint64 state_enter_ms = 5;
  uint64 request_count = 6;
}

message OpticalPowerSummary {
  uint64 generation = 1;
  uint32 total_count = 2;
  uint32 off_count = 3;
  uint32 standby_count = 4;
  uint32 on_count = 5;
  uint64 total_power_mw = 6;
  uint64 last_update_ms = 7;
}

message ReportOpticalNodeAccessRequest {
  string node_id = 1;
  uint32 node_index = 2;
  uint64 request_ts_ms = 3;
  string reason = 4;
}

message ReportOpticalNodeAccessReply {
  SchedulerStatus status = 1;
  uint64 generation = 2;
  OpticalPowerNodeView node = 3;
  OpticalPowerSummary summary = 4;
}

message GetOpticalPowerViewRequest {
  bool include_nodes = 1;
  uint32 offset = 2;
  uint32 limit = 3;
}

message GetOpticalPowerViewReply {
  SchedulerStatus status = 1;
  uint64 generation = 2;
  OpticalPowerSummary summary = 3;
  repeated OpticalPowerNodeView nodes = 4;
}
```

在 `SchedulerService` 中新增：

```proto
rpc ReportOpticalNodeAccess(ReportOpticalNodeAccessRequest) returns (ReportOpticalNodeAccessReply);
rpc GetOpticalPowerView(GetOpticalPowerViewRequest) returns (GetOpticalPowerViewReply);
```

分页规则：

- `include_nodes=false` 时只返回 summary。
- `include_nodes=true` 时返回 `[offset, offset + limit)` 范围内节点。
- `limit=0` 可解释为默认 100，最大限制建议 1000，避免一次 RPC 返回 10000 个节点造成不必要开销。

## 配置与运行路径

在 `SchedulerConfig` 中新增：

```text
OPTICAL_POWER_ENABLED=true
OPTICAL_POWER_NODE_COUNT=10000
OPTICAL_POWER_NODE_ID_PREFIX=optical-node-
OPTICAL_POWER_INITIAL_STATE=OFF
OPTICAL_POWER_ON_IDLE_TO_STANDBY_MS=600000
OPTICAL_POWER_STANDBY_IDLE_TO_OFF_MS=3600000
OPTICAL_POWER_OFF_MW=0
OPTICAL_POWER_STANDBY_MW=5000
OPTICAL_POWER_ON_MW=35000
OPTICAL_POWER_SNAPSHOT_INTERVAL_MS=5000
OPTICAL_POWER_SNAPSHOT_PATH=@SCHEDULER_ROOT@/logs/scheduler_power_view.txt
```

说明：

- `OPTICAL_POWER_NODE_COUNT` 默认 10000，必须大于 0。
- `OPTICAL_POWER_INITIAL_STATE` 只接受 `OFF`、`STANDBY`、`ON`。
- `OPTICAL_POWER_STANDBY_IDLE_TO_OFF_MS` 必须大于 `OPTICAL_POWER_ON_IDLE_TO_STANDBY_MS`。
- 功耗统一用 milliwatts，展示层可转换为 watts。
- snapshot 路径必须放在运行根目录下。当前仓库 `config/base.conf` 的运行根目录是 `/mnt/md0/wjh/zb_run_dir_v3`，多机模板中已有 `SCHEDULER_ROOT`，因此推荐落到 `@SCHEDULER_ROOT@/logs/`。
- 不要把功耗快照、日志、benchmark 结果写入 `/home/wjh/ZBStorage`。

## 一致性与故障语义

- 该功能是内存态模拟，不参与 MDS 元数据原子性、rename/unlink 或对象复制一致性。
- Scheduler 重启后功耗状态按配置重新初始化，`generation` 从 1 开始。
- 访问事件重复上报时，节点保持 `ON`，刷新最近访问时间并增加 `request_count`。重复事件可能让 request count 偏大，但不会破坏功耗状态。
- 访问事件时间戳早于当前 `last_request_ms` 时，不回退 `last_request_ms`。状态仍可保持或变为 `ON`，避免乱序 RPC 导致节点提前降级。
- 无效 node id、越界 index、node id 与 index 不一致返回 `SCHED_INVALID_ARGUMENT`。
- 未启用 `OPTICAL_POWER_ENABLED` 时，两个新 RPC 返回明确错误，例如 `SCHED_INVALID_ARGUMENT` 或 `SCHED_INTERNAL_ERROR`，建议使用 `SCHED_INVALID_ARGUMENT` 并说明 power manager disabled。
- `GetClusterView` 不返回仿真功耗节点，现有 MDS/FUSE 依赖的集群视图不受影响。

## 测试计划

单元测试：

- `OpticalPowerManager.Init`：默认 10000 个节点，初始状态和总功耗正确。
- `RecordAccess`：访问 `optical-node-3` 后该节点变为 `ON`，`on_count=1`，`off_count=9999`，总功耗增加。
- `Tick`：构造短阈值，验证 `ON -> STANDBY -> OFF` 转换。
- 多节点统计：访问多个节点后统计数量与总功耗匹配。
- node id/index 校验：越界、格式错误、不一致时返回错误。
- 时间乱序：旧 timestamp 不导致 `last_request_ms` 回退。
- 分页 snapshot：`include_nodes=true`、`offset`、`limit` 返回数量正确。

服务层测试：

- 构造 `SchedulerServiceImpl` 时注入 `OpticalPowerManager`，直接调用 `ReportOpticalNodeAccess` 和 `GetOpticalPowerView` 验证 proto 填充。
- 保持现有 `GetClusterView` 行为不变，仿真功耗节点不出现在 `nodes` 中。

配置测试：

- 解析所有 `OPTICAL_POWER_*` 配置。
- 非法状态名、非法整数、`standby_idle <= on_idle` 报错。

构建与手工验证：

- 修改 `CMakeLists.txt`，把 `src/scheduler/power/OpticalPowerManager.cpp` 加入 `scheduler_server` 和相关测试 target。
- 可新增 `scheduler_power_manager_test` 本地二进制，不依赖 brpc。
- 可新增 `scheduler_power_test` RPC 工具，支持：
  - `--access_node=optical-node-10`
  - `--access_index=10`
  - `--get_power_view=true`
  - `--include_nodes=false`

## Benchmark 与实验计划

Microbenchmark：

- `Tick(10000 nodes)` 单次耗时。
- `RecordAccess` 单次耗时。
- 每秒 1k、10k、100k access event 下 Scheduler CPU 使用。

实验指标：

- `total_power_mw`
- `off_count`
- `standby_count`
- `on_count`
- `request_count` 分布。
- 不同 `ON_IDLE_TO_STANDBY_MS`、`STANDBY_IDLE_TO_OFF_MS` 下的平均功耗。

建议实验场景：

- 均匀随机访问 10000 个 optical node。
- Zipf 热点访问，观察热点节点保持 ON，冷节点回到 OFF。
- 批量归档写入后长时间空闲，观察总功耗阶梯下降。
- 查询型 workload 中比较不同 idle 阈值的能耗与响应行为。

所有 benchmark 结果、trace、日志必须写到运行根目录，例如 `/mnt/md0/wjh/zb_run_dir_v3/experiments/` 或 `SCHEDULER_ROOT/logs/`，不要写入仓库。

## 实现步骤

1. 修改 `src/msg/scheduler.proto`，新增 `OpticalPowerState`、功耗 view/summary/access/view RPC message，并在 `SchedulerService` 中加入两个 RPC。
2. 修改 `SchedulerConfig`，增加 `OpticalPowerConfig` 字段和 `OPTICAL_POWER_*` 配置解析。
3. 新增 `src/scheduler/power/OpticalPowerManager.h/.cpp`，实现数组初始化、id/index 解析、访问触发、tick 状态转换、summary 和 snapshot。
4. 修改 `SchedulerServiceImpl`，构造函数接收 `OpticalPowerManager*`，实现 `ReportOpticalNodeAccess` 和 `GetOpticalPowerView`。
5. 修改 `scheduler_server.cpp`，构造 power manager，在 tick 线程中调用 `Tick`，按配置输出 power snapshot。
6. 修改 `CMakeLists.txt`，把新源文件加入 `scheduler_server`，新增单元测试 target。
7. 修改 `deploy/multi_host/templates/scheduler.conf.tpl`，加入默认 10000 节点功耗配置和 snapshot 路径。
8. 新增 `tests/scheduler_power_manager_test.cpp` 覆盖核心状态机。
9. 可选新增 `src/client/test/scheduler_power_test.cpp`，用于手工触发访问和查询功耗视图。
10. 更新 `src/scheduler/README.md`，说明配置项、RPC 和手工测试命令。
11. 后续接入真实访问链路时，在 MDS/FUSE/optical node 选择一个单一上报点，避免同一次用户请求被多个组件重复计数。

## 风险与替代方案

- 风险：现有 `NodePowerState` 没有 standby，如果强行复用会改变真实节点生命周期语义。
  - 方案：新增独立 `OpticalPowerState`，只用于功耗模拟。
- 风险：请求上报点选择不当会重复计数。
  - 方案：第一版先提供 RPC 和测试工具；真实链路接入时选择 MDS archive manager 或 FUSE optical data path 的一个上报点，并在文档中声明。
- 风险：Scheduler tick O(N) 扫描可能随着未来百万节点规模变慢。
  - 方案：10000 节点下 O(N) 简单可靠；如果未来扩展，可使用按 deadline 排序的小顶堆或时间轮。
- 风险：重启后功耗状态丢失影响长期实验连续性。
  - 方案：第一版明确不持久化；如需要长实验恢复，可后续增加 snapshot load。
- 风险：OFF 到 ON 瞬时转换低估真实访问延迟。
  - 方案：第一版用于功耗统计；后续可加入 `STARTING` 或 warmup delay，并把请求延迟指标纳入实验。
