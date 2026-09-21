# Scheduler 模块说明

`Scheduler` 是集群控制平面，职责包括：

- 接收 real node 和 virtual node 心跳
- 维护节点/磁盘健康状态全局视图
- 将节点状态作为带generation的完整目录提案提交给CMS；只有CMS提交结果可以改变MDS放置视图
- 为旧心跳协议返回单节点兼容的 `group_id/role/epoch` 投影；公共节点模型不维护主备组
- 提供节点生命周期控制接口：`StartNode/StopNode/RebootNode`
- 提供节点管理接口：`SetNodeAdminState`（ENABLED/DRAINING/DISABLED）
- 提供统一节点模型：存储、元数据和光盘库节点的容量、性能、寿命与四级功耗
- 提供显式注册、排空/退休、窗口指标上报和功率/能耗查询接口

## 统一节点管理

新增 RPC：

- `RegisterManagedNode`：按默认模板或自定义设备组显式注册节点。
- `RemoveManagedNode`：先进入 DRAINING，再在数据清空后完成 RETIRED；强制模式用于故障实验。
- `ReportNodeMetrics`：上报读写次数、字节数、队列、活跃请求和最近访问时间。
- `ReportManagedNodeReadiness`：显式确认服务端点、设备清单和控制端点已就绪，防止仅凭心跳提前进入Working。
- `ReportNodeAccessEvent`：接收 trace/仿真的单次节点访问事件。
- `ReportNodeDrainProgress`：上报迁移剩余对象、字节和活动请求，三项归零后进入 EXITING。
- `AdvanceSimulationTime`：显式推进 simulated node；真实墙钟 tick 不会推进模拟节点。
- `ListManagedNodes`：分页查询节点生命周期、四级功耗、容量、利用率和累计能耗。
- `GetManagedNode`：按ID读取完整节点、设备清单、退役历史和光盘库聚合状态。
- `ReportOpticalLibraryStatus/SetManagedNodeServiceMode`：上报光盘分类计数，并支持自动或手动只读降级。
- `GetDrainMigration`：查询真实排空任务、剩余对象/字节、重试和阻塞原因。
- `InjectManagedNodeFailure/RepairManagedNode`：可靠性实验和人工修复。
- `BatchRegisterManagedNodes/BatchRemoveManagedNodes/BatchReportNodeAccessEvents`：长期场景批量入口。
- `RunLongTermSimulation/GetManagedMetricsHistory`：分步推进多年模拟并读取功率/能耗时序。

原有节点首次心跳仍可自动注册，以保持部署兼容。退休节点保留 tombstone，同 ID 的旧进程心跳会被拒绝。

Real、Virtual 节点会在服务入口聚合 I/O 指标，并在心跳成功后上报。MDS 会注册为 metadata managed node，并上报主要文件系统 RPC 指标。窗口使用 reporter epoch 和 sequence 幂等去重，网络响应丢失不会造成窗口重复计数。

2026-09-14 光盘引擎适配：OpticalNodeManager 目前没有对外导出实际盘片库存和异步下载/打包/刻录任务统计。光盘节点保留身份和引擎就绪心跳，但不再上报旧 ImageStore 的虚构零队列/库存，缺少 inventory 的节点保持 JOINING，CMS 不准入。后续由光盘模块提供实际清单、后台任务与设备指标，再接入公共 NodeInventorySnapshot、OpticalLibraryStatus 和 ReportNodeMetrics。当前不能将光盘 RPC 返回视为后台 IO 完成。

T03 测试入口及新版适配说明见 `tests/scheduler/README.md`。其中真实 IO、Scheduler 模型能耗、功耗仪实测使用独立的数据来源标签。

功耗等级为 `PEAK/MEDIUM/STANDBY/OFF`，与原有 `ON/OFF/STARTING/STOPPING` 启停状态分开。策略等级/模型能耗和物理已执行等级/执行能耗分别记录；物理执行默认关闭，启用后失败会保留错误并按间隔重试，实验不会把未执行的关机误计成真实节能。

存储节点删除会自动执行：先由CMS提交目录关闭源节点新写准入，再实际对象枚举、按 inode 成组、分块复制、目标回读校验、MDS 原子位置切换、源清理、源端二次枚举、CMS inode零引用复核、停机和退休。迁移目标只选保留字节语义的物理节点；待机目标会先被唤醒。MDS 提交前绝不删除源对象；无法安全映射的旧对象会使任务 BLOCKED。元数据节点必须零引用，光盘库必须已观测且零本地光盘，物理节点还必须确认断电，`force`也不能绕过这些退役条件。

CMS按设计承担权威职责：校验完整成员目录和Retired tombstone、拒绝陈旧generation、分配不可变紧凑ID（存储/元数据16 bit，光盘库24 bit）、过滤未就绪/不健康/只读/未唤醒节点，并在光盘暂存区达到80%时停止新归档放置。Scheduler不直接写MDS缓存。

策略配置：

```text
POWER_PEAK_UTILIZATION=0.80
POWER_PEAK_QUEUE_DEPTH=8
POWER_METRICS_FRESHNESS_MS=5000
POWER_STANDBY_AFTER_MS=60000
POWER_OFF_AFTER_MS=600000
POWER_MINIMUM_RESIDENCY_MS=1000
POWER_ALLOW_OFF_WITH_RESIDENT_DATA=false
POWER_MIN_ONLINE_STORAGE_NODES=1
POWER_MIN_ONLINE_METADATA_NODES=1
POWER_MIN_ONLINE_OPTICAL_NODES=0
POWER_MIN_ONLINE_STORAGE_CAPACITY_BYTES=0
POWER_MIN_ONLINE_STORAGE_FREE_BYTES=0
POWER_ACTUATION_ENABLED=false
POWER_ACTUATION_RETRY_MS=5000
POWER_WAKE_CMD_TEMPLATE=
POWER_STANDBY_CMD_TEMPLATE=
POWER_OFF_CMD_TEMPLATE=
METRICS_HISTORY_MAX_SAMPLES=100000
MDS_ADDRESS=127.0.0.1:9000
MIGRATION_RPC_TIMEOUT_MS=10000
MIGRATION_COPY_CHUNK_BYTES=4194304
MIGRATION_RETRY_BASE_MS=1000
MIGRATION_MAX_RETRIES=20
MANAGED_STATE_SNAPSHOT_PATH=/runtime-root/state/managed_nodes.pb
```

默认禁止仍有驻留数据的节点自动进入 OFF。

managed 节点目录、身份/设备清单、生命周期、功率、累计能耗、可靠性随机状态和时序历史会定期保存为v2 Protobuf快照，并在Scheduler启动时恢复。`ClusterState`不再维护第二份节点表，只从同一个`NodeRecord`目录生成旧RPC兼容视图。

## 工作机制

1. 节点先启动服务，再上报readiness和 `ReportHeartbeat`；心跳包括服务地址、实际设备容量/健康、方向性带宽和累计写入。
2. Scheduler 根据超时阈值标记 `HEALTHY/SUSPECT/DEAD`，并从唯一 `NodeRecord` 派生工作、加入、失效和退休列表。
3. Scheduler周期性发布全量目录提案；CMS校验generation、成员完整性和tombstone，分配紧凑ID后提交。
4. 只有CMS提交成功才更新MDS本地 `NodeStateCache` 和PG；MDS不再轮询Scheduler旧视图作为事实源。
5. 新放置只选择 `WORKING + HEALTHY + ENABLED + READ_WRITE + readiness完成 + 已唤醒 + device_healthy` 节点，并使用带宽、功率档位、利用率和队列深度计算权重。
6. 退役时目录提交、Create/迁移/归档位置提交和最终引用扫描共享事务门禁，保证零引用结论不被并发新增位置破坏。

## 生命周期控制

Scheduler 内置 Shell 执行器（可选）：

- `START_CMD_TEMPLATE`
- `STOP_CMD_TEMPLATE`
- `REBOOT_CMD_TEMPLATE`

可用占位符：`{node_id}` `{address}` `{force}`。
如果模板未配置，控制操作会被接受并更新期望状态，但不会触发外部命令。

## 快速运行

```bash
./build/scheduler_server --config=config/scheduler.conf.example --port=9100
```

## 快速测试控制接口

```bash
./build/scheduler_control_test --scheduler=127.0.0.1:9100 --node_id=vpool --do_reboot=true
```
