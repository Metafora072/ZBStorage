# Scheduler 正式验证报告（2026-08-03）

## 1. 结论

Scheduler 本阶段的核心功能已经在独立 Release 集群中通过正式联机验证：统一节点目录、功耗与能耗状态、Trace 回放、批量 RPC、故障与修复、长期仿真、指标历史、状态快照恢复，以及真实对象的排空迁移和 MDS 原子位置切换均可工作。

排空实验首次暴露出一个跨模块问题：迁移已复制对象并切换 MDS，但目标数据节点没有文件级辅助元数据，FUSE 因而返回 `ENOENT`。本次已增加严格限定于 `STATUS_NOT_FOUND` 的 MDS 元数据降级读取路径；重新构建并重启全栈后，迁移文件的 SHA-256 与源数据完全一致。

仓库整体仍不能表述为“所有数据节点功能零缺口”：虚拟节点按当前实现返回合成 payload，光盘节点的对象读取也返回合成 payload，导致通用模块冒烟中的两项内容一致性检查失败。它们不影响本次 Scheduler 到两个真实节点之间的迁移结论，但需要在后续明确“仿真语义”还是改成真实 payload 持久化。

## 2. 隔离环境

- 外存：`/dev/md0`，ext4，91 TiB 总量，实验前约 29 TiB 可用。
- 正式运行目录：`/mnt/md0/zbstorage_scheduler_formal_20260803_e2i7vm`。
- Release 构建目录：`/home/dsf/ZBStorage/build-release`。
- 旧集群 `/mnt/md0/Projects/cgy/zb_run_dir_v3` 及其 9000/9100/19080/29080 端口未停止、未修改、未复用。
- 新集群端口：MDS 19000、Scheduler 19100、真实节点 39080/59081、虚拟节点 49080、光盘节点 59080。
- 物理功耗执行保持关闭，避免正式验证命令真实关机；验证了策略状态和能耗模型，没有执行硬件电源操作。

## 3. 构建与自动测试

- 全量 Release 构建成功，`CMAKE_BUILD_TYPE=Release`。
- 静态 RocksDB 构建成功：`build-release/third_party/rocksdb/librocksdb.a`，约 37 MiB。
- Scheduler 相关 C++ 测试 5/5 通过：
  - `scheduler_node_model_test`
  - `node_metrics_collector_test`
  - `scheduler_snapshot_test`
  - `scheduler_managed_service_test`
  - `scheduler_drain_migration_test`
- Trace 生成器 Python 测试 3/3 通过。
- `scheduler_control_test` 的 Stop/Start/Reboot RPC 通过。

## 4. 联机集群与基础 I/O

- Scheduler、两个真实节点、虚拟节点、光盘节点、MDS、FUSE 全部正常启动。
- Scheduler 集群视图能看到在线数据节点；统一 managed catalog 同时包含真实、虚拟、光盘和 MDS 节点。
- MDS 的 mkdir/create/lookup/rename/unlink/rmdir 全部通过。
- 真实节点对象 Write/Read/Delete 通过。
- 在 `/real` 写入 12 MiB 随机文件后，源文件与 FUSE 读回 SHA-256 均为：
  `2786aac7f4ae9137cd23ec54bcbb1eeaadbf1a36507ff6e98f2119d22a0eb345`。
- 全栈重启后，文件仍可读且哈希不变，证明 RocksDB 命名空间和真实节点数据可恢复。

通用 `module_io_smoke_test` 首轮结果为 3/5：Scheduler、MDS、真实节点通过；虚拟节点和光盘节点均因返回内容与写入内容不一致而失败。RPC 状态为成功，失败点是 payload 一致性，不是 Scheduler 注册或健康检查。

## 5. Trace、功耗、能耗和历史指标

- 固定种子 `20260803` 生成 24,144 条合法 trace 操作，文件约 824 KiB。
- 回放结果：
  - metadata events：24,144
  - data events：15,953
  - 总耗时：4.38 秒
- `sim-data-1` / `sim-data-2` 分别累计 7,879 / 8,074 次数据访问；`sim-mds` 累计 24,144 次元数据访问。
- 高队列深度批量事件将存储和光盘模拟节点推到 `MANAGED_POWER_PEAK`。
- 增加 700 秒空闲尾段后，三个 Trace 模拟节点均进入 `MANAGED_POWER_OFF`，能耗继续按状态积分。
- 批量注册、批量访问事件、故障注入和修复均成功；`batch-sim-a` 故障计数从 0 变为 1，修复后恢复 `MANAGED_HEALTH_HEALTHY`。
- 六年长期仿真用 30 天步长完成，共 73 步；最终时间精确达到目标。
- 指标历史返回 168 个样本，最后样本时间等于长期仿真最终时间。
- 到达采样寿命的模拟节点进入 draining，光盘库节点因默认十年寿命在六年时仍 working，符合默认模型。

## 6. 真实排空迁移与 MDS 切换

测试文件 inode 为 6，初始位置：

```text
node-real-01@127.0.0.1:39080 / disk-01
```

执行 `RemoveManagedNode(node-real-01)` 后：

- 节点先进入 `MANAGED_LIFECYCLE_DRAINING`。
- Scheduler 枚举出 3 个对象，总计 12,582,912 字节。
- 3 个对象全部复制并逐对象读回校验。
- MDS 位置提交成功后才删除源对象。
- 迁移任务进入 `DRAIN_MIGRATION_COMPLETED`，无重试、无错误。
- `migrated_objects=3`，`migrated_bytes=12582912`。
- 源节点自动进入 `MANAGED_LIFECYCLE_RETIRED` 和 `MANAGED_POWER_OFF`。
- MDS 最终位置：

```text
node-real-02@127.0.0.1:59081 / disk-01
```

- 源对象目录不再包含 3 个数据对象；目标目录包含 `obj-6-0`、`obj-6-1`、`obj-6-2`，每个 4 MiB。

首次迁移后 FUSE 读取暴露了目标节点缺少文件级辅助元数据的问题。本次修改 FUSE：只有在 `ResolveFileRead` 返回 `STATUS_NOT_FOUND` 且 MDS 已给出有效 file size/object unit 时，才按稳定对象 ID 生成切片并读取；连接错误、校验错误和其他存储错误不会被掩盖。

修复后重新构建 FUSE、重启完整集群，迁移文件 SHA-256 再次与原始输入完全一致。MDS 位置仍指向第二真实节点，第一真实节点的 retired 状态和所有模拟节点的能耗/生命周期状态均由 Scheduler 快照恢复。

## 7. 正式验证中完善的实验基础设施

- `start_demo_stack.sh` 现在写入独立 Scheduler managed-state 快照路径和正确的实验 MDS 地址。
- 脚本显式写入迁移、功耗水位和历史容量配置，避免非默认端口误连旧 MDS。
- 可选启动光盘节点和第二真实节点，支持各自端口、节点 ID、盘数和第二真实节点容量。
- 拓扑、PID、日志、配置、MDS RocksDB 和对象数据全部位于本次独立运行目录。
- `.gitignore` 已忽略 `build-release/`，避免正式构建产物污染提交范围。

## 8. 剩余风险与下一步

1. 虚拟节点目前只保存大小、校验和等仿真状态，读取返回等长合成数据；若 `/virtual` 被定义为 POSIX 内容一致存储，则仍需实现 payload 持久化。若它只用于规模/性能仿真，应调整冒烟测试并在接口文档中明确这一语义。
2. 光盘 ImageStore 已写入镜像文件，但当前对象和归档文件读取仍生成等长合成数据；`SIMULATE_IO=false` 下是否应返回镜像真实内容需要修正或澄清。
3. MDS 通过 managed-node 注册进入统一目录，但不进入数据节点 ClusterState，因此 `actuated_power_level` 不能由心跳确认。启用真实功耗执行前，需要为元数据节点设计独立、安全的执行地址与存活确认机制，不能简单把 MDS 加入数据放置视图。
4. 节点早于 MDS 启动时会记录少量 `Connection refused`，MDS 就绪后自动恢复；属于启动顺序噪声，不影响稳态实验。
5. 本次没有启用真实关机/唤醒命令，也没有执行跨主机网络故障实验；这两项需要在可控硬件环境中单独验收。
