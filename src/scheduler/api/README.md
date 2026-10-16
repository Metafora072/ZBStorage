# Scheduler 对外接口与联调契约

本目录提供给其他模块调用 Scheduler 的 C++ 客户端。线协议仍是 [scheduler.proto](../../msg/scheduler.proto) 的 SchedulerService；没有新建一套节点结构，也不改变现有调用方。C++ 使用 [SchedulerClient.h](SchedulerClient.h)，链接 CMake 目标 zb_scheduler_api_client。非 C++ 调用方可直接使用 protobuf/brpc，或向 BRPC 的 HTTP/JSON 入口发送请求。

## 开发与联调边界

| 责任方 | 实现和维护的内容 |
| --- | --- |
| Scheduler（我们） | 节点状态与能耗、CMS 目录提案、排空协调；真实磁盘配套的写门禁与迁移；进程内磁盘/光盘模型及正式数据协议适配；本模块闭环回归 |
| 光盘节点负责人 | 真实设备库存及后台下载/打包/取盘/刻录统计、实际数据执行和恢复、实际库存与任务快照的采集、归档完成回报 |
| MDS 负责人 | 权威文件位置、冷目录判定、归档下发/租约/完成确认、Masstree 发布和冷热元数据清理 |
| 客户端负责人 | 文件访问与准备入口、位置缓存和错误处理；与我们共同维护共享入口的节点 ID/实例代际传递 |

缺少真实光盘库存或归档回报时，Scheduler 保持准入检查，不补成成功。模型控制状态与生成式字节不充当真实数据证明。

本轮公共契约扩展为 `RealNodeService.BeginNodeDrain`、`InstallMigratedFile`，以及客户端 `FileInfo` 尾部可选的节点 ID/实例代际字段；后者在各光盘 RPC 上传递既有 LogicalNodeRoute。MDS 和真实光盘业务实现由对应负责人维护。

## 谁调用什么

| 调用方 | 推荐接口 | 目的 |
| --- | --- | --- |
| 真实磁盘、光盘节点和 MDS | RegisterManagedNode、ReportManagedNodeReadiness、ReportHeartbeat、ReportNodeMetrics；光盘另用 ReportOpticalLibraryStatus | 报身份、服务地址、设备库存、就绪、负载与后台任务 |
| 节点部署/控制工具 | GetManagedNode、ListManagedNodes、StartNode、StopNode、RebootNode、RemoveManagedNode、GetDrainMigration | 查询、启停、排空和追踪阻塞原因 |
| Trace/仿真控制器 | ReportNodeAccessEvent、BatchReportNodeAccessEvents、AdvanceSimulationTime、RunLongTermSimulation、GetManagedMetricsHistory | 注入模拟事件并读取模型时序；真实 I/O 不应重复注入同一访问 |
| 客户端/FUSE | MDS 文件位置及 [LogicalNodeRoute](../../common/rpc/LogicalNodeRoute.h) | 数据面仍按 MDS 权威位置访问目标节点；不让 Scheduler 代替 MDS 分配文件 |
| MDS/放置方 | [GetCmsNodeCatalog](../../msg/mds.proto) | 读取 CMS 已提交节点目录；Scheduler 的管理查询不是放置权威 |

RegisterManagedNode 可以用于显式配置节点；已有真实节点也保留首次心跳自动注册的兼容路径。需要自定义硬件、执行模式或实例代际时应显式注册，不依赖默认模板。物理节点的服务须先启动，再上报就绪和心跳；库存未就绪时不会因进程存活而自动取得写入准入。光盘节点必须报告实际盘片/任务状态，不应填入虚构的空库存。

## C++ 调用

~~~cpp
#include "scheduler/api/SchedulerClient.h"

zb::scheduler::api::SchedulerClient scheduler("127.0.0.1:9100", 2000);
std::string error;
zb::rpc::GetManagedNodeRequest request;
request.set_node_id("node-01");
zb::rpc::ManagedNodeReply reply;
if (!scheduler.GetManagedNode(request, &reply, &error)) {
    // error 区分 RPC 传输失败与 SchedulerStatus 非 SCHED_OK。
    // reply.status() 在收到业务响应时保留服务端错误码。
}
~~~

客户端覆盖 SchedulerService 的所有现有 RPC；每次调用返回 bool，同时将完整 protobuf 回复交给调用方。没有隐式重试，特别是注册、控制、指标和移除等可变更状态的 RPC。`Connect` 只初始化 BRPC Channel，不探测远端健康；首次 RPC 的传输结果才说明服务可达。连接初始化后可在多个线程并发使用；不要在并发调用过程中销毁客户端。ListAllManagedNodes 自动翻页，并在目录版本变化时失败而不是返回混合目录；高频变化的集群可改用 ListManagedNodes 分页并自行重试。

其他语言可直接调用 BRPC HTTP/JSON，例如：

~~~sh
curl -sS -H 'Content-Type: application/json' \
  -d '{"node_id":"node-01"}' \
  http://127.0.0.1:9100/zb.rpc.SchedulerService/GetManagedNode
~~~

成功须检查回复中的 status.code，不应仅以 HTTP 200 或 RPC 传输成功判定业务成功。当前接口未附加身份认证；部署时只向可信内网开放 Scheduler 控制端口。

## 工作流与完成条件

1. **加入**：显式注册时通过 ManagedNodeSpec 给出 node_id、kind、execution_mode、instance_generation；兼容的首次心跳自动注册使用心跳中的节点类型和默认模板。随后上报服务地址、设备容量/健康、readiness。Scheduler 本地状态可经 GetManagedNode 查询。需要做文件放置时，还必须在 MDS 的 GetCmsNodeCatalog 中看到同一身份的 Working 且满足读写准入。
2. **运行**：节点以报告者 epoch + 单调序号上报指标窗口。上报失败可按相同序号重试；成功后再确认本地窗口，避免重复计算。真实访问由节点指标汇总；ReportNodeAccessEvent 主要供 Trace/仿真注入。
3. **排空**：RemoveManagedNode 发起排空，不表示文件已迁移。调用方可通过 GetDrainMigration 查看进度和错误；只有 MDS/CMS 确认 Retired、源文件引用为零且源数据清空，才是完成。无可用目标或核验失败时保持阻塞，不得强制跳过元数据/数据安全检查。
4. **模型数据面**：从 MDS 文件位置获取逻辑节点 ID、实例代际与地址；磁盘前台 RPC 在 BRPC attachment 中设置 LogicalNodeRoute。旧代际、缺目标或 CMS 未确认的模型实例拒绝请求。模型读取的生成式字节不能当作真实副本迁移或内容校验。

单位：容量、IO 和带宽使用字节或字节/秒；报告时间使用毫秒，访问事件时间使用微秒；功率为毫瓦，累计能量为微焦。instance_generation 是服务实例代际，不是库存 generation、硬件 technology_generation、CMS 目录版本或文件版本。

真实磁盘排空先调用 `BeginNodeDrain(node_id)`：回复须包含 `STATUS_OK` 且 `write_admission_closed=true`。源节点先拒绝新写，再等待在途对象写/文件提交结束，最后 fsync 关闭标记和目录；重启仍拒绝写计划、对象写和文件提交。该门禁用于退役，不因普通启动 RPC 自动打开；失败保持关闭并允许重试。复制和目标回读校验后，`InstallMigratedFile` 核对目标对象长度，保存源文件大小、对象单位、版本和修改时间，再向 MDS 条件提交位置。MDS 成功后才清理源对象和旧文件元数据，最终再次核验零引用。

每次复制重试先调用 MDS `GetFileLocation` 核对权威源；若位置已经提交到其他节点，则只清理旧源，避免响应丢失或重启后用旧内容覆盖目标上的后续写入。位置无法解析时保留源并报告阻塞。部署排空能力时须同步更新真实磁盘服务；旧节点未实现门禁/元数据接收 RPC 时明确失败，不退回无保护迁移。

## 验证

- scheduler_real_drain_integration_test：真实 Scheduler、MDS 和磁盘进程检查持久写门禁、源进程异常重启、迁移版本接续和迁移后继续写入。
- scheduler_inprocess_disk_server_restart_test：独立 Scheduler/MDS 进程检查 CMS 准入、模型持久化及异常重启。
- scheduler_model_closure_integration_test：独立 Scheduler/MDS 进程检查模型迁移、代际与光盘归档；具备 FUSE 时通过正式客户端和 zbctl 检查准备及读取链路，数据为模型生成。
- tests/scheduler/real_system_smoke.py：真实外存、FUSE、MDS/CMS 和两个真实数据节点的读写、重启、排空与退役闭环；不测物理功耗。

单元/组件测试不交付；上述三个 CTest 系统集成用例统一使用 `scheduler_system_closure` 标签。

本接口方便联调，但不声明物理电表、实体光盘刻录或其他模块的冷目录/Masstree 流程已经验收。
