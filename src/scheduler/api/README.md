# Scheduler 对外接口与联调契约

本目录提供给其他模块调用 Scheduler 的 C++ 客户端。线协议仍是 [scheduler.proto](../../msg/scheduler.proto) 的 SchedulerService；没有新建一套节点结构，也不改变现有调用方。C++ 使用 [SchedulerClient.h](SchedulerClient.h)，链接 CMake 目标 zb_scheduler_api_client。非 C++ 调用方可直接使用 protobuf/brpc，或向 BRPC 的 HTTP/JSON 入口发送请求。

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

## 验证

- scheduler_public_api_test：启动真正的 BRPC SchedulerService，验证注册、稳定翻页、查询及业务错误/传输错误区分。
- scheduler_owner_smoke：节点模型、管理、迁移及进程内数据 RPC 的开发回归。
- tests/scheduler/real_system_smoke.py：真实外存、FUSE、MDS/CMS 和两个真实数据节点的读写、重启、排空与退役闭环；不测物理功耗。

本接口方便联调，但不声明物理电表、实体光盘刻录或其他模块的冷目录/Masstree 流程已经验收。
