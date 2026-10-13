#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "scheduler.pb.h"

namespace brpc { class Channel; }

namespace zb::scheduler::api {

// SchedulerService 的跨模块 C++ 调用入口，复用 scheduler.proto 线协议。
// 各 RPC 返回 true 表示传输成功且响应状态为 SCHED_OK；false 时 error 给出原因，
// 已收到的业务响应仍保留在 reply 中，调用方可读取 status.code()。
// 批量 RPC 的 true 只表示批次处理成功，各子请求结果须检查 results 中的 status。
// 状态变更类 RPC 不做隐式重试，避免注册、启停、移除等操作被重复执行。
// 首次使用会自动初始化 Channel；初始化完成后可并发调用，但不可并发销毁对象。
class SchedulerClient {
public:
    // 指定 Scheduler 地址和单次 RPC 超时（毫秒，0 使用默认值 2000）。
    explicit SchedulerClient(std::string address, uint32_t timeout_ms = 2000);
    ~SchedulerClient();
    SchedulerClient(const SchedulerClient&) = delete;
    SchedulerClient& operator=(const SchedulerClient&) = delete;

    // 初始化 BRPC Channel；不探测远端是否在线，实际可达性由 RPC 结果判定。
    bool Connect(std::string* error);
    // 返回构造时指定的 Scheduler 服务地址。
    const std::string& address() const { return address_; }

    // 上报节点心跳、服务地址、设备状态和就绪信息，维持节点存活视图。
    bool ReportHeartbeat(const rpc::HeartbeatRequest&, rpc::HeartbeatReply*, std::string* error);
    // 获取兼容旧节点协议的集群视图；文件放置仍以 CMS 已提交目录为准。
    bool GetClusterView(const rpc::GetClusterViewRequest&, rpc::GetClusterViewReply*, std::string* error);
    // 设置节点的管理状态（启用、排空或禁用）。
    bool SetNodeAdminState(const rpc::SetNodeAdminStateRequest&, rpc::SetNodeAdminStateReply*, std::string* error);
    // 请求启动节点，返回操作受理结果；实际完成情况需继续查询操作状态。
    bool StartNode(const rpc::StartNodeRequest&, rpc::NodeOperationReply*, std::string* error);
    // 请求停止节点，返回操作受理结果；不代表节点已断电。
    bool StopNode(const rpc::StopNodeRequest&, rpc::NodeOperationReply*, std::string* error);
    // 请求重启节点，返回操作受理结果。
    bool RebootNode(const rpc::RebootNodeRequest&, rpc::NodeOperationReply*, std::string* error);
    // 查询启停或重启操作的当前状态及错误信息。
    bool GetOperationStatus(const rpc::GetOperationStatusRequest&, rpc::NodeOperationReply*, std::string* error);
    // 显式注册受管节点及其类型、执行模式、硬件和实例代际等属性。
    bool RegisterManagedNode(const rpc::RegisterManagedNodeRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 发起受管节点排空/退役；请求成功不等于迁移和退役已经完成。
    bool RemoveManagedNode(const rpc::RemoveManagedNodeRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 上报节点 I/O 指标窗口，供负载、能耗模型和调度策略使用。
    bool ReportNodeMetrics(const rpc::ReportNodeMetricsRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 注入单次访问事件，主要用于 Trace/仿真；真实 I/O 不应重复注入。
    bool ReportNodeAccessEvent(const rpc::ReportNodeAccessEventRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 上报排空期间剩余对象、字节和活动请求等进度。
    bool ReportNodeDrainProgress(const rpc::ReportNodeDrainProgressRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 上报光盘库的盘片、暂存区、光驱和后台任务等实际状态。
    bool ReportOpticalLibraryStatus(const rpc::ReportOpticalLibraryStatusRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 设置受管节点的服务模式，例如读写或只读降级。
    bool SetManagedNodeServiceMode(const rpc::SetManagedNodeServiceModeRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 上报服务初始化、设备清单和元数据等就绪条件。
    bool ReportManagedNodeReadiness(const rpc::ReportManagedNodeReadinessRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 推进模拟节点的逻辑时间，不推进真实节点的墙钟时间。
    bool AdvanceSimulationTime(const rpc::AdvanceSimulationTimeRequest&, rpc::AdvanceSimulationTimeReply*, std::string* error);
    // 按过滤条件、偏移量和页大小查询受管节点目录。
    bool ListManagedNodes(const rpc::ListManagedNodesRequest&, rpc::ListManagedNodesReply*, std::string* error);
    // 按 node_id 查询单个受管节点的完整运行视图。
    bool GetManagedNode(const rpc::GetManagedNodeRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 查询节点排空迁移任务的进度、重试和阻塞原因。
    bool GetDrainMigration(const rpc::GetDrainMigrationRequest&, rpc::GetDrainMigrationReply*, std::string* error);
    // 为故障/可靠性实验注入受管节点失效。
    bool InjectManagedNodeFailure(const rpc::ManagedNodeFailureRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 修复处于失败状态且未退役的节点，清除失败原因并恢复健康标记。
    bool RepairManagedNode(const rpc::ManagedNodeFailureRequest&, rpc::ManagedNodeReply*, std::string* error);
    // 批量注册受管节点，适用于大规模场景初始化。
    bool BatchRegisterManagedNodes(const rpc::BatchRegisterManagedNodesRequest&, rpc::BatchManagedNodeReply*, std::string* error);
    // 批量发起受管节点排空/移除，具体结果见逐项响应。
    bool BatchRemoveManagedNodes(const rpc::BatchRemoveManagedNodesRequest&, rpc::BatchManagedNodeReply*, std::string* error);
    // 批量上报访问事件，主要用于 Trace/长期仿真。
    bool BatchReportNodeAccessEvents(const rpc::BatchReportNodeAccessEventsRequest&, rpc::BatchManagedNodeReply*, std::string* error);
    // 按目标时间和步长推进长期仿真，返回已推进时间及集群汇总。
    bool RunLongTermSimulation(const rpc::RunLongTermSimulationRequest&, rpc::RunLongTermSimulationReply*, std::string* error);
    // 查询受管节点集群的指标、功率和累计能耗汇总历史。
    bool GetManagedMetricsHistory(const rpc::GetManagedMetricsHistoryRequest&, rpc::GetManagedMetricsHistoryReply*, std::string* error);

    // 自动翻页获取全部匹配节点；各页目录版本不一致时失败并清空 nodes。
    // 若目录频繁变化，可改用 ListManagedNodes 自行处理分页与重试。
    // generation 成功时返回目录版本；filter.offset 会被忽略。
    bool ListAllManagedNodes(const rpc::ListManagedNodesRequest& filter,
                             std::vector<rpc::ManagedNodeView>* nodes,
                             uint64_t* generation,
                             std::string* error);

private:
    // 统一执行 RPC，并区分参数、连接、传输和服务端业务状态错误。
    template <class Request, class Reply, class Method>
    bool Call(const char* name, const Request& request, Reply* response,
              Method method, std::string* error);

    std::string address_;
    uint32_t timeout_ms_;
    std::mutex connect_mu_;
    std::unique_ptr<brpc::Channel> channel_;
};

} // namespace zb::scheduler::api
