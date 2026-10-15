#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include <optical_node_manager.h>

#include "../config/OpticalNodeConfig.h"
#include "ReadEventSessionRegistry.h"

namespace zb::optical_node {

struct ReplicationStatusSnapshot {
    bool replication_enabled{false};
    bool is_primary{true};
    uint64_t epoch{1};
    uint64_t applied_lsn{0};
    std::string node_id;
    std::string group_id;
    std::string peer_node_id;
    std::string peer_address;
    std::string primary_node_id;
    std::string primary_address;
    std::string secondary_node_id;
    std::string secondary_address;
};

class OpticalStorageServiceImpl {
public:
    // 构造即完成归档引擎启动：用 config 构造 OpticalNodeManager 并调用 Run
    // （创建归档目录 + 拉起后台线程）。失败不抛异常，通过 IsArchiveEngineReady() 暴露。
    explicit OpticalStorageServiceImpl(const OpticalNodeConfig& config);

    void ConfigureReplication(const std::string& node_id,
                              const std::string& group_id,
                              bool replication_enabled,
                              bool is_primary,
                              const std::string& peer_node_id,
                              const std::string& peer_address,
                              uint32_t replication_timeout_ms);
    void ApplySchedulerAssignment(bool is_primary,
                                  uint64_t epoch,
                                  const std::string& group_id,
                                  const std::string& primary_node_id,
                                  const std::string& primary_address,
                                  const std::string& secondary_node_id,
                                  const std::string& secondary_address);
    ReplicationStatusSnapshot GetReplicationStatus() const;

    // 接收 MDS 下发的一批归档文件（对应 OpticalNodeService.SendArchiveMetadata），
    // 转发给内部 OpticalNodeManager（仅校验入队，立即返回）。
    volumemanager::ErrorCode SendArchiveMetadata(
        const optical_node_manager::SendArchiveMetadataRequest& request);

    // 提交整文件异步准备任务（对应 OpticalNodeService.PrepareRead）。
    // 入参与线协议一致：disk_id / image_id 带前缀（本层剥离尾部序号），
    // session_id 指定完成通知通道。成功（READ_DELAY）只表示受理，不代表文件可读。
    struct PrepareReadParams {
        uint64_t session_id{0};
        std::string request_id;
        std::string inode_id;
        std::string disk_id;
        std::string image_id;
        uint64_t file_size{0};
        bool     has_file_version{false};
        uint64_t file_version{0};
    };
    volumemanager::ErrorCode PrepareRead(const PrepareReadParams& params, uint64_t* task_id);

    // 为一次独立打开建立读会话（对应 OpticalNodeService.AcquireReadSession）；
    // 转发给 OpticalNodeManager，成功时回填 read_handle / file_size / max_read_bytes。
    volumemanager::ErrorCode AcquireReadSession(uint64_t task_id,
                                                optical_node_manager::AcquireReadSessionResult* out);

    // 在已建立的读会话上同步读取文件区间（对应 OpticalNodeService.ReadPreparedFile）；
    // 转发给 OpticalNodeManager，数据追加到 *out 末尾并回填 bytes_read / eof。
    volumemanager::ErrorCode ReadPreparedFile(uint64_t read_handle,
                                              uint64_t offset,
                                              uint64_t size,
                                              std::string* out,
                                              optical_node_manager::ReadPreparedFileResult* result);

    // 关闭一次读取会话（对应 OpticalNodeService.ReleaseReadSession）；转发给 OpticalNodeManager。
    volumemanager::ErrorCode ReleaseReadSession(uint64_t read_handle);

    // 释放本轮准备的准备引用（对应 OpticalNodeService.ReleasePreparedRead）；转发给 OpticalNodeManager。
    volumemanager::ErrorCode ReleasePreparedRead(uint64_t task_id);

    // 登记客户端的读取完成通知会话（对应 OpticalNodeService.SubscribeReadEvents）。
    // server_stream 由 brpc 适配层 StreamAccept 取得；返回节点内唯一的 session_id，
    // 失败返回 0。本层只保存会话，不感知准备任务。
    uint64_t SubscribeReadEvents(const std::string& client_id, brpc::StreamId server_stream);

    // 通知会话管理组件；供适配层取 handler，以及后续 ReadReady 发送使用。
    ReadEventSessionRegistry* read_event_sessions() { return &read_event_sessions_; }

    // 归档引擎是否已成功启动（状态为 RUNNING）。
    bool IsArchiveEngineReady() const;

    // 正常关机：停止归档引擎并把运行期状态落盘（转发给 OpticalNodeManager::Shutdown）。
    // 幂等；须在 brpc server 停止接收请求之后调用。
    bool Shutdown();

    // 归档引擎当前状态 + 最近一次失败原因，仅用于排障 / 日志。
    std::string GetArchiveStatusDetail() const;

    // 采集一次光盘库库存快照（光驱忙闲 / 盘片 / 暂存区 / 后台任务），
    // 转发给 OpticalNodeManager；供对外上报（心跳设备清单 / 光盘库状态）读取。
    optical_node_manager::OpticalLibraryInventory GetLibraryInventory() const;

private:
    // 准备任务 READY 后（后台线程回调）把 ReadReady 序列化并写入对应通知 Stream；
    // 会话已关闭或写入失败时仅记录，本阶段不重试。
    void SendReadReady(const optical_node_manager::PreparedFileReadyEvent& ev);

    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    // 副本同步超时；当前仅由 ConfigureReplication 写入，后续副本数据同步启用时消费。
    uint32_t replication_timeout_ms_{2000};

    // 读取完成通知通道（session_id -> brpc Stream）；由 SubscribeReadEvents 登记。
    ReadEventSessionRegistry read_event_sessions_;

    // 本节点的归档引擎：接收 MDS 下发的归档文件、驱动镜像封装与刻录，
    // 并承载数据面读链路（PrepareRead / AcquireReadSession / ReadPreparedFile）。
    optical_node_manager::OpticalNodeManager optical_node_manager_;
};

} // namespace zb::optical_node
