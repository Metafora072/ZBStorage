#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include <optical_node_manager.h>

#include "../config/OpticalNodeConfig.h"

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
    // 转发给内部 OpticalNodeManager。当前 OpticalNodeManager::SendArchiveMetadata
    // 尚未实现，调用会在链接期报未定义符号。
    volumemanager::ErrorCode SendArchiveMetadata(
        const optical_node_manager::SendArchiveMetadataRequest& request);

    // 归档引擎是否已成功启动（状态为 RUNNING）。
    bool IsArchiveEngineReady() const;

    // 归档引擎当前状态 + 最近一次失败原因，仅用于排障 / 日志。
    std::string GetArchiveStatusDetail() const;

private:
    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    // 副本同步超时；当前仅由 ConfigureReplication 写入，后续副本数据同步启用时消费。
    uint32_t replication_timeout_ms_{2000};

    // 本节点的归档引擎：接收 MDS 下发的归档文件、驱动镜像封装与刻录。
    // 当前仅被本 Impl 持有并初始化，SendArchiveMetadata 业务逻辑尚未落地。
    optical_node_manager::OpticalNodeManager optical_node_manager_;
};

} // namespace zb::optical_node
