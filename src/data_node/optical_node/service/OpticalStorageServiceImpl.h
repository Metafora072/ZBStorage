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
    // 转发给内部 OpticalNodeManager（仅校验入队，立即返回）。
    volumemanager::ErrorCode SendArchiveMetadata(
        const optical_node_manager::SendArchiveMetadataRequest& request);

    // 提交异步读任务，立即返回 task_id。入参 disk_id 形如 "optical-disk-<seq>"、
    // image_id 形如 "img-<seq>"；本层取出尾部 <seq> 后再转发给
    // OpticalNodeManager::RequestAsyncReadFile。
    volumemanager::ErrorCode RequestAsyncReadFile(const std::string& disk_id,
                                                  const std::string& image_id,
                                                  const std::string& inode_id,
                                                  uint64_t* task_id);

    // 按 task_id 读取 [offset, offset+read_size) 区间；转发给 OpticalNodeManager。
    volumemanager::ErrorCode ReadObjectByTaskId(uint64_t task_id,
                                                std::string* out,
                                                uint64_t offset,
                                                uint64_t read_size);

    // 按 inode_id 读取最近一次 RequestAsyncReadFile 产物的区间；转发给 OpticalNodeManager。
    volumemanager::ErrorCode ReadObjectByInodeId(const std::string& inode_id,
                                                 std::string* out,
                                                 uint64_t offset,
                                                 uint64_t read_size);

    // 归档引擎是否已成功启动（状态为 RUNNING）。
    bool IsArchiveEngineReady() const;

    // 归档引擎当前状态 + 最近一次失败原因，仅用于排障 / 日志。
    std::string GetArchiveStatusDetail() const;

private:
    mutable std::mutex repl_mu_;
    ReplicationStatusSnapshot repl_;
    // 副本同步超时；当前仅由 ConfigureReplication 写入，后续副本数据同步启用时消费。
    uint32_t replication_timeout_ms_{2000};

    // 本节点的归档引擎：接收 MDS 下发的归档文件、驱动镜像封装与刻录，
    // 并承载数据面异步读（RequestAsyncReadFile / ReadObjectBy*）。
    optical_node_manager::OpticalNodeManager optical_node_manager_;
};

} // namespace zb::optical_node
