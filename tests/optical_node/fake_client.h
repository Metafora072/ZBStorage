#pragma once

// 仿真上层交互的假客户端：
//  - FakeMdsDriver 扮演 MDS，通过 brpc 向光节点下发归档批次（SendArchiveMetadata）；
//  - FakeReadClient 扮演数据面客户端，发起异步读并按分片取数据。
// 当前光节点不调用任何 MDS RPC（volume_id/disk_id 本地递增、打包汇报走控制台），
// 因此 MDS 侧不需要真的 server，只以客户端形态驱动。

#include <brpc/channel.h>
#include <optical_node.pb.h>

#include <cstdint>
#include <string>
#include <vector>

namespace zb::optical_node::test {

class FakeMdsDriver {
public:
    explicit FakeMdsDriver(const std::string& node_address);

    // 下发一批归档文件元数据；返回 false 时 *error 带传输/状态错误详情。
    bool SendBatch(uint64_t batch_id,
                   const std::vector<zb::rpc::ArchiveFile>& files,
                   std::string* error);

private:
    brpc::Channel channel_;
    zb::rpc::OpticalNodeService_Stub stub_;
    bool init_ok_{false};
};

class FakeReadClient {
public:
    explicit FakeReadClient(const std::string& node_address);

    // 提交异步读任务；MDS_OK 表示已受理（*task_id 有效）。
    bool RequestRead(const std::string& disk_id,
                     const std::string& image_id,
                     uint64_t inode_id,
                     uint64_t* task_id,
                     zb::rpc::MdsStatusCode* status,
                     std::string* error);

    // 按 task_id 读 [offset, offset+read_size)，数据追加到 *data 末尾。
    bool ReadByTask(uint64_t task_id,
                    uint64_t offset,
                    uint64_t read_size,
                    std::string* data,
                    zb::rpc::MdsStatusCode* status,
                    std::string* error);

    // 按 inode_id 读最近一次读请求的产物区间。
    bool ReadByInode(uint64_t inode_id,
                     uint64_t offset,
                     uint64_t read_size,
                     std::string* data,
                     zb::rpc::MdsStatusCode* status,
                     std::string* error);

private:
    brpc::Channel channel_;
    zb::rpc::OpticalNodeService_Stub stub_;
    bool init_ok_{false};
};

}  // namespace zb::optical_node::test