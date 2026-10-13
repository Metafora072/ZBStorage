#pragma once

// 仿真上层交互的假客户端：
//  - FakeMdsDriver 扮演 MDS，通过 brpc 向光节点下发归档批次（SendArchiveMetadata）；
//  - FakeReadClient 扮演数据面客户端，实现「光盘读接口文档」约定的一条完整链路：
//      SubscribeReadEvents（附带 Stream 接收 ReadReady 通知）
//      → PrepareRead → （收到 ReadReady）→ AcquireReadSession
//      → ReadPreparedFile → ReleaseReadSession → ReleasePreparedRead。
// 当前光节点不调用任何 MDS RPC（volume_id/disk_id 本地递增、打包汇报走控制台），
// 因此 MDS 侧不需要真的 server，只以客户端形态驱动。

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/stream.h>
#include <optical_node.pb.h>

#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
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

// 客户端经通知 Stream 收到的 ReadReady（完整文件准备完成事件）。
struct ReadReadyNotice {
    std::string request_id;
    uint64_t task_id{0};
    uint64_t inode_id{0};
    uint64_t file_size{0};
    bool has_file_version{false};
    uint64_t file_version{0};
};

// 假数据面客户端：实现光盘读接口的 6 个 RPC 与 ReadReady 通知接收。
class FakeReadClient {
public:
    explicit FakeReadClient(const std::string& node_address);
    ~FakeReadClient();

    FakeReadClient(const FakeReadClient&) = delete;
    FakeReadClient& operator=(const FakeReadClient&) = delete;

    // 建立通知通道（SubscribeReadEvents，随 RPC 附带 Stream）；成功回填 session_id。
    bool SubscribeReadEvents(const std::string& client_id,
                             uint64_t* session_id,
                             zb::rpc::OpticalStatusCode* status,
                             std::string* error);

    // 提交整文件异步准备任务；成功（OPTICAL_OK）只表示受理，不代表可读。
    bool PrepareRead(uint64_t session_id,
                     const std::string& request_id,
                     uint64_t inode_id,
                     const std::string& disk_id,
                     const std::string& image_id,
                     uint64_t file_size,
                     uint64_t* task_id,
                     zb::rpc::OpticalStatusCode* status,
                     std::string* error);

    // 为一次独立打开建立读会话；成功回填 read_handle / file_size / max_read_bytes。
    bool AcquireReadSession(uint64_t task_id,
                            uint64_t* read_handle,
                            uint64_t* file_size,
                            uint32_t* max_read_bytes,
                            zb::rpc::OpticalStatusCode* status,
                            std::string* error);

    // 在已建立的读会话上读取 [offset, offset+size)；数据追加到 *out 末尾，
    // 成功时回填 bytes_read / eof。
    bool ReadPreparedFile(uint64_t read_handle,
                          uint64_t offset,
                          uint64_t size,
                          std::string* out,
                          uint64_t* bytes_read,
                          bool* eof,
                          zb::rpc::OpticalStatusCode* status,
                          std::string* error);

    // 关闭一次读取会话；只减少读取引用。
    bool ReleaseReadSession(uint64_t read_handle,
                            zb::rpc::OpticalStatusCode* status,
                            std::string* error);

    // 释放本轮准备的准备引用；不再允许为该任务创建新读会话。
    bool ReleasePreparedRead(uint64_t task_id,
                             zb::rpc::OpticalStatusCode* status,
                             std::string* error);

    // 等待匹配 request_id 的 ReadReady（超时返回 false）；命中后从待取列表中移除。
    bool WaitReadReady(const std::string& request_id,
                       ReadReadyNotice* out,
                       int timeout_ms);

    // 取出并清空当前已到达的全部 ReadReady（用于观察是否补发通知等）。
    std::vector<ReadReadyNotice> DrainReadReady();

    // 通知通道是否已建立（订阅成功后为 true）。
    bool streaming() const { return stream_created_; }

private:
    // 通知接收处理器：解析 Stream 上的 ReadReady 消息并缓存，供测试线程等待/取用。
    class Receiver : public brpc::StreamInputHandler {
    public:
        int on_received_messages(brpc::StreamId id,
                                 butil::IOBuf* const messages[],
                                 size_t size) override;
        void on_idle_timeout(brpc::StreamId id) override;
        void on_closed(brpc::StreamId id) override;

        void Push(const ReadReadyNotice& notice);
        bool Wait(const std::string& request_id, ReadReadyNotice* out, int timeout_ms);
        std::vector<ReadReadyNotice> Drain();

    private:
        std::mutex mu_;
        std::condition_variable cv_;
        std::vector<ReadReadyNotice> pending_;
    };

    // brpc 客户端 Stream 的 handler 生命周期必须覆盖到「对端把连接彻底拆除」之后：
    // 节点侧 server 停止 / RST 时可能再回调一次 on_closed，若此时 handler 已随客户端析构，
    // 就会命中基类纯虚函数并 abort（pure virtual method called）。故接收器不随客户端析构，
    // 而是登记到进程级表里保留到进程退出（测试进程存活期有限，数量至多为客户端创建次数）。
    static std::vector<std::unique_ptr<Receiver>>& RetainedReceivers();

    brpc::Channel channel_;
    zb::rpc::OpticalNodeService_Stub stub_;
    bool init_ok_{false};
    // 通知接收处理器：由 RetainedReceivers() 持有，本指针仅借用。
    Receiver* receiver_{nullptr};
    // 订阅通知通道所用的 Controller：与 Stream 同生命周期，避免 Stream 仍在使用时被析构。
    brpc::Controller subscribe_controller_;
    brpc::StreamId stream_id_{brpc::INVALID_STREAM_ID};
    bool stream_created_{false};
    uint64_t session_id_{0};
    std::string client_id_;
};

}  // namespace zb::optical_node::test
