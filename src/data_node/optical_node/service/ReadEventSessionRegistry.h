#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <brpc/stream.h>
#include <butil/iobuf.h>

namespace zb::optical_node {

// 通知管理组件：保存 session_id 与其对应的 brpc Stream，并在需要时向该 Stream
// 写入 ReadReady 消息（对应光盘读接口文档 4.1 的「通知管理组件」）。
// 本组件只管通知通道本身，不感知准备任务；任务与 session 的对应关系由协调层保存。
//
// 线程安全：订阅（RegisterSession）、发送（SendMessage）、对端关闭回调（Handler::on_closed）
// 与关机（CloseAll）可能来自不同线程，会话表由 mu_ 保护；brpc::StreamWrite 非线程安全，
// 因此每个会话各带一把写锁。
class ReadEventSessionRegistry {
public:
    ReadEventSessionRegistry() = default;
    ~ReadEventSessionRegistry();

    ReadEventSessionRegistry(const ReadEventSessionRegistry&) = delete;
    ReadEventSessionRegistry& operator=(const ReadEventSessionRegistry&) = delete;

    // 登记一个已接受的通知会话，返回节点内唯一的 session_id；流无效时返回 0。
    // server_stream 由 brpc::StreamAccept 取得，其关闭由本组件负责。
    uint64_t RegisterSession(const std::string& client_id, brpc::StreamId server_stream);

    // 按 session_id 向对应 Stream 写一条消息；会话不存在或写入失败时返回 false。
    bool SendMessage(uint64_t session_id, const butil::IOBuf& message);

    // 指定会话是否存在；供 PrepareRead 校验 session_id 是否有效。
    bool HasSession(uint64_t session_id);

    // 关闭全部通知会话（关机时调用，幂等）。
    void CloseAll();

    // 传给 brpc::StreamOptions.handler，用于感知对端关闭；生命周期与本组件一致。
    brpc::StreamInputHandler* handler() { return &handler_; }

private:
    // 会话记录：client_id、服务端 Stream 与该会话的写锁。
    struct Session {
        std::string client_id;
        brpc::StreamId stream_id{brpc::INVALID_STREAM_ID};
        std::mutex write_mu;
    };

    // 凭 StreamId 反查并移除会话（对端关闭时由 brpc 回调触发）。
    void OnStreamClosed(brpc::StreamId stream_id);

    // 感知对端关闭；转发给 OnStreamClosed。
    class Handler : public brpc::StreamInputHandler {
    public:
        explicit Handler(ReadEventSessionRegistry* owner) : owner_(owner) {}

        int on_received_messages(brpc::StreamId id,
                                 butil::IOBuf* const messages[],
                                 size_t size) override;
        void on_idle_timeout(brpc::StreamId id) override;
        void on_closed(brpc::StreamId id) override;

    private:
        ReadEventSessionRegistry* owner_{nullptr};
    };

    Handler handler_{this};
    // session_id 分配器；0 保留为「无效」。
    std::atomic<uint64_t> next_session_id_{1};

    std::mutex mu_;
    // session_id -> 会话。
    std::unordered_map<uint64_t, std::shared_ptr<Session>> sessions_;
    // 反向索引：StreamId -> session_id，供 on_closed 反查。
    std::unordered_map<brpc::StreamId, uint64_t> stream_to_session_;
};

}  // namespace zb::optical_node