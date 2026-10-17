#pragma once
#include <bthread/rwlock.h>

namespace zb::rpc_support {
// RPC 等待会迁移 pthread；跨 RPC 持锁必须使用 bthread 所有权模型。
// 适配 std::unique_lock/shared_lock，维持原来的读共享/写独占语义。
class FiberSharedMutex {
public:
    void lock() { lock_.wrlock(); }
    void unlock() { lock_.unlock(); }
    bool try_lock() { return lock_.try_wrlock(); }
    void lock_shared() { lock_.rdlock(); }
    void unlock_shared() { lock_.unlock(); }
    bool try_lock_shared() { return lock_.try_rdlock(); }
private:
    bthread::RWLock lock_;
};
} // namespace zb::rpc_support
