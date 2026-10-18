#pragma once

#include <bthread/bthread.h>
#include <chrono>
#include <cstdint>
#include <system_error>

namespace zb::rpc_route {

// std::shared_mutex 绑定 pthread，不能跨 bthread 挂起/迁移持有。
// 本锁支持 std::shared_lock / unique_lock，同时兼容后台 pthread。
class ModelSharedMutex {
public:
    ModelSharedMutex() { Check(bthread_rwlock_init(&lock_, nullptr)); }
    ~ModelSharedMutex() { bthread_rwlock_destroy(&lock_); }
    ModelSharedMutex(const ModelSharedMutex&) = delete;
    ModelSharedMutex& operator=(const ModelSharedMutex&) = delete;
    void lock() { Check(bthread_rwlock_wrlock(&lock_)); }
    bool try_lock() { return bthread_rwlock_trywrlock(&lock_) == 0; }
    void unlock() { bthread_rwlock_unlock(&lock_); }
    void lock_shared() { Check(bthread_rwlock_rdlock(&lock_)); }
    bool try_lock_shared() { return bthread_rwlock_tryrdlock(&lock_) == 0; }
    void unlock_shared() { bthread_rwlock_unlock(&lock_); }
private:
    static void Check(int code) {
        if (code) throw std::system_error(code, std::system_category(), "model admission lock");
    }
    bthread_rwlock_t lock_;
};

// 模型设备等待只挂起当前 bthread，释放 BRPC worker 给管理及其他节点。
// 用单调时钟重新计算剩余时间，兼容中断及普通 pthread 调用者。
inline void WaitModelUntil(std::chrono::steady_clock::time_point deadline) {
    while (std::chrono::steady_clock::now() < deadline) {
        const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining > 0) bthread_usleep(static_cast<uint64_t>(remaining));
    }
}

} // namespace zb::rpc_route
