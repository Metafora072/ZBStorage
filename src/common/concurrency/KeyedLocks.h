#pragma once
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace zb::concurrency {
// Owners retain the shared_ptr until after their lock guard is destroyed.
class KeyedLocks {
public:
    std::shared_ptr<std::shared_mutex> Get(const std::string& key) {
        std::lock_guard<std::mutex> guard(mu_);
        if (++calls_ % 256 == 0) {
            for (auto it = locks_.begin(); it != locks_.end();)
                if (it->second.expired()) it = locks_.erase(it); else ++it;
        }
        auto& weak = locks_[key];
        auto lock = weak.lock();
        if (!lock) { lock = std::make_shared<std::shared_mutex>(); weak = lock; }
        return lock;
    }
private:
    std::mutex mu_;
    size_t calls_{0};
    std::unordered_map<std::string, std::weak_ptr<std::shared_mutex>> locks_;
};
}
