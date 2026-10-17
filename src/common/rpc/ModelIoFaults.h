#pragma once

#include <cstdint>
#include <mutex>
#include <string>

namespace zb::model {

enum class IoFault { kNone, kFailure, kTimeout };

// 请求受理点故障注入：概率单位 ppm，失败与超时互斥；0 默认关闭。
// 固定节点/种子及相同受理顺序得到同一序列；不承诺并发到达顺序或跨重启延续 RNG。
class IoFaultPolicy {
public:
    IoFaultPolicy(uint32_t failure_ppm, uint32_t timeout_ppm, uint64_t seed, const std::string& node_id)
        : failure_(failure_ppm), timeout_(timeout_ppm), state_(seed) {
        if (!state_) {
            state_ = 14695981039346656037ULL;
            for (unsigned char c : node_id) { state_ ^= c; state_ *= 1099511628211ULL; }
        }
        if (!state_) state_ = 1;
    }
    IoFault Next() {
        if (failure_ == 0 && timeout_ == 0) return IoFault::kNone;
        std::lock_guard<std::mutex> lock(mu_);
        state_ ^= state_ >> 12; state_ ^= state_ << 25; state_ ^= state_ >> 27;
        const uint64_t draw = (state_ * 2685821657736338717ULL) % 1000000;
        if (draw < failure_) return IoFault::kFailure;
        return draw < static_cast<uint64_t>(failure_) + timeout_ ? IoFault::kTimeout : IoFault::kNone;
    }
private:
    uint32_t failure_, timeout_;
    uint64_t state_;
    std::mutex mu_;
};

} // namespace zb::model
