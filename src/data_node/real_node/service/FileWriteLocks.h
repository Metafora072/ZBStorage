#pragma once

#include <mutex>
#include <random>
#include <shared_mutex>
#include <unordered_map>
#include "common/concurrency/KeyedLocks.h"
#include "real_node.pb.h"

namespace zb::real_node {

// RPC-boundary file locks. Per-inode gates keep Release from overtaking an
// executing mutation; unrelated inodes and object slices can still run together.
class FileWriteLocks {
public:
    struct Access {
        std::shared_ptr<std::shared_mutex> gate;
        std::shared_lock<std::shared_mutex> guard;
        bool allowed{false};
        explicit Access(std::shared_ptr<std::shared_mutex> g)
            : gate(std::move(g)), guard(*gate) {}
    };

    bool Acquire(uint64_t inode, std::string* token, rpc::Status* status) {
        if (!inode) { status->set_code(rpc::STATUS_INVALID_ARGUMENT); return false; }
        auto gate = gates_.Get(std::to_string(inode));
        std::unique_lock<std::shared_mutex> guard(*gate, std::try_to_lock);
        if (!guard.owns_lock()) { Busy(status); return false; }
        std::lock_guard<std::mutex> lock(mu_);
        if (owners_.count(inode)) { Busy(status); return false; }
        *token = session_ + "-" + std::to_string(++sequence_);
        owners_.emplace(inode, *token);
        return true;
    }

    bool Release(uint64_t inode, const std::string& token, rpc::Status* status) {
        auto gate = gates_.Get(std::to_string(inode));
        std::unique_lock<std::shared_mutex> guard(*gate);
        std::lock_guard<std::mutex> lock(mu_);
        auto it = owners_.find(inode);
        if (token.empty() || it == owners_.end() || it->second != token) {
            Busy(status); return false;
        }
        owners_.erase(it);
        return true;
    }

    Access Check(uint64_t inode, const std::string& token, bool required, rpc::Status* status) {
        Access access(gates_.Get(std::to_string(inode)));
        std::lock_guard<std::mutex> lock(mu_);
        auto it = owners_.find(inode);
        access.allowed = it == owners_.end() ? (!required && token.empty())
                                             : (!token.empty() && it->second == token);
        if (!access.allowed) Busy(status);
        return access;
    }

private:
    static void Busy(rpc::Status* status) {
        status->set_code(rpc::STATUS_BUSY);
        status->set_message("FILE_WRITE_LOCK_BUSY_OR_INVALID_TOKEN");
    }
    concurrency::KeyedLocks gates_;
    std::mutex mu_;
    std::unordered_map<uint64_t, std::string> owners_;
    const std::string session_ = std::to_string(std::random_device{}()) + "-" +
                                 std::to_string(std::random_device{}());
    uint64_t sequence_{0};
};

} // namespace zb::real_node
