#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "optical_node.pb.h"

namespace zb::archive_inventory {

// 仅用于分页变更检测，不是内容校验或安全签名。调用方仍需校验节点代际和 MDS 版本。
inline std::string Token(const std::vector<rpc::ArchiveImageInfo>& images,
                         const std::vector<rpc::ArchiveImageFile>& files = {}) {
    uint64_t hash = 14695981039346656037ULL;
    const auto append = [&hash](const std::string& bytes) {
        const uint64_t size = bytes.size();
        for (unsigned i = 0; i != 8; ++i) {
            hash ^= (size >> (i * 8)) & 255;
            hash *= 1099511628211ULL;
        }
        for (unsigned char byte : bytes) {
            hash ^= byte;
            hash *= 1099511628211ULL;
        }
    };
    for (const auto& image : images) append(image.SerializeAsString());
    append("files");
    for (const auto& file : files) append(file.SerializeAsString());
    return std::to_string(hash);
}

template <class Request>
inline bool ValidatePage(const Request& request, rpc::OpticalStatus* status) {
    if (request.node_id().empty() || request.instance_generation() == 0 ||
        request.limit() > 1024 || (request.offset() != 0 && request.snapshot_token().empty())) {
        status->set_code(rpc::OPTICAL_INVALID_ARGUMENT);
        status->set_message("node_id/nonzero generation required; limit <= 1024; continuation requires token");
        return false;
    }
    return true;
}

inline std::string TaskToken(const std::vector<rpc::ArchiveTaskInfo>& tasks) {
    // 复用带长度前缀的序列哈希；只作变更检测，不作安全/载荷校验。
    std::vector<rpc::ArchiveImageInfo> encoded;
    encoded.reserve(tasks.size());
    for (const auto& task : tasks) encoded.emplace_back().set_image_id(task.SerializeAsString());
    return Token(encoded);
}

template <class Request, class Reply>
inline bool Page(const Request& request, const std::string& token, size_t count,
                 Reply* reply, size_t* end) {
    reply->set_snapshot_token(token);
    if (!request.snapshot_token().empty() && request.snapshot_token() != token) {
        reply->mutable_status()->set_code(rpc::OPTICAL_CONFLICT);
        reply->mutable_status()->set_message("inventory changed; discard accumulated pages and restart");
        return false;
    }
    if (request.offset() > count) {
        reply->mutable_status()->set_code(rpc::OPTICAL_INVALID_ARGUMENT);
        reply->mutable_status()->set_message("offset exceeds inventory size");
        return false;
    }
    *end = request.offset() + std::min<size_t>(
        request.limit() ? request.limit() : 128, count - request.offset());
    reply->set_has_more(*end < count);
    reply->set_next_offset(reply->has_more() ? static_cast<uint32_t>(*end) : 0);
    reply->mutable_status()->set_code(rpc::OPTICAL_OK);
    return true;
}

} // namespace zb::archive_inventory
