#pragma once

#include <brpc/controller.h>

#include <charconv>
#include <cstdint>
#include <string>

namespace zb::rpc_route {

// The attachment works for methods whose protobuf request is Empty as well.
// All calls to a shared data address carry the same explicit logical target.
inline bool SetLogicalNodeRoute(brpc::Controller* controller,
                                const std::string& node_id,
                                uint64_t generation) {
    if (!controller || node_id.empty() || node_id.size() > 128 || generation == 0 ||
        node_id.find('\n') != std::string::npos) {
        return false;
    }
    const std::string encoded = "ZBSIM1\n" + node_id + "\n" +
                                std::to_string(generation) + "\n";
    controller->request_attachment().append(encoded);
    return true;
}

inline bool GetLogicalNodeRoute(const brpc::Controller& controller,
                                std::string* node_id,
                                uint64_t* generation) {
    if (!node_id || !generation) return false;
    const std::string encoded = controller.request_attachment().to_string();
    constexpr char prefix[] = "ZBSIM1\n";
    if (encoded.size() <= sizeof(prefix) || encoded.compare(0, sizeof(prefix) - 1, prefix) != 0) {
        return false;
    }
    const size_t id_start = sizeof(prefix) - 1;
    const size_t id_end = encoded.find('\n', id_start);
    if (id_end == std::string::npos || id_end == id_start || id_end - id_start > 128 ||
        encoded.back() != '\n') {
        return false;
    }
    const size_t value_start = id_end + 1;
    if (value_start >= encoded.size() - 1) return false;
    const char* first = encoded.data() + value_start;
    const char* last = encoded.data() + encoded.size() - 1;
    uint64_t parsed = 0;
    const auto result = std::from_chars(first, last, parsed);
    if (result.ec != std::errc{} || result.ptr != last || parsed == 0) return false;
    *node_id = encoded.substr(id_start, id_end - id_start);
    *generation = parsed;
    return true;
}

} // namespace zb::rpc_route
