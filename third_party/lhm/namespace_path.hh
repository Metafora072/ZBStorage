#ifndef MASSTREELHM_NAMESPACE_PATH_HH
#define MASSTREELHM_NAMESPACE_PATH_HH

#include <string>

#include "path_key.hh"

namespace MasstreeLHM {
namespace NamespacePath {

inline bool is_root(const std::string& normalized_path) {
    return normalized_path == "/";
}

inline bool has_child_component(const ParsedPath& parsed) {
    return !is_root(parsed.normalized_path) && !parsed.components.empty()
        && !parsed.hashes.empty();
}

inline std::string parent_path(const std::string& normalized_path) {
    if (normalized_path == "/") {
        return "/";
    }

    size_t pos = normalized_path.find_last_of('/');
    if (pos == 0) {
        return "/";
    }
    return normalized_path.substr(0, pos);
}

inline bool make_parent_parsed(const ParsedPath& child, ParsedPath& parent) {
    if (!has_child_component(child)) {
        return false;
    }
    parent = child;
    parent.normalized_path = parent_path(child.normalized_path);
    parent.components.pop_back();
    parent.hashes.pop_back();
    return true;
}

inline bool is_prefix_path(const std::string& prefix, const std::string& path) {
    if (prefix == "/") {
        return true;
    }
    if (path.size() < prefix.size()) {
        return false;
    }
    if (path.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    return path.size() == prefix.size() || path[prefix.size()] == '/';
}

}  // namespace NamespacePath
}  // namespace MasstreeLHM

#endif
