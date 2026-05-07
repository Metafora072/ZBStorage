#include "MdsServiceImpl.h"

#include <brpc/controller.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "../masstree_meta/MasstreeManifest.h"
#include "../storage/UnifiedInodeLocation.h"
#include "../storage/UnifiedInodeRecord.h"
#include "real_node.pb.h"

namespace zb::mds {

namespace {

constexpr const char* kReadOnlyOpticalMessage = "READ_ONLY_OPTICAL";
constexpr const char* kNoSpaceRealPolicyMessage = "NO_SPACE_REAL_POLICY";
constexpr const char* kNoSpaceVirtualPolicyMessage = "NO_SPACE_VIRTUAL_POLICY";
constexpr const char* kCrossTierRenameMessage = "CROSS_TIER_RENAME";
constexpr size_t kMaxRetainedMasstreeImportJobs = 1000;
constexpr uint32_t kDefaultPosixBlockSize = 4096;
constexpr uint64_t kDefaultDirectorySizeBytes = 4096;
constexpr const char* kMasstreeQueryModeRandomInode = "random_inode";
constexpr const char* kMasstreeQueryModeRandomPathLookup = "random_path_lookup";
constexpr size_t kMinRepeatWidth = 6;
std::mutex g_masstree_query_path_source_mu;

std::string TrimCopy(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

std::string ToLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

bool IsPathListSourceMode(const std::string& source_mode) {
    return ToLowerAscii(TrimCopy(source_mode)) == "path_list";
}

std::string TemplateManifestPathForId(const std::string& masstree_root, const std::string& template_id) {
    return (std::filesystem::path(masstree_root) / "templates" / template_id / "template.staging" / "manifest.txt")
        .string();
}

size_t DecimalDigits(uint64_t value) {
    size_t digits = 1;
    while (value >= 10U) {
        value /= 10U;
        ++digits;
    }
    return digits;
}

std::string FixedWidthDecimal(uint64_t value, size_t width) {
    std::ostringstream out;
    out.width(static_cast<std::streamsize>(width));
    out.fill('0');
    out << value;
    return out.str();
}

std::string RepeatDirName(const std::string& prefix, uint64_t repeat_index, size_t width) {
    return prefix + "_" + FixedWidthDecimal(repeat_index, width);
}

std::string JoinLogicalMasstreePath(const std::string& path_prefix,
                                    const std::string& repeat_dir_name,
                                    const std::string& relative_file_path) {
    std::string full_path = path_prefix;
    if (full_path.empty() || full_path.back() != '/') {
        full_path.push_back('/');
    }
    full_path += repeat_dir_name;
    if (!relative_file_path.empty()) {
        full_path.push_back('/');
        full_path += relative_file_path;
    }
    return full_path;
}

std::string PathBaseName(const std::string& path) {
    const size_t slash = path.rfind('/');
    return slash == std::string::npos ? path : path.substr(slash + 1U);
}

bool NormalizePathListRelativePath(const std::string& raw_path,
                                   std::string* normalized_path,
                                   bool* explicit_dir,
                                   std::string* error) {
    if (!normalized_path || !explicit_dir) {
        if (error) {
            *error = "path_list normalization outputs are null";
        }
        return false;
    }
    std::string trimmed = TrimCopy(raw_path);
    if (trimmed.empty()) {
        if (error) {
            *error = "empty path_list line";
        }
        return false;
    }
    std::replace(trimmed.begin(), trimmed.end(), '\\', '/');
    *explicit_dir = !trimmed.empty() && trimmed.back() == '/';
    if (trimmed == "." || trimmed == "./") {
        normalized_path->clear();
        if (error) {
            error->clear();
        }
        return true;
    }
    while (trimmed.rfind("./", 0) == 0) {
        trimmed.erase(0, 2);
    }
    while (!trimmed.empty() && trimmed.front() == '/') {
        trimmed.erase(trimmed.begin());
    }
    while (!trimmed.empty() && trimmed.back() == '/') {
        trimmed.pop_back();
    }

    std::stringstream stream(trimmed);
    std::string token;
    std::vector<std::string> parts;
    while (std::getline(stream, token, '/')) {
        token = TrimCopy(token);
        if (token.empty() || token == ".") {
            continue;
        }
        if (token == "..") {
            if (error) {
                *error = "path_list contains unsupported '..' segment";
            }
            return false;
        }
        parts.push_back(token);
    }

    std::ostringstream rebuilt;
    for (size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            rebuilt << '/';
        }
        rebuilt << parts[i];
    }
    *normalized_path = rebuilt.str();
    if (error) {
        error->clear();
    }
    return true;
}

bool PathListEntryIsDirectory(const std::string& normalized_path,
                              bool explicit_dir,
                              bool leaf_nodes_are_files) {
    if (normalized_path.empty() || explicit_dir) {
        return true;
    }
    if (leaf_nodes_are_files) {
        return false;
    }
    return PathBaseName(normalized_path).find('.') == std::string::npos;
}

bool TemplateManifestMatchesNamespaceManifest(const zb::mds::MasstreeNamespaceManifest& candidate,
                                              const zb::mds::MasstreeNamespaceManifest& target) {
    return candidate.source_mode == target.source_mode &&
           candidate.path_list_fingerprint == target.path_list_fingerprint &&
           candidate.repeat_dir_prefix == target.repeat_dir_prefix &&
           candidate.path_list_leaf_nodes_are_files == target.path_list_leaf_nodes_are_files &&
           candidate.template_base_file_count == target.template_base_file_count &&
           candidate.template_repeat_count == target.template_repeat_count &&
           candidate.file_count == target.file_count &&
           candidate.inode_count == target.inode_count &&
           candidate.dentry_count == target.dentry_count;
}

bool ResolveTemplateManifestForNamespace(const std::string& masstree_root,
                                         const zb::mds::MasstreeNamespaceManifest& namespace_manifest,
                                         std::string* template_id,
                                         zb::mds::MasstreeNamespaceManifest* template_manifest,
                                         std::string* error) {
    if (!template_id || !template_manifest) {
        if (error) {
            *error = "template manifest outputs are null";
        }
        return false;
    }

    if (!namespace_manifest.template_id.empty()) {
        const std::string manifest_path = TemplateManifestPathForId(masstree_root, namespace_manifest.template_id);
        if (zb::mds::MasstreeNamespaceManifest::LoadFromFile(manifest_path, template_manifest, error)) {
            *template_id = namespace_manifest.template_id;
            return true;
        }
    }

    namespace fs = std::filesystem;
    const fs::path templates_root = fs::path(masstree_root) / "templates";
    std::error_code ec;
    if (!fs::exists(templates_root, ec)) {
        if (error) {
            *error = "masstree templates root not found: " + templates_root.string();
        }
        return false;
    }

    for (const auto& entry : fs::directory_iterator(templates_root, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to iterate masstree templates root: " + templates_root.string() +
                         " error=" + ec.message();
            }
            return false;
        }
        if (!entry.is_directory()) {
            continue;
        }
        zb::mds::MasstreeNamespaceManifest candidate;
        std::string local_error;
        if (!zb::mds::MasstreeNamespaceManifest::LoadFromFile(
                (entry.path() / "template.staging" / "manifest.txt").string(), &candidate, &local_error)) {
            continue;
        }
        if (!TemplateManifestMatchesNamespaceManifest(candidate, namespace_manifest)) {
            continue;
        }
        *template_manifest = std::move(candidate);
        *template_id = entry.path().filename().string();
        if (error) {
            error->clear();
        }
        return true;
    }

    if (error) {
        *error = "failed to resolve template manifest for namespace: " + namespace_manifest.namespace_id;
    }
    return false;
}

bool BuildQueryPathSourceFromPathList(const std::string& path_list_file,
                                      bool path_list_leaf_nodes_are_files,
                                      const std::string& output_path,
                                      uint64_t* path_count,
                                      std::string* error) {
    if (path_count) {
        *path_count = 0;
    }
    std::ifstream input(path_list_file);
    if (!input) {
        if (error) {
            *error = "failed to open path_list_file: " + path_list_file;
        }
        return false;
    }
    std::ofstream output(output_path, std::ios::trunc);
    if (!output) {
        if (error) {
            *error = "failed to create query path source: " + output_path;
        }
        return false;
    }

    struct PathEntry {
        std::string normalized_path;
        bool explicit_dir{false};
    };

    std::vector<PathEntry> entries;
    std::string line;
    while (std::getline(input, line)) {
        std::string normalized_path;
        bool explicit_dir = false;
        std::string local_error;
        if (!NormalizePathListRelativePath(line, &normalized_path, &explicit_dir, &local_error)) {
            if (local_error == "empty path_list line") {
                continue;
            }
            if (error) {
                *error = local_error;
            }
            return false;
        }
        if (normalized_path.empty()) {
            continue;
        }
        entries.push_back({std::move(normalized_path), explicit_dir});
    }

    std::unordered_set<std::string> parent_paths;
    for (const auto& entry : entries) {
        std::string current = entry.normalized_path;
        while (true) {
            const size_t slash = current.rfind('/');
            if (slash == std::string::npos) {
                break;
            }
            current.resize(slash);
            if (current.empty()) {
                break;
            }
            parent_paths.insert(current);
        }
    }

    uint64_t count = 0;
    for (const auto& entry : entries) {
        const bool has_children = parent_paths.find(entry.normalized_path) != parent_paths.end();
        const bool is_dir =
            entry.explicit_dir || has_children ||
            PathListEntryIsDirectory(entry.normalized_path,
                                     false,
                                     path_list_leaf_nodes_are_files);
        if (is_dir) {
            continue;
        }
        output << entry.normalized_path << '\n';
        ++count;
    }

    output.flush();
    if (!output.good()) {
        if (error) {
            *error = "failed to flush query path source: " + output_path;
        }
        return false;
    }
    if (count == 0) {
        if (error) {
            *error = "path_list_file does not contain any file path: " + path_list_file;
        }
        return false;
    }
    if (path_count) {
        *path_count = count;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool PickRandomNonEmptyLineFromFile(const std::string& path, std::string* line_out, std::string* error) {
    if (!line_out) {
        if (error) {
            *error = "line output is null";
        }
        return false;
    }
    std::error_code stat_error;
    const uint64_t file_size = std::filesystem::file_size(path, stat_error);
    if (stat_error || file_size == 0) {
        if (error) {
            *error = "failed to stat file: " + path;
        }
        return false;
    }

    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> offset_dist(0, file_size - 1U);
    constexpr uint32_t kMaxAttempts = 64;
    for (uint32_t attempt = 0; attempt < kMaxAttempts; ++attempt) {
        std::ifstream input(path, std::ios::binary);
        if (!input) {
            if (error) {
                *error = "failed to open file: " + path;
            }
            return false;
        }
        const uint64_t offset = offset_dist(rng);
        input.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
        if (!input.good()) {
            input.clear();
            input.seekg(0, std::ios::beg);
        } else if (offset != 0) {
            std::string discard;
            std::getline(input, discard);
        }
        std::string line;
        while (std::getline(input, line)) {
            line = TrimCopy(line);
            if (!line.empty()) {
                *line_out = std::move(line);
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
    }

    std::ifstream fallback(path);
    if (!fallback) {
        if (error) {
            *error = "failed to reopen file: " + path;
        }
        return false;
    }
    std::string line;
    while (std::getline(fallback, line)) {
        line = TrimCopy(line);
        if (!line.empty()) {
            *line_out = std::move(line);
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        *error = "query path source is empty: " + path;
    }
    return false;
}

bool PickRandomExistingMasstreeTemplateId(const std::string& masstree_root,
                                          std::string* template_id,
                                          std::string* error) {
    if (!template_id) {
        if (error) {
            *error = "template_id output is null";
        }
        return false;
    }
    namespace fs = std::filesystem;
    const fs::path templates_root = fs::path(masstree_root) / "templates";
    std::error_code ec;
    if (!fs::exists(templates_root, ec)) {
        if (error) {
            *error = "masstree templates root not found: " + templates_root.string();
        }
        return false;
    }
    if (ec) {
        if (error) {
            *error = "failed to inspect masstree templates root: " + templates_root.string() +
                     " error=" + ec.message();
        }
        return false;
    }

    std::vector<std::string> template_ids;
    for (const auto& entry : fs::directory_iterator(templates_root, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to iterate masstree templates root: " + templates_root.string() +
                         " error=" + ec.message();
            }
            return false;
        }
        if (!entry.is_directory()) {
            continue;
        }
        const fs::path manifest_path = entry.path() / "template.staging" / "manifest.txt";
        if (!fs::exists(manifest_path, ec)) {
            if (ec) {
                if (error) {
                    *error = "failed to inspect template manifest: " + manifest_path.string() +
                             " error=" + ec.message();
                }
                return false;
            }
            continue;
        }
        MasstreeNamespaceManifest manifest;
        std::string load_error;
        if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path.string(), &manifest, &load_error)) {
            continue;
        }
        template_ids.push_back(entry.path().filename().string());
    }

    if (template_ids.empty()) {
        if (error) {
            *error = "no available masstree templates found under: " + templates_root.string();
        }
        return false;
    }

    static thread_local std::mt19937_64 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> distribution(0, template_ids.size() - 1U);
    *template_id = template_ids[distribution(generator)];
    if (error) {
        error->clear();
    }
    return true;
}

bool EnsureTemplateQueryPathSource(zb::mds::MasstreeNamespaceManifest* template_manifest,
                                   const std::string& fallback_path_list_file,
                                   std::string* error) {
    if (!template_manifest) {
        if (error) {
            *error = "template manifest is null";
        }
        return false;
    }
    if (!IsPathListSourceMode(template_manifest->source_mode)) {
        if (error) {
            *error = "template does not support txt-backed lookup path generation";
        }
        return false;
    }

    std::filesystem::path query_path_source_path =
        template_manifest->query_path_source_path.empty()
            ? (std::filesystem::path(template_manifest->manifest_path).parent_path() / "query_path_source.txt")
            : std::filesystem::path(template_manifest->query_path_source_path);

    if (template_manifest->query_path_source_count != 0 &&
        std::filesystem::exists(query_path_source_path)) {
        template_manifest->query_path_source_path = query_path_source_path.string();
        if (error) {
            error->clear();
        }
        return true;
    }

    const std::lock_guard<std::mutex> lock(g_masstree_query_path_source_mu);

    zb::mds::MasstreeNamespaceManifest latest_manifest;
    std::string reload_error;
    if (zb::mds::MasstreeNamespaceManifest::LoadFromFile(template_manifest->manifest_path,
                                                         &latest_manifest,
                                                         &reload_error)) {
        if (!latest_manifest.query_path_source_path.empty() &&
            latest_manifest.query_path_source_count != 0 &&
            std::filesystem::exists(latest_manifest.query_path_source_path)) {
            *template_manifest = std::move(latest_manifest);
            if (error) {
                error->clear();
            }
            return true;
        }
        *template_manifest = std::move(latest_manifest);
        query_path_source_path =
            template_manifest->query_path_source_path.empty()
                ? (std::filesystem::path(template_manifest->manifest_path).parent_path() / "query_path_source.txt")
                : std::filesystem::path(template_manifest->query_path_source_path);
    }

    const std::string path_list_file =
        !template_manifest->path_list_file.empty() ? template_manifest->path_list_file : fallback_path_list_file;
    if (path_list_file.empty()) {
        if (error) {
            *error = "template query path source is missing path_list_file";
        }
        return false;
    }

    std::filesystem::create_directories(query_path_source_path.parent_path());
    const std::filesystem::path tmp_path = query_path_source_path.string() + ".tmp";
    uint64_t path_count = 0;
    if (!BuildQueryPathSourceFromPathList(path_list_file,
                                          template_manifest->path_list_leaf_nodes_are_files,
                                          tmp_path.string(),
                                          &path_count,
                                          error)) {
        std::error_code remove_ec;
        std::filesystem::remove(tmp_path, remove_ec);
        return false;
    }

    std::error_code rename_ec;
    std::filesystem::rename(tmp_path, query_path_source_path, rename_ec);
    if (rename_ec) {
        std::error_code remove_ec;
        std::filesystem::remove(tmp_path, remove_ec);
        if (error) {
            *error = "failed to publish query path source: " + rename_ec.message();
        }
        return false;
    }

    template_manifest->path_list_file = path_list_file;
    template_manifest->query_path_source_path = query_path_source_path.string();
    template_manifest->query_path_source_count = path_count;
    if (!template_manifest->SaveToFile(template_manifest->manifest_path, error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool IsLocationMetadataMissing(const std::string& error) {
    return error.empty();
}

bool IsInvalidReaddirRequest(const std::string& error) {
    return error == "not a directory" ||
           error == "invalid archive readdir token" ||
           error == "archive readdir token generation mismatch" ||
           error == "invalid masstree readdir token" ||
           error == "masstree readdir token generation mismatch";
}

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(path);
    while (std::getline(stream, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

std::string ArchiveObjectId(const zb::rpc::ArchiveCandidate& candidate) {
    return candidate.object_id();
}

bool ParseStableObjectId(const std::string& object_id, uint64_t* inode_id) {
    if (!inode_id) {
        return false;
    }
    constexpr const char* kPrefix = "obj-";
    if (object_id.rfind(kPrefix, 0) != 0) {
        return false;
    }
    const size_t split = object_id.find('-', 4);
    if (split == std::string::npos || split == 4) {
        return false;
    }
    try {
        *inode_id = static_cast<uint64_t>(std::stoull(object_id.substr(4, split - 4)));
    } catch (...) {
        return false;
    }
    return *inode_id != 0;
}

bool IsWriteOpenFlags(uint32_t flags) {
    const int access_mode = static_cast<int>(flags) & O_ACCMODE;
    if (access_mode == O_WRONLY || access_mode == O_RDWR) {
        return true;
    }
#ifdef O_TRUNC
    if ((flags & static_cast<uint32_t>(O_TRUNC)) != 0U) {
        return true;
    }
#endif
#ifdef O_APPEND
    if ((flags & static_cast<uint32_t>(O_APPEND)) != 0U) {
        return true;
    }
#endif
    return false;
}

std::string NormalizeMasstreeQueryMode(std::string mode) {
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return mode.empty() ? std::string(kMasstreeQueryModeRandomPathLookup) : mode;
}

bool IsMasstreeQueryModeValid(const std::string& mode) {
    return mode == kMasstreeQueryModeRandomInode || mode == kMasstreeQueryModeRandomPathLookup;
}

uint32_t ComputeObjectCount(uint64_t file_size, uint64_t object_unit_size) {
    if (file_size == 0 || object_unit_size == 0) {
        return 0;
    }
    return static_cast<uint32_t>((file_size + object_unit_size - 1) / object_unit_size);
}

bool NormalizePolicyPath(const std::string& path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::string p = path;
    std::replace(p.begin(), p.end(), '\\', '/');
    if (p.empty() || p[0] != '/') {
        p.insert(p.begin(), '/');
    }
    std::string out;
    out.reserve(p.size() + 1);
    bool prev_slash = false;
    for (char ch : p) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
            out.push_back(ch);
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    if (out.empty()) {
        out = "/";
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    *normalized = std::move(out);
    return true;
}

bool IsTierPlacementTarget(zb::rpc::PathPlacementTarget target) {
    return target == zb::rpc::PATH_PLACEMENT_REAL_ONLY ||
           target == zb::rpc::PATH_PLACEMENT_VIRTUAL_ONLY;
}

bool PlacementTargetToNodeType(zb::rpc::PathPlacementTarget target, NodeType* type) {
    if (!type) {
        return false;
    }
    switch (target) {
        case zb::rpc::PATH_PLACEMENT_REAL_ONLY:
            *type = NodeType::kReal;
            return true;
        case zb::rpc::PATH_PLACEMENT_VIRTUAL_ONLY:
            *type = NodeType::kVirtual;
            return true;
        default:
            return false;
    }
}

const char* NoSpaceMessageForPlacementTarget(zb::rpc::PathPlacementTarget target) {
    switch (target) {
        case zb::rpc::PATH_PLACEMENT_REAL_ONLY:
            return kNoSpaceRealPolicyMessage;
        case zb::rpc::PATH_PLACEMENT_VIRTUAL_ONLY:
            return kNoSpaceVirtualPolicyMessage;
        default:
            return "NO_SPACE_POLICY";
    }
}

void LogRequestFailure(const char* method,
                       const std::string& path,
                       const std::string& error,
                       bool has_target = false,
                       zb::rpc::PathPlacementTarget target = zb::rpc::PATH_PLACEMENT_REAL_ONLY) {
    std::cerr << "[mds] " << method << " failed: path=" << path;
    if (has_target) {
        std::cerr << " target=" << static_cast<int>(target);
    }
    if (!error.empty()) {
        std::cerr << " error=" << error;
    }
    std::cerr << std::endl;
}

std::vector<std::string> BuildPathPrefixCandidates(const std::string& normalized_path) {
    std::vector<std::string> out;
    if (normalized_path.empty() || normalized_path[0] != '/') {
        return out;
    }
    std::string current = normalized_path;
    while (true) {
        out.push_back(current);
        if (current == "/") {
            break;
        }
        const size_t slash = current.find_last_of('/');
        if (slash == std::string::npos || slash == 0) {
            current = "/";
        } else {
            current = current.substr(0, slash);
        }
    }
    return out;
}

void EnsureReplicaObjectId(zb::rpc::ReplicaLocation* replica, const std::string& object_id) {
    if (!replica || object_id.empty()) {
        return;
    }
    replica->set_object_id(object_id);
}

zb::rpc::ReplicaLocation ToReplicaLocation(uint64_t inode_id,
                                           uint64_t file_size,
                                           const zb::rpc::DiskFileLocation& location) {
    zb::rpc::ReplicaLocation replica;
    replica.set_node_id(location.node_id());
    replica.set_node_address(location.node_address());
    replica.set_disk_id(location.disk_id());
    replica.set_object_id("obj-" + std::to_string(inode_id) + "-0");
    replica.set_size(file_size);
    replica.set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    return replica;
}

zb::rpc::ReplicaLocation ToReplicaLocation(uint64_t inode_id,
                                           uint64_t file_size,
                                           const zb::rpc::OpticalFileLocation& location) {
    zb::rpc::ReplicaLocation replica;
    replica.set_node_id(location.node_id());
    replica.set_node_address(location.node_address());
    replica.set_disk_id(location.disk_id());
    replica.set_object_id(location.file_id().empty() ? "obj-" + std::to_string(inode_id) + "-0"
                                                     : location.file_id());
    replica.set_size(file_size);
    replica.set_storage_tier(zb::rpc::STORAGE_TIER_OPTICAL);
    replica.set_replica_state(zb::rpc::REPLICA_READY);
    replica.set_image_id(location.image_id());
    return replica;
}

bool EncodeUnifiedInodePayload(const UnifiedInodeRecord& record, std::string* payload, std::string* error) {
    return MetaCodec::EncodeUnifiedInodeRecord(record, payload, error);
}

uint64_t ComputeBlocks512(uint64_t size_bytes) {
    return size_bytes == 0 ? 0 : ((size_bytes + 511ULL) / 512ULL);
}

void FillPosixInodeDefaults(zb::rpc::InodeAttr* attr) {
    if (!attr) {
        return;
    }
    if (attr->type() == zb::rpc::INODE_DIR && attr->size() == 0) {
        attr->set_size(kDefaultDirectorySizeBytes);
    }
    if (attr->blksize() == 0) {
        attr->set_blksize(kDefaultPosixBlockSize);
    }
    attr->set_blocks_512(ComputeBlocks512(attr->size()));
    attr->set_rdev_major(0);
    attr->set_rdev_minor(0);
}

} // namespace

MdsServiceImpl::MdsServiceImpl(RocksMetaStore* store,
                               ObjectAllocator* allocator,
                               uint64_t default_object_unit_size,
                               const std::string& archive_meta_root,
                               const std::string& masstree_root,
                               const ArchiveMetaStore::Options& archive_meta_options,
                               uint32_t archive_import_page_size_bytes,
                               bool strict_tier_bypass_pg,
                               bool masstree_preload_all_sparse_on_start,
                               double masstree_preload_memory_utilization_limit,
                               uint64_t masstree_preload_memory_reserve_bytes,
                               double masstree_preload_estimate_multiplier,
                               bool masstree_preload_background,
                               FileArchiveCandidateQueue* candidate_queue,
                               ArchiveLeaseManager* lease_manager)
    : store_(store),
      allocator_(allocator),
      default_object_unit_size_(default_object_unit_size),
      archive_meta_root_(archive_meta_root),
      masstree_root_(masstree_root),
      archive_import_page_size_bytes_(archive_import_page_size_bytes),
      strict_tier_bypass_pg_(strict_tier_bypass_pg),
      masstree_preload_all_sparse_on_start_(masstree_preload_all_sparse_on_start),
      masstree_preload_memory_utilization_limit_(masstree_preload_memory_utilization_limit),
      masstree_preload_memory_reserve_bytes_(masstree_preload_memory_reserve_bytes),
      masstree_preload_estimate_multiplier_(masstree_preload_estimate_multiplier),
      masstree_preload_background_(masstree_preload_background),
      candidate_queue_(candidate_queue),
      lease_manager_(lease_manager),
      archive_namespace_catalog_(store),
      archive_meta_store_(),
      masstree_namespace_catalog_(store),
      masstree_import_service_(store, &masstree_namespace_catalog_),
      masstree_meta_store_(),
      archive_import_service_(store, &archive_namespace_catalog_),
      meta_router_(store,
                   &archive_namespace_catalog_,
                   &archive_meta_store_,
                   &masstree_namespace_catalog_,
                   &masstree_meta_store_) {
    std::string error;
    archive_meta_store_.Init(archive_meta_root, archive_meta_options, &error);
    RecoverArchiveCatalog(&error);
    EnsureRoot(&error);
    masstree_import_worker_ = std::thread(&MdsServiceImpl::RunMasstreeImportWorker, this);
    TryPreloadMasstreeSparseIndexes();
}

MdsServiceImpl::~MdsServiceImpl() {
    {
        std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
        stop_masstree_import_worker_ = true;
    }
    masstree_import_job_cv_.notify_all();
    if (masstree_import_worker_.joinable()) {
        masstree_import_worker_.join();
    }
    if (masstree_preload_worker_.joinable()) {
        masstree_preload_worker_.join();
    }
}

void MdsServiceImpl::TryPreloadMasstreeSparseIndexes() {
    if (!masstree_preload_all_sparse_on_start_ || masstree_root_.empty()) {
        return;
    }
    if (masstree_preload_background_) {
        masstree_preload_worker_ = std::thread(&MdsServiceImpl::RunMasstreePreloadWorker, this);
        return;
    }
    RunMasstreePreloadWorker();
}

void MdsServiceImpl::RunMasstreePreloadWorker() {
    std::vector<MasstreeNamespaceRoute> routes;
    std::string error;
    if (!masstree_namespace_catalog_.ListRoutes(&routes, &error)) {
        std::cerr << "[masstree-preload] failed to list routes: " << error << std::endl;
        return;
    }
    if (routes.empty()) {
        std::cerr << "[masstree-preload] no routes found, skip preload" << std::endl;
        return;
    }

    uint64_t estimated_total_bytes = 0;
    for (const auto& route : routes) {
        uint64_t estimated_bytes = 0;
        std::string local_error;
        if (!masstree_meta_store_.EstimateGenerationSparseMemoryBytes(
                route, masstree_preload_estimate_multiplier_, &estimated_bytes, &local_error)) {
            std::cerr << "[masstree-preload] failed to estimate route namespace=" << route.namespace_id
                      << " generation=" << route.generation_id << ": " << local_error << std::endl;
            return;
        }
        estimated_total_bytes += estimated_bytes;
    }

    SystemMemorySnapshot snapshot;
    if (!ReadSystemMemorySnapshot(&snapshot)) {
        std::cerr << "[masstree-preload] failed to read system memory, skip preload" << std::endl;
        return;
    }

    const uint64_t utilization_cap_bytes = static_cast<uint64_t>(
        static_cast<long double>(snapshot.available_bytes) *
        static_cast<long double>(masstree_preload_memory_utilization_limit_));
    const bool within_utilization_limit = estimated_total_bytes <= utilization_cap_bytes;
    const bool keeps_reserve = snapshot.available_bytes >= estimated_total_bytes &&
                               snapshot.available_bytes - estimated_total_bytes >=
                                   masstree_preload_memory_reserve_bytes_;

    std::cerr << "[masstree-preload] routes=" << routes.size()
              << " estimated_bytes=" << estimated_total_bytes
              << " mem_available_bytes=" << snapshot.available_bytes
              << " mem_total_bytes=" << snapshot.total_bytes
              << " reserve_bytes=" << masstree_preload_memory_reserve_bytes_
              << " utilization_limit=" << masstree_preload_memory_utilization_limit_ << std::endl;
    if (!within_utilization_limit || !keeps_reserve) {
        std::cerr << "[masstree-preload] skip preload due to memory budget" << std::endl;
        return;
    }

    MasstreeMetaStore::PreloadReport report;
    if (!masstree_meta_store_.PreloadAllGenerations(routes, &report, &error) && !error.empty()) {
        std::cerr << "[masstree-preload] preload completed with errors: " << error << std::endl;
    }
    report.estimated_total_bytes = estimated_total_bytes;
    std::cerr << "[masstree-preload] finished loaded=" << report.loaded_count
              << " failed=" << report.failed_count
              << " elapsed_ms=" << report.elapsed_ms << std::endl;
}

bool MdsServiceImpl::RecoverArchiveCatalog(std::string* error) {
    if (!store_ || archive_meta_root_.empty()) {
        if (error) {
            error->clear();
        }
        return true;
    }

    ArchiveGenerationPublisher publisher(&archive_namespace_catalog_);
    std::vector<ArchiveNamespaceRoute> routes;
    if (!archive_namespace_catalog_.ListRoutes(&routes, error)) {
        return false;
    }

    for (const auto& route : routes) {
        if (route.namespace_id.empty()) {
            continue;
        }
        std::vector<std::string> removed_paths;
        std::string cleanup_error;
        if (!publisher.CleanupNamespaceStaging(archive_meta_root_, route.namespace_id, &removed_paths, &cleanup_error)) {
            if (error) {
                *error = cleanup_error;
            }
            return false;
        }

        ArchiveNamespaceRoute current;
        std::string current_error;
        if (archive_namespace_catalog_.LookupCurrentRoute(route.path_prefix, &current, &current_error)) {
            continue;
        }
        if (!current_error.empty()) {
            if (error) {
                *error = current_error;
            }
            return false;
        }
        ArchiveNamespaceRoute recovered;
        std::string recover_error;
        if (!publisher.RecoverCurrentRouteFromLatest(archive_meta_root_, route.namespace_id, &recovered, &recover_error)) {
            if (error) {
                *error = recover_error;
            }
            return false;
        }
    }

    if (error) {
        error->clear();
    }
    return true;
}

void MdsServiceImpl::Lookup(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::LookupRequest* request,
                            zb::rpc::LookupReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!meta_router_.ResolvePath(request->path(), &inode_id, &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Getattr(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::GetattrRequest* request,
                             zb::rpc::GetattrReply* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!meta_router_.GetInode(request->inode_id(), &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "inode not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Open(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::OpenRequest* request,
                          zb::rpc::OpenReply* response,
                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    uint64_t inode_id = 0;
    if (!ResolvePath(request->path(), &inode_id, &attr, &error)) {
        if (error.empty()) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        } else {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        }
        return;
    }

    if (attr.type() == zb::rpc::INODE_FILE) {
        std::string location_error;
        if (!BuildFileLocationView(inode_id, attr, response->mutable_location(), &location_error)) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       location_error.empty() ? "failed to load file location" : location_error);
            return;
        }
        if (IsWriteOpenFlags(request->flags()) &&
            attr.file_archive_state() == zb::rpc::INODE_ARCHIVE_ARCHIVED) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, kReadOnlyOpticalMessage);
            return;
        }
    }

    uint64_t handle_id = AllocateHandleId(&error);
    if (handle_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (!store_->Put(HandleKey(handle_id), MetaCodec::EncodeUInt64(inode_id), &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    response->set_handle_id(handle_id);
    if (attr.type() != zb::rpc::INODE_FILE) {
        *response->mutable_location()->mutable_attr() = attr;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
    (void)request->flags();
}

void MdsServiceImpl::Close(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::CloseRequest* request,
                           zb::rpc::CloseReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->handle_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "handle_id is empty");
        return;
    }

    std::string error;
    rocksdb::WriteBatch batch;
    batch.Delete(HandleKey(request->handle_id()));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Create(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::CreateRequest* request,
                            zb::rpc::CreateReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !allocator_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        LogRequestFailure("Create.ResolveParent", request->path(), error.empty() ? "parent not found" : error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, error.empty() ? "parent not found" : error);
        return;
    }

    if (DentryExists(parent_inode, name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "path already exists");
        return;
    }
    if (!error.empty()) {
        LogRequestFailure("Create.DentryExists", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::InodeAttr parent_attr;
    if (!GetInode(parent_inode, &parent_attr, &error)) {
        LogRequestFailure("Create.GetParentInode", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (parent_attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "parent is not a directory");
        return;
    }

    uint64_t inode_id = AllocateInodeId(&error);
    if (inode_id == 0) {
        LogRequestFailure("Create.AllocateInodeId", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t now = NowSeconds();
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_FILE);
    attr.set_mode(request->mode());
    attr.set_uid(request->uid());
    attr.set_gid(request->gid());
    attr.set_size(0);
    attr.set_atime(now);
    attr.set_mtime(now);
    attr.set_ctime(now);
    attr.set_nlink(1);
    attr.set_object_unit_size(request->object_unit_size() ? request->object_unit_size() : default_object_unit_size_);
    attr.set_replica(request->replica() ? request->replica() : 1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    attr.set_file_name(name);
    FillPosixInodeDefaults(&attr);

    zb::rpc::PathPlacementPolicyRecord placement_policy;
    std::string policy_error;
    const bool has_path_policy =
        MatchPathPlacementPolicy(request->path(), &placement_policy, nullptr, &policy_error);
    if (!policy_error.empty()) {
        LogRequestFailure("Create.MatchPathPlacementPolicy", request->path(), policy_error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, policy_error);
        return;
    }

    zb::rpc::ReplicaLocation primary_location;
    bool location_ok = false;
    NodeType preferred_type = NodeType::kReal;
    const bool has_strict_tier_policy =
        has_path_policy &&
        placement_policy.strict() &&
        PlacementTargetToNodeType(placement_policy.target(), &preferred_type);
    if (has_strict_tier_policy) {
        location_ok = SelectFilePrimaryLocationWithPreference(
            inode_id, attr, preferred_type, true, &primary_location, &error);
    } else {
        location_ok = SelectFilePrimaryLocation(inode_id, attr, &primary_location, &error);
    }
    if (!location_ok) {
        if (has_strict_tier_policy) {
            error = NoSpaceMessageForPlacementTarget(placement_policy.target());
        }
        LogRequestFailure("Create.SelectFilePrimaryLocation",
                          request->path(),
                          error,
                          has_strict_tier_policy,
                          placement_policy.target());
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    zb::rpc::DiskFileLocation disk_location;
    disk_location.set_node_id(primary_location.node_id());
    std::string primary_location_address;
    if (ResolveNodeAddress(primary_location.node_id(), &primary_location_address, &error)) {
        disk_location.set_node_address(primary_location_address);
    } else {
        error.clear();
    }
    disk_location.set_disk_id(primary_location.disk_id());
    ApplyDiskLocationToAttr(disk_location, &attr);
    UnifiedInodeRecord inode_record;
    if (!AttrToUnifiedInodeRecord(attr, parent_inode, UnifiedStorageTier::kDisk, &inode_record, &error)) {
        LogRequestFailure("Create.BuildUnifiedInode", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    ApplyDiskLocationToUnifiedRecord(disk_location, &inode_record);
    std::string inode_payload;
    if (!EncodeUnifiedInodePayload(inode_record, &inode_payload, &error)) {
        LogRequestFailure("Create.EncodeUnifiedInode", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), inode_payload);
    if (!store_->WriteBatch(&batch, &error)) {
        LogRequestFailure("Create.WriteBatch", request->path(), error);
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::FileLocationView* view = response->mutable_location();
    *view->mutable_attr() = attr;
    *view->mutable_disk_location() = disk_location;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Mkdir(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::MkdirRequest* request,
                           zb::rpc::MkdirReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, error.empty() ? "parent not found" : error);
        return;
    }

    if (DentryExists(parent_inode, name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "path already exists");
        return;
    }
    if (!error.empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    zb::rpc::InodeAttr parent_attr;
    if (!GetInode(parent_inode, &parent_attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    if (parent_attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "parent is not a directory");
        return;
    }

    uint64_t inode_id = AllocateInodeId(&error);
    if (inode_id == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    uint64_t now = NowSeconds();
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_DIR);
    attr.set_mode(request->mode());
    attr.set_uid(request->uid());
    attr.set_gid(request->gid());
    attr.set_size(0);
    attr.set_atime(now);
    attr.set_mtime(now);
    attr.set_ctime(now);
    attr.set_nlink(2);
    attr.set_object_unit_size(default_object_unit_size_);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    attr.set_file_name(name);
    FillPosixInodeDefaults(&attr);

    UnifiedInodeRecord inode_record;
    if (!AttrToUnifiedInodeRecord(attr, parent_inode, UnifiedStorageTier::kNone, &inode_record, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    std::string inode_payload;
    if (!EncodeUnifiedInodePayload(inode_record, &inode_payload, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id));
    batch.Put(InodeKey(inode_id), inode_payload);
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Readdir(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::ReaddirRequest* request,
                             zb::rpc::ReaddirReply* response,
                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    std::vector<zb::rpc::Dentry> entries;
    bool has_more = false;
    std::string next_token;
    if (!meta_router_.Readdir(request->path(),
                              request->start_after(),
                              request->limit(),
                              &entries,
                              &has_more,
                              &next_token,
                              &error)) {
        FillStatus(response->mutable_status(),
                   IsInvalidReaddirRequest(error) ? zb::rpc::MDS_INVALID_ARGUMENT : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "readdir failed" : error);
        return;
    }
    for (const auto& entry : entries) {
        *response->add_entries() = entry;
    }
    response->set_has_more(has_more);
    if (!next_token.empty()) {
        response->set_next_token(next_token);
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Rename(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::RenameRequest* request,
                            zb::rpc::RenameReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->old_path().empty() || request->new_path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    zb::rpc::PathPlacementPolicyRecord old_policy;
    zb::rpc::PathPlacementPolicyRecord new_policy;
    std::string old_policy_error;
    std::string new_policy_error;
    const bool has_old_policy =
        MatchPathPlacementPolicy(request->old_path(), &old_policy, nullptr, &old_policy_error);
    const bool has_new_policy =
        MatchPathPlacementPolicy(request->new_path(), &new_policy, nullptr, &new_policy_error);
    if (!old_policy_error.empty() || !new_policy_error.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   !old_policy_error.empty() ? old_policy_error : new_policy_error);
        return;
    }
    if (has_old_policy &&
        has_new_policy &&
        IsTierPlacementTarget(old_policy.target()) &&
        IsTierPlacementTarget(new_policy.target()) &&
        old_policy.target() != new_policy.target()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, kCrossTierRenameMessage);
        return;
    }

    std::string error;
    uint64_t old_parent = 0;
    std::string old_name;
    if (!ResolveParent(request->old_path(), &old_parent, &old_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "old parent not found");
        return;
    }

    uint64_t new_parent = 0;
    std::string new_name;
    if (!ResolveParent(request->new_path(), &new_parent, &new_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "new parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(old_parent, old_name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "source not found");
        return;
    }

    if (DentryExists(new_parent, new_name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_ALREADY_EXISTS, "target exists");
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }
    zb::rpc::InodeAttr attr;
    if (!GetInode(inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "failed to load inode" : error);
        return;
    }
    attr.set_file_name(new_name);

    UnifiedInodeRecord inode_record;
    std::string existing_inode_payload;
    std::string load_error;
    if (store_->Get(InodeKey(inode_id), &existing_inode_payload, &load_error) &&
        MetaCodec::DecodeUnifiedInodeRecord(existing_inode_payload, &inode_record, nullptr)) {
        ApplyAttrToUnifiedRecord(attr, &inode_record);
        inode_record.parent_inode_id = new_parent;
        inode_record.file_name = new_name;
        inode_record.file_name_len = static_cast<uint16_t>(new_name.size());
    } else if (!AttrToUnifiedInodeRecord(attr, new_parent, UnifiedStorageTier::kNone, &inode_record, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }
    std::string encoded_inode_payload;
    if (!EncodeUnifiedInodePayload(inode_record, &encoded_inode_payload, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(old_parent, old_name));
    batch.Put(DentryKey(new_parent, new_name), inode_data);
    batch.Put(InodeKey(inode_id), encoded_inode_payload);
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Unlink(google::protobuf::RpcController* cntl_base,
                            const zb::rpc::UnlinkRequest* request,
                            zb::rpc::UnlinkReply* response,
                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(parent_inode, name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }

    zb::rpc::InodeAttr attr;
    if (!GetInode(inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "not a file");
        return;
    }

    zb::rpc::ReplicaLocation cleanup_anchor;
    std::string anchor_error;
    if (LoadFilePrimaryLocation(inode_id, attr, &cleanup_anchor, &anchor_error) &&
        !cleanup_anchor.node_id().empty() &&
        cleanup_anchor.storage_tier() == zb::rpc::STORAGE_TIER_DISK) {
        std::string cleanup_error;
        if (!DeleteFileMetaOnAnchor(cleanup_anchor, inode_id, true, &cleanup_error)) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       cleanup_error.empty() ? "failed to delete file meta on anchor"
                                             : cleanup_error);
            return;
        }
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    batch.Delete(InodeKey(inode_id));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (!DeleteInodeData(inode_id, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::Rmdir(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::RmdirRequest* request,
                           zb::rpc::RmdirReply* response,
                           google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    if (request->path().empty()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "path is empty");
        return;
    }

    if (request->path() == "/") {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "cannot remove root");
        return;
    }

    std::string error;
    uint64_t parent_inode = 0;
    std::string name;
    if (!ResolveParent(request->path(), &parent_inode, &name, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "parent not found");
        return;
    }

    std::string inode_data;
    if (!store_->Get(DentryKey(parent_inode, name), &inode_data, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "path not found");
        return;
    }

    uint64_t inode_id = 0;
    if (!MetaCodec::DecodeUInt64(inode_data, &inode_id)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "invalid inode data");
        return;
    }

    zb::rpc::InodeAttr attr;
    if (!GetInode(inode_id, &attr, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    if (attr.type() != zb::rpc::INODE_DIR) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "not a directory");
        return;
    }

    std::string prefix = DentryPrefix(inode_id);
    std::unique_ptr<rocksdb::Iterator> it(store_->db()->NewIterator(rocksdb::ReadOptions()));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        if (!it->key().starts_with(prefix)) {
            break;
        }
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_EMPTY, "directory not empty");
        return;
    }

    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    batch.Delete(InodeKey(inode_id));
    if (!store_->WriteBatch(&batch, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error);
        return;
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetFileLocation(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::GetFileLocationRequest* request,
                                   zb::rpc::GetFileLocationReply* response,
                                   google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "inode not found" : error);
        return;
    }
    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode is not file");
        return;
    }

    zb::rpc::FileLocationView location;
    if (!BuildFileLocationView(request->inode_id(), attr, &location, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to load file location" : error);
        return;
    }

    *response->mutable_location() = location;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::UpdateInodeStat(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::UpdateInodeStatRequest* request,
                                     zb::rpc::UpdateInodeStatReply* response,
                                     google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->inode_id() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode_id is empty");
        return;
    }
    if (request->version() == 0) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "version is empty");
        return;
    }

    std::string error;
    zb::rpc::InodeAttr attr;
    if (!GetInode(request->inode_id(), &attr, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "inode not found" : error);
        return;
    }
    if (attr.type() != zb::rpc::INODE_FILE) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "inode is not file");
        return;
    }
    if (request->version() < attr.version()) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_STALE_EPOCH, "stale inode stat version");
        return;
    }

    if (attr.file_archive_state() == zb::rpc::INODE_ARCHIVE_ARCHIVED) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, kReadOnlyOpticalMessage);
        return;
    }

    attr.set_size(request->file_size());
    if (request->object_unit_size() > 0) {
        attr.set_object_unit_size(request->object_unit_size());
    }
    attr.set_version(request->version());
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    const uint64_t now_sec = NowSeconds();
    const uint64_t mtime = request->mtime() > 0 ? request->mtime() : now_sec;
    attr.set_mtime(mtime);
    attr.set_ctime(now_sec);
    FillPosixInodeDefaults(&attr);

    if (!PutInode(request->inode_id(), attr, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to persist inode stat" : error);
        return;
    }

    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::SetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::SetPathPlacementPolicyRequest* request,
                                            zb::rpc::SetPathPlacementPolicyReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    std::string error;
    if (!SavePathPlacementPolicy(request->policy(), &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error.empty() ? "failed to set path placement policy" : error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::DeletePathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                               const zb::rpc::DeletePathPlacementPolicyRequest* request,
                                               zb::rpc::DeletePathPlacementPolicyReply* response,
                                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    std::string error;
    if (!DeletePathPlacementPolicyByPrefix(request->path_prefix(), &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error.empty() ? "failed to delete path placement policy" : error);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::GetPathPlacementPolicyRequest* request,
                                            zb::rpc::GetPathPlacementPolicyReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    zb::rpc::PathPlacementPolicyRecord policy;
    std::string matched_prefix;
    std::string error;
    const bool found = MatchPathPlacementPolicy(request->path(), &policy, &matched_prefix, &error);
    if (!error.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   error);
        return;
    }
    response->set_found(found);
    if (found) {
        *response->mutable_policy() = policy;
        response->set_matched_prefix(matched_prefix);
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ImportArchiveNamespace(google::protobuf::RpcController* cntl_base,
                                            const zb::rpc::ImportArchiveNamespaceRequest* request,
                                            zb::rpc::ImportArchiveNamespaceReply* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->path_prefix().empty() || request->namespace_id().empty() || request->generation_id().empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "path_prefix/namespace_id/generation_id is required");
        return;
    }

    ArchiveImportService::Request import_request;
    import_request.archive_root = archive_meta_root_;
    import_request.namespace_id = request->namespace_id();
    import_request.generation_id = request->generation_id();
    import_request.path_prefix = request->path_prefix();
    import_request.page_size_bytes =
        request->page_size_bytes() > 0 ? request->page_size_bytes() : archive_import_page_size_bytes_;
    import_request.publish_route = request->publish_route();

    ArchiveImportService::Result result;
    std::string error;
    if (!archive_import_service_.ImportPathPrefix(import_request, &result, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "archive import failed" : error);
        return;
    }
    response->set_manifest_path(result.manifest_path);
    response->set_inode_count(result.inode_count);
    response->set_dentry_count(result.dentry_count);
    response->set_inode_min(result.inode_min);
    response->set_inode_max(result.inode_max);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ImportMasstreeNamespace(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::ImportMasstreeNamespaceRequest* request,
                                             zb::rpc::ImportMasstreeNamespaceReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (masstree_root_.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "masstree_root is not configured");
        return;
    }
    if (request->path_prefix().empty() || request->namespace_id().empty() || request->generation_id().empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "path_prefix/namespace_id/generation_id is required");
        return;
    }
    std::string selected_template_id = request->template_id();
    if (selected_template_id.empty()) {
        std::string error;
        if (!PickRandomExistingMasstreeTemplateId(masstree_root_, &selected_template_id, &error)) {
            FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, error);
            return;
        }
    }
    MasstreeImportService::TemplateImportRequest import_request;
    import_request.masstree_root = masstree_root_;
    import_request.namespace_id = request->namespace_id();
    import_request.generation_id = request->generation_id();
    import_request.path_prefix = request->path_prefix();
    import_request.template_id = selected_template_id;
    import_request.template_mode = request->template_mode();
    import_request.inode_start = request->inode_start();
    import_request.verify_inode_samples = request->verify_inode_samples();
    import_request.verify_dentry_samples = request->verify_dentry_samples();
    import_request.publish_route = request->publish_route();

    std::shared_ptr<MasstreeImportJob> job = EnqueueMasstreeNamespaceImportJob(import_request);
    response->set_job_id(job->job_id);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "QUEUED");
}

void MdsServiceImpl::GenerateMasstreeTemplate(google::protobuf::RpcController* cntl_base,
                                              const zb::rpc::GenerateMasstreeTemplateRequest* request,
                                              zb::rpc::GenerateMasstreeTemplateReply* response,
                                              google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (masstree_root_.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "masstree_root is not configured");
        return;
    }
    if (request->template_id().empty() || request->path_list_file().empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "template_id/path_list_file is required");
        return;
    }

    MasstreeImportService::TemplateGenerationRequest generate_request;
    generate_request.masstree_root = masstree_root_;
    generate_request.template_id = request->template_id();
    generate_request.path_list_file = request->path_list_file();
    generate_request.repeat_dir_prefix = request->repeat_dir_prefix();
    generate_request.path_list_leaf_nodes_are_files = request->path_list_leaf_nodes_are_files();
    generate_request.verify_inode_samples = request->verify_inode_samples();
    generate_request.verify_dentry_samples = request->verify_dentry_samples();

    std::shared_ptr<MasstreeImportJob> job = EnqueueMasstreeTemplateJob(generate_request);
    response->set_job_id(job->job_id);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "QUEUED");
}

void MdsServiceImpl::GetMasstreeImportJob(google::protobuf::RpcController* cntl_base,
                                          const zb::rpc::GetMasstreeImportJobRequest* request,
                                          zb::rpc::GetMasstreeImportJobReply* response,
                                          google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->job_id().empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "job_id is required");
        return;
    }
    std::shared_ptr<MasstreeImportJob> job = FindMasstreeImportJob(request->job_id());
    if (!job) {
        response->set_found(false);
        FillStatus(response->mutable_status(), zb::rpc::MDS_NOT_FOUND, "job not found");
        return;
    }
    response->set_found(true);
    FillMasstreeImportJobInfo(*job, response->mutable_job());
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetRandomMasstreeFileAttr(google::protobuf::RpcController* cntl_base,
                                               const zb::rpc::GetRandomMasstreeFileAttrRequest* request,
                                               zb::rpc::GetRandomMasstreeFileAttrReply* response,
                                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    MasstreeNamespaceRoute route;
    std::string error;
    if (!ResolveMasstreeRoute(request->namespace_id(), request->path_prefix(), &route, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "masstree namespace not found" : error);
        return;
    }
    const std::string query_mode = NormalizeMasstreeQueryMode(request->query_mode());
    if (!IsMasstreeQueryModeValid(query_mode)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "unsupported masstree query_mode: " + request->query_mode());
        return;
    }

    uint64_t inode_id = 0;
    zb::rpc::InodeAttr attr;
    std::string file_name;
    std::string full_path;
    if (!PickRandomMasstreeFile(route, query_mode, &inode_id, &attr, &file_name, &full_path, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "masstree file not found" : error);
        return;
    }

    response->set_namespace_id(route.namespace_id);
    response->set_path_prefix(route.path_prefix);
    response->set_generation_id(route.generation_id);
    response->set_inode_id(inode_id);
    response->set_file_name(file_name);
    response->set_full_path(full_path);
    *response->mutable_attr() = attr;
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetRandomMasstreeLookupPaths(google::protobuf::RpcController* cntl_base,
                                                  const zb::rpc::GetRandomMasstreeLookupPathsRequest* request,
                                                  zb::rpc::GetRandomMasstreeLookupPathsReply* response,
                                                  google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    struct LookupPathCandidate {
        MasstreeNamespaceRoute route;
        MasstreeNamespaceManifest route_manifest;
        MasstreeNamespaceManifest template_manifest;
        std::string template_id;
        uint64_t weight{0};
    };

    std::vector<MasstreeNamespaceRoute> routes;
    std::string error;
    if (!masstree_namespace_catalog_.ListRoutes(&routes, &error)) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "failed to list masstree routes" : error);
        return;
    }

    std::vector<LookupPathCandidate> candidates;
    candidates.reserve(routes.size());
    uint64_t total_weight = 0;
    for (const auto& route : routes) {
        MasstreeNamespaceManifest route_manifest;
        std::string local_error;
        if (route.manifest_path.empty() ||
            !MasstreeNamespaceManifest::LoadFromFile(route.manifest_path, &route_manifest, &local_error)) {
            continue;
        }
        if (!IsPathListSourceMode(route_manifest.source_mode) || route_manifest.file_count == 0 ||
            route_manifest.template_repeat_count == 0 || route_manifest.repeat_dir_prefix.empty()) {
            continue;
        }

        LookupPathCandidate candidate;
        candidate.route = route;
        candidate.route_manifest = route_manifest;
        if (!ResolveTemplateManifestForNamespace(masstree_root_,
                                                 route_manifest,
                                                 &candidate.template_id,
                                                 &candidate.template_manifest,
                                                 &local_error)) {
            continue;
        }
        candidate.weight = std::max<uint64_t>(1ULL, route_manifest.file_count);
        total_weight += candidate.weight;
        candidates.push_back(std::move(candidate));
    }

    if (candidates.empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_NOT_FOUND,
                   "no path_list masstree namespace/template is available for random path lookup");
        return;
    }

    static thread_local std::mt19937_64 rng{std::random_device{}()};
    const uint32_t sample_count = std::max<uint32_t>(1U, request->sample_count());
    for (uint32_t i = 0; i < sample_count; ++i) {
        bool produced = false;
        std::string last_error;
        for (uint32_t attempt = 0; attempt < 32U; ++attempt) {
            const LookupPathCandidate* selected = nullptr;
            if (total_weight > 0) {
                std::uniform_int_distribution<uint64_t> dist(1ULL, total_weight);
                uint64_t draw = dist(rng);
                for (const auto& candidate : candidates) {
                    if (draw <= candidate.weight) {
                        selected = &candidate;
                        break;
                    }
                    draw -= candidate.weight;
                }
            }
            if (!selected) {
                std::uniform_int_distribution<size_t> dist(0, candidates.size() - 1U);
                selected = &candidates[dist(rng)];
            }

            MasstreeNamespaceManifest template_manifest = selected->template_manifest;
            if (!EnsureTemplateQueryPathSource(&template_manifest, request->path_list_file(), &last_error)) {
                continue;
            }

            std::string relative_file_path;
            if (!PickRandomNonEmptyLineFromFile(template_manifest.query_path_source_path,
                                                &relative_file_path,
                                                &last_error)) {
                continue;
            }

            const uint64_t repeat_count =
                selected->route_manifest.template_repeat_count != 0
                    ? selected->route_manifest.template_repeat_count
                    : template_manifest.template_repeat_count;
            const std::string repeat_dir_prefix =
                !selected->route_manifest.repeat_dir_prefix.empty()
                    ? selected->route_manifest.repeat_dir_prefix
                    : template_manifest.repeat_dir_prefix;
            if (repeat_count == 0 || repeat_dir_prefix.empty()) {
                last_error = "invalid repeat_dir_prefix/template_repeat_count in namespace manifest";
                continue;
            }

            std::uniform_int_distribution<uint64_t> repeat_dist(0, repeat_count - 1U);
            const uint64_t repeat_index = repeat_dist(rng);
            zb::rpc::MasstreeLookupPathSample* sample = response->add_samples();
            sample->set_namespace_id(selected->route.namespace_id);
            sample->set_path_prefix(selected->route.path_prefix);
            sample->set_generation_id(selected->route.generation_id);
            sample->set_template_id(selected->template_id);
            sample->set_full_path(JoinLogicalMasstreePath(selected->route.path_prefix,
                                                          RepeatDirName(repeat_dir_prefix,
                                                                        repeat_index,
                                                                        std::max<size_t>(kMinRepeatWidth,
                                                                                         DecimalDigits(repeat_count - 1U))),
                                                          relative_file_path));
            produced = true;
            break;
        }
        if (!produced) {
            FillStatus(response->mutable_status(),
                       zb::rpc::MDS_INTERNAL_ERROR,
                       last_error.empty() ? "failed to build random masstree lookup path" : last_error);
            return;
        }
    }

    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetMasstreeClusterStats(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::GetMasstreeClusterStatsRequest* request,
                                             zb::rpc::GetMasstreeClusterStatsReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    (void)request;
    if (!store_ || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }

    MasstreeStatsStore stats_store(store_);
    MasstreeClusterStatsRecord stats;
    std::string error;
    if (!stats_store.LoadClusterStats(&stats, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "masstree cluster stats not found" : error);
        return;
    }

    response->set_disk_node_count(stats.disk_node_count);
    response->set_optical_node_count(stats.optical_node_count);
    response->set_disk_device_count(stats.disk_device_count);
    response->set_optical_device_count(stats.optical_device_count);
    response->set_total_capacity_bytes(stats.total_capacity_bytes);
    response->set_used_capacity_bytes(stats.used_capacity_bytes);
    response->set_free_capacity_bytes(stats.free_capacity_bytes);
    response->set_total_file_count(stats.total_file_count);
    response->set_total_file_bytes(stats.total_file_bytes);
    response->set_total_metadata_bytes(stats.total_metadata_bytes);
    response->set_avg_file_size_bytes(stats.avg_file_size_bytes);
    response->set_min_file_size_bytes(stats.min_file_size_bytes);
    response->set_max_file_size_bytes(stats.max_file_size_bytes);
    response->set_cursor_node_index(stats.cursor.node_index);
    response->set_cursor_disk_index(stats.cursor.disk_index);
    response->set_cursor_image_index(stats.cursor.image_index_in_disk);
    response->set_cursor_image_used_bytes(stats.cursor.image_used_bytes);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::GetMasstreeNamespaceStats(google::protobuf::RpcController* cntl_base,
                                               const zb::rpc::GetMasstreeNamespaceStatsRequest* request,
                                               zb::rpc::GetMasstreeNamespaceStatsReply* response,
                                               google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;
    if (!store_ || !request || !response) {
        FillStatus(response ? response->mutable_status() : nullptr,
                   zb::rpc::MDS_INTERNAL_ERROR,
                   "Service not initialized");
        return;
    }
    if (request->namespace_id().empty()) {
        FillStatus(response->mutable_status(),
                   zb::rpc::MDS_INVALID_ARGUMENT,
                   "namespace_id is required");
        return;
    }

    MasstreeStatsStore stats_store(store_);
    MasstreeNamespaceStatsRecord stats;
    std::string error;
    if (!stats_store.LoadNamespaceStats(request->namespace_id(), &stats, &error)) {
        FillStatus(response->mutable_status(),
                   error.empty() ? zb::rpc::MDS_NOT_FOUND : zb::rpc::MDS_INTERNAL_ERROR,
                   error.empty() ? "masstree namespace stats not found" : error);
        return;
    }

    response->set_namespace_id(stats.namespace_id);
    response->set_generation_id(stats.generation_id);
    response->set_file_count(stats.file_count);
    response->set_total_file_bytes(stats.total_file_bytes);
    response->set_avg_file_size_bytes(stats.avg_file_size_bytes);
    response->set_start_global_image_id(stats.start_global_image_id);
    response->set_end_global_image_id(stats.end_global_image_id);
    response->set_start_cursor_node_index(stats.start_cursor.node_index);
    response->set_start_cursor_disk_index(stats.start_cursor.disk_index);
    response->set_start_cursor_image_index(stats.start_cursor.image_index_in_disk);
    response->set_start_cursor_image_used_bytes(stats.start_cursor.image_used_bytes);
    response->set_end_cursor_node_index(stats.end_cursor.node_index);
    response->set_end_cursor_disk_index(stats.end_cursor.disk_index);
    response->set_end_cursor_image_index(stats.end_cursor.image_index_in_disk);
    response->set_end_cursor_image_used_bytes(stats.end_cursor.image_used_bytes);
    response->set_total_metadata_bytes(stats.total_metadata_bytes);
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ReportArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                             const zb::rpc::ReportArchiveCandidatesRequest* request,
                                             zb::rpc::ReportArchiveCandidatesReply* response,
                                             google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!candidate_queue_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "archive candidate queue disabled");
        response->set_accepted(0);
        response->set_dropped(static_cast<uint32_t>(request->candidates_size()));
        return;
    }

    std::vector<FileArchiveCandidateEntry> batch;
    batch.reserve(static_cast<size_t>(request->candidates_size()));
    uint32_t dropped = 0;
    const uint64_t fallback_report_ts = request->report_ts_ms() > 0 ? request->report_ts_ms() : NowMilliseconds();

    for (const auto& item : request->candidates()) {
        const std::string object_id = ArchiveObjectId(item);
        uint64_t inode_id = 0;
        if (item.disk_id().empty() || object_id.empty() || !ParseStableObjectId(object_id, &inode_id)) {
            ++dropped;
            continue;
        }
        FileArchiveCandidateEntry candidate;
        candidate.inode_id = inode_id;
        candidate.node_id = !item.node_id().empty() ? item.node_id() : request->node_id();
        candidate.node_address = !item.node_address().empty() ? item.node_address() : request->node_address();
        candidate.disk_id = item.disk_id();
        candidate.file_size = item.size_bytes();
        candidate.object_count = 0;
        candidate.last_access_ts_ms = item.last_access_ts_ms();
        candidate.archive_state = item.archive_state().empty() ? "pending" : item.archive_state();
        candidate.version = item.version();
        candidate.report_ts_ms = item.report_ts_ms() > 0 ? item.report_ts_ms() : fallback_report_ts;
        zb::rpc::InodeAttr attr;
        std::string inode_error;
        if (!GetInode(inode_id, &attr, &inode_error) ||
            attr.type() != zb::rpc::INODE_FILE ||
            attr.file_archive_state() != zb::rpc::INODE_ARCHIVE_PENDING) {
            ++dropped;
            continue;
        }
        batch.push_back(std::move(candidate));
    }

    const FileArchiveCandidateQueue::PushResult push = candidate_queue_->PushBatch(batch);
    response->set_accepted(static_cast<uint32_t>(push.accepted));
    response->set_dropped(static_cast<uint32_t>(dropped + push.dropped));
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ReportFileArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                                 const zb::rpc::ReportFileArchiveCandidatesRequest* request,
                                                 zb::rpc::ReportFileArchiveCandidatesReply* response,
                                                 google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!candidate_queue_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "archive candidate queue disabled");
        response->set_accepted(0);
        response->set_dropped(static_cast<uint32_t>(request->candidates_size()));
        return;
    }

    std::vector<FileArchiveCandidateEntry> batch;
    batch.reserve(static_cast<size_t>(request->candidates_size()));
    uint32_t dropped = 0;
    const uint64_t fallback_report_ts = request->report_ts_ms() > 0 ? request->report_ts_ms() : NowMilliseconds();
    for (const auto& item : request->candidates()) {
        FileArchiveCandidateEntry candidate;
        candidate.inode_id = item.inode_id();
        candidate.node_id = !item.node_id().empty() ? item.node_id() : request->node_id();
        candidate.node_address = !item.node_address().empty() ? item.node_address() : request->node_address();
        candidate.disk_id = item.disk_id();
        candidate.file_size = item.file_size();
        candidate.object_count = item.object_count();
        candidate.last_access_ts_ms = item.last_access_ts_ms();
        candidate.archive_state = item.archive_state().empty() ? "pending" : item.archive_state();
        candidate.version = item.version();
        candidate.report_ts_ms = item.report_ts_ms() > 0 ? item.report_ts_ms() : fallback_report_ts;
        if (candidate.inode_id == 0 || candidate.disk_id.empty()) {
            ++dropped;
            continue;
        }
        zb::rpc::InodeAttr attr;
        std::string inode_error;
        if (!GetInode(candidate.inode_id, &attr, &inode_error) ||
            attr.type() != zb::rpc::INODE_FILE ||
            attr.file_archive_state() != zb::rpc::INODE_ARCHIVE_PENDING) {
            ++dropped;
            continue;
        }
        batch.push_back(std::move(candidate));
    }

    const FileArchiveCandidateQueue::PushResult push = candidate_queue_->PushBatch(batch);
    response->set_accepted(static_cast<uint32_t>(push.accepted));
    response->set_dropped(static_cast<uint32_t>(dropped + push.dropped));
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::ClaimArchiveTask(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::ClaimArchiveTaskRequest* request,
                                      zb::rpc::ClaimArchiveTaskReply* response,
                                      google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_granted(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Claim(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "claim failed" : error);
        response->set_granted(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::RenewArchiveLease(google::protobuf::RpcController* cntl_base,
                                       const zb::rpc::RenewArchiveLeaseRequest* request,
                                       zb::rpc::RenewArchiveLeaseReply* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_renewed(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Renew(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "renew failed" : error);
        response->set_renewed(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

void MdsServiceImpl::CommitArchiveTask(google::protobuf::RpcController* cntl_base,
                                       const zb::rpc::CommitArchiveTaskRequest* request,
                                       zb::rpc::CommitArchiveTaskReply* response,
                                       google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    (void)cntl_base;

    if (!response) {
        return;
    }
    if (!request) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INVALID_ARGUMENT, "request is null");
        return;
    }
    if (!lease_manager_) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, "archive lease manager disabled");
        response->set_committed(false);
        return;
    }

    std::string error;
    if (!lease_manager_->Commit(*request, response, &error)) {
        FillStatus(response->mutable_status(), zb::rpc::MDS_INTERNAL_ERROR, error.empty() ? "commit failed" : error);
        response->set_committed(false);
        return;
    }
    FillStatus(response->mutable_status(), zb::rpc::MDS_OK, "OK");
}

bool MdsServiceImpl::EnsureRoot(std::string* error) {
    zb::rpc::InodeAttr root;
    if (GetInode(kRootInodeId, &root, error)) {
        return true;
    }
    if (error && !error->empty()) {
        return false;
    }

    uint64_t now = NowSeconds();
    root.set_inode_id(kRootInodeId);
    root.set_type(zb::rpc::INODE_DIR);
    root.set_mode(0755);
    root.set_uid(0);
    root.set_gid(0);
    root.set_size(kDefaultDirectorySizeBytes);
    root.set_atime(now);
    root.set_mtime(now);
    root.set_ctime(now);
    root.set_nlink(2);
    root.set_object_unit_size(default_object_unit_size_);
    root.set_replica(1);
    root.set_version(1);
    root.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    root.set_file_name("/");
    FillPosixInodeDefaults(&root);

    return PutInode(kRootInodeId, root, error);
}

bool MdsServiceImpl::ResolvePath(const std::string& path,
                                 uint64_t* inode_id,
                                 zb::rpc::InodeAttr* attr,
                                 std::string* error) {
    return meta_router_.ResolvePath(path, inode_id, attr, error);
}

bool MdsServiceImpl::ResolveParent(const std::string& path,
                                   uint64_t* parent_inode,
                                   std::string* name,
                                   std::string* error) {
    if (!parent_inode || !name) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(path);
    if (parts.empty()) {
        if (error) {
            *error = "invalid path";
        }
        return false;
    }
    *name = parts.back();
    parts.pop_back();
    uint64_t current = kRootInodeId;
    for (const auto& part : parts) {
        std::string data;
        if (!store_->Get(DentryKey(current, part), &data, error)) {
            return false;
        }
        if (!MetaCodec::DecodeUInt64(data, &current)) {
            if (error) {
                *error = "invalid dentry";
            }
            return false;
        }
    }
    *parent_inode = current;
    return true;
}

bool MdsServiceImpl::GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error) {
    return meta_router_.GetInode(inode_id, attr, error);
}

bool MdsServiceImpl::PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error) {
    UnifiedInodeRecord record;
    std::string existing_payload;
    std::string get_error;
    if (store_->Get(InodeKey(inode_id), &existing_payload, &get_error) &&
        MetaCodec::DecodeUnifiedInodeRecord(existing_payload, &record, nullptr)) {
        ApplyAttrToUnifiedRecord(attr, &record);
    } else if (!AttrToUnifiedInodeRecord(attr, 0, UnifiedStorageTier::kNone, &record, error)) {
        return false;
    }

    std::string payload;
    if (!EncodeUnifiedInodePayload(record, &payload, error)) {
        return false;
    }
    return store_->Put(InodeKey(inode_id), payload, error);
}

bool MdsServiceImpl::PutDentry(uint64_t parent_inode, const std::string& name, uint64_t inode_id, std::string* error) {
    return store_->Put(DentryKey(parent_inode, name), MetaCodec::EncodeUInt64(inode_id), error);
}

bool MdsServiceImpl::DeleteDentry(uint64_t parent_inode, const std::string& name, std::string* error) {
    rocksdb::WriteBatch batch;
    batch.Delete(DentryKey(parent_inode, name));
    return store_->WriteBatch(&batch, error);
}

bool MdsServiceImpl::DentryExists(uint64_t parent_inode, const std::string& name, std::string* error) {
    return store_->Exists(DentryKey(parent_inode, name), error);
}

bool MdsServiceImpl::DeleteInodeData(uint64_t inode_id, std::string* error) {
    rocksdb::WriteBatch batch;
    batch.Delete(FileArchiveStateKey(inode_id));
    if (!DeleteDiskFileLocation(inode_id, &batch, error)) {
        return false;
    }
    if (!DeleteOpticalFileLocation(inode_id, &batch, error)) {
        return false;
    }
    return store_->WriteBatch(&batch, error);
}

bool MdsServiceImpl::ResolveObjectReplicas(uint32_t replica_count,
                                           const std::string& object_id,
                                           uint64_t placement_epoch,
                                           std::vector<zb::rpc::ReplicaLocation>* replicas,
                                           std::string* error) const {
    if (!replicas || replica_count == 0 || object_id.empty()) {
        if (error) {
            *error = "invalid object replica resolve args";
        }
        return false;
    }
    if (!allocator_) {
        if (error) {
            *error = "allocator is unavailable";
        }
        return false;
    }
    std::string local_error;
    if (!allocator_->AllocateObjectByPg(replica_count, object_id, placement_epoch, replicas, &local_error)) {
        if (error) {
            *error = local_error.empty() ? "AllocateObjectByPg failed" : local_error;
        }
        return false;
    }
    if (replicas->size() < replica_count) {
        if (error) {
            *error = "insufficient replicas from PG resolve";
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::SelectFilePrimaryLocation(uint64_t inode_id,
                                               const zb::rpc::InodeAttr& attr,
                                               zb::rpc::ReplicaLocation* location,
                                               std::string* error) const {
    return SelectFilePrimaryLocationWithPreference(inode_id, attr, NodeType::kReal, false, location, error);
}

bool MdsServiceImpl::SelectFilePrimaryLocationWithPreference(uint64_t inode_id,
                                                             const zb::rpc::InodeAttr& attr,
                                                             NodeType preferred_type,
                                                             bool strict_type,
                                                             zb::rpc::ReplicaLocation* location,
                                                             std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid file primary location args";
        }
        return false;
    }
    if (!allocator_) {
        if (error) {
            *error = "allocator is unavailable";
        }
        return false;
    }

    const std::string object_id = BuildStableObjectId(inode_id, 0);
    std::vector<zb::rpc::ReplicaLocation> replicas;
    std::string local_error;
    if (strict_type) {
        const bool ok = strict_tier_bypass_pg_
                            ? allocator_->AllocateObjectDirectByType(1,
                                                                     object_id,
                                                                     preferred_type,
                                                                     &replicas,
                                                                     &local_error)
                            : allocator_->AllocateObjectByPgWithType(1,
                                                                     object_id,
                                                                     0,
                                                                     preferred_type,
                                                                     true,
                                                                     &replicas,
                                                                     &local_error);
        if (!ok || replicas.empty()) {
            if (error) {
                *error = local_error.empty() ? "no replica matches strict placement policy" : local_error;
            }
            return false;
        }
    } else if (!ResolveObjectReplicas(1, object_id, 0, &replicas, &local_error) || replicas.empty()) {
        replicas.clear();
        if (!allocator_->AllocateObject(1, object_id, &replicas) || replicas.empty()) {
            if (error) {
                *error = local_error.empty() ? "failed to allocate file primary location" : local_error;
            }
            return false;
        }
    }

    for (const auto& replica : replicas) {
        if (replica.node_id().empty() || replica.disk_id().empty()) {
            continue;
        }
        *location = replica;
        if (location->storage_tier() != zb::rpc::STORAGE_TIER_DISK) {
            location->set_storage_tier(zb::rpc::STORAGE_TIER_DISK);
        }
        location->set_replica_state(zb::rpc::REPLICA_READY);
        EnsureReplicaObjectId(location, object_id);
        StripReplicaAddresses(location);
        return true;
    }

    (void)attr;
    if (error) {
        *error = strict_type ? "no usable replica for strict placement policy"
                             : "no usable disk replica for file primary location";
    }
    return false;
}

bool MdsServiceImpl::MatchPathPlacementPolicy(const std::string& path,
                                              zb::rpc::PathPlacementPolicyRecord* policy,
                                              std::string* matched_prefix,
                                              std::string* error) const {
    if (error) {
        error->clear();
    }
    if (policy) {
        policy->Clear();
    }
    if (matched_prefix) {
        matched_prefix->clear();
    }
    std::string normalized_path;
    if (!NormalizePolicyPath(path, &normalized_path)) {
        if (error) {
            *error = "invalid policy lookup path";
        }
        return false;
    }
    const std::vector<std::string> candidates = BuildPathPrefixCandidates(normalized_path);
    for (const auto& prefix : candidates) {
        std::string payload;
        std::string local_error;
        if (!store_->Get(PathPlacementPolicyKey(prefix), &payload, &local_error)) {
            if (!local_error.empty()) {
                if (error) {
                    *error = local_error;
                }
                return false;
            }
            continue;
        }
        zb::rpc::PathPlacementPolicyRecord loaded;
        if (!loaded.ParseFromString(payload)) {
            if (error) {
                *error = "invalid path placement policy payload";
            }
            return false;
        }
        if (policy) {
            *policy = loaded;
        }
        if (matched_prefix) {
            *matched_prefix = prefix;
        }
        return true;
    }
    return false;
}

bool MdsServiceImpl::SavePathPlacementPolicy(const zb::rpc::PathPlacementPolicyRecord& policy, std::string* error) {
    std::string normalized_prefix;
    if (!NormalizePolicyPath(policy.path_prefix(), &normalized_prefix)) {
        if (error) {
            *error = "invalid path_prefix";
        }
        return false;
    }
    if (policy.target() == zb::rpc::PATH_PLACEMENT_UNSPECIFIED) {
        if (error) {
            *error = "policy target is unspecified";
        }
        return false;
    }
    zb::rpc::PathPlacementPolicyRecord normalized = policy;
    normalized.set_path_prefix(normalized_prefix);
    std::string payload;
    if (!normalized.SerializeToString(&payload)) {
        if (error) {
            *error = "failed to encode path placement policy";
        }
        return false;
    }
    return store_->Put(PathPlacementPolicyKey(normalized_prefix), payload, error);
}

bool MdsServiceImpl::DeletePathPlacementPolicyByPrefix(const std::string& path_prefix, std::string* error) {
    std::string normalized_prefix;
    if (!NormalizePolicyPath(path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid path_prefix";
        }
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Delete(PathPlacementPolicyKey(normalized_prefix));
    return store_->WriteBatch(&batch, error);
}

bool MdsServiceImpl::LoadDiskFileLocation(uint64_t inode_id,
                                          zb::rpc::DiskFileLocation* location,
                                          std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid disk file location output";
        }
        return false;
    }
    location->Clear();
    std::string inode_payload;
    std::string inode_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &inode_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            PopulateDiskFileLocationFromUnifiedRecord(record, location)) {
            if (allocator_) {
                std::string resolved_disk_id;
                if (allocator_->ResolveDiskId(location->node_id(), record.disk_id, &resolved_disk_id) &&
                    !resolved_disk_id.empty()) {
                    location->set_disk_id(resolved_disk_id);
                }
            }
            std::string node_address;
            std::string resolve_error;
            if (ResolveNodeAddress(location->node_id(), &node_address, &resolve_error) && !node_address.empty()) {
                location->set_node_address(node_address);
            }
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MdsServiceImpl::BuildDiskFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                                   zb::rpc::DiskFileLocation* location,
                                                   std::string* error) const {
    if (!location) {
        if (error) {
            *error = "disk location output is null";
        }
        return false;
    }
    if (!PopulateDiskFileLocationFromInodeAttr(attr, location)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    if (allocator_) {
        std::string resolved_disk_id;
        if (allocator_->ResolveDiskId(location->node_id(), attr.disk_id(), &resolved_disk_id) &&
            !resolved_disk_id.empty()) {
            location->set_disk_id(resolved_disk_id);
        }
    }
    std::string node_address;
    std::string resolve_error;
    if (ResolveNodeAddress(location->node_id(), &node_address, &resolve_error) && !node_address.empty()) {
        location->set_node_address(node_address);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MdsServiceImpl::SaveDiskFileLocation(uint64_t inode_id,
                                          const zb::rpc::DiskFileLocation& location,
                                          rocksdb::WriteBatch* batch) const {
    if (!batch || inode_id == 0) {
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr)) {
            ApplyDiskLocationToUnifiedRecord(location, &record);
            std::string encoded;
            if (!EncodeUnifiedInodePayload(record, &encoded, nullptr)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
            return true;
        }
        return false;
    } else if (!local_error.empty()) {
        return false;
    }
    return true;
}

bool MdsServiceImpl::DeleteDiskFileLocation(uint64_t inode_id,
                                            rocksdb::WriteBatch* batch,
                                            std::string* error) const {
    if (!batch || inode_id == 0) {
        if (error) {
            *error = "invalid disk file location delete args";
        }
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            record.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kDisk)) {
            record.storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kNone);
            ClearDiskLocationInUnifiedRecord(&record);
            std::string encoded;
            if (!EncodeUnifiedInodePayload(record, &encoded, error)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MdsServiceImpl::LoadOpticalFileLocation(uint64_t inode_id,
                                             zb::rpc::OpticalFileLocation* location,
                                             std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid optical file location output";
        }
        return false;
    }
    location->Clear();
    std::string inode_payload;
    std::string inode_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &inode_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            PopulateOpticalFileLocationFromUnifiedRecord(record, location)) {
            if (allocator_ && record.optical_disk_id <= static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
                std::string resolved_disk_id;
                if (allocator_->ResolveDiskId(location->node_id(),
                                              static_cast<uint32_t>(record.optical_disk_id),
                                              &resolved_disk_id) &&
                    !resolved_disk_id.empty()) {
                    location->set_disk_id(resolved_disk_id);
                }
            }
            std::string node_address;
            std::string resolve_error;
            if (ResolveNodeAddress(location->node_id(), &node_address, &resolve_error) && !node_address.empty()) {
                location->set_node_address(node_address);
            }
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MdsServiceImpl::BuildOpticalFileLocationFromAttr(const zb::rpc::InodeAttr& attr,
                                                      uint64_t inode_id,
                                                      zb::rpc::OpticalFileLocation* location,
                                                      std::string* error) const {
    if (!location) {
        if (error) {
            *error = "optical location output is null";
        }
        return false;
    }
    if (inode_id == 0) {
        if (error) {
            *error = "inode_id is zero";
        }
        return false;
    }
    if (!PopulateOpticalFileLocationFromInodeAttr(attr, inode_id, location)) {
        if (error) {
            error->clear();
        }
        return false;
    }
    std::string node_address;
    std::string resolve_error;
    if (ResolveNodeAddress(location->node_id(), &node_address, &resolve_error) && !node_address.empty()) {
        location->set_node_address(node_address);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MdsServiceImpl::LoadMasstreeOpticalFileLocation(uint64_t inode_id,
                                                     zb::rpc::OpticalFileLocation* location,
                                                     std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid masstree optical file location output";
        }
        return false;
    }

    std::vector<MasstreeNamespaceRoute> routes;
    if (!masstree_namespace_catalog_.LookupByInode(inode_id, &routes, error)) {
        return false;
    }
    if (routes.empty() && !masstree_namespace_catalog_.ListRoutes(&routes, error)) {
        return false;
    }
    for (const auto& route : routes) {
        if (route.inode_min != 0 && inode_id < route.inode_min) {
            continue;
        }
        if (route.inode_max != 0 && inode_id > route.inode_max) {
            continue;
        }
        zb::rpc::OpticalFileLocation candidate;
        std::string local_error;
        if (masstree_meta_store_.GetOpticalFileLocation(route, inode_id, &candidate, &local_error)) {
            if (!candidate.node_id().empty()) {
                std::string node_address;
                std::string resolve_error;
                if (ResolveNodeAddress(candidate.node_id(), &node_address, &resolve_error) && !node_address.empty()) {
                    candidate.set_node_address(node_address);
                }
            }
            *location = std::move(candidate);
            if (error) {
                error->clear();
            }
            return true;
        }
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MdsServiceImpl::SaveOpticalFileLocation(uint64_t inode_id,
                                             const zb::rpc::OpticalFileLocation& location,
                                             rocksdb::WriteBatch* batch) const {
    if (!batch || inode_id == 0) {
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr)) {
            ApplyOpticalLocationToUnifiedRecord(location, &record);
            std::string encoded;
            if (!EncodeUnifiedInodePayload(record, &encoded, nullptr)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
            return true;
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::DeleteOpticalFileLocation(uint64_t inode_id,
                                               rocksdb::WriteBatch* batch,
                                               std::string* error) const {
    if (!batch || inode_id == 0) {
        if (error) {
            *error = "invalid optical file location delete args";
        }
        return false;
    }
    std::string inode_payload;
    std::string local_error;
    if (store_->Get(InodeKey(inode_id), &inode_payload, &local_error)) {
        UnifiedInodeRecord record;
        if (MetaCodec::DecodeUnifiedInodeRecord(inode_payload, &record, nullptr) &&
            record.storage_tier == static_cast<uint8_t>(UnifiedStorageTier::kOptical)) {
            record.storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kNone);
            ClearOpticalLocationInUnifiedRecord(&record);
            std::string encoded;
            if (!EncodeUnifiedInodePayload(record, &encoded, error)) {
                return false;
            }
            batch->Put(InodeKey(inode_id), encoded);
        }
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MdsServiceImpl::BuildFileLocationView(uint64_t inode_id,
                                           const zb::rpc::InodeAttr& attr,
                                           zb::rpc::FileLocationView* view,
                                           std::string* error) const {
    if (!view || inode_id == 0) {
        if (error) {
            *error = "invalid file location view args";
        }
        return false;
    }
    view->Clear();
    *view->mutable_attr() = attr;
    zb::rpc::DiskFileLocation disk;
    zb::rpc::OpticalFileLocation optical;
    std::string attr_disk_error;
    if (BuildDiskFileLocationFromAttr(attr, &disk, &attr_disk_error)) {
        *view->mutable_disk_location() = disk;
        ApplyDiskLocationToAttr(disk, view->mutable_attr());
        if (error) {
            error->clear();
        }
        return true;
    } else if (!attr_disk_error.empty()) {
        if (error) {
            *error = attr_disk_error;
        }
        return false;
    }
    std::string attr_optical_error;
    if (BuildOpticalFileLocationFromAttr(attr, inode_id, &optical, &attr_optical_error)) {
        *view->mutable_optical_location() = optical;
        ApplyOpticalLocationToAttr(optical, view->mutable_attr());
        if (error) {
            error->clear();
        }
        return true;
    } else if (!attr_optical_error.empty()) {
        if (error) {
            *error = attr_optical_error;
        }
        return false;
    }
    std::string disk_error;
    const bool has_disk = LoadDiskFileLocation(inode_id, &disk, &disk_error);
    if (has_disk) {
        *view->mutable_disk_location() = disk;
        ApplyDiskLocationToAttr(disk, view->mutable_attr());
    } else if (!IsLocationMetadataMissing(disk_error)) {
        if (error) {
            *error = disk_error;
        }
        return false;
    }
    std::string optical_error;
    const bool has_optical = LoadOpticalFileLocation(inode_id, &optical, &optical_error);
    if (has_optical) {
        *view->mutable_optical_location() = optical;
        ApplyOpticalLocationToAttr(optical, view->mutable_attr());
    } else if (!IsLocationMetadataMissing(optical_error)) {
        if (error) {
            *error = optical_error;
        }
        return false;
    } else {
        std::string masstree_optical_error;
        if (LoadMasstreeOpticalFileLocation(inode_id, &optical, &masstree_optical_error)) {
            *view->mutable_optical_location() = optical;
            ApplyOpticalLocationToAttr(optical, view->mutable_attr());
        } else if (!masstree_optical_error.empty()) {
            if (error) {
                *error = masstree_optical_error;
            }
            return false;
        }
    }
    return true;
}

bool MdsServiceImpl::LoadFilePrimaryLocation(uint64_t inode_id,
                                             const zb::rpc::InodeAttr& attr,
                                             zb::rpc::ReplicaLocation* anchor,
                                             std::string* error) const {
    if (!anchor || inode_id == 0) {
        if (error) {
            *error = "invalid file primary location output";
        }
        return false;
    }
    zb::rpc::DiskFileLocation disk;
    std::string attr_disk_error;
    if (BuildDiskFileLocationFromAttr(attr, &disk, &attr_disk_error)) {
        *anchor = ToReplicaLocation(inode_id, attr.size(), disk);
        return true;
    }
    if (!attr_disk_error.empty()) {
        if (error) {
            *error = attr_disk_error;
        }
        return false;
    }
    std::string disk_error;
    if (LoadDiskFileLocation(inode_id, &disk, &disk_error)) {
        *anchor = ToReplicaLocation(inode_id, attr.size(), disk);
        return true;
    }
    if (!IsLocationMetadataMissing(disk_error)) {
        if (error) {
            *error = disk_error;
        }
        return false;
    }
    zb::rpc::OpticalFileLocation optical;
    std::string attr_optical_error;
    if (BuildOpticalFileLocationFromAttr(attr, inode_id, &optical, &attr_optical_error)) {
        *anchor = ToReplicaLocation(inode_id, attr.size(), optical);
        return true;
    }
    if (!attr_optical_error.empty()) {
        if (error) {
            *error = attr_optical_error;
        }
        return false;
    }
    std::string optical_error;
    if (LoadOpticalFileLocation(inode_id, &optical, &optical_error)) {
        *anchor = ToReplicaLocation(inode_id, attr.size(), optical);
        return true;
    }
    if (!IsLocationMetadataMissing(optical_error)) {
        if (error) {
            *error = optical_error;
        }
        return false;
    }
    std::string masstree_optical_error;
    if (LoadMasstreeOpticalFileLocation(inode_id, &optical, &masstree_optical_error)) {
        *anchor = ToReplicaLocation(inode_id, attr.size(), optical);
        return true;
    }
    if (!masstree_optical_error.empty()) {
        if (error) {
            *error = masstree_optical_error;
        }
        return false;
    }
    if (error) {
        *error = "file has no usable replica location";
    }
    return false;
}

std::string MdsServiceImpl::BuildStableObjectId(uint64_t inode_id, uint32_t object_index) {
    return "obj-" + std::to_string(inode_id) + "-" + std::to_string(object_index);
}

void MdsServiceImpl::StripReplicaAddresses(zb::rpc::ReplicaLocation* replica) {
    if (!replica) {
        return;
    }
    replica->clear_node_address();
    replica->clear_primary_address();
    replica->clear_secondary_address();
}

bool MdsServiceImpl::ResolveNodeAddress(const std::string& node_id, std::string* address, std::string* error) const {
    if (node_id.empty() || !address) {
        if (error) {
            *error = "node_id is empty";
        }
        return false;
    }
    if (!allocator_ || !allocator_->ResolveNodeAddress(node_id, address) || address->empty()) {
        if (error) {
            *error = "failed to resolve node address for node_id=" + node_id;
        }
        return false;
    }
    return true;
}

bool MdsServiceImpl::DeleteFileMetaOnAnchor(const zb::rpc::ReplicaLocation& anchor,
                                            uint64_t inode_id,
                                            bool purge_objects,
                                            std::string* error) {
    if (inode_id == 0) {
        if (error) {
            *error = "inode_id is zero";
        }
        return false;
    }
    if (anchor.node_id().empty()) {
        if (error) {
            *error = "anchor node_id is empty";
        }
        return false;
    }
    if (purge_objects && anchor.disk_id().empty()) {
        if (error) {
            *error = "anchor disk_id is empty";
        }
        return false;
    }
    std::string address;
    if (!ResolveNodeAddress(anchor.node_id(), &address, error)) {
        return false;
    }
    brpc::Channel* channel = GetDataChannel(address, error);
    if (!channel) {
        return false;
    }

    zb::rpc::RealNodeService_Stub stub(channel);
    brpc::Controller cntl;
    cntl.set_timeout_ms(3000);
    zb::rpc::DeleteFileMetaRequest req;
    req.set_inode_id(inode_id);
    req.set_disk_id(anchor.disk_id());
    req.set_purge_objects(purge_objects);
    zb::rpc::DeleteFileMetaReply resp;
    stub.DeleteFileMeta(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        if (error) {
            *error = cntl.ErrorText();
        }
        return false;
    }
    if (resp.status().code() == zb::rpc::STATUS_OK || resp.status().code() == zb::rpc::STATUS_NOT_FOUND) {
        return true;
    }
    if (error) {
        *error = resp.status().message();
    }
    return false;
}

bool MdsServiceImpl::ResolveMasstreeRoute(const std::string& namespace_id,
                                          const std::string& path_prefix,
                                          MasstreeNamespaceRoute* route,
                                          std::string* error) const {
    if (!route) {
        if (error) {
            *error = "masstree route output is null";
        }
        return false;
    }
    *route = MasstreeNamespaceRoute();
    if (!path_prefix.empty()) {
        return masstree_namespace_catalog_.LookupByPath(path_prefix, route, error);
    }

    std::vector<MasstreeNamespaceRoute> routes;
    if (!masstree_namespace_catalog_.ListRoutes(&routes, error)) {
        return false;
    }
    if (namespace_id.empty()) {
        if (routes.empty()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        std::uniform_int_distribution<size_t> dist(0, routes.size() - 1);
        *route = routes[dist(rng)];
        if (error) {
            error->clear();
        }
        return true;
    }
    for (const auto& candidate : routes) {
        if (candidate.namespace_id == namespace_id) {
            *route = candidate;
            if (error) {
                error->clear();
            }
            return true;
        }
    }
    if (error) {
        error->clear();
    }
    return false;
}

bool MdsServiceImpl::PickRandomMasstreeFileInode(const MasstreeNamespaceRoute& route,
                                                 uint64_t* inode_id,
                                                 zb::rpc::InodeAttr* attr,
                                                 std::string* error) const {
    if (!inode_id || !attr) {
        if (error) {
            *error = "masstree random file outputs are null";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    if (route.manifest_path.empty() ||
        !MasstreeNamespaceManifest::LoadFromFile(route.manifest_path, &manifest, error)) {
        return false;
    }
    if (manifest.file_count == 0) {
        if (error) {
            *error = "masstree namespace has no files";
        }
        return false;
    }
    if (manifest.inode_min == 0 || manifest.inode_min > manifest.inode_max) {
        if (error) {
            *error = "invalid masstree file inode range";
        }
        return false;
    }

    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist(manifest.inode_min, manifest.inode_max);
    const uint64_t random_attempts =
        std::min<uint64_t>(std::max<uint64_t>(32ULL, manifest.file_count == 0 ? 32ULL : 64ULL),
                           manifest.inode_max - manifest.inode_min + 1ULL);
    for (uint64_t attempt = 0; attempt < random_attempts; ++attempt) {
        const uint64_t candidate_inode = dist(rng);
        zb::rpc::InodeAttr candidate_attr;
        std::string local_error;
        if (!masstree_meta_store_.GetInode(route, candidate_inode, &candidate_attr, &local_error)) {
            continue;
        }
        if (candidate_attr.type() != zb::rpc::INODE_FILE) {
            continue;
        }
        *inode_id = candidate_inode;
        *attr = std::move(candidate_attr);
        if (error) {
            error->clear();
        }
        return true;
    }

    for (uint64_t candidate_inode = manifest.inode_min; candidate_inode <= manifest.inode_max; ++candidate_inode) {
        zb::rpc::InodeAttr candidate_attr;
        std::string local_error;
        if (!masstree_meta_store_.GetInode(route, candidate_inode, &candidate_attr, &local_error)) {
            continue;
        }
        if (candidate_attr.type() != zb::rpc::INODE_FILE) {
            continue;
        }
        *inode_id = candidate_inode;
        *attr = std::move(candidate_attr);
        if (error) {
            error->clear();
        }
        return true;
    }

    if (error) {
        *error = "masstree file not found";
    }
    return false;
}

bool MdsServiceImpl::PickRandomMasstreeFile(const MasstreeNamespaceRoute& route,
                                            const std::string& query_mode,
                                            uint64_t* inode_id,
                                            zb::rpc::InodeAttr* attr,
                                            std::string* file_name,
                                            std::string* full_path,
                                            std::string* error) const {
    if (!inode_id || !attr || !file_name || !full_path) {
        if (error) {
            *error = "masstree random file outputs are null";
        }
        return false;
    }
    if (!PickRandomMasstreeFileInode(route, inode_id, attr, error)) {
        return false;
    }
    if (!masstree_meta_store_.BuildFullPath(route, *inode_id, full_path, file_name, error)) {
        return false;
    }
    if (query_mode == kMasstreeQueryModeRandomPathLookup) {
        uint64_t resolved_inode = 0;
        zb::rpc::InodeAttr resolved_attr;
        if (!masstree_meta_store_.ResolvePath(route, *full_path, &resolved_inode, &resolved_attr, error)) {
            return false;
        }
        if (resolved_inode != *inode_id) {
            if (error) {
                *error = "masstree path lookup returned unexpected inode";
            }
            return false;
        }
        *attr = std::move(resolved_attr);
    }
    if (error) {
        error->clear();
    }
    return true;
}

void MdsServiceImpl::RunMasstreeImportWorker() {
    while (true) {
        std::shared_ptr<MasstreeImportJob> job;
        {
            std::unique_lock<std::mutex> lock(masstree_import_job_mu_);
            masstree_import_job_cv_.wait(lock, [this]() {
                return stop_masstree_import_worker_ || !masstree_import_job_queue_.empty();
            });
            if (stop_masstree_import_worker_ && masstree_import_job_queue_.empty()) {
                return;
            }
            job = masstree_import_job_queue_.front();
            masstree_import_job_queue_.pop_front();
            job->state = zb::rpc::MASSTREE_IMPORT_JOB_RUNNING;
        }

        std::string error;
        MasstreeImportService::Result result;
        bool ok = false;
        if (job->kind == MasstreeImportJob::Kind::kGenerateTemplate) {
            ok = masstree_import_service_.GenerateTemplate(job->template_request, &result, &error);
        } else {
            ok = masstree_import_service_.ImportTemplateNamespace(job->import_request, &result, &error);
        }
        if (ok) {
            std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
            job->result = std::move(result);
            job->error_message.clear();
            job->state = zb::rpc::MASSTREE_IMPORT_JOB_COMPLETED;
            TrimMasstreeImportJobsLocked();
        } else {
            std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
            job->error_message = error.empty() ? "masstree job failed" : error;
            job->state = zb::rpc::MASSTREE_IMPORT_JOB_FAILED;
            TrimMasstreeImportJobsLocked();
        }
    }
}

void MdsServiceImpl::TrimMasstreeImportJobsLocked() {
    if (masstree_import_jobs_.size() <= kMaxRetainedMasstreeImportJobs) {
        return;
    }

    size_t scan_budget = masstree_import_job_history_.size();
    while (masstree_import_jobs_.size() > kMaxRetainedMasstreeImportJobs &&
           !masstree_import_job_history_.empty() &&
           scan_budget-- > 0) {
        const std::string job_id = masstree_import_job_history_.front();
        masstree_import_job_history_.pop_front();

        auto it = masstree_import_jobs_.find(job_id);
        if (it == masstree_import_jobs_.end()) {
            continue;
        }

        const zb::rpc::MasstreeImportJobState state = it->second->state;
        if (state == zb::rpc::MASSTREE_IMPORT_JOB_PENDING ||
            state == zb::rpc::MASSTREE_IMPORT_JOB_RUNNING) {
            masstree_import_job_history_.push_back(job_id);
            continue;
        }

        masstree_import_jobs_.erase(it);
    }
}

std::shared_ptr<MdsServiceImpl::MasstreeImportJob>
MdsServiceImpl::EnqueueMasstreeTemplateJob(const MasstreeImportService::TemplateGenerationRequest& request) {
    auto job = std::make_shared<MasstreeImportJob>();
    job->kind = MasstreeImportJob::Kind::kGenerateTemplate;
    {
        std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
        std::ostringstream oss;
        oss << "masstree-job-" << NowMilliseconds() << "-" << masstree_import_next_job_id_++;
        job->job_id = oss.str();
        job->template_request = request;
        masstree_import_jobs_[job->job_id] = job;
        masstree_import_job_history_.push_back(job->job_id);
        masstree_import_job_queue_.push_back(job);
        TrimMasstreeImportJobsLocked();
    }
    masstree_import_job_cv_.notify_one();
    return job;
}

std::shared_ptr<MdsServiceImpl::MasstreeImportJob>
MdsServiceImpl::EnqueueMasstreeNamespaceImportJob(const MasstreeImportService::TemplateImportRequest& request) {
    auto job = std::make_shared<MasstreeImportJob>();
    job->kind = MasstreeImportJob::Kind::kImportTemplateNamespace;
    {
        std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
        std::ostringstream oss;
        oss << "masstree-job-" << NowMilliseconds() << "-" << masstree_import_next_job_id_++;
        job->job_id = oss.str();
        job->import_request = request;
        masstree_import_jobs_[job->job_id] = job;
        masstree_import_job_history_.push_back(job->job_id);
        masstree_import_job_queue_.push_back(job);
        TrimMasstreeImportJobsLocked();
    }
    masstree_import_job_cv_.notify_one();
    return job;
}

std::shared_ptr<MdsServiceImpl::MasstreeImportJob>
MdsServiceImpl::FindMasstreeImportJob(const std::string& job_id) const {
    std::lock_guard<std::mutex> lock(masstree_import_job_mu_);
    auto it = masstree_import_jobs_.find(job_id);
    if (it == masstree_import_jobs_.end()) {
        return nullptr;
    }
    return it->second;
}

void MdsServiceImpl::FillMasstreeImportJobInfo(const MasstreeImportJob& job,
                                               zb::rpc::MasstreeImportJobInfo* info) {
    if (!info) {
        return;
    }
    info->Clear();
    info->set_job_id(job.job_id);
    info->set_template_id(job.kind == MasstreeImportJob::Kind::kGenerateTemplate
                              ? job.template_request.template_id
                              : job.import_request.template_id);
    if (job.kind == MasstreeImportJob::Kind::kImportTemplateNamespace) {
        info->set_namespace_id(job.import_request.namespace_id);
        info->set_generation_id(job.import_request.generation_id);
        info->set_path_prefix(job.import_request.path_prefix);
    }
    info->set_file_count(job.result.file_count);
    info->set_state(job.state);
    info->set_error_message(job.error_message);
    info->set_manifest_path(job.result.manifest_path);
    info->set_root_inode_id(job.result.root_inode_id);
    info->set_inode_count(job.result.inode_count);
    info->set_dentry_count(job.result.dentry_count);
    info->set_inode_min(job.result.inode_min);
    info->set_inode_max(job.result.inode_max);
    info->set_inode_pages_bytes(job.result.inode_pages_bytes);
    info->set_level1_dir_count(job.result.level1_dir_count);
    info->set_leaf_dir_count(job.result.leaf_dir_count);
    info->set_total_file_bytes(job.result.total_file_bytes);
    info->set_avg_file_size_bytes(job.result.avg_file_size_bytes);
}

brpc::Channel* MdsServiceImpl::GetDataChannel(const std::string& address, std::string* error) {
    if (address.empty()) {
        if (error) {
            *error = "empty target address";
        }
        return nullptr;
    }

    std::lock_guard<std::mutex> lock(channel_mu_);
    auto it = channels_.find(address);
    if (it != channels_.end()) {
        return it->second.get();
    }

    auto channel = std::make_unique<brpc::Channel>();
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = 3000;
    options.max_retry = 0;
    if (channel->Init(address.c_str(), &options) != 0) {
        if (error) {
            *error = "failed to init channel to " + address;
        }
        return nullptr;
    }
    brpc::Channel* raw = channel.get();
    channels_[address] = std::move(channel);
    return raw;
}

uint64_t MdsServiceImpl::AllocateInodeId(std::string* error) {
    std::string value;
    uint64_t next_id = kRootInodeId + 1;
    if (store_->Get(NextInodeKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "Invalid next inode value";
            }
            return 0;
        }
    } else if (error && !error->empty()) {
        return 0;
    }

    uint64_t allocated = next_id;
    uint64_t new_value = next_id + 1;
    if (!store_->Put(NextInodeKey(), MetaCodec::EncodeUInt64(new_value), error)) {
        return 0;
    }
    return allocated;
}

uint64_t MdsServiceImpl::AllocateHandleId(std::string* error) {
    std::string value;
    uint64_t next_id = 1;
    if (store_->Get(NextHandleKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "Invalid next handle value";
            }
            return 0;
        }
    } else if (error && !error->empty()) {
        return 0;
    }

    uint64_t allocated = next_id;
    uint64_t new_value = next_id + 1;
    if (!store_->Put(NextHandleKey(), MetaCodec::EncodeUInt64(new_value), error)) {
        return 0;
    }
    return allocated;
}

uint64_t MdsServiceImpl::NowSeconds() {
    using namespace std::chrono;
    return duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
}

uint64_t MdsServiceImpl::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void MdsServiceImpl::FillStatus(zb::rpc::MdsStatus* status,
                                zb::rpc::MdsStatusCode code,
                                const std::string& message) {
    if (!status) {
        return;
    }
    status->set_code(code);
    status->set_message(message);
}

} // namespace zb::mds
