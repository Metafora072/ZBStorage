#include "MasstreeBulkMetaGenerator.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <vector>

#include "MasstreeDecimalUtils.h"
#include "MasstreeManifest.h"
#include "MasstreeOpticalProfile.h"
#include "MasstreePageLayout.h"
#include "../storage/UnifiedInodeRecord.h"
#include "mds.pb.h"

namespace fs = std::filesystem;

namespace zb::mds {

namespace {

constexpr uint32_t kDefaultDirMode = 0755;
constexpr uint32_t kDefaultFileMode = 0644;
constexpr uint32_t kDefaultBlockSize = 4096;
constexpr uint64_t kDefaultDirSize = 4096;
constexpr char kSourceModeSynthetic[] = "synthetic";
constexpr char kSourceModePathList[] = "path_list";
constexpr char kDefaultRepeatDirPrefix[] = "copy";
constexpr size_t kMinRepeatWidth = 6;
constexpr size_t kMasstreeIoBufferBytes = 8U * 1024U * 1024U;
constexpr size_t kInvalidNodeIndex = static_cast<size_t>(-1);

struct PathListNode {
    std::string name;
    std::string relative_path;
    size_t parent{kInvalidNodeIndex};
    uint32_t depth{0};
    std::vector<size_t> children;
    bool explicit_entry{false};
    bool explicit_dir{false};
};

struct PathListPlan {
    std::vector<PathListNode> nodes;
    uint64_t base_file_count{0};
    uint64_t base_dir_count{0};
    uint64_t dir_count{0};
    uint64_t repeat_count{0};
    uint64_t actual_file_count{0};
    uint64_t actual_inode_count{0};
    uint64_t actual_dentry_count{0};
    uint64_t base_max_depth{0};
    uint64_t max_depth{0};
    uint64_t wrapper_name_width{kMinRepeatWidth};
    std::vector<uint64_t> base_dir_count_by_depth;
    std::vector<uint64_t> base_file_count_by_depth;
    std::vector<uint64_t> dir_count_by_depth;
    std::vector<uint64_t> file_count_by_depth;
    std::string fingerprint;
    std::string repeat_dir_prefix;
};

bool NormalizePath(std::string path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    std::string out;
    out.reserve(path.size() + 1);
    bool prev_slash = false;
    for (char ch : path) {
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
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    if (out.empty()) {
        out = "/";
    }
    *normalized = std::move(out);
    return true;
}

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

std::string NormalizeSourceMode(std::string mode) {
    mode = Trim(std::move(mode));
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return mode.empty() ? std::string(kSourceModeSynthetic) : mode;
}

bool IsPathListMode(const std::string& source_mode) {
    return NormalizeSourceMode(source_mode) == kSourceModePathList;
}

bool NormalizeRepeatDirPrefix(std::string prefix,
                              std::string* normalized,
                              std::string* error) {
    if (!normalized) {
        if (error) {
            *error = "repeat_dir_prefix output is null";
        }
        return false;
    }
    prefix = Trim(std::move(prefix));
    if (prefix.empty()) {
        prefix = kDefaultRepeatDirPrefix;
    }
    std::replace(prefix.begin(), prefix.end(), '\\', '/');
    if (prefix.find('/') != std::string::npos) {
        if (error) {
            *error = "repeat_dir_prefix must be a single path segment";
        }
        return false;
    }
    if (prefix == "." || prefix == "..") {
        if (error) {
            *error = "repeat_dir_prefix is invalid";
        }
        return false;
    }
    *normalized = std::move(prefix);
    if (error) {
        error->clear();
    }
    return true;
}

std::vector<std::string> SplitRelativePath(const std::string& path) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (start < path.size()) {
        const size_t slash = path.find('/', start);
        if (slash == std::string::npos) {
            parts.push_back(path.substr(start));
            break;
        }
        parts.push_back(path.substr(start, slash - start));
        start = slash + 1U;
    }
    return parts;
}

bool NormalizeRelativeTreePath(std::string raw,
                               std::string* normalized,
                               bool* explicit_dir,
                               std::string* error) {
    if (!normalized || !explicit_dir) {
        if (error) {
            *error = "path_list normalization output is null";
        }
        return false;
    }
    raw = Trim(std::move(raw));
    *explicit_dir = false;
    if (raw.empty()) {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    std::replace(raw.begin(), raw.end(), '\\', '/');
    while (raw.size() >= 2U && raw[0] == '.' && raw[1] == '/') {
        raw.erase(0, 2U);
    }
    while (!raw.empty() && raw.front() == '/') {
        raw.erase(raw.begin());
    }
    while (!raw.empty() && raw.back() == '/') {
        raw.pop_back();
        *explicit_dir = true;
    }
    if (raw.empty() || raw == ".") {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    std::string out;
    out.reserve(raw.size());
    bool prev_slash = false;
    for (char ch : raw) {
        if (ch == '/') {
            if (!prev_slash) {
                out.push_back(ch);
            }
            prev_slash = true;
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    if (out.empty()) {
        normalized->clear();
        if (error) {
            error->clear();
        }
        return true;
    }

    const std::vector<std::string> parts = SplitRelativePath(out);
    std::ostringstream rebuilt;
    bool first = true;
    for (const std::string& part : parts) {
        if (part.empty() || part == ".") {
            continue;
        }
        if (part == "..") {
            if (error) {
                *error = "path_list contains parent traversal";
            }
            return false;
        }
        if (!first) {
            rebuilt << '/';
        }
        rebuilt << part;
        first = false;
    }
    *normalized = rebuilt.str();
    if (error) {
        error->clear();
    }
    return true;
}

bool NodeIsDirectory(const PathListNode& node, bool leaf_nodes_are_files) {
    if (node.parent == kInvalidNodeIndex || node.explicit_dir || !node.children.empty()) {
        return true;
    }
    if (leaf_nodes_are_files) {
        return false;
    }
    return node.explicit_entry && node.name.find('.') == std::string::npos;
}

void AppendLe16(std::string* out, uint16_t value) {
    out->push_back(static_cast<char>(value & 0xFFU));
    out->push_back(static_cast<char>((value >> 8U) & 0xFFU));
}

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

void AppendLe64(std::string* out, uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

std::string FixedWidthDecimal(uint64_t value, size_t width) {
    std::ostringstream out;
    out.width(static_cast<std::streamsize>(width));
    out.fill('0');
    out << value;
    return out.str();
}

std::string DirName(const char prefix, uint64_t value) {
    return std::string(1, prefix) + FixedWidthDecimal(value, 6);
}

std::string FileName(uint64_t value) {
    return "f" + FixedWidthDecimal(value, 9);
}

std::string RepeatDirName(const std::string& prefix, uint64_t repeat_index, size_t width) {
    return prefix + "_" + FixedWidthDecimal(repeat_index, width);
}

uint64_t CeilDiv(uint64_t lhs, uint64_t rhs) {
    return rhs == 0 ? 0 : ((lhs + rhs - 1) / rhs);
}

size_t DecimalDigits(uint64_t value) {
    size_t digits = 1;
    while (value >= 10U) {
        value /= 10U;
        ++digits;
    }
    return digits;
}

uint64_t NowSeconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<seconds>(system_clock::now().time_since_epoch()).count());
}

template <typename Stream>
void ConfigureStreamBuffer(Stream* stream, std::vector<char>* buffer) {
    if (!stream || !buffer || buffer->empty()) {
        return;
    }
    stream->rdbuf()->pubsetbuf(buffer->data(), static_cast<std::streamsize>(buffer->size()));
}

bool WriteExact(std::ofstream* out, const std::string& data) {
    return out && out->write(data.data(), static_cast<std::streamsize>(data.size())).good();
}

bool WriteInodeRecord(std::ofstream* out,
                      uint64_t inode_id,
                      const zb::rpc::InodeAttr& attr,
                      uint64_t parent_inode_id,
                      std::string* error) {
    if (!out) {
        if (error) {
            *error = "inode record output is unavailable";
        }
        return false;
    }
    std::string payload;
    UnifiedInodeRecord unified_record;
    if (!AttrToUnifiedInodeRecord(attr, parent_inode_id, UnifiedStorageTier::kNone, &unified_record, error) ||
        !EncodeUnifiedInodeRecord(unified_record, &payload, error)) {
        return false;
    }
    std::string wire_record;
    wire_record.reserve(sizeof(uint64_t) + sizeof(uint32_t) + payload.size());
    AppendLe64(&wire_record, inode_id);
    AppendLe32(&wire_record, static_cast<uint32_t>(payload.size()));
    wire_record.append(payload);
    if (!WriteExact(out, wire_record)) {
        if (error) {
            *error = "failed to write inode record";
        }
        return false;
    }
    return true;
}

bool WriteDentryRecord(std::ofstream* out,
                       uint64_t parent_inode,
                       const std::string& name,
                       uint64_t child_inode,
                       zb::rpc::InodeType type,
                       std::string* error) {
    if (!out) {
        if (error) {
            *error = "dentry record output is unavailable";
        }
        return false;
    }
    if (name.size() > 0xFFFFU) {
        if (error) {
            *error = "dentry name exceeds record encoding limit";
        }
        return false;
    }
    std::string wire_record;
    wire_record.reserve(sizeof(uint64_t) + sizeof(uint16_t) + name.size() + sizeof(uint64_t) + sizeof(uint8_t));
    AppendLe64(&wire_record, parent_inode);
    AppendLe16(&wire_record, static_cast<uint16_t>(name.size()));
    wire_record.append(name);
    AppendLe64(&wire_record, child_inode);
    wire_record.push_back(static_cast<char>(type));
    if (!WriteExact(out, wire_record)) {
        if (error) {
            *error = "failed to write dentry record";
        }
        return false;
    }
    return true;
}

zb::rpc::InodeAttr BuildDirectoryAttr(uint64_t inode_id,
                                      const std::string& file_name,
                                      uint32_t nlink,
                                      uint64_t now_sec) {
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_DIR);
    attr.set_mode(kDefaultDirMode);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(kDefaultDirSize);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(nlink);
    attr.set_blksize(kDefaultBlockSize);
    attr.set_blocks_512((attr.size() + 511ULL) / 512ULL);
    attr.set_rdev_major(0);
    attr.set_rdev_minor(0);
    attr.set_object_unit_size(0);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    attr.set_file_name(file_name);
    return attr;
}

zb::rpc::InodeAttr BuildFileAttr(uint64_t inode_id,
                                 const std::string& file_name,
                                 uint64_t file_size_bytes,
                                 uint64_t now_sec) {
    zb::rpc::InodeAttr attr;
    attr.set_inode_id(inode_id);
    attr.set_type(zb::rpc::INODE_FILE);
    attr.set_mode(kDefaultFileMode);
    attr.set_uid(0);
    attr.set_gid(0);
    attr.set_size(file_size_bytes);
    attr.set_atime(now_sec);
    attr.set_mtime(now_sec);
    attr.set_ctime(now_sec);
    attr.set_nlink(1);
    attr.set_blksize(kDefaultBlockSize);
    attr.set_blocks_512((attr.size() + 511ULL) / 512ULL);
    attr.set_rdev_major(0);
    attr.set_rdev_minor(0);
    attr.set_object_unit_size(0);
    attr.set_replica(1);
    attr.set_version(1);
    attr.set_file_archive_state(zb::rpc::INODE_ARCHIVE_PENDING);
    attr.set_file_name(file_name);
    return attr;
}

uint64_t HashBytes(const std::string& value) {
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : value) {
        hash ^= static_cast<uint64_t>(ch);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string HashHex(const std::string& value) {
    std::ostringstream out;
    out << std::hex << HashBytes(value);
    return out.str();
}

uint64_t GenerateFileSizeBytes(uint64_t namespace_seed,
                               uint64_t generation_seed,
                               uint64_t file_index,
                               const MasstreeOpticalProfile& profile) {
    const uint64_t span = profile.max_file_size_bytes - profile.min_file_size_bytes;
    const uint64_t seed = namespace_seed ^
                          (generation_seed << 1U) ^
                          (file_index * 11400714819323198485ULL);
    return profile.min_file_size_bytes + (seed % (span + 1ULL));
}

void EnsureDepthVectorSize(std::vector<uint64_t>* counts, size_t depth) {
    if (!counts) {
        return;
    }
    if (counts->size() <= depth) {
        counts->resize(depth + 1U, 0);
    }
}

bool WriteVerifyManifest(const fs::path& path,
                         const MasstreeNamespaceManifest& manifest,
                         std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to open verify manifest: " + path.string();
        }
        return false;
    }
    out << "masstree_namespace_verify_v1\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "source_mode=" << manifest.source_mode << "\n";
    out << "path_prefix=" << manifest.path_prefix << "\n";
    out << "root_inode_id=" << manifest.root_inode_id << "\n";
    out << "inode_min=" << manifest.inode_min << "\n";
    out << "inode_max=" << manifest.inode_max << "\n";
    out << "inode_count=" << manifest.inode_count << "\n";
    out << "dentry_count=" << manifest.dentry_count << "\n";
    out << "target_file_count=" << manifest.target_file_count << "\n";
    out << "file_count=" << manifest.file_count << "\n";
    out << "template_base_file_count=" << manifest.template_base_file_count << "\n";
    out << "template_base_dir_count=" << manifest.template_base_dir_count << "\n";
    out << "dir_count=" << manifest.dir_count << "\n";
    out << "template_repeat_count=" << manifest.template_repeat_count << "\n";
    out << "repeat_dir_prefix=" << manifest.repeat_dir_prefix << "\n";
    out << "path_list_leaf_nodes_are_files=" << (manifest.path_list_leaf_nodes_are_files ? 1 : 0) << "\n";
    out << "path_list_fingerprint=" << manifest.path_list_fingerprint << "\n";
    out << "level1_dir_count=" << manifest.level1_dir_count << "\n";
    out << "leaf_dir_count=" << manifest.leaf_dir_count << "\n";
    out << "template_base_max_depth=" << manifest.template_base_max_depth << "\n";
    out << "max_depth=" << manifest.max_depth << "\n";
    out << "max_files_per_leaf_dir=" << manifest.max_files_per_leaf_dir << "\n";
    out << "max_subdirs_per_dir=" << manifest.max_subdirs_per_dir << "\n";
    out << "min_file_size_bytes=" << manifest.min_file_size_bytes << "\n";
    out << "max_file_size_bytes=" << manifest.max_file_size_bytes << "\n";
    out << "avg_file_size_bytes=" << manifest.avg_file_size_bytes << "\n";
    out << "total_file_bytes=" << manifest.total_file_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write verify manifest: " + path.string();
        }
        return false;
    }
    return true;
}

bool WriteStructureStats(const fs::path& path,
                         const MasstreeNamespaceManifest& manifest,
                         const std::vector<uint64_t>& dir_count_by_depth,
                         const std::vector<uint64_t>& file_count_by_depth,
                         const std::vector<uint64_t>& base_dir_count_by_depth,
                         const std::vector<uint64_t>& base_file_count_by_depth,
                         std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to open structure stats: " + path.string();
        }
        return false;
    }
    out << "masstree_namespace_structure_v1\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "source_mode=" << manifest.source_mode << "\n";
    out << "path_prefix=" << manifest.path_prefix << "\n";
    out << "target_file_count=" << manifest.target_file_count << "\n";
    out << "file_count=" << manifest.file_count << "\n";
    out << "dir_count=" << manifest.dir_count << "\n";
    out << "inode_count=" << manifest.inode_count << "\n";
    out << "dentry_count=" << manifest.dentry_count << "\n";
    out << "template_base_file_count=" << manifest.template_base_file_count << "\n";
    out << "template_base_dir_count=" << manifest.template_base_dir_count << "\n";
    out << "template_repeat_count=" << manifest.template_repeat_count << "\n";
    out << "template_base_max_depth=" << manifest.template_base_max_depth << "\n";
    out << "max_depth=" << manifest.max_depth << "\n";
    out << "actual_level_count=" << dir_count_by_depth.size() << "\n";
    for (size_t depth = 0; depth < dir_count_by_depth.size(); ++depth) {
        const uint64_t file_count = depth < file_count_by_depth.size() ? file_count_by_depth[depth] : 0;
        out << "actual_level[" << depth << "]=dirs:" << dir_count_by_depth[depth]
            << ",files:" << file_count << "\n";
    }
    out << "base_level_count=" << base_dir_count_by_depth.size() << "\n";
    for (size_t depth = 0; depth < base_dir_count_by_depth.size(); ++depth) {
        const uint64_t file_count = depth < base_file_count_by_depth.size() ? base_file_count_by_depth[depth] : 0;
        out << "base_level[" << depth << "]=dirs:" << base_dir_count_by_depth[depth]
            << ",files:" << file_count << "\n";
    }
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write structure stats: " + path.string();
        }
        return false;
    }
    return true;
}

bool BuildPathListPlan(const MasstreeBulkMetaGenerator::Request& request,
                       PathListPlan* plan,
                       std::string* error) {
    if (!plan) {
        if (error) {
            *error = "path_list plan output is null";
        }
        return false;
    }
    if (request.path_list_file.empty()) {
        if (error) {
            *error = "path_list_file is required for path_list source_mode";
        }
        return false;
    }
    if (request.file_count == 0) {
        if (error) {
            *error = "file_count must be greater than zero for path_list mode";
        }
        return false;
    }

    std::string repeat_dir_prefix;
    if (!NormalizeRepeatDirPrefix(request.repeat_dir_prefix, &repeat_dir_prefix, error)) {
        return false;
    }

    std::ifstream input(request.path_list_file);
    if (!input) {
        if (error) {
            *error = "failed to open path_list_file: " + request.path_list_file;
        }
        return false;
    }

    PathListPlan local_plan;
    local_plan.nodes.push_back(PathListNode());
    local_plan.nodes[0].name = "/";
    local_plan.nodes[0].relative_path.clear();
    local_plan.nodes[0].parent = kInvalidNodeIndex;
    std::unordered_map<std::string, size_t> node_index_by_path;
    node_index_by_path.emplace(std::string(), 0U);

    std::string line;
    size_t line_no = 0;
    bool has_any_node = false;
    while (std::getline(input, line)) {
        ++line_no;
        std::string normalized_path;
        bool explicit_dir = false;
        std::string normalize_error;
        if (!NormalizeRelativeTreePath(line, &normalized_path, &explicit_dir, &normalize_error)) {
            if (error) {
                *error = "invalid path_list entry at line " + std::to_string(line_no) + ": " + normalize_error;
            }
            return false;
        }
        if (normalized_path.empty()) {
            continue;
        }
        has_any_node = true;
        const std::vector<std::string> parts = SplitRelativePath(normalized_path);
        size_t current = 0;
        std::string current_path;
        for (size_t i = 0; i < parts.size(); ++i) {
            current_path = current_path.empty() ? parts[i] : current_path + "/" + parts[i];
            auto it = node_index_by_path.find(current_path);
            if (it == node_index_by_path.end()) {
                const size_t new_index = local_plan.nodes.size();
                PathListNode node;
                node.name = parts[i];
                node.relative_path = current_path;
                node.parent = current;
                node.depth = local_plan.nodes[current].depth + 1U;
                local_plan.nodes.push_back(std::move(node));
                local_plan.nodes[current].children.push_back(new_index);
                node_index_by_path.emplace(current_path, new_index);
                current = new_index;
            } else {
                current = it->second;
            }
        }
        if (explicit_dir) {
            local_plan.nodes[current].explicit_dir = true;
        }
        local_plan.nodes[current].explicit_entry = true;
    }

    if (!input.good() && !input.eof()) {
        if (error) {
            *error = "failed to read path_list_file: " + request.path_list_file;
        }
        return false;
    }
    if (!has_any_node) {
        if (error) {
            *error = "path_list_file does not contain any valid paths";
        }
        return false;
    }

    for (PathListNode& node : local_plan.nodes) {
        std::sort(node.children.begin(), node.children.end(), [&](size_t lhs, size_t rhs) {
            return local_plan.nodes[lhs].name < local_plan.nodes[rhs].name;
        });
    }

    std::vector<size_t> fingerprint_order;
    fingerprint_order.reserve(local_plan.nodes.size() - 1U);
    for (size_t i = 1; i < local_plan.nodes.size(); ++i) {
        fingerprint_order.push_back(i);
        const PathListNode& node = local_plan.nodes[i];
        const bool is_dir = NodeIsDirectory(node, request.path_list_leaf_nodes_are_files);
        const size_t depth = static_cast<size_t>(node.depth);
        if (is_dir) {
            ++local_plan.base_dir_count;
            EnsureDepthVectorSize(&local_plan.base_dir_count_by_depth, depth);
            ++local_plan.base_dir_count_by_depth[depth];
        } else {
            ++local_plan.base_file_count;
            EnsureDepthVectorSize(&local_plan.base_file_count_by_depth, depth);
            ++local_plan.base_file_count_by_depth[depth];
        }
        if (node.depth > local_plan.base_max_depth) {
            local_plan.base_max_depth = node.depth;
        }
    }
    if (local_plan.base_file_count == 0) {
        if (error) {
            *error = "path_list tree does not contain any file nodes";
        }
        return false;
    }

    std::sort(fingerprint_order.begin(), fingerprint_order.end(), [&](size_t lhs, size_t rhs) {
        return local_plan.nodes[lhs].relative_path < local_plan.nodes[rhs].relative_path;
    });
    std::string fingerprint_input;
    for (size_t node_index : fingerprint_order) {
        fingerprint_input.push_back(
            NodeIsDirectory(local_plan.nodes[node_index], request.path_list_leaf_nodes_are_files) ? 'D' : 'F');
        fingerprint_input.push_back(':');
        fingerprint_input.append(local_plan.nodes[node_index].relative_path);
        fingerprint_input.push_back('\n');
    }
    local_plan.fingerprint = HashHex(fingerprint_input);
    local_plan.repeat_dir_prefix = repeat_dir_prefix;
    local_plan.repeat_count = CeilDiv(request.file_count, local_plan.base_file_count);
    local_plan.actual_file_count = local_plan.repeat_count * local_plan.base_file_count;
    local_plan.dir_count = 1ULL + local_plan.repeat_count * (1ULL + local_plan.base_dir_count);
    const uint64_t base_non_root_node_count = static_cast<uint64_t>(local_plan.nodes.size() - 1U);
    local_plan.actual_inode_count = 1ULL + local_plan.repeat_count * (1ULL + base_non_root_node_count);
    local_plan.actual_dentry_count = local_plan.repeat_count * (1ULL + base_non_root_node_count);
    local_plan.max_depth = local_plan.base_max_depth + 1ULL;
    local_plan.dir_count_by_depth.assign(static_cast<size_t>(local_plan.max_depth + 1ULL), 0);
    local_plan.file_count_by_depth.assign(static_cast<size_t>(local_plan.max_depth + 1ULL), 0);
    local_plan.dir_count_by_depth[0] = 1;
    local_plan.dir_count_by_depth[1] = local_plan.repeat_count;
    for (size_t depth = 1; depth < local_plan.base_dir_count_by_depth.size(); ++depth) {
        local_plan.dir_count_by_depth[depth + 1U] +=
            local_plan.repeat_count * local_plan.base_dir_count_by_depth[depth];
    }
    for (size_t depth = 1; depth < local_plan.base_file_count_by_depth.size(); ++depth) {
        local_plan.file_count_by_depth[depth + 1U] +=
            local_plan.repeat_count * local_plan.base_file_count_by_depth[depth];
    }
    local_plan.wrapper_name_width =
        std::max<size_t>(kMinRepeatWidth,
                         DecimalDigits(local_plan.repeat_count == 0 ? 0 : (local_plan.repeat_count - 1ULL)));
    if (repeat_dir_prefix.size() + 1U + local_plan.wrapper_name_width > unified_inode_layout::kFileNameSize) {
        if (error) {
            *error = "repeat_dir_prefix is too long for inode file_name";
        }
        return false;
    }

    *plan = std::move(local_plan);
    if (error) {
        error->clear();
    }
    return true;
}

bool AssignPathListSubtreeInodes(const PathListPlan& plan,
                                 size_t node_index,
                                 bool path_list_leaf_nodes_are_files,
                                 uint64_t parent_inode_id,
                                 uint64_t now_sec,
                                 uint64_t namespace_seed,
                                 uint64_t generation_seed,
                                 const MasstreeOpticalProfile& optical_profile,
                                 uint64_t* next_inode,
                                 uint64_t* file_ordinal,
                                 MasstreeDecimalAccumulator* total_file_bytes_accumulator,
                                 std::vector<uint64_t>* node_inode_ids,
                                 std::ofstream* inode_out,
                                 std::string* error) {
    if (!next_inode || !file_ordinal || !total_file_bytes_accumulator || !node_inode_ids ||
        node_index >= node_inode_ids->size()) {
        if (error) {
            *error = "invalid path_list emit state";
        }
        return false;
    }
    const PathListNode& node = plan.nodes[node_index];
    const uint64_t inode_id = (*next_inode)++;
    (*node_inode_ids)[node_index] = inode_id;
    const bool is_dir = NodeIsDirectory(node, path_list_leaf_nodes_are_files);
    if (is_dir) {
        const uint32_t nlink = static_cast<uint32_t>(2U + node.children.size());
        if (!WriteInodeRecord(inode_out,
                              inode_id,
                              BuildDirectoryAttr(inode_id, node.name, nlink, now_sec),
                              parent_inode_id,
                              error)) {
            return false;
        }
        for (size_t child_index : node.children) {
            if (!AssignPathListSubtreeInodes(plan,
                                             child_index,
                                             path_list_leaf_nodes_are_files,
                                             inode_id,
                                             now_sec,
                                             namespace_seed,
                                             generation_seed,
                                             optical_profile,
                                             next_inode,
                                             file_ordinal,
                                             total_file_bytes_accumulator,
                                             node_inode_ids,
                                             inode_out,
                                             error)) {
                return false;
            }
        }
        return true;
    }

    const uint64_t current_file_ordinal = (*file_ordinal)++;
    const uint64_t file_size_bytes =
        GenerateFileSizeBytes(namespace_seed, generation_seed, current_file_ordinal, optical_profile);
    total_file_bytes_accumulator->Add(file_size_bytes);
    if (!WriteInodeRecord(inode_out,
                          inode_id,
                          BuildFileAttr(inode_id, node.name, file_size_bytes, now_sec),
                          parent_inode_id,
                          error)) {
        return false;
    }
    return true;
}

bool EmitPathListDentryGroup(const PathListPlan& plan,
                             size_t parent_index,
                             bool path_list_leaf_nodes_are_files,
                             uint64_t parent_inode_id,
                             const std::vector<uint64_t>& node_inode_ids,
                             std::ofstream* dentry_out,
                             std::string* error) {
    if (!dentry_out || parent_index >= plan.nodes.size() || node_inode_ids.size() != plan.nodes.size()) {
        if (error) {
            *error = "invalid path_list dentry emit state";
        }
        return false;
    }
    const PathListNode& parent = plan.nodes[parent_index];
    for (size_t child_index : parent.children) {
        if (child_index >= node_inode_ids.size() || node_inode_ids[child_index] == 0) {
            if (error) {
                *error = "path_list child inode assignment is incomplete";
            }
            return false;
        }
        const PathListNode& child = plan.nodes[child_index];
        const bool child_is_dir = NodeIsDirectory(child, path_list_leaf_nodes_are_files);
        if (!WriteDentryRecord(dentry_out,
                               parent_inode_id,
                               child.name,
                               node_inode_ids[child_index],
                               child_is_dir ? zb::rpc::INODE_DIR : zb::rpc::INODE_FILE,
                               error)) {
            return false;
        }
    }
    for (size_t child_index : parent.children) {
        if (!NodeIsDirectory(plan.nodes[child_index], path_list_leaf_nodes_are_files)) {
            continue;
        }
        if (!EmitPathListDentryGroup(plan,
                                     child_index,
                                     path_list_leaf_nodes_are_files,
                                     node_inode_ids[child_index],
                                     node_inode_ids,
                                     dentry_out,
                                     error)) {
            return false;
        }
    }
    return true;
}

} // namespace

bool MasstreeBulkMetaGenerator::SummarizePathList(const Request& request,
                                                  PathListSummary* summary,
                                                  std::string* error) const {
    if (summary) {
        *summary = PathListSummary();
    }
    PathListPlan plan;
    if (!BuildPathListPlan(request, &plan, error)) {
        return false;
    }
    if (summary) {
        summary->actual_file_count = plan.actual_file_count;
        summary->inode_count = plan.actual_inode_count;
        summary->dentry_count = plan.actual_dentry_count;
        summary->base_file_count = plan.base_file_count;
        summary->base_dir_count = plan.base_dir_count;
        summary->dir_count = plan.dir_count;
        summary->repeat_count = plan.repeat_count;
        summary->base_max_depth = plan.base_max_depth;
        summary->max_depth = plan.max_depth;
        summary->fingerprint = plan.fingerprint;
        summary->repeat_dir_prefix = plan.repeat_dir_prefix;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeBulkMetaGenerator::Generate(const Request& request,
                                         Result* result,
                                         std::string* error) const {
    if (result) {
        *result = Result();
    }
    const std::string source_mode = NormalizeSourceMode(request.source_mode);
    if (request.output_root.empty() || request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.inode_start == 0 || request.file_count == 0) {
        if (error) {
            *error = "invalid masstree bulk meta generator request";
        }
        return false;
    }
    if (!IsPathListMode(source_mode) &&
        (request.max_files_per_leaf_dir == 0 || request.max_subdirs_per_dir == 0)) {
        if (error) {
            *error = "invalid masstree bulk meta generator request";
        }
        return false;
    }

    std::string normalized_path_prefix;
    if (!NormalizePath(request.path_prefix, &normalized_path_prefix)) {
        if (error) {
            *error = "invalid masstree bulk meta generator path_prefix";
        }
        return false;
    }

    const uint64_t now_sec = NowSeconds();
    const MasstreeOpticalProfile optical_profile = MasstreeOpticalProfile::Fixed();
    const uint64_t namespace_seed = HashBytes(request.namespace_id);
    const uint64_t generation_seed = HashBytes(request.generation_id);

    const fs::path staging_dir = fs::path(request.output_root) /
                                 request.namespace_id /
                                 (request.generation_id + ".staging");
    std::error_code ec;
    if (fs::exists(staging_dir, ec)) {
        if (error) {
            *error = "masstree bulk meta generator staging dir already exists: " + staging_dir.string();
        }
        return false;
    }
    if (!fs::create_directories(staging_dir, ec) || ec) {
        if (error) {
            *error = "failed to create masstree bulk meta generator staging dir: " + staging_dir.string();
        }
        return false;
    }

    const fs::path inode_records_path = staging_dir / "inode.records";
    const fs::path dentry_records_path = staging_dir / "dentry.records";
    const fs::path manifest_path = staging_dir / "manifest.txt";
    const fs::path verify_manifest_path = staging_dir / "verify_manifest.txt";
    const fs::path structure_stats_path = staging_dir / "structure_stats.txt";

    std::ofstream inode_out;
    std::ofstream dentry_out;
    std::vector<char> inode_buffer(kMasstreeIoBufferBytes);
    std::vector<char> dentry_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&inode_out, &inode_buffer);
    ConfigureStreamBuffer(&dentry_out, &dentry_buffer);
    inode_out.open(inode_records_path, std::ios::binary | std::ios::trunc);
    dentry_out.open(dentry_records_path, std::ios::binary | std::ios::trunc);
    if (!inode_out || !dentry_out) {
        if (error) {
            *error = "failed to open masstree bulk meta generator record outputs";
        }
        return false;
    }

    uint64_t root_inode_id = request.inode_start;
    uint64_t inode_min = request.inode_start;
    uint64_t inode_max = request.inode_start;
    uint64_t inode_count = 0;
    uint64_t dentry_count = 0;
    uint64_t target_file_count = request.file_count;
    uint64_t actual_file_count = 0;
    uint64_t base_file_count = 0;
    uint64_t base_dir_count = 0;
    uint64_t dir_count = 0;
    uint64_t repeat_count = 1;
    uint64_t level1_dir_count = 0;
    uint64_t leaf_dir_count = 0;
    uint64_t base_max_depth = 0;
    uint64_t max_depth = 0;
    std::string repeat_dir_prefix;
    std::string path_list_fingerprint;
    std::vector<uint64_t> base_dir_count_by_depth;
    std::vector<uint64_t> base_file_count_by_depth;
    std::vector<uint64_t> dir_count_by_depth;
    std::vector<uint64_t> file_count_by_depth;
    MasstreeDecimalAccumulator total_file_bytes_accumulator;

    if (IsPathListMode(source_mode)) {
        PathListPlan plan;
        if (!BuildPathListPlan(request, &plan, error)) {
            return false;
        }

        actual_file_count = plan.actual_file_count;
        inode_count = plan.actual_inode_count;
        dentry_count = plan.actual_dentry_count;
        inode_max = request.inode_start + inode_count - 1ULL;
        base_file_count = plan.base_file_count;
        base_dir_count = plan.base_dir_count;
        dir_count = plan.dir_count;
        repeat_count = plan.repeat_count;
        repeat_dir_prefix = plan.repeat_dir_prefix;
        path_list_fingerprint = plan.fingerprint;
        level1_dir_count = plan.repeat_count;
        leaf_dir_count = plan.repeat_count * plan.base_dir_count;
        base_max_depth = plan.base_max_depth;
        max_depth = plan.max_depth;
        base_dir_count_by_depth = plan.base_dir_count_by_depth;
        base_file_count_by_depth = plan.base_file_count_by_depth;
        dir_count_by_depth = plan.dir_count_by_depth;
        file_count_by_depth = plan.file_count_by_depth;

        const uint32_t root_nlink = static_cast<uint32_t>(2U + plan.repeat_count);
        if (!WriteInodeRecord(&inode_out,
                              root_inode_id,
                              BuildDirectoryAttr(root_inode_id, "/", root_nlink, now_sec),
                              0,
                              error)) {
            return false;
        }

        const uint64_t per_repeat_inode_count = static_cast<uint64_t>(plan.nodes.size());
        for (uint64_t repeat_index = 0; repeat_index < plan.repeat_count; ++repeat_index) {
            const uint64_t wrapper_inode = request.inode_start + 1ULL + repeat_index * per_repeat_inode_count;
            const std::string wrapper_name = RepeatDirName(plan.repeat_dir_prefix,
                                                           repeat_index,
                                                           static_cast<size_t>(plan.wrapper_name_width));
            if (!WriteDentryRecord(&dentry_out,
                                   root_inode_id,
                                   wrapper_name,
                                   wrapper_inode,
                                   zb::rpc::INODE_DIR,
                                   error)) {
                return false;
            }
        }

        uint64_t next_inode = request.inode_start + 1ULL;
        uint64_t file_ordinal = 0;
        for (uint64_t repeat_index = 0; repeat_index < plan.repeat_count; ++repeat_index) {
            const uint64_t wrapper_inode = next_inode++;
            const std::string wrapper_name = RepeatDirName(plan.repeat_dir_prefix,
                                                           repeat_index,
                                                           static_cast<size_t>(plan.wrapper_name_width));
            const uint32_t wrapper_nlink = static_cast<uint32_t>(2U + plan.nodes[0].children.size());
            if (!WriteInodeRecord(&inode_out,
                                  wrapper_inode,
                                  BuildDirectoryAttr(wrapper_inode, wrapper_name, wrapper_nlink, now_sec),
                                  root_inode_id,
                                  error)) {
                return false;
            }
            std::vector<uint64_t> node_inode_ids(plan.nodes.size(), 0);
            for (size_t child_index : plan.nodes[0].children) {
                if (!AssignPathListSubtreeInodes(plan,
                                                 child_index,
                                                 request.path_list_leaf_nodes_are_files,
                                                 wrapper_inode,
                                                 now_sec,
                                                 namespace_seed,
                                                 generation_seed,
                                                 optical_profile,
                                                 &next_inode,
                                                 &file_ordinal,
                                                 &total_file_bytes_accumulator,
                                                 &node_inode_ids,
                                                 &inode_out,
                                                 error)) {
                    return false;
                }
            }
            if (!EmitPathListDentryGroup(plan,
                                         0,
                                         request.path_list_leaf_nodes_are_files,
                                         wrapper_inode,
                                         node_inode_ids,
                                         &dentry_out,
                                         error)) {
                return false;
            }
        }
        if (next_inode != request.inode_start + inode_count) {
            if (error) {
                *error = "path_list inode generation count mismatch";
            }
            return false;
        }
        if (file_ordinal != actual_file_count) {
            if (error) {
                *error = "path_list file generation count mismatch";
            }
            return false;
        }
    } else {
        const uint64_t leaf_dir_count_synthetic = CeilDiv(request.file_count, request.max_files_per_leaf_dir);
        const uint64_t level1_dir_count_synthetic =
            std::max<uint64_t>(1, CeilDiv(leaf_dir_count_synthetic, request.max_subdirs_per_dir));
        const uint64_t level1_inode_count = level1_dir_count_synthetic;
        const uint64_t leaf_inode_count = leaf_dir_count_synthetic;
        actual_file_count = request.file_count;
        base_file_count = request.file_count;
        base_dir_count = 0;
        dir_count = 1ULL + level1_inode_count + leaf_inode_count;
        inode_count = 1 + level1_inode_count + leaf_inode_count + request.file_count;
        dentry_count = level1_dir_count_synthetic + leaf_dir_count_synthetic + request.file_count;
        inode_max = request.inode_start + inode_count - 1ULL;
        level1_dir_count = level1_dir_count_synthetic;
        leaf_dir_count = leaf_dir_count_synthetic;
        base_max_depth = 0;
        max_depth = 3;
        dir_count_by_depth.assign(static_cast<size_t>(max_depth + 1ULL), 0);
        file_count_by_depth.assign(static_cast<size_t>(max_depth + 1ULL), 0);
        dir_count_by_depth[0] = 1;
        dir_count_by_depth[1] = level1_dir_count_synthetic;
        dir_count_by_depth[2] = leaf_dir_count_synthetic;
        file_count_by_depth[3] = request.file_count;

        uint64_t next_inode = request.inode_start;
        root_inode_id = next_inode++;
        const uint64_t level1_inode_start = next_inode;
        next_inode += level1_inode_count;
        const uint64_t leaf_inode_start = next_inode;
        next_inode += leaf_inode_count;
        const uint64_t file_inode_start = next_inode;

        if (!WriteInodeRecord(&inode_out,
                              root_inode_id,
                              BuildDirectoryAttr(root_inode_id,
                                                 "/",
                                                 static_cast<uint32_t>(2 + level1_dir_count_synthetic),
                                                 now_sec),
                              0,
                              error)) {
            return false;
        }

        for (uint64_t level1 = 0; level1 < level1_dir_count_synthetic; ++level1) {
            const uint64_t level1_inode = level1_inode_start + level1;
            const std::string level1_name = DirName('d', level1);
            const uint64_t leaves_in_this_level1 =
                std::min<uint64_t>(request.max_subdirs_per_dir,
                                   leaf_dir_count_synthetic -
                                       level1 * static_cast<uint64_t>(request.max_subdirs_per_dir));
            if (!WriteInodeRecord(&inode_out,
                                  level1_inode,
                                  BuildDirectoryAttr(level1_inode,
                                                     level1_name,
                                                     static_cast<uint32_t>(2 + leaves_in_this_level1),
                                                     now_sec),
                                  root_inode_id,
                                  error) ||
                !WriteDentryRecord(&dentry_out,
                                   root_inode_id,
                                   level1_name,
                                   level1_inode,
                                   zb::rpc::INODE_DIR,
                                   error)) {
                return false;
            }
        }

        for (uint64_t leaf = 0; leaf < leaf_dir_count_synthetic; ++leaf) {
            const uint64_t level1 = leaf / request.max_subdirs_per_dir;
            const uint64_t slot = leaf % request.max_subdirs_per_dir;
            const uint64_t leaf_inode = leaf_inode_start + leaf;
            const uint64_t files_in_leaf =
                std::min<uint64_t>(request.max_files_per_leaf_dir,
                                   request.file_count -
                                       leaf * static_cast<uint64_t>(request.max_files_per_leaf_dir));
            const std::string leaf_name = DirName('s', slot);
            if (!WriteInodeRecord(&inode_out,
                                  leaf_inode,
                                  BuildDirectoryAttr(leaf_inode,
                                                     leaf_name,
                                                     static_cast<uint32_t>(2 + files_in_leaf),
                                                     now_sec),
                                  level1_inode_start + level1,
                                  error) ||
                !WriteDentryRecord(&dentry_out,
                                   level1_inode_start + level1,
                                   leaf_name,
                                   leaf_inode,
                                   zb::rpc::INODE_DIR,
                                   error)) {
                return false;
            }
        }

        for (uint64_t file_index = 0; file_index < request.file_count; ++file_index) {
            const uint64_t leaf = file_index / request.max_files_per_leaf_dir;
            const uint64_t file_inode = file_inode_start + file_index;
            const std::string file_name = FileName(file_index);
            const uint64_t file_size_bytes =
                GenerateFileSizeBytes(namespace_seed, generation_seed, file_index, optical_profile);
            total_file_bytes_accumulator.Add(file_size_bytes);
            if (!WriteInodeRecord(&inode_out,
                                  file_inode,
                                  BuildFileAttr(file_inode, file_name, file_size_bytes, now_sec),
                                  leaf_inode_start + leaf,
                                  error) ||
                !WriteDentryRecord(&dentry_out,
                                   leaf_inode_start + leaf,
                                   file_name,
                                   file_inode,
                                   zb::rpc::INODE_FILE,
                                   error)) {
                return false;
            }
        }
    }

    inode_out.flush();
    dentry_out.flush();
    if (!inode_out.good() || !dentry_out.good()) {
        if (error) {
            *error = "failed to flush masstree bulk meta generator outputs";
        }
        return false;
    }
    inode_out.close();
    dentry_out.close();
    if (inode_out.fail() || dentry_out.fail()) {
        if (error) {
            *error = "failed to close masstree bulk meta generator outputs";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    manifest.namespace_id = request.namespace_id;
    manifest.path_prefix = normalized_path_prefix;
    manifest.generation_id = request.generation_id;
    manifest.source_mode = source_mode;
    manifest.template_id = request.namespace_id;
    manifest.path_list_file = request.path_list_file;
    manifest.path_list_leaf_nodes_are_files = request.path_list_leaf_nodes_are_files;
    manifest.path_list_fingerprint = path_list_fingerprint;
    manifest.repeat_dir_prefix = repeat_dir_prefix;
    manifest.manifest_path = manifest_path.string();
    manifest.inode_records_path = inode_records_path.string();
    manifest.dentry_records_path = dentry_records_path.string();
    manifest.inode_pages_path = (staging_dir / "inode_pages.seg").string();
    manifest.inode_sparse_index_path = (staging_dir / "inode_sparse.idx").string();
    manifest.dentry_pages_path = (staging_dir / "dentry_pages.seg").string();
    manifest.dentry_sparse_index_path = (staging_dir / "dentry_sparse.idx").string();
    manifest.verify_manifest_path = verify_manifest_path.string();
    manifest.structure_stats_path = structure_stats_path.string();
    manifest.page_size_bytes = kMasstreeDefaultPageSizeBytes;
    manifest.root_inode_id = root_inode_id;
    manifest.inode_min = inode_min;
    manifest.inode_max = inode_max;
    manifest.inode_count = inode_count;
    manifest.dentry_count = dentry_count;
    manifest.target_file_count = target_file_count;
    manifest.file_count = actual_file_count;
    manifest.template_base_file_count = base_file_count;
    manifest.template_base_dir_count = base_dir_count;
    manifest.dir_count = dir_count;
    manifest.template_repeat_count = repeat_count;
    manifest.level1_dir_count = level1_dir_count;
    manifest.leaf_dir_count = leaf_dir_count;
    manifest.template_base_max_depth = base_max_depth;
    manifest.max_depth = max_depth;
    manifest.max_files_per_leaf_dir = request.max_files_per_leaf_dir;
    manifest.max_subdirs_per_dir = request.max_subdirs_per_dir;
    manifest.min_file_size_bytes = optical_profile.min_file_size_bytes;
    manifest.max_file_size_bytes = optical_profile.max_file_size_bytes;
    manifest.total_file_bytes = total_file_bytes_accumulator.ToString();
    manifest.avg_file_size_bytes = actual_file_count == 0 ? 0 :
                                   total_file_bytes_accumulator.DivideBy(actual_file_count);

    if (!manifest.SaveToFile(manifest_path.string(), error) ||
        !WriteVerifyManifest(verify_manifest_path, manifest, error) ||
        !WriteStructureStats(structure_stats_path,
                             manifest,
                             dir_count_by_depth,
                             file_count_by_depth,
                             base_dir_count_by_depth,
                             base_file_count_by_depth,
                             error)) {
        return false;
    }

    if (result) {
        result->staging_dir = staging_dir.string();
        result->manifest_path = manifest_path.string();
        result->inode_records_path = inode_records_path.string();
        result->dentry_records_path = dentry_records_path.string();
        result->verify_manifest_path = verify_manifest_path.string();
        result->structure_stats_path = structure_stats_path.string();
        result->source_mode = source_mode;
        result->root_inode_id = root_inode_id;
        result->inode_min = inode_min;
        result->inode_max = inode_max;
        result->target_file_count = target_file_count;
        result->inode_count = inode_count;
        result->dentry_count = dentry_count;
        result->base_file_count = base_file_count;
        result->base_dir_count = base_dir_count;
        result->dir_count = dir_count;
        result->repeat_count = repeat_count;
        result->level1_dir_count = level1_dir_count;
        result->leaf_dir_count = leaf_dir_count;
        result->base_max_depth = base_max_depth;
        result->max_depth = max_depth;
        result->avg_file_size_bytes = manifest.avg_file_size_bytes;
        result->total_file_bytes = manifest.total_file_bytes;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
