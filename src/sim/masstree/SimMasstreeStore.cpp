#include "sim/masstree/SimMasstreeStore.h"

#include "sim/masstree/SimMasstreeManifest.h"
#include "sim/masstree/SimMasstreeOpticalAllocator.h"
#include "sim/masstree/SimMasstreePageLayout.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <iomanip>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <thread>

namespace zb::sim::masstree {

namespace {

std::string NormalizePath(std::string path) {
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    std::string collapsed;
    collapsed.reserve(path.size());
    bool prev_slash = false;
    for (char ch : path) {
        if (ch == '/') {
            if (!prev_slash) {
                collapsed.push_back(ch);
            }
            prev_slash = true;
        } else {
            collapsed.push_back(ch);
            prev_slash = false;
        }
    }
    path = std::move(collapsed);
    while (path.size() > 1 && path.back() == '/') {
        path.pop_back();
    }
    return path;
}

std::string TrimCopy(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::stringstream ss(NormalizePath(path));
    std::string part;
    while (std::getline(ss, part, '/')) {
        if (!part.empty()) {
            parts.push_back(part);
        }
    }
    return parts;
}

std::string JoinPath(const std::vector<std::string>& parts, size_t count) {
    std::string out = "/";
    for (size_t i = 0; i < count; ++i) {
        if (i != 0) {
            out += "/";
        }
        out += parts[i];
    }
    return out;
}

bool LooksLikeDirectory(const std::string& raw, bool leaf_nodes_are_files) {
    if (raw.empty()) {
        return true;
    }
    if (raw.back() == '/' || raw.back() == '\\') {
        return true;
    }
    if (leaf_nodes_are_files) {
        return false;
    }
    const auto parts = SplitPath(raw);
    if (parts.empty()) {
        return true;
    }
    return parts.back().find('.') == std::string::npos;
}

std::string NormalizeRepeatDirPrefix(std::string prefix) {
    prefix = TrimCopy(std::move(prefix));
    if (prefix.empty()) {
        prefix = "copy";
    }
    std::replace(prefix.begin(), prefix.end(), '\\', '_');
    std::replace(prefix.begin(), prefix.end(), '/', '_');
    if (prefix == "." || prefix == "..") {
        prefix = "copy";
    }
    return prefix;
}

std::string RepeatWrapperName(const std::string& prefix, uint64_t index) {
    std::ostringstream oss;
    oss << prefix << std::setw(6) << std::setfill('0') << index;
    return oss.str();
}

std::string JoinRepeatedPath(const std::string& wrapper, const std::string& base_path) {
    const std::string normalized = NormalizePath(base_path);
    if (normalized == "/") {
        return "/" + wrapper;
    }
    return "/" + wrapper + normalized;
}

uint64_t Fnv1aAppend(uint64_t hash, const std::string& value) {
    for (unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string Hex64(uint64_t value) {
    std::ostringstream oss;
    oss << std::hex << std::setw(16) << std::setfill('0') << value;
    return oss.str();
}

std::string FingerprintPaths(const std::vector<std::string>& paths) {
    uint64_t hash = 14695981039346656037ULL;
    for (const auto& path : paths) {
        hash = Fnv1aAppend(hash, path);
        hash = Fnv1aAppend(hash, "\n");
    }
    return Hex64(hash);
}

uint64_t PathDepth(const std::string& path) {
    return SplitPath(path).size();
}

uint64_t NowSeconds() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

SimInodeRecord MakeInode(uint64_t inode_id,
                         uint64_t parent_inode,
                         const std::string& name,
                         SimInodeType type,
                         uint64_t size_bytes) {
    SimInodeRecord record;
    record.inode_id = inode_id;
    record.parent_inode_id = parent_inode;
    record.file_name = name;
    record.type = type;
    record.storage_tier = type == SimInodeType::File ? SimStorageTier::Optical : SimStorageTier::None;
    record.size_bytes = size_bytes;
    record.atime = record.mtime = record.ctime = NowSeconds();
    record.mode = type == SimInodeType::Dir ? 0755 : 0644;
    record.nlink = type == SimInodeType::Dir ? 2 : 1;
    record.optical_length = size_bytes;
    return record;
}

void ApplyOpticalLocation(const SimOpticalLocation& location, SimInodeRecord* record) {
    if (!record) {
        return;
    }
    record->optical_node_id = location.node_index;
    record->optical_disk_id = location.disk_index;
    record->optical_image_id = location.image_index;
    record->optical_offset = location.offset_bytes;
    record->optical_length = location.length_bytes;
}

SimOpticalLocation LocationFromRecord(const SimInodeRecord& record) {
    SimOpticalLocation location;
    location.node_index = static_cast<uint32_t>(record.optical_node_id);
    location.disk_index = static_cast<uint32_t>(record.optical_disk_id);
    location.image_index = record.optical_image_id;
    location.offset_bytes = record.optical_offset;
    location.length_bytes = record.optical_length;
    return location;
}

} // namespace

bool SimMasstreeStore::Init(const std::filesystem::path& root, std::string* error) {
    root_ = root;
    masstree_root_ = root_ / ".sim" / "masstree";
    catalog_path_ = masstree_root_ / "catalog.txt";
    next_inode_path_ = masstree_root_ / "next_inode.txt";
    optical_cursor_path_ = masstree_root_ / "optical_cursor.txt";
    std::error_code ec;
    std::filesystem::create_directories(masstree_root_, ec);
    if (ec) {
        if (error) {
            *error = "failed to create masstree root: " + ec.message();
        }
        return false;
    }
    if (!index_.Init(error)) {
        return false;
    }
    return LoadCatalog(error);
}

bool SimMasstreeStore::GenerateTemplate(const TemplateRequest& request, TemplateResult* result, std::string* error) {
    if (request.template_id.empty()) {
        if (error) {
            *error = "template_id is required";
        }
        return false;
    }
    std::vector<std::string> base_paths;
    if (!request.path_list_file.empty()) {
        std::ifstream in(request.path_list_file);
        if (!in) {
            if (error) {
                *error = "failed to open path_list_file: " + request.path_list_file;
            }
            return false;
        }
        std::string line;
        while (std::getline(in, line)) {
            line = TrimCopy(line);
            if (line.empty() || line[0] == '#') {
                continue;
            }
            if (!LooksLikeDirectory(line, request.leaf_nodes_are_files)) {
                base_paths.push_back(NormalizePath(line));
            }
        }
    } else {
        const uint64_t count = std::max<uint64_t>(1, request.target_file_count);
        for (uint64_t i = 0; i < count; ++i) {
            base_paths.push_back("/synthetic/file_" + std::to_string(i) + ".bin");
        }
    }
    std::sort(base_paths.begin(), base_paths.end());
    base_paths.erase(std::unique(base_paths.begin(), base_paths.end()), base_paths.end());
    if (base_paths.empty()) {
        if (error) {
            *error = "template source has no file paths";
        }
        return false;
    }
    const uint64_t target_file_count =
        request.target_file_count == 0 ? static_cast<uint64_t>(base_paths.size()) : request.target_file_count;
    const std::string repeat_prefix = NormalizeRepeatDirPrefix(request.repeat_dir_prefix);
    const uint64_t repeat_count =
        request.path_list_file.empty() || target_file_count <= base_paths.size()
            ? 1
            : ((target_file_count + base_paths.size() - 1ULL) / base_paths.size());
    std::set<std::string> base_dirs;
    for (const auto& base_path : base_paths) {
        const auto parts = SplitPath(base_path);
        for (size_t i = 1; i < parts.size(); ++i) {
            base_dirs.insert(JoinPath(parts, i));
        }
    }

    std::vector<std::string> input_paths;
    input_paths.reserve(static_cast<size_t>(std::min<uint64_t>(target_file_count, 1000000ULL)));
    for (uint64_t i = 0; i < target_file_count; ++i) {
        const std::string& base = base_paths[static_cast<size_t>(i % base_paths.size())];
        if (repeat_count == 1) {
            input_paths.push_back(base);
        } else {
            input_paths.push_back(JoinRepeatedPath(RepeatWrapperName(repeat_prefix, i / base_paths.size()), base));
        }
    }

    const std::filesystem::path dir = masstree_root_ / "templates" / request.template_id / "template";
    std::error_code ec;
    if (std::filesystem::exists(dir, ec)) {
        std::filesystem::remove_all(dir, ec);
    }
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to create template directory: " + ec.message();
        }
        return false;
    }

    std::map<std::string, uint64_t> inode_by_path;
    std::vector<SimInodeRecord> inodes;
    std::vector<SimDentryRecord> dentries;
    std::vector<std::string> samples;
    std::set<std::string> level1_dirs;
    std::set<std::string> leaf_dirs;
    uint64_t max_depth = 0;
    uint64_t next_inode = 1;
    inode_by_path["/"] = next_inode;
    inodes.push_back(MakeInode(next_inode, 0, "", SimInodeType::Dir, 0));
    ++next_inode;

    for (const auto& file_path : input_paths) {
        const auto parts = SplitPath(file_path);
        max_depth = std::max<uint64_t>(max_depth, parts.size());
        uint64_t parent = 1;
        for (size_t i = 0; i < parts.size(); ++i) {
            const bool is_file = i + 1 == parts.size();
            const std::string current = JoinPath(parts, i + 1);
            auto it = inode_by_path.find(current);
            if (it != inode_by_path.end()) {
                parent = it->second;
                continue;
            }
            const uint64_t inode = next_inode++;
            inode_by_path[current] = inode;
            const SimInodeType type = is_file ? SimInodeType::File : SimInodeType::Dir;
            const uint64_t size = is_file ? request.default_file_size_bytes : 0;
            inodes.push_back(MakeInode(inode, parent, parts[i], type, size));
            dentries.push_back(SimDentryRecord{parent, parts[i], inode, type});
            parent = inode;
        }
        if (!parts.empty()) {
            level1_dirs.insert(parts.front());
        }
        if (parts.size() > 1) {
            leaf_dirs.insert(JoinPath(parts, parts.size() - 1));
        }
        if (samples.size() < 10000U) {
            samples.push_back(file_path);
        }
    }

    std::sort(inodes.begin(), inodes.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.inode_id < rhs.inode_id;
    });
    std::sort(dentries.begin(), dentries.end(), [](const auto& lhs, const auto& rhs) {
        return DentryKeyLess(lhs.parent_inode_id, lhs.name, rhs.parent_inode_id, rhs.name);
    });

    uint64_t inode_pages_bytes = 0;
    uint64_t dentry_pages_bytes = 0;
    if (!WriteInodePages(dir / "inode_pages.bin", dir / "inode_sparse.txt", inodes, &inode_pages_bytes, error) ||
        !WriteDentryPages(dir / "dentry_pages.bin", dir / "dentry_sparse.txt", dentries, &dentry_pages_bytes, error)) {
        return false;
    }
    const uint64_t inode_page_count = (inodes.size() + kSimMasstreePageTargetEntries - 1U) / kSimMasstreePageTargetEntries;
    const uint64_t dentry_page_count = (dentries.size() + kSimMasstreePageTargetEntries - 1U) / kSimMasstreePageTargetEntries;
    std::ofstream sample_out(dir / "samples.txt");
    for (const auto& sample : samples) {
        sample_out << sample << "\n";
    }
    std::ofstream stats_out(dir / "structure_stats.txt");
    stats_out << "base_file_count=" << base_paths.size() << "\n";
    stats_out << "base_dir_count=" << (base_dirs.size() + 1ULL) << "\n";
    stats_out << "target_file_count=" << target_file_count << "\n";
    stats_out << "repeat_count=" << repeat_count << "\n";
    stats_out << "inode_count=" << inodes.size() << "\n";
    stats_out << "dentry_count=" << dentries.size() << "\n";
    stats_out << "max_depth=" << max_depth << "\n";

    SimMasstreeManifest manifest;
    manifest.kind = "template";
    manifest.template_id = request.template_id;
    manifest.namespace_id = request.template_id;
    manifest.generation_id = "template";
    manifest.source_mode = request.path_list_file.empty() ? "synthetic" : "path_list";
    manifest.path_list_file = request.path_list_file;
    manifest.path_list_fingerprint = FingerprintPaths(base_paths);
    manifest.repeat_dir_prefix = repeat_prefix;
    manifest.path_list_leaf_nodes_are_files = request.leaf_nodes_are_files;
    manifest.path_prefix = "/";
    manifest.manifest_path = dir / "manifest.txt";
    manifest.inode_pages_path = dir / "inode_pages.bin";
    manifest.dentry_pages_path = dir / "dentry_pages.bin";
    manifest.inode_sparse_path = dir / "inode_sparse.txt";
    manifest.dentry_sparse_path = dir / "dentry_sparse.txt";
    manifest.samples_path = dir / "samples.txt";
    manifest.structure_stats_path = dir / "structure_stats.txt";
    manifest.root_inode_id = 1;
    manifest.inode_min = 1;
    manifest.inode_max = next_inode - 1;
    manifest.inode_count = inodes.size();
    manifest.dentry_count = dentries.size();
    manifest.inode_page_count = inode_page_count;
    manifest.dentry_page_count = dentry_page_count;
    manifest.target_file_count = target_file_count;
    manifest.file_count = target_file_count;
    manifest.template_base_file_count = base_paths.size();
    manifest.template_base_dir_count = base_dirs.size() + 1ULL;
    manifest.dir_count = inodes.size() - target_file_count;
    manifest.template_repeat_count = repeat_count;
    manifest.level1_dir_count = level1_dirs.size();
    manifest.leaf_dir_count = leaf_dirs.size();
    manifest.template_base_max_depth = 0;
    for (const auto& path : base_paths) {
        manifest.template_base_max_depth = std::max<uint64_t>(manifest.template_base_max_depth, PathDepth(path));
    }
    manifest.max_depth = max_depth;
    manifest.max_files_per_leaf_dir = request.max_files_per_leaf_dir;
    manifest.max_subdirs_per_dir = request.max_subdirs_per_dir;
    manifest.min_file_size_bytes = request.default_file_size_bytes;
    manifest.max_file_size_bytes = request.default_file_size_bytes;
    manifest.avg_file_size_bytes = request.default_file_size_bytes;
    manifest.total_file_bytes = target_file_count * request.default_file_size_bytes;
    manifest.inode_pages_bytes = inode_pages_bytes;
    manifest.dentry_pages_bytes = dentry_pages_bytes;
    if (!SaveManifest(manifest, error)) {
        return false;
    }
    if (result) {
        result->manifest = manifest;
    }
    return true;
}

bool SimMasstreeStore::ImportNamespace(const NamespaceImportRequest& request,
                                       NamespaceImportResult* result,
                                       std::string* error) {
    if (request.template_id.empty() || request.namespace_id.empty() || request.generation_id.empty()) {
        if (error) {
            *error = "template_id, namespace_id and generation_id are required";
        }
        return false;
    }
    SimMasstreeManifest source;
    const auto source_manifest = masstree_root_ / "templates" / request.template_id / "template" / "manifest.txt";
    if (!LoadManifest(source_manifest, &source, error)) {
        return false;
    }

    const auto dir = masstree_root_ / "namespaces" / request.namespace_id / request.generation_id;
    std::error_code ec;
    if (std::filesystem::exists(dir, ec)) {
        std::filesystem::remove_all(dir, ec);
    }
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        if (error) {
            *error = "failed to create namespace directory: " + ec.message();
        }
        return false;
    }

    std::vector<SimInodeRecord> inodes;
    std::vector<SimDentryRecord> dentries;
    if (!LoadAllInodes(source, &inodes, error) || !LoadAllDentries(source, &dentries, error)) {
        return false;
    }
    uint64_t inode_start = 0;
    if (!ReserveInodeRange(source.inode_count, request.inode_start, &inode_start, error)) {
        return false;
    }
    if (inode_start < source.inode_min) {
        if (error) {
            *error = "inode_start is smaller than template inode_min";
        }
        return false;
    }
    const uint64_t inode_offset = inode_start - source.inode_min;

    SimOpticalCursor start_cursor;
    if (!LoadOpticalCursor(&start_cursor, error)) {
        return false;
    }
    SimMasstreeOpticalProfile profile;
    SimMasstreeOpticalAllocator allocator(start_cursor);
    uint64_t start_global_image_id = profile.GlobalImageId(start_cursor);
    uint64_t end_global_image_id = start_global_image_id;
    uint64_t file_count = 0;
    uint64_t total_file_bytes = 0;
    std::ofstream optical_out(dir / "optical_layout.txt", std::ios::trunc);
    if (!optical_out) {
        if (error) {
            *error = "failed to write optical layout";
        }
        return false;
    }
    for (auto& inode : inodes) {
        inode.inode_id += inode_offset;
        if (inode.parent_inode_id != 0) {
            inode.parent_inode_id += inode_offset;
        }
        if (inode.type == SimInodeType::File) {
            SimOpticalLocation location;
            if (!allocator.Allocate(inode.size_bytes, &location, error)) {
                return false;
            }
            ApplyOpticalLocation(location, &inode);
            end_global_image_id = allocator.current_global_image_id();
            ++file_count;
            total_file_bytes += inode.size_bytes;
            optical_out << inode.inode_id << "\t"
                        << location.node_index << "\t"
                        << location.disk_index << "\t"
                        << location.image_index << "\t"
                        << location.offset_bytes << "\t"
                        << location.length_bytes << "\n";
        }
    }
    for (auto& dentry : dentries) {
        dentry.parent_inode_id += inode_offset;
        dentry.child_inode_id += inode_offset;
    }
    std::sort(inodes.begin(), inodes.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.inode_id < rhs.inode_id;
    });
    std::sort(dentries.begin(), dentries.end(), [](const auto& lhs, const auto& rhs) {
        return DentryKeyLess(lhs.parent_inode_id, lhs.name, rhs.parent_inode_id, rhs.name);
    });

    uint64_t inode_pages_bytes = 0;
    uint64_t dentry_pages_bytes = 0;
    if (!WriteInodePages(dir / "inode_pages.bin", dir / "inode_sparse.txt", inodes, &inode_pages_bytes, error) ||
        !WriteDentryPages(dir / "dentry_pages.bin", dir / "dentry_sparse.txt", dentries, &dentry_pages_bytes, error)) {
        return false;
    }
    std::filesystem::copy_file(source.samples_path,
                               dir / "samples.txt",
                               std::filesystem::copy_options::overwrite_existing,
                               ec);
    if (ec) {
        if (error) {
            *error = "failed to copy query samples: " + ec.message();
        }
        return false;
    }
    std::ofstream alloc_out(dir / "allocation_summary.txt", std::ios::trunc);
    alloc_out << "file_count=" << file_count << "\n";
    alloc_out << "total_file_bytes=" << total_file_bytes << "\n";
    alloc_out << "start_global_image_id=" << start_global_image_id << "\n";
    alloc_out << "end_global_image_id=" << end_global_image_id << "\n";

    SimMasstreeManifest manifest = source;
    manifest.kind = "namespace";
    manifest.namespace_id = request.namespace_id;
    manifest.generation_id = request.generation_id;
    manifest.path_prefix = NormalizePath(request.path_prefix.empty() ? ("/masstree_demo/" + request.namespace_id) : request.path_prefix);
    manifest.manifest_path = dir / "manifest.txt";
    manifest.inode_pages_path = dir / "inode_pages.bin";
    manifest.dentry_pages_path = dir / "dentry_pages.bin";
    manifest.inode_sparse_path = dir / "inode_sparse.txt";
    manifest.dentry_sparse_path = dir / "dentry_sparse.txt";
    manifest.samples_path = dir / "samples.txt";
    manifest.optical_layout_path = dir / "optical_layout.txt";
    manifest.allocation_summary_path = dir / "allocation_summary.txt";
    manifest.root_inode_id = source.root_inode_id + inode_offset;
    manifest.inode_min = source.inode_min + inode_offset;
    manifest.inode_max = source.inode_max + inode_offset;
    manifest.inode_pages_bytes = inode_pages_bytes;
    manifest.dentry_pages_bytes = dentry_pages_bytes;
    manifest.inode_page_count = (inodes.size() + kSimMasstreePageTargetEntries - 1U) / kSimMasstreePageTargetEntries;
    manifest.dentry_page_count = (dentries.size() + kSimMasstreePageTargetEntries - 1U) / kSimMasstreePageTargetEntries;
    manifest.file_count = file_count;
    manifest.total_file_bytes = total_file_bytes;
    manifest.start_cursor = start_cursor;
    manifest.end_cursor = allocator.cursor();
    manifest.start_global_image_id = start_global_image_id;
    manifest.end_global_image_id = end_global_image_id;
    if (!SaveManifest(manifest, error)) {
        return false;
    }
    if (!SaveOpticalCursor(allocator.cursor(), error)) {
        return false;
    }
    namespaces_[request.namespace_id] = manifest;
    if (!LoadNamespaceIndex(request.namespace_id, manifest, error) || !SaveCatalog(error)) {
        return false;
    }
    if (result) {
        result->manifest = manifest;
    }
    return true;
}

bool SimMasstreeStore::LoadCatalog(std::string* error) {
    namespaces_.clear();
    index_.Clear();
    std::ifstream in(catalog_path_);
    if (!in) {
        return true;
    }
    std::string line;
    while (std::getline(in, line)) {
        const size_t tab = line.find('\t');
        if (tab == std::string::npos) {
            continue;
        }
        const std::string namespace_id = line.substr(0, tab);
        const std::string manifest_path = line.substr(tab + 1);
        SimMasstreeManifest manifest;
        if (LoadManifest(manifest_path, &manifest, error)) {
            namespaces_[namespace_id] = manifest;
            if (!LoadNamespaceIndex(namespace_id, manifest, error)) {
                return false;
            }
        }
    }
    return true;
}

bool SimMasstreeStore::SaveCatalog(std::string* error) const {
    std::ofstream out(catalog_path_);
    if (!out) {
        if (error) {
            *error = "failed to write masstree catalog: " + catalog_path_.string();
        }
        return false;
    }
    for (const auto& item : namespaces_) {
        out << item.first << "\t" << item.second.manifest_path.string() << "\n";
    }
    return true;
}

bool SimMasstreeStore::LoadAllInodes(const SimMasstreeManifest& manifest,
                                     std::vector<SimInodeRecord>* records,
                                     std::string* error) const {
    if (!records) {
        if (error) {
            *error = "inode records output is null";
        }
        return false;
    }
    std::vector<SimInodeSparseEntry> sparse;
    if (!LoadInodeSparse(manifest.inode_sparse_path, &sparse, error)) {
        return false;
    }
    records->clear();
    for (const auto& entry : sparse) {
        std::vector<SimInodeRecord> page;
        if (!ReadInodePageAt(manifest.inode_pages_path, entry.page_offset, &page, error)) {
            return false;
        }
        records->insert(records->end(), page.begin(), page.end());
    }
    return true;
}

bool SimMasstreeStore::LoadAllDentries(const SimMasstreeManifest& manifest,
                                       std::vector<SimDentryRecord>* records,
                                       std::string* error) const {
    if (!records) {
        if (error) {
            *error = "dentry records output is null";
        }
        return false;
    }
    std::vector<SimDentrySparseEntry> sparse;
    if (!LoadDentrySparse(manifest.dentry_sparse_path, &sparse, error)) {
        return false;
    }
    records->clear();
    for (const auto& entry : sparse) {
        std::vector<SimDentryRecord> page;
        if (!ReadDentryPageAt(manifest.dentry_pages_path, entry.page_offset, &page, error)) {
            return false;
        }
        records->insert(records->end(), page.begin(), page.end());
    }
    return true;
}

bool SimMasstreeStore::ReserveInodeRange(uint64_t inode_count,
                                         uint64_t requested_start,
                                         uint64_t* inode_start,
                                         std::string* error) {
    if (!inode_start || inode_count == 0) {
        if (error) {
            *error = "invalid inode range reservation args";
        }
        return false;
    }
    uint64_t next_inode = 2;
    std::ifstream in(next_inode_path_);
    if (in) {
        in >> next_inode;
    }
    *inode_start = requested_start == 0 ? next_inode : requested_start;
    const uint64_t new_next = std::max(next_inode, *inode_start + inode_count);
    std::ofstream out(next_inode_path_, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to write next inode file: " + next_inode_path_.string();
        }
        return false;
    }
    out << new_next << "\n";
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeStore::LoadOpticalCursor(SimOpticalCursor* cursor, std::string* error) const {
    if (!cursor) {
        if (error) {
            *error = "optical cursor output is null";
        }
        return false;
    }
    *cursor = {};
    std::ifstream in(optical_cursor_path_);
    if (!in) {
        return true;
    }
    in >> cursor->node_index >> cursor->disk_index >> cursor->image_index >> cursor->image_used_bytes;
    if (!in) {
        if (error) {
            *error = "failed to parse optical cursor: " + optical_cursor_path_.string();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeStore::SaveOpticalCursor(const SimOpticalCursor& cursor, std::string* error) const {
    std::ofstream out(optical_cursor_path_, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to write optical cursor: " + optical_cursor_path_.string();
        }
        return false;
    }
    out << cursor.node_index << " "
        << cursor.disk_index << " "
        << cursor.image_index << " "
        << cursor.image_used_bytes << "\n";
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeStore::LoadNamespaceIndex(const std::string& namespace_id,
                                          const SimMasstreeManifest& manifest,
                                          std::string* error) {
    std::vector<SimInodeSparseEntry> inode_sparse;
    if (!LoadInodeSparse(manifest.inode_sparse_path, &inode_sparse, error)) {
        return false;
    }
    for (const auto& entry : inode_sparse) {
        if (!index_.PutInodePageBoundary(namespace_id, entry.max_inode_id, entry.page_offset, error)) {
            return false;
        }
    }
    std::vector<SimDentrySparseEntry> dentry_sparse;
    if (!LoadDentrySparse(manifest.dentry_sparse_path, &dentry_sparse, error)) {
        return false;
    }
    for (const auto& entry : dentry_sparse) {
        if (!index_.PutDentryPageBoundary(namespace_id, entry.max_parent_inode, entry.max_name, entry.page_offset, error)) {
            return false;
        }
    }
    return true;
}

bool SimMasstreeStore::LookupInode(const std::string& namespace_id,
                                   uint64_t inode_id,
                                   SimInodeRecord* record,
                                   std::string* error) const {
    const auto ns = namespaces_.find(namespace_id);
    if (ns == namespaces_.end()) {
        if (error) {
            *error = "namespace not found: " + namespace_id;
        }
        return false;
    }
    SimInodeSparseEntry sparse;
    if (!index_.FindInodePageBoundary(namespace_id, inode_id, &sparse, error)) {
        if (error && error->empty()) {
            *error = "inode sparse boundary not found";
        }
        return false;
    }
    std::vector<SimInodeRecord> page;
    if (!ReadInodePageAt(ns->second.inode_pages_path, sparse.page_offset, &page, error)) {
        return false;
    }
    for (const auto& candidate : page) {
        if (candidate.inode_id == inode_id) {
            *record = candidate;
            return true;
        }
    }
    if (error) {
        *error = "inode not found in page";
    }
    return false;
}

bool SimMasstreeStore::LookupPath(const std::string& namespace_id,
                                  const std::string& full_path,
                                  SimInodeRecord* record,
                                  std::string* error) const {
    const auto ns = namespaces_.find(namespace_id);
    if (ns == namespaces_.end()) {
        if (error) {
            *error = "namespace not found: " + namespace_id;
        }
        return false;
    }
    std::string relative = NormalizePath(full_path);
    const std::string prefix = NormalizePath(ns->second.path_prefix);
    if (relative.rfind(prefix, 0) == 0) {
        relative = relative.substr(prefix.size());
        if (relative.empty()) {
            relative = "/";
        }
    }
    uint64_t parent = ns->second.root_inode_id;
    for (const auto& part : SplitPath(relative)) {
        SimDentrySparseEntry sparse;
        if (!index_.FindDentryPageBoundary(namespace_id, parent, part, &sparse, error)) {
            if (error && error->empty()) {
                *error = "dentry sparse boundary not found";
            }
            return false;
        }
        std::vector<SimDentryRecord> page;
        if (!ReadDentryPageAt(ns->second.dentry_pages_path, sparse.page_offset, &page, error)) {
            return false;
        }
        bool found = false;
        for (const auto& dentry : page) {
            if (dentry.parent_inode_id == parent && dentry.name == part) {
                parent = dentry.child_inode_id;
                found = true;
                break;
            }
        }
        if (!found) {
            if (error) {
                *error = "dentry not found: " + part;
            }
            return false;
        }
    }
    return LookupInode(namespace_id, parent, record, error);
}

bool SimMasstreeStore::BuildFullPath(const std::string& namespace_id,
                                     uint64_t inode_id,
                                     std::string* full_path,
                                     std::string* error) const {
    if (!full_path) {
        if (error) {
            *error = "full path output is null";
        }
        return false;
    }
    const auto ns = namespaces_.find(namespace_id);
    if (ns == namespaces_.end()) {
        if (error) {
            *error = "namespace not found: " + namespace_id;
        }
        return false;
    }
    std::vector<std::string> parts;
    uint64_t current = inode_id;
    for (uint32_t depth = 0; depth < 1024; ++depth) {
        SimInodeRecord inode;
        if (!LookupInode(namespace_id, current, &inode, error)) {
            return false;
        }
        if (current == ns->second.root_inode_id || inode.parent_inode_id == 0) {
            break;
        }
        parts.push_back(inode.file_name);
        current = inode.parent_inode_id;
    }
    std::reverse(parts.begin(), parts.end());
    std::string relative;
    for (const auto& part : parts) {
        relative += "/";
        relative += part;
    }
    *full_path = NormalizePath(ns->second.path_prefix + relative);
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeStore::Readdir(const std::string& namespace_id,
                               const std::string& full_path,
                               uint32_t limit,
                               std::vector<SimDentryRecord>* entries,
                               bool* has_more,
                               std::string* next_token,
                               std::string* error) const {
    if (!entries || !has_more || !next_token) {
        if (error) {
            *error = "invalid readdir outputs";
        }
        return false;
    }
    const auto ns = namespaces_.find(namespace_id);
    if (ns == namespaces_.end()) {
        if (error) {
            *error = "namespace not found: " + namespace_id;
        }
        return false;
    }
    SimInodeRecord dir;
    if (!LookupPath(namespace_id, full_path, &dir, error)) {
        return false;
    }
    if (dir.type != SimInodeType::Dir) {
        if (error) {
            *error = "path is not a directory: " + full_path;
        }
        return false;
    }
    std::vector<SimDentryRecord> all;
    if (!LoadAllDentries(ns->second, &all, error)) {
        return false;
    }
    entries->clear();
    *has_more = false;
    next_token->clear();
    const uint32_t effective_limit = limit == 0 ? 100 : limit;
    for (const auto& dentry : all) {
        if (dentry.parent_inode_id != dir.inode_id) {
            continue;
        }
        if (entries->size() >= effective_limit) {
            *has_more = true;
            *next_token = entries->empty() ? "" : entries->back().name;
            break;
        }
        entries->push_back(dentry);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool SimMasstreeStore::ReadSamples(const SimMasstreeManifest& manifest,
                                   std::vector<std::string>* samples,
                                   std::string* error) const {
    std::ifstream in(manifest.samples_path);
    if (!in) {
        if (error) {
            *error = "failed to open samples: " + manifest.samples_path.string();
        }
        return false;
    }
    samples->clear();
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            samples->push_back(line);
        }
    }
    return true;
}

bool SimMasstreeStore::Query(const std::string& namespace_id,
                             const std::string& query_mode,
                             const std::string& query_path,
                             uint64_t query_inode_id,
                             uint32_t readdir_limit,
                             bool sleep_for_latency,
                             QueryResult* result,
                             std::string* error) {
    const auto ns = namespaces_.find(namespace_id);
    if (ns == namespaces_.end()) {
        if (error) {
            *error = "namespace not found: " + namespace_id;
        }
        return false;
    }
    std::uniform_int_distribution<uint64_t> latency_dist(100, 200);
    const uint64_t latency_ms = latency_dist(rng_);
    if (sleep_for_latency) {
        std::this_thread::sleep_for(std::chrono::milliseconds(latency_ms));
    }
    QueryResult local;
    local.latency_ms = latency_ms;
    local.namespace_id = namespace_id;
    local.query_mode = query_mode.empty() ? "random_path_lookup" : query_mode;

    if (local.query_mode == "random_path_lookup") {
        std::vector<std::string> samples;
        if (!ReadSamples(ns->second, &samples, error) || samples.empty()) {
            return false;
        }
        std::uniform_int_distribution<size_t> sample_dist(0, samples.size() - 1);
        const std::string relative_path = samples[sample_dist(rng_)];
        local.full_path =
            NormalizePath(ns->second.path_prefix + "/" + relative_path.substr(relative_path.front() == '/' ? 1 : 0));
        if (!LookupPath(namespace_id, local.full_path, &local.record, &local.error)) {
            if (error) {
                *error = local.error;
            }
            return false;
        }
        local.inode_id = local.record.inode_id;
    } else if (local.query_mode == "resolve_path" || local.query_mode == "get_optical_location") {
        if (query_path.empty()) {
            if (local.query_mode == "get_optical_location" && query_inode_id != 0) {
                if (!LookupInode(namespace_id, query_inode_id, &local.record, &local.error)) {
                    if (error) {
                        *error = local.error;
                    }
                    return false;
                }
                local.inode_id = local.record.inode_id;
                (void)BuildFullPath(namespace_id, local.inode_id, &local.full_path, nullptr);
                local.optical_location = LocationFromRecord(local.record);
                local.ok = true;
                if (result) {
                    *result = std::move(local);
                }
                if (error) {
                    error->clear();
                }
                return true;
            }
            if (error) {
                *error = "query path is required for mode: " + local.query_mode;
            }
            return false;
        }
        local.full_path = NormalizePath(query_path);
        if (!LookupPath(namespace_id, local.full_path, &local.record, &local.error)) {
            if (error) {
                *error = local.error;
            }
            return false;
        }
        local.inode_id = local.record.inode_id;
        if (local.query_mode == "get_optical_location") {
            local.optical_location = LocationFromRecord(local.record);
        }
    } else if (local.query_mode == "get_inode") {
        if (query_inode_id == 0) {
            if (error) {
                *error = "inode_id is required for get_inode";
            }
            return false;
        }
        if (!LookupInode(namespace_id, query_inode_id, &local.record, &local.error)) {
            if (error) {
                *error = local.error;
            }
            return false;
        }
        local.inode_id = local.record.inode_id;
        (void)BuildFullPath(namespace_id, local.inode_id, &local.full_path, nullptr);
    } else if (local.query_mode == "build_full_path") {
        if (query_inode_id == 0) {
            if (error) {
                *error = "inode_id is required for build_full_path";
            }
            return false;
        }
        local.inode_id = query_inode_id;
        if (!LookupInode(namespace_id, query_inode_id, &local.record, error) ||
            !BuildFullPath(namespace_id, query_inode_id, &local.full_path, error)) {
            return false;
        }
    } else if (local.query_mode == "readdir") {
        if (query_path.empty()) {
            if (error) {
                *error = "query path is required for readdir";
            }
            return false;
        }
        local.full_path = NormalizePath(query_path);
        if (!Readdir(namespace_id,
                     local.full_path,
                     readdir_limit,
                     &local.dentries,
                     &local.has_more,
                     &local.next_token,
                     error)) {
            return false;
        }
        if (!LookupPath(namespace_id, local.full_path, &local.record, nullptr)) {
            local.record = {};
        }
        local.inode_id = local.record.inode_id;
    } else {
        if (error) {
            *error = "unsupported masstree query mode: " + local.query_mode;
        }
        return false;
    }
    local.ok = true;
    if (result) {
        *result = std::move(local);
    }
    if (error) {
        error->clear();
    }
    return true;
}

SimMasstreeStats SimMasstreeStore::Stats() const {
    SimMasstreeStats stats;
    stats.namespace_count = namespaces_.size();
    for (const auto& item : namespaces_) {
        stats.total_file_count += item.second.file_count;
        stats.total_file_bytes += item.second.total_file_bytes;
        stats.total_inode_count += item.second.inode_count;
        stats.total_dentry_count += item.second.dentry_count;
    }
    return stats;
}

} // namespace zb::sim::masstree
