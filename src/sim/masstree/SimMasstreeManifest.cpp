#include "sim/masstree/SimMasstreeManifest.h"

#include <fstream>
#include <sstream>

namespace zb::sim::masstree {

namespace {

std::string ParentString(const std::filesystem::path& base, const std::filesystem::path& path) {
    std::error_code ec;
    return std::filesystem::relative(path, base, ec).generic_string();
}

} // namespace

bool SaveManifest(const SimMasstreeManifest& manifest, std::string* error) {
    std::ofstream out(manifest.manifest_path);
    if (!out) {
        if (error) {
            *error = "failed to write manifest: " + manifest.manifest_path.string();
        }
        return false;
    }
    const auto base = manifest.manifest_path.parent_path();
    out << "sim_masstree_manifest_v1\n";
    out << "layout_version=" << manifest.layout_version << "\n";
    out << "kind=" << manifest.kind << "\n";
    out << "template_id=" << manifest.template_id << "\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "source_mode=" << manifest.source_mode << "\n";
    out << "path_list_file=" << manifest.path_list_file << "\n";
    out << "path_list_fingerprint=" << manifest.path_list_fingerprint << "\n";
    out << "repeat_dir_prefix=" << manifest.repeat_dir_prefix << "\n";
    out << "path_list_leaf_nodes_are_files=" << (manifest.path_list_leaf_nodes_are_files ? 1 : 0) << "\n";
    out << "path_prefix=" << manifest.path_prefix << "\n";
    out << "inode_pages_path=" << ParentString(base, manifest.inode_pages_path) << "\n";
    out << "dentry_pages_path=" << ParentString(base, manifest.dentry_pages_path) << "\n";
    out << "inode_sparse_path=" << ParentString(base, manifest.inode_sparse_path) << "\n";
    out << "dentry_sparse_path=" << ParentString(base, manifest.dentry_sparse_path) << "\n";
    out << "samples_path=" << ParentString(base, manifest.samples_path) << "\n";
    out << "optical_layout_path=" << ParentString(base, manifest.optical_layout_path) << "\n";
    out << "structure_stats_path=" << ParentString(base, manifest.structure_stats_path) << "\n";
    out << "allocation_summary_path=" << ParentString(base, manifest.allocation_summary_path) << "\n";
    out << "root_inode_id=" << manifest.root_inode_id << "\n";
    out << "inode_min=" << manifest.inode_min << "\n";
    out << "inode_max=" << manifest.inode_max << "\n";
    out << "inode_count=" << manifest.inode_count << "\n";
    out << "dentry_count=" << manifest.dentry_count << "\n";
    out << "inode_page_count=" << manifest.inode_page_count << "\n";
    out << "dentry_page_count=" << manifest.dentry_page_count << "\n";
    out << "target_file_count=" << manifest.target_file_count << "\n";
    out << "file_count=" << manifest.file_count << "\n";
    out << "template_base_file_count=" << manifest.template_base_file_count << "\n";
    out << "template_base_dir_count=" << manifest.template_base_dir_count << "\n";
    out << "dir_count=" << manifest.dir_count << "\n";
    out << "template_repeat_count=" << manifest.template_repeat_count << "\n";
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
    out << "inode_pages_bytes=" << manifest.inode_pages_bytes << "\n";
    out << "dentry_pages_bytes=" << manifest.dentry_pages_bytes << "\n";
    out << "start_global_image_id=" << manifest.start_global_image_id << "\n";
    out << "end_global_image_id=" << manifest.end_global_image_id << "\n";
    out << "start_cursor_node_index=" << manifest.start_cursor.node_index << "\n";
    out << "start_cursor_disk_index=" << manifest.start_cursor.disk_index << "\n";
    out << "start_cursor_image_index=" << manifest.start_cursor.image_index << "\n";
    out << "start_cursor_image_used_bytes=" << manifest.start_cursor.image_used_bytes << "\n";
    out << "end_cursor_node_index=" << manifest.end_cursor.node_index << "\n";
    out << "end_cursor_disk_index=" << manifest.end_cursor.disk_index << "\n";
    out << "end_cursor_image_index=" << manifest.end_cursor.image_index << "\n";
    out << "end_cursor_image_used_bytes=" << manifest.end_cursor.image_used_bytes << "\n";
    return true;
}

bool LoadManifest(const std::filesystem::path& path, SimMasstreeManifest* manifest, std::string* error) {
    if (!manifest) {
        if (error) {
            *error = "manifest output is null";
        }
        return false;
    }
    std::ifstream in(path);
    if (!in) {
        if (error) {
            *error = "failed to open manifest: " + path.string();
        }
        return false;
    }
    SimMasstreeManifest parsed;
    parsed.manifest_path = path;
    const auto base = path.parent_path();
    std::string line;
    bool header = false;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        if (!header) {
            header = true;
            if (line != "sim_masstree_manifest_v1") {
                if (error) {
                    *error = "invalid manifest header: " + path.string();
                }
                return false;
            }
            continue;
        }
        const size_t eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, eq);
        const std::string value = line.substr(eq + 1);
        if (key == "layout_version") parsed.layout_version = static_cast<uint32_t>(std::stoul(value));
        else if (key == "kind") parsed.kind = value;
        else if (key == "template_id") parsed.template_id = value;
        else if (key == "namespace_id") parsed.namespace_id = value;
        else if (key == "generation_id") parsed.generation_id = value;
        else if (key == "source_mode") parsed.source_mode = value;
        else if (key == "path_list_file") parsed.path_list_file = value;
        else if (key == "path_list_fingerprint") parsed.path_list_fingerprint = value;
        else if (key == "repeat_dir_prefix") parsed.repeat_dir_prefix = value;
        else if (key == "path_list_leaf_nodes_are_files") parsed.path_list_leaf_nodes_are_files = value == "1" || value == "true";
        else if (key == "path_prefix") parsed.path_prefix = value;
        else if (key == "inode_pages_path") parsed.inode_pages_path = base / value;
        else if (key == "dentry_pages_path") parsed.dentry_pages_path = base / value;
        else if (key == "inode_sparse_path") parsed.inode_sparse_path = base / value;
        else if (key == "dentry_sparse_path") parsed.dentry_sparse_path = base / value;
        else if (key == "samples_path") parsed.samples_path = base / value;
        else if (key == "optical_layout_path") parsed.optical_layout_path = base / value;
        else if (key == "structure_stats_path") parsed.structure_stats_path = base / value;
        else if (key == "allocation_summary_path") parsed.allocation_summary_path = base / value;
        else if (key == "root_inode_id") parsed.root_inode_id = static_cast<uint64_t>(std::stoull(value));
        else if (key == "inode_min") parsed.inode_min = static_cast<uint64_t>(std::stoull(value));
        else if (key == "inode_max") parsed.inode_max = static_cast<uint64_t>(std::stoull(value));
        else if (key == "inode_count") parsed.inode_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "dentry_count") parsed.dentry_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "inode_page_count") parsed.inode_page_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "dentry_page_count") parsed.dentry_page_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "target_file_count") parsed.target_file_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "file_count") parsed.file_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "template_base_file_count") parsed.template_base_file_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "template_base_dir_count") parsed.template_base_dir_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "dir_count") parsed.dir_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "template_repeat_count") parsed.template_repeat_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "level1_dir_count") parsed.level1_dir_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "leaf_dir_count") parsed.leaf_dir_count = static_cast<uint64_t>(std::stoull(value));
        else if (key == "template_base_max_depth") parsed.template_base_max_depth = static_cast<uint64_t>(std::stoull(value));
        else if (key == "max_depth") parsed.max_depth = static_cast<uint64_t>(std::stoull(value));
        else if (key == "max_files_per_leaf_dir") parsed.max_files_per_leaf_dir = static_cast<uint64_t>(std::stoull(value));
        else if (key == "max_subdirs_per_dir") parsed.max_subdirs_per_dir = static_cast<uint64_t>(std::stoull(value));
        else if (key == "min_file_size_bytes") parsed.min_file_size_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "max_file_size_bytes") parsed.max_file_size_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "avg_file_size_bytes") parsed.avg_file_size_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "total_file_bytes") parsed.total_file_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "inode_pages_bytes") parsed.inode_pages_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "dentry_pages_bytes") parsed.dentry_pages_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "start_global_image_id") parsed.start_global_image_id = static_cast<uint64_t>(std::stoull(value));
        else if (key == "end_global_image_id") parsed.end_global_image_id = static_cast<uint64_t>(std::stoull(value));
        else if (key == "start_cursor_node_index") parsed.start_cursor.node_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "start_cursor_disk_index") parsed.start_cursor.disk_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "start_cursor_image_index") parsed.start_cursor.image_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "start_cursor_image_used_bytes") parsed.start_cursor.image_used_bytes = static_cast<uint64_t>(std::stoull(value));
        else if (key == "end_cursor_node_index") parsed.end_cursor.node_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "end_cursor_disk_index") parsed.end_cursor.disk_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "end_cursor_image_index") parsed.end_cursor.image_index = static_cast<uint32_t>(std::stoul(value));
        else if (key == "end_cursor_image_used_bytes") parsed.end_cursor.image_used_bytes = static_cast<uint64_t>(std::stoull(value));
    }
    *manifest = std::move(parsed);
    return true;
}

} // namespace zb::sim::masstree
