#include "MasstreeManifest.h"

#include <algorithm>
#include <cctype>
#include <fstream>

namespace zb::mds {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

bool ParseU64Field(const std::string& value, uint64_t* out) {
    if (!out) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace

bool MasstreeNamespaceManifest::LoadFromFile(const std::string& manifest_path,
                                             MasstreeNamespaceManifest* manifest,
                                             std::string* error) {
    if (!manifest) {
        if (error) {
            *error = "manifest output is null";
        }
        return false;
    }

    std::ifstream input(manifest_path);
    if (!input) {
        if (error) {
            *error = "failed to open masstree namespace manifest: " + manifest_path;
        }
        return false;
    }

    MasstreeNamespaceManifest parsed;
    parsed.manifest_path = manifest_path;
    std::string line;
    bool header_checked = false;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (trimmed != "masstree_namespace_manifest_v1") {
                if (error) {
                    *error = "invalid masstree namespace manifest header: " + manifest_path;
                }
                return false;
            }
            continue;
        }
        const size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "invalid masstree namespace manifest line " + std::to_string(line_no);
            }
            return false;
        }
        const std::string key = Trim(trimmed.substr(0, eq));
        const std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "namespace_id") {
            parsed.namespace_id = value;
        } else if (key == "layout_version") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid layout_version in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.layout_version = static_cast<uint32_t>(parsed_value);
        } else if (key == "source_mode") {
            parsed.source_mode = value;
        } else if (key == "template_id") {
            parsed.template_id = value;
        } else if (key == "path_list_file") {
            parsed.path_list_file = value;
        } else if (key == "path_list_fingerprint") {
            parsed.path_list_fingerprint = value;
        } else if (key == "repeat_dir_prefix") {
            parsed.repeat_dir_prefix = value;
        } else if (key == "path_list_leaf_nodes_are_files") {
            parsed.path_list_leaf_nodes_are_files =
                (value == "1" || value == "true" || value == "TRUE" || value == "yes");
        } else if (key == "path_prefix") {
            parsed.path_prefix = value;
        } else if (key == "generation_id") {
            parsed.generation_id = value;
        } else if (key == "inode_records_path") {
            parsed.inode_records_path = value;
        } else if (key == "dentry_records_path") {
            parsed.dentry_records_path = value;
        } else if (key == "inode_pages_path") {
            parsed.inode_pages_path = value;
        } else if (key == "inode_sparse_index_path") {
            parsed.inode_sparse_index_path = value;
        } else if (key == "dentry_pages_path") {
            parsed.dentry_pages_path = value;
        } else if (key == "dentry_sparse_index_path") {
            parsed.dentry_sparse_index_path = value;
        } else if (key == "verify_manifest_path") {
            parsed.verify_manifest_path = value;
        } else if (key == "structure_stats_path") {
            parsed.structure_stats_path = value;
        } else if (key == "cluster_stats_path") {
            parsed.cluster_stats_path = value;
        } else if (key == "allocation_summary_path") {
            parsed.allocation_summary_path = value;
        } else if (key == "optical_layout_path") {
            parsed.optical_layout_path = value;
        } else if (key == "query_path_source_path") {
            parsed.query_path_source_path = value;
        } else if (key == "root_inode_id") {
            if (!ParseU64Field(value, &parsed.root_inode_id)) {
                if (error) {
                    *error = "invalid root_inode_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_min") {
            if (!ParseU64Field(value, &parsed.inode_min)) {
                if (error) {
                    *error = "invalid inode_min in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_max") {
            if (!ParseU64Field(value, &parsed.inode_max)) {
                if (error) {
                    *error = "invalid inode_max in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_count") {
            if (!ParseU64Field(value, &parsed.inode_count)) {
                if (error) {
                    *error = "invalid inode_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "dentry_count") {
            if (!ParseU64Field(value, &parsed.dentry_count)) {
                if (error) {
                    *error = "invalid dentry_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "inode_page_count") {
            if (!ParseU64Field(value, &parsed.inode_page_count)) {
                if (error) {
                    *error = "invalid inode_page_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "page_size_bytes") {
            if (!ParseU64Field(value, &parsed.page_size_bytes)) {
                if (error) {
                    *error = "invalid page_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "dentry_page_count") {
            if (!ParseU64Field(value, &parsed.dentry_page_count)) {
                if (error) {
                    *error = "invalid dentry_page_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "target_file_count") {
            if (!ParseU64Field(value, &parsed.target_file_count)) {
                if (error) {
                    *error = "invalid target_file_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "file_count") {
            if (!ParseU64Field(value, &parsed.file_count)) {
                if (error) {
                    *error = "invalid file_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "template_base_file_count") {
            if (!ParseU64Field(value, &parsed.template_base_file_count)) {
                if (error) {
                    *error = "invalid template_base_file_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "template_base_dir_count") {
            if (!ParseU64Field(value, &parsed.template_base_dir_count)) {
                if (error) {
                    *error = "invalid template_base_dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "dir_count") {
            if (!ParseU64Field(value, &parsed.dir_count)) {
                if (error) {
                    *error = "invalid dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "template_repeat_count") {
            if (!ParseU64Field(value, &parsed.template_repeat_count)) {
                if (error) {
                    *error = "invalid template_repeat_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "level1_dir_count") {
            if (!ParseU64Field(value, &parsed.level1_dir_count)) {
                if (error) {
                    *error = "invalid level1_dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "leaf_dir_count") {
            if (!ParseU64Field(value, &parsed.leaf_dir_count)) {
                if (error) {
                    *error = "invalid leaf_dir_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "template_base_max_depth") {
            if (!ParseU64Field(value, &parsed.template_base_max_depth)) {
                if (error) {
                    *error = "invalid template_base_max_depth in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_depth") {
            if (!ParseU64Field(value, &parsed.max_depth)) {
                if (error) {
                    *error = "invalid max_depth in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_files_per_leaf_dir") {
            if (!ParseU64Field(value, &parsed.max_files_per_leaf_dir)) {
                if (error) {
                    *error = "invalid max_files_per_leaf_dir in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_subdirs_per_dir") {
            if (!ParseU64Field(value, &parsed.max_subdirs_per_dir)) {
                if (error) {
                    *error = "invalid max_subdirs_per_dir in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "min_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.min_file_size_bytes)) {
                if (error) {
                    *error = "invalid min_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "max_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.max_file_size_bytes)) {
                if (error) {
                    *error = "invalid max_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "avg_file_size_bytes") {
            if (!ParseU64Field(value, &parsed.avg_file_size_bytes)) {
                if (error) {
                    *error = "invalid avg_file_size_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "query_path_source_count") {
            if (!ParseU64Field(value, &parsed.query_path_source_count)) {
                if (error) {
                    *error = "invalid query_path_source_count in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "total_file_bytes") {
            parsed.total_file_bytes = value;
        } else if (key == "start_global_image_id") {
            if (!ParseU64Field(value, &parsed.start_global_image_id)) {
                if (error) {
                    *error = "invalid start_global_image_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "end_global_image_id") {
            if (!ParseU64Field(value, &parsed.end_global_image_id)) {
                if (error) {
                    *error = "invalid end_global_image_id in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "start_cursor_node_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_node_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_node_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_disk_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_disk_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_disk_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_image_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid start_cursor_image_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.start_cursor_image_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "start_cursor_image_used_bytes") {
            if (!ParseU64Field(value, &parsed.start_cursor_image_used_bytes)) {
                if (error) {
                    *error = "invalid start_cursor_image_used_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        } else if (key == "end_cursor_node_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_node_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_node_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_disk_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_disk_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_disk_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_image_index") {
            uint64_t parsed_value = 0;
            if (!ParseU64Field(value, &parsed_value)) {
                if (error) {
                    *error = "invalid end_cursor_image_index in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
            parsed.end_cursor_image_index = static_cast<uint32_t>(parsed_value);
        } else if (key == "end_cursor_image_used_bytes") {
            if (!ParseU64Field(value, &parsed.end_cursor_image_used_bytes)) {
                if (error) {
                    *error = "invalid end_cursor_image_used_bytes in masstree namespace manifest: " + manifest_path;
                }
                return false;
            }
        }
    }

    if (!header_checked) {
        if (error) {
            *error = "empty masstree namespace manifest: " + manifest_path;
        }
        return false;
    }
    if (parsed.namespace_id.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing namespace_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.path_prefix.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing path_prefix: " + manifest_path;
        }
        return false;
    }
    if (parsed.generation_id.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing generation_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.root_inode_id == 0) {
        if (error) {
            *error = "masstree namespace manifest missing root_inode_id: " + manifest_path;
        }
        return false;
    }
    if (parsed.inode_pages_path.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing inode_pages_path: " + manifest_path;
        }
        return false;
    }
    if (parsed.inode_sparse_index_path.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing inode_sparse_index_path: " + manifest_path;
        }
        return false;
    }
    if (parsed.dentry_pages_path.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing dentry_pages_path: " + manifest_path;
        }
        return false;
    }
    if (parsed.dentry_sparse_index_path.empty()) {
        if (error) {
            *error = "masstree namespace manifest missing dentry_sparse_index_path: " + manifest_path;
        }
        return false;
    }

    *manifest = std::move(parsed);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeNamespaceManifest::SaveToFile(const std::string& manifest_path, std::string* error) const {
    std::ofstream out(manifest_path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree namespace manifest: " + manifest_path;
        }
        return false;
    }

    out << "masstree_namespace_manifest_v1\n";
    out << "layout_version=" << layout_version << "\n";
    out << "namespace_id=" << namespace_id << "\n";
    out << "source_mode=" << source_mode << "\n";
    out << "template_id=" << template_id << "\n";
    out << "path_list_file=" << path_list_file << "\n";
    out << "path_list_fingerprint=" << path_list_fingerprint << "\n";
    out << "repeat_dir_prefix=" << repeat_dir_prefix << "\n";
    out << "path_list_leaf_nodes_are_files=" << (path_list_leaf_nodes_are_files ? 1 : 0) << "\n";
    out << "path_prefix=" << path_prefix << "\n";
    out << "generation_id=" << generation_id << "\n";
    out << "inode_records_path=" << inode_records_path << "\n";
    out << "dentry_records_path=" << dentry_records_path << "\n";
    out << "inode_pages_path=" << inode_pages_path << "\n";
    out << "inode_sparse_index_path=" << inode_sparse_index_path << "\n";
    out << "dentry_pages_path=" << dentry_pages_path << "\n";
    out << "dentry_sparse_index_path=" << dentry_sparse_index_path << "\n";
    out << "verify_manifest_path=" << verify_manifest_path << "\n";
    out << "structure_stats_path=" << structure_stats_path << "\n";
    out << "cluster_stats_path=" << cluster_stats_path << "\n";
    out << "allocation_summary_path=" << allocation_summary_path << "\n";
    out << "optical_layout_path=" << optical_layout_path << "\n";
    out << "query_path_source_path=" << query_path_source_path << "\n";
    out << "root_inode_id=" << root_inode_id << "\n";
    out << "inode_min=" << inode_min << "\n";
    out << "inode_max=" << inode_max << "\n";
    out << "inode_count=" << inode_count << "\n";
    out << "dentry_count=" << dentry_count << "\n";
    out << "inode_page_count=" << inode_page_count << "\n";
    out << "page_size_bytes=" << page_size_bytes << "\n";
    out << "dentry_page_count=" << dentry_page_count << "\n";
    out << "target_file_count=" << target_file_count << "\n";
    out << "file_count=" << file_count << "\n";
    out << "template_base_file_count=" << template_base_file_count << "\n";
    out << "template_base_dir_count=" << template_base_dir_count << "\n";
    out << "dir_count=" << dir_count << "\n";
    out << "template_repeat_count=" << template_repeat_count << "\n";
    out << "level1_dir_count=" << level1_dir_count << "\n";
    out << "leaf_dir_count=" << leaf_dir_count << "\n";
    out << "template_base_max_depth=" << template_base_max_depth << "\n";
    out << "max_depth=" << max_depth << "\n";
    out << "max_files_per_leaf_dir=" << max_files_per_leaf_dir << "\n";
    out << "max_subdirs_per_dir=" << max_subdirs_per_dir << "\n";
    out << "min_file_size_bytes=" << min_file_size_bytes << "\n";
    out << "max_file_size_bytes=" << max_file_size_bytes << "\n";
    out << "avg_file_size_bytes=" << avg_file_size_bytes << "\n";
    out << "query_path_source_count=" << query_path_source_count << "\n";
    out << "total_file_bytes=" << total_file_bytes << "\n";
    out << "start_global_image_id=" << start_global_image_id << "\n";
    out << "end_global_image_id=" << end_global_image_id << "\n";
    out << "start_cursor_node_index=" << start_cursor_node_index << "\n";
    out << "start_cursor_disk_index=" << start_cursor_disk_index << "\n";
    out << "start_cursor_image_index=" << start_cursor_image_index << "\n";
    out << "start_cursor_image_used_bytes=" << start_cursor_image_used_bytes << "\n";
    out << "end_cursor_node_index=" << end_cursor_node_index << "\n";
    out << "end_cursor_disk_index=" << end_cursor_disk_index << "\n";
    out << "end_cursor_image_index=" << end_cursor_image_index << "\n";
    out << "end_cursor_image_used_bytes=" << end_cursor_image_used_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree namespace manifest: " + manifest_path;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
