#pragma once

#include <cstdint>
#include <string>

namespace zb::mds {

struct MasstreeNamespaceManifest {
    uint32_t layout_version{1};
    std::string namespace_id;
    std::string path_prefix;
    std::string generation_id;
    std::string source_mode;
    std::string template_id;
    std::string path_list_file;
    std::string path_list_fingerprint;
    std::string repeat_dir_prefix;
    bool path_list_leaf_nodes_are_files{false};
    std::string manifest_path;
    std::string inode_records_path;
    std::string dentry_records_path;
    std::string inode_pages_path;
    std::string inode_sparse_index_path;
    std::string dentry_pages_path;
    std::string dentry_sparse_index_path;
    std::string verify_manifest_path;
    std::string structure_stats_path;
    std::string cluster_stats_path;
    std::string allocation_summary_path;
    std::string optical_layout_path;
    std::string query_path_source_path;
    uint64_t page_size_bytes{0};
    uint64_t root_inode_id{0};
    uint64_t inode_min{0};
    uint64_t inode_max{0};
    uint64_t inode_count{0};
    uint64_t dentry_count{0};
    uint64_t inode_page_count{0};
    uint64_t dentry_page_count{0};
    uint64_t target_file_count{0};
    uint64_t file_count{0};
    uint64_t template_base_file_count{0};
    uint64_t template_base_dir_count{0};
    uint64_t dir_count{0};
    uint64_t template_repeat_count{0};
    uint64_t level1_dir_count{0};
    uint64_t leaf_dir_count{0};
    uint64_t template_base_max_depth{0};
    uint64_t max_depth{0};
    uint64_t max_files_per_leaf_dir{0};
    uint64_t max_subdirs_per_dir{0};
    uint64_t min_file_size_bytes{0};
    uint64_t max_file_size_bytes{0};
    uint64_t avg_file_size_bytes{0};
    uint64_t query_path_source_count{0};
    std::string total_file_bytes;
    uint64_t start_global_image_id{0};
    uint64_t end_global_image_id{0};
    uint32_t start_cursor_node_index{0};
    uint32_t start_cursor_disk_index{0};
    uint32_t start_cursor_image_index{0};
    uint64_t start_cursor_image_used_bytes{0};
    uint32_t end_cursor_node_index{0};
    uint32_t end_cursor_disk_index{0};
    uint32_t end_cursor_image_index{0};
    uint64_t end_cursor_image_used_bytes{0};

    static bool LoadFromFile(const std::string& manifest_path,
                             MasstreeNamespaceManifest* manifest,
                             std::string* error);
    bool SaveToFile(const std::string& manifest_path, std::string* error) const;
};

} // namespace zb::mds
