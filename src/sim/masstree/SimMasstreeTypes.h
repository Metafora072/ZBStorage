#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace zb::sim::masstree {

enum class SimInodeType : uint8_t {
    File = 0,
    Dir = 1,
};

enum class SimStorageTier : uint8_t {
    None = 0,
    Disk = 1,
    Optical = 2,
};

struct SimInodeRecord {
    uint64_t inode_id{0};
    uint64_t parent_inode_id{0};
    SimInodeType type{SimInodeType::File};
    SimStorageTier storage_tier{SimStorageTier::None};
    uint64_t size_bytes{0};
    uint64_t atime{0};
    uint64_t mtime{0};
    uint64_t ctime{0};
    uint32_t mode{0};
    uint32_t uid{0};
    uint32_t gid{0};
    uint32_t nlink{0};
    std::string file_name;
    uint64_t optical_node_id{0};
    uint64_t optical_disk_id{0};
    uint32_t optical_image_id{0};
    uint64_t optical_offset{0};
    uint64_t optical_length{0};
};

struct SimOpticalCursor {
    uint32_t node_index{0};
    uint32_t disk_index{0};
    uint32_t image_index{0};
    uint64_t image_used_bytes{0};
};

struct SimOpticalLocation {
    uint32_t node_index{0};
    uint32_t disk_index{0};
    uint32_t image_index{0};
    uint64_t offset_bytes{0};
    uint64_t length_bytes{0};
};

struct SimDentryRecord {
    uint64_t parent_inode_id{0};
    std::string name;
    uint64_t child_inode_id{0};
    SimInodeType type{SimInodeType::File};
};

struct SimInodeSparseEntry {
    uint64_t page_offset{0};
    uint64_t max_inode_id{0};
};

struct SimDentrySparseEntry {
    uint64_t page_offset{0};
    uint64_t max_parent_inode{0};
    std::string max_name;
};

struct SimMasstreeManifest {
    uint32_t layout_version{1};
    std::string kind;
    std::string template_id;
    std::string namespace_id;
    std::string generation_id;
    std::string source_mode;
    std::string path_list_file;
    std::string path_list_fingerprint;
    std::string repeat_dir_prefix;
    bool path_list_leaf_nodes_are_files{false};
    std::string path_prefix;
    std::filesystem::path manifest_path;
    std::filesystem::path inode_pages_path;
    std::filesystem::path dentry_pages_path;
    std::filesystem::path inode_sparse_path;
    std::filesystem::path dentry_sparse_path;
    std::filesystem::path samples_path;
    std::filesystem::path optical_layout_path;
    std::filesystem::path structure_stats_path;
    std::filesystem::path allocation_summary_path;
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
    uint64_t total_file_bytes{0};
    uint64_t inode_pages_bytes{0};
    uint64_t dentry_pages_bytes{0};
    uint64_t start_global_image_id{0};
    uint64_t end_global_image_id{0};
    SimOpticalCursor start_cursor;
    SimOpticalCursor end_cursor;
};

struct SimMasstreeStats {
    uint64_t namespace_count{0};
    uint64_t total_file_count{0};
    uint64_t total_file_bytes{0};
    uint64_t total_inode_count{0};
    uint64_t total_dentry_count{0};
};

} // namespace zb::sim::masstree
