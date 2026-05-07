#pragma once

#include <cstdint>
#include <string>

namespace zb::mds {

class MasstreeBulkMetaGenerator {
public:
    struct Request {
        std::string output_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        std::string source_mode;
        std::string path_list_file;
        std::string repeat_dir_prefix;
        bool path_list_leaf_nodes_are_files{false};
        uint64_t inode_start{0};
        uint64_t file_count{100000000ULL};
        uint32_t max_files_per_leaf_dir{2048};
        uint32_t max_subdirs_per_dir{256};
    };

    struct PathListSummary {
        uint64_t actual_file_count{0};
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t base_file_count{0};
        uint64_t base_dir_count{0};
        uint64_t dir_count{0};
        uint64_t repeat_count{0};
        uint64_t base_max_depth{0};
        uint64_t max_depth{0};
        std::string fingerprint;
        std::string repeat_dir_prefix;
    };

    struct Result {
        std::string staging_dir;
        std::string manifest_path;
        std::string inode_records_path;
        std::string dentry_records_path;
        std::string verify_manifest_path;
        std::string structure_stats_path;
        std::string source_mode;
        uint64_t root_inode_id{0};
        uint64_t inode_min{0};
        uint64_t inode_max{0};
        uint64_t target_file_count{0};
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t base_file_count{0};
        uint64_t base_dir_count{0};
        uint64_t dir_count{0};
        uint64_t repeat_count{0};
        uint64_t level1_dir_count{0};
        uint64_t leaf_dir_count{0};
        uint64_t base_max_depth{0};
        uint64_t max_depth{0};
        uint64_t avg_file_size_bytes{0};
        std::string total_file_bytes;
    };

    bool SummarizePathList(const Request& request,
                           PathListSummary* summary,
                           std::string* error) const;
    bool Generate(const Request& request,
                  Result* result,
                  std::string* error) const;
};

} // namespace zb::mds
