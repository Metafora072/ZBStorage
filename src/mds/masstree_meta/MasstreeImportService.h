#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "MasstreeBulkImporter.h"
#include "MasstreeBulkMetaGenerator.h"
#include "MasstreeGenerationPublisher.h"
#include "MasstreeNamespaceCatalog.h"
#include "MasstreeStatsStore.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {

class MasstreeImportService {
public:
    struct TemplateGenerationRequest {
        std::string masstree_root;
        std::string template_id;
        std::string path_list_file;
        std::string repeat_dir_prefix;
        bool path_list_leaf_nodes_are_files{false};
        uint32_t verify_inode_samples{16};
        uint32_t verify_dentry_samples{16};
    };

    struct TemplateImportRequest {
        std::string masstree_root;
        std::string namespace_id;
        std::string generation_id;
        std::string path_prefix;
        std::string template_id;
        std::string template_mode;
        uint64_t inode_start{0};
        uint32_t verify_inode_samples{16};
        uint32_t verify_dentry_samples{16};
        bool publish_route{true};
    };

    struct Result {
        std::string manifest_path;
        std::string staging_dir;
        uint64_t root_inode_id{0};
        uint64_t inode_min{0};
        uint64_t inode_max{0};
        uint64_t inode_count{0};
        uint64_t dentry_count{0};
        uint64_t file_count{0};
        uint64_t level1_dir_count{0};
        uint64_t leaf_dir_count{0};
        uint64_t inode_pages_bytes{0};
        uint64_t avg_file_size_bytes{0};
        std::string total_file_bytes;
    };

    MasstreeImportService(RocksMetaStore* meta_store,
                          MasstreeNamespaceCatalog* catalog);

    bool GenerateTemplate(const TemplateGenerationRequest& request,
                          Result* result,
                          std::string* error) const;

    bool ImportTemplateNamespace(const TemplateImportRequest& request,
                                 Result* result,
                                 std::string* error) const;

private:
    bool ReserveInodeRange(uint64_t inode_count,
                           uint64_t* inode_start,
                           std::string* error) const;
    bool NormalizePathPrefix(const std::string& path_prefix,
                             std::string* normalized,
                             std::string* error) const;

    RocksMetaStore* meta_store_{};
    MasstreeNamespaceCatalog* catalog_{};
    mutable std::mutex import_mu_;
};

} // namespace zb::mds
