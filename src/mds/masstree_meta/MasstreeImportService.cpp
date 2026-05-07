#include "MasstreeImportService.h"

#include <algorithm>
#include <cctype>
#include <filesystem>

#include "MasstreeManifest.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

namespace fs = std::filesystem;

constexpr const char* kMasstreeTemplateGenerationId = "template";
constexpr const char* kMasstreeTemplateModePageFast = "page_fast";
constexpr const char* kMasstreeTemplateModeLegacyRecords = "legacy_records";
constexpr const char* kMasstreeSourceModePathList = "path_list";
constexpr uint64_t kMasstreeTemplateTargetFileCount = 100000000ULL;

struct InternalImportRequest {
    std::string masstree_root;
    std::string namespace_id;
    std::string generation_id;
    std::string path_prefix;
    std::string path_list_file;
    std::string repeat_dir_prefix;
    bool path_list_leaf_nodes_are_files{false};
    std::string template_id;
    std::string template_mode;
    uint64_t inode_start{0};
    uint64_t file_count{0};
    uint32_t verify_inode_samples{16};
    uint32_t verify_dentry_samples{16};
    bool publish_route{true};
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

std::string TemplateManifestPath(const std::string& masstree_root, const std::string& template_id) {
    return (fs::path(masstree_root) / "templates" / template_id /
            (std::string(kMasstreeTemplateGenerationId) + ".staging") / "manifest.txt")
        .string();
}

fs::path TemplateDirectoryPath(const std::string& masstree_root, const std::string& template_id) {
    return fs::path(masstree_root) / "templates" / template_id;
}

std::string TemplatePathPrefix(const std::string& template_id) {
    return "/__masstree_template__/" + template_id;
}

std::string NormalizeTemplateMode(std::string mode) {
    std::transform(mode.begin(), mode.end(), mode.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return mode;
}

bool IsTemplateModeValid(const std::string& mode) {
    return mode.empty() || mode == kMasstreeTemplateModePageFast || mode == kMasstreeTemplateModeLegacyRecords;
}

bool UseLegacyTemplateRecordsMode(const std::string& mode) {
    return NormalizeTemplateMode(mode) == kMasstreeTemplateModeLegacyRecords;
}

bool ValidateTemplateManifest(const InternalImportRequest& request,
                              const MasstreeNamespaceManifest& manifest,
                              std::string* error) {
    const std::string manifest_source_mode = NormalizeTemplateMode(manifest.source_mode);
    const uint64_t manifest_target_file_count =
        manifest.target_file_count == 0 ? manifest.file_count : manifest.target_file_count;
    if (!request.path_list_file.empty()) {
        if (manifest_source_mode != kMasstreeSourceModePathList) {
            if (error) {
                *error = "existing masstree template parameters do not match import request";
            }
            return false;
        }
        MasstreeBulkMetaGenerator generator;
        MasstreeBulkMetaGenerator::Request summary_request;
        summary_request.source_mode = kMasstreeSourceModePathList;
        summary_request.file_count = manifest_target_file_count;
        summary_request.path_list_file = request.path_list_file;
        summary_request.repeat_dir_prefix =
            request.repeat_dir_prefix.empty() ? manifest.repeat_dir_prefix : request.repeat_dir_prefix;
        summary_request.path_list_leaf_nodes_are_files = request.path_list_leaf_nodes_are_files;
        MasstreeBulkMetaGenerator::PathListSummary summary;
        if (!generator.SummarizePathList(summary_request, &summary, error)) {
            return false;
        }
        if (manifest.path_list_fingerprint != summary.fingerprint ||
            manifest.repeat_dir_prefix != summary.repeat_dir_prefix ||
            manifest.template_base_file_count != summary.base_file_count ||
            manifest.template_repeat_count != summary.repeat_count ||
            manifest.file_count != summary.actual_file_count) {
            if (error) {
                *error = "existing masstree path_list template does not match import request";
            }
            return false;
        }
        return true;
    }
    if (!request.repeat_dir_prefix.empty() && manifest.repeat_dir_prefix != request.repeat_dir_prefix) {
        if (error) {
            *error = "existing masstree template parameters do not match import request";
        }
        return false;
    }
    if (request.file_count != 0 && manifest_target_file_count != request.file_count) {
        if (error) {
            *error = "existing masstree template parameters do not match import request";
        }
        return false;
    }
    return true;
}

bool EnsureTemplateManifest(const InternalImportRequest& request,
                            bool allow_create,
                            MasstreeNamespaceManifest* manifest,
                            std::string* error) {
    if (!manifest || request.masstree_root.empty() || request.template_id.empty()) {
        if (error) {
            *error = "invalid masstree template request";
        }
        return false;
    }

    const std::string manifest_path = TemplateManifestPath(request.masstree_root, request.template_id);
    if (fs::exists(manifest_path)) {
        if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path, manifest, error)) {
            return false;
        }
        if (manifest->inode_pages_path.empty() || !fs::exists(manifest->inode_pages_path) ||
            manifest->dentry_pages_path.empty() || !fs::exists(manifest->dentry_pages_path) ||
            manifest->optical_layout_path.empty() || !fs::exists(manifest->optical_layout_path)) {
            MasstreeBulkImporter importer;
            MasstreeBulkImporter::Request import_request;
            import_request.manifest_path = manifest_path;
            import_request.verify_inode_samples = request.verify_inode_samples;
            import_request.verify_dentry_samples = request.verify_dentry_samples;
            import_request.start_cursor = {};

            MasstreeBulkImporter::Result import_result;
            if (!importer.Import(import_request, nullptr, &import_result, error)) {
                return false;
            }
            if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path, manifest, error)) {
                return false;
            }
        }
        return ValidateTemplateManifest(request, *manifest, error);
    }

    if (!allow_create) {
        if (error) {
            *error = "masstree template not found: " + request.template_id;
        }
        return false;
    }

    const fs::path template_dir = TemplateDirectoryPath(request.masstree_root, request.template_id);
    if (fs::exists(template_dir)) {
        std::error_code cleanup_ec;
        fs::remove_all(template_dir, cleanup_ec);
        if (cleanup_ec) {
            if (error) {
                *error = "failed to clean stale masstree template dir: " + template_dir.string();
            }
            return false;
        }
    }

    MasstreeBulkMetaGenerator generator;
    MasstreeBulkMetaGenerator::Request template_request;
    template_request.output_root = (fs::path(request.masstree_root) / "templates").string();
    template_request.namespace_id = request.template_id;
    template_request.generation_id = kMasstreeTemplateGenerationId;
    template_request.path_prefix = TemplatePathPrefix(request.template_id);
    template_request.source_mode = kMasstreeSourceModePathList;
    template_request.path_list_file = request.path_list_file;
    template_request.repeat_dir_prefix = request.repeat_dir_prefix;
    template_request.path_list_leaf_nodes_are_files = request.path_list_leaf_nodes_are_files;
    template_request.inode_start = 1;
    template_request.file_count = request.file_count;
    template_request.max_files_per_leaf_dir = 2048U;
    template_request.max_subdirs_per_dir = 256U;

    MasstreeBulkMetaGenerator::Result template_result;
    if (!generator.Generate(template_request, &template_result, error)) {
        return false;
    }
    MasstreeBulkImporter importer;
    MasstreeBulkImporter::Request import_request;
    import_request.manifest_path = template_result.manifest_path;
    import_request.verify_inode_samples = request.verify_inode_samples;
    import_request.verify_dentry_samples = request.verify_dentry_samples;
    import_request.start_cursor = {};

    MasstreeBulkImporter::Result import_result;
    if (!importer.Import(import_request, nullptr, &import_result, error)) {
        return false;
    }

    if (!MasstreeNamespaceManifest::LoadFromFile(template_result.manifest_path, manifest, error)) {
        return false;
    }
    return ValidateTemplateManifest(request, *manifest, error);
}

bool PrepareTemplateImportManifest(const InternalImportRequest& request,
                                   const MasstreeNamespaceManifest& template_manifest,
                                   MasstreeNamespaceManifest* manifest,
                                   std::string* staging_dir,
                                   uint64_t* inode_id_offset,
                                   std::string* error) {
    if (!manifest || !staging_dir || !inode_id_offset) {
        if (error) {
            *error = "invalid masstree template import args";
        }
        return false;
    }

    const fs::path target_staging_dir =
        fs::path(request.masstree_root) / "staging" / request.namespace_id / (request.generation_id + ".staging");
    std::error_code ec;
    if (fs::exists(target_staging_dir, ec)) {
        if (error) {
            *error = "masstree import staging dir already exists: " + target_staging_dir.string();
        }
        return false;
    }
    if (!fs::create_directories(target_staging_dir, ec) || ec) {
        if (error) {
            *error = "failed to create masstree import staging dir: " + target_staging_dir.string();
        }
        return false;
    }

    *staging_dir = target_staging_dir.string();
    *inode_id_offset = request.inode_start - template_manifest.inode_min;

    *manifest = template_manifest;
    manifest->template_id = request.template_id;
    manifest->namespace_id = request.namespace_id;
    manifest->generation_id = request.generation_id;
    manifest->path_prefix = request.path_prefix;
    manifest->manifest_path = (target_staging_dir / "manifest.txt").string();
    manifest->inode_pages_path = (target_staging_dir / "inode_pages.seg").string();
    manifest->inode_sparse_index_path = (target_staging_dir / "inode_sparse.idx").string();
    manifest->dentry_pages_path = (target_staging_dir / "dentry_pages.seg").string();
    manifest->dentry_sparse_index_path = (target_staging_dir / "dentry_sparse.idx").string();
    manifest->verify_manifest_path.clear();
    manifest->cluster_stats_path.clear();
    manifest->allocation_summary_path.clear();
    manifest->optical_layout_path.clear();
    manifest->root_inode_id += *inode_id_offset;
    manifest->inode_min += *inode_id_offset;
    manifest->inode_max += *inode_id_offset;
    return manifest->SaveToFile(manifest->manifest_path, error);
}

} // namespace

MasstreeImportService::MasstreeImportService(RocksMetaStore* meta_store,
                                             MasstreeNamespaceCatalog* catalog)
    : meta_store_(meta_store),
      catalog_(catalog) {
}

bool MasstreeImportService::GenerateTemplate(const TemplateGenerationRequest& request,
                                             Result* result,
                                             std::string* error) const {
    std::lock_guard<std::mutex> lock(import_mu_);
    if (result) {
        *result = Result();
    }
    if (!meta_store_ || !catalog_ || request.masstree_root.empty() ||
        request.template_id.empty() || request.path_list_file.empty()) {
        if (error) {
            *error = "invalid masstree template generation request";
        }
        return false;
    }

    InternalImportRequest internal_request;
    internal_request.masstree_root = request.masstree_root;
    internal_request.template_id = request.template_id;
    internal_request.path_list_file = request.path_list_file;
    internal_request.repeat_dir_prefix = request.repeat_dir_prefix;
    internal_request.path_list_leaf_nodes_are_files = request.path_list_leaf_nodes_are_files;
    internal_request.file_count = kMasstreeTemplateTargetFileCount;
    internal_request.verify_inode_samples = request.verify_inode_samples;
    internal_request.verify_dentry_samples = request.verify_dentry_samples;

    MasstreeNamespaceManifest template_manifest;
    if (!EnsureTemplateManifest(internal_request, true, &template_manifest, error)) {
        return false;
    }

    if (result) {
        result->manifest_path = template_manifest.manifest_path;
        result->staging_dir = TemplateDirectoryPath(request.masstree_root, request.template_id).string();
        result->root_inode_id = template_manifest.root_inode_id;
        result->inode_min = template_manifest.inode_min;
        result->inode_max = template_manifest.inode_max;
        result->inode_count = template_manifest.inode_count;
        result->dentry_count = template_manifest.dentry_count;
        result->file_count = template_manifest.file_count;
        result->level1_dir_count = template_manifest.level1_dir_count;
        result->leaf_dir_count = template_manifest.leaf_dir_count;
        if (!template_manifest.inode_pages_path.empty() && fs::exists(template_manifest.inode_pages_path)) {
            std::error_code size_ec;
            const uint64_t inode_pages_bytes = fs::file_size(template_manifest.inode_pages_path, size_ec);
            result->inode_pages_bytes = size_ec ? 0 : inode_pages_bytes;
        } else {
            result->inode_pages_bytes = 0;
        }
        result->avg_file_size_bytes = template_manifest.avg_file_size_bytes;
        result->total_file_bytes = template_manifest.total_file_bytes;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeImportService::ImportTemplateNamespace(const TemplateImportRequest& request,
                                                    Result* result,
                                                    std::string* error) const {
    std::lock_guard<std::mutex> lock(import_mu_);
    if (result) {
        *result = Result();
    }
    if (!meta_store_ || !catalog_ || request.masstree_root.empty() ||
        request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.template_id.empty()) {
        if (error) {
            *error = "invalid masstree template import request";
        }
        return false;
    }
    InternalImportRequest normalized;
    normalized.masstree_root = request.masstree_root;
    normalized.namespace_id = request.namespace_id;
    normalized.generation_id = request.generation_id;
    normalized.template_id = request.template_id;
    normalized.template_mode = NormalizeTemplateMode(request.template_mode);
    normalized.inode_start = request.inode_start;
    normalized.verify_inode_samples = request.verify_inode_samples;
    normalized.verify_dentry_samples = request.verify_dentry_samples;
    normalized.publish_route = request.publish_route;
    if (!NormalizePathPrefix(request.path_prefix, &normalized.path_prefix, error)) {
        return false;
    }
    if (!IsTemplateModeValid(normalized.template_mode)) {
        if (error) {
            *error = "unsupported masstree template_mode: " + normalized.template_mode;
        }
        return false;
    }

    MasstreeStatsStore stats_store(meta_store_);
    MasstreeClusterStatsRecord cluster_before;
    if (!stats_store.LoadClusterStats(&cluster_before, error)) {
        return false;
    }

    std::string staging_dir;
    std::string working_manifest_path;
    std::string source_inode_records_path;
    std::string source_dentry_records_path;
    MasstreeNamespaceManifest template_manifest;
    uint64_t inode_id_offset = 0;
    uint64_t root_inode_id = 0;
    uint64_t inode_min = 0;
    uint64_t inode_max = 0;
    uint64_t inode_count = 0;
    uint64_t dentry_count = 0;
    uint64_t level1_dir_count_for_result = 0;
    uint64_t leaf_dir_count_for_result = 0;

    if (!EnsureTemplateManifest(normalized, false, &template_manifest, error)) {
        return false;
    }
    if (normalized.inode_start == 0 &&
        !ReserveInodeRange(template_manifest.inode_count, &normalized.inode_start, error)) {
        return false;
    }
    MasstreeNamespaceManifest working_manifest;
    if (!PrepareTemplateImportManifest(normalized,
                                       template_manifest,
                                       &working_manifest,
                                       &staging_dir,
                                       &inode_id_offset,
                                       error)) {
        return false;
    }
    working_manifest_path = working_manifest.manifest_path;
    source_inode_records_path = template_manifest.inode_records_path;
    source_dentry_records_path = template_manifest.dentry_records_path;
    root_inode_id = working_manifest.root_inode_id;
    inode_min = working_manifest.inode_min;
    inode_max = working_manifest.inode_max;
    inode_count = working_manifest.inode_count;
    dentry_count = working_manifest.dentry_count;
    level1_dir_count_for_result = working_manifest.level1_dir_count;
    leaf_dir_count_for_result = working_manifest.leaf_dir_count;

    MasstreeGenerationPublisher publisher(catalog_);
    auto cleanup = [&]() {
        std::string cleanup_error;
        publisher.CleanupStagingDir(staging_dir, &cleanup_error);
    };

    MasstreeBulkImporter importer;
    MasstreeBulkImporter::Request import_request;
    import_request.manifest_path = working_manifest_path;
    if (UseLegacyTemplateRecordsMode(normalized.template_mode)) {
        import_request.source_inode_records_path = source_inode_records_path;
        import_request.source_dentry_records_path = source_dentry_records_path;
    } else {
        import_request.source_inode_pages_path = template_manifest.inode_pages_path;
        import_request.source_dentry_pages_path = template_manifest.dentry_pages_path;
        import_request.source_dentry_sparse_path = template_manifest.dentry_sparse_index_path;
        import_request.source_optical_layout_path = template_manifest.optical_layout_path;
        import_request.source_inode_records_path = source_inode_records_path;
        import_request.source_dentry_records_path = source_dentry_records_path;
    }
    import_request.inode_id_offset = inode_id_offset;
    import_request.verify_inode_samples = normalized.verify_inode_samples;
    import_request.verify_dentry_samples = normalized.verify_dentry_samples;
    import_request.start_cursor = cluster_before.cursor;

    MasstreeBulkImporter::Result import_result;
    if (!importer.Import(import_request, nullptr, &import_result, error)) {
        cleanup();
        return false;
    }

    bool had_previous_namespace_stats = false;
    std::string previous_namespace_stats_raw;
    if (!stats_store.LoadNamespaceStatsRaw(normalized.namespace_id,
                                           &had_previous_namespace_stats,
                                           &previous_namespace_stats_raw,
                                           error)) {
        cleanup();
        return false;
    }

    MasstreeNamespaceStatsRecord previous_namespace_stats;
    const MasstreeNamespaceStatsRecord* previous_namespace_stats_ptr = nullptr;
    if (had_previous_namespace_stats &&
        !stats_store.LoadNamespaceStats(normalized.namespace_id, &previous_namespace_stats, error)) {
        cleanup();
        return false;
    }
    if (had_previous_namespace_stats) {
        previous_namespace_stats_ptr = &previous_namespace_stats;
    }

    MasstreeNamespaceStatsRecord namespace_stats;
    namespace_stats.namespace_id = normalized.namespace_id;
    namespace_stats.generation_id = normalized.generation_id;
    namespace_stats.file_count = import_result.file_count;
    namespace_stats.total_file_bytes = import_result.total_file_bytes;
    namespace_stats.total_metadata_bytes = std::to_string(import_result.inode_pages_bytes);
    namespace_stats.avg_file_size_bytes = import_result.avg_file_size_bytes;
    namespace_stats.start_global_image_id = import_result.start_global_image_id;
    namespace_stats.end_global_image_id = import_result.end_global_image_id;
    namespace_stats.start_cursor = import_result.start_cursor;
    namespace_stats.end_cursor = import_result.end_cursor;

    const MasstreeClusterStatsRecord cluster_after =
        stats_store.BuildReplacedClusterStats(cluster_before,
                                             previous_namespace_stats_ptr,
                                             namespace_stats,
                                             import_result.end_cursor);

    bool had_previous_cluster_stats = false;
    std::string previous_cluster_stats_raw;
    if (!stats_store.LoadClusterStatsRaw(&had_previous_cluster_stats, &previous_cluster_stats_raw, error)) {
        cleanup();
        return false;
    }

    rocksdb::WriteBatch stats_batch;
    if (!stats_store.PutClusterStats(cluster_after, &stats_batch, error) ||
        !stats_store.PutNamespaceStats(namespace_stats, &stats_batch, error) ||
        !meta_store_->WriteBatch(&stats_batch, error)) {
        cleanup();
        return false;
    }

    MasstreeGenerationPublisher::PublishRequest publish_request;
    publish_request.masstree_root = normalized.masstree_root;
    publish_request.namespace_id = normalized.namespace_id;
    publish_request.generation_id = normalized.generation_id;
    publish_request.manifest_path = working_manifest_path;
    publish_request.publish_route = normalized.publish_route;

    std::string final_manifest_path;
    if (!publisher.Publish(publish_request, &final_manifest_path, error)) {
        rocksdb::WriteBatch rollback_batch;
        std::string rollback_error;
        if (stats_store.RestoreRawClusterStats(had_previous_cluster_stats,
                                               previous_cluster_stats_raw,
                                               &rollback_batch,
                                               &rollback_error) &&
            stats_store.RestoreRawNamespaceStats(normalized.namespace_id,
                                                had_previous_namespace_stats,
                                                previous_namespace_stats_raw,
                                                &rollback_batch,
                                                &rollback_error)) {
            meta_store_->WriteBatch(&rollback_batch, &rollback_error);
        }
        cleanup();
        return false;
    }

    if (result) {
        result->manifest_path = final_manifest_path;
        result->staging_dir = staging_dir;
        result->root_inode_id = root_inode_id;
        result->inode_min = inode_min;
        result->inode_max = inode_max;
        result->inode_count = inode_count;
        result->dentry_count = dentry_count;
        result->file_count = import_result.file_count;
        result->level1_dir_count = level1_dir_count_for_result;
        result->leaf_dir_count = leaf_dir_count_for_result;
        result->inode_pages_bytes = import_result.inode_pages_bytes;
        result->avg_file_size_bytes = import_result.avg_file_size_bytes;
        result->total_file_bytes = import_result.total_file_bytes;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeImportService::ReserveInodeRange(uint64_t inode_count,
                                              uint64_t* inode_start,
                                              std::string* error) const {
    if (!meta_store_ || !inode_start || inode_count == 0) {
        if (error) {
            *error = "invalid masstree inode range reservation args";
        }
        return false;
    }

    std::string value;
    uint64_t next_id = kRootInodeId + 1ULL;
    if (meta_store_->Get(NextInodeKey(), &value, error)) {
        if (!MetaCodec::DecodeUInt64(value, &next_id)) {
            if (error) {
                *error = "invalid next inode value";
            }
            return false;
        }
    } else if (error && !error->empty()) {
        return false;
    }

    *inode_start = next_id;
    const uint64_t new_next_id = next_id + inode_count;
    if (!meta_store_->Put(NextInodeKey(), MetaCodec::EncodeUInt64(new_next_id), error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeImportService::NormalizePathPrefix(const std::string& path_prefix,
                                                std::string* normalized,
                                                std::string* error) const {
    if (!NormalizePath(path_prefix, normalized)) {
        if (error) {
            *error = "invalid masstree path_prefix";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
