#pragma once

#include "sim/masstree/SimMasstreeIndexRuntime.h"
#include "sim/masstree/SimMasstreeTypes.h"

#include <filesystem>
#include <random>
#include <unordered_map>

namespace zb::sim::masstree {

struct TemplateRequest {
    std::string template_id;
    std::string path_list_file;
    uint64_t target_file_count{0};
    uint64_t default_file_size_bytes{100ULL * 1024ULL * 1024ULL};
    std::string repeat_dir_prefix{"copy"};
    bool leaf_nodes_are_files{false};
    uint32_t max_files_per_leaf_dir{2048};
    uint32_t max_subdirs_per_dir{256};
};

struct TemplateResult {
    SimMasstreeManifest manifest;
};

struct NamespaceImportRequest {
    std::string template_id;
    std::string namespace_id;
    std::string generation_id;
    std::string path_prefix;
    uint64_t inode_start{0};
};

struct NamespaceImportResult {
    SimMasstreeManifest manifest;
};

struct QueryResult {
    bool ok{false};
    uint64_t latency_ms{0};
    std::string query_mode;
    std::string namespace_id;
    std::string full_path;
    uint64_t inode_id{0};
    SimInodeRecord record;
    SimOpticalLocation optical_location;
    std::vector<SimDentryRecord> dentries;
    bool has_more{false};
    std::string next_token;
    std::string error;
};

class SimMasstreeStore {
public:
    bool Init(const std::filesystem::path& root, std::string* error);

    bool GenerateTemplate(const TemplateRequest& request, TemplateResult* result, std::string* error);
    bool ImportNamespace(const NamespaceImportRequest& request, NamespaceImportResult* result, std::string* error);
    bool Query(const std::string& namespace_id,
               const std::string& query_mode,
               const std::string& query_path,
               uint64_t query_inode_id,
               uint32_t readdir_limit,
               bool sleep_for_latency,
               QueryResult* result,
               std::string* error);
    SimMasstreeStats Stats() const;

private:
    bool LoadCatalog(std::string* error);
    bool SaveCatalog(std::string* error) const;
    bool LoadNamespaceIndex(const std::string& namespace_id, const SimMasstreeManifest& manifest, std::string* error);
    bool LoadAllInodes(const SimMasstreeManifest& manifest, std::vector<SimInodeRecord>* records, std::string* error) const;
    bool LoadAllDentries(const SimMasstreeManifest& manifest, std::vector<SimDentryRecord>* records, std::string* error) const;
    bool ReserveInodeRange(uint64_t inode_count, uint64_t requested_start, uint64_t* inode_start, std::string* error);
    bool LoadOpticalCursor(SimOpticalCursor* cursor, std::string* error) const;
    bool SaveOpticalCursor(const SimOpticalCursor& cursor, std::string* error) const;
    bool LookupInode(const std::string& namespace_id, uint64_t inode_id, SimInodeRecord* record, std::string* error) const;
    bool LookupPath(const std::string& namespace_id, const std::string& full_path, SimInodeRecord* record, std::string* error) const;
    bool BuildFullPath(const std::string& namespace_id, uint64_t inode_id, std::string* full_path, std::string* error) const;
    bool Readdir(const std::string& namespace_id,
                 const std::string& full_path,
                 uint32_t limit,
                 std::vector<SimDentryRecord>* entries,
                 bool* has_more,
                 std::string* next_token,
                 std::string* error) const;
    bool ReadSamples(const SimMasstreeManifest& manifest, std::vector<std::string>* samples, std::string* error) const;

    std::filesystem::path root_;
    std::filesystem::path masstree_root_;
    std::filesystem::path catalog_path_;
    std::filesystem::path next_inode_path_;
    std::filesystem::path optical_cursor_path_;
    std::unordered_map<std::string, SimMasstreeManifest> namespaces_;
    SimMasstreeIndexRuntime index_;
    mutable std::mt19937_64 rng_{std::random_device{}()};
};

} // namespace zb::sim::masstree
