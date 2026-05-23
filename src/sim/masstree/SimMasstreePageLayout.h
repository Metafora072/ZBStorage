#pragma once

#include "sim/masstree/SimMasstreeTypes.h"

#include <filesystem>

namespace zb::sim::masstree {

constexpr uint32_t kSimMasstreePageTargetEntries = 256;

bool WriteInodePages(const std::filesystem::path& pages_path,
                     const std::filesystem::path& sparse_path,
                     const std::vector<SimInodeRecord>& records,
                     uint64_t* page_bytes,
                     std::string* error);

bool WriteDentryPages(const std::filesystem::path& pages_path,
                      const std::filesystem::path& sparse_path,
                      const std::vector<SimDentryRecord>& records,
                      uint64_t* page_bytes,
                      std::string* error);

bool ReadInodePageAt(const std::filesystem::path& pages_path,
                     uint64_t page_offset,
                     std::vector<SimInodeRecord>* records,
                     std::string* error);

bool ReadDentryPageAt(const std::filesystem::path& pages_path,
                      uint64_t page_offset,
                      std::vector<SimDentryRecord>* records,
                      std::string* error);

bool LoadInodeSparse(const std::filesystem::path& sparse_path,
                     std::vector<SimInodeSparseEntry>* entries,
                     std::string* error);

bool LoadDentrySparse(const std::filesystem::path& sparse_path,
                      std::vector<SimDentrySparseEntry>* entries,
                      std::string* error);

bool DentryKeyLess(uint64_t lhs_parent,
                   const std::string& lhs_name,
                   uint64_t rhs_parent,
                   const std::string& rhs_name);

} // namespace zb::sim::masstree
