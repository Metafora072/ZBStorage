#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace zb::storagenode {

constexpr std::uint64_t kDefaultOpticalDiscCapacityBytes = 2ULL * 1000ULL * 1000ULL * 1000ULL * 1000ULL;
constexpr double kDefaultOpticalDiscReadMbps = 36.0;
constexpr double kDefaultOpticalDiscWriteMbps = 36.0;
constexpr std::size_t kDiscStatusCount = 5;

enum class DiscStatus : std::uint32_t {
    Blank = 0,
    InUse = 1,
    Recycled = 2,
    Finalized = 3,
    Lost = 4,
};

struct OpticalDiscBin {
    char device_id[32];
    char library_id[16];
    std::uint64_t capacity;
    DiscStatus status;
    double write_throughput_MBps;
    double read_throughput_MBps;
};

static_assert(sizeof(DiscStatus) == 4, "DiscStatus size mismatch");
static_assert(std::is_standard_layout<OpticalDiscBin>::value, "OpticalDiscBin must be standard layout");
static_assert(sizeof(OpticalDiscBin) == 80, "OpticalDiscBin size mismatch");

struct DiscStatistics {
    std::uint64_t disc_count{0};
    long double capacity_bytes{0};
    std::array<std::uint64_t, kDiscStatusCount> status_counts{};
    long double total_write_throughput_MBps{0};
    long double total_read_throughput_MBps{0};
};

struct LibraryStatistics {
    std::string library_id;
    DiscStatistics statistics;
};

struct CatalogLoadResult {
    std::size_t batch_file_count{0};
    std::size_t skipped_batch_file_count{0};
    std::size_t delta_operation_count{0};
};

struct DiscUsageOverlay {
    bool enabled{false};
    std::uint64_t used_disc_count{0};
    std::uint32_t cursor_node_index{0};
    std::uint32_t cursor_disk_index{0};
    std::uint32_t cursor_image_index{0};
    std::uint64_t cursor_image_used_bytes{0};
    std::uint32_t discs_per_node{10000};
    std::uint64_t image_capacity_bytes{100000000000ULL};
};

struct DiscUsageView {
    OpticalDiscBin disc{};
    std::uint64_t total_bytes{0};
    std::uint64_t used_bytes{0};
    std::uint64_t free_bytes{0};
};

struct LibraryUsageView {
    std::string library_id;
    std::uint64_t disc_count{0};
    std::string total_bytes{"0"};
    std::string used_bytes{"0"};
    std::string free_bytes{"0"};
    std::array<std::uint64_t, kDiscStatusCount> status_counts{};
};

struct DiscCapacityAdjustment {
    std::int64_t disc_delta_count{0};
    bool capacity_delta_negative{false};
    std::string capacity_delta_abs_bytes{"0"};
};

std::string FixedString(const char* value, std::size_t size);
bool SetFixedString(char* target, std::size_t size, const std::string& value, std::string* error);
const char* DiscStatusName(DiscStatus status);
bool ParseDiscStatus(const std::string& value, DiscStatus* status);
bool IsValidDiscStatus(DiscStatus status);
double BytesToTB(long double bytes);

class OpticalDiscCatalog {
public:
    OpticalDiscCatalog(std::filesystem::path disc_dir, std::filesystem::path delta_path);

    bool Load(CatalogLoadResult* result, std::string* error);
    bool ComputeStatistics(DiscStatistics* global,
                           std::vector<LibraryStatistics>* libraries,
                           std::string* error) const;
    bool GetDisc(const std::string& device_id, OpticalDiscBin* disc, bool* found, std::string* error) const;
    bool ListDiscs(const std::string& library_id,
                   std::uint64_t offset,
                   std::uint64_t limit,
                   std::vector<OpticalDiscBin>* discs,
                   std::string* error) const;
    bool GetDiscUsage(const std::string& library_id,
                      const std::string& device_id,
                      DiscUsageView* usage,
                      bool* found,
                      std::string* error) const;
    bool GetLibraryUsage(const std::string& library_id,
                         LibraryUsageView* usage,
                         std::string* error) const;
    bool ComputeCapacityAdjustment(DiscCapacityAdjustment* adjustment,
                                   std::string* error) const;
    bool NextAddedDiscId(std::string* device_id, std::string* error) const;
    void SetUsageOverlay(const DiscUsageOverlay& overlay);
    void ClearUsageOverlay();
    bool AddDisc(const OpticalDiscBin& disc, std::string* error);
    bool DeleteDisc(const std::string& device_id, std::string* error);

    const std::filesystem::path& disc_dir() const { return disc_dir_; }
    const std::filesystem::path& delta_path() const { return delta_path_; }
    std::size_t batch_file_count() const { return batch_files_.size(); }
    std::size_t skipped_batch_file_count() const { return skipped_batch_file_count_; }
    std::size_t delta_operation_count() const { return delta_operation_count_; }

private:
    struct BatchFile {
        std::filesystem::path path;
        std::uint64_t batch_index{0};
    };

    bool CollectBatchFiles(std::string* error);
    bool ReplayDeltaLog(std::string* error);
    bool FindBaselineDisc(const std::string& device_id, OpticalDiscBin* disc, bool* found, std::string* error) const;
    bool ScanBaseline(const std::function<bool(const OpticalDiscBin&)>& visitor, std::string* error) const;
    std::uint64_t EstimateUsedBytes(const OpticalDiscBin& disc) const;
    void ApplyUsageOverlay(OpticalDiscBin* disc) const;
    bool AppendDeltaLine(const std::string& line, std::string* error) const;

    std::filesystem::path disc_dir_;
    std::filesystem::path delta_path_;
    std::vector<BatchFile> batch_files_;
    std::unordered_map<std::uint64_t, std::filesystem::path> batch_path_by_index_;
    std::unordered_map<std::string, OpticalDiscBin> added_discs_;
    std::unordered_set<std::string> deleted_discs_;
    DiscUsageOverlay usage_overlay_;
    std::size_t skipped_batch_file_count_{0};
    std::size_t delta_operation_count_{0};
};

} // namespace zb::storagenode
