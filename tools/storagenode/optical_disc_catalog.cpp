#include "optical_disc_catalog.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <regex>
#include <sstream>
#include <utility>

namespace fs = std::filesystem;

namespace zb::storagenode {

namespace {

constexpr char kDeltaHeader[] = "optical_disc_catalog_delta_v1";
constexpr std::size_t kReadBufferRecords = 8192;

std::vector<std::string> SplitTabs(const std::string& line) {
    std::vector<std::string> fields;
    std::size_t begin = 0;
    for (;;) {
        const std::size_t tab = line.find('\t', begin);
        fields.push_back(line.substr(begin, tab == std::string::npos ? tab : tab - begin));
        if (tab == std::string::npos) {
            return fields;
        }
        begin = tab + 1;
    }
}

bool ParseUint64(const std::string& value, std::uint64_t* parsed) {
    if (!parsed || value.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const std::uint64_t result = std::stoull(value, &consumed);
        if (consumed != value.size()) {
            return false;
        }
        *parsed = result;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseDouble(const std::string& value, double* parsed) {
    if (!parsed || value.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const double result = std::stod(value, &consumed);
        if (consumed != value.size() || result < 0) {
            return false;
        }
        *parsed = result;
        return true;
    } catch (...) {
        return false;
    }
}

bool IsValidIdentifier(const std::string& value) {
    return !value.empty() && std::all_of(value.begin(), value.end(), [](unsigned char ch) {
        return std::isalnum(ch) != 0 || ch == '_' || ch == '-';
    });
}

OpticalDiscBin NormalizeBaselineDisc(OpticalDiscBin disc) {
    if (disc.capacity == 0) {
        disc.capacity = kDefaultOpticalDiscCapacityBytes;
    }
    return disc;
}

bool ValidateDisc(const OpticalDiscBin& disc, std::string* error) {
    const std::string device_id = FixedString(disc.device_id, sizeof(disc.device_id));
    const std::string library_id = FixedString(disc.library_id, sizeof(disc.library_id));
    if (!IsValidIdentifier(device_id)) {
        if (error) {
            *error = "invalid device_id: " + device_id;
        }
        return false;
    }
    if (!IsValidIdentifier(library_id)) {
        if (error) {
            *error = "invalid library_id: " + library_id;
        }
        return false;
    }
    if (disc.capacity == 0) {
        if (error) {
            *error = "disc capacity must be greater than zero";
        }
        return false;
    }
    if (!IsValidDiscStatus(disc.status)) {
        if (error) {
            *error = "invalid disc status";
        }
        return false;
    }
    if (disc.write_throughput_MBps < 0 || disc.read_throughput_MBps < 0) {
        if (error) {
            *error = "disc throughput must not be negative";
        }
        return false;
    }
    return true;
}

void AddStatistics(const OpticalDiscBin& disc, DiscStatistics* statistics) {
    ++statistics->disc_count;
    statistics->capacity_bytes += static_cast<long double>(disc.capacity);
    ++statistics->status_counts[static_cast<std::size_t>(disc.status)];
    statistics->total_write_throughput_MBps += disc.write_throughput_MBps;
    statistics->total_read_throughput_MBps += disc.read_throughput_MBps;
}

bool BuildDiscFromFields(const std::vector<std::string>& fields, OpticalDiscBin* disc, std::string* error) {
    if (!disc || fields.size() != 7) {
        if (error) {
            *error = "invalid ADD delta record";
        }
        return false;
    }
    OpticalDiscBin parsed{};
    if (!SetFixedString(parsed.device_id, sizeof(parsed.device_id), fields[1], error) ||
        !SetFixedString(parsed.library_id, sizeof(parsed.library_id), fields[2], error) ||
        !ParseUint64(fields[3], &parsed.capacity) ||
        !ParseDiscStatus(fields[4], &parsed.status) ||
        !ParseDouble(fields[5], &parsed.write_throughput_MBps) ||
        !ParseDouble(fields[6], &parsed.read_throughput_MBps)) {
        if (error && error->empty()) {
            *error = "invalid ADD delta fields";
        }
        return false;
    }
    if (!ValidateDisc(parsed, error)) {
        return false;
    }
    *disc = parsed;
    return true;
}

std::string AddDeltaLine(const OpticalDiscBin& disc) {
    std::ostringstream out;
    out << "ADD\t" << FixedString(disc.device_id, sizeof(disc.device_id))
        << '\t' << FixedString(disc.library_id, sizeof(disc.library_id))
        << '\t' << disc.capacity
        << '\t' << DiscStatusName(disc.status)
        << '\t' << disc.write_throughput_MBps
        << '\t' << disc.read_throughput_MBps;
    return out.str();
}

bool ParseGeneratedDiscLocation(const std::string& device_id, std::uint64_t* batch, std::uint64_t* offset) {
    constexpr std::size_t kPrefixSize = 5;
    constexpr std::size_t kDigitsSize = 10;
    if (!batch || !offset || device_id.size() != kPrefixSize + kDigitsSize ||
        device_id.compare(0, kPrefixSize, "disc_") != 0) {
        return false;
    }
    const std::string digits = device_id.substr(kPrefixSize);
    if (!std::all_of(digits.begin(), digits.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
        return false;
    }
    *batch = static_cast<std::uint64_t>(std::stoull(digits.substr(0, 5)));
    *offset = static_cast<std::uint64_t>(std::stoull(digits.substr(5, 5)));
    return true;
}

bool ParseGeneratedDiscOrdinal(const std::string& device_id, std::uint64_t* ordinal) {
    if (!ordinal) {
        return false;
    }
    std::uint64_t batch = 0;
    std::uint64_t offset = 0;
    if (!ParseGeneratedDiscLocation(device_id, &batch, &offset)) {
        return false;
    }
    constexpr std::uint64_t kDiscsPerBatch = 100000ULL;
    if (batch > (std::numeric_limits<std::uint64_t>::max() - offset) / kDiscsPerBatch) {
        return false;
    }
    *ordinal = batch * kDiscsPerBatch + offset;
    return true;
}

bool ParseExtraDiscId(const std::string& device_id, std::uint64_t* sequence) {
    constexpr const char* kPrefix = "disc_extra_";
    constexpr std::size_t kPrefixSize = 11;
    constexpr std::size_t kDigitsSize = 10;
    if (!sequence || device_id.size() != kPrefixSize + kDigitsSize ||
        device_id.compare(0, kPrefixSize, kPrefix) != 0) {
        return false;
    }
    const std::string digits = device_id.substr(kPrefixSize);
    if (!std::all_of(digits.begin(), digits.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
        return false;
    }
    *sequence = static_cast<std::uint64_t>(std::stoull(digits));
    return true;
}

std::string FormatExtraDiscId(std::uint64_t sequence) {
    std::ostringstream out;
    out << "disc_extra_" << std::setw(10) << std::setfill('0') << sequence;
    return out.str();
}

#if defined(__SIZEOF_INT128__)
std::string UInt128ToString(unsigned __int128 value) {
    if (value == 0) {
        return "0";
    }
    std::string out;
    while (value != 0) {
        out.push_back(static_cast<char>('0' + static_cast<unsigned int>(value % 10)));
        value /= 10;
    }
    std::reverse(out.begin(), out.end());
    return out;
}

std::string Int128AbsToString(__int128 value) {
    return UInt128ToString(value < 0 ? static_cast<unsigned __int128>(-value)
                                     : static_cast<unsigned __int128>(value));
}
#endif

} // namespace

std::string FixedString(const char* value, std::size_t size) {
    std::size_t count = 0;
    while (count < size && value[count] != '\0') {
        ++count;
    }
    return std::string(value, count);
}

bool SetFixedString(char* target, std::size_t size, const std::string& value, std::string* error) {
    if (!target || size == 0 || value.empty() || value.size() >= size || !IsValidIdentifier(value)) {
        if (error) {
            *error = "invalid identifier or identifier is too long: " + value;
        }
        return false;
    }
    std::memset(target, 0, size);
    std::memcpy(target, value.data(), value.size());
    return true;
}

const char* DiscStatusName(DiscStatus status) {
    switch (status) {
        case DiscStatus::Blank:
            return "blank";
        case DiscStatus::InUse:
            return "in_use";
        case DiscStatus::Recycled:
            return "recycled";
        case DiscStatus::Finalized:
            return "finalized";
        case DiscStatus::Lost:
            return "lost";
    }
    return "unknown";
}

bool ParseDiscStatus(const std::string& value, DiscStatus* status) {
    if (!status) {
        return false;
    }
    if (value == "blank") {
        *status = DiscStatus::Blank;
    } else if (value == "in_use") {
        *status = DiscStatus::InUse;
    } else if (value == "recycled") {
        *status = DiscStatus::Recycled;
    } else if (value == "finalized") {
        *status = DiscStatus::Finalized;
    } else if (value == "lost") {
        *status = DiscStatus::Lost;
    } else {
        return false;
    }
    return true;
}

bool IsValidDiscStatus(DiscStatus status) {
    return static_cast<std::size_t>(status) < kDiscStatusCount;
}

double BytesToTB(long double bytes) {
    constexpr long double kBytesPerTB = 1000.0L * 1000.0L * 1000.0L * 1000.0L;
    return static_cast<double>(bytes / kBytesPerTB);
}

OpticalDiscCatalog::OpticalDiscCatalog(fs::path disc_dir, fs::path delta_path)
    : disc_dir_(std::move(disc_dir)), delta_path_(std::move(delta_path)) {}

bool OpticalDiscCatalog::Load(CatalogLoadResult* result, std::string* error) {
    batch_files_.clear();
    batch_path_by_index_.clear();
    added_discs_.clear();
    deleted_discs_.clear();
    skipped_batch_file_count_ = 0;
    delta_operation_count_ = 0;
    if (!CollectBatchFiles(error) || !ReplayDeltaLog(error)) {
        return false;
    }
    if (result) {
        result->batch_file_count = batch_files_.size();
        result->skipped_batch_file_count = skipped_batch_file_count_;
        result->delta_operation_count = delta_operation_count_;
    }
    return true;
}

bool OpticalDiscCatalog::CollectBatchFiles(std::string* error) {
    std::error_code ec;
    if (!fs::exists(disc_dir_, ec) || !fs::is_directory(disc_dir_, ec)) {
        if (error) {
            *error = "disc directory not found: " + disc_dir_.string();
        }
        return false;
    }
    const std::regex pattern(R"(^disc_batch_(\d+)\.bin$)");
    for (const auto& entry : fs::directory_iterator(disc_dir_, ec)) {
        if (ec) {
            if (error) {
                *error = "failed to scan disc directory: " + ec.message();
            }
            return false;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string file_name = entry.path().filename().string();
        std::smatch match;
        if (!std::regex_match(file_name, match, pattern)) {
            continue;
        }
        const std::uintmax_t file_size = entry.file_size(ec);
        if (ec) {
            if (error) {
                *error = "failed to stat batch file: " + entry.path().string() + ": " + ec.message();
            }
            return false;
        }
        if (file_size % sizeof(OpticalDiscBin) != 0) {
            ++skipped_batch_file_count_;
            continue;
        }
        const std::uint64_t index = std::stoull(match[1].str());
        batch_files_.push_back({entry.path(), index});
        batch_path_by_index_[index] = entry.path();
    }
    std::sort(batch_files_.begin(), batch_files_.end(), [](const BatchFile& left, const BatchFile& right) {
        return left.batch_index < right.batch_index;
    });
    if (batch_files_.empty()) {
        if (error) {
            *error = "no disc_batch_*.bin files found in: " + disc_dir_.string();
        }
        return false;
    }
    return true;
}

bool OpticalDiscCatalog::ReplayDeltaLog(std::string* error) {
    std::error_code ec;
    if (!fs::exists(delta_path_, ec)) {
        return true;
    }
    std::ifstream input(delta_path_);
    if (!input) {
        if (error) {
            *error = "failed to open delta log: " + delta_path_.string();
        }
        return false;
    }
    std::string line;
    if (!std::getline(input, line) || line != kDeltaHeader) {
        if (error) {
            *error = "invalid delta log header: " + delta_path_.string();
        }
        return false;
    }
    std::size_t line_number = 1;
    while (std::getline(input, line)) {
        ++line_number;
        if (line.empty()) {
            continue;
        }
        const std::vector<std::string> fields = SplitTabs(line);
        if (fields.empty()) {
            continue;
        }
        if (fields[0] == "ADD") {
            OpticalDiscBin disc{};
            if (!BuildDiscFromFields(fields, &disc, error)) {
                if (error) {
                    *error += " at delta line " + std::to_string(line_number);
                }
                return false;
            }
            const std::string id = FixedString(disc.device_id, sizeof(disc.device_id));
            added_discs_[id] = disc;
            deleted_discs_.erase(id);
        } else if (fields[0] == "DELETE" && fields.size() == 2 && IsValidIdentifier(fields[1])) {
            added_discs_.erase(fields[1]);
            deleted_discs_.insert(fields[1]);
        } else {
            if (error) {
                *error = "invalid delta operation at line " + std::to_string(line_number);
            }
            return false;
        }
        ++delta_operation_count_;
    }
    return true;
}

bool OpticalDiscCatalog::ScanBaseline(const std::function<bool(const OpticalDiscBin&)>& visitor,
                                      std::string* error) const {
    std::vector<OpticalDiscBin> buffer(kReadBufferRecords);
    for (const auto& batch_file : batch_files_) {
        std::ifstream input(batch_file.path, std::ios::binary);
        if (!input) {
            if (error) {
                *error = "failed to open batch file: " + batch_file.path.string();
            }
            return false;
        }
        input.seekg(0, std::ios::end);
        const std::streamoff file_size = input.tellg();
        input.seekg(0, std::ios::beg);
        if (file_size < 0 || file_size % static_cast<std::streamoff>(sizeof(OpticalDiscBin)) != 0) {
            if (error) {
                *error = "invalid batch file size: " + batch_file.path.string();
            }
            return false;
        }
        while (input) {
            input.read(reinterpret_cast<char*>(buffer.data()),
                       static_cast<std::streamsize>(buffer.size() * sizeof(OpticalDiscBin)));
            const std::streamsize bytes_read = input.gcount();
            if (bytes_read % static_cast<std::streamsize>(sizeof(OpticalDiscBin)) != 0) {
                if (error) {
                    *error = "incomplete disc record in batch file: " + batch_file.path.string();
                }
                return false;
            }
            const std::size_t record_count = static_cast<std::size_t>(bytes_read) / sizeof(OpticalDiscBin);
            for (std::size_t index = 0; index < record_count; ++index) {
                if (!visitor(NormalizeBaselineDisc(buffer[index]))) {
                    return true;
                }
            }
        }
        if (input.bad()) {
            if (error) {
                *error = "failed to read batch file: " + batch_file.path.string();
            }
            return false;
        }
    }
    return true;
}

bool OpticalDiscCatalog::ComputeStatistics(DiscStatistics* global,
                                           std::vector<LibraryStatistics>* libraries,
                                           std::string* error) const {
    if (!global || !libraries) {
        if (error) {
            *error = "statistics output is null";
        }
        return false;
    }
    *global = {};
    std::unordered_map<std::string, DiscStatistics> by_library;
    bool invalid_record = false;
    const bool scan_ok = ScanBaseline([&](const OpticalDiscBin& disc) {
        if (!IsValidDiscStatus(disc.status)) {
            invalid_record = true;
            return false;
        }
        const std::string id = FixedString(disc.device_id, sizeof(disc.device_id));
        if (deleted_discs_.count(id) != 0) {
            return true;
        }
        OpticalDiscBin effective_disc = disc;
        ApplyUsageOverlay(&effective_disc);
        const std::string library_id = FixedString(effective_disc.library_id, sizeof(effective_disc.library_id));
        AddStatistics(effective_disc, global);
        AddStatistics(effective_disc, &by_library[library_id]);
        return true;
    }, error);
    if (!scan_ok) {
        return false;
    }
    if (invalid_record) {
        if (error) {
            *error = "invalid disc status found in baseline batch files";
        }
        return false;
    }
    for (const auto& item : added_discs_) {
        OpticalDiscBin effective_disc = item.second;
        ApplyUsageOverlay(&effective_disc);
        AddStatistics(effective_disc, global);
        AddStatistics(effective_disc, &by_library[FixedString(effective_disc.library_id, sizeof(effective_disc.library_id))]);
    }
    libraries->clear();
    libraries->reserve(by_library.size());
    for (auto& item : by_library) {
        libraries->push_back({item.first, item.second});
    }
    std::sort(libraries->begin(), libraries->end(), [](const LibraryStatistics& left, const LibraryStatistics& right) {
        return left.library_id < right.library_id;
    });
    return true;
}

bool OpticalDiscCatalog::FindBaselineDisc(const std::string& device_id,
                                          OpticalDiscBin* disc,
                                          bool* found,
                                          std::string* error) const {
    if (!found) {
        if (error) {
            *error = "found output is null";
        }
        return false;
    }
    *found = false;
    std::uint64_t batch = 0;
    std::uint64_t offset = 0;
    if (ParseGeneratedDiscLocation(device_id, &batch, &offset)) {
        const auto path_it = batch_path_by_index_.find(batch);
        if (path_it == batch_path_by_index_.end()) {
            return true;
        }
        std::ifstream input(path_it->second, std::ios::binary);
        if (!input) {
            if (error) {
                *error = "failed to open batch file: " + path_it->second.string();
            }
            return false;
        }
        input.seekg(static_cast<std::streamoff>(offset * sizeof(OpticalDiscBin)), std::ios::beg);
        OpticalDiscBin candidate{};
        input.read(reinterpret_cast<char*>(&candidate), sizeof(candidate));
        if (!input) {
            return true;
        }
        if (FixedString(candidate.device_id, sizeof(candidate.device_id)) == device_id) {
            if (disc) {
                *disc = NormalizeBaselineDisc(candidate);
                ApplyUsageOverlay(disc);
            }
            *found = true;
        }
        return true;
    }
    // Batch files are generated by generate_discs and therefore use the
    // disc_<batch><offset> identifier layout. Avoid a multi-gigabyte scan for
    // arbitrary identifiers used by catalog additions.
    return true;
}

bool OpticalDiscCatalog::GetDisc(const std::string& device_id,
                                 OpticalDiscBin* disc,
                                 bool* found,
                                 std::string* error) const {
    if (!found || !IsValidIdentifier(device_id)) {
        if (error) {
            *error = "invalid device_id: " + device_id;
        }
        return false;
    }
    *found = false;
    if (deleted_discs_.count(device_id) != 0) {
        return true;
    }
    const auto added_it = added_discs_.find(device_id);
    if (added_it != added_discs_.end()) {
        if (disc) {
            *disc = added_it->second;
            ApplyUsageOverlay(disc);
        }
        *found = true;
        return true;
    }
    return FindBaselineDisc(device_id, disc, found, error);
}

bool OpticalDiscCatalog::ListDiscs(const std::string& library_id,
                                   std::uint64_t offset,
                                   std::uint64_t limit,
                                   std::vector<OpticalDiscBin>* discs,
                                   std::string* error) const {
    if (!discs || !IsValidIdentifier(library_id)) {
        if (error) {
            *error = "invalid library_id: " + library_id;
        }
        return false;
    }
    discs->clear();
    std::uint64_t matched = 0;
    bool full = false;
    if (!ScanBaseline([&](const OpticalDiscBin& disc) {
            OpticalDiscBin effective_disc = disc;
            ApplyUsageOverlay(&effective_disc);
            if (FixedString(effective_disc.library_id, sizeof(effective_disc.library_id)) != library_id ||
                deleted_discs_.count(FixedString(effective_disc.device_id, sizeof(effective_disc.device_id))) != 0) {
                return true;
            }
            if (matched++ >= offset && discs->size() < limit) {
                discs->push_back(effective_disc);
            }
            full = discs->size() >= limit;
            return !full;
        }, error)) {
        return false;
    }
    if (full) {
        return true;
    }
    std::vector<OpticalDiscBin> additions;
    for (const auto& item : added_discs_) {
        OpticalDiscBin effective_disc = item.second;
        ApplyUsageOverlay(&effective_disc);
        if (FixedString(effective_disc.library_id, sizeof(effective_disc.library_id)) == library_id) {
            additions.push_back(effective_disc);
        }
    }
    std::sort(additions.begin(), additions.end(), [](const OpticalDiscBin& left, const OpticalDiscBin& right) {
        return FixedString(left.device_id, sizeof(left.device_id)) < FixedString(right.device_id, sizeof(right.device_id));
    });
    for (const auto& disc : additions) {
        if (matched++ >= offset && discs->size() < limit) {
            discs->push_back(disc);
        }
    }
    return true;
}

bool OpticalDiscCatalog::GetDiscUsage(const std::string& library_id,
                                      const std::string& device_id,
                                      DiscUsageView* usage,
                                      bool* found,
                                      std::string* error) const {
    if (!usage || !found || !IsValidIdentifier(library_id) || !IsValidIdentifier(device_id)) {
        if (error) {
            *error = "invalid library_id or device_id";
        }
        return false;
    }
    *usage = {};
    *found = false;
    OpticalDiscBin disc{};
    bool disc_found = false;
    if (!GetDisc(device_id, &disc, &disc_found, error)) {
        return false;
    }
    if (!disc_found) {
        return true;
    }
    if (FixedString(disc.library_id, sizeof(disc.library_id)) != library_id) {
        return true;
    }
    usage->disc = disc;
    usage->total_bytes = disc.capacity;
    usage->used_bytes = EstimateUsedBytes(disc);
    usage->free_bytes = usage->total_bytes > usage->used_bytes
                            ? usage->total_bytes - usage->used_bytes
                            : 0;
    *found = true;
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalDiscCatalog::GetLibraryUsage(const std::string& library_id,
                                         LibraryUsageView* usage,
                                         std::string* error) const {
    if (!usage || !IsValidIdentifier(library_id)) {
        if (error) {
            *error = "invalid library_id: " + library_id;
        }
        return false;
    }
    *usage = {};
    usage->library_id = library_id;
#if defined(__SIZEOF_INT128__)
    unsigned __int128 total_bytes = 0;
    unsigned __int128 used_bytes = 0;
#else
    long double total_bytes = 0;
    long double used_bytes = 0;
#endif
    bool invalid_record = false;
    const bool scan_ok = ScanBaseline([&](const OpticalDiscBin& disc) {
        if (!IsValidDiscStatus(disc.status)) {
            invalid_record = true;
            return false;
        }
        OpticalDiscBin effective_disc = disc;
        ApplyUsageOverlay(&effective_disc);
        const std::string id = FixedString(effective_disc.device_id, sizeof(effective_disc.device_id));
        if (deleted_discs_.count(id) != 0 ||
            FixedString(effective_disc.library_id, sizeof(effective_disc.library_id)) != library_id) {
            return true;
        }
        const std::uint64_t used = EstimateUsedBytes(effective_disc);
        ++usage->disc_count;
        ++usage->status_counts[static_cast<std::size_t>(effective_disc.status)];
        total_bytes += effective_disc.capacity;
        used_bytes += used;
        return true;
    }, error);
    if (!scan_ok) {
        return false;
    }
    if (invalid_record) {
        if (error) {
            *error = "invalid disc status found in baseline batch files";
        }
        return false;
    }
    for (const auto& item : added_discs_) {
        OpticalDiscBin effective_disc = item.second;
        ApplyUsageOverlay(&effective_disc);
        if (FixedString(effective_disc.library_id, sizeof(effective_disc.library_id)) != library_id) {
            continue;
        }
        const std::uint64_t used = EstimateUsedBytes(effective_disc);
        ++usage->disc_count;
        ++usage->status_counts[static_cast<std::size_t>(effective_disc.status)];
        total_bytes += effective_disc.capacity;
        used_bytes += used;
    }
#if defined(__SIZEOF_INT128__)
    const unsigned __int128 free_bytes = total_bytes > used_bytes ? total_bytes - used_bytes : 0;
    usage->total_bytes = UInt128ToString(total_bytes);
    usage->used_bytes = UInt128ToString(used_bytes);
    usage->free_bytes = UInt128ToString(free_bytes);
#else
    const long double free_bytes = total_bytes > used_bytes ? total_bytes - used_bytes : 0;
    usage->total_bytes = std::to_string(static_cast<std::uint64_t>(total_bytes));
    usage->used_bytes = std::to_string(static_cast<std::uint64_t>(used_bytes));
    usage->free_bytes = std::to_string(static_cast<std::uint64_t>(free_bytes));
#endif
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalDiscCatalog::ComputeCapacityAdjustment(DiscCapacityAdjustment* adjustment,
                                                  std::string* error) const {
    if (!adjustment) {
        if (error) {
            *error = "capacity adjustment output is null";
        }
        return false;
    }
    *adjustment = {};
#if defined(__SIZEOF_INT128__)
    __int128 capacity_delta = 0;
#else
    long double capacity_delta = 0;
#endif
    for (const auto& item : added_discs_) {
        ++adjustment->disc_delta_count;
        capacity_delta += item.second.capacity;
    }
    for (const auto& id : deleted_discs_) {
        OpticalDiscBin baseline{};
        bool found = false;
        if (!FindBaselineDisc(id, &baseline, &found, error)) {
            return false;
        }
        if (!found) {
            continue;
        }
        --adjustment->disc_delta_count;
        capacity_delta -= baseline.capacity;
    }
#if defined(__SIZEOF_INT128__)
    adjustment->capacity_delta_negative = capacity_delta < 0;
    adjustment->capacity_delta_abs_bytes = Int128AbsToString(capacity_delta);
#else
    adjustment->capacity_delta_negative = capacity_delta < 0;
    adjustment->capacity_delta_abs_bytes =
        std::to_string(static_cast<std::uint64_t>(capacity_delta < 0 ? -capacity_delta : capacity_delta));
#endif
    if (error) {
        error->clear();
    }
    return true;
}

bool OpticalDiscCatalog::NextAddedDiscId(std::string* device_id, std::string* error) const {
    if (!device_id) {
        if (error) {
            *error = "device_id output is null";
        }
        return false;
    }
    std::uint64_t max_sequence = 0;
    for (const auto& item : added_discs_) {
        std::uint64_t sequence = 0;
        if (ParseExtraDiscId(item.first, &sequence)) {
            max_sequence = std::max(max_sequence, sequence);
        }
    }
    for (const auto& id : deleted_discs_) {
        std::uint64_t sequence = 0;
        if (ParseExtraDiscId(id, &sequence)) {
            max_sequence = std::max(max_sequence, sequence);
        }
    }
    if (max_sequence == std::numeric_limits<std::uint64_t>::max()) {
        if (error) {
            *error = "extra disc id sequence is exhausted";
        }
        return false;
    }
    *device_id = FormatExtraDiscId(max_sequence + 1);
    if (error) {
        error->clear();
    }
    return true;
}

void OpticalDiscCatalog::SetUsageOverlay(const DiscUsageOverlay& overlay) {
    usage_overlay_ = overlay;
}

void OpticalDiscCatalog::ClearUsageOverlay() {
    usage_overlay_ = {};
}

std::uint64_t OpticalDiscCatalog::EstimateUsedBytes(const OpticalDiscBin& disc) const {
    if (!usage_overlay_.enabled) {
        return disc.status == DiscStatus::InUse ? disc.capacity : 0;
    }
    const std::string id = FixedString(disc.device_id, sizeof(disc.device_id));
    std::uint64_t ordinal = 0;
    if (!ParseGeneratedDiscOrdinal(id, &ordinal)) {
        return disc.status == DiscStatus::InUse ? disc.capacity : 0;
    }
    if (usage_overlay_.used_disc_count == 0 || ordinal >= usage_overlay_.used_disc_count) {
        return 0;
    }
    const std::uint64_t cursor_ordinal =
        static_cast<std::uint64_t>(usage_overlay_.cursor_node_index) *
            static_cast<std::uint64_t>(usage_overlay_.discs_per_node) +
        static_cast<std::uint64_t>(usage_overlay_.cursor_disk_index);
    if (ordinal < cursor_ordinal || ordinal + 1 < usage_overlay_.used_disc_count) {
        return disc.capacity;
    }
    if (ordinal > cursor_ordinal) {
        return 0;
    }
    const std::uint64_t image_bytes =
        static_cast<std::uint64_t>(usage_overlay_.cursor_image_index) *
            usage_overlay_.image_capacity_bytes +
        usage_overlay_.cursor_image_used_bytes;
    return std::min<std::uint64_t>(disc.capacity, image_bytes);
}

void OpticalDiscCatalog::ApplyUsageOverlay(OpticalDiscBin* disc) const {
    if (!disc || !usage_overlay_.enabled) {
        return;
    }
    const std::string id = FixedString(disc->device_id, sizeof(disc->device_id));
    std::uint64_t ordinal = 0;
    if (!ParseGeneratedDiscOrdinal(id, &ordinal)) {
        return;
    }
    if (ordinal < usage_overlay_.used_disc_count) {
        disc->status = DiscStatus::InUse;
    } else if (disc->status == DiscStatus::InUse) {
        disc->status = DiscStatus::Blank;
    }
}

bool OpticalDiscCatalog::AppendDeltaLine(const std::string& line, std::string* error) const {
    std::error_code ec;
    const fs::path parent = delta_path_.parent_path();
    if (!parent.empty()) {
        fs::create_directories(parent, ec);
        if (ec) {
            if (error) {
                *error = "failed to create delta directory: " + ec.message();
            }
            return false;
        }
    }
    const bool create_header = !fs::exists(delta_path_, ec) || fs::file_size(delta_path_, ec) == 0;
    std::ofstream output(delta_path_, std::ios::app);
    if (!output) {
        if (error) {
            *error = "failed to append delta log: " + delta_path_.string();
        }
        return false;
    }
    if (create_header) {
        output << kDeltaHeader << '\n';
    }
    output << line << '\n';
    output.flush();
    if (!output) {
        if (error) {
            *error = "failed to flush delta log: " + delta_path_.string();
        }
        return false;
    }
    return true;
}

bool OpticalDiscCatalog::AddDisc(const OpticalDiscBin& disc, std::string* error) {
    if (!ValidateDisc(disc, error)) {
        return false;
    }
    const std::string id = FixedString(disc.device_id, sizeof(disc.device_id));
    OpticalDiscBin existing{};
    bool found = false;
    if (!GetDisc(id, &existing, &found, error)) {
        return false;
    }
    if (found) {
        if (error) {
            *error = "disc already exists: " + id;
        }
        return false;
    }
    bool baseline_found = false;
    if (!FindBaselineDisc(id, nullptr, &baseline_found, error)) {
        return false;
    }
    if (baseline_found) {
        if (error) {
            *error = "cannot re-add deleted baseline disc: " + id;
        }
        return false;
    }
    if (!AppendDeltaLine(AddDeltaLine(disc), error)) {
        return false;
    }
    added_discs_[id] = disc;
    deleted_discs_.erase(id);
    ++delta_operation_count_;
    return true;
}

bool OpticalDiscCatalog::DeleteDisc(const std::string& device_id, std::string* error) {
    OpticalDiscBin existing{};
    bool found = false;
    if (!GetDisc(device_id, &existing, &found, error)) {
        return false;
    }
    if (!found) {
        if (error) {
            *error = "disc not found: " + device_id;
        }
        return false;
    }
    if (!AppendDeltaLine("DELETE\t" + device_id, error)) {
        return false;
    }
    added_discs_.erase(device_id);
    deleted_discs_.insert(device_id);
    ++delta_operation_count_;
    return true;
}

} // namespace zb::storagenode
