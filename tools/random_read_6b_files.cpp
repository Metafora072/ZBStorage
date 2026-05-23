#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr int kTopDirBegin = 2;
constexpr int kTopDirEnd = 51;
constexpr int kDirFanout = 100;
constexpr int kFilesPerLeaf = 120;
constexpr uint64_t kDefaultFileSize = 16ULL * 1024ULL;

struct Options {
    fs::path root;
    uint64_t count{100};
    uint64_t file_size{kDefaultFileSize};
    std::optional<uint64_t> seed;
    bool continue_on_error{false};
    bool verbose{false};
    bool help{false};
};

struct ReadResult {
    uint64_t bytes_read{0};
    uint64_t elapsed_ns{0};
};

enum class ReadStatus {
    kOk,
    kNotFound,
    kError,
};

void PrintUsage(const char* program) {
    std::cout
        << "Usage: " << program << " <root> [-n COUNT] [--file-size BYTES] [--seed N]\n"
        << "       " << program << " <root> [--continue-on-error] [--verbose]\n\n"
        << "Randomly read files from a 6-billion-file vdb directory tree and report latency.\n\n"
        << "Directory layout:\n"
        << "  <root>/dir002..dir051/vdb.1_1.dir..vdb.1_100.dir/\n"
        << "    vdb.2_1.dir..vdb.2_100.dir/vdb.3_1.dir..vdb.3_100.dir/\n"
        << "      vdb_f0001.file..vdb_f0120.file\n\n"
        << "Options:\n"
        << "  -n, --count COUNT       Number of random files to read, default: 100\n"
        << "  --file-size BYTES       Bytes to read from each file, default: 16384\n"
        << "  --seed N                Random seed for reproducible path selection\n"
        << "  --continue-on-error     Continue after read errors and report failed samples\n"
        << "  --verbose               Print every sampled file path and latency\n"
        << "  -h, --help              Show this help\n";
}

bool ParsePositiveU64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0' || value == 0ULL) {
        return false;
    }
    *out = static_cast<uint64_t>(value);
    return true;
}

bool ParseU64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long value = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0') {
        return false;
    }
    *out = static_cast<uint64_t>(value);
    return true;
}

bool ParseArgs(int argc, char* argv[], Options* options) {
    if (!options) {
        return false;
    }
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const std::string& name) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << name << " requires a value\n";
                return nullptr;
            }
            return argv[++i];
        };

        if (arg == "-h" || arg == "--help") {
            options->help = true;
            return true;
        }
        if (arg == "-n" || arg == "--count") {
            const char* value = require_value(arg);
            if (!value || !ParsePositiveU64(value, &options->count)) {
                std::cerr << "invalid positive integer for " << arg << ": "
                          << (value ? value : "") << "\n";
                return false;
            }
            continue;
        }
        if (arg.rfind("--count=", 0) == 0) {
            if (!ParsePositiveU64(arg.substr(8), &options->count)) {
                std::cerr << "invalid positive integer for --count: " << arg.substr(8) << "\n";
                return false;
            }
            continue;
        }
        if (arg == "--file-size") {
            const char* value = require_value(arg);
            if (!value || !ParsePositiveU64(value, &options->file_size)) {
                std::cerr << "invalid positive integer for " << arg << ": "
                          << (value ? value : "") << "\n";
                return false;
            }
            continue;
        }
        if (arg.rfind("--file-size=", 0) == 0) {
            if (!ParsePositiveU64(arg.substr(12), &options->file_size)) {
                std::cerr << "invalid positive integer for --file-size: " << arg.substr(12) << "\n";
                return false;
            }
            continue;
        }
        if (arg == "--seed") {
            const char* value = require_value(arg);
            uint64_t parsed = 0;
            if (!value || !ParseU64(value, &parsed)) {
                std::cerr << "invalid integer for " << arg << ": " << (value ? value : "") << "\n";
                return false;
            }
            options->seed = parsed;
            continue;
        }
        if (arg.rfind("--seed=", 0) == 0) {
            uint64_t parsed = 0;
            if (!ParseU64(arg.substr(7), &parsed)) {
                std::cerr << "invalid integer for --seed: " << arg.substr(7) << "\n";
                return false;
            }
            options->seed = parsed;
            continue;
        }
        if (arg == "--continue-on-error") {
            options->continue_on_error = true;
            continue;
        }
        if (arg == "--verbose") {
            options->verbose = true;
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            std::cerr << "unknown option: " << arg << "\n";
            return false;
        }
        if (!options->root.empty()) {
            std::cerr << "multiple root directories provided: " << options->root.string()
                      << " and " << arg << "\n";
            return false;
        }
        options->root = arg;
    }

    if (options->help) {
        return true;
    }
    if (options->root.empty()) {
        std::cerr << "root directory is required\n";
        return false;
    }
    return true;
}

std::string FormatDirName(const std::string& prefix, int value, int width) {
    std::ostringstream oss;
    oss << prefix << std::setw(width) << std::setfill('0') << value;
    return oss.str();
}

fs::path BuildRandomPath(const fs::path& root, std::mt19937_64* rng) {
    std::uniform_int_distribution<int> top_dist(kTopDirBegin, kTopDirEnd);
    std::uniform_int_distribution<int> dir_dist(1, kDirFanout);
    std::uniform_int_distribution<int> file_dist(1, kFilesPerLeaf);

    const int top = top_dist(*rng);
    const int level1 = dir_dist(*rng);
    const int level2 = dir_dist(*rng);
    const int level3 = dir_dist(*rng);
    const int file_index = file_dist(*rng);

    std::ostringstream leaf_file;
    leaf_file << "vdb_f" << std::setw(4) << std::setfill('0') << file_index << ".file";

    return root / FormatDirName("dir", top, 3) / ("vdb.1_" + std::to_string(level1) + ".dir") /
           ("vdb.2_" + std::to_string(level2) + ".dir") /
           ("vdb.3_" + std::to_string(level3) + ".dir") / leaf_file.str();
}

ReadStatus ReadFile(const fs::path& path, uint64_t expected_size, ReadResult* result, std::string* error) {
    if (!result) {
        if (error) {
            *error = "result output is null";
        }
        return ReadStatus::kError;
    }
    std::error_code ec;
    if (!fs::exists(path, ec)) {
        if (error) {
            error->clear();
        }
        return ReadStatus::kNotFound;
    }
    if (ec) {
        if (error) {
            *error = "failed to check file existence: " + ec.message();
        }
        return ReadStatus::kError;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        if (error) {
            *error = "failed to open file";
        }
        return ReadStatus::kError;
    }

    std::vector<char> buffer(static_cast<size_t>(std::min<uint64_t>(expected_size, 1024ULL * 1024ULL)));
    if (buffer.empty()) {
        buffer.resize(1);
    }

    uint64_t total = 0;
    const auto started = std::chrono::steady_clock::now();
    while (total < expected_size && input) {
        const uint64_t remaining = expected_size - total;
        const size_t current = static_cast<size_t>(std::min<uint64_t>(remaining, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(current));
        const std::streamsize got = input.gcount();
        if (got <= 0) {
            break;
        }
        total += static_cast<uint64_t>(got);
    }
    const auto finished = std::chrono::steady_clock::now();

    result->bytes_read = total;
    result->elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finished - started).count());

    if (input.bad()) {
        if (error) {
            *error = "failed while reading file";
        }
        return ReadStatus::kError;
    }
    if (error) {
        error->clear();
    }
    return ReadStatus::kOk;
}

double Percentile(std::vector<uint64_t> values, double pct) {
    if (values.empty()) {
        return 0.0;
    }
    if (values.size() == 1) {
        return static_cast<double>(values.front());
    }
    std::sort(values.begin(), values.end());
    const double pos = static_cast<double>(values.size() - 1U) * pct;
    const size_t low = static_cast<size_t>(pos);
    const size_t high = std::min(low + 1U, values.size() - 1U);
    const double weight = pos - static_cast<double>(low);
    return static_cast<double>(values[low]) * (1.0 - weight) +
           static_cast<double>(values[high]) * weight;
}

double Average(const std::vector<uint64_t>& values) {
    if (values.empty()) {
        return 0.0;
    }
    long double total = 0.0L;
    for (uint64_t value : values) {
        total += static_cast<long double>(value);
    }
    return static_cast<double>(total / static_cast<long double>(values.size()));
}

double NsToMs(double ns) {
    return ns / 1000000.0;
}

} // namespace

int main(int argc, char* argv[]) {
    Options options;
    if (!ParseArgs(argc, argv, &options)) {
        PrintUsage(argv[0]);
        return 1;
    }
    if (options.help) {
        PrintUsage(argv[0]);
        return 0;
    }

    std::error_code ec;
    options.root = fs::absolute(options.root, ec);
    if (ec) {
        std::cerr << "failed to resolve root directory: " << ec.message() << "\n";
        return 1;
    }
    if (!fs::is_directory(options.root, ec) || ec) {
        std::cerr << "root directory not found or not a directory: " << options.root.string() << "\n";
        return 1;
    }

    std::mt19937_64 rng(options.seed.value_or(std::random_device{}()));
    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(static_cast<size_t>(std::min<uint64_t>(options.count, 1000000ULL)));

    uint64_t bytes_read_total = 0;
    uint64_t failures = 0;
    std::string first_error;

    const auto wall_started = std::chrono::steady_clock::now();
    const uint64_t max_attempts = std::max<uint64_t>(options.count + 1000ULL, options.count * 1000ULL);
    uint64_t attempts = 0;
    while (latencies_ns.size() < options.count && attempts < max_attempts) {
        ++attempts;
        const fs::path path = BuildRandomPath(options.root, &rng);
        ReadResult result;
        std::string error;
        const ReadStatus status = ReadFile(path, options.file_size, &result, &error);
        if (status == ReadStatus::kNotFound) {
            continue;
        }
        if (status == ReadStatus::kOk) {
            latencies_ns.push_back(result.elapsed_ns);
            bytes_read_total += result.bytes_read;
            if (options.verbose) {
                std::cout << "sample=" << latencies_ns.size() << " latency_ms="
                          << std::fixed << std::setprecision(6) << NsToMs(result.elapsed_ns)
                          << " path=" << path.string() << "\n";
            }
            continue;
        }

        ++failures;
        if (first_error.empty()) {
            first_error = path.string() + ": " + error;
        }
        if (options.verbose) {
            std::cerr << "attempt=" << attempts << " error=" << error
                      << " path=" << path.string() << "\n";
        }
        if (!options.continue_on_error) {
            std::cerr << "read failed at attempt " << attempts << ": "
                      << path.string() << ": " << error << "\n";
            return 1;
        }
    }
    if (latencies_ns.size() < options.count) {
        std::cerr << "failed to collect requested readable files: requested=" << options.count
                  << ", completed=" << latencies_ns.size()
                  << ", attempts=" << attempts << "\n";
        return 1;
    }
    const auto wall_finished = std::chrono::steady_clock::now();
    const uint64_t wall_elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(wall_finished - wall_started).count());

    const uint64_t total_file_space =
        static_cast<uint64_t>(kTopDirEnd - kTopDirBegin + 1) *
        static_cast<uint64_t>(kDirFanout) *
        static_cast<uint64_t>(kDirFanout) *
        static_cast<uint64_t>(kDirFanout) *
        static_cast<uint64_t>(kFilesPerLeaf);

    std::cout << std::fixed << std::setprecision(6);
    std::cout << "文件总数：60亿，文件总大小：89.41TB\n";
    std::cout << "==== Random Read 6B Files ====\n";
    std::cout << "root=" << options.root.string() << "\n";
    std::cout << "top_dirs=dir" << std::setw(3) << std::setfill('0') << kTopDirBegin
              << "..dir" << std::setw(3) << kTopDirEnd << std::setfill(' ') << "\n";
    std::cout << "level1_dirs_per_top=100\n";
    std::cout << "level2_dirs_per_level1=100\n";
    std::cout << "level3_dirs_per_level2=100\n";
    std::cout << "files_per_leaf_dir=120\n";
    std::cout << "total_file_space=" << total_file_space << "\n";
    std::cout << "file_size_bytes=" << options.file_size << "\n";
    std::cout << "samples_requested=" << options.count << "\n";
    std::cout << "samples_completed=" << latencies_ns.size() << "\n";
    std::cout << "samples_failed=" << failures << "\n";
    if (options.seed.has_value()) {
        std::cout << "seed=" << *options.seed << "\n";
    }
    std::cout << "bytes_read_total=" << bytes_read_total << "\n";
    std::cout << "wall_latency_ms=" << NsToMs(wall_elapsed_ns) << "\n";
    std::cout << "avg_read_latency_ms=" << NsToMs(Average(latencies_ns)) << "\n";
    std::cout << "min_read_latency_ms="
              << NsToMs(latencies_ns.empty() ? 0.0 : static_cast<double>(*std::min_element(latencies_ns.begin(),
                                                                                           latencies_ns.end())))
              << "\n";
    std::cout << "p50_read_latency_ms=" << NsToMs(Percentile(latencies_ns, 0.50)) << "\n";
    std::cout << "p95_read_latency_ms=" << NsToMs(Percentile(latencies_ns, 0.95)) << "\n";
    std::cout << "max_read_latency_ms="
              << NsToMs(latencies_ns.empty() ? 0.0 : static_cast<double>(*std::max_element(latencies_ns.begin(),
                                                                                           latencies_ns.end())))
              << "\n";
    if (!first_error.empty()) {
        std::cout << "first_error=" << first_error << "\n";
    }

    return failures == 0 ? 0 : 2;
}
