#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

#include "optical_disc_catalog.h"

namespace {

constexpr std::uint64_t kDefaultNodeCount = 10000;
constexpr std::uint64_t kDefaultDiscsPerNode = 10000;
constexpr std::uint64_t kDefaultSmallDiscsPerNode = 9000;
constexpr std::uint64_t kDefaultBatchSize = 100000;
constexpr std::uint64_t kDiscsPerLibrary = 20000;
constexpr std::uint64_t kSmallDiscCapacityBytes = 1000000000000ULL;
constexpr std::uint64_t kLargeDiscCapacityBytes = 10000000000000ULL;

struct Options {
    std::filesystem::path out_dir;
    std::uint64_t node_count{kDefaultNodeCount};
    std::uint64_t discs_per_node{kDefaultDiscsPerNode};
    std::uint64_t small_discs_per_node{kDefaultSmallDiscsPerNode};
    std::uint64_t batch_size{kDefaultBatchSize};
};

bool ParseU64(const std::string& name, const std::string& value, std::uint64_t* out) {
    if (!out || value.empty()) {
        std::cerr << "invalid " << name << ": " << value << "\n";
        return false;
    }
    try {
        std::size_t consumed = 0;
        const std::uint64_t parsed = std::stoull(value, &consumed);
        if (consumed != value.size()) {
            std::cerr << "invalid " << name << ": " << value << "\n";
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        std::cerr << "invalid " << name << ": " << value << "\n";
        return false;
    }
}

std::string U128ToString(unsigned __int128 value) {
    if (value == 0) {
        return "0";
    }
    std::string out;
    while (value != 0) {
        const unsigned digit = static_cast<unsigned>(value % 10);
        out.push_back(static_cast<char>('0' + digit));
        value /= 10;
    }
    std::reverse(out.begin(), out.end());
    return out;
}

void PrintUsage(const char* program) {
    std::cerr
        << "Usage: " << program << " <out_dir>\n"
        << "       " << program << " --out_dir=<dir> [--node_count=10000] [--discs_per_node=10000]\n"
        << "                    [--small_discs_per_node=9000] [--batch_size=100000]\n\n"
        << "Generates legacy_mixed_v1 optical inventory:\n"
        << "  per node: 9000 x 1TB discs + 1000 x 10TB discs\n"
        << "  default total: 100,000,000 discs, 190 EB\n";
}

bool ParseArgs(int argc, char** argv, Options* options) {
    if (!options) {
        return false;
    }
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            return false;
        }
        if (arg.rfind("--", 0) != 0) {
            if (!options->out_dir.empty()) {
                std::cerr << "unexpected positional argument: " << arg << "\n";
                return false;
            }
            options->out_dir = arg;
            continue;
        }
        const std::size_t eq = arg.find('=');
        if (eq == std::string::npos) {
            std::cerr << "expected --key=value argument: " << arg << "\n";
            return false;
        }
        const std::string key = arg.substr(2, eq - 2);
        const std::string value = arg.substr(eq + 1);
        if (key == "out_dir") {
            options->out_dir = value;
        } else if (key == "node_count") {
            if (!ParseU64(key, value, &options->node_count)) {
                return false;
            }
        } else if (key == "discs_per_node") {
            if (!ParseU64(key, value, &options->discs_per_node)) {
                return false;
            }
        } else if (key == "small_discs_per_node") {
            if (!ParseU64(key, value, &options->small_discs_per_node)) {
                return false;
            }
        } else if (key == "batch_size") {
            if (!ParseU64(key, value, &options->batch_size)) {
                return false;
            }
        } else {
            std::cerr << "unknown option: --" << key << "\n";
            return false;
        }
    }
    if (options->out_dir.empty()) {
        PrintUsage(argv[0]);
        return false;
    }
    if (options->node_count == 0 || options->discs_per_node == 0 || options->batch_size == 0 ||
        options->small_discs_per_node > options->discs_per_node) {
        std::cerr << "invalid legacy mixed disc generation options\n";
        return false;
    }
    if (options->batch_size > 100000) {
        std::cerr << "batch_size must be <= 100000 to preserve disc_<batch><offset> identifiers\n";
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    Options options;
    if (!ParseArgs(argc, argv, &options)) {
        return 2;
    }

    std::error_code ec;
    std::filesystem::create_directories(options.out_dir, ec);
    if (ec) {
        std::cerr << "failed to create output directory: " << options.out_dir
                  << " error=" << ec.message() << "\n";
        return 2;
    }

    const std::uint64_t total_discs = options.node_count * options.discs_per_node;
    const std::uint64_t batch_count = (total_discs + options.batch_size - 1) / options.batch_size;
    const std::uint64_t large_discs_per_node = options.discs_per_node - options.small_discs_per_node;
    if (batch_count > 100000) {
        std::cerr << "batch_count=" << batch_count
                  << " exceeds 100000 and cannot be encoded in disc_<batch><offset> identifiers\n";
        return 2;
    }
    const unsigned __int128 per_node_capacity =
        static_cast<unsigned __int128>(options.small_discs_per_node) * kSmallDiscCapacityBytes +
        static_cast<unsigned __int128>(large_discs_per_node) * kLargeDiscCapacityBytes;
    const unsigned __int128 total_capacity =
        per_node_capacity * static_cast<unsigned __int128>(options.node_count);

    std::cout << "Generate legacy_mixed_v1 discs"
              << " out_dir=" << options.out_dir
              << " node_count=" << options.node_count
              << " discs_per_node=" << options.discs_per_node
              << " small_discs_per_node=" << options.small_discs_per_node
              << " large_discs_per_node=" << large_discs_per_node
              << " batch_size=" << options.batch_size
              << " total_discs=" << total_discs
              << " total_capacity_bytes=" << U128ToString(total_capacity)
              << "\n";

    std::uint64_t written_total = 0;
    for (std::uint64_t batch = 0; batch < batch_count; ++batch) {
        const std::filesystem::path filename =
            options.out_dir / ("disc_batch_" + std::to_string(batch) + ".bin");
        std::ofstream output(filename, std::ios::binary | std::ios::trunc);
        if (!output.is_open()) {
            std::cerr << "failed to open " << filename << " for writing\n";
            return 2;
        }

        const std::uint64_t discs_in_file =
            std::min(options.batch_size, total_discs - written_total);
        for (std::uint64_t i = 0; i < discs_in_file; ++i, ++written_total) {
            const std::uint64_t disc_index = written_total % options.discs_per_node;
            const std::uint64_t offset = written_total % options.batch_size;
            const std::uint64_t library_index = written_total / kDiscsPerLibrary;

            char id_buf[32];
            char lib_buf[16];
            std::snprintf(id_buf, sizeof(id_buf), "disc_%05u%05u",
                          static_cast<unsigned>(batch),
                          static_cast<unsigned>(offset));
            std::snprintf(lib_buf, sizeof(lib_buf), "lib_%05u",
                          static_cast<unsigned>(library_index));

            zb::storagenode::OpticalDiscBin disc{};
            std::strncpy(disc.device_id, id_buf, sizeof(disc.device_id) - 1);
            std::strncpy(disc.library_id, lib_buf, sizeof(disc.library_id) - 1);
            disc.capacity = disc_index < options.small_discs_per_node
                                ? kSmallDiscCapacityBytes
                                : kLargeDiscCapacityBytes;
            disc.status = zb::storagenode::DiscStatus::Blank;
            disc.write_throughput_MBps = zb::storagenode::kDefaultOpticalDiscWriteMbps;
            disc.read_throughput_MBps = zb::storagenode::kDefaultOpticalDiscReadMbps;

            output.write(reinterpret_cast<const char*>(&disc), sizeof(disc));
        }

        if (!output) {
            std::cerr << "failed to write " << filename << "\n";
            return 2;
        }
        std::cout << "Wrote " << filename << " (" << discs_in_file << " discs)\n";
    }

    std::cout << "Done. Total written: " << written_total
              << " discs total_capacity_bytes=" << U128ToString(total_capacity) << "\n";
    return 0;
}
