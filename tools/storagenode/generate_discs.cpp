#include <cstdio>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <cstring>
#include <string>
#include <cstdint>
#include <filesystem>

#include "optical_disc_catalog.h"

namespace {

constexpr int kBatchSize = 100000;
constexpr int kDiscsPerLibrary = 20000;

} // namespace

int main(int argc, char** argv) {
    // Usage: generate_discs <total_count> <out_dir>
    // 默认生成 1000000 张，输出到 /mnt/md0/node/disc1
    long long total = 1000000; // 默认总数
    std::string out_dir = "/mnt/md0/node/disc1";
    if (argc >= 2) total = std::stoll(argv[1]);
    if (argc >= 3) out_dir = argv[2];

    std::error_code ec;
    std::filesystem::create_directories(out_dir, ec);
    if (ec) {
        std::cerr << "Failed to create output directory: " << out_dir << " error=" << ec.message() << "\n";
        return 2;
    }

    int batch_count = static_cast<int>((total + kBatchSize - 1) / kBatchSize);
    int written_total = 0;

    std::cout << "Generate discs: total=" << total << " out_dir=" << out_dir << "\n";
    for (int batch = 0; batch < batch_count; ++batch) {
        std::string filename = out_dir + "/disc_batch_" + std::to_string(batch) + ".bin";
        std::ofstream ofs(filename, std::ios::binary | std::ios::trunc);
        if (!ofs.is_open()) {
            std::cerr << "Failed to open " << filename << " for writing\n";
            return 2;
        }

        int discs_in_this_file = std::min(kBatchSize, static_cast<int>(total - written_total));
        for (int i = 0; i < discs_in_this_file; ++i, ++written_total) {
            char id_buf[32];
            char lib_buf[16];
            std::snprintf(id_buf, sizeof(id_buf), "disc_%05d%05d", batch, i);
            int lib_idx = written_total / kDiscsPerLibrary; // 每 20000 张分配一个库
            std::snprintf(lib_buf, sizeof(lib_buf), "lib_%05d", lib_idx);

            zb::storagenode::OpticalDiscBin disc{};
            std::strncpy(disc.device_id, id_buf, sizeof(disc.device_id) - 1);
            std::strncpy(disc.library_id, lib_buf, sizeof(disc.library_id) - 1);
            disc.capacity = zb::storagenode::kDefaultOpticalDiscCapacityBytes;
            disc.status = zb::storagenode::DiscStatus::Blank;
            disc.write_throughput_MBps = zb::storagenode::kDefaultOpticalDiscWriteMbps;
            disc.read_throughput_MBps = zb::storagenode::kDefaultOpticalDiscReadMbps;

            ofs.write(reinterpret_cast<const char*>(&disc), sizeof(disc));
        }

        ofs.close();
        std::cout << "Wrote " << filename << " (" << discs_in_this_file << " discs)\n";
    }

    std::cout << "Done. Total written: " << written_total << " discs\n";
    return 0;
}
