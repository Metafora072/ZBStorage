#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "optical_disc_catalog.h"

namespace fs = std::filesystem;

namespace {

constexpr std::size_t kProgressStep = 10000;
constexpr double kBytesPerTB = 1000.0 * 1000.0 * 1000.0 * 1000.0;

using zb::storagenode::DiscStatus;
using zb::storagenode::OpticalDiscBin;

struct BatchFile {
    fs::path path;
    std::uint64_t batch_index = 0;
};

std::string toString(const char* buf, std::size_t len) {
    std::size_t n = 0;
    while (n < len && buf[n] != '\0') {
        ++n;
    }
    return std::string(buf, n);
}

void setFixedChar(char* out, std::size_t out_size, const std::string& value) {
    std::memset(out, 0, out_size);
    if (!value.empty()) {
        std::strncpy(out, value.c_str(), out_size - 1);
    }
}

std::vector<BatchFile> collectBatchFiles(const std::string& disc_dir) {
    std::vector<BatchFile> files;
    if (!fs::exists(disc_dir) || !fs::is_directory(disc_dir)) {
        return files;
    }

    std::regex pattern(R"(^disc_batch_(\d+)\.bin$)");
    for (const auto& entry : fs::directory_iterator(disc_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        const auto file_name = entry.path().filename().string();
        std::smatch match;
        if (!std::regex_match(file_name, match, pattern)) {
            continue;
        }

        BatchFile bf;
        bf.path = entry.path();
        bf.batch_index = static_cast<std::uint64_t>(std::stoull(match[1].str()));
        files.push_back(bf);
    }

    std::sort(files.begin(), files.end(), [](const BatchFile& a, const BatchFile& b) {
        if (a.batch_index != b.batch_index) {
            return a.batch_index < b.batch_index;
        }
        return a.path.string() < b.path.string();
    });
    return files;
}

void printDisc(const OpticalDiscBin* disc) {
    if (!disc) {
        std::cout << "    未找到对应光盘" << std::endl;
        return;
    }

    std::cout << "    光盘ID: " << toString(disc->device_id, sizeof(disc->device_id))
              << "，光盘库: " << toString(disc->library_id, sizeof(disc->library_id))
              << "，容量: " << disc->capacity
              << "，状态: " << static_cast<int>(disc->status)
              << std::endl;
}

double bytesToTB(uint64_t bytes) {
    return static_cast<double>(bytes) / kBytesPerTB;
}

std::string makeDiscId(std::uint64_t value) {
    std::ostringstream oss;
    oss << "disc_" << std::setw(10) << std::setfill('0') << value;
    return oss.str();
}

} // namespace

int main(int argc, char** argv) {
    // Usage: import_discs_standalone [disc_dir] [import_limit]
    std::string disc_dir = "/mnt/md0/node/disc1";
    std::size_t import_limit = 1000;
    if (argc >= 2) {
        disc_dir = argv[1];
    }
    if (argc >= 3) {
        import_limit = static_cast<std::size_t>(std::stoull(argv[2]));
    }

    auto batch_files = collectBatchFiles(disc_dir);
    if (batch_files.empty()) {
        std::cout << "[导入] 未找到批次光盘文件: " << disc_dir << std::endl;
        return 0;
    }

    std::unordered_map<std::string, OpticalDiscBin> discs;
    discs.reserve(import_limit + 16);

    uint64_t total_capacity = 0;
    std::size_t total_discs = 0;

    for (const auto& batch_file : batch_files) {
        std::ifstream ifs(batch_file.path, std::ios::binary);
        if (!ifs.is_open()) {
            std::cerr << "[导入] 打开失败: " << batch_file.path << std::endl;
            continue;
        }

        ifs.seekg(0, std::ios::end);
        std::streamoff file_size = ifs.tellg();
        ifs.seekg(0, std::ios::beg);

        if (file_size < 0 || (file_size % static_cast<std::streamoff>(sizeof(OpticalDiscBin))) != 0) {
            std::cerr << "[导入] 文件大小异常，跳过: " << batch_file.path << std::endl;
            continue;
        }

        std::size_t disc_count = static_cast<std::size_t>(file_size / static_cast<std::streamoff>(sizeof(OpticalDiscBin)));
        std::vector<OpticalDiscBin> buffer(disc_count);
        if (!buffer.empty()) {
            ifs.read(reinterpret_cast<char*>(buffer.data()), static_cast<std::streamsize>(buffer.size() * sizeof(OpticalDiscBin)));
            if (!ifs) {
                std::cerr << "[导入] 读取失败: " << batch_file.path << std::endl;
                continue;
            }
        }

        std::cout << "[导入] 已加载批次文件: " << batch_file.path.filename().string()
                  << "，批次光盘数: " << buffer.size() << std::endl;

        for (const auto& disc : buffer) {
            std::string id = toString(disc.device_id, sizeof(disc.device_id));
            if (id.empty()) {
                continue;
            }

            auto [it, inserted] = discs.emplace(id, disc);
            if (!inserted) {
                continue;
            }

            total_capacity += disc.capacity;
            ++total_discs;

            if (total_discs % kProgressStep == 0 || total_discs == import_limit) {
                std::cout << "[导入] 已导入光盘数量: " << total_discs
                          << "，累计容量: " << std::fixed << std::setprecision(2)
                          << bytesToTB(total_capacity) << " TB" << std::endl;
            }

            if (total_discs >= import_limit) {
                std::cout << "[导入] 已达到演示上限 " << import_limit << " 张光盘，停止继续导入。" << std::endl;
                break;
            }
        }

        if (total_discs >= import_limit) {
            break;
        }
    }

    std::cout << "[汇总] 所有光盘总容量: " << std::fixed << std::setprecision(2)
              << bytesToTB(total_capacity) << " TB" << std::endl;
    std::cout << "[汇总] 导入光盘总数: " << total_discs << std::endl;

    if (discs.empty()) {
        std::cout << "[结束] 没有可导入的光盘，程序退出。" << std::endl;
        return 0;
    }

    const auto first_it = discs.begin();
    const std::string first_id = first_it->first;
    std::cout << "[查] 查询第一张光盘: " << first_id << std::endl;
    printDisc(&first_it->second);

    const std::string new_disc_id = makeDiscId(1000000000ULL);
    std::cout << "[增] 新增一张演示光盘: " << new_disc_id << std::endl;
    OpticalDiscBin new_disc{};
    setFixedChar(new_disc.device_id, sizeof(new_disc.device_id), new_disc_id);
    setFixedChar(new_disc.library_id, sizeof(new_disc.library_id), "lib_00000");
    new_disc.capacity = zb::storagenode::kDefaultOpticalDiscCapacityBytes;
    new_disc.status = DiscStatus::Blank;
    new_disc.write_throughput_MBps = zb::storagenode::kDefaultOpticalDiscWriteMbps;
    new_disc.read_throughput_MBps = zb::storagenode::kDefaultOpticalDiscReadMbps;
    auto [new_it, new_ok] = discs.emplace(new_disc_id, new_disc);
    if (new_ok) {
        total_capacity += new_it->second.capacity;
    }
    std::cout << "    新增成功，当前光盘数: " << discs.size() << std::endl;

    std::cout << "[改] 将第一张光盘状态改为 Finalized，并调整容量" << std::endl;
    auto upd_it = discs.find(first_id);
    if (upd_it != discs.end()) {
        upd_it->second.status = DiscStatus::Finalized;
        upd_it->second.capacity += 512ULL * 1024ULL * 1024ULL;
        total_capacity += 512ULL * 1024ULL * 1024ULL;
        std::cout << "    修改成功" << std::endl;
        printDisc(&upd_it->second);
    } else {
        std::cout << "    修改失败，未找到目标光盘" << std::endl;
    }

    std::string removed_id;
    for (const auto& kv : discs) {
        if (kv.first != first_id && kv.first != new_disc_id) {
            removed_id = kv.first;
            break;
        }
    }
    if (!removed_id.empty()) {
        std::cout << "[删] 删除一张其它光盘: " << removed_id << std::endl;
        auto del_it = discs.find(removed_id);
        if (del_it != discs.end()) {
            total_capacity -= del_it->second.capacity;
            discs.erase(del_it);
            std::cout << "    删除成功，当前光盘数: " << discs.size() << std::endl;
        }
    }

    std::cout << "[查] 再次查询新增光盘: " << new_disc_id << std::endl;
    auto q1 = discs.find(new_disc_id);
    printDisc(q1 == discs.end() ? nullptr : &q1->second);

    if (!removed_id.empty()) {
        std::cout << "[查] 再次查询已删除光盘: " << removed_id << std::endl;
        auto q2 = discs.find(removed_id);
        printDisc(q2 == discs.end() ? nullptr : &q2->second);
    }

    std::cout << "[汇总] 操作后光盘总容量: " << std::fixed << std::setprecision(2)
              << bytesToTB(total_capacity) << " TB" << std::endl;
    std::cout << "[结束] 光盘导入与增删改查演示完成。" << std::endl;
    return 0;
}
