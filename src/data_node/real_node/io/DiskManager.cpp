#include "DiskManager.h"

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

namespace fs = std::filesystem;

namespace zb::real_node {

namespace {

std::string Trim(std::string value) {
    value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
        return !std::isspace(ch);
    }));
    value.erase(std::find_if(value.rbegin(), value.rend(), [](unsigned char ch) {
        return !std::isspace(ch);
    }).base(), value.end());
    return value;
}

std::string FormatSyntheticRealDiskId(uint32_t index, uint32_t disk_count) {
    int width = 2;
    if (disk_count >= 100) {
        width = 3;
    }
    std::ostringstream out;
    out << "disk-" << std::setw(width) << std::setfill('0') << (index + 1);
    return out.str();
}

} // namespace

zb::msg::Status DiskManager::Init(const std::string& config_str) {
    return InitFromConfig(config_str);
}

zb::msg::Status DiskManager::InitFromConfig(const std::string& config_str) {
    std::unique_lock<std::shared_mutex> guard(mu_);
    use_synthetic_capacity_ = false;
    synthetic_capacity_bytes_ = 0;
    disks_.clear();
    if (config_str.empty()) {
        return zb::msg::Status::InvalidArgument("Empty disk config string");
    }

    size_t start = 0;
    while (start < config_str.size()) {
        size_t end = config_str.find(';', start);
        if (end == std::string::npos) {
            end = config_str.size();
        }
        std::string token = Trim(config_str.substr(start, end - start));
        if (!token.empty()) {
            size_t sep = token.find(':');
            if (sep == std::string::npos) {
                return zb::msg::Status::InvalidArgument("Invalid disk config entry: " + token);
            }
            std::string id = Trim(token.substr(0, sep));
            std::string mount_point = Trim(token.substr(sep + 1));
            if (id.empty() || mount_point.empty()) {
                return zb::msg::Status::InvalidArgument("Invalid disk config entry (empty field): " + token);
            }

            DiskContext disk;
            disk.id = id;
            disk.mount_point = mount_point;
            disk.is_healthy = RefreshStats(&disk);
            disks_[disk.id] = disk;
        }
        start = end + 1;
    }

    if (disks_.empty()) {
        return zb::msg::Status::InvalidArgument("No valid disk entries found in config");
    }

    return zb::msg::Status::Ok();
}

zb::msg::Status DiskManager::InitFromDataRoot(const std::string& data_root) {
    std::unique_lock<std::shared_mutex> guard(mu_);
    use_synthetic_capacity_ = false;
    synthetic_capacity_bytes_ = 0;
    disks_.clear();
    if (data_root.empty()) {
        return zb::msg::Status::InvalidArgument("DATA_ROOT is empty");
    }

    fs::path root(data_root);
    if (!fs::exists(root) || !fs::is_directory(root)) {
        return zb::msg::Status::NotFound("DATA_ROOT not found: " + data_root);
    }

    for (const auto& entry : fs::directory_iterator(root)) {
        if (!entry.is_directory()) {
            continue;
        }
        std::string mount_point = entry.path().string();
        std::string disk_id;
        if (!LoadDiskIdFromFile(mount_point, &disk_id)) {
            disk_id = entry.path().filename().string();
        }

        DiskContext disk;
        disk.id = disk_id;
        disk.mount_point = mount_point;
        disk.is_healthy = RefreshStats(&disk);
        disks_[disk.id] = disk;
    }

    if (disks_.empty()) {
        return zb::msg::Status::NotFound("No disks found under DATA_ROOT: " + data_root);
    }

    return zb::msg::Status::Ok();
}

zb::msg::Status DiskManager::InitFromBaseDir(const std::string& base_dir,
                                             uint32_t disk_count,
                                             uint64_t disk_capacity_bytes) {
    std::unique_lock<std::shared_mutex> guard(mu_);
    use_synthetic_capacity_ = true;
    synthetic_capacity_bytes_ = disk_capacity_bytes;
    disks_.clear();
    if (base_dir.empty()) {
        return zb::msg::Status::InvalidArgument("DISK_BASE_DIR is empty");
    }
    if (disk_count == 0) {
        return zb::msg::Status::InvalidArgument("DISK_COUNT must be > 0");
    }
    if (disk_capacity_bytes == 0) {
        return zb::msg::Status::InvalidArgument("DISK_CAPACITY_BYTES must be > 0");
    }

    std::error_code ec;
    fs::path base(base_dir);
    fs::create_directories(base, ec);
    if (ec) {
        return zb::msg::Status::IoError("Failed to create DISK_BASE_DIR: " + ec.message());
    }

    for (uint32_t i = 0; i < disk_count; ++i) {
        const std::string disk_id = FormatSyntheticRealDiskId(i, disk_count);
        const fs::path disk_path = base / disk_id;
        fs::create_directories(disk_path, ec);
        if (ec) {
            return zb::msg::Status::IoError("Failed to create disk dir " + disk_path.string() + ": " + ec.message());
        }

        DiskContext disk;
        disk.id = disk_id;
        disk.mount_point = disk_path.string();
        disk.is_healthy = RefreshSyntheticStats(&disk, synthetic_capacity_bytes_);
        disks_[disk.id] = std::move(disk);
    }

    if (disks_.empty()) {
        return zb::msg::Status::NotFound("No disks initialized under DISK_BASE_DIR: " + base_dir);
    }
    return zb::msg::Status::Ok();
}

zb::msg::Status DiskManager::Refresh() {
    std::unordered_map<std::string, DiskContext> snapshot;
    bool synthetic;
    uint64_t capacity;
    {
        std::shared_lock<std::shared_mutex> guard(mu_);
        snapshot = disks_; synthetic = use_synthetic_capacity_; capacity = synthetic_capacity_bytes_;
    }
    if (snapshot.empty()) return zb::msg::Status::NotFound("No disks initialized");
    for (auto& entry : snapshot) {
        auto& disk = entry.second;
        disk.is_healthy = synthetic ? RefreshSyntheticStats(&disk, capacity) : RefreshStats(&disk);
    }
    {
        std::unique_lock<std::shared_mutex> guard(mu_);
        disks_.swap(snapshot); // Disk membership is initialized before serving requests.
    }
    return zb::msg::Status::Ok();
}

std::string DiskManager::GetMountPoint(const std::string& disk_id) const {
    std::shared_lock<std::shared_mutex> guard(mu_);
    auto it = disks_.find(disk_id);
    if (it == disks_.end() || !it->second.is_healthy) {
        return {};
    }
    return it->second.mount_point;
}

bool DiskManager::IsHealthy(const std::string& disk_id) const {
    std::shared_lock<std::shared_mutex> guard(mu_);
    auto it = disks_.find(disk_id);
    return it != disks_.end() && it->second.is_healthy;
}

std::vector<zb::msg::DiskReport> DiskManager::GetReport() const {
    std::shared_lock<std::shared_mutex> guard(mu_);
    std::vector<zb::msg::DiskReport> reports;
    reports.reserve(disks_.size());
    for (const auto& [id, disk] : disks_) {
        zb::msg::DiskReport report;
        report.id = disk.id;
        report.mount_point = disk.mount_point;
        report.capacity_bytes = disk.capacity_bytes;
        report.free_bytes = disk.free_bytes;
        report.is_healthy = disk.is_healthy;
        reports.push_back(std::move(report));
    }
    return reports;
}

bool DiskManager::LoadDiskIdFromFile(const std::string& mount_point, std::string* out_id) {
    fs::path id_path = fs::path(mount_point) / ".disk_id";
    std::ifstream input(id_path.string());
    if (!input) {
        return false;
    }
    std::string id;
    std::getline(input, id);
    id = Trim(id);
    if (id.empty()) {
        return false;
    }
    *out_id = id;
    return true;
}

uint64_t DiskManager::CalculateDirectoryUsageBytes(const std::string& dir_path) {
    std::error_code ec;
    if (!fs::exists(dir_path, ec) || !fs::is_directory(dir_path, ec)) {
        return 0;
    }

    uint64_t total = 0;
    fs::recursive_directory_iterator it(dir_path, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    while (!ec && it != end) {
        if (it->is_regular_file(ec)) {
            total += static_cast<uint64_t>(it->file_size(ec));
        }
        it.increment(ec);
    }
    return total;
}

bool DiskManager::RefreshStats(DiskContext* disk) {
    if (!disk) {
        return false;
    }
    try {
        fs::space_info info = fs::space(disk->mount_point);
        disk->capacity_bytes = info.capacity;
        disk->free_bytes = info.available;
        return true;
    } catch (const fs::filesystem_error&) {
        disk->capacity_bytes = 0;
        disk->free_bytes = 0;
        return false;
    }
}

bool DiskManager::RefreshSyntheticStats(DiskContext* disk, uint64_t synthetic_capacity_bytes) {
    if (!disk || synthetic_capacity_bytes == 0) {
        return false;
    }

    std::error_code ec;
    if (!fs::exists(disk->mount_point, ec) || !fs::is_directory(disk->mount_point, ec)) {
        disk->capacity_bytes = 0;
        disk->free_bytes = 0;
        return false;
    }

    const uint64_t used_bytes = CalculateDirectoryUsageBytes(disk->mount_point);
    disk->capacity_bytes = synthetic_capacity_bytes;
    disk->free_bytes = synthetic_capacity_bytes > used_bytes ? (synthetic_capacity_bytes - used_bytes) : 0;
    return true;
}

} // namespace zb::real_node
