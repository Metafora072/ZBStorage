#include "OpticalNodeConfig.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <limits>
#include <stdexcept>

namespace zb::optical_node {

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

bool ParseUint32(const std::string& text, uint32_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint32_t>(std::stoul(text));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseBool(const std::string& value, bool* out) {
    if (!out) {
        return false;
    }
    std::string lowered = value;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (lowered == "1" || lowered == "true" || lowered == "yes" || lowered == "on") {
        *out = true;
        return true;
    }
    if (lowered == "0" || lowered == "false" || lowered == "no" || lowered == "off") {
        *out = false;
        return true;
    }
    return false;
}

bool ParseUint64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(text));
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseDouble(const std::string& text, double* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = std::stod(text);
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

} // namespace

OpticalNodeConfig OpticalNodeConfig::LoadFromFile(const std::string& path, std::string* error) {
    OpticalNodeConfig cfg;
    std::ifstream input(path);
    if (!input) {
        if (error) {
            *error = "Failed to open config file: " + path;
        }
        return cfg;
    }

    std::string line;
    bool drive_read_configured = false;
    bool drive_write_configured = false;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        size_t eq = trimmed.find('=');
        if (eq == std::string::npos) {
            if (error) {
                *error = "Invalid config line " + std::to_string(line_no) + ": " + line;
            }
            return {};
        }

        std::string key = Trim(trimmed.substr(0, eq));
        std::string value = Trim(trimmed.substr(eq + 1));
        if (key == "NODE_ID") {
            cfg.node_id = value;
        } else if (key == "NODE_ADDRESS") {
            cfg.node_address = value;
        } else if (key == "SCHEDULER_ADDR") {
            cfg.scheduler_addr = value;
        } else if (key == "MDS_ADDRESS") {
            cfg.mds_address = value;
        } else if (key == "GROUP_ID") {
            cfg.group_id = value;
        } else if (key == "NODE_ROLE") {
            cfg.node_role = value;
        } else if (key == "PEER_NODE_ID") {
            cfg.peer_node_id = value;
        } else if (key == "PEER_ADDRESS") {
            cfg.peer_address = value;
        } else if (key == "REPLICATION_ENABLED") {
            if (!ParseBool(value, &cfg.replication_enabled)) {
                if (error) {
                    *error = "Invalid REPLICATION_ENABLED at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "REPLICATION_TIMEOUT_MS") {
            if (!ParseUint32(value, &cfg.replication_timeout_ms)) {
                if (error) {
                    *error = "Invalid REPLICATION_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "NODE_WEIGHT") {
            if (!ParseUint32(value, &cfg.node_weight)) {
                if (error) {
                    *error = "Invalid NODE_WEIGHT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "VIRTUAL_NODE_COUNT") {
            if (!ParseUint32(value, &cfg.virtual_node_count)) {
                if (error) {
                    *error = "Invalid VIRTUAL_NODE_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "HEARTBEAT_INTERVAL_MS") {
            if (!ParseUint32(value, &cfg.heartbeat_interval_ms)) {
                if (error) {
                    *error = "Invalid HEARTBEAT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_ROOT") {
            cfg.archive_root = value;
        } else if (key == "VOLUME_SIZE_BYTES") {
            if (!ParseUint64(value, &cfg.volume_size_bytes)) {
                if (error) {
                    *error = "Invalid VOLUME_SIZE_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "SIZE_THRESHOLD") {
            if (!ParseDouble(value, &cfg.size_threshold)) {
                if (error) {
                    *error = "Invalid SIZE_THRESHOLD at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "CAPACITY_IN_IMAGES") {
            if (!ParseUint64(value, &cfg.capacity_in_images)) {
                if (error) {
                    *error = "Invalid CAPACITY_IN_IMAGES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "AVAILABLE_ID_COUNT") {
            uint32_t count = 0;
            if (!ParseUint32(value, &count) || count > 255) {
                if (error) {
                    *error = "Invalid AVAILABLE_ID_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.available_id_count = static_cast<uint8_t>(count);
        } else if (key == "DISC_CAPACITY_BYTES") {
            if (!ParseUint64(value, &cfg.disc_capacity_bytes)) {
                if (error) {
                    *error = "Invalid DISC_CAPACITY_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "STANDARD_IMAGES_PER_DISC") {
            if (!ParseUint32(value, &cfg.standard_images_per_disc)) {
                if (error) {
                    *error = "Invalid STANDARD_IMAGES_PER_DISC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "TOTAL_DISC_COUNT") {
            if (!ParseUint64(value, &cfg.total_disc_count)) {
                if (error) {
                    *error = "Invalid TOTAL_DISC_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "SLOT_COUNT") {
            if (!ParseUint64(value, &cfg.slot_count)) {
                if (error) {
                    *error = "Invalid SLOT_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISC_BLOCK_SIZE_BYTES") {
            if (!ParseUint64(value, &cfg.disc_block_size_bytes)) {
                if (error) {
                    *error = "Invalid DISC_BLOCK_SIZE_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "MAX_WRITE_IMAGES") {
            if (!ParseUint32(value, &cfg.max_write_images)) {
                if (error) {
                    *error = "Invalid MAX_WRITE_IMAGES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DRIVE_COUNT") {
            if (!ParseUint32(value, &cfg.drive_count)) {
                if (error) *error = "Invalid DRIVE_COUNT at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "ARM_COUNT") {
            if (!ParseUint32(value, &cfg.arm_count)) {
                if (error) *error = "Invalid ARM_COUNT at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "BURN_IMAGE_SIZE_BYTES") {
            if (!ParseUint64(value, &cfg.burn_image_size_bytes)) {
                if (error) *error = "Invalid BURN_IMAGE_SIZE_BYTES at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "BURN_BANDWIDTH_MBPS") {
            if (!ParseDouble(value, &cfg.burn_bandwidth_mbps)) {
                if (error) *error = "Invalid BURN_BANDWIDTH_MBPS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "BURN_FIXED_SECONDS") {
            if (!ParseDouble(value, &cfg.burn_fixed_seconds)) {
                if (error) *error = "Invalid BURN_FIXED_SECONDS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "READ_BANDWIDTH_MBPS") {
            if (!ParseDouble(value, &cfg.read_bandwidth_mbps)) {
                if (error) *error = "Invalid READ_BANDWIDTH_MBPS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "READ_FIXED_SECONDS") {
            if (!ParseDouble(value, &cfg.read_fixed_seconds)) {
                if (error) *error = "Invalid READ_FIXED_SECONDS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DRIVE_LOAD_SECONDS") {
            if (!ParseDouble(value, &cfg.drive_load_seconds)) {
                if (error) *error = "Invalid DRIVE_LOAD_SECONDS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DRIVE_EJECT_SECONDS") {
            if (!ParseDouble(value, &cfg.drive_eject_seconds)) {
                if (error) *error = "Invalid DRIVE_EJECT_SECONDS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "GRASP_SECONDS") {
            if (!ParseDouble(value, &cfg.grasp_seconds)) {
                if (error) *error = "Invalid GRASP_SECONDS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "ARM_SPEED_X") {
            if (!ParseDouble(value, &cfg.arm_speed_x)) {
                if (error) *error = "Invalid ARM_SPEED_X at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "ARM_SPEED_Y") {
            if (!ParseDouble(value, &cfg.arm_speed_y)) {
                if (error) *error = "Invalid ARM_SPEED_Y at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "ARM_SPEED_Z") {
            if (!ParseDouble(value, &cfg.arm_speed_z)) {
                if (error) *error = "Invalid ARM_SPEED_Z at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RACK_ROWS") {
            if (!ParseDouble(value, &cfg.rack_rows)) {
                if (error) *error = "Invalid RACK_ROWS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RACK_COLS") {
            if (!ParseDouble(value, &cfg.rack_cols)) {
                if (error) *error = "Invalid RACK_COLS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RACK_DEPTH") {
            if (!ParseDouble(value, &cfg.rack_depth)) {
                if (error) *error = "Invalid RACK_DEPTH at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "SLOT_WIDTH") {
            if (!ParseDouble(value, &cfg.slot_width)) {
                if (error) *error = "Invalid SLOT_WIDTH at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "SLOT_HEIGHT") {
            if (!ParseDouble(value, &cfg.slot_height)) {
                if (error) *error = "Invalid SLOT_HEIGHT at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "SLOT_DEPTH") {
            if (!ParseDouble(value, &cfg.slot_depth)) {
                if (error) *error = "Invalid SLOT_DEPTH at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "EXTERNAL_NETWORK_BANDWIDTH_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.external_network_bandwidth_bytes_per_sec)) {
                if (error) *error = "Invalid EXTERNAL_NETWORK_BANDWIDTH_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "FIXED_ACCESS_LATENCY_US") {
            if (!ParseUint64(value, &cfg.fixed_access_latency_us)) {
                if (error) *error = "Invalid FIXED_ACCESS_LATENCY_US at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "OPTICAL_LOAD_LATENCY_US") {
            if (!ParseUint64(value, &cfg.optical_load_latency_us)) {
                if (error) *error = "Invalid OPTICAL_LOAD_LATENCY_US at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "OPTICAL_SEEK_LATENCY_US") {
            if (!ParseUint64(value, &cfg.optical_seek_latency_us)) {
                if (error) *error = "Invalid OPTICAL_SEEK_LATENCY_US at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "CACHE_READ_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.cache_read_bytes_per_sec)) {
                if (error) *error = "Invalid CACHE_READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "CACHE_WRITE_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.cache_write_bytes_per_sec)) {
                if (error) *error = "Invalid CACHE_WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DRIVE_READ_BYTES_PER_SEC") {
            drive_read_configured = true;
            if (!ParseUint64(value, &cfg.drive_read_bytes_per_sec)) {
                if (error) *error = "Invalid DRIVE_READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DRIVE_WRITE_BYTES_PER_SEC") {
            drive_write_configured = true;
            if (!ParseUint64(value, &cfg.drive_write_bytes_per_sec)) {
                if (error) *error = "Invalid DRIVE_WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_PEAK_MILLIWATTS") {
            if (!ParseUint64(value, &cfg.power_peak_milliwatts)) {
                if (error) *error = "Invalid POWER_PEAK_MILLIWATTS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_MEDIUM_MILLIWATTS") {
            if (!ParseUint64(value, &cfg.power_medium_milliwatts)) {
                if (error) *error = "Invalid POWER_MEDIUM_MILLIWATTS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_STANDBY_MILLIWATTS") {
            if (!ParseUint64(value, &cfg.power_standby_milliwatts)) {
                if (error) *error = "Invalid POWER_STANDBY_MILLIWATTS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_OFF_MILLIWATTS") {
            if (!ParseUint64(value, &cfg.power_off_milliwatts)) {
                if (error) *error = "Invalid POWER_OFF_MILLIWATTS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RELIABILITY_MEAN_LIFETIME_MS") {
            if (!ParseUint64(value, &cfg.reliability_mean_lifetime_ms)) {
                if (error) *error = "Invalid RELIABILITY_MEAN_LIFETIME_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RELIABILITY_LIFETIME_STDDEV_MS") {
            if (!ParseUint64(value, &cfg.reliability_lifetime_stddev_ms)) {
                if (error) *error = "Invalid RELIABILITY_LIFETIME_STDDEV_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "RELIABILITY_MINIMUM_LIFETIME_MS") {
            if (!ParseUint64(value, &cfg.reliability_minimum_lifetime_ms)) {
                if (error) *error = "Invalid RELIABILITY_MINIMUM_LIFETIME_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "TECHNOLOGY_GENERATION") {
            if (!ParseUint32(value, &cfg.technology_generation)) {
                if (error) *error = "Invalid TECHNOLOGY_GENERATION at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "INSTANCE_GENERATION") {
            if (!ParseUint64(value, &cfg.instance_generation)) {
                if (error) *error = "Invalid INSTANCE_GENERATION at line " + std::to_string(line_no);
                return {};
            }
        }
    }

    if (cfg.node_weight == 0) {
        cfg.node_weight = 1;
    }
    if (cfg.node_role.empty()) {
        cfg.node_role = "PRIMARY";
    }
    if (cfg.virtual_node_count == 0) {
        cfg.virtual_node_count = 1;
    }
    if (cfg.heartbeat_interval_ms == 0) {
        cfg.heartbeat_interval_ms = 2000;
    }
    if (cfg.archive_root.empty()) {
        cfg.archive_root = "/tmp/zb_optical";
    }
    if (cfg.volume_size_bytes == 0) {
        cfg.volume_size_bytes = 10ull * 1024 * 1024 * 1024;
    }
    if (cfg.size_threshold <= 0.0 || cfg.size_threshold > 1.0) {
        cfg.size_threshold = 0.9;
    }
    if (cfg.capacity_in_images == 0) {
        cfg.capacity_in_images = 1000;
    }
    if (cfg.available_id_count == 0) {
        cfg.available_id_count = 5;
    }
    if (cfg.disc_capacity_bytes == 0) {
        cfg.disc_capacity_bytes = 1099511627776ULL;
    }
    if (cfg.standard_images_per_disc == 0) {
        cfg.standard_images_per_disc = 100;
    }
    if (cfg.total_disc_count == 0) {
        // 现有 cd_manager_sim 不支持零盘，不能把请求的零盘伪报成 1000 片。
        // 这里只校验上报配置，不修改内部装盘/迁出算法。
        if (error) *error = "TOTAL_DISC_COUNT=0 is unsupported by cd_manager_sim; cannot claim an observed empty physical library";
        return {};
    }
    // 槽位数须 >= 光盘数量，否则 local_disc_count > total_slots 会被服务端校验拒绝。
    if (cfg.slot_count == 0) {
        cfg.slot_count = 100000;
    }
    if (cfg.slot_count < cfg.total_disc_count) {
        cfg.slot_count = cfg.total_disc_count;
    }
    if (cfg.disc_block_size_bytes == 0) {
        cfg.disc_block_size_bytes = 2048;
    }
    // 背压阈值下限为 1：为 0 会让下载侧永久暂停，无法自恢复。
    if (cfg.max_write_images < 1) {
        cfg.max_write_images = 1;
    }
    // 光盘库硬件 / 模拟参数兜底：零或非法值回落到安全默认，避免除零 / 负区间 / 非法设备组。
    if (cfg.drive_count == 0) {
        cfg.drive_count = 20;
    }
    if (cfg.arm_count == 0) {
        cfg.arm_count = 4;
    }
    if (cfg.burn_image_size_bytes == 0) {
        cfg.burn_image_size_bytes = 10ull * 1024 * 1024 * 1024;
    }
    if (cfg.burn_bandwidth_mbps <= 0.0) {
        cfg.burn_bandwidth_mbps = 20.0;
    }
    if (cfg.read_bandwidth_mbps <= 0.0) {
        cfg.read_bandwidth_mbps = 100.0;
    }
    if (cfg.arm_speed_x <= 0.0) {
        cfg.arm_speed_x = 2.0;
    }
    if (cfg.arm_speed_y <= 0.0) {
        cfg.arm_speed_y = 1.0;
    }
    if (cfg.arm_speed_z <= 0.0) {
        cfg.arm_speed_z = 1.0;
    }
    if (cfg.rack_rows < 1.0) {
        cfg.rack_rows = 1.0;
    }
    if (cfg.rack_cols < 1.0) {
        cfg.rack_cols = 1.0;
    }
    if (cfg.rack_depth < 1.0) {
        cfg.rack_depth = 1.0;
    }
    if (cfg.external_network_bandwidth_bytes_per_sec == 0) {
        cfg.external_network_bandwidth_bytes_per_sec = 10000000000ull;
    }
    // 功率档位必须满足 peak >= medium >= standby >= off；违反时整体回落到默认模板，
    // 避免注册被服务端校验持续拒绝。
    if (!(cfg.power_peak_milliwatts >= cfg.power_medium_milliwatts &&
          cfg.power_medium_milliwatts >= cfg.power_standby_milliwatts &&
          cfg.power_standby_milliwatts >= cfg.power_off_milliwatts)) {
        cfg.power_peak_milliwatts = 400000;
        cfg.power_medium_milliwatts = 200000;
        cfg.power_standby_milliwatts = 50000;
        cfg.power_off_milliwatts = 0;
    }
    if (cfg.technology_generation == 0) {
        cfg.technology_generation = 1;
    }
    if (cfg.instance_generation == 0) {
        cfg.instance_generation = 1;
    }
    // 寿命均值 / 最小值必须大于 0，否则注册会被服务端 profile 校验拒绝。
    if (cfg.reliability_mean_lifetime_ms == 0) {
        cfg.reliability_mean_lifetime_ms = 315360000000ull;
    }
    if (cfg.reliability_minimum_lifetime_ms == 0) {
        cfg.reliability_minimum_lifetime_ms = 31536000000ull;
    }
    // 设备组带宽必须大于 0（hdd-cache 组要求读写至少一项 >0；光驱组要求读写都 >0），
    // 否则注册会被服务端 profile 校验拒绝 → registrar 判定为明确拒绝 → 退化为默认模板。
    if (cfg.cache_read_bytes_per_sec == 0) {
        cfg.cache_read_bytes_per_sec = 200000000ull;
    }
    if (cfg.cache_write_bytes_per_sec == 0) {
        cfg.cache_write_bytes_per_sec = 200000000ull;
    }
    // 未显式设置上报额定带宽时，从现有模拟参数派生；显式额定上限保持原值。
    const auto rate = [&](double mbps, uint64_t* bytes) {
        const long double value = static_cast<long double>(mbps) * 1000000.0L;
        if (!std::isfinite(mbps) || value < 1 ||
            value > std::numeric_limits<uint64_t>::max()) return false;
        *bytes = static_cast<uint64_t>(value);
        return true;
    };
    if ((!drive_read_configured || cfg.drive_read_bytes_per_sec == 0) &&
        !rate(cfg.read_bandwidth_mbps, &cfg.drive_read_bytes_per_sec)) {
        if (error) *error = "READ_BANDWIDTH_MBPS cannot be represented in reported bytes/sec";
        return {};
    }
    if ((!drive_write_configured || cfg.drive_write_bytes_per_sec == 0) &&
        !rate(cfg.burn_bandwidth_mbps, &cfg.drive_write_bytes_per_sec)) {
        if (error) *error = "BURN_BANDWIDTH_MBPS cannot be represented in reported bytes/sec";
        return {};
    }
    return cfg;
}

} // namespace zb::optical_node
