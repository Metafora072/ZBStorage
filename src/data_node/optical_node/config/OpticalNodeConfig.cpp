#include "OpticalNodeConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
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

std::vector<std::string> Split(const std::string& input, char delimiter) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(input);
    while (std::getline(stream, token, delimiter)) {
        token = Trim(token);
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool ParseUint64(const std::string& text, uint64_t* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        *out = std::stoull(text);
        return true;
    } catch (const std::exception&) {
        return false;
    }
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

bool ParseDiskCapacityMap(const std::string& value,
                          std::unordered_map<std::string, uint64_t>* out) {
    if (!out) {
        return false;
    }
    out->clear();
    if (value.empty()) {
        return true;
    }
    const std::vector<std::string> items = Split(value, ',');
    for (const auto& item : items) {
        const size_t sep = item.find(':');
        if (sep == std::string::npos) {
            return false;
        }
        const std::string disk_id = Trim(item.substr(0, sep));
        const std::string capacity_text = Trim(item.substr(sep + 1));
        uint64_t capacity = 0;
        if (disk_id.empty() || !ParseUint64(capacity_text, &capacity)) {
            return false;
        }
        (*out)[disk_id] = capacity;
    }
    return true;
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
        } else if (key == "DISKS") {
            cfg.disk_ids = Split(value, ',');
        } else if (key == "ARCHIVE_ROOT") {
            cfg.archive_root = value;
        } else if (key == "CACHE_ROOT") {
            cfg.cache_root = value;
        } else if (key == "SIMULATE_IO") {
            if (!ParseBool(value, &cfg.simulate_io)) {
                if (error) {
                    *error = "Invalid SIMULATE_IO at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "OPTICAL_READ_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.optical_read_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid OPTICAL_READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "OPTICAL_WRITE_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.optical_write_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid OPTICAL_WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "CACHE_READ_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.cache_read_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid CACHE_READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "CACHE_DISC_SLOTS") {
            if (!ParseUint32(value, &cfg.cache_disc_slots)) {
                if (error) {
                    *error = "Invalid CACHE_DISC_SLOTS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "LIBRARY_DISC_SLOTS") {
            if (!ParseUint32(value, &cfg.library_disc_slots)) {
                if (error) *error = "Invalid LIBRARY_DISC_SLOTS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "MAX_IMAGE_SIZE_BYTES") {
            if (!ParseUint64(value, &cfg.max_image_size_bytes)) {
                if (error) {
                    *error = "Invalid MAX_IMAGE_SIZE_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISK_CAPACITY_BYTES") {
            if (!ParseUint64(value, &cfg.disk_capacity_bytes)) {
                if (error) {
                    *error = "Invalid DISK_CAPACITY_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISK_CAPACITY_MAP") {
            if (!ParseDiskCapacityMap(value, &cfg.disk_capacity_map)) {
                if (error) {
                    *error = "Invalid DISK_CAPACITY_MAP at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "MOUNT_POINT_PREFIX") {
            cfg.mount_point_prefix = value;
        } else if (key == "STARTUP_SCAN_MODE") {
            cfg.startup_scan_mode = value;
        }
    }

    if (cfg.disk_ids.empty()) {
        cfg.disk_ids.push_back("disk-01");
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
    if (cfg.max_image_size_bytes == 0) {
        cfg.max_image_size_bytes = 1024ULL * 1024ULL * 1024ULL;
    }
    if (cfg.archive_root.empty()) {
        cfg.archive_root = "/tmp/zb_optical";
    }
    if (cfg.cache_root.empty()) {
        cfg.cache_root = cfg.archive_root + "/cache";
    }
    if (cfg.mount_point_prefix.empty()) {
        cfg.mount_point_prefix = "/optical";
    }
    std::transform(cfg.startup_scan_mode.begin(),
                   cfg.startup_scan_mode.end(),
                   cfg.startup_scan_mode.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (cfg.startup_scan_mode != "full" && cfg.startup_scan_mode != "fast") {
        cfg.startup_scan_mode = "fast";
    }
    if (cfg.optical_read_bytes_per_sec == 0) {
        cfg.optical_read_bytes_per_sec = 1;
    }
    if (cfg.optical_write_bytes_per_sec == 0) {
        cfg.optical_write_bytes_per_sec = 1;
    }
    if (cfg.cache_read_bytes_per_sec == 0) {
        cfg.cache_read_bytes_per_sec = 1;
    }
    if (cfg.cache_disc_slots == 0) {
        cfg.cache_disc_slots = 1;
    }
    if (cfg.library_disc_slots == 0) cfg.library_disc_slots = 10000;
    return cfg;
}

} // namespace zb::optical_node
