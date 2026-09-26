#include "OpticalNodeConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
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
        } else if (key == "AVAILABLE_VOLUME_ID_COUNT") {
            uint32_t count = 0;
            if (!ParseUint32(value, &count) || count > 255) {
                if (error) {
                    *error = "Invalid AVAILABLE_VOLUME_ID_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.available_volume_id_count = static_cast<uint8_t>(count);
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
        cfg.capacity_in_images = 10;
    }
    if (cfg.available_volume_id_count == 0) {
        cfg.available_volume_id_count = 5;
    }
    if (cfg.disc_capacity_bytes == 0) {
        cfg.disc_capacity_bytes = 1099511627776ULL;
    }
    if (cfg.standard_images_per_disc == 0) {
        cfg.standard_images_per_disc = 100;
    }
    return cfg;
}

} // namespace zb::optical_node
