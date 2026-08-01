#include "NodeConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <stdexcept>

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

} // namespace

NodeConfig NodeConfig::LoadFromFile(const std::string& path, std::string* error) {
    NodeConfig cfg;
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
        if (key == "ZB_DISKS") {
            cfg.disks_env = value;
        } else if (key == "DATA_ROOT") {
            cfg.data_root = value;
        } else if (key == "DISK_BASE_DIR") {
            cfg.disk_base_dir = value;
        } else if (key == "DISK_COUNT") {
            try {
                cfg.disk_count = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid DISK_COUNT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISK_CAPACITY_BYTES") {
            try {
                cfg.disk_capacity_bytes = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid DISK_CAPACITY_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISK_READ_BYTES_PER_SEC") {
            try {
                cfg.disk_read_bytes_per_sec = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception&) {
                if (error) *error = "Invalid DISK_READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DISK_WRITE_BYTES_PER_SEC") {
            try {
                cfg.disk_write_bytes_per_sec = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception&) {
                if (error) *error = "Invalid DISK_WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "NODE_ID") {
            cfg.node_id = value;
        } else if (key == "NODE_ADDRESS") {
            cfg.node_address = value;
        } else if (key == "SCHEDULER_ADDR") {
            cfg.scheduler_addr = value;
        } else if (key == "MDS_ADDR") {
            cfg.mds_addr = value;
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
            try {
                cfg.replication_timeout_ms = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid REPLICATION_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "NODE_WEIGHT") {
            try {
                cfg.node_weight = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid NODE_WEIGHT at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "HEARTBEAT_INTERVAL_MS") {
            try {
                cfg.heartbeat_interval_ms = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid HEARTBEAT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_REPORT_INTERVAL_MS") {
            try {
                cfg.archive_report_interval_ms = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_REPORT_TOPK") {
            try {
                cfg.archive_report_topk = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_TOPK at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_REPORT_MIN_AGE_MS") {
            try {
                cfg.archive_report_min_age_ms = static_cast<uint64_t>(std::stoull(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_MIN_AGE_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_TRACK_MAX_OBJECTS") {
            try {
                cfg.archive_track_max_objects = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid ARCHIVE_TRACK_MAX_OBJECTS at line " +
                             std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_META_DIR") {
            cfg.archive_meta_dir = value;
        } else if (key == "ARCHIVE_META_SNAPSHOT_INTERVAL_OPS") {
            try {
                cfg.archive_meta_snapshot_interval_ops = static_cast<uint32_t>(std::stoul(value));
            } catch (const std::exception&) {
                if (error) {
                    *error = "Invalid ARCHIVE_META_SNAPSHOT_INTERVAL_OPS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_META_WAL_FSYNC") {
            if (!ParseBool(value, &cfg.archive_meta_wal_fsync)) {
                if (error) {
                    *error = "Invalid ARCHIVE_META_WAL_FSYNC at line " + std::to_string(line_no);
                }
                return {};
            }
        }
    }

    if (cfg.node_role.empty()) {
        cfg.node_role = "PRIMARY";
    }
    if (cfg.archive_report_interval_ms == 0) {
        cfg.archive_report_interval_ms = 3000;
    }
    if (cfg.archive_report_topk == 0) {
        cfg.archive_report_topk = 1;
    }
    if (cfg.archive_track_max_objects == 0) {
        cfg.archive_track_max_objects = 1;
    }
    if (cfg.archive_meta_snapshot_interval_ops == 0) {
        cfg.archive_meta_snapshot_interval_ops = 1;
    }

    return cfg;
}

std::vector<DiskSpec> NodeConfig::ParseDisksEnv(std::string* error) const {
    std::vector<DiskSpec> specs;
    if (disks_env.empty()) {
        return specs;
    }

    size_t start = 0;
    while (start < disks_env.size()) {
        size_t end = disks_env.find(';', start);
        if (end == std::string::npos) {
            end = disks_env.size();
        }
        std::string token = Trim(disks_env.substr(start, end - start));
        if (!token.empty()) {
            size_t sep = token.find(':');
            if (sep == std::string::npos) {
                if (error) {
                    *error = "Invalid ZB_DISKS entry: " + token;
                }
                return {};
            }
            DiskSpec spec;
            spec.id = Trim(token.substr(0, sep));
            spec.mount_point = Trim(token.substr(sep + 1));
            if (spec.id.empty() || spec.mount_point.empty()) {
                if (error) {
                    *error = "Invalid ZB_DISKS entry (empty field): " + token;
                }
                return {};
            }
            specs.push_back(std::move(spec));
        }
        start = end + 1;
    }

    return specs;
}

} // namespace zb::real_node
