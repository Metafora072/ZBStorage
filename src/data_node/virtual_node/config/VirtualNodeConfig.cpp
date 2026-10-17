#include "VirtualNodeConfig.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace zb::virtual_node {

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
    if (!out || text.empty()) return false;
    uint64_t value = 0;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) return false;
    *out = value;
    return true;
}

bool ParseUint32(const std::string& text, uint32_t* out) {
    uint64_t value = 0;
    if (!ParseUint64(text, &value) || value > std::numeric_limits<uint32_t>::max()) return false;
    *out = static_cast<uint32_t>(value);
    return true;
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

VirtualNodeConfig VirtualNodeConfig::LoadFromFile(const std::string& path, std::string* error) {
    VirtualNodeConfig cfg;
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
        } else if (key == "INSTANCE_GENERATION") {
            if (!ParseUint64(value, &cfg.instance_generation) || cfg.instance_generation == 0) {
                if (error) *error = "Invalid INSTANCE_GENERATION at line " + std::to_string(line_no);
                return {};
            }
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
        } else if (key == "ARCHIVE_REPORT_INTERVAL_MS") {
            if (!ParseUint32(value, &cfg.archive_report_interval_ms)) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_REPORT_TOPK") {
            if (!ParseUint32(value, &cfg.archive_report_topk)) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_TOPK at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_REPORT_MIN_AGE_MS") {
            if (!ParseUint64(value, &cfg.archive_report_min_age_ms)) {
                if (error) {
                    *error = "Invalid ARCHIVE_REPORT_MIN_AGE_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_TRACK_MAX_OBJECTS") {
            if (!ParseUint32(value, &cfg.archive_track_max_objects)) {
                if (error) {
                    *error = "Invalid ARCHIVE_TRACK_MAX_OBJECTS at line " +
                             std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "ARCHIVE_META_DIR") {
            cfg.archive_meta_dir = value;
        } else if (key == "ARCHIVE_META_SNAPSHOT_INTERVAL_OPS") {
            if (!ParseUint32(value, &cfg.archive_meta_snapshot_interval_ops)) {
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
        } else if (key == "ALLOW_DYNAMIC_DISKS") {
            if (!ParseBool(value, &cfg.allow_dynamic_disks)) {
                if (error) {
                    *error = "Invalid ALLOW_DYNAMIC_DISKS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DISKS" || key == "DISK_IDS") {
            cfg.disk_ids = Split(value, ',');
        } else if (key == "READ_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.read_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid READ_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "WRITE_BYTES_PER_SEC") {
            if (!ParseUint64(value, &cfg.write_bytes_per_sec)) {
                if (error) {
                    *error = "Invalid WRITE_BYTES_PER_SEC at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "READ_MBPS") {
            uint64_t mbps = 0;
            if (!ParseUint64(value, &mbps) ||
                mbps > std::numeric_limits<uint64_t>::max() / (1024ULL * 1024ULL)) {
                if (error) {
                    *error = "Invalid READ_MBPS at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.read_bytes_per_sec = mbps * 1024ULL * 1024ULL;
        } else if (key == "WRITE_MBPS") {
            uint64_t mbps = 0;
            if (!ParseUint64(value, &mbps) ||
                mbps > std::numeric_limits<uint64_t>::max() / (1024ULL * 1024ULL)) {
                if (error) {
                    *error = "Invalid WRITE_MBPS at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.write_bytes_per_sec = mbps * 1024ULL * 1024ULL;
        } else if (key == "READ_BASE_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.read_base_latency_ms)) {
                if (error) {
                    *error = "Invalid READ_BASE_LATENCY_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "WRITE_BASE_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.write_base_latency_ms)) {
                if (error) {
                    *error = "Invalid WRITE_BASE_LATENCY_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "WAKE_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.wake_latency_ms) ||
                cfg.wake_latency_ms > 3600000) {
                if (error) *error = "Invalid WAKE_LATENCY_MS at line " +
                                    std::to_string(line_no);
                return {};
            }
        } else if (key == "REBOOT_LATENCY_MS") {
            if (!ParseUint32(value, &cfg.reboot_latency_ms) || cfg.reboot_latency_ms > 3600000) {
                if (error) *error = "Invalid REBOOT_LATENCY_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "JITTER_MS") {
            if (!ParseUint32(value, &cfg.jitter_ms)) {
                if (error) {
                    *error = "Invalid JITTER_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "JITTER_SEED") {
            if (!ParseUint32(value, &cfg.jitter_seed)) {
                if (error) *error = "Invalid JITTER_SEED at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "IO_FAILURE_PROBABILITY_PPM" || key == "IO_TIMEOUT_PROBABILITY_PPM") {
            uint32_t probability = 0;
            if (!ParseUint32(value, &probability) || probability > 1000000) {
                if (error) *error = "Invalid " + key + " at line " + std::to_string(line_no);
                return {};
            }
            if (key == "IO_FAILURE_PROBABILITY_PPM") cfg.io_failure_probability_ppm = probability;
            else cfg.io_timeout_probability_ppm = probability;
        } else if (key == "IO_TIMEOUT_MS") {
            if (!ParseUint32(value, &cfg.io_timeout_ms) || cfg.io_timeout_ms == 0 || cfg.io_timeout_ms > 3600000) {
                if (error) *error = "Invalid IO_TIMEOUT_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "MAX_CONCURRENT_IO") {
            if (!ParseUint32(value, &cfg.max_concurrent_io) || cfg.max_concurrent_io == 0 ||
                cfg.max_concurrent_io > 1024) {
                if (error) *error = "Invalid MAX_CONCURRENT_IO at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "IO_QUEUE_LIMIT") {
            if (!ParseUint32(value, &cfg.io_queue_limit) || cfg.io_queue_limit > 65536) {
                if (error) *error = "Invalid IO_QUEUE_LIMIT at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "DISK_CAPACITY_BYTES") {
            if (!ParseUint64(value, &cfg.disk_capacity_bytes)) {
                if (error) {
                    *error = "Invalid DISK_CAPACITY_BYTES at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "MOUNT_POINT_PREFIX") {
            cfg.mount_point_prefix = value;
        }
    }

    if (static_cast<uint64_t>(cfg.io_failure_probability_ppm) + cfg.io_timeout_probability_ppm > 1000000) {
        if (error) *error = "IO failure and timeout probabilities must sum to <= 1000000 ppm";
        return {};
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
    if (cfg.mount_point_prefix.empty()) {
        cfg.mount_point_prefix = "/virtual";
    }
    return cfg;
}

} // namespace zb::virtual_node
