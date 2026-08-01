#include "SchedulerConfig.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <stdexcept>

namespace zb::scheduler {

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

bool ParseDouble(const std::string& text, double* out) {
    if (!out || text.empty()) {
        return false;
    }
    try {
        size_t consumed = 0;
        const double value = std::stod(text, &consumed);
        if (consumed != text.size()) {
            return false;
        }
        *out = value;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

bool ParseBool(const std::string& text, bool* out) {
    if (!out) {
        return false;
    }
    std::string lowered = text;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (lowered == "true" || lowered == "1" || lowered == "yes") {
        *out = true;
        return true;
    }
    if (lowered == "false" || lowered == "0" || lowered == "no") {
        *out = false;
        return true;
    }
    return false;
}

} // namespace

SchedulerConfig SchedulerConfig::LoadFromFile(const std::string& path, std::string* error) {
    SchedulerConfig cfg;
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
        if (key == "SUSPECT_TIMEOUT_MS") {
            if (!ParseUint64(value, &cfg.suspect_timeout_ms)) {
                if (error) {
                    *error = "Invalid SUSPECT_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "DEAD_TIMEOUT_MS") {
            if (!ParseUint64(value, &cfg.dead_timeout_ms)) {
                if (error) {
                    *error = "Invalid DEAD_TIMEOUT_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "TICK_INTERVAL_MS") {
            if (!ParseUint64(value, &cfg.tick_interval_ms)) {
                if (error) {
                    *error = "Invalid TICK_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS") {
            if (!ParseUint64(value, &cfg.cluster_view_snapshot_interval_ms)) {
                if (error) {
                    *error = "Invalid CLUSTER_VIEW_SNAPSHOT_INTERVAL_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_PEAK_UTILIZATION") {
            if (!ParseDouble(value, &cfg.power_peak_utilization)) {
                if (error) {
                    *error = "Invalid POWER_PEAK_UTILIZATION at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_PEAK_QUEUE_DEPTH") {
            uint64_t parsed = 0;
            if (!ParseUint64(value, &parsed) || parsed > UINT32_MAX) {
                if (error) {
                    *error = "Invalid POWER_PEAK_QUEUE_DEPTH at line " + std::to_string(line_no);
                }
                return {};
            }
            cfg.power_peak_queue_depth = static_cast<uint32_t>(parsed);
        } else if (key == "POWER_METRICS_FRESHNESS_MS") {
            if (!ParseUint64(value, &cfg.power_metrics_freshness_ms)) {
                if (error) {
                    *error = "Invalid POWER_METRICS_FRESHNESS_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_STANDBY_AFTER_MS") {
            if (!ParseUint64(value, &cfg.power_standby_after_ms)) {
                if (error) {
                    *error = "Invalid POWER_STANDBY_AFTER_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_OFF_AFTER_MS") {
            if (!ParseUint64(value, &cfg.power_off_after_ms)) {
                if (error) {
                    *error = "Invalid POWER_OFF_AFTER_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_MINIMUM_RESIDENCY_MS") {
            if (!ParseUint64(value, &cfg.power_minimum_residency_ms)) {
                if (error) {
                    *error = "Invalid POWER_MINIMUM_RESIDENCY_MS at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_ALLOW_OFF_WITH_RESIDENT_DATA") {
            if (!ParseBool(value, &cfg.power_allow_off_with_resident_data)) {
                if (error) {
                    *error = "Invalid POWER_ALLOW_OFF_WITH_RESIDENT_DATA at line " + std::to_string(line_no);
                }
                return {};
            }
        } else if (key == "POWER_MIN_ONLINE_STORAGE_NODES" ||
                   key == "POWER_MIN_ONLINE_METADATA_NODES" ||
                   key == "POWER_MIN_ONLINE_OPTICAL_NODES") {
            uint64_t parsed = 0;
            if (!ParseUint64(value, &parsed) || parsed > UINT32_MAX) {
                if (error) *error = "Invalid " + key + " at line " + std::to_string(line_no);
                return {};
            }
            if (key == "POWER_MIN_ONLINE_STORAGE_NODES")
                cfg.power_min_online_storage_nodes = static_cast<uint32_t>(parsed);
            else if (key == "POWER_MIN_ONLINE_METADATA_NODES")
                cfg.power_min_online_metadata_nodes = static_cast<uint32_t>(parsed);
            else
                cfg.power_min_online_optical_nodes = static_cast<uint32_t>(parsed);
        } else if (key == "POWER_MIN_ONLINE_STORAGE_CAPACITY_BYTES") {
            if (!ParseUint64(value, &cfg.power_min_online_storage_capacity_bytes)) {
                if (error) *error = "Invalid POWER_MIN_ONLINE_STORAGE_CAPACITY_BYTES at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_MIN_ONLINE_STORAGE_FREE_BYTES") {
            if (!ParseUint64(value, &cfg.power_min_online_storage_free_bytes)) {
                if (error) *error = "Invalid POWER_MIN_ONLINE_STORAGE_FREE_BYTES at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_ACTUATION_ENABLED") {
            if (!ParseBool(value, &cfg.power_actuation_enabled)) {
                if (error) *error = "Invalid POWER_ACTUATION_ENABLED at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_ACTUATION_RETRY_MS") {
            if (!ParseUint64(value, &cfg.power_actuation_retry_ms) || cfg.power_actuation_retry_ms == 0) {
                if (error) *error = "Invalid POWER_ACTUATION_RETRY_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "POWER_WAKE_CMD_TEMPLATE") {
            cfg.power_wake_cmd_template = value;
        } else if (key == "POWER_STANDBY_CMD_TEMPLATE") {
            cfg.power_standby_cmd_template = value;
        } else if (key == "POWER_OFF_CMD_TEMPLATE") {
            cfg.power_off_cmd_template = value;
        } else if (key == "METRICS_HISTORY_MAX_SAMPLES") {
            uint64_t parsed = 0;
            if (!ParseUint64(value, &parsed) || parsed > 1000000) {
                if (error) *error = "Invalid METRICS_HISTORY_MAX_SAMPLES at line " + std::to_string(line_no);
                return {};
            }
            cfg.metrics_history_max_samples = static_cast<uint32_t>(parsed);
        } else if (key == "MDS_ADDRESS") {
            cfg.mds_address = value;
        } else if (key == "MIGRATION_RPC_TIMEOUT_MS") {
            uint64_t parsed = 0;
            if (!ParseUint64(value, &parsed) || parsed == 0 || parsed > UINT32_MAX) {
                if (error) *error = "Invalid MIGRATION_RPC_TIMEOUT_MS at line " + std::to_string(line_no);
                return {};
            }
            cfg.migration_rpc_timeout_ms = static_cast<uint32_t>(parsed);
        } else if (key == "MIGRATION_COPY_CHUNK_BYTES") {
            if (!ParseUint64(value, &cfg.migration_copy_chunk_bytes) || cfg.migration_copy_chunk_bytes == 0) {
                if (error) *error = "Invalid MIGRATION_COPY_CHUNK_BYTES at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "MIGRATION_RETRY_BASE_MS") {
            if (!ParseUint64(value, &cfg.migration_retry_base_ms) || cfg.migration_retry_base_ms == 0) {
                if (error) *error = "Invalid MIGRATION_RETRY_BASE_MS at line " + std::to_string(line_no);
                return {};
            }
        } else if (key == "MIGRATION_MAX_RETRIES") {
            uint64_t parsed = 0;
            if (!ParseUint64(value, &parsed) || parsed == 0 || parsed > UINT32_MAX) {
                if (error) *error = "Invalid MIGRATION_MAX_RETRIES at line " + std::to_string(line_no);
                return {};
            }
            cfg.migration_max_retries = static_cast<uint32_t>(parsed);
        } else if (key == "START_CMD_TEMPLATE") {
            cfg.start_cmd_template = value;
        } else if (key == "STOP_CMD_TEMPLATE") {
            cfg.stop_cmd_template = value;
        } else if (key == "REBOOT_CMD_TEMPLATE") {
            cfg.reboot_cmd_template = value;
        } else if (key == "CLUSTER_VIEW_SNAPSHOT_PATH") {
            cfg.cluster_view_snapshot_path = value;
        } else if (key == "MANAGED_STATE_SNAPSHOT_PATH") {
            cfg.managed_state_snapshot_path = value;
        }
    }

    if (cfg.power_peak_utilization < 0.0 || cfg.power_peak_utilization > 1.0) {
        if (error) {
            *error = "POWER_PEAK_UTILIZATION must be in [0, 1]";
        }
        return {};
    }
    if (cfg.power_off_after_ms <= cfg.power_standby_after_ms) {
        if (error) {
            *error = "POWER_OFF_AFTER_MS must be greater than POWER_STANDBY_AFTER_MS";
        }
        return {};
    }
    if (cfg.power_actuation_enabled &&
        (cfg.power_wake_cmd_template.empty() || cfg.power_standby_cmd_template.empty() ||
         cfg.power_off_cmd_template.empty())) {
        if (error) *error = "all POWER_*_CMD_TEMPLATE values are required when power actuation is enabled";
        return {};
    }

    return cfg;
}

} // namespace zb::scheduler
