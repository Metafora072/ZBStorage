#include "sim/SimFileIO.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <sstream>
#include <vector>

namespace zb::sim {

namespace {

constexpr uint64_t kFnv1a64Offset = 14695981039346656037ULL;

void FillPatternChunk(std::vector<char>* buffer,
                      size_t size,
                      uint64_t absolute_offset,
                      const std::string& label) {
    buffer->resize(size);
    const uint64_t label_hash = std::hash<std::string>{}(label);
    for (size_t i = 0; i < size; ++i) {
        const uint64_t value = absolute_offset + i + label_hash;
        (*buffer)[i] = static_cast<char>('A' + (value % 26ULL));
    }
}

} // namespace

uint64_t Fnv1a64Append(uint64_t hash, const char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<unsigned char>(data[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string FormatHex64(uint64_t value) {
    std::ostringstream oss;
    oss << "0x" << std::hex << std::setw(16) << std::setfill('0') << value;
    return oss.str();
}

std::string FormatBytes(uint64_t bytes) {
    const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"};
    double value = static_cast<double>(bytes);
    size_t unit = 0;
    while (value >= 1024.0 && unit + 1 < sizeof(units) / sizeof(units[0])) {
        value /= 1024.0;
        ++unit;
    }
    std::ostringstream oss;
    if (unit == 0) {
        oss << bytes << units[unit];
    } else {
        oss << std::fixed << std::setprecision(2) << value << units[unit];
    }
    return oss.str();
}

bool WritePatternFile(const std::filesystem::path& path,
                      const std::string& label,
                      uint32_t chunk_size_bytes,
                      uint64_t total_size_bytes,
                      bool sync_on_close,
                      uint64_t* bytes_written,
                      uint64_t* hash_out,
                      uint64_t* elapsed_us,
                      std::string* error) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
        if (error) {
            *error = "failed to open file for write: " + path.string();
        }
        return false;
    }

    uint64_t hash = kFnv1a64Offset;
    uint64_t written = 0;
    std::vector<char> buffer;
    const auto started_at = std::chrono::steady_clock::now();
    while (written < total_size_bytes) {
        const size_t current_size = static_cast<size_t>(
            std::min<uint64_t>(std::max<uint32_t>(1U, chunk_size_bytes), total_size_bytes - written));
        FillPatternChunk(&buffer, current_size, written, label);
        out.write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        if (!out) {
            if (error) {
                *error = "failed to write file: " + path.string();
            }
            return false;
        }
        hash = Fnv1a64Append(hash, buffer.data(), buffer.size());
        written += static_cast<uint64_t>(buffer.size());
    }

    if (sync_on_close) {
        out.flush();
        if (!out) {
            if (error) {
                *error = "failed to flush file: " + path.string();
            }
            return false;
        }
    }
    out.close();
    if (!out) {
        if (error) {
            *error = "failed to close file after write: " + path.string();
        }
        return false;
    }

    const auto finished_at = std::chrono::steady_clock::now();
    if (bytes_written) {
        *bytes_written = written;
    }
    if (hash_out) {
        *hash_out = hash;
    }
    if (elapsed_us) {
        *elapsed_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ReadFileAndHash(const std::filesystem::path& path,
                     uint32_t chunk_size_bytes,
                     uint64_t* bytes_read,
                     uint64_t* hash_out,
                     uint64_t* elapsed_us,
                     std::string* error) {
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open()) {
        if (error) {
            *error = "failed to open file for read: " + path.string();
        }
        return false;
    }

    uint64_t hash = kFnv1a64Offset;
    uint64_t read_total = 0;
    std::vector<char> buffer(std::max<uint32_t>(1U, chunk_size_bytes));
    const auto started_at = std::chrono::steady_clock::now();
    while (in) {
        in.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = in.gcount();
        if (count <= 0) {
            break;
        }
        hash = Fnv1a64Append(hash, buffer.data(), static_cast<size_t>(count));
        read_total += static_cast<uint64_t>(count);
    }
    if (in.bad()) {
        if (error) {
            *error = "failed to read file: " + path.string();
        }
        return false;
    }

    const auto finished_at = std::chrono::steady_clock::now();
    if (bytes_read) {
        *bytes_read = read_total;
    }
    if (hash_out) {
        *hash_out = hash;
    }
    if (elapsed_us) {
        *elapsed_us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(finished_at - started_at).count());
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ComputeFileHash(const std::filesystem::path& path,
                     uint64_t* hash_out,
                     uint64_t* size_out,
                     std::string* error) {
    uint64_t elapsed_us = 0;
    return ReadFileAndHash(path, 1024U * 1024U, size_out, hash_out, &elapsed_us, error);
}

} // namespace zb::sim
