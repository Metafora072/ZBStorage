#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

namespace zb::sim {

struct SimIoResult {
    uint64_t bytes_written{0};
    uint64_t bytes_read{0};
    uint64_t write_hash{0};
    uint64_t read_hash{0};
    uint64_t write_elapsed_us{0};
    uint64_t read_elapsed_us{0};
};

uint64_t Fnv1a64Append(uint64_t hash, const char* data, size_t size);
std::string FormatHex64(uint64_t value);
std::string FormatBytes(uint64_t bytes);

bool WritePatternFile(const std::filesystem::path& path,
                      const std::string& label,
                      uint32_t chunk_size_bytes,
                      uint64_t total_size_bytes,
                      bool sync_on_close,
                      uint64_t* bytes_written,
                      uint64_t* hash_out,
                      uint64_t* elapsed_us,
                      std::string* error);

bool ReadFileAndHash(const std::filesystem::path& path,
                     uint32_t chunk_size_bytes,
                     uint64_t* bytes_read,
                     uint64_t* hash_out,
                     uint64_t* elapsed_us,
                     std::string* error);

bool ComputeFileHash(const std::filesystem::path& path,
                     uint64_t* hash_out,
                     uint64_t* size_out,
                     std::string* error);

} // namespace zb::sim
