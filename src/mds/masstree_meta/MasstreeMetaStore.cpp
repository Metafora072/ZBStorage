#include "MasstreeMetaStore.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <sstream>
#include <string_view>

#include "MasstreeInodeRecordCodec.h"
#include "MasstreeOpticalLayoutTranslator.h"
#include "MasstreeOpticalProfile.h"
#include "MasstreePageReader.h"

namespace zb::mds {

namespace {

constexpr const char* kMasstreeReaddirTokenPrefix = "zb-masstree-readdir-v1:";

struct InodeRecordView {
    uint64_t inode_id{0};
    std::string payload;
};

struct DentryRecordView {
    uint64_t parent_inode{0};
    std::string name;
    uint64_t child_inode{0};
    zb::rpc::InodeType type{zb::rpc::INODE_FILE};
};

std::vector<std::string> SplitPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(path);
    while (std::getline(stream, token, '/')) {
        if (!token.empty()) {
            parts.push_back(token);
        }
    }
    return parts;
}

bool NormalizePath(std::string path, std::string* normalized) {
    if (!normalized || path.empty()) {
        return false;
    }
    std::replace(path.begin(), path.end(), '\\', '/');
    if (path.empty() || path.front() != '/') {
        path.insert(path.begin(), '/');
    }
    std::string out;
    out.reserve(path.size() + 1);
    bool prev_slash = false;
    for (char ch : path) {
        if (ch == '/') {
            if (prev_slash) {
                continue;
            }
            prev_slash = true;
            out.push_back(ch);
            continue;
        }
        prev_slash = false;
        out.push_back(ch);
    }
    while (out.size() > 1 && out.back() == '/') {
        out.pop_back();
    }
    if (out.empty()) {
        out = "/";
    }
    *normalized = std::move(out);
    return true;
}

uint32_t DecodeLe32(const char* data) {
    uint32_t value = 0;
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

uint64_t DecodeLe64(const char* data) {
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(data[i])) << (i * 8U);
    }
    return value;
}

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8U);
}

bool ReadExact(std::ifstream* input, char* data, size_t len) {
    return input && data && input->read(data, static_cast<std::streamsize>(len)).good();
}

bool ReadString(std::ifstream* input, size_t len, std::string* out) {
    if (!out) {
        return false;
    }
    out->assign(len, '\0');
    return len == 0 ? true : ReadExact(input, &(*out)[0], len);
}

bool ReadInodeRecord(std::ifstream* input, InodeRecordView* record, std::string* error) {
    if (!input || !record) {
        if (error) {
            *error = "invalid masstree inode record read args";
        }
        return false;
    }
    char inode_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, inode_buf, sizeof(inode_buf))) {
        if (input->eof()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        if (error) {
            *error = "failed to read masstree inode record header";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!ReadExact(input, len_buf, sizeof(len_buf))) {
        if (error) {
            *error = "corrupted masstree inode record length";
        }
        return false;
    }
    record->inode_id = DecodeLe64(inode_buf);
    const uint32_t payload_len = DecodeLe32(len_buf);
    if (!ReadString(input, payload_len, &record->payload)) {
        if (error) {
            *error = "corrupted masstree inode record payload";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool ReadDentryRecord(std::ifstream* input, DentryRecordView* record, std::string* error) {
    if (!input || !record) {
        if (error) {
            *error = "invalid masstree dentry record read args";
        }
        return false;
    }
    char parent_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, parent_buf, sizeof(parent_buf))) {
        if (input->eof()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        if (error) {
            *error = "failed to read masstree dentry record header";
        }
        return false;
    }
    char name_len_buf[sizeof(uint16_t)] = {};
    if (!ReadExact(input, name_len_buf, sizeof(name_len_buf))) {
        if (error) {
            *error = "corrupted masstree dentry record name length";
        }
        return false;
    }
    const uint16_t name_len = DecodeLe16(name_len_buf);
    if (!ReadString(input, name_len, &record->name)) {
        if (error) {
            *error = "corrupted masstree dentry record name";
        }
        return false;
    }
    char child_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, child_buf, sizeof(child_buf))) {
        if (error) {
            *error = "corrupted masstree dentry record child inode";
        }
        return false;
    }
    char raw_type = 0;
    if (!ReadExact(input, &raw_type, sizeof(raw_type))) {
        if (error) {
            *error = "corrupted masstree dentry record type";
        }
        return false;
    }
    record->parent_inode = DecodeLe64(parent_buf);
    record->child_inode = DecodeLe64(child_buf);
    record->type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(raw_type));
    if (error) {
        error->clear();
    }
    return true;
}

char EncodeHexNibble(unsigned char value) {
    return static_cast<char>(value < 10 ? ('0' + value) : ('a' + (value - 10)));
}

bool DecodeHexNibble(char ch, unsigned char* value) {
    if (!value) {
        return false;
    }
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (uch >= '0' && uch <= '9') {
        *value = static_cast<unsigned char>(uch - '0');
        return true;
    }
    if (uch >= 'a' && uch <= 'f') {
        *value = static_cast<unsigned char>(uch - 'a' + 10);
        return true;
    }
    if (uch >= 'A' && uch <= 'F') {
        *value = static_cast<unsigned char>(uch - 'A' + 10);
        return true;
    }
    return false;
}

std::string HexEncode(std::string_view input) {
    std::string output;
    output.reserve(input.size() * 2U);
    for (unsigned char ch : input) {
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch >> 4U)));
        output.push_back(EncodeHexNibble(static_cast<unsigned char>(ch & 0x0FU)));
    }
    return output;
}

bool HexDecode(std::string_view input, std::string* output) {
    if (!output || (input.size() % 2U) != 0U) {
        return false;
    }
    output->clear();
    output->reserve(input.size() / 2U);
    for (size_t i = 0; i < input.size(); i += 2U) {
        unsigned char hi = 0;
        unsigned char lo = 0;
        if (!DecodeHexNibble(input[i], &hi) || !DecodeHexNibble(input[i + 1U], &lo)) {
            return false;
        }
        output->push_back(static_cast<char>((hi << 4U) | lo));
    }
    return true;
}

std::string EncodeMasstreeReaddirToken(const std::string& generation_id, const std::string& last_name) {
    std::string payload = generation_id;
    payload.push_back('\0');
    payload.append(last_name);
    return std::string(kMasstreeReaddirTokenPrefix) + HexEncode(payload);
}

bool DecodeMasstreeReaddirToken(const std::string& token,
                                std::string* generation_id,
                                std::string* last_name) {
    if (!generation_id || !last_name ||
        token.rfind(kMasstreeReaddirTokenPrefix, 0) != 0) {
        return false;
    }
    std::string payload;
    if (!HexDecode(token.substr(std::char_traits<char>::length(kMasstreeReaddirTokenPrefix)), &payload)) {
        return false;
    }
    const size_t separator = payload.find('\0');
    if (separator == std::string::npos) {
        return false;
    }
    *generation_id = payload.substr(0, separator);
    *last_name = payload.substr(separator + 1U);
    return true;
}

bool BuildMasstreeOpticalFileLocation(const UnifiedInodeRecord& inode,
                                      uint64_t inode_id,
                                      zb::rpc::OpticalFileLocation* location,
                                      std::string* error) {
    if (!location) {
        if (error) {
            *error = "optical location output is null";
        }
        return false;
    }
    if (inode.inode_type != static_cast<uint8_t>(zb::rpc::INODE_FILE) ||
        inode.storage_tier != static_cast<uint8_t>(UnifiedStorageTier::kOptical)) {
        if (error) {
            error->clear();
        }
        return false;
    }

    const MasstreeOpticalProfile profile = MasstreeOpticalProfile::Fixed();
    if (inode.optical_node_id > std::numeric_limits<uint32_t>::max() ||
        inode.optical_disk_id > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "invalid masstree optical location ids";
        }
        return false;
    }
    const uint32_t node_index = static_cast<uint32_t>(inode.optical_node_id);
    const uint32_t disk_index = static_cast<uint32_t>(inode.optical_disk_id);
    const uint32_t image_index_in_disk = inode.optical_image_id;
    if (node_index >= profile.optical_node_count ||
        disk_index >= profile.disks_per_node ||
        image_index_in_disk >= profile.ImagesPerDisk(disk_index)) {
        if (error) {
            *error = "invalid masstree optical location indexes";
        }
        return false;
    }

    location->Clear();
    location->set_node_id(profile.NodeId(node_index));
    location->set_disk_id(profile.DiskId(disk_index));
    location->set_image_id(profile.ImageId(profile.GlobalImageId(node_index, disk_index, image_index_in_disk)));
    location->set_file_id("obj-" + std::to_string(inode_id) + "-0");
    if (error) {
        error->clear();
    }
    return true;
}

bool ConvertLegacyOpticalLocationToCurrent(UnifiedInodeRecord* inode, std::string* error) {
    if (!inode || inode->storage_tier != static_cast<uint8_t>(UnifiedStorageTier::kOptical)) {
        return true;
    }
    if (inode->optical_node_id > std::numeric_limits<uint32_t>::max() ||
        inode->optical_disk_id > std::numeric_limits<uint32_t>::max()) {
        if (error) {
            *error = "legacy masstree optical location ids exceed supported range";
        }
        return false;
    }

    const MasstreeOpticalLayoutTranslator translator;
    uint32_t node_index = 0;
    uint32_t disk_index = 0;
    uint32_t image_index_in_disk = 0;
    if (!translator.TranslateLegacyToActive(static_cast<uint32_t>(inode->optical_node_id),
                                            static_cast<uint32_t>(inode->optical_disk_id),
                                            inode->optical_image_id,
                                            &node_index,
                                            &disk_index,
                                            &image_index_in_disk,
                                            error)) {
        if (error) {
            *error = "failed to convert legacy masstree optical location";
        }
        return false;
    }
    inode->optical_node_id = node_index;
    inode->optical_disk_id = disk_index;
    inode->optical_image_id = image_index_in_disk;
    inode->storage_loc0 = inode->optical_node_id;
    inode->storage_loc1 = (inode->optical_disk_id << 32U) |
                          static_cast<uint64_t>(inode->optical_image_id);
    return true;
}

} // namespace

bool MasstreeMetaStore::ResolvePath(const MasstreeNamespaceRoute& route,
                                    const std::string& path,
                                    uint64_t* inode_id,
                                    zb::rpc::InodeAttr* attr,
                                    std::string* error) const {
    if (!inode_id) {
        if (error) {
            *error = "inode_id output is null";
        }
        return false;
    }

    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    std::string relative_path;
    if (!StripRoutePrefix(route.path_prefix, path, &relative_path, error)) {
        return false;
    }
    std::vector<std::string> parts = SplitPath(relative_path);
    uint64_t current = generation->manifest->root_inode_id;
    if (parts.empty()) {
        *inode_id = current;
        if (attr) {
            return ReadInodeAttr(*generation, current, attr, error);
        }
        if (error) {
            error->clear();
        }
        return true;
    }

    for (const auto& name : parts) {
        uint64_t next = 0;
        if (!FindDentry(*generation, current, name, &next, nullptr, error)) {
            return false;
        }
        current = next;
    }

    *inode_id = current;
    if (attr) {
        return ReadInodeAttr(*generation, current, attr, error);
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::GetInode(const MasstreeNamespaceRoute& route,
                                 uint64_t inode_id,
                                 zb::rpc::InodeAttr* attr,
                                 std::string* error) const {
    if (!attr || inode_id == 0) {
        if (error) {
            *error = "invalid masstree inode lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    return ReadInodeAttr(*generation, inode_id, attr, error);
}

bool MasstreeMetaStore::BuildFullPath(const MasstreeNamespaceRoute& route,
                                      uint64_t inode_id,
                                      std::string* full_path,
                                      std::string* file_name,
                                      std::string* error) const {
    if (!full_path || !file_name || inode_id == 0) {
        if (error) {
            *error = "invalid masstree full path lookup args";
        }
        return false;
    }

    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    std::vector<std::string> components;
    uint64_t current = inode_id;
    while (current != 0) {
        UnifiedInodeRecord inode;
        if (!ReadUnifiedInode(*generation, current, &inode, error)) {
            return false;
        }
        if (current == generation->manifest->root_inode_id) {
            break;
        }
        if (inode.file_name.empty() || inode.file_name == "/") {
            if (error) {
                *error = "masstree inode missing file name";
            }
            return false;
        }
        components.push_back(inode.file_name);
        if (inode.parent_inode_id == 0) {
            if (error) {
                *error = "masstree inode parent chain is broken";
            }
            return false;
        }
        current = inode.parent_inode_id;
    }

    if (current != generation->manifest->root_inode_id) {
        if (error) {
            *error = "masstree inode is outside generation root";
        }
        return false;
    }

    std::string normalized_prefix;
    if (!NormalizePath(route.path_prefix, &normalized_prefix)) {
        if (error) {
            *error = "invalid masstree route prefix";
        }
        return false;
    }
    if (normalized_prefix.size() > 1 && normalized_prefix.back() == '/') {
        normalized_prefix.pop_back();
    }

    *file_name = components.empty() ? "/" : components.front();
    *full_path = normalized_prefix;
    for (auto it = components.rbegin(); it != components.rend(); ++it) {
        if (full_path->empty() || full_path->back() != '/') {
            full_path->push_back('/');
        }
        full_path->append(*it);
    }
    if (full_path->empty()) {
        *full_path = "/";
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::GetOpticalFileLocation(const MasstreeNamespaceRoute& route,
                                               uint64_t inode_id,
                                               zb::rpc::OpticalFileLocation* location,
                                               std::string* error) const {
    if (!location || inode_id == 0) {
        if (error) {
            *error = "invalid masstree optical location lookup args";
        }
        return false;
    }
    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }
    UnifiedInodeRecord inode;
    if (!ReadUnifiedInode(*generation, inode_id, &inode, error)) {
        return false;
    }
    return BuildMasstreeOpticalFileLocation(inode, inode_id, location, error);
}

bool MasstreeMetaStore::Readdir(const MasstreeNamespaceRoute& route,
                                const std::string& path,
                                const std::string& start_after,
                                uint32_t limit,
                                std::vector<zb::rpc::Dentry>* entries,
                                bool* has_more,
                                std::string* next_token,
                                std::string* error) const {
    if (!entries) {
        if (error) {
            *error = "entries output is null";
        }
        return false;
    }
    entries->clear();
    if (has_more) {
        *has_more = false;
    }
    if (next_token) {
        next_token->clear();
    }

    std::shared_ptr<LoadedGeneration> generation;
    if (!EnsureGenerationLoaded(route, &generation, error)) {
        return false;
    }

    uint64_t inode_id = 0;
    zb::rpc::InodeAttr attr;
    if (!ResolvePath(route, path, &inode_id, &attr, error)) {
        return false;
    }
    if (attr.type() != zb::rpc::INODE_DIR) {
        if (error) {
            *error = "not a directory";
        }
        return false;
    }

    std::string decoded_start_after;
    if (start_after.empty()) {
        decoded_start_after.clear();
    } else if (start_after.rfind(kMasstreeReaddirTokenPrefix, 0) == 0) {
        std::string token_generation_id;
        if (!DecodeMasstreeReaddirToken(start_after, &token_generation_id, &decoded_start_after)) {
            if (error) {
                *error = "invalid masstree readdir token";
            }
            return false;
        }
        if (token_generation_id != generation->manifest->generation_id) {
            if (error) {
                *error = "masstree readdir token generation mismatch";
            }
            return false;
        }
    } else {
        decoded_start_after = start_after;
    }

    MasstreeDentrySparseEntry sparse_entry;
    if (!generation->runtime->FindDentryPageBoundary(generation->manifest->namespace_id,
                                                     inode_id,
                                                     decoded_start_after,
                                                     &sparse_entry,
                                                     error)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    uint64_t page_offset = sparse_entry.page_offset;
    while (entries->size() < limit) {
        MasstreeDentryPage page;
        if (!MasstreePageReader::LoadDentryPage(generation->manifest->dentry_pages_path,
                                                page_offset,
                                                &page,
                                                error)) {
            return false;
        }
        size_t index = entries->empty()
                           ? MasstreePageReader::LowerBoundDentryInPage(page, inode_id, decoded_start_after)
                           : 0;
        if (entries->empty() && !decoded_start_after.empty() && index < page.entries.size() &&
            page.entries[index].parent_inode == inode_id &&
            page.entries[index].name == decoded_start_after) {
            ++index;
        }
        for (; index < page.entries.size(); ++index) {
            const auto& hit = page.entries[index];
            if (hit.parent_inode != inode_id) {
                if (error) {
                    error->clear();
                }
                return true;
            }
            zb::rpc::Dentry entry;
            entry.set_name(hit.name);
            entry.set_inode_id(hit.child_inode);
            entry.set_type(hit.type);
            entries->push_back(std::move(entry));
            if (entries->size() == limit) {
                if (has_more) {
                    *has_more = index + 1 < page.entries.size() ||
                                (page.next_page_offset > page.page_offset);
                }
                if (next_token && has_more && *has_more) {
                    *next_token = EncodeMasstreeReaddirToken(generation->manifest->generation_id, hit.name);
                }
                if (error) {
                    error->clear();
                }
                return true;
            }
        }
        if (page.entries.empty() ||
            page.entries.back().parent_inode != inode_id ||
            page.next_page_offset <= page.page_offset) {
            break;
        }
        page_offset = page.next_page_offset;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::EstimateGenerationSparseMemoryBytes(const MasstreeNamespaceRoute& route,
                                                            double estimate_multiplier,
                                                            uint64_t* bytes,
                                                            std::string* error) const {
    if (!bytes || !(estimate_multiplier > 0.0)) {
        if (error) {
            *error = "invalid masstree preload estimate args";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    if (!MasstreeNamespaceManifest::LoadFromFile(ResolveManifestPath(route), &manifest, error)) {
        return false;
    }

    std::error_code ec;
    const uint64_t inode_sparse_bytes = std::filesystem::file_size(manifest.inode_sparse_index_path, ec);
    if (ec) {
        if (error) {
            *error = "failed to stat masstree inode sparse index: " + manifest.inode_sparse_index_path;
        }
        return false;
    }
    const uint64_t dentry_sparse_bytes = std::filesystem::file_size(manifest.dentry_sparse_index_path, ec);
    if (ec) {
        if (error) {
            *error = "failed to stat masstree dentry sparse index: " + manifest.dentry_sparse_index_path;
        }
        return false;
    }

    const long double total_sparse_bytes =
        static_cast<long double>(inode_sparse_bytes) + static_cast<long double>(dentry_sparse_bytes);
    *bytes = static_cast<uint64_t>(total_sparse_bytes * static_cast<long double>(estimate_multiplier));
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::PreloadGeneration(const MasstreeNamespaceRoute& route, std::string* error) const {
    std::shared_ptr<LoadedGeneration> generation;
    return EnsureGenerationLoaded(route, &generation, error);
}

bool MasstreeMetaStore::PreloadAllGenerations(const std::vector<MasstreeNamespaceRoute>& routes,
                                              PreloadReport* report,
                                              std::string* error) const {
    const auto started_at = std::chrono::steady_clock::now();
    if (report) {
        *report = PreloadReport{};
        report->route_count = static_cast<uint64_t>(routes.size());
    }
    for (const auto& route : routes) {
        std::string local_error;
        if (!PreloadGeneration(route, &local_error)) {
            if (report) {
                ++report->failed_count;
            }
            if (error && error->empty()) {
                *error = "failed to preload route " + route.namespace_id + ": " + local_error;
            }
            continue;
        }
        if (report) {
            ++report->loaded_count;
        }
    }
    if (report) {
        report->elapsed_ms = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at)
                .count());
    }
    if (error && (!report || report->failed_count == 0)) {
        error->clear();
    }
    return !report || report->failed_count == 0;
}

bool MasstreeMetaStore::EnsureGenerationLoaded(const MasstreeNamespaceRoute& route,
                                               std::shared_ptr<LoadedGeneration>* generation,
                                               std::string* error) const {
    if (!generation) {
        if (error) {
            *error = "generation output is null";
        }
        return false;
    }
    const std::string manifest_path = ResolveManifestPath(route);
    if (manifest_path.empty()) {
        if (error) {
            *error = "masstree manifest path is empty";
        }
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        auto it = generation_cache_.find(manifest_path);
        if (it != generation_cache_.end()) {
            *generation = it->second;
            if (error) {
                error->clear();
            }
            return true;
        }
    }

    MasstreeNamespaceManifest parsed;
    if (!MasstreeNamespaceManifest::LoadFromFile(manifest_path, &parsed, error)) {
        return false;
    }

    auto loaded = std::make_shared<LoadedGeneration>();
    loaded->manifest = std::make_shared<MasstreeNamespaceManifest>(std::move(parsed));
    loaded->runtime = std::make_shared<MasstreeIndexRuntime>();
    if (!loaded->runtime->Init(error)) {
        return false;
    }
    if (!LoadGenerationData(*loaded->manifest, loaded->runtime.get(), error)) {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(generation_mu_);
        generation_cache_[manifest_path] = loaded;
    }
    *generation = std::move(loaded);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::LoadGenerationData(const MasstreeNamespaceManifest& manifest,
                                           MasstreeIndexRuntime* runtime,
                                           std::string* error) const {
    if (!runtime) {
        if (error) {
            *error = "masstree runtime is null";
        }
        return false;
    }

    uint64_t inode_page_count = 0;
    uint64_t dentry_page_count = 0;

    std::ifstream inode_sparse_in(manifest.inode_sparse_index_path, std::ios::binary);
    std::ifstream dentry_sparse_in(manifest.dentry_sparse_index_path, std::ios::binary);
    if (!inode_sparse_in || !dentry_sparse_in) {
        if (error) {
            *error = "failed to open masstree sparse index files";
        }
        return false;
    }

    while (true) {
        char len_buf[sizeof(uint32_t)] = {};
        if (!ReadExact(&inode_sparse_in, len_buf, sizeof(len_buf))) {
            if (inode_sparse_in.eof()) {
                break;
            }
            if (error) {
                *error = "failed to read masstree inode sparse index header";
            }
            return false;
        }
        const uint32_t payload_len = DecodeLe32(len_buf);
        std::string payload(payload_len, '\0');
        if (payload_len != 0 &&
            !inode_sparse_in.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
            if (error) {
                *error = "failed to read masstree inode sparse index payload";
            }
            return false;
        }
        MasstreeInodeSparseEntry entry;
        if (!DecodeMasstreeInodeSparseEntry(payload, &entry, error) ||
            !runtime->PutInodePageBoundary(manifest.namespace_id,
                                           entry.max_inode_id,
                                           entry.page_offset,
                                           error)) {
            return false;
        }
        ++inode_page_count;
    }

    while (true) {
        char len_buf[sizeof(uint32_t)] = {};
        if (!ReadExact(&dentry_sparse_in, len_buf, sizeof(len_buf))) {
            if (dentry_sparse_in.eof()) {
                break;
            }
            if (error) {
                *error = "failed to read masstree dentry sparse index header";
            }
            return false;
        }
        const uint32_t payload_len = DecodeLe32(len_buf);
        std::string payload(payload_len, '\0');
        if (payload_len != 0 &&
            !dentry_sparse_in.read(&payload[0], static_cast<std::streamsize>(payload_len)).good()) {
            if (error) {
                *error = "failed to read masstree dentry sparse index payload";
            }
            return false;
        }
        MasstreeDentrySparseEntry entry;
        if (!DecodeMasstreeDentrySparseEntry(payload, &entry, error) ||
            !runtime->PutDentryPageBoundary(manifest.namespace_id,
                                            entry.max_parent_inode,
                                            entry.max_name,
                                            entry.page_offset,
                                            error)) {
            return false;
        }
        ++dentry_page_count;
    }

    if (manifest.inode_page_count != 0 && inode_page_count != manifest.inode_page_count) {
        if (error) {
            *error = "masstree generation inode sparse page count mismatch";
        }
        return false;
    }
    if (manifest.dentry_page_count != 0 && dentry_page_count != manifest.dentry_page_count) {
        if (error) {
            *error = "masstree generation dentry sparse page count mismatch";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::ReadInodeAttr(const LoadedGeneration& generation,
                                      uint64_t inode_id,
                                      zb::rpc::InodeAttr* attr,
                                      std::string* error) const {
    UnifiedInodeRecord inode;
    if (!ReadUnifiedInode(generation, inode_id, &inode, error)) {
        return false;
    }
    UnifiedInodeRecordToAttr(inode, attr);
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::ReadUnifiedInode(const LoadedGeneration& generation,
                                         uint64_t inode_id,
                                         UnifiedInodeRecord* inode,
                                         std::string* error) const {
    if (!inode) {
        if (error) {
            *error = "inode output is null";
        }
        return false;
    }
    MasstreeInodeRecord record;
    if (!ReadInodeRecord(generation, inode_id, &record, error)) {
        return false;
    }
    *inode = record.inode;
    if (generation.manifest->optical_layout_version != MasstreeOpticalProfile::kUniform2TbLayoutName &&
        !ConvertLegacyOpticalLocationToCurrent(inode, error)) {
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::ReadInodeRecord(const LoadedGeneration& generation,
                                        uint64_t inode_id,
                                        MasstreeInodeRecord* record,
                                        std::string* error) const {
    return ReadInodeRecordFromSparsePages(generation, inode_id, record, error);
}

bool MasstreeMetaStore::ReadInodeRecordFromSparsePages(const LoadedGeneration& generation,
                                                       uint64_t inode_id,
                                                       MasstreeInodeRecord* record,
                                                       std::string* error) const {
    if (!record || inode_id == 0) {
        if (error) {
            *error = "invalid masstree inode record read args";
        }
        return false;
    }
    MasstreeInodeSparseEntry sparse_entry;
    if (!generation.runtime->FindInodePageBoundary(generation.manifest->namespace_id,
                                                   inode_id,
                                                   &sparse_entry,
                                                   error)) {
        return false;
    }
    MasstreeInodePage page;
    if (!MasstreePageReader::LoadInodePage(generation.manifest->inode_pages_path,
                                           sparse_entry.page_offset,
                                           &page,
                                           error)) {
        return false;
    }
    MasstreeInodePageEntry found;
    if (!MasstreePageReader::FindInodeInPage(page, inode_id, &found, error)) {
        return false;
    }
    if (!MasstreeInodeRecordCodec::Decode(found.payload, record, error)) {
        return false;
    }
    if (record->inode.inode_id != inode_id) {
        if (error) {
            *error = "masstree inode page payload inode mismatch";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::FindDentry(const LoadedGeneration& generation,
                                   uint64_t parent_inode,
                                   const std::string& name,
                                   uint64_t* child_inode,
                                   zb::rpc::InodeType* type,
                                   std::string* error) const {
    return FindDentryInSparsePages(generation, parent_inode, name, child_inode, type, error);
}

bool MasstreeMetaStore::FindDentryInSparsePages(const LoadedGeneration& generation,
                                                uint64_t parent_inode,
                                                const std::string& name,
                                                uint64_t* child_inode,
                                                zb::rpc::InodeType* type,
                                                std::string* error) const {
    MasstreeDentrySparseEntry sparse_entry;
    if (!generation.runtime->FindDentryPageBoundary(generation.manifest->namespace_id,
                                                    parent_inode,
                                                    name,
                                                    &sparse_entry,
                                                    error)) {
        return false;
    }
    MasstreeDentryPage page;
    if (!MasstreePageReader::LoadDentryPage(generation.manifest->dentry_pages_path,
                                            sparse_entry.page_offset,
                                            &page,
                                            error)) {
        return false;
    }
    MasstreeDentryPageEntry found;
    if (!MasstreePageReader::FindDentryInPage(page, parent_inode, name, &found, error)) {
        return false;
    }
    if (child_inode) {
        *child_inode = found.child_inode;
    }
    if (type) {
        *type = found.type;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeMetaStore::StripRoutePrefix(const std::string& route_prefix,
                                         const std::string& path,
                                         std::string* relative_path,
                                         std::string* error) const {
    if (!relative_path) {
        if (error) {
            *error = "relative_path output is null";
        }
        return false;
    }
    std::string normalized_route;
    std::string normalized_path;
    if (!NormalizePath(route_prefix, &normalized_route) || !NormalizePath(path, &normalized_path)) {
        if (error) {
            *error = "invalid masstree route path";
        }
        return false;
    }
    if (normalized_path == normalized_route) {
        *relative_path = "/";
        if (error) {
            error->clear();
        }
        return true;
    }
    const std::string prefix = normalized_route + "/";
    if (normalized_path.rfind(prefix, 0) != 0) {
        if (error) {
            *error = "path does not belong to masstree route";
        }
        return false;
    }
    *relative_path = normalized_path.substr(normalized_route.size());
    if (relative_path->empty()) {
        *relative_path = "/";
    }
    if (error) {
        error->clear();
    }
    return true;
}

std::string MasstreeMetaStore::ResolveManifestPath(const MasstreeNamespaceRoute& route) {
    return route.manifest_path;
}

} // namespace zb::mds
