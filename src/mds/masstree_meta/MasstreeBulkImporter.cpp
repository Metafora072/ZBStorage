#include "MasstreeBulkImporter.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <vector>

#include "MasstreeDecimalUtils.h"
#include "MasstreeIndexRuntime.h"
#include "MasstreeInodeRecordCodec.h"
#include "MasstreeManifest.h"
#include "MasstreeOpticalAllocator.h"
#include "MasstreePageLayout.h"
#include "MasstreePageReader.h"
#include "MasstreeOpticalProfile.h"
#include "../storage/UnifiedInodeRecord.h"
#include "mds.pb.h"

namespace zb::mds {

namespace {

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

constexpr size_t kMasstreeIoBufferBytes = 8U * 1024U * 1024U;

template <typename Stream>
void ConfigureStreamBuffer(Stream* stream, std::vector<char>* buffer) {
    if (!stream || !buffer || buffer->empty()) {
        return;
    }
    stream->rdbuf()->pubsetbuf(buffer->data(), static_cast<std::streamsize>(buffer->size()));
}

uint16_t DecodeLe16(const char* data) {
    return static_cast<uint16_t>(static_cast<unsigned char>(data[0])) |
           (static_cast<uint16_t>(static_cast<unsigned char>(data[1])) << 8U);
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
            *error = "invalid inode record read args";
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
            *error = "failed to read inode record header";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!ReadExact(input, len_buf, sizeof(len_buf))) {
        if (error) {
            *error = "corrupted inode record length";
        }
        return false;
    }
    record->inode_id = DecodeLe64(inode_buf);
    const uint32_t payload_len = DecodeLe32(len_buf);
    if (!ReadString(input, payload_len, &record->payload)) {
        if (error) {
            *error = "corrupted inode record payload";
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
            *error = "invalid dentry record read args";
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
            *error = "failed to read dentry record header";
        }
        return false;
    }
    char name_len_buf[sizeof(uint16_t)] = {};
    if (!ReadExact(input, name_len_buf, sizeof(name_len_buf))) {
        if (error) {
            *error = "corrupted dentry record name length";
        }
        return false;
    }
    const uint16_t name_len = DecodeLe16(name_len_buf);
    if (!ReadString(input, name_len, &record->name)) {
        if (error) {
            *error = "corrupted dentry record name";
        }
        return false;
    }
    char child_buf[sizeof(uint64_t)] = {};
    if (!ReadExact(input, child_buf, sizeof(child_buf))) {
        if (error) {
            *error = "corrupted dentry record child inode";
        }
        return false;
    }
    char raw_type = 0;
    if (!ReadExact(input, &raw_type, sizeof(raw_type))) {
        if (error) {
            *error = "corrupted dentry record type";
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

void AppendLe32(std::string* out, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
        out->push_back(static_cast<char>((value >> (i * 8U)) & 0xFFU));
    }
}

bool WriteExact(std::ofstream* output, const std::string& data) {
    return output && output->write(data.data(), static_cast<std::streamsize>(data.size())).good();
}

bool WriteLengthPrefixedRecord(std::ofstream* output, const std::string& payload) {
    std::string wire_record;
    wire_record.reserve(sizeof(uint32_t) + payload.size());
    AppendLe32(&wire_record, static_cast<uint32_t>(payload.size()));
    wire_record.append(payload);
    return WriteExact(output, wire_record);
}

bool ReadLengthPrefixedPayload(std::ifstream* input, std::string* payload, std::string* error) {
    if (!input || !payload) {
        if (error) {
            *error = "invalid length-prefixed payload read args";
        }
        return false;
    }
    char len_buf[sizeof(uint32_t)] = {};
    if (!ReadExact(input, len_buf, sizeof(len_buf))) {
        if (input->eof()) {
            if (error) {
                error->clear();
            }
            return false;
        }
        if (error) {
            *error = "failed to read length-prefixed payload size";
        }
        return false;
    }
    const uint32_t payload_len = DecodeLe32(len_buf);
    payload->assign(payload_len, '\0');
    if (payload_len != 0 && !ReadExact(input, payload->data(), payload_len)) {
        if (error) {
            *error = "failed to read length-prefixed payload body";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool BuildUnifiedRecordFromRawPayload(const std::string& payload,
                                      uint64_t parent_inode_id,
                                      UnifiedInodeRecord* record,
                                      std::string* error) {
    (void)parent_inode_id;
    if (!record) {
        if (error) {
            *error = "unified inode output is null";
        }
        return false;
    }
    if (DecodeUnifiedInodeRecord(payload, record, nullptr)) {
        if (error) {
            error->clear();
        }
        return true;
    }
    if (error) {
        *error = "failed to decode unified inode record payload";
    }
    return false;
}

void ApplyOpticalLocation(const MasstreeOpticalProfile& profile,
                          uint64_t global_image_id,
                          UnifiedInodeRecord* record) {
    if (!record) {
        return;
    }
    uint32_t node_index = 0;
    uint32_t disk_index = 0;
    uint32_t image_index_in_disk = 0;
    if (!profile.DecodeGlobalImageId(global_image_id, &node_index, &disk_index, &image_index_in_disk)) {
        return;
    }
    record->storage_tier = static_cast<uint8_t>(UnifiedStorageTier::kOptical);
    record->disk_node_id = 0;
    record->disk_id = 0;
    record->optical_node_id = node_index;
    record->optical_disk_id = disk_index;
    record->optical_image_id = image_index_in_disk;
}

void PatchLe64(std::string* data, size_t offset, uint64_t value) {
    if (!data || offset + sizeof(uint64_t) > data->size()) {
        return;
    }
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
        (*data)[offset + i] = static_cast<char>((value >> (i * 8U)) & 0xFFU);
    }
}

bool RebaseEncodedDentryPagePayload(const std::string& payload,
                                    uint64_t inode_id_offset,
                                    uint32_t verify_dentry_samples,
                                    std::string* rebased_payload,
                                    uint64_t* entry_count,
                                    uint64_t* max_parent_inode,
                                    std::string* max_name,
                                    std::vector<DentryRecordView>* verify_samples,
                                    std::string* error) {
    if (!rebased_payload || !entry_count || !max_parent_inode || !max_name) {
        if (error) {
            *error = "invalid masstree dentry page rebase args";
        }
        return false;
    }
    if (payload.size() < sizeof(uint32_t)) {
        if (error) {
            *error = "invalid masstree dentry page payload";
        }
        return false;
    }
    *rebased_payload = payload;
    *entry_count = 0;
    *max_parent_inode = 0;
    max_name->clear();

    size_t cursor = 0;
    const uint32_t count = DecodeLe32(payload.data());
    cursor += sizeof(uint32_t);
    for (uint32_t i = 0; i < count; ++i) {
        if (cursor + sizeof(uint64_t) + sizeof(uint16_t) > payload.size()) {
            if (error) {
                *error = "corrupted masstree dentry page header";
            }
            return false;
        }
        const size_t parent_offset = cursor;
        const uint64_t parent_inode = DecodeLe64(payload.data() + cursor) + inode_id_offset;
        PatchLe64(rebased_payload, parent_offset, parent_inode);
        cursor += sizeof(uint64_t);

        const uint16_t name_len = DecodeLe16(payload.data() + cursor);
        cursor += sizeof(uint16_t);
        if (cursor + name_len + sizeof(uint64_t) + sizeof(uint8_t) > payload.size()) {
            if (error) {
                *error = "corrupted masstree dentry page entry";
            }
            return false;
        }

        const std::string name = payload.substr(cursor, name_len);
        cursor += name_len;

        const size_t child_offset = cursor;
        const uint64_t child_inode = DecodeLe64(payload.data() + cursor) + inode_id_offset;
        PatchLe64(rebased_payload, child_offset, child_inode);
        cursor += sizeof(uint64_t);

        const auto type = static_cast<zb::rpc::InodeType>(static_cast<unsigned char>(payload[cursor]));
        cursor += sizeof(uint8_t);

        ++(*entry_count);
        *max_parent_inode = parent_inode;
        *max_name = name;
        if (verify_samples && verify_samples->size() < verify_dentry_samples) {
            DentryRecordView sample;
            sample.parent_inode = parent_inode;
            sample.name = name;
            sample.child_inode = child_inode;
            sample.type = type;
            verify_samples->push_back(std::move(sample));
        }
    }
    if (cursor != payload.size()) {
        if (error) {
            *error = "unexpected trailing bytes in masstree dentry page";
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool TranslateDentrySparseFromTemplate(std::ifstream* source_sparse_in,
                                       const MasstreeNamespaceManifest& manifest,
                                       MasstreeIndexRuntime* runtime,
                                       uint64_t inode_id_offset,
                                       std::ofstream* sparse_out,
                                       uint64_t* dentry_page_count,
                                       std::string* error) {
    if (!source_sparse_in || !runtime || !sparse_out || !dentry_page_count) {
        if (error) {
            *error = "invalid masstree dentry sparse translate args";
        }
        return false;
    }
    std::string sparse_payload;
    while (ReadLengthPrefixedPayload(source_sparse_in, &sparse_payload, error)) {
        MasstreeDentrySparseEntry entry;
        if (!DecodeMasstreeDentrySparseEntry(sparse_payload, &entry, error)) {
            return false;
        }
        entry.max_parent_inode += inode_id_offset;
        std::string rebased_payload;
        if (!EncodeMasstreeDentrySparseEntry(entry, &rebased_payload, error) ||
            !WriteLengthPrefixedRecord(sparse_out, rebased_payload) ||
            !runtime->PutDentryPageBoundary(manifest.namespace_id,
                                            entry.max_parent_inode,
                                            entry.max_name,
                                            entry.page_offset,
                                            error)) {
            return false;
        }
        ++(*dentry_page_count);
    }
    if (error && !error->empty()) {
        return false;
    }
    return true;
}

bool NormalizeOpticalCursor(const MasstreeOpticalProfile& profile,
                            MasstreeOpticalClusterCursor* cursor,
                            std::string* error) {
    if (!cursor) {
        if (error) {
            *error = "optical cursor is null";
        }
        return false;
    }
    while (cursor->node_index < profile.optical_node_count) {
        if (cursor->disk_index >= profile.disks_per_node) {
            cursor->disk_index = 0;
            cursor->image_index_in_disk = 0;
            cursor->image_used_bytes = 0;
            ++cursor->node_index;
            continue;
        }
        const uint32_t images_per_disk = profile.ImagesPerDisk(cursor->disk_index);
        if (cursor->image_index_in_disk >= images_per_disk) {
            cursor->image_index_in_disk = 0;
            cursor->image_used_bytes = 0;
            ++cursor->disk_index;
            continue;
        }
        if (cursor->image_used_bytes >= profile.image_capacity_bytes) {
            cursor->image_used_bytes = 0;
            ++cursor->image_index_in_disk;
            continue;
        }
        if (error) {
            error->clear();
        }
        return true;
    }
    if (error) {
        *error = "masstree optical cluster capacity is exhausted";
    }
    return false;
}

bool AdvanceOpticalCursorToNextImage(const MasstreeOpticalProfile& profile,
                                     MasstreeOpticalClusterCursor* cursor,
                                     std::string* error) {
    if (!cursor) {
        if (error) {
            *error = "optical cursor is null";
        }
        return false;
    }
    cursor->image_used_bytes = 0;
    ++cursor->image_index_in_disk;
    return NormalizeOpticalCursor(profile, cursor, error);
}

bool ParseOpticalLayoutRun(const std::string& value,
                           MasstreeBulkImporter::OpticalImageRun* run,
                           std::string* error) {
    if (!run) {
        if (error) {
            *error = "optical layout run output is null";
        }
        return false;
    }
    const size_t first = value.find(',');
    const size_t second = first == std::string::npos ? std::string::npos : value.find(',', first + 1U);
    if (first == std::string::npos || second == std::string::npos) {
        if (error) {
            *error = "invalid masstree optical layout run";
        }
        return false;
    }
    try {
        run->global_image_id = static_cast<uint64_t>(std::stoull(value.substr(0, first)));
        run->file_count = static_cast<uint32_t>(std::stoul(value.substr(first + 1U, second - first - 1U)));
        run->end_image_used_bytes = static_cast<uint64_t>(std::stoull(value.substr(second + 1U)));
    } catch (...) {
        if (error) {
            *error = "invalid masstree optical layout run values";
        }
        return false;
    }
    return true;
}

bool LoadOpticalLayout(const std::string& path,
                       std::vector<MasstreeBulkImporter::OpticalImageRun>* runs,
                       std::string* error) {
    if (!runs) {
        if (error) {
            *error = "optical layout output is null";
        }
        return false;
    }
    std::ifstream input(path);
    if (!input) {
        if (error) {
            *error = "failed to open masstree optical layout: " + path;
        }
        return false;
    }
    runs->clear();
    std::string line;
    bool header_checked = false;
    while (std::getline(input, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        if (!header_checked) {
            header_checked = true;
            if (line != "masstree_optical_layout_v1") {
                if (error) {
                    *error = "invalid masstree optical layout header: " + path;
                }
                return false;
            }
            continue;
        }
        const size_t eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        const std::string key = line.substr(0, eq);
        const std::string value = line.substr(eq + 1U);
        if (key.rfind("run[", 0) == 0) {
            MasstreeBulkImporter::OpticalImageRun run;
            if (!ParseOpticalLayoutRun(value, &run, error)) {
                return false;
            }
            runs->push_back(std::move(run));
        }
    }
    if (!header_checked) {
        if (error) {
            *error = "empty masstree optical layout: " + path;
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

bool BuildTargetOpticalRuns(const MasstreeOpticalProfile& profile,
                            const MasstreeOpticalClusterCursor& start_cursor,
                            const std::vector<MasstreeBulkImporter::OpticalImageRun>& template_runs,
                            std::vector<MasstreeBulkImporter::OpticalImageRun>* target_runs,
                            uint64_t* start_global_image_id,
                            uint64_t* end_global_image_id,
                            MasstreeOpticalClusterCursor* end_cursor,
                            std::string* error) {
    if (!target_runs || !start_global_image_id || !end_global_image_id || !end_cursor) {
        if (error) {
            *error = "invalid target optical run args";
        }
        return false;
    }
    target_runs->clear();
    *start_global_image_id = 0;
    *end_global_image_id = 0;
    MasstreeOpticalClusterCursor cursor = start_cursor;
    if (template_runs.empty()) {
        if (!NormalizeOpticalCursor(profile, &cursor, error)) {
            return false;
        }
        *end_cursor = cursor;
        return true;
    }
    if (!NormalizeOpticalCursor(profile, &cursor, error)) {
        return false;
    }
    target_runs->reserve(template_runs.size());
    for (size_t i = 0; i < template_runs.size(); ++i) {
        const auto& template_run = template_runs[i];
        MasstreeBulkImporter::OpticalImageRun target_run;
        target_run.global_image_id = profile.GlobalImageId(cursor);
        target_run.file_count = template_run.file_count;
        target_run.end_image_used_bytes = template_run.end_image_used_bytes;
        if (target_run.end_image_used_bytes == 0 || target_run.end_image_used_bytes > profile.image_capacity_bytes) {
            if (error) {
                *error = "invalid template optical layout image_used_bytes";
            }
            return false;
        }
        if (i == 0) {
            *start_global_image_id = target_run.global_image_id;
        }
        *end_global_image_id = target_run.global_image_id;
        target_runs->push_back(std::move(target_run));
        cursor.image_used_bytes = template_run.end_image_used_bytes;
        if (i + 1U < template_runs.size()) {
            if (!AdvanceOpticalCursorToNextImage(profile, &cursor, error)) {
                return false;
            }
        }
    }
    *end_cursor = cursor;
    if (error) {
        error->clear();
    }
    return true;
}

bool ResolveOpticalProfile(uint32_t layout_version,
                           MasstreeOpticalProfile* profile,
                           std::string* error) {
    if (!profile) {
        if (error) {
            *error = "invalid optical profile output";
        }
        return false;
    }
    if (layout_version == MasstreeOpticalProfile::kLegacyMixedLayoutVersion) {
        *profile = MasstreeOpticalProfile::LegacyMixed();
        if (error) {
            error->clear();
        }
        return true;
    }
    if (layout_version == MasstreeOpticalProfile::kUniform2TbLayoutVersion) {
        *profile = MasstreeOpticalProfile::Fixed();
        if (error) {
            error->clear();
        }
        return true;
    }
    if (error) {
        *error = "unsupported optical_layout_version: " + std::to_string(layout_version);
    }
    return false;
}

std::string OpticalLayoutName(const MasstreeOpticalProfile& profile) {
    return profile.layout_version == MasstreeOpticalProfile::kLegacyMixedLayoutVersion
               ? MasstreeOpticalProfile::kLegacyMixedLayoutName
               : MasstreeOpticalProfile::kUniform2TbLayoutName;
}

bool BuildDentryPages(std::ifstream* dentry_in,
                      const MasstreeNamespaceManifest& manifest,
                      MasstreeIndexRuntime* runtime,
                      uint64_t inode_id_offset,
                      uint32_t verify_dentry_samples,
                      uint64_t* dentry_imported,
                      uint64_t* dentry_page_count,
                      uint64_t* verified_dentry_samples,
                      std::string* error) {
    if (!dentry_in || !runtime || !dentry_imported || !dentry_page_count || !verified_dentry_samples) {
        if (error) {
            *error = "invalid masstree dentry sparse build args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.dentry_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.dentry_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree dentry sparse outputs";
        }
        return false;
    }

    std::vector<MasstreeDentryPageEntry> current_page;
    current_page.reserve(1024);
    size_t current_payload_bytes = sizeof(uint32_t);
    std::vector<DentryRecordView> verify_samples;
    verify_samples.reserve(verify_dentry_samples);
    DentryRecordView dentry_record;

    auto flush_page = [&](std::string* flush_error) -> bool {
        if (current_page.empty()) {
            return true;
        }
        std::string page_payload;
        if (!EncodeMasstreeDentryPage(current_page, &page_payload, flush_error)) {
            return false;
        }
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, page_payload)) {
            if (flush_error) {
                *flush_error = "failed to write masstree dentry page";
            }
            return false;
        }
        MasstreeDentrySparseEntry sparse_entry;
        sparse_entry.page_offset = page_offset;
        sparse_entry.max_parent_inode = current_page.back().parent_inode;
        sparse_entry.max_name = current_page.back().name;
        std::string sparse_payload;
        if (!EncodeMasstreeDentrySparseEntry(sparse_entry, &sparse_payload, flush_error) ||
            !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
            !runtime->PutDentryPageBoundary(manifest.namespace_id,
                                            sparse_entry.max_parent_inode,
                                            sparse_entry.max_name,
                                            sparse_entry.page_offset,
                                            flush_error)) {
            return false;
        }
        ++(*dentry_page_count);
        current_page.clear();
        current_payload_bytes = sizeof(uint32_t);
        return true;
    };

    while (ReadDentryRecord(dentry_in, &dentry_record, error)) {
        MasstreeDentryPageEntry page_entry;
        page_entry.parent_inode = dentry_record.parent_inode + inode_id_offset;
        page_entry.name = dentry_record.name;
        page_entry.child_inode = dentry_record.child_inode + inode_id_offset;
        page_entry.type = dentry_record.type;
        const size_t encoded_size = EncodedMasstreeDentryPageEntrySize(page_entry);
        const size_t total_with_entry = current_payload_bytes + encoded_size;
        if (!current_page.empty() && total_with_entry > manifest.page_size_bytes) {
            if (!flush_page(error)) {
                return false;
            }
        }
        current_page.push_back(std::move(page_entry));
        current_payload_bytes += encoded_size;
        ++(*dentry_imported);
        if (verify_samples.size() < verify_dentry_samples) {
            DentryRecordView sample = dentry_record;
            sample.parent_inode += inode_id_offset;
            sample.child_inode += inode_id_offset;
            verify_samples.push_back(std::move(sample));
        }
    }
    if (error && !error->empty()) {
        return false;
    }
    if (!flush_page(error)) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree dentry sparse outputs";
        }
        return false;
    }

    for (const auto& sample : verify_samples) {
        MasstreeDentrySparseEntry sparse_entry;
        if (!runtime->FindDentryPageBoundary(manifest.namespace_id,
                                             sample.parent_inode,
                                             sample.name,
                                             &sparse_entry,
                                             error)) {
            if (error && error->empty()) {
                *error = "masstree importer dentry sparse verification failed";
            }
            return false;
        }
        MasstreeDentryPage page;
        if (!MasstreePageReader::LoadDentryPage(manifest.dentry_pages_path,
                                                sparse_entry.page_offset,
                                                &page,
                                                error)) {
            return false;
        }
        MasstreeDentryPageEntry found;
        if (!MasstreePageReader::FindDentryInPage(page,
                                                  sample.parent_inode,
                                                  sample.name,
                                                  &found,
                                                  error)) {
            if (error && error->empty()) {
                *error = "masstree importer dentry page verification failed";
            }
            return false;
        }
        if (found.child_inode != sample.child_inode || found.type != sample.type) {
            if (error) {
                *error = "masstree importer dentry page verification mismatch";
            }
            return false;
        }
        ++(*verified_dentry_samples);
    }
    return true;
}

bool BuildDentryPagesFromTemplate(std::ifstream* source_pages_in,
                                  std::ifstream* source_sparse_in,
                                  const MasstreeNamespaceManifest& manifest,
                                  MasstreeIndexRuntime* runtime,
                                  uint64_t inode_id_offset,
                                  uint32_t verify_dentry_samples,
                                  uint64_t* dentry_imported,
                                  uint64_t* dentry_page_count,
                                  uint64_t* verified_dentry_samples,
                                  std::string* error) {
    if (!source_pages_in || !runtime || !dentry_imported || !dentry_page_count || !verified_dentry_samples) {
        if (error) {
            *error = "invalid masstree dentry template import args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.dentry_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.dentry_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree dentry template outputs";
        }
        return false;
    }

    std::vector<DentryRecordView> verify_samples;
    verify_samples.reserve(verify_dentry_samples);
    uint64_t pages_written = 0;
    std::string source_payload;
    while (ReadLengthPrefixedPayload(source_pages_in, &source_payload, error)) {
        std::string target_payload;
        uint64_t page_entry_count = 0;
        uint64_t max_parent_inode = 0;
        std::string max_name;
        if (!RebaseEncodedDentryPagePayload(source_payload,
                                            inode_id_offset,
                                            verify_dentry_samples,
                                            &target_payload,
                                            &page_entry_count,
                                            &max_parent_inode,
                                            &max_name,
                                            &verify_samples,
                                            error)) {
            return false;
        }
        if (page_entry_count == 0) {
            continue;
        }
        *dentry_imported += page_entry_count;
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, target_payload)) {
            if (error) {
                *error = "failed to write masstree dentry template page";
            }
            return false;
        }
        if (!source_sparse_in) {
            MasstreeDentrySparseEntry sparse_entry;
            sparse_entry.page_offset = page_offset;
            sparse_entry.max_parent_inode = max_parent_inode;
            sparse_entry.max_name = max_name;
            std::string sparse_payload;
            if (!EncodeMasstreeDentrySparseEntry(sparse_entry, &sparse_payload, error) ||
                !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
                !runtime->PutDentryPageBoundary(manifest.namespace_id,
                                                sparse_entry.max_parent_inode,
                                                sparse_entry.max_name,
                                                sparse_entry.page_offset,
                                                error)) {
                return false;
            }
            ++(*dentry_page_count);
        }
        ++pages_written;
    }
    if (error && !error->empty()) {
        return false;
    }
    if (source_sparse_in &&
        !TranslateDentrySparseFromTemplate(source_sparse_in,
                                          manifest,
                                          runtime,
                                          inode_id_offset,
                                          &sparse_out,
                                          dentry_page_count,
                                          error)) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree dentry template outputs";
        }
        return false;
    }
    if (*dentry_page_count != pages_written) {
        if (error) {
            *error = "masstree template dentry sparse/page count mismatch";
        }
        return false;
    }

    for (const auto& sample : verify_samples) {
        MasstreeDentrySparseEntry sparse_entry;
        if (!runtime->FindDentryPageBoundary(manifest.namespace_id,
                                             sample.parent_inode,
                                             sample.name,
                                             &sparse_entry,
                                             error)) {
            if (error && error->empty()) {
                *error = "masstree template dentry sparse verification failed";
            }
            return false;
        }
        MasstreeDentryPage page;
        if (!MasstreePageReader::LoadDentryPage(manifest.dentry_pages_path,
                                                sparse_entry.page_offset,
                                                &page,
                                                error)) {
            return false;
        }
        MasstreeDentryPageEntry found;
        if (!MasstreePageReader::FindDentryInPage(page,
                                                  sample.parent_inode,
                                                  sample.name,
                                                  &found,
                                                  error)) {
            if (error && error->empty()) {
                *error = "masstree template dentry page verification failed";
            }
            return false;
        }
        if (found.child_inode != sample.child_inode || found.type != sample.type) {
            if (error) {
                *error = "masstree template dentry page verification mismatch";
            }
            return false;
        }
        ++(*verified_dentry_samples);
    }
    return true;
}

bool BuildInodePages(std::ifstream* inode_in,
                     const MasstreeNamespaceManifest& manifest,
                     MasstreeIndexRuntime* runtime,
                     const MasstreeOpticalProfile& optical_profile,
                     const MasstreeOpticalClusterCursor& start_cursor,
                     uint64_t inode_id_offset,
                     uint32_t verify_inode_samples,
                     uint64_t* inode_imported,
                     uint64_t* inode_page_count,
                     uint64_t* inode_pages_bytes,
                     uint64_t* verified_inode_samples,
                     uint64_t* file_count,
                     uint64_t* start_global_image_id,
                     uint64_t* end_global_image_id,
                     uint64_t* avg_file_size_bytes,
                     std::string* total_file_bytes,
                     MasstreeOpticalClusterCursor* end_cursor,
                     std::vector<MasstreeBulkImporter::OpticalImageRun>* optical_image_runs,
                     std::string* error) {
    if (!inode_in || !runtime || !inode_imported || !inode_page_count || !inode_pages_bytes ||
        !verified_inode_samples || !file_count || !start_global_image_id || !end_global_image_id ||
        !avg_file_size_bytes || !total_file_bytes || !end_cursor) {
        if (error) {
            *error = "invalid masstree inode sparse build args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.inode_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.inode_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree inode sparse outputs";
        }
        return false;
    }

    MasstreeOpticalAllocator allocator(optical_profile, start_cursor);
    MasstreeDecimalAccumulator total_file_bytes_accumulator;
    std::vector<MasstreeInodePageEntry> current_page;
    current_page.reserve(1024);
    size_t current_payload_bytes = sizeof(uint32_t);
    std::vector<uint64_t> verify_samples;
    verify_samples.reserve(verify_inode_samples);
    InodeRecordView inode_record;

    auto flush_page = [&](std::string* flush_error) -> bool {
        if (current_page.empty()) {
            return true;
        }
        std::string page_payload;
        if (!EncodeMasstreeInodePage(current_page, &page_payload, flush_error)) {
            return false;
        }
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, page_payload)) {
            if (flush_error) {
                *flush_error = "failed to write masstree inode page";
            }
            return false;
        }
        MasstreeInodeSparseEntry sparse_entry;
        sparse_entry.page_offset = page_offset;
        sparse_entry.max_inode_id = current_page.back().inode_id;
        std::string sparse_payload;
        if (!EncodeMasstreeInodeSparseEntry(sparse_entry, &sparse_payload, flush_error) ||
            !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
            !runtime->PutInodePageBoundary(manifest.namespace_id,
                                           sparse_entry.max_inode_id,
                                           sparse_entry.page_offset,
                                           flush_error)) {
            return false;
        }
        *inode_pages_bytes += sizeof(uint32_t) + page_payload.size();
        ++(*inode_page_count);
        current_page.clear();
        current_payload_bytes = sizeof(uint32_t);
        return true;
    };

    while (ReadInodeRecord(inode_in, &inode_record, error)) {
        UnifiedInodeRecord inode_record_data;
        if (!BuildUnifiedRecordFromRawPayload(inode_record.payload, 0, &inode_record_data, error)) {
            return false;
        }
        const uint64_t adjusted_inode_id = inode_record.inode_id + inode_id_offset;
        inode_record_data.inode_id = adjusted_inode_id;
        if (inode_record_data.parent_inode_id != 0) {
            inode_record_data.parent_inode_id += inode_id_offset;
        }

        MasstreeInodeRecord blob_record;
        blob_record.inode = inode_record_data;
        if (blob_record.inode.inode_type == static_cast<uint8_t>(zb::rpc::INODE_FILE)) {
            uint64_t global_image_id = 0;
            if (!allocator.Allocate(blob_record.inode.size_bytes, &global_image_id, error)) {
                return false;
            }
            ApplyOpticalLocation(optical_profile, global_image_id, &blob_record.inode);
            if (*file_count == 0) {
                *start_global_image_id = global_image_id;
            }
            *end_global_image_id = global_image_id;
            if (optical_image_runs) {
                if (optical_image_runs->empty() || optical_image_runs->back().global_image_id != global_image_id) {
                    MasstreeBulkImporter::OpticalImageRun run;
                    run.global_image_id = global_image_id;
                    run.file_count = 1;
                    run.end_image_used_bytes = allocator.cursor().image_used_bytes;
                    optical_image_runs->push_back(std::move(run));
                } else {
                    auto& run = optical_image_runs->back();
                    ++run.file_count;
                    run.end_image_used_bytes = allocator.cursor().image_used_bytes;
                }
            }
            ++(*file_count);
            total_file_bytes_accumulator.Add(blob_record.inode.size_bytes);
        }

        std::string encoded_record_payload;
        if (!MasstreeInodeRecordCodec::Encode(blob_record, &encoded_record_payload, error)) {
            return false;
        }

        MasstreeInodePageEntry page_entry;
        page_entry.inode_id = adjusted_inode_id;
        page_entry.payload = std::move(encoded_record_payload);
        const size_t encoded_size = EncodedMasstreeInodePageEntrySize(page_entry);
        const size_t total_with_entry = current_payload_bytes + encoded_size;
        if (!current_page.empty() && total_with_entry > manifest.page_size_bytes) {
            if (!flush_page(error)) {
                return false;
            }
        }
        current_page.push_back(std::move(page_entry));
        current_payload_bytes += encoded_size;
        ++(*inode_imported);
        if (verify_samples.size() < verify_inode_samples) {
            verify_samples.push_back(adjusted_inode_id);
        }
    }
    if (error && !error->empty()) {
        return false;
    }
    if (!flush_page(error)) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree inode sparse outputs";
        }
        return false;
    }

    for (uint64_t inode_id : verify_samples) {
        MasstreeInodeSparseEntry sparse_entry;
        if (!runtime->FindInodePageBoundary(manifest.namespace_id, inode_id, &sparse_entry, error)) {
            if (error && error->empty()) {
                *error = "masstree importer inode sparse verification failed";
            }
            return false;
        }
        MasstreeInodePage page;
        if (!MasstreePageReader::LoadInodePage(manifest.inode_pages_path,
                                               sparse_entry.page_offset,
                                               &page,
                                               error)) {
            return false;
        }
        MasstreeInodePageEntry found;
        if (!MasstreePageReader::FindInodeInPage(page, inode_id, &found, error)) {
            if (error && error->empty()) {
                *error = "masstree importer inode page verification failed";
            }
            return false;
        }
        MasstreeInodeRecord decoded;
        if (!MasstreeInodeRecordCodec::Decode(found.payload, &decoded, error) ||
            decoded.inode.inode_id != inode_id) {
            if (error && error->empty()) {
                *error = "masstree importer inode page verification mismatch";
            }
            return false;
        }
        ++(*verified_inode_samples);
    }

    *total_file_bytes = total_file_bytes_accumulator.ToString();
    *avg_file_size_bytes = *file_count == 0 ? 0 : total_file_bytes_accumulator.DivideBy(*file_count);
    *end_cursor = allocator.cursor();
    return true;
}

bool BuildInodePagesFromTemplate(std::ifstream* source_pages_in,
                                 const MasstreeNamespaceManifest& manifest,
                                 MasstreeIndexRuntime* runtime,
                                 const MasstreeOpticalProfile& optical_profile,
                                 const MasstreeOpticalClusterCursor& start_cursor,
                                 const std::vector<MasstreeBulkImporter::OpticalImageRun>* template_optical_runs,
                                 uint64_t inode_id_offset,
                                 uint32_t verify_inode_samples,
                                 uint64_t* inode_imported,
                                 uint64_t* inode_page_count,
                                 uint64_t* inode_pages_bytes,
                                 uint64_t* verified_inode_samples,
                                 uint64_t* file_count,
                                 uint64_t* start_global_image_id,
                                 uint64_t* end_global_image_id,
                                 uint64_t* avg_file_size_bytes,
                                 std::string* total_file_bytes,
                                 MasstreeOpticalClusterCursor* end_cursor,
                                 std::vector<MasstreeBulkImporter::OpticalImageRun>* optical_image_runs,
                                 std::string* error) {
    if (!source_pages_in || !runtime || !inode_imported || !inode_page_count || !inode_pages_bytes ||
        !verified_inode_samples || !file_count || !start_global_image_id || !end_global_image_id ||
        !avg_file_size_bytes || !total_file_bytes || !end_cursor) {
        if (error) {
            *error = "invalid masstree inode template import args";
        }
        return false;
    }
    std::ofstream pages_out;
    std::ofstream sparse_out;
    std::vector<char> pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&pages_out, &pages_buffer);
    ConfigureStreamBuffer(&sparse_out, &sparse_buffer);
    pages_out.open(manifest.inode_pages_path, std::ios::binary | std::ios::trunc);
    sparse_out.open(manifest.inode_sparse_index_path, std::ios::binary | std::ios::trunc);
    if (!pages_out || !sparse_out) {
        if (error) {
            *error = "failed to open masstree inode template outputs";
        }
        return false;
    }

    std::vector<uint64_t> verify_samples;
    verify_samples.reserve(verify_inode_samples);
    std::vector<MasstreeBulkImporter::OpticalImageRun> rebased_optical_runs;
    size_t optical_run_index = 0;
    uint32_t files_remaining_in_run = 0;
    if (template_optical_runs && !template_optical_runs->empty()) {
        if (!BuildTargetOpticalRuns(optical_profile,
                                    start_cursor,
                                    *template_optical_runs,
                                    &rebased_optical_runs,
                                    start_global_image_id,
                                    end_global_image_id,
                                    end_cursor,
                                    error)) {
            return false;
        }
        if (optical_image_runs) {
            *optical_image_runs = rebased_optical_runs;
        }
        files_remaining_in_run = rebased_optical_runs.front().file_count;
    }
    MasstreeOpticalAllocator allocator(optical_profile, start_cursor);
    std::string source_payload;
    while (ReadLengthPrefixedPayload(source_pages_in, &source_payload, error)) {
        MasstreeInodePage source_page;
        if (!DecodeMasstreeInodePage(source_payload, &source_page, error)) {
            return false;
        }
        if (source_page.entries.empty()) {
            continue;
        }
        std::vector<MasstreeInodePageEntry> target_entries;
        target_entries.reserve(source_page.entries.size());
        for (const auto& source_entry : source_page.entries) {
            bool has_optical_image = false;
            if (!MasstreeInodeRecordCodec::HasOpticalImage(source_entry.payload, &has_optical_image, error)) {
                return false;
            }
            const uint64_t adjusted_inode_id = source_entry.inode_id + inode_id_offset;
            uint64_t global_image_id = 0;
            if (has_optical_image) {
                if (!rebased_optical_runs.empty()) {
                    if (optical_run_index >= rebased_optical_runs.size()) {
                        if (error) {
                            *error = "template optical layout underflow";
                        }
                        return false;
                    }
                    global_image_id = rebased_optical_runs[optical_run_index].global_image_id;
                    if (files_remaining_in_run == 0) {
                        if (error) {
                            *error = "template optical layout file_count mismatch";
                        }
                        return false;
                    }
                    --files_remaining_in_run;
                    if (files_remaining_in_run == 0 && optical_run_index + 1U < rebased_optical_runs.size()) {
                        ++optical_run_index;
                        files_remaining_in_run = rebased_optical_runs[optical_run_index].file_count;
                    }
                } else {
                    MasstreeInodeRecord record;
                    if (!MasstreeInodeRecordCodec::Decode(source_entry.payload, &record, error) ||
                        !allocator.Allocate(record.inode.size_bytes, &global_image_id, error)) {
                        return false;
                    }
                }
                if (rebased_optical_runs.empty()) {
                    if (*file_count == 0) {
                        *start_global_image_id = global_image_id;
                    }
                    *end_global_image_id = global_image_id;
                    if (optical_image_runs) {
                        if (optical_image_runs->empty() || optical_image_runs->back().global_image_id != global_image_id) {
                            MasstreeBulkImporter::OpticalImageRun run;
                            run.global_image_id = global_image_id;
                            run.file_count = 1;
                            run.end_image_used_bytes = allocator.cursor().image_used_bytes;
                            optical_image_runs->push_back(std::move(run));
                        } else {
                            auto& run = optical_image_runs->back();
                            ++run.file_count;
                            run.end_image_used_bytes = allocator.cursor().image_used_bytes;
                        }
                    }
                }
                ++(*file_count);
            }
            std::string encoded_payload;
            if (!MasstreeInodeRecordCodec::RebaseEncoded(source_entry.payload,
                                                         adjusted_inode_id,
                                                         has_optical_image,
                                                         global_image_id,
                                                         &encoded_payload,
                                                         error)) {
                return false;
            }
            MasstreeInodePageEntry target_entry;
            target_entry.inode_id = adjusted_inode_id;
            target_entry.payload = std::move(encoded_payload);
            target_entries.push_back(std::move(target_entry));
            ++(*inode_imported);
            if (verify_samples.size() < verify_inode_samples) {
                verify_samples.push_back(adjusted_inode_id);
            }
        }

        std::string target_payload;
        if (!EncodeMasstreeInodePage(target_entries, &target_payload, error)) {
            return false;
        }
        const uint64_t page_offset = static_cast<uint64_t>(pages_out.tellp());
        if (!WriteLengthPrefixedRecord(&pages_out, target_payload)) {
            if (error) {
                *error = "failed to write masstree inode template page";
            }
            return false;
        }
        MasstreeInodeSparseEntry sparse_entry;
        sparse_entry.page_offset = page_offset;
        sparse_entry.max_inode_id = target_entries.back().inode_id;
        std::string sparse_payload;
        if (!EncodeMasstreeInodeSparseEntry(sparse_entry, &sparse_payload, error) ||
            !WriteLengthPrefixedRecord(&sparse_out, sparse_payload) ||
            !runtime->PutInodePageBoundary(manifest.namespace_id,
                                           sparse_entry.max_inode_id,
                                           sparse_entry.page_offset,
                                           error)) {
            return false;
        }
        *inode_pages_bytes += sizeof(uint32_t) + target_payload.size();
        ++(*inode_page_count);
    }
    if (error && !error->empty()) {
        return false;
    }
    pages_out.flush();
    sparse_out.flush();
    if (!pages_out.good() || !sparse_out.good()) {
        if (error) {
            *error = "failed to flush masstree inode template outputs";
        }
        return false;
    }

    for (uint64_t inode_id : verify_samples) {
        MasstreeInodeSparseEntry sparse_entry;
        if (!runtime->FindInodePageBoundary(manifest.namespace_id, inode_id, &sparse_entry, error)) {
            if (error && error->empty()) {
                *error = "masstree template inode sparse verification failed";
            }
            return false;
        }
        MasstreeInodePage page;
        if (!MasstreePageReader::LoadInodePage(manifest.inode_pages_path,
                                               sparse_entry.page_offset,
                                               &page,
                                               error)) {
            return false;
        }
        MasstreeInodePageEntry found;
        if (!MasstreePageReader::FindInodeInPage(page, inode_id, &found, error)) {
            if (error && error->empty()) {
                *error = "masstree template inode page verification failed";
            }
            return false;
        }
        MasstreeInodeRecord decoded;
        if (!MasstreeInodeRecordCodec::Decode(found.payload, &decoded, error) ||
            decoded.inode.inode_id != inode_id) {
            if (error && error->empty()) {
                *error = "masstree template inode page verification mismatch";
            }
            return false;
        }
        ++(*verified_inode_samples);
    }

    *total_file_bytes = manifest.total_file_bytes;
    *avg_file_size_bytes = manifest.avg_file_size_bytes;
    if (rebased_optical_runs.empty()) {
        *end_cursor = allocator.cursor();
    } else if (optical_run_index + 1U != rebased_optical_runs.size() || files_remaining_in_run != 0) {
        if (error) {
            *error = "template optical layout did not match imported file count";
        }
        return false;
    }
    if (*file_count != manifest.file_count) {
        if (error) {
            *error = "template inode import file_count mismatch";
        }
        return false;
    }
    return true;
}

bool WriteImportSummary(const std::string& summary_path,
                        const MasstreeNamespaceManifest& manifest,
                        const MasstreeBulkImporter::Result& result,
                        std::string* error) {
    std::ofstream out(summary_path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree import summary: " + summary_path;
        }
        return false;
    }
    out << "masstree_import_summary_v2\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "inode_imported=" << result.inode_imported << "\n";
    out << "dentry_imported=" << result.dentry_imported << "\n";
    out << "dentry_page_count=" << result.dentry_page_count << "\n";
    out << "inode_page_count=" << result.inode_page_count << "\n";
    out << "inode_pages_bytes=" << result.inode_pages_bytes << "\n";
    out << "verified_inode_samples=" << result.verified_inode_samples << "\n";
    out << "verified_dentry_samples=" << result.verified_dentry_samples << "\n";
    out << "file_count=" << result.file_count << "\n";
    out << "total_file_bytes=" << result.total_file_bytes << "\n";
    out << "avg_file_size_bytes=" << result.avg_file_size_bytes << "\n";
    out << "start_global_image_id=" << result.start_global_image_id << "\n";
    out << "end_global_image_id=" << result.end_global_image_id << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree import summary: " + summary_path;
        }
        return false;
    }
    return true;
}

bool WriteClusterStats(const std::string& path,
                       const MasstreeNamespaceManifest& manifest,
                       const MasstreeBulkImporter::Result& result,
                       const MasstreeOpticalProfile& profile,
                       std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree cluster stats: " + path;
        }
        return false;
    }
    out << "masstree_cluster_stats_v1\n";
    out << "namespace_id=" << manifest.namespace_id << "\n";
    out << "generation_id=" << manifest.generation_id << "\n";
    out << "optical_node_count=" << profile.optical_node_count << "\n";
    out << "optical_disk_count=" << (static_cast<uint64_t>(profile.optical_node_count) *
                                      static_cast<uint64_t>(profile.disks_per_node)) << "\n";
    out << "total_capacity_bytes=" << profile.TotalCapacityBytesDecimal() << "\n";
    out << "file_count=" << result.file_count << "\n";
    out << "total_file_bytes=" << result.total_file_bytes << "\n";
    out << "avg_file_size_bytes=" << result.avg_file_size_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree cluster stats: " + path;
        }
        return false;
    }
    return true;
}

bool WriteAllocationSummary(const std::string& path,
                            const MasstreeBulkImporter::Result& result,
                            std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree allocation summary: " + path;
        }
        return false;
    }
    out << "masstree_allocation_summary_v1\n";
    out << "file_count=" << result.file_count << "\n";
    out << "start_global_image_id=" << result.start_global_image_id << "\n";
    out << "end_global_image_id=" << result.end_global_image_id << "\n";
    out << "start_cursor=" << result.start_cursor.node_index << "," << result.start_cursor.disk_index << ","
        << result.start_cursor.image_index_in_disk << "," << result.start_cursor.image_used_bytes << "\n";
    out << "end_cursor=" << result.end_cursor.node_index << "," << result.end_cursor.disk_index << ","
        << result.end_cursor.image_index_in_disk << "," << result.end_cursor.image_used_bytes << "\n";
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree allocation summary: " + path;
        }
        return false;
    }
    return true;
}

bool WriteOpticalLayout(const std::string& path,
                        const MasstreeBulkImporter::Result& result,
                        std::string* error) {
    std::ofstream out(path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create masstree optical layout: " + path;
        }
        return false;
    }
    out << "masstree_optical_layout_v1\n";
    out << "file_count=" << result.file_count << "\n";
    out << "run_count=" << result.optical_image_runs.size() << "\n";
    for (size_t i = 0; i < result.optical_image_runs.size(); ++i) {
        const auto& run = result.optical_image_runs[i];
        out << "run[" << i << "]=" << run.global_image_id << "," << run.file_count << ","
            << run.end_image_used_bytes << "\n";
    }
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write masstree optical layout: " + path;
        }
        return false;
    }
    return true;
}

} // namespace

bool MasstreeBulkImporter::Import(const Request& request,
                                  MasstreeIndexRuntime* runtime,
                                  Result* result,
                                  std::string* error) const {
    if (result) {
        *result = Result();
    }
    if (request.manifest_path.empty()) {
        if (error) {
            *error = "masstree bulk importer manifest_path is empty";
        }
        return false;
    }

    MasstreeNamespaceManifest manifest;
    if (!MasstreeNamespaceManifest::LoadFromFile(request.manifest_path, &manifest, error)) {
        return false;
    }

    MasstreeOpticalProfile optical_profile;
    if (!ResolveOpticalProfile(request.optical_layout_version, &optical_profile, error)) {
        return false;
    }
    MasstreeIndexRuntime local_runtime;
    MasstreeIndexRuntime* active_runtime = runtime ? runtime : &local_runtime;
    std::string init_error;
    if (!active_runtime->Init(&init_error)) {
        if (error) {
            *error = init_error;
        }
        return false;
    }

    std::ifstream inode_in;
    std::ifstream dentry_in;
    std::ifstream inode_pages_in;
    std::ifstream dentry_pages_in;
    std::ifstream dentry_sparse_in;
    std::vector<char> inode_in_buffer(kMasstreeIoBufferBytes);
    std::vector<char> dentry_in_buffer(kMasstreeIoBufferBytes);
    std::vector<char> inode_pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> dentry_pages_buffer(kMasstreeIoBufferBytes);
    std::vector<char> dentry_sparse_buffer(kMasstreeIoBufferBytes);
    ConfigureStreamBuffer(&inode_in, &inode_in_buffer);
    ConfigureStreamBuffer(&dentry_in, &dentry_in_buffer);
    ConfigureStreamBuffer(&inode_pages_in, &inode_pages_buffer);
    ConfigureStreamBuffer(&dentry_pages_in, &dentry_pages_buffer);
    ConfigureStreamBuffer(&dentry_sparse_in, &dentry_sparse_buffer);
    const std::string inode_records_path =
        request.source_inode_records_path.empty() ? manifest.inode_records_path : request.source_inode_records_path;
    const std::string dentry_records_path =
        request.source_dentry_records_path.empty() ? manifest.dentry_records_path : request.source_dentry_records_path;
    const std::string inode_pages_path =
        request.source_inode_pages_path.empty() ? manifest.inode_pages_path : request.source_inode_pages_path;
    const std::string dentry_pages_path =
        request.source_dentry_pages_path.empty() ? manifest.dentry_pages_path : request.source_dentry_pages_path;
    const std::string dentry_sparse_path =
        request.source_dentry_sparse_path.empty() ? manifest.dentry_sparse_index_path : request.source_dentry_sparse_path;
    const std::string optical_layout_path =
        request.source_optical_layout_path.empty() ? manifest.optical_layout_path : request.source_optical_layout_path;
    const bool use_template_pages =
        !request.source_inode_pages_path.empty() && !request.source_dentry_pages_path.empty();
    std::vector<MasstreeBulkImporter::OpticalImageRun> template_optical_runs;
    std::ifstream* template_dentry_sparse_in = nullptr;
    if (use_template_pages) {
        inode_pages_in.open(inode_pages_path, std::ios::binary);
        dentry_pages_in.open(dentry_pages_path, std::ios::binary);
        if (!inode_pages_in || !dentry_pages_in) {
            if (error) {
                *error = "failed to open masstree template page inputs";
            }
            return false;
        }
        if (!dentry_sparse_path.empty()) {
            dentry_sparse_in.open(dentry_sparse_path, std::ios::binary);
            if (!dentry_sparse_in) {
                if (error) {
                    *error = "failed to open masstree template dentry sparse input";
                }
                return false;
            }
            template_dentry_sparse_in = &dentry_sparse_in;
        }
        if (!optical_layout_path.empty()) {
            if (!LoadOpticalLayout(optical_layout_path, &template_optical_runs, error)) {
                return false;
            }
        }
    } else {
        inode_in.open(inode_records_path, std::ios::binary);
        dentry_in.open(dentry_records_path, std::ios::binary);
        if (!inode_in || !dentry_in) {
            if (error) {
                *error = "failed to open masstree importer inputs/outputs";
            }
            return false;
        }
    }

    Result local_result;
    local_result.start_cursor = request.start_cursor;
    local_result.end_cursor = request.start_cursor;
    local_result.total_file_bytes = "0";
    if (manifest.page_size_bytes == 0) {
        manifest.page_size_bytes = kMasstreeDefaultPageSizeBytes;
    }
    const std::filesystem::path staging_dir_path = std::filesystem::path(request.manifest_path).parent_path();
    if (manifest.inode_pages_path.empty()) {
        manifest.inode_pages_path = (staging_dir_path / "inode_pages.seg").string();
    }
    if (manifest.inode_sparse_index_path.empty()) {
        manifest.inode_sparse_index_path = (staging_dir_path / "inode_sparse.idx").string();
    }
    if (manifest.dentry_pages_path.empty()) {
        manifest.dentry_pages_path = (staging_dir_path / "dentry_pages.seg").string();
    }
    if (manifest.dentry_sparse_index_path.empty()) {
        manifest.dentry_sparse_index_path = (staging_dir_path / "dentry_sparse.idx").string();
    }
    if (use_template_pages) {
        if (!BuildInodePagesFromTemplate(&inode_pages_in,
                                         manifest,
                                         active_runtime,
                                         optical_profile,
                                         request.start_cursor,
                                         template_optical_runs.empty() ? nullptr : &template_optical_runs,
                                         request.inode_id_offset,
                                         request.verify_inode_samples,
                                         &local_result.inode_imported,
                                         &local_result.inode_page_count,
                                         &local_result.inode_pages_bytes,
                                         &local_result.verified_inode_samples,
                                         &local_result.file_count,
                                         &local_result.start_global_image_id,
                                         &local_result.end_global_image_id,
                                         &local_result.avg_file_size_bytes,
                                         &local_result.total_file_bytes,
                                         &local_result.end_cursor,
                                         &local_result.optical_image_runs,
                                         error)) {
            return false;
        }
        if (!BuildDentryPagesFromTemplate(&dentry_pages_in,
                                          template_dentry_sparse_in,
                                          manifest,
                                          active_runtime,
                                          request.inode_id_offset,
                                          request.verify_dentry_samples,
                                          &local_result.dentry_imported,
                                          &local_result.dentry_page_count,
                                          &local_result.verified_dentry_samples,
                                          error)) {
            return false;
        }
    } else {
        if (!BuildInodePages(&inode_in,
                             manifest,
                             active_runtime,
                             optical_profile,
                             request.start_cursor,
                             request.inode_id_offset,
                             request.verify_inode_samples,
                             &local_result.inode_imported,
                             &local_result.inode_page_count,
                             &local_result.inode_pages_bytes,
                             &local_result.verified_inode_samples,
                             &local_result.file_count,
                             &local_result.start_global_image_id,
                             &local_result.end_global_image_id,
                             &local_result.avg_file_size_bytes,
                             &local_result.total_file_bytes,
                             &local_result.end_cursor,
                             &local_result.optical_image_runs,
                             error)) {
            return false;
        }

        if (!BuildDentryPages(&dentry_in,
                              manifest,
                              active_runtime,
                              request.inode_id_offset,
                              request.verify_dentry_samples,
                              &local_result.dentry_imported,
                              &local_result.dentry_page_count,
                              &local_result.verified_dentry_samples,
                              error)) {
            return false;
        }
    }

    if (local_result.inode_imported != manifest.inode_count) {
        if (error) {
            *error = "masstree importer inode count mismatch";
        }
        return false;
    }
    if (local_result.dentry_imported != manifest.dentry_count) {
        if (error) {
            *error = "masstree importer dentry count mismatch";
        }
        return false;
    }
    if (local_result.file_count != manifest.file_count) {
        if (error) {
            *error = "masstree importer file count mismatch";
        }
        return false;
    }

    const std::string summary_path = (staging_dir_path / "import_summary.txt").string();
    const std::string cluster_stats_path = (staging_dir_path / "cluster_stats.txt").string();
    const std::string allocation_summary_path = (staging_dir_path / "allocation_summary.txt").string();
    const std::string output_optical_layout_path = (staging_dir_path / "optical_layout.txt").string();

    manifest.cluster_stats_path = cluster_stats_path;
    manifest.allocation_summary_path = allocation_summary_path;
    manifest.optical_layout_path = output_optical_layout_path;
    manifest.min_file_size_bytes = optical_profile.min_file_size_bytes;
    manifest.max_file_size_bytes = optical_profile.max_file_size_bytes;
    manifest.avg_file_size_bytes = local_result.avg_file_size_bytes;
    manifest.total_file_bytes = local_result.total_file_bytes;
    manifest.start_global_image_id = local_result.start_global_image_id;
    manifest.end_global_image_id = local_result.end_global_image_id;
    manifest.start_cursor_node_index = local_result.start_cursor.node_index;
    manifest.start_cursor_disk_index = local_result.start_cursor.disk_index;
    manifest.start_cursor_image_index = local_result.start_cursor.image_index_in_disk;
    manifest.start_cursor_image_used_bytes = local_result.start_cursor.image_used_bytes;
    manifest.end_cursor_node_index = local_result.end_cursor.node_index;
    manifest.end_cursor_disk_index = local_result.end_cursor.disk_index;
    manifest.end_cursor_image_index = local_result.end_cursor.image_index_in_disk;
    manifest.end_cursor_image_used_bytes = local_result.end_cursor.image_used_bytes;
    manifest.optical_layout_version = OpticalLayoutName(optical_profile);
    manifest.inode_page_count = local_result.inode_page_count;
    manifest.dentry_page_count = local_result.dentry_page_count;

    if (!manifest.SaveToFile(request.manifest_path, error) ||
        !WriteImportSummary(summary_path, manifest, local_result, error) ||
        !WriteClusterStats(cluster_stats_path, manifest, local_result, optical_profile, error) ||
        !WriteAllocationSummary(allocation_summary_path, local_result, error) ||
        !WriteOpticalLayout(output_optical_layout_path, local_result, error)) {
        return false;
    }

    if (result) {
        *result = local_result;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
