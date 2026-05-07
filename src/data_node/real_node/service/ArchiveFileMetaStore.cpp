#include "ArchiveFileMetaStore.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <sstream>
#include <utility>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace zb::real_node {

namespace {

constexpr char kArchiveWalMagic[] = {'F', 'M', 'W', '1'};
constexpr uint32_t kMaxWalPayloadBytes = 64U * 1024U * 1024U;

} // namespace

bool ArchiveFileMetaStore::Init(const std::string& dir_path,
                                size_t max_files,
                                uint32_t snapshot_interval_ops,
                                std::string* error,
                                bool wal_fsync) {
    if (dir_path.empty()) {
        if (error) {
            *error = "archive file meta dir is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    dir_path_ = dir_path;
    wal_path_ = (fs::path(dir_path_) / "archive_file_meta.wal").string();
    snapshot_path_ = (fs::path(dir_path_) / "archive_file_meta.snapshot").string();
    max_files_ = std::max<size_t>(1, max_files);
    snapshot_interval_ops_ = std::max<uint32_t>(1, snapshot_interval_ops);
    wal_fsync_enabled_ = wal_fsync;
    updates_since_snapshot_ = 0;
    metas_.clear();
    wal_out_.close();

    std::error_code ec;
    fs::create_directories(dir_path_, ec);
    if (ec) {
        if (error) {
            *error = "failed to create archive file meta dir: " + dir_path_;
        }
        return false;
    }

    if (!LoadSnapshotLocked(error)) {
        return false;
    }
    if (!ReplayWalLocked(error)) {
        return false;
    }
    EvictIfNeededLocked();

    wal_out_.open(wal_path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!wal_out_.is_open()) {
        if (error) {
            *error = "failed to open archive file meta wal: " + wal_path_;
        }
        return false;
    }
    if (!EnsureWalMagicLocked(error)) {
        return false;
    }
    inited_ = true;
    return true;
}

void ArchiveFileMetaStore::SetMaxFiles(size_t max_files) {
    std::lock_guard<std::mutex> lock(mu_);
    max_files_ = std::max<size_t>(1, max_files);
    EvictIfNeededLocked();
}

void ArchiveFileMetaStore::TrackFileAccess(uint64_t inode_id,
                                           const std::string& disk_id,
                                           uint64_t file_size,
                                           uint32_t object_count,
                                           bool is_write,
                                           uint64_t now_ms,
                                           uint64_t version) {
    if (inode_id == 0 || !inited_) {
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildFileKey(inode_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        EvictIfNeededLocked();
        ArchiveFileMeta meta;
        meta.inode_id = inode_id;
        it = metas_.emplace(key, std::move(meta)).first;
    }

    ArchiveFileMeta& meta = it->second;
    if (!disk_id.empty()) {
        meta.disk_id = disk_id;
    }
    if (file_size > 0) {
        meta.file_size = std::max<uint64_t>(meta.file_size, file_size);
    }
    if (object_count > 0) {
        meta.object_count = std::max<uint32_t>(meta.object_count, object_count);
    }
    meta.last_access_ts_ms = now_ms;
    meta.version = std::max<uint64_t>(meta.version, version);
    if (is_write && meta.archive_state == FileArchiveState::kArchived) {
        meta.archive_state = FileArchiveState::kPending;
    }

    const ArchiveFileMeta persisted = meta;
    EvictIfNeededLocked();
    if (AppendWalRecordLocked("U\t" + SerializeMetaLine(persisted))) {
        ++updates_since_snapshot_;
        (void)MaybeSnapshotLocked();
    }
}

bool ArchiveFileMetaStore::UpsertFile(uint64_t inode_id,
                                      const std::string& disk_id,
                                      uint64_t file_size,
                                      uint32_t object_count,
                                      uint64_t version,
                                      bool reset_to_pending) {
    if (inode_id == 0 || !inited_) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildFileKey(inode_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        EvictIfNeededLocked();
        ArchiveFileMeta meta;
        meta.inode_id = inode_id;
        meta.archive_state = reset_to_pending ? FileArchiveState::kPending : FileArchiveState::kPending;
        it = metas_.emplace(key, std::move(meta)).first;
    }

    ArchiveFileMeta& meta = it->second;
    if (!disk_id.empty()) {
        meta.disk_id = disk_id;
    }
    meta.file_size = file_size;
    meta.object_count = object_count;
    meta.version = std::max<uint64_t>(meta.version, version);
    if (reset_to_pending) {
        meta.archive_state = FileArchiveState::kPending;
    }

    const ArchiveFileMeta persisted = meta;
    if (!AppendWalRecordLocked("U\t" + SerializeMetaLine(persisted))) {
        return false;
    }
    ++updates_since_snapshot_;
    (void)MaybeSnapshotLocked();
    return true;
}

bool ArchiveFileMetaStore::UpdateFileArchiveState(uint64_t inode_id,
                                                  const std::string& disk_id,
                                                  FileArchiveState archive_state,
                                                  uint64_t version) {
    if (inode_id == 0 || !inited_) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildFileKey(inode_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        ArchiveFileMeta meta;
        meta.inode_id = inode_id;
        meta.disk_id = disk_id;
        meta.archive_state = archive_state;
        meta.version = version > 0 ? version : 1;
        it = metas_.emplace(key, std::move(meta)).first;
    } else {
        if (!disk_id.empty()) {
            it->second.disk_id = disk_id;
        }
        it->second.archive_state = archive_state;
        if (version > 0) {
            it->second.version = std::max<uint64_t>(it->second.version, version);
        }
    }

    const ArchiveFileMeta persisted = it->second;
    if (!AppendWalRecordLocked("U\t" + SerializeMetaLine(persisted))) {
        return false;
    }
    ++updates_since_snapshot_;
    (void)MaybeSnapshotLocked();
    return true;
}

void ArchiveFileMetaStore::RemoveFile(uint64_t inode_id) {
    if (inode_id == 0 || !inited_) {
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildFileKey(inode_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        return;
    }
    metas_.erase(it);
    if (AppendWalRecordLocked("D\t" + std::to_string(inode_id))) {
        ++updates_since_snapshot_;
        (void)MaybeSnapshotLocked();
    }
}

std::vector<ArchiveFileMeta> ArchiveFileMetaStore::CollectCandidates(uint32_t max_candidates,
                                                                     uint64_t min_age_ms,
                                                                     uint64_t now_ms) const {
    if (!inited_ || max_candidates == 0) {
        return {};
    }

    std::vector<ArchiveFileMeta> out;
    std::lock_guard<std::mutex> lock(mu_);
    out.reserve(metas_.size());
    for (const auto& item : metas_) {
        const ArchiveFileMeta& meta = item.second;
        if (meta.archive_state != FileArchiveState::kPending) {
            continue;
        }
        if (meta.last_access_ts_ms == 0 || now_ms <= meta.last_access_ts_ms) {
            continue;
        }
        if (now_ms - meta.last_access_ts_ms < min_age_ms) {
            continue;
        }
        out.push_back(meta);
    }

    std::sort(out.begin(), out.end(), [](const ArchiveFileMeta& lhs, const ArchiveFileMeta& rhs) {
        if (lhs.last_access_ts_ms != rhs.last_access_ts_ms) {
            return lhs.last_access_ts_ms < rhs.last_access_ts_ms;
        }
        return lhs.inode_id < rhs.inode_id;
    });
    if (out.size() > static_cast<size_t>(max_candidates)) {
        out.resize(static_cast<size_t>(max_candidates));
    }
    return out;
}

std::vector<ArchiveFileMeta> ArchiveFileMetaStore::SnapshotMetas() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<ArchiveFileMeta> out;
    out.reserve(metas_.size());
    for (const auto& item : metas_) {
        out.push_back(item.second);
    }
    return out;
}

bool ArchiveFileMetaStore::ClearAll(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_) {
        if (error) {
            *error = "archive file meta store is not initialized";
        }
        return false;
    }
    metas_.clear();
    updates_since_snapshot_ = snapshot_interval_ops_;
    return WriteSnapshotLocked(error);
}

bool ArchiveFileMetaStore::FlushSnapshot(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    return WriteSnapshotLocked(error);
}

std::string ArchiveFileMetaStore::BuildFileKey(uint64_t inode_id) {
    return std::to_string(inode_id);
}

bool ArchiveFileMetaStore::ParseMetaLine(const std::string& line, ArchiveFileMeta* meta) {
    if (!meta) {
        return false;
    }
    const std::vector<std::string> parts = SplitTabs(line);
    if (parts.size() != 7) {
        return false;
    }
    try {
        meta->inode_id = static_cast<uint64_t>(std::stoull(parts[0]));
        meta->disk_id = parts[1];
        meta->file_size = static_cast<uint64_t>(std::stoull(parts[2]));
        meta->object_count = static_cast<uint32_t>(std::stoul(parts[3]));
        meta->last_access_ts_ms = static_cast<uint64_t>(std::stoull(parts[4]));
        if (!ParseState(parts[5], &meta->archive_state)) {
            return false;
        }
        meta->version = static_cast<uint64_t>(std::stoull(parts[6]));
    } catch (...) {
        return false;
    }
    return meta->inode_id != 0;
}

std::string ArchiveFileMetaStore::SerializeMetaLine(const ArchiveFileMeta& meta) {
    std::ostringstream oss;
    oss << meta.inode_id << '\t'
        << meta.disk_id << '\t'
        << meta.file_size << '\t'
        << meta.object_count << '\t'
        << meta.last_access_ts_ms << '\t'
        << StateToString(meta.archive_state) << '\t'
        << meta.version;
    return oss.str();
}

std::vector<std::string> ArchiveFileMetaStore::SplitTabs(const std::string& line) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(line);
    while (std::getline(stream, token, '\t')) {
        parts.push_back(token);
    }
    return parts;
}

uint32_t ArchiveFileMetaStore::Crc32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char ch : data) {
        crc ^= static_cast<uint32_t>(ch);
        for (int i = 0; i < 8; ++i) {
            const uint32_t mask = static_cast<uint32_t>(-(crc & 1u));
            crc = (crc >> 1u) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

bool ArchiveFileMetaStore::WriteUint32LE(std::ostream* out, uint32_t value) {
    if (!out) {
        return false;
    }
    const char bytes[4] = {
        static_cast<char>(value & 0xFF),
        static_cast<char>((value >> 8) & 0xFF),
        static_cast<char>((value >> 16) & 0xFF),
        static_cast<char>((value >> 24) & 0xFF),
    };
    out->write(bytes, 4);
    return out->good();
}

bool ArchiveFileMetaStore::FsyncPath(const std::string& path) {
    if (path.empty()) {
        return false;
    }
#ifdef _WIN32
    const int fd = _open(path.c_str(), _O_RDWR | _O_BINARY);
    if (fd < 0) {
        return false;
    }
    const int rc = _commit(fd);
    _close(fd);
    return rc == 0;
#else
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return false;
    }
    int rc = ::fdatasync(fd);
    if (rc != 0) {
        rc = ::fsync(fd);
    }
    ::close(fd);
    return rc == 0;
#endif
}

const char* ArchiveFileMetaStore::StateToString(FileArchiveState state) {
    switch (state) {
        case FileArchiveState::kArchiving:
            return "archiving";
        case FileArchiveState::kArchived:
            return "archived";
        case FileArchiveState::kPending:
        default:
            return "pending";
    }
}

bool ArchiveFileMetaStore::ParseState(const std::string& text, FileArchiveState* state) {
    if (!state) {
        return false;
    }
    if (text == "pending") {
        *state = FileArchiveState::kPending;
        return true;
    }
    if (text == "archiving") {
        *state = FileArchiveState::kArchiving;
        return true;
    }
    if (text == "archived") {
        *state = FileArchiveState::kArchived;
        return true;
    }
    return false;
}

bool ArchiveFileMetaStore::LoadSnapshotLocked(std::string* error) {
    std::ifstream in(snapshot_path_);
    if (!in.is_open()) {
        return true;
    }
    std::string line;
    size_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        if (line.empty()) {
            continue;
        }
        ArchiveFileMeta meta;
        if (!ParseMetaLine(line, &meta)) {
            if (error) {
                *error = "invalid archive file snapshot at line " + std::to_string(line_no);
            }
            return false;
        }
        metas_[BuildFileKey(meta.inode_id)] = std::move(meta);
    }
    return true;
}

bool ArchiveFileMetaStore::ReplayWalLocked(std::string* error) {
    std::ifstream in(wal_path_, std::ios::in | std::ios::binary);
    if (!in.is_open()) {
        return true;
    }

    char prefix[4] = {0, 0, 0, 0};
    in.read(prefix, 4);
    const std::streamsize prefix_read = in.gcount();
    if (prefix_read == 0 && in.eof()) {
        return true;
    }

    if (prefix_read != 4 ||
        prefix[0] != kArchiveWalMagic[0] ||
        prefix[1] != kArchiveWalMagic[1] ||
        prefix[2] != kArchiveWalMagic[2] ||
        prefix[3] != kArchiveWalMagic[3]) {
        in.close();
        std::error_code ec;
        fs::resize_file(wal_path_, 0, ec);
        if (ec) {
            if (error) {
                *error = "failed to truncate incompatible file wal: " + wal_path_;
            }
            return false;
        }
        return true;
    }

    in.clear();
    in.seekg(static_cast<std::streamoff>(sizeof(kArchiveWalMagic)), std::ios::beg);

    uint64_t last_good_offset = sizeof(kArchiveWalMagic);
    bool need_truncate = false;
    while (true) {
        const std::streampos record_begin = in.tellg();
        if (record_begin < 0) {
            break;
        }

        char len_buf[4] = {0, 0, 0, 0};
        in.read(len_buf, 4);
        const std::streamsize len_read = in.gcount();
        if (len_read == 0 && in.eof()) {
            break;
        }
        if (len_read != 4) {
            need_truncate = true;
            break;
        }
        const uint32_t payload_len =
            static_cast<uint32_t>(static_cast<unsigned char>(len_buf[0])) |
            (static_cast<uint32_t>(static_cast<unsigned char>(len_buf[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(len_buf[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(len_buf[3])) << 24);
        if (payload_len > kMaxWalPayloadBytes) {
            need_truncate = true;
            break;
        }

        std::string payload(payload_len, '\0');
        if (payload_len > 0) {
            in.read(payload.data(), static_cast<std::streamsize>(payload_len));
            if (in.gcount() != static_cast<std::streamsize>(payload_len)) {
                need_truncate = true;
                break;
            }
        }

        char crc_buf[4] = {0, 0, 0, 0};
        in.read(crc_buf, 4);
        if (in.gcount() != 4) {
            need_truncate = true;
            break;
        }
        const uint32_t crc_on_disk =
            static_cast<uint32_t>(static_cast<unsigned char>(crc_buf[0])) |
            (static_cast<uint32_t>(static_cast<unsigned char>(crc_buf[1])) << 8) |
            (static_cast<uint32_t>(static_cast<unsigned char>(crc_buf[2])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(crc_buf[3])) << 24);
        if (crc_on_disk != Crc32(payload)) {
            need_truncate = true;
            break;
        }

        if (payload.size() < 2 || payload[1] != '\t') {
            need_truncate = true;
            break;
        }
        const char op = payload[0];
        const std::string body = payload.substr(2);
        if (op == 'U') {
            ArchiveFileMeta meta;
            if (!ParseMetaLine(body, &meta)) {
                need_truncate = true;
                break;
            }
            metas_[BuildFileKey(meta.inode_id)] = std::move(meta);
        } else if (op == 'D') {
            uint64_t inode_id = 0;
            try {
                inode_id = static_cast<uint64_t>(std::stoull(body));
            } catch (...) {
                need_truncate = true;
                break;
            }
            if (inode_id == 0) {
                need_truncate = true;
                break;
            }
            metas_.erase(BuildFileKey(inode_id));
        } else {
            need_truncate = true;
            break;
        }

        const std::streampos record_end = in.tellg();
        if (record_end > 0) {
            last_good_offset = static_cast<uint64_t>(record_end);
        } else {
            last_good_offset = static_cast<uint64_t>(record_begin) + 8 + payload_len;
        }
    }

    if (need_truncate) {
        std::error_code ec;
        fs::resize_file(wal_path_, last_good_offset, ec);
        if (ec) {
            if (error) {
                *error = "failed to truncate corrupted file wal tail: " + wal_path_;
            }
            return false;
        }
    }
    return true;
}

bool ArchiveFileMetaStore::EnsureWalMagicLocked(std::string* error) {
    if (!wal_out_.is_open()) {
        if (error) {
            *error = "archive file wal stream is not open";
        }
        return false;
    }

    std::error_code ec;
    const uint64_t file_size = fs::exists(wal_path_, ec) ? fs::file_size(wal_path_, ec) : 0;
    if (ec) {
        if (error) {
            *error = "failed to stat archive file wal: " + wal_path_;
        }
        return false;
    }
    if (file_size == 0) {
        wal_out_.write(kArchiveWalMagic, static_cast<std::streamsize>(sizeof(kArchiveWalMagic)));
        wal_out_.flush();
        if (!wal_out_.good()) {
            if (error) {
                *error = "failed to write archive file wal magic: " + wal_path_;
            }
            return false;
        }
        if (wal_fsync_enabled_ && !FsyncPath(wal_path_)) {
            if (error) {
                *error = "failed to fsync archive file wal magic: " + wal_path_;
            }
            return false;
        }
        return true;
    }
    if (file_size < sizeof(kArchiveWalMagic)) {
        if (error) {
            *error = "archive file wal is too small for magic header: " + wal_path_;
        }
        return false;
    }
    return true;
}

bool ArchiveFileMetaStore::AppendWalRecordLocked(const std::string& payload) {
    if (!wal_out_.is_open()) {
        return false;
    }
    if (!EnsureWalMagicLocked(nullptr)) {
        return false;
    }
    const uint32_t len = static_cast<uint32_t>(payload.size());
    if (!WriteUint32LE(&wal_out_, len)) {
        return false;
    }
    if (!payload.empty()) {
        wal_out_.write(payload.data(), static_cast<std::streamsize>(payload.size()));
    }
    if (!WriteUint32LE(&wal_out_, Crc32(payload))) {
        return false;
    }
    wal_out_.flush();
    if (!wal_out_.good()) {
        return false;
    }
    if (wal_fsync_enabled_ && !FsyncPath(wal_path_)) {
        return false;
    }
    return true;
}

bool ArchiveFileMetaStore::MaybeSnapshotLocked() {
    if (updates_since_snapshot_ < snapshot_interval_ops_) {
        return true;
    }
    std::string error;
    return WriteSnapshotLocked(&error);
}

bool ArchiveFileMetaStore::WriteSnapshotLocked(std::string* error) {
    if (snapshot_path_.empty() || wal_path_.empty() || dir_path_.empty()) {
        if (error) {
            *error = "archive file meta store not initialized";
        }
        return false;
    }
    const std::string tmp_path = snapshot_path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
        if (!out.is_open()) {
            if (error) {
                *error = "failed to open archive file snapshot tmp: " + tmp_path;
            }
            return false;
        }
        for (const auto& item : metas_) {
            out << SerializeMetaLine(item.second) << "\n";
        }
        out.flush();
        if (!out.good()) {
            if (error) {
                *error = "failed to write archive file snapshot tmp: " + tmp_path;
            }
            return false;
        }
    }

    std::error_code ec;
    fs::remove(snapshot_path_, ec);
    ec.clear();
    fs::rename(tmp_path, snapshot_path_, ec);
    if (ec) {
        if (error) {
            *error = "failed to rotate archive file snapshot: " + snapshot_path_;
        }
        return false;
    }
    if (wal_fsync_enabled_ && !FsyncPath(snapshot_path_)) {
        if (error) {
            *error = "failed to fsync archive file snapshot: " + snapshot_path_;
        }
        return false;
    }

    wal_out_.close();
    {
        std::ofstream wal_reset(wal_path_, std::ios::out | std::ios::trunc);
        if (!wal_reset.is_open()) {
            if (error) {
                *error = "failed to reset archive file wal: " + wal_path_;
            }
            return false;
        }
    }
    wal_out_.open(wal_path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!wal_out_.is_open()) {
        if (error) {
            *error = "failed to reopen archive file wal: " + wal_path_;
        }
        return false;
    }
    if (!EnsureWalMagicLocked(error)) {
        return false;
    }
    if (wal_fsync_enabled_ && !FsyncPath(wal_path_)) {
        if (error) {
            *error = "failed to fsync archive file wal reset: " + wal_path_;
        }
        return false;
    }
    updates_since_snapshot_ = 0;
    return true;
}

void ArchiveFileMetaStore::EvictIfNeededLocked() {
    while (metas_.size() > max_files_) {
        auto victim = metas_.end();
        uint64_t oldest_ts = std::numeric_limits<uint64_t>::max();
        for (auto it = metas_.begin(); it != metas_.end(); ++it) {
            if (it->second.last_access_ts_ms < oldest_ts) {
                oldest_ts = it->second.last_access_ts_ms;
                victim = it;
            }
        }
        if (victim == metas_.end()) {
            break;
        }
        const ArchiveFileMeta evicted = victim->second;
        metas_.erase(victim);
        if (wal_out_.is_open() && AppendWalRecordLocked("D\t" + std::to_string(evicted.inode_id))) {
            ++updates_since_snapshot_;
        }
    }
}

} // namespace zb::real_node
