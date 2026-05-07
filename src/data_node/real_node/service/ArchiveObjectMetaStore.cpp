#include "ArchiveObjectMetaStore.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <set>
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

constexpr double kReadHeatDelta = 1.0;
constexpr double kWriteHeatDelta = 2.0;
constexpr double kHeatDecay = 0.95;
constexpr char kArchiveWalMagic[] = {'A', 'M', 'W', '1'};
constexpr uint32_t kMaxWalPayloadBytes = 64U * 1024U * 1024U;

} // namespace

bool ArchiveObjectMetaStore::Init(const std::string& dir_path,
                                  size_t max_objects,
                                  uint32_t snapshot_interval_ops,
                                  std::string* error,
                                  bool wal_fsync) {
    if (dir_path.empty()) {
        if (error) {
            *error = "archive meta dir is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    dir_path_ = dir_path;
    wal_path_ = (fs::path(dir_path_) / "archive_meta.wal").string();
    snapshot_path_ = (fs::path(dir_path_) / "archive_meta.snapshot").string();
    max_objects_ = std::max<size_t>(1, max_objects);
    snapshot_interval_ops_ = std::max<uint32_t>(1, snapshot_interval_ops);
    wal_fsync_enabled_ = wal_fsync;
    updates_since_snapshot_ = 0;
    metas_.clear();
    wal_out_.close();

    std::error_code ec;
    fs::create_directories(dir_path_, ec);
    if (ec) {
        if (error) {
            *error = "failed to create archive meta dir: " + dir_path_;
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
            *error = "failed to open archive meta wal: " + wal_path_;
        }
        return false;
    }
    if (!EnsureWalMagicLocked(error)) {
        return false;
    }
    inited_ = true;
    return true;
}

void ArchiveObjectMetaStore::SetMaxObjects(size_t max_objects) {
    std::lock_guard<std::mutex> lock(mu_);
    max_objects_ = std::max<size_t>(1, max_objects);
    EvictIfNeededLocked();
}

void ArchiveObjectMetaStore::TrackObjectAccess(const std::string& disk_id,
                                              const std::string& object_id,
                                        uint64_t end_offset,
                                        bool is_write,
                                        uint64_t checksum,
                                        uint64_t now_ms) {
    if (disk_id.empty() || object_id.empty() || !inited_) {
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildObjectKey(disk_id, object_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        EvictIfNeededLocked();
        ArchiveObjectMeta meta;
        meta.disk_id = disk_id;
        meta.SetArchiveObjectId(object_id);
        it = metas_.emplace(key, std::move(meta)).first;
    }

    ArchiveObjectMeta& meta = it->second;
    meta.last_access_ts_ms = now_ms;
    meta.size_bytes = std::max<uint64_t>(meta.size_bytes, end_offset);
    if (checksum != 0) {
        meta.checksum = checksum;
    }
    meta.heat_score = meta.heat_score * kHeatDecay + (is_write ? kWriteHeatDelta : kReadHeatDelta);
    if (is_write) {
        ++meta.write_ops;
        if (meta.archive_state == "archived") {
            meta.archive_state = "pending";
        }
    } else {
        ++meta.read_ops;
    }
    ++meta.version;
    ArchiveObjectMeta persisted = meta;
    EvictIfNeededLocked();

    if (AppendWalRecordLocked("U\t" + SerializeMetaLine(persisted))) {
        ++updates_since_snapshot_;
        (void)MaybeSnapshotLocked();
    }
}

void ArchiveObjectMetaStore::RemoveObject(const std::string& disk_id, const std::string& object_id) {
    if (disk_id.empty() || object_id.empty() || !inited_) {
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildObjectKey(disk_id, object_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        return;
    }
    metas_.erase(it);
    if (AppendWalRecordLocked("D\t" + disk_id + "\t" + object_id)) {
        ++updates_since_snapshot_;
        (void)MaybeSnapshotLocked();
    }
}

bool ArchiveObjectMetaStore::UpdateObjectArchiveState(const std::string& disk_id,
                                                      const std::string& object_id,
                                               const std::string& archive_state,
                                               uint64_t version) {
    if (disk_id.empty() || object_id.empty() || archive_state.empty() || !inited_) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = BuildObjectKey(disk_id, object_id);
    auto it = metas_.find(key);
    if (it == metas_.end()) {
        ArchiveObjectMeta meta;
        meta.disk_id = disk_id;
        meta.SetArchiveObjectId(object_id);
        meta.archive_state = archive_state;
        meta.version = version > 0 ? version : 1;
        it = metas_.emplace(key, std::move(meta)).first;
    } else {
        it->second.archive_state = archive_state;
        it->second.version = version > 0 ? version : (it->second.version + 1);
    }

    const ArchiveObjectMeta persisted = it->second;
    if (!AppendWalRecordLocked("U\t" + SerializeMetaLine(persisted))) {
        return false;
    }
    ++updates_since_snapshot_;
    (void)MaybeSnapshotLocked();
    return true;
}

std::vector<ArchiveCandidateView> ArchiveObjectMetaStore::CollectCandidates(uint32_t max_candidates,
                                                                            uint64_t min_age_ms,
                                                                            uint64_t now_ms) const {
    if (!inited_ || max_candidates == 0) {
        return {};
    }

    auto is_better = [](const ArchiveCandidateView& a, const ArchiveCandidateView& b) {
        if (a.score != b.score) {
            return a.score > b.score;
        }
        if (a.last_access_ts_ms != b.last_access_ts_ms) {
            return a.last_access_ts_ms < b.last_access_ts_ms;
        }
        if (a.ArchiveObjectId() != b.ArchiveObjectId()) {
            return a.ArchiveObjectId() < b.ArchiveObjectId();
        }
        return a.disk_id < b.disk_id;
    };
    struct CandidateWorseFirst {
        bool operator()(const ArchiveCandidateView& a, const ArchiveCandidateView& b) const {
            if (a.score != b.score) {
                return a.score < b.score;
            }
            if (a.last_access_ts_ms != b.last_access_ts_ms) {
                return a.last_access_ts_ms > b.last_access_ts_ms;
            }
            if (a.ArchiveObjectId() != b.ArchiveObjectId()) {
                return a.ArchiveObjectId() > b.ArchiveObjectId();
            }
            return a.disk_id > b.disk_id;
        }
    };
    std::multiset<ArchiveCandidateView, CandidateWorseFirst> top_k;

    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& item : metas_) {
        const ArchiveObjectMeta& meta = item.second;
        if (meta.archive_state != "pending") {
            continue;
        }
        if (meta.last_access_ts_ms == 0 || now_ms <= meta.last_access_ts_ms) {
            continue;
        }
        const uint64_t age_ms = now_ms - meta.last_access_ts_ms;
        if (age_ms < min_age_ms) {
            continue;
        }

        ArchiveCandidateView candidate;
        candidate.disk_id = meta.disk_id;
        candidate.SetArchiveObjectId(meta.ArchiveObjectId());
        candidate.last_access_ts_ms = meta.last_access_ts_ms;
        candidate.size_bytes = meta.size_bytes;
        candidate.checksum = meta.checksum;
        candidate.heat_score = meta.heat_score;
        candidate.read_ops = meta.read_ops;
        candidate.write_ops = meta.write_ops;
        candidate.version = meta.version;
        candidate.archive_state = meta.archive_state;

        const double age_sec = static_cast<double>(age_ms) / 1000.0;
        const double size_mb = std::max<double>(1.0, static_cast<double>(meta.size_bytes) / (1024.0 * 1024.0));
        const double penalty =
            1.0 + std::max<double>(0.0, meta.heat_score) + static_cast<double>(meta.read_ops) +
            2.0 * static_cast<double>(meta.write_ops);
        candidate.score = (age_sec * size_mb) / penalty;
        if (top_k.size() < static_cast<size_t>(max_candidates)) {
            top_k.insert(std::move(candidate));
            continue;
        }
        auto worst_it = top_k.begin();
        if (worst_it == top_k.end()) {
            continue;
        }
        if (is_better(candidate, *worst_it)) {
            top_k.erase(worst_it);
            top_k.insert(std::move(candidate));
        }
    }

    std::vector<ArchiveCandidateView> out;
    out.reserve(top_k.size());
    for (const auto& entry : top_k) {
        out.push_back(entry);
    }
    std::sort(out.begin(), out.end(), is_better);
    return out;
}

std::vector<ArchiveObjectMeta> ArchiveObjectMetaStore::SnapshotMetas() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<ArchiveObjectMeta> out;
    out.reserve(metas_.size());
    for (const auto& item : metas_) {
        out.push_back(item.second);
    }
    return out;
}

bool ArchiveObjectMetaStore::ClearAll(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (!inited_) {
        if (error) {
            *error = "archive object meta store is not initialized";
        }
        return false;
    }
    metas_.clear();
    updates_since_snapshot_ = snapshot_interval_ops_;
    return WriteSnapshotLocked(error);
}

bool ArchiveObjectMetaStore::FlushSnapshot(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    return WriteSnapshotLocked(error);
}

std::string ArchiveObjectMetaStore::BuildObjectKey(const std::string& disk_id, const std::string& object_id) {
    return disk_id + "|" + object_id;
}

bool ArchiveObjectMetaStore::ParseMetaLine(const std::string& line, ArchiveObjectMeta* meta) {
    if (!meta) {
        return false;
    }
    std::vector<std::string> parts = SplitTabs(line);
    if (parts.size() != 10) {
        return false;
    }
    try {
        meta->disk_id = parts[0];
        meta->SetArchiveObjectId(parts[1]);
        meta->size_bytes = static_cast<uint64_t>(std::stoull(parts[2]));
        meta->checksum = static_cast<uint64_t>(std::stoull(parts[3]));
        meta->last_access_ts_ms = static_cast<uint64_t>(std::stoull(parts[4]));
        meta->heat_score = std::stod(parts[5]);
        meta->archive_state = parts[6];
        meta->version = static_cast<uint64_t>(std::stoull(parts[7]));
        meta->read_ops = static_cast<uint64_t>(std::stoull(parts[8]));
        meta->write_ops = static_cast<uint64_t>(std::stoull(parts[9]));
    } catch (...) {
        return false;
    }
    return !meta->disk_id.empty() && !meta->ArchiveObjectId().empty();
}

std::string ArchiveObjectMetaStore::SerializeMetaLine(const ArchiveObjectMeta& meta) {
    std::ostringstream oss;
    oss << meta.disk_id << '\t'
        << meta.ArchiveObjectId() << '\t'
        << meta.size_bytes << '\t'
        << meta.checksum << '\t'
        << meta.last_access_ts_ms << '\t'
        << meta.heat_score << '\t'
        << meta.archive_state << '\t'
        << meta.version << '\t'
        << meta.read_ops << '\t'
        << meta.write_ops;
    return oss.str();
}

std::vector<std::string> ArchiveObjectMetaStore::SplitTabs(const std::string& line) {
    std::vector<std::string> parts;
    std::string token;
    std::istringstream stream(line);
    while (std::getline(stream, token, '\t')) {
        parts.push_back(token);
    }
    return parts;
}

uint32_t ArchiveObjectMetaStore::Crc32(const std::string& data) {
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

bool ArchiveObjectMetaStore::WriteUint32LE(std::ostream* out, uint32_t value) {
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

bool ArchiveObjectMetaStore::FsyncPath(const std::string& path) {
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

bool ArchiveObjectMetaStore::LoadSnapshotLocked(std::string* error) {
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
        ArchiveObjectMeta meta;
        if (!ParseMetaLine(line, &meta)) {
            if (error) {
                *error = "invalid archive snapshot at line " + std::to_string(line_no);
            }
            return false;
        }
        metas_[BuildObjectKey(meta.disk_id, meta.ArchiveObjectId())] = std::move(meta);
    }
    return true;
}

bool ArchiveObjectMetaStore::ReplayWalLocked(std::string* error) {
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
                *error = "failed to truncate incompatible wal: " + wal_path_;
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
            ArchiveObjectMeta meta;
            if (!ParseMetaLine(body, &meta)) {
                need_truncate = true;
                break;
            }
            metas_[BuildObjectKey(meta.disk_id, meta.ArchiveObjectId())] = std::move(meta);
        } else if (op == 'D') {
            std::vector<std::string> parts = SplitTabs(body);
            if (parts.size() != 2 || parts[0].empty() || parts[1].empty()) {
                need_truncate = true;
                break;
            }
            metas_.erase(BuildObjectKey(parts[0], parts[1]));
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
                *error = "failed to truncate corrupted wal tail: " + wal_path_;
            }
            return false;
        }
    }
    return true;
}

bool ArchiveObjectMetaStore::EnsureWalMagicLocked(std::string* error) {
    if (!wal_out_.is_open()) {
        if (error) {
            *error = "archive wal stream is not open";
        }
        return false;
    }

    std::error_code ec;
    const uint64_t file_size = fs::exists(wal_path_, ec) ? fs::file_size(wal_path_, ec) : 0;
    if (ec) {
        if (error) {
            *error = "failed to stat archive wal: " + wal_path_;
        }
        return false;
    }
    if (file_size == 0) {
        wal_out_.write(kArchiveWalMagic, static_cast<std::streamsize>(sizeof(kArchiveWalMagic)));
        wal_out_.flush();
        if (!wal_out_.good()) {
            if (error) {
                *error = "failed to write archive wal magic: " + wal_path_;
            }
            return false;
        }
        if (wal_fsync_enabled_ && !FsyncPath(wal_path_)) {
            if (error) {
                *error = "failed to fsync archive wal magic: " + wal_path_;
            }
            return false;
        }
        return true;
    }
    if (file_size < sizeof(kArchiveWalMagic)) {
        if (error) {
            *error = "archive wal is too small for magic header: " + wal_path_;
        }
        return false;
    }
    return true;
}

bool ArchiveObjectMetaStore::AppendWalRecordLocked(const std::string& payload) {
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

bool ArchiveObjectMetaStore::MaybeSnapshotLocked() {
    if (updates_since_snapshot_ < snapshot_interval_ops_) {
        return true;
    }
    std::string error;
    return WriteSnapshotLocked(&error);
}

bool ArchiveObjectMetaStore::WriteSnapshotLocked(std::string* error) {
    if (snapshot_path_.empty() || wal_path_.empty() || dir_path_.empty()) {
        if (error) {
            *error = "archive meta store not initialized";
        }
        return false;
    }
    const std::string tmp_path = snapshot_path_ + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
        if (!out.is_open()) {
            if (error) {
                *error = "failed to open archive snapshot tmp: " + tmp_path;
            }
            return false;
        }
        for (const auto& item : metas_) {
            out << SerializeMetaLine(item.second) << "\n";
        }
        out.flush();
        if (!out.good()) {
            if (error) {
                *error = "failed to write archive snapshot tmp: " + tmp_path;
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
            *error = "failed to rotate archive snapshot: " + snapshot_path_;
        }
        return false;
    }
    if (wal_fsync_enabled_ && !FsyncPath(snapshot_path_)) {
        if (error) {
            *error = "failed to fsync archive snapshot: " + snapshot_path_;
        }
        return false;
    }

    wal_out_.close();
    {
        std::ofstream wal_reset(wal_path_, std::ios::out | std::ios::trunc);
        if (!wal_reset.is_open()) {
            if (error) {
                *error = "failed to reset archive wal: " + wal_path_;
            }
            return false;
        }
    }
    wal_out_.open(wal_path_, std::ios::out | std::ios::app | std::ios::binary);
    if (!wal_out_.is_open()) {
        if (error) {
            *error = "failed to reopen archive wal: " + wal_path_;
        }
        return false;
    }
    if (!EnsureWalMagicLocked(error)) {
        return false;
    }
    if (wal_fsync_enabled_ && !FsyncPath(wal_path_)) {
        if (error) {
            *error = "failed to fsync archive wal reset: " + wal_path_;
        }
        return false;
    }
    updates_since_snapshot_ = 0;
    return true;
}

void ArchiveObjectMetaStore::EvictIfNeededLocked() {
    while (metas_.size() > max_objects_) {
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
        const ArchiveObjectMeta evicted = victim->second;
        metas_.erase(victim);
        if (wal_out_.is_open() &&
            AppendWalRecordLocked("D\t" + evicted.disk_id + "\t" + evicted.ArchiveObjectId())) {
            ++updates_since_snapshot_;
        }
    }
}

} // namespace zb::real_node
