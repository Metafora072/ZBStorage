#include "OpticalReadModel.h"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <fstream>
#include <limits>
#include <utility>

namespace zb::scheduler {

namespace {

std::string Trim(std::string text) {
    const auto start = text.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return {};
    const auto end = text.find_last_not_of(" \t\r\n");
    return text.substr(start, end - start + 1);
}

bool ParseUnsigned(const std::string& text, uint64_t* value) {
    if (text.empty()) return false;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), *value);
    return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
}

uint64_t EpochMilliseconds() {
    using namespace std::chrono;
    return static_cast<uint64_t>(duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count());
}

bool ArchiveActive(OpticalArchivePhase phase) {
    return phase == OpticalArchivePhase::kTransfer ||
           phase == OpticalArchivePhase::kPack ||
           phase == OpticalArchivePhase::kBurn;
}

bool ArchivePending(OpticalArchivePhase phase) {
    return ArchiveActive(phase) || phase == OpticalArchivePhase::kAwaitingMds;
}

bool ArchiveDuration(uint64_t bytes, uint64_t bytes_per_sec, uint64_t fixed_us,
                     uint64_t* duration_us) {
    if (!duration_us || bytes_per_sec == 0) return false;
    constexpr long double kMaxUs = 365.0L * 24 * 60 * 60 * 1000000;
    const long double value = std::ceil(
        static_cast<long double>(bytes) * 1000000.0L / bytes_per_sec) + fixed_us;
    if (value > kMaxUs) return false;
    *duration_us = static_cast<uint64_t>(value);
    return true;
}

bool ArchiveMediaBytes(uint64_t logical_bytes, uint32_t ratio_permille,
                       uint32_t alignment, uint64_t* media_bytes) {
    if (!media_bytes || ratio_permille == 0 || ratio_permille > 1000 ||
        alignment == 0) return false;
    const unsigned __int128 scaled =
        static_cast<unsigned __int128>(logical_bytes) * ratio_permille;
    const unsigned __int128 compressed = (scaled + 999) / 1000;
    const unsigned __int128 rounded =
        ((compressed + alignment - 1) / alignment) * alignment;
    if (rounded > std::numeric_limits<uint64_t>::max()) return false;
    *media_bytes = static_cast<uint64_t>(rounded);
    return true;
}

} // namespace

OpticalReadConfig OpticalReadConfig::LoadFromFile(const std::string& path,
                                                   std::string* error) {
    OpticalReadConfig config;
    std::ifstream input(path);
    if (!input) {
        if (error) *error = "failed to open optical model config: " + path;
        return {};
    }
    std::string line;
    size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        line = Trim(line);
        if (line.empty() || line[0] == '#') continue;
        const size_t separator = line.find('=');
        if (separator == std::string::npos) {
            if (error) *error = "invalid optical config line " + std::to_string(line_number);
            return {};
        }
        const std::string key = Trim(line.substr(0, separator));
        const std::string value = Trim(line.substr(separator + 1));
        if (key == "NODE_ID") {
            config.node_id = value;
            continue;
        }
        if (key == "INSTANCE_GENERATION") {
            if (!ParseUnsigned(value, &config.instance_generation) ||
                config.instance_generation == 0) {
                if (error) *error = "invalid INSTANCE_GENERATION at line " +
                                    std::to_string(line_number);
                return {};
            }
            continue;
        }
        if (key == "STATE_DIR") {
            config.state_dir = value;
            continue;
        }
        uint64_t number = 0;
        if (!ParseUnsigned(value, &number)) {
            if (error) *error = "invalid " + key + " at line " + std::to_string(line_number);
            return {};
        }
        if (key == "DRIVE_COUNT" && number <= UINT32_MAX)
            config.drive_count = static_cast<uint32_t>(number);
        else if (key == "DISC_COUNT" && number <= UINT32_MAX)
            config.disc_count = static_cast<uint32_t>(number);
        else if (key == "DISC_CAPACITY_BYTES") config.disc_capacity_bytes = number;
        else if (key == "DISC_READ_BYTES_PER_SEC") config.read_bytes_per_sec = number;
        else if (key == "CACHE_CAPACITY_BYTES") config.cache_capacity_bytes = number;
        else if (key == "CACHE_READ_BYTES_PER_SEC") config.cache_read_bytes_per_sec = number;
        else if (key == "CACHE_BASE_LATENCY_US" && number <= UINT32_MAX)
            config.cache_base_latency_us = static_cast<uint32_t>(number);
        else if (key == "LOAD_LATENCY_MS" && number <= UINT32_MAX)
            config.load_latency_ms = static_cast<uint32_t>(number);
        else if (key == "SEEK_LATENCY_MS" && number <= UINT32_MAX)
            config.seek_latency_ms = static_cast<uint32_t>(number);
        else if (key == "WAKE_LATENCY_MS" && number <= 3600000)
            config.wake_latency_ms = static_cast<uint32_t>(number);
        else if (key == "MAX_READ_TASKS" && number <= UINT32_MAX)
            config.max_read_tasks = static_cast<uint32_t>(number);
        else if (key == "TASK_RETENTION_MS" && number <= UINT32_MAX)
            config.retry_retention_ms = static_cast<uint32_t>(number);
        else if (key == "MAX_ARCHIVE_TASKS" && number <= UINT32_MAX)
            config.max_archive_tasks = static_cast<uint32_t>(number);
        else if (key == "ARCHIVE_TRANSFER_BYTES_PER_SEC")
            config.archive_transfer_bytes_per_sec = number;
        else if (key == "ARCHIVE_PACK_BYTES_PER_SEC")
            config.archive_pack_bytes_per_sec = number;
        else if (key == "ARCHIVE_BURN_BYTES_PER_SEC")
            config.archive_burn_bytes_per_sec = number;
        else if (key == "ARCHIVE_BURN_FIXED_MS" && number <= UINT32_MAX)
            config.archive_burn_fixed_ms = static_cast<uint32_t>(number);
        else if (key == "ARCHIVE_COMPRESSION_PERMILLE" && number > 0 && number <= 1000)
            config.archive_compression_permille = static_cast<uint32_t>(number);
        else if (key == "ARCHIVE_ALIGNMENT_BYTES" && number > 0 && number <= 1048576)
            config.archive_alignment_bytes = static_cast<uint32_t>(number);
        else {
            if (error) *error = "unknown or out-of-range optical key " + key +
                                " at line " + std::to_string(line_number);
            return {};
        }
    }
    return config;
}

OpticalReadModel::OpticalReadModel(OpticalReadConfig config) : config_(std::move(config)) {
    const auto now = Clock::now();
    drive_available_.resize(std::max<uint32_t>(1, config_.drive_count), now);
    cache_available_ = now;
    archive_transfer_available_ = now;
    archive_pack_available_ = now;
}

bool OpticalReadModel::InitStateStore(std::string* error) {
    std::lock_guard<std::mutex> lock(mu_);
    if (state_store_ || !files_.empty() || !tasks_.empty()) {
        if (error) *error = "optical state must open before serving requests";
        return false;
    }
    auto store = std::make_unique<OpticalStateStore>();
    std::vector<OpticalFileRecord> loaded;
    std::vector<OpticalTaskRecord> loaded_tasks;
    std::vector<OpticalArchiveRecord> loaded_archives;
    uint64_t next_task_id = 1;
    if (!store->Open(config_.state_dir, config_.node_id, config_.disc_count,
                     config_.disc_capacity_bytes, &loaded, &loaded_tasks, &loaded_archives,
                     &next_task_id, error)) return false;
    if (loaded_tasks.size() > config_.max_read_tasks) {
        if (error) *error = "recovered optical task count exceeds configured limit";
        return false;
    }
    for (const auto& file : loaded) {
        files_.emplace(file.inode_id,
                       File{file.version, file.size, file.disc_id, file.image_id,
                            file.media_bytes});
        disc_used_[file.disc_id] += file.media_bytes;
        stored_bytes_ += file.media_bytes;
    }
    const auto now = Clock::now();
    const uint64_t epoch_ms = EpochMilliseconds();
    Clock::time_point latest_pending = now;
    for (const auto& record : loaded_tasks) {
        Task task;
        task.inode_id = record.inode_id;
        task.version = record.version;
        task.size = record.size;
        task.remaining_us = record.remaining_us;
        task.release_epoch_ms = record.release_epoch_ms;
        task.released = record.released;
        task.cache_hit = record.cache_hit;
        if (record.released) {
            const uint64_t age_ms = epoch_ms >= record.release_epoch_ms
                ? epoch_ms - record.release_epoch_ms : 0;
            if (age_ms > config_.retry_retention_ms) {
                std::string delete_error;
                if (!store->DeleteTask(record.task_id, &delete_error)) {
                    if (error) *error = delete_error;
                    return false;
                }
                continue;
            }
            task.ready_at = now;
            task.released_at = now - std::chrono::milliseconds(age_ms);
        } else {
            // A stopped Scheduler does not advance modeled device work.
            // Restart the last durable remaining duration conservatively.
            task.ready_at = now + std::chrono::microseconds(record.remaining_us);
            latest_pending = std::max(latest_pending, task.ready_at);
        }
        tasks_.emplace(record.task_id, task);
        auto& latest = latest_task_[record.inode_id];
        latest = std::max(latest, record.task_id);
    }
    std::fill(drive_available_.begin(), drive_available_.end(), latest_pending);
    cache_available_ = latest_pending;
    uint32_t recovered_pending_archives = 0;
    for (const auto& record : loaded_archives) {
        if (ArchivePending(record.phase) &&
            ++recovered_pending_archives > config_.max_archive_tasks) {
            if (error) *error = "recovered optical archive queue exceeds configured limit";
            return false;
        }
        ArchiveJob job;
        job.record = record;
        if (ArchiveActive(record.phase)) {
            if (files_.count(record.inode_id) != 0 ||
                disc_used_.count(record.disc_id) != 0 ||
                disc_reserved_.count(record.disc_id) != 0 ||
                record.size > config_.cache_capacity_bytes - staging_used_bytes_ ||
                record.media_bytes > config_.disc_capacity_bytes) {
                if (error) *error = "recovered optical archive reservations conflict";
                return false;
            }
            for (const auto& [_, existing] : archives_) {
                if (ArchiveActive(existing.record.phase) &&
                    existing.record.inode_id == record.inode_id) {
                    if (error) *error = "duplicate active optical archive inode";
                    return false;
                }
            }
            const uint64_t used = disc_used_[record.disc_id];
            const uint64_t reserved = disc_reserved_[record.disc_id];
            if (used > config_.disc_capacity_bytes ||
                reserved > config_.disc_capacity_bytes - used ||
                record.media_bytes > config_.disc_capacity_bytes - used - reserved) {
                if (error) *error = "recovered optical archive disc capacity exceeded";
                return false;
            }
            disc_reserved_[record.disc_id] = reserved + record.media_bytes;
            staging_used_bytes_ += record.size;
            // Scheduler downtime does not advance any model stage. Recovered
            // resource queues conservatively serialize their remaining work.
            Clock::time_point* available = &archive_transfer_available_;
            if (record.phase == OpticalArchivePhase::kPack)
                available = &archive_pack_available_;
            if (record.phase == OpticalArchivePhase::kBurn) {
                auto slot = std::min_element(drive_available_.begin(), drive_available_.end());
                job.ready_at = std::max(*slot, now) +
                    std::chrono::microseconds(record.remaining_us);
                *slot = job.ready_at;
            } else {
                job.ready_at = std::max(*available, now) +
                    std::chrono::microseconds(record.remaining_us);
                *available = job.ready_at;
            }
        }
        archives_.emplace(record.operation_id, std::move(job));
    }
    std::unordered_map<std::string, bool> occupied_discs;
    for (const auto& [id, _] : disc_used_) occupied_discs[id] = true;
    for (const auto& [id, _] : disc_reserved_) occupied_discs[id] = true;
    if (occupied_discs.size() > config_.disc_count) {
        if (error) *error = "recovered optical archive disc count exceeded";
        return false;
    }
    next_task_id_ = next_task_id;
    state_store_ = std::move(store);
    return true;
}

OpticalModelResult OpticalReadModel::InstallFile(uint64_t inode_id, uint64_t version,
                                                 uint64_t size, const std::string& disc_id,
                                                 const std::string& image_id) {
    if (inode_id == 0 || version == 0 || disc_id.empty() || image_id.empty())
        return {OpticalModelCode::kInvalid, "invalid optical file identity"};
    std::lock_guard<std::mutex> lock(mu_);
    auto old = files_.find(inode_id);
    if (old != files_.end()) {
        const File& file = old->second;
        if (file.version == version && file.size == size &&
            file.disc_id == disc_id && file.image_id == image_id) return {};
        return {OpticalModelCode::kInvalid, "existing inode requires a versioned replacement contract"};
    }
    for (const auto& [_, archive] : archives_) {
        if (archive.record.inode_id == inode_id && ArchivePending(archive.record.phase))
            return {OpticalModelCode::kBusy, "optical inode has an unfinished archive"};
    }
    size_t occupied_discs = disc_used_.size();
    for (const auto& [id, _] : disc_reserved_)
        if (disc_used_.count(id) == 0) ++occupied_discs;
    if (size > config_.disc_capacity_bytes ||
        size > std::numeric_limits<uint64_t>::max() - stored_bytes_ ||
        config_.disc_count == 0 ||
        (disc_used_.find(disc_id) == disc_used_.end() &&
         disc_reserved_.find(disc_id) == disc_reserved_.end() &&
         occupied_discs >= config_.disc_count))
        return {OpticalModelCode::kNoSpace, "disc inventory full"};
    const auto existing_disc = disc_used_.find(disc_id);
    const uint64_t used = existing_disc == disc_used_.end() ? 0 : existing_disc->second;
    const uint64_t reserved = disc_reserved_.count(disc_id) ? disc_reserved_.at(disc_id) : 0;
    if (used > config_.disc_capacity_bytes ||
        reserved > config_.disc_capacity_bytes - used ||
        size > config_.disc_capacity_bytes - used - reserved)
        return {OpticalModelCode::kNoSpace, "disc capacity exceeded"};
    if (state_store_) {
        std::string error;
        if (!state_store_->PutFile({inode_id, version, size, disc_id, image_id, size}, &error))
            return {OpticalModelCode::kIoError, "optical inventory persistence failed: " + error};
    }
    disc_used_[disc_id] = used + size;
    stored_bytes_ += size;
    files_.emplace(inode_id, File{version, size, disc_id, image_id, size});
    return {};
}

OpticalModelResult OpticalReadModel::RemoveFile(uint64_t inode_id, uint64_t version,
                                                const std::string& disc_id,
                                                const std::string& image_id) {
    if (inode_id == 0 || version == 0 || disc_id.empty() || image_id.empty())
        return {OpticalModelCode::kInvalid, "invalid optical removal identity"};
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = files_.find(inode_id);
    if (it == files_.end()) return {OpticalModelCode::kNotFound, "optical file not found"};
    if (it->second.version != version || it->second.disc_id != disc_id ||
        it->second.image_id != image_id)
        return {OpticalModelCode::kInvalid, "optical removal version or location mismatch"};
    for (const auto& [_, archive] : archives_) {
        if (archive.record.inode_id == inode_id && ArchivePending(archive.record.phase))
            return {OpticalModelCode::kBusy, "optical archive has not been acknowledged"};
    }
    if (state_store_) {
        std::string error;
        std::vector<uint64_t> task_ids;
        for (const auto& [id, task] : tasks_)
            if (task.inode_id == inode_id) task_ids.push_back(id);
        if (!state_store_->DeleteFileAndTasks(inode_id, task_ids, &error))
            return {OpticalModelCode::kIoError, "optical inventory delete failed: " + error};
    }
    const uint64_t size = it->second.media_bytes;
    files_.erase(it);
    stored_bytes_ -= size;
    auto used = disc_used_.find(disc_id);
    if (used != disc_used_.end()) {
        used->second -= size;
        if (used->second == 0) disc_used_.erase(used);
    }
    const auto cached = cache_.find(inode_id);
    if (cached != cache_.end()) {
        cached_bytes_ -= cached->second.size;
        cache_.erase(cached);
    }
    latest_task_.erase(inode_id);
    for (auto task = tasks_.begin(); task != tasks_.end();) {
        if (task->second.inode_id == inode_id) task = tasks_.erase(task);
        else ++task;
    }
    return {};
}

void OpticalReadModel::PruneLocked(Clock::time_point now) {
    const auto retention = std::chrono::milliseconds(config_.retry_retention_ms);
    for (auto it = tasks_.begin(); it != tasks_.end();) {
        const bool expired = it->second.released
            ? now - it->second.released_at > retention
            : now - it->second.ready_at > retention;
        if (expired) {
            if (state_store_) {
                std::string error;
                if (!state_store_->DeleteTask(it->first, &error)) {
                    ++it;
                    continue;
                }
            }
            auto latest = latest_task_.find(it->second.inode_id);
            if (latest != latest_task_.end() && latest->second == it->first)
                latest_task_.erase(latest);
            it = tasks_.erase(it);
        } else ++it;
    }
}

OpticalModelResult OpticalReadModel::RequestRead(uint64_t inode_id,
                                                 const std::string& disc_id,
                                                 const std::string& image_id) {
    if (inode_id == 0 || disc_id.empty() || image_id.empty())
        return {OpticalModelCode::kInvalid, "missing optical read location"};
    std::lock_guard<std::mutex> lock(mu_);
    const auto now = Clock::now();
    PruneLocked(now);
    const auto file = files_.find(inode_id);
    if (file == files_.end() || file->second.disc_id != disc_id ||
        file->second.image_id != image_id)
        return {OpticalModelCode::kNotFound, "file or optical location not found"};
    if (tasks_.size() >= config_.max_read_tasks ||
        next_task_id_ == 0 || next_task_id_ == std::numeric_limits<uint64_t>::max())
        return {OpticalModelCode::kBusy, "optical read task queue full"};
    const auto hit = cache_.find(inode_id);
    const bool cache_hit = hit != cache_.end() && hit->second.version == file->second.version;
    Clock::time_point ready = now;
    size_t drive_slot = 0;
    if (cache_hit) {
        if (config_.cache_read_bytes_per_sec == 0)
            return {OpticalModelCode::kInvalid, "optical cache bandwidth unavailable"};
        const long double cache_us = std::ceil(
            static_cast<long double>(file->second.size) * 1000000.0L /
            config_.cache_read_bytes_per_sec);
        constexpr long double kMaxTransferUs = 24.0L * 60 * 60 * 1000000;
        if (cache_us > kMaxTransferUs)
            return {OpticalModelCode::kInvalid, "optical cache duration exceeds model limit"};
        ready = std::max(now, cache_available_) +
                std::chrono::microseconds(config_.cache_base_latency_us) +
                std::chrono::microseconds(static_cast<int64_t>(cache_us));
    } else {
        if (config_.drive_count == 0 || config_.read_bytes_per_sec == 0)
            return {OpticalModelCode::kInvalid, "optical drive resource unavailable"};
        const long double transfer_us = std::ceil(
            static_cast<long double>(file->second.size) * 1000000.0L /
            config_.read_bytes_per_sec);
        constexpr long double kMaxTransferUs = 24.0L * 60 * 60 * 1000000;
        if (transfer_us > kMaxTransferUs)
            return {OpticalModelCode::kInvalid, "optical transfer duration exceeds model limit"};
        auto slot = std::min_element(drive_available_.begin(), drive_available_.end());
        drive_slot = static_cast<size_t>(slot - drive_available_.begin());
        ready = std::max(*slot, now) +
                std::chrono::milliseconds(static_cast<uint64_t>(config_.load_latency_ms) +
                                           config_.seek_latency_ms) +
                std::chrono::microseconds(static_cast<int64_t>(transfer_us));
    }
    const auto modeled_wait = std::chrono::duration_cast<std::chrono::microseconds>(
        ready - now).count();
    const uint64_t remaining_us = static_cast<uint64_t>(std::max<int64_t>(0, modeled_wait));
    if (remaining_us > 24ULL * 60 * 60 * 1000000)
        return {OpticalModelCode::kInvalid, "optical total task duration exceeds model limit"};
    const uint64_t id = next_task_id_;
    if (state_store_) {
        std::string error;
        if (!state_store_->PutTask({id, inode_id, file->second.version, file->second.size,
                                    remaining_us, 0, false, cache_hit}, id + 1, &error))
            return {OpticalModelCode::kIoError, "optical read task persistence failed: " + error};
    }
    if (cache_hit) {
        cache_available_ = ready;
        hit->second.lru = ++lru_clock_;
    } else {
        drive_available_[drive_slot] = ready;
    }
    ++next_task_id_;
    tasks_.emplace(id, Task{inode_id, file->second.version, file->second.size,
                            remaining_us, 0, ready, {}, false, cache_hit});
    latest_task_[inode_id] = id;
    return {OpticalModelCode::kOk, {}, id, {}, cache_hit,
            remaining_us};
}

void OpticalReadModel::CacheReadyLocked(uint64_t inode_id, const Task& task) {
    if (task.size > config_.cache_capacity_bytes || config_.cache_capacity_bytes == 0) return;
    auto existing = cache_.find(inode_id);
    if (existing != cache_.end()) {
        existing->second.lru = ++lru_clock_;
        return;
    }
    if (task.size > config_.cache_capacity_bytes - staging_used_bytes_) return;
    while (cached_bytes_ > config_.cache_capacity_bytes - staging_used_bytes_ - task.size) {
        auto victim = std::min_element(cache_.begin(), cache_.end(),
            [](const auto& left, const auto& right) {
                return left.second.lru < right.second.lru;
            });
        if (victim == cache_.end()) return;
        cached_bytes_ -= victim->second.size;
        cache_.erase(victim);
    }
    cache_.emplace(inode_id, Cache{task.version, task.size, ++lru_clock_});
    cached_bytes_ += task.size;
}

OpticalModelResult OpticalReadModel::ReadTaskLocked(uint64_t task_id, uint64_t offset,
                                                    uint64_t size, Clock::time_point now) {
    const auto it = tasks_.find(task_id);
    if (it == tasks_.end()) return {OpticalModelCode::kNotFound, "optical task not found"};
    Task& task = it->second;
    const auto retention = std::chrono::milliseconds(config_.retry_retention_ms);
    if ((task.released && now - task.released_at > retention) ||
        (!task.released && now - task.ready_at > retention))
        return {OpticalModelCode::kNotFound, "optical task expired"};
    if (now < task.ready_at) return {OpticalModelCode::kNotReady, "MODEL_NOT_READY", task_id};
    if (size > 4ULL * 1024 * 1024)
        return {OpticalModelCode::kInvalid, "optical read chunk exceeds 4 MiB"};
    const uint64_t count = offset >= task.size ? 0 : std::min(size, task.size - offset);
    const bool release = offset >= task.size || count == task.size - offset;
    if (release && !task.released && state_store_) {
        std::string error;
        const uint64_t release_epoch_ms = EpochMilliseconds();
        if (!state_store_->UpdateTask({task_id, task.inode_id, task.version,
                                       task.size, 0, release_epoch_ms, true,
                                       task.cache_hit}, &error))
            return {OpticalModelCode::kIoError,
                    "optical read task release persistence failed: " + error};
        task.release_epoch_ms = release_epoch_ms;
        task.remaining_us = 0;
    }
    CacheReadyLocked(task.inode_id, task);
    OpticalModelResult result;
    result.task_id = task_id;
    result.cache_hit = task.cache_hit;
    result.data.assign(static_cast<size_t>(count), 'x');
    if (release) {
        task.released = true;
        if (task.released_at == Clock::time_point{}) task.released_at = now;
    }
    return result;
}

OpticalModelResult OpticalReadModel::ReadTask(uint64_t task_id, uint64_t offset,
                                              uint64_t size) {
    std::lock_guard<std::mutex> lock(mu_);
    const auto now = Clock::now();
    PruneLocked(now);
    return ReadTaskLocked(task_id, offset, size, now);
}

OpticalModelResult OpticalReadModel::ReadLatest(uint64_t inode_id, uint64_t offset,
                                                uint64_t size) {
    std::lock_guard<std::mutex> lock(mu_);
    const auto now = Clock::now();
    PruneLocked(now);
    const auto it = latest_task_.find(inode_id);
    if (it == latest_task_.end()) return {OpticalModelCode::kNotFound, "no task for inode"};
    return ReadTaskLocked(it->second, offset, size, now);
}

OpticalModelResult OpticalReadModel::SubmitVerifiedArchive(
    const OpticalArchiveRecord& request) {
    const auto valid_id = [](const std::string& value) {
        return !value.empty() && value.size() <= 256 &&
               value.find_first_of("\0\r\n", 0, 3) == std::string::npos;
    };
    if (request.inode_id == 0 || request.version == 0 ||
        !valid_id(request.operation_id) || !valid_id(request.source_lease_id) ||
        !valid_id(request.source_node_id) || !valid_id(request.source_disk_id) ||
        !valid_id(request.disc_id) || !valid_id(request.image_id))
        return {OpticalModelCode::kInvalid, "verified archive identity is incomplete"};
    std::lock_guard<std::mutex> lock(mu_);
    if (!state_store_)
        return {OpticalModelCode::kIoError, "durable optical state is required for archive"};
    const auto existing = archives_.find(request.operation_id);
    if (existing != archives_.end()) {
        const auto& old = existing->second.record;
        if (old.inode_id == request.inode_id && old.version == request.version &&
            old.size == request.size && old.source_lease_id == request.source_lease_id &&
            old.source_node_id == request.source_node_id &&
            old.source_disk_id == request.source_disk_id &&
            old.disc_id == request.disc_id && old.image_id == request.image_id) {
            if (old.phase == OpticalArchivePhase::kFailed)
                return {OpticalModelCode::kNotReady,
                        "prior optical archive operation failed; use a new operation ID"};
            return {OpticalModelCode::kOk, "idempotent optical archive request"};
        }
        return {OpticalModelCode::kInvalid, "archive operation ID reused with different manifest"};
    }
    if (files_.count(request.inode_id) != 0)
        return {OpticalModelCode::kInvalid, "optical inode already has a local location"};
    uint32_t pending = 0;
    for (const auto& [_, job] : archives_) {
        if (ArchivePending(job.record.phase)) ++pending;
        if (ArchivePending(job.record.phase) && job.record.inode_id == request.inode_id)
            return {OpticalModelCode::kBusy, "optical inode already has an archive task"};
    }
    if (pending >= config_.max_archive_tasks)
        return {OpticalModelCode::kBusy, "optical archive task queue full"};
    uint64_t media_bytes = 0;
    if (!ArchiveMediaBytes(request.size, config_.archive_compression_permille,
                           config_.archive_alignment_bytes, &media_bytes))
        return {OpticalModelCode::kInvalid,
                "optical encoded size exceeds model limit"};
    if (disc_used_.count(request.disc_id) != 0 ||
        disc_reserved_.count(request.disc_id) != 0)
        return {OpticalModelCode::kInvalid,
                "archive requires a blank dedicated disc until batch packing is supported"};
    size_t occupied_discs = disc_used_.size();
    for (const auto& [id, _] : disc_reserved_)
        if (disc_used_.count(id) == 0) ++occupied_discs;
    if (media_bytes > config_.disc_capacity_bytes ||
        request.size > config_.cache_capacity_bytes - staging_used_bytes_ ||
        (disc_used_.count(request.disc_id) == 0 &&
         disc_reserved_.count(request.disc_id) == 0 &&
         occupied_discs >= config_.disc_count))
        return {OpticalModelCode::kNoSpace, "optical archive media or staging full"};
    const uint64_t used = disc_used_.count(request.disc_id)
        ? disc_used_.at(request.disc_id) : 0;
    const uint64_t reserved = disc_reserved_.count(request.disc_id)
        ? disc_reserved_.at(request.disc_id) : 0;
    if (used > config_.disc_capacity_bytes ||
        reserved > config_.disc_capacity_bytes - used ||
        media_bytes > config_.disc_capacity_bytes - used - reserved)
        return {OpticalModelCode::kNoSpace, "optical archive disc capacity exceeded"};
    uint64_t transfer_us = 0;
    if (!ArchiveDuration(request.size, config_.archive_transfer_bytes_per_sec,
                         0, &transfer_us))
        return {OpticalModelCode::kInvalid, "optical archive transfer duration exceeds limit"};
    const auto now = Clock::now();
    const auto ready_at = std::max(now, archive_transfer_available_) +
        std::chrono::microseconds(transfer_us);
    const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
        ready_at - now).count();
    if (remaining < 0 || remaining > 365LL * 24 * 60 * 60 * 1000000)
        return {OpticalModelCode::kBusy, "optical archive transfer queue horizon exceeded"};
    OpticalArchiveRecord record = request;
    record.media_bytes = media_bytes;
    record.phase = OpticalArchivePhase::kTransfer;
    record.remaining_us = static_cast<uint64_t>(remaining);
    std::string error;
    if (!state_store_->PutArchive(record, &error))
        return {OpticalModelCode::kIoError, "optical archive admission persistence failed: " + error};
    archive_transfer_available_ = ready_at;
    disc_reserved_[record.disc_id] = reserved + record.media_bytes;
    staging_used_bytes_ += record.size;
    while (cached_bytes_ > config_.cache_capacity_bytes - staging_used_bytes_) {
        auto victim = std::min_element(cache_.begin(), cache_.end(),
            [](const auto& lhs, const auto& rhs) {
                return lhs.second.lru < rhs.second.lru;
            });
        if (victim == cache_.end()) break;
        cached_bytes_ -= victim->second.size;
        cache_.erase(victim);
    }
    archives_.emplace(record.operation_id, ArchiveJob{record, ready_at});
    return {OpticalModelCode::kOk, "optical archive task durably accepted"};
}

OpticalModelResult OpticalReadModel::AdvanceArchives() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!state_store_)
        return {OpticalModelCode::kIoError, "durable optical state is required for archive"};
    const auto now = Clock::now();
    std::vector<std::string> operation_ids;
    operation_ids.reserve(archives_.size());
    for (const auto& [id, _] : archives_) operation_ids.push_back(id);
    std::sort(operation_ids.begin(), operation_ids.end());
    for (const auto& id : operation_ids) {
        auto& job = archives_.at(id);
        while (ArchiveActive(job.record.phase) && now >= job.ready_at) {
            OpticalArchiveRecord next = job.record;
            if (next.phase == OpticalArchivePhase::kBurn) {
                if (files_.count(next.inode_id) != 0)
                    return {OpticalModelCode::kInvalid,
                            "optical archive target changed before local completion"};
                next.phase = OpticalArchivePhase::kAwaitingMds;
                next.remaining_us = 0;
                std::string error;
                if (!state_store_->CompleteArchive(
                        next, {next.inode_id, next.version, next.size,
                               next.disc_id, next.image_id, next.media_bytes}, &error))
                    return {OpticalModelCode::kIoError,
                            "optical archive local completion failed: " + error};
                const uint64_t reserved = disc_reserved_.at(next.disc_id);
                if (reserved == next.media_bytes) disc_reserved_.erase(next.disc_id);
                else disc_reserved_[next.disc_id] = reserved - next.media_bytes;
                staging_used_bytes_ -= next.size;
                disc_used_[next.disc_id] += next.media_bytes;
                stored_bytes_ += next.media_bytes;
                files_.emplace(next.inode_id,
                               File{next.version, next.size, next.disc_id, next.image_id,
                                    next.media_bytes});
                job.record = std::move(next);
                break;
            }
            uint64_t duration_us = 0;
            Clock::time_point next_ready;
            Clock::time_point* available = nullptr;
            if (next.phase == OpticalArchivePhase::kTransfer) {
                next.phase = OpticalArchivePhase::kPack;
                available = &archive_pack_available_;
                if (!ArchiveDuration(next.size, config_.archive_pack_bytes_per_sec,
                                     0, &duration_us))
                    return {OpticalModelCode::kInvalid, "optical archive pack duration exceeds limit"};
            } else {
                next.phase = OpticalArchivePhase::kBurn;
                auto slot = std::min_element(drive_available_.begin(), drive_available_.end());
                available = &*slot;
                if (!ArchiveDuration(next.media_bytes, config_.archive_burn_bytes_per_sec,
                                     static_cast<uint64_t>(config_.archive_burn_fixed_ms) * 1000,
                                     &duration_us))
                    return {OpticalModelCode::kInvalid, "optical archive burn duration exceeds limit"};
            }
            next_ready = std::max(job.ready_at, *available) +
                std::chrono::microseconds(duration_us);
            const auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
                next_ready - now).count();
            if (remaining > 365LL * 24 * 60 * 60 * 1000000)
                return {OpticalModelCode::kBusy, "optical archive stage queue horizon exceeded"};
            next.remaining_us = static_cast<uint64_t>(std::max<int64_t>(0, remaining));
            std::string error;
            if (!state_store_->PutArchive(next, &error))
                return {OpticalModelCode::kIoError,
                        "optical archive stage persistence failed: " + error};
            *available = next_ready;
            job.record = std::move(next);
            job.ready_at = next_ready;
        }
    }
    return {};
}

OpticalModelResult OpticalReadModel::AcknowledgeArchive(
    const std::string& operation_id, const std::string& source_lease_id) {
    std::lock_guard<std::mutex> lock(mu_);
    const auto found = archives_.find(operation_id);
    if (found == archives_.end()) return {OpticalModelCode::kNotFound, "optical archive not found"};
    if (found->second.record.source_lease_id != source_lease_id)
        return {OpticalModelCode::kInvalid, "optical archive lease mismatch"};
    if (found->second.record.phase == OpticalArchivePhase::kCommitted) return {};
    if (found->second.record.phase != OpticalArchivePhase::kAwaitingMds)
        return {OpticalModelCode::kNotReady, "optical archive has no publishable completion"};
    auto next = found->second.record;
    next.phase = OpticalArchivePhase::kCommitted;
    std::string error;
    if (!state_store_->PutArchive(next, &error))
        return {OpticalModelCode::kIoError, "optical archive acknowledgement failed: " + error};
    found->second.record = std::move(next);
    return {};
}

OpticalModelResult OpticalReadModel::FailArchive(
    const std::string& operation_id, const std::string& source_lease_id) {
    std::lock_guard<std::mutex> lock(mu_);
    const auto found = archives_.find(operation_id);
    if (found == archives_.end()) return {OpticalModelCode::kNotFound, "optical archive not found"};
    if (found->second.record.source_lease_id != source_lease_id)
        return {OpticalModelCode::kInvalid, "optical archive lease mismatch"};
    if (found->second.record.phase == OpticalArchivePhase::kFailed) return {};
    if (!ArchiveActive(found->second.record.phase))
        return {OpticalModelCode::kInvalid,
                "completed optical archive requires MDS reconciliation"};
    auto next = found->second.record;
    next.phase = OpticalArchivePhase::kFailed;
    next.remaining_us = 0;
    std::string error;
    if (!state_store_->PutArchive(next, &error))
        return {OpticalModelCode::kIoError, "optical archive failure persistence failed: " + error};
    const uint64_t reserved = disc_reserved_.at(next.disc_id);
    if (reserved == next.media_bytes) disc_reserved_.erase(next.disc_id);
    else disc_reserved_[next.disc_id] = reserved - next.media_bytes;
    staging_used_bytes_ -= next.size;
    found->second.record = std::move(next);
    return {};
}

bool OpticalReadModel::GetArchive(const std::string& operation_id,
                                  OpticalArchiveRecord* output) const {
    if (!output) return false;
    std::lock_guard<std::mutex> lock(mu_);
    const auto found = archives_.find(operation_id);
    if (found == archives_.end()) return false;
    *output = found->second.record;
    return true;
}

std::vector<OpticalArchiveRecord> OpticalReadModel::PendingArchives() const {
    std::lock_guard<std::mutex> lock(mu_);
    std::vector<OpticalArchiveRecord> out;
    for (const auto& [_, job] : archives_)
        if (ArchivePending(job.record.phase)) out.push_back(job.record);
    std::sort(out.begin(), out.end(), [](const auto& a, const auto& b) {
        return a.operation_id < b.operation_id;
    });
    return out;
}

OpticalModelStats OpticalReadModel::Stats() const {
    std::lock_guard<std::mutex> lock(mu_);
    OpticalModelStats stats;
    stats.tasks = static_cast<uint32_t>(tasks_.size());
    const auto now = Clock::now();
    for (const auto& item : tasks_) {
        if (!item.second.released && item.second.ready_at > now) ++stats.pending_tasks;
    }
    stats.occupied_discs = static_cast<uint32_t>(disc_used_.size());
    for (const auto& [id, _] : disc_reserved_)
        if (disc_used_.count(id) == 0) ++stats.occupied_discs;
    stats.recording_discs = static_cast<uint32_t>(disc_reserved_.size());
    stats.recorded_discs = stats.occupied_discs - stats.recording_discs;
    for (const auto& available_at : drive_available_) {
        if (available_at > now) ++stats.busy_drives;
    }
    stats.cached_files = static_cast<uint32_t>(cache_.size());
    stats.cached_bytes = cached_bytes_;
    stats.staging_used_bytes = staging_used_bytes_;
    for (const auto& [_, bytes] : disc_reserved_)
        stats.reserved_disc_bytes += bytes;
    for (const auto& [_, archive] : archives_) {
        if (ArchivePending(archive.record.phase)) ++stats.pending_write_tasks;
    }
    stats.stored_bytes = stored_bytes_;
    return stats;
}

} // namespace zb::scheduler
