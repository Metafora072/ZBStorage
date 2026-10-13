#include "OpticalStateStore.h"

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/write_batch.h>

#include <charconv>
#include <filesystem>
#include <limits>
#include <unordered_map>

namespace zb::scheduler {
namespace {

constexpr char kIdentityKey[] = "M:identity";
constexpr char kFilePrefix[] = "F:";
constexpr char kTaskPrefix[] = "T:";
constexpr char kArchivePrefix[] = "A:";
constexpr char kNextTaskIdKey[] = "M:next_task_id";

std::string FileKey(uint64_t inode_id) {
    return std::string(kFilePrefix) + std::to_string(inode_id);
}

std::string TaskKey(uint64_t task_id) {
    return std::string(kTaskPrefix) + std::to_string(task_id);
}

std::string ArchiveKey(const std::string& operation_id) {
    return std::string(kArchivePrefix) + operation_id;
}

void AppendUint64(std::string* output, uint64_t value) {
    for (unsigned int i = 0; i < 8; ++i)
        output->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
}

bool TakeUint64(const std::string& input, size_t* offset, uint64_t* value) {
    if (!offset || !value || *offset > input.size() || input.size() - *offset < 8) return false;
    *value = 0;
    for (unsigned int i = 0; i < 8; ++i)
        *value |= static_cast<uint64_t>(static_cast<unsigned char>(input[*offset + i])) << (8 * i);
    *offset += 8;
    return true;
}

void AppendString(std::string* output, const std::string& value) {
    AppendUint64(output, value.size());
    *output += value;
}

bool TakeString(const std::string& input, size_t* offset, std::string* value) {
    uint64_t size = 0;
    if (!TakeUint64(input, offset, &size) || size == 0 || size > 256 ||
        size > input.size() - *offset) return false;
    value->assign(input.data() + *offset, static_cast<size_t>(size));
    *offset += static_cast<size_t>(size);
    return true;
}

bool ParseUint64(const std::string& text, uint64_t* value) {
    if (text.empty() || !value) return false;
    const auto result = std::from_chars(text.data(), text.data() + text.size(), *value);
    return result.ec == std::errc{} && result.ptr == text.data() + text.size();
}

std::string EncodeFile(const OpticalFileRecord& file) {
    std::string result = "OF2";
    AppendUint64(&result, file.inode_id);
    AppendUint64(&result, file.version);
    AppendUint64(&result, file.size);
    AppendUint64(&result, file.disc_id.size());
    result += file.disc_id;
    AppendUint64(&result, file.image_id.size());
    result += file.image_id;
    AppendUint64(&result, file.media_bytes);
    return result;
}

bool DecodeFile(const std::string& input, OpticalFileRecord* file) {
    if (!file || (input.compare(0, 3, "OF1") != 0 &&
                  input.compare(0, 3, "OF2") != 0)) return false;
    const bool current = input.compare(0, 3, "OF2") == 0;
    size_t offset = 3;
    uint64_t disc_size = 0;
    uint64_t image_size = 0;
    if (!TakeUint64(input, &offset, &file->inode_id) ||
        !TakeUint64(input, &offset, &file->version) ||
        !TakeUint64(input, &offset, &file->size) ||
        !TakeUint64(input, &offset, &disc_size) ||
        disc_size == 0 || disc_size > input.size() - offset) return false;
    file->disc_id.assign(input.data() + offset, static_cast<size_t>(disc_size));
    offset += static_cast<size_t>(disc_size);
    if (!TakeUint64(input, &offset, &image_size) ||
        image_size == 0 || image_size > input.size() - offset ||
        file->inode_id == 0 || file->version == 0) return false;
    file->image_id.assign(input.data() + offset, static_cast<size_t>(image_size));
    offset += static_cast<size_t>(image_size);
    if (!current) file->media_bytes = file->size;
    else if (!TakeUint64(input, &offset, &file->media_bytes)) return false;
    return offset == input.size() && (file->size == 0 || file->media_bytes != 0);
}

std::string EncodeTask(const OpticalTaskRecord& task) {
    std::string result = "OT1";
    AppendUint64(&result, task.task_id);
    AppendUint64(&result, task.inode_id);
    AppendUint64(&result, task.version);
    AppendUint64(&result, task.size);
    AppendUint64(&result, task.remaining_us);
    AppendUint64(&result, task.release_epoch_ms);
    result.push_back(task.released ? 1 : 0);
    result.push_back(task.cache_hit ? 1 : 0);
    return result;
}

bool DecodeTask(const std::string& input, OpticalTaskRecord* task) {
    if (!task || input.size() != 3 + 6 * 8 + 2 ||
        input.compare(0, 3, "OT1") != 0) return false;
    size_t offset = 3;
    if (!TakeUint64(input, &offset, &task->task_id) ||
        !TakeUint64(input, &offset, &task->inode_id) ||
        !TakeUint64(input, &offset, &task->version) ||
        !TakeUint64(input, &offset, &task->size) ||
        !TakeUint64(input, &offset, &task->remaining_us) ||
        !TakeUint64(input, &offset, &task->release_epoch_ms) ||
        task->task_id == 0 || task->inode_id == 0 || task->version == 0 ||
        task->remaining_us > 24ULL * 60 * 60 * 1000000 ||
        static_cast<unsigned char>(input[offset]) > 1 ||
        static_cast<unsigned char>(input[offset + 1]) > 1) return false;
    task->released = input[offset] != 0;
    task->cache_hit = input[offset + 1] != 0;
    return (task->released && task->release_epoch_ms != 0) ||
           (!task->released && task->release_epoch_ms == 0);
}

std::string EncodeArchive(const OpticalArchiveRecord& archive) {
    std::string result = "OA2";
    result.push_back(static_cast<char>(archive.phase));
    AppendUint64(&result, archive.inode_id);
    AppendUint64(&result, archive.version);
    AppendUint64(&result, archive.size);
    AppendUint64(&result, archive.remaining_us);
    AppendString(&result, archive.operation_id);
    AppendString(&result, archive.source_lease_id);
    AppendString(&result, archive.source_node_id);
    AppendString(&result, archive.source_disk_id);
    AppendString(&result, archive.disc_id);
    AppendString(&result, archive.image_id);
    AppendUint64(&result, archive.media_bytes);
    return result;
}

bool DecodeArchive(const std::string& input, OpticalArchiveRecord* archive) {
    if (!archive || input.size() < 4 ||
        (input.compare(0, 3, "OA1") != 0 && input.compare(0, 3, "OA2") != 0))
        return false;
    const bool current = input.compare(0, 3, "OA2") == 0;
    const uint8_t phase = static_cast<uint8_t>(input[3]);
    if (phase < static_cast<uint8_t>(OpticalArchivePhase::kTransfer) ||
        phase > static_cast<uint8_t>(OpticalArchivePhase::kFailed)) return false;
    archive->phase = static_cast<OpticalArchivePhase>(phase);
    size_t offset = 4;
    const bool decoded = TakeUint64(input, &offset, &archive->inode_id) &&
           TakeUint64(input, &offset, &archive->version) &&
           TakeUint64(input, &offset, &archive->size) &&
           TakeUint64(input, &offset, &archive->remaining_us) &&
           TakeString(input, &offset, &archive->operation_id) &&
           TakeString(input, &offset, &archive->source_lease_id) &&
           TakeString(input, &offset, &archive->source_node_id) &&
           TakeString(input, &offset, &archive->source_disk_id) &&
           TakeString(input, &offset, &archive->disc_id) &&
           TakeString(input, &offset, &archive->image_id);
    if (!decoded) return false;
    if (!current) archive->media_bytes = archive->size;
    else if (!TakeUint64(input, &offset, &archive->media_bytes)) return false;
    return offset == input.size() && archive->inode_id != 0 && archive->version != 0 &&
           (archive->size == 0 || archive->media_bytes != 0) &&
           archive->remaining_us <= 365ULL * 24 * 60 * 60 * 1000000 &&
           (phase <= static_cast<uint8_t>(OpticalArchivePhase::kBurn) ||
            archive->remaining_us == 0);
}

} // namespace

OpticalStateStore::OpticalStateStore() = default;
OpticalStateStore::~OpticalStateStore() = default;

bool OpticalStateStore::Open(const std::string& path, const std::string& node_id,
                             uint32_t disc_count, uint64_t disc_capacity,
                             std::vector<OpticalFileRecord>* files,
                             std::vector<OpticalTaskRecord>* tasks,
                             std::vector<OpticalArchiveRecord>* archives,
                             uint64_t* next_task_id, std::string* error) {
    if (db_ || !files || !tasks || !archives || !next_task_id || path.empty() || node_id.empty() || disc_count == 0 ||
        disc_capacity == 0) {
        if (error) *error = "invalid or repeated optical state open";
        return false;
    }
    std::error_code path_error;
    const bool existed = std::filesystem::exists(path, path_error);
    if (path_error) {
        if (error) *error = "optical state path: " + path_error.message();
        return false;
    }
    rocksdb::Options options;
    options.create_if_missing = true;
    options.paranoid_checks = true;
    rocksdb::DB* raw = nullptr;
    auto status = rocksdb::DB::Open(options, path, &raw);
    if (!status.ok()) {
        if (error) *error = "optical state open: " + status.ToString();
        return false;
    }
    db_.reset(raw);
    const std::string expected = "ZB_OPTICAL_MODEL_V1\n" + node_id + "\n" +
                                 std::to_string(disc_count) + "\n" +
                                 std::to_string(disc_capacity) + "\n";
    std::string identity;
    status = db_->Get(rocksdb::ReadOptions(), kIdentityKey, &identity);
    if (status.IsNotFound() && !existed) {
        rocksdb::WriteOptions write_options;
        write_options.sync = true;
        status = db_->Put(write_options, kIdentityKey, expected);
    } else if (status.IsNotFound()) {
        if (error) *error = "existing optical state has no identity marker";
        return false;
    } else if (status.ok() && identity != expected) {
        if (error) *error = "optical node identity or disc geometry changed";
        return false;
    }
    if (!status.ok()) {
        if (error) *error = "optical state identity: " + status.ToString();
        return false;
    }

    std::unordered_map<std::string, uint64_t> disc_used;
    std::vector<OpticalFileRecord> loaded;
    std::vector<OpticalTaskRecord> loaded_tasks;
    std::vector<OpticalArchiveRecord> loaded_archives;
    uint64_t loaded_next_task_id = 1;
    bool next_task_id_seen = false;
    uint64_t total_used = 0;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        const std::string key = it->key().ToString();
        if (key == kIdentityKey) continue;
        if (key == kNextTaskIdKey) {
            if (!ParseUint64(it->value().ToString(), &loaded_next_task_id) ||
                loaded_next_task_id == 0) {
                if (error) *error = "invalid optical next task id";
                return false;
            }
            next_task_id_seen = true;
            continue;
        }
        if (key.compare(0, 2, kTaskPrefix) == 0) {
            uint64_t task_id = 0;
            OpticalTaskRecord task;
            if (!ParseUint64(key.substr(2), &task_id) ||
                TaskKey(task_id) != key ||
                !DecodeTask(it->value().ToString(), &task) ||
                task.task_id != task_id) {
                if (error) *error = "invalid optical read task record";
                return false;
            }
            loaded_tasks.push_back(task);
            continue;
        }
        if (key.compare(0, 2, kArchivePrefix) == 0) {
            OpticalArchiveRecord archive;
            if (!DecodeArchive(it->value().ToString(), &archive) ||
                ArchiveKey(archive.operation_id) != key) {
                if (error) *error = "invalid optical archive record";
                return false;
            }
            loaded_archives.push_back(std::move(archive));
            continue;
        }
        uint64_t inode_id = 0;
        OpticalFileRecord file;
        if (key.compare(0, 2, kFilePrefix) != 0 ||
            !ParseUint64(key.substr(2), &inode_id) ||
            FileKey(inode_id) != key ||
            !DecodeFile(it->value().ToString(), &file) ||
            file.inode_id != inode_id || file.media_bytes > disc_capacity) {
            if (error) *error = "invalid optical file inventory record";
            return false;
        }
        auto found = disc_used.find(file.disc_id);
        if (found == disc_used.end() && disc_used.size() >= disc_count) {
            if (error) *error = "optical disc count exceeded on recovery";
            return false;
        }
        const uint64_t used = found == disc_used.end() ? 0 : found->second;
        if (used > disc_capacity || file.media_bytes > disc_capacity - used ||
            file.media_bytes > std::numeric_limits<uint64_t>::max() - total_used) {
            if (error) *error = "optical disc capacity exceeded on recovery";
            return false;
        }
        disc_used[file.disc_id] = used + file.media_bytes;
        total_used += file.media_bytes;
        loaded.push_back(std::move(file));
    }
    if (!it->status().ok()) {
        if (error) *error = "optical inventory scan: " + it->status().ToString();
        return false;
    }
    std::unordered_map<uint64_t, const OpticalFileRecord*> by_inode;
    for (const auto& file : loaded) by_inode.emplace(file.inode_id, &file);
    for (const auto& task : loaded_tasks) {
        const auto found = by_inode.find(task.inode_id);
        if (!next_task_id_seen || task.task_id >= loaded_next_task_id ||
            found == by_inode.end() || found->second->version != task.version ||
            found->second->size != task.size) {
            if (error) *error = "optical task and file inventory disagree";
            return false;
        }
    }
    for (const auto& archive : loaded_archives) {
        const auto found = by_inode.find(archive.inode_id);
        if (archive.phase == OpticalArchivePhase::kAwaitingMds &&
            (found == by_inode.end() || found->second->version != archive.version ||
             found->second->size != archive.size ||
             found->second->media_bytes != archive.media_bytes ||
             found->second->disc_id != archive.disc_id ||
             found->second->image_id != archive.image_id)) {
            if (error) *error = "optical archive outbox and file inventory disagree";
            return false;
        }
        if (archive.phase == OpticalArchivePhase::kCommitted && found != by_inode.end() &&
            (found->second->version != archive.version ||
             found->second->size != archive.size ||
             found->second->media_bytes != archive.media_bytes ||
             found->second->disc_id != archive.disc_id ||
             found->second->image_id != archive.image_id)) {
            if (error) *error = "committed optical archive identity changed";
            return false;
        }
    }
    *files = std::move(loaded);
    *tasks = std::move(loaded_tasks);
    *archives = std::move(loaded_archives);
    *next_task_id = loaded_next_task_id;
    return true;
}

bool OpticalStateStore::PutArchive(const OpticalArchiveRecord& archive,
                                   std::string* error) {
    if (!db_ || archive.operation_id.empty()) {
        if (error) *error = "invalid optical archive persistence request";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Put(options, ArchiveKey(archive.operation_id),
                                 EncodeArchive(archive));
    if (!status.ok() && error) *error = "optical archive persistence: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::CompleteArchive(const OpticalArchiveRecord& archive,
                                        const OpticalFileRecord& file,
                                        std::string* error) {
    if (!db_ || archive.phase != OpticalArchivePhase::kAwaitingMds ||
        archive.inode_id != file.inode_id || archive.version != file.version ||
        archive.size != file.size || archive.media_bytes != file.media_bytes ||
        archive.disc_id != file.disc_id ||
        archive.image_id != file.image_id) {
        if (error) *error = "invalid optical archive completion";
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Put(FileKey(file.inode_id), EncodeFile(file));
    batch.Put(ArchiveKey(archive.operation_id), EncodeArchive(archive));
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Write(options, &batch);
    if (!status.ok() && error) *error = "optical archive completion: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::PutFile(const OpticalFileRecord& file, std::string* error) {
    if (!db_ || file.inode_id == 0 || file.version == 0 ||
        file.disc_id.empty() || file.image_id.empty()) {
        if (error) *error = "invalid optical file persistence request";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Put(options, FileKey(file.inode_id), EncodeFile(file));
    if (!status.ok() && error) *error = "optical file persistence: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::PutTask(const OpticalTaskRecord& task,
                                uint64_t next_task_id, std::string* error) {
    if (!db_ || task.task_id == 0 || next_task_id <= task.task_id) {
        if (error) *error = "invalid optical read task persistence request";
        return false;
    }
    rocksdb::WriteBatch batch;
    batch.Put(TaskKey(task.task_id), EncodeTask(task));
    batch.Put(kNextTaskIdKey, std::to_string(next_task_id));
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Write(options, &batch);
    if (!status.ok() && error) *error = "optical read task persistence: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::UpdateTask(const OpticalTaskRecord& task, std::string* error) {
    if (!db_ || task.task_id == 0) {
        if (error) *error = "invalid optical read task update";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Put(options, TaskKey(task.task_id), EncodeTask(task));
    if (!status.ok() && error) *error = "optical read task update: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::DeleteTask(uint64_t task_id, std::string* error) {
    if (!db_ || task_id == 0) {
        if (error) *error = "invalid optical read task delete";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    const auto status = db_->Delete(options, TaskKey(task_id));
    if (!status.ok() && error) *error = "optical read task delete: " + status.ToString();
    return status.ok();
}

bool OpticalStateStore::DeleteFileAndTasks(uint64_t inode_id,
                                           const std::vector<uint64_t>& task_ids,
                                           std::string* error) {
    if (!db_ || inode_id == 0) {
        if (error) *error = "invalid optical file delete";
        return false;
    }
    rocksdb::WriteOptions options;
    options.sync = true;
    rocksdb::WriteBatch batch;
    batch.Delete(FileKey(inode_id));
    for (uint64_t task_id : task_ids) batch.Delete(TaskKey(task_id));
    const auto status = db_->Write(options, &batch);
    if (!status.ok() && error) *error = "optical file delete: " + status.ToString();
    return status.ok();
}

} // namespace zb::scheduler
