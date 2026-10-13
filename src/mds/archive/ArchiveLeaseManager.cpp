#include "ArchiveLeaseManager.h"

#include <algorithm>
#include <chrono>
#include <random>

#include "../storage/MetaSchema.h"

namespace zb::mds {

namespace {

uint64_t ResolveArchiveInodeId(const zb::rpc::ClaimArchiveTaskRequest& request) {
    return request.inode_id();
}

uint64_t ResolveArchiveInodeId(const zb::rpc::RenewArchiveLeaseRequest& request) {
    return request.inode_id();
}

uint64_t ResolveArchiveInodeId(const zb::rpc::CommitArchiveTaskRequest& request) {
    return request.inode_id();
}

uint64_t ResolveArchiveInodeId(const zb::rpc::ArchiveLeaseRecord& record) {
    return record.inode_id();
}

template <typename T>
std::string ResolveArchiveLeaseKey(const T& value) {
    const uint64_t inode_id = ResolveArchiveInodeId(value);
    if (inode_id != 0) {
        return FileArchiveStateKey(inode_id);
    }
    return std::string();
}

} // namespace

ArchiveLeaseManager::ArchiveLeaseManager(RocksMetaStore* store)
    : ArchiveLeaseManager(store, Options()) {}

ArchiveLeaseManager::ArchiveLeaseManager(RocksMetaStore* store, Options options)
    : store_(store), options_(options) {
    if (options_.min_lease_ms == 0) {
        options_.min_lease_ms = 1;
    }
    if (options_.default_lease_ms < options_.min_lease_ms) {
        options_.default_lease_ms = options_.min_lease_ms;
    }
    if (options_.max_lease_ms < options_.default_lease_ms) {
        options_.max_lease_ms = options_.default_lease_ms;
    }
}

bool ArchiveLeaseManager::Claim(const zb::rpc::ClaimArchiveTaskRequest& request,
                                zb::rpc::ClaimArchiveTaskReply* reply,
                                std::string* error) {
    if (!reply) {
        if (error) {
            *error = "claim reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    const uint64_t inode_id = ResolveArchiveInodeId(request);
    const std::string lease_key = ResolveArchiveLeaseKey(request);
    if (lease_key.empty()) {
        if (error) {
            *error = "archive inode_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(lease_key, inode_id, &record, error)) {
        return false;
    }

    const uint64_t now_ms = NowMilliseconds();
    reply->set_granted(false);
    if (record.state() != zb::rpc::ARCHIVE_STATE_PENDING &&
        record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING) {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
        record.clear_lease_id();
        record.set_lease_expire_ts_ms(0);
        record.set_update_ts_ms(now_ms);
        record.set_version(record.version() + 1);
        if (!PersistRecordLocked(record, error)) {
            return false;
        }
    }
    if (record.state() == zb::rpc::ARCHIVE_STATE_ARCHIVING && record.lease_expire_ts_ms() > now_ms) {
        if (record.owner_node_id() == request.node_id() &&
            record.owner_disk_id() == request.disk_id()) {
            reply->set_granted(true);
        }
        *reply->mutable_record() = record;
        return true;
    }

    const uint64_t lease_ms = NormalizeLeaseMs(request.requested_lease_ms());
    record.set_state(zb::rpc::ARCHIVE_STATE_ARCHIVING);
    record.set_owner_node_id(request.node_id());
    record.set_owner_disk_id(request.disk_id());
    record.set_lease_id(GenerateLeaseId());
    record.set_lease_expire_ts_ms(now_ms + lease_ms);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);

    if (!PersistRecordLocked(record, error)) {
        return false;
    }
    reply->set_granted(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::Renew(const zb::rpc::RenewArchiveLeaseRequest& request,
                                zb::rpc::RenewArchiveLeaseReply* reply,
                                std::string* error) {
    if (!reply) {
        if (error) {
            *error = "renew reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    const uint64_t inode_id = ResolveArchiveInodeId(request);
    const std::string lease_key = ResolveArchiveLeaseKey(request);
    if (lease_key.empty() || request.lease_id().empty()) {
        if (error) {
            *error = "archive inode_id or lease_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(lease_key, inode_id, &record, error)) {
        return false;
    }

    reply->set_renewed(false);
    const uint64_t now_ms = NowMilliseconds();
    if (record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.lease_id() != request.lease_id() ||
        (!request.node_id().empty() && record.owner_node_id() != request.node_id()) ||
        record.lease_expire_ts_ms() <= now_ms) {
        *reply->mutable_record() = record;
        return true;
    }

    const uint64_t lease_ms = NormalizeLeaseMs(request.requested_lease_ms());
    record.set_lease_expire_ts_ms(now_ms + lease_ms);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);
    if (!PersistRecordLocked(record, error)) {
        return false;
    }

    reply->set_renewed(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::Commit(const zb::rpc::CommitArchiveTaskRequest& request,
                                 zb::rpc::CommitArchiveTaskReply* reply,
                                 std::string* error) {
    if (!reply) {
        if (error) {
            *error = "commit reply is null";
        }
        return false;
    }
    if (!store_) {
        if (error) {
            *error = "lease manager store not initialized";
        }
        return false;
    }
    const uint64_t inode_id = ResolveArchiveInodeId(request);
    const std::string lease_key = ResolveArchiveLeaseKey(request);
    if (lease_key.empty()) {
        if (error) {
            *error = "archive inode_id is empty";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mu_);
    zb::rpc::ArchiveLeaseRecord record;
    if (!LoadOrInitRecordLocked(lease_key, inode_id, &record, error)) {
        return false;
    }

    reply->set_committed(false);
    reply->set_idempotent(false);
    const uint64_t now_ms = NowMilliseconds();
    if (record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.lease_id() != request.lease_id() ||
        (!request.node_id().empty() && record.owner_node_id() != request.node_id())) {
        *reply->mutable_record() = record;
        return true;
    }

    const bool success = request.success();
    if (!success && record.lease_expire_ts_ms() <= now_ms) {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    } else {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    }

    record.set_last_op_id(!request.op_id().empty() ? request.op_id() : record.lease_id());
    record.clear_lease_id();
    record.set_lease_expire_ts_ms(0);
    record.set_update_ts_ms(now_ms);
    record.set_version(record.version() + 1);

    if (success) {
        record.set_state(zb::rpc::ARCHIVE_STATE_PENDING);
        record.set_archive_ts_ms(now_ms);
        rocksdb::WriteBatch batch;
        batch.Delete(lease_key);
        if (!store_->WriteBatch(&batch, error)) {
            return false;
        }
    } else {
        if (!PersistRecordLocked(record, error)) {
            return false;
        }
    }

    reply->set_committed(true);
    *reply->mutable_record() = record;
    return true;
}

bool ArchiveLeaseManager::ValidateModelLease(uint64_t inode_id,
                                             const std::string& node_id,
                                             const std::string& disk_id,
                                             const std::string& lease_id,
                                             std::string* error) const {
    if (!store_ || inode_id == 0 || node_id.empty() || disk_id.empty() || lease_id.empty()) {
        if (error) *error = "incomplete modeled archive lease";
        return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    std::string payload;
    if (!store_->Get(FileArchiveStateKey(inode_id), &payload, error)) {
        if (error && error->empty()) *error = "archive lease not found";
        return false;
    }
    zb::rpc::ArchiveLeaseRecord record;
    if (!record.ParseFromString(payload) ||
        record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.owner_node_id() != node_id || record.owner_disk_id() != disk_id ||
        record.lease_id() != lease_id ||
        record.lease_expire_ts_ms() <= NowMilliseconds()) {
        if (error) *error = "archive lease changed or expired";
        return false;
    }
    return true;
}

bool ArchiveLeaseManager::CommitModelLeaseWithInodeCas(
    uint64_t inode_id, const std::string& node_id, const std::string& disk_id,
    const std::string& lease_id, const std::string& expected_inode_payload,
    rocksdb::WriteBatch* batch, bool* matched, std::string* error) {
    if (!store_ || !batch || !matched || inode_id == 0 || node_id.empty() ||
        disk_id.empty() || lease_id.empty()) {
        if (error) *error = "incomplete modeled archive commit";
        return false;
    }
    // Serializes Claim/Renew/Commit against the inode CAS and lease deletion.
    std::lock_guard<std::mutex> lock(mu_);
    const std::string key = FileArchiveStateKey(inode_id);
    std::string payload;
    if (!store_->Get(key, &payload, error)) {
        if (error && error->empty()) *error = "archive lease not found";
        return false;
    }
    zb::rpc::ArchiveLeaseRecord record;
    if (!record.ParseFromString(payload) ||
        record.state() != zb::rpc::ARCHIVE_STATE_ARCHIVING ||
        record.owner_node_id() != node_id || record.owner_disk_id() != disk_id ||
        record.lease_id() != lease_id ||
        record.lease_expire_ts_ms() <= NowMilliseconds()) {
        if (error) *error = "archive lease changed or expired";
        return false;
    }
    batch->Delete(key);
    return store_->WriteBatchIfValueEquals(InodeKey(inode_id), expected_inode_payload,
                                           batch, matched, error);
}

bool ArchiveLeaseManager::LoadOrInitRecordLocked(const std::string& lease_key,
                                                 uint64_t inode_id,
                                                 zb::rpc::ArchiveLeaseRecord* record,
                                                 std::string* error) const {
    if (!record) {
        if (error) {
            *error = "record pointer is null";
        }
        return false;
    }
    std::string data;
    std::string get_error;
    if (store_->Get(lease_key, &data, &get_error)) {
        if (!record->ParseFromString(data)) {
            if (error) {
                *error = "invalid archive lease record for archive unit";
            }
            return false;
        }
        return true;
    }
    if (!get_error.empty()) {
        if (error) {
            *error = get_error;
        }
        return false;
    }

    record->Clear();
    if (inode_id != 0) {
        record->set_inode_id(inode_id);
    }
    record->set_state(zb::rpc::ARCHIVE_STATE_PENDING);
    record->set_version(1);
    record->set_update_ts_ms(NowMilliseconds());
    return true;
}

bool ArchiveLeaseManager::PersistRecordLocked(const zb::rpc::ArchiveLeaseRecord& record, std::string* error) const {
    std::string value;
    if (!record.SerializeToString(&value)) {
        if (error) {
            *error = "failed to serialize archive lease record";
        }
        return false;
    }
    const std::string lease_key = ResolveArchiveLeaseKey(record);
    if (lease_key.empty()) {
        if (error) {
            *error = "archive lease record missing inode_id";
        }
        return false;
    }
    return store_->Put(lease_key, value, error);
}

uint64_t ArchiveLeaseManager::NormalizeLeaseMs(uint64_t requested_lease_ms) const {
    uint64_t lease_ms = requested_lease_ms > 0 ? requested_lease_ms : options_.default_lease_ms;
    lease_ms = std::max<uint64_t>(lease_ms, options_.min_lease_ms);
    lease_ms = std::min<uint64_t>(lease_ms, options_.max_lease_ms);
    return lease_ms;
}

std::string ArchiveLeaseManager::GenerateLeaseId() {
    static thread_local std::mt19937_64 rng(std::random_device{}());
    static const char kHex[] = "0123456789abcdef";
    std::string out(32, '0');
    for (size_t i = 0; i < out.size(); i += 16) {
        uint64_t value = rng();
        for (size_t j = 0; j < 16; ++j) {
            out[i + j] = kHex[(value >> ((15 - j) * 4)) & 0xF];
        }
    }
    return out;
}

uint64_t ArchiveLeaseManager::NowMilliseconds() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

} // namespace zb::mds
