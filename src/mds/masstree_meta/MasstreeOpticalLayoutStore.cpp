#include "MasstreeOpticalLayoutStore.h"

#include <filesystem>
#include <fstream>

#include "../storage/MetaSchema.h"

namespace zb::mds {

MasstreeOpticalLayoutStore::MasstreeOpticalLayoutStore(RocksMetaStore* store)
    : store_(store) {
}

bool MasstreeOpticalLayoutStore::LoadCurrent(bool* found,
                                            MasstreeOpticalLayoutTranslation* translation,
                                            std::string* error) const {
    if (!store_ || !found || !translation) {
        if (error) {
            *error = "invalid optical layout store load args";
        }
        return false;
    }
    std::string payload;
    std::string local_error;
    if (!store_->Get(MasstreeOpticalLayoutCurrentKey(), &payload, &local_error)) {
        if (!local_error.empty()) {
            if (error) {
                *error = local_error;
            }
            return false;
        }
        *found = false;
        if (error) {
            error->clear();
        }
        return true;
    }
    if (!MasstreeOpticalLayoutTranslation::Decode(payload, translation, error)) {
        return false;
    }
    *found = true;
    return true;
}

bool MasstreeOpticalLayoutStore::PutCurrent(const MasstreeOpticalLayoutTranslation& translation,
                                           rocksdb::WriteBatch* batch,
                                           std::string* error) const {
    if (!batch || !translation.Validate(error)) {
        return false;
    }
    batch->Put(MasstreeOpticalLayoutCurrentKey(), translation.Encode());
    if (error) {
        error->clear();
    }
    return true;
}

bool MasstreeOpticalLayoutStore::SaveSnapshot(const MasstreeOpticalLayoutTranslation& translation,
                                             const std::string& snapshot_path,
                                             std::string* error) const {
    if (snapshot_path.empty() || !translation.Validate(error)) {
        if (error && error->empty()) {
            *error = "optical layout snapshot path is empty";
        }
        return false;
    }
    const std::filesystem::path path(snapshot_path);
    std::error_code ec;
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path(), ec);
    }
    if (ec) {
        if (error) {
            *error = "failed to create optical layout snapshot directory: " + ec.message();
        }
        return false;
    }
    const std::filesystem::path temp_path = path.string() + ".tmp";
    std::ofstream out(temp_path, std::ios::trunc);
    if (!out) {
        if (error) {
            *error = "failed to create optical layout snapshot: " + temp_path.string();
        }
        return false;
    }
    out << translation.Encode();
    out.flush();
    if (!out.good()) {
        if (error) {
            *error = "failed to write optical layout snapshot: " + temp_path.string();
        }
        return false;
    }
    out.close();
    std::filesystem::rename(temp_path, path, ec);
    if (ec) {
        if (error) {
            *error = "failed to publish optical layout snapshot: " + ec.message();
        }
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace zb::mds
