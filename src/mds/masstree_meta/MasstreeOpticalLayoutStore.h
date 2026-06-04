#pragma once

#include <string>

#include "MasstreeOpticalLayoutTranslator.h"
#include "../storage/RocksMetaStore.h"

namespace zb::mds {

class MasstreeOpticalLayoutStore {
public:
    explicit MasstreeOpticalLayoutStore(RocksMetaStore* store);

    bool LoadCurrent(bool* found,
                     MasstreeOpticalLayoutTranslation* translation,
                     std::string* error) const;
    bool PutCurrent(const MasstreeOpticalLayoutTranslation& translation,
                    rocksdb::WriteBatch* batch,
                    std::string* error) const;
    bool SaveSnapshot(const MasstreeOpticalLayoutTranslation& translation,
                      const std::string& snapshot_path,
                      std::string* error) const;

private:
    RocksMetaStore* store_{};
};

} // namespace zb::mds
