#pragma once

#include <brpc/channel.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "DrainMigrationCoordinator.h"

namespace zb::scheduler {

class BrpcObjectMigrationBackend : public ObjectMigrationBackend {
public:
    BrpcObjectMigrationBackend(std::string mds_address,
                               uint32_t timeout_ms,
                               uint64_t copy_chunk_bytes);
    bool ListObjects(const std::string& source_address,
                     std::vector<MigrationObject>* objects,
                     std::string* error) override;
    bool CopyAndVerify(const std::string& source_address,
                       const MigrationObject& object,
                       const MigrationTarget& target,
                       std::string* error) override;
    bool CommitFileLocation(const std::string& migration_id,
                            uint64_t inode_id,
                            const std::string& source_node_id,
                            const std::string& source_address,
                            const std::string& source_disk_id,
                            const MigrationTarget& target,
                            std::string* error) override;
    bool VerifyCmsNodeReferencesZero(const std::string& node_id,
                                     uint64_t* disk_references,
                                     uint64_t* optical_references,
                                     std::string* error) override;
    bool DeleteSourceObject(const std::string& source_address,
                            const MigrationObject& object,
                            std::string* error) override;

private:
    brpc::Channel* ChannelFor(const std::string& address, std::string* error);
    std::string mds_address_;
    uint32_t timeout_ms_{5000};
    uint64_t copy_chunk_bytes_{4ULL * 1024ULL * 1024ULL};
    std::mutex channel_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

} // namespace zb::scheduler
