#pragma once
#include "DrainMigrationCoordinator.h"
#include "mds.pb.h"
#include "optical_node.pb.h"

namespace zb::scheduler {
// 仅负责文件化真实光盘库的整库交接；不实现机械换盘/重新刻录。
// 目标选择后由源/目标持久收据固定；Scheduler 重启可从收据续传。
class PhysicalOpticalDrain {
public:
    PhysicalOpticalDrain(NodePowerManager* nodes, std::string mds, uint32_t timeout)
        : nodes_(nodes), mds_(std::move(mds)), timeout_(timeout) {}
    bool Begin(const std::string&, uint64_t, std::string*);
    bool Advance(DrainMigrationTask*, uint64_t, std::string*);
private:
    bool Call(const zb::rpc::OpticalFileLocation&, zb::rpc::OpticalHandoffAction,
              const zb::rpc::OpticalLibraryHandoffRecord&, zb::rpc::OpticalLibraryHandoffReply*,
              std::string*, const std::string& path = {}, uint64_t offset = 0,
              const std::string& data = {});
    bool Commit(const zb::rpc::OpticalLibraryHandoffRecord&, std::string*);
    NodePowerManager* nodes_;
    std::string mds_;
    uint32_t timeout_;
    struct Cursor { std::string manifest; size_t file{0}; };
    // 仅迁移执行线程访问；持久进度以接收端文件/收据为准，重启可重建游标。
    std::unordered_map<std::string, Cursor> cursors_;
};
} // namespace zb::scheduler
