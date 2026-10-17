#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "optical_node.pb.h"

namespace brpc { class Channel; }

namespace zb::scheduler::api {

// 给 MDS 使用的光盘归档清单客户端。地址、节点、代际和后端类型均从 CMS 获取，
// 不从 Scheduler 的本地提案推断；仅查询，不负责分配、版本提交或删除源数据。
// false 时保留已收到的业务回复，但自动分页输出会清空，禁止消费部分清单。
// 不隐式重试。CONFLICT 时调用方应从第一页重新读取；旧服务未实现时明确失败。
class OpticalArchiveClient {
public:
    OpticalArchiveClient(std::string address, std::string node_id, uint64_t instance_generation,
                         bool modeled, uint32_t timeout_ms = 5000);
    ~OpticalArchiveClient();
    OpticalArchiveClient(const OpticalArchiveClient&) = delete;
    OpticalArchiveClient& operator=(const OpticalArchiveClient&) = delete;

    // 查询本地已完成介质清单。request 的身份必须与构造参数完全一致。
    bool ListArchiveImages(const rpc::ListArchiveImagesRequest&, rpc::ListArchiveImagesReply*,
                           std::string* error);
    // 查询一个盘片/镜像的文件映射；has_file_version=false 时由 MDS 另行核对版本。
    bool GetArchiveImageFiles(const rpc::GetArchiveImageFilesRequest&, rpc::GetArchiveImageFilesReply*,
                              std::string* error);
    // 查询现存归档阶段任务；任务终态不等于文件完成，真实终态任务可能被原引擎清理。
    bool ListArchiveTasks(const rpc::ListArchiveTasksRequest&, rpc::ListArchiveTasksReply*, std::string* error);
    bool ListAllTasks(std::vector<rpc::ArchiveTaskInfo>*, std::string* error, uint32_t page_size = 128);
    // 固定快照令牌自动翻页，出错不返回半份清单；page_size 为 1..1024。
    bool ListAllImages(std::vector<rpc::ArchiveImageInfo>*, std::string* error, uint32_t page_size = 128);
    bool GetAllImageFiles(const std::string& disk_id, const std::string& image_id,
                          std::vector<rpc::ArchiveImageFile>*, std::string* error,
                          uint32_t page_size = 128);

private:
    bool Connect(std::string* error);
    template <class Request, class Reply, class Method>
    bool Call(const Request&, Reply*, Method, std::string* error);
    std::string address_, node_id_;
    uint64_t generation_;
    bool modeled_;
    uint32_t timeout_ms_;
    std::mutex connect_mu_;
    std::unique_ptr<brpc::Channel> channel_;
};

} // namespace zb::scheduler::api
