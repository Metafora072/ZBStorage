#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace zb::optical_node {

struct OpticalNodeConfig {
    // 节点身份与网络
    std::string node_id;
    std::string node_address;
    std::string scheduler_addr;
    std::string group_id;
    std::string node_role;
    std::string peer_node_id;
    std::string peer_address;

    // 副本复制
    bool replication_enabled{false};
    uint32_t replication_timeout_ms{2000};

    // 心跳上报
    uint32_t node_weight{1};
    uint32_t virtual_node_count{1};
    uint32_t heartbeat_interval_ms{2000};

    // 归档引擎（OpticalNodeManager）
    // 数据根目录（Run 的 root_dir）
    std::string archive_root{"/tmp/zb_optical"};
    // 卷镜像大小上限（字节，默认 10 GiB）与封装触发阈值（百分比 0.0-1.0），对应构造函数参数
    uint64_t volume_size_bytes{10ull * 1024 * 1024 * 1024};
    double size_threshold{0.9};

    // image_dir 可容纳的镜像数上限
    uint64_t capacity_in_images{10};
    // available_volume_ids 队列容量上限
    uint8_t available_volume_id_count{5};
    // 首次启动按 FIFO 灌入队列的初始 volume_id 集合
    std::vector<uint64_t> initial_available_volume_ids{1, 2, 3, 4, 5};

    static OpticalNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::optical_node
