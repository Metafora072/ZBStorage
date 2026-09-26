#pragma once

#include <cstdint>
#include <string>

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

    // 光盘打包：单张光盘容量上限（字节）与单张光盘标准镜像数。
    // 标准镜像数为基线口径，实际单张光盘按容量可能容纳更多镜像。
    uint64_t disc_capacity_bytes{1099511627776ULL};
    uint32_t standard_images_per_disc{100};

    static OpticalNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::optical_node
