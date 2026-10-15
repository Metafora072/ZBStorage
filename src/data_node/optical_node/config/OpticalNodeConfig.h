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

    // image_dir 可容纳的镜像数上限（硬上限，READ + WRITE 共用；默认盘 1TiB / 卷 10GiB
    // 下单盘约 113 个镜像，故取值需远大于单盘镜像数）。
    uint64_t capacity_in_images{1000};
    // 可用 ID 池的容量上限（AVAILABLE_ID_COUNT）：volume_id（MDS image_id）与
    // disk_id（MDS disc_id）两个池共用同一容量，取用时按需向各自计数器补齐。
    uint8_t available_id_count{5};

    // 光盘打包：单张光盘容量上限（字节）与单张光盘标准镜像数。
    // 标准镜像数为基线口径，实际单张光盘按容量可能容纳更多镜像。
    uint64_t disc_capacity_bytes{1099511627776ULL};
    uint32_t standard_images_per_disc{100};
    // 库内光盘数量（对应 Scheduler 的 local_disc_count）：库位按此数量装载盘片，
    // 其状态（空白/在刻/已刻）随刻录变化，但盘片数量保持不变。0 或非法值按 1000 生效。
    uint64_t total_disc_count{1000};
    // 光盘库槽位数（光盘架总槽数，对应 Scheduler 的 total_slots）。须 >= 光盘数量；
    // 0 或非法值按 100000 生效，小于光盘数量时抬升到光盘数量。
    uint64_t slot_count{100000};
    // 光盘数据块大小（字节）：超级块、元数据区与每个 vimg 均按此对齐。
    uint64_t disc_block_size_bytes{2048};

    // 最大写镜像数：image_dir 中允许同时存在的写（WRITE）镜像数上限，用于下载侧背压
    // ——超过该值即暂停下载原始文件，等 Zip 压缩 / 刻录释放把写镜像数降下来。
    // 配置约束：单盘镜像数 < MAX_WRITE_IMAGES < CAPACITY_IN_IMAGES。
    //   下限侧：必须大于单盘镜像数，否则永远攒不满一张盘（永不封印 → 永不释放 → 活锁）；
    //   上限侧：必须小于 image_dir 硬上限，否则背压来不及生效、先撞上 VOLUME_FULL_NO_READABLE。
    // 下限为 1（配置为 0 或负数时按 1 生效）。
    uint32_t max_write_images{128};

    // 光盘库硬件 / 模拟参数：逐项透传给 cd_manager（替代原先写死在 cd_manager.cpp 里的常量）。
    // 光驱数与机械臂数。
    uint32_t drive_count{20};
    uint32_t arm_count{4};
    // 刻录 / 读盘参数。
    uint64_t burn_image_size_bytes{10ull * 1024 * 1024 * 1024};
    double burn_bandwidth_mbps{20.0};
    double burn_fixed_seconds{1.0};
    double read_bandwidth_mbps{100.0};
    double read_fixed_seconds{0.5};
    // 光驱 / 机械臂动作耗时（秒）。
    double drive_load_seconds{12.0};
    double drive_eject_seconds{3.0};
    double grasp_seconds{2.0};
    // 机械臂移动速度（米/秒）。
    double arm_speed_x{2.0};
    double arm_speed_y{1.0};
    double arm_speed_z{1.0};
    // 货架几何（行/列/深度）与单个盘位尺寸（米）。
    double rack_rows{50.0};
    double rack_cols{200.0};
    double rack_depth{10.0};
    double slot_width{0.015};
    double slot_height{0.15};
    double slot_depth{0.3};

    // 注册 profile 静态属性：注册 ManagedNodeSpec 时逐项回填，缺省沿用默认光盘库模板口径。
    uint64_t external_network_bandwidth_bytes_per_sec{10000000000ull};
    uint64_t fixed_access_latency_us{500};
    uint64_t optical_load_latency_us{60000000ull};
    uint64_t optical_seek_latency_us{500000};
    // hdd-cache 设备组（image_dir 缓存）读/写带宽。
    uint64_t cache_read_bytes_per_sec{200000000ull};
    uint64_t cache_write_bytes_per_sec{200000000ull};
    // 光驱设备组读/写带宽。
    uint64_t drive_read_bytes_per_sec{100000000ull};
    uint64_t drive_write_bytes_per_sec{10000000ull};
    // 功率（毫瓦）：峰/中/待机/关。
    uint64_t power_peak_milliwatts{400000};
    uint64_t power_medium_milliwatts{200000};
    uint64_t power_standby_milliwatts{50000};
    uint64_t power_off_milliwatts{0};
    // 可靠性：寿命均值 / 标准差 / 最小值（毫秒）。
    uint64_t reliability_mean_lifetime_ms{315360000000ull};
    uint64_t reliability_lifetime_stddev_ms{63072000000ull};
    uint64_t reliability_minimum_lifetime_ms{31536000000ull};
    // 技术代际与实例代际。
    uint32_t technology_generation{1};
    uint64_t instance_generation{1};

    static OpticalNodeConfig LoadFromFile(const std::string& path, std::string* error);
};

} // namespace zb::optical_node
