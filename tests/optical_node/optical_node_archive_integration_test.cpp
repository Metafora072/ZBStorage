// optical_node 单模块集成测试：
//   进程内起真光节点 server（OpticalStorageServiceImpl + BrpcOpticalNodeService），
//   配合假 real_node / scheduler 两个 brpc server，以及假 MDS / client 两个 brpc 客户端，
//   在小规模参数（卷镜像 10MiB、每文件 1MiB 按 256KiB 切成 4 个对象分片、光盘容量 100MiB、
//   image_dir 上限 100 / 背压上限 30；evict 档位改为 14 / 12）下仿真完整链路：
//     归档下发（四种批次形态）→ （下载侧背压节流）从 real_node 按分片下载 → 压缩封装
//     → 打包汇报（控制台）→ 按容量积攒 → 装满封印为一张光盘（disc_sim/disc_<id>.vdisc）→ 刻录
//     → 逐个释放 image_dir 中的镜像副本 → 刻录汇报（控制台）
//     数据面读 → 镜像被释放时按元数据偏移从 vdisc 提取回 image_dir（CD_READ）
//     → 分片读到 FINISH → 满载后的 LRU 淘汰与重新提取（evict 档位）
//
// 用法: optical_node_archive_integration_test --corpus <dir> --work-dir <dir> --scenario smoke|full|evict
// 输出: 全程日志写到 stderr（由 run_optical_node_archive_test.py 落盘为 <work-dir>/output.log）。
//       日志按"测试点"组织：
//         [CASE] A3.1 第 1/12 卷：数据面读 + CD_READ 重载
//             测试目标 / 期望 / 验证方法
//           [STEP] 读 volume_1 inode=1000（镜像在光盘库，预期触发一次 CD_READ 装载）
//           [PASS] 读回数据与语料逐字节一致（inode=1000，共 1048576 字节，耗时 21043ms）
//         [CASE-END] A3.1 result=PASS checks=5 failed=0
//       末尾输出"测试点汇总"表与各测试点目标清单；测试点编号见 README.md。

#include "fake_client.h"
#include "fake_services.h"
#include "optical_node_test_support.h"

#include <brpc/server.h>
#include <optical_node.pb.h>

#include "BrpcOpticalNodeService.h"
#include "OpticalStorageServiceImpl.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <ctime>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace zb::optical_node;
using namespace zb::optical_node::test;

// 小规模测试参数（与 prepare_archive_corpus.py 的假设一致）。
// 卷镜像 10 MiB × 每卷 10 个 1 MiB 文件（实测压缩率 ≈0.983）→ 单镜像 ≈ 10.1 MiB；
// 光盘容量 100 MiB → 单盘恰好容纳 10 个镜像，因此需要 >10 个卷才会封印出第一张盘。
constexpr uint64_t kVolumeSizeBytes = 10ull * 1024 * 1024;
constexpr double kSizeThreshold = 0.9;
constexpr uint64_t kDiscCapacityBytes = 100ull * 1024 * 1024;
constexpr uint32_t kStandardImagesPerDisc = 10;
constexpr size_t kFilesPerVolume = 10;

// 归档文件的多分片形态：1 MiB 文件按 256 KiB 切成 4 个对象分片（末片为余数）。
// 覆盖点：下载侧必须逐片取回并在磁盘按绝对偏移重组（见测试点 A2 的分片形态断言）。
constexpr uint64_t kObjectUnitBytes = 256ull * 1024;
constexpr uint64_t kFileSizeBytes = 1024ull * 1024;
constexpr size_t kShardsPerFile = static_cast<size_t>(kFileSizeBytes / kObjectUnitBytes);

constexpr uint8_t kAvailableVolumeIdCount = 5;
constexpr uint64_t kReadShardBytes = 4ull * 1024 * 1024;

// ---------------------------------------------------------------------------
// 测试档位（profile）：容量约束参数按场景区分，让「写链路顺跑」与「容量满载边界」各有场地。
// 不变式：单盘镜像数(10) < 背压上限(MAX_WRITE_IMAGES) < image_dir 容量(CAPACITY_IN_IMAGES)。
//   normal（smoke / full）：容量 100 / 背压上限 30 —— 缓存充足，写链路在充足余量下连续跨越
//       多张光盘；LRU 淘汰在此不可达（需先堆满 100 个镜像），故由 evict 档位单独覆盖。
//   evict：容量 14 / 背压上限 12 —— 小容量缓存，读入若干已刻录卷后必然满载并触发淘汰与重载。
// ---------------------------------------------------------------------------
constexpr uint64_t kCapacityInImages = 100;
constexpr uint32_t kMaxWriteImages = 30;
constexpr uint64_t kSmallCapacityInImages = 14;
constexpr uint32_t kSmallMaxWriteImages = 12;

// full / evict 档位下 A3 读取的卷数：读够即停，把「满载后淘汰」留给 A5（避免 A3 阶段就触发淘汰）。
constexpr size_t kA3ReadVolumes = 12;

struct Profile {
    std::string name;              // 档位名（写入运行头部日志）
    uint64_t capacity_in_images;   // image_dir 硬上限（CAPACITY_IN_IMAGES）
    uint32_t max_write_images;     // 下载背压阈值（MAX_WRITE_IMAGES）
    size_t a3_read_volumes;        // 测试点 A3 读取的已刻录卷数
    bool check_eviction;           // 是否执行测试点 A5（满载淘汰与重载）
    bool check_backpressure;       // 是否执行测试点 B2（背压触发与恢复）
};

Profile ProfileFor(const std::string& scenario) {
    if (scenario == "evict") {
        return {"evict", kSmallCapacityInImages, kSmallMaxWriteImages, kA3ReadVolumes, true, true};
    }
    if (scenario == "full") {
        return {"normal", kCapacityInImages, kMaxWriteImages, kA3ReadVolumes, false, true};
    }
    return {"normal", kCapacityInImages, kMaxWriteImages, 1, false, false};
}

// 仿真时长：单次光盘操作（装盘 12s + 读/刻录 + 退盘 3s + 机械臂）约 20~30s，
// 且产线不建议为测试改动仿真参数，故超时留足余量。
constexpr int kPackReportTimeoutMs = 180000;
constexpr int kBurnTimeoutMs = 300000;
constexpr int kCdReadTimeoutMs = 240000;
constexpr int kCacheHitTimeoutMs = 15000;
// 背压触发/解除的等待上限：解除依赖一次刻录完成（释放写镜像并 notify），故与刻录同量级。
constexpr int kBackpressureTimeoutMs = 300000;

// ---------------------------------------------------------------------------
// 测试点描述文案：目标（为什么要测）/ 期望（判定条件）/ 方法（靠什么观测）。
// 编号与 README.md「测试点与断言覆盖面」一节一致；同类测试点（A1/A3/A4）多次实例共用文案。
// ---------------------------------------------------------------------------

constexpr const char* kCaseA0Objective =
    "确认被测光节点与它的全部仿真对手方（假 real_node / scheduler / MDS / client）就绪，"
    "且语料分组真值、工作目录齐备——后续所有断言都必须建立在可信前提之上。";
constexpr const char* kCaseA0Expectation =
    "语料分组数 > 0 且每卷文件数一致；档位参数满足「单盘镜像数 < 背压上限 < image_dir 容量」；"
    "OpticalNodeManager 状态为 RUNNING（IsArchiveEngineReady 为真）；"
    "光节点、假 real_node、假 scheduler 三个 brpc server 均成功监听；archive 工作目录已创建。";
constexpr const char* kCaseA0Method =
    "载入 manifest.tsv 并交叉校验 expected_pack_plan.tsv；调用 IsArchiveEngineReady()/GetArchiveStatusDetail()；"
    "读取各 LocalServer 的 listen_address()；stat 各工作目录。";

constexpr const char* kCaseA1Objective =
    "验证写链路：MDS 下发的归档文件应被节点按 target_node_id/target_disk_id 从 real_node 分片下载、"
    "按 object_unit_size 拼装，压缩后按阈值累加，达到打包阈值时封装为卷镜像并上报。"
    "镜像随后进入光盘待打包集合，等盘装满后由封印流程一次性封成一张光盘（写入 vdisc 并提交刻录）。"
    "下发批次形态故意与卷边界不对齐（逐文件小批次 / 整卷单批次 / 跨卷边界批次 / 大小混合批次），"
    "以覆盖「一个批次内的文件分属多张卷镜像」与「多个批次累计成一张卷镜像」两种切分情形。";
constexpr const char* kCaseA1Expectation =
    "打包汇报行的 count 与 inode 集合等于 prepare_archive_corpus.py 用 zlib9 精确预测的分组"
    "（批次如何切分都不改变卷分组）；封装后卷镜像出现在 image_dir（封盘刻录前一直留在缓存目录）、"
    "体积 ∈ (0, 卷大小]；各卷 volume_id 互不重复。";
constexpr const char* kCaseA1Method =
    "SendArchiveMetadata 下发批次；抓取节点 stdout 的 [ReportFilesPackedToImage] 行解析 image_id/count/inode_ids；"
    "轮询 image_dir 的文件系统事实（不使用固定 sleep）。";

constexpr const char* kCaseA7Objective =
    "验证光盘级封装与刻录：多个卷镜像应按容量积攒，装不下下一个时才把当前集合封印成一张光盘"
    "（生成 disc_sim/disc_<disc_id>.vdisc），刻录完成后逐个释放 image_dir 中的镜像副本并上报"
    "ReportImagesBurnedToDisc；未装满的尾盘应继续留在 pending（不刻录）。";
constexpr const char* kCaseA7Expectation =
    "至少封印并刻录 1 张光盘；每张盘的汇报 count 与 image_ids 数量一致；"
    "刻录汇报的镜像恰为「按顺序打包的前 N 个卷」（其余为未装满的尾盘）；"
    "每张盘都有对应 vdisc 文件且体积不超过盘容量；已刻录镜像已从 image_dir 释放；尾盘镜像仍在 image_dir。";
constexpr const char* kCaseA7Method =
    "抓取节点 stdout 的 [ReportImagesBurnedToDisc] 行解析 disc_id/count/image_ids；"
    "以 meta/disc_<id>_meta 数量作为「已封印盘数」确定性上界等待刻录完成；"
    "枚举 disc_sim 与 image_dir 的文件系统事实交叉验证（不使用固定 sleep）。";

constexpr const char* kCaseA2Objective =
    "验证节点没有漏读或重复读 real_node 的对象——下载完整性只能靠跨卷汇总计数证明。"
    "覆盖点：归档文件按 256 KiB 切成 4 个对象分片，节点必须逐片取回并在磁盘按绝对偏移重组。";
constexpr const char* kCaseA2Expectation =
    "ResolveFileRead 调用次数 == 已归档文件数；ReadObject 调用次数 == 对象总数"
    "（文件数 × 每文件分片数，本参数下为 4）；语料中每个文件的分片大小均为 256 KiB。";
constexpr const char* kCaseA2Method =
    "统计假 real_node 的两个原子计数器，与语料 manifest.tsv 推算出的期望值比对；"
    "并交叉校验每个文件的分片数与分片大小（多分片形态确实生效）。";

constexpr const char* kCaseA3Objective =
    "验证读链路在镜像已刻录（不在 image_dir）时的行为：读请求必须触发 CD_READ，把对应卷镜像"
    "按元数据记录的偏移从 vdisc 光盘文件中复制回 image_dir，再把目标 inode 从镜像中按分片读出，"
    "末片读完后任务进入 FINISH；vdisc 作为光盘本体持久保留、不被读取搬移或删除。";
constexpr const char* kCaseA3Expectation =
    "读回数据与语料原始 jpg 数据逐字节一致；镜像回到 image_dir；disc_sim 中的 vdisc 数量保持不变"
    "（读回只复制、不搬移）；末片读完后 ReadObjectByInodeId 返回 MDS_NOT_FOUND（FINISH 时 inode→task 索引被清除）。";
constexpr const char* kCaseA3Method =
    "RequestAsyncReadFile 取 task_id，按 4MiB 分片轮询读并拼接后与本地语料比对；"
    "用 RPC 状态码作为 FINISH 证据；每步等待均记录实际耗时。";

constexpr const char* kCaseA4Objective =
    "验证同一卷内第二次读不重复走光盘装载：镜像已在 image_dir 时应直接命中缓存，"
    "避免无谓的 CD_READ（单次装载约 20~30s）。";
constexpr const char* kCaseA4Expectation =
    "第二次读耗时 < 3000ms（远小于一次 CD 装载的仿真时长）；image_dir 与 disc_sim 的目录内容"
    "在第二次读前后完全不变；读回数据仍与语料一致。";
constexpr const char* kCaseA4Method =
    "记录读前读后的目录快照并比对，同时以读耗时作为交叉证据（两者需同时成立）。";

constexpr const char* kCaseA5Objective =
    "验证 image_dir 达到容量上限后读入新卷时，节点应淘汰一个已有的 READ 镜像（直接删除缓存副本）"
    "以腾出空间，且被淘汰的卷之后仍能从其 vdisc 重新装载读通——淘汰只影响缓存层级，不影响数据可用性。";
constexpr const char* kCaseA5Expectation =
    "淘汰后 image_dir 镜像数恰为 capacity_in_images；刚读入的卷保留在 image_dir；"
    "disc_sim 中的 vdisc 数量不变（淘汰只删缓存副本，不动光盘本体）；"
    "victim 恰为读前读后快照的差集那一个；被淘汰卷重新读时能再次从 vdisc 装载并读通、并回到 image_dir。";
constexpr const char* kCaseA5Method =
    "先读满 image_dir 再读一个「已刻录但不在 image_dir」的卷，随后对比读前/读后目录快照求出 victim，"
    "再对 victim 发起一次完整读（含字节比对）。";

constexpr const char* kCaseA6Objective =
    "验证归档流程结束后不残留中间产物：input/ 中不应留下已归档文件，log/ 目录应存在"
    "（任务终态日志按 600s 周期清理生成，测试期内不出现属已知缺口）。";
constexpr const char* kCaseA6Expectation =
    "input/ 下 .archive 文件数为 0；log/ 目录存在。";
constexpr const char* kCaseA6Method =
    "枚举 input/ 目录列表；stat log/；若测试期内意外生成了任务日志则打印提示。";

constexpr const char* kCaseB1Objective =
    "验证「MDS 批次切分」与「卷镜像分组」解耦：节点按文件顺序逐个压缩累加，因此批次边界"
    "（单文件批次、跨卷边界批次、大小混合批次）不应改变卷分组，也不应造成文件遗漏或重复。"
    "同时覆盖「多个 MDS 批次累计成一轮下载」——下载线程的一轮配额 = 一个卷镜像容量，"
    "故卷 1 的 10 个单文件批次恰好凑成一轮。";
constexpr const char* kCaseB1Expectation =
    "四种批次形态（逐文件小批次 / 整卷单批次 / 跨卷边界批次 / 大小混合批次）均被覆盖；"
    "存在跨越卷边界的批次；单文件批次累计成卷（不各自成卷）；"
    "全部卷镜像覆盖的文件集合 == 下发的文件集合（每个文件恰好属于一个卷镜像，无遗漏、无重复）。";
constexpr const char* kCaseB1Method =
    "由预测分组与批次计划在测试侧做结构统计；再用打包汇报汇总出的 inode 计数与下发集合比对"
    "（不依赖固定 sleep，全部来自已校验过的汇报行）。";

constexpr const char* kCaseB2Objective =
    "验证下载背压的两端：写镜像数达上限（MAX_WRITE_IMAGES）时应暂停下载、不把 image_dir 堆到硬上限；"
    "刻录完成释放写镜像后应唤醒被阻塞的下载线程（不能睡死）。";
constexpr const char* kCaseB2Expectation =
    "写阶段采样到 throttled=true 且写镜像数达到背压上限；过冲不超过 1 个卷镜像；"
    "随后在刻录完成释放写镜像后采样到 throttled=false（阻塞被解除、下载线程被唤醒）。";
constexpr const char* kCaseB2Method =
    "逐批次采样 GetArchiveStatusDetail() 的 backpressure 快照并累计"
    "（触发过/峰值/最近一次），以快照数值而非最终文件数作为判定与失败证据。";

std::string NowString() {
    const std::time_t now = std::time(nullptr);
    char buf[32] = {0};
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now));
    return buf;
}

struct Options {
    std::string corpus_dir;
    std::string work_dir;
    std::string scenario = "smoke";
};

// 被测节点的工作目录（与 OpticalNodeManager::InitializeDir 的派生规则保持一致）。
struct Env {
    std::string archive_root;
    std::string input_dir;
    std::string temp_dir;
    std::string image_dir;
    std::string read_dir;
    std::string disc_sim_dir;
    std::string meta_dir;
    std::string log_dir;

    std::vector<std::string> dirs() const {
        return {input_dir, temp_dir, image_dir, read_dir, disc_sim_dir, log_dir};
    }
};

Options ParseArgs(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--corpus" || arg == "--work-dir" || arg == "--scenario") {
            Require(i + 1 < argc, "参数缺少取值: " + arg);
            const std::string value = argv[++i];
            if (arg == "--corpus") {
                options.corpus_dir = value;
            } else if (arg == "--work-dir") {
                options.work_dir = value;
            } else {
                options.scenario = value;
            }
        } else {
            std::cerr << "未知参数: " << arg << std::endl;
            std::exit(2);
        }
    }
    Require(!options.corpus_dir.empty(), "必须指定 --corpus");
    Require(!options.work_dir.empty(), "必须指定 --work-dir");
    Require(options.scenario == "smoke" || options.scenario == "full" ||
                options.scenario == "evict",
            "--scenario 只能是 smoke / full / evict");
    return options;
}

Env MakeEnv(const std::string& work_dir) {
    Env env;
    std::string root = work_dir;
    if (!root.empty() && root.back() != '/') {
        root += '/';
    }
    env.archive_root = root + "archive/";
    env.input_dir = env.archive_root + "input/";
    env.temp_dir = env.archive_root + "temp/";
    env.image_dir = env.archive_root + "image/";
    env.read_dir = env.archive_root + "read/";
    env.disc_sim_dir = env.archive_root + "disc_sim/";
    env.meta_dir = env.archive_root + "meta/";
    env.log_dir = env.archive_root + "log/";
    return env;
}

std::string ImagePath(const Env& env, uint64_t volume_id) {
    return env.image_dir + "volume_" + std::to_string(volume_id) + ".vimg";
}

// 光盘文件（vdisc）：一张已刻录光盘一个文件。
std::string VdiscPath(const Env& env, uint64_t disc_id) {
    return env.disc_sim_dir + "disc_" + std::to_string(disc_id) + ".vdisc";
}

// image_dir 中的卷镜像文件名（形如 volume_<id>.vimg）。
std::vector<std::string> ImageFiles(const Env& env) {
    return ListDirBySuffix(env.image_dir, ".vimg");
}

// disc_sim 中的光盘文件名（形如 disc_<id>.vdisc）。
std::vector<std::string> DiscFiles(const Env& env) {
    return ListDirBySuffix(env.disc_sim_dir, ".vdisc");
}

// meta/ 下已封印光盘的元数据文件数（disc_<id>_meta；排除 pending_disc_meta）。
// 封印时即刻生成该文件，因此可作为「已封印盘数」的确定性上界。
size_t SealedDiscMetaCount(const Env& env) {
    size_t count = 0;
    for (const std::string& name : ListDirBySuffix(env.meta_dir, "_meta")) {
        if (name.rfind("disc_", 0) == 0) {
            ++count;
        }
    }
    return count;
}

// 从 volume_<id>.vimg 取出 <id>。
uint64_t ParseVolumeIdFromName(const std::string& name) {
    const std::string prefix = "volume_";
    if (name.rfind(prefix, 0) != 0) {
        return 0;
    }
    return ::strtoull(name.c_str() + prefix.size(), nullptr, 10);
}

// ---------------------------------------------------------------------------
// 批次形态覆盖（测试点 B1 的输入）：把全部卷的文件按顺序切成若干 MDS 批次。
//
// 节点侧只有「一个下载线程 + 一个压缩线程」，文件处理顺序 == 下发顺序，因此批次如何切分
// 都不改变卷分组（预测分组仍逐卷成立）——这正是要验证的解耦性质。故意让部分批次边界与
// 卷边界错开，覆盖：
//   ① 逐文件小批次：10 次单文件下发；节点一轮下载配额 = 一个卷镜像容量（10 MiB），
//      故这 10 个批次恰好凑成一轮（覆盖「多个 MDS 批次累计成一轮下载」）；
//   ② 整卷单批次：基线下发形态；
//   ③ 跨卷边界批次：一个批次的文件分属两张卷镜像；
//   ④ 大小混合批次：单文件批次与跨卷大批次混排。
// ---------------------------------------------------------------------------

struct BatchSpec {
    std::string shape;              // 批次形态名（写入日志与测试点标题）
    std::vector<uint64_t> inodes;   // 本批次下发的文件（按全局顺序）
};

std::vector<BatchSpec> BuildBatchSchedule(const std::vector<PackPlanGroup>& plan) {
    std::vector<uint64_t> flat;
    for (const PackPlanGroup& group : plan) {
        flat.insert(flat.end(), group.inodes.begin(), group.inodes.end());
    }

    std::vector<BatchSpec> batches;
    size_t cursor = 0;
    const auto take = [&](size_t count, const char* shape) {
        BatchSpec spec;
        spec.shape = shape;
        const size_t end = std::min(cursor + count, flat.size());
        for (; cursor < end; ++cursor) {
            spec.inodes.push_back(flat[cursor]);
        }
        batches.push_back(std::move(spec));
    };

    // 卷 1：逐文件小批次。
    for (size_t i = 0; i < kFilesPerVolume; ++i) {
        take(1, "逐文件小批次");
    }
    // 卷 2：整卷单批次。
    take(kFilesPerVolume, "整卷单批次");
    // 卷 3~4：7 / 7 / 6 切分，中间那个批次跨越卷边界。
    take(7, "跨卷边界批次");
    take(7, "跨卷边界批次");
    take(6, "跨卷边界批次");
    // 卷 5~7：1 / 14 / 2 / 13 大小混合（含单文件批次与跨越卷边界的大批次）。
    take(1, "大小混合批次");
    take(14, "大小混合批次");
    take(2, "大小混合批次");
    take(13, "大小混合批次");
    // 其余卷：整卷单批次（基线形态，保证全量文件都被下发）。
    while (cursor < flat.size()) {
        take(kFilesPerVolume, "整卷单批次");
    }
    return batches;
}

// 该卷的文件是否已全部下发（可据此判定其打包汇报何时必然出现）。
bool AllInodesSent(const PackPlanGroup& group, const std::set<uint64_t>& sent) {
    for (uint64_t inode_id : group.inodes) {
        if (sent.count(inode_id) == 0) {
            return false;
        }
    }
    return true;
}

// 卷的批次构成描述：单批次给出形态名；多批次累计给出批次数与形态集合。
std::string VolumeShapeText(size_t volume_index,
                            const std::vector<BatchSpec>& batches,
                            const std::vector<std::set<size_t>>& batches_of_volume) {
    const std::set<size_t>& ids = batches_of_volume[volume_index];
    if (ids.size() == 1) {
        return batches[*ids.begin()].shape;
    }
    std::set<std::string> shapes;
    for (size_t index : ids) {
        shapes.insert(batches[index].shape);
    }
    std::string joined;
    for (const std::string& shape : shapes) {
        joined += (joined.empty() ? std::string() : " + ") + shape;
    }
    return std::to_string(ids.size()) + " 个批次累计（" + joined + "）";
}

// 卷序号集合 → "1,2"（1 起编号，便于与日志中的"第 N 卷"对齐）。
std::string JoinVolumes(const std::set<size_t>& volumes) {
    std::string text;
    for (size_t volume : volumes) {
        text += (text.empty() ? std::string() : ",") + std::to_string(volume + 1);
    }
    return text;
}

// 一次读的结果。
struct ReadOutcome {
    bool ok{false};
    double elapsed_ms{0.0};
    uint64_t task_id{0};
    std::string data;
    std::string error;
};

// 读某卷下某个 inode 的全部分片，并与语料原始字节比对。
ReadOutcome ReadInode(FakeReadClient* client,
                      const Corpus& corpus,
                      const Env& env,
                      uint64_t volume_id,
                      uint64_t inode_id,
                      int ready_timeout_ms) {
    ReadOutcome outcome;
    const CorpusFile* file = corpus.Find(inode_id);
    if (file == nullptr) {
        outcome.error = "语料中不存在 inode=" + std::to_string(inode_id);
        return outcome;
    }

    zb::rpc::MdsStatusCode status = zb::rpc::MDS_INTERNAL_ERROR;
    std::string err;
    const std::string image_id = "img-" + std::to_string(volume_id);
    if (!client->RequestRead("optical-disk-1", image_id, inode_id, &outcome.task_id, &status, &err)) {
        outcome.error = "RequestAsyncReadFile 传输失败: " + err;
        return outcome;
    }
    if (status != zb::rpc::MDS_OK || outcome.task_id == 0) {
        outcome.error = "RequestAsyncReadFile 未受理, status=" + std::to_string(static_cast<int>(status)) +
                        " message=" + err;
        return outcome;
    }

    const auto begin = std::chrono::steady_clock::now();
    std::string data;
    // 首片轮询：镜像装载期间返回 MDS_INTERNAL_ERROR（TASK_NOT_FINISH 的映射），
    // 因此这里把非 MDS_OK 一律视作"尚未就绪"。
    const bool ready = WaitFor(
        [&] {
            data.clear();
            std::string read_err;
            if (!client->ReadByTask(outcome.task_id, 0, kReadShardBytes, &data, &status, &read_err)) {
                return false;
            }
            return status == zb::rpc::MDS_OK;
        },
        ready_timeout_ms,
        "读任务就绪（inode=" + std::to_string(inode_id) + "，镜像已从光盘库装载完成）", env.dirs());
    if (!ready) {
        outcome.error = "读任务未就绪";
        return outcome;
    }

    for (uint64_t offset = kReadShardBytes; offset < file->file_size; offset += kReadShardBytes) {
        const uint64_t read_size = std::min(kReadShardBytes, file->file_size - offset);
        std::string piece;
        std::string read_err;
        if (!client->ReadByTask(outcome.task_id, offset, read_size, &piece, &status, &read_err)) {
            outcome.error = "分片读传输失败 offset=" + std::to_string(offset) + ": " + read_err;
            return outcome;
        }
        if (status != zb::rpc::MDS_OK) {
            outcome.error = "分片读失败 offset=" + std::to_string(offset) +
                            " status=" + std::to_string(static_cast<int>(status)) + " message=" + read_err;
            return outcome;
        }
        data += piece;
    }

    outcome.elapsed_ms = std::chrono::duration<double, std::milli>(
                             std::chrono::steady_clock::now() - begin).count();
    outcome.data = std::move(data);

    std::string expected;
    if (!corpus.ReadFileContent(inode_id, &expected)) {
        outcome.error = "读取语料原始数据失败 inode=" + std::to_string(inode_id);
        return outcome;
    }
    if (outcome.data != expected) {
        outcome.error = "读回数据与语料不一致 inode=" + std::to_string(inode_id) +
                        " actual_size=" + std::to_string(outcome.data.size()) +
                        " expected_size=" + std::to_string(expected.size());
        return outcome;
    }
    outcome.ok = true;
    return outcome;
}

// 末片读完后任务应进入 FINISH：inode→task 索引被清除，故 ReadObjectByInodeId 返回 MDS_NOT_FOUND。
void ExpectReadFinished(FakeReadClient* client, uint64_t inode_id) {
    std::string extra;
    zb::rpc::MdsStatusCode status = zb::rpc::MDS_INTERNAL_ERROR;
    std::string err;
    client->ReadByInode(inode_id, 0, 4096, &extra, &status, &err);
    ExpectWithDetail(status == zb::rpc::MDS_NOT_FOUND,
                     "末片读完后任务进入 FINISH（ReadObjectByInodeId 返回 MDS_NOT_FOUND，inode=" +
                         std::to_string(inode_id) + "）",
                     "实际 status=" + std::to_string(static_cast<int>(status)));
}

// 打包汇报行 → 卷镜像 id 与 inode 集合。
struct PackedVolume {
    uint64_t volume_id{0};
    std::vector<uint64_t> inodes;
};

// 刻录汇报汇总（测试点 A7）：已刻录光盘与其覆盖的镜像。
struct DiscOutcome {
    size_t disc_count{0};                            // 已刻录光盘数
    size_t burned_count{0};                          // 已刻录镜像数（= 已打包卷的前缀长度）
    std::vector<BurnReport> discs;                   // 每张盘的汇报
    std::map<uint64_t, uint64_t> volume_to_disc;     // volume_id → disc_id
};

// 下发一个归档批次（每个文件带上分片信息，供节点去 real_node 下载）。
void SendArchiveBatch(FakeMdsDriver* mds,
                      const Corpus& corpus,
                      const BatchSpec& batch,
                      uint64_t batch_id) {
    std::vector<zb::rpc::ArchiveFile> files;
    files.reserve(batch.inodes.size());
    for (uint64_t inode_id : batch.inodes) {
        const CorpusFile* file = corpus.Find(inode_id);
        Require(file != nullptr, "语料中存在 inode=" + std::to_string(inode_id));
        zb::rpc::ArchiveFile archive;
        archive.set_inode_id(file->inode_id);
        archive.set_size(file->file_size);
        archive.set_object_unit_size(file->object_unit_size);
        archive.set_target_node_id(file->node_id);
        archive.set_target_disk_id(file->disk_id);
        files.push_back(std::move(archive));
    }
    std::string err;
    const bool sent = mds->SendBatch(batch_id, files, &err);
    Require(sent, "SendArchiveMetadata 受理批次 batch_id=" + std::to_string(batch_id) +
                      (sent ? std::string() : "；失败详情: " + err));
}

// 等某一卷的打包汇报 → 校验汇报 → 等该卷镜像出现在 image_dir。
// 全过程归入一个测试点实例（case_id / case_title 由调用方按卷序号与批次形态生成）。
PackedVolume AwaitPackedVolume(StdoutCapture* capture,
                               size_t* report_cursor,
                               const Env& env,
                               const PackPlanGroup& group,
                               const std::string& case_id,
                               const std::string& case_title,
                               std::set<uint64_t>* seen_volume_ids) {
    PackedVolume packed;
    CaseScope scope(case_id, case_title, kCaseA1Objective, kCaseA1Expectation, kCaseA1Method);

    // 1. 等本次打包的汇报行（控制台替代 MDS ReportFilesPackedToImage）。
    Step("等待节点打印打包汇报行（控制台替代 MDS ReportFilesPackedToImage）");
    std::string line;
    const bool reported = WaitFor(
        [&] {
            capture->NextLineWithPrefix(report_cursor, kPackReportPrefix, &line);
            return !line.empty();
        },
        kPackReportTimeoutMs,
        "打包汇报行出现（预测 volume_index=" + std::to_string(group.volume_index) + "）", env.dirs());
    Require(reported, "打包汇报行已出现");
    PackReport report;
    const bool parsed = ParsePackReport(line, &report);
    Require(parsed, "打包汇报行可解析（" + line + "）");
    packed.volume_id = report.image_id;
    packed.inodes = report.inodes;

    ExpectEqualU64(report.count, group.inodes.size(), "打包汇报 count 与预测分组一致");
    ExpectWithDetail(SameInodeSet(report.inodes, group.inodes),
                     "打包汇报 inode 集合与预测分组一致",
                     "actual=" + JoinInodes(report.inodes) + " expected=" + JoinInodes(group.inodes));

    // 2. 封装出的写镜像出现在 image_dir：封盘刻录前所有待打包镜像都留在缓存目录，
    //    只有整张盘刻录完成时才会被逐个释放（见测试点 A7）。
    const uint64_t volume_id = packed.volume_id;
    Expect(seen_volume_ids->insert(volume_id).second,
           "卷镜像 id 全局唯一（本次 volume_id=" + std::to_string(volume_id) + "）");
    Step("轮询 image_dir 出现写镜像 volume_" + std::to_string(volume_id) + ".vimg");
    WaitFor([&] { return FileExists(ImagePath(env, volume_id)); }, kBurnTimeoutMs,
            "image_dir 出现写镜像 volume_" + std::to_string(volume_id), env.dirs());

    uint64_t image_size = 0;
    const bool sized = GetFileSize(ImagePath(env, volume_id), &image_size);
    ExpectWithDetail(sized && image_size > 0 && image_size <= kVolumeSizeBytes,
                     "卷镜像大小落在 (0, 卷大小] 内",
                     "stat 成功=" + std::string(sized ? "true" : "false") +
                         " size=" + std::to_string(image_size));
    return packed;
}

// 收尾断言：归档中间产物已清理，日志目录存在（task_done 日志因 600s 清理间隔在测试期内不生成）。
void ExpectCleanEndState(const Env& env) {
    const std::vector<std::string> archive_left = ListDirBySuffix(env.input_dir, ".archive");
    ExpectWithDetail(archive_left.empty(), "input/ 无残留归档中间文件",
                     "残留 " + std::to_string(archive_left.size()) + " 个 .archive 文件");
    ExpectWithDetail(FileExists(env.log_dir), "log/ 目录存在", env.log_dir + " 不存在");
    const std::vector<std::string> task_done = ListDirBySuffix(env.log_dir, ".log");
    if (!task_done.empty()) {
        std::cerr << "  [INFO] log/ 下已生成任务日志: " << task_done.size() << " 个" << std::endl;
    } else {
        std::cerr << "  [TODO] task_done_yyyymmdd.log 未生成：清理线程间隔为 600s，"
                     "测试期内不会触发（已知缺口，见 README「已知缺口」）" << std::endl;
    }
}

// 语料统计：已归档文件数与总字节数（由预测分组推算）。
void CorpusStats(const Corpus& corpus, size_t* files_out, uint64_t* bytes_out) {
    size_t files = 0;
    uint64_t bytes = 0;
    for (const PackPlanGroup& group : corpus.plan()) {
        for (uint64_t inode_id : group.inodes) {
            const CorpusFile* file = corpus.Find(inode_id);
            if (file == nullptr) {
                continue;
            }
            ++files;
            bytes += file->file_size;
        }
    }
    *files_out = files;
    *bytes_out = bytes;
}

std::string JoinNames(const std::vector<std::string>& names) {
    std::ostringstream os;
    for (size_t i = 0; i < names.size(); ++i) {
        if (i > 0) {
            os << ",";
        }
        os << names[i];
    }
    return os.str();
}

// ---------------------------------------------------------------------------
// 背压观测（测试点 B2 的证据来源）
//
// 被测节点把归档下载背压快照追加在 GetStatusDetail() 文本末尾，形如：
//   "... backpressure=(throttled=true write_images=30/30 input_pending=0/1048576)"
// 测试只需解析这几个数，即可判定「为什么暂停 / 是否恢复」，不必靠最终文件数反推。
// 注意：是否过载由这两个数值与各自阈值的比较决定（与节点侧判定口径一致）。
// ---------------------------------------------------------------------------

struct BackpressureSample {
    bool throttled{false};
    uint64_t write_images{0};
    uint64_t write_image_limit{0};
    uint64_t input_pending_bytes{0};
    uint64_t input_pending_limit{0};
    bool write_overloaded{false};
    bool input_overloaded{false};

    std::string Describe() const {
        std::ostringstream os;
        os << "throttled=" << (throttled ? "true" : "false")
           << " write_images=" << write_images << "/" << write_image_limit
           << " input_pending=" << input_pending_bytes << "/" << input_pending_limit;
        return os.str();
    }
};

// 解析 "key=A/B" 形式的数值对（如 write_images=30/30）；缺失或格式不符返回 false。
bool ParseSlashPair(const std::string& text,
                    const std::string& key,
                    uint64_t* first,
                    uint64_t* second) {
    const size_t pos = text.find(key);
    if (pos == std::string::npos) {
        return false;
    }
    const char* begin = text.c_str() + pos + key.size();
    char* end = nullptr;
    *first = ::strtoull(begin, &end, 10);
    if (end == nullptr || *end != '/') {
        return false;
    }
    *second = ::strtoull(end + 1, nullptr, 10);
    return true;
}

bool ParseBackpressureSample(const std::string& detail, BackpressureSample* out) {
    if (out == nullptr) {
        return false;
    }
    const size_t pos = detail.find("backpressure=(throttled=");
    if (pos == std::string::npos) {
        return false;
    }
    const std::string tail = detail.substr(pos);
    out->throttled = tail.find("throttled=true") != std::string::npos;
    if (!ParseSlashPair(tail, "write_images=", &out->write_images, &out->write_image_limit) ||
        !ParseSlashPair(tail, "input_pending=", &out->input_pending_bytes,
                        &out->input_pending_limit)) {
        return false;
    }
    out->write_overloaded =
        out->write_image_limit > 0 && out->write_images >= out->write_image_limit;
    out->input_overloaded =
        out->input_pending_limit > 0 && out->input_pending_bytes >= out->input_pending_limit;
    return true;
}

// 采样器：写阶段逐批次调用，累计「是否触发过 / 峰值 / 最近一次」。
class BackpressureWatch {
public:
    BackpressureWatch(OpticalStorageServiceImpl* service, const Profile& profile)
        : service_(service), profile_(profile) {}

    void Sample() {
        if (service_ == nullptr) {
            return;
        }
        BackpressureSample sample;
        if (!ParseBackpressureSample(service_->GetArchiveStatusDetail(), &sample)) {
            return;
        }
        ++sample_count_;
        last_ = sample;
        peak_write_images_ = std::max(peak_write_images_, sample.write_images);
        peak_input_pending_ = std::max(peak_input_pending_, sample.input_pending_bytes);
        if (sample.throttled) {
            saw_throttled_ = true;
        }
        if (sample.write_overloaded) {
            saw_write_overloaded_ = true;
        }
        if (sample.input_overloaded) {
            saw_input_overloaded_ = true;
        }
    }

    size_t sample_count() const { return sample_count_; }
    const BackpressureSample& last() const { return last_; }
    uint64_t peak_write_images() const { return peak_write_images_; }
    bool saw_throttled() const { return saw_throttled_; }
    bool saw_write_overloaded() const { return saw_write_overloaded_; }
    bool saw_input_overloaded() const { return saw_input_overloaded_; }

    std::string Describe() const {
        std::ostringstream os;
        os << "采样 " << sample_count_ << " 次；最近一次[" << last_.Describe() << "]；峰值 write_images="
           << peak_write_images_ << "/" << profile_.max_write_images << "；峰值 input_pending="
           << peak_input_pending_ << " 字节；曾出现 throttled/写镜像过载/原始积压过载="
           << (saw_throttled_ ? "Y" : "N") << "/" << (saw_write_overloaded_ ? "Y" : "N") << "/"
           << (saw_input_overloaded_ ? "Y" : "N");
        return os.str();
    }

private:
    OpticalStorageServiceImpl* service_{nullptr};
    Profile profile_;
    BackpressureSample last_;
    size_t sample_count_{0};
    uint64_t peak_write_images_{0};
    uint64_t peak_input_pending_{0};
    bool saw_throttled_{false};
    bool saw_write_overloaded_{false};
    bool saw_input_overloaded_{false};
};

int RunScenario(const Options& options) {
    const auto run_begin = std::chrono::steady_clock::now();
    const std::string started_at = NowString();
    const Profile profile = ProfileFor(options.scenario);

    Corpus corpus;
    std::string error;
    Require(corpus.Load(options.corpus_dir, &error),
            "语料载入成功（" + options.corpus_dir + "）" +
                (error.empty() ? std::string() : "；失败详情: " + error));

    const Env env = MakeEnv(options.work_dir);
    const bool dir_created = ::mkdir(options.work_dir.c_str(), 0755) == 0 || errno == EEXIST;
    Require(dir_created, "工作目录可创建（" + options.work_dir + "）");

    size_t corpus_files = 0;
    uint64_t corpus_bytes = 0;
    CorpusStats(corpus, &corpus_files, &corpus_bytes);

    // 运行头部：让日志本身可自解释（含场景、路径、被测参数与超时设置）。
    std::cerr << std::string(96, '=') << std::endl;
    std::cerr << "optical_node 归档集成测试：scenario=" << options.scenario << "，档位="
              << profile.name << std::endl;
    std::cerr << "开始时间: " << started_at << std::endl;
    std::cerr << "工作目录: " << options.work_dir << std::endl;
    std::cerr << "语料目录: " << options.corpus_dir << "（" << corpus.plan().size() << " 卷 / "
              << corpus_files << " 文件 / " << corpus_bytes << " 字节）" << std::endl;
    std::cerr << "被测参数: 卷镜像=" << kVolumeSizeBytes << " 字节; 打包阈值=" << kSizeThreshold
              << "; image_dir 容量=" << profile.capacity_in_images
              << "; 最大写镜像数(背压上限)=" << profile.max_write_images
              << "; available_volume_id_count=" << static_cast<int>(kAvailableVolumeIdCount)
              << "; 光盘容量=" << kDiscCapacityBytes
              << " 字节; 单盘标准镜像数=" << kStandardImagesPerDisc
              << "; 对象分片=" << kObjectUnitBytes << " 字节/片（每文件 " << kShardsPerFile << " 片）"
              << "; 读分片=" << kReadShardBytes << " 字节" << std::endl;
    std::cerr << "超时设置: 打包汇报=" << kPackReportTimeoutMs << "ms; 刻录=" << kBurnTimeoutMs
              << "ms; CD_READ=" << kCdReadTimeoutMs << "ms; 缓存命中=" << kCacheHitTimeoutMs
              << "ms; 背压=" << kBackpressureTimeoutMs
              << "ms（CD 仿真时长不可压缩，故超时留足余量）" << std::endl;
    std::cerr << std::string(96, '=') << std::endl;

    // 1. 假 real_node（提供归档文件分片数据）。
    FakeRealNodeService real_node(&corpus);
    LocalServer real_node_server(&real_node);

    // 2. 假 scheduler（GetClusterView 指向假 real_node）。
    FakeSchedulerService scheduler;
    scheduler.node_address = real_node_server.Address();
    LocalServer scheduler_server(&scheduler);

    // 3. 进程内真光节点（构造即 Run；不链接含 main 的 optical_node_server.cpp）。
    OpticalNodeConfig config;
    config.node_id = "optical-1";
    config.node_address = "127.0.0.1:0";
    config.scheduler_addr = scheduler_server.Address();
    config.archive_root = env.archive_root;
    config.volume_size_bytes = kVolumeSizeBytes;
    config.size_threshold = kSizeThreshold;
    config.capacity_in_images = profile.capacity_in_images;
    config.available_volume_id_count = kAvailableVolumeIdCount;
    config.max_write_images = profile.max_write_images;
    config.disc_capacity_bytes = kDiscCapacityBytes;
    config.standard_images_per_disc = kStandardImagesPerDisc;
    OpticalStorageServiceImpl service(config);
    BrpcOpticalNodeService node_service(&service);
    LocalServer node_server(&node_service);

    // 4. 假 MDS / 假 client（brpc 客户端）。
    FakeMdsDriver mds(node_server.Address());
    FakeReadClient client(node_server.Address());

    // 测试点 A0：环境与配置就绪。
    {
        CaseScope scope("A0", "环境与配置就绪", kCaseA0Objective, kCaseA0Expectation, kCaseA0Method);
        Step("光节点=" + node_server.Address() + "；假 real_node=" + real_node_server.Address() +
             "；假 scheduler=" + scheduler_server.Address() + "；假 MDS/client 已连到光节点");
        ExpectWithDetail(!corpus.plan().empty(),
                         "语料预测打包分组非空（expected_pack_plan.tsv 已就绪）",
                         "plan().size()=" + std::to_string(corpus.plan().size()));
        ExpectWithDetail(kStandardImagesPerDisc < profile.max_write_images &&
                             profile.max_write_images < profile.capacity_in_images,
                         "档位参数满足不变式：单盘镜像数 < 背压上限 < image_dir 容量",
                         "单盘镜像数=" + std::to_string(kStandardImagesPerDisc) + " 背压上限=" +
                             std::to_string(profile.max_write_images) + " 容量=" +
                             std::to_string(profile.capacity_in_images));
        const bool ready = service.IsArchiveEngineReady();
        ExpectWithDetail(ready, "被测光节点 Run() 成功且状态为 RUNNING",
                         service.GetArchiveStatusDetail());
        Require(ready, "被测光节点处于 RUNNING 状态（后续全部步骤依赖它）");
        Expect(!node_server.Address().empty() && !real_node_server.Address().empty() &&
                   !scheduler_server.Address().empty(),
               "三个 brpc server 均已监听（光节点 / 假 real_node / 假 scheduler）");
        Expect(FileExists(env.input_dir) && FileExists(env.temp_dir) && FileExists(env.image_dir) &&
                   FileExists(env.read_dir) && FileExists(env.disc_sim_dir) && FileExists(env.meta_dir) &&
                   FileExists(env.log_dir),
               "archive 目录及 input/temp/image/read/disc_sim/meta/log 子目录均已创建");
    }

    StdoutCapture capture;
    size_t report_cursor = 0;

    // 5. 写链路：按批次形态下发全部文件（批次边界故意与卷边界错开，见 BuildBatchSchedule）。
    //    全部文件都要跑完，否则不会触发封印与刻录。
    const size_t volume_total = corpus.plan().size();
    Require(volume_total > 0, "待归档卷数 > 0");
    bool plan_uniform = true;
    for (const PackPlanGroup& group : corpus.plan()) {
        if (group.inodes.size() != kFilesPerVolume) {
            plan_uniform = false;
            break;
        }
    }
    Require(volume_total >= 7 && plan_uniform,
            "语料预测分组为 ≥7 卷且每卷 " + std::to_string(kFilesPerVolume) +
                " 个文件（批次形态覆盖用例的前置条件）");

    const std::vector<BatchSpec> batches = BuildBatchSchedule(corpus.plan());
    std::vector<std::set<size_t>> batches_of_volume(volume_total);
    std::vector<std::set<size_t>> volumes_of_batch(batches.size());
    {
        std::map<uint64_t, size_t> volume_of_inode;
        for (size_t v = 0; v < volume_total; ++v) {
            for (uint64_t inode_id : corpus.plan()[v].inodes) {
                volume_of_inode[inode_id] = v;
            }
        }
        for (size_t b = 0; b < batches.size(); ++b) {
            for (uint64_t inode_id : batches[b].inodes) {
                const size_t volume_index = volume_of_inode.at(inode_id);
                volumes_of_batch[b].insert(volume_index);
                batches_of_volume[volume_index].insert(b);
            }
        }
    }

    std::vector<PackedVolume> packed_volumes;
    std::set<uint64_t> volume_ids;
    std::set<uint64_t> sent_inodes;
    size_t peak_write_images = 0;
    size_t plan_cursor = 0;
    BackpressureWatch watch(&service, profile);
    for (size_t b = 0; b < batches.size(); ++b) {
        Step("假 MDS 下发第 " + std::to_string(b + 1) + "/" + std::to_string(batches.size()) +
             " 个批次（形态=" + batches[b].shape + "，文件数=" +
             std::to_string(batches[b].inodes.size()) + "，覆盖卷 " +
             JoinVolumes(volumes_of_batch[b]) + "）");
        SendArchiveBatch(&mds, corpus, batches[b], static_cast<uint64_t>(b + 1));
        for (uint64_t inode_id : batches[b].inodes) {
            sent_inodes.insert(inode_id);
        }

        // 本批次下发后，凡「文件已全部下发」的卷，其打包汇报必然已经（或将很快）出现；
        // 逐个按预测分组校验，从而同时覆盖「批次跨卷」「多批次累计成卷」两种切分。
        while (plan_cursor < volume_total &&
               AllInodesSent(corpus.plan()[plan_cursor], sent_inodes)) {
            const std::string index_text =
                std::to_string(plan_cursor + 1) + "/" + std::to_string(volume_total);
            packed_volumes.push_back(AwaitPackedVolume(
                &capture, &report_cursor, env, corpus.plan()[plan_cursor],
                "A1." + std::to_string(plan_cursor + 1),
                "第 " + index_text + " 卷：批次形态=" +
                    VolumeShapeText(plan_cursor, batches, batches_of_volume) +
                    " → 归档下发 → 压缩封装 → 打包汇报",
                &volume_ids));
            ++plan_cursor;
        }

        // 背压观测：写阶段 image_dir 里只有写镜像，逐批次采样（快照 + 占用峰值）。
        // 节点侧按 MAX_WRITE_IMAGES 暂停下载，因此不会一路堆到硬上限而触发 VOLUME_FULL。
        // 注意这里无需测试主动节流——节流由被测节点自己完成，本用例只做观测。
        peak_write_images = std::max(peak_write_images, ImageFiles(env).size());
        watch.Sample();
    }
    Require(plan_cursor == volume_total,
            "全部卷的打包汇报均已校验（已校验 " + std::to_string(plan_cursor) + "/" +
                std::to_string(volume_total) + " 卷）");

    // 测试点 B1：批次形态覆盖与文件守恒。
    {
        CaseScope scope("B1", "批次形态覆盖（下发切分不改变卷分组）", kCaseB1Objective,
                        kCaseB1Expectation, kCaseB1Method);
        std::map<std::string, size_t> shape_counts;
        size_t spanning_batches = 0;
        size_t single_file_batches = 0;
        for (size_t b = 0; b < batches.size(); ++b) {
            ++shape_counts[batches[b].shape];
            if (volumes_of_batch[b].size() > 1) {
                ++spanning_batches;
            }
            if (batches[b].inodes.size() == 1) {
                ++single_file_batches;
            }
        }
        std::string shape_text;
        for (const auto& item : shape_counts) {
            shape_text += (shape_text.empty() ? std::string() : "，") + item.first + "×" +
                          std::to_string(item.second);
        }
        Step("批次形态分布：" + shape_text + "；批次总数=" + std::to_string(batches.size()) +
             "（单文件批次=" + std::to_string(single_file_batches) + "，跨越卷边界的批次=" +
             std::to_string(spanning_batches) + "）");
        const std::vector<std::string> expected_shapes = {"逐文件小批次", "整卷单批次",
                                                          "跨卷边界批次", "大小混合批次"};
        std::vector<std::string> missing_shapes;
        for (const std::string& shape : expected_shapes) {
            if (shape_counts.count(shape) == 0) {
                missing_shapes.push_back(shape);
            }
        }
        ExpectWithDetail(missing_shapes.empty(),
                         "四种批次形态均被覆盖（逐文件小批次 / 整卷单批次 / 跨卷边界批次 / 大小混合批次）",
                         "缺失形态=" + JoinNames(missing_shapes));
        Expect(single_file_batches >= kFilesPerVolume,
               "单文件批次至少 " + std::to_string(kFilesPerVolume) +
                   " 个（覆盖多个 MDS 批次累计成一轮下载）");
        Expect(spanning_batches >= 2,
               "存在跨越卷边界的批次（覆盖一个批次的文件分属两张卷镜像）");
        ExpectWithDetail(batches_of_volume[0].size() == kFilesPerVolume,
                         "第 1 卷由 " + std::to_string(kFilesPerVolume) +
                             " 个单文件批次累计而成（小批次未各自成卷）",
                         "实际批次数=" + std::to_string(batches_of_volume[0].size()));

        // 文件守恒：全部卷镜像覆盖的 inode 恰好等于下发的 inode 集合（无遗漏、无重复）。
        std::map<uint64_t, size_t> inode_hits;
        for (const PackedVolume& packed : packed_volumes) {
            for (uint64_t inode_id : packed.inodes) {
                ++inode_hits[inode_id];
            }
        }
        size_t duplicated = 0;
        size_t missing = 0;
        for (const auto& item : inode_hits) {
            if (item.second > 1) {
                ++duplicated;
            }
        }
        for (uint64_t inode_id : sent_inodes) {
            if (inode_hits.count(inode_id) == 0) {
                ++missing;
            }
        }
        ExpectEqualU64(static_cast<uint64_t>(packed_volumes.size()),
                       static_cast<uint64_t>(volume_total), "打包汇报数与预测卷数一致");
        ExpectWithDetail(duplicated == 0 && missing == 0,
                         "批次切分未造成文件遗漏或重复（每个文件恰好归属一个卷镜像）",
                         "重复=" + std::to_string(duplicated) + " 遗漏=" + std::to_string(missing));
    }

    // 测试点 B2：下载背压的触发与恢复（背压上限低于硬上限的档位才有意义）。
    if (profile.check_backpressure) {
        CaseScope scope("B2", "背压触发与恢复（写镜像达上限 → 暂停下载 → 刻录释放唤醒）",
                        kCaseB2Objective, kCaseB2Expectation, kCaseB2Method);
        Step("写阶段背压采样：" + watch.Describe());
        ExpectWithDetail(watch.saw_throttled(),
                         "下载确因负载过载被暂停过（采样到 throttled=true）", watch.Describe());
        ExpectWithDetail(watch.peak_write_images() >= profile.max_write_images,
                         "写镜像数确实堆到过背压上限：" + std::to_string(profile.max_write_images),
                         watch.Describe());
        ExpectWithDetail(watch.saw_write_overloaded(),
                         "触发原因覆盖「写镜像数达上限」",
                         watch.Describe());
        ExpectWithDetail(watch.peak_write_images() <= profile.max_write_images + 1,
                         "过冲不超过 1 个卷镜像（背压判定在「下一个文件之前」）",
                         watch.Describe());
        Step("背压原因观测：写镜像过载=" + std::string(watch.saw_write_overloaded() ? "有" : "无") +
             "，原始文件积压过载=" + std::string(watch.saw_input_overloaded() ? "有" : "无") +
             "（阈值=" + std::to_string(kVolumeSizeBytes / 10) + " 字节 = 卷镜像容量 10%）");

        Step("等待背压解除（依赖刻录完成释放写镜像后的唤醒，未唤醒即睡死）");
        const bool recovered = WaitFor(
            [&] {
                watch.Sample();
                return watch.sample_count() > 0 && !watch.last().throttled;
            },
            kBackpressureTimeoutMs, "背压解除（快照 throttled=false，写镜像占用回落）", env.dirs());
        ExpectWithDetail(recovered,
                         "背压已解除且下载线程被唤醒（最近一次快照 write_images=" +
                             std::to_string(watch.last().write_images) + "）",
                         watch.Describe());
    }

    // 测试点 A7：光盘封印与刻录（跨卷汇总）。
    // 封印规则：镜像按打包顺序积攒，当「下一个装不下」时才把当前集合封成一张光盘；
    // 因此「已刻录」的镜像必然是被打包卷的一个前缀，其余为未装满的尾盘（继续留在 image_dir）。
    DiscOutcome disc_outcome;
    {
        CaseScope scope("A7", "光盘封印与刻录（跨卷汇总）", kCaseA7Objective, kCaseA7Expectation,
                        kCaseA7Method);

        // 背压生效观测：写阶段逐批次采样的占用峰值必须低于 image_dir 硬上限。
        // 若背压失效，占用会一路堆到 capacity，随后 MoveFrom(WRITE) 因全是 WRITE、
        // 无可淘汰项而返回 VOLUME_FULL_NO_READABLE —— 本项与 A1 全卷通过互为证据。
        Step("写阶段 image_dir 占用峰值=" + std::to_string(peak_write_images) +
             "（背压上限 " + std::to_string(profile.max_write_images) + "，硬上限 " +
             std::to_string(profile.capacity_in_images) + "）");
        ExpectWithDetail(peak_write_images < profile.capacity_in_images,
                         "背压生效：写阶段占用始终低于 image_dir 硬上限（未触发 VOLUME_FULL）",
                         "峰值=" + std::to_string(peak_write_images) + " 硬上限=" +
                             std::to_string(profile.capacity_in_images));

        // 封印时即生成 meta/disc_<id>_meta，故「已封印盘数」是确定性的；等刻录汇报行数追平它，
        // 说明所有已封印的盘都已完成刻录（无需固定 sleep）。
        Step("等待刻录汇报行（控制台替代 MDS ReportImagesBurnedToDisc）：已封印盘数=" +
             std::to_string(SealedDiscMetaCount(env)));
        const bool burned = WaitFor(
            [&] {
                const size_t sealed = SealedDiscMetaCount(env);
                return sealed > 0 && capture.CountLinesWithPrefix(kBurnReportPrefix) >= sealed;
            },
            kBurnTimeoutMs, "所有已封印光盘的刻录汇报行到齐", env.dirs());
        Require(burned, "至少封印并刻录了 1 张光盘（否则后续 CD_READ 重载无法验证）");

        const std::vector<std::string> burn_lines = capture.CollectLinesWithPrefix(kBurnReportPrefix);
        for (const std::string& line : burn_lines) {
            BurnReport report;
            Require(ParseBurnReport(line, &report), "刻录汇报行可解析（" + line + "）");
            ExpectWithDetail(report.count == report.image_ids.size(),
                             "刻录汇报 count 与 image_ids 数量一致（disc_id=" +
                                 std::to_string(report.disc_id) + "）",
                             "count=" + std::to_string(report.count) + " image_ids=" +
                                 std::to_string(report.image_ids.size()));
            disc_outcome.discs.push_back(report);
            for (uint64_t image_id : report.image_ids) {
                disc_outcome.volume_to_disc[image_id] = report.disc_id;
            }
        }
        disc_outcome.disc_count = disc_outcome.discs.size();
        disc_outcome.burned_count = disc_outcome.volume_to_disc.size();
        Step("刻录汇报汇总：" + std::to_string(disc_outcome.disc_count) + " 张盘 / " +
             std::to_string(disc_outcome.burned_count) + " 个镜像（已打包卷数=" +
             std::to_string(packed_volumes.size()) + "）");

        ExpectEqualU64(static_cast<uint64_t>(SealedDiscMetaCount(env)), disc_outcome.disc_count,
                       "已封印盘数等于刻录汇报的盘数");
        Expect(disc_outcome.burned_count > 0, "至少 1 个镜像已刻录到光盘");
        Expect(disc_outcome.burned_count < packed_volumes.size(),
               "存在未装满的尾盘（刻录只覆盖被封印的盘，尾盘继续留在 pending）");

        // 1. 每张盘都有对应的 vdisc 文件，且体积不超过盘容量。
        for (const BurnReport& report : disc_outcome.discs) {
            const std::string path = VdiscPath(env, report.disc_id);
            uint64_t size = 0;
            const bool sized = GetFileSize(path, &size);
            ExpectWithDetail(sized && size > 0 && size <= kDiscCapacityBytes,
                             "光盘文件存在且体积不超过盘容量（disc_" + std::to_string(report.disc_id) +
                                 ".vdisc）",
                             "stat 成功=" + std::string(sized ? "true" : "false") +
                                 " size=" + std::to_string(size));
        }
        ExpectEqualU64(static_cast<uint64_t>(DiscFiles(env).size()), disc_outcome.disc_count,
                       "disc_sim 中的 vdisc 数量等于已刻录光盘数");

        // 2. 刻录汇报的镜像恰为「按顺序打包的前 N 个卷」。
        bool prefix_ok = true;
        for (size_t i = 0; i < packed_volumes.size(); ++i) {
            const bool is_burned = disc_outcome.volume_to_disc.count(packed_volumes[i].volume_id) > 0;
            const bool should_burn = i < disc_outcome.burned_count;
            if (is_burned != should_burn) {
                prefix_ok = false;
                break;
            }
        }
        ExpectWithDetail(prefix_ok,
                         "刻录汇报的镜像恰为按顺序打包的前 " +
                             std::to_string(disc_outcome.burned_count) + " 个卷",
                         "已刻录集合=" + std::to_string(disc_outcome.burned_count) +
                             " 顺序不符或存在空洞");

        // 3. 已刻录镜像已从 image_dir 释放；未刻录的尾盘仍留在 image_dir（仍可读）。
        size_t released = 0;
        size_t pending_in_image_dir = 0;
        for (const PackedVolume& packed : packed_volumes) {
            const bool present = FileExists(ImagePath(env, packed.volume_id));
            const bool burned_image =
                disc_outcome.volume_to_disc.count(packed.volume_id) > 0;
            if (burned_image && !present) {
                ++released;
            }
            if (!burned_image && present) {
                ++pending_in_image_dir;
            }
        }
        ExpectEqualU64(static_cast<uint64_t>(released), disc_outcome.burned_count,
                       "已刻录镜像均已从 image_dir 释放（无残留）");
        ExpectEqualU64(static_cast<uint64_t>(pending_in_image_dir),
                       packed_volumes.size() - disc_outcome.burned_count,
                       "未装满尾盘的镜像仍留在 image_dir（未刻录、未释放）");
    }

    // 测试点 A2：下载完整性（非竞态可观测量）：每个文件 1 次 ResolveFileRead，每个对象 1 次 ReadObject。
    uint64_t expected_resolves = 0;
    uint64_t expected_reads = 0;
    for (const PackedVolume& packed : packed_volumes) {
        for (uint64_t inode_id : packed.inodes) {
            const CorpusFile* file = corpus.Find(inode_id);
            Require(file != nullptr, "语料中存在 inode=" + std::to_string(inode_id));
            ++expected_resolves;
            expected_reads += file->objects.size();
        }
    }
    {
        CaseScope scope("A2", "下载完整性与多分片重组（跨卷汇总计数）", kCaseA2Objective,
                        kCaseA2Expectation, kCaseA2Method);
        Step("假 real_node 计数器：ResolveFileRead=" +
             std::to_string(real_node.resolve_calls.load()) + "（期望 " +
             std::to_string(expected_resolves) + "）；ReadObject=" +
             std::to_string(real_node.read_calls.load()) + "（期望 " + std::to_string(expected_reads) +
             "）");
        ExpectEqualU64(static_cast<uint64_t>(real_node.resolve_calls.load()), expected_resolves,
                       "ResolveFileRead 调用次数等于已归档文件数");
        ExpectEqualU64(static_cast<uint64_t>(real_node.read_calls.load()), expected_reads,
                       "ReadObject 调用次数等于对象总数（文件数 × 每文件分片数）");

        // 多分片形态确认：1 MiB 文件按 256 KiB 切成 4 片，因此「ReadObject 次数 == 文件数 × 4」
        // 本身就是逐片取回、按绝对偏移重组的强证据（分片连续性由节点侧校验，不符会下载失败）。
        bool unit_consistent = true;
        size_t bad_shards = 0;
        for (const PackedVolume& packed : packed_volumes) {
            for (uint64_t inode_id : packed.inodes) {
                const CorpusFile* file = corpus.Find(inode_id);
                if (file == nullptr) {
                    continue;
                }
                if (file->object_unit_size != kObjectUnitBytes) {
                    unit_consistent = false;
                }
                if (file->objects.size() != kShardsPerFile) {
                    ++bad_shards;
                }
            }
        }
        Step("分片形态：对象分片大小=" + std::to_string(kObjectUnitBytes) + " 字节，每文件分片数期望=" +
             std::to_string(kShardsPerFile));
        ExpectWithDetail(unit_consistent, "所有已归档文件的对象分片大小均为 256 KiB（多分片覆盖生效）",
                         "存在非 256 KiB 分片的文件");
        ExpectEqualU64(static_cast<uint64_t>(bad_shards), 0,
                       "每个文件的分片数均为 4（1 MiB / 256 KiB）");
    }

    // 6. 读链路：镜像已被刻录（已从 image_dir 释放）⇒ 触发 CD_READ，
    // 按元数据记录的偏移从 vdisc 复制回 image_dir。
    // 只读已刻录卷的前若干个：把「满载后淘汰」留给测试点 A5（避免 A3 阶段就触发淘汰）。
    const size_t read_total =
        std::min<size_t>(disc_outcome.burned_count, profile.a3_read_volumes);
    Require(read_total > 0, "至少存在一个已刻录卷可供读回（CD_READ 前置条件）");
    for (size_t i = 0; i < read_total; ++i) {
        const PackedVolume& packed = packed_volumes[i];
        const uint64_t first_inode = packed.inodes.front();
        const std::string index_text = std::to_string(i + 1) + "/" + std::to_string(read_total);

        // 测试点 A3.<i>：数据面读 + CD_READ 重载。
        {
            CaseScope scope("A3." + std::to_string(i + 1),
                            "第 " + index_text + " 卷：数据面读 + CD_READ 重载", kCaseA3Objective,
                            kCaseA3Expectation, kCaseA3Method);
            Step("读 volume_" + std::to_string(packed.volume_id) + " inode=" +
                 std::to_string(first_inode) + "（镜像在光盘库，预期触发一次 CD_READ 装载）");
            const ReadOutcome outcome =
                ReadInode(&client, corpus, env, packed.volume_id, first_inode, kCdReadTimeoutMs);
            ExpectWithDetail(outcome.ok,
                             "读回数据与语料逐字节一致（inode=" + std::to_string(first_inode) +
                                 "，共 " + std::to_string(outcome.data.size()) + " 字节，耗时 " +
                                 std::to_string(static_cast<uint64_t>(outcome.elapsed_ms)) + "ms）",
                             outcome.error);
            ExpectReadFinished(&client, first_inode);
            Expect(FileExists(ImagePath(env, packed.volume_id)),
                   "CD_READ 后镜像回到 image_dir（volume_" + std::to_string(packed.volume_id) + "）");
            ExpectEqualU64(static_cast<uint64_t>(DiscFiles(env).size()), disc_outcome.disc_count,
                           "光盘文件持久保留（读回只复制区间，不搬移/删除 vdisc）");
            Expect(ImageFiles(env).size() <= profile.capacity_in_images,
                   "image_dir 镜像数不超过容量上限（当前 " +
                       std::to_string(ImageFiles(env).size()) + " / " +
                       std::to_string(profile.capacity_in_images) + "）");
        }

        // 测试点 A4.<i>：同卷另一 inode，镜像已在 image_dir，应为缓存命中（不触发第二次 CD_READ）。
        if (packed.inodes.size() >= 2) {
            CaseScope scope("A4." + std::to_string(i + 1),
                            "第 " + index_text + " 卷：缓存命中（镜像已在 image_dir，不应再次触发 CD_READ）",
                            kCaseA4Objective, kCaseA4Expectation, kCaseA4Method);
            const uint64_t second_inode = packed.inodes[1];
            const std::vector<std::string> images_before = ImageFiles(env);
            const std::vector<std::string> discs_before = DiscFiles(env);
            Step("读同卷第二个 inode=" + std::to_string(second_inode) +
                 "，读前快照 image=[" + JoinNames(images_before) + "] disc_sim=[" +
                 JoinNames(discs_before) + "]");
            const ReadOutcome hit =
                ReadInode(&client, corpus, env, packed.volume_id, second_inode, kCacheHitTimeoutMs);
            ExpectWithDetail(hit.ok,
                             "缓存命中读回数据与语料一致（inode=" + std::to_string(second_inode) + "）",
                             hit.error);
            const uint64_t hit_ms = static_cast<uint64_t>(hit.elapsed_ms);
            ExpectWithDetail(hit.elapsed_ms < 3000.0,
                             "缓存命中耗时 < 3000ms（实测 " + std::to_string(hit_ms) +
                                 "ms，远小于一次 CD 装载）",
                             "耗时 " + std::to_string(hit_ms) + "ms，疑似再次走 CD_READ");
            const std::vector<std::string> images_after = ImageFiles(env);
            const std::vector<std::string> discs_after = DiscFiles(env);
            ExpectWithDetail(images_after == images_before && discs_after == discs_before,
                             "缓存命中前后 image_dir / disc_sim 内容不变",
                             "读前 image=[" + JoinNames(images_before) + "] disc_sim=[" +
                                 JoinNames(discs_before) + "]；读后 image=[" + JoinNames(images_after) +
                                 "] disc_sim=[" + JoinNames(discs_after) + "]");
        }
    }

    // 测试点 A5：满载后的 LRU 淘汰与重新提取（evict 档位：容量 14 / 背压上限 12）。
    // 前置：A3 已读入若干卷，叠加「尾盘未刻录镜像」后 image_dir 恰好满载。
    // 选一个「已刻录但不在 image_dir」的卷（= 已刻录前缀的最后一个，不在 A3 读取范围内），
    // 其读回必然走 CD_READ 并从 vdisc 提取，从而触发一次淘汰。
    if (profile.check_eviction && disc_outcome.burned_count > read_total &&
        packed_volumes.size() > profile.capacity_in_images) {
        CaseScope scope("A5", "LRU 淘汰与重载（image_dir 满载后读入新卷）", kCaseA5Objective,
                        kCaseA5Expectation, kCaseA5Method);
        Step("读前快照 " + DirSnapshot(env.image_dir) + "；" + DirSnapshot(env.disc_sim_dir));
        ExpectEqualU64(static_cast<uint64_t>(ImageFiles(env).size()),
                       profile.capacity_in_images,
                       "读满后 image_dir 镜像数量等于 capacity_in_images");

        const PackedVolume& last = packed_volumes[disc_outcome.burned_count - 1];
        const uint64_t last_inode = last.inodes.front();
        const std::vector<std::string> images_before = ImageFiles(env);
        const std::vector<std::string> discs_before = DiscFiles(env);
        Expect(!FileExists(ImagePath(env, last.volume_id)),
               "待读入的卷已从 image_dir 释放（volume_" + std::to_string(last.volume_id) +
                   "，读回必须走 CD_READ）");

        Step("读 volume_" + std::to_string(last.volume_id) + " inode=" + std::to_string(last_inode) +
             "（image_dir 已满，预期淘汰一个 READ 受害者）");
        const ReadOutcome outcome =
            ReadInode(&client, corpus, env, last.volume_id, last_inode, kCdReadTimeoutMs);
        ExpectWithDetail(outcome.ok, "满载后读入新卷仍可读通（字节与语料一致）", outcome.error);

        const std::vector<std::string> images_after = ImageFiles(env);
        const std::vector<std::string> discs_after = DiscFiles(env);
        Step("读后快照 image=[" + JoinNames(images_after) + "] disc_sim=[" +
             JoinNames(discs_after) + "]");
        ExpectEqualU64(static_cast<uint64_t>(images_after.size()),
                       profile.capacity_in_images,
                       "淘汰后 image_dir 镜像数量保持不变");
        ExpectWithDetail(discs_after == discs_before,
                         "淘汰只删缓存副本，disc_sim 中的 vdisc 不变",
                         "读前 disc_sim=[" + JoinNames(discs_before) + "]；读后 disc_sim=[" +
                             JoinNames(discs_after) + "]");
        Expect(FileExists(ImagePath(env, last.volume_id)),
               "刚加载的卷保留在 image_dir（volume_" + std::to_string(last.volume_id) + "）");

        // victim = (读前镜像 ∪ {新卷}) \ 读后镜像：恰好一个，且不是刚加载的卷。
        std::set<std::string> expected_after(images_before.begin(), images_before.end());
        expected_after.insert("volume_" + std::to_string(last.volume_id) + ".vimg");
        std::vector<std::string> victims;
        for (const std::string& name : expected_after) {
            const bool present =
                std::find(images_after.begin(), images_after.end(), name) != images_after.end();
            if (!present) {
                victims.push_back(name);
            }
        }
        ExpectEqualU64(static_cast<uint64_t>(victims.size()), 1,
                       "恰好淘汰一个 READ 受害者（原 image_dir 中的镜像）");
        if (victims.size() == 1) {
            const uint64_t victim = ParseVolumeIdFromName(victims.front());
            Expect(victim != last.volume_id && victim != 0,
                   "被淘汰的不是刚加载的卷（victim=" + std::to_string(victim) +
                       "，新卷=" + std::to_string(last.volume_id) + "）");
            uint64_t victim_inode = 0;
            for (const PackedVolume& packed : packed_volumes) {
                if (packed.volume_id == victim && !packed.inodes.empty()) {
                    victim_inode = packed.inodes.front();
                    break;
                }
            }
            Require(victim_inode != 0,
                    "可定位被淘汰卷的 inode（victim=" + std::to_string(victim) + "）");
            Step("被淘汰的镜像 volume_" + std::to_string(victim) + "，重新读以验证可从 vdisc 重新提取");
            const ReadOutcome reload =
                ReadInode(&client, corpus, env, victim, victim_inode, kCdReadTimeoutMs);
            ExpectWithDetail(reload.ok, "被淘汰卷重载后可读通（字节与语料一致）", reload.error);
            Expect(FileExists(ImagePath(env, victim)),
                   "重载后被淘汰卷回到 image_dir（volume_" + std::to_string(victim) + "）");
            ExpectEqualU64(static_cast<uint64_t>(DiscFiles(env).size()),
                           disc_outcome.disc_count, "重载后 vdisc 仍持久保留");
        }
    }

    // 测试点 A6：归档中间态清理。
    {
        CaseScope scope("A6", "归档中间态清理", kCaseA6Objective, kCaseA6Expectation, kCaseA6Method);
        ExpectCleanEndState(env);
    }

    const uint64_t elapsed_s = static_cast<uint64_t>(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - run_begin).count());
    std::cerr << "\n[RUN] 结束时间: " << NowString() << "，scenario=" << options.scenario
              << "，总耗时 " << elapsed_s << "s" << std::endl;
    DumpCaseSummary();
    return FailureCounter() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    const Options options = ParseArgs(argc, argv);
    const int rc = RunScenario(options);
    if (rc == 0) {
        std::cerr << "[PASS] optical_node 归档集成测试通过（scenario=" << options.scenario << "）" << std::endl;
    } else {
        std::cerr << "[FAIL] 共 " << FailureCounter() << " 项断言失败（scenario=" << options.scenario << "）"
                  << std::endl;
    }
    return rc;
}