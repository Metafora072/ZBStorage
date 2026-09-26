// optical_node 单模块集成测试：
//   进程内起真光节点 server（OpticalStorageServiceImpl + BrpcOpticalNodeService），
//   配合假 real_node / scheduler 两个 brpc server，以及假 MDS / client 两个 brpc 客户端，
//   在小规模参数（卷镜像 100MiB、image_dir 上限 5）下仿真完整链路：
//     归档下发 → 从 real_node 按分片下载 → 压缩封装 → 打包汇报（控制台）→ 刻录 → 释放到光盘库
//     数据面读 → 镜像被换出时 CD_READ 重载 → 分片读到 FINISH → 容量 5 的 LRU 淘汰
//
// 用法: optical_node_archive_integration_test --corpus <dir> --work-dir <dir> --scenario smoke|full
// 输出: 全程日志写到 stderr（由 run_optical_node_archive_test.py 落盘为 <work-dir>/output.log）。
//       日志按"测试点"组织：
//         [CASE] A3.1 第 1/5 卷：数据面读 + CD_READ 重载
//             测试目标 / 期望 / 验证方法
//           [STEP] 读 volume_1 inode=1000（镜像在光盘库，预期触发一次 CD_READ 装载）
//           [PASS] 读回数据与语料逐字节一致（inode=1000，共 8388608 字节，耗时 21043ms）
//         [CASE-END] A3.1 result=PASS checks=5 failed=0
//       末尾输出"测试点汇总"表与各测试点目标清单；测试点编号见 README.md。

#include "fake_client.h"
#include "fake_services.h"
#include "optical_node_test_support.h"

#include <brpc/server.h>
#include <optical_node.pb.h>

#include "BrpcOpticalNodeService.h"
#include "OpticalStorageServiceImpl.h"

#include <chrono>
#include <cstring>
#include <ctime>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace zb::optical_node;
using namespace zb::optical_node::test;

// 小规模测试参数（与 prepare_archive_corpus.py 的 --selftest 假设一致）。
constexpr uint64_t kVolumeSizeBytes = 100ull * 1024 * 1024;
constexpr double kSizeThreshold = 0.9;
constexpr uint64_t kCapacityInImages = 5;
constexpr uint8_t kAvailableVolumeIdCount = 5;
constexpr uint64_t kReadShardBytes = 4ull * 1024 * 1024;

// 仿真时长：单次光盘操作（装盘 12s + 读/刻录 + 退盘 3s + 机械臂）约 20~30s，
// 且产线不建议为测试改动仿真参数，故超时留足余量。
constexpr int kPackReportTimeoutMs = 180000;
constexpr int kBurnTimeoutMs = 240000;
constexpr int kCdReadTimeoutMs = 240000;
constexpr int kCacheHitTimeoutMs = 15000;

// ---------------------------------------------------------------------------
// 测试点描述文案：目标（为什么要测）/ 期望（判定条件）/ 方法（靠什么观测）。
// 编号与 README.md「测试点与断言覆盖面」一节一致；同类测试点（A1/A3/A4）多次实例共用文案。
// ---------------------------------------------------------------------------

constexpr const char* kCaseA0Objective =
    "确认被测光节点与它的全部仿真对手方（假 real_node / scheduler / MDS / client）就绪，"
    "且语料分组真值、工作目录齐备——后续所有断言都必须建立在可信前提之上。";
constexpr const char* kCaseA0Expectation =
    "语料分组数 > 0；OpticalNodeManager 状态为 RUNNING（IsArchiveEngineReady 为真）；"
    "光节点、假 real_node、假 scheduler 三个 brpc server 均成功监听；archive 工作目录已创建。";
constexpr const char* kCaseA0Method =
    "载入 manifest.tsv 并交叉校验 expected_pack_plan.tsv；调用 IsArchiveEngineReady()/GetArchiveStatusDetail()；"
    "读取各 LocalServer 的 listen_address()；stat 各工作目录。";

constexpr const char* kCaseA1Objective =
    "验证写链路：MDS 下发的归档文件应被节点按 target_node_id/target_disk_id 从 real_node 分片下载、"
    "按 object_unit_size 拼装，压缩后按阈值累加，达到打包阈值时封装为卷镜像并上报，"
    "最终由 CD_BURN 把镜像从 image_dir 释放到光盘库模拟区 disc_sim。";
constexpr const char* kCaseA1Expectation =
    "打包汇报行的 count 与 inode 集合等于 prepare_archive_corpus.py 用 zlib9 精确预测的分组；"
    "卷镜像只出现在 disc_sim、体积 ∈ (0, 100MiB]；刻录完成后 image_dir 不残留该卷；各卷 volume_id 互不重复。";
constexpr const char* kCaseA1Method =
    "SendArchiveMetadata 下发批次；抓取节点 stdout 的 [ReportFilesPackedToImage] 行解析 image_id/count/inode_ids；"
    "轮询 image_dir 与 disc_sim 的文件系统事实（不使用固定 sleep）。";

constexpr const char* kCaseA2Objective =
    "验证节点没有漏读或重复读 real_node 的对象——下载完整性只能靠跨卷汇总计数证明。";
constexpr const char* kCaseA2Expectation =
    "ResolveFileRead 调用次数 == 已归档文件数；ReadObject 调用次数 == 对象总数（文件数 × 每文件对象数）。";
constexpr const char* kCaseA2Method =
    "统计假 real_node 的两个原子计数器，与语料 manifest.tsv 推算出的期望值比对。";

constexpr const char* kCaseA3Objective =
    "验证读链路在镜像已刻录（不在 image_dir）时的行为：读请求必须触发 CD_READ 把卷镜像从光盘库"
    "重新装载到 image_dir，再把目标 inode 从镜像中按分片读出，末片读完后任务进入 FINISH。";
constexpr const char* kCaseA3Expectation =
    "读回的 8MiB 与语料原始 jpg 数据逐字节一致；镜像回到 image_dir 且从 disc_sim 移除；"
    "image_dir 中 READ 镜像数随已读卷数递增；末片读完后 ReadObjectByInodeId 返回 MDS_NOT_FOUND"
    "（FINISH 时 inode→task 索引被清除）。";
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
    "验证 image_dir 达到容量上限后读入新卷时，节点应淘汰一个已有的 READ 镜像（换出回光盘库）"
    "以腾出空间，且被淘汰的卷之后仍能重新装载读通——淘汰只影响缓存层级，不影响数据可用性。";
constexpr const char* kCaseA5Expectation =
    "淘汰后 image_dir 镜像数恰为 capacity_in_images(5) 且 disc_sim 恰有 1 个被换出的镜像；"
    "刚读入的卷保留在 image_dir；victim != 刚加载的卷；被淘汰卷重新读时能再次装载并读通、并回到 image_dir。";
constexpr const char* kCaseA5Method =
    "先读满 5 卷再读第 6 卷，随后枚举两个目录的 .vimg 并解析 volume_id，"
    "再对被淘汰卷发起一次完整读（含字节比对）。";

constexpr const char* kCaseA6Objective =
    "验证归档流程结束后不残留中间产物：input/ 中不应留下已归档文件，log/ 目录应存在"
    "（任务终态日志按 600s 周期清理生成，测试期内不出现属已知缺口）。";
constexpr const char* kCaseA6Expectation =
    "input/ 下 .archive 文件数为 0；log/ 目录存在。";
constexpr const char* kCaseA6Method =
    "枚举 input/ 目录列表；stat log/；若测试期内意外生成了任务日志则打印提示。";

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
    Require(options.scenario == "smoke" || options.scenario == "full",
            "--scenario 只能是 smoke 或 full");
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

std::string DiscPath(const Env& env, uint64_t volume_id) {
    return env.disc_sim_dir + "volume_" + std::to_string(volume_id) + ".vimg";
}

// image_dir 中的卷镜像文件名（形如 volume_<id>.vimg）。
std::vector<std::string> ImageFiles(const Env& env) {
    return ListDirBySuffix(env.image_dir, ".vimg");
}

std::vector<std::string> DiscFiles(const Env& env) {
    return ListDirBySuffix(env.disc_sim_dir, ".vimg");
}

// 从 volume_<id>.vimg 取出 <id>。
uint64_t ParseVolumeIdFromName(const std::string& name) {
    const std::string prefix = "volume_";
    if (name.rfind(prefix, 0) != 0) {
        return 0;
    }
    return ::strtoull(name.c_str() + prefix.size(), nullptr, 10);
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

// 下发一批归档文件 → 等打包汇报 → 等镜像出现 → 等刻录完成释放到 disc_sim。
// 全过程归入一个测试点实例（case_id / case_title 由调用方按卷序号生成）。
PackedVolume RunOneVolume(FakeMdsDriver* mds,
                          StdoutCapture* capture,
                          size_t* report_cursor,
                          const Corpus& corpus,
                          const Env& env,
                          const PackPlanGroup& group,
                          uint64_t batch_id,
                          const std::string& case_id,
                          const std::string& case_title,
                          std::set<uint64_t>* seen_volume_ids) {
    PackedVolume packed;
    CaseScope scope(case_id, case_title, kCaseA1Objective, kCaseA1Expectation, kCaseA1Method);

    // 1. 假 MDS 下发批次（每个文件带上分片信息，供节点去 real_node 下载）。
    Step("假 MDS 下发批次 batch_id=" + std::to_string(batch_id) + "：文件数=" +
         std::to_string(group.inodes.size()) +
         "，预测打包分组 volume_index=" + std::to_string(group.volume_index) +
         "，inode_ids=" + JoinInodes(group.inodes));
    std::vector<zb::rpc::ArchiveFile> files;
    files.reserve(group.inodes.size());
    for (uint64_t inode_id : group.inodes) {
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

    // 2. 等本次打包的汇报行（控制台替代 MDS ReportFilesPackedToImage）。
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

    // 3. 写镜像应出现在 image_dir，随后刻录完成被释放到 disc_sim。
    const uint64_t volume_id = packed.volume_id;
    Expect(seen_volume_ids->insert(volume_id).second,
           "卷镜像 id 全局唯一（本次 volume_id=" + std::to_string(volume_id) + "）");
    Step("轮询 image_dir 出现写镜像 volume_" + std::to_string(volume_id) + ".vimg");
    WaitFor([&] { return FileExists(ImagePath(env, volume_id)); }, kBurnTimeoutMs,
            "image_dir 出现写镜像 volume_" + std::to_string(volume_id), env.dirs());
    Step("轮询 disc_sim 出现已刻录镜像（CD_BURN 完成并释放）");
    WaitFor([&] { return FileExists(DiscPath(env, volume_id)); }, kBurnTimeoutMs,
            "disc_sim 出现已刻录镜像 volume_" + std::to_string(volume_id), env.dirs());

    uint64_t image_size = 0;
    const bool sized = GetFileSize(DiscPath(env, volume_id), &image_size);
    ExpectWithDetail(sized && image_size > 0 && image_size <= kVolumeSizeBytes,
                     "disc_sim 镜像大小落在 (0, 100MiB] 内",
                     "stat 成功=" + std::string(sized ? "true" : "false") +
                         " size=" + std::to_string(image_size));
    Expect(!FileExists(ImagePath(env, volume_id)), "刻录完成后 image_dir 不再残留该卷镜像");
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

int RunScenario(const Options& options) {
    const auto run_begin = std::chrono::steady_clock::now();
    const std::string started_at = NowString();

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
    std::cerr << "optical_node 归档集成测试：scenario=" << options.scenario << std::endl;
    std::cerr << "开始时间: " << started_at << std::endl;
    std::cerr << "工作目录: " << options.work_dir << std::endl;
    std::cerr << "语料目录: " << options.corpus_dir << "（" << corpus.plan().size() << " 卷 / "
              << corpus_files << " 文件 / " << corpus_bytes << " 字节）" << std::endl;
    std::cerr << "被测参数: 卷镜像=" << kVolumeSizeBytes << " 字节; 打包阈值=" << kSizeThreshold
              << "; image_dir 容量=" << kCapacityInImages << "; available_volume_id_count="
              << static_cast<int>(kAvailableVolumeIdCount) << "; 读分片=" << kReadShardBytes
              << " 字节" << std::endl;
    std::cerr << "超时设置: 打包汇报=" << kPackReportTimeoutMs << "ms; 刻录=" << kBurnTimeoutMs
              << "ms; CD_READ=" << kCdReadTimeoutMs << "ms; 缓存命中=" << kCacheHitTimeoutMs
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
    config.capacity_in_images = kCapacityInImages;
    config.available_volume_id_count = kAvailableVolumeIdCount;
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
        ExpectWithDetail(!corpus.plan().empty(), "语料预测打包分组非空（expected_pack_plan.tsv 已就绪）",
                         "plan().size()=" + std::to_string(corpus.plan().size()));
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

    // 5. 写链路：按卷下发批次并等刻录释放。
    const bool full = (options.scenario == "full");
    const size_t volume_total = full ? corpus.plan().size() : 1;
    Require(volume_total > 0, "待归档卷数 > 0");
    if (!full && corpus.plan().size() > 1) {
        std::cerr << "[RUN] smoke 场景只跑第 1 卷，语料共 " << corpus.plan().size() << " 卷"
                  << std::endl;
    }

    std::vector<PackedVolume> packed_volumes;
    std::set<uint64_t> volume_ids;
    for (size_t i = 0; i < volume_total; ++i) {
        const PackPlanGroup& group = corpus.plan()[i];
        const std::string index_text = std::to_string(i + 1) + "/" + std::to_string(volume_total);
        PackedVolume packed = RunOneVolume(
            &mds, &capture, &report_cursor, corpus, env, group, static_cast<uint64_t>(i + 1),
            "A1." + std::to_string(i + 1),
            "第 " + index_text + " 卷：归档下发 → 压缩封装 → 打包汇报 → 刻录释放", &volume_ids);
        packed_volumes.push_back(std::move(packed));
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
        CaseScope scope("A2", "下载完整性（跨卷汇总计数）", kCaseA2Objective, kCaseA2Expectation,
                        kCaseA2Method);
        Step("假 real_node 计数器：ResolveFileRead=" +
             std::to_string(real_node.resolve_calls.load()) + "（期望 " +
             std::to_string(expected_resolves) + "）；ReadObject=" +
             std::to_string(real_node.read_calls.load()) + "（期望 " + std::to_string(expected_reads) +
             "）");
        ExpectEqualU64(static_cast<uint64_t>(real_node.resolve_calls.load()), expected_resolves,
                       "ResolveFileRead 调用次数等于已归档文件数");
        ExpectEqualU64(static_cast<uint64_t>(real_node.read_calls.load()), expected_reads,
                       "ReadObject 调用次数等于对象总数（文件数 × 每文件对象数）");
    }

    // 6. 读链路：镜像已被刻录（不在 image_dir）⇒ 触发 CD_READ 重载。
    // 只读到 image_dir 容量上限：超出容量的卷留给 §7 的淘汰场景，否则此处读第 6 卷就会提前触发淘汰，
    // §7 的"读满后淘汰"退化为缓存命中。
    const size_t read_total =
        full ? std::min<size_t>(volume_total, static_cast<size_t>(kCapacityInImages)) : volume_total;
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
            Expect(!FileExists(DiscPath(env, packed.volume_id)),
                   "CD_READ 后 disc_sim 不再保留该镜像（volume_" + std::to_string(packed.volume_id) +
                       "）");
            ExpectEqualU64(static_cast<uint64_t>(ImageFiles(env).size()), i + 1,
                           "image_dir 中 READ 镜像数量随已读卷数递增");
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

    // 测试点 A5：容量 5 的 LRU 淘汰与重载（full 场景）。
    if (full && packed_volumes.size() > kCapacityInImages) {
        CaseScope scope("A5", "LRU 淘汰与重载（image_dir 满 capacity_in_images 后读新卷）",
                        kCaseA5Objective, kCaseA5Expectation, kCaseA5Method);
        Step("读前快照 " + DirSnapshot(env.image_dir) + "；" + DirSnapshot(env.disc_sim_dir));
        ExpectEqualU64(static_cast<uint64_t>(ImageFiles(env).size()), kCapacityInImages,
                       "读满后 image_dir 镜像数量等于 capacity_in_images");

        const PackedVolume& last = packed_volumes[kCapacityInImages];
        const uint64_t last_inode = last.inodes.front();
        Step("读 volume_" + std::to_string(last.volume_id) + " inode=" + std::to_string(last_inode) +
             "（image_dir 已满，预期淘汰一个 READ 受害者）");
        const ReadOutcome outcome =
            ReadInode(&client, corpus, env, last.volume_id, last_inode, kCdReadTimeoutMs);
        ExpectWithDetail(outcome.ok, "满载后读入新卷仍可读通（字节与语料一致）", outcome.error);

        const std::vector<std::string> images = ImageFiles(env);
        const std::vector<std::string> discs = DiscFiles(env);
        Step("读后快照 image=[" + JoinNames(images) + "] disc_sim=[" + JoinNames(discs) + "]");
        ExpectEqualU64(static_cast<uint64_t>(images.size()), kCapacityInImages,
                       "淘汰后 image_dir 镜像数量保持不变");
        ExpectEqualU64(static_cast<uint64_t>(discs.size()), 1,
                       "淘汰后 disc_sim 恰好新增一个被换出的镜像");
        Expect(FileExists(ImagePath(env, last.volume_id)),
               "刚加载的卷保留在 image_dir（volume_" + std::to_string(last.volume_id) + "）");

        if (discs.size() == 1) {
            const uint64_t victim = ParseVolumeIdFromName(discs.front());
            Expect(victim != last.volume_id,
                   "被淘汰的不是刚加载的卷（victim=" + std::to_string(victim) +
                       "，新卷=" + std::to_string(last.volume_id) + "）");
            Step("被淘汰的镜像 volume_" + std::to_string(victim) + "，重新读以验证可重载");
            uint64_t victim_inode = 0;
            for (const PackedVolume& packed : packed_volumes) {
                if (packed.volume_id == victim && !packed.inodes.empty()) {
                    victim_inode = packed.inodes.front();
                    break;
                }
            }
            Require(victim_inode != 0, "可定位被淘汰卷的 inode（victim=" + std::to_string(victim) + "）");
            const ReadOutcome reload =
                ReadInode(&client, corpus, env, victim, victim_inode, kCdReadTimeoutMs);
            ExpectWithDetail(reload.ok, "被淘汰卷重载后可读通（字节与语料一致）", reload.error);
            Expect(FileExists(ImagePath(env, victim)),
                   "重载后被淘汰卷回到 image_dir（volume_" + std::to_string(victim) + "）");
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