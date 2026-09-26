#pragma once

// optical_node 单模块集成测试的公共工具：断言、轮询等待、目录快照、stdout 抓取、上报行解析。
// 仅测试使用，不参与产线编译。

#include <dirent.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace zb::optical_node::test {

// 被测节点打印的打包汇报行前缀（MDS ReportFilesPackedToImage 未完善前的控制台替代）。
inline constexpr const char* kPackReportPrefix = "[ReportFilesPackedToImage]";

// 被测节点打印的刻录汇报行前缀（MDS ReportImagesBurnedToDisc 未完善前的控制台替代）。
inline constexpr const char* kBurnReportPrefix = "[ReportImagesBurnedToDisc]";

// ---------------------------------------------------------------------------
// 测试点日志
//
// 一个"测试点"（Case）记录四件事：测试目标（为什么测）、期望（判定条件）、
// 验证方法（靠什么观测手段）、逐项检查结论。测试点可带实例序号（如 "A3.2"），
// 同一测试目标的多次实例共用种类前缀（"A3"）以在汇总中只列一次目标说明。
// ---------------------------------------------------------------------------

struct CaseRecord {
    std::string id;           // 实例编号，如 "A1.3"
    std::string kind;         // 测试点种类，如 "A1"
    std::string title;        // 实例标题，如 "第 3/6 卷：归档下发 → 打包汇报 → 刻录释放"
    std::string objective;    // 测试目标：为什么要测这一项
    std::string expectation;  // 期望：判定成立的条件
    std::string method;       // 验证方法：靠什么观测手段得出结论
    size_t checks{0};
    size_t failed{0};
    bool finished{false};
};

inline int& FailureCounter() {
    static int failures = 0;
    return failures;
}

inline std::mutex& CaseLogMutex() {
    static std::mutex mu;
    return mu;
}

inline std::vector<CaseRecord>& CaseRecords() {
    static std::vector<CaseRecord> records;
    return records;
}

inline std::vector<int>& CaseStack() {
    static std::vector<int> stack;
    return stack;
}

inline int& CurrentCaseIndex() {
    static int index = -1;
    return index;
}

inline std::string PadRight(const std::string& text, size_t width) {
    return text.size() >= width ? text : text + std::string(width - text.size(), ' ');
}

// 记录一次检查结论：打印结论行、计入当前测试点与全局失败计数。
inline bool RecordCheck(bool passed, const std::string& what) {
    std::lock_guard<std::mutex> lock(CaseLogMutex());
    const int index = CurrentCaseIndex();
    if (index >= 0 && index < static_cast<int>(CaseRecords().size())) {
        CaseRecord& record = CaseRecords()[static_cast<size_t>(index)];
        ++record.checks;
        if (!passed) {
            ++record.failed;
        }
    }
    if (!passed) {
        ++FailureCounter();
    }
    std::cerr << (passed ? "    [PASS] " : "    [FAIL] ") << what << std::endl;
    return passed;
}

inline void ReportFailure(const std::string& what) {
    RecordCheck(false, what);
}

// 结论汇总：逐测试点结果表 + 各测试点目标清单 + 合计。
inline void DumpCaseSummary() {
    std::lock_guard<std::mutex> lock(CaseLogMutex());
    const std::vector<CaseRecord>& records = CaseRecords();
    std::cerr << "\n" << std::string(96, '=') << std::endl;
    std::cerr << "测试点汇总" << std::endl;
    std::cerr << std::string(96, '-') << std::endl;
    if (records.empty()) {
        std::cerr << "（无测试点记录：测试在环境准备阶段即中止）" << std::endl;
        std::cerr << std::string(96, '=') << std::endl;
        return;
    }

    size_t total_checks = 0;
    size_t total_failed = 0;
    std::cerr << PadRight("编号", 9) << PadRight("结果", 7) << PadRight("检查", 7)
              << PadRight("失败", 7) << "测试点" << std::endl;
    for (const CaseRecord& record : records) {
        total_checks += record.checks;
        total_failed += record.failed;
        const std::string result =
            record.failed > 0 ? "FAIL" : (record.finished ? "PASS" : "ABORT");
        std::cerr << PadRight(record.id, 9) << PadRight(result, 7)
                  << PadRight(std::to_string(record.checks), 7)
                  << PadRight(std::to_string(record.failed), 7) << record.title << std::endl;
    }

    std::cerr << std::string(96, '-') << std::endl;
    std::cerr << "合计: " << records.size() << " 个测试点 / " << total_checks << " 项检查 / 失败 "
              << total_failed << " 项；全局断言失败计数 " << FailureCounter() << std::endl;
    std::cerr << std::string(96, '-') << std::endl;
    std::cerr << "测试点目标清单：" << std::endl;
    std::vector<std::string> kinds;
    for (const CaseRecord& record : records) {
        if (std::find(kinds.begin(), kinds.end(), record.kind) != kinds.end()) {
            continue;
        }
        kinds.push_back(record.kind);
        // 同种类实例的标题形如"第 3/6 卷：xxx"，汇总里取全角冒号后的通用描述。
        std::string label = record.title;
        const size_t colon = label.find("：");
        if (colon != std::string::npos) {
            label = label.substr(colon + std::strlen("："));
        }
        std::cerr << "  [" << record.kind << "] " << label << std::endl;
        std::cerr << "      目标: " << record.objective << std::endl;
        std::cerr << "      期望: " << record.expectation << std::endl;
        std::cerr << "      方法: " << record.method << std::endl;
    }
    std::cerr << std::string(96, '=') << std::endl;
}

// 测试点实例：进入时打印目标 / 期望 / 验证方法，退出时打印该实例的结论。
class CaseScope {
public:
    CaseScope(const std::string& id,
              const std::string& title,
              const std::string& objective,
              const std::string& expectation,
              const std::string& method) {
        std::lock_guard<std::mutex> lock(CaseLogMutex());
        CaseRecord record;
        record.id = id;
        const size_t dot = id.find('.');
        record.kind = dot == std::string::npos ? id : id.substr(0, dot);
        record.title = title;
        record.objective = objective;
        record.expectation = expectation;
        record.method = method;
        CaseRecords().push_back(record);
        CaseStack().push_back(CurrentCaseIndex());
        CurrentCaseIndex() = static_cast<int>(CaseRecords().size()) - 1;

        std::cerr << "\n[CASE] " << id << "  " << title << std::endl;
        std::cerr << "    测试目标: " << objective << std::endl;
        std::cerr << "    期望: " << expectation << std::endl;
        std::cerr << "    验证方法: " << method << std::endl;
    }

    ~CaseScope() {
        std::lock_guard<std::mutex> lock(CaseLogMutex());
        const int index = CurrentCaseIndex();
        if (index >= 0 && index < static_cast<int>(CaseRecords().size())) {
            CaseRecord& record = CaseRecords()[static_cast<size_t>(index)];
            record.finished = true;
            std::cerr << "[CASE-END] " << record.id << " result="
                      << (record.failed == 0 ? "PASS" : "FAIL")
                      << " checks=" << record.checks << " failed=" << record.failed << std::endl;
        }
        if (!CaseStack().empty()) {
            CurrentCaseIndex() = CaseStack().back();
            CaseStack().pop_back();
        }
    }

    CaseScope(const CaseScope&) = delete;
    CaseScope& operator=(const CaseScope&) = delete;
    CaseScope(CaseScope&&) = delete;
    CaseScope& operator=(CaseScope&&) = delete;
};

// 测试点内部的流程步骤（描述做了什么，不计入检查数）。
inline void Step(const std::string& text) {
    std::lock_guard<std::mutex> lock(CaseLogMutex());
    std::cerr << "  [STEP] " << text << std::endl;
}

// ---------------------------------------------------------------------------
// 断言
// ---------------------------------------------------------------------------

// Expect：条件不成立时记录失败并继续执行。
inline bool Expect(bool cond, const std::string& what) {
    return RecordCheck(cond, what);
}

// ExpectWithDetail：成立时只打印 describe，失败时追加 detail（避免把失败原因写进通过结论）。
inline bool ExpectWithDetail(bool cond, const std::string& describe, const std::string& detail) {
    return RecordCheck(cond, cond ? describe : describe + "；失败详情: " + detail);
}

// Require：前置条件不成立时输出汇总并立即退出，避免后续步骤在错误状态下刷出噪声。
// 成立时不打印、不计数（前置条件本身不是被测行为）。
inline void Require(bool cond, const std::string& what) {
    if (cond) {
        return;
    }
    RecordCheck(false, "前置条件不成立: " + what);
    std::cerr << "[FATAL] 前置条件不满足，测试终止" << std::endl;
    DumpCaseSummary();
    std::exit(2);
}

inline bool ExpectEqualU64(uint64_t actual, uint64_t expected, const std::string& what) {
    std::ostringstream os;
    os << what << "（actual=" << actual << " expected=" << expected << "）";
    return RecordCheck(actual == expected, os.str());
}

// ---------------------------------------------------------------------------
// 文件与目录
// ---------------------------------------------------------------------------

inline bool FileExists(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
}

inline bool GetFileSize(const std::string& path, uint64_t* out) {
    struct stat st{};
    if (::stat(path.c_str(), &st) != 0 || !S_ISREG(st.st_mode)) {
        return false;
    }
    *out = static_cast<uint64_t>(st.st_size);
    return true;
}

inline bool ReadWholeFile(const std::string& path, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    FILE* fp = std::fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        return false;
    }
    out->clear();
    char buf[65536];
    size_t n = 0;
    while ((n = std::fread(buf, 1, sizeof(buf), fp)) > 0) {
        out->append(buf, n);
    }
    std::fclose(fp);
    return true;
}

// 读取 [offset, offset+size) 区间；越界返回 false（fake real_node 使用）。
inline bool ReadFileRange(const std::string& path, uint64_t offset, uint64_t size, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    FILE* fp = std::fopen(path.c_str(), "rb");
    if (fp == nullptr) {
        return false;
    }
    if (::fseek(fp, static_cast<long>(offset), SEEK_SET) != 0) {
        std::fclose(fp);
        return false;
    }
    out->clear();
    out->resize(static_cast<size_t>(size));
    const size_t n = std::fread(&(*out)[0], 1, static_cast<size_t>(size), fp);
    std::fclose(fp);
    if (n != static_cast<size_t>(size)) {
        out->clear();
        return false;
    }
    return true;
}

inline std::vector<std::string> ListDir(const std::string& dir) {
    std::vector<std::string> names;
    DIR* handle = ::opendir(dir.c_str());
    if (handle == nullptr) {
        return names;
    }
    while (struct dirent* entry = ::readdir(handle)) {
        const std::string name = entry->d_name;
        if (name != "." && name != "..") {
            names.push_back(name);
        }
    }
    ::closedir(handle);
    std::sort(names.begin(), names.end());
    return names;
}

// 按后缀过滤目录项（如 ".vimg"）。
inline std::vector<std::string> ListDirBySuffix(const std::string& dir, const std::string& suffix) {
    std::vector<std::string> matched;
    for (const std::string& name : ListDir(dir)) {
        if (name.size() >= suffix.size() &&
            name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0) {
            matched.push_back(name);
        }
    }
    return matched;
}

inline std::string DirSnapshot(const std::string& dir) {
    std::ostringstream os;
    os << dir << " => [";
    for (const std::string& name : ListDir(dir)) {
        uint64_t size = 0;
        GetFileSize(dir + "/" + name, &size);
        os << name << "(" << size << ") ";
    }
    os << "]";
    return os.str();
}

inline void DumpDirs(const std::vector<std::string>& dirs) {
    std::cerr << "  目录快照：" << std::endl;
    for (const std::string& dir : dirs) {
        std::cerr << "    " << DirSnapshot(dir) << std::endl;
    }
}

// ---------------------------------------------------------------------------
// 轮询等待（替代固定 sleep）
// ---------------------------------------------------------------------------

// 20ms 轮询直到谓词成立：成功记录一次检查（含实际耗时，可作为 CD 装载等时长的交叉证据），
// 超时打印目录快照并记录一次失败。
template <typename Pred>
inline bool WaitFor(Pred pred, int timeout_ms, const std::string& what,
                    const std::vector<std::string>& dirs = {}) {
    const auto begin = std::chrono::steady_clock::now();
    bool hit = false;
    while (std::chrono::steady_clock::now() - begin < std::chrono::milliseconds(timeout_ms)) {
        if (pred()) {
            hit = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (!hit) {
        hit = pred();
    }
    const uint64_t elapsed_ms = static_cast<uint64_t>(
        std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - begin).count());
    if (hit) {
        return RecordCheck(true, what + " —— 已在 " + std::to_string(elapsed_ms) + "ms 内满足");
    }
    if (!dirs.empty()) {
        DumpDirs(dirs);
    }
    return RecordCheck(false, what + " —— 超时 " + std::to_string(timeout_ms) +
                                  "ms 未满足（已轮询 " + std::to_string(elapsed_ms) + "ms）");
}

// ---------------------------------------------------------------------------
// 字符串
// ---------------------------------------------------------------------------

inline std::vector<std::string> SplitBy(const std::string& text, char sep) {
    std::vector<std::string> parts;
    std::string current;
    std::istringstream stream(text);
    while (std::getline(stream, current, sep)) {
        parts.push_back(current);
    }
    return parts;
}

inline std::string Trim(const std::string& text) {
    const size_t begin = text.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return std::string();
    }
    const size_t end = text.find_last_not_of(" \t\r\n");
    return text.substr(begin, end - begin + 1);
}

inline std::vector<uint64_t> ParseInodeCsv(const std::string& csv) {
    std::vector<uint64_t> inodes;
    for (const std::string& token : SplitBy(csv, ',')) {
        const std::string trimmed = Trim(token);
        if (trimmed.empty()) {
            continue;
        }
        inodes.push_back(::strtoull(trimmed.c_str(), nullptr, 10));
    }
    return inodes;
}

inline std::string JoinInodes(std::vector<uint64_t> inodes) {
    std::sort(inodes.begin(), inodes.end());
    std::ostringstream os;
    for (size_t i = 0; i < inodes.size(); ++i) {
        if (i > 0) {
            os << ",";
        }
        os << inodes[i];
    }
    return os.str();
}

// 集合比较（顺序无关）。
inline bool SameInodeSet(std::vector<uint64_t> lhs, std::vector<uint64_t> rhs) {
    std::sort(lhs.begin(), lhs.end());
    std::sort(rhs.begin(), rhs.end());
    return lhs == rhs;
}

// ---------------------------------------------------------------------------
// 打包汇报行解析
// ---------------------------------------------------------------------------

struct PackReport {
    uint64_t image_id{0};
    uint64_t count{0};
    std::vector<uint64_t> inodes;
};

// 解析形如 "[ReportFilesPackedToImage] image_id=3 count=12 inode_ids=1000,1001" 的行。
inline bool ParsePackReport(const std::string& line, PackReport* out) {
    if (out == nullptr || line.rfind(kPackReportPrefix, 0) != 0) {
        return false;
    }
    const size_t img_pos = line.find("image_id=");
    const size_t cnt_pos = line.find("count=");
    const size_t ids_pos = line.find("inode_ids=");
    if (img_pos == std::string::npos || cnt_pos == std::string::npos || ids_pos == std::string::npos) {
        return false;
    }
    out->image_id = ::strtoull(line.c_str() + img_pos + std::strlen("image_id="), nullptr, 10);
    out->count = ::strtoull(line.c_str() + cnt_pos + std::strlen("count="), nullptr, 10);
    out->inodes = ParseInodeCsv(line.substr(ids_pos + std::strlen("inode_ids=")));
    return true;
}

// 刻录汇报：一张光盘与其包含的全部卷镜像。
struct BurnReport {
    uint64_t disc_id{0};
    uint64_t count{0};
    std::vector<uint64_t> image_ids;
};

// 解析形如
// "[ReportImagesBurnedToDisc] disc_id=1 count=11 image_ids=1,2,..." 的行。
inline bool ParseBurnReport(const std::string& line, BurnReport* out) {
    if (out == nullptr || line.rfind(kBurnReportPrefix, 0) != 0) {
        return false;
    }
    const size_t disc_pos = line.find("disc_id=");
    const size_t cnt_pos = line.find("count=");
    const size_t ids_pos = line.find("image_ids=");
    if (disc_pos == std::string::npos || cnt_pos == std::string::npos ||
        ids_pos == std::string::npos) {
        return false;
    }
    out->disc_id = ::strtoull(line.c_str() + disc_pos + std::strlen("disc_id="), nullptr, 10);
    out->count = ::strtoull(line.c_str() + cnt_pos + std::strlen("count="), nullptr, 10);
    out->image_ids = ParseInodeCsv(line.substr(ids_pos + std::strlen("image_ids=")));
    return true;
}

// ---------------------------------------------------------------------------
// stdout 抓取
// ---------------------------------------------------------------------------

// 抓取本进程 stdout（含被测节点打印的打包汇报行），并把每行转发到 stderr，
// 便于 ctest --output-on-failure 展示。
class StdoutCapture {
public:
    StdoutCapture() {
        int fds[2] = {-1, -1};
        if (::pipe(fds) != 0) {
            return;
        }
        read_fd_ = fds[0];
        saved_fd_ = ::dup(STDOUT_FILENO);
        if (saved_fd_ < 0 || ::dup2(fds[1], STDOUT_FILENO) < 0) {
            ::close(fds[0]);
            ::close(fds[1]);
            read_fd_ = -1;
            return;
        }
        ::close(fds[1]);
        thread_ = std::thread([this] { ReadLoop(); });
    }

    ~StdoutCapture() {
        if (thread_.joinable()) {
            std::cout.flush();
            std::fflush(nullptr);
            stop_.store(true);
            thread_.join();
        }
        if (saved_fd_ >= 0) {
            ::dup2(saved_fd_, STDOUT_FILENO);
            ::close(saved_fd_);
        }
        if (read_fd_ >= 0) {
            ::close(read_fd_);
        }
    }

    StdoutCapture(const StdoutCapture&) = delete;
    StdoutCapture& operator=(const StdoutCapture&) = delete;

    std::vector<std::string> Lines() {
        std::lock_guard<std::mutex> lock(mu_);
        return lines_;
    }

    // 从 *cursor 起查找第一条以 prefix 开头的行；命中则推进 cursor 并回填该行。
    bool NextLineWithPrefix(size_t* cursor, const std::string& prefix, std::string* out) {
        std::lock_guard<std::mutex> lock(mu_);
        for (size_t i = *cursor; i < lines_.size(); ++i) {
            if (lines_[i].rfind(prefix, 0) == 0) {
                *out = lines_[i];
                *cursor = i + 1;
                return true;
            }
        }
        *cursor = lines_.size();
        return false;
    }

    // 已抓取行中匹配 prefix 的条数。
    size_t CountLinesWithPrefix(const std::string& prefix) {
        std::lock_guard<std::mutex> lock(mu_);
        size_t count = 0;
        for (const std::string& line : lines_) {
            if (line.rfind(prefix, 0) == 0) {
                ++count;
            }
        }
        return count;
    }

    // 返回全部以 prefix 开头的行（按抓取顺序）。
    // 与 NextLineWithPrefix 不同，本方法不推进游标，适合汇总类断言
    // （两类汇报行交错出现时，共用游标会互相吞掉对方的行）。
    std::vector<std::string> CollectLinesWithPrefix(const std::string& prefix) {
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<std::string> matched;
        for (const std::string& line : lines_) {
            if (line.rfind(prefix, 0) == 0) {
                matched.push_back(line);
            }
        }
        return matched;
    }

private:
    void ReadLoop() {
        std::string pending;
        char buf[4096];
        while (!stop_.load(std::memory_order_relaxed)) {
            struct pollfd pfd{read_fd_, POLLIN, 0};
            if (::poll(&pfd, 1, 50) <= 0) {
                continue;
            }
            const ssize_t n = ::read(read_fd_, buf, sizeof(buf));
            if (n <= 0) {
                break;
            }
            pending.append(buf, static_cast<size_t>(n));
            size_t pos = 0;
            while ((pos = pending.find('\n')) != std::string::npos) {
                const std::string line = pending.substr(0, pos);
                pending.erase(0, pos + 1);
                {
                    std::lock_guard<std::mutex> lock(mu_);
                    lines_.push_back(line);
                }
                std::cerr << "[node] " << line << std::endl;
            }
        }
    }

    int read_fd_{-1};
    int saved_fd_{-1};
    std::atomic<bool> stop_{false};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> lines_;
};

}  // namespace zb::optical_node::test