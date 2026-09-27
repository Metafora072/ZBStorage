// 运行期状态快照（meta/runtime_state_meta）编解码单测。
//
// 覆盖三类性质：
//   1. round-trip：每种任务类型 / 归档批次逐字段往返一致（含空串、定长子段、嵌入 '\0' 的字符串）；
//   2. 截断鲁棒性：任意长度前缀、尾部多余字节、枚举越界，都必须只返回 false，
//      既不崩溃也不抛异常（这是「关机快照被截断后重启仍能启动」的前提）；
//   3. 边界：读取游标的越界 / 空指针 / 伪长度，以及文件头与 section 帧的自洽。
//
// 只依赖头文件 optical_node_manager_classes.h（快照编解码全为 inline），不需要链接
// optical_node_manager 静态库。
#include <optical_node_manager_classes.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using optical_node_manager::AppendI32;
using optical_node_manager::AppendSection;
using optical_node_manager::AppendString;
using optical_node_manager::AppendU32;
using optical_node_manager::AppendU64;
using optical_node_manager::AppendU8;
using optical_node_manager::ArchiveFileInfo;
using optical_node_manager::ParseArchiveRequestBlob;
using optical_node_manager::ParseWRTaskBlob;
using optical_node_manager::RUNTIME_STATE_FORMAT_VERSION;
using optical_node_manager::RUNTIME_STATE_HEADER_SIZE;
using optical_node_manager::RUNTIME_STATE_MAGIC;
using optical_node_manager::RUNTIME_STATE_SECTION_HEADER_SIZE;
using optical_node_manager::RuntimeStateMetaHeader;
using optical_node_manager::RuntimeStateReader;
using optical_node_manager::RuntimeStateSectionTag;
using optical_node_manager::SendArchiveMetadataRequest;
using optical_node_manager::SerializeArchiveRequestBlob;
using optical_node_manager::SerializeWRTaskBlob;

void Check(bool ok, const std::string& message) {
    if (!ok) {
        throw std::runtime_error(message);
    }
}

// 按本机字节序改写 blob 中某个 u32 字段（快照编码本身就是本机字节序 memcpy，
// 因此这里用同样的方式构造「枚举越界 / 魔数损坏」等非法输入）。
void PatchU32(std::vector<uint8_t>* blob, size_t offset, uint32_t value) {
    Check(blob != nullptr && offset + sizeof(value) <= blob->size(), "patch offset out of range");
    std::memcpy(blob->data() + offset, &value, sizeof(value));
}

// ---------------------------------------------------------------------------
// 构造各类任务的样本
// ---------------------------------------------------------------------------

// READ / READY：读产物已就绪，file_path 指向 read/ 下的分片产物。
WR_task::WRTask MakeReadReadyTask() {
    WR_task::WRTask task(41, WR_task::WRTaskType::READ);
    task.SetReadTask("7", "1009", "12345");
    task.state = WR_task::WRTaskState::READY;
    task.state_changed_at_ms = 1758900000123ull;
    task.file_path = "read/file_12345_data.bin";
    task.attempt_count = 3;
    task.last_error_code = volumemanager::ErrorCode::SUCCESS;
    task.last_error_detail.clear();
    task.is_failed_terminal = false;
    return task;
}

// WRITE / START：仅有 inode 信息，disk_id / volume_id 由压缩阶段分配。
WR_task::WRTask MakeWriteTask() {
    WR_task::WRTask task(42, WR_task::WRTaskType::WRITE);
    task.SetWriteTask("67890");
    task.file_path = "67890.bin";
    task.state_changed_at_ms = 1758900000456ull;
    return task;
}

// CD_READ / LOADING：无 inode，inode_id_num 应为 INVALID_INODE_ID。
WR_task::WRTask MakeCDReadTask() {
    WR_task::WRTask task(43, WR_task::WRTaskType::CD_READ);
    task.SetCDReadTask("7", "1009");
    task.state = WR_task::WRTaskState::LOADING;
    task.state_changed_at_ms = 1758900000789ull;
    return task;
}

// DISC_BURN / CD_BURNING：盘内多个卷镜像 + 预期盘大小，覆盖 volume_ids 向量。
WR_task::WRTask MakeDiscBurnTask() {
    WR_task::WRTask task(44, WR_task::WRTaskType::DISC_BURN);
    task.SetDiscBurnTask("12", "disc_sim/disc_12.vdisc", {"1001", "1002", "1003"}, 5000000000ull);
    task.state = WR_task::WRTaskState::CD_BURNING;
    task.state_changed_at_ms = 1758900000900ull;
    task.attempt_count = 1;
    return task;
}

// CD_BURN / START：单卷刻录旧链路。
WR_task::WRTask MakeCDBurnTask() {
    WR_task::WRTask task(45, WR_task::WRTaskType::CD_BURN);
    task.SetCDBurnTask("9", "88", "image/volume_88.vimg");
    task.state_changed_at_ms = 1758900001000ull;
    return task;
}

// READ_BATCH_BY_VOLUME：仅 volume_id，其余字符串全为空（覆盖零长度字符串）。
WR_task::WRTask MakeReadBatchTask() {
    WR_task::WRTask task(46, WR_task::WRTaskType::READ_BATCH_BY_VOLUME);
    task.SetReadBatchByVolumeTask("1009");
    task.state_changed_at_ms = 1758900001100ull;
    return task;
}

// FAILED 终态：last_error_detail 内嵌 '\0'，验证字符串按「u32 长度 + 字节」编码而非 C 串。
WR_task::WRTask MakeFailedTask() {
    WR_task::WRTask task(47, WR_task::WRTaskType::READ);
    task.SetReadTask("7", "1009", "999");
    task.SetFailed(volumemanager::ErrorCode::READ_FAILED,
                   std::string("mount_volume_failed\0volume_id=1009", 34));
    task.attempt_count = 32;
    return task;
}

// WRITE 且 inode_id 为空：覆盖 inode 为空时 inode_id_num 落 INVALID_INODE_ID。
WR_task::WRTask MakeEmptyPayloadWriteTask() {
    WR_task::WRTask task(48, WR_task::WRTaskType::WRITE);
    task.SetWriteTask("");
    task.state_changed_at_ms = 1758900001200ull;
    return task;
}

// ---------------------------------------------------------------------------
// 任务字段比对
// ---------------------------------------------------------------------------

// 返回首个不一致的字段名；全部一致返回空串。
std::string FirstTaskMismatch(const WR_task::WRTask& expected, const WR_task::WRTask& actual) {
    if (expected.task_id != actual.task_id) return "task_id";
    if (expected.type != actual.type) return "type";
    if (expected.state != actual.state) return "state";
    if (expected.disk_id != actual.disk_id) return "disk_id";
    if (expected.volume_id != actual.volume_id) return "volume_id";
    if (expected.inode_id != actual.inode_id) return "inode_id";
    if (expected.inode_id_num != actual.inode_id_num) return "inode_id_num";
    if (expected.state_changed_at_ms != actual.state_changed_at_ms) return "state_changed_at_ms";
    if (expected.file_path != actual.file_path) return "file_path";
    if (expected.volume_ids != actual.volume_ids) return "volume_ids";
    if (expected.expected_image_size_bytes != actual.expected_image_size_bytes) {
        return "expected_image_size_bytes";
    }
    if (expected.last_error_code != actual.last_error_code) return "last_error_code";
    if (expected.last_error_detail != actual.last_error_detail) return "last_error_detail";
    if (expected.attempt_count != actual.attempt_count) return "attempt_count";
    if (expected.is_failed_terminal != actual.is_failed_terminal) return "is_failed_terminal";
    return std::string();
}

void RoundTripTask(const WR_task::WRTask& task, const std::string& label) {
    const std::vector<uint8_t> blob = SerializeWRTaskBlob(task);
    WR_task::WRTask parsed;
    Check(ParseWRTaskBlob(blob.data(), blob.size(), parsed), label + ": 完整 blob 解析失败");
    const std::string mismatch = FirstTaskMismatch(task, parsed);
    Check(mismatch.empty(), label + ": 字段不一致 " + mismatch);
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

void TestWriterTaskRoundTrip() {
    RoundTripTask(MakeReadReadyTask(), "READ/READY");
    RoundTripTask(MakeWriteTask(), "WRITE/START");
    RoundTripTask(MakeCDReadTask(), "CD_READ/LOADING");
    RoundTripTask(MakeDiscBurnTask(), "DISC_BURN/CD_BURNING");
    RoundTripTask(MakeCDBurnTask(), "CD_BURN/START");
    RoundTripTask(MakeReadBatchTask(), "READ_BATCH_BY_VOLUME");
    RoundTripTask(MakeFailedTask(), "READ/FAILED");
    RoundTripTask(MakeEmptyPayloadWriteTask(), "WRITE/empty inode");

    // inode_id_num 不入盘，必须由 inode_id 重新推导，且空 inode 落 INVALID_INODE_ID。
    const WR_task::WRTask read_task = MakeReadReadyTask();
    Check(read_task.inode_id_num == 12345ull, "样本自检: READ inode_id_num");
    const std::vector<uint8_t> read_blob = SerializeWRTaskBlob(read_task);
    WR_task::WRTask parsed_read;
    Check(ParseWRTaskBlob(read_blob.data(), read_blob.size(), parsed_read), "READ 解析失败");
    Check(parsed_read.inode_id_num == 12345ull, "inode_id_num 应由 inode_id 推导");

    const WR_task::WRTask cd_read_task = MakeCDReadTask();
    const std::vector<uint8_t> cd_blob = SerializeWRTaskBlob(cd_read_task);
    WR_task::WRTask parsed_cd;
    Check(ParseWRTaskBlob(cd_blob.data(), cd_blob.size(), parsed_cd), "CD_READ 解析失败");
    Check(parsed_cd.inode_id_num == WR_task::WRTask::INVALID_INODE_ID,
          "空 inode 应解析为 INVALID_INODE_ID");

    // 嵌入 '\0' 的字符串必须按长度而非 C 串还原。
    const WR_task::WRTask failed_task = MakeFailedTask();
    const std::vector<uint8_t> failed_blob = SerializeWRTaskBlob(failed_task);
    WR_task::WRTask parsed_failed;
    Check(ParseWRTaskBlob(failed_blob.data(), failed_blob.size(), parsed_failed), "FAILED 解析失败");
    Check(parsed_failed.last_error_detail.size() == 34 &&
              parsed_failed.last_error_detail[19] == '\0',
          "内嵌 '\\0' 的 last_error_detail 未按长度还原");
    Check(parsed_failed.is_failed_terminal && parsed_failed.attempt_count == 32,
          "FAILED 终态字段丢失");
}

void TestWriterTaskTruncationAndCorruption() {
    const std::vector<WR_task::WRTask> samples = {
        MakeReadReadyTask(), MakeWriteTask(),      MakeCDReadTask(),    MakeDiscBurnTask(),
        MakeCDBurnTask(),    MakeReadBatchTask(),  MakeFailedTask(),    MakeEmptyPayloadWriteTask(),
    };

    for (const WR_task::WRTask& task : samples) {
        const std::vector<uint8_t> blob = SerializeWRTaskBlob(task);
        Check(!blob.empty(), "blob 不应为空");

        // 任意严格前缀都必须解析失败：编码是自定界的，短一字节就凑不出完整字段。
        for (size_t len = 0; len < blob.size(); ++len) {
            WR_task::WRTask parsed_prefix;
            Check(!ParseWRTaskBlob(blob.data(), len, parsed_prefix),
                  "截断到 " + std::to_string(len) + "/" + std::to_string(blob.size()) +
                      " 字节的 blob 被误判为合法");
        }

        // 尾部多余字节同样必须拒绝（避免把新版本多写的字段当旧格式读）。
        std::vector<uint8_t> trailing = blob;
        trailing.push_back(0);
        WR_task::WRTask parsed;
        Check(!ParseWRTaskBlob(trailing.data(), trailing.size(), parsed),
              "尾部多余字节被误判为合法");
        // 完整 blob 必须成功，作为上面断言的自检。
        Check(ParseWRTaskBlob(blob.data(), blob.size(), parsed), "完整 blob 解析失败（自检）");

        // 空指针 / 零长度输入不得崩溃。
        Check(!ParseWRTaskBlob(nullptr, 0, parsed), "空指针 blob 被接受");

        // type(offset 8) / state(offset 12) 越界必须拒绝。
        std::vector<uint8_t> bad_type = blob;
        PatchU32(&bad_type, 8, 200);
        Check(!ParseWRTaskBlob(bad_type.data(), bad_type.size(), parsed), "越界 type 被接受");

        std::vector<uint8_t> bad_state = blob;
        PatchU32(&bad_state, 12, 200);
        Check(!ParseWRTaskBlob(bad_state.data(), bad_state.size(), parsed), "越界 state 被接受");
    }
}

void TestArchiveRequestRoundTrip() {
    // 空批次（files 为空）也要能往返。
    SendArchiveMetadataRequest empty_batch;
    empty_batch.batch_id = 77;
    const std::vector<uint8_t> empty_blob = SerializeArchiveRequestBlob(empty_batch);
    SendArchiveMetadataRequest parsed_empty;
    Check(ParseArchiveRequestBlob(empty_blob.data(), empty_blob.size(), parsed_empty),
          "空批次解析失败");
    Check(parsed_empty.batch_id == 77 && parsed_empty.files.empty(), "空批次字段不一致");

    // 多文件批次：逐字段往返（含空 target_node_id / target_disk_id 的边界样本）。
    SendArchiveMetadataRequest batch;
    batch.batch_id = 1024;
    batch.files = {
        ArchiveFileInfo{1001, 1024ull * 1024, 256ull * 1024, "real-node-1", "disk-3"},
        ArchiveFileInfo{1002, 4096, 4096, "real-node-2", ""},
        ArchiveFileInfo{1003, 0, 0, "", ""},
    };
    const std::vector<uint8_t> blob = SerializeArchiveRequestBlob(batch);
    SendArchiveMetadataRequest parsed;
    Check(ParseArchiveRequestBlob(blob.data(), blob.size(), parsed), "归档批次解析失败");
    Check(parsed.batch_id == batch.batch_id, "batch_id 不一致");
    Check(parsed.files.size() == batch.files.size(), "files 数量不一致");
    for (size_t i = 0; i < batch.files.size(); ++i) {
        Check(parsed.files[i].inode_id == batch.files[i].inode_id, "inode_id 不一致");
        Check(parsed.files[i].size == batch.files[i].size, "size 不一致");
        Check(parsed.files[i].object_unit_size == batch.files[i].object_unit_size,
              "object_unit_size 不一致");
        Check(parsed.files[i].target_node_id == batch.files[i].target_node_id,
              "target_node_id 不一致");
        Check(parsed.files[i].target_disk_id == batch.files[i].target_disk_id,
              "target_disk_id 不一致");
    }

    // 截断 / 尾部多余字节 / 空指针。
    for (size_t len = 0; len < blob.size(); ++len) {
        SendArchiveMetadataRequest truncated;
        Check(!ParseArchiveRequestBlob(blob.data(), len, truncated),
              "截断到 " + std::to_string(len) + " 字节的归档批次被误判为合法");
    }
    std::vector<uint8_t> trailing = blob;
    trailing.push_back(1);
    Check(!ParseArchiveRequestBlob(trailing.data(), trailing.size(), parsed),
          "归档批次尾部多余字节被接受");
    Check(!ParseArchiveRequestBlob(nullptr, 0, parsed), "空指针归档批次被接受");
}

void TestHeaderRoundTripAndRejection() {
    Check(RUNTIME_STATE_HEADER_SIZE == 24 && RUNTIME_STATE_SECTION_HEADER_SIZE == 12,
          "快照帧大小常量与实现不一致");

    RuntimeStateMetaHeader header;
    header.section_count = 5;
    header.created_at_ms = 1758900000000ull;
    std::vector<uint8_t> bytes;
    header.Serialize(bytes);
    Check(bytes.size() == RUNTIME_STATE_HEADER_SIZE, "文件头长度不符");

    RuntimeStateMetaHeader parsed;
    Check(RuntimeStateMetaHeader::Parse(bytes.data(), bytes.size(), parsed), "文件头解析失败");
    Check(parsed.magic_number == RUNTIME_STATE_MAGIC, "魔数不一致");
    Check(parsed.format_version == RUNTIME_STATE_FORMAT_VERSION, "格式版本不一致");
    Check(parsed.section_count == 5 && parsed.created_at_ms == 1758900000000ull,
          "文件头字段不一致");

    // 缓冲区过短 / 空指针 / 魔数损坏 / 版本不符，均须拒绝。
    for (size_t len = 0; len < bytes.size(); ++len) {
        RuntimeStateMetaHeader short_header;
        Check(!RuntimeStateMetaHeader::Parse(bytes.data(), len, short_header),
              "过短文件头被接受 len=" + std::to_string(len));
    }
    RuntimeStateMetaHeader null_header;
    Check(!RuntimeStateMetaHeader::Parse(nullptr, RUNTIME_STATE_HEADER_SIZE, null_header),
          "空指针文件头被接受");

    std::vector<uint8_t> bad_magic = bytes;
    PatchU32(&bad_magic, 0, RUNTIME_STATE_MAGIC + 1);
    Check(!RuntimeStateMetaHeader::Parse(bad_magic.data(), bad_magic.size(), null_header),
          "错误魔数被接受");

    std::vector<uint8_t> bad_version = bytes;
    PatchU32(&bad_version, 4, RUNTIME_STATE_FORMAT_VERSION + 1);
    Check(!RuntimeStateMetaHeader::Parse(bad_version.data(), bad_version.size(), null_header),
          "错误版本被接受");
}

void TestPrimitivesAndReaderBounds() {
    std::vector<uint8_t> buf;
    AppendU8(buf, 0xAB);
    AppendU32(buf, 0x11223344u);
    AppendU64(buf, 0x0102030405060708ull);
    AppendI32(buf, -12345);
    AppendString(buf, std::string("a\0b", 3));
    AppendString(buf, std::string());

    RuntimeStateReader reader(buf.data(), buf.size());
    uint8_t u8 = 0;
    uint32_t u32 = 0;
    uint64_t u64 = 0;
    int32_t i32 = 0;
    std::string str;
    Check(reader.ReadU8(&u8) && u8 == 0xAB, "u8 往返不一致");
    Check(reader.ReadU32(&u32) && u32 == 0x11223344u, "u32 往返不一致");
    Check(reader.ReadU64(&u64) && u64 == 0x0102030405060708ull, "u64 往返不一致");
    Check(reader.ReadI32(&i32) && i32 == -12345, "i32 往返不一致");
    Check(reader.ReadString(&str) && str.size() == 3 && str[1] == '\0',
          "内嵌 '\\0' 字符串往返不一致");
    Check(reader.ReadString(&str) && str.empty(), "空字符串往返不一致");
    Check(reader.remaining() == 0, "读取后仍有剩余字节");

    // 游标越界 / 空指针输出 / 伪长度 / 超长子读取器，一律 false 且不崩溃。
    Check(!reader.ReadU64(&u64), "越界读取被接受");
    RuntimeStateReader full(buf.data(), buf.size());
    Check(!full.ReadU64(nullptr), "空指针输出被接受");

    std::vector<uint8_t> fake_len;
    AppendU32(fake_len, 1000);
    RuntimeStateReader fake_reader(fake_len.data(), fake_len.size());
    Check(!fake_reader.ReadString(&str), "伪长度字符串被接受");

    RuntimeStateReader sub_src(buf.data(), buf.size());
    RuntimeStateReader sub;
    Check(!sub_src.SubReader(buf.size() + 1, &sub), "超长子读取器被接受");
    Check(!sub_src.SubReader(4, nullptr), "空指针子读取器被接受");
    Check(sub_src.SubReader(4, &sub) && sub.remaining() == 4, "合法子读取器失败");
}

// 验证「文件头 + 多 section」帧：读取侧按 tag/len/SubReader 切段后应恰好走完文件，
// 且声明长度超出剩余字节时子读取器失败（截断的快照不会越读）。
void TestSectionFramingRoundTrip() {
    const std::vector<uint8_t> counters_payload = {1, 2, 3, 4, 5};
    const std::vector<WR_task::WRTask> tasks = {MakeReadReadyTask(), MakeDiscBurnTask()};
    std::vector<uint8_t> tasks_payload;
    for (const WR_task::WRTask& task : tasks) {
        const std::vector<uint8_t> blob = SerializeWRTaskBlob(task);
        AppendU32(tasks_payload, static_cast<uint32_t>(blob.size()));
        tasks_payload.insert(tasks_payload.end(), blob.begin(), blob.end());
    }

    RuntimeStateMetaHeader header;
    header.section_count = 2;
    header.created_at_ms = 1758900002000ull;
    std::vector<uint8_t> file;
    header.Serialize(file);
    AppendSection(file, RuntimeStateSectionTag::kCounters, counters_payload);
    AppendSection(file, RuntimeStateSectionTag::kTasks, tasks_payload);
    Check(file.size() == RUNTIME_STATE_HEADER_SIZE + RUNTIME_STATE_SECTION_HEADER_SIZE +
                             counters_payload.size() + RUNTIME_STATE_SECTION_HEADER_SIZE +
                             tasks_payload.size(),
          "快照文件总长与帧常量不符");

    RuntimeStateReader reader(file.data() + RUNTIME_STATE_HEADER_SIZE,
                              file.size() - RUNTIME_STATE_HEADER_SIZE);

    uint32_t tag = 0;
    uint64_t len = 0;
    RuntimeStateReader section;
    Check(reader.ReadU32(&tag) &&
              tag == static_cast<uint32_t>(RuntimeStateSectionTag::kCounters),
          "第 1 段 tag 不符");
    Check(reader.ReadU64(&len) && len == counters_payload.size(), "第 1 段长度不符");
    Check(reader.SubReader(len, &section) && section.remaining() == counters_payload.size(),
          "第 1 段 payload 切片失败");

    Check(reader.ReadU32(&tag) && tag == static_cast<uint32_t>(RuntimeStateSectionTag::kTasks),
          "第 2 段 tag 不符");
    Check(reader.ReadU64(&len) && len == tasks_payload.size(), "第 2 段长度不符");
    Check(reader.SubReader(len, &section), "第 2 段 payload 切片失败");
    for (const WR_task::WRTask& task : tasks) {
        uint32_t record_len = 0;
        Check(section.ReadU32(&record_len), "任务记录长度读取失败");
        RuntimeStateReader record;
        Check(section.SubReader(record_len, &record), "任务记录切片失败");
        WR_task::WRTask parsed;
        Check(ParseWRTaskBlob(record.remaining_data(), record.remaining(), parsed),
              "section 内的任务记录解析失败");
        const std::string mismatch = FirstTaskMismatch(task, parsed);
        Check(mismatch.empty(), "section 内任务字段不一致 " + mismatch);
    }
    Check(section.remaining() == 0, "tasks section 读取后仍有剩余字节");
    Check(reader.remaining() == 0, "两段之后仍有剩余字节");

    // 快照在 section payload 中途被截断：声明的段长度超过剩余字节，切段必须失败
    // （这是「截断的快照不会越读、只需丢弃该段」的帧级保证）。
    const size_t second_section_header_offset = RUNTIME_STATE_HEADER_SIZE +
                                                RUNTIME_STATE_SECTION_HEADER_SIZE +
                                                counters_payload.size();
    {
        const size_t truncated_size =
            second_section_header_offset + RUNTIME_STATE_SECTION_HEADER_SIZE +
            tasks_payload.size() / 2;
        RuntimeStateReader truncated(file.data() + second_section_header_offset,
                                     truncated_size - second_section_header_offset);
        uint32_t truncated_tag = 0;
        uint64_t declared_len = 0;
        RuntimeStateReader truncated_section;
        Check(truncated.ReadU32(&truncated_tag) &&
                  truncated_tag == static_cast<uint32_t>(RuntimeStateSectionTag::kTasks) &&
                  truncated.ReadU64(&declared_len) && declared_len == tasks_payload.size(),
              "截断样本的 tag / 声明长度读取失败");
        Check(!truncated.SubReader(declared_len, &truncated_section),
              "被截断的 section 声明长度仍被接受");
    }
}

}  // namespace

int main() {
    try {
        TestHeaderRoundTripAndRejection();
        TestPrimitivesAndReaderBounds();
        TestWriterTaskRoundTrip();
        TestWriterTaskTruncationAndCorruption();
        TestArchiveRequestRoundTrip();
        TestSectionFramingRoundTrip();
        std::cout << "PASS: runtime_state 文件头 / 原语边界 / 任务往返 / 截断鲁棒性 / 归档批次 / section 帧\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "FAIL: " << e.what() << '\n';
        return 1;
    }
}
