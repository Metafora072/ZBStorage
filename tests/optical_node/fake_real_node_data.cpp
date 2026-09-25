#include "fake_real_node_data.h"

#include "optical_node_test_support.h"

#include <algorithm>
#include <fstream>
#include <functional>
#include <sstream>

namespace zb::optical_node::test {

namespace {

// 逐行读文本，跳过空行与 '#' 注释行。
bool ForEachDataLine(const std::string& path,
                     const std::function<bool(const std::string&)>& handle,
                     std::string* error) {
    std::ifstream input(path);
    if (!input.is_open()) {
        *error = "无法打开文件: " + path;
        return false;
    }
    std::string line;
    size_t line_no = 0;
    while (std::getline(input, line)) {
        ++line_no;
        const std::string trimmed = Trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }
        if (!handle(trimmed)) {
            *error = "解析失败 " + path + ":" + std::to_string(line_no);
            return false;
        }
    }
    return true;
}

}  // namespace

bool Corpus::Load(const std::string& corpus_dir, std::string* error) {
    std::string dir = corpus_dir;
    if (!dir.empty() && dir.back() != '/') {
        dir += '/';
    }

    // manifest.tsv: inode_id, file_size, object_unit_size, object_count,
    //               node_id, disk_id, volume_index, object_dir
    const bool manifest_ok = ForEachDataLine(
        dir + "manifest.tsv",
        [this, error](const std::string& line) {
            const std::vector<std::string> fields = SplitBy(line, '\t');
            if (fields.size() < 8) {
                *error = "manifest.tsv 字段数不足 8: " + line;
                return false;
            }
            CorpusFile file;
            file.inode_id = ::strtoull(fields[0].c_str(), nullptr, 10);
            file.file_size = ::strtoull(fields[1].c_str(), nullptr, 10);
            file.object_unit_size = ::strtoull(fields[2].c_str(), nullptr, 10);
            const uint64_t object_count = ::strtoull(fields[3].c_str(), nullptr, 10);
            file.node_id = Trim(fields[4]);
            file.disk_id = Trim(fields[5]);
            file.volume_index = ::strtoull(fields[6].c_str(), nullptr, 10);
            file.object_dir = Trim(fields[7]);

            uint64_t covered = 0;
            for (uint64_t index = 0; index < object_count; ++index) {
                ObjectInfo object;
                object.object_index = index;
                object.object_offset = 0;
                object.length = std::min(file.object_unit_size, file.file_size - covered);
                object.object_id = "obj-" + std::to_string(file.inode_id) + "-" +
                                   std::to_string(index);
                object.path = file.object_dir + "/" + object.object_id + ".dat";
                covered += object.length;
                file.objects.push_back(object);
            }
            if (covered != file.file_size) {
                *error = "manifest.tsv 分片覆盖不足 inode=" + std::to_string(file.inode_id);
                return false;
            }
            // 存入 files_ 后再登记对象指针，保证指针指向 map 内稳定的存储。
            CorpusFile& stored = files_[file.inode_id];
            stored = std::move(file);
            for (const ObjectInfo& object : stored.objects) {
                objects_by_id_[object.object_id] = &object;
            }
            return true;
        },
        error);
    if (!manifest_ok) {
        return false;
    }

    // expected_pack_plan.tsv: volume_index, count, inode_id,inode_id,...
    const bool plan_ok = ForEachDataLine(
        dir + "expected_pack_plan.tsv",
        [this, error](const std::string& line) {
            const std::vector<std::string> fields = SplitBy(line, '\t');
            if (fields.size() < 3) {
                *error = "expected_pack_plan.tsv 字段数不足 3: " + line;
                return false;
            }
            PackPlanGroup group;
            group.volume_index = ::strtoull(fields[0].c_str(), nullptr, 10);
            const uint64_t count = ::strtoull(fields[1].c_str(), nullptr, 10);
            group.inodes = ParseInodeCsv(fields[2]);
            if (group.inodes.size() != count) {
                *error = "expected_pack_plan.tsv count 与 inode 列表不一致: " + line;
                return false;
            }
            plan_.push_back(group);
            return true;
        },
        error);
    if (!plan_ok) {
        return false;
    }

    if (files_.empty() || plan_.empty()) {
        *error = "语料为空: " + dir;
        return false;
    }
    return true;
}

const CorpusFile* Corpus::Find(uint64_t inode_id) const {
    const auto it = files_.find(inode_id);
    return it == files_.end() ? nullptr : &it->second;
}

const ObjectInfo* Corpus::FindObject(const std::string& object_id) const {
    const auto it = objects_by_id_.find(object_id);
    return it == objects_by_id_.end() ? nullptr : it->second;
}

bool Corpus::ReadFileContent(uint64_t inode_id, std::string* out) const {
    const CorpusFile* file = Find(inode_id);
    if (file == nullptr || out == nullptr) {
        return false;
    }
    out->clear();
    out->reserve(static_cast<size_t>(file->file_size));
    for (const ObjectInfo& object : file->objects) {
        std::string piece;
        if (!ReadFileRange(object.path, object.object_offset, object.length, &piece)) {
            return false;
        }
        out->append(piece);
    }
    return out->size() == file->file_size;
}

}  // namespace zb::optical_node::test