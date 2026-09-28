#pragma once

// 语料描述：prepare_archive_corpus.py 产出的 manifest.tsv / expected_pack_plan.tsv 的内存视图。
// fake real_node 按这里的对象分片提供数据；测试用这里的期望分组与原始字节做断言。

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace zb::optical_node::test {

// 单个 real_node 对象（对应一个 obj-<inode>-<index>.dat 文件）。
struct ObjectInfo {
    uint64_t object_index{0};
    uint64_t object_offset{0};  // 对象内偏移，语料为 0
    uint64_t length{0};
    std::string object_id;      // 形如 obj-<inode>-<index>
    std::string path;           // 对象文件绝对路径
};

// 一个归档文件（= 一个 inode）的分片布局。
struct CorpusFile {
    uint64_t inode_id{0};
    uint64_t file_size{0};
    uint64_t object_unit_size{0};
    uint64_t volume_index{0};
    std::string node_id;    // real_node 节点 id，例如 real-1
    std::string disk_id;    // 所在磁盘 id，例如 disk0
    std::string object_dir;
    std::vector<ObjectInfo> objects;
};

// 期望的打包分组（按卷）：脚本用 zlib level 9 复刻 volume_manager 的触发条件算出。
struct PackPlanGroup {
    uint64_t volume_index{0};
    std::vector<uint64_t> inodes;
};

class Corpus {
public:
    // 载入 <corpus_dir>/manifest.tsv 与 <corpus_dir>/expected_pack_plan.tsv。
    bool Load(const std::string& corpus_dir, std::string* error);

    const CorpusFile* Find(uint64_t inode_id) const;
    const ObjectInfo* FindObject(const std::string& object_id) const;

    // 按对象顺序拼回原始文件内容（用于比对读回的数据）。
    bool ReadFileContent(uint64_t inode_id, std::string* out) const;

    const std::vector<PackPlanGroup>& plan() const { return plan_; }
    const std::unordered_map<uint64_t, CorpusFile>& files() const { return files_; }

private:
    std::unordered_map<uint64_t, CorpusFile> files_;
    std::unordered_map<std::string, const ObjectInfo*> objects_by_id_;
    std::vector<PackPlanGroup> plan_;
};

}  // namespace zb::optical_node::test