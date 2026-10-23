#include <iostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "config.h"
#include "compiler.hh"
#include "masstree.hh"
#include "masstree_get.hh"
#include "masstree_insert.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "masstree_tcursor.hh"
#include "kvthread.hh"
#include "lhm_namespace.hh"
#include "path_key.hh"
#include "string.hh"
#include "timestamp.hh"

struct simple_table_params : Masstree::nodeparams<15, 15> {
    using value_type = uint64_t;
    using value_print_type = Masstree::value_print<value_type>;
    using threadinfo_type = ::threadinfo;
};

using table_type = Masstree::basic_table<simple_table_params>;
using cursor_type = Masstree::tcursor<simple_table_params>;
using Str = Masstree::Str;
using MasstreeLHM::entry_kind_name;
using MasstreeLHM::directory_root_debug_info;
using MasstreeLHM::inode_ref;
using MasstreeLHM::LhmNamespace;
using MasstreeLHM::namespace_entry;
using MasstreeLHM::ParsedPath;
using MasstreeLHM::PathKey;
using MasstreeLHM::readdir_record;

relaxed_atomic<mrcu_epoch_type> globalepoch(1);
relaxed_atomic<mrcu_epoch_type> active_epoch(1);
volatile bool recovering = false;
kvtimestamp_t initial_timestamp;

struct CollectScanner {
    std::vector<std::pair<std::string, uint64_t>>* out;

    template <typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {
    }

    bool visit_value(Str key, uint64_t value, threadinfo&) {
        out->emplace_back(std::string(key.data(), key.length()), value);
        return true;
    }
};

static std::string format_components(const std::vector<uint64_t>& components) {
    std::ostringstream out;
    for (size_t i = 0; i < components.size(); ++i) {
        if (i != 0) {
            out << '/';
        }
        out << components[i];
    }
    return out.str();
}

static std::string format_entry(const namespace_entry& entry) {
    std::ostringstream out;
    out << "{kind=" << entry_kind_name(entry.kind)
        << ", name=" << MasstreeLHM::entry_name(entry)
        << ", ref=(block=" << entry.ref.block_id
        << ", offset=" << entry.ref.offset << ")}";
    return out.str();
}

static std::string format_directory_root_debug(const directory_root_debug_info& info) {
    std::ostringstream out;
    out << "{found=" << (info.found ? "true" : "false")
        << ", kind=" << (info.is_leaf ? "leaf" : "internode")
        << ", has_meta=" << (info.has_meta ? "true" : "false")
        << ", height=" << info.height
        << ", size=" << info.size
        << ", child0_is_leaf=" << (info.child0_is_leaf ? "true" : "false")
        << ", child0_size=" << info.child0_size
        << ", child1_exists=" << (info.child1_exists ? "true" : "false")
        << ", child1_is_leaf=" << (info.child1_is_leaf ? "true" : "false")
        << ", child1_size=" << info.child1_size
        << "}";
    return out.str();
}

struct DecodedPathScanner {
    std::vector<std::pair<std::vector<uint64_t>, uint64_t>>* out;

    template <typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {
    }

    bool visit_value(Str key, uint64_t value, threadinfo&) {
        out->emplace_back(PathKey::decode(key), value);
        return true;
    }
};

// 统一的插入/覆盖辅助函数，用来减少测试代码里的重复样板。
// 这里直接复用 Masstree 原生 cursor 接口，方便后续把测试重点放在
// “key 的表达是否正确”而不是插入流程本身。
static void upsert_value(table_type& table, Str key, uint64_t value, threadinfo& ti) {
    cursor_type cursor(table, key);
    bool already_present = cursor.find_insert(ti);
    cursor.value() = value;
    if (already_present) {
        cursor.finish(0, ti);
    } else {
        fence();
        cursor.finish(1, ti);
    }
}

int main() {
    threadinfo* ti = threadinfo::make(threadinfo::TI_MAIN, 0);
    initial_timestamp = timestamp();

    {
        // 第一组测试：保留最原始的字符串 key 测试，作为基线行为。
        // 这组测试用于证明当前的改动没有破坏 Masstree 原本的
        // 插入、点查、扫描和删除语义。
        table_type table;
        table.initialize(*ti);

        std::vector<std::pair<std::string, uint64_t>> sample = {
            {"apple", 1},
            {"banana", 2},
            {"cherry", 3},
            {"date", 4}
        };

        for (const auto& entry : sample) {
            upsert_value(table, Str(entry.first), entry.second, *ti);
        }

        std::cout << "String key point lookups:\n";
        for (const auto& entry : sample) {
            uint64_t value = 0;
            bool found = table.get(Str(entry.first), value, *ti);
            std::cout << "  get(" << entry.first << ") -> "
                      << (found ? std::to_string(value) : "not found") << '\n';
        }

        CollectScanner scanner;
        std::vector<std::pair<std::string, uint64_t>> collected;
        scanner.out = &collected;
        table.scan(Str(""), false, scanner, *ti);
        std::cout << "String key full scan:\n";
        for (const auto& kv : collected) {
            std::cout << "  " << kv.first << " -> " << kv.second << '\n';
        }

        for (size_t i = 0; i < sample.size(); ++i) {
            if (i % 2 == 0) {
                Str key(sample[i].first);
                cursor_type cursor(table, key);
                if (cursor.find_locked(*ti)) {
                    cursor.finish(-1, *ti);
                } else {
                    cursor.finish(0, *ti);
                }
            }
        }

        std::cout << "String key after removals:\n";
        for (const auto& entry : sample) {
            uint64_t value = 0;
            bool found = table.get(Str(entry.first), value, *ti);
            std::cout << "  get(" << entry.first << ") -> "
                      << (found ? std::to_string(value) : "not found") << '\n';
        }

        table.destroy(*ti);
    }

    {
        // 第二组测试：直接使用手工构造的 vector<uint64_t> 路径分量。
        // 这组测试的目标是验证：
        // 1. PathKey 能否把路径分量数组编码成 Masstree 可接受的 key。
        // 2. 共享前缀的路径分量数组能否被正确插入和查询。
        // 3. scan 返回的二进制 key 能否被 decode 还原回原始分量数组。
        //
        // 如果这组测试通过，说明“vector 风格路径 key -> Masstree”
        // 这条链路已经打通。
        table_type path_table;
        path_table.initialize(*ti);

        std::vector<std::pair<PathKey, uint64_t>> path_sample = {
            {PathKey({1, 10, 100}), 1000},
            {PathKey({1, 10, 101}), 1001},
            {PathKey({1, 20, 200}), 2000},
            {PathKey({2, 30, 300}), 3000}
        };

        for (const auto& entry : path_sample) {
            upsert_value(path_table, entry.first.as_str(), entry.second, *ti);
        }

        std::cout << "Vector-style path key point lookups:\n";
        for (const auto& entry : path_sample) {
            uint64_t value = 0;
            bool found = path_table.get(entry.first.as_str(), value, *ti);
            std::cout << "  get(" << entry.first.debug_string() << ") -> "
                      << (found ? std::to_string(value) : "not found") << '\n';
        }

        PathKey shared_prefix_a({1, 10, 100});
        PathKey shared_prefix_b({1, 10, 101});
        uint64_t value_a = 0;
        uint64_t value_b = 0;
        bool found_a = path_table.get(shared_prefix_a.as_str(), value_a, *ti);
        bool found_b = path_table.get(shared_prefix_b.as_str(), value_b, *ti);
        std::cout << "Shared-prefix lookup check:\n";
        // 这里专门挑选前两条共享前缀的路径，验证它们在共享前缀条件下
        // 仍然可以被稳定地区分和命中。
        std::cout << "  " << shared_prefix_a.debug_string() << " -> "
                  << (found_a ? std::to_string(value_a) : "not found") << '\n';
        std::cout << "  " << shared_prefix_b.debug_string() << " -> "
                  << (found_b ? std::to_string(value_b) : "not found") << '\n';

        DecodedPathScanner scanner;
        std::vector<std::pair<std::vector<uint64_t>, uint64_t>> decoded;
        scanner.out = &decoded;
        path_table.scan(Str(""), false, scanner, *ti);
        std::cout << "Vector-style path key full scan:\n";
        // 如果 scan 后 decode 出来的组件数组和插入时一致，说明
        // PathKey 的编码与解码协议是自洽的。
        for (const auto& kv : decoded) {
            std::cout << "  [" << format_components(kv.first) << "] -> "
                      << kv.second << '\n';
        }

        path_table.destroy(*ti);
    }

    {
        // 第三组测试：从真实的绝对路径字符串出发，走完整的
        // “路径解析 -> 分量哈希 -> PathKey -> Masstree” 链路。
        // 这组测试用于证明：
        // 1. parse_absolute_path() 已经能把真实路径拆分并规范化。
        // 2. 每一级路径分量都能被稳定哈希成 64-bit 分量。
        // 3. 这些哈希分量可以被进一步编码成 Masstree key。
        // 4. 最终可以用原始路径字符串重新生成 key 并完成点查。
        table_type parsed_path_table;
        parsed_path_table.initialize(*ti);

        std::vector<std::pair<std::string, uint64_t>> file_paths = {
            {"/usr/local/bin/gcc", 11},
            {"/usr/local/bin/clang", 12},
            {"/usr/local/share/doc", 13},
            {"/var/log/messages", 14}
        };

        std::vector<ParsedPath> parsed_inputs;
        parsed_inputs.reserve(file_paths.size());
        for (const auto& entry : file_paths) {
            ParsedPath parsed = PathKey::parse_absolute_path(entry.first);
            upsert_value(parsed_path_table, PathKey(parsed.hashes).as_str(), entry.second, *ti);
            parsed_inputs.push_back(std::move(parsed));
        }

        std::cout << "Absolute path parser point lookups:\n";
        for (size_t i = 0; i < file_paths.size(); ++i) {
            // 这里不复用插入时缓存的 PathKey，而是再次从原始路径字符串
            // 重新生成 key 做查询，用来证明“路径字符串 -> key”的转换
            // 是稳定且可重现的。
            PathKey key = PathKey::from_absolute_path(file_paths[i].first);
            uint64_t value = 0;
            bool found = parsed_path_table.get(key.as_str(), value, *ti);
            std::cout << "  get(" << parsed_inputs[i].normalized_path << ") -> "
                      << (found ? std::to_string(value) : "not found") << '\n';
            std::cout << "    hashes=[" << format_components(parsed_inputs[i].hashes)
                      << "]\n";
        }

        ParsedPath prefix_a = PathKey::parse_absolute_path("/usr/local/bin/gcc");
        ParsedPath prefix_b = PathKey::parse_absolute_path("/usr/local/bin/clang");
        std::cout << "Parsed shared-prefix check:\n";
        // 这两条路径共享 /usr/local/bin 前缀。这里输出其哈希分量数组，
        // 是为了直观看到前三级分量相同、最后一级不同，从而说明
        // 当前解析器已经具有“层级语义映射”的效果。
        std::cout << "  /usr/local/bin/gcc components=["
                  << format_components(prefix_a.hashes) << "]\n";
        std::cout << "  /usr/local/bin/clang components=["
                  << format_components(prefix_b.hashes) << "]\n";

        DecodedPathScanner scanner;
        std::vector<std::pair<std::vector<uint64_t>, uint64_t>> decoded;
        scanner.out = &decoded;
        parsed_path_table.scan(Str(""), false, scanner, *ti);
        std::cout << "Absolute path parser full scan:\n";
        // scan 的结果如果能被反解成完整的哈希组件数组，说明当前
        // 路径解析器产出的 key 已经真正进入 Masstree 存储，而不是
        // 只停留在解析阶段。
        for (const auto& kv : decoded) {
            std::cout << "  [" << format_components(kv.first) << "] -> "
                      << kv.second << '\n';
        }

        parsed_path_table.destroy(*ti);
    }

    {
        // 第四组测试：把目录从“普通 value”推进到“Masstree layer 入口”。
        // 这组测试主要验证两件事：
        // 1. 空目录本身可以只依赖 layer 入口存在，而不是依赖目录 value 或后代文件。
        // 2. 文件仍然可以继续作为普通 value，挂在这些目录 layer 之下。
        LhmNamespace ns;
        ns.initialize(*ti);

        namespace_entry entry;
        bool ok = false;

        std::cout << "Namespace wrapper checks:\n";

        // 父目录不存在时，文件创建必须失败。
        ok = ns.creat_file("/usr/local/bin/gcc", MasstreeLHM::make_inode_ref(10, 100), *ti);
        std::cout << "  creat_file(/usr/local/bin/gcc) without parents -> "
                  << (ok ? "unexpected success" : "expected failure") << '\n';

        // 逐级创建空目录。这里的关键不只是 mkdir 成功，
        // 而是 mkdir 之后即使目录下还没有任何文件，lookup_directory 也必须成功，
        // 这说明目录已经不再依赖“目录 value”存在，而是依赖真实 layer 入口存在。
        ok = ns.mkdir("/usr", MasstreeLHM::make_inode_ref(1, 0), *ti);
        std::cout << "  mkdir(/usr) -> " << (ok ? "success" : "failure") << '\n';
        bool found_usr = ns.lookup_directory("/usr", entry, *ti);
        std::cout << "  lookup_directory(/usr) after mkdir -> "
                  << (found_usr ? format_entry(entry) : "not found") << '\n';
        ok = ns.mkdir("/usr/local", MasstreeLHM::make_inode_ref(2, 0), *ti);
        std::cout << "  mkdir(/usr/local) -> " << (ok ? "success" : "failure") << '\n';
        bool found_local = ns.lookup_directory("/usr/local", entry, *ti);
        std::cout << "  lookup_directory(/usr/local) after mkdir -> "
                  << (found_local ? format_entry(entry) : "not found") << '\n';
        ok = ns.mkdir("/usr/local/bin", MasstreeLHM::make_inode_ref(3, 0), *ti);
        std::cout << "  mkdir(/usr/local/bin) -> " << (ok ? "success" : "failure") << '\n';
        bool found_bin_before_files = ns.lookup_directory("/usr/local/bin", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/bin) before files -> "
                  << (found_bin_before_files ? format_entry(entry) : "not found") << '\n';
        ok = ns.mkdir("/usr/local/share", MasstreeLHM::make_inode_ref(4, 0), *ti);
        std::cout << "  mkdir(/usr/local/share) -> " << (ok ? "success" : "failure") << '\n';
        bool found_share_before_files = ns.lookup_directory("/usr/local/share", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/share) before files -> "
                  << (found_share_before_files ? format_entry(entry) : "not found") << '\n';
        std::vector<readdir_record> local_children_before_files = ns.readdir("/usr/local", *ti);
        std::cout << "  readdir(/usr/local) after mkdir only:\n";
        for (const auto& record : local_children_before_files) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        // 继续在 /usr/local 下批量创建多个子目录，强制目录 root 从 leaf root
        // 长成 internode root，用来验证 root split 之后目录元数据仍能跟着新 root 走。
        std::vector<std::string> overflow_dirs = {
            "/usr/local/tmp0",
            "/usr/local/tmp1",
            "/usr/local/tmp2",
            "/usr/local/tmp3",
            "/usr/local/tmp4",
            "/usr/local/tmp5"
        };
        for (size_t i = 0; i < overflow_dirs.size(); ++i) {
            ok = ns.mkdir(overflow_dirs[i], MasstreeLHM::make_inode_ref(30 + i, 0), *ti);
            std::cout << "  mkdir(" << overflow_dirs[i] << ") -> "
                      << (ok ? "success" : "failure") << '\n';
        }
        bool found_tmp5 = ns.lookup_directory("/usr/local/tmp5", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/tmp5) after root split -> "
                  << (found_tmp5 ? format_entry(entry) : "not found") << '\n';
        std::vector<readdir_record> local_children_after_split = ns.readdir("/usr/local", *ti);
        std::cout << "  readdir(/usr/local) after root split:\n";
        for (const auto& record : local_children_after_split) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        // 在这些已经内核化为 layer 的目录之下继续创建文件，
        // 用来验证“目录走 layer、文件走普通 value”的混合语义已经打通。
        ok = ns.creat_file("/usr/local/bin/gcc", MasstreeLHM::make_inode_ref(10, 100), *ti);
        std::cout << "  creat_file(/usr/local/bin/gcc) after mkdir -> "
                  << (ok ? "success" : "failure") << '\n';
        ok = ns.creat_file("/usr/local/bin/clang", MasstreeLHM::make_inode_ref(10, 101), *ti);
        std::cout << "  creat_file(/usr/local/bin/clang) -> "
                  << (ok ? "success" : "failure") << '\n';
        ok = ns.creat_file("/usr/local/share/doc", MasstreeLHM::make_inode_ref(11, 0), *ti);
        std::cout << "  creat_file(/usr/local/share/doc) -> "
                  << (ok ? "success" : "failure") << '\n';

        // 重复创建同一路径应当失败，避免无意覆盖已有目录项。
        ok = ns.creat_file("/usr/local/bin/gcc", MasstreeLHM::make_inode_ref(12, 0), *ti);
        std::cout << "  creat_file(/usr/local/bin/gcc) duplicate -> "
                  << (ok ? "unexpected success" : "expected failure") << '\n';

        bool found_dir = ns.lookup_directory("/usr/local/bin", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/bin) -> "
                  << (found_dir ? format_entry(entry) : "not found") << '\n';

        bool found_file = ns.lookup_file("/usr/local/bin/gcc", entry, *ti);
        std::cout << "  lookup_file(/usr/local/bin/gcc) -> "
                  << (found_file ? format_entry(entry) : "not found") << '\n';

        bool found_missing = ns.lookup_file("/usr/local/bin/clang", entry, *ti);
        std::cout << "  lookup_file(/usr/local/bin/clang) -> "
                  << (found_missing ? format_entry(entry) : "not found") << '\n';

        // 目录和文件现在共享同一条路径字符串入口，但目录查找已经优先走
        // layer 入口，而文件查找仍然走普通 value。
        bool wrong_kind = ns.lookup_file("/usr/local/bin", entry, *ti);
        std::cout << "  lookup_file(/usr/local/bin) on directory -> "
                  << (wrong_kind ? "unexpected success" : "expected failure") << '\n';

        // 最小 readdir 测试：当前先返回直接孩子的哈希分量和值类型，
        // 用来证明单表方案已经可以支撑“目录扫描到直接孩子”这一语义。
        std::vector<readdir_record> dir_entries = ns.readdir("/usr/local/bin", *ti);
        std::cout << "  readdir(/usr/local/bin):\n";
        for (const auto& record : dir_entries) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        std::vector<readdir_record> share_entries = ns.readdir("/usr/local/share", *ti);
        std::cout << "  readdir(/usr/local/share):\n";
        for (const auto& record : share_entries) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        std::vector<readdir_record> missing_dir_entries = ns.readdir("/usr/local/missing", *ti);
        std::cout << "  readdir(/usr/local/missing) -> "
                  << (missing_dir_entries.empty() ? "empty" : "unexpected entries") << '\n';

        // 目录 rename 当前开始优先走 O(1) 子树重挂路径：
        // 旧父目录摘掉 edge，新父目录挂上同一个 subtree root，
        // 并更新 root 尾随的 directory_meta。
        ok = ns.rename_path("/usr/local/bin", "/usr/local/tools", *ti);
        std::cout << "  rename(/usr/local/bin -> /usr/local/tools) -> "
                  << (ok ? "success" : "failure") << '\n';

        bool old_bin_found = ns.lookup_directory("/usr/local/bin", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/bin) after rename -> "
                  << (old_bin_found ? format_entry(entry) : "not found") << '\n';

        bool new_tools_found = ns.lookup_directory("/usr/local/tools", entry, *ti);
        std::cout << "  lookup_directory(/usr/local/tools) after rename -> "
                  << (new_tools_found ? format_entry(entry) : "not found") << '\n';

        bool renamed_gcc_found = ns.lookup_file("/usr/local/tools/gcc", entry, *ti);
        std::cout << "  lookup_file(/usr/local/tools/gcc) after rename -> "
                  << (renamed_gcc_found ? format_entry(entry) : "not found") << '\n';

        bool old_gcc_found = ns.lookup_file("/usr/local/bin/gcc", entry, *ti);
        std::cout << "  lookup_file(/usr/local/bin/gcc) after rename -> "
                  << (old_gcc_found ? format_entry(entry) : "not found") << '\n';

        std::vector<readdir_record> tools_entries = ns.readdir("/usr/local/tools", *ti);
        std::cout << "  readdir(/usr/local/tools):\n";
        for (const auto& record : tools_entries) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        // 定向测试：专门触发“删除后 gc_layer 收缩 root internode，并把目录 meta
        // 迁移到唯一 child root”这条路径。
        //
        // 这里构造 /gc 目录，并在其下创建 64 个空子目录，强制 /gc 的目录 root
        // 长到 height=2（root internode 的 child 也为 internode）。
        // 然后删除前 56 个目录，只保留最后 8 个连续孩子，促使旧 root 收缩为
        // 唯一 child internode。若这条路径成立，收缩后 /gc 应该仍是目录，
        // 并且 root height 会从 2 降到 1，同时保留目录 meta。
        ok = ns.mkdir("/gc", MasstreeLHM::make_inode_ref(100, 0), *ti);
        std::cout << "  mkdir(/gc) -> " << (ok ? "success" : "failure") << '\n';
        std::vector<std::string> gc_dirs;
        gc_dirs.reserve(64);
        for (int i = 0; i < 64; ++i) {
            std::ostringstream path;
            path << "/gc/d";
            if (i < 10) {
                path << '0';
            }
            path << i;
            gc_dirs.push_back(path.str());
            ok = ns.mkdir(gc_dirs.back(), MasstreeLHM::make_inode_ref(200 + i, 0), *ti);
            std::cout << "  mkdir(" << gc_dirs.back() << ") -> "
                      << (ok ? "success" : "failure") << '\n';
        }

        directory_root_debug_info gc_root_before{};
        bool gc_root_before_found = ns.debug_directory_root_info("/gc", gc_root_before, *ti);
        std::cout << "  debug_root(/gc) before deletions -> "
                  << (gc_root_before_found ? format_directory_root_debug(gc_root_before)
                                           : "not found")
                  << '\n';

        for (int i = 0; i < 56; ++i) {
            ok = ns.remove_path_for_test(gc_dirs[i], *ti);
            std::cout << "  remove(" << gc_dirs[i] << ") -> "
                      << (ok ? "success" : "failure") << '\n';
        }

        // gc_layer 通过 RCU callback 延后执行，这里主动推进一次 quiesce，
        // 让目录 root 收缩路径真正有机会运行。
        globalepoch.store(globalepoch.load() + 1);
        active_epoch.store(globalepoch.load());
        ti->rcu_quiesce();
        globalepoch.store(globalepoch.load() + 1);
        active_epoch.store(globalepoch.load());
        ti->rcu_quiesce();

        directory_root_debug_info gc_root_after{};
        bool gc_root_after_found = ns.debug_directory_root_info("/gc", gc_root_after, *ti);
        std::cout << "  debug_root(/gc) after deletions -> "
                  << (gc_root_after_found ? format_directory_root_debug(gc_root_after)
                                          : "not found")
                  << '\n';

        // 如果此时 root 还没有收缩到我们想观察的形态，就继续逐个删除剩余目录，
        // 直到 root 形态发生变化。我们希望看到的是：
        // height 从 2 降到 1，但根节点仍然是 internode。
        directory_root_debug_info previous_gc_shape = gc_root_after;
        for (int i = 56; i < 63; ++i) {
            ok = ns.remove_path_for_test(gc_dirs[i], *ti);
            std::cout << "  remove(" << gc_dirs[i] << ") during shrink -> "
                      << (ok ? "success" : "failure") << '\n';

            globalepoch.store(globalepoch.load() + 1);
            active_epoch.store(globalepoch.load());
            ti->rcu_quiesce();
            globalepoch.store(globalepoch.load() + 1);
            active_epoch.store(globalepoch.load());
            ti->rcu_quiesce();

            directory_root_debug_info current_gc_shape{};
            bool current_found = ns.debug_directory_root_info("/gc", current_gc_shape, *ti);
            if (!current_found) {
                std::cout << "  debug_root(/gc) during shrink -> not found\n";
                break;
            }
            if (current_gc_shape.is_leaf != previous_gc_shape.is_leaf
                || current_gc_shape.height != previous_gc_shape.height
                || current_gc_shape.size != previous_gc_shape.size
                || current_gc_shape.child0_is_leaf != previous_gc_shape.child0_is_leaf) {
                std::cout << "  debug_root(/gc) during shrink -> "
                          << format_directory_root_debug(current_gc_shape) << '\n';
            }
            previous_gc_shape = current_gc_shape;
        }

        directory_root_debug_info gc_root_final{};
        bool gc_root_final_found = ns.debug_directory_root_info("/gc", gc_root_final, *ti);
        std::cout << "  debug_root(/gc) final -> "
                  << (gc_root_final_found ? format_directory_root_debug(gc_root_final)
                                          : "not found")
                  << '\n';

        std::vector<readdir_record> gc_entries = ns.readdir("/gc", *ti);
        std::cout << "  readdir(/gc) after deletions:\n";
        for (const auto& record : gc_entries) {
            std::cout << "    child_name=" << record.child_name
                      << " child_hash=" << record.child_component_hash
                      << " full_hashes=[" << format_components(record.full_hashes) << "] "
                      << format_entry(record.entry) << '\n';
        }

        bool found_gc_d63 = ns.lookup_directory("/gc/d63", entry, *ti);
        std::cout << "  lookup_directory(/gc/d63) after gc_layer -> "
                  << (found_gc_d63 ? format_entry(entry) : "not found") << '\n';

        ns.destroy(*ti);
    }

    std::cout << "Masstree basic interface test complete." << std::endl;
    return 0;
}
