#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include "config.h"
#include "compiler.hh"
#include "directory_meta.hh"
#include "kvthread.hh"
#include "lhm_namespace.hh"
#include "path_key.hh"
#include "timestamp.hh"

using MasstreeLHM::LhmNamespace;
using MasstreeLHM::ParsedPath;
using MasstreeLHM::entry_is_directory;
using MasstreeLHM::inode_ref;
using MasstreeLHM::make_inode_ref;
using MasstreeLHM::namespace_entry;

relaxed_atomic<mrcu_epoch_type> globalepoch(1);
relaxed_atomic<mrcu_epoch_type> active_epoch(1);
volatile bool recovering = false;
kvtimestamp_t initial_timestamp;

struct benchmark_record {
    std::string path;
    bool is_directory;
    inode_ref ref;
};

struct benchmark_config {
    std::string input_path = "/mnt/batchtest/filepath/path_kv_ideep.txt";
    std::string summary_csv = "benchmark_summary.csv";
    size_t load_limit = 50000;
    size_t stat_ops = 20000;
    size_t ls_ops = 5000;
    size_t create_ops = 10000;
    size_t threads = 1;
    uint32_t create_block_base = 1000000000U;
    uint32_t delete_quiesce_interval = 256U;
    uint32_t seed = 42U;
};

struct dataset_plan {
    std::vector<benchmark_record> preload_records;
    std::vector<std::string> stat_paths;
    std::vector<std::string> ls_paths;
    std::vector<benchmark_record> create_records;
    size_t skipped_bad_lines = 0;
    size_t skipped_long_component = 0;
    size_t skipped_missing_parent = 0;
    size_t skipped_duplicate = 0;
};

struct local_metrics {
    uint64_t success = 0;
    uint64_t failure = 0;
    long double latency_ns = 0;
};

struct benchmark_summary {
    std::string operation;
    size_t threads = 0;
    size_t requested_ops = 0;
    size_t attempted_ops = 0;
    uint64_t success = 0;
    uint64_t failure = 0;
    double elapsed_seconds = 0;
    double throughput_ops = 0;
    double avg_latency_us = 0;
};

static std::string parent_path(const std::string& normalized_path) {
    if (normalized_path == "/") {
        return "/";
    }
    size_t pos = normalized_path.find_last_of('/');
    if (pos == 0) {
        return "/";
    }
    return normalized_path.substr(0, pos);
}

static bool path_components_fit(const std::string& path) {
    if (path.empty() || path == "/") {
        return true;
    }
    size_t start = 1;
    while (start <= path.size()) {
        size_t end = path.find('/', start);
        if (end == std::string::npos) {
            end = path.size();
        }
        if (end > start && end - start > MasstreeLHM::kMaxEntryNameBytes) {
            return false;
        }
        start = end + 1;
    }
    return true;
}

static bool parse_uint32_field(const std::string& json, const std::string& key,
                               uint32_t& out) {
    size_t pos = json.find(key);
    if (pos == std::string::npos) {
        return false;
    }
    pos += key.size();
    size_t end = pos;
    while (end < json.size() && json[end] >= '0' && json[end] <= '9') {
        ++end;
    }
    if (end == pos) {
        return false;
    }
    out = static_cast<uint32_t>(strtoul(json.substr(pos, end - pos).c_str(), nullptr, 10));
    return true;
}

static bool parse_perm_type(const std::string& json, char& out) {
    static const std::string marker = "\"perm\":\"";
    size_t pos = json.find(marker);
    if (pos == std::string::npos || pos + marker.size() >= json.size()) {
        return false;
    }
    out = json[pos + marker.size()];
    return true;
}

static bool parse_dataset_line(const std::string& line, benchmark_record& out) {
    size_t sep = line.find('\t');
    if (sep == std::string::npos) {
        return false;
    }

    std::string raw_path = line.substr(0, sep);
    const std::string json = line.substr(sep + 1);
    uint32_t inode = 0;
    char perm_type = '\0';
    if (!parse_uint32_field(json, "\"inode\":", inode) || !parse_perm_type(json, perm_type)) {
        return false;
    }

    out.path = raw_path.empty() ? "/" : raw_path;
    out.is_directory = perm_type == 'd';
    out.ref = make_inode_ref(inode, 0);
    return true;
}

template <typename T>
static void shuffle_and_trim(std::vector<T>& items, size_t wanted, std::mt19937& rng) {
    std::shuffle(items.begin(), items.end(), rng);
    if (items.size() > wanted) {
        items.resize(wanted);
    }
}

static dataset_plan build_dataset_plan(const benchmark_config& config) {
    dataset_plan plan;
    std::ifstream input(config.input_path.c_str());
    if (!input) {
        throw std::runtime_error("无法打开输入数据文件: " + config.input_path);
    }

    std::unordered_set<std::string> loaded_paths;
    std::unordered_set<std::string> loaded_directories;
    loaded_paths.insert("/");
    loaded_directories.insert("/");

    std::vector<std::string> all_loaded_paths;
    std::vector<std::string> all_loaded_directories;
    all_loaded_paths.push_back("/");
    all_loaded_directories.push_back("/");

    std::string line;
    bool first_line = true;
    while (std::getline(input, line)) {
        if (first_line) {
            first_line = false;
            continue;
        }

        benchmark_record record;
        if (!parse_dataset_line(line, record)) {
            ++plan.skipped_bad_lines;
            continue;
        }
        if (record.path == "/") {
            continue;
        }
        if (!path_components_fit(record.path)) {
            ++plan.skipped_long_component;
            continue;
        }

        std::string parent = parent_path(record.path);
        if (loaded_directories.find(parent) == loaded_directories.end()) {
            ++plan.skipped_missing_parent;
            continue;
        }

        if (loaded_paths.find(record.path) != loaded_paths.end()) {
            ++plan.skipped_duplicate;
            continue;
        }

        if (plan.preload_records.size() >= config.load_limit) {
            break;
        }

        plan.preload_records.push_back(record);
        loaded_paths.insert(record.path);
        all_loaded_paths.push_back(record.path);
        if (record.is_directory) {
            loaded_directories.insert(record.path);
            all_loaded_directories.push_back(record.path);
        }
    }

    std::mt19937 rng(config.seed);
    plan.stat_paths = all_loaded_paths;
    shuffle_and_trim(plan.stat_paths, std::min(config.stat_ops, all_loaded_paths.size()), rng);

    plan.ls_paths = all_loaded_directories;
    shuffle_and_trim(plan.ls_paths, std::min(config.ls_ops, all_loaded_directories.size()), rng);

    // create/delete 基准当前先聚焦普通文件。
    // 这里直接基于“已加载完成的目录集合”构造一批稳定的新文件路径，
    // 避免为了找候选再把 84GB 输入文件继续向后顺序扫完整。
    if (all_loaded_directories.empty()) {
        all_loaded_directories.push_back("/");
    }
    for (size_t i = 0; i < config.create_ops; ++i) {
        const std::string& parent = all_loaded_directories[i % all_loaded_directories.size()];
        std::ostringstream name;
        name << "__bench_create_" << i;
        benchmark_record record;
        record.is_directory = false;
        record.ref = make_inode_ref(config.create_block_base + static_cast<uint32_t>(i), 0);
        if (parent == "/") {
            record.path = "/" + name.str();
        } else {
            record.path = parent + "/" + name.str();
        }
        if (loaded_paths.find(record.path) != loaded_paths.end()) {
            continue;
        }
        plan.create_records.push_back(std::move(record));
    }

    return plan;
}

static void load_namespace(LhmNamespace& ns, const dataset_plan& plan, threadinfo& ti) {
    for (size_t i = 0; i < plan.preload_records.size(); ++i) {
        const benchmark_record& record = plan.preload_records[i];
        bool ok = record.is_directory
            ? ns.mkdir(record.path, record.ref, ti)
            : ns.creat_file(record.path, record.ref, ti);
        if (!ok) {
            std::ostringstream out;
            out << "预加载失败，索引=" << i << " 路径=" << record.path;
            throw std::runtime_error(out.str());
        }
    }
}

template <typename WorkFn>
static benchmark_summary run_parallel_benchmark(const std::string& operation_name,
                                                size_t requested_ops,
                                                size_t thread_count,
                                                WorkFn work_fn) {
    benchmark_summary summary;
    summary.operation = operation_name;
    summary.threads = thread_count;
    summary.requested_ops = requested_ops;
    summary.attempted_ops = requested_ops;

    if (requested_ops == 0) {
        return summary;
    }

    std::vector<std::thread> workers;
    std::vector<local_metrics> locals(thread_count);
    std::atomic<size_t> ready_count(0);
    std::atomic<bool> start_flag(false);

    workers.reserve(thread_count);
    for (size_t tid = 0; tid < thread_count; ++tid) {
        size_t begin = requested_ops * tid / thread_count;
        size_t end = requested_ops * (tid + 1) / thread_count;
        workers.emplace_back([&, tid, begin, end]() {
            threadinfo* ti = threadinfo::make(threadinfo::TI_PROCESS,
                                              static_cast<int>(tid + 1));
            ready_count.fetch_add(1, std::memory_order_release);
            while (!start_flag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            local_metrics local;
            for (size_t i = begin; i < end; ++i) {
                auto op_begin = std::chrono::steady_clock::now();
                bool ok = work_fn(i, *ti);
                auto op_end = std::chrono::steady_clock::now();
                local.latency_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(
                    op_end - op_begin).count();
                if (ok) {
                    ++local.success;
                } else {
                    ++local.failure;
                }
            }
            ti->rcu_quiesce();
            locals[tid] = local;
        });
    }

    while (ready_count.load(std::memory_order_acquire) != thread_count) {
        std::this_thread::yield();
    }

    auto bench_begin = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);
    for (size_t i = 0; i < workers.size(); ++i) {
        workers[i].join();
    }
    auto bench_end = std::chrono::steady_clock::now();

    summary.elapsed_seconds = std::chrono::duration_cast<std::chrono::duration<double> >(
        bench_end - bench_begin).count();
    for (size_t i = 0; i < locals.size(); ++i) {
        summary.success += locals[i].success;
        summary.failure += locals[i].failure;
        summary.avg_latency_us += static_cast<double>(locals[i].latency_ns);
    }
    if (summary.elapsed_seconds > 0) {
        summary.throughput_ops = summary.attempted_ops / summary.elapsed_seconds;
    }
    if (summary.attempted_ops > 0) {
        summary.avg_latency_us /= static_cast<double>(summary.attempted_ops);
        summary.avg_latency_us /= 1000.0;
    }
    return summary;
}

static benchmark_summary benchmark_stat(LhmNamespace& ns, const std::vector<std::string>& paths,
                                        size_t threads) {
    return run_parallel_benchmark("stat", paths.size(), threads,
                                  [&](size_t idx, threadinfo& ti) -> bool {
        namespace_entry entry;
        return ns.lookup_entry(paths[idx], entry, ti);
    });
}

static benchmark_summary benchmark_ls(LhmNamespace& ns, const std::vector<std::string>& paths,
                                      size_t threads) {
    return run_parallel_benchmark("ls", paths.size(), threads,
                                  [&](size_t idx, threadinfo& ti) -> bool {
        std::vector<MasstreeLHM::readdir_record> result = ns.readdir(paths[idx], ti);
        (void) result;
        return true;
    });
}

static benchmark_summary benchmark_create(LhmNamespace& ns,
                                          const std::vector<benchmark_record>& records,
                                          size_t threads) {
    return run_parallel_benchmark("create", records.size(), threads,
                                  [&](size_t idx, threadinfo& ti) -> bool {
        return ns.creat_file(records[idx].path, records[idx].ref, ti);
    });
}

static benchmark_summary benchmark_delete(LhmNamespace& ns,
                                          const std::vector<benchmark_record>& records,
                                          size_t threads,
                                          uint32_t quiesce_interval) {
    return run_parallel_benchmark("delete", records.size(), threads,
                                  [&](size_t idx, threadinfo& ti) -> bool {
        bool ok = ns.remove_path_for_test(records[idx].path, ti);
        if (quiesce_interval != 0 && ((idx + 1) % quiesce_interval) == 0) {
            ti.rcu_quiesce();
        }
        return ok;
    });
}

static void write_summary_csv(const benchmark_config& config,
                              const dataset_plan& plan,
                              const std::vector<benchmark_summary>& summaries) {
    std::ofstream out(config.summary_csv.c_str());
    if (!out) {
        throw std::runtime_error("无法写出 summary CSV: " + config.summary_csv);
    }

    out << "operation,threads,requested_ops,attempted_ops,success,failure,"
           "elapsed_seconds,throughput_ops_per_sec,avg_latency_us,load_limit,"
           "loaded_records,skipped_bad_lines,skipped_long_component,"
           "skipped_missing_parent,skipped_duplicate,input_path\n";

    for (size_t i = 0; i < summaries.size(); ++i) {
        const benchmark_summary& s = summaries[i];
        out << s.operation << ','
            << s.threads << ','
            << s.requested_ops << ','
            << s.attempted_ops << ','
            << s.success << ','
            << s.failure << ','
            << std::fixed << std::setprecision(6) << s.elapsed_seconds << ','
            << std::fixed << std::setprecision(3) << s.throughput_ops << ','
            << std::fixed << std::setprecision(3) << s.avg_latency_us << ','
            << config.load_limit << ','
            << plan.preload_records.size() << ','
            << plan.skipped_bad_lines << ','
            << plan.skipped_long_component << ','
            << plan.skipped_missing_parent << ','
            << plan.skipped_duplicate << ','
            << '"' << config.input_path << '"' << '\n';
    }
}

static void print_summary(const dataset_plan& plan,
                          const std::vector<benchmark_summary>& summaries) {
    std::cout << "Dataset preparation summary:\n";
    std::cout << "  preload_records=" << plan.preload_records.size() << '\n';
    std::cout << "  stat_paths=" << plan.stat_paths.size() << '\n';
    std::cout << "  ls_paths=" << plan.ls_paths.size() << '\n';
    std::cout << "  create_records=" << plan.create_records.size() << '\n';
    std::cout << "  skipped_bad_lines=" << plan.skipped_bad_lines << '\n';
    std::cout << "  skipped_long_component=" << plan.skipped_long_component << '\n';
    std::cout << "  skipped_missing_parent=" << plan.skipped_missing_parent << '\n';
    std::cout << "  skipped_duplicate=" << plan.skipped_duplicate << '\n';

    std::cout << "\nBenchmark summary:\n";
    for (size_t i = 0; i < summaries.size(); ++i) {
        const benchmark_summary& s = summaries[i];
        std::cout << "  " << s.operation
                  << ": ops=" << s.attempted_ops
                  << ", success=" << s.success
                  << ", failure=" << s.failure
                  << ", throughput=" << std::fixed << std::setprecision(3)
                  << s.throughput_ops << " ops/s"
                  << ", avg_latency=" << std::fixed << std::setprecision(3)
                  << s.avg_latency_us << " us\n";
    }
}

static benchmark_config parse_args(int argc, char** argv) {
    benchmark_config config;
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        auto need_value = [&](const char* name) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error(std::string("参数缺少取值: ") + name);
            }
            return std::string(argv[++i]);
        };

        if (arg == "--input") {
            config.input_path = need_value("--input");
        } else if (arg == "--summary-csv") {
            config.summary_csv = need_value("--summary-csv");
        } else if (arg == "--load-limit") {
            config.load_limit = static_cast<size_t>(strtoull(need_value("--load-limit").c_str(), nullptr, 10));
        } else if (arg == "--stat-ops") {
            config.stat_ops = static_cast<size_t>(strtoull(need_value("--stat-ops").c_str(), nullptr, 10));
        } else if (arg == "--ls-ops") {
            config.ls_ops = static_cast<size_t>(strtoull(need_value("--ls-ops").c_str(), nullptr, 10));
        } else if (arg == "--create-ops") {
            config.create_ops = static_cast<size_t>(strtoull(need_value("--create-ops").c_str(), nullptr, 10));
        } else if (arg == "--threads") {
            config.threads = static_cast<size_t>(strtoull(need_value("--threads").c_str(), nullptr, 10));
        } else if (arg == "--seed") {
            config.seed = static_cast<uint32_t>(strtoul(need_value("--seed").c_str(), nullptr, 10));
        } else {
            throw std::runtime_error("未知参数: " + arg);
        }
    }

    if (config.threads == 0) {
        config.threads = 1;
    }
    return config;
}

int main(int argc, char** argv) {
    try {
        benchmark_config config = parse_args(argc, argv);
        initial_timestamp = timestamp();

        dataset_plan plan = build_dataset_plan(config);
        threadinfo* main_ti = threadinfo::make(threadinfo::TI_MAIN, 0);

        LhmNamespace ns;
        ns.initialize(*main_ti);
        load_namespace(ns, plan, *main_ti);

        std::vector<benchmark_summary> summaries;
        summaries.push_back(benchmark_stat(ns, plan.stat_paths, config.threads));
        summaries.push_back(benchmark_ls(ns, plan.ls_paths, config.threads));
        summaries.push_back(benchmark_create(ns, plan.create_records, config.threads));
        summaries.push_back(benchmark_delete(ns, plan.create_records, config.threads,
                                             config.delete_quiesce_interval));

        write_summary_csv(config, plan, summaries);
        print_summary(plan, summaries);

        ns.destroy(*main_ti);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "benchmark error: " << ex.what() << '\n';
        return 1;
    }
}
