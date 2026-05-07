#include <cstdint>
#include <iostream>
#include <string>

#include "mds/masstree_meta/MasstreeBulkMetaGenerator.h"

namespace {

std::string GetFlagValue(const std::string& arg, const std::string& name) {
    const std::string prefix = "--" + name + "=";
    return arg.rfind(prefix, 0) == 0 ? arg.substr(prefix.size()) : std::string();
}

bool ParseU64(const std::string& value, uint64_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint64_t>(std::stoull(value));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseU32(const std::string& value, uint32_t* out) {
    if (!out || value.empty()) {
        return false;
    }
    try {
        *out = static_cast<uint32_t>(std::stoul(value));
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseBool(const std::string& value, bool* out) {
    if (!out) {
        return false;
    }
    if (value == "1" || value == "true" || value == "TRUE" || value == "yes") {
        *out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "FALSE" || value == "no") {
        *out = false;
        return true;
    }
    return false;
}

void PrintUsage() {
    std::cerr
        << "Usage:\n"
        << "  masstree_meta_generate_tool"
        << " --output_root=<dir>"
        << " --namespace_id=<id>"
        << " --generation_id=<id>"
        << " --path_prefix=<path>"
        << " --inode_start=<u64>"
        << " [--source_mode=synthetic|path_list]"
        << " [--path_list_file=<path>]"
        << " [--repeat_dir_prefix=<prefix>]"
        << " [--path_list_leaf_nodes_are_files=true|false]"
        << " [--file_count=<u64>]"
        << " [--max_files_per_leaf_dir=<u32>]"
        << " [--max_subdirs_per_dir=<u32>]\n";
}

} // namespace

int main(int argc, char** argv) {
    zb::mds::MasstreeBulkMetaGenerator::Request request;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (const std::string value = GetFlagValue(arg, "output_root"); !value.empty()) {
            request.output_root = value;
        } else if (const std::string value = GetFlagValue(arg, "namespace_id"); !value.empty()) {
            request.namespace_id = value;
        } else if (const std::string value = GetFlagValue(arg, "generation_id"); !value.empty()) {
            request.generation_id = value;
        } else if (const std::string value = GetFlagValue(arg, "path_prefix"); !value.empty()) {
            request.path_prefix = value;
        } else if (const std::string value = GetFlagValue(arg, "source_mode"); !value.empty()) {
            request.source_mode = value;
        } else if (const std::string value = GetFlagValue(arg, "path_list_file"); !value.empty()) {
            request.path_list_file = value;
        } else if (const std::string value = GetFlagValue(arg, "repeat_dir_prefix"); !value.empty()) {
            request.repeat_dir_prefix = value;
        } else if (const std::string value = GetFlagValue(arg, "path_list_leaf_nodes_are_files"); !value.empty()) {
            if (!ParseBool(value, &request.path_list_leaf_nodes_are_files)) {
                std::cerr << "invalid --path_list_leaf_nodes_are_files\n";
                return 1;
            }
        } else if (const std::string value = GetFlagValue(arg, "inode_start"); !value.empty()) {
            if (!ParseU64(value, &request.inode_start)) {
                std::cerr << "invalid --inode_start\n";
                return 1;
            }
        } else if (const std::string value = GetFlagValue(arg, "file_count"); !value.empty()) {
            if (!ParseU64(value, &request.file_count)) {
                std::cerr << "invalid --file_count\n";
                return 1;
            }
        } else if (const std::string value = GetFlagValue(arg, "max_files_per_leaf_dir"); !value.empty()) {
            if (!ParseU32(value, &request.max_files_per_leaf_dir)) {
                std::cerr << "invalid --max_files_per_leaf_dir\n";
                return 1;
            }
        } else if (const std::string value = GetFlagValue(arg, "max_subdirs_per_dir"); !value.empty()) {
            if (!ParseU32(value, &request.max_subdirs_per_dir)) {
                std::cerr << "invalid --max_subdirs_per_dir\n";
                return 1;
            }
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage();
            return 0;
        } else {
            std::cerr << "unknown arg: " << arg << "\n";
            PrintUsage();
            return 1;
        }
    }

    if (request.output_root.empty() || request.namespace_id.empty() || request.generation_id.empty() ||
        request.path_prefix.empty() || request.inode_start == 0) {
        PrintUsage();
        return 1;
    }

    zb::mds::MasstreeBulkMetaGenerator generator;
    zb::mds::MasstreeBulkMetaGenerator::Result result;
    std::string error;
    if (!generator.Generate(request, &result, &error)) {
        std::cerr << "generate failed: " << error << "\n";
        return 1;
    }

    std::cout << "staging_dir=" << result.staging_dir << "\n";
    std::cout << "manifest_path=" << result.manifest_path << "\n";
    std::cout << "inode_records_path=" << result.inode_records_path << "\n";
    std::cout << "dentry_records_path=" << result.dentry_records_path << "\n";
    std::cout << "verify_manifest_path=" << result.verify_manifest_path << "\n";
    std::cout << "structure_stats_path=" << result.structure_stats_path << "\n";
    std::cout << "source_mode=" << result.source_mode << "\n";
    std::cout << "root_inode_id=" << result.root_inode_id << "\n";
    std::cout << "inode_min=" << result.inode_min << "\n";
    std::cout << "inode_max=" << result.inode_max << "\n";
    std::cout << "target_file_count=" << result.target_file_count << "\n";
    std::cout << "inode_count=" << result.inode_count << "\n";
    std::cout << "dentry_count=" << result.dentry_count << "\n";
    std::cout << "base_file_count=" << result.base_file_count << "\n";
    std::cout << "base_dir_count=" << result.base_dir_count << "\n";
    std::cout << "dir_count=" << result.dir_count << "\n";
    std::cout << "repeat_count=" << result.repeat_count << "\n";
    std::cout << "level1_dir_count=" << result.level1_dir_count << "\n";
    std::cout << "leaf_dir_count=" << result.leaf_dir_count << "\n";
    std::cout << "base_max_depth=" << result.base_max_depth << "\n";
    std::cout << "max_depth=" << result.max_depth << "\n";
    return 0;
}
