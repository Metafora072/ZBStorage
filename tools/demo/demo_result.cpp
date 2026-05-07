#include "demo_result.h"

#include <algorithm>
#include <cctype>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <vector>

namespace zb::demo {

namespace {

std::vector<std::string> SplitLines(const std::string& text) {
    std::vector<std::string> lines;
    std::istringstream input(text);
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        lines.push_back(line);
    }
    return lines;
}

bool StartsWith(const std::string& value, const std::string& prefix) {
    return value.rfind(prefix, 0) == 0;
}

std::string Trim(const std::string& value) {
    const auto begin = std::find_if_not(value.begin(), value.end(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    });
    const auto end = std::find_if_not(value.rbegin(), value.rend(), [](unsigned char ch) {
        return std::isspace(ch) != 0;
    }).base();
    if (begin >= end) {
        return {};
    }
    return std::string(begin, end);
}

const std::unordered_map<std::string, std::string>& MetricDisplayNames() {
    static const std::unordered_map<std::string, std::string> kNames = {
        {"target_dir", "\u76ee\u6807\u76ee\u5f55"},
        {"repeat", "\u91cd\u590d\u6b21\u6570"},
        {"expected_file_size_bytes", "\u671f\u671b\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"chunk_size_bytes", "\u5757\u5927\u5c0f\u5b57\u8282"},
        {"verify_hash", "\u6821\u9a8c\u54c8\u5e0c"},
        {"keep_file", "\u4fdd\u7559\u6587\u4ef6"},
        {"sync_on_close", "\u5173\u95ed\u524d\u5237\u76d8"},
        {"mount_point", "\u6302\u8f7d\u70b9"},
        {"logical_path", "\u903b\u8f91\u8def\u5f84"},
        {"mounted_path", "\u6302\u8f7d\u70b9\u6587\u4ef6\u8def\u5f84"},
        {"bytes_written", "\u5199\u5165\u5b57\u8282\u6570"},
        {"bytes_read", "\u8bfb\u53d6\u5b57\u8282\u6570"},
        {"write_hash", "\u5199\u5165\u54c8\u5e0c"},
        {"read_hash", "\u8bfb\u53d6\u54c8\u5e0c"},
        {"write_latency_ms", "\u5199\u5165\u65f6\u5ef6\u6beb\u79d2"},
        {"read_latency_ms", "\u8bfb\u53d6\u65f6\u5ef6\u6beb\u79d2"},
        {"write_throughput_mib_s", "\u5199\u5165\u541e\u5410MiB\u6bcf\u79d2"},
        {"read_throughput_mib_s", "\u8bfb\u53d6\u541e\u5410MiB\u6bcf\u79d2"},
        {"total_bytes_written", "\u7d2f\u8ba1\u5199\u5165\u5b57\u8282\u6570"},
        {"total_bytes_read", "\u7d2f\u8ba1\u8bfb\u53d6\u5b57\u8282\u6570"},
        {"avg_write_latency_ms", "\u5e73\u5747\u5199\u5165\u65f6\u5ef6\u6beb\u79d2"},
        {"avg_read_latency_ms", "\u5e73\u5747\u8bfb\u53d6\u65f6\u5ef6\u6beb\u79d2"},
        {"avg_write_throughput_mib_s", "\u5e73\u5747\u5199\u5165\u541e\u5410MiB\u6bcf\u79d2"},
        {"avg_read_throughput_mib_s", "\u5e73\u5747\u8bfb\u53d6\u541e\u5410MiB\u6bcf\u79d2"},
        {"inode_id", "Inode\u53f7"},
        {"inspected_size_bytes", "\u6821\u9a8c\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"inspected_node_id", "\u6821\u9a8c\u8282\u70b9ID"},
        {"inspected_disk_id", "\u6821\u9a8c\u78c1\u76d8ID"},
        {"inspected_tier", "\u6821\u9a8c\u5c42\u7ea7"},
        {"object_unit_size_bytes", "\u5bf9\u8c61\u5355\u5143\u5927\u5c0f\u5b57\u8282"},
        {"object_count", "\u5bf9\u8c61\u6570\u91cf"},
        {"first_object_id", "\u9996\u4e2a\u5bf9\u8c61ID"},
        {"last_object_id", "\u672b\u4e2a\u5bf9\u8c61ID"},
        {"object_ids", "\u5bf9\u8c61ID\u5217\u8868"},
        {"backend_object_id", "\u540e\u7aef\u5bf9\u8c61ID"},
        {"backend_objects", "\u6d89\u53ca\u540e\u7aef\u5bf9\u8c61"},
        {"backend_mount_point", "\u540e\u7aef\u78c1\u76d8\u76ee\u5f55"},
        {"backend_object_path", "\u540e\u7aef\u8282\u70b9\u6587\u4ef6\u8def\u5f84"},
        {"backend_object_paths", "\u540e\u7aef\u8282\u70b9\u6587\u4ef6\u8def\u5f84\u5217\u8868"},
        {"backend_object_exists", "\u540e\u7aef\u5bf9\u8c61\u5b58\u5728"},
        {"backend_object_size_bytes", "\u540e\u7aef\u5bf9\u8c61\u5927\u5c0f\u5b57\u8282"},
        {"backend_object_hash", "\u540e\u7aef\u5bf9\u8c61\u54c8\u5e0c"},
        {"backend_dir_excerpt", "\u540e\u7aef\u76ee\u5f55\u6458\u5f55"},
        {"real_total_capacity_bytes", "\u771f\u5b9e\u5c42\u603b\u5bb9\u91cf\u5b57\u8282"},
        {"real_used_capacity_bytes", "\u771f\u5b9e\u5c42\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"real_free_capacity_bytes", "\u771f\u5b9e\u5c42\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"virtual_total_capacity_bytes", "\u865a\u62df\u5c42\u603b\u5bb9\u91cf\u5b57\u8282"},
        {"virtual_used_capacity_bytes", "\u865a\u62df\u5c42\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"virtual_free_capacity_bytes", "\u865a\u62df\u5c42\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"real_physical_nodes", "\u771f\u5b9e\u7269\u7406\u8282\u70b9\u6570"},
        {"real_logical_nodes", "\u771f\u5b9e\u903b\u8f91\u8282\u70b9\u6570"},
        {"real_disks", "\u771f\u5b9e\u78c1\u76d8\u6570"},
        {"virtual_logical_nodes", "\u865a\u62df\u903b\u8f91\u8282\u70b9\u6570"},
        {"virtual_disks", "\u865a\u62df\u78c1\u76d8\u6570"},
        {"online_logical_nodes", "\u5728\u7ebf\u903b\u8f91\u8282\u70b9\u6570"},
        {"optical_nodes", "\u5149\u8282\u70b9\u6570"},
        {"optical_devices", "\u5149\u8bbe\u5907\u6570"},
        {"cold_total_capacity_bytes", "\u51b7\u5c42\u603b\u5bb9\u91cf\u5b57\u8282"},
        {"cold_used_capacity_bytes", "\u51b7\u5c42\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"cold_free_capacity_bytes", "\u51b7\u5c42\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"total_file_count", "\u603b\u6587\u4ef6\u6570"},
        {"total_file_bytes", "\u603b\u6587\u4ef6\u5b57\u8282\u6570"},
        {"avg_file_size_bytes", "\u5e73\u5747\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"total_metadata_bytes", "\u603b\u5143\u6570\u636e\u5b57\u8282\u6570"},
        {"min_file_size_bytes", "\u6700\u5c0f\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"max_file_size_bytes", "\u6700\u5927\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"query_samples", "\u67e5\u8be2\u6837\u672c\u6570"},
        {"query_mode", "\u67e5\u8be2\u6a21\u5f0f"},
        {"query_success_count", "\u67e5\u8be2\u6210\u529f\u6570"},
        {"query_failure_count", "\u67e5\u8be2\u5931\u8d25\u6570"},
        {"query_success_rate", "\u67e5\u8be2\u6210\u529f\u7387"},
        {"total_query_latency", "\u603b\u67e5\u8be2\u65f6\u5ef6"},
        {"avg_query_latency", "\u5e73\u5747\u67e5\u8be2\u65f6\u5ef6"},
        {"min_query_latency", "\u6700\u5c0f\u65f6\u5ef6"},
        {"max_query_latency", "\u6700\u5927\u65f6\u5ef6"},
        {"sample_index", "\u6837\u672c\u5e8f\u53f7"},
        {"query_ok", "\u67e5\u8be2\u6210\u529f"},
        {"query_latency", "\u67e5\u8be2\u65f6\u5ef6"},
        {"status_code", "\u72b6\u6001\u7801"},
        {"status_text", "\u72b6\u6001\u4fe1\u606f"},
        {"namespace_id", "\u547d\u540d\u7a7a\u95f4ID"},
        {"full_path", "\u5b8c\u6574\u8def\u5f84"},
        {"path_prefix", "\u8def\u5f84\u524d\u7f00"},
        {"generation_id", "\u7248\u672cID"},
        {"file_name", "\u6587\u4ef6\u540d"},
        {"type", "\u7c7b\u578b"},
        {"mode", "\u6a21\u5f0f"},
        {"uid", "\u7528\u6237ID"},
        {"gid", "\u7ec4ID"},
        {"size_bytes", "\u5927\u5c0f\u5b57\u8282"},
        {"size_human", "\u5927\u5c0f"},
        {"atime", "\u8bbf\u95ee\u65f6\u95f4"},
        {"mtime", "\u4fee\u6539\u65f6\u95f4"},
        {"ctime", "\u53d8\u66f4\u65f6\u95f4"},
        {"nlink", "\u786c\u94fe\u63a5\u6570"},
        {"replica", "\u526f\u672c\u6570"},
        {"version", "\u7248\u672c\u53f7"},
        {"file_archive_state", "\u6587\u4ef6\u5f52\u6863\u72b6\u6001"},
        {"template_id", "\u6a21\u677fID"},
        {"template_mode", "\u6a21\u677f\u6a21\u5f0f"},
        {"path_list_leaf_nodes_are_files", "\u53f6\u5b50\u8282\u70b9\u6309\u6587\u4ef6\u5904\u7406"},
        {"job_id", "\u4efb\u52a1ID"},
        {"job_status", "\u4efb\u52a1\u72b6\u6001"},
        {"manifest_path", "\u6e05\u5355\u8def\u5f84"},
        {"root_inode_id", "\u6839Inode\u53f7"},
        {"inode_count", "Inode\u603b\u6570"},
        {"dentry_count", "Dentry\u603b\u6570"},
        {"level1_dir_count", "\u4e00\u7ea7\u76ee\u5f55\u6570"},
        {"leaf_dir_count", "\u53f6\u5b50\u76ee\u5f55\u6570"},
        {"file_count", "\u6587\u4ef6\u603b\u6570"},
        {"import_total_file_bytes", "\u5bfc\u5165\u6587\u4ef6\u603b\u5b57\u8282\u6570"},
        {"import_avg_file_size_bytes", "\u5bfc\u5165\u5e73\u5747\u6587\u4ef6\u5927\u5c0f\u5b57\u8282"},
        {"inode_range", "Inode\u8303\u56f4"},
        {"inode_pages_bytes", "Inode\u9875\u5b57\u8282\u6570"},
        {"previous_namespace_present", "\u5bfc\u5165\u524d\u547d\u540d\u7a7a\u95f4\u5df2\u5b58\u5728"},
        {"before_total_file_count", "\u5bfc\u5165\u524d\u603b\u6587\u4ef6\u6570"},
        {"before_total_file_bytes", "\u5bfc\u5165\u524d\u603b\u6587\u4ef6\u5b57\u8282\u6570"},
        {"before_total_metadata_bytes", "\u5bfc\u5165\u524d\u603b\u5143\u6570\u636e\u5b57\u8282\u6570"},
        {"before_used_capacity_bytes", "\u5bfc\u5165\u524d\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"before_free_capacity_bytes", "\u5bfc\u5165\u524d\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"after_total_file_count", "\u5bfc\u5165\u540e\u603b\u6587\u4ef6\u6570"},
        {"after_total_file_bytes", "\u5bfc\u5165\u540e\u603b\u6587\u4ef6\u5b57\u8282\u6570"},
        {"after_total_metadata_bytes", "\u5bfc\u5165\u540e\u603b\u5143\u6570\u636e\u5b57\u8282\u6570"},
        {"after_used_capacity_bytes", "\u5bfc\u5165\u540e\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"after_free_capacity_bytes", "\u5bfc\u5165\u540e\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"delta_total_file_count", "\u672c\u6b21\u65b0\u589e\u6587\u4ef6\u6570"},
        {"delta_total_file_bytes", "\u672c\u6b21\u65b0\u589e\u6587\u4ef6\u5b57\u8282\u6570"},
        {"delta_total_metadata_bytes", "\u672c\u6b21\u65b0\u589e\u5143\u6570\u636e\u5b57\u8282\u6570"},
        {"delta_used_capacity_bytes", "\u672c\u6b21\u65b0\u589e\u5df2\u7528\u5bb9\u91cf\u5b57\u8282"},
        {"delta_free_capacity_bytes", "\u672c\u6b21\u51cf\u5c11\u5269\u4f59\u5bb9\u91cf\u5b57\u8282"},
        {"mds_root_inode", "MDS\u6839Inode\u53f7"},
        {"cluster_generation", "\u96c6\u7fa4\u7248\u672c\u53f7"},
        {"online_nodes", "\u5728\u7ebf\u8282\u70b9\u6570"},
        {"real_root", "\u771f\u5b9e\u5c42\u6839\u76ee\u5f55"},
        {"virtual_root", "\u865a\u62df\u5c42\u6839\u76ee\u5f55"}
    };
    return kNames;
}

const std::unordered_map<std::string, std::string>& CheckDisplayNames() {
    static const std::unordered_map<std::string, std::string> kNames = {
        {"io.bytes_written", "\u5199\u5165\u5b57\u8282\u6570\u6821\u9a8c"},
        {"io.bytes_read", "\u8bfb\u53d6\u5b57\u8282\u6570\u6821\u9a8c"},
        {"io.hash_match", "\u8bfb\u5199\u54c8\u5e0c\u4e00\u81f4\u6027\u6821\u9a8c"},
        {"metadata.size_bytes", "\u5143\u6570\u636e\u5927\u5c0f\u6821\u9a8c"},
        {"metadata.tier", "\u5143\u6570\u636e\u5c42\u7ea7\u6821\u9a8c"},
        {"backend.object_exists", "\u540e\u7aef\u5bf9\u8c61\u5b58\u5728\u6821\u9a8c"},
        {"backend.object_size", "\u540e\u7aef\u5bf9\u8c61\u5927\u5c0f\u6821\u9a8c"},
        {"backend.object_hash", "\u540e\u7aef\u5bf9\u8c61\u54c8\u5e0c\u6821\u9a8c"},
        {"virtual.object_count", "\u865a\u62df\u5bf9\u8c61\u6570\u91cf\u6821\u9a8c"},
        {"virtual.object_ids", "\u865a\u62df\u5bf9\u8c61\u5217\u8868\u6821\u9a8c"}
    };
    return kNames;
}

std::string DisplayMetricKey(const std::string& key) {
    const auto& names = MetricDisplayNames();
    const auto it = names.find(key);
    if (it == names.end()) {
        return key;
    }
    return it->second + " (" + key + ")";
}

std::string DisplayCheckName(const std::string& name) {
    const auto& names = CheckDisplayNames();
    const auto it = names.find(name);
    if (it == names.end()) {
        return name;
    }
    return it->second + " (" + name + ")";
}

std::string GroupForMetric(const std::string& key) {
    static const std::unordered_map<std::string, std::string> kGroups = {
        {"target_dir", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"repeat", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"expected_file_size_bytes", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"chunk_size_bytes", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"verify_hash", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"keep_file", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"sync_on_close", "\u6d4b\u8bd5\u914d\u7f6e"},
        {"mount_point", "\u6d4b\u8bd5\u914d\u7f6e"},

        {"real_total_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},
        {"real_used_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},
        {"real_free_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},
        {"virtual_total_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},
        {"virtual_used_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},
        {"virtual_free_capacity_bytes", "\u5bb9\u91cf\u4fe1\u606f"},

        {"bytes_written", "\u5199\u5165\u7ed3\u679c"},
        {"bytes_read", "\u5199\u5165\u7ed3\u679c"},
        {"write_hash", "\u5199\u5165\u7ed3\u679c"},
        {"read_hash", "\u5199\u5165\u7ed3\u679c"},
        {"write_latency_ms", "\u5199\u5165\u7ed3\u679c"},
        {"read_latency_ms", "\u5199\u5165\u7ed3\u679c"},
        {"write_throughput_mib_s", "\u5199\u5165\u7ed3\u679c"},
        {"read_throughput_mib_s", "\u5199\u5165\u7ed3\u679c"},
        {"total_bytes_written", "\u5199\u5165\u7ed3\u679c"},
        {"total_bytes_read", "\u5199\u5165\u7ed3\u679c"},
        {"avg_write_latency_ms", "\u5199\u5165\u7ed3\u679c"},
        {"avg_read_latency_ms", "\u5199\u5165\u7ed3\u679c"},
        {"avg_write_throughput_mib_s", "\u5199\u5165\u7ed3\u679c"},
        {"avg_read_throughput_mib_s", "\u5199\u5165\u7ed3\u679c"},

        {"logical_path", "\u8def\u5f84\u6838\u9a8c"},
        {"mounted_path", "\u8def\u5f84\u6838\u9a8c"},

        {"inode_id", "\u5143\u6570\u636e\u6838\u9a8c"},
        {"inspected_size_bytes", "\u5143\u6570\u636e\u6838\u9a8c"},
        {"inspected_node_id", "\u5143\u6570\u636e\u6838\u9a8c"},
        {"inspected_disk_id", "\u5143\u6570\u636e\u6838\u9a8c"},
        {"inspected_tier", "\u5143\u6570\u636e\u6838\u9a8c"},

        {"object_unit_size_bytes", "\u865a\u62df\u5bf9\u8c61\u5207\u5206\u8ba1\u5212"},
        {"object_count", "\u865a\u62df\u5bf9\u8c61\u5207\u5206\u8ba1\u5212"},
        {"first_object_id", "\u865a\u62df\u5bf9\u8c61\u5207\u5206\u8ba1\u5212"},
        {"last_object_id", "\u865a\u62df\u5bf9\u8c61\u5207\u5206\u8ba1\u5212"},
        {"object_ids", "\u865a\u62df\u5bf9\u8c61\u5207\u5206\u8ba1\u5212"},

        {"backend_object_id", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_objects", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_mount_point", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_object_path", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_object_paths", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_object_exists", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_object_size_bytes", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_object_hash", "\u540e\u7aef\u6838\u9a8c"},
        {"backend_dir_excerpt", "\u540e\u7aef\u6838\u9a8c"},

        {"query_samples", "\u67e5\u8be2\u7ed3\u679c"},
        {"query_mode", "\u67e5\u8be2\u7ed3\u679c"},
        {"query_success_count", "\u67e5\u8be2\u7ed3\u679c"},
        {"query_failure_count", "\u67e5\u8be2\u7ed3\u679c"},
        {"query_success_rate", "\u67e5\u8be2\u7ed3\u679c"},
        {"total_query_latency", "\u67e5\u8be2\u7ed3\u679c"},
        {"avg_query_latency", "\u67e5\u8be2\u7ed3\u679c"},
        {"min_query_latency", "\u67e5\u8be2\u7ed3\u679c"},
        {"max_query_latency", "\u67e5\u8be2\u7ed3\u679c"},
        {"sample_index", "\u67e5\u8be2\u6837\u672c"},
        {"query_ok", "\u67e5\u8be2\u6837\u672c"},
        {"query_latency", "\u67e5\u8be2\u6837\u672c"},
        {"status_code", "\u67e5\u8be2\u6837\u672c"},
        {"status_text", "\u67e5\u8be2\u6837\u672c"},
        {"namespace_id", "\u67e5\u8be2\u6837\u672c"},
        {"full_path", "\u67e5\u8be2\u6837\u672c"},
        {"path_prefix", "\u67e5\u8be2\u6837\u672c"},
        {"generation_id", "\u67e5\u8be2\u6837\u672c"},
        {"file_name", "\u67e5\u8be2\u6837\u672c"},
        {"type", "\u67e5\u8be2\u6837\u672c"},
        {"mode", "\u67e5\u8be2\u6837\u672c"},
        {"uid", "\u67e5\u8be2\u6837\u672c"},
        {"gid", "\u67e5\u8be2\u6837\u672c"},
        {"size_bytes", "\u67e5\u8be2\u6837\u672c"},
        {"size_human", "\u67e5\u8be2\u6837\u672c"},
        {"atime", "\u67e5\u8be2\u6837\u672c"},
        {"mtime", "\u67e5\u8be2\u6837\u672c"},
        {"ctime", "\u67e5\u8be2\u6837\u672c"},
        {"nlink", "\u67e5\u8be2\u6837\u672c"},
        {"replica", "\u67e5\u8be2\u6837\u672c"},
        {"version", "\u67e5\u8be2\u6837\u672c"},
        {"file_archive_state", "\u67e5\u8be2\u6837\u672c"},

        {"template_id", "\u5bfc\u5165\u7ed3\u679c"},
        {"template_mode", "\u5bfc\u5165\u7ed3\u679c"},
        {"job_id", "\u5bfc\u5165\u7ed3\u679c"},
        {"job_status", "\u5bfc\u5165\u7ed3\u679c"},
        {"manifest_path", "\u5bfc\u5165\u7ed3\u679c"},
        {"root_inode_id", "\u5bfc\u5165\u7ed3\u679c"},
        {"inode_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"dentry_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"level1_dir_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"leaf_dir_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"file_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"import_total_file_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"import_avg_file_size_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"inode_range", "\u5bfc\u5165\u7ed3\u679c"},
        {"inode_pages_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"previous_namespace_present", "\u5bfc\u5165\u7ed3\u679c"},
        {"before_total_file_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"before_total_file_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"before_total_metadata_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"before_used_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"before_free_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"after_total_file_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"after_total_file_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"after_total_metadata_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"after_used_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"after_free_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"delta_total_file_count", "\u5bfc\u5165\u7ed3\u679c"},
        {"delta_total_file_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"delta_total_metadata_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"delta_used_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"},
        {"delta_free_capacity_bytes", "\u5bfc\u5165\u7ed3\u679c"}
    };
    const auto it = kGroups.find(key);
    return it == kGroups.end() ? "\u5173\u952e\u6307\u6807" : it->second;
}

void ParseStdout(const std::string& stdout_text,
                 std::vector<DemoMetric>* metrics,
                 std::vector<DemoCheck>* checks) {
    if (!metrics || !checks) {
        return;
    }
    for (const std::string& raw_line : SplitLines(stdout_text)) {
        const std::string line = Trim(raw_line);
        if (line.empty() || StartsWith(line, "====") || line == "{" || line == "}" || line == "attr={") {
            continue;
        }
        if (StartsWith(line, "check.") || StartsWith(line, "\u6821\u9a8c.")) {
            DemoCheck check;
            const size_t eq = line.find('=');
            if (eq == std::string::npos) {
                continue;
            }
            const size_t prefix_len = StartsWith(line, "\u6821\u9a8c.") ? std::string("\u6821\u9a8c.").size() : 6;
            check.name = line.substr(prefix_len, eq - prefix_len);
            const size_t detail_pos = line.find(" detail=\"", eq + 1);
            const std::string status = detail_pos == std::string::npos
                                           ? line.substr(eq + 1)
                                           : line.substr(eq + 1, detail_pos - (eq + 1));
            check.ok = status == "PASS" || status == "\u901a\u8fc7";
            if (detail_pos != std::string::npos) {
                const size_t detail_begin = detail_pos + 9;
                const size_t detail_end = !line.empty() && line.back() == '"' ? line.size() - 1 : line.size();
                check.detail = line.substr(detail_begin, detail_end - detail_begin);
            }
            checks->push_back(std::move(check));
            continue;
        }
        const size_t eq = line.find('=');
        if (eq == std::string::npos) {
            continue;
        }
        DemoMetric metric;
        metric.key = Trim(line.substr(0, eq));
        metric.value = Trim(line.substr(eq + 1));
        metrics->push_back(std::move(metric));
    }
}

void PrintMetricSection(const std::string& title, const std::vector<const DemoMetric*>& metrics) {
    if (metrics.empty()) {
        return;
    }
    std::cout << "\n[" << title << "]\n";
    size_t width = 0;
    for (const auto* metric : metrics) {
        width = std::max(width, DisplayMetricKey(metric->key).size());
    }
    for (const auto* metric : metrics) {
        const std::string display_key = DisplayMetricKey(metric->key);
        std::cout << std::left << std::setw(static_cast<int>(width + 2)) << display_key
                  << metric->value << '\n';
    }
}

void PrintMetrics(const std::vector<DemoMetric>& metrics) {
    if (metrics.empty()) {
        return;
    }
    static const std::vector<std::string> kGroupOrder = {
        "\u6d4b\u8bd5\u914d\u7f6e",
        "\u5bb9\u91cf\u4fe1\u606f",
        "\u5199\u5165\u7ed3\u679c",
        "\u8def\u5f84\u6838\u9a8c",
        "\u5143\u6570\u636e\u6838\u9a8c",
        "\u540e\u7aef\u6838\u9a8c",
        "\u67e5\u8be2\u7ed3\u679c",
        "\u67e5\u8be2\u6837\u672c",
        "\u5bfc\u5165\u7ed3\u679c",
        "\u5173\u952e\u6307\u6807"
    };

    std::unordered_map<std::string, std::vector<const DemoMetric*>> grouped;
    for (const auto& metric : metrics) {
        grouped[GroupForMetric(metric.key)].push_back(&metric);
    }

    for (const auto& group_name : kGroupOrder) {
        const auto it = grouped.find(group_name);
        if (it != grouped.end()) {
            PrintMetricSection(group_name, it->second);
        }
    }
}

void PrintChecks(const std::vector<DemoCheck>& checks) {
    if (checks.empty()) {
        return;
    }
    std::cout << "\n[\u6821\u9a8c\u7ed3\u679c]\n";
    for (const auto& check : checks) {
        std::cout << (check.ok ? "\u901a\u8fc7 " : "\u5931\u8d25 ") << DisplayCheckName(check.name);
        if (!check.detail.empty()) {
            std::cout << "  " << check.detail;
        }
        std::cout << '\n';
    }
}

} // namespace

ScopedStreamCapture::ScopedStreamCapture()
    : stdout_stream_(new std::ostringstream()),
      stderr_stream_(new std::ostringstream()) {
    cout_buf_ = std::cout.rdbuf(stdout_stream_->rdbuf());
    cerr_buf_ = std::cerr.rdbuf(stderr_stream_->rdbuf());
}

ScopedStreamCapture::~ScopedStreamCapture() {
    if (cout_buf_) {
        std::cout.rdbuf(cout_buf_);
    }
    if (cerr_buf_) {
        std::cerr.rdbuf(cerr_buf_);
    }
    delete stdout_stream_;
    delete stderr_stream_;
}

std::string ScopedStreamCapture::Stdout() const {
    return stdout_stream_ ? stdout_stream_->str() : std::string();
}

std::string ScopedStreamCapture::Stderr() const {
    return stderr_stream_ ? stderr_stream_->str() : std::string();
}

DemoRunResult BuildResultFromOutput(const std::string& title,
                                    const std::string& command,
                                    const std::string& usage,
                                    bool ok,
                                    const std::string& success_summary,
                                    const std::string& failure_summary,
                                    const std::string& stdout_text,
                                    const std::string& stderr_text) {
    DemoRunResult result;
    result.title = title;
    result.command = command;
    result.usage = usage;
    result.ok = ok;
    result.summary = ok ? success_summary : failure_summary;
    result.raw_stdout = stdout_text;
    result.raw_stderr = stderr_text;
    ParseStdout(stdout_text, &result.metrics, &result.checks);
    return result;
}

void RenderResult(const DemoRunResult& result) {
    std::cout << "\n========================================\n";
    std::cout << " " << result.title << '\n';
    std::cout << "========================================\n";
    std::cout << "\u7ed3\u679c: " << (result.ok ? "\u901a\u8fc7" : "\u5931\u8d25") << '\n';
    if (!result.summary.empty()) {
        std::cout << "\u6458\u8981: " << result.summary << '\n';
    }
    if (!result.command.empty()) {
        std::cout << "\u547d\u4ee4: " << result.command << '\n';
    }
    if (!result.usage.empty()) {
        std::cout << "\u7528\u6cd5: " << result.usage << '\n';
    }

    PrintMetrics(result.metrics);
    PrintChecks(result.checks);
}

} // namespace zb::demo
