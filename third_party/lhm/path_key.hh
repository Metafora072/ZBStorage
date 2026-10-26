#ifndef MASSTREE_PATH_KEY_HH
#define MASSTREE_PATH_KEY_HH

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "compiler.hh"
#include "str.hh"

namespace MasstreeLHM {

// ParsedPath 保存一次路径规范化的中间结果，便于上层同时查看
// 原始路径分量和对应的哈希结果。
struct ParsedPath {
    std::string normalized_path;
    std::vector<std::string> components;
    std::vector<uint64_t> hashes;
};

class PathKey {
  public:
    PathKey() = default;

    // 根据已经哈希好的路径分量构造 Masstree 可接受的二进制 key。
    // 每个分量固定占用 8 字节，这样 Masstree 现有的逐层 shift 逻辑
    // 就可以直接把每一级路径分量当成一层来消费。
    explicit PathKey(std::vector<uint64_t> components)
        : components_(std::move(components)),
          storage_(components_.size() * sizeof(uint64_t), '\0') {
        for (size_t i = 0; i < components_.size(); ++i) {
            uint64_t be = host_to_net_order(components_[i]);
            memcpy(&storage_[i * sizeof(uint64_t)], &be, sizeof(be));
        }
    }

    // 将内部编码后的二进制内容暴露为 lcdf::Str，方便直接复用
    // 现有 Masstree 的 get/insert/scan 接口，而不必修改树的内部实现。
    lcdf::Str as_str() const {
        return lcdf::Str(storage_);
    }

    // 返回编码前的哈希路径分量，主要用于测试、调试和上层元数据逻辑。
    const std::vector<uint64_t>& components() const {
        return components_;
    }

    // 以十六进制形式输出哈希路径，便于日志打印和人工检查。
    std::string debug_string() const {
        std::ostringstream out;
        for (size_t i = 0; i < components_.size(); ++i) {
            if (i != 0) {
                out << '/';
            }
            out << "0x" << std::hex << std::setw(16) << std::setfill('0')
                << components_[i];
        }
        return out.str();
    }

    // 将 Masstree 中保存的原始二进制 key 反解回 64-bit 路径分量数组。
    // 这个方法主要用于 scan 结果检查和测试验证。
    static std::vector<uint64_t> decode(lcdf::Str encoded) {
        std::vector<uint64_t> result;
        if (encoded.length() % int(sizeof(uint64_t)) != 0) {
            return result;
        }

        result.reserve(encoded.length() / int(sizeof(uint64_t)));
        for (int offset = 0; offset < encoded.length(); offset += int(sizeof(uint64_t))) {
            uint64_t be = 0;
            memcpy(&be, encoded.data() + offset, sizeof(be));
            result.push_back(net_to_host_order(be));
        }
        return result;
    }

    // 将单个路径分量哈希成稳定的 64-bit 指纹。
    // 目前使用 FNV-1a 64-bit，原因是实现简单、确定性强、便于测试复现。
    static uint64_t hash_component(const std::string& component) {
        static constexpr uint64_t kFnvOffset = 14695981039346656037ull;
        static constexpr uint64_t kFnvPrime = 1099511628211ull;

        uint64_t hash = kFnvOffset;
        for (unsigned char c : component) {
            hash ^= c;
            hash *= kFnvPrime;
        }
        return hash;
    }

    // 解析并规范化绝对路径，然后逐级计算每个路径分量的哈希值。
    // 这是从 "/a/b/c" 到 LHM 所需 vector<uint64_t> 表示的桥接入口。
    static ParsedPath parse_absolute_path(const std::string& path) {
        if (path.empty() || path.front() != '/') {
            throw std::invalid_argument("path must be absolute");
        }

        ParsedPath parsed;
        parsed.normalized_path = "/";

        size_t i = 0;
        while (i < path.size()) {
            while (i < path.size() && path[i] == '/') {
                ++i;
            }
            if (i >= path.size()) {
                break;
            }

            size_t j = i;
            while (j < path.size() && path[j] != '/') {
                ++j;
            }

            std::string component = path.substr(i, j - i);
            if (component == "." || component == "..") {
                throw std::invalid_argument("'.' and '..' are not supported yet");
            }

            parsed.components.push_back(component);
            parsed.hashes.push_back(hash_component(component));
            if (parsed.normalized_path.size() > 1) {
                parsed.normalized_path.push_back('/');
            }
            parsed.normalized_path += component;
            i = j;
        }

        return parsed;
    }

    // 常用便捷接口：如果调用方只关心最终的 PathKey，而不关心中间解析结果，
    // 可以直接调用这个方法。
    static PathKey from_absolute_path(const std::string& path) {
        return PathKey(parse_absolute_path(path).hashes);
    }

  private:
    std::vector<uint64_t> components_;
    std::string storage_;
};

}  // namespace MasstreeLHM

#endif
