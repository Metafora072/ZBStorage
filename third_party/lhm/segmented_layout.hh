#ifndef MASSTREELHM_SEGMENTED_LAYOUT_HH
#define MASSTREELHM_SEGMENTED_LAYOUT_HH

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/stat.h>

#include "metadata_layout.hh"
#include "snapshot_io.hh"

namespace MasstreeLHM {

// Creation default is independent of the persisted-format compatibility limit.
constexpr uint64_t kDefaultSegmentBytes = uint64_t{512} << 20;
constexpr uint64_t kMaxSegmentBytes = uint64_t{1} << 40;
constexpr uint64_t kSegmentedSnapshotVersion = 4;
constexpr uint64_t kSegmentedSnapshotMagic = 0x5a424c484d534e32ULL;
constexpr uint64_t kMaxSegmentCount = uint64_t{1} << 20;

struct SegmentIdentity {
    uint64_t kind = 0, index = 0, dev = 0, ino = 0, bytes = 0;
};
inline bool operator==(const SegmentIdentity& a, const SegmentIdentity& b) {
    return a.kind == b.kind && a.index == b.index && a.dev == b.dev &&
           a.ino == b.ino && a.bytes == b.bytes;
}
struct SegmentLayout {
    uint64_t segment_bytes = kDefaultSegmentBytes;
    std::array<uint64_t, 3> logical_bytes{};
    // Canonical order: kind 0/1/2, then contiguous segment indices from zero.
    // Every kind retains segment zero, including an empty bucket store.
    std::vector<SegmentIdentity> segments;
};
inline bool operator==(const SegmentLayout& a, const SegmentLayout& b) {
    return a.segment_bytes == b.segment_bytes && a.logical_bytes == b.logical_bytes &&
           a.segments == b.segments;
}
inline bool ValidSegmentBytes(uint64_t bytes) {
    return bytes >= (uint64_t{1} << 16) && bytes <= kMaxSegmentBytes &&
           (bytes & (bytes - 1)) == 0;
}
inline std::string SegmentBasePath(const std::string& store, unsigned kind) {
    if (kind > 2) throw std::runtime_error("invalid segment file kind");
    return store + (kind == 0 ? "" : kind == 1 ? ".heads" : ".buckets");
}
inline std::string SegmentPath(const std::string& store, unsigned kind, uint64_t index) {
    auto path = SegmentBasePath(store, kind);
    if (index) {
        auto number = std::to_string(index);
        if (number.size() < 6) number.insert(0, 6 - number.size(), '0');
        path += ".part" + number;
    }
    return path;
}
inline uint64_t SegmentCount(uint64_t logical_bytes, uint64_t segment_bytes) {
    if (!ValidSegmentBytes(segment_bytes)) throw std::runtime_error("invalid persisted segment size");
    return logical_bytes ? 1 + (logical_bytes - 1) / segment_bytes : 1;
}
inline void ValidateSegmentLayout(const SegmentLayout& layout) {
    if (!ValidSegmentBytes(layout.segment_bytes) || layout.segments.size() > kMaxSegmentCount ||
        layout.logical_bytes[0] < kMetadataBlockBytes)
        throw std::runtime_error("invalid segmented layout header");
    size_t pos = 0;
    std::set<std::pair<uint64_t, uint64_t>> identities;
    for (unsigned kind = 0; kind < 3; ++kind) {
        const auto bytes = layout.logical_bytes[kind];
        if (bytes % kMetadataBlockBytes || bytes > uint64_t(std::numeric_limits<int64_t>::max()))
            throw std::runtime_error("invalid segmented logical length");
        const uint64_t count = SegmentCount(bytes, layout.segment_bytes);
        if (count > kMaxSegmentCount || count > layout.segments.size() - pos)
            throw std::runtime_error("missing segmented layout entries");
        for (uint64_t index = 0; index < count; ++index) {
            const auto& segment = layout.segments[pos++];
            const auto expected = std::min(layout.segment_bytes, bytes - index * layout.segment_bytes);
            if (segment.kind != kind || segment.index != index || !segment.ino ||
                segment.bytes != expected)
                throw std::runtime_error("invalid segmented layout identity/order/length");
            if (!identities.emplace(segment.dev, segment.ino).second)
                throw std::runtime_error("multiple segments refer to the same file identity");
        }
    }
    if (pos != layout.segments.size()) throw std::runtime_error("unexpected segmented layout entries");
}
inline SegmentIdentity StatSegment(const std::string& path, unsigned kind, uint64_t index) {
    struct stat st{};
    if (::stat(path.c_str(), &st) || !S_ISREG(st.st_mode) || st.st_size < 0)
        throw std::runtime_error("cannot stat segment: " + path);
    return {kind, index, uint64_t(st.st_dev), uint64_t(st.st_ino), uint64_t(st.st_size)};
}
// Scanning is only for opening/validating a store. Reads use cached segment fds.
// Reserved .part names must be canonical; gaps and unexpectedly short old
// segments fail validation instead of changing the logical address mapping.
inline SegmentLayout ScanSegmentLayout(const std::string& store, uint64_t segment_bytes) {
    if (!ValidSegmentBytes(segment_bytes)) throw std::runtime_error("invalid persisted segment size");
    SegmentLayout layout; layout.segment_bytes = segment_bytes;
    std::array<std::vector<SegmentIdentity>, 3> groups;
    const std::filesystem::path anchor(store);
    const auto parent = anchor.has_parent_path() ? anchor.parent_path() : std::filesystem::path(".");
    for (const auto& entry : std::filesystem::directory_iterator(parent)) {
        const auto name = entry.path().filename().string();
        for (unsigned kind = 0; kind < 3; ++kind) {
            const auto base = std::filesystem::path(SegmentBasePath(store, kind)).filename().string();
            uint64_t index = 0;
            if (name != base) {
                const auto prefix = base + ".part";
                if (name.compare(0, prefix.size(), prefix) != 0) continue;
                const auto number = name.substr(prefix.size());
                const auto parsed = std::from_chars(number.data(), number.data() + number.size(), index);
                if (number.empty() || parsed.ec != std::errc() || parsed.ptr != number.data() + number.size() ||
                    !index || std::filesystem::path(SegmentPath(store, kind, index)).filename() != entry.path().filename())
                    throw std::runtime_error("invalid reserved segment name: " + entry.path().string());
            }
            if (groups[kind].size() >= kMaxSegmentCount)
                throw std::runtime_error("too many segment files");
            groups[kind].push_back(StatSegment(entry.path().string(), kind, index));
            break;
        }
    }
    for (unsigned kind = 0; kind < 3; ++kind) {
        auto& group = groups[kind];
        std::sort(group.begin(), group.end(), [](const auto& a, const auto& b) { return a.index < b.index; });
        for (const auto& segment : group) {
            if (segment.bytes > segment_bytes ||
                segment.bytes > uint64_t(std::numeric_limits<int64_t>::max()) - layout.logical_bytes[kind])
                throw std::runtime_error("oversized segment: " + SegmentPath(store, kind, segment.index));
            layout.logical_bytes[kind] += segment.bytes;
            layout.segments.push_back(segment);
        }
    }
    ValidateSegmentLayout(layout);
    return layout;
}
inline void ValidateSegmentFiles(const std::string& store, const SegmentLayout& layout, bool allow_growth = false) {
    ValidateSegmentLayout(layout);
    for (const auto& expected : layout.segments) {
        const auto path = SegmentPath(store, unsigned(expected.kind), expected.index);
        const auto actual = StatSegment(path, unsigned(expected.kind), expected.index);
        if (actual.dev != expected.dev || actual.ino != expected.ino || actual.bytes > layout.segment_bytes ||
            (allow_growth ? actual.bytes < expected.bytes : actual.bytes != expected.bytes))
            throw std::runtime_error("segment/checkpoint identity or length mismatch: " + path);
    }
}
inline void WriteSegmentLayout(SnapshotWriter& out, const SegmentLayout& layout) {
    ValidateSegmentLayout(layout);
    out.u64(layout.segment_bytes);
    for (auto bytes : layout.logical_bytes) out.u64(bytes);
    out.u64(layout.segments.size());
    for (const auto& segment : layout.segments) {
        out.u64(segment.kind); out.u64(segment.index); out.u64(segment.dev);
        out.u64(segment.ino); out.u64(segment.bytes);
    }
}
inline SegmentLayout ReadSegmentLayout(SnapshotReader& in) {
    SegmentLayout layout; layout.segment_bytes = in.u64();
    for (auto& bytes : layout.logical_bytes) bytes = in.u64();
    const auto count = in.u64();
    if (count < 3 || count > kMaxSegmentCount) throw std::runtime_error("invalid segment identity count");
    layout.segments.reserve(size_t(count));
    for (uint64_t i = 0; i < count; ++i) {
        SegmentIdentity segment;
        segment.kind = in.u64(); segment.index = in.u64(); segment.dev = in.u64();
        segment.ino = in.u64(); segment.bytes = in.u64();
        layout.segments.push_back(segment);
    }
    ValidateSegmentLayout(layout);
    return layout;
}

} // namespace MasstreeLHM
#endif
