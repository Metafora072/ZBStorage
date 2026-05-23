#include "sim/masstree/SimMasstreePageLayout.h"

#include "sim/masstree/SimMasstreeCodec.h"

#include <algorithm>
#include <fstream>
#include <sstream>

namespace zb::sim::masstree {

namespace {

template <typename T>
bool WritePod(std::ostream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    return static_cast<bool>(out);
}

template <typename T>
bool ReadPod(std::istream& in, T* value) {
    in.read(reinterpret_cast<char*>(value), sizeof(T));
    return static_cast<bool>(in);
}

} // namespace

bool DentryKeyLess(uint64_t lhs_parent,
                   const std::string& lhs_name,
                   uint64_t rhs_parent,
                   const std::string& rhs_name) {
    if (lhs_parent != rhs_parent) {
        return lhs_parent < rhs_parent;
    }
    return lhs_name < rhs_name;
}

bool WriteInodePages(const std::filesystem::path& pages_path,
                     const std::filesystem::path& sparse_path,
                     const std::vector<SimInodeRecord>& records,
                     uint64_t* page_bytes,
                     std::string* error) {
    std::ofstream pages(pages_path, std::ios::binary | std::ios::trunc);
    std::ofstream sparse(sparse_path, std::ios::trunc);
    if (!pages || !sparse) {
        if (error) {
            *error = "failed to open inode page/sparse outputs";
        }
        return false;
    }
    uint64_t written = 0;
    for (size_t begin = 0; begin < records.size(); begin += kSimMasstreePageTargetEntries) {
        const uint64_t page_offset = static_cast<uint64_t>(pages.tellp());
        const size_t end = std::min<size_t>(records.size(), begin + kSimMasstreePageTargetEntries);
        const uint32_t count = static_cast<uint32_t>(end - begin);
        if (!WritePod(pages, count)) {
            if (error) {
                *error = "failed to write inode page header";
            }
            return false;
        }
        for (size_t i = begin; i < end; ++i) {
            if (!WriteInodeRecord(pages, records[i], error)) {
                return false;
            }
        }
        sparse << records[end - 1].inode_id << " " << page_offset << "\n";
        written = static_cast<uint64_t>(pages.tellp());
    }
    if (page_bytes) {
        *page_bytes = written;
    }
    return true;
}

bool WriteDentryPages(const std::filesystem::path& pages_path,
                      const std::filesystem::path& sparse_path,
                      const std::vector<SimDentryRecord>& records,
                      uint64_t* page_bytes,
                      std::string* error) {
    std::ofstream pages(pages_path, std::ios::binary | std::ios::trunc);
    std::ofstream sparse(sparse_path, std::ios::trunc);
    if (!pages || !sparse) {
        if (error) {
            *error = "failed to open dentry page/sparse outputs";
        }
        return false;
    }
    uint64_t written = 0;
    for (size_t begin = 0; begin < records.size(); begin += kSimMasstreePageTargetEntries) {
        const uint64_t page_offset = static_cast<uint64_t>(pages.tellp());
        const size_t end = std::min<size_t>(records.size(), begin + kSimMasstreePageTargetEntries);
        const uint32_t count = static_cast<uint32_t>(end - begin);
        if (!WritePod(pages, count)) {
            if (error) {
                *error = "failed to write dentry page header";
            }
            return false;
        }
        for (size_t i = begin; i < end; ++i) {
            if (!WriteDentryRecord(pages, records[i], error)) {
                return false;
            }
        }
        sparse << records[end - 1].parent_inode_id << "\t"
               << records[end - 1].name << "\t"
               << page_offset << "\n";
        written = static_cast<uint64_t>(pages.tellp());
    }
    if (page_bytes) {
        *page_bytes = written;
    }
    return true;
}

bool ReadInodePageAt(const std::filesystem::path& pages_path,
                     uint64_t page_offset,
                     std::vector<SimInodeRecord>* records,
                     std::string* error) {
    if (!records) {
        if (error) {
            *error = "inode page output is null";
        }
        return false;
    }
    std::ifstream in(pages_path, std::ios::binary);
    if (!in) {
        if (error) {
            *error = "failed to open inode pages: " + pages_path.string();
        }
        return false;
    }
    in.seekg(static_cast<std::streamoff>(page_offset), std::ios::beg);
    uint32_t count = 0;
    if (!ReadPod(in, &count)) {
        if (error) {
            *error = "failed to read inode page header";
        }
        return false;
    }
    records->clear();
    records->reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        SimInodeRecord record;
        if (!ReadInodeRecord(in, &record, error)) {
            return false;
        }
        records->push_back(std::move(record));
    }
    return true;
}

bool ReadDentryPageAt(const std::filesystem::path& pages_path,
                      uint64_t page_offset,
                      std::vector<SimDentryRecord>* records,
                      std::string* error) {
    if (!records) {
        if (error) {
            *error = "dentry page output is null";
        }
        return false;
    }
    std::ifstream in(pages_path, std::ios::binary);
    if (!in) {
        if (error) {
            *error = "failed to open dentry pages: " + pages_path.string();
        }
        return false;
    }
    in.seekg(static_cast<std::streamoff>(page_offset), std::ios::beg);
    uint32_t count = 0;
    if (!ReadPod(in, &count)) {
        if (error) {
            *error = "failed to read dentry page header";
        }
        return false;
    }
    records->clear();
    records->reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        SimDentryRecord record;
        if (!ReadDentryRecord(in, &record, error)) {
            return false;
        }
        records->push_back(std::move(record));
    }
    return true;
}

bool LoadInodeSparse(const std::filesystem::path& sparse_path,
                     std::vector<SimInodeSparseEntry>* entries,
                     std::string* error) {
    std::ifstream in(sparse_path);
    if (!in) {
        if (error) {
            *error = "failed to open inode sparse: " + sparse_path.string();
        }
        return false;
    }
    entries->clear();
    SimInodeSparseEntry entry;
    while (in >> entry.max_inode_id >> entry.page_offset) {
        entries->push_back(entry);
    }
    return true;
}

bool LoadDentrySparse(const std::filesystem::path& sparse_path,
                      std::vector<SimDentrySparseEntry>* entries,
                      std::string* error) {
    std::ifstream in(sparse_path);
    if (!in) {
        if (error) {
            *error = "failed to open dentry sparse: " + sparse_path.string();
        }
        return false;
    }
    entries->clear();
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        std::stringstream ss(line);
        std::string parent;
        std::string name;
        std::string offset;
        if (!std::getline(ss, parent, '\t') ||
            !std::getline(ss, name, '\t') ||
            !std::getline(ss, offset)) {
            continue;
        }
        SimDentrySparseEntry entry;
        entry.max_parent_inode = static_cast<uint64_t>(std::stoull(parent));
        entry.max_name = name;
        entry.page_offset = static_cast<uint64_t>(std::stoull(offset));
        entries->push_back(std::move(entry));
    }
    return true;
}

} // namespace zb::sim::masstree
