#ifndef ZB_LHM_SNAPSHOT_IO_HH
#define ZB_LHM_SNAPSHOT_IO_HH
#include <fstream>
#include <stdexcept>
#include <cstdint>
namespace MasstreeLHM {
// Explicit little-endian scalar stream; no pointers or STL object layouts.
class SnapshotWriter {
    std::ofstream out_;
    uint64_t hash_ = 14695981039346656037ULL;
public:
    explicit SnapshotWriter(const std::string& p) : out_(p, std::ios::binary|std::ios::trunc) {
        if (!out_) throw std::runtime_error("cannot create snapshot");
    }
    void u64(uint64_t n) {
        for (int i=0;i<8;++i) { unsigned char c=n&255; n>>=8;
            out_.put(char(c)); hash_=(hash_^c)*1099511628211ULL; }
        if (!out_) throw std::runtime_error("snapshot write failed");
    }
    void finish() { auto h=hash_; u64(h); out_.flush();
        if (!out_) throw std::runtime_error("snapshot flush failed"); out_.close(); }
};
class SnapshotReader {
    std::ifstream in_;
    uint64_t hash_ = 14695981039346656037ULL;
public:
    explicit SnapshotReader(const std::string& p) : in_(p, std::ios::binary) {
        if (!in_) throw std::runtime_error("normal-close snapshot missing");
    }
    uint64_t u64() {
        uint64_t n=0;
        for (int i=0;i<8;++i) { int c=in_.get();
            if (c==EOF) throw std::runtime_error("truncated snapshot");
            n|=uint64_t(c)<<(8*i); hash_=(hash_^uint8_t(c))*1099511628211ULL; }
        return n;
    }
    uint32_t u32() { auto n=u64(); if(n>UINT32_MAX) throw std::runtime_error("snapshot integer overflow"); return n; }
    void finish() { auto h=hash_; if(u64()!=h || in_.get()!=EOF) throw std::runtime_error("snapshot checksum/trailer mismatch"); }
};
}
#endif
