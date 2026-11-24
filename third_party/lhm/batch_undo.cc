#include "batch_undo.hh"
#include "metadata_layout.hh"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <unistd.h>

namespace MasstreeLHM {
namespace {
constexpr uint64_t kSeed = 14695981039346656037ULL;
constexpr uint64_t kPrime = 1099511628211ULL;
constexpr uint64_t kMagic = 0x314f444e554d484cULL; // LHMUNDO1 family, journal v3
constexpr size_t kHeaderPrefixBytes = 13 * 8;
constexpr size_t kMaxReceipt = 16384;
constexpr uint64_t kMaxOffset = uint64_t(std::numeric_limits<off_t>::max());
using SegmentKey = std::pair<uint64_t,uint64_t>;

[[noreturn]] void Fail(const std::string& message) {
    throw std::runtime_error("batch undo: " + message);
}
struct Fd {
    int n = -1;
    explicit Fd(int value = -1) : n(value) {}
    ~Fd() { if (n >= 0) ::close(n); }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    int release() { const int out=n; n=-1; return out; }
};
uint64_t Hash(const void* data, size_t size, uint64_t hash = kSeed) {
    auto p = static_cast<const unsigned char*>(data);
    while (size--) hash = (hash ^ *p++) * kPrime;
    return hash;
}
void Put(std::vector<unsigned char>& out, uint64_t value) {
    for (unsigned i = 0; i != 8; ++i) { out.push_back(value & 255); value >>= 8; }
}
uint64_t Get(const unsigned char* p) {
    uint64_t value = 0;
    for (unsigned i = 0; i != 8; ++i) value |= uint64_t(p[i]) << (i * 8);
    return value;
}
struct Identity { uint64_t dev, ino, bytes; };
Identity Stat(int fd) {
    struct stat st{};
    if (::fstat(fd, &st) || !S_ISREG(st.st_mode) || st.st_size < 0)
        Fail("cannot stat regular file");
    return {uint64_t(st.st_dev), uint64_t(st.st_ino), uint64_t(st.st_size)};
}
void Sync(int fd) {
    int rc; do { rc=::fsync(fd); } while(rc && errno==EINTR);
    if (rc) Fail("fsync failed: " + std::string(std::strerror(errno)));
}
void SyncDirectory(const std::string& store) {
    auto dir = std::filesystem::path(store).parent_path();
    if (dir.empty()) dir = ".";
    Fd fd(::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (fd.n < 0) Fail("cannot open parent directory");
    Sync(fd.n);
}
void Write(int fd, const void* data, size_t bytes) {
    const auto* p = static_cast<const unsigned char*>(data);
    while (bytes) {
        auto n = ::write(fd, p, bytes);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) Fail("journal/receipt write failed");
        p += n; bytes -= size_t(n);
    }
}
void ReadAt(int fd, void* data, size_t bytes, uint64_t offset) {
    if (offset > kMaxOffset || bytes > kMaxOffset - offset) Fail("offset overflow");
    auto* p = static_cast<unsigned char*>(data);
    while (bytes) {
        auto n = ::pread(fd, p, bytes, off_t(offset));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) Fail("short or failed read");
        p += n; bytes -= size_t(n); offset += uint64_t(n);
    }
}
void WriteAt(int fd, const void* data, size_t bytes, uint64_t offset) {
    if (offset > kMaxOffset || bytes > kMaxOffset - offset) Fail("offset overflow");
    const auto* p = static_cast<const unsigned char*>(data);
    while (bytes) {
        auto n = ::pwrite(fd, p, bytes, off_t(offset));
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) Fail("restore write failed");
        p += n; bytes -= size_t(n); offset += uint64_t(n);
    }
}
struct Fingerprint {
    uint64_t bytes, checksum;
    bool operator==(const Fingerprint& b) const { return bytes == b.bytes && checksum == b.checksum; }
};
Fingerprint FingerprintOf(const std::string& path) {
    Fd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW));
    if (fd.n < 0) Fail("cannot open snapshot: " + path);
    const auto st = Stat(fd.n);
    if (st.bytes < 24 || st.bytes % 8) Fail("invalid snapshot length");
    std::array<unsigned char, 16> head{};
    ReadAt(fd.n, head.data(), head.size(), 0);
    if (Get(head.data()) != kSegmentedSnapshotMagic || !SupportedSnapshotVersion(Get(head.data() + 8)))
        Fail("unsupported snapshot format: expected version 4 or 5");
    std::vector<unsigned char> buffer(1024 * 1024);
    uint64_t offset = 0, hash = kSeed;
    while (offset < st.bytes - 8) {
        auto count = size_t(std::min<uint64_t>(buffer.size(), st.bytes - 8 - offset));
        ReadAt(fd.n, buffer.data(), count, offset);
        hash = Hash(buffer.data(), count, hash); offset += count;
    }
    std::array<unsigned char, 8> trailer{};
    ReadAt(fd.n, trailer.data(), trailer.size(), offset);
    if (hash != Get(trailer.data())) Fail("snapshot checksum mismatch");
    return {st.bytes, hash};
}
SegmentLayout SnapshotLayout(const std::string& path) {
    SnapshotReader in(path);
    if(in.u64()!=kSegmentedSnapshotMagic || !SupportedSnapshotVersion(in.u64()))
        Fail("unsupported snapshot version");
    return ReadSegmentLayout(in);
}
bool SameLayout(const SegmentLayout& a,const SegmentLayout& b) {
    if(a.segment_bytes!=b.segment_bytes || a.logical_bytes!=b.logical_bytes || a.segments.size()!=b.segments.size())
        return false;
    for(size_t i=0;i<a.segments.size();++i) {
        const auto& x=a.segments[i]; const auto& y=b.segments[i];
        if(x.kind!=y.kind || x.index!=y.index || x.dev!=y.dev || x.ino!=y.ino || x.bytes!=y.bytes)
            return false;
    }
    return true;
}
bool PathExists(const std::string& path) {
    struct stat st{};
    if (!::lstat(path.c_str(), &st)) return true;
    if (errno == ENOENT) return false;
    Fail("cannot inspect " + path);
}
void Remove(const std::string& path) {
    if (::unlink(path.c_str()) && errno != ENOENT) Fail("cannot remove " + path);
}
std::vector<unsigned char> Frame(uint64_t type, uint64_t kind, uint64_t block,
                                 const void* data, size_t bytes) {
    std::vector<unsigned char> out;
    out.reserve(32 + bytes + 8);
    Put(out, type); Put(out, kind); Put(out, block); Put(out, bytes);
    const auto* begin = static_cast<const unsigned char*>(data);
    out.insert(out.end(), begin, begin + bytes);
    Put(out, Hash(out.data(), out.size()));
    return out;
}
struct Header {
    Fingerprint base{};
    SegmentLayout layout;
    uint64_t inode_high=0, directory_next=0, bytes=0;
    bool service=false;
};
std::vector<unsigned char> EncodeHeader(const Header& h) {
    ValidateSegmentLayout(h.layout);
    std::vector<unsigned char> out;
    const uint64_t bytes=(14+5*h.layout.segments.size())*8;
    for(auto n:{kMagic,uint64_t(h.service?5:4),uint64_t(kMetadataBlockBytes),bytes,
                h.base.bytes,h.base.checksum,h.inode_high,h.directory_next,
                h.layout.segment_bytes,h.layout.logical_bytes[0],h.layout.logical_bytes[1],
                h.layout.logical_bytes[2],uint64_t(h.layout.segments.size())}) Put(out,n);
    for(const auto& s:h.layout.segments)
        for(auto n:{s.kind,s.index,s.dev,s.ino,s.bytes}) Put(out,n);
    Put(out,Hash(out.data(),out.size())); return out;
}
Header ReadHeader(int fd) {
    std::array<unsigned char,kHeaderPrefixBytes> prefix{};
    ReadAt(fd,prefix.data(),prefix.size(),0);
    auto p=[&](size_t i){return Get(prefix.data()+8*i);};
    if(p(0)!=kMagic || (p(1)!=3 && p(1)!=4 && p(1)!=5) || p(2)!=kMetadataBlockBytes)
        Fail("unsupported journal format: expected version 3, 4 or service version 5");
    const auto bytes=p(3), count=p(12), file_bytes=Stat(fd).bytes;
    if(count>(std::numeric_limits<size_t>::max()/8-14)/5 ||
       bytes!=(14+5*count)*8 || bytes>file_bytes || count<3 || count>kMaxSegmentCount)
        Fail("invalid segmented journal header size");
    std::vector<unsigned char> raw(size_t(bytes),0);
    ReadAt(fd,raw.data(),raw.size(),0);
    if(Get(raw.data()+raw.size()-8)!=Hash(raw.data(),raw.size()-8))
        Fail("journal header checksum mismatch");
    Header h; h.base={p(4),p(5)}; h.inode_high=p(6); h.directory_next=p(7); h.bytes=bytes;
    h.service=p(1)==5;
    h.layout.segment_bytes=p(8); h.layout.logical_bytes={p(9),p(10),p(11)};
    for(uint64_t i=0;i<count;++i) {
        const auto* s=raw.data()+kHeaderPrefixBytes+i*40;
        h.layout.segments.push_back({Get(s),Get(s+8),Get(s+16),Get(s+24),Get(s+32)});
    }
    ValidateSegmentLayout(h.layout);
    if(h.base.bytes<24 || h.base.bytes%8 || !h.inode_high || !h.directory_next ||
       h.inode_high>=h.layout.logical_bytes[0]/kMetadataBlockBytes ||
       h.directory_next-1>h.layout.logical_bytes[2]/kMetadataBlockBytes)
        Fail("invalid journal allocation bounds");
    return h;
}
bool OldBlock(const Header& h,uint64_t kind,uint64_t block) {
    return kind<3 && block<h.layout.logical_bytes[kind]/kMetadataBlockBytes;
}
struct Record { uint64_t type=0,kind=0,block=0; std::vector<unsigned char> data; };
// A torn final frame never authorizes data I/O or segment publication.
bool NextRecord(int fd,uint64_t size,uint64_t& offset,Record& record) {
    if(offset>size) Fail("journal offset overflow");
    if(size-offset<32) return false;
    std::array<unsigned char,32> head{};
    ReadAt(fd,head.data(),head.size(),offset);
    record.type=Get(head.data()); record.kind=Get(head.data()+8);
    record.block=Get(head.data()+16); const auto length=Get(head.data()+24);
    if((record.type<1 || record.type>5) ||
       (record.type==1 && (record.kind>2 || length!=kMetadataBlockBytes)) ||
       (record.type==2 && (record.kind || record.block || length<24 || length>kMaxReceipt+24)) ||
       (record.type==3 && (record.kind>2 || !record.block || length<25 || length>24+255)) ||
       (record.type==4 && (record.kind || record.block || length!=24)) ||
       (record.type==5 && (record.kind || record.block || length!=16)))
        Fail("invalid journal record header");
    if(size-offset-32<length+8) return false;
    record.data.resize(size_t(length));
    ReadAt(fd,record.data.data(),record.data.size(),offset+32);
    std::array<unsigned char,8> checksum{};
    ReadAt(fd,checksum.data(),checksum.size(),offset+32+length);
    if(Get(checksum.data())!=Hash(record.data.data(),record.data.size(),Hash(head.data(),head.size())))
        Fail("journal record checksum mismatch");
    offset+=32+length+8; return true;
}
struct NewSegment {
    SegmentIdentity identity{};
    std::string temporary;
};
NewSegment DecodeNewSegment(const std::string& store,const Record& r) {
    const auto size=Get(r.data.data()+16);
    if(size!=r.data.size()-24) Fail("invalid new-segment name length");
    std::string name(reinterpret_cast<const char*>(r.data.data()+24),size_t(size));
    const auto prefix=std::filesystem::path(store).filename().string()+".segment-new-"+
        std::to_string(r.kind)+"-"+std::to_string(r.block)+"-";
    if(name.size()!=prefix.size()+6 || name.compare(0,prefix.size(),prefix)!=0 ||
       name.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",prefix.size())!=std::string::npos)
        Fail("invalid new-segment temporary name");
    NewSegment result;
    result.identity={r.kind,r.block,Get(r.data.data()),Get(r.data.data()+8),0};
    result.temporary=(std::filesystem::path(store).parent_path()/name).string();
    return result;
}
using OpenSegments=std::map<SegmentKey,std::unique_ptr<Fd>>;
std::unique_ptr<Fd> OpenExpected(const std::string& path,const SegmentIdentity& expected,
                               uint64_t segment_bytes,bool allow_missing,bool allow_growth) {
    const int n=::open(path.c_str(),O_RDWR|O_CLOEXEC|O_NOFOLLOW);
    if(n<0) {
        if(allow_missing && errno==ENOENT) return nullptr;
        Fail("cannot open recovery segment: "+path);
    }
    auto fd=std::make_unique<Fd>(n); const auto st=Stat(n);
    if(st.dev!=expected.dev || st.ino!=expected.ino || st.bytes>segment_bytes ||
       st.bytes%kMetadataBlockBytes || (allow_growth?st.bytes<expected.bytes:st.bytes!=expected.bytes))
        Fail("journal/segment identity or length mismatch: "+path);
    return fd;
}
void RemoveExpected(const std::string& path,const SegmentIdentity& identity) {
    struct stat st{};
    if(::lstat(path.c_str(),&st)) {
        if(errno==ENOENT) return;
        Fail("cannot inspect segment before removal: "+path);
    }
    if(!S_ISREG(st.st_mode) || uint64_t(st.st_dev)!=identity.dev || uint64_t(st.st_ino)!=identity.ino)
        Fail("refusing to remove replaced segment: "+path);
    Remove(path);
}
// All official part files must be explained by the baseline or durable new-file
// records. Temporary names are excluded; an unregistered temp has no data pages.
void RejectUnknownSegments(const std::string& store,const SegmentLayout& base,
                           const std::vector<NewSegment>& created) {
    std::set<std::string> known;
    for(const auto& s:base.segments) known.insert(SegmentPath(store,unsigned(s.kind),s.index));
    for(const auto& s:created) known.insert(SegmentPath(store,unsigned(s.identity.kind),s.identity.index));
    auto parent=std::filesystem::path(store).parent_path(); if(parent.empty()) parent=".";
    for(const auto& entry:std::filesystem::directory_iterator(parent)) {
        const auto name=entry.path().filename().string();
        for(unsigned kind=0;kind<3;++kind) {
            const auto prefix=std::filesystem::path(SegmentPath(store,kind,0)).filename().string()+".part";
            if(name.compare(0,prefix.size(),prefix)!=0 || name.size()==prefix.size()) continue;
            const auto path=(std::filesystem::path(store).parent_path()/name).string();
            if(!known.count(path)) Fail("unregistered data segment: "+path);
        }
    }
}
void CheckCommittedLayout(const SegmentLayout& before,const SegmentLayout& after,
                          const std::vector<NewSegment>& created) {
    if(after.segment_bytes!=before.segment_bytes || after.segments.size()!=before.segments.size()+created.size())
        Fail("committed segment inventory does not match journal");
    std::map<SegmentKey,SegmentIdentity> expected;
    for(const auto& s:before.segments) expected.emplace(SegmentKey{s.kind,s.index},s);
    for(const auto& s:created) expected.emplace(SegmentKey{s.identity.kind,s.identity.index},s.identity);
    for(unsigned kind=0;kind<3;++kind)
        if(after.logical_bytes[kind]<before.logical_bytes[kind]) Fail("committed file shrank during append");
    for(const auto& s:after.segments) {
        const auto i=expected.find({s.kind,s.index});
        if(i==expected.end() || s.dev!=i->second.dev || s.ino!=i->second.ino || s.bytes<i->second.bytes)
            Fail("committed segment identity differs from journal");
    }
}
void PublishReceipt(const std::string& store,const std::string& receipt) {
    auto temp=store+".batch-receipt.tmp", final=store+".batch-receipt";
    Fd fd(::open(temp.c_str(),O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC|O_NOFOLLOW,0600));
    if(fd.n<0) Fail("cannot create receipt");
    Write(fd.n,receipt.data(),receipt.size()); Sync(fd.n);
    if(::rename(temp.c_str(),final.c_str())) Fail("cannot publish receipt");
    SyncDirectory(store); BatchFault("undo_receipt");
}
} // namespace

void BatchFault(const char* name) {
#ifdef ZB_LHM_TESTING
    const char* value = std::getenv("ZB_LHM_CRASH_AT");
    if (!value) return;
    const size_t n = std::strlen(name);
    if (std::strncmp(value, name, n) || (value[n] && value[n] != ':')) return;
    static thread_local uint64_t count = 0;
    uint64_t occurrence = 1;
    if (value[n] == ':') {
        char* end = nullptr; errno = 0;
        occurrence = std::strtoull(value + n + 1, &end, 10);
        if (errno || !occurrence || !end || *end) return;
    }
    if (++count == occurrence) { ::kill(::getpid(), SIGKILL); ::_exit(137); }
#else
    (void)name;
#endif
}

BatchUndo::~BatchUndo() { if(fd_>=0) ::close(fd_); }
bool BatchUndo::Exists(const std::string& store) { return PathExists(store+".undo"); }
bool BatchUndo::IsServiceJournal(const std::string& store) {
    Fd fd(::open((store+".undo").c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));
    if(fd.n<0) Fail("cannot inspect journal kind");
    return ReadHeader(fd.n).service;
}
std::string BatchUndo::SnapshotFingerprint(const std::string& snapshot) {
    const auto value=FingerprintOf(snapshot);
    // Check the variable segment inventory as well as the outer checksum.
    (void)SnapshotLayout(snapshot);
    return std::to_string(value.bytes)+":"+std::to_string(value.checksum);
}
std::unique_ptr<BatchUndo> BatchUndo::Begin(const std::string& store,
    uint64_t inode_high_water,uint64_t directory_next_block,bool service) {
    if(Exists(store)) Fail("pending journal requires recovery");
    Header h; h.base=FingerprintOf(store+".snapshot"); h.layout=SnapshotLayout(store+".snapshot");
    h.service=service;
    h.inode_high=inode_high_water; h.directory_next=directory_next_block;
    ValidateSegmentFiles(store,h.layout);
    if(!SameLayout(h.layout,ScanSegmentLayout(store,h.layout.segment_bytes)))
        Fail("checkpoint segment inventory mismatch");
    const auto header=EncodeHeader(h);
    auto temp=store+".undo.init"; Remove(temp);
    auto journal=std::unique_ptr<BatchUndo>(new BatchUndo);
    journal->store_=store; journal->baseline_=h.layout;
    journal->service_=service;
    for(const auto& s:h.layout.segments) journal->next_segment_[s.kind]=s.index+1;
    journal->base_bytes_=h.base.bytes; journal->base_checksum_=h.base.checksum;
    journal->fd_=::open(temp.c_str(),O_RDWR|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600);
    if(journal->fd_<0) Fail("cannot create journal");
    Write(journal->fd_,header.data(),header.size()); Sync(journal->fd_);
    (void)ReadHeader(journal->fd_);
    if(::rename(temp.c_str(),(store+".undo").c_str())) Fail("cannot publish journal");
    SyncDirectory(store); BatchFault("undo_begin"); return journal;
}
bool BatchUndo::IsOld(unsigned kind,uint64_t block) const {
    return kind<3 && block<baseline_.logical_bytes[kind]/kMetadataBlockBytes;
}
bool BatchUndo::BeforeWrite(unsigned kind,int data_fd,uint64_t block) noexcept {
    if(poisoned_ || prepared_ || kind>2) return false;
    try {
        if(!IsOld(kind,block) || seen_[kind].count(block)) return true;
        const uint64_t pages=baseline_.segment_bytes/kMetadataBlockBytes;
        const uint64_t index=block/pages;
        const auto match=std::find_if(baseline_.segments.begin(),baseline_.segments.end(),
            [&](const SegmentIdentity& s){return s.kind==kind && s.index==index;});
        if(match==baseline_.segments.end()) Fail("old page outside baseline segments");
        const auto actual=Stat(data_fd);
        if(actual.dev!=match->dev || actual.ino!=match->ino || actual.bytes<match->bytes)
            Fail("write descriptor does not match protected segment");
        alignas(4096) std::array<unsigned char,kMetadataBlockBytes> previous{};
        ReadAt(data_fd,previous.data(),previous.size(),(block%pages)*kMetadataBlockBytes);
        const auto record=Frame(1,kind,block,previous.data(),previous.size());
        seen_[kind].insert(block);
        Write(fd_,record.data(),record.size()); Sync(fd_);
        BatchFault("undo_record"); if(kind==2) BatchFault("undo_record_bucket"); return true;
    } catch(const std::exception& e) {
        poisoned_=true; try { error_=e.what(); } catch(...) {} return false;
    } catch(...) { poisoned_=true; return false; }
}
int BatchUndo::CreateSegment(unsigned kind,uint64_t index,int open_flags) {
    if(poisoned_ || prepared_ || kind>2 || !index || index!=next_segment_[kind])
        Fail("new segment is outside append sequence");
    try {
        if(open_flags & ~O_DIRECT) Fail("unsupported segment I/O flags");
        const auto final=SegmentPath(store_,kind,index);
        // An unrelated file at the final path must never be replaced or deleted.
        if(PathExists(final)) Fail("new segment path already exists: "+final);
        std::string name=store_+".segment-new-"+std::to_string(kind)+"-"+std::to_string(index)+"-XXXXXX";
        std::vector<char> writable(name.begin(),name.end()); writable.push_back(0);
        Fd fd(::mkostemp(writable.data(),O_CLOEXEC));
        if(fd.n<0) Fail("cannot create segment temporary file");
        name=writable.data(); const auto identity=Stat(fd.n);
        Sync(fd.n); BatchFault("segment_temp_created");
        const auto basename=std::filesystem::path(name).filename().string();
        if(basename.size()>255) Fail("segment temporary name too long");
        std::vector<unsigned char> payload;
        Put(payload,identity.dev); Put(payload,identity.ino); Put(payload,basename.size());
        payload.insert(payload.end(),basename.begin(),basename.end());
        const auto record=Frame(3,kind,index,payload.data(),payload.size());
        Write(fd_,record.data(),record.size()); Sync(fd_); ++next_segment_[kind];
        BatchFault("segment_recorded");
        // link() publishes without overwriting any existing directory entry.
        if(::link(name.c_str(),final.c_str())) Fail("cannot publish new segment: "+final);
        SyncDirectory(store_); BatchFault("segment_published");
        RemoveExpected(name,{kind,index,identity.dev,identity.ino,0}); SyncDirectory(store_);
        if(open_flags) {
            const int current=::fcntl(fd.n,F_GETFL);
            if(current<0 || ::fcntl(fd.n,F_SETFL,current|open_flags))
                Fail("cannot set new segment I/O flags");
        }
        return fd.release();
    } catch(const std::exception& e) {
        poisoned_=true; try { error_=e.what(); } catch(...) {} throw;
    } catch(...) { poisoned_=true; throw; }
}
uint64_t BatchUndo::CreateIndex() {
    if(poisoned_ || prepared_) Fail("cannot create index in failed/prepared batch");
    try {
        auto name=store_+".index-XXXXXX";
        std::vector<char> writable(name.begin(),name.end()); writable.push_back(0);
        Fd fd(::mkostemp(writable.data(),O_CLOEXEC));
        if(fd.n<0) Fail("cannot create immutable index");
        name=writable.data(); const auto identity=Stat(fd.n);
        const auto token=IndexToken(name.substr(name.size()-6));
        Sync(fd.n); BatchFault("index_created");
        std::vector<unsigned char> payload;
        Put(payload,token); Put(payload,identity.dev); Put(payload,identity.ino);
        const auto record=Frame(4,0,0,payload.data(),payload.size());
        Write(fd_,record.data(),record.size()); Sync(fd_);
        SyncDirectory(store_); BatchFault("index_registered");
        return token;
    } catch(const std::exception& e) {
        poisoned_=true; try { error_=e.what(); } catch(...) {} throw;
    } catch(...) { poisoned_=true; throw; }
}
void BatchUndo::PrepareCommit(const std::string& new_snapshot,const std::string& receipt) {
    if(service_) Fail("offline receipt commit cannot finalize a service journal");
    if(poisoned_ || prepared_) Fail("journal is failed or already prepared");
    if(receipt.empty() || receipt.size()>kMaxReceipt) Fail("invalid receipt length");
    const auto next=FingerprintOf(new_snapshot);
    ValidateIndexFiles(store_,CheckpointIndexes(new_snapshot));
    if(next==Fingerprint{base_bytes_,base_checksum_}) Fail("unchanged checkpoint cannot commit a new batch");
    std::vector<unsigned char> payload;
    Put(payload,next.bytes); Put(payload,next.checksum); Put(payload,receipt.size());
    payload.insert(payload.end(),receipt.begin(),receipt.end());
    const auto record=Frame(2,0,0,payload.data(),payload.size());
    prepared_=true; Write(fd_,record.data(),record.size()); Sync(fd_); BatchFault("undo_commit");
}
void BatchUndo::PrepareServiceCommit(const std::string& new_snapshot) {
    if(!service_ || poisoned_ || prepared_) Fail("invalid service commit state");
    const auto next=FingerprintOf(new_snapshot);
    ValidateIndexFiles(store_,CheckpointIndexes(new_snapshot));
    if(next==Fingerprint{base_bytes_,base_checksum_}) Fail("service checkpoint generation did not advance");
    std::vector<unsigned char> payload; Put(payload,next.bytes); Put(payload,next.checksum);
    const auto record=Frame(5,0,0,payload.data(),payload.size());
    prepared_=true; Write(fd_,record.data(),record.size()); Sync(fd_); BatchFault("service_prepared");
}
bool BatchUndo::Recover(const std::string& store) {
    if(!Exists(store)) {
        if(PathExists(store+".undo.init")) { Remove(store+".undo.init"); SyncDirectory(store); }
        return false;
    }
    Fd journal(::open((store+".undo").c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));
    if(journal.n<0) Fail("cannot open journal");
    const auto header=ReadHeader(journal.n); const auto journal_size=Stat(journal.n).bytes;
    uint64_t next_segment[3]{};
    for(const auto& s:header.layout.segments) next_segment[s.kind]=s.index+1;
    std::vector<NewSegment> created;
    std::map<uint64_t,SegmentIdentity> indexes;
    uint64_t offset=header.bytes; Record record; bool prepared=false;
    Fingerprint next{}; std::string receipt;
    // Validate every complete frame before any restore, truncate or removal.
    while(NextRecord(journal.n,journal_size,offset,record)) {
        if(prepared) Fail("record follows commit preparation");
        if(record.type==1) {
            if(!OldBlock(header,record.kind,record.block)) Fail("undo record outside checkpoint allocation");
        } else if(record.type==3) {
            if(record.block!=next_segment[record.kind]++) Fail("new-segment journal sequence is invalid");
            created.push_back(DecodeNewSegment(store,record));
        } else if(record.type==4) {
            const auto token=Get(record.data.data()); (void)IndexPath(store,token);
            const auto dev=Get(record.data.data()+8),ino=Get(record.data.data()+16);
            if(!ino || !indexes.emplace(token,SegmentIdentity{0,0,dev,ino,0}).second)
                Fail("invalid/duplicate new index record");
        } else if(record.type==5) {
            if(!header.service) Fail("service commit in offline journal");
            next={Get(record.data.data()),Get(record.data.data()+8)}; prepared=true;
        } else {
            if(header.service) Fail("offline receipt in service journal");
            next={Get(record.data.data()),Get(record.data.data()+8)};
            const auto length=Get(record.data.data()+16);
            if(!length || length>kMaxReceipt || length!=record.data.size()-24) Fail("invalid commit receipt");
            receipt.assign(reinterpret_cast<const char*>(record.data.data()+24),size_t(length)); prepared=true;
        }
    }
    if(prepared && offset!=journal_size) Fail("trailing bytes after commit preparation");
    RejectUnknownSegments(store,header.layout,created);
    OpenSegments existing;
    for(const auto& s:header.layout.segments)
        existing.emplace(SegmentKey{s.kind,s.index},OpenExpected(SegmentPath(store,unsigned(s.kind),s.index),s,
                          header.layout.segment_bytes,false,true));
    // Keep all new descriptors pinned through recovery. Missing files are valid
    // before publication, or after an earlier interrupted rollback removed them.
    std::vector<std::unique_ptr<Fd>> new_fds;
    for(const auto& s:created) {
        new_fds.push_back(OpenExpected(SegmentPath(store,unsigned(s.identity.kind),s.identity.index),s.identity,
                                      header.layout.segment_bytes,true,true));
        new_fds.push_back(OpenExpected(s.temporary,s.identity,header.layout.segment_bytes,true,true));
    }
    const auto current=FingerprintOf(store+".snapshot");
    const auto current_layout=SnapshotLayout(store+".snapshot");
    const auto catalog=CheckpointIndexes(store+".snapshot");
    ValidateIndexFiles(store,catalog);
    std::vector<std::unique_ptr<Fd>> index_fds;
    std::set<std::pair<uint64_t,uint64_t>> new_index_identities;
    for(const auto& item:indexes) {
        const auto path=IndexPath(store,item.first);
        if(!new_index_identities.emplace(item.second.dev,item.second.ino).second)
            Fail("duplicate newly created index identity");
        for(const auto& s:header.layout.segments)
            if(s.dev==item.second.dev && s.ino==item.second.ino) Fail("new index aliases data segment");
        auto fd=std::make_unique<Fd>(::open(path.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));
        if(fd->n<0) {
            if(errno!=ENOENT) Fail("cannot open new index during recovery");
        } else {
            const auto actual=Stat(fd->n);
            if(actual.dev!=item.second.dev || actual.ino!=item.second.ino)
                Fail("new index identity mismatch");
        }
        index_fds.push_back(std::move(fd));
    }
    if(prepared && current==next) {
        for(const auto& item:indexes) {
            const auto found=std::find_if(catalog.files.begin(),catalog.files.end(),[&](const IndexFile& f) {
                return f.token==item.first && f.dev==item.second.dev && f.ino==item.second.ino;
            });
            if(found==catalog.files.end()) Fail("published checkpoint omitted registered index");
        }
        CheckCommittedLayout(header.layout,current_layout,created);
        ValidateSegmentFiles(store,current_layout);
        for(const auto& s:created) RemoveExpected(s.temporary,s.identity);
        SyncDirectory(store);
        if(!header.service) PublishReceipt(store,receipt);
    } else if(current==header.base) {
        for(const auto& f:catalog.files) {
            if(indexes.count(f.token) || new_index_identities.count({f.dev,f.ino}))
                Fail("rollback index aliases committed index");
        }
        if(!SameLayout(current_layout,header.layout)) Fail("journal baseline differs from snapshot segment inventory");
        offset=header.bytes;
        const uint64_t pages=header.layout.segment_bytes/kMetadataBlockBytes;
        while(NextRecord(journal.n,journal_size,offset,record)) {
            if(record.type!=1) continue;
            const auto i=existing.find({record.kind,record.block/pages});
            if(i==existing.end()) Fail("restore segment missing from baseline");
            WriteAt(i->second->n,record.data.data(),record.data.size(),(record.block%pages)*kMetadataBlockBytes);
            BatchFault("undo_restore"); if(record.kind==2) BatchFault("undo_restore_bucket");
        }
        for(const auto& s:header.layout.segments) {
            const int fd=existing.at({s.kind,s.index})->n;
            if(::ftruncate(fd,off_t(s.bytes))) Fail("cannot restore segment length");
            Sync(fd);
        }
        for(const auto& s:created) {
            RemoveExpected(SegmentPath(store,unsigned(s.identity.kind),s.identity.index),s.identity);
            RemoveExpected(s.temporary,s.identity); BatchFault("undo_segment_removed");
        }
        for(const auto& item:indexes) {
            RemoveExpected(IndexPath(store,item.first),item.second); BatchFault("undo_index_removed");
        }
        SyncDirectory(store); BatchFault("undo_restored");
    } else Fail("journal does not match the published checkpoint");
    Remove(store+".undo"); SyncDirectory(store); BatchFault("undo_removed"); return true;
}
} // namespace MasstreeLHM
