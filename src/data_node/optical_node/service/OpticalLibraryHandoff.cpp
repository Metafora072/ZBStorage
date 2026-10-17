#include "OpticalStorageServiceImpl.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <set>
#include <sys/stat.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include "common/rpc/HandoffFiles.h"
#include <space_manager/sha256.h>

namespace zb::optical_node {
namespace fs = std::filesystem;
namespace {
constexpr const char* kDirs[] = {"input", "temp", "image", "read", "disc_sim", "meta", "log"};
bool SameManifest(zb::rpc::OpticalLibraryHandoffRecord a, zb::rpc::OpticalLibraryHandoffRecord b) {
    a.clear_phase(); b.clear_phase();
    return a.SerializeAsString() == b.SerializeAsString();
}
bool ValidateManifest(const zb::rpc::OpticalLibraryHandoffRecord& r) {
    if (r.files_size() == 0 || r.files_size() > 100000 || r.manifest_id() != zb::handoff::ManifestId(r)) return false;
    std::set<std::string> seen;
    for (const auto& f : r.files()) {
        if (!zb::handoff::SafePath(f.path()) || !seen.insert(f.path()).second ||
            f.sha256().size() != 64 || f.size() > static_cast<uint64_t>(std::numeric_limits<off_t>::max())) return false;
    }
    return seen.count("log/runtime_state_snapshot") != 0;
}
bool Matches(const fs::path& path, const zb::rpc::OpticalHandoffFile& file) {
    std::error_code ec;
    if (!fs::is_regular_file(fs::symlink_status(path, ec)) || ec ||
        fs::file_size(path, ec) != file.size() || ec) return false;
    std::string sha;
    return space_manager::Sha256FileHex(path.string(), &sha) && sha == file.sha256();
}
} // namespace

bool OpticalStorageServiceImpl::LoadHandoff() {
    const auto path = fs::path(archive_root_) / "library_handoff.pb";
    if (!fs::exists(path)) return true;
    std::string bytes;
    if (!zb::handoff::Read(path, &bytes) || !handoff_.ParseFromString(bytes)) {
        handoff_.Clear(); handoff_.set_phase("CORRUPT");
        return false; // 损坏时保持封闭，绝不启动空引擎覆盖旧产物。
    }
    const bool identity = (handoff_.source().node_id() == engine_config_.node_id &&
                           handoff_.source().instance_generation() == instance_generation_) ||
                          (handoff_.target().node_id() == engine_config_.node_id &&
                           handoff_.target().instance_generation() == instance_generation_);
    if (!identity || handoff_.format_signature() != format_signature_ ||
        (handoff_.phase() != "FREEZING" && !ValidateManifest(handoff_))) {
        handoff_.set_phase("CORRUPT"); return false;
    }
    return true;
}
bool OpticalStorageServiceImpl::SaveHandoff() {
    return zb::handoff::AtomicWrite(fs::path(archive_root_) / "library_handoff.pb", handoff_.SerializeAsString());
}
bool OpticalStorageServiceImpl::HandoffBlocksAdmission() const {
    return !handoff_.phase().empty() && handoff_.phase() != "ACTIVE";
}

void OpticalStorageServiceImpl::ManageLibraryHandoff(const zb::rpc::OpticalLibraryHandoffRequest& request,
                                                     zb::rpc::OpticalLibraryHandoffReply* reply) {
    std::unique_lock<zb::rpc_support::FiberSharedMutex> gate(handoff_mu_);
    const auto fail = [&](const std::string& message) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_NOT_READY);
        reply->mutable_status()->set_message(message);
    };
    const auto id = GetReplicationStatus();
    if (request.node_id() != id.node_id || request.instance_generation() != instance_generation_) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_STALE_GENERATION); return;
    }
    if (request.action() == zb::rpc::HANDOFF_QUERY) {
        if (!request.path().empty() && handoff_.phase() == "RECEIVING") {
            const auto it = std::find_if(handoff_.files().begin(), handoff_.files().end(),
                [&](const auto& f) { return f.path() == request.path(); });
            if (it == handoff_.files().end()) { fail("file outside receiver manifest"); return; }
            const auto path = fs::path(archive_root_) / ".handoff-receive" / request.path();
            std::error_code ec;
            if (fs::is_regular_file(fs::symlink_status(path, ec)) && !ec) {
                const auto size = fs::file_size(path, ec);
                if (!ec && size <= it->size() && (size != it->size() || Matches(path, *it))) reply->set_received_bytes(size);
            }
        }
        *reply->mutable_record() = handoff_;
        if (handoff_.phase().empty()) reply->mutable_record()->set_format_signature(format_signature_);
        reply->mutable_status(); return;
    }
    const auto& r = request.record();
    if (r.operation_id().empty() || r.operation_id().size() > 128 ||
        r.source().node_id().empty() || r.target().node_id().empty() ||
        r.source().node_id() == r.target().node_id() ||
        r.source().instance_generation() == 0 || r.target().instance_generation() == 0 ||
        r.source().node_address().empty() || r.target().node_address().empty()) {
        reply->mutable_status()->set_code(zb::rpc::OPTICAL_INVALID_ARGUMENT); return;
    }
    const bool source = r.source().node_id() == id.node_id && r.source().instance_generation() == instance_generation_;
    const bool target = r.target().node_id() == id.node_id && r.target().instance_generation() == instance_generation_;
    if (!source && !target) { fail("handoff does not address this instance"); return; }
    try {
        const fs::path root(archive_root_), staging = root / ".handoff-receive";
        if (request.action() == zb::rpc::HANDOFF_FREEZE) {
            if (!source) { fail("only source can freeze"); return; }
            if (handoff_.phase() == "ACTIVE" && handoff_.operation_id() != r.operation_id()) {
                // 接收库日后也可以成为迁出源。先确认上一份接收已进入 MDS
                // 权威映射，不能覆盖一个尚未完成提交的接收收据。
                brpc::Channel channel; brpc::ChannelOptions options; options.timeout_ms = 5000; options.max_retry = 0;
                if (channel.Init(mds_address_.c_str(), &options) != 0) { fail("previous handoff MDS unavailable"); return; }
                zb::rpc::MdsService_Stub stub(&channel); brpc::Controller controller;
                zb::rpc::CommitOpticalLibraryHandoffRequest check; *check.mutable_record() = handoff_;
                zb::rpc::CommitOpticalLibraryHandoffReply result;
                stub.CommitOpticalLibraryHandoff(&controller, &check, &result, nullptr);
                if (controller.Failed() || !result.has_status() || result.status().code() != zb::rpc::MDS_OK || !result.committed()) {
                    fail("previous receive not authority-committed; retain receipt"); return;
                }
                handoff_.Clear();
            }
            if (!handoff_.phase().empty() && (handoff_.operation_id() != r.operation_id() ||
                handoff_.target().SerializeAsString() != r.target().SerializeAsString())) {
                fail("source is reserved by another handoff"); return;
            }
            if (handoff_.phase().empty()) {
                handoff_ = r; handoff_.set_phase("FREEZING");
                handoff_.set_format_signature(format_signature_);
                if (!SaveHandoff()) { fail("cannot persist source admission fence"); return; }
            }
            if (handoff_.phase() == "FREEZING") {
                if (!optical_node_manager_->IsReady()) {
                    if (optical_node_manager_->GetStatusDetail().find("NOT persisted") != std::string::npos) {
                        fail("source checkpoint failed; retain original in-memory state for operator recovery"); return;
                    }
                    ResetHandoffEngine();
                    if (!optical_node_manager_->Run()) { fail("cannot restore source to take a complete checkpoint"); return; }
                }
                const auto inv = optical_node_manager_->GetInventory();
                if (!inv.metadata_ready || !inv.inventory_valid || !inv.background_valid ||
                    inv.pending_archive_batches || inv.background_tasks || inv.drive_busy) {
                    fail("waiting for admitted tasks and read references to finish"); return;
                }
                optical_node_manager_->Shutdown();
                if (optical_node_manager_->GetStatusDetail().find("NOT persisted") != std::string::npos) {
                    fail("source restart checkpoint failed"); return;
                }
                handoff_.clear_files();
                std::vector<std::string> paths;
                for (const auto* dir : kDirs) {
                    const auto parent = root / dir;
                    if (!fs::exists(parent)) continue;
                    if (fs::is_symlink(parent)) { fail("symlink artifact directory"); return; }
                    for (const auto& entry : fs::directory_iterator(parent)) {
                        const std::string relative = std::string(dir) + "/" + entry.path().filename().string();
                        if (std::string(dir) == "log" && relative != "log/runtime_state_snapshot") continue;
                        if (!fs::is_regular_file(entry.symlink_status()) || !zb::handoff::SafePath(relative)) {
                            fail("unsupported nonregular archive artifact"); return;
                        }
                        paths.push_back(relative);
                    }
                }
                std::sort(paths.begin(), paths.end());
                for (const auto& path : paths) {
                    auto* f = handoff_.add_files(); f->set_path(path); f->set_size(fs::file_size(root / path));
                    std::string sha;
                    if (!space_manager::Sha256FileHex((root / path).string(), &sha)) { fail("source checksum failed"); return; }
                    f->set_sha256(sha);
                }
                handoff_.set_manifest_id(zb::handoff::ManifestId(handoff_));
                if (!ValidateManifest(handoff_)) { fail("source checkpoint missing or manifest invalid"); return; }
                handoff_.set_phase("EXPORTED");
                if (!SaveHandoff()) { handoff_.set_phase("FREEZING"); fail("source manifest persistence failed"); return; }
            }
        } else if (request.action() == zb::rpc::HANDOFF_ACCEPT) {
            if (!target || r.format_signature() != format_signature_ || !ValidateManifest(r)) {
                fail("incompatible geometry or invalid manifest; requires compatible empty receiver"); return;
            }
            if (!handoff_.phase().empty()) {
                if (!SameManifest(handoff_, r)) { fail("receiver already reserved by another manifest"); return; }
            } else {
                const auto inv = optical_node_manager_->GetInventory();
                if (!inv.metadata_ready || !inv.inventory_valid || !inv.staging_valid || !inv.background_valid ||
                    inv.disc_used || inv.staging_used_images || inv.pending_archive_batches || inv.background_tasks ||
                    inv.drive_busy || !optical_node_manager_->ListArchivedImages().empty()) {
                    fail("receiver must be empty and idle"); return;
                }
                // 先关闭准入，持久保留占用；失败时不重新开放接收库。
                handoff_ = r; handoff_.set_phase("RECEIVING");
                if (!SaveHandoff()) { fail("receiver reservation persistence failed"); return; }
                optical_node_manager_->Shutdown();
            }
            if (handoff_.phase() == "RECEIVING") {
                if (fs::is_symlink(staging)) { fail("symlink staging root"); return; }
                for (const auto* dir : kDirs) {
                    fs::create_directories(staging / dir);
                    if (fs::is_symlink(staging / dir)) { fail("symlink staging directory"); return; }
                }
            }
        } else {
            if (!SameManifest(handoff_, r) || !ValidateManifest(handoff_)) {
                fail("handoff manifest changed"); return;
            }
            if (request.action() == zb::rpc::HANDOFF_READ || request.action() == zb::rpc::HANDOFF_WRITE) {
                auto metric = metrics_migration_.Start(request.action() == zb::rpc::HANDOFF_READ ?
                    zb::metrics::OperationKind::kRead : zb::metrics::OperationKind::kWrite);
                const auto found = std::find_if(handoff_.files().begin(), handoff_.files().end(),
                    [&](const auto& f) { return f.path() == request.path(); });
                if (found == handoff_.files().end() || request.offset() > found->size()) { fail("file outside manifest"); return; }
                if (request.action() == zb::rpc::HANDOFF_READ) {
                    if (!source || handoff_.phase() != "EXPORTED" || request.chunk_size() == 0 || request.chunk_size() > 1048576) {
                        fail("source not exported or invalid chunk size"); return;
                    }
                    const size_t size = std::min<uint64_t>(request.chunk_size(), found->size() - request.offset());
                    std::string data(size, '\0');
                    const int fd = ::open((root / request.path()).c_str(), O_RDONLY | O_NOFOLLOW);
                    if (fd < 0) { fail("cannot open source artifact"); return; }
                    size_t n = 0;
                    while (n < size) {
                        const auto got = ::pread(fd, data.data() + n, size - n, request.offset() + n);
                        if (got < 0 && errno == EINTR) continue;
                        if (got <= 0) break;
                        n += got;
                    }
                    ::close(fd);
                    if (n != size) { fail("short source artifact read"); return; }
                    reply->set_data(std::move(data));
                    metric.SetBytes(size);
                } else {
                    if (!target || handoff_.phase() != "RECEIVING" || request.data().size() > 1048576 ||
                        request.data().size() > found->size() - request.offset()) { fail("invalid receiver chunk"); return; }
                    const int fd = ::open((staging / request.path()).c_str(), O_WRONLY | O_CREAT | O_NOFOLLOW, 0600);
                    if (fd < 0) { fail("cannot open receiver artifact"); return; }
                    struct stat st{};
                    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || request.offset() > static_cast<uint64_t>(st.st_size)) {
                        ::close(fd); fail("receiver chunks must append or overwrite a contiguous prefix"); return;
                    }
                    const bool ok = zb::handoff::WriteAll(fd, request.data().data(), request.data().size(), request.offset()) && ::fsync(fd) == 0;
                    ::close(fd);
                    if (!ok) { fail("receiver chunk persistence failed"); return; }
                    metric.SetBytes(request.data().size());
                }
            } else if (request.action() == zb::rpc::HANDOFF_ACTIVATE) {
                if (!target) { fail("only receiver can activate"); return; }
                if (handoff_.phase() != "ACTIVE") {
                    if (handoff_.phase() != "RECEIVING" && handoff_.phase() != "INSTALLING") { fail("invalid activation phase"); return; }
                    // 验证所有字节后才安装；INSTALLING 重启按逐文件幂等恢复，不覆盖不匹配的产物。
                    for (const auto& f : handoff_.files()) {
                        const auto p = fs::exists(staging / f.path()) ? staging / f.path() : root / f.path();
                        if (!Matches(p, f)) { fail("receiver size/SHA256 mismatch: " + f.path()); return; }
                    }
                    handoff_.set_phase("INSTALLING");
                    if (!SaveHandoff()) { fail("cannot persist installation phase"); return; }
                    for (const auto& f : handoff_.files()) {
                        if (!fs::exists(staging / f.path())) continue;
                        fs::create_directories((root / f.path()).parent_path());
                        fs::rename(staging / f.path(), root / f.path());
                        if (!zb::handoff::SyncDir((root / f.path()).parent_path()) ||
                            !zb::handoff::SyncDir((staging / f.path()).parent_path())) { fail("installation fsync failed"); return; }
                    }
                    // Run 会把重启快照重命名为 .loaded；保留一份不可变导入
                    // 快照，覆盖 Run 后、ACTIVE 落盘前崩溃的恢复窗口。
                    fs::create_directories(staging / "log");
                    fs::copy_file(root / "log/runtime_state_snapshot", staging / "log/runtime_state_snapshot",
                                  fs::copy_options::overwrite_existing);
                    const int checkpoint = ::open((staging / "log/runtime_state_snapshot").c_str(), O_RDONLY | O_NOFOLLOW);
                    const bool durable_checkpoint = checkpoint >= 0 && ::fsync(checkpoint) == 0;
                    if (checkpoint >= 0) ::close(checkpoint);
                    if (!durable_checkpoint || !zb::handoff::SyncDir(staging / "log")) { fail("cannot preserve import checkpoint"); return; }
                    ResetHandoffEngine();
                    if (!optical_node_manager_->Run() || !optical_node_manager_->ArchiveIndexReady() ||
                        !optical_node_manager_->GetInventory().metadata_ready) { fail("receiver restart rebuild failed"); return; }
                    handoff_.set_phase("ACTIVE");
                    if (!SaveHandoff()) { handoff_.set_phase("INSTALLING"); fail("receiver receipt persistence failed"); return; }
                }
            } else if (request.action() == zb::rpc::HANDOFF_RELEASE) {
                if (!source || (handoff_.phase() != "EXPORTED" && handoff_.phase() != "RELEASED")) { fail("invalid source release phase"); return; }
                if (handoff_.phase() != "RELEASED") {
                    brpc::Channel channel; brpc::ChannelOptions opts; opts.timeout_ms = 5000; opts.max_retry = 0;
                    if (channel.Init(mds_address_.c_str(), &opts) != 0) { fail("cannot reach configured MDS"); return; }
                    zb::rpc::MdsService_Stub stub(&channel); brpc::Controller controller;
                    zb::rpc::CommitOpticalLibraryHandoffRequest check; *check.mutable_record() = handoff_;
                    zb::rpc::CommitOpticalLibraryHandoffReply checked;
                    stub.CommitOpticalLibraryHandoff(&controller, &check, &checked, nullptr);
                    if (controller.Failed() || !checked.has_status() || checked.status().code() != zb::rpc::MDS_OK || !checked.committed()) {
                        fail("MDS has not committed this exact handoff; retain source"); return;
                    }
                    // 原源产物移入隔离目录而非不可恢复删除；不再进入重建/库存路径。
                    const auto quarantine = root / ".handoff-retired";
                    fs::create_directories(quarantine);
                    for (const auto* dir : kDirs) {
                        if (fs::exists(root / dir)) fs::rename(root / dir, quarantine / dir);
                    }
                    if (!zb::handoff::SyncDir(quarantine) || !zb::handoff::SyncDir(root)) { fail("source isolation fsync failed"); return; }
                    handoff_.set_phase("RELEASED");
                    if (!SaveHandoff()) { handoff_.set_phase("EXPORTED"); fail("source release persistence failed"); return; }
                }
            } else { fail("unsupported handoff action"); return; }
        }
        *reply->mutable_record() = handoff_; reply->mutable_status();
    } catch (const fs::filesystem_error& e) { fail(std::string("handoff filesystem error: ") + e.what()); }
}
} // namespace zb::optical_node
