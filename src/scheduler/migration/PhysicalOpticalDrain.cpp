#include "PhysicalOpticalDrain.h"
#include <algorithm>
#include <chrono>
#include <brpc/channel.h>
#include <brpc/controller.h>

namespace zb::scheduler {
namespace {
zb::rpc::OpticalFileLocation Endpoint(const ManagedNodeRuntime& n) {
    zb::rpc::OpticalFileLocation result;
    result.set_node_id(n.profile.node_id); result.set_instance_generation(n.profile.instance_generation);
    result.set_node_address(n.identity.service_address); return result;
}
}
bool PhysicalOpticalDrain::Call(const zb::rpc::OpticalFileLocation& endpoint, zb::rpc::OpticalHandoffAction action,
    const zb::rpc::OpticalLibraryHandoffRecord& record, zb::rpc::OpticalLibraryHandoffReply* reply,
    std::string* error, const std::string& path, uint64_t offset, const std::string& data) {
    brpc::Channel channel; brpc::ChannelOptions options; options.timeout_ms = timeout_; options.max_retry = 0;
    reply->Clear();
    if (channel.Init(endpoint.node_address().c_str(), &options) != 0) { if (error) *error = "cannot connect optical handoff endpoint"; return false; }
    zb::rpc::OpticalLibraryHandoffRequest request;
    request.set_node_id(endpoint.node_id()); request.set_instance_generation(endpoint.instance_generation());
    request.set_action(action); *request.mutable_record() = record;
    request.set_path(path); request.set_offset(offset); request.set_data(data); request.set_chunk_size(1048576);
    brpc::Controller controller; zb::rpc::OpticalNodeService_Stub stub(&channel);
    stub.ManageLibraryHandoff(&controller, &request, reply, nullptr);
    if (controller.Failed() || !reply->has_status() || reply->status().code() != zb::rpc::OPTICAL_OK) {
        if (error) *error = controller.Failed() ? controller.ErrorText() : reply->status().message();
        return false;
    }
    return true;
}
bool PhysicalOpticalDrain::Begin(const std::string& id, uint64_t now, std::string* error) {
    ManagedNodeRuntime source;
    if (!nodes_->GetNode(id, &source)) { if (error) *error = "source absent"; return false; }
    zb::rpc::OpticalLibraryHandoffReply query;
    if (!Call(Endpoint(source), zb::rpc::HANDOFF_QUERY, {}, &query, error)) return false;
    auto record = query.record();
    if (record.phase() == "ACTIVE" && record.target().node_id() == id) record.Clear();
    if (record.phase().empty()) {
        auto targets = nodes_->ListNodes();
        std::sort(targets.begin(), targets.end(), [](const auto& a, const auto& b) { return a.profile.node_id < b.profile.node_id; });
        for (const auto& target : targets) {
            const auto& inv = target.optical_library;
            if (target.profile.node_id == id || target.profile.kind != ManagedNodeKind::kOpticalLibrary ||
                target.profile.execution_mode != ExecutionMode::kPhysical || target.lifecycle != LifecycleState::kWorking ||
                target.health != ManagedHealthState::kHealthy || target.identity.service_address.empty() ||
                !inv.observed || !inv.inventory_valid || !inv.staging_valid || !inv.background_valid ||
                inv.recorded_disc_count || inv.recording_disc_count || inv.staging_used_bytes ||
                inv.pending_archive_batches || inv.background_tasks || inv.busy_drives) continue;
            zb::rpc::OpticalLibraryHandoffReply receiver;
            if (!Call(Endpoint(target), zb::rpc::HANDOFF_QUERY, {}, &receiver, nullptr) ||
                !receiver.record().phase().empty() ||
                receiver.record().format_signature() != query.record().format_signature()) continue;
            *record.mutable_source() = Endpoint(source); *record.mutable_target() = Endpoint(target);
            record.set_operation_id("physical-library-" + std::to_string(now) + "-" + std::to_string(source.profile.instance_generation));
            break;
        }
        if (!record.has_target()) { if (error) *error = "no idle empty physical optical receiver; source retained"; return false; }
    }
    if (!Call(Endpoint(source), zb::rpc::HANDOFF_FREEZE, record, &query, error)) {
        zb::rpc::OpticalLibraryHandoffReply fenced;
        if (!Call(Endpoint(source), zb::rpc::HANDOFF_QUERY, {}, &fenced, nullptr) ||
            fenced.record().operation_id() != record.operation_id() || fenced.record().phase() != "FREEZING") return false;
    }
    return nodes_->BeginRemoveNode(id, now, error);
}
bool PhysicalOpticalDrain::Commit(const zb::rpc::OpticalLibraryHandoffRecord& record, std::string* error) {
    brpc::Channel channel; brpc::ChannelOptions opts; opts.timeout_ms = timeout_; opts.max_retry = 0;
    if (channel.Init(mds_.c_str(), &opts) != 0) { if (error) *error = "cannot initialize MDS handoff channel"; return false; }
    zb::rpc::CommitOpticalLibraryHandoffRequest request; *request.mutable_record() = record; request.set_commit(true);
    zb::rpc::CommitOpticalLibraryHandoffReply reply; brpc::Controller controller; zb::rpc::MdsService_Stub stub(&channel);
    stub.CommitOpticalLibraryHandoff(&controller, &request, &reply, nullptr);
    if (controller.Failed() || !reply.has_status() || reply.status().code() != zb::rpc::MDS_OK || !reply.committed()) {
        if (error) *error = controller.Failed() ? controller.ErrorText() : reply.status().message();
        return false;
    }
    return true;
}
bool PhysicalOpticalDrain::Advance(DrainMigrationTask* task, uint64_t now, std::string* error) {
    ManagedNodeRuntime source;
    if (!nodes_->GetNode(task->source_node_id, &source)) { if (error) *error = "source absent"; return false; }
    zb::rpc::OpticalLibraryHandoffReply query;
    if (!Call(Endpoint(source), zb::rpc::HANDOFF_QUERY, {}, &query, error)) return false;
    auto record = query.record();
    if (record.phase() == "FREEZING") {
        if (!Call(Endpoint(source), zb::rpc::HANDOFF_FREEZE, record, &query, error)) return false;
        record = query.record();
    }
    if (record.phase() == "EXPORTED") {
        zb::rpc::OpticalLibraryHandoffReply receiver;
        if (!Call(record.target(), zb::rpc::HANDOFF_ACCEPT, record, &receiver, error)) return false;
        task->total_objects = record.files_size(); task->total_bytes = 0;
        for (const auto& f : record.files()) task->total_bytes += f.size();
        if (receiver.record().phase() == "RECEIVING") {
            auto& cursor = cursors_[task->source_node_id];
            if (cursor.manifest != record.manifest_id()) { cursor.manifest = record.manifest_id(); cursor.file = 0; }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(100);
            uint32_t chunks = 0;
            // 一步最多 32MiB/约100ms（单 RPC 仍受配置超时限制），不拿全局目录锁。
            // 保留文件游标，避免每传一块都从第一个大盘文件重新计算 SHA256。
            while (cursor.file < static_cast<size_t>(record.files_size())) {
                const auto& f = record.files(cursor.file);
                if (!Call(record.target(), zb::rpc::HANDOFF_QUERY, record, &receiver, error, f.path())) return false;
                const auto offset = receiver.received_bytes();
                if (offset == f.size() && f.size() != 0) { ++cursor.file; continue; }
                zb::rpc::OpticalLibraryHandoffReply chunk, written;
                if (!Call(record.source(), zb::rpc::HANDOFF_READ, record, &chunk, error, f.path(), offset) ||
                    !Call(record.target(), zb::rpc::HANDOFF_WRITE, record, &written, error, f.path(), offset, chunk.data())) return false;
                if (offset + chunk.data().size() == f.size()) ++cursor.file;
                if (++chunks >= 32 || std::chrono::steady_clock::now() >= deadline) { if (error) error->clear(); return false; }
            }
        }
        if (!Call(record.target(), zb::rpc::HANDOFF_ACTIVATE, record, &receiver, error)) {
            cursors_.erase(task->source_node_id); return false;
        }
        if (!Commit(record, error)) return false;
        if (!Call(record.source(), zb::rpc::HANDOFF_RELEASE, record, &query, error)) return false;
        record = query.record();
    }
    if (record.phase() != "RELEASED") { if (error) *error = "physical library handoff not released"; return false; }
    task->migrated_objects = record.files_size(); task->migrated_bytes = task->total_bytes;
    // 必须等真实节点上报已隔离的空库存，保留通用停机/零引用/退役安全门禁。
    nodes_->GetNode(task->source_node_id, &source);
    if (!source.optical_library.inventory_valid || source.optical_library.local_disc_count != 0 ||
        source.optical_library.background_tasks || source.optical_library.staging_used_bytes) {
        if (error) error->clear(); return false;
    }
    // 库存与心跳分两个 RPC 上报，不能拿“新空库存 + 旧 resident 字节”
    // 进入通用完成检查而永久 Blocked。精确 MDS 提交和源 RELEASED 已证明
    // 全部活动产物完成交接，此处提交本任务自己的最终排空进度。
    return nodes_->ReportDrainProgress(task->source_node_id, now, 0, 0, 0, error);
}
} // namespace zb::scheduler
