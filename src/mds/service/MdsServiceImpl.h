#pragma once

#include <brpc/channel.h>

#include <memory>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../allocator/ObjectAllocator.h"
#include "../allocator/CmsNodeRegistry.h"
#include "../archive_meta/ArchiveGenerationPublisher.h"
#include "../archive_meta/ArchiveImportService.h"
#include "../archive_meta/ArchiveMetaStore.h"
#include "../archive_meta/ArchiveNamespaceCatalog.h"
#include "../common/SystemMemory.h"
#include "../archive/FileArchiveCandidateQueue.h"
#include "../archive/ArchiveLeaseManager.h"
#include "../masstree_meta/MasstreeImportService.h"
#include "../masstree_meta/MasstreeMetaStore.h"
#include "../masstree_meta/MasstreeNamespaceCatalog.h"
#include "../masstree_meta/MasstreeStatsStore.h"
#include "../storage/MetaCodec.h"
#include "../storage/MetaStoreRouter.h"
#include "../storage/MetaSchema.h"
#include "../storage/RocksMetaStore.h"
#include "../storage/RocksNamespaceStore.h"
#include "mds.pb.h"
#include "../location/FileLocationResolver.h"
#include "common/metrics/NodeMetricsCollector.h"
#include "common/rpc/FiberSharedMutex.h"
#include <bthread/mutex.h>

namespace zb::mds {

class MdsServiceImpl : public zb::rpc::MdsService {
public:
    MdsServiceImpl(RocksMetaStore* store,
                   ObjectAllocator* allocator,
                   uint64_t default_object_unit_size,
                   const std::string& archive_meta_root,
                   const std::string& masstree_root,
                   const ArchiveMetaStore::Options& archive_meta_options,
                   uint32_t archive_import_page_size_bytes,
                   bool strict_tier_bypass_pg,
                   bool masstree_preload_all_sparse_on_start,
                   double masstree_preload_memory_utilization_limit,
                   uint64_t masstree_preload_memory_reserve_bytes,
                   double masstree_preload_estimate_multiplier,
                   bool masstree_preload_background,
                   CmsNodeRegistry* node_registry,
                   FileArchiveCandidateQueue* candidate_queue = nullptr,
                   ArchiveLeaseManager* lease_manager = nullptr,
                   NamespaceStore* namespace_store = nullptr, bool unified_namespace = false);
    ~MdsServiceImpl() override;
    void CommitPhysicalOpticalArchive(google::protobuf::RpcController*, const zb::rpc::ModelOpticalArchiveRequest*,
                                      zb::rpc::ModelOpticalArchiveReply*, google::protobuf::Closure*) override;
    // 整库权威交接：目标持久重建完成后切换；查询用于源释放门禁。
    void CommitOpticalLibraryHandoff(google::protobuf::RpcController*,
        const zb::rpc::CommitOpticalLibraryHandoffRequest*, zb::rpc::CommitOpticalLibraryHandoffReply*,
        google::protobuf::Closure*) override;

    zb::metrics::NodeMetricsCollector* MetricsCollector() { return &metrics_; }
    zb::rpc_support::FiberSharedMutex* PlacementTransactionMutex() { return &placement_transaction_mu_; }

    void Lookup(google::protobuf::RpcController* cntl_base,
                const zb::rpc::LookupRequest* request,
                zb::rpc::LookupReply* response,
                google::protobuf::Closure* done) override;

    void Getattr(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::GetattrRequest* request,
                 zb::rpc::GetattrReply* response,
                 google::protobuf::Closure* done) override;

    void Open(google::protobuf::RpcController* cntl_base,
              const zb::rpc::OpenRequest* request,
              zb::rpc::OpenReply* response,
              google::protobuf::Closure* done) override;

    void Close(google::protobuf::RpcController* cntl_base,
               const zb::rpc::CloseRequest* request,
               zb::rpc::CloseReply* response,
               google::protobuf::Closure* done) override;

    void Create(google::protobuf::RpcController* cntl_base,
                const zb::rpc::CreateRequest* request,
                zb::rpc::CreateReply* response,
                google::protobuf::Closure* done) override;

    void Mkdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::MkdirRequest* request,
               zb::rpc::MkdirReply* response,
               google::protobuf::Closure* done) override;

    void Readdir(google::protobuf::RpcController* cntl_base,
                 const zb::rpc::ReaddirRequest* request,
                 zb::rpc::ReaddirReply* response,
                 google::protobuf::Closure* done) override;

    void Rename(google::protobuf::RpcController* cntl_base,
                const zb::rpc::RenameRequest* request,
                zb::rpc::RenameReply* response,
                google::protobuf::Closure* done) override;

    void Unlink(google::protobuf::RpcController* cntl_base,
                const zb::rpc::UnlinkRequest* request,
                zb::rpc::UnlinkReply* response,
                google::protobuf::Closure* done) override;

    void Rmdir(google::protobuf::RpcController* cntl_base,
               const zb::rpc::RmdirRequest* request,
               zb::rpc::RmdirReply* response,
               google::protobuf::Closure* done) override;
    void AllocateAvailableImageId(google::protobuf::RpcController*, const zb::rpc::AllocateAvailableImageIdRequest*,
        zb::rpc::AllocateAvailableImageIdReply*, google::protobuf::Closure*) override;
    void AllocateAvailableDiscId(google::protobuf::RpcController*, const zb::rpc::AllocateAvailableDiscIdRequest*,
        zb::rpc::AllocateAvailableDiscIdReply*, google::protobuf::Closure*) override;
    void ReportFilesPackedToImage(google::protobuf::RpcController*, const zb::rpc::ReportFilesPackedToImageRequest*,
        zb::rpc::SimpleArchiveReportReply*, google::protobuf::Closure*) override;
    void ReportImagesBurnedToDisc(google::protobuf::RpcController*, const zb::rpc::ReportImagesBurnedToDiscRequest*,
        zb::rpc::SimpleArchiveReportReply*, google::protobuf::Closure*) override;
    void SendSubtreeArchive(google::protobuf::RpcController*,
                           const zb::rpc::SendSubtreeArchiveRequest*,
                           zb::rpc::SendSubtreeArchiveReply*, google::protobuf::Closure*) override;
    void GetFileLocation(google::protobuf::RpcController* cntl_base,
                       const zb::rpc::GetFileLocationRequest* request,
                       zb::rpc::GetFileLocationReply* response,
                       google::protobuf::Closure* done) override;
    void CommitFileMigration(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::CommitFileMigrationRequest* request,
                             zb::rpc::CommitFileMigrationReply* response,
                             google::protobuf::Closure* done) override;
    void ModelOpticalArchive(google::protobuf::RpcController* cntl_base,
                             const zb::rpc::ModelOpticalArchiveRequest* request,
                             zb::rpc::ModelOpticalArchiveReply* response,
                             google::protobuf::Closure* done) override;
    // Scheduler 模型光盘排空的最小位置 CAS 适配，不执行真实光盘业务。
    void CommitModelOpticalMigration(google::protobuf::RpcController*,
                                    const zb::rpc::CommitModelOpticalMigrationRequest*,
                                    zb::rpc::CommitModelOpticalMigrationReply*,
                                    google::protobuf::Closure*) override;
    void CommitCmsNodeCatalog(google::protobuf::RpcController* cntl_base,
                              const zb::rpc::CommitCmsNodeCatalogRequest* request,
                              zb::rpc::CommitCmsNodeCatalogReply* response,
                              google::protobuf::Closure* done) override;
    void GetCmsNodeCatalog(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::GetCmsNodeCatalogRequest* request,
                           zb::rpc::GetCmsNodeCatalogReply* response,
                           google::protobuf::Closure* done) override;
    void VerifyNodeReferences(google::protobuf::RpcController* cntl_base,
                              const zb::rpc::VerifyNodeReferencesRequest* request,
                              zb::rpc::VerifyNodeReferencesReply* response,
                              google::protobuf::Closure* done) override;
    void UpdateInodeStat(google::protobuf::RpcController* cntl_base,
                         const zb::rpc::UpdateInodeStatRequest* request,
                         zb::rpc::UpdateInodeStatReply* response,
                         google::protobuf::Closure* done) override;

    void ReportArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::ReportArchiveCandidatesRequest* request,
                                 zb::rpc::ReportArchiveCandidatesReply* response,
                                 google::protobuf::Closure* done) override;
    void ReportFileArchiveCandidates(google::protobuf::RpcController* cntl_base,
                                     const zb::rpc::ReportFileArchiveCandidatesRequest* request,
                                     zb::rpc::ReportFileArchiveCandidatesReply* response,
                                     google::protobuf::Closure* done) override;
    void ClaimArchiveTask(google::protobuf::RpcController* cntl_base,
                          const zb::rpc::ClaimArchiveTaskRequest* request,
                          zb::rpc::ClaimArchiveTaskReply* response,
                          google::protobuf::Closure* done) override;
    void RenewArchiveLease(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::RenewArchiveLeaseRequest* request,
                           zb::rpc::RenewArchiveLeaseReply* response,
                           google::protobuf::Closure* done) override;
    void CommitArchiveTask(google::protobuf::RpcController* cntl_base,
                           const zb::rpc::CommitArchiveTaskRequest* request,
                           zb::rpc::CommitArchiveTaskReply* response,
                           google::protobuf::Closure* done) override;
    void SetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::SetPathPlacementPolicyRequest* request,
                                zb::rpc::SetPathPlacementPolicyReply* response,
                                google::protobuf::Closure* done) override;
    void DeletePathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::DeletePathPlacementPolicyRequest* request,
                                   zb::rpc::DeletePathPlacementPolicyReply* response,
                                   google::protobuf::Closure* done) override;
    void GetPathPlacementPolicy(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::GetPathPlacementPolicyRequest* request,
                                zb::rpc::GetPathPlacementPolicyReply* response,
                                google::protobuf::Closure* done) override;
    void ImportArchiveNamespace(google::protobuf::RpcController* cntl_base,
                                const zb::rpc::ImportArchiveNamespaceRequest* request,
                                zb::rpc::ImportArchiveNamespaceReply* response,
                                google::protobuf::Closure* done) override;
    void GenerateMasstreeTemplate(google::protobuf::RpcController* cntl_base,
                                  const zb::rpc::GenerateMasstreeTemplateRequest* request,
                                  zb::rpc::GenerateMasstreeTemplateReply* response,
                                  google::protobuf::Closure* done) override;
    void ImportMasstreeNamespace(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::ImportMasstreeNamespaceRequest* request,
                                 zb::rpc::ImportMasstreeNamespaceReply* response,
                                 google::protobuf::Closure* done) override;
    void GetMasstreeImportJob(google::protobuf::RpcController* cntl_base,
                              const zb::rpc::GetMasstreeImportJobRequest* request,
                              zb::rpc::GetMasstreeImportJobReply* response,
                              google::protobuf::Closure* done) override;
    void GetRandomMasstreeFileAttr(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::GetRandomMasstreeFileAttrRequest* request,
                                   zb::rpc::GetRandomMasstreeFileAttrReply* response,
                                   google::protobuf::Closure* done) override;
    void GetRandomMasstreeLookupPaths(google::protobuf::RpcController* cntl_base,
                                      const zb::rpc::GetRandomMasstreeLookupPathsRequest* request,
                                      zb::rpc::GetRandomMasstreeLookupPathsReply* response,
                                      google::protobuf::Closure* done) override;
    void GetMasstreeClusterStats(google::protobuf::RpcController* cntl_base,
                                 const zb::rpc::GetMasstreeClusterStatsRequest* request,
                                 zb::rpc::GetMasstreeClusterStatsReply* response,
                                 google::protobuf::Closure* done) override;
    void GetMasstreeNamespaceStats(google::protobuf::RpcController* cntl_base,
                                   const zb::rpc::GetMasstreeNamespaceStatsRequest* request,
                                   zb::rpc::GetMasstreeNamespaceStatsReply* response,
                                   google::protobuf::Closure* done) override;

private:
    void HandleOpticalArchive(const zb::rpc::ModelOpticalArchiveRequest*, zb::rpc::ModelOpticalArchiveReply*, bool physical);
    bool VerifyPhysicalArchive(const zb::rpc::ModelOpticalArchiveRequest&, std::string*) const;
    bool ResolveOpticalHandoff(zb::rpc::OpticalFileLocation*, std::string*) const;
    void ResolvePackedOpticalNode(zb::rpc::OpticalFileLocation*) const;
    bool CountColdNodeReferences(const std::string&, uint64_t*, uint64_t*, std::string*) const;
    struct MasstreeImportJob {
        enum class Kind {
            kGenerateTemplate,
            kImportTemplateNamespace,
        };

        std::string job_id;
        Kind kind{Kind::kImportTemplateNamespace};
        MasstreeImportService::TemplateGenerationRequest template_request;
        MasstreeImportService::TemplateImportRequest import_request;
        MasstreeImportService::Result result;
        zb::rpc::MasstreeImportJobState state{zb::rpc::MASSTREE_IMPORT_JOB_PENDING};
        std::string error_message;
    };

    bool RecoverArchiveCatalog(std::string* error);
    bool EnsureRoot(std::string* error);
    bool ResolvePath(const std::string& path, uint64_t* inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool ResolveParent(const std::string& path, uint64_t* parent_inode, std::string* name, std::string* error);
    bool GetInode(uint64_t inode_id, zb::rpc::InodeAttr* attr, std::string* error);
    bool PutInode(uint64_t inode_id, const zb::rpc::InodeAttr& attr, std::string* error);
    bool DeleteInodeData(uint64_t inode_id, std::string* error);
    bool ResolveObjectReplicas(uint32_t replica_count,
                               const std::string& object_id,
                               uint64_t placement_epoch,
                               std::vector<zb::rpc::ReplicaLocation>* replicas,
                               std::string* error) const;
    bool SelectFilePrimaryLocation(uint64_t inode_id,
                                   const zb::rpc::InodeAttr& attr,
                                   zb::rpc::ReplicaLocation* location,
                                   std::string* error) const;
    bool SelectFilePrimaryLocationWithPreference(uint64_t inode_id,
                                                 const zb::rpc::InodeAttr& attr,
                                                 NodeType preferred_type,
                                                 bool strict_type,
                                                 zb::rpc::ReplicaLocation* location,
                                                 std::string* error) const;
    bool MatchPathPlacementPolicy(const std::string& path,
                                  zb::rpc::PathPlacementPolicyRecord* policy,
                                  std::string* matched_prefix,
                                  std::string* error) const;
    bool SavePathPlacementPolicy(const zb::rpc::PathPlacementPolicyRecord& policy, std::string* error);
    bool DeletePathPlacementPolicyByPrefix(const std::string& path_prefix, std::string* error);
    bool LoadDiskFileLocation(uint64_t inode_id, zb::rpc::DiskFileLocation* location, std::string* error) const;
    bool BuildDiskFileLocationFromAttr(const zb::rpc::InodeAttr&, zb::rpc::DiskFileLocation*, std::string*) const;
    bool BuildOpticalFileLocationFromAttr(const zb::rpc::InodeAttr&, uint64_t,
                                         zb::rpc::OpticalFileLocation*, std::string*) const;
    bool SaveDiskFileLocation(uint64_t inode_id,
                              const zb::rpc::DiskFileLocation& location,
                              rocksdb::WriteBatch* batch) const;
    bool LoadOpticalFileLocation(uint64_t inode_id, zb::rpc::OpticalFileLocation* location, std::string* error) const;
    bool LoadMasstreeOpticalFileLocation(uint64_t inode_id,
                                         zb::rpc::OpticalFileLocation* location,
                                         std::string* error) const;
    bool SaveOpticalFileLocation(uint64_t inode_id,
                                 const zb::rpc::OpticalFileLocation& location,
                                 rocksdb::WriteBatch* batch) const;
    bool BuildFileLocationView(uint64_t inode_id,
                               const zb::rpc::InodeAttr& attr,
                               zb::rpc::FileLocationView* view,
                               std::string* error) const;
    bool LoadFilePrimaryLocation(uint64_t inode_id,
                                 const zb::rpc::InodeAttr& attr,
                                 zb::rpc::ReplicaLocation* anchor,
                                 std::string* error) const;
    static std::string BuildStableObjectId(uint64_t inode_id, uint32_t object_index);
    static void StripReplicaAddresses(zb::rpc::ReplicaLocation* replica);
    bool ResolveNodeAddress(const std::string& node_id, std::string* address, std::string* error) const;
    bool DeleteFileMetaOnAnchor(const zb::rpc::ReplicaLocation& anchor,
                                uint64_t inode_id,
                                bool purge_objects,
                                std::string* error);
    bool ResolveMasstreeRoute(const std::string& namespace_id,
                              const std::string& path_prefix,
                              MasstreeNamespaceRoute* route,
                              std::string* error) const;
    bool PickRandomMasstreeFile(const MasstreeNamespaceRoute& route,
                                const std::string& query_mode,
                                uint64_t* inode_id,
                                zb::rpc::InodeAttr* attr,
                                std::string* file_name,
                                std::string* full_path,
                                std::string* error) const;
    bool PickRandomMasstreeFileInode(const MasstreeNamespaceRoute& route,
                                     uint64_t* inode_id,
                                     zb::rpc::InodeAttr* attr,
                                     std::string* error) const;
    void RunMasstreeImportWorker();
    void TrimMasstreeImportJobsLocked();
    std::shared_ptr<MasstreeImportJob> EnqueueMasstreeTemplateJob(
        const MasstreeImportService::TemplateGenerationRequest& request);
    std::shared_ptr<MasstreeImportJob> EnqueueMasstreeNamespaceImportJob(
        const MasstreeImportService::TemplateImportRequest& request);
    std::shared_ptr<MasstreeImportJob> FindMasstreeImportJob(const std::string& job_id) const;
    static void FillMasstreeImportJobInfo(const MasstreeImportJob& job, zb::rpc::MasstreeImportJobInfo* info);
    brpc::Channel* GetDataChannel(const std::string& address, std::string* error);
    void TryPreloadMasstreeSparseIndexes();
    void RunMasstreePreloadWorker();

    uint64_t AllocateHandleId(std::string* error);
    static uint64_t NowSeconds();
    static uint64_t NowMilliseconds();
    static void FillStatus(zb::rpc::MdsStatus* status, zb::rpc::MdsStatusCode code, const std::string& message);

    RocksMetaStore* store_{};
    RocksNamespaceStore default_namespace_store_;
    bool unified_namespace_ = false;
    std::unique_ptr<RecordFileLocationResolver> location_resolver_;
    // Simple archive allocation records only; no image -> inode reverse index yet.
    struct SimpleArchiveAllocation { uint64_t id; std::string node_id; };
    std::mutex simple_archive_mutex_;
    uint64_t next_simple_archive_id_{1};
    std::unordered_map<std::string, SimpleArchiveAllocation> simple_images_, simple_discs_;
    NamespaceStore* namespace_store_{}; // borrowed, must outlive this service
    ObjectAllocator* allocator_{};
    CmsNodeRegistry* node_registry_{};
    uint64_t default_object_unit_size_{0};
    std::string archive_meta_root_;
    std::string masstree_root_;
    uint32_t archive_import_page_size_bytes_{0};
    bool strict_tier_bypass_pg_{false};
    bool masstree_preload_all_sparse_on_start_{true};
    double masstree_preload_memory_utilization_limit_{0.70};
    uint64_t masstree_preload_memory_reserve_bytes_{128ULL * 1024ULL * 1024ULL * 1024ULL};
    double masstree_preload_estimate_multiplier_{2.5};
    bool masstree_preload_background_{true};
    FileArchiveCandidateQueue* candidate_queue_{};
    ArchiveLeaseManager* lease_manager_{};
    ArchiveNamespaceCatalog archive_namespace_catalog_;
    ArchiveMetaStore archive_meta_store_;
    MasstreeNamespaceCatalog masstree_namespace_catalog_;
    MasstreeImportService masstree_import_service_;
    MasstreeMetaStore masstree_meta_store_;
    ArchiveImportService archive_import_service_;
    MetaStoreRouter meta_router_;
    mutable std::mutex masstree_import_job_mu_;
    std::condition_variable masstree_import_job_cv_;
    std::deque<std::shared_ptr<MasstreeImportJob>> masstree_import_job_queue_;
    std::deque<std::string> masstree_import_job_history_;
    std::unordered_map<std::string, std::shared_ptr<MasstreeImportJob>> masstree_import_jobs_;
    std::thread masstree_import_worker_;
    std::thread masstree_preload_worker_;
    zb::metrics::NodeMetricsCollector metrics_;
    uint64_t masstree_import_next_job_id_{1};
    bool stop_masstree_import_worker_{false};
    mutable std::mutex channel_mu_;
    mutable bthread::Mutex file_migration_mu_;
    // Shared by Create and exclusively held by catalog admission changes and
    // retirement verification. This closes the allocation-vs-retire race.
    mutable zb::rpc_support::FiberSharedMutex placement_transaction_mu_;
    std::unordered_map<std::string, std::unique_ptr<brpc::Channel>> channels_;
};

} // namespace zb::mds
