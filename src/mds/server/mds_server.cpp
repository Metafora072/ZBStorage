#include <brpc/server.h>
#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>

#include "../allocator/ObjectAllocator.h"
#include "../allocator/NodeStateCache.h"
#include "../allocator/CmsNodeRegistry.h"
#include "../allocator/PGManager.h"
#include "../archive/ArchiveBatchStager.h"
#include "../archive/FileArchiveCandidateQueue.h"
#include "../archive/ArchiveLeaseManager.h"
#include "../archive/OpticalArchiveManager.h"
#include "../config/MdsConfig.h"
#include "../service/MdsServiceImpl.h"
#include "../storage/RocksMetaStore.h"
#include "scheduler.pb.h"
#include "common/metrics/SchedulerMetricsClient.h"

DEFINE_string(config, "", "Path to MDS config file");
DEFINE_int32(port, 9000, "Port for MDS brpc server");
DEFINE_int32(idle_timeout_sec, -1, "Idle timeout for connections");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_config.empty()) {
        std::cerr << "Missing --config, please specify config file path" << std::endl;
        return 1;
    }

    std::string error;
    zb::mds::MdsConfig cfg = zb::mds::MdsConfig::LoadFromFile(FLAGS_config, &error);
    if (!error.empty()) {
        std::cerr << "Failed to load config: " << error << std::endl;
        return 1;
    }

    zb::mds::RocksMetaStore store;
    if (!store.Open(cfg.db_path, &error)) {
        std::cerr << "Failed to open RocksDB: " << error << std::endl;
        return 1;
    }

    // With Scheduler integration enabled, placement remains empty until CMS
    // accepts a complete catalog proposal. Static MDS nodes are only a
    // standalone-mode bootstrap, never a competing authority.
    const std::vector<zb::mds::NodeInfo> bootstrap_nodes =
        cfg.scheduler_address.empty() ? cfg.nodes : std::vector<zb::mds::NodeInfo>{};
    zb::mds::NodeStateCache cache(bootstrap_nodes);
    zb::mds::PGManager::Options pg_options;
    pg_options.pg_count = cfg.pg_count;
    pg_options.replica = cfg.replica > 0 ? cfg.replica : 1;
    zb::mds::PGManager pg_manager(pg_options);
    if (!bootstrap_nodes.empty()) {
        const uint64_t initial_pg_epoch = cfg.pg_view_epoch > 0 ? cfg.pg_view_epoch : 1;
        if (!pg_manager.RebuildFromNodes(bootstrap_nodes, initial_pg_epoch, &error)) {
            std::cerr << "Failed to initialize PG placement view: " << error << std::endl;
            return 1;
        }
    }
    zb::mds::ObjectAllocator allocator(&cache, &pg_manager);
    zb::mds::CmsNodeRegistry node_registry(&cache, &pg_manager, &store);
    if (!node_registry.Restore(&error)) {
        std::cerr << "Failed to restore CMS node catalog: " << error << std::endl;
        return 1;
    }
    zb::mds::FileArchiveCandidateQueue candidate_queue(cfg.archive_candidate_queue_size);
    zb::mds::ArchiveLeaseManager::Options lease_options;
    lease_options.default_lease_ms = cfg.archive_lease_default_ms;
    lease_options.min_lease_ms = cfg.archive_lease_min_ms;
    lease_options.max_lease_ms = cfg.archive_lease_max_ms;
    zb::mds::ArchiveLeaseManager lease_manager(&store, lease_options);
    zb::mds::ArchiveBatchStager batch_stager;
    zb::mds::ArchiveMetaStore::Options archive_meta_options;
    archive_meta_options.max_cached_generations = cfg.archive_meta_cache_generations;
    archive_meta_options.max_cached_bytes = cfg.archive_meta_cache_bytes;
    zb::mds::MdsServiceImpl service(&store,
                                    &allocator,
                                    cfg.object_unit_size,
                                    cfg.archive_meta_root,
                                    cfg.masstree_root,
                                    archive_meta_options,
                                    cfg.archive_import_page_size_bytes,
                                    cfg.strict_tier_bypass_pg,
                                    cfg.masstree_preload_all_sparse_on_start,
                                    cfg.masstree_preload_memory_utilization_limit,
                                    cfg.masstree_preload_memory_reserve_bytes,
                                    cfg.masstree_preload_estimate_multiplier,
                                    cfg.masstree_preload_background,
                                    &node_registry,
                                    &candidate_queue,
                                    &lease_manager);
    std::unique_ptr<zb::mds::OpticalArchiveManager> archive_manager;
    if (cfg.enable_optical_archive) {
        zb::mds::ArchiveBatchStager::Options stager_options;
        stager_options.disc_size_bytes = cfg.archive_disc_size_bytes;
        stager_options.strict_full_disc = cfg.archive_strict_full_disc;
        stager_options.max_batch_age_ms = cfg.archive_batch_max_age_ms;
        std::string staging_dir = cfg.archive_staging_dir;
        if (staging_dir.empty()) {
            staging_dir = cfg.db_path + "/archive_staging";
        }
        if (!batch_stager.Init(staging_dir, stager_options, &error)) {
            std::cerr << "Failed to init archive batch stager: " << error << std::endl;
            return 1;
        }

        zb::mds::OpticalArchiveManager::Options options;
        options.archive_trigger_bytes = cfg.archive_trigger_bytes;
        options.archive_target_bytes = cfg.archive_target_bytes;
        options.cold_file_ttl_sec = cfg.cold_file_ttl_sec;
        options.max_objects_per_round = cfg.archive_max_objects_per_round;
        options.default_object_unit_size = cfg.object_unit_size;
        archive_manager =
            std::make_unique<zb::mds::OpticalArchiveManager>(&store,
                                                              &cache,
                                                              &candidate_queue,
                                                              &batch_stager,
                                                              &lease_manager,
                                                              options,
                                                              service.PlacementTransactionMutex());
    }

    std::atomic<bool> stop_sync{false};
    std::thread sync_thread;
    std::thread archive_thread;
    if (!cfg.scheduler_address.empty()) {
        sync_thread = std::thread([&]() {
            uint32_t refresh_ms = cfg.scheduler_refresh_ms > 0 ? cfg.scheduler_refresh_ms : 2000;
            const std::string managed_node_id = cfg.managed_node_id.empty()
                                                    ? "mds-" + std::to_string(FLAGS_port)
                                                    : cfg.managed_node_id;
            std::unique_ptr<brpc::Channel> channel;
            bool metadata_registered = false;
            while (!stop_sync.load()) {
                if (!channel) {
                    auto new_channel = std::make_unique<brpc::Channel>();
                    brpc::ChannelOptions options;
                    options.protocol = "baidu_std";
                    options.timeout_ms = 2000;
                    options.max_retry = 0;
                    if (new_channel->Init(cfg.scheduler_address.c_str(), &options) != 0) {
                        std::cerr << "Failed to connect Scheduler at " << cfg.scheduler_address << std::endl;
                        std::this_thread::sleep_for(std::chrono::milliseconds(refresh_ms));
                        continue;
                    }
                    channel = std::move(new_channel);
                }
                if (!metadata_registered) {
                    std::string register_error;
                    const uint64_t now_ms = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count());
                    metadata_registered = zb::metrics::EnsureMetadataNodeRegistered(
                        channel.get(), managed_node_id, now_ms, &register_error);
                    if (!metadata_registered) {
                        std::cerr << "Failed to register MDS managed node: " << register_error << std::endl;
                    }
                }
                if (metadata_registered) {
                    const uint64_t now_ms = static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count());
                    std::string metrics_error;
                    std::error_code space_error;
                    const auto metadata_space = std::filesystem::space(cfg.db_path, space_error);
                    const std::string service_address = "127.0.0.1:" + std::to_string(FLAGS_port);
                    if (space_error || !zb::metrics::ReportMetadataNodeHeartbeat(
                                           channel.get(), managed_node_id, service_address,
                                           metadata_space.capacity, metadata_space.available,
                                           now_ms, &metrics_error)) {
                        std::cerr << "MDS managed heartbeat failed: "
                                  << (space_error ? space_error.message() : metrics_error) << std::endl;
                        metadata_registered = false;
                    } else if (!zb::metrics::ReportCollectedMetrics(channel.get(),
                                                             managed_node_id,
                                                             service.MetricsCollector(),
                                                             zb::rpc::MANAGED_ACCESS_DEFAULT,
                                                             now_ms,
                                                             &metrics_error)) {
                        std::cerr << "MDS metrics report failed: " << metrics_error << std::endl;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(refresh_ms));
            }
        });
    }
    if (archive_manager) {
        archive_thread = std::thread([&]() {
            uint32_t interval_ms = cfg.archive_scan_interval_ms > 0 ? cfg.archive_scan_interval_ms : 5000;
            while (!stop_sync.load()) {
                std::string archive_error;
                archive_manager->RunOnce(&archive_error);
                if (!archive_error.empty()) {
                    std::cerr << "archive round warning: " << archive_error << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
            }
        });
    }
    brpc::Server server;
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        std::cerr << "Failed to add MDS service" << std::endl;
        return 1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_sec;

    if (server.Start(FLAGS_port, &options) != 0) {
        std::cerr << "Failed to start MDS server on port " << FLAGS_port << std::endl;
        stop_sync.store(true);
        if (sync_thread.joinable()) {
            sync_thread.join();
        }
        if (archive_thread.joinable()) {
            archive_thread.join();
        }
        return 1;
    }

    server.RunUntilAskedToQuit();
    stop_sync.store(true);
    if (sync_thread.joinable()) {
        sync_thread.join();
    }
    if (archive_thread.joinable()) {
        archive_thread.join();
    }
    return 0;
}
