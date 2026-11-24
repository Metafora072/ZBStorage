#pragma once
#include <stdint.h>
#include <stddef.h>

// Plain C ABI: no Masstree, protobuf or STL layouts cross the shared-library boundary.
#ifdef __cplusplus
extern "C" {
#endif
typedef struct zb_lhm_context zb_lhm_context;
typedef struct {
    uint64_t files, directories, inode_blocks, buckets;
} zb_lhm_bulk_stats;
// TSV type/path, explicit root, parents precede children. Exclusive fresh file.
// On failure a newly created output is removed; existing outputs are untouched.
zb_lhm_context* zb_lhm_open_bulk(const char* file, const char* manifest,
    zb_lhm_bulk_stats* stats, char* error, size_t error_size);
typedef struct {
    uint64_t inode_id, size, ctime_ns, mtime_ns, version;
    uint16_t mode;
    uint8_t type; // 1=file, 2=directory (native LHM values)
} zb_lhm_inode;
// Full ZB fields, transported as plain values. Times here are seconds, unlike
// the native/basic bridge nanosecond fields. Filename is length-delimited.
typedef struct {
    uint64_t inode_id, size, atime, mtime, ctime, object_unit_size, version;
    uint64_t disk_node_id, optical_node_id, optical_disk_id, blocks_512;
    uint32_t mode, uid, gid, nlink, replica, disk_id, optical_image_id;
    uint32_t blksize, rdev_major, rdev_minor;
    uint8_t type, archive_state, storage_tier, disk_node_is_virtual, name_length;
    char file_name[64];
} zb_lhm_full_inode;
// Checked namespace calls: status values are independent of protobuf enums.
enum zb_lhm_status { ZB_LHM_OK=0, ZB_LHM_MISSING=1, ZB_LHM_EXISTS=2,
    ZB_LHM_INVALID=3, ZB_LHM_NOT_EMPTY=4, ZB_LHM_ERROR=5, ZB_LHM_BUSY=6, ZB_LHM_READONLY=7 };
typedef int (*zb_lhm_entry_callback)(void*, const char*, uint32_t, uint64_t, uint8_t);
int zb_lhm_lookup_checked(zb_lhm_context*, const char*, zb_lhm_full_inode*);
int zb_lhm_read_checked(zb_lhm_context*, uint64_t, zb_lhm_full_inode*);
int zb_lhm_reserve_checked(zb_lhm_context*, const char*, uint8_t, uint64_t*, uint64_t*);
int zb_lhm_commit_checked(zb_lhm_context*, uint64_t, const zb_lhm_full_inode*);
int zb_lhm_rename_checked(zb_lhm_context*, const char*, const char*);
int zb_lhm_remove_checked(zb_lhm_context*, const char*, uint8_t);
// Callback runs synchronously on the bridge worker; it must not reenter the bridge.
int zb_lhm_readdir_checked(zb_lhm_context*, const char*, const char*, uint32_t,
                           zb_lhm_entry_callback, void*, int*);
// Opaque, process-local scan. Context must outlive it; calls must be sequential.
// Structural changes to the selected subtree are excluded by the contract.
// Callbacks execute synchronously on the worker and must not reenter the bridge.
// Callback results are provisional until Next succeeds; discard them on error.
typedef struct zb_lhm_scan zb_lhm_scan;
typedef int (*zb_lhm_inode_callback)(void*, const zb_lhm_full_inode*);
int zb_lhm_scan_begin(zb_lhm_context*, const char* root, int files_only,
                      uint32_t work_budget, zb_lhm_scan**);
int zb_lhm_scan_next(zb_lhm_scan*, uint32_t max_entries,
                     zb_lhm_inode_callback, void*, int* done);
void zb_lhm_scan_close(zb_lhm_scan*);
// type uses the native bridge convention: 1=file, 2=directory.
int zb_lhm_reserve(zb_lhm_context*, const char* path, uint8_t type,
                   uint64_t* token, uint64_t* inode_id);
// Success consumes the token. Failure retains it for retry/cancellation.
int zb_lhm_commit(zb_lhm_context*, uint64_t token, const zb_lhm_full_inode*);
int zb_lhm_cancel(zb_lhm_context*, uint64_t token);
uint64_t zb_lhm_pending(zb_lhm_context*);
#ifdef ZB_LHM_TESTING
typedef struct {
    uint64_t bucket_reads, file_entries, directory_entries, steps;
} zb_lhm_scan_stats;
int zb_lhm_scan_stats_for_test(zb_lhm_scan*, zb_lhm_scan_stats*);
int zb_lhm_scan_buckets_for_test(zb_lhm_context*, const char*, uint64_t* slots, uint64_t* buckets);
int zb_lhm_fail_next_publish_for_test(zb_lhm_context*);
int zb_lhm_fail_next_lookup_read_for_test(zb_lhm_context*);
#endif
int zb_lhm_read_full(zb_lhm_context*, uint64_t id, zb_lhm_full_inode*);
int zb_lhm_lookup_full(zb_lhm_context*, const char* path, zb_lhm_full_inode*);
int zb_lhm_update_full(zb_lhm_context*, const zb_lhm_full_inode*);

// Returns 1 on success, 0 for invalid/missing/error. Detailed statuses are step 3/4.
// Fresh file only; parent must exist. Use zb_lhm_reopen for normal reopen.
zb_lhm_context* zb_lhm_open(const char* fresh_backing_file);
// Caller must quiesce all calls on ctx before close; the pointer then becomes invalid.
void zb_lhm_close(zb_lhm_context* ctx);
// Automatically recover a service journal, retain the checkpoint and protect writes.
// Pending offline import journals still require the offline recovery entry point.
// Clean v6 shared namespaces support relocated files for read-only queries and
// create no service undo. Writable opens, offline append and pending recovery
// still require the original physical file identities.
zb_lhm_context* zb_lhm_reopen(const char* file, char* error, size_t error_size);
// Consumes ctx even on failure; retains the checkpoint and rolls back or leaves
// its journal for recovery. A failed close is never reported as a new commit.
int zb_lhm_close_checked(zb_lhm_context*, char* error, size_t error_size);
// Serialize with service operations. BUSY means an outstanding create reservation;
// retry after it finishes. ERROR disables the session until close/reopen recovery.
// Mutations after the last successful checkpoint can be lost after a crash.
int zb_lhm_checkpoint(zb_lhm_context*, char* error, size_t error_size);
int zb_lhm_create(zb_lhm_context* ctx, const char* path, int directory);
int zb_lhm_lookup(zb_lhm_context* ctx, const char* path, zb_lhm_inode* out);
int zb_lhm_read_inode(zb_lhm_context* ctx, uint64_t id, zb_lhm_inode* out);
int zb_lhm_update_inode(zb_lhm_context* ctx, const zb_lhm_inode* attributes);
int zb_lhm_rename(zb_lhm_context* ctx, const char* from, const char* to);
// Functional harness removal; only files or empty directories. Formal API in step 4.
int zb_lhm_remove(zb_lhm_context* ctx, const char* path);
int zb_lhm_sync(zb_lhm_context* ctx);
#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
extern "C" {
#endif
// Synchronous streaming append. Target must be a new child of an existing
// directory. Manifest begins with dir /; remaining paths are rooted there.
// Budget bounds parser records (not final indexes or backend block caches).
// Failure may leave the successfully imported prefix. No retry/rollback promise.
typedef struct {
    uint64_t root_inode_id, files, directories, processed_records, error_line;
} zb_lhm_import_result;
int zb_lhm_import_manifest(zb_lhm_context*, const char* target, const char* manifest,
    uint64_t memory_budget_bytes, zb_lhm_import_result*, char* error, size_t error_size);

// Exclusive OFFLINE batch import into an existing, normally closed store.
// Target is a new top-level directory. The manifest's actual bytes are checked
// against its lowercase SHA256 before commit. Nonzero expected counts must
// match. Pre-publication failure rolls back the entire batch; interrupted
// rollback is resumed by recover/import. An error after checkpoint publication
// may already be committed: retry the same ID to resolve that outcome. Only the
// most recently committed batch ID is idempotent.
// All three functions return zb_lhm_status (0 == success).
int zb_lhm_import_batch(const char* file, const char* target, const char* manifest,
    const char* batch_id, const char* manifest_sha256, uint64_t memory_budget_bytes,
    uint64_t expected_files, uint64_t expected_directories, zb_lhm_import_result*,
    int* already_committed, char* error, size_t error_size);
int zb_lhm_recover_batch(const char* file, char* error, size_t error_size);
// Read-only status. Returns the last commit receipt as bounded key=value lines,
// or an empty string for a clean baseline. Pending recovery is an error.
int zb_lhm_batch_status(const char* file, char* receipt, size_t receipt_size,
    char* error, size_t error_size);
// Compile a grouped, parent-before-child TSV once into relocatable LHM pages.
// Output directory must not exist. The format is tied to the LHM page layout.
int zb_lhm_compile_template(const char* manifest, const char* output_directory,
    uint64_t memory_budget_bytes, char* error, size_t error_size);
// Header-only inspection; result contains counts, with root_inode_id=0.
int zb_lhm_compiled_info(const char* template_directory, zb_lhm_import_result*,
    char* error, size_t error_size);
// Same offline transaction contract as import_batch. Identity is the SHA256 of
// header.bin, which binds source SHA256, layout and every compiled file digest.
// The receipt's manifest_sha256 field contains this compiled header identity.
int zb_lhm_import_compiled_batch(const char* file, const char* target,
    const char* template_directory, const char* batch_id, const char* header_sha256,
    uint64_t memory_budget_bytes, zb_lhm_import_result*, int* already_committed,
    char* error, size_t error_size);
// Nanosecond wall-clock stages. Storage counters describe only bulk page I/O;
// pwrite/sync/writeback details are subsets of submit_ns and data_sync_ns.
typedef struct {
    uint64_t checkpoint_validation_ns, template_validation_ns, reopen_ns, reserve_ns;
    uint64_t read_hash_ns, relocate_ns, submit_ns, data_sync_ns;
    uint64_t directory_install_ns, snapshot_ns, commit_ns, total_ns;
    uint64_t write_calls, write_bytes, pwrite_ns;
    uint64_t main_sync_ns, heads_sync_ns, buckets_sync_ns;
    uint64_t writeback_calls, writeback_wait_ns;
} zb_lhm_compiled_profile;
int zb_lhm_import_compiled_batch_profiled(const char* file, const char* target,
    const char* template_directory, const char* batch_id, const char* header_sha256,
    uint64_t memory_budget_bytes, zb_lhm_import_result*, int* already_committed,
    zb_lhm_compiled_profile*, char* error, size_t error_size);
// Opt-in v6, read-only shared-template database. First import requires an empty
// store; subsequent imports reuse the same immutable route file per header SHA.
int zb_lhm_import_shared_compiled_batch_profiled(const char*,const char*,const char*,
    const char*,const char*,uint64_t,zb_lhm_import_result*,int*,zb_lhm_compiled_profile*,char*,size_t);
// Counts loaded template routes, not instance-expanded directories.
int zb_lhm_shared_index_stats(zb_lhm_context*,uint64_t* templates,uint64_t* instances,uint64_t* routes);
// Offline v5 compiled-batch conversion. Mapping: target TAB template-directory TAB
// header-SHA256 per line, covering every committed batch. Preserves data pages,
// inode IDs and .pre-shared checkpoint/receipt backups. Idempotent after commit.
int zb_lhm_convert_shared_index(const char*,const char*,uint64_t,char*,size_t);
#ifdef __cplusplus
}
#endif
