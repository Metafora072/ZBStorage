#pragma once
#include <stdint.h>

// Plain C ABI: no Masstree, protobuf or STL layouts cross the shared-library boundary.
#ifdef __cplusplus
extern "C" {
#endif
typedef struct zb_lhm_context zb_lhm_context;
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
    ZB_LHM_INVALID=3, ZB_LHM_NOT_EMPTY=4, ZB_LHM_ERROR=5 };
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
// Fresh file only; the parent directory must already exist. No reopen/recovery.
zb_lhm_context* zb_lhm_open(const char* fresh_backing_file);
// Caller must quiesce all calls on ctx before close; the pointer then becomes invalid.
void zb_lhm_close(zb_lhm_context* ctx);
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
