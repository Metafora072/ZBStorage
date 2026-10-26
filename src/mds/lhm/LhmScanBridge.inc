// Included after LhmNamespaceBridge.inc; uses its checked worker calls.
struct zb_lhm_scan {
    zb_lhm_context* context = nullptr;
    MasstreeLHM::LhmNamespace::subtree_scan_cursor cursor;
    bool files_only = true;
    uint32_t work_budget = 0;
    int failure = ZB_LHM_OK;
};

extern "C" {
int zb_lhm_scan_begin(zb_lhm_context* ctx, const char* path, int files_only,
                      uint32_t work_budget, zb_lhm_scan** out) {
    if (out) *out = nullptr;
    return checked(ctx, [=](threadinfo& ti) -> int {
        std::string normalized;
        if (!out || !work_budget || (files_only != 0 && files_only != 1)
            || !parse_service_path(path, normalized)) return ZB_LHM_INVALID;
        zb_lhm_full_inode attr{};
        const int rc = lookup_checked(ctx, normalized, attr, ti);
        if (rc != ZB_LHM_OK) return rc;
        if (attr.type != 2) return ZB_LHM_INVALID;
        auto scan = std::make_unique<zb_lhm_scan>();
        scan->context = ctx;
        scan->files_only = files_only;
        scan->work_budget = work_budget;
        if (!ctx->ns.begin_subtree_scan(normalized, scan->cursor, ti)) return ZB_LHM_ERROR;
        *out = scan.release();
        return ZB_LHM_OK;
    });
}

int zb_lhm_scan_next(zb_lhm_scan* scan, uint32_t max_entries,
                     zb_lhm_inode_callback callback, void* opaque, int* done) {
    if (done) *done = 0;
    if (!scan || !callback || !done || !max_entries) return ZB_LHM_INVALID;
    if (scan->failure != ZB_LHM_OK) return scan->failure;
    const int rc = checked(scan->context, [=](threadinfo& ti) -> int {
        uint32_t emitted = 0;
        for (uint32_t work = 0; work < scan->work_budget && emitted < max_entries
                && !scan->cursor.stack.empty(); ++work) {
            MasstreeLHM::inode_disk inode{};
            bool found = false;
            if (!scan->context->ns.subtree_scan_step(scan->cursor, inode, found, ti))
                return ZB_LHM_ERROR;
            if (!found || (scan->files_only && inode.type != 1)) continue;
            zb_lhm_full_inode attr{};
            if (!zb::mds::lhm_codec::Decode(inode, attr) || !callback(opaque, &attr))
                return ZB_LHM_ERROR;
            ++emitted;
        }
        *done = scan->cursor.stack.empty();
        return ZB_LHM_OK;
    });
    if (rc != ZB_LHM_OK) {
        scan->failure = rc;
        scan->cursor.stack.clear();
        *done = 0;
    }
    return rc;
}

void zb_lhm_scan_close(zb_lhm_scan* scan) {
    // Cursor owns only plain values/buffers, not native nodes or RCU references.
    delete scan;
}

#ifdef ZB_LHM_TESTING
int zb_lhm_scan_buckets_for_test(zb_lhm_context* ctx, const char* path,
                                uint64_t* slots, uint64_t* buckets) {
    return checked(ctx, [=](threadinfo& ti) -> int {
        if (!path || !slots || !buckets) return ZB_LHM_INVALID;
        MasstreeLHM::DirectoryBlockStore::file_index_debug_info info;
        if (!ctx->ns.debug_file_index_info(path, info, ti)) return ZB_LHM_ERROR;
        *slots = info.directory_slots;
        *buckets = info.unique_buckets;
        return ZB_LHM_OK;
    });
}
int zb_lhm_scan_stats_for_test(zb_lhm_scan* scan, zb_lhm_scan_stats* out) {
    if (!scan || !out) return ZB_LHM_INVALID;
    return checked(scan->context, [=](threadinfo&) -> int {
        *out = {scan->cursor.bucket_reads, scan->cursor.file_entries,
                scan->cursor.directory_entries, scan->cursor.steps};
        return ZB_LHM_OK;
    });
}
#endif
}
