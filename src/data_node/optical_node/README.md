# optical_node module

## Role
`optical_node` is the cold-archive data node. It downloads files from hot data nodes, packs them into
volume images, packs those images onto optical discs, burns the discs, and serves data-plane reads
(including re-loading a volume image from its burned disc when it is no longer cached).

## Core responsibilities
- Manage the volume-image directory (`image/`): capacity cap, READ/WRITE categories, LRU eviction of
  READ images, and the available `volume_id` pool.
- Keep the packing state: accumulate compressed files into a pending volume group until
  `SIZE_THRESHOLD` is reached (or the next file would overflow), then pack a volume image.
- Pack volume images onto discs: accumulate images while they fit in `DISC_CAPACITY_BYTES`, seal the
  set as one disc (commit a `DISC_BURN` task), and release the cached write images one by one as the
  burn completes. The disc file itself (`disc_sim/disc_<disk_id>.vdisc`) is materialized **on burn
  completion**, not at seal time (the images exist only in `image/` until then).
- Persist and rebuild disc metadata: `meta/pending_disc_meta` (in-progress disc) is renamed to
  `meta/disc_<disk_id>_meta` at seal and kept only until the disc is materialized **and** indexed —
  it is then deleted, so its presence means "sealed but not finished yet". `meta/node_discs_meta` is
  the durable append-only index of `volume_id -> disc_id / offset / size / SHA-256`, scanned at
  startup to rebuild `disc_image_index_`.
- Persist and restore runtime state: `Shutdown()` writes a snapshot of the whole in-memory state to
  `log/runtime_state_snapshot`, and the next `Run()` reads it back (combining it with the real
  directory contents) so unfinished work keeps progressing across a restart.
- Rebuild the volume-image management table from **snapshot + directory**: the directory decides
  existence, the snapshot supplies the READ/WRITE category and the LRU order (neither can be derived
  from the flat `volume_<id>.vimg` naming).
- Throttle archive downloads (back pressure): pause before the next file when the write-image count
  reaches `MAX_WRITE_IMAGES`, when the uncompressed files in `input/` exceed 10% of a volume image,
  or when one download round has already fetched one volume image worth of bytes (the round quota
  accumulates across MDS batches). Blocked rounds resume on compression progress or on write-image
  release after a burn.
- Simulate optical library scheduling (read / pack / burn) through `cd_manager_sim`. Timing uses
  built-in defaults (e.g. `burn_bandwidth_mbps`), and is not configurable from the node config file.
- Serve the brpc service `OpticalNodeService`: the archive entry (`SendArchiveMetadata`) plus the
  data-plane read chain (`SubscribeReadEvents` → `PrepareRead` → `ReadReady` push → `AcquireReadSession`
  → `ReadPreparedFile` → `ReleaseReadSession` / `ReleasePreparedRead`).

## Implementation status
- Archive engine is wired in: constructing `OpticalStorageServiceImpl(config)` immediately runs
  `OpticalNodeManager::Run`, which creates
  `input/ temp/ image/ read/ disc_sim/ meta/ log/` under `ARCHIVE_ROOT` and starts the
  background workers (`archive` download / `zip` pack / `cd_read` / `cd_burn` / `cleanup`). A failure
  makes the node exit at startup (fail fast).
- `OpticalNodeService.SendArchiveMetadata` receives batches and, per file, asynchronously downloads
  it from the hot node into a WRITE task that feeds the pack pipeline. The download thread rebuilds
  the `node_id -> address` map per batch, reassembles `object_unit_size` shards on disk by absolute
  offset, and applies the back-pressure check before each file.
- Disc packing / burning / read-back are implemented: seal on capacity overflow, materialize the
  vdisc (`superblock + metadata area + data area`, all aligned to `DISC_BLOCK_SIZE_BYTES`), release
  write images on burn completion, and copy a volume image range back from the vdisc into `image/`
  (with LRU eviction) on a read miss.
- The two MDS reports (`ReportFilesPackedToImage` / `ReportImagesBurnedToDisc`) are not implemented
  yet; both are printed to the console with the MDS request field names as placeholders.
- `volume_id` (MDS `image_id`) and `disk_id` (MDS `disc_id`) are generated locally as increasing
  counters, because MDS `AllocateAvailableImageId` / `AllocateAvailableDiscId` are not implemented.
- Runtime state **is** persistent: `OpticalNodeManager::Shutdown()` (called by `optical_node_server`
  after `RunUntilAskedToQuit()`, i.e. on SIGINT/SIGTERM) stops the workers and atomically writes
  `log/runtime_state_snapshot` (task table, the three task queues, ID counters and available-ID pools,
  archive queue + download breakpoint, volume-read index, pending-file set, image management table).
  The next `Run()` reads it back — combining the image table with the real `image/` contents — then
  reconciles leftovers (re-submit a sealed-but-unburned disc, index a materialized-but-unindexed disc,
  drop half-written `vdisc.tmp`) and renames the file to `runtime_state_snapshot.loaded` so it is
  replayed only once. Not persisted (must be redone after a restart): in-progress `WriteObject` shard
  state, `cd_manager_sim` active tasks (their burns restart from zero), and the download round quota.
- Scheduler heartbeat reports readiness from `IsArchiveEngineReady()`. The archive engine does not export a device inventory or task telemetry yet, so the heartbeat keeps the node in `JOINING` with
  `readiness_message = "archive engine running; awaiting optical inventory and task telemetry"`
  instead of publishing fabricated empty inventory / idle measurements.
- At startup, before the heartbeat loop begins, the node runs a **one-shot** `RegisterManagedNode`
  (`kind = MANAGED_KIND_OPTICAL_LIBRARY`, `MANAGED_EXECUTION_PHYSICAL`, `participates_in_placement =
  false`, `activate_immediately = false`) so the scheduler keeps it in `JOINING` until real inventory
  arrives. Registration is retried a bounded number of times (transient transport failures only; a
  definitive server rejection stops retries), then is never sent again — the loop that follows is
  pure heartbeat. If registration is not confirmed, the heartbeat's implicit auto-registration
  applies instead (degrading the profile to the scheduler default optical template).
  The spec's device groups (hdd-cache / disc / optical-drive), four-level power, reliability and
  optical latencies are built from the node config (see the config table below), not from the
  scheduler defaults; `SCHED_ALREADY_EXISTS` (e.g. after a node restart) is treated as success.
- The data-plane read chain is wired end to end. `SubscribeReadEvents` accepts a client brpc Stream and
  registers it in `ReadEventSessionRegistry` (`session_id → server Stream`); `PrepareRead` registers a
  READ task with its `request_id` / `session_id` **before** enqueueing, so completion can never race the
  registration; when the prepared artifact is READY the manager invokes `PreparedFileReadyCallback`,
  which serializes a `ReadReadyEvent` and pushes it onto that session's Stream. `AcquireReadSession`
  opens one independent read session per open (`read_handle → task_id`) and takes a read reference,
  `ReadPreparedFile` returns file ranges with `bytes_read` / `eof` (no implicit cursor; EOF does not
  release anything), and `ReleaseReadSession` / `ReleasePreparedRead` drop the read / prepare
  references — the extracted artifact is unlinked only when both reach zero. Notification sessions and
  read sessions are **process-local and not persisted**: after a restart the client re-subscribes, and a
  recovered task (whose `session_id` was not persisted) is never sent `ReadReady`.
- The optical node does not register `RealNodeService`; data-plane reads are served by
  `OpticalNodeService` via the 6 read RPCs (`SubscribeReadEvents` / `PrepareRead` /
  `AcquireReadSession` / `ReadPreparedFile` / `ReleaseReadSession` / `ReleasePreparedRead`) plus the
  server-pushed `ReadReady` event. Optical nodes are excluded from normal replica placement by default.

## Config
`KEY=VALUE` text, parsed by `OpticalNodeConfig::LoadFromFile`. `#` starts a comment.
Values are also clamped to safe defaults when zero/invalid (see `OpticalNodeConfig.cpp`).

| key | default | meaning |
| --- | --- | --- |
| `NODE_ID` | `optical-node-<port>` | node id |
| `NODE_ADDRESS` | `127.0.0.1:<port>` | advertised address |
| `GROUP_ID` | `NODE_ID` | replication group |
| `NODE_ROLE` | `PRIMARY` | `PRIMARY` / `SECONDARY` |
| `SCHEDULER_ADDR` | empty | heartbeat is started only when non-empty |
| `PEER_NODE_ID` / `PEER_ADDRESS` | empty | peer replica node |
| `REPLICATION_ENABLED` | `false` | replication toggle |
| `REPLICATION_TIMEOUT_MS` | `2000` | replication timeout |
| `NODE_WEIGHT` | `1` | heartbeat weight |
| `VIRTUAL_NODE_COUNT` | `1` | heartbeat virtual node count |
| `HEARTBEAT_INTERVAL_MS` | `2000` | heartbeat interval |
| `ARCHIVE_ROOT` | `/tmp/zb_optical` | archive working root |
| `VOLUME_SIZE_BYTES` | 10 GiB | max size of one volume image |
| `SIZE_THRESHOLD` | `0.9` | packing trigger ratio (0.0-1.0) |
| `CAPACITY_IN_IMAGES` | `1000` | max images kept in `image/` (READ + WRITE share the cap) |
| `AVAILABLE_ID_COUNT` | `5` | capacity of the `volume_id` / `disk_id` pools (shared) |
| `DISC_CAPACITY_BYTES` | `1099511627776` | max size of one optical disc (bytes, 1 TiB) |
| `STANDARD_IMAGES_PER_DISC` | `100` | baseline number of images per disc (actual disc may hold more, bounded by capacity) |
| `TOTAL_DISC_COUNT` | `1000` | total disc slots in the library; startup-only, reported as optical capacity |
| `DISC_BLOCK_SIZE_BYTES` | `2048` | disc block size; superblock, metadata area and every image are aligned to it |
| `MAX_WRITE_IMAGES` | `128` | max concurrent WRITE images; download back-pressure threshold |

Library hardware / simulation parameters passed through to `cd_manager` (formerly hard-coded in
`cd_manager.cpp`) and to the `RegisterManagedNode` device groups:

| key | default | meaning |
| --- | --- | --- |
| `DRIVE_COUNT` | `20` | number of optical drives |
| `ARM_COUNT` | `4` | number of mechanical arms |
| `BURN_IMAGE_SIZE_BYTES` | `10737418240` | fallback burn size when the real image size is unknown |
| `BURN_BANDWIDTH_MBPS` | `20.0` | burn bandwidth (MB/s) |
| `BURN_FIXED_SECONDS` | `1.0` | fixed burn latency (s) |
| `READ_BANDWIDTH_MBPS` | `100.0` | disc read bandwidth (MB/s) |
| `READ_FIXED_SECONDS` | `0.5` | fixed disc read latency (s) |
| `DRIVE_LOAD_SECONDS` / `DRIVE_EJECT_SECONDS` | `12.0` / `3.0` | drive load / eject time (s) |
| `GRASP_SECONDS` | `2.0` | arm grasp time (s) |
| `ARM_SPEED_X` / `ARM_SPEED_Y` / `ARM_SPEED_Z` | `2.0` / `1.0` / `1.0` | arm speed per axis (m/s) |
| `RACK_ROWS` / `RACK_COLS` / `RACK_DEPTH` | `50` / `200` / `10` | rack geometry (random slot placement) |
| `SLOT_WIDTH` / `SLOT_HEIGHT` / `SLOT_DEPTH` | `0.015` / `0.15` / `0.3` | single slot size (m) |
| `EXTERNAL_NETWORK_BANDWIDTH_BYTES_PER_SEC` | `10000000000` | node external network cap |
| `FIXED_ACCESS_LATENCY_US` | `500` | fixed (cache-hit) access latency |
| `OPTICAL_LOAD_LATENCY_US` / `OPTICAL_SEEK_LATENCY_US` | `60000000` / `500000` | disc load / seek latency |
| `CACHE_READ_BYTES_PER_SEC` / `CACHE_WRITE_BYTES_PER_SEC` | `200000000` | hdd-cache group read/write bandwidth |
| `DRIVE_READ_BYTES_PER_SEC` / `DRIVE_WRITE_BYTES_PER_SEC` | `100000000` / `10000000` | optical-drive group read/write bandwidth |
| `POWER_PEAK_MILLIWATTS` / `POWER_MEDIUM_MILLIWATTS` / `POWER_STANDBY_MILLIWATTS` / `POWER_OFF_MILLIWATTS` | `400000` / `200000` / `50000` / `0` | four power levels (mW); must be non-increasing |
| `RELIABILITY_MEAN_LIFETIME_MS` / `RELIABILITY_LIFETIME_STDDEV_MS` / `RELIABILITY_MINIMUM_LIFETIME_MS` | 10 / 2 / 1 years | node lifetime model (ms) |
| `TECHNOLOGY_GENERATION` / `INSTANCE_GENERATION` | `1` / `1` | hardware technology generation / service instance generation |

The `hdd-cache` device group capacity is derived from `CAPACITY_IN_IMAGES × VOLUME_SIZE_BYTES`
(the `image/` total size); the `disc` group uses `DISC_CAPACITY_BYTES × TOTAL_DISC_COUNT`; the
`optical-drive` group uses `DRIVE_COUNT`.

Constraint across `STANDARD_IMAGES_PER_DISC`, `MAX_WRITE_IMAGES` and `CAPACITY_IN_IMAGES`:
**images per disc < `MAX_WRITE_IMAGES` < `CAPACITY_IN_IMAGES`**.
A too small `MAX_WRITE_IMAGES` means the disc never fills (no seal, no release, downloads stuck);
a too large one means back pressure reacts too late and `MoveFrom(WRITE)` hits
`VOLUME_FULL_NO_READABLE`.

Cluster deployment renders `deploy/multi_host/templates/optical_node.conf.tpl`; the demo stack
(`scripts/start_demo_stack.sh`, `START_OPTICAL=true`) renders a ready-to-run `optical_node.conf` into
`${RUN_DIR}/config/`.

## Run
```bash
# via the demo stack (renders config + starts the node on OPTICAL_PORT, default 39080)
START_OPTICAL=true scripts/start_demo_stack.sh

# or directly with a config file
./build/optical_node_server --config=<path>/optical_node.conf --port=39080
```

## Tests
`tests/optical_node/` runs the whole lifecycle in-process against fake real_node / scheduler / MDS
clients as one ctest case (`optical_node`): archive batch shapes → download back pressure → pack →
seal → burn → data-plane read over the 6 read RPCs (subscribe / prepare / ReadReady push / acquire /
range read / two-level release) with CD read-back and cache hits → read-session & reference lifecycle
→ LRU eviction → graceful shutdown and restart recovery. The corpus is synthesized in-process (no
external jpg). See `tests/optical_node/README.md`.