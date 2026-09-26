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
  set as one disc (write `disc_sim/disc_<disk_id>.vdisc` + commit a `CD_BURN` task), and release the
  cached write images one by one as the burn completes.
- Persist and rebuild disc metadata: `meta/pending_disc_meta` (in-progress disc),
  `meta/disc_<disk_id>_meta` (sealed disc), `meta/node_discs_meta` (append-only index of
  `volume_id -> disc_id / offset / size / SHA-256`, scanned at startup to rebuild `disc_image_index_`).
- Throttle archive downloads (back pressure): pause before the next file when the write-image count
  reaches `MAX_WRITE_IMAGES`, when the uncompressed files in `input/` exceed 10% of a volume image,
  or when one download round has already fetched one volume image worth of bytes (the round quota
  accumulates across MDS batches). Blocked rounds resume on compression progress or on write-image
  release after a burn.
- Simulate optical library scheduling (read / pack / burn) through `cd_manager_sim`. Timing uses
  built-in defaults (e.g. `burn_bandwidth_mbps`), and is not configurable from the node config file.
- Serve the brpc service `OpticalNodeService` (archive entry + data-plane read).

## Implementation status
- Archive engine is wired in: constructing `OpticalStorageServiceImpl(config)` immediately runs
  `OpticalNodeManager::Run`, which creates
  `input/ temp/ image/ read/ disc_sim/ meta/ log/ write_buffer/` under `ARCHIVE_ROOT` and starts the
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
- Runtime state is not persistent: the task table and the queues live in memory only. A restart
  rebuilds just the `volume_id -> disc` index from `meta/node_discs_meta`.
- Scheduler heartbeat reports readiness from `IsArchiveEngineReady()`. The archive engine does not
  export a device inventory or task telemetry yet, so the heartbeat keeps the node in `JOINING` with
  `readiness_message = "archive engine running; awaiting optical inventory and task telemetry"`
  instead of publishing fabricated empty inventory / idle measurements.
- The optical node does not register `RealNodeService`; data-plane reads are served by
  `OpticalNodeService` (`RequestAsyncReadFile` / `ReadObjectByTaskId` / `ReadObjectByInodeId`).
  Optical nodes are excluded from normal replica placement by default.

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
| `AVAILABLE_VOLUME_ID_COUNT` | `5` | capacity of the `volume_id` queue |
| `DISC_CAPACITY_BYTES` | `1099511627776` | max size of one optical disc (bytes, 1 TiB) |
| `STANDARD_IMAGES_PER_DISC` | `100` | baseline number of images per disc (actual disc may hold more, bounded by capacity) |
| `DISC_BLOCK_SIZE_BYTES` | `2048` | disc block size; superblock, metadata area and every image are aligned to it |
| `MAX_WRITE_IMAGES` | `128` | max concurrent WRITE images; download back-pressure threshold |

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
`tests/optical_node/` runs the whole pipeline in-process against fake real_node / scheduler / MDS
clients (smoke / full / evict scenarios). See `tests/optical_node/README.md`.

## Design docs
- `optical_node_manager/MDS归档接口9_12.md` - MDS file-archive interface design.