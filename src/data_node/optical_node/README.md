# optical_node module

## Role
`optical_node` is the cold-archive data node. It packs files from hot data nodes into volume
images, burns them to optical discs, and reports archive progress to MDS.

## Core responsibilities
- Manage the volume-image directory (capacity cap + LRU eviction) and the available `volume_id` pool.
- Simulate optical library scheduling (read / pack / burn) through `cd_manager_sim`. Timing uses
  built-in defaults (e.g. `burn_bandwidth_mbps`), and is not configurable from the node config file.
- Persist and read volume-image metadata.
- Serve two brpc services: `RealNodeService` (data plane) and `OpticalNodeService` (archive entry).

## Implementation status
- Archive engine is wired in: constructing `OpticalStorageServiceImpl(config)` immediately runs
  `OpticalNodeManager::Run`, which creates `input/temp/image/read/disc_sim` under `ARCHIVE_ROOT`
  and starts the background workers. A failure makes the node exit at startup (fail fast).
- `OpticalNodeService.SendArchiveMetadata` is currently a **placeholder** that returns an error.
  The real flow (download from the hot node, pack the image, report to MDS) is not implemented yet.
- All 12 `RealNodeService` RPCs are **stubs** returning `optical node storage not implemented`.
  Optical nodes are excluded from normal replica placement, so nothing calls them by default.

## Config
`KEY=VALUE` text, parsed by `OpticalNodeConfig::LoadFromFile`. `#` starts a comment.

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
| `CAPACITY_IN_IMAGES` | `10` | max images kept in `image/` |
| `AVAILABLE_VOLUME_ID_COUNT` | `5` | capacity of the `volume_id` queue |
| `INITIAL_AVAILABLE_VOLUME_IDS` | `1,2,3,4,5` | volume ids seeded at startup (comma-separated) |

Cluster deployment renders `deploy/multi_host/templates/optical_node.conf.tpl`;
`config/optical_node.conf` is a ready-to-run local sample.

## Run
```bash
./build/optical_node_server --config=config/optical_node.conf --port=39080
```

## Design docs
- `optical_node_manager/MDS归档接口9_10.md` - MDS file-archive interface design.
