## Build

Standalone build from this directory:

```bash
cmake -S src/sim -B build-sim
cmake --build build-sim
```

The simulator does not depend on brpc, protobuf, RocksDB, gflags, MDS, Scheduler, or data-node services.
It only needs a C++17 compiler and the C++ standard library. On GCC 8, CMake links `stdc++fs` automatically.
On Linux, `SIM_MASSTREE_REAL_INDEX` defaults to `ON` and compiles the repository's Masstree wrapper into
the standalone sim binary for the metadata sparse index. It still does not link brpc, protobuf, RocksDB, or gflags.
Disable it with `-DSIM_MASSTREE_REAL_INDEX=OFF` to use the standard-library fallback index.

## Default resource model

The default online disk resource model follows `deploy/multi_host/cluster.local.env`:

```text
REAL_NODE_COUNT=1
VIRTUAL_NODE_COUNT=99
DISKS_PER_LOGICAL_NODE=24
ONLINE_DISK_CAPACITY_BYTES=2000000000000
OPTICAL_NODE_COUNT=10000
OPTICAL_DISCS_PER_NODE=10000
OPTICAL_1TB_DISCS_PER_NODE=5000
OPTICAL_10TB_DISCS_PER_NODE=5000
```

The simulator represents those as `--disk_nodes=100` logical disk nodes, with each logical node capacity set to:

```text
--disk_capacity_bytes=48000000000000
```

When optical import simulation is used, the simulator models 10000 optical nodes by default. Each optical node has
10000 discs: 5000 discs are 1 TB and 5000 discs are 10 TB. The per-node optical capacity is:

```text
--optical_nodes=10000
--optical_capacity_bytes=55000000000000000
```

You can override this with `--disk_nodes`, `--disk_capacity_bytes`, `--disk_capacity_gb`,
`--optical_nodes`, `--optical_capacity_bytes`, or `--optical_capacity_gb`.

## Run

Start the menu UI:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim
```

Menu commands:

```text
1  cluster statistics
2  file write and read-back verification
3  batch import statistics simulation
4  metadata query latency simulation
5  file metadata template generation
6  batch file metadata import
7  file metadata query
q  quit
```

Each menu command can include `key=value` overrides:

```text
2 path=/demo/a.bin size_mb=10
3 import_files=1000000 default_file_size_mb=100
4 query_samples=10 query_sleep=false
5 template_id=t1 path_list_file=paths.txt target_file_count=1000000
6 template_id=t1 namespace_id=ns1 generation_id=gen-001 path_prefix=/data/ns1
7 namespace_id=ns1 masstree_query_mode=random_path_lookup query_samples=10
```

Cluster statistics print total counters first, followed by node lines. Capacity fields use large binary units
up to YiB where needed, and per-node lines do not print `file_count`.

Run one scenario directly:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=cluster
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=io --path=/demo/a.bin --size_mb=100
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=import --import_files=1000000 --default_file_size_mb=100
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=query --query_samples=10
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=all
```

## Standalone Masstree metadata

Generate a template from a path list:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_template --template_id=t1 --path_list_file=paths.txt --target_file_count=1000000 --default_file_size_mb=100
```

Import the template into a namespace generation:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_import --template_id=t1 --namespace_id=demo-ns --generation_id=gen-001 --path_prefix=/masstree_demo/demo-ns
```

Query imported metadata:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_query --namespace_id=demo-ns --query_samples=10 --query_sleep=false
```

Supported metadata query modes:

```text
random_path_lookup    Pick a sample path from the imported namespace and resolve it.
resolve_path          Resolve --path to inode metadata.
get_inode             Read metadata for --inode_id.
build_full_path       Build a full path for --inode_id.
get_optical_location  Return optical node/disk/image/offset for --path or --inode_id.
readdir               List children under --path, controlled by --readdir_limit.
```

Examples:

```bash
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_query --namespace_id=demo-ns --masstree_query_mode=resolve_path --path=/masstree_demo/demo-ns/a.bin
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_query --namespace_id=demo-ns --masstree_query_mode=get_inode --inode_id=2
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=masstree_query --namespace_id=demo-ns --masstree_query_mode=readdir --path=/masstree_demo/demo-ns --readdir_limit=20
```

The standalone Masstree metadata files are stored under:

```text
/tmp/zb_sim/.sim/masstree
```

cmake -S src/sim -B build-sim
cmake --build build-sim
./build-sim/zb_sim_tool --root=/tmp/zb_sim --scenario=cluster
