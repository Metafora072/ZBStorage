# Multi-Host Deployment

This directory contains templates and helper scripts for a three-host layout:

- `MDS + Scheduler` on host A
- `demo/FUSE client` on host B
- `real_node + virtual_node + optical_node` on host C

## 1. Edit cluster variables

Copy and edit:

```bash
cp deploy/multi_host/cluster.env deploy/multi_host/cluster.local.env
```

Fill the real host IPs and local data roots.

## 2. Render configs

```bash
bash scripts/deploy/render_multi_host_configs.sh deploy/multi_host/cluster.local.env
```

Rendered output goes under:

- `deploy/multi_host/rendered/mds`
- `deploy/multi_host/rendered/data`
- `deploy/multi_host/rendered/client`

Copy each rendered role directory to the corresponding host if needed.

## 3. Start services

On the MDS/Scheduler host:

```bash
bash scripts/deploy/start_scheduler.sh
bash scripts/deploy/start_mds.sh
```

On the data-node host:

```bash
bash scripts/deploy/start_data_nodes.sh
```

On the client host:

```bash
bash scripts/deploy/start_client_fuse.sh
bash scripts/deploy/run_demo_client.sh
```

## 4. Health check

On the client host:

```bash
bash scripts/deploy/check_cluster.sh
```

## Notes

- These scripts do not use SSH. Run each role script on the host that owns that role.
- Existing single-host demo scripts remain unchanged.
- `SCHEDULER_HOST` defaults to `MDS_HOST`; set it explicitly if Scheduler is moved to another host.
- The client only needs `MDS_ADDR`, `SCHEDULER_ADDR`, and a local mount point.
