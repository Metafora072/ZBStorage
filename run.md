多机部署
# MDS/Scheduler 机器
bash scripts/deploy/start_scheduler.sh
bash scripts/deploy/start_mds.sh

# 存储节点机器
bash scripts/deploy/start_data_nodes.sh

# 客户端机器
bash scripts/deploy/start_client_fuse.sh
bash scripts/deploy/check_cluster.sh

