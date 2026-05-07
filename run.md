多机部署
# MDS/Scheduler 机器
bash scripts/deploy/start_scheduler.sh
bash scripts/deploy/start_mds.sh

# 存储节点机器
bash scripts/deploy/start_data_nodes.sh

# 客户端机器
bash scripts/deploy/start_client_fuse.sh
bash scripts/deploy/check_cluster.sh


单机部署
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target scheduler_server mds_server real_node_server virtual_node_server zb_fuse_client system_demo_tool -j$(nproc)

bash scripts/start_demo_stack.sh start
bash scripts/start_demo_stack.sh status
bash scripts/start_demo_stack.sh stop

mkdir -p logs
nohup bash scripts/import_masstree_demo.sh 1000 > logs/import_1000yi.log 2>&1 &
tail -f logs/import_1000yi.log

生成 Masstree 模板
export MASSTREE_TEMPLATE_ID=template-100m
export MASSTREE_PATH_LIST_FILE=examples/masstree_path_list_sample.txt
export MASSTREE_PATH_LIST_LEAF_NODES_ARE_FILES=true

mkdir -p logs
nohup bash scripts/generate_masstree_template.sh \
  "$MASSTREE_TEMPLATE_ID" \
  "$MASSTREE_PATH_LIST_FILE" \
  copy \
  true > logs/generate_template.log 2>&1 &

tail -f logs/generate_template.log

用模板导入 1000 亿元数据
export MASSTREE_TEMPLATE_ID=template-100m
export MASSTREE_TEMPLATE_MODE=page_fast
export NAMESPACE_PREFIX=demo-ns

mkdir -p logs
nohup bash scripts/import_masstree_demo.sh 1000 > logs/import_1000yi.log 2>&1 &

tail -f logs/import_1000yi.log

