# 多机部署运行说明

本文档说明如何将当前系统拆分为多台机器运行，并通过 RPC 互通。

当前推荐的三机部署形态：

- `MDS + Scheduler` 部署在机器 A
- `demo/FUSE 客户端` 部署在机器 B
- `real_node + virtual_node + optical_node` 部署在机器 C

## 1. 总体架构

模块通信关系如下：

- `system_demo_tool` 和 `zb_fuse_client` 只需要连接 `MDS` 和 `Scheduler`
- `MDS` 通过 `Scheduler` 获取集群视图
- `real_node`、`virtual_node`、`optical_node` 向 `Scheduler` 上报心跳
- `real_node`、`virtual_node` 会向 `MDS` 上报归档候选

默认端口：

- `MDS`: `9000`
- `Scheduler`: `9100`
- `real_node`: `19080`
- `virtual_node`: `29080`
- `optical_node`: `39080`

## 2. 目录与脚本

多机部署相关文件：

- 集群变量示例: [deploy/multi_host/cluster.env](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/deploy/multi_host/cluster.env)
- 配置模板目录: [deploy/multi_host/templates](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/deploy/multi_host/templates)
- 渲染脚本: [scripts/deploy/render_multi_host_configs.sh](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/deploy/render_multi_host_configs.sh)
- 启动脚本目录: [scripts/deploy](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/deploy)

现有单机脚本仍然保留，不受影响：

- [scripts/start_demo_stack.sh](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/start_demo_stack.sh)
- [scripts/run_system_demo.sh](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/run_system_demo.sh)

## 3. 先决条件

- 三台机器网络互通
- 端口 `9000/9100/19080/29080/39080` 已放通
- 各机器都已经部署相同版本的二进制
- 各机器本地目录已经准备好
- 各配置中的地址必须写真实可达 IP，不要写 `127.0.0.1`

建议的本地目录规划：

- 机器 A `MDS`: `/data/zb/mds`
- 机器 A `Scheduler`: `/data/zb/scheduler`
- 机器 B `client`: `/data/zb/client`
- 机器 C `data_nodes`: `/data/zb/data_nodes`

## 4. 配置集群变量

先复制一份集群变量文件：

```bash
cp deploy/multi_host/cluster.env deploy/multi_host/cluster.local.env
```

然后修改 `deploy/multi_host/cluster.local.env`，至少填这些值：

```conf
MDS_HOST=10.0.0.11
SCHEDULER_HOST=10.0.0.11
CLIENT_HOST=10.0.0.12
DATA_HOST=10.0.0.13

MDS_PORT=9000
SCHEDULER_PORT=9100
REAL_PORT=19080
VIRTUAL_PORT=29080
OPTICAL_PORT=39080

MDS_ROOT=/data/zb/mds
SCHEDULER_ROOT=/data/zb/scheduler
DATA_ROOT=/data/zb/data_nodes
CLIENT_ROOT=/data/zb/client
```

如果当前不启用光盘节点，可以保留：

```conf
ENABLE_OPTICAL_NODE=false
```

## 5. 渲染配置文件

执行：

```bash
bash scripts/deploy/render_multi_host_configs.sh deploy/multi_host/cluster.local.env
```

渲染后输出目录：

- `deploy/multi_host/rendered/mds`
- `deploy/multi_host/rendered/data`
- `deploy/multi_host/rendered/client`

会生成这些配置：

- `rendered/mds/mds.conf`
- `rendered/mds/scheduler.conf`
- `rendered/data/real_node.conf`
- `rendered/data/virtual_node.conf`
- `rendered/data/optical_node.conf`，仅在 `ENABLE_OPTICAL_NODE=true` 时生成
- `rendered/client/client.env`

## 6. 分发配置到各机器

将渲染后的角色目录复制到对应机器：

- 机器 A: `deploy/multi_host/rendered/mds`
- 机器 B: `deploy/multi_host/rendered/client`
- 机器 C: `deploy/multi_host/rendered/data`

如果你不想复制整个仓库，可以至少同步：

- 对应配置文件
- 对应启动脚本
- 已编译好的二进制

## 7. 启动顺序

建议按下面顺序启动。

### 7.1 机器 A：启动 Scheduler

```bash
bash scripts/deploy/start_scheduler.sh
```

默认读取：

- `deploy/multi_host/rendered/mds/scheduler.conf`
- `deploy/multi_host/rendered/mds/mds.env`

### 7.2 机器 A：启动 MDS

```bash
bash scripts/deploy/start_mds.sh
```

默认读取：

- `deploy/multi_host/rendered/mds/mds.conf`
- `deploy/multi_host/rendered/mds/mds.env`

### 7.3 机器 C：启动数据节点

```bash
bash scripts/deploy/start_data_nodes.sh
```

这会启动：

- `real_node_server`
- `virtual_node_server`
- `optical_node_server`，仅在 `ENABLE_OPTICAL_NODE=true` 时启动

### 7.4 机器 B：启动 FUSE 客户端

```bash
bash scripts/deploy/start_client_fuse.sh
```

默认读取：

- `deploy/multi_host/rendered/client/client.env`

挂载点默认是：

```text
<CLIENT_ROOT>/mnt
```

### 7.5 机器 B：运行 demo

```bash
bash scripts/deploy/run_demo_client.sh
```

这个脚本本质上是给 [scripts/run_system_demo.sh](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/run_system_demo.sh) 预先注入：

- `MDS_ADDR`
- `SCHEDULER_ADDR`
- `RUN_DIR`
- `MOUNT_POINT`

## 8. 健康检查

在客户端机器执行：

```bash
bash scripts/deploy/check_cluster.sh
```

这会：

- 检查 `MDS` 端口可达
- 检查 `Scheduler` 端口可达
- 调用 `system_demo_tool --scenario=health`

## 9. 停止顺序

建议按反向顺序停止。

客户端机器：

```bash
bash scripts/deploy/stop_client_fuse.sh
```

数据节点机器：

```bash
bash scripts/deploy/stop_data_nodes.sh
```

MDS/Scheduler 机器：

```bash
bash scripts/deploy/stop_mds.sh
bash scripts/deploy/stop_scheduler.sh
```

## 10. 模板生成与 namespace 导入

模板生成和模板导入仍然通过 `MDS` 完成。

可以在客户端机器上发起，只要：

- `system_demo_tool` 能连到远端 `MDS`
- `path_list_file` 对 `MDS` 机器是有效路径

示例：生成模板

```bash
MDS_ADDR=10.0.0.11:9000 \
SCHEDULER_ADDR=10.0.0.11:9100 \
bash scripts/generate_masstree_template.sh template-a /data/path_lists/tree.txt copy
```

示例：导入 namespace

```bash
MDS_ADDR=10.0.0.11:9000 \
SCHEDULER_ADDR=10.0.0.11:9100 \
bash scripts/import_masstree_demo.sh 1
```

交互 demo 中也可以直接执行：

```text
4 namespace=demo-ns-001 generation=gen-001 template_id=template-a template_mode=page_fast
```

## 11. 常见问题

### 11.1 为什么不能继续用 `start_demo_stack.sh`

因为 [start_demo_stack.sh](/c:/Users/w1j2h/Desktop/AllZB/ZBPro/ZBStorage/scripts/start_demo_stack.sh) 是单机脚本，内部大量写死：

- `127.0.0.1`
- 单一 `RUN_DIR`
- 单机数据目录

它适合单机 demo，不适合拆机部署。

### 11.2 为什么客户端不需要直接连接 real/virtual/optical 节点

因为客户端只连：

- `MDS`
- `Scheduler`

后续数据节点地址解析由：

- `Scheduler cluster view`
- `MDS` 返回的副本位置

共同完成。

### 11.3 模板文件存在哪台机器

Masstree 模板目录、namespace generation manifest、sparse 索引都存放在：

- `MDS` 机器的 `MASSTREE_ROOT`

因此 `MDS_ROOT` 所在磁盘空间需要单独规划。

### 11.4 `path_list_file` 应该放在哪

建议放在 `MDS` 机器本地固定目录，并通过绝对路径传入。  
否则客户端发起模板生成时，MDS 侧可能无法访问这个路径。

## 12. 推荐的实际操作示例

假设三台机器 IP：

- `MDS`: `10.0.0.11`
- `client`: `10.0.0.12`
- `data`: `10.0.0.13`

操作流程：

1. 编辑 `deploy/multi_host/cluster.local.env`
2. 运行

```bash
bash scripts/deploy/render_multi_host_configs.sh deploy/multi_host/cluster.local.env
```

3. 将 `rendered/mds`、`rendered/data`、`rendered/client` 分别复制到三台机器
4. 在 MDS/Scheduler 机运行

```bash
bash scripts/deploy/start_scheduler.sh
```

5. 在数据节点机运行

```bash
bash scripts/deploy/start_data_nodes.sh
```

6. 在 MDS/Scheduler 机运行

```bash
bash scripts/deploy/start_mds.sh
```

7. 在客户端机运行

```bash
bash scripts/deploy/start_client_fuse.sh
bash scripts/deploy/check_cluster.sh
bash scripts/deploy/run_demo_client.sh
```

做到这里，多机部署就可以开始正常跑 `P1` 到 `P5` 了。
