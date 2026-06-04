# system_demo_tool 运行测试报告

## 测试环境

- 测试时间：2026-05-29 06:12-06:13 UTC
- 执行入口：`bash scripts/run_system_demo.sh`
- 运行根目录：`/mnt/md0/wjh/zb_run_dir_v3`
- FUSE 挂载点：`/mnt/md0/wjh/zb_run_dir_v3/mnt`
- 原始输出目录：`/mnt/md0/wjh/zb_run_dir_v3/logs/demo_function_report`

测试前服务状态正常：

```text
scheduler: RUNNING
real_node: RUNNING
virtual_node: RUNNING
mds: RUNNING
fuse: RUNNING
```

RPC 端口 `9000`、`9100`、`19080`、`29080` 均处于开放状态。

为保证 Masstree 查询功能有真实可查数据，测试前额外导入了一个 20 文件的小型真实 Masstree 命名空间：

```bash
TEMPLATE_ID=template-demo-report-small-20260529061217 \
OVERWRITE_TEMPLATE=true \
MASSTREE_VERIFY_INODE_SAMPLES=1 \
MASSTREE_VERIFY_DENTRY_SAMPLES=1 \
bash scripts/import_random_masstree_metadata.sh 20 demo-report-real gen-report-real
```

## 执行结果汇总

| 菜单项 | 功能 | 执行命令 | 结果 | 原始输出 |
| --- | --- | --- | --- | --- |
| `0` | 环境健康检查 | `0` | 通过 | `menu_0_health.out` |
| `1` | TC-P1 全局统计 | `1` | 通过 | `menu_1_stats.out` |
| `2` | TC-P2 真实节点读写 | `2 file_size_mb=1 chunk_size_kb=64 keep_file=false verify_hash=true` | 通过 | `menu_2_real_io.out` |
| `3` | TC-P3 虚拟节点读写 | `3 file_size_mb=1 chunk_size_kb=64 keep_file=false verify_hash=true` | 通过 | `menu_3_virtual_io.out` |
| `4` | TC-P4 Masstree 导入 | `4 namespace=demo-report-sim generation=gen-report-sim ...` | 通过 | `menu_4_masstree_import.out` |
| `5` | TC-P5 Masstree 查询 | `5 n=3 query_mode=random_inode output_limit=3 success_latency_limit_ms=0` | 通过 | `menu_5_masstree_query.out` |
| `6` | 50 亿文件测试 | `6 script=scripts/random_read_6b_files.sh /mnt/md0/wjh/zb_run_dir_v3/mnt/virtual -n 1 --file-size 1 --continue-on-error --verbose` | 失败 | `menu_6_50yi_script.out` |

## 通过项摘要

### 0. 环境健康检查

结果：通过。

关键输出：

```text
mds_root_inode=1
cluster_generation=307
online_nodes=2
mount_point=/mnt/md0/wjh/zb_run_dir_v3/mnt
real_root=/real
virtual_root=/virtual
```

### 1. TC-P1 全局统计

结果：通过。

关键输出：

```text
real_physical_nodes=1
real_logical_nodes=1
real_disks=24
virtual_logical_nodes=99
virtual_disks=2376
online_logical_nodes=100
optical_nodes=10000
optical_devices=100000000
total_file_count=101059007069
total_metadata_bytes=36063435213972 (32.80TB)
```

### 2. TC-P2 真实节点读写

结果：通过。

本次使用 1 MiB 文件做快速验证。写入和回读字节数一致，hash 一致，后端对象存在。

```text
bytes_written=1048576
bytes_read=1048576
write_hash=0x839f3ba0fca8df95
read_hash=0x839f3ba0fca8df95
backend_object_exists=true
backend_object_size_bytes=1048576 (1.00MB)
```

### 3. TC-P3 虚拟节点读写

结果：通过。

本次使用 1 MiB 文件做快速验证，写入和回读字节数一致，虚拟层对象元数据生成成功。

```text
bytes_written=1048576
bytes_read=1048576
inspected_tier=virtual
object_count=1
first_object_id=obj-134548596225-0
```

注意：输出中 `read_hash=0x0000000000000000`，但功能结果仍为通过。后续如要严格校验虚拟层数据内容一致性，建议检查该 demo 对虚拟层 `verify_hash` 的处理逻辑。

### 4. TC-P4 Masstree 导入

结果：通过。

本次按菜单默认路径执行模拟导入，并将规模参数缩小为 20 个文件。

```text
import_mode=simulated
namespace_id=demo-report-sim-20260529_061337
generation_id=gen-report-sim
job_status=completed elapsed=0s
file_count=20
delta_total_file_count=20
delta_total_metadata_bytes=4096
```

### 5. TC-P5 Masstree 查询

结果：通过。

本次使用 `random_inode` 查询 3 个样本。

```text
query_samples=3
query_mode=random_inode
query_success_count=3
query_success_rate=1.0000
avg_query_latency=147.12ms
min_query_latency=64.43ms
max_query_latency=243.38ms
```

## 失败项详情

### 6. 50 亿文件测试

结果：失败。

执行命令：

```text
6 script=scripts/random_read_6b_files.sh /mnt/md0/wjh/zb_run_dir_v3/mnt/virtual -n 1 --file-size 1 --continue-on-error --verbose
```

失败输出：

```text
failed to collect requested readable files: requested=1, completed=0, attempts=1001

========================================
 50亿文件测试
========================================
结果: 失败
摘要: 50亿文件测试失败
命令: 6 script=scripts/random_read_6b_files.sh /mnt/md0/wjh/zb_run_dir_v3/mnt/virtual -n 1 --file-size 1 --continue-on-error --verbose
用法: 6 script=<path> [script args...]

[关键指标]
script_path   scripts/random_read_6b_files.sh
script_arg_1  /mnt/md0/wjh/zb_run_dir_v3/mnt/virtual
script_arg_2  -n
script_arg_3  1
script_arg_4  --file-size
script_arg_5  1
script_arg_6  --continue-on-error
script_arg_7  --verbose
command       bash 'scripts/random_read_6b_files.sh' '/mnt/md0/wjh/zb_run_dir_v3/mnt/virtual' '-n' '1' '--file-size' '1' '--continue-on-error' '--verbose'
exit_code     256
```

可能原因：

1. `scripts/random_read_6b_files.sh` 调用的是 `build/random_read_6b_files`，该工具要求输入目录符合固定的 60 亿文件压测目录结构：

```text
<root>/dir002..dir051/vdb.1_1.dir..vdb.1_100.dir/
  vdb.2_1.dir..vdb.2_100.dir/vdb.3_1.dir..vdb.3_100.dir/
    vdb_f0001.file..vdb_f0120.file
```

2. 本次传入的 `/mnt/md0/wjh/zb_run_dir_v3/mnt/virtual` 是 demo FUSE 的虚拟层目录，并不存在上述 `dir002/.../vdb_f*.file` 压测数据树。

3. 菜单中展示的示例脚本是 `scripts/import_50yi.sh`，但当前仓库没有这个脚本。实际可用的是 `scripts/random_read_6b_files.sh`，它只负责随机读取已有 60 亿文件目录树，不负责生成目录树。

4. `exit_code=256` 是 `std::system()` 返回值，通常对应子进程退出码 `1`。

建议：

- 如果要通过第 6 项，需要先准备符合 `random_read_6b_files` 要求的压测目录树，再把该目录作为脚本第一个参数。
- 如果项目期望菜单第 6 项完成“导入/生成 50 亿文件测试数据”，需要补充 `scripts/import_50yi.sh` 或修改菜单示例。
- 如果第 6 项只是通用脚本入口，建议在文档中明确：它不会自动生成 50 亿/60 亿文件数据集。

## 文档修正

测试时发现 `doc/run.md` 中交互菜单编号仍是旧版。已按当前 `tools/demo/system_demo_tool.cpp` 的实际菜单修正为：

```text
0 环境健康检查
1 TC-P1 全局统计
2 TC-P2 真实节点读写
3 TC-P3 虚拟节点读写
4 TC-P4 Masstree 导入
5 TC-P5 Masstree 查询
6 50亿文件测试
q 退出
```
