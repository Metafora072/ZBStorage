# LHM v2 使用与接口

版本：2026-11-04 工作区实现。配套 [设计文档](lhm-v2-design.md)。普通 Lookup/Getattr/Create/Mkdir/Readdir/Rename/Unlink/Rmdir/Open/Close 和归档接口沿用 [service-api.md](service-api.md)。

## 1. 新建、正常关闭与重开

新建配置：

```ini
NAMESPACE_BACKEND=lhm
LHM_STORE_MODE=create
LHM_STORE_FILE=/mnt/metaiotest/filepath/results/my-run/namespace.meta
MDS_DB_PATH=/mnt/metaiotest/filepath/results/my-run/control
ENABLE_OPTICAL_ARCHIVE=false
MASSTREE_PRELOAD_ALL_SPARSE_ON_START=false
```

其他节点配置按部署补齐。父目录须存在，主文件及 `.heads` 须不存在，不覆盖旧存储。默认 mode 为 create。

正常停止 MDS 使用 SIGTERM 或 SIGINT，等待进程成功退出，并确认产生 `.snapshot`。不要使用 SIGKILL。下次使用同一控制库和存储绝对路径，改为：

```ini
LHM_STORE_MODE=reopen
```

reopen 不允许同时设置 `LHM_BULK_MANIFEST`。旧格式、快照缺失/损坏/身份不匹配、另一个进程占用存储都会失败。`.snapshot` 在服务可写期间不存在，这是正常行为。

## 2. 流式追加 RPC：ImportNamespace

这是同步管理接口，接收 MDS 服务器本地可读清单路径；不是上传文件的接口。供受信任的部署/导入调用方使用。运行期间桥接 worker 被本次导入占用，普通 LHM 请求会等待。客户端超时不代表服务端取消，不能直接自动重试。

```json
{
  "target_path": "/copy_0001",
  "manifest_path": "/mnt/metaiotest/filepath/datasets/example.tsv",
  "memory_budget_bytes": "4194304"
}
```

示例路径只是占位符，使用已有清单；本工作区性能实验输入遵守 AGENTS.md 固定数据集规则。

| 参数 | 约定 |
|---|---|
| target_path | 规范绝对路径，不能为 `/`；父目录存在，目标不存在 |
| manifest_path | 服务器本地文件路径，不通过 RPC 发送整棵树 |
| memory_budget_bytes | 0 使用 4 MiB；最小 1 MiB，只约束解析工作区 |

返回：

```text
status: {code, message}
root_inode_id
files
directories
processed_records
error_line
```

目录统计包括导入目标根；processed_records 是已经成功创建的记录数。成功时 error_line 为 0。路径和 ID 映射用 Lookup 查询，不返回全量映射表。失败可能留下已导入前缀，统计仍有效；不提供回滚或断点续传。已经导入成功的目标再次提交会失败。

同一个存储可依次导入 `/copy_0001`、`/copy_0002`，正常重开后继续 `/copy_0003`。每份清单的 `/` 映射到本次 target_path。

```bash
curl -sS -H 'Content-Type: application/json' \
  http://127.0.0.1:9000/zb.rpc.MdsService/ImportNamespace \
  --data '{"target_path":"/copy_0001","manifest_path":"/absolute/input.tsv","memory_budget_bytes":"4194304"}'
```

必须检查业务 status；HTTP 200 不等于成功。uint64 JSON 参数使用十进制字符串。尚未实现 Start/Get 异步任务 API，本版使用一个同步调用。

## 3. 清单格式和属性

LF 换行、TAB 分隔。前两列必须为 type、path，其余为可选属性列；列名不能重复，最多 32 列。不支持 CSV 引号或转义。下面用 `\t` 表示真实 TAB：

```text
type\tpath\tsize\tmode\tuid\tmtime\toptical_image_id\toptical_disk_id
dir\t/\t0\t493\t1000\t1700000000\t0\t0
dir\t/data\t0\t493\t1000\t1700000000\t0\t0
file\t/data/a\t1000000000\t420\t1000\t1700000000\t7\t9
```

第一条必须为 dir /；父目录先于孩子，支持 DFS/BFS。路径规范化后必须不变；不能出现重复斜杠、尾部斜杠、`.`、`..`、NUL、CR。名称最多 64 字节，映射后的路径深度不超过 4096。重复路径会在创建时失败，不提前建立全量去重表。

可选属性：

| 列 | 含义 |
|---|---|
| size | 字节数，只是元数据，不生成内容 |
| mode、uid、gid、nlink | 权限、所有者和链接计数；mode 使用十进制，例如 420=0644 |
| atime、mtime、ctime | 秒级时间；mtime/ctime 必须能转换为内部纳秒值 |
| version | 初始元数据版本 |
| object_unit_size、replica | 数据布局属性 |
| disk_node_id、disk_id、disk_node_is_virtual | 磁盘位置，节点 ID 为 MDS 内部紧凑编号 |
| optical_node_id、optical_disk_id、optical_image_id | 光盘位置，须与 MDS 节点/位置管理约定一致 |
| storage_tier | 0=NONE、1=DISK、2=OPTICAL |
| archive_state | 0=PENDING、1=ARCHIVING、2=ARCHIVED、3=IMAGE_READY |
| blocks_512、blksize、rdev_major、rdev_minor | 其他已有 inode 属性 |

数字均为无符号十进制，越界或非法枚举拒绝。省略列或空单元格使用默认值：文件 mode=0644、目录 mode=0755，nlink=1/2，version=1，时间为本次导入开始时间，其余为零。size 不自动生成，blocks_512 不从 size 自动推导。

不接受 inode_id、父 inode ID、磁盘地址或运行时索引编号；由后端生成。file_name 从目标路径提取。

位置字段是属性保留，不会自动为每份副本分配新的光盘位置，也不会创建控制库中的完整数据位置记录或实际文件内容。调用方须保证位置有效；填写字段不意味着文件已可读或可直接归档。仅验证命名空间使用 Lookup/Getattr/Readdir 即可。

## 4. 内部 C++ / C 接口

```cpp
LhmNamespaceStore store(file);                     // 新建
LhmNamespaceStore store(file, true, &error);       // 正常重开
NamespaceImportResult result;
auto status = store.ImportManifest(target, manifest, budget, &result);
bool closed = store.Close(&error);                // 显式检查快照失败
```

两行同名 store 是分别展示两种构造方式，不应放在同一作用域。关闭前释放创建预留和 scanner；否则 Close 返回错误并保留实例。真正开始关闭后，写快照失败也会关闭实例并返回失败，不能继续使用。

C ABI 提供 `zb_lhm_open`、`zb_lhm_reopen`、`zb_lhm_import_manifest`、`zb_lhm_close_checked`。后者无论成功失败都消费 context；旧 `zb_lhm_close` 保留兼容，失败打印诊断。调用方必须先停止并等待所有并发调用。

现有 `LHM_BULK_MANIFEST` 仍要求新库，仍只接受 type/path 两列、全量构建和默认属性；它不是新的流式导入入口。

## 5. 验证

```bash
BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
CCACHE_DISABLE=1 cmake --build "$BUILD" --target mds_server lhm_lifecycle_test -j 3
"$BUILD/src/mds/lhm/lhm_lifecycle_test" /mnt/metaiotest/filepath/results/new-functional-run
python3 tests/mds/lhm_lifecycle_rpc_test.py --build "$BUILD" \
  --work-dir /mnt/metaiotest/filepath/results/new-rpc-run \
  --manifest /mnt/metaiotest/filepath/results/new-functional-run/manifest.tsv
```

结果目录必须是新目录。测试生成的是小型功能边界 fixture，不是性能数据集。需要 localhost socket 权限；结束会关闭服务。测试后按 AGENTS.md 整理证据和清理可再生运行数据。
