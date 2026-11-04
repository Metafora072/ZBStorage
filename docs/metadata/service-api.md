# 元数据服务使用与接口说明

本文面向 MDS 调用方，介绍批量创建命名空间、普通 RPC 和服务内部接口。配套阅读：[元数据管理设计与代码导读](namespace-design.md)。以本仓库 LHM 批量导入实现为基准；字段定义见 [mds.proto](../../src/msg/mds.proto)，行为以服务实现为准。

> 2026-11-04：v2 已增加宽编号、共享目录头、流式追加和正常重开。新增接口及启动方式见 [LHM v2 接口](lhm-v2-api.md)，实现见 [v2 设计](lhm-v2-design.md)。本文第 3 节继续描述兼容的旧启动批量入口。

## 1. 先分清三种入口

| 入口 | 调用者 | 用途 |
|---|---|---|
| MDS RPC：`zb.rpc.MdsService` | 外部客户端 | Lookup、Create、Readdir、Open、归档等 |
| `NamespaceStore` C++ 接口 | MDS 内部及嵌入式程序 | 路径解析、inode 读写、预留创建、子树扫描 |
| `LHM_BULK_MANIFEST` 启动配置 | 启动 MDS 的使用者 | 在新库中一次性导入目录树 |

现在可通过 `ImportNamespace` RPC 向运行中的 MDS 提交服务器本地 TSV 路径，详见 v2 接口文档。批量导入只生成元数据，不创建数据文件、不分配数据节点；导入的文件可以查询、列目录及修改命名空间，但不能据此认定文件内容可读或可以直接归档。

LHM 模式统一管理热、冷命名空间，仍保留 RocksDB 控制库。v2 支持新建和正常关闭后的重开；异常恢复和事务不在范围内。

## 2. 编译与启动

以下命令从仓库根目录执行。开发机已有构建目录时应复用；新环境需要 C/C++ 工具链、CMake、Protobuf、gflags、zlib、FUSE3 以及 brpc/RocksDB 的依赖。项目依赖声明见 [CMakeLists.txt](../../CMakeLists.txt)。

```bash
BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
cmake -S . -B "$BUILD" -DZB_BUILD_LHM_BRIDGE=ON -DBUILD_TESTING=ON
cmake --build "$BUILD" --target mds_server lhm_bulk_import_test -j 4
RUN=$(mktemp -d /mnt/metaiotest/filepath/results/mds_manual.XXXXXX)
mkdir -p "$RUN/scratch" "$RUN/evidence"
```

`BUILD` 是本工作区已有目录的示例，不是安装后的固定路径。较大编译或实验前检查目标文件系统容量；数据、日志和数据库都放在 `/mnt/metaiotest/filepath`。

将已有的 TSV 清单绝对路径赋给 `MANIFEST`，然后写入配置：

```bash
MANIFEST=/absolute/path/to/namespace.tsv
cat > "$RUN/mds.conf" <<EOF_CONFIG
MDS_DB_PATH=$RUN/scratch/control
NAMESPACE_BACKEND=lhm
LHM_STORE_FILE=$RUN/scratch/namespace.meta
LHM_BULK_MANIFEST=$MANIFEST
ENABLE_OPTICAL_ARCHIVE=false
MASSTREE_PRELOAD_ALL_SPARSE_ON_START=false
MASSTREE_ROOT=$RUN/scratch/templates
NODES=node-real-01@127.0.0.1:9100
DISKS=node-real-01:disk-01
REPLICA=1
PG_COUNT=16
EOF_CONFIG
"$BUILD/mds_server" --config="$RUN/mds.conf" --port=9000 \
  > "$RUN/evidence/mds.log" 2>&1 &
MDS_PID=$!
```

这是独立模式的配置示例。节点地址必须替换成自己的数据节点配置；仅验证导入后的 Lookup/Readdir 不需要启动数据节点。普通 Create 涉及放置选择，文件内容读写需要对应的数据服务。接入 Scheduler 的部署还需按节点名录流程配置，不能直接用这里的静态节点代替。

上述 create 模式下父目录须存在，`namespace.meta` 和 `.heads` 须不存在；控制库也使用新目录。已有命名空间、句柄或 LHM 后端标记可能使 create 模式启动被拒绝；正常重开须显式配置 `LHM_STORE_MODE=reopen` 并使用配套控制库。不设置 `LHM_BULK_MANIFEST` 则创建空命名空间。不允许在 RocksDB 后端设置此导入参数。

日志中的 `LHM bulk import files=... directories=... inode_blocks=... buckets=...` 表示导入统计；最终还应确认服务已监听端口。测试完成执行 `kill "$MDS_PID"; wait "$MDS_PID"` 关闭服务，再按工作区保留策略整理数据。不要删除服务仍在使用的目录。

## 3. 批量创建元数据

### 3.1 输入格式

TSV 使用 LF 换行，每行两列，分隔符为真正的 TAB。第一行严格为 `type<TAB>path`。全文以这棵树为例：

```text
/
├── project/
│   ├── a.txt
│   └── empty/
└── b.txt
```

对应清单：

```text
type	path
dir	/
dir	/project
file	/project/a.txt
dir	/project/empty
file	/b.txt
```

规则：

- 第一条数据为 `dir /`；`type` 只接受 `dir` 和 `file`。
- 目录须显式列出，父目录须先于子项；DFS、BFS 或混合顺序均可。
- 路径必须绝对且规范。不允许重复、尾部斜杠、重复斜杠、`.`、`..`、NUL、CR、额外列或空行。
- 每个名称分量最多 64 字节，中文按编码后的字节数计算。没有额外 UTF-8 合法性校验；建议输入 UTF-8。
- 不接受自定义 inode ID、size 或 mode 列。内部按父目录组织文件，无需自行生成临时编号。

### 3.2 默认属性与输出

| 属性 | 文件 | 目录 |
|---|---|---|
| mode | `0644` | `0755` |
| size | 0 | 0 |
| uid/gid | 0/0 | 0/0 |
| nlink | 1 | 2 |
| version | 1 | 1 |
| atime/mtime/ctime | 导入时的秒级时间 | 同左 |
| file_name | 最后一个路径分量 | 最后一个路径分量，root 为 `/` |
| 存储位置、副本、对象单元 | 未分配，相关字段为零 | 同左 |

输出为新存储文件和当前进程可用的命名空间。路径与 ID 的对应关系通过 Lookup 获取；不返回整张映射表。root 对外 ID 固定为 1，其余 ID 是不透明标识，客户端不能假定连续。

先校验完整清单，再创建输出文件。已有输出不覆盖；校验失败不创建输出，构建失败销毁 context 并尝试删除本次输出，MDS 不对外发布半成品。当前清单与构建计划全部驻留内存，大规模导入需要预估内存；本兼容入口不提供流式构建；新的流式入口见 v2 文档，正常关闭会保存可重开的索引快照。

## 4. RPC 公共约定

brpc 支持 protobuf 客户端，也可在测试中使用 HTTP JSON：

```bash
MDS=http://127.0.0.1:9000
rpc() {
  curl --fail-with-body -sS -H 'Content-Type: application/json' \
    "$MDS/zb.rpc.MdsService/$1" --data "$2"
}
rpc Lookup '{"path":"/project/a.txt"}'
```

HTTP 成功不代表业务成功，必须检查返回 `status.code`。protobuf JSON 的枚举可能以名称显示，默认值字段可能省略；uint64 建议用十进制字符串传递，避免 JavaScript 大整数精度损失。

普通 LHM 路径接口可规范化重复斜杠及尾部斜杠，清单校验更严格；客户端统一发送规范绝对路径最清晰。名称限制仍为 64 字节。

### 4.1 查询与公共属性

| RPC | 请求 | 返回 | 说明 |
|---|---|---|---|
| `Lookup` | `path: string` | `status, attr: InodeAttr` | 路径解析并取得属性 |
| `Getattr` | `inode_id: uint64` | `status, attr` | 已知 ID 时无需重新解析路径 |
| `GetFileLocation` | `inode_id: uint64` | `status, location: FileLocationView` | 属性加磁盘/光盘位置，依赖位置元数据 |

`InodeAttr` 常用字段：`inode_id`、`type`、`file_name`、`mode`、`uid/gid`、`size`、`atime/mtime/ctime`、`nlink`、`version`。时间为秒，size 为字节，mode 为权限数值；`file_name` 不是完整路径。

protobuf 的 `INODE_FILE=0`、`INODE_DIR=1`，不要把内部 C 桥接类型编号直接传给 RPC。`storage_tier` 的 NONE/DISK/OPTICAL 分别为 0/1/2；`replica` 与 `object_unit_size` 描述数据布局。归档属性为 `optical_image_id`、`optical_disk_id`、`file_archive_state` 等；节点紧凑 ID 与对外节点字符串也不是同一个值。

Lookup 的返回形状如下，省略号只是解释，不能直接作为 JSON 请求：

```text
status: {code: MDS_OK, message: OK}
attr: {inode_id: <实际ID>, type: INODE_FILE, file_name: a.txt, size: 0, mode: 420, ...}
```

实际 ID 必须取服务返回值。例如：

```bash
INODE_ID=$(rpc Lookup '{"path":"/project/a.txt"}' | python3 -c \
  'import json,sys; print(json.load(sys.stdin)["attr"]["inode_id"])')
rpc Getattr "{\"inode_id\":\"$INODE_ID\"}"
```

生产客户端应先检查 status，再取 attr。以上脚本用于已确认成功的演示流程。

### 4.2 创建与删除

| RPC | 请求字段 | 返回 | 使用约束 |
|---|---|---|---|
| `Mkdir` | path、mode、uid、gid | status、attr | 父目录已存在，目标不存在 |
| `Create` | path、mode、uid、gid、replica、object_unit_size | status、location | 创建文件元数据并选择放置位置 |
| `Unlink` | path | status | 删除文件命名空间项 |
| `Rmdir` | path | status | 删除空目录，不能删除根 |

普通 Create/Mkdir 的 mode 使用请求值，**并非批量导入的 0644/0755 默认规则**，应显式传入：JSON 的 420 等于八进制 0644，493 等于 0755。uid/gid 省略时为 0；Create 的 replica=0 使用 1，object_unit_size=0 使用服务默认值。

```bash
rpc Mkdir '{"path":"/project/new","mode":493,"uid":0,"gid":0}'
rpc Create '{"path":"/project/new/c.txt","mode":420,"replica":1}'
rpc Unlink '{"path":"/project/new/c.txt"}'
rpc Rmdir '{"path":"/project/new"}'
```

Create 不是上传内容；Unlink 也不能作为物理数据已擦除的证明。底层删除后，原 inode ID 的命名空间读取失效。目标已存在不会覆盖；删除非空目录失败。

### 4.3 改名与属性更新

`Rename(old_path, new_path) -> status` 支持文件跨目录移动和目录移动，ID 保持不变。目标父目录须存在；禁止目标覆盖、操作根目录及将目录移到自身子树。相同源目标也按已存在处理。

```bash
rpc Rename '{"old_path":"/project/a.txt","new_path":"/project/renamed.txt"}'
rpc Getattr "{\"inode_id\":\"$INODE_ID\"}"
```

`UpdateInodeStat` 返回 `status, attr`：

| 请求字段 | 含义 |
|---|---|
| inode_id | 要更新的文件 ID |
| file_size | 写入的新大小；省略会是 0，不表示保留旧值 |
| object_unit_size | 大于 0 时更新；0 保留原值 |
| version | 必须非零且不小于当前版本；较小返回 STALE_EPOCH，相等允许；不是服务端自动递增 |
| mtime | 大于 0 使用请求时间，否则使用当前时间 |

该操作更新 ctime，并将归档状态置为 PENDING；已归档文件受只读检查限制。它不是任意属性修改接口，不支持在这里直接设置 uid/gid、mode、镜像或光盘位置。改变 size 不会生成对应字节的数据。

```bash
rpc UpdateInodeStat "{\"inode_id\":\"$INODE_ID\",\"file_size\":\"128\",\"version\":\"2\"}"
```

### 4.4 分页列目录

`Readdir(path, limit, start_after)` 只返回直接孩子。`entries` 的每项是 `{name, inode_id, type}`，不是完整 inode 属性。

- `limit=0` 表示不限制条目数；大目录使用正数。
- 按名称字节序返回，`start_after` 为排他名称边界，不是路径、页码或 inode ID。
- 首次传空字符串，后续将 `next_token` 原样作为 `start_after`。
- 依据 `has_more` 决定是否继续，不要仅根据页长推断完成。
- 这不是快照；遍历期间改变目录可能导致跨页结果变化。

```bash
rpc Readdir '{"path":"/project","limit":1,"start_after":""}'
# 从上次回复取 next_token 后继续，而不是写死名称。
```

需要完整属性时对每个 inode_id 调用 Getattr。需要递归整棵子树时，外部客户端可递归 Readdir；服务内部可使用下一节的增量扫描。

### 4.5 Open 与 Close

`Open(path, flags) -> status, handle_id, location`；`Close(handle_id) -> status`。

handle_id 是 MDS 分配的句柄编号，不是 inode ID，也不是客户端操作系统文件描述符。当前服务将 `H/<handle_id> -> 编码后的 inode_id` 保存于控制库；Close 删除该关系。文件 Open 会构建位置视图；批量导入但未设置位置的文件不能据此保证可打开或可读。只需查属性时用 Lookup/Getattr。

```bash
# 对具有有效数据位置的文件调用：
rpc Open '{"path":"/some-placed-file","flags":0}'
# 使用返回的真实 handle_id 调用 Close。
```

flags 沿用打开标志，已归档文件拒绝写打开；不要推导为已实现完整 POSIX 打开/删除生命周期语义。当前 Getattr/GetFileLocation 请求接收的是 inode_id，不能传 handle_id。

## 5. 服务内部 C++ 接口

头文件：[NamespaceStore.h](../../src/mds/storage/NamespaceStore.h)。RPC 调用方不需要链接这一层。

| 方法 | 参数/返回要点 |
|---|---|
| ResolvePath | path → inode_id、InodeAttr；bool + error |
| GetInode | inode_id → InodeAttr；bool + error |
| PutInode | inode_id、完整 InodeAttr；保留后端结构地址，不能用于新建路径 |
| ReserveCreate | path、InodeType → `unique_ptr<NamespaceCreation>` |
| Rename / Remove | 路径操作 → NamespaceStatus；Remove 还需指定类型 |
| Readdir | path、start_after、limit → entries、has_more、next_token、error |
| BeginSubtreeScan | root_path、options → `unique_ptr<NamespaceScanner>` |

ResolvePath/GetInode 返回 false 且 error 为空代表未找到，非空代表错误。创建流程是 ReserveCreate → 读取 `inode_id()` → 填完整属性 → Commit；未提交可 Cancel，预留对象析构自动取消。Commit 的属性 ID、类型和名称必须与预留相符。LHM 不支持旧式 AllocateInodeId + Create 组合。

子树扫描代码示例：

```cpp
zb::mds::SubtreeScanOptions options;
options.files_only = false; // true 默认只返回文件
options.work_budget = 1024;
std::unique_ptr<zb::mds::NamespaceScanner> scan;
auto status = store.BeginSubtreeScan("/project", options, &scan);
if (!status.ok()) throw std::runtime_error(status.message);
for (;;) {
    zb::mds::SubtreeScanBatch batch;
    status = scan->Next(256, &batch);
    if (!status.ok()) throw std::runtime_error(status.message);
    for (const auto& inode : batch.entries) {
        // 使用完整 InodeAttr；输出不含完整路径。
    }
    if (batch.done) break;
}
```

root_path 必须为目录，结果不包含根自身。`Next(max_entries)` 的参数须大于 0；work_budget 限制单次推进工作量，不是时间或字节数。允许空批次但 done=false，必须继续；顺序未定义。失败后丢弃当前批次，扫描终止，先前成功批次仍有效。

扫描期间保持选定子树结构不变，祖先不能移动；没有快照，属性是读取时的值。游标不支持持久化和并发 Next。通用扫描器借用 store，调用方应保持 store 生命周期覆盖扫描；LHM 具体实现额外持有底层 context，但不应以此依赖其他后端。

## 6. 归档接口

这里描述已接通的简化流程，不使用旧任务租约协议。

`SendSubtreeArchive` 接收目录或 inode 列表，二选一：

```json
{
  "root_path": "/project",
  "optical_node_address": "127.0.0.1:9200",
  "batch_id": "101",
  "max_files_per_batch": 256,
  "scan_work_budget": 1024,
  "rpc_timeout_ms": 5000
}
```

列表方式移除 root_path，增加 `"inode_ids":["实际文件ID1","实际文件ID2"]`。ID 不能为 0；重复 ID 按首次出现去重；目录 ID 不递归展开。

| 参数 | 默认/上限 |
|---|---|
| root_path 或 inode_ids | 必须且只能提供一种 |
| optical_node_address | 目标物理光盘协议服务地址 |
| batch_id | 调用方提供新的非零编号 |
| max_files_per_batch | 0 使用 256，上限 4096 |
| scan_work_budget | 0 使用 1024，上限 65536；只影响目录扫描 |
| rpc_timeout_ms | 0 使用 5000，上限 60000；针对每次下游调用 |

文件要求磁盘主存储、replica=1、PENDING 状态及有效来源位置；非空文件还需 object_unit_size>0。导入清单产生的无位置文件不满足这些条件。

返回 `acknowledged_files`、`acknowledged_batches`、`attempted_batches`、`last_batch_may_be_accepted` 和 status。成功仅表示元数据下发受理，不代表镜像或刻录完成。后片失败保留前片进度，不回滚、不自动重试。最后发送结束标记以触发未满镜像/光盘处理；空目录不发送。上游超时应覆盖整个同步发送过程，超时不能解释成远端未执行。

光盘节点启用 `SIMPLE_ARCHIVE_MDS_ENABLED=true` 并配置 `MDS_ADDRESS` 后调用：

| RPC | 请求 | 返回/作用 |
|---|---|---|
| AllocateAvailableImageId | node_id、node_address、allocate_ts_ms | image_id，如 img-N |
| AllocateAvailableDiscId | 同上 | disc_id，如 disc-N |
| ReportFilesPackedToImage | node_id、image_id、inode_ids | status、updated_files；写镜像 ID 和 IMAGE_READY |
| ReportImagesBurnedToDisc | node_id、disc_id、image_ids | status、updated_files；写光盘 ID 和 ARCHIVED |

容量由光盘节点 `VOLUME_SIZE_BYTES`、`SIZE_THRESHOLD`、`DISC_CAPACITY_BYTES` 控制，不是 TSV 或 SendSubtreeArchive 参数。ID 分配仅保证当前单 MDS 进程范围；归档期间文件不变，采用单任务模型。没有镜像到 inode 的反向索引，光盘回报通过根目录扫描匹配镜像。不会自动切换客户端读路由或删除热副本。错误返回的 updated_files 可能非零，表示部分回写已经完成。

## 7. 错误与排查

| code | 含义 | 调用方处理 |
|---|---|---|
| MDS_OK=0 | 成功 | 读取业务结果 |
| MDS_INVALID_ARGUMENT=1 | 参数/类型不符或该模式不支持 | 检查 message 和接口约束 |
| MDS_NOT_FOUND=2 | 路径或 ID 不存在 | 检查路径与生命周期 |
| MDS_ALREADY_EXISTS=3 | 目标已存在 | 换目标或显式处理旧对象 |
| MDS_NOT_EMPTY=4 | 目录非空 | 先处理子项 |
| MDS_INTERNAL_ERROR=5 | 后端、位置或服务错误 | 保留日志并定位，勿假定可安全重试 |
| MDS_STALE_EPOCH=6 | 代次不符 | 按相关控制协议刷新状态 |

不同 RPC 的路径校验和错误映射不完全统一，错误详情应读取 message。传输失败与业务失败分开处理。LHM 的事务和恢复不在本版本范围，修改失败不能一概推断“没有任何状态改变”。

排查顺序：确认 `namespace_backend=lhm` → 检查导入日志和新文件要求 → Lookup 检查路径 → Getattr 检查 ID → 涉及内容/归档时检查位置和节点服务。`ENABLE_OPTICAL_ARCHIVE=false` 关闭旧后台归档流程，并不表示上面的简化显式归档 RPC 未实现。

## 8. 验证入口与进一步阅读

- [lhm_bulk_import_test.cpp](../../tests/mds/lhm_bulk_import_test.cpp)：最直接覆盖批量创建和后续普通操作。
- [lhm_bulk_startup_test.py](../../tests/mds/lhm_bulk_startup_test.py)：启动真实 MDS 并通过 RPC 校验整棵导入树。
- [namespace_rpc_test.py](../../tests/mds/namespace_rpc_test.py)：普通 RPC 和可选归档联调。
- [subtree_scan_test.cpp](../../tests/mds/subtree_scan_test.cpp)：扫描边界、错误和生命周期。
- [批量导入复现命令](../design/lhm-bulk-import.md)、[简化归档细节](../design/simple-archive-callbacks.md)。

这些是手动测试目标/脚本，不应只运行 ctest 就认定已执行。上述命令中的示例树用于解释格式，不应替代工作区规定的性能实验数据集。
