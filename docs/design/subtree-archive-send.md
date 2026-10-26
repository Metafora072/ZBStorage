# 子树归档元数据构建和发送

2026-10-23 更新：四个分配/上报 RPC 和尾批结束处理已接通，见
[简化归档闭环](simple-archive-callbacks.md)。下文保留最初元数据发送阶段的说明；
当前发送器还会在全部文件发送后追加结束标记，回写由光盘节点的完成上报执行。

本轮完成位置查询接口、分批归档清单和 MDS 到真实光盘协议的发送链路。
复用已有 `OpticalNodeService.SendArchiveMetadata`，没有修改光盘节点业务。
返回成功只表示元数据受理，不代表文件下载、镜像生成、刻录或归档完成。

## 入口

新增同步 RPC `MdsService.SendSubtreeArchive`：

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

- `root_path` 必须是目录；不返回目录项，只下发其后代文件。
- `optical_node_address` 是支持现有物理光盘协议的服务地址。模拟光盘协议要求的租约、操作 ID 等字段不由本入口生成。
- `batch_id` 由调用方指定非零新编号，每个分片使用相同编号；没有持久化去重或全局编号分配。
- 每批文件上限默认 256，最大 4096；每次扫描工作预算默认 1024，最大 65536。
- 每个下游 RPC 超时默认 5000 ms，最大 60000 ms；单个请求最多 4 MiB。
- 同步调用可能持续整个子树发送的时间。上游调用超时应覆盖全部发送；超时后不能假定 MDS 已停止，也不能自动重试。

回复包含 `status`、`acknowledged_files`、`acknowledged_batches`、`attempted_batches`、
`last_batch_may_be_accepted`。数量是收到成功回复的文件/分片数量，不是镜像中已经存好的文件数。
空子树成功返回零，不发送空请求。空扫描批次且 done=false 会继续推进。

如果前几片成功而后一片失败，保留前几片计数并停止。发送失败或回复无效时，
`last_batch_may_be_accepted=true` 表示不能排除当前片已被远端部分/全部接收。
如果下一片在构建阶段失败，当前没有请求发出，该标志为 false，但此前成功片仍然有效。
没有跨片回滚，也没有自动重试；brpc `max_retry=0`。

## 调用链及职责

```text
SendSubtreeArchive RPC
  → ArchiveManifestBuilder
      → NamespaceScanner::Next（现有分批子树扫描）
      → FileLocationResolver::Resolve(inode_id)
  → ArchiveFile 分片
  → OpticalNodeService.SendArchiveMetadata
```

`location/FileLocationResolver.h/.cpp`：

- 对外只接收 inode ID，返回带完整属性和磁盘/光盘位置的 FileLocationView。
- `RecordFileLocationResolver` 使用注入的 inode、位置记录、节点/磁盘解析函数，不依赖 RocksDB 类型或 key。
- 完整 DL/OL 记录优先；只有记录缺失才从紧凑 inode 字段恢复位置。记录损坏或读取失败直接报错，不能静默回退到拼接出的节点/磁盘名。
- 保留任意字符串节点、磁盘和镜像 ID；节点地址及实例代次由现有 MDS 名录解析。
- 保留原光盘 compact ID 解析和库交接映射逻辑，通过回调注入。
- GetFileLocation、Open 的位置视图以及主位置查询也复用这一实现。旧 Masstree 路由回退仅在旧模式且位置缺失时保留；LHM 不回退。

`archive/SubtreeArchive.h/.cpp`：

- `ArchiveManifestBuilder` 持有扫描对象，借用位置解析器，逐片生成 ArchiveFile。
- 首版只接受 `INODE_FILE`、`INODE_STORAGE_DISK`、`replica=1` 且旧状态为 `INODE_ARCHIVE_PENDING` 的文件。旧状态仅用于拒绝明显不符合条件的文件，未复用它实现文档的新三阶段状态机。
- 非空文件要求 object_unit_size>0；零大小文件可发送。
- 不静默过滤不符合条件的文件：发现即终止，当前片结果丢弃。
- 解析位置时重新按 ID 读取属性，和扫描结果比较；观察到修改会拒绝发送。该检查不等于冻结或快照，也无法消除检查后的并发修改窗口。
- ArchiveFile 仅设置 inode_id、size、object_unit_size、target_node_id、target_disk_id 五个字段；原有 modeled-protocol 附加字段留空。
- 所有片都复用同一个 sink，顺序发送。若回复提供逐文件 admissions，还要验证全部对应 ID 和成功状态，不能仅检查顶层 OK。

`service/ArchiveSubtreeService.cpp` 负责参数默认值、组装接口和返回计数。
位置仍从 LHM inode + 控制库 DL/OL 获取，本轮不迁移 DL/OL 的物理存储。

## 运行前提和范围

调用方保证选定文件是本次待归档的热文件，整个归档期间文件和目录结构保持不变，
祖先路径不改名/移动；一个光盘库同一时间只有这一批工作。本轮不实现冻结、
跨 MDS 的互斥、定时选目录、任务状态、租约、恢复、去重或事务。

发送成功不修改 inode 归档状态、不删除热数据、不更改数据位置。
两个 ID 分配接口、两次镜像/光盘结果上报和关系管理尚未在此链路接入。
因此本轮不能作为完整归档闭环，也不能重复提交同一个目录来模拟重试。

## 验证与复现

测试数据目录必须在 `/mnt/metaiotest/filepath`，使用新的路径：

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
CCACHE_DISABLE=1 cmake --build "$ZB_BUILD" \
  --target archive_pipeline_test mds_server real_node_server lhm_mds_service_test -j2
"$ZB_BUILD/src/mds/lhm/archive_pipeline_test" \
  /mnt/metaiotest/filepath/results/archive_unit_repeat
python3 tests/mds/namespace_rpc_test.py --build "$ZB_BUILD" --backend lhm \
  --work-dir /mnt/metaiotest/filepath/results/archive_rpc_repeat --exercise-archive
```

单元测试覆盖完整位置/旧字段回退/损坏与读取错误、光盘不透明 ID、文件集合与批次边界、
空目录、不支持类型/状态/副本、扫描后修改、部分发送失败和不重试。
RPC 测试启动真实 MDS/数据节点与记录型 OpticalNodeService 测试端，经网络验证分片内容、
原始数据位置、文件大小和第二片失败时的进度。记录端只模拟元数据受理，不生成镜像或刻录；
测试结果不能用作光盘节点读取文件或烧录成功的证据。
另覆盖下游超时、缺失 status、顶层 OK 但逐文件 admission 失败，确认失败分片不会重试。
