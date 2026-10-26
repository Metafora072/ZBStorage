# 第五步：MDS 统一 LHM 后端

已实际以 LHM 模式启动 MDS 和本机数据节点并通过 RPC 测试。默认配置仍为 rocksdb，
显式选择 lhm 后，热路径和原冷命名空间的查询均只进入同一个 LhmNamespaceStore。
原 MasstreeLHM-index 未修改，构建使用 third_party/lhm。

## 启用方式

编译时启用 `-DZB_BUILD_LHM_BRIDGE=ON`。在现有 MDS 配置中增加：

```ini
NAMESPACE_BACKEND=lhm
LHM_STORE_FILE=/mnt/metaiotest/filepath/results/my_zb_run/namespace.meta
MDS_DB_PATH=/mnt/metaiotest/filepath/results/my_zb_run/control_db
ENABLE_OPTICAL_ARCHIVE=false
```

父目录必须存在，namespace.meta 必须不存在。control_db 建议使用新目录；存在 I/、D/、H/
或 LHM 后端标记时拒绝启动。已有 LHM 文件不截断，不支持重新打开；已使用的 LHM 控制库
也不能改回 rocksdb 启动。这是首版鲜明的边界，不是恢复机制。
没有启用 LHM 编译选项时，选择 lhm 会明确报错，不自动退回 RocksDB。

MDS_DB_PATH 在 LHM 模式中仍用于节点名录、放置策略、当前句柄编号/记录等控制信息。
本步未移除 RocksDB 库，也未迁移已有文件数据及外部对象 ID 引用。

2026-10-22 同步上游 `8a1ab48` 后，还保留上游新增的完整位置记录 `DL/<inode_id>`、
`OL/<inode_id>`。这些记录保存不能由紧凑 inode 数值字段无损表达的节点/磁盘字符串和
实例代次。inode 是否存在及其属性仍通过 NamespaceStore 查询，位置查询随后读取完整
位置记录；LHM 创建也保存 DL，删除时清理 DL/OL。因此“位置查询完全不访问 RocksDB”
目前不成立，后续归档清单应复用位置解析，不能只把 InodeAttr 中的数字转成字符串。
创建时先保存完整位置再发布 namespace，发布失败尝试清理位置记录；不提供跨后端事务。

## 查询与启动隔离

MetaStoreRouter 的 unified 模式直接调用 NamespaceStore，Lookup/Getattr/Readdir 不访问
Archive/Masstree 路由。MDS 不启动旧冷索引预加载，也不恢复 Archive 路由。
LoadMasstreeOpticalFileLocation 在 LHM 模式中不回退旧目录。

RocksMetaStore 在服务启动前启用命名空间访问禁令，Get/Put/WriteBatch/条件 WriteBatch
拒绝 I/ 和 D/ 操作。直接 db() 迭代器不受该包装检查，因此未迁移的节点引用扫描被禁用，
本步不声称已经移除了所有源代码中的直接 inode 访问。第六步将替换这些高级业务入口。

## 导入

保留 ImportMasstreeNamespace / GetMasstreeImportJob RPC 作为兼容入口。在 LHM 模式中：

- 只能导入明确指定的生成模板，使用原模板格式读取器作为一次性输入。
- 必须提供 template_id，inode_start=0，template_mode 留空或 page_fast，publish_route=true。
- 为每个对象调用 ReserveCreate/Commit，由 LHM 分配真实 ID，不保留源 ID，不预分配连续区间。
- 目标根路径必须不存在且其父目录存在；保留模板的相对目录结构和属性。
- publish_route=true 表示导入后目录直接可见；不会写 AN/MTR 路由。
- 返回的 inode_min/max 仅为统计值，不代表连续区间，不能用于后续路由或遍历。
- 导入完成后源模板可离线，查询、更新、重命名均由 LHM 完成。
- 这是生成模板的元数据物化，不是已有 Rocks/冷存储文件数据迁移；不重写外部对象引用。
- 逐项发布，失败时任务为 FAILED，已成功导入的部分可能保留，不提供事务回滚。
  并发业务操作不被整个导入任务锁住；模板中的重复 inode/环路会报错。

GenerateMasstreeTemplateRequest 新增 file_count，LHM 模式要求明确指定，防止测试不慎
触发旧生成器的 1 亿文件默认规模。Rocks 模式的 file_count=0 保留旧默认行为。
测试仅使用两个文件的小型功能夹具，不是性能实验数据集。

为支持源模板目录枚举，修复旧 MasstreeMetaStore 在最后一页后继续读取 EOF 的问题；
原有分页令牌保持不变，导入器只检查令牌是否前进，不按编码字符串字典序比较。

## 暂不支持的 RPC

LHM 模式明确拒绝 ImportArchiveNamespace、ReportArchiveCandidates、ReportFileArchiveCandidates、
ClaimArchiveTask、RenewArchiveLease、CommitArchiveTask、CommitFileMigration、VerifyNodeReferences、
GetRandomMasstreeFileAttr、GetRandomMasstreeLookupPaths、GetMasstreeClusterStats、GetMasstreeNamespaceStats。
上游新增的 ModelOpticalArchive、CommitPhysicalOpticalArchive、CommitModelOpticalMigration、
CommitOpticalLibraryHandoff 也尚未适配 LHM，因此使用同样的拒绝调用规则。
现有协议没有 NOT_SUPPORTED，返回 MDS_INVALID_ARGUMENT，消息明确说明 LHM 模式不支持该入口。
文件迁移及节点引用扫描在第六步实现；旧 Masstree 专项统计不能代表 LHM 结果。

## 验收证据

结果目录：/mnt/metaiotest/filepath/results/zb_lhm_step5_20261015。

- LHM 模式 63 项实际 RPC 检查通过；L0=0 重跑同样通过。
- Rocks 模式原 28 项 RPC 回归通过。
- 在启动前植入无效的根 AN/MTR 路由；LHM 查询和导入仍通过，路由原值保持不变。
- Rocks I/D 包装访问被禁用时，基本操作、导入、离线源模板后的查询与重命名均成功。
- 关停后审计控制库，不含 I/、D/ 键。
- 进程内 MDS 服务层测试通过，不依赖 socket 权限。
- 7 项启动拒绝检查通过：未知后端、启用归档、缺少文件路径、重开控制库、改回 Rocks、
  已存在存储文件、已有旧命名空间；已有文件内容保持原样。
- Rocks/LHM 契约回归通过；原 LHM 工作树 diff 与隔离后的快照完全一致。

复现（每次使用全新结果目录）：

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
python3 /home/zwt/zyb/ZBStorage/tests/mds/namespace_rpc_test.py \
  --build "$ZB_BUILD" --backend lhm \
  --work-dir /mnt/metaiotest/filepath/results/zb_lhm_rpc_repeat
```

下一步是第六步：将文件位置迁移、节点引用检查及必要的 inode 遍历迁移到统一后端接口，
随后第七步处理内存句柄及真实文件 I/O。
