# ZB 统一 LHM 命名空间实施计划

2026-10-23：简化归档分配/上报与光盘尾批闭环已实现，见 [当前归档实现](simple-archive-callbacks.md)。
以下原始迁移阶段中的旧 Archive 入口仍不启用。

## 目标和边界

替换热 RocksDB 命名空间及 ZB 自带的 Masstree 冷命名空间。路径、目录枚举、
inode 属性、文件实际位置统一由 ZB 专用 LHM（`third_party/lhm`）管理。数据本体及节点 I/O 协议不变。
Archive 本轮不启用；不能仅凭 ENABLE_OPTICAL_ARCHIVE=false 就认定所有 archive RPC
不可达，需要在统一后端模式中显式限制未迁移的入口。不设计恢复或事务。
运行期错误处理、并发正确性、资源释放仍需保证。

控制状态逻辑独立：句柄采用内存表；节点名录及路径放置策略保持原职责，后续决定
是否去掉它们的 RocksDB 存储。本轮成功标准是命名空间不依赖 RocksDB 的 D/、I/，
不是立即消除项目中所有 RocksDB 链接。

## 分阶段实现与验收

各步完成对应检查后再进入下一步。步骤 1、2、3、4、5 已实现；第二步见 [真实 LHM 桥接](lhm-inode-bridge.md)，
第三步见 [创建预留与完整属性](lhm-creation-attributes.md)。第四步见 [命名空间适配](lhm-namespace-store.md)。第五步见 [MDS 后端模式](lhm-backend-mode.md)。步骤 6 至 8 为待实施计划。

### 1. 固定 inode ID 契约，调整类型感知的分配接口

- AllocateInodeId 增加文件/目录类型，Create 和 Mkdir 分别传递正确类型。
- RocksDB 继续使用原来的递增编号；上层只能把 ID 当不透明标识。
- 新增 LhmInodeId 编解码：文件为 block<<32|slot，目录为 bit63|head_block。
- 根目录对外保留 1；0 非法；文件槽位范围 0..62。限制逻辑块号，避免目录标记冲突
  和当前 uint32 物理块换算溢出。根目录的实际引用由未来 adapter 实例提供。
- 验收：固定编码向量、所有 63 个槽位、最大块号、非法编码、根目录和类型区分测试；
  原 namespace_store_test 与 28 项真实 RPC 回归通过。
- 限制：本步只固定编码和接口，不声称已读取 LHM inode，不分配 LHM 实际块。

### 2. 隔离 LHM 构建并提供直接 inode 访问

- 通过独立编译单元/桥接接口隔离两份 Masstree 的 threadinfo、全局符号和头文件。
  优先让统一模式不链接旧冷 Masstree 运行时；若基线与新后端共存，先验证符号隔离。
- 建立 LHM 实例生命周期和每线程上下文；明确哪些操作串行化、哪些支持并发。
- 提供按编码 ID 直接读写 inode：文件读 inode 块槽位，目录读头块的内嵌 inode，
  根 ID 1 使用实例根引用。检查类型、存在性、删除状态，禁止对非法 ID 盲读。
- 验收：真实 LHM 创建文件/目录后，路径查询与 ID 查询指向同一个 inode；根、缺失、
  删除后 ID 查询、属性写回测试；链接和线程上下文生命周期测试。
- 块/槽位在 inode 存活期间保持稳定；首版不复用会造成旧 ID 指向新对象的编号。

### 3. 完成创建预留流程和完整属性适配

- 用显式预留对象替换当前分离的 AllocateInodeId/Create：预留 inode 位置并返回 ID，
  MDS 根据 ID 选择数据位置，再发布目录项；失败时释放预留资源。预留不是事务协议。
- 明确目录预留需要的父目录上下文；避免让半完成的文件被 Lookup/Readdir 看见。
- 映射 ZB 的 uid/gid/mode、大小、时间、版本、对象单元、副本数、归档标志、位置等。
  两边 inode 都为 256B 也不能直接复制；定义有版本的字段布局，检查范围与时间单位。
- 验收：创建成功、重复路径、缺失父目录、放置失败、发布失败、取消预留；对比完整
  属性和位置；验证失败不留下可见条目和未释放预留。不涉及故障恢复。

### 4. 实现完整 LhmNamespaceStore

- 实现路径查询、按 ID 查询/更新、创建、删除和目录/文件重命名。
- readdir 首版可收集后按字节序排序，提供排他 start_after、limit、has_more，
  明确此方案的大目录代价；之后再优化迭代器，不能把全量收集宣称为高效分页。
- 将测试辅助删除提升为正式接口；明确非空目录、根删除、目标存在、移入自身子树、
  文件作为父目录、路径规范化和名字长度等错误。
- 验收：同一语义测试集分别跑 Rocks 和 LHM；目录移动后所有子路径可查、ID 与位置
  不变；分页无重复遗漏；不存在与后端错误能区分。加入并发创建的 ID 唯一性检查。

### 5. 统一路由，替换旧 Masstree 冷命名空间

- 增加明确的后端模式；LHM 模式 Lookup/Getattr/Readdir 全部进入同一个 LHM 实例，
  不再按路径前缀回退到旧 Masstree 或 Archive。
- 将原冷命名空间的导入入口适配为导入 LHM。新旧模式隔离，旧路由表不参与 LHM 查询。
- 先支持全新 LHM 存储；若需迁移已有 ZB 数据，单独定义旧 ID 到新 ID 的转换，并更新
  引用这些 ID 的对象和控制记录，不能仅复制名字。未实现旧数据迁移时明确拒绝该用法。
- 旧批量生成器可能依赖连续 ID 区间，需要改为使用 LHM 返回的实际 ID。
- 验收：两个原冷热路径下的文件均只经 LHM；故意保留冲突旧路由验证不被命中；
  禁止旧后端 namespace 读取后测试仍通过；未迁移 Archive RPC 明确返回不支持。

### 6. 清除业务模块对 inode 存储的直接访问

2026-10-22：根据简化归档方案，已先实现独立的
[分批子树扫描接口](subtree-scan.md)，包含 Rocks 通用实现和 LHM 原生增量游标。
该能力尚未接入下面的迁移/引用检查 RPC，也不代表本步骤整体完成。

- 迁移文件位置更新、CommitFileMigration、VerifyNodeReferences 和相关 inode 扫描。
  为必要的遍历/位置更新补充语义接口，不向 service 暴露 LHM 块格式。
- 节点名录、策略、PG 仍由现有模块负责；实际文件位置通过 LHM 保存。
- 验收：迁移后路径/ID 查询与节点引用计数正确；修改控制库 I/ 不影响 LHM 结果；
  审计并验证统一模式没有 D/、I/ 读写及冷后端回退。

### 7. 独立句柄表并验证真实文件 I/O

- 将 H/、next_handle 改为内存 HandleTable，支持同一 inode 多次打开，互不干扰的关闭，
  检查未知句柄；客户端缓存继续使用稳定 inode ID。
- 明确打开后删除语义：实现引用计数与最后关闭后的回收，或首版拒绝删除已打开文件，
  不能无声地让旧句柄访问已复用槽位。
- 验收：打开、写、读、大小更新、重命名后继续读写、重复打开与分别关闭、删除语义。
  先 RPC/节点 I/O，具备 FUSE 环境后增加挂载验证。服务退出后句柄丢失符合本轮边界。

### 8. 集成验收与性能验证

- 在统一模式完成新建、导入、查询、分页、重命名、删除、位置更新和真实 I/O 全流程。
- 保留 Rocks 基线模式用于对照；记录哪些控制信息仍依赖 RocksDB。
- 性能只使用工作区规定的数据集。普通实验使用固定九组 realistic_balanced_small；
  只有针对逐级 KV 反局部性问题才使用规定的 depth16/depth32 anti-locality 数据与 trace。
- 数据不可用时不临时生成替代实验数据；小型 API 测试夹具不作为性能数据集。
- 对比内存包括用户态、页缓存、dentry/inode slab 等，遵守工作区完整内存记账规则。
- 验收：先功能等价，再报告性能；不以单一 RSS 或剔除慢样本证明优势。

## 运行位置与第一步复现

所有数据库、日志和结果放在 `/mnt/metaiotest/filepath/results/`，源码树只保存代码与文档。
沿用已经配置好的外部构建目录；也可按 namespace-store.md 从头配置。

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
ZB_RUN=/mnt/metaiotest/filepath/results/zb_lhm_step1_20261014
mkdir -p "$ZB_RUN"
CCACHE_DISABLE=1 cmake --build "$ZB_BUILD" \
  --target mds_server real_node_server namespace_store_test lhm_inode_id_test -j8 \
  > "$ZB_RUN/build.log" 2>&1
"$ZB_BUILD/lhm_inode_id_test" > "$ZB_RUN/inode_id.log" 2>&1
"$ZB_BUILD/namespace_store_test" "$ZB_RUN/contract" > "$ZB_RUN/contract.log" 2>&1
python3 /home/zwt/zyb/ZBStorage/tests/mds/namespace_rpc_test.py \
  --build "$ZB_BUILD" --work-dir "$ZB_RUN/rpc"
```

contract/rpc 必须为不存在的新目录；重跑时更换 ZB_RUN。RPC 测试需要可创建 socket 的环境。
