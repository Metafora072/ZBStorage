# 分批扫描目录子树

实现入口：`NamespaceStore::BeginSubtreeScan`。当前为 MDS 内部 C++ 接口，尚未增加扫描 RPC，也未连接归档发送流程。

## 调用约定

```cpp
std::unique_ptr<NamespaceScanner> scanner;
SubtreeScanOptions options;
options.files_only = true;
options.work_budget = 1024;
auto status = store.BeginSubtreeScan("/project", options, &scanner);
if (!status.ok()) return status;
for (;;) {
    SubtreeScanBatch batch;
    status = scanner->Next(1000, &batch);
    if (!status.ok()) return status;
    // 消费 batch.entries：每项为包含 inode_id 的完整 InodeAttr。
    // 成功但 entries 为空并不代表扫描完成。
    if (batch.done) break;
}
```

- 扫描指定目录的后代，不包含根目录本身。默认只返回文件；`files_only=false` 同时返回后代目录。
- 根路径必须是绝对路径；合并重复斜线和尾斜线，拒绝 NUL、`.`、`..`。不存在返回 NOT_FOUND，文件根路径返回 INVALID_ARGUMENT。
- `Next` 的 `max_entries` 必须大于零，返回数量不超过它。返回顺序不保证字典序，也不保证两个后端的顺序相同。
- `work_budget` 必须大于零，限制每次 Next 的遍历步骤数。允许空结果且 `done=false`；最终也可能出现独立的空结束批次。EOF 后再次调用仍返回成功、空批次、`done=true`。
- 工作量单位是后端步骤，不是统一 I/O 次数或严格时间上限。LHM 单步可能读取并解码一个 16 KiB 桶，或定位一个子目录；通用实现单步可能调用一次最多 128 项的 Readdir。
- 输出批次每次重置。存储/解码失败时丢弃本次批次，扫描进入终止错误状态；此前成功交付的批次仍有效。参数错误不消耗游标。关闭通过析构完成，无持久化扫描进度。
- 同一个扫描对象必须串行调用。整个扫描期间，调用方必须保证选中子树的结构不变，包括扫描根的祖先路径不被改名/移动；实现没有主动冻结机制。文件属性按读取时获取，不提供属性快照。不同扫描对象可以独立推进。
- 通用扫描器借用 NamespaceStore，store 必须活得更久。LHM 扫描器持有共享 context，支持扫描器晚于适配器析构。

## 通用实现

`storage/NamespaceScan.cpp` 使用显式深度优先栈。每个栈帧记录目录路径、Readdir 名字游标、小页和页内位置。遇到子目录压栈，当前目录读完弹栈；文件使用 GetInode 补全属性。内存受深度和每层小页限制，不收集整个子树。

Rocks 后端使用这一实现。为保证扫描不漏报损坏条目，RocksNamespaceStore::Readdir 的损坏 dentry、缺失/损坏 inode 和迭代器错误现在都返回失败并清空结果，不再静默跳过。

通用扫描要求该 NamespaceStore 能通过 GetInode 读取其列出的所有 inode。旧版混合冷热路由的跨后端扫描不在本次覆盖范围；统一 LHM 模式下，已导入的冷热文件均由同一后端处理。

## LHM 原生实现

调用链：

```text
LhmNamespaceStore::BeginSubtreeScan / LhmScanner::Next
  → zb_lhm_scan_begin / zb_lhm_scan_next（C ABI）
  → 现有 LHM worker（每批一个任务）
  → LhmNamespace::subtree_scan_step
      → DirectoryBlockStore::scan_file_step
      → MasstreeDirectoryIndex::next_child_slot
```

所有原生改动仅在 `third_party/lhm`，原始 `MasstreeLHM-index` 不变。

1. Begin 解析起点，取得目录头和文件索引 ID；栈帧保存数值身份和续扫位置。
2. 每个目录先扫描文件桶，再进入直接子目录。文件索引的桶元数据在 split 时追加，当前实现没有桶合并/退休；按桶元数据遍历，避免多个哈希槽指向同桶造成重复。
3. 一次读取、严格解码一个桶，暂存该桶条目，跨 Next 继续消费；不重新读取已消费桶，不执行全目录排序。进入子目录前释放文件缓冲区。
4. 子目录记录上次哈希键，按 exclusive successor 续扫。查找沿 Masstree 内部节点分隔键剪枝，绝不递归进入子目录层。每次使用路径哈希重新取得当前层入口，跨调用不保存节点裸指针或 RCU 引用。此定位仍有深度成本，不宣称 O(1)。
5. 文件目录项的编码地址直接读取 inode，使用现有 codec 一次返回完整属性。没有逐文件完整路径查询，没有 RocksDB namespace 查询。
6. 每步计入预算，空桶、空目录、返回父目录也消耗步骤，预算耗尽即释放 worker。缓冲空间约为一个桶的解码条目、O(depth) 栈和路径哈希，加上本批输出；不包含已有引擎缓存。

注意：未来若 LHM 增加桶合并/退休、修改目录索引表示或要求扫描中并发修改子树，需要同时更新游标策略。扫描对象本身不是目录快照。

## 验证

新增 `subtree_scan_test <fresh-work-directory>`，目录必须位于 `/mnt/metaiotest/filepath`。

- Rocks/LHM 共享契约：429 个文件、342 个后代目录的微型功能夹具，覆盖 300 个同级目录、40 层深度、400 个宽目录文件、中文文件名、删除与未提交条目。
- 每批 1/17/1000 条、只文件/包含目录、空目录、根扫描、子树边界、规范化路径、参数错误、EOF、提前关闭和 LHM context 生命周期。
- 两个暂停游标交错使用；扫描范围外创建目录使祖先索引分裂后继续扫描。
- 原生 bridge 的测试计数比较哈希槽数、唯一桶数、实际桶读取数；不同批次大小必须读取相同数量的桶，文件集合不可重复/遗漏。
- 工作预算、注入读取失败、回调失败和 Rocks 损坏 inode 报错。测试计数与注入接口只在 BUILD_TESTING 时提供。

该测试属于正确性验证，不是吞吐量、延迟或内存对比实验。扫描 RPC、归档候选筛选、发送任务和镜像关系管理均为后续工作。
