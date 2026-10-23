# LHM over Masstree: Current Design (Concise)

## 1. 目标

LHM 的当前目标是：

- 使用 Masstree 作为**内存命名空间路由层**；
- 使用目录感知布局作为**SSD 元数据真相层**；
- 支持 `stat / ls / create / delete / rename`；
- 保留目录级高效 `rename` 的结构优势；
- 为后续与 RocksDB 映射方案（InfiniFS / SingularFS）对比提供统一实现基线。

---

## 2. 当前总体架构

### 2.1 分层

1. 内存层（LHM + Masstree）
- 路由路径：`父目录 + 最后一级组件`
- 热索引：叶子值可携带 `name + inode_ref`
- 用于快速 `stat/create/delete/rename` 判定与定位

2. 磁盘层（SSD metadata）
- `stable directory blocks`：目录项真相层（名字 + inode_ref）
- `inode/file metadata blocks`：对象元数据真相层
- `superblock/checkpoint/allocator state`：控制与预留恢复字段

### 2.2 真相与缓存边界

- 磁盘目录块与 inode 块是 **source of truth**。
- Masstree 叶子值是**可重建的加速副本**。
- 写路径遵循：优先更新真相层，再更新叶子索引（或用版本校验延迟修正）。

---

## 3. 路径与索引语义

### 3.1 路径编码

- 绝对路径按 `/` 切分为分量；
- 分量哈希为 `uint64_t`；
- 作为层级 key 在 Masstree 中路由。

### 3.2 名字归属原则（已定稿）

- **名字属于 directory entry，不属于 inode**。
- inode 只表示对象本体元数据。
- `rename` 主要修改目录映射，不修改 inode 本体。

---

## 4. 元数据布局（定稿）

文件：`metadata_layout.hh`

### 4.1 固定参数

- `metadata block size = 16KB`
- `directory buffer size = 16KB`
- `inode size = 256B`

### 4.2 已定结构

- `metadata_common_block_header`：64B
- `superblock_payload`：64B
- `checkpoint_payload`：64B
- `allocator_state_payload`：64B
- `inode_disk`：256B
- `inode_block_header`：192B
- `directory_entry_disk_header`：24B（后接变长名字）
- `directory_block_header`：64B

### 4.3 inode block 组织

- `64B common header`
- `192B inode_block_header`
- `63 * 256B inode slots`
- 总计 16KB

### 4.4 directory block 组织

- `64B common header`
- `64B directory_block_header`
- `16256B payload`（顺序存放变长目录项）

---

## 5. 当前持久化实现状态（已落地）

- `persistent_store` 已接入，后端文件：`/mnt/batchtest/lhm/lhm_namespace.meta`；
- 采用 `O_DIRECT` + 16KB 固定块 I/O；
- 已实现：`superblock`、inode block 读写、directory block 读写、顺序块分配；
- 已实现目录块链基础接口：
  - `CreateDirectoryChain(dir_inode_id)`：创建目录链头空块；
  - `AppendDirectoryChainBlock(tail_ref, dir_inode_id)`：为目录链追加新块并维护 `prev/next`。
- 根目录 inode 已持久化并在内存中缓存 `root_persistent_ref`。

### 5.1 inode 分配策略（最新）

- 已从“线性游标 + 每次块级读改写”切换为：
  - `global atomic ticket` 发号；
  - `TLS chunk` 本地缓存（每线程一次领取 256 个 ticket）；
  - `inode_alloc_epoch` 生命周期代次校验（防止线程误用旧缓存）。
- ticket 到槽位映射：
  - `block_id = 1 + ticket / kInodesPerBlock`
  - `slot = ticket % kInodesPerBlock`
- 新 inode block 初始化策略：
  - 仅在线程 refill chunk 时检查并按需初始化；
  - 初始化路径受互斥保护，避免并发重复建块；
  - 常规分配路径不再做每次 inode block 的 RMW。
- `OpenOrCreate` 启动阶段仍保留一次线性扫描，用于恢复 `next_inode_ticket` 游标。

### 5.2 `L0 + L1` 缓存模型（本轮新增）

当前持久化写路径采用两级缓存：

1. `L0`（线程局部缓冲，cold-start 优先）
- 每线程一个活动 inode block 缓冲（16KB）；
- 作用：将同线程连续 inode 写（create/delete）先聚合在 L0，减少每次操作触发 L1 锁路径；
- 字段：`has_active_block / active_dirty / active_block_id / pending_ops / active_image`；
- flush 触发：
  - 活动块切换；
  - `pending_ops >= 64`；
  - `Sync/Close` 全局强制 flush。
- 运行开关（新增）：
  - `LhmNamespace::set_l0_cache_enabled(bool)`；
  - 环境变量 `LHM_ENABLE_L0_CACHE=0/1`（初始化时读取）。

2. `L1`（共享固定大小 cache，steady-state 优先）
- `PersistentStore` 内 **sharded inode block cache**：
  - `kInodeCacheShardCount = 8`
  - `kInodeCacheEntriesPerShard = 64`
  - 总缓存槽位 `8 * 64 = 512` 个 inode block（每个 16KB）
- 每个 shard 一把锁（`shard.mu`）；
- 每个 shard 维护 `dirty_bitmap`；
- slot 字段：`valid / dirty / block_id / image`。

### 5.3 刷新与持久化边界（更新）

- `WriteInode` 默认先写 `L0`，再由 `L0` 下发到 `L1`；
- `L1` 脏块不立即落盘，按位图延迟刷盘；
- `Sync()` 顺序：
  1. flush 所有 `L0` 线程缓冲到 `L1`；
  2. flush `L1` 脏 inode block 到 SSD；
  3. `fsync(fd)`。
- `Close()` 顺序与 `Sync()` 一致，随后 reset 缓冲状态。

### 5.4 `ReadInode` / `WriteInode` 路径（更新）

`ReadInode(ref)`：

1. 先查当前线程 `L0`（若 `active_block_id == ref.block_id`，直接返回）；
2. 否则查 `L1` shard/slot 命中；
3. 再不命中则 direct read SSD。

`WriteInode(ref, inode)`：

1. 获取当前线程 `L0` 缓冲；
2. 若目标 `block_id` 与活动块不同，先 flush 当前 `L0` 活动块到 `L1`，再加载新块；
3. 在 `L0` 内更新 inode slot + `slot_bitmap/used_count`；
4. 置 `active_dirty=true`，累计 `pending_ops`；
5. 触发阈值时将 `L0` 活动块下发到 `L1`（不直接下盘）。

若关闭 `L0`（`set_l0_cache_enabled(false)`）：

1. `WriteInode` 直接走 `L1` shard 缓存路径；
2. `Sync/Close` 不再执行 `L0` flush，仅刷 `L1`；
3. 用于“常规稳态测试只测 L1”场景。

---

## 6. 当前接口流程（按已实现代码）

### 6.1 `stat(path)` / `lookup(path)`

1. 解析路径；
2. 只定位一次父目录 `root`（`locate_directory_root(parent)`）；
3. 只查一次最后一级槽位（`lookup_child_from_parent_root`）；
4. 按 `name_length + memcmp` 做名字确认；
5. 返回 `namespace_entry`（文件或目录）。

要点：已去掉旧路径中的“整路径按目录判定 + 再查 value”的双遍历。

### 6.2 `ls(path)`

1. 解析路径；
2. 对非根目录仅执行一次 `locate_directory_root` 并校验目录元数据名字；
3. 从该层 root 左到右扫描当前层目录项；
4. 命中目录边只恢复目录项，不下钻子目录。

要点：当前 `ls` 只扫当前层，不做全表扫描。

### 6.3 `create(path)` / `mkdir(path)`

1. 解析路径并一次性解析父目录上下文：
  - `parent_root`
  - `parent_ref`
2. 分配 inode slot（`global atomic ticket + TLS chunk(256)`）；
3. 在 `parent_root` 上单次游标创建：
  - 文件：`find_insert` 后插入（不再维护冲突链）；
  - 目录：`create_layer_with_meta`；
4. 持久化 inode（`persist_create_success`），不再重复查父 inode。
5. 目录创建时会先创建目录块链头，并将目录 inode 的 `primary_ref/aux_ref` 指向该链头块。

### 6.4 `delete(path)` / `rmdir(path)`

1. 解析路径；
2. 定位父目录 root；
3. 单次游标删除最后一级槽位（目录会做非空检查）；
4. 返回被删 entry；
5. inode 持久化 tombstone 更新（`persist_delete_success`）。

补充：测试接口 `remove_path_for_test` 已改为“直接删 + 直接持久化”，不再先 `lookup` 再删。

### 6.5 `rename(old, new)`

- 文件：创建新路径项 + 删除旧路径项；持久化时更新目标父 inode。
- 目录：沿用 O(1) layer attach/detach 路线（不重写整棵子树）；目录元数据名在新位置更新。

### 6.6 哈希碰撞策略（当前）

- 当前采用“忽略哈希碰撞”模式：同父目录下同 `component_hash` 只保留单槽位。
- 已移除 `conflict_bucket` 及 `entry_kind::conflict_chain` 路线。
- 语义影响：极低概率哈希碰撞时，无法同时保留两个同 hash 不同名条目（行为由现有 create 路径决定）。

---

## 7. 一致性与恢复边界

- 当前以“结构可持久化 + 路径性能收紧”为主；
- 已有恢复相关字段预留（`seq/version/checksum/checkpoint refs`）；
- 完整崩溃恢复流程（replay/rebuild/orphan cleanup）尚未实现。
- `L0` 适用前提：
  - 冷启动阶段由上层负载分配保证“同目录主要由同线程写”；
  - 若出现跨线程同目录并发写，仍需依赖上层路由/语义约束，`L0` 本身不提供跨线程目录序列化。
- 若线程在 `Sync/Close` 前退出：
  - `L0` 缓冲通过全局注册表在 `Sync/Close` 统一 flush；
  - 但若进程崩溃，仍以已下发到 `L1/SSD` 的数据为准。

---

## 8. 本轮性能相关改动摘要

- inode 分配：全表扫描 -> 线性顺序分配；
- inode 分配并发：线性顺序游标 -> `global atomic + TLS chunk(256)`；
- `stat`：双遍历 -> 父目录一次定位 + 一次孩子槽位查询；
- `create/mkdir`：去掉重复父目录查找与重复父 inode 查找；
- 删除冲突桶路径：不再维护 `conflict_bucket/conflict_chain`；
- `create` 持久化：去掉创建后额外 inode 读取；
- `ls`：保持当前层扫描，不下钻；
- `delete` 测试路径：去掉预先 `lookup`。

### 8.1 并发测试快照（iwide, release, create+delete）

- 参数：`threads=1,2,4,8,16,32`，`ops_per_thread=5000`
- 改造前吞吐（ops/s）：
  - `233883, 474742, 864157, 1.649e6, 1.976e6, 2.642e6`
- 改造后吞吐（ops/s）：
  - `1.144e6, 2.061e6, 3.278e6, 4.171e6, 3.661e6, 5.163e6`
- 32 线程点位提升：
  - `2.642e6 -> 5.163e6`（约 `+95.4%`）
- 结论：
  - 分配器竞争显著缓解；
  - 当前高并发写路径的下一瓶颈转移到 `WriteInode` 的块级 RMW。

---

## 9. 下一步

1. 完成目录块落盘与内存命名空间的一致化规则（含刷盘策略）。
2. 增加恢复流程（checkpoint + replay）。
3. 在 SSD 持久化版本上对比 InfiniFS/SingularFS（RocksDB 映射）口径。

---

## 10. 对比实验口径（后续）

与 RocksDB 映射方案对比时，统一关注：

- `stat / ls / create / delete / rename` 延迟
- 写放大
- 内存放大
- 恢复时间（恢复实现后）

当前文档仅保留现行设计，不再记录中间迭代过程。

---

## 11. 下一阶段重构计划：Masstree 只保留目录路由，文件 entry 下沉到 directory block

### 11.1 背景与问题

当前实现中，目录和普通文件 entry 都存在 Masstree 内存索引里：

- 目录表现为 Masstree layer edge，目录 root 尾随 `directory_meta`；
- 文件表现为父目录 layer 中的普通 leaf value，value 为 `namespace_entry`；
- `lookup/stat` 先定位父目录 root，再在父目录 root 内查最后一级槽位；
- `readdir` 扫描当前目录 layer；
- 目录 `rename` 通过 layer attach/detach 保持 O(1) 子树移动。

这个结构验证了 LHM 的核心路由和目录 rename 机制，但它的内存模型仍然接近“全量 namespace 常驻 DRAM”。实验中约 `500K entries -> 500MB` 的量级说明，如果每个文件 entry 都必须常驻 Masstree，那么十亿、百亿、千亿级 namespace 会直接被 DRAM 容量限制。下一阶段重构的目标是把 LHM 从全量内存索引改成 bounded-memory 的目录路由 + directory block 模型。

### 11.2 新目标结构

新的核心原则：

> Masstree 只保存目录路由结构，不再保存普通文件 entry。

具体含义：

1. Masstree 中仍保留目录 layer edge。
2. 每个目录 root 仍携带 `directory_meta`，但 `directory_meta` 需要指向该目录的 directory block chain。
3. 当前目录下的普通文件 entry 存放在该目录对应的 directory block 中。
4. 当前目录下的子目录 entry 需要同时具备两种身份：
   - Masstree 中的 layer edge，用于路径路由和目录 rename；
   - directory block 中的一条 directory entry，用于 `readdir`、恢复和持久化真相。
5. Masstree 变成可重建的目录路由缓存；directory block/inode block 才是完整 namespace source of truth。

目标内存变化：

- 原先：目录 entry + 文件 entry 都消耗 Masstree node/value 内存。
- 重构后：Masstree 只随目录数量增长，普通文件数量主要体现在 SSD directory block 和可控缓存中。
- 对大规模 namespace，内存上限应由“目录路由规模 + directory block cache 大小”决定，而不是由总 entry 数直接决定。

### 11.3 新 lookup 路径

`lookup("/a/b/f")` 的目标流程：

1. 解析路径并计算 component hash。
2. 在 Masstree 中只路由到父目录 `/a/b` 的目录 root。
3. 从父目录 root 的 `directory_meta` 取到 directory block 引用。
4. 通过 directory block cache 读取父目录 block。
5. 在该 directory block 中按 `component_hash + name` 查找最后一级 `f`。
6. 找到文件 entry 后返回其 inode 引用；如果需要完整 inode 属性，再读取 inode block。

目录 lookup 也应走同一语义：

1. 先通过父目录 block 找到子目录 entry，确认名字、类型和 inode ref；
2. 再通过 Masstree layer edge 找到子目录 route root；
3. 两者必须一致，否则说明内存 route cache 与持久化目录真相层发生偏离，需要恢复或修正。

### 11.4 directory entry 是否直接存 inode 的讨论

有两个设计选项。

#### 方案 A：directory entry 只存 `inode_ref`

格式类似：

```text
directory_entry = {
  component_hash,
  name,
  kind,
  inode_ref
}
```

lookup 文件时：

1. 读父目录 block；
2. 找到 entry；
3. 返回 `inode_ref`；
4. 如果调用方需要 inode 属性，再读 inode block。

优点：

- directory entry 小，目录 block 容量高；
- inode 是单一真相，rename 只改目录映射和父 inode 字段；
- hard link、属性更新、tombstone、generation 更容易集中处理；
- 与当前 `inode_disk` / `inode_ref` 设计一致。

缺点：

- `stat` 如果必须返回完整 inode 属性，会多一次 inode block 读；
- cold lookup 路径可能变成 directory block IO + inode block IO。

#### 方案 B：directory entry 内联部分 inode hot fields

格式类似：

```text
directory_entry = {
  component_hash,
  name,
  kind,
  inode_ref,
  cached_mode,
  cached_size,
  cached_mtime,
  cached_generation
}
```

lookup 文件时：

1. 读父目录 block；
2. 找到 entry；
3. 对普通 `stat` 可直接返回 hot attributes；
4. 需要强一致完整 inode 时再读 inode block。

优点：

- hot `stat` 可减少一次 inode block IO；
- 对 metadata-only workload 延迟更好；
- directory block cache 命中时可直接回答常见查询。

缺点：

- directory entry 变大，目录 block 可容纳 entry 数下降；
- inode 字段在 directory entry 和 inode block 中产生重复，需要一致性协议；
- 属性更新可能要同时更新 inode block 和父目录 block；
- hard link 或多目录引用场景会更复杂。

当前倾向：

- 第一阶段采用方案 A：directory entry 只存 `inode_ref` 和最小名字/类型信息。
- 保留格式扩展位，后续可选择性增加 hot inode fields 或 per-directory stat cache。
- 这样能先把“文件 entry 不常驻 Masstree”这个关键内存问题解决，同时避免过早引入双写一致性复杂度。

### 11.5 对 create/delete/readdir/rename 的影响

`create_file(path)`：

1. Masstree 定位父目录 root；
2. 读/锁定父目录 directory block；
3. 检查 `component_hash + name` 是否存在；
4. 分配 inode；
5. 将文件 entry 插入 directory block；
6. 写 inode block；
7. 不再向 Masstree 插入文件 value。

`mkdir(path)`：

1. 定位父目录 root；
2. 在父目录 directory block 插入子目录 entry；
3. 分配并初始化子目录 inode；
4. 创建子目录 directory block chain；
5. 在 Masstree 中创建子目录 layer edge，并让新目录 root 的 `directory_meta` 指向子目录 directory block。

`delete_file(path)`：

1. 定位父目录 root；
2. 从父目录 directory block 删除文件 entry 或写 tombstone；
3. 更新 inode tombstone；
4. Masstree 不参与文件删除。

`rmdir(path)`：

1. 定位父目录 root 和子目录 root；
2. 检查子目录 directory block 是否为空；
3. 从父目录 directory block 删除子目录 entry；
4. 从 Masstree 移除子目录 layer edge；
5. 更新子目录 inode tombstone。

`readdir(path)`：

1. 定位目录 root；
2. 读该目录 directory block chain；
3. 返回 block 中的直接孩子 entries；
4. 不再扫描 Masstree leaf values 作为 readdir 主路径。

目录 `rename(old, new)`：

1. 定位 old parent root 和 new parent root；
2. 从 old parent directory block 移除旧目录 entry；
3. 向 new parent directory block 插入新目录 entry；
4. Masstree 执行 layer detach/attach；
5. 更新被移动目录 root 的 `directory_meta` 中的名字/hash/parent 信息；
6. 更新被移动目录 inode 的 parent inode id；
7. 子树后代 directory blocks 和 inode 不需要重写。

### 11.6 缓存与内存边界

重构后需要至少两个缓存层：

1. 目录路由缓存：Masstree。
   - 只保存目录 root/layer edge；
   - 大小主要随目录数量增长；
   - 未来可进一步支持 cold directory route eviction。

2. directory block cache。
   - 缓存活跃目录的 16KB directory block；
   - 可采用固定容量 LRU/clock/sharded cache；
   - 脏 block 延迟刷盘；
   - 内存上限由配置控制。

可选第三层：

3. inode block cache。
   - 当前 `PersistentStore` 已有 L0/L1 inode block cache；
   - 后续应和 directory block cache 统一统计与配置。

新的内存 claim 应从“所有 entry 常驻内存”改成：

> LHM keeps only directory routing state and bounded metadata block caches in DRAM; file entries reside in directory blocks and are fetched through the directory block cache.

### 11.7 一致性与恢复要求

重构后，directory block 必须成为恢复的核心真相层：

1. checkpoint 记录 root directory inode/ref 和 allocator 状态；
2. 恢复时从 root directory block 开始扫描目录树；
3. 对每个子目录 entry 重建 Masstree layer edge；
4. 文件 entry 不重建到 Masstree，只保留在 directory block 中；
5. inode block 用于验证 inode generation、tombstone 和 parent inode id；
6. 若 Masstree route cache 丢失，可以从 directory block 全量或按需重建。

这要求 directory block 写入具有明确事务边界。至少需要保证：

- create file：directory entry 与 inode 初始化不会产生不可达 live inode；
- mkdir：父目录 entry、子目录 inode、子目录 directory block、Masstree route root 的顺序可恢复；
- rename dir：old parent block、new parent block、moved dir inode/meta 的更新具备原子语义或可恢复中间态；
- delete/rmdir：entry 删除与 inode tombstone 的顺序可通过 tombstone/generation 修复。

### 11.8 代码重构边界

为了支持该结构，`LhmNamespace` 需要从当前单体类拆分为 facade：

```text
LhmNamespace               对外兼容接口
  PathCodec                路径解析、hash、parent path
  MasstreeDirectoryIndex   只管理目录 layer/root/attach/detach
  DirectoryBlockStore      管理 directory entry 的查找、插入、删除、扫描
  InodeManager             管理 inode 分配、读写、tombstone、parent 更新
  NamespaceOperations      编排 create/delete/readdir/rename 的语义和写入顺序
```

对外接口原则：

- 保持 `lookup_entry`、`lookup_file`、`lookup_directory`、`creat_file`、`mkdir`、`rename_path`、`readdir` 等接口不变；
- 先通过 facade 兼容现有 evaluation adapter；
- 第一阶段只改变内部结构，不改变 benchmark 调用方式。

### 11.9 第一阶段实施建议

第一阶段目标不是一次性完成 crash consistency，而是先改变内存结构：

1. 抽出 `MasstreeDirectoryIndex`，让它只支持目录 route 操作。
2. 新增 `DirectoryBlockStore` 的内存/持久化双模式接口。
3. 修改 file create/lookup/delete/readdir，使文件 entry 不再进入 Masstree。
4. 保留目录 layer edge 的 O(1) rename 路径。
5. 保留 `LhmNamespace` 对外 API。
6. 跑现有 smoke、path-depth、rename-subtree、footprint 实验，确认内存曲线从“总 entry 数驱动”转向“目录数 + cache size 驱动”。

第一阶段可以暂不解决：

- 完整 crash recovery；
- hard link；
- directory block compaction；
- 多 block 大目录的复杂 split/merge；
- hot inode fields 内联；
- route cache eviction。

这些应在文件 entry 下沉和 directory block lookup 路径稳定后再做。

---

## 12. 2026-05-28 重构进度：路径语义工具抽取

### 12.1 本步目标

本步是 directory-block 重构前的低风险准备，不改变现有对外接口和行为，只把 `LhmNamespace` 内部的路径语义工具先抽出：

- 父路径计算；
- root/child 判断；
- `ParsedPath` 的父目录构造；
- prefix/subtree 关系判断。

新增文件：

```text
MasstreeLHM/namespace_path.hh
```

### 12.2 当前改动

`namespace_path.hh` 新增 `NamespacePath` 工具命名空间：

```text
NamespacePath::is_root(...)
NamespacePath::has_child_component(...)
NamespacePath::parent_path(...)
NamespacePath::make_parent_parsed(...)
NamespacePath::is_prefix_path(...)
```

`LhmNamespace` 已 include `namespace_path.hh`，并将原私有 `parent_path`、`is_prefix_path` 改为兼容包装：

```text
LhmNamespace::parent_path(...) -> NamespacePath::parent_path(...)
LhmNamespace::is_prefix_path(...) -> NamespacePath::is_prefix_path(...)
```

这样现有调用点暂时不需要大规模替换，外部 API 和内部行为保持不变。

### 12.3 设计意义

这一步的目的不是性能优化，而是降低后续拆分风险。后续拆出 `MasstreeDirectoryIndex`、`DirectoryBlockStore`、`NamespaceOperations` 时，这些模块都会需要统一的路径语义。如果父路径、前缀判断和父目录 ParsedPath 构造继续散落在 `LhmNamespace` 里，后续很容易出现 lookup、rename、delete 使用不同路径规则的问题。

### 12.4 下一步

下一步建议抽出 `MasstreeDirectoryIndex` 的最小骨架，先只搬迁目录路由相关能力：

- locate directory root；
- lookup child slot；
- create directory layer；
- attach/detach existing directory layer；
- scan direct directory children。

第一阶段仍保持文件 entry 存在 Masstree 中，只做结构拆分；待目录索引边界稳定后，再把普通文件 entry 下沉到 directory block。

---

## 13. 2026-05-28 重构进度：namespace 类型抽离

### 13.1 本步目标

本步继续为 `MasstreeDirectoryIndex` 拆分做准备：先把 `LhmNamespace` 顶部的共享命名空间类型抽到独立头文件，使后续目录索引模块可以复用这些类型，而不需要 include 整个 `lhm_namespace.hh`。

新增文件：

```text
MasstreeLHM/namespace_types.hh
```

### 13.2 抽离内容

从 `lhm_namespace.hh` 移入 `namespace_types.hh` 的内容包括：

- `entry_kind`
- `namespace_entry`
- `readdir_record`
- `subtree_record`
- `directory_root_debug_info`
- `entry_name(...)`
- `entry_name_view(...)`
- `entry_name_fits(...)`
- `make_namespace_entry(...)`
- `entry_is_valid(...)`
- `entry_is_directory(...)`
- `entry_is_file(...)`
- `entry_name_equals(...)`
- `entry_kind_name(...)`
- `namespace_table_params`

`lhm_namespace.hh` 现在 include `namespace_types.hh`，对外接口和运行行为不变。

### 13.3 设计意义

这一步把“命名空间记录格式”和“命名空间操作编排”分开。后续 `MasstreeDirectoryIndex` 应该只依赖：

```text
namespace_types.hh
namespace_path.hh
directory_meta.hh
Masstree cursor/table headers
```

而不应该依赖整个 `LhmNamespace`。这能避免新的目录索引模块和 facade 之间形成循环依赖。

### 13.4 验证

已通过 Release 编译检查：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

### 13.5 下一步

下一步可以正式创建 `MasstreeDirectoryIndex`，先迁移不涉及持久化的目录路由 primitives：

- `single_component_key`
- `canonicalize_directory_root`
- `locate_directory_root`
- `lookup_child_slot_from_parent_root`
- `lookup_child_from_parent_root`
- `lookup_directory_edge_from_parent_root`

迁移时仍保持 `LhmNamespace` facade 对外 API 不变。

---

## 14. 2026-05-28 重构进度：引入 MasstreeDirectoryIndex ownership

### 14.1 本步目标

本步正式创建目录路由索引类，但只迁移 Masstree table 的 ownership，不改变 namespace 行为：

```text
MasstreeLHM/masstree_directory_index.hh
```

新增类：

```text
MasstreeDirectoryIndex
```

### 14.2 当前职责

当前 `MasstreeDirectoryIndex` 只负责：

- 持有 `Masstree::basic_table<namespace_table_params>`；
- 管理 `initialize(ti)` / `destroy(ti)`；
- 暴露 `root()`；
- 暴露底层 `table()` 给旧代码继续使用；
- 统一导出 `table_type`、`node_type`、`cursor_type`、`unlocked_cursor_type`、`value_type`。

`LhmNamespace` 中原来的裸 `table_` 成员已替换为：

```text
MasstreeDirectoryIndex directory_index_;
```

现有代码中仍有不少目录路由操作留在 `LhmNamespace` 内部，例如 `locate_directory_root`、`lookup_child_from_parent_root`、`rename_directory_o1_*` 等。本步只是先把 table lifetime 边界切出来，为后续迁移这些 primitives 做准备。

### 14.3 行为边界

本步不改变：

- 文件 entry 仍在 Masstree leaf value 中；
- 目录仍是 Masstree layer edge；
- `lookup/create/delete/readdir/rename` 对外行为；
- `LhmNamespace` 对外 API；
- evaluation adapter 调用方式。

### 14.4 验证

已通过 Release 编译检查：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

### 14.5 下一步

下一步应开始从 `LhmNamespace` 迁移只依赖 Masstree table/root 的目录路由 primitives。建议顺序：

1. `single_component_key`；
2. `canonicalize_directory_root`；
3. `locate_directory_root`；
4. `lookup_child_slot_from_parent_root`；
5. `lookup_child_from_parent_root`；
6. `lookup_directory_edge_from_parent_root`。

迁移后，`LhmNamespace` 应只编排语义流程，不直接操作 Masstree cursor。

---

## 15. 2026-05-28 重构进度：迁移无状态目录路由 primitive

### 15.1 本步目标

本步将最底层、无状态的目录路由辅助逻辑从 `LhmNamespace` 移入 `MasstreeDirectoryIndex`：

- 单组件 key 编码；
- directory root canonicalization。

这些逻辑只依赖 Masstree node/root 语义，不依赖持久化、不依赖 namespace 操作编排，因此适合优先迁移。

### 15.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
MasstreeDirectoryIndex::single_component_key
MasstreeDirectoryIndex::make_single_component_key(...)
MasstreeDirectoryIndex::canonicalize_directory_root(node_type*)
MasstreeDirectoryIndex::canonicalize_directory_root(const node_type*)
```

`LhmNamespace` 中保留同名兼容包装：

```text
LhmNamespace::make_single_component_key(...)
LhmNamespace::canonicalize_directory_root(...)
```

包装函数内部直接转发到 `MasstreeDirectoryIndex`。这样现有调用点暂时不需要一次性替换，行为保持不变。

### 15.3 验证

已通过 Release 编译检查：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

### 15.4 下一步

下一步迁移第一个真正使用 Masstree table/root 的目录路由操作：

```text
locate_directory_root(parsed, out, ti)
```

建议先在 `MasstreeDirectoryIndex` 中实现：

```text
bool locate_directory_root(const ParsedPath& parsed, const node_type*& out, threadinfo& ti) const;
```

然后让 `LhmNamespace::locate_directory_root` 保留为兼容包装并转发。这样可以继续保持对外行为不变，同时逐步减少 `LhmNamespace` 直接操作 Masstree cursor 的范围。

---

## 16. 2026-05-28 重构进度：迁移 locate_directory_root

### 16.1 本步目标

本步迁移第一个实际使用 Masstree cursor 的目录路由操作：

```text
locate_directory_root(parsed, out, ti)
```

该函数负责从全局 root 出发，按 `ParsedPath.hashes` 逐级沿 layer edge 定位目标目录 root。

### 16.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
bool locate_directory_root(const ParsedPath& parsed, const node_type*& out, threadinfo& ti) const;
```

内部逻辑：

1. 从 `root()` 开始；
2. root 路径直接返回；
3. 对每个 component hash 构造 `single_component_key`；
4. 使用 `unlocked_cursor_type::find_unlocked_edge` 查找 layer edge；
5. 命中 layer 后通过 `canonicalize_directory_root` 得到目录 root；
6. 任一级不是 layer edge 则返回失败。

`LhmNamespace::locate_directory_root` 保留为兼容包装，内部转发到：

```text
directory_index_.locate_directory_root(parsed, out, ti)
```

### 16.3 行为边界

本步不改变 lookup/create/delete/readdir/rename 行为，只改变代码归属：

- Masstree cursor 路由逻辑开始进入 `MasstreeDirectoryIndex`；
- `LhmNamespace` 继续负责操作语义编排；
- 对外 API 不变。

### 16.4 验证

已通过 Release 编译检查：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

### 16.5 下一步

下一步迁移 child slot 查询：

```text
lookup_child_slot_from_parent_root(parent_root, child_hash, out, ti)
lookup_child_from_parent_root(parent_root, child_name, child_hash, out, ti)
lookup_directory_edge_from_parent_root(parent_root, child_name, child_hash, out, ti)
```

这会进一步把“父目录 root 内的孩子查找”从 `LhmNamespace` 中移到 `MasstreeDirectoryIndex`。

---

## 17. 2026-05-28 重构进度：迁移 child slot lookup

### 17.1 本步目标

本步将父目录 root 内的 child 查询逻辑迁移到 `MasstreeDirectoryIndex`。这是 lookup、create、delete、rename 都会复用的局部目录路由能力。

### 17.2 当前改动

`MasstreeDirectoryIndex` 新增查询结果结构：

```text
MasstreeDirectoryIndex::child_slot_lookup
MasstreeDirectoryIndex::directory_edge_lookup
```

并新增三个查询函数：

```text
lookup_child_slot_from_parent_root(parent_root, child_hash, out, ti)
lookup_child_from_parent_root(parent_root, child_name, child_hash, out, ti)
lookup_directory_edge_from_parent_root(parent_root, child_name, child_hash, out, ti)
```

语义保持原样：

- child hash 命中 layer edge 时，返回目录 root；
- child hash 命中普通 value 时，返回文件 entry；
- 目录查找必须用 `directory_meta` 中的真实名字再次确认；
- 文件查找必须用 `namespace_entry.name` 再次确认；
- 哈希碰撞策略仍然沿用当前“单槽位 + 名字确认失败即未命中”的行为。

`LhmNamespace` 中保留同名兼容包装，并将内部实现转发到 `directory_index_`。

### 17.3 行为边界

本步不改变：

- 文件 entry 仍位于 Masstree leaf value；
- 目录 entry 仍位于 Masstree layer edge + directory_meta；
- `lookup_entry` 的外部结果；
- `rename_directory_o1_*` 的行为。

### 17.4 验证

已通过 Release 编译检查：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

### 17.5 下一步

下一步建议迁移目录结构修改 primitives：

```text
create_directory_on_parent_root(...)
rename_directory_o1_fast_from_parsed(...) 中的 attach/detach 底层片段
rename_directory_o1_from_parsed(...) 中的 attach/detach 底层片段
```

更稳妥的顺序是先抽：

```text
create_directory_layer(parent_root, child_hash, meta, out_root, ti)
attach_existing_directory_layer(parent_root, child_hash, child_root, ti)
remove_directory_layer(parent_root, child_hash, expected_child_root, ti)
```

然后让 `LhmNamespace` 继续负责 rename 语义检查和持久化更新。

---

## 18. 2026-05-28 重构进度：迁移目录 layer 修改 primitive

### 18.1 本步目标

本步将目录结构修改的底层 Masstree cursor 操作迁移到 `MasstreeDirectoryIndex`，让 `LhmNamespace` 继续负责语义检查、路径解析和持久化编排。

### 18.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
create_directory_layer(parent_root, child_hash, meta, out_root, ti)
attach_existing_directory_layer(parent_root, child_hash, child_root, ti)
remove_directory_layer(parent_root, child_hash, expected_child_root, ti)
```

对应职责：

- `create_directory_layer`：在父目录 root 下创建新的目录 layer，并写入 `directory_meta`；
- `attach_existing_directory_layer`：将已有目录 root 挂到新的父目录 edge 下；
- `remove_directory_layer`：在删除前确认当前 edge 指向 expected child root，然后移除 layer edge。

`LhmNamespace` 中以下路径已改为调用 `directory_index_`：

- `create_directory_on_parent_root(...)`
- `create_directory_fast_from_parsed(...)`
- `create_directory_from_parsed(...)`
- `rename_directory_o1_fast_from_parsed(...)` 中的 attach/detach；
- `rename_directory_o1_from_parsed(...)` 中的 attach/detach。

### 18.3 行为边界

本步不改变目录 rename 的语义：

- rename 前仍由 `LhmNamespace` 检查 root 路径、prefix/self rename、目标存在性等；
- rename 时仍更新被移动目录 root 上的 `directory_meta`；
- rename 成功后仍由 `persist_rename_success` 更新 inode parent 信息；
- 子树后代仍不重写。

### 18.4 验证

Release 编译通过：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

额外执行了一个小型目录 rename trace：

```text
python3 evaluation/platform/common/generate_trace.py \
  --kind micro_rename_scalability \
  --namespace evaluation/platform/datasets/smoke.tsv \
  --output /tmp/lhm_refactor_rename_20260528_1.tsv \
  --max-scan 1000 \
  --subtree-sizes 1,10 \
  --repeat 5 \
  --warmup-repeat 1 \
  --seed 2026052802

python3 evaluation/platform/runners/run_trace.py \
  --system lhm \
  --namespace evaluation/platform/datasets/smoke.tsv \
  --trace /tmp/lhm_refactor_rename_20260528_1.tsv \
  --result-dir /tmp/lhm_refactor_rename_20260528_1 \
  --max-entries 1000 \
  --lhm-persistence-dir /tmp/lhm_refactor_rename_persist_20260528_1 \
  --trace-replay-bin build_release/trace_replay

python3 evaluation/platform/common/check_run_sanity.py \
  /tmp/lhm_refactor_rename_20260528_1/lhm
```

结果：

```text
sanity_ok systems=1
```

另跑过一个 80-op `macro_mixed` trace，`trace_replay` exit code 为 0，stat/create/delete/rename 全成功；但 sanity 对 readdir 要求 100% 成功时失败，原因是该 mixed trace 中存在 rename 后继续 readdir 旧目录路径的情况，不能作为本轮结构迁移的严格 100% 成功 smoke。

### 18.5 下一步

下一步可以继续迁移剩余直接操作 cursor 的路径：

- 文件 value 插入：`create_file_on_parent_root` / `create_entry_from_parsed` 中的 file slot insert；
- child 删除：`force_remove_child_slot_fast`、`remove_entry_fast_from_parsed`、`remove_entry_from_parsed`；
- readdir 扫描：`append_directory_children_from_node`。

在文件 entry 下沉到 directory block 前，建议先完成这些 Masstree 操作边界拆分。

---

## 19. 2026-05-28 重构进度：迁移文件 value 插入 primitive

### 19.1 本步目标

本步将普通文件 entry 的 Masstree leaf value 插入逻辑迁移到 `MasstreeDirectoryIndex`。这仍然是过渡阶段：文件 entry 目前还在 Masstree 中，尚未下沉到 directory block；本步只是继续缩小 `LhmNamespace` 直接操作 cursor 的范围。

### 19.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
insert_file_value(parent_root, child_hash, entry, overwrite_existing_value, ti)
```

语义：

- `overwrite_existing_value=true`：沿用 fast mode 行为，已存在普通 value 时覆盖；
- `overwrite_existing_value=false`：沿用 non-fast 行为，已存在普通 value 时失败；
- 如果目标 slot 是 directory layer，则始终失败；
- 新 slot 插入时执行原有 `find_insert -> value -> fence -> finish(1)` 路径。

`LhmNamespace` 中以下路径已改为调用该 primitive：

- `create_file_on_parent_root(...)`
- `create_entry_fast_from_parsed(...)`
- `create_entry_from_parsed(...)`

### 19.3 行为边界

本步不改变：

- fast mode 的覆盖语义；
- non-fast mode 的存在性失败语义；
- 目录 layer 与文件 value 的区分；
- create 后的 inode 持久化流程。

### 19.4 验证

Release 编译通过：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

执行 80-op `macro_mixed` trace：

```text
trace=/tmp/lhm_refactor_create_20260528_1.tsv
result=/tmp/lhm_refactor_create_20260528_1/lhm
```

结果摘要：

```text
stat    61/61
readdir  4/4
create   7/7
delete   3/3
rename   5/5
```

重新执行目录 rename 专项 trace：

```text
result=/tmp/lhm_refactor_rename_20260528_2/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 19.5 回归要求

后续完成结构拆分和 directory block 下沉后，不能只看编译和 smoke。必须跑完整指标回归，至少包括：

1. 接口正确性：
   - stat；
   - readdir；
   - create/delete；
   - file rename；
   - directory rename；
   - mixed trace。

2. 并发吞吐：
   - create/delete 多线程；
   - stat 多线程；
   - rename 或 mixed workload 的基本并发 sanity。

3. 路径解析：
   - path-depth latency；
   - avg/p50/p95/p99；
   - 与重构前 summary 对比，不能出现非预期退化。

4. 重命名：
   - subtree rename scalability；
   - entries_touched/reattach counters；
   - LHM 仍应保持目录 rename 近似 O(1) 子树重挂。

5. 内存占用：
   - footprint sweep；
   - 文件 entry 下沉后，内存曲线应从总 entry 数驱动转向目录数 + cache size 驱动；
   - 若 directory block cache 引入新内存，应单独统计。

6. 持久化统计：
   - block read/write ops；
   - bytes read/written；
   - L0/L1 flush；
   - fsync/sync ops。

任何一个核心指标下降，都需要先定位原因，再决定是修复实现、调整缓存策略，还是明确修改论文 claim。

### 19.6 下一步

下一步可以迁移 child 删除相关 cursor 操作：

```text
force_remove_child_slot_fast(...)
remove_entry_fast_from_parsed(...)
remove_entry_from_parsed(...)
```

建议先在 `MasstreeDirectoryIndex` 中抽：

```text
force_remove_child_slot(parent_root, child_hash, ti)
remove_child_slot(parent_root, child_hash, removed_out, ti)
remove_empty_directory_layer(parent_root, child_hash, child_name, removed_out, ti)
```

然后 `LhmNamespace` 继续负责目录非空检查、名字检查和持久化 tombstone。

---

## 20. 2026-05-28 重构进度：迁移 child 删除 primitive

### 20.1 本步目标

本步将 child 删除相关 Masstree cursor 操作迁移到 `MasstreeDirectoryIndex`，继续减少 `LhmNamespace` 对底层 cursor 的直接操作。

### 20.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
directory_root_has_children(node)
force_remove_child_slot(parent_root, child_hash, ti)
remove_child_slot_fast(parent_root, child_hash, removed_out, ti)
remove_child_slot_checked(parent_root, child_hash, child_name, removed_out, ti)
```

语义：

- `force_remove_child_slot`：用于 fast rename 目标槽位清理；目标不存在也视为成功；
- `remove_child_slot_fast`：沿用 fast remove 行为，删除 layer 或普通 value，不做名字确认；
- `remove_child_slot_checked`：沿用 non-fast remove 行为：
  - 目录必须匹配 `directory_meta.name`；
  - 目录必须为空；
  - 文件必须匹配 `namespace_entry.name` 且类型为 file；
  - 删除成功时通过 `removed_out` 返回被删 entry。

`LhmNamespace` 中以下路径已改为调用 `directory_index_`：

- `force_remove_child_slot_fast(...)`
- `remove_entry_fast_from_parsed(...)`
- `remove_entry_from_parsed(...)`

`LhmNamespace` 中旧的 `directory_root_has_children` 已移除。

### 20.3 行为边界

本步不改变：

- fast mode 删除语义；
- non-fast mode 的名字/类型/空目录检查语义；
- delete 后 inode tombstone 持久化；
- rename 中目标槽位 fast 清理行为。

### 20.4 验证

Release 编译通过：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

执行 100-op `macro_mixed` trace：

```text
trace=/tmp/lhm_refactor_delete_20260528_1.tsv
result=/tmp/lhm_refactor_delete_20260528_1/lhm
```

结果摘要：

```text
stat    78/78
readdir  9/9
create   8/8
delete   5/5
```

重新执行目录 rename 专项 trace：

```text
result=/tmp/lhm_refactor_rename_20260528_3/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 20.5 下一步

下一步可以迁移 readdir 直接扫描逻辑：

```text
append_directory_children(...)
append_directory_children_from_node(...)
```

迁移后，`LhmNamespace` 中直接递归遍历 Masstree node 的代码会进一步减少。等 readdir 迁移完成后，`MasstreeDirectoryIndex` 将基本覆盖当前仍在 Masstree 中的目录路由、文件 value 插入、child 查询和 child 删除 primitives。

---

## 21. 2026-05-28 重构进度：迁移 readdir 扫描 primitive

### 21.1 本步目标

本步将 `readdir` 使用的 Masstree node 递归扫描逻辑迁移到 `MasstreeDirectoryIndex`。迁移后，`LhmNamespace::readdir` 只负责：

1. 解析路径；
2. 定位目录 root；
3. 校验目录 `directory_meta`；
4. 调用 `directory_index_.append_directory_children(...)`。

实际扫描当前目录 layer 的 leaf/internode、恢复文件 value 和目录 edge 的逻辑由 `MasstreeDirectoryIndex` 负责。

### 21.2 当前改动

`MasstreeDirectoryIndex` 新增：

```text
append_directory_children(parent_root, parent_hashes, out)
append_directory_children_from_node(node, parent_hashes, out)
```

语义保持不变：

- 扫描当前目录 layer；
- 普通 value 作为文件 entry 返回；
- layer edge 作为子目录 entry 返回；
- 命中子目录 edge 时只读取该子目录 root 的 `directory_meta`，不下钻子目录；
- `readdir_record.full_hashes`、`child_component_hash`、`child_name`、`entry` 填充方式不变。

`LhmNamespace` 中旧的 `append_directory_children` / `append_directory_children_from_node` 已移除。

### 21.3 行为边界

本步不改变：

- readdir 返回直接孩子，不递归；
- 当前阶段文件 entry 仍来自 Masstree leaf value；
- 子目录 entry 仍来自 layer root 的 `directory_meta`；
- entries_scanned 统计口径。

### 21.4 验证

Release 编译通过：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

执行 100-op `macro_mixed` trace：

```text
trace=/tmp/lhm_refactor_readdir_20260528_1.tsv
result=/tmp/lhm_refactor_readdir_20260528_1/lhm
```

结果摘要：

```text
stat    72/72
readdir  8/8
create  12/12
delete   4/4
rename   4/4
```

重新执行目录 rename 专项 trace：

```text
result=/tmp/lhm_refactor_rename_20260528_4/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 21.5 当前结构状态

到本步为止，`MasstreeDirectoryIndex` 已经覆盖当前 Masstree 内存索引的大部分底层 primitive：

- table ownership；
- root access；
- single component key；
- root canonicalization；
- directory root locate；
- child slot lookup；
- directory edge lookup；
- directory layer create；
- directory layer attach/detach；
- file value insert；
- child remove；
- readdir scan。

`LhmNamespace` 仍负责：

- 对外 API；
- path parse / parent path 编排；
- create/delete/rename 语义；
- inode 分配；
- `PersistentStore` 调用；
- fast/non-fast mode 分支；
- subtree scan 和少量旧 rename fallback 逻辑。

### 21.6 下一步

下一步可以先暂停继续搬迁小函数，做一次阶段性整理：

1. 检查 `LhmNamespace` 中剩余直接使用 `cursor_type` / `unlocked_cursor_type` 的位置；
2. 判断哪些仍应归属于 `MasstreeDirectoryIndex`；
3. 将阶段性重构后的 API 边界固化；
4. 再开始设计 `DirectoryBlockStore`，准备把文件 entry 从 Masstree value 下沉到 directory block。

## 22. 迁移 subtree scan 到 MasstreeDirectoryIndex

### 22.1 本步目标

在第 21 步完成 readdir 扫描迁移之后，本步继续把 `scan_subtree` 的 Masstree 扫描细节从 `LhmNamespace` 下沉到 `MasstreeDirectoryIndex`。

这样做的目的不是改变 rename/delete 的外部语义，而是继续收敛边界：

- `MasstreeDirectoryIndex` 负责如何遍历当前 Masstree namespace index；
- `LhmNamespace` 只负责决定什么时候需要 subtree scan，以及 scan 结果如何参与 rename/delete 语义；
- 后续引入 directory block 后，可以在 index 层统一替换文件 entry 的枚举来源。

### 22.2 代码调整

新增/迁移内容：

- `MasstreeDirectoryIndex::scan_subtree(const ParsedPath&, threadinfo&)`；
- `MasstreeDirectoryIndex::subtree_scanner`；
- `LhmNamespace::scan_subtree(...)` 改为薄包装，直接转发给 `directory_index_`。

同时移除 `LhmNamespace` 内部旧的 `subtree_scanner` 实现。至此，`LhmNamespace` 不再直接持有 Masstree cursor/table scan 类型。

阶段性搜索确认：

```text
rg cursor_type/find_insert/find_locked_edge/table().scan ...
```

结果显示这些底层 Masstree 操作只剩在 `masstree_directory_index.hh` 中；`lhm_namespace.hh` 只保留对 `directory_index_` 的高层调用。

### 22.3 行为边界

本步不改变：

- subtree scan 返回当前 Masstree 表示下的目录和文件记录；
- directory rename 仍通过 subtree scan 收集待更新路径；
- 文件 entry 当前仍在 Masstree value 中；
- directory block 尚未接入。

也就是说，本步仍属于“拆分职责、收窄边界”的准备阶段，而不是数据布局切换。

### 22.4 验证

Release 编译通过：

```text
cmake --build build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

执行 100-op `macro_mixed` trace：

```text
trace=/tmp/lhm_refactor_subtree_20260528_1.tsv
result=/tmp/lhm_refactor_subtree_20260528_1/lhm
```

结果摘要：

```text
stat    70/70
readdir  9/9
create   9/9
delete   6/6
rename   6/6
```

重新执行目录 rename 专项 trace：

```text
result=/tmp/lhm_refactor_rename_20260528_5/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 22.5 当前结构状态

到本步为止，所有直接 Masstree table ownership、cursor 操作、layer edge 操作、readdir scan、subtree scan 都已经集中到 `MasstreeDirectoryIndex`。

`LhmNamespace` 当前边界更清晰：

- 对外 API 和 path 语义仍在 `LhmNamespace`；
- 目录路由索引的底层访问在 `MasstreeDirectoryIndex`；
- namespace value 类型在 `namespace_types.hh`；
- path helper 在 `namespace_path.hh`；
- 持久化 record/block 管理由 `PersistentStore` 维持。

下一步建议开始设计并引入 `DirectoryBlockStore` 的最小骨架：先定义目录块内 entry 格式、block handle/cache 接口和 lookup/readdir 抽象，但暂不一次性切换 create/delete/rename 的持久化路径。

## 23. 引入 DirectoryBlockStore 最小骨架

### 23.1 本步目标

本步开始为“文件 entry 下沉到目录块”建立代码落点，但不立即改变当前 namespace 行为。

核心原则：

- 先定义目录块读路径和 cache 边界；
- 先让 `LhmNamespace` 能持有目录块访问对象；
- 暂不把 create/delete/rename/readdir 从 Masstree value 切到 directory block；
- 保持当前所有外部接口和测试语义不变。

### 23.2 代码调整

新增文件：

```text
directory_block_store.hh
```

新增 `DirectoryBlockStore`，当前提供：

- `DirectoryBlockStore::entry`：目录块内 entry 的内存表示；
- `bind(PersistentStore&)` / `reset()`：绑定现有持久化 store；
- 64-slot direct-mapped directory block cache；
- `read_block(...)`：按 `inode_ref` 读取目录块并校验 owner inode；
- `read_entries(...)`：扫描一个目录块链；
- `lookup(...)`：在目录块链内按 `component_hash + name` 查找 entry；
- `append_entry_to_image(...)`：目录块 entry 编码 helper，供后续写路径接入；
- cache stats：`cache_hits/cache_misses/malformed_blocks`；
- 目录块链 scan 带 hop 上限，避免损坏链条导致无限循环。

`LhmNamespace` 的调整：

- include `directory_block_store.hh`；
- 新增成员 `DirectoryBlockStore directory_blocks_`；
- `initialize(...)` 后绑定 `persistent_store_`；
- `destroy(...)` 时 reset；
- `reset_persistence_stats()` 同时 reset directory block cache stats；
- 新增 `directory_block_stats()` 便于后续观测。

### 23.3 行为边界

本步没有把任何命名空间操作切换到目录块：

- lookup 仍由 Masstree directory route/value 完成；
- file create 仍向 Masstree leaf value 插入 `namespace_entry`；
- readdir 仍扫描 Masstree 当前目录 root；
- rename/delete 仍操作 Masstree slot/layer；
- directory block 目前只具备可读/可缓存/可编码的基础能力。

这一步的价值是把后续切换点变清楚：下一阶段可以先让 directory create 时生成目录块 meta 映射，再逐步把 file entry 的 insert/lookup/readdir/delete 挪到 `DirectoryBlockStore`。

### 23.4 兼容性细节

当前 `PersistentStore::AppendDirectoryChainBlock(...)` 已更新 `directory_block_header.next_block_id`，但没有同步更新 common header 的 `next_block_id`。

因此 `DirectoryBlockStore` 扫描目录块链时以 `directory_block_header.next_block_id` 为准，不强制校验 common header 的 next 字段。这样可以兼容现有持久化实现，避免本步把无关布局修正混入行为重构。

### 23.5 验证

Release 编译通过：

```text
cmake --build /home/zwt/zyb/build_release --target trace_replay -j 8
[100%] Built target trace_replay
```

执行 100-op `macro_mixed` trace：

```text
trace=/tmp/lhm_refactor_dirblock_20260528_1.tsv
result=/tmp/lhm_refactor_dirblock_20260528_2/lhm
check_run_sanity.py -> sanity_ok systems=1
```

结果摘要：

```text
stat    54/54
readdir 18/18
create  15/15
delete   8/8
rename   5/5
```

执行目录 rename 专项 trace：

```text
trace=/tmp/lhm_refactor_dirblock_rename_20260528_1.tsv
result=/tmp/lhm_refactor_dirblock_rename_20260528_1/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 23.6 下一步

下一步建议不要立刻把所有文件 entry 从 Masstree 挪走，而是先补齐“目录 inode -> directory block head”的解析 helper：

1. 给 `LhmNamespace` 增加通过目录 entry/ref 读取目录 inode 的 helper；
2. 从 directory inode 的 `primary_ref` 得到 directory block chain head；
3. 用 `DirectoryBlockStore` 对根目录和普通目录做只读 scan 验证；
4. 再接入 file create 的“双写阶段”：Masstree 保持现有权威路径，同时把新文件 entry 写入父目录 directory block。

双写阶段通过后，再逐步切换 lookup/readdir/delete 的读取来源。

## 24. 阶段性性能回归对比：当前版本 vs MasstreeLHM-backup

### 24.1 测试目的

在引入 `MasstreeDirectoryIndex` / `DirectoryBlockStore` 后，验证当前版本相对重构前备份版本没有明显接口性能回退。

对比对象：

- backup：`/home/zwt/zyb/MasstreeLHM-backup`
- current：`/home/zwt/zyb/MasstreeLHM`

备份版本通过 `/tmp` overlay 源码树构建，overlay 中 `MasstreeLHM` 指向 `MasstreeLHM-backup`，其他 evaluation/rocksdb/FlatFS 复用当前仓库。

### 24.2 测试覆盖

本轮覆盖四类指标：

1. 路径解析/lookup 延迟：`micro_stat_depth`
2. 目录 rename 性能：`micro_rename_scalability`
3. 内存占用：100K wide namespace 的 `rss_kb_index_delta_after_load_clean`
4. 并发吞吐：8 线程 `concurrency_stat`

测试输入：

```text
/tmp/lhm_perf_deep_50k.tsv
/tmp/lhm_perf_wide_100k.tsv
/tmp/lhm_perf_rename_scale.tsv
/tmp/lhm_perf_trace_stat_depth.tsv
/tmp/lhm_perf_trace_rename.tsv
/tmp/lhm_perf_trace_concurrency_stat.tsv
/tmp/lhm_perf_trace_memory_min.tsv
```

最终主结果目录：

```text
/tmp/lhm_perf_compare_20260528_after_facade_inline
/tmp/lhm_perf_compare_20260528_rename_long
```

所有 final run 均通过：

```text
check_run_sanity.py -> sanity_ok systems=1
```

### 24.3 发现并修正的问题

初始测试发现当前版本在 100K namespace 下比 backup 多约 1MB clean RSS。

原因：

- `DirectoryBlockStore` 内嵌了 `64 * 16KB` 的 directory block cache；
- 当前阶段 directory block 尚未接管任何 namespace 读写路径；
- 这个 cache 即使未使用也常驻内存，违背“骨架阶段不增加内存开销”的目标。

修正：

- 将 `DirectoryBlockStore` cache 改为 lazy allocation；
- 只有第一次实际读取 directory block 时才分配 64-slot cache；
- 当前阶段不读 directory block，因此固定 1MB 开销消失。

第二个发现是抽象边界拆分后，微秒级热路径可能留下 wrapper 调用开销。

修正：

- 对 `MasstreeDirectoryIndex` 的 lookup/create/remove/rename primitive 增加强内联标记；
- 对 `LhmNamespace` facade 上的 lookup/route wrapper 增加强内联标记；
- 目标是在保留拆分边界的同时，不牺牲当前热路径。

### 24.4 最终主结果

最终主结果来自：

```text
/tmp/lhm_perf_compare_20260528_after_facade_inline
```

整体 report 摘要：

```text
test                  backup                 current
stat_depth throughput 576048 ops/s           596892 ops/s
memory_100k RSS delta 38416 KB               38416 KB
concurrency throughput
                      362377 ops/s           424843 ops/s
```

路径深度 stat 延迟：

```text
depth   backup avg/us   current avg/us   delta
1       0.474           0.413           -12.8%
2       0.370           0.386            +4.2%
4       0.526           0.554            +5.3%
8       0.860           0.892            +3.8%
16      2.067           1.493           -27.8%
32      2.466           2.553            +3.5%
40      3.543           3.753            +5.9%
```

结论：路径解析/lookup 整体吞吐 current 略高；各 depth 的单项均值存在小幅波动，最大约 6% 的慢项仍在微秒级噪声范围内。

并发 stat：

```text
backup  80000/80000 success, avg=1.928us, p50=1.562us, p95=3.267us
current 80000/80000 success, avg=1.596us, p50=1.285us, p95=2.952us
```

结论：并发吞吐未下降，current 更快。

### 24.5 rename 长跑复测

短 trace 的 rename 单项均值对离群点敏感，因此追加了更长的 rename trace：

```text
trace=/tmp/lhm_perf_trace_rename_long.tsv
repeat=200
warmup_repeat=20
result=/tmp/lhm_perf_compare_20260528_rename_long
```

结果：

```text
subtree   backup avg/us   current avg/us   delta
1         4.771           5.024            +5.3%
10        3.100           3.212            +3.6%
100       3.022           3.251            +7.6%
1000      3.056           3.150            +3.1%
```

整体 wall-clock throughput：

```text
backup  126206 ops/s
current 128389 ops/s
```

结论：目录 rename 的整体吞吐未下降；按 label 聚合的 per-op latency 均值有 3-8% 波动，仍属于当前微秒级测试下需要持续观察的范围。

### 24.6 当前判断

修正后：

- 内存占用：与 backup 持平；
- 路径解析/lookup：整体吞吐未下降；
- 并发 stat：未下降，current 更快；
- rename：整体吞吐未下降，per-label 微延迟有小幅波动。

下一阶段进入 directory block 双写/读路径切换前，应继续沿用这一套回归测试；如果后续任何单项出现稳定超过 10% 的下降，应先暂停功能切换并定位原因。

## 25. dentryBlock v1：Masstree 只保留目录路由

### 25.1 本阶段目标

本阶段按新的结构方案实现一版最小可运行原型：

- Masstree 不再保存普通 file entry；
- Masstree 只保存目录 route 结构；
- 每个目录对应一个 dentryBlock chain；
- file entry / directory child entry 存在父目录 dentryBlock 中；
- lookup 先通过 Masstree 定位父目录 route root，再读父目录 dentryBlock 查找目标 entry；
- readdir 直接扫描该目录 dentryBlock；
- hash 冲突和并发控制暂列 TODO，本阶段不处理。

### 25.2 directory_meta 布局

`directory_meta` 已降到 32B：

```cpp
struct directory_meta {
    uint32_t head_block_id;
    uint32_t tail_block_id;
    uint64_t component_hash;
    uint64_t version;
    uint32_t entry_count_hint;
    uint16_t flags;
    uint16_t reserved;
};
```

含义：

- `head_block_id`：该目录 dentryBlock chain 的首 block；
- `tail_block_id`：当前追加写入的尾 block；
- `component_hash`：该目录在父目录下的 component hash；
- `version`：目录 block 引用/entry hint 的内存版本；
- `entry_count_hint`：目录项数量 hint，不作为真相；
- `flags/reserved`：后续扩展。

本阶段不在 `directory_meta` 保存 name，也不保存 inode_ref。目录自身 inode-like 信息后续应迁入 dentryBlock header 或单独 inode block；当前原型只保留 dentryBlock 的路由引用。

### 25.3 dentryBlock 内部格式

每个 block 使用已有 16KB `directory_block_image`，payload 内部采用“双端增长”：

- entry record 从 payload 头部向后追加；
- sorted index 从 payload 尾部向前追加；
- `directory_block_header::entry_count` 表示有效 entry 数；
- `directory_block_header::delta_count` 复用为 index 数；
- index 按 `component_hash` 排序，lookup 用二分查找；
- 删除当前只移除 index 并降低 entry count，不做 payload compact。

entry record header 为 16B：

```cpp
struct dentry_entry_header {
    uint64_t component_hash;
    uint16_t record_bytes;
    uint8_t kind;
    uint8_t name_length;
    uint16_t flags;
    uint16_t ref_bytes;
};
```

index entry 为 16B：

```cpp
struct dentry_index_entry {
    uint64_t component_hash;
    uint16_t entry_offset;
    uint16_t flags;
    uint32_t reserved;
};
```

文件 entry 当前内容：

- `component_hash`；
- `kind=file`；
- `name_length + name`；
- `file_ref_payload`，8B，占位保存 `inode_block_id / inode_slot / generation`。

目录 entry 当前内容：

- `component_hash`；
- `kind=directory`；
- `name_length + name`；
- `directory_ref_payload`，4B，只保存 child directory 的 `head_block_id`。

本阶段明确不保存 offset；directory ref 只有 block id。

### 25.4 已切换的接口行为

当前读写路径：

- file create：定位父目录 route root，把 file dentry 写入父目录 dentryBlock；
- directory create：创建子目录 route root，分配子目录 dentryBlock，再把 directory dentry 写入父目录 dentryBlock；
- stat/lookup：
  - 中间路径组件仍走 Masstree route；
  - 最后一段如果是目录，优先从 Masstree child route 命中；
  - 否则从父目录 dentryBlock 查 file dentry；
- readdir：读取目标目录 dentryBlock chain 并返回其中 entries；
- delete：目录删除移除 Masstree child route 和父目录 dentry；文件删除只移除父目录 dentry；
- rename：目录 rename 更新 Masstree route，并同步移除/插入父目录 dentry；文件 rename 通过 dentryBlock remove + insert 完成。

### 25.5 当前重要限制

1. hash 冲突未处理：index key 只有 `component_hash`，冲突时行为未定义。
2. 并发控制未重新设计：当前仍依赖原有 Masstree 并发路径，dentryBlock 更新还没有独立锁/事务。
3. file inode 持久化临时关闭：现有 `PersistentStore::AllocateInodeSlot()` 会写 inode block，而 directory block 也从同一 block id 空间分配；如果继续写 file inode，可能覆盖 dentryBlock。因此本阶段 file ref 使用 transient ref，`persist_create_success/delete/rename` 对 file inode 写入为 no-op。
4. dentryBlock 没有 cache/write-back：每次 lookup/insert/remove 当前都可能直接读写 16KB block，性能不是最终形态。
5. 删除不 compact：长期删除/插入会产生 payload 空洞，需要后续 block compaction 或重写策略。

### 25.6 正确性与内存测试

构建：

```text
cmake --build /home/zwt/zyb/build_release --target trace_replay -j 8
```

smoke：

```text
result=/tmp/lhm_dentryblock_smoke_20260528_after_inode_noop_v3/lhm
check_run_sanity.py -> sanity_ok systems=1

loaded_records=1000
ops=100
throughput_ops_per_sec=1994.7912
lhm_rename_ops=3
lhm_rename_success=3
lhm_rename_fail=0
rss_kb_index_delta_after_load_clean=15720
lhm_persist_block_read_ops=157
lhm_persist_block_write_ops=47
```

100K wide namespace 内存对比：

```text
namespace=/tmp/lhm_perf_wide_100k.tsv
trace=/tmp/lhm_perf_trace_memory_min.tsv
backup_result=/tmp/lhm_perf_compare_20260528_after_facade_inline/backup/memory_100k/lhm
current_result=/tmp/lhm_dentryblock_memory_100k_current_after_inode_noop_v3/lhm
check_run_sanity.py -> sanity_ok systems=1
```

结果：

```text
metric                                  backup        dentryBlock v1
loaded_records                          100000        100000
rss_kb_index_delta_after_load_clean     38416 KB      20288 KB
rss_kb_after_load_clean                 47376 KB      29248 KB
load_sec                                0.060s        53.635s
throughput_ops_per_sec                  19740 ops/s   3276 ops/s
lhm_persist_block_read_ops              0             10
lhm_persist_block_write_ops             0             5
```

内存下降：

```text
reduction_kb  = 38416 - 20288 = 18128 KB
reduction_pct = 47.19%
```

### 25.7 阶段判断

dentryBlock v1 已经证明“只把目录 route 放在 Masstree、普通 file entry 下沉到目录 block”可以显著降低内存索引占用：在同一个 100K wide namespace 上，clean RSS index delta 下降约 47.2%。

但当前版本不是性能最终版。性能下降主要来自 dentryBlock 直接同步读写 16KB block，且没有目录 block cache、write-back、批量 build、allocator namespace 分离。下一步如果要让性能回到重构前水平，应优先做：

1. 分离 inode block allocator 和 directory block allocator；
2. 增加 dentryBlock cache，lookup 先命中内存 block；
3. namespace load/build 阶段批量构造 dentryBlock，避免每插入一个 entry 都读写整块；
4. 增加 hash collision 处理；
5. 设计 dentryBlock 并发锁或目录级 latch。

## 26. 50w 规模内存复测

### 26.1 测试目的

在做 allocator 分离和 dentryBlock cache 之前，先确认 dentryBlock v1 的内存下降是否能在 50w 规模下复现，并区分两类目录形状：

1. formal iwide：使用 `path_kv_generate --preset iwide-10m` 的正式 iwide 分布，目录数量较多；
2. hand-wide：固定 1000 个顶层目录，剩余几乎全是文件 entry，与上一轮 100K wide 更接近。

两组均使用同一个最小 trace：

```text
/tmp/lhm_perf_trace_root_stat_20260528.tsv
```

trace 只执行一次 `stat("/")`，目的是让 report 记录 load 后 clean RSS，而不引入 lookup workload 噪声。

### 26.2 formal iwide：目录占比较高

数据集：

```text
namespace=/tmp/lhm_perf_iwide_500k_20260528.tsv
manifest=/tmp/lhm_perf_iwide_500k_20260528.manifest.json
loaded_records=500000
generator_done: files=199939, dir_entries=300064, records=500003
```

结果目录：

```text
backup=/tmp/lhm_dentryblock_compare_500k_backup_20260528/lhm
current=/tmp/lhm_dentryblock_compare_500k_current_20260528/lhm
check_run_sanity.py -> sanity_ok systems=2
```

结果：

```text
metric                                  backup        dentryBlock v1
loaded_records                          500000        500000
rss_kb_index_delta_after_load_clean     335688 KB     311152 KB
rss_kb_after_load_clean                 344648 KB     320112 KB
load_sec                                53.424s       286.356s
throughput_ops_per_sec                  2075 ops/s    1783 ops/s
```

内存下降：

```text
reduction_kb  = 335688 - 311152 = 24536 KB
reduction_pct = 7.31%
```

解释：这个分布中目录 entry 约 30w、文件 entry 约 20w。dentryBlock v1 只把 file entry 从 Masstree 中移走，目录 route 仍保留在 Masstree。因此当目录本身成为主规模项时，内存下降会明显变小。

### 26.3 hand-wide：文件占比较高

数据集：

```text
namespace=/tmp/lhm_perf_handwide_500k_20260528.tsv
loaded_records=500000
shape: root=1, top_dirs=1000, files=498999
```

结果目录：

```text
backup=/tmp/lhm_dentryblock_compare_500k_handwide_backup_20260528/lhm
current=/tmp/lhm_dentryblock_compare_500k_handwide_current_20260528/lhm
check_run_sanity.py -> sanity_ok systems=2
```

结果：

```text
metric                                  backup        dentryBlock v1
loaded_records                          500000        500000
rss_kb_index_delta_after_load_clean     89524 KB      15892 KB
rss_kb_after_load_clean                 98484 KB      24852 KB
maxrss_kb_after_load_clean              121928 KB     68456 KB
load_sec                                2.024s        380.887s
throughput_ops_per_sec                  2136 ops/s    1414 ops/s
```

内存下降：

```text
reduction_kb  = 89524 - 15892 = 73632 KB
reduction_pct = 82.25%
```

解释：这个分布和 100K wide 一样，目录数量固定、文件 entry 是主规模项。dentryBlock v1 移走约 49.9w file entries 后，clean RSS 基本收敛到“1000 个目录 route + 固定运行时开销”的量级，因此下降比例比 100K wide 更大。

### 26.4 阶段判断

50w 结果说明 dentryBlock 方案的内存收益符合结构预期，但它不是一个固定百分比：

- 如果 namespace 主要由文件 entry 增长，收益很明显，hand-wide 50w 下降约 82.25%；
- 如果 namespace 中目录数量也随规模大幅增长，目录 route 仍会常驻 Masstree，formal iwide 50w 只下降约 7.31%；
- 因此后续论文或设计讨论不能只说“下降 X%”，必须同时报告目录/文件比例，或者把内存模型写成：

```text
Memory ~= O(number_of_directories_in_route_index)
        + O(directory_block_cache_size)
        + fixed runtime overhead
```

而不是旧版的：

```text
Memory ~= O(number_of_directories + number_of_files)
```

当前 dentryBlock v1 的加载性能非常差，hand-wide 50w 从 2.024s 增加到 380.887s。这不是最终设计性能，主要来自每条 entry 插入都读写 16KB block、没有批量 build、没有 write-back。下一步应先处理 allocator 分离和 dentryBlock cache/batch build，否则性能问题会掩盖内存结构收益。

## 27. allocator namespace 分离与 dentryBlock cache

### 27.1 allocator namespace 分离

本阶段修复 inode block 与 directory block 共用 block id 导致的覆盖问题。之前的冲突是：

```text
inode logical block 1      -> physical block 1
directory logical block 1  -> physical block 1
```

当 file inode 持久化打开后，inode 写入会覆盖 dentryBlock，导致大量 stat 失败。

当前实现采用物理层奇偶分离：

```text
superblock                 -> physical block 0
inode logical block N      -> physical block 2N - 1
directory logical block N  -> physical block 2N
```

这样 `inode_ref.block_id=1` 与 `directory block_id=1` 可以同时存在，但映射到不同物理块。`directory_meta` 仍只保存 `head_block_id/tail_block_id`，不需要 offset。

相关实现：

- `PersistentStore::InodePhysicalBlockId(...)`
- `PersistentStore::DirectoryPhysicalBlockId(...)`
- `PersistentStore::AllocateDirectoryBlockLocked(...)`
- `PersistentStore::RecomputeNextDirectoryBlock(...)`

file inode 持久化已重新打开：

- `allocate_inode_ref_for_create(...)` 在 persistence 可用时调用 `AllocateInodeSlot(...)`；
- file create 写 inode；
- file delete 更新 tombstone/link_count；
- file rename 更新 parent inode id 和版本；
- directory inode 持久化仍暂未完整接入，目录的权威路由仍是 Masstree route + dentryBlock head。

### 27.2 dentryBlock cache

`DirectoryBlockStore` 新增固定容量 LRU dirty cache：

```text
key:   directory block_id
value: 16KB directory_block_image
state: clean / dirty
policy: LRU eviction
default capacity: 64 blocks
env: LHM_DENTRY_CACHE_BLOCKS
```

读路径：

```text
read_block(block_id):
  cache hit  -> copy cached image
  cache miss -> PersistentStore::ReadDirectoryBlock + insert clean cache entry
```

写路径：

```text
write_block(block_id, image):
  update/insert cache entry
  mark dirty
  do not immediately write 16KB block
```

flush 时机：

- cache eviction；
- `DirectoryBlockStore::flush_all()`；
- `LhmNamespace::sync_persistence()`；
- `LhmNamespace::destroy()` before `PersistentStore::Sync()/Close()`。

一个关键修复：directory block 扩链时，必须先 flush dirty old tail，再调用 `AppendDirectoryChainBlock(...)` 更新 tail 的 `next_block_id`，随后丢弃 old tail cache。否则会出现：

```text
dirty old tail in cache
AppendDirectoryChainBlock reads stale old tail from disk and writes next_block_id
later dirty cache flush overwrites next_block_id back to 0
```

该 bug 在第一次 cache smoke 中表现为 stat 失败，修复后 smoke 通过。

### 27.3 新增 report counters

`lhm_adapter` 现在额外输出 dentryBlock cache 统计：

```text
lhm_dentry_block_reads
lhm_dentry_block_writes
lhm_dentry_cache_hits
lhm_dentry_cache_misses
lhm_dentry_cache_flushes
lhm_dentry_cache_evictions
lhm_dentry_lookups
lhm_dentry_inserts
lhm_dentry_removes
```

注意：当前 runner 在 replay 前调用 `reset_persistence_stats()`，因此这些 counters 只覆盖 replay 阶段，不覆盖 namespace load 阶段。后续如果要分析 load build，需要增加 load-phase stats dump。

### 27.4 correctness

编译：

```text
cmake --build /home/zwt/zyb/build_release --target trace_replay -j 8
```

smoke：

```text
result=/tmp/lhm_alloc_cache_smoke_20260528_v3/lhm
check_run_sanity.py -> sanity_ok systems=1

load_sec=0.013287106
rss_kb_index_delta_after_load_clean=16172
lhm_persist_block_read_ops=1
lhm_persist_block_write_ops=42
stat    54/54
readdir 13/13
create  21/21
delete   9/9
rename   3/3
```

cache disabled smoke also passed:

```text
LHM_DENTRY_CACHE_BLOCKS=0
result=/tmp/lhm_alloc_nocache_smoke_20260528/lhm
check_run_sanity.py -> sanity_ok systems=1
```

### 27.5 100K hand-wide cache sensitivity

100K hand-wide, default 64-block cache：

```text
result=/tmp/lhm_alloc_cache_memory_100k_20260528/lhm
check_run_sanity.py -> sanity_ok systems=1

load_sec=44.9187403
rss_kb_index_delta_after_load_clean=22368
lhm_persist_block_write_ops=192
```

100K hand-wide, 2048-block cache：

```text
result=/tmp/lhm_alloc_cache2048_memory_100k_20260528/lhm
check_run_sanity.py -> sanity_ok systems=1

load_sec=1.23296468
rss_kb_index_delta_after_load_clean=37780
lhm_persist_block_write_ops=1129
```

与旧版 backup：

```text
backup rss_kb_index_delta_after_load_clean=38416
```

2048-block cache 几乎覆盖 1000 个活跃目录的 tail block，因此加载速度明显恢复；但 2048 * 16KB 约 32MB cache 会吃掉大部分内存收益，100K 下相对 backup 只下降约 1.66%。

### 27.6 50w hand-wide

对比：

```text
backup:
  result=/tmp/lhm_dentryblock_compare_500k_handwide_backup_20260528/lhm
  load_sec=2.0243485
  rss_kb_index_delta_after_load_clean=89524

dentryBlock v1 no-cache/no inode persistence:
  result=/tmp/lhm_dentryblock_compare_500k_handwide_current_20260528/lhm
  load_sec=380.887346
  rss_kb_index_delta_after_load_clean=15892
  reduction_vs_backup=82.25%

allocator split + 2048-block cache:
  result=/tmp/lhm_alloc_cache2048_500k_handwide_20260528/lhm
  check_run_sanity.py -> sanity_ok systems=1
  load_sec=7.1193232
  rss_kb_index_delta_after_load_clean=47800
  reduction_vs_backup=46.61%
```

结论：2048-block cache 将 50w hand-wide 加载从 380.9s 降到 7.1s，但因为 cache 本身约 32MB，加上恢复 file inode 持久化后的 inode cache/writeback 开销，内存下降从 82.25% 缩小到 46.61%。

### 27.7 50w formal iwide

对比：

```text
backup:
  result=/tmp/lhm_dentryblock_compare_500k_backup_20260528/lhm
  load_sec=53.4235558
  rss_kb_index_delta_after_load_clean=335688

dentryBlock v1 no-cache/no inode persistence:
  result=/tmp/lhm_dentryblock_compare_500k_current_20260528/lhm
  load_sec=286.356165
  rss_kb_index_delta_after_load_clean=311152
  reduction_vs_backup=7.31%

allocator split + 2048-block cache:
  result=/tmp/lhm_alloc_cache2048_500k_iwide_20260528/lhm
  check_run_sanity.py -> sanity_ok systems=1
  load_sec=223.024398
  rss_kb_index_delta_after_load_clean=343508
  reduction_vs_backup=-2.33%
```

formal iwide 中目录约 30w、文件约 20w。目录 route 仍然常驻 Masstree，2048-block cache 不能覆盖大量分散目录；因此 cache 内存反而超过 file-entry 下沉节省，RSS 略高于 backup。

### 27.8 阶段判断

allocator namespace 分离是必要且正确的：file inode 持久化重新打开后，smoke 和 100K/50w sanity 都通过，说明 inode block 不再覆盖 dentryBlock。

dentryBlock cache 的效果取决于 active directory working set：

- active directory tail 能被 cache 覆盖时，write-back 能显著降低 load 写放大；
- cache 过小会频繁 dirty eviction，加载仍慢；
- cache 过大会吃掉 file-entry 下沉节省的内存；
- 对 formal iwide 这类高目录数/高目录 churn workload，仅靠 LRU cache 不够。

下一步不是继续单纯增大 cache，而是做 namespace load/build 阶段的父目录聚合：

```text
load namespace:
  group entries by parent directory
  sequentially build each directory's dentryBlock chain
  write each block once
```

这样可以在小 cache 下避免反复读写同一目录 block，同时保留 dentryBlock 的内存优势。

## 28. 文件名上限提升到 255B（2026-06-01）

### 28.1 背景

重构前 LHM 使用：

```cpp
static constexpr size_t kMaxEntryNameBytes = 63;
```

这个限制来自旧 `namespace_entry` 的内联名字数组，而不是 Masstree 自身能力。dentryBlock 重构后，Masstree 内存索引树只保存目录 route；文件和目录的真实名字保存在父目录 dentryBlock 中。因此 63B 不再是必要限制。

### 28.2 本次修改

1. `kMaxEntryNameBytes` 从 63 提升到 255，对齐常见 POSIX `NAME_MAX`。
2. evaluation 中默认 `--max-component-bytes` 从 63 同步提升到 255：
   - `validate_namespace.py`
   - `run_footprint_sweep.py`
   - `run_isolated_stat_depth.py`
   - `run_xfs_memory_sweep.py`
   - `evaluation/platform/README.md`
3. 将 Masstree route 的 table value 从完整 `namespace_entry` 拆成小结构：

```cpp
struct route_value {
    entry_kind kind;
    inode_ref ref;
};
```

原因：如果直接把 `namespace_entry::name` 扩到 256B，而 Masstree leaf 仍以内联 value 形式持有 `namespace_entry`，即使目录 edge 是 layer pointer，`leafvalue` 的 union 也会按最大成员放大。这会使 Masstree leaf 节点超过 `threadinfo::pool_allocate()` 的小对象池上限，并且违背“route-only index”的内存目标。

### 28.3 当前结构含义

- Masstree route：
  - key：单个 component hash；
  - value：仅 legacy value-slot 使用的 `route_value`；
  - directory child：仍通过 layer edge 表示；
  - 不保存真实文件名/目录名。
- dentryBlock：
  - 保存 `component_hash`；
  - 保存 `name_length`；
  - 保存真实 name；
  - 保存 file inode ref 或 directory block ref。
- `namespace_entry`：
  - 仍作为 API 返回对象使用；
  - 现在可承载 255B name；
  - 不再作为 Masstree table value。

### 28.4 验证

已重新构建：

```text
cmake --build /home/zwt/zyb/build_release --target trace_replay -j 8
=> Built target trace_replay
```

已构造 255B 目录名 + 255B 文件名 smoke：

```text
namespace=/tmp/lhm_name255_test_20260601/ns.tsv
trace=/tmp/lhm_name255_test_20260601/trace.tsv
LHM_PERSISTENCE_DIR=/tmp/lhm_name255_test_20260601/persist
--no-restore-renames
```

结果：

```text
stat     2/2 success
readdir  1/1 success
rename   1/1 success
```

注意：默认 trace runner 会 restore rename；如果在同一 trace 中 rename 后立刻 stat 新路径，需要使用 `--no-restore-renames`，否则新路径会被恢复逻辑改回旧路径。

### 28.5 后续注意

当前设计支持到 255B，原因是 dentryBlock entry header 的 `name_length` 为 `uint8_t`。如果未来要支持超过 255B，需要进一步改为 `uint16_t name_length`，并重新评估 16KB dentryBlock 的 entry 容量、格式兼容和测试口径。

## 29. Directory inode 内嵌与 dentryBlock hash LRU（2026-06-01）

### 29.1 目标

本轮按以下顺序推进：

1. 将 `DirectoryBlockStore` 的 cache 从 vector 线性 LRU 改成 hash + list LRU。
2. 升级 directory block layout，在 block 头部加入 embedded directory inode。
3. 调整目录创建、目录 lookup/stat、目录 rename 的 inode 路径。
4. 跑小规模回归测试。

### 29.2 dentryBlock cache

旧实现：

```text
vector<cache_entry>
find by linear scan
evict by min(last_access)
```

新实现：

```text
std::list<cache_entry> cache_lru_
std::unordered_map<uint32_t, list::iterator> cache_index_
```

语义：

- `read_block(block_id)`：
  - map 命中后 splice 到 LRU 头部；
  - 未命中则读 `PersistentStore::ReadDirectoryBlock()`，必要时淘汰尾部。
- `write_block(block_id)`：
  - 命中则更新 image、标 dirty、移动到 LRU 头部；
  - 未命中则插入 dirty entry。
- 淘汰 dirty entry 时先 flush。

这样 cache lookup 从 `O(cache_blocks)` 降到均摊 `O(1)`，可以支持更大的 dentryBlock cache。

### 29.3 directory block layout

新 layout：

```text
metadata_common_block_header   64B
directory_block_header         64B
embedded inode_disk           256B
payload                     16000B
```

相比旧 layout：

```text
payload 16256B -> 16000B
```

容量下降约 1.57%，但点目录查询可以直接在 directory head block 中取目录 inode，避免再跳 inode block。

当前约定：

- file inode：仍存放在 inode block 中。
- directory inode：存放在该目录的 head directory block 中。
- directory entry：只保存 child directory 的 `head_block_id`。
- directory inode id：由 head block id 编码：

```text
dir_inode_id = (1ULL << 63) | head_block_id
```

这样目录 rename 不会改变 inode id，因为 head block id 不变。

### 29.4 路径调整

`mkdir(path)`：

```text
locate parent route
allocate child directory head block
initialize embedded directory inode in child head block
insert directory entry into parent dentryBlock
attach Masstree directory layer
```

目录不再占用 inode block slot。

`lookup/stat(directory)`：

```text
locate parent route
lookup directory entry in parent dentryBlock
get child head_block_id
read child head directory block
return embedded directory inode
```

`lookup/stat(file)` 不变：

```text
lookup file entry in parent dentryBlock
get inode_ref
read inode block
```

后续 30.x 设计调整后，目录 embedded inode 不再维护 `parent_inode_id`；
同名目录迁移只改 Masstree route tree，不再更新 directory block header。

### 29.5 验证

构建：

```text
cmake --build /home/zwt/zyb/build_release --target trace_replay -j 8
=> Built target trace_replay
```

手写 smoke：

```text
result=/tmp/lhm_dirinode_lru_regress_20260601
LHM_STAT_READ_INODE_BLOCK=1
```

结果：

```text
loaded_records=5
stat    5/5
readdir 2/2
rename  1/1
255B directory/file name covered
directory stat and file stat both covered
```

cache capacity=1 eviction smoke：

```text
result=/tmp/lhm_dirinode_lru_regress_20260601_cache1
LHM_DENTRY_CACHE_BLOCKS=1
loaded_records=19
stat    9/9
readdir 9/9
```

50K wide dataset smoke：

```text
namespace=/mnt/metaiotest/filepath/datasets/wide_50k_dirs1024.tsv
result=/tmp/lhm_dirinode_50k_wide_regress_20260601
LHM_DENTRY_CACHE_BLOCKS=128
loaded_records=50000
load_sec=19.225686
rss_kb_index_delta_after_load_clean=20548
stat    3/3
readdir 1/1
create  1/1
delete  1/1
rename  1/1
```

### 29.6 后续注意

当前 directory block layout 已变更，旧持久化文件不保证兼容；正式实验应使用 fresh `LHM_PERSISTENCE_DIR`。

当前 directory block cache 仍是单 mutex，hash LRU 解决了查找复杂度，但还不是最终并发 cache。后续如果并发吞吐瓶颈出现在 cache lock，应继续做 shard cache。

## 30. 2026-06-04 meta16 与子目录 dentry 去重

本阶段根据新的设计取舍做两点结构调整：

1. `directory_meta` 从 32B 压缩到 16B。
2. 父目录 dentry block 不再保存子目录 entry。

新的 `directory_meta` 只保留运行时热字段：

```cpp
struct directory_meta {
    uint32_t head_block_id;
    uint32_t tail_block_id;
    uint16_t flags;
    uint16_t layout_version;
    uint32_t reserved;
};
```

被删除的字段：

- `component_hash`：Masstree edge key 已经携带 component hash。
- `version`：并发版本应由 Masstree node version / directory block header 负责。
- `entry_count_hint`：文件 entry 统计属于 directory block header，不属于 route meta。

### 30.1 子目录名字位置

不引入内存 name arena。目录名字写在“子目录自己的 head directory block”中：

```text
child route meta -> child head_block_id
child head directory block -> self-name record + embedded directory inode
```

self-name 使用 directory block payload 中的特殊记录：

```text
dentry_kind::self_directory
```

该记录：

- 不进入普通 dentry index；
- 不计入普通文件 entry 数；
- 固定预留 `kMaxEntryNameBytes` 容量，保证 rename 到更长目录名时不搬动已有文件 entries；
- `read_entries()` 会跳过它。

### 30.2 操作路径变化

`mkdir(child)`：

```text
allocate child directory block
write embedded directory inode
write child self-name into child head block
create Masstree child layer with 16B directory_meta
```

不再执行：

```text
insert child directory entry into parent dentry block
```

`rename(directory)`：

```text
locate old parent route and child root
remove old parent layer edge
attach child root to new parent layer edge
if basename changes, update child head block self-name
```

目录 embedded inode 当前不维护 `parent_inode_id`：目录父子关系以 Masstree route tree 为准。
因此同名目录迁移（`/a/x -> /b/x`）是纯 route-tree 操作，不再写 child directory block，也不再更新 directory inode/header。
如果未来需要 `..`、fsck 或持久化恢复时从 inode 反查父目录，再重新引入 parent 指针或恢复日志。

不再执行：

```text
remove directory dentry from old parent block
insert directory dentry into new parent block
```

`readdir(parent)` 现在是两路合并：

```text
1. scan parent Masstree layer, enumerate direct child directories
2. read each child head block self-name
3. read parent dentry block chain, enumerate direct files
```

这符合当前取舍：优先保护目录 rename 和内存，不优先优化 readdir。后续如果 readdir 成为瓶颈，再考虑 directory-name cache 或批量预取。

### 30.3 验证

构建：

```text
make -C MasstreeLHM -B -j2
cmake --build build_release --target trace_replay -j2
```

语义 smoke：

```text
namespace=evaluation/platform/datasets/smoke.tsv
trace=/tmp/lhm_meta16_ops.tsv
restore_renames=0
result=/tmp/lhm_meta16_semantic_no_restore_20260604a/lhm
```

结果：

```text
loaded_records=1000
lhm_load_dir_success=20
lhm_load_file_success=979
stat   4/4
readdir 2/2
create 1/1
delete 1/1
rename 1/1
lhm_rename_success=1
lhm_dentry_inserts=1
lhm_dentry_removes=1
```

其中 `lhm_dentry_inserts/removes` 只来自临时文件 create/delete；目录 rename 未再修改父目录 dentry block。

### 30.4 同名目录迁移语义收敛

本阶段将“目录 rename”进一步区分为：

```text
directory reparent, same basename: /a/x -> /b/x
directory rename, basename changes: /a/x -> /b/y
```

当前优化目标是第一类：同名目录迁移。此时目录自身名字不变，child head block 中的 self-name 不需要更新；目录父子关系以 Masstree route tree 为唯一真相，directory embedded inode 的 `parent_inode_id` 固定置 0，不参与查询、readdir 或 rename 热路径。

实现后同名目录迁移路径：

```text
locate old parent route and child root
remove old parent edge
attach the same child root under new parent with the same basename hash
return
```

不执行：

```text
write child self-name
write embedded directory inode parent
remove/insert parent dentry block entry
```

如果 basename 变化，仍会更新 child self-name，以维持 readdir 返回名字正确；但仍不更新 directory inode parent。

验证：

```text
namespace=evaluation/platform/datasets/smoke.tsv
trace=/tmp/lhm_reparent_32.tsv
result=/tmp/lhm_reparent_32_result/lhm
ops=32 same-basename directory reparent
```

结果：

```text
rename success=32/32
rename avg=16.111us p50=3.507us p95=6.574us p99=399.143us
lhm_persist_block_write_ops=0
lhm_persist_bytes_written=0
lhm_dentry_block_writes=0
lhm_dentry_inserts=0
lhm_dentry_removes=0
```

对照旧版 all-entry-in-Masstree 的 controlled subtree rename 历史结果：

```text
subtree=1      p50=3.096us p95=4.444us
subtree=10     p50=1.153us p95=1.733us
subtree=100    p50=2.711us p95=3.874us
subtree=1000   p50=3.064us p95=4.475us
subtree=10000  p50=1.127us p95=1.513us
```

解释：当前同名 reparent 已恢复为零 dentry/persistence 写的 O(1) route-tree 操作；p50 量级与旧版接近。p95 略高，当前 smoke trace 仍包含 directory_meta/root 维护和较小样本冷启动噪声，后续正式实验应在同一 trace、同一热身策略下复跑。

### 30.5 当前阻塞的正式对比项

尝试用 `/mnt/metaiotest/filepath/datasets/lhm_v2/width_5m_name16.tsv` 前 50K/500K 文件主导样本跑当前版本宏测试时，50K 在 180s 内未完成，500K 无 timeout 长跑会话未及时返回。该问题不是本次 directory inode parent 改动导致，而是 width 数据集中极少目录承载大量文件，当前 directory block 插入/查找仍以顺序扫描为主，超宽目录会退化。

结论：在做“文件 entry 移出 Masstree”后的正式 width 对比前，必须先实现 directory block 内部索引；否则测试测到的是 block 内顺序扫描瓶颈，而不是 route-tree 内存结构本身。

## 31. 2026-06-04：directory-local extendible hash FileIndex prototype

本分支目录为 `MasstreeLHM-index`。本阶段实现 directory-local FileIndex，用于替代
普通文件 entry 移出 DRAM route tree 后的 directory block 顺序扫描。

### 31.1 现有抽象对应关系

当前代码中没有显式 `DirectoryRoot` 类名，现有抽象对应为：

```text
DirectoryRoot / DRAM route tree  -> MasstreeDirectoryIndex + directory layer root
DirectoryRoot metadata           -> directory_meta { head_block_id, tail_block_id }
DMB / directory block            -> DirectoryBlockStore + directory_block_image
file entry                       -> DirectoryBlockStore::entry(kind=file)
```

当前不把普通 file entry 插回 `MasstreeDirectoryIndex`；文件仍只在父目录的
`DirectoryBlockStore` FileIndex 中。子目录仍只通过 route tree layer 表示，不写入
父目录 FileIndex/DMB。

### 31.2 FileIndex layout

`DirectoryBlockStore` 新增 runtime FileIndex 状态：

```text
INLINE:
  小目录沿用已有 DMB chain，可线性扫描。

EXT_HASH:
  每个目录一个 runtime bucket directory。
  bucket directory slot -> bucket DMB block_id。
  bucket local_depth 保存在 runtime state。
  bucket DMB 复用现有 directory_block_image 和 block-local sorted index。
```

当前阈值：

```text
kInlineFileIndexEntryThreshold = 128
```

超过阈值后，目录从 INLINE 转换为 EXT_HASH。转换时读取旧 inline entries，分配新的
bucket DMB 作为初始 bucket；旧 inline DMB 不再作为 file lookup/readdir 主路径，
当前 prototype 不做 recovery，因此允许旧 block 成为历史数据。

### 31.3 Lookup / create / remove / readdir / rename 流程

`stat/open lookup`：

```text
1. 先通过 MasstreeDirectoryIndex 解析完整路径是否为目录。
2. 若不是目录，解析 parent directory root。
3. 在 parent DirectoryBlockStore FileIndex 中查 basename。
4. INLINE: 扫描 inline DMB chain。
5. EXT_HASH:
   hash(name) -> directory slot -> bucket DMB
   只读目标 bucket
   先用 fp16 过滤候选，再比较完整 filename
```

`create(file)`：

```text
route 到 parent directory root
不插入 DRAM route tree
插入 parent FileIndex
INLINE 超过阈值后转换 EXT_HASH
EXT_HASH bucket 满时只 split 当前 bucket
```

`mkdir`：

```text
route 到 parent
分配 child directory DMB 和 directory layer root
插入 DRAM route tree
不写 parent FileIndex/DMB
```

`readdir`：

```text
先扫描 route tree 的 child directory layers
再扫描 parent FileIndex 文件 entries
EXT_HASH 模式下扫描 unique buckets 做枚举
```

`directory rename`：

```text
只 detach/attach route tree layer
不修改 old_parent/new_parent FileIndex
不重写 descendants
同名 reparent 不更新 child self-name
```

`file rename`：

```text
从 old_parent FileIndex remove
插入 new_parent FileIndex
不进入 DRAM route tree
```

### 31.4 Extendible hash split 正确性

禁止使用 `hash % bucket_count` 的全局 resize。当前 split 逻辑：

```text
old_bucket full
if old.local_depth == global_depth:
  bucket directory double
  global_depth++
  仅复制 bucket pointers，不移动任何 entry

allocate new_bucket
old.local_depth++
new.local_depth = old.local_depth
更新原本指向 old_bucket 且新 depth bit=1 的 directory slots
只读取 old_bucket entries
清空 old/new bucket DMB
按新的 directory bits 重新分布 old_bucket entries
```

因此一次 split 的 entry movement 上界为 old bucket 当前 entry 数；不会随整个目录
entry 数增长。测试 hook 记录：

```text
file_index_last_split_moved
file_index_max_split_moved
```

### 31.5 Prototype 简化语义

当前 prototype 有意不做：

```text
crash recovery / journal replay / checkpoint recovery
完整 POSIX file-dir 同名冲突处理
bucket local_depth 持久化恢复
old inline DMB 转 EXT_HASH 后的空间回收
全路径字符串持久化身份
```

fingerprint 只作为候选过滤，不作为 correctness 判断。`find_entry_in_image()` 会在
fp16 命中后 decode record 并比较完整 filename。

### 31.6 测试

新增测试程序：

```text
MasstreeLHM-index/lhm_file_index_test.cc
```

覆盖：

```text
Small directory test
Large directory split test
Local split test
mkdir routing test
directory rename test
fp16 collision test
Width scalability microbenchmark: 10 / 100 / 1K / 10K / 100K
```

编译命令：

```bash
g++ -std=c++20 -O2 -g -pthread -I MasstreeLHM-index \
  -include MasstreeLHM-index/config.h \
  MasstreeLHM-index/lhm_file_index_test.cc \
  MasstreeLHM-index/compiler.cc MasstreeLHM-index/kvthread.cc \
  MasstreeLHM-index/memdebug.cc MasstreeLHM-index/persistent_store.cc \
  MasstreeLHM-index/str.cc MasstreeLHM-index/straccum.cc \
  MasstreeLHM-index/string.cc MasstreeLHM-index/string_slice.cc \
  -ljemalloc -lnuma -lm -o /tmp/lhm_file_index_test
```

验证结果：

```text
ASan: PASS
optimized: PASS

width=10     layout=INLINE   avg_stat_us=0.904
width=100    layout=INLINE   avg_stat_us=0.940
width=1000   layout=EXT_HASH avg_stat_us=1.486 max_split_moved=333
width=10000  layout=EXT_HASH avg_stat_us=1.715 max_split_moved=333
width=100000 layout=EXT_HASH avg_stat_us=220.843 max_split_moved=333
```

100K 的 optimized 单次结果偏高，主要受当前同步 DMB/cache/persistence prototype 和测试
环境噪声影响；但 point lookup 已经只定位目标 bucket，不再扫全目录 DMB chain。
