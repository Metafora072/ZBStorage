# vdisc 打包与刻录 任务文档

## 1. 目标
在光盘节点把多个 vimg 打包成一个 vdisc 文件，并以 vdisc 为单位完成刻录。

## 2. 现状
- 刻录链路目前是「1 个 vimg = 1 个 BURN 任务」：`ZipTaskProcessor` 产出 vimg → 建 BURN 任务 → `BurnTaskProcessor` 提交 cd_manager → 完成后按单个 `volume_id` 释放。
- 读路径已按「一张光盘包含多个镜像」建模：`RequestAsyncReadFile(disk_id, image_id, inode_id)`。
- `image_dir_manager` 只识别 `volume_<id>.vimg`，且以「文件数」统计容量占用。
- MDS 侧的 ID 分配与两次上报接口尚未实现。

## 3. 需求

### 3.1 vdisc 文件格式
- 由三段构成：**超级块**、**vimg 索引表**、**vimg 数据区**。
- 超级块：记录 vdisc 格式版本（便于后续演进）、光盘身份、各区域位置与长度、文件总长度，并带自身完整性校验。
- 索引表：每个 vimg 一条记录，含 `volume_id`、在 vdisc 中的偏移与长度、该 vimg 的 SHA-256。
- 数据区：各 vimg 原始字节按索引顺序依次存放。
- 索引区与数据区起始位置按光盘扇区（具体值待定，先默认为2048B）对齐。

### 3.2 分组规则
- 新增配置项：**固定条数 n**、**单盘容量（字节）**。
- 待刻录 vimg 数量达到 `n+1` 即开始打包，从`n-1`开始尝试，最高到`n+1`，若实际打包数不足`n+1`，剩余的镜像留待下一次。
- 单盘容量为硬约束：再加入一个 vimg 会超容量则立即封盘。
- 长期不足 `n-1` 时，节点停止或超时需强制封盘，避免 vimg 长期滞留不刻。

### 3.3 存储与容量占用
- vdisc 生成在 `image/` 下，命名 `disc_<disc_id>.vdisc`。
- 打包成功后删除已被包走的 vimg 文件。
- `image/` 的容量统计仍以 vimg 计量：一个含 k 个 vimg 的 vdisc 占用 k 个镜像位。
  - `image_dir_manager` 需能识别 `.vdisc`，并支持「单条记录占用多个镜像位」。
- cd_manager 刻录完成后：vdisc 移动到 `disc_sim/`，同时解除其占用的 k 个镜像位。
  - `disc_sim/` 中的 vdisc 是光盘的持久副本，长期保留，不因读取而移除。
- `image/` 中 vimg 的换出（LRU / 容量淘汰）**直接删除**，不再移动到 `disc_sim/`。

### 3.4 刻录单元
- **vdisc 取代 vimg 作为刻录单元**：BURN 任务面向 `.vdisc` 文件，光盘身份使用 `disc_id`。
- 刻录完成后按该 vdisc 的索引一次性释放其中全部 `image_id`。

### 3.5 校验
- 每个 vimg 的校验码使用 **SHA-256（OpenSSL 实现）**。
- 提供两类能力：读取 vdisc（超级块 + 索引表）、逐 vimg 校验 SHA-256；挂载/刻录前可调用。

### 3.6 元数据落盘（`meta/disc_meta`）与内存索引
- vdisc 内的超级块与索引**仅用于迁移/离线校验**时读取；节点日常运行的定位信息一律取自磁盘上的 `meta/disc_meta`。
- `meta/disc_meta` 为**单文件**，多个 vdisc 的元数据以**追加**方式写入，每次刻录追加一条记录。
- 每条记录至少包含：`disc_id`、该盘内各 vimg 的 `volume_id` / 偏移 / 长度 / SHA-256，以及记录自身的长度与完整性校验，便于追加解析与损坏时截断。
- 在 `optical_node_manager_structs.h` 中新增数据结构 `DiscMetaStore`（命名可调整），提供**读共享、写互斥**语义：`disc_id -> DiscMeta` 的映射 + 读写锁，用于按 `disc_id` 快速查找元数据。
- 节点 `Run()` 启动时检查 `meta/` 下是否存在 `disc_meta`：存在则读入内存重建 `DiscMetaStore`；尾部记录不完整或校验失败时截断到最后一个完整记录。
- 读取路径通过 `DiscMetaStore` 由 `disc_id` 直接定位到目标 vimg 的偏移/长度，**一次读取只读取该一个 vimg**，不整盘扫描或整盘读取。

### 3.7 刻录完成时的元数据同步
- 在刻录完成回调中、**把 `image/` 下的 vdisc 移动到 `disc_sim/` 之前**，完成两件事：
  1. 将该 vdisc 的元数据**追加写入** `meta/disc_meta` 并落盘；
  2. 同步写入内存中的 `DiscMetaStore`。
- 顺序要求：元数据落盘 → 更新内存 → 移动 vdisc 文件，避免中途崩溃导致元数据缺失。

### 3.8 读盘路径（从光盘库读取）
- 读请求的定位信息为 `disc_id` + `volume_id`（+ `inode_id`）。
- 读取步骤：
  1. 由 `disc_id` 定位 `disc_sim/` 下的 vdisc 文件（`disc_<disc_id>.vdisc`）；
  2. 由 `volume_id` 在 `DiscMetaStore` 中查出目标 vimg 在该 vdisc 内的偏移与长度；
  3. 从 vdisc 中读取该 vimg 的**实际大小**字节；
  4. 将该 vimg 复制一份到 `image/` 并登记为读镜像，后续挂载与分片读取沿用既有流程。
- `disc_sim/` 中的 vdisc 在读取过程中与读取后均**不移除、不修改**。
- `image/` 中 vimg 的换出**直接删除**，不再产生 `disc_sim/` 下的新文件；`disc_sim/` 只承载 vdisc。

## 4. 约束
- 不改变 `cd_manager_sim` 的调度模型（仍需文件路径与大小即可）。
- 不引入除 OpenSSL 外的新依赖；整体 CRC 复用已有的 zlib。
- vdisc 一次写定，不要求增量更新。
- `disc_meta` 只追加、不复写；追加写入需串行化（与 `DiscMetaStore` 的写互斥一致）。

## 5. 验收标准
1. 给定一组 vimg，能产出单个 vdisc，索引中的 `offset` / `size` / `sha256` 与实际内容一致。
2. 对 vdisc 逐 vimg 校验，能检出被篡改或损坏的 vimg。
3. 封盘条数在 `[n-1, n+1]` 内，且不超出单盘容量。
4. 打包前后与刻录完成后的 `image/` 占用计量、`disc_sim/` 归属符合 3.3。
5. 重启后能由 `meta/disc_meta` 重建全部 disc 元数据，按 `disc_id` 查询结果与写入前一致。
6. 新增 vdisc 的元数据在其被移入 `disc_sim/` 之前已完成落盘。
7. 一次读请求只读取一个 vimg 的数据范围。
8. 读盘完成后 `disc_sim/` 中的 vdisc 保持不变，且 `image/` 下出现可读的该 vimg。
9. `image/` 中 vimg 被换出后文件被删除，且不会在 `disc_sim/` 下产生新文件。

## 6. 暂不包含
- MDS 的 `AllocateAvailableDiscId` / `ReportFilesPackedToImage` / `ReportImagesBurnedToDisc` 接口实现。
- 纠删码、坏盘/坏道与介质损坏处理。
- 图片跨 vdisc 切分。
- `disc_meta` 的压缩、归档与历史清理。
