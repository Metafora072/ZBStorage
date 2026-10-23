# 第四步：LhmNamespaceStore

本步完成可供 MDS 注入的 NamespaceStore 实现，MDS 默认仍是 RocksNamespaceStore。
所有原生修改仅在 ZBStorage/third_party/lhm；原 MasstreeLHM-index 不参与 ZB 编译。
第五步现已完成后端选择、统一冷热路由和真实 RPC 验收，见 [MDS 后端模式](lhm-backend-mode.md)。

## 接口与行为

- ResolvePath / GetInode / PutInode 复用完整属性编码，根 ID 为 1。
- 创建使用 ReserveCreate → NamespaceCreation::Commit，失败可 Cancel，析构自动取消。
  AllocateInodeId/Create 是 Rocks 旧式兼容入口，LHM 明确拒绝；LHM 的空间分配必须绑定路径和类型。
- Rename 禁止覆盖目标，目标与源相同返回 ALREADY_EXISTS，与现有 Rocks 行为一致。
  拒绝根、移入自身子树和文件充当目标父目录。移动文件或目录保持 ID；目录后代不逐项改 ID。
  原生路径修改成功后，在同一工作线程任务内同步完整属性中的 file_name，保留应用时间、
  版本及位置属性；保留原生修改后的结构引用和代际。底层写入失败返回错误，不承诺事务回滚。
- Remove 区分 unlink/rmdir，检查类型、非空目录和根目录。删除后原 ID 不可读，也不复用。
  原生正式入口 remove_path 执行解除路径和 inode 删除标记；原测试入口保留为兼容包装。
- Readdir 只列直接孩子，按名字字节序排序，start_after 排他，limit=0 无上限。
  has_more 表示还有后续项；next_token 为本页最后名称，空页为空。支持同一字符串作为
  start_after 输入和 next_token 输出。失败不向调用方返回部分条目。
- 接受重复斜线及末尾斜线；拒绝相对路径、NUL、`.`/`..`、超过 64 字节的名字。
  64 字节是 ZB 完整 inode 扩展的名字上限，按字节而不是字符计数。

路径定位通过 checked 入口观察目录块读取失败，避免将 I/O 失败误报为不存在；
inode 扩展解码错误和目录枚举失败同样返回后端错误。按 ID 读取使用分配范围检查，
区分未分配地址与范围内读失败，不增加全量 ID 索引。
ReserveCreate 的公共返回契约仍是对象或错误字符串；MDS 的创建 RPC 状态映射在第五步处理。

## 实现组织

- src/mds/lhm/LhmNamespaceStore.h/.cpp：NamespaceStore 适配，protobuf 输出与分页收集。
- LhmInodeStore：共享完整属性编码与创建对象，使用带状态码的桥接调用。
- LhmStatus.h：C ABI 状态到 MDS 状态映射。
- LhmNamespaceBridge.inc：由 LhmBridge.cpp 包含，复用内部 context/worker，避免暴露
  Masstree C++ 类型。检查和修改位于同一串行任务，防止多个 RPC 的检查/执行交错。
- third_party/lhm/lhm_namespace.hh：可靠目录扫描返回值、正式删除入口、按 ID 读错误输出。
- third_party/lhm/persistent_store.hh：分配地址范围查询。
- third_party/lhm/directory_block_store.hh：目录块读错误观察，避免把读取失败当作路径不存在。

目录分页首版每次收集并排序整个当前目录，时间 O(n log n)，暂态内存 O(n)。
没有性能结论，也没有加入恢复、事务或旧存储迁移。后端串行化不代表高并发吞吐已经优化。

## 验证

lhm_namespace_test 在同一进程中，对 Rocks 和 LHM 运行相同契约：创建、查询、属性更新、
跨目录文件重命名、移动含文件的目录、位置/时间/版本保持、分页排序和游标、非空目录、
类型错误、删除后文件/目录 ID 失效、预留析构取消。小型夹具覆盖跨 inode 块及多页读取。

LHM 专项覆盖循环移动拒绝、无效路径、预留冲突、父目录消失后的提交、游标输出别名、
4 个调用线程创建 100 个唯一 ID、同路径只能有一个预留，以及缺失扩展的解码错误传播、目录块读取失败注入与错误状态重置。
分别使用 L0=0 和 L0=1；旧创建、基础桥接、Masstree 共存、Rocks 契约和 ID 编码测试均通过。
本步没有修改 MDS 服务路径，没有运行或声称运行 LHM 的 RPC 测试。

结果：/mnt/metaiotest/filepath/results/zb_lhm_step4_20261015/summary.json。
复现：在既有外部构建目录构建 lhm_namespace_test，传入一个不存在的 /mnt 下结果目录：

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
CCACHE_DISABLE=1 cmake --build "$ZB_BUILD" --target lhm_namespace_test -j4
LHM_ENABLE_L0_CACHE=0 "$ZB_BUILD/src/mds/lhm/lhm_namespace_test" \
  /mnt/metaiotest/filepath/results/zb_lhm_step4_repeat_off
```
## 追加：目录子树扫描（2026-10-22）

`LhmNamespaceStore` 已提供 `BeginSubtreeScan`，返回可暂停的扫描对象。
它使用原生增量游标，不复用逐页全量读取/排序的 `Readdir`。
参数、生命周期、错误约定、实现和测试见 [子树扫描设计](subtree-scan.md)。
