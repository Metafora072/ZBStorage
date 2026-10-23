# 第三步：作用域创建预留与完整 ZB inode 属性

## 实现边界

第三步实现 LhmInodeStore（当时尚非完整 NamespaceStore）：预留、提交、取消、完整 InodeAttr
按路径/ID 读写。MDS 的 Create/Mkdir 已使用公共 NamespaceCreation，但默认后端仍为
RocksNamespaceStore。LHM 的正式 Readdir、Rename/Remove 错误语义和后端切换在第四步以后。
本步不实现恢复、事务、已有 ZB 存储迁移或 Archive。
后续第四步已完成 LhmNamespaceStore，见 [命名空间适配](lhm-namespace-store.md)。

## 创建流程

```
ReserveCreate(path, type) → reservation + inode ID
  → 使用 ID 选择文件位置、准备完整属性
  → reservation.Commit(attr) → 完整 inode 写入 → 发布目录项
  → 任一提前返回：reservation 析构 → Cancel
```

公共接口位于 NamespaceStore.h。Rocks 兼容实现只预留递增编号，不提前写 D/ 或 I/；
取消后编号留空。MDS 在选择位置、构造属性或提交过程中提前返回时，预留对象自动析构。
不再在 MDS 内裸调用 AllocateInodeId。默认后端的 ID 序列和已有 RPC 行为保持兼容。

LHM 预留时分配真实槽位或目录头块，记录规范化路径、父 inode ID、类型及唯一令牌。
未发布 inode 带 reserved 标记；路径树没有目录项，按 ID 读取也拒绝所有 pending 对象。
提交再次检查目标不存在、父路径仍指向同一个父 inode、属性 ID/类型/名称正确且 nlink 非零。
完整属性先写入，再发布目录项。两步之间由已有桥接工作线程串行化，不存在其他桥接请求
观察半完成状态的窗口。pending 状态还保护写入后但发布失败的对象不被按 ID 访问。

取消将未发布空间放回对应类型的复用池，释放活跃预留记录。此处“释放”表示可再次分配，
不表示截短元数据文件或将块归还操作系统。已发布对象的编号仍不复用；未发布预留空间
可能获得相同 ID，但新令牌不同，旧令牌不能提交/取消新预留。没有新增全部 inode 的 ID 索引；
只保存活跃预留及取消池，按 ID 读取在有 pending 时检查这些暂态记录。

提交失败保留令牌，允许修正属性后重试或取消。显式 Cancel 返回取消结果；RAII 析构执行
尽力取消，若底层写入持续失败，pending 记录保留到实例关闭以维持不可见性。这里没有承诺
设备 I/O 故障下的事务回滚。测试注入的是“inode 已写入、目录项尚未发布”的流程失败。

LhmInodeStore 的预留对象持有共享 context，即使上层 store 对象先销毁，也不会悬空引用。
公共 Rocks 兼容预留对象借用 store 指针，要求 store 活得更久。

## 完整属性布局

不另建属性数据库或全量属性内存表。原生 inode_disk 仍为 256 字节，其中 164 字节 reserved
区域使用 161 字节保存显式小端编码。LhmAttributeCodec.h 定义格式，禁止直接序列化 C 结构体。

| 原生 inode 字段 | 对应 ZB 属性 |
| --- | --- |
| inode_id/type | ID 与文件/目录类型；桥接转换根 ID 1 |
| size_bytes/mode/link_count | size/mode/nlink |
| ctime_ns/mtime_ns | ZB 秒值乘 10^9；写入前检查溢出 |
| stable_version | 初始值来自 ZB version；保留作为原生结构版本 |
| reserved[0..160] | 版本化的 ZB 扩展字段 |

扩展依次保存：magic `ZBA1`、布局版本 1、名字长度、虚拟节点标志、存储层级、归档状态、
uid、gid、atime、object_unit_size、replica、disk_node_id、disk_id、optical_node_id、
optical_disk_id、optical_image_id、blksize、blocks_512、rdev_major/minor、ZB version、64 字节名字。
末尾 3 字节清零保留。名字仍保存在目录项中；扩展中的名字副本用于满足 ZB 按 ID 返回
file_name 的协议。第四步的正式 rename 必须同步这份名字，不能直接复用只修改路径树的入口。

ZB version 单独保存在扩展中，避免原生 rename 等结构版本增长改变应用版本语义。
C ABI 只传普通数值和长度限定的 64 字节名字，protobuf 转换位于独立静态适配库
zb_lhm_inode_store；共享库内部仍不引入 protobuf 类型或 ZB 的旧 Masstree。

范围检查：mode <= 65535，mtime/ctime 秒值可无溢出转换成 uint64 纳秒，名字 <=64 字节，
类型/层级/归档状态合法，nlink 非零。其余数值按 protobuf 声明宽度无损保存。
更新完整属性保留原生 ID、parent/reference、代际和删除状态；不能通过 PutInode 修改名字。
缺少扩展 magic/version 的旧原生 inode 不被伪装为完整 ZB 属性；新建桥接实例的根会初始化扩展。
第二步的原生基础测试接口仍可用，但其创建的 inode 不保证具有完整 ZB 扩展。

## 测试

lhm_creation_test 使用小型功能夹具，分别在 L0 开/关下验证：

- 26 个 protobuf 字段全部往返一致，覆盖超出 32 位的 ID/位置/大小及 64 字节名字；
- 完整属性更新和同步后读取一致；直接读取元数据文件中的扩展 magic；
- 预留对象按路径/ID 不可见，模拟放置失败的析构取消不留下活跃预留；
- 文件/目录连续取消 25 次仍复用同一未发布空间；
- 重复预留、重复提交、错误类型/名称、长度/权限/时间溢出被拒绝；
- 父目录在预留后被删除时提交失败，随后可取消；
- inode 写入后的发布故障注入，取消、空间复用和再次提交成功；
- 旧令牌不能操作新预留。

故障注入入口仅在 BUILD_TESTING 下编译。第二步的基础读写、缓存、原 Masstree 共存测试
及 Rocks 契约回归同时运行。正式错误码分类、分页和完整名称重命名在后续阶段验收。

## 构建与运行

沿用外部构建目录；所有输出位于 /mnt/metaiotest/filepath：

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
ZB_RUN=/mnt/metaiotest/filepath/results/zb_lhm_step3_check
mkdir -p "$ZB_RUN"
cmake -S /home/zwt/zyb/ZBStorage -B "$ZB_BUILD" -DZB_BUILD_LHM_BRIDGE=ON \
  > "$ZB_RUN/configure.log" 2>&1
CCACHE_DISABLE=1 cmake --build "$ZB_BUILD" \
  --target lhm_creation_test lhm_bridge_test lhm_coexist_test mds_server namespace_store_test -j8 \
  > "$ZB_RUN/build.log" 2>&1
LHM_ENABLE_L0_CACHE=0 "$ZB_BUILD/src/mds/lhm/lhm_creation_test" "$ZB_RUN/cache_off"
LHM_ENABLE_L0_CACHE=1 "$ZB_BUILD/src/mds/lhm/lhm_creation_test" "$ZB_RUN/cache_on"
```

cache_off/cache_on 必须为新目录。这不是性能实验，不生成替代实验数据集。
本轮实际记录：`/mnt/metaiotest/filepath/results/zb_lhm_step3_20261015/`。
