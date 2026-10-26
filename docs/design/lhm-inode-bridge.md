# 第二步：真实 LHM inode 访问桥接

本文记录第二步边界；后续新增的完整属性与预留接口见 [第三步说明](lhm-creation-attributes.md)。

## 已实现的调用路径

桥接模块使用 `/home/zwt/zyb/ZBStorage/third_party/lhm`，由 ZB 的可选构建目标
`zb_lhm_bridge` 编译。MDS 的默认 RocksDB 后端没有切换。

```
zb_lhm_read_inode(id)
  → 检查编码、处理对外根 ID 1
  → LhmNamespace::read_inode_by_id(native_id)
  → 文件：PersistentStore::ReadInode(block, slot)
  → 目录：DirectoryBlockStore::read_directory_inode(head)
```

不建立 ID 映射表，不查询 RocksDB，不遍历命名空间树。根 ID 1 映射到实例持有的
根引用；不同时接受原生根 ID 作为外部别名。文件/目录 ID 均校验实际记录 ID、类型、
删除标记和 link_count。底层读取失败目前与不存在统一返回失败，细分状态留到后续接口适配。

`read_inode_by_id` 返回原生完整 `inode_disk`。桥接的普通 C 结构目前只导出本步
需要的 ID、size、mode、type、ctime_ns、mtime_ns、version，尚非完整 ZB InodeAttr。
`update_inode_by_id` 更新 size/mode/ctime/mtime，递增版本，保留身份、结构引用和其他字段。
UID/GID、ZB 文件位置等字段布局属于第三步，不伪装成已经完成。

`lookup_inode(path)` 经过路径索引得到 ID，再使用缓存一致的 inode 读取。
原来的 `lookup_entry_with_inode_block_direct` 为实验提供绕过缓存的读取，可能看不到
尚未刷盘的数据；保留其原有语义，不将它用作服务 Lookup。

## 目录与删除

目录读写经过 DirectoryBlockStore，避免绕过其脏块缓存。根目录还同步其 inode 槽位副本。
原先目录删除的 persist_delete_success 直接返回成功，导致按 ID 仍能读到旧记录；
现在与文件类似写入删除标记、link_count=0、tombstone 时间和新版本。

桥接 create/rename/remove 是本阶段功能验证入口，复用现有 LHM 操作；remove 只允许
文件和空目录，拒绝删除根。它们还不是正式的 NamespaceStore 实现，也没有完成全部
路径错误语义和创建预留的实现。现有 LHM fast_mode 保持默认设置。

## 构建、符号与线程

- 用共享库和普通 C ABI 隔离 Masstree/STL/protobuf 类型。
- ELF version script 仅导出 `zb_lhm_*`，其余符号局部化，并使用 -Bsymbolic。
- `-z defs` 要求没有未解析引用；LHM 单独使用自身 config.h，避免混用旧配置。
- 一个库级专用线程执行所有实例的 LHM 操作；各调用者同步等待完成，线程上下文不跨
  执行线程使用。此为正确性优先的串行实现，不代表并发吞吐能力。
- upstream threadinfo 没有析构/注销 API，桥接保留一个库生命周期的 threadinfo；
  不按请求或实例不断新建。命名空间 close 在工作线程上销毁并刷盘，工作线程退出时 join。
- 调用方必须保证 close 时该 context 没有并发调用，关闭后不得继续使用指针。
- 当前符号隔离构建支持 Linux ELF。未把共享库安装/部署到其他机器。

## 存储生命周期

LhmNamespace::initialize 新增可选显式 backing_file；老调用保留环境变量路径行为。
桥接强制显式传入文件路径，要求父目录存在，以 O_EXCL 拒绝覆盖已有文件。
只支持新建存储，不提供 reopen/recovery。初始化后实际读取根 inode，失败返回空指针，
不会把 LHM 内存回退当成成功的持久化实例。失败可能留下本次新建文件，调用者应记录失败，
不把它当成可重新打开的有效命名空间。

## 测试与复现

构建开关默认关闭：

```bash
ZB_BUILD=/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/build
cmake -S /home/zwt/zyb/ZBStorage -B "$ZB_BUILD" -DZB_BUILD_LHM_BRIDGE=ON
CCACHE_DISABLE=1 cmake --build "$ZB_BUILD" --target lhm_bridge_test lhm_coexist_test -j8
```

构建输出/日志须重定向到结果目录。测试需显式传入不存在的新目录：

```bash
ZB_RUN=/mnt/metaiotest/filepath/results/zb_lhm_step2_check
mkdir -p "$ZB_RUN"
LHM_ENABLE_L0_CACHE=0 "$ZB_BUILD/src/mds/lhm/lhm_bridge_test" "$ZB_RUN/cache_off"
LHM_ENABLE_L0_CACHE=1 "$ZB_BUILD/src/mds/lhm/lhm_bridge_test" "$ZB_RUN/cache_on"
"$ZB_BUILD/src/mds/lhm/lhm_coexist_test" "$ZB_RUN/coexist"
```

测试验证真实新建存储的路径/ID 属性一致、根映射、属性更新、ID 保持、同步、缺失/非法 ID、
删除后拒绝读写、多个调用线程和实例隔离。同步后直接读取元数据文件中由 ID 计算的字节偏移，
独立检查文件和目录的 size 字段。测试分别开启和关闭 L0；其余缓存采用当前 LHM 默认设置。
这些小型功能夹具不是性能实验，不创建替代 benchmark 数据集。

共存测试链接 ZB 原 Masstree 的真实 runtime：先写旧树，创建/查询新 LHM，关闭 LHM 后
继续读写旧树并检查旧 globalepoch/active_epoch 未被改变。额外符号哨兵测试故意在宿主
导出同名全局变量，验证没有被共享库绑定。

本次日志和摘要位于 `/mnt/metaiotest/filepath/results/zb_lhm_step2_20261015/`。
早期 functional.log 记录缓存绕过问题，修正后 functional2.log、cache_off.log、cache_on.log、
coexist.log 为通过记录。原 Rocks namespace 契约、ID 契约和 28 项 RPC 回归均通过。
