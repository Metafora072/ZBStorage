# LHM 批量构建命名空间

## 使用入口

仅用于新建空 LHM 存储。先读入清单并检查，再批量构造块，最后发布运行时索引；不调用逐条 ReserveCreate/Commit。导入完成后沿用 NamespaceStore 的查询、分页、扫描、创建、改名、删除接口。

清单是 UTF-8 TSV，使用 LF 换行，第一行为 `type<TAB>path`，只包含这两列：

```text
type	path
dir	/
dir	/a
file	/a/f1
dir	/a/empty
file	/root-file
```

- 第一条必须是 `dir /`，目录必须显式列出，父目录先于子项。
- DFS、BFS 或混合顺序均可；文件 inode 会按父目录分组排列，组内保持输入顺序。
- 路径必须绝对且规范，不接受重复路径、尾部斜杠、重复斜杠、`.`、`..`、NUL、CR 或额外列。每个分量最多 64 **字节**，不是 64 个字符。
- 默认文件大小 0、mode 0644；目录 mode 0755；uid/gid 0，nlink 文件 1 / 目录 2，version 1；时间取导入时间。尚未分配数据节点和光盘位置。
- 输入只构造元数据，不创建文件内容，不执行数据放置；不能凭导入成功宣称文件内容已经可读。

MDS 配置增加：

```ini
NAMESPACE_BACKEND=lhm
LHM_STORE_FILE=/mnt/metaiotest/filepath/results/<run>/scratch/namespace.meta
LHM_BULK_MANIFEST=/absolute/path/to/namespace.tsv
ENABLE_OPTICAL_ARCHIVE=false
```

其他 MDS 配置保持原有要求，控制库也必须满足现有 fresh-store 限制。不设置 `LHM_BULK_MANIFEST` 时仍按原来的空库方式启动。RocksDB 后端不能设置此参数。

直接在 C++ 中使用：

```cpp
zb_lhm_bulk_stats stats{};
std::string error;
zb::mds::LhmNamespaceStore ns(fresh_file, manifest_file, &stats, &error);
if (!ns.ready()) { /* report error */ }
```

## 构建过程与代码位置

1. `src/mds/lhm/LhmBulkBridge.inc` 校验 TSV，用临时数组下标记录父子关系。这些下标只在构建过程中使用，不成为持久 inode ID，也不建立运行时 inode-ID 映射表。
2. `third_party/lhm/namespace_bulk_build.inc` 分配目录头块，将目录属性写入嵌入 inode，记录目录自身名称。所有父目录获得最终 ID 后，再准备文件 inode。
3. `PersistentStore::WriteFreshFileInodes` 从 ticket 1 起排列文件；ticket 0 保留 root。每个 inode 块有 63 个槽，组装完整块后提交。文件 ID 仍是 `(block_id << 32) | slot`；普通目录仍是高位标记加目录头块号；外部 root 仍为 1。
4. `third_party/lhm/directory_bulk_build.inc` 在内存中按文件名哈希低位分桶，实际序列化长度决定桶是否容得下。超出容量则分区，最终桶一次提交完整内容，不逐文件触发磁盘桶分裂；保留空兄弟桶，兼容后续普通插入。沿用最大哈希深度 20，超过时构建失败。
5. 所有块 flush/sync 后，将准备好的 ext-hash 路由安装到目录，按父子顺序建立目录 Masstree。成功后才将 context 交给调用方或启动服务。
6. 更新 inode 分配游标，失效旧 TLS 预留区和 L0 块；普通增删改继续使用同一套地址和块格式。

`inode_blocks` 统计含 root 的有效 inode 块数，`buckets` 为文件索引桶数，**不是实际 write 系统调用次数或文件物理占用**。现有 root 初始化和目录块分配仍可能产生空块初始化写入；最小存储还包含分配器预留块。没有声称整个构建只进行一次顺序磁盘写入。

测试同时发现并修复两项兼容问题：销毁存储时清除当前工作线程的 TLS 缓存指针，避免对象地址复用后访问已释放缓冲区；目录索引 root() 返回分裂后的真实根，避免宽根目录的遍历遗漏。这些修改仅在 ZB 专用 LHM 副本。

## 失败及范围

- 使用 O_EXCL 创建新文件，已有输出不会覆盖。清单校验失败时不创建输出。
- 构建失败销毁 context，删除本次创建的输出。MDS 不发布半成品。不做事务、恢复、断点续建、并发导入或在线合并。
- **进程退出后不能直接重新打开这个文件恢复索引。** 本功能是当前进程启动时构建并立即使用，仍遵循现有 LHM fresh-store 模式。
- 当前清单、父子关系、待写 inode 和桶计划驻留内存，内存随条目数和路径总字节增长；尚未实现外部排序或限内存流式构建。
- inode ID 由内部地址分配，调用方应使用 ResolvePath/GetInode；输入不接受自定义 inode ID。

## 测试

```bash
cmake --build "$BUILD" --target lhm_bulk_import_test lhm_namespace_test subtree_scan_test mds_server -j 4
"$BUILD/src/mds/lhm/lhm_bulk_import_test" "$RUN/scratch/default"
LHM_ENABLE_L0_CACHE=0 LHM_DENTRY_CACHE_BLOCKS=0 \
  "$BUILD/src/mds/lhm/lhm_bulk_import_test" "$RUN/scratch/cache_off"
"$BUILD/src/mds/lhm/lhm_namespace_test" "$RUN/scratch/namespace"
"$BUILD/src/mds/lhm/subtree_scan_test" "$RUN/scratch/subtree"
python3 tests/mds/lhm_bulk_startup_test.py --build "$BUILD" \
  --manifest "$RUN/scratch/default/tree.tsv" --work-dir "$RUN/scratch/startup"
```

`RUN` 应是 `/mnt/metaiotest/filepath/results/` 下唯一的新运行目录，先创建其 scratch 父目录，各测试目标目录必须不存在。输出和日志应保存在该运行目录；测试生成的是小型功能 fixture，不是性能实验数据集。

覆盖非法清单和已有输出保护、0/1/62/63/64/126/256 文件的槽位边界、重复实例生命周期、650 文件宽目录、60 个根子目录、20 层嵌套、空目录、64 字节名称；对照逐条创建及输入清单验证路径/ID 点查、默认属性、分页目录和批量子树扫描；随后修改属性、跨目录移动、删除、重新创建及触发新桶分裂。RPC 测试启动真正 MDS，核对整棵导入树并执行 mkdir/rename/rmdir，退出时关闭服务。
