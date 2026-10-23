# 命名空间后端拆分评估与接口边界

2026-10-22 增量：已增加 `BeginSubtreeScan / NamespaceScanner::Next`，支持分批遍历
目录子树；LHM 使用增量桶/目录游标，见 [子树扫描设计](subtree-scan.md)。
本文件以下内容保留各历史阶段的拆分背景。

后续范围已扩展为热命名空间和 Masstree 冷命名空间统一迁移，见
[LHM 实施计划](lhm-migration-plan.md)。下文保留第一轮拆分背景；其中“需要 ID 映射”
仅适用于保留 ZB 旧编号的方案。新建 LHM 存储优先使用可解码的位置编号，不新增 ID 索引。

## 抽象前评估（2026-10-13）

基于原版 main `a8faecedf4e940cc032f996bf9fc6862d28b5440`。
原版已构建并启动 `mds_server` 和 `real_node_server`，28 项本机 RPC 功能检查通过。
涵盖根目录、创建、重复创建、分页目录、属性更新、文件位置、目录及文件重命名、
非空目录拒删、删除后不可查询。未验证 FUSE 挂载、文件内容读写、集群调度和光盘归档。
基线记录在 `/mnt/metaiotest/filepath/results/zb_namespace_refactor_20261013/baseline_local/`。

### 结论

可行。先隔离热命名空间的语义操作，再实现 LHM adapter。
不要把 RocksDB Get/Put/Iterator 作为公共接口；LHM 应收到完整路径，自主执行路径定位。
事务和恢复不属于本轮设计范围。RocksDB 实现沿用现有编码及批量写入。

当前 ZB 热路径为 `D/<parent>/<name>` 逐级解析后读取 `I/<inode>`；
冷路径由 MetaStoreRouter 按归档路由或 Masstree 路由分派。
MdsServiceImpl 同时负责参数、放置策略、节点清理和直接 RocksDB 命名空间写入，需要拆开。

### 对照本地 LHM

默认实现是 `/home/zwt/zyb/ZBStorage/third_party/lhm/lhm_namespace.hh`。

| ZB 所需能力 | LHM 当前入口 | 接入缺口 |
| --- | --- | --- |
| 完整路径查询及属性 | lookup_entry / lookup_entry_with_inode_block_direct | inode_disk 到 ZB InodeAttr 字段映射 |
| 创建目录、文件 | mkdir / creat_file | 注入 ZB 属性及稳定 inode ID，映射已存在/父目录不存在等错误 |
| 目录分页 | readdir | 当前返回完整 vector；需按名称排序、start_after 和 limit，区分空目录与失败 |
| 重命名 | rename_path | 保持 ID、属性和数据位置，核验目标已存在及目录移动语义 |
| 删除 | remove_path_for_test | 提升为正式删除接口，区分 unlink/rmdir，拒绝非空目录 |
| 按 ID 查询、属性更新 | 无直接等价公共入口 | 建立 inode ID 到引用的映射并提供完整属性读写 |

LHM 自行分配 inode_ref，不能直接把它当成 ZB 的 uint64 inode ID。
两者 256 字节 inode 布局也不相同；需要字段转换，不能 memcpy。
目录名长度、路径规范化和错误语义应单独对齐。ZB 名称编码限制应在 adapter 中遵守。
两份 Masstree 的全局符号/线程上下文需要隔离后才能链接；本轮不链接 LHM。

## 本轮实现边界

NamespaceStore 提供完整路径 ResolvePath、Create、Rename、Remove、Readdir，
以及 GetInode、PutInode、AllocateInodeId。公共接口只使用语义字段，不暴露 RocksDB
key、iterator、batch 或 UnifiedInodeRecord。属性暂用现有 protobuf 类型，避免改变 RPC 协议。
Create 接收已分配 ID 和已选好的数据位置；未来 LHM adapter 需要维护该 ID 与内部 ref 的映射。
只读查询沿用现有 bool + error 约定：未找到为 false 且 error 为空，存储错误为非空。
修改操作返回明确的 MdsStatusCode 和 message。

RocksNamespaceStore 收拢热命名空间编码与访问；MetaStoreRouter 保留冷路径分派，
热路径委托 NamespaceStore。MdsServiceImpl 保留 RPC、放置策略和节点清理，
构造器允许注入后端，默认仍为 RocksNamespaceStore；注入对象必须比 service 活得更久。

这不是整个 ZB 的无 RocksDB 改造。CMS/PG、句柄、归档路由、归档/迁移操作及 inode 扫描
仍有直接 RocksDB 依赖；尤其高级操作仍直接读写 I/ 记录。以后启用 LHM 前，必须
迁移这些访问或明确禁止这些高级功能，不能仅替换后端就宣称整个系统已兼容。
基本 namespace 操作的隔离是第一阶段，不引入未实现的 LHM 运行开关。

## 验证方法

使用 `tests/mds/namespace_rpc_test.py` 在全新 work-dir 启动真实进程，
抽象前后执行同一组检查。所有运行数据必须置于 `/mnt/metaiotest/filepath`。
这是一组小型 API 功能夹具，不生成实验数据集、不测吞吐量或内存占用。
构建使用外部 Release 目录；禁用 ccache，并显式使用系统共享 LevelDB，
以避开本机 `/usr/local/lib/libleveldb.a` 的 Snappy 链接缺失。

本次最终验证：原版与抽象版均通过 28 项 RPC 检查；排除运行时的时间戳和节点端口后，
两组请求及响应一致。独立 `namespace_store_test` 验证位置/属性保留、分页、删除类型、
热目录项到冷 inode 的 resolver 回退和损坏记录错误传播。完整归档流程仍未验证。
结果摘要为上述结果目录的 `summary.json`，详细 RPC 为 `refactored_final/result.json`。

RocksNamespaceStore 的可选 InodeReader 只用于路径/目录结果的属性解析，以保留原版
跨冷热层的 inode 回退；它的 GetInode 始终只读热后端，避免 router 回调递归。
基本删除由后端一起删除 inode 和内嵌位置，service 只继续清理归档控制状态。
位置查询的备用读取也经过 MetaStoreRouter。剩余直接 I/ 访问包括
CommitFileMigration、SaveDiskFileLocation、SaveOpticalFileLocation，
VerifyNodeReferences 仍扫描 RocksDB；归档及导入模块还有各自的直接访问。
并发压力、全部异常路径与多节点运行未纳入本轮验证。

### 在本机复现

依赖为 CMake、Ninja、C/C++ 编译器、Protobuf/protoc、LevelDB、Snappy、OpenSSL、
gflags、FUSE3、pkg-config、zlib；先初始化项目的 brpc/rocksdb submodule。
下例的 `ZB_RUN` 可改成新的结果目录；两个测试子目录必须尚不存在。

```bash
cd /home/zwt/zyb/ZBStorage
git submodule update --init --depth 1
ZB_RUN=/mnt/metaiotest/filepath/results/zb_namespace_check
mkdir -p "$ZB_RUN"
cmake -S . -B "$ZB_RUN/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_FLAGS_RELEASE='-O2 -DNDEBUG -g0' \
  -DCMAKE_CXX_FLAGS_RELEASE='-O2 -DNDEBUG -g0' \
  -DBUILD_TESTING=ON -DDOWNLOAD_GTEST=OFF -DFAIL_ON_WARNINGS=OFF \
  -DWITH_ALL_TESTS=OFF -DWITH_BENCHMARK_TOOLS=OFF -DWITH_CORE_TOOLS=OFF \
  -DWITH_LIBURING=OFF -DWITH_TESTS=OFF -DWITH_TOOLS=OFF \
  -DLEVELDB_LIB=/usr/lib/x86_64-linux-gnu/libleveldb.so \
  -DLEVELDB_INCLUDE_PATH=/usr/include > "$ZB_RUN/configure.log" 2>&1
CCACHE_DISABLE=1 cmake --build "$ZB_RUN/build" \
  --target mds_server real_node_server namespace_store_test -j8 \
  > "$ZB_RUN/build.log" 2>&1
"$ZB_RUN/build/namespace_store_test" "$ZB_RUN/contract" > "$ZB_RUN/contract.log" 2>&1
python3 tests/mds/namespace_rpc_test.py --build "$ZB_RUN/build" --work-dir "$ZB_RUN/rpc"
```

RPC 测试自动启动和关闭本轮服务，通过 localhost 访问临时端口；需要允许 socket 的执行环境。
此处 LevelDB 路径针对当前 x86_64 Debian/Ubuntu 环境，其他环境需调整。
