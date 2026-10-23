# ZB 专用 LHM 源码副本

2026-10-15 从 `/home/zwt/zyb/MasstreeLHM-index` 当前工作树复制，包含原有
block-index/ext-hash 和 rename profiling 修改，以及 ZB 集成步骤 2、3 的扩展。
仅复制源码、文档、构建输入和当前平台 config.h；不复制 Git 元数据、二进制或实验输出。
保留上游 LICENSE 和 AUTHORS。config.h 是当前 Linux 环境的配置快照，跨平台需重新配置。

此后 ZB 需要的 LHM 修改仅在本目录进行。不得修改原 MasstreeLHM-index，
也不得改变 MasstreeLHM 软链接。原目录已撤回本轮 ZB 集成修改，保留此前用户改动。

ZB 扩展集中于 lhm_namespace.hh：显式存储文件初始化、按 ID 读写 inode、
缓存一致的路径读取、目录删除标记、创建预留/提交/取消及完整属性更新。
ZB 属性编码和 C ABI 仍位于 ../../src/mds/lhm。

构建默认使用本目录。已有 CMake 缓存需显式更新：
`-DZB_LHM_SOURCE_DIR=/home/zwt/zyb/ZBStorage/third_party/lhm`。
构建与测试产物统一放在 /mnt/metaiotest/filepath。

第四步新增：可靠目录扫描、正式删除、完整 NamespaceStore 桥接，
以及 persistent_store.hh 的分配范围只读查询、directory_block_store.hh 的读错误观察；详见 ../../docs/design/lhm-namespace-store.md。
