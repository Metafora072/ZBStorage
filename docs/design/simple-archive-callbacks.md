# 简化归档：ID 分配、完成上报和尾批封装

本轮接通真实光盘引擎与 MDS 的四个接口，以及归档发送流的结束标记。
采用单 MDS、单次归档任务、全新运行目录；归档期间文件及目录结构保持不变。
不实现事务、冲突协调、失败重试、重启恢复或跨实例唯一编号。

## 启用

物理光盘节点配置增加：

```ini
SIMPLE_ARCHIVE_MDS_ENABLED=true
MDS_ADDRESS=127.0.0.1:9000
NODE_ID=optical-simple
NODE_ADDRESS=127.0.0.1:9200
```

`MDS_ADDRESS` 是已有配置项。新开关默认 false，保持原独立光盘测试的本地编号行为；
启用时镜像/光盘编号来自 MDS，分配失败不会回退到本地编号。
MDS 和光盘节点必须同时升级：旧物理节点不识别空的结束标记。
镜像容量及封装阈值继续由光盘节点的 VOLUME_SIZE_BYTES、SIZE_THRESHOLD 决定，
光盘容量使用 DISC_CAPACITY_BYTES。MDS 不按原始文件大小提前划分镜像。

## 四个 RPC

- AllocateAvailableImageId(node_id, node_address, allocate_ts_ms) -> image_id。
- AllocateAvailableDiscId(node_id, node_address, allocate_ts_ms) -> disc_id。
- ReportFilesPackedToImage(node_id, image_id, inode_ids) -> status, updated_files。
- ReportImagesBurnedToDisc(node_id, disc_id, image_ids) -> status, updated_files。

数字编号共用单 MDS 进程内递增序列，对外分别包装为 img-N 和 disc-N。
保留现有 inode 格式，因此当前分配范围为 1..UINT32_MAX，耗尽报错；不冒充完整
uint64 空间。重启后编号重新开始，不能对旧光盘存储重新启用此简单模式。
分配记录只保存编号及所属节点；地址和时间戳由协议接收，暂不持久保存或用于路由。

第一次上报直接通过 NamespaceStore::GetInode/PutInode 写 optical_image_id，
清零 optical_disk_id，状态写新值 INODE_ARCHIVE_IMAGE_READY=3。
保留旧 PENDING=0、ARCHIVING=1、ARCHIVED=2 的数值与含义；ARCHIVED 表示刻录完成。
LHM 编码允许新状态，不修改原始 LHM，只修改 ZB 的属性适配层。
第二次上报写 optical_disk_id 和 INODE_ARCHIVE_ARCHIVED。
Rocks 对照后端的 v3 inode 在原有 64 字节保留区增加 ARC1 标记的归档副本字段
（标记 4B、镜像 4B、光盘 8B、节点 8B），主位置仍用原位置编码；旧零保留区记录照常读取。
旧版本程序重写记录会丢失该扩展，不支持带新归档状态的降级运行。
按文件顺序更新，不回滚；updated_files 表示已完成写入数，包括错误返回时的部分进度。

本轮尚未引入 image_id -> inode_ids 反向索引。第二次上报通过整个根目录的
分批扫描查找匹配 optical_image_id 的文件，代价 O(命名空间文件数)，内存使用有界。
这是下一步设计索引前的功能基线，不应当作为最终高效实现。
归档节点归属暂存在 MDS 的分配记录中；本轮不改 optical_node_id、不迁移 DL/OL，
不切换 storage_tier、不删除热副本，不宣称已接通归档副本的客户端读路由。

## 尾批结束

SendArchiveMetadataRequest 新增 end_of_batch。目录发送成功后，MDS 单独发送
files 为空、end_of_batch=true 的请求；空目录不发送。文件分片进度计数不包含结束标记，
结束标记失败则整个发送返回错误，已确认文件数量保留。

光盘节点按 FIFO 消费文件下载请求，再向同一压缩队列提交 ARCHIVE_FLUSH 任务。
该任务在先前 WRITE 处理后运行：封装未满镜像、向 MDS 上报、纳入光盘、封盘提交刻录。
入队成功不等于刻录完成；最后的 MDS 状态更新来自刻录完成回调。
结束标记只承诺当前单任务流的尾部处理，不是多任务隔离机制；也不提供重启重放保证。

正常打包和尾批打包都调用两次 MDS 上报。分配/上报调用关闭 brpc 自动重试。
镜像上报失败使相应 WRITE/FLUSH 任务失败；刻录上报失败记入节点诊断和日志，
不伪造 MDS 更新成功。原光盘引擎已有的刻录调度行为保持不变。

同时修正了卷容量溢出路径：一次 AddFileToCollect 最多封装一个镜像；
若先封装已有文件，当前文件留给下一镜像，避免同一次调用用同一 ID 两次封装覆盖产物。

## 验证

- archive_pipeline_test：文件分片、最终结束标记、空目录、失败不重试。
- lhm_mds_service_test：两个镜像归入一张盘，IMAGE_READY 编码、最终 inode 回写、主位置不变。
- namespace_rpc_test.py --exercise-archive --exercise-simple-archive：启动实际 MDS 和光盘
  引擎，用两个微型文件（含空文件）验证下载、压缩、未满镜像封装、未满盘模拟刻录及回写。
  数据源与节点查询为夹具服务，刻录使用现有 cd_manager_sim，实际生成 vimg/vdisc 文件；
  不代表验证了物理光驱烧录。测试 peer 另验证容量溢出不会覆盖前一个镜像。

运行产物均放 /mnt/metaiotest/filepath/results；保持固定实验数据集不变。
