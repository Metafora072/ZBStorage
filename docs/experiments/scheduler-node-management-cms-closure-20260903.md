# Scheduler、节点管理与CMS闭环验证报告（2026-09-03）

## 1. 结论

本阶段属于节点管理和Scheduler职责的代码已实现，并在保留RocksDB状态的多进程全栈中完成闭环验证：节点注册与就绪、实际清单、健康/生命周期、四级功率与能耗、可靠性、CMS权威目录、真实I/O加权放置、排空迁移、CMS零引用退役门禁、退休后禁止重新放置及重启恢复均通过。

CMS现为节点成员、紧凑ID与MDS放置准入的权威：Scheduler只提交状态提案；MDS不再轮询Scheduler视图直接覆盖放置缓存。目录关闭写准入、所有inode位置写入和最终零引用扫描由同一事务门禁串行化。

## 2. 本组职责与跨模块边界

| 范围 | 归属 | 本次状态 |
|---|---|---|
| 公共节点属性、注册/readiness、心跳清单、健康与工作/加入/失效/退休列表 | 节点管理 | 完成 |
| 生命周期、排空、功率策略/能耗积分、可靠性、快照恢复 | 节点管理/Scheduler | 完成 |
| 调度候选过滤、带宽/功率/负载权重、待机唤醒、光盘暂存80%背压 | Scheduler | 完成 |
| Scheduler目录提案、CMS提交/紧凑ID、MDS只消费提交视图 | Scheduler与CMS边界 | 为闭环完成 |
| 对象枚举、复制、目标回读校验、MDS原子位置切换、源删除、CMS零引用复核 | Scheduler排空编排 | 完成 |
| POSIX/FUSE语义、MDS命名空间/Masstree业务功能 | Client/MDS负责人 | 本次只做集成验证，不宣称由Scheduler实现 |
| Real/Virtual/Optical对象读写实现与指标采集 | 各DataNode负责人 | Scheduler只消费心跳；真实节点用于正式验证 |
| 光盘DiscID→Slot明细、机械臂/光驱队列、物理换盘、固定10 GB镜像、EC、DL-SubNameSpace | Optical/CMS归档负责人 | 公共模型已预留，非本组内部调度职责 |

为实现设计规定的CMS权威与安全退役，本次补充了最小CMS/MDS边界代码；这不是把CMS命名空间或光盘控制器的全部职责归入Scheduler。

## 3. 关键实现

- 公共 `NodeRecord` 统一规划硬件、实际设备清单、readiness、生命周期、功率/执行功率、能耗、可靠性、排空进度和光盘聚合状态。
- Scheduler持有运行状态并发布全量目录提案；CMS拒绝陈旧generation、成员遗漏和Retired复活，并分配不可变ID：存储/元数据16 bit，光盘库24 bit。
- CMS提交后才生成MDS `NodeInfo`；元数据节点不参与数据放置。候选必须Working、Healthy、Enabled、ReadWrite、已唤醒且有健康可写设备。
- 放置权重综合内部写带宽、外部网络上限、功率档位、利用率和队列深度。
- 光盘库只有全部本地盘已Recorded且待写任务为0时才自动只读；staging使用率达到80%后CMS停止新归档放置。
- 普通Create、文件迁移位置提交和光盘归档位置提交持有共享门禁；CMS准入变更和零引用扫描持有独占门禁。迁移/归档提交还会复核目标仍被CMS准入。
- 排空迁移只选具备真实字节保持语义的物理节点，必要时先唤醒；复制完成后回读比对，再原子切换MDS位置，最后删除源对象。

## 4. 构建与自动测试

- 仓库内RocksDB已作为正式目标构建，未借用系统占位库。
- 关闭RocksDB上游测试注册后，本项目CTest集合为10项，不再出现大量`NOT_BUILT`第三方测试。
- 10项均通过：Masstree layout/store、PG迁移、公共storage model、Scheduler节点模型、指标、快照、managed service、排空迁移、CMS registry。
- CMS registry测试覆盖紧凑ID、RocksDB持久化恢复、陈旧/遗漏目录拒绝、真实加权分配和光盘80%背压；排空测试覆盖CMS引用非零时禁止退役。

## 5. 正式多进程实验

实验目录：`/mnt/md0/zbstorage_cms_scheduler_20260903_final_qs5nxS`。使用独立端口启动Scheduler、MDS、两个真实节点、虚拟节点、光盘节点和FUSE；功率物理执行关闭，避免在共享机器上真实关机。

### 5.1 CMS驱动的真实I/O

核心模块冒烟7/7通过：Scheduler视图、CMS目录、CMS调度后真实数据写读、MDS命名空间、真实节点精确I/O、虚拟节点仿真语义、光盘项按范围跳过。CMS调度用例通过MDS创建文件，验证返回目标属于CMS已提交目录，再向目标节点写入并逐字节读回。

### 5.2 排空、退役和退休后写入

通过FUSE在`/real`写入12 MiB随机文件，源文件与读取结果SHA-256均为：

```text
eec0dff194c0414ccd2c1499b2689dfe92bdf9f5ac1dc78151ebcc6a9a0dfb2b
```

初始CMS引用为`node-real-01=1`、`node-real-02=0`。调用删除第一节点后约1.3秒完成：3个对象、12,582,912字节全部复制和回读校验；inode原子切换至第二节点；源端二次枚举为空；CMS复核旧节点磁盘/光盘引用均为0；第一节点最终为Retired/Off并记录实际退出时间。

退役后再写入5 MiB文件，写读SHA-256均为：

```text
67466727ea528f7c37be6155164a246adaac4c84071d4f6d950d53533cdb158d
```

此时旧节点引用仍为0，新节点引用增至2，证明Retired节点未再次进入放置。

### 5.3 保留状态重启

完整停止并用相同目录重启后，两份文件哈希均保持不变。Scheduler恢复第一节点Retired/Off、compact_id=2及累计能耗；CMS从MDS RocksDB恢复已提交目录、generation和分配游标，ID保持为：metadata=1、real-01=2、real-02=3、virtual=4，光盘库在独立24 bit空间为1。再次完整重启仍保持同一组ID。重启后的核心冒烟仍为7/7，CMS调度目标为第二真实节点。

## 6. 数据合理性

运行目录中的MDS元数据节点上报实际文件系统容量约99.61 TB，配置SSD带宽为5 GB/s；真实磁电节点按200 MB/s/盘和并发度计算内部带宽；功率与累计能耗满足`microjoules = milliwatts × milliseconds`。退役节点最终功率0 W，工作节点按负载处于Medium/Peak，空闲节点可降至Standby/Off。实验没有启用物理执行，所以“策略功率/模型能耗”可验证，“真实电表耗电”不在本次软件实验结论内。

## 7. 已识别但不属于本组的缺口

光盘节点现有 `ImageStore::ReadObject()` 在`SIMULATE_IO=false`时仍返回等长`x`，因此通用对象接口无法逐字节验证写入payload。光盘节点注册、聚合状态、功率和CMS放置背压正常；该内容一致性问题属于Optical数据面实现，不应被记为Scheduler或节点管理未完成。

虚拟节点明确采用仿真语义，只保留长度/统计并返回合成数据，不作为正式持久化目标，也不再作为排空迁移目标。若产品未来要求虚拟节点具备POSIX字节一致性，应由Virtual DataNode负责人改变数据面契约。

CMS当前零引用验证扫描现行权威 `I/` UnifiedInodeRecord。其他团队若引入新的inode磁盘格式或额外位置索引，必须同步扩展CMS引用枚举接口；Scheduler不应猜测新格式。
