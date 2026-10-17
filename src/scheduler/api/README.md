# Scheduler 对外接口与联调契约

统一入口为 [SchedulerApi.h](SchedulerApi.h)：Scheduler 管理、磁盘/光盘数据调用、光盘清单自动分页，以及配套 MDS 权威查询与提交。链接 `zb_scheduler_api_client`，复用正式 protobuf，不另建节点模型。非 C++ 调用方可直接使用 protobuf/brpc 或 BRPC HTTP/JSON。

## 开发与联调边界

| 责任方 | 实现和维护的内容 |
| --- | --- |
| Scheduler（我们） | 节点状态与能耗、CMS 目录提案、排空协调；真实磁盘配套的写门禁与迁移；进程内磁盘/光盘模型及正式数据协议适配；本模块闭环回归 |
| 光盘节点负责人 | 真实设备库存及后台下载/打包/取盘/刻录统计、实际数据执行和恢复、实际库存与任务快照的采集、归档完成回报 |
| MDS 负责人 | 权威文件位置、冷目录判定、归档下发/租约/完成确认、Masstree 发布和冷热元数据清理 |
| 客户端负责人 | 文件访问与准备入口、位置缓存和错误处理；与我们共同维护共享入口的节点 ID/实例代际传递 |

缺少真实光盘库存或归档回报时，Scheduler 保持准入检查，不补成成功。模型控制状态与生成式字节不充当真实数据证明。

先前受托补齐只读采集/上报与已完成介质/文件映射；本轮获得额外授权，实现真实文件化光盘库退役所需的跨模块交接、MDS 条件位置切换和源隔离，以及已排空磁盘源的真实归档完成确认。不改下载、压缩、封盘、刻录或内部调度算法，不替代负责人负责的正常在线归档自动发现/发布与光盘前台写协议。

本轮公共契约扩展为 `RealNodeService.BeginNodeDrain`、`InstallMigratedFile`，以及客户端 `FileInfo` 尾部可选的节点 ID/实例代际字段；后者在各光盘 RPC 上传递既有 LogicalNodeRoute。MDS 和真实光盘业务实现由对应负责人维护。
真实交接另增加 `ManageLibraryHandoff`、`CommitOpticalLibraryHandoff`、`CommitPhysicalOpticalArchive`，详见下文；MDS 位置事务锁及原归档管理器的同一锁类型做协程安全适配，锁边界和归档算法不变。

## 谁调用什么

| 调用方 | 推荐接口 | 目的 |
| --- | --- | --- |
| 真实磁盘、光盘节点和 MDS | RegisterManagedNode、ReportManagedNodeReadiness、ReportHeartbeat、ReportNodeMetrics；光盘另用 ReportOpticalLibraryStatus | 报身份、服务地址、设备库存、就绪、负载与后台任务 |
| 节点部署/控制工具 | GetManagedNode、ListManagedNodes、StartNode、StopNode、RebootNode、RemoveManagedNode、GetDrainMigration | 查询、启停、排空和追踪阻塞原因 |
| Trace/仿真控制器 | ReportNodeAccessEvent、BatchReportNodeAccessEvents、AdvanceSimulationTime、RunLongTermSimulation、GetManagedMetricsHistory | 注入模拟事件并读取模型时序；真实 I/O 不应重复注入同一访问 |
| 客户端/FUSE | MDS 文件位置及 [LogicalNodeRoute](../../common/rpc/LogicalNodeRoute.h) | 数据面仍按 MDS 权威位置访问目标节点；不让 Scheduler 代替 MDS 分配文件 |
| MDS/放置方 | [GetCmsNodeCatalog](../../msg/mds.proto) | 读取 CMS 已提交节点目录；Scheduler 的管理查询不是放置权威 |

RegisterManagedNode 可以用于显式配置节点；已有真实节点也保留首次心跳自动注册的兼容路径。需要自定义硬件、执行模式或实例代际时应显式注册，不依赖默认模板。物理节点的服务须先启动，再上报就绪和心跳；库存未就绪时不会因进程存活而自动取得写入准入。光盘节点必须报告实际盘片/任务状态，不应填入虚构的空库存。

## 光盘上报 v1

新增字段为尾部兼容扩展，但 v1 功能需要同时部署新版光盘服务、Scheduler 与 MDS/CMS。旧接收器会忽略新字段，不具备分项质量、独立归档准入或新增代际校验能力；不能把线协议可解码当作功能已经接通。

1. 先用完整 `ManagedNodeSpec` 注册。失败保留完整请求重试；`ALREADY_EXISTS` 必须核对返回的已有属性，旧服务缺少视图时再调用 `GetManagedNode`。不覆盖既有身份、实例代际或 retired tombstone；属性冲突需要运维协调，不能降级成默认模板后继续上报。

   兼容例外仅用于明确返回“注册 RPC 未实现”的旧心跳观察端：继续发送带 `require_registered=true` 的观测心跳，但不宣称已注册、不消费其角色分配、不确认指标窗口。真正的 Scheduler 若尚无该身份仍会拒绝这种心跳，不以默认模板补注册；传输超时、业务拒绝、属性冲突不走此观察兼容路径。
2. 心跳设置 `require_registered=true`，并在心跳、库状态、指标中携带 `instance_generation`。不匹配时在修改状态之前拒绝。未启用 v1 的旧节点保留原协议兼容；一旦光盘身份启用 v1，不再接受缺代际的上报或降级回旧版质量字段。
3. 同一次 `GetLibraryInventory` 采样先发 `ReportOpticalLibraryStatus`，再发心跳。采集失败设置 `inventory_unavailable=true`，保留上一份库存及其采样时间，不把旧库存刷新成新观测。元数据就绪只表示现有启动序列已结束且运行快照未报告加载失败，不是逐项索引/数据完整性核验，也不等于 MDS 已提交归档。

`ManagedOpticalLibraryStatus.telemetry_version=1` 的新增字段：

| 字段 | 语义及使用位置 |
| --- | --- |
| `inventory_valid/staging_valid/background_valid` | 分项采集成功标记；未知或失败不作空闲、零盘或归档准入证据 |
| `observation_source/physical_health_observed` | 当前来源是 `cd_manager_sim` 和镜像保留额度，硬件健康未探测；不能将坏盘计数 0 解释成硬件确认无坏盘 |
| `collection_error` | 采集或恢复失败原因，查询及快照保留 |
| `observed_at_ms/valid_until_ms` | 实际采样时间和有效期限；过期不能继续作为新归档或自动休眠依据 |
| `pending_archive_batches/background_tasks` | 队列及在途下载批次、全部非终态任务；补充原有读写任务和忙光驱计数，供功率保护使用；不同层级计数可能重叠，不能相加当作文件数 |
| `staging_read_reserved_bytes/staging_write_reserved_bytes` | 按镜像数乘卷上限统计的保留额度，不是实际文件长度或物理磁盘占用；与 `staging_used_bytes` 同口径 |
| `configured_disc_count` | 静态计划盘数；`local_disc_count` 为现有模型运行态的盘数，二者均不是实体盘片传感器读数 |
| `accepts_archive_writes` | 独立的光盘归档能力；CMS 仍检查就绪、健康、读写模式、功率及 80% 暂存区水位；不打开普通副本放置 |

设备 `health_source=software/model/unknown` 区分软件可用性、模型可用性和未知；不声称 SMART、光驱或坏盘硬件诊断。现有模型不支持零盘及实体盘片迁出，`TOTAL_DISC_COUNT=0` 显式报错，不再伪报 1000 片，也不放松零盘退役约束。

光驱额定带宽未显式配置时，由既有读盘/刻录 MB/s 参数换算为 bytes/s；显式 `DRIVE_*_BYTES_PER_SEC` 保留为额定上限，需要部署方与所用模型对齐。已有目录若保存了旧的不同属性，不自动覆盖；升级时使用一致配置或通过既有节点替换流程建立新身份。

缓存与光盘的指标按 `access_path + reporter_epoch` 分别保存序号和窗口水位，跨路径较晚到达的窗口不被另一条路径拒收。同路径相同 epoch/序号重试幂等，旧序号拒绝；重启后的新 epoch 仍须满足本路径时间水位。旧报告者标记 `retired`，保留水位用于确认迟到重试，不再接受其新窗口或参与负载汇总。新鲜活动窗口按最大资源利用率、合并队列深度形成节点负载，避免后一个空闲路径覆盖前一个忙路径。水位随 managed 快照保存，旧快照的默认路径继续使用原水位。

手动接口闭环：`python3 tests/scheduler/optical_reporting_integration_test.py --build build-release`。它启动正式服务验证上报、CMS 准入和恢复，不是实体光盘刻录或电表验收。

## C++ 调用

### 提供给 MDS / 集成方的功能总表

统一包含 [SchedulerApi.h](SchedulerApi.h)，链接 `zb_scheduler_api_client`。各头文件的功能与字段注释均使用中文。

| 工作 | 客户端 / RPC | 输入、输出与完成条件 |
| --- | --- | --- |
| 节点信息、容量、库存、功率、能耗、就绪 | `SchedulerClient.GetManagedNode / ListManagedNodes / GetClusterView / GetManagedMetricsHistory` | 输出公共节点模型；放置权威仍由 MDS 的 CMS 已提交目录提供 |
| 注册、管理状态、只读降级、就绪上报 | `RegisterManagedNode / SetNodeAdminState / SetManagedNodeServiceMode / ReportManagedNodeReadiness` | 只注册目录不自动创建一个有数据服务的模型；模型创建走部署配置热加载 |
| 启停、重启与查询结果 | `StartNode / StopNode / RebootNode / GetOperationStatus` | 返回 operation_id；需检查 operation.status。配置管理快照时成功结果及 ID 随快照恢复，在途操作恢复为中断失败，不盲目重放硬件命令 |
| 动态扩容 | `ReloadModelConfigs` | 服务器先部署配置文件，RPC 只排队热加载；主循环创建实例→采集→CMS 提交→开放数据入口。不接收远程路径，不重启进程；原 SIGHUP 入口保留 |
| 排空与退役 | `RemoveManagedNode / ReportNodeDrainProgress / GetDrainMigration` | 受理不等于完成；源数据和 MDS 零引用核验后才退役，没有目标明确阻塞。生产/进程内数据节点不能用 force+finalize 绕过协调器；已退役节点的 finalize 幂等返回 |
| 指标、任务与能耗输入 | `ReportHeartbeat / ReportNodeMetrics / ReportOpticalLibraryStatus / ReportNodeAccessEvent` 及批量版本 | 窗口按路径/epoch/sequence 去重；模型 IO 不再由 Trace 重复注入。metrics_streams 输出各路径累计读写操作数/字节数，后台模型编码量不等于实际网络字节 |
| 可靠性、离线长期仿真 | `InjectManagedNodeFailure / RepairManagedNode / AdvanceSimulationTime / RunLongTermSimulation` | 模型操作，不调用实体电源；不替代正式客户端 IO |
| 磁盘读写与归档搬运 | [DiskNodeClient](DataNodeClient.h) 包含全部 15 个 `RealNodeService` 方法 | 对象读写/删除/枚举、文件读计划、写分配/版本提交、归档状态、库存及源清理；按节点和代际路由，无隐式重试 |
| 迁移配套 | `DiskNodeClient.BeginNodeDrain / InstallMigratedFile` | 真实/进程内模型均支持。源先封新写；目标核对对象齐全、大小和版本后安装元数据；之后由 MDS 条件切换位置 |
| 光盘归档、读与任务查询 | [OpticalNodeClient](DataNodeClient.h) 包含全部现有 `OpticalNodeService` 方法 | 下发归档、查询阶段任务、查询完成介质/文件、通知订阅、异步准备、读会话/区间读及释放；旧读 RPC 保留 |
| MDS 完成事实拉取 | `OpticalArchiveClient.ListAllTasks / ListAllImages / GetAllImageFiles` | 固定令牌分页，错误不交付部分清单；任务终态不等于 MDS 提交，版本未知明确标记 |
| 真实整库交接 | `OpticalNodeClient.ManageLibraryHandoff` | 精确身份/代际/清单，冻结、续传、接收重建、权威确认后隔离；不接受模型数据当真实副本 |
| 配套 MDS 权威提交和核验 | [MdsPlacementClient](MdsPlacementClient.h) | 模型归档/光盘迁移、已排空真实源归档确认、真实整库交接，以及位置/零引用查询；保留原始回复，不自动重试变更 |

`DataNodeTarget` 必须来自 MDS 的已提交位置/CMS 条目，含地址、节点 ID、非零代际；共享地址下仍按节点隔离。`DataNodeClient` 可传自定义 `brpc::Controller`，保留调用方的 RPC 超时和 `log_id`，并检查其路由不冲突；通知订阅须传已创建 Stream 的 Controller，并自行维护 Stream 生命周期。上层 run_id / 父子请求关联仍需 Trace、客户端在正式上下文契约中提供，不能凭服务地址或时间近似冒充。

不支持的正式协议仍不能算作交付成功：光盘客户端写会话/分片/提交协议尚未由光盘、客户端、MDS 冻结，因此不另造模型专用前台写入；真实盘片转移后的物理可达性和 MDS 权威切换也不是一个查询 RPC 能代替的。本模块提供已有状态/库存/任务、控制及数据接口，业务方据此接入自己的工作流。

### MDS 归档清单接口

真实光盘和 Scheduler 进程内光盘模型共同实现以下 `OpticalNodeService` RPC：

| RPC | 输入 | 输出与含义 |
| --- | --- | --- |
| `ListArchiveImages` | CMS 节点 ID、非零实例代际、分页参数 | 已有持久介质索引中的盘片/镜像、镜像偏移/长度、已有 SHA-256；不包括仅受理或尚未完成的任务 |
| `GetArchiveImageFiles` | 同一身份及盘片 ID + 镜像 ID、分页参数 | 该镜像的 inode/逻辑大小和可用的文件版本；不解压业务内容，不修改权威位置 |
| `ListArchiveTasks` | CMS 节点 ID、非零代际、分页参数 | 现存归档阶段任务、状态、失败详情、可用的文件版本与盘片/镜像标识。真实旧引擎任务可能没有 operation_id，且会定期清理终态任务；空列表不是成功或失败证明 |

真实后端复用 `RebuildDiscImageIndex` 的结果，从 `vdisc` 卷头和文件元数据区只读解析，不修改原库存格式，不复制整个镜像；单卷元数据查询上限 64MiB，超过上限、格式错误或文件缺失明确失败，不返回成功的部分清单。索引未完整重建时返回 NOT_READY，不把部分索引冒充完整空库。模型从自身持久逻辑文件记录查询；查询不推进任务时间或触发唤醒、刻录、位置提交。

响应回显 `node_id`、`instance_generation` 和 `modeled`。`modeled=false` 仅表示非 Scheduler 无载荷模型后端，文件化光盘产物也不是物理光盘实测证明。真实卷没有业务文件版本，所以 `has_file_version=false`；不能用卷格式版本替代。SHA-256 是原索引记录，本次查询没有重新验证载荷。

分页 `limit=0` 默认 128，最大 1024。第一页 token 为空，后续页必须传上一页 `snapshot_token`；镜像列表和单镜像文件列表的 token 不能混用。变化返回 `OPTICAL_CONFLICT`，丢弃全部累计页后从头重读；代际/真实节点身份错误返回 `OPTICAL_STALE_GENERATION`。token 仅用于变更检测，不是内容校验或预留锁；不同镜像的查询不构成原子事务。空库成功返回空列表，不存在的镜像返回 NOT_FOUND；旧服务未实现则明确失败，不降级为空清单。

MDS 的使用顺序：

1. 从自身权威 CMS 目录取得地址、节点、代际和真实/模型类型。
2. 枚举已完成介质，查询对应文件映射，不解析日志。
3. 自行核对原任务清单、租约、大小、版本、可读性和源/目标后端一致性。版本未知时，不能凭 inode/大小认定没有并发更新。
4. 执行 MDS 的位置/冷热元数据事务，确认完成后才允许清理源副本。

这两个 RPC 提供可重复拉取的本地完成事实，不是写入会话、MDS 提交或自动退役接口，不替代目录归档业务。原引擎打印保留作诊断，但不再是获得已完成映射的唯一方式。

~~~cpp
#include "scheduler/api/OpticalArchiveClient.h"

// entry 来自 MdsService.GetCmsNodeCatalog 的已提交条目。
const auto& node = entry.managed_view();
zb::scheduler::api::OpticalArchiveClient optical(
    node.service_address(), node.spec().node_id(), node.spec().instance_generation(),
    node.spec().execution_mode() != zb::rpc::MANAGED_EXECUTION_PHYSICAL);
std::string error;
std::vector<zb::rpc::ArchiveImageInfo> images;
if (!optical.ListAllImages(&images, &error)) return;
for (const auto& image : images) {
    std::vector<zb::rpc::ArchiveImageFile> files;
    if (!optical.GetAllImageFiles(image.disk_id(), image.image_id(), &files, &error)) return;
    // 与 MDS 原始清单对账；这里不自动提交权威位置或删除源。
}
~~~

封装检查返回身份、后端类型、镜像身份和分页令牌，自动翻页失败时输出清空，不交付半份清单；无隐式重试。该客户端可在初始化后并发使用，不可并发销毁。不改变既有读/归档协议的行为。

后端支持矩阵：真实光盘正式数据面使用六个通知/准备/会话读接口；三个 deprecated 旧读 RPC 在当前进程内模型中保留，真实光盘服务没有对应实现时会明确报未实现，不能据此宣称读取成功。`OpticalNodeClient` 为它们提供调用入口不等于所有版本后端都支持。`ListArchiveTasks` 观察现存阶段任务，不是永久任务审计库；尚在归档下载队列的批次还应结合节点 `pending_archive_batches / background_tasks` 和质量标记判断，不能将空任务列表解释为节点空闲或归档完成。

### Scheduler 管理接口

~~~cpp
#include "scheduler/api/SchedulerClient.h"

zb::scheduler::api::SchedulerClient scheduler("127.0.0.1:9100", 2000);
std::string error;
zb::rpc::GetManagedNodeRequest request;
request.set_node_id("node-01");
zb::rpc::ManagedNodeReply reply;
if (!scheduler.GetManagedNode(request, &reply, &error)) {
    // error 区分 RPC 传输失败与 SchedulerStatus 非 SCHED_OK。
    // reply.status() 在收到业务响应时保留服务端错误码。
}
~~~

客户端覆盖 SchedulerService 的所有现有 RPC；每次调用返回 bool，同时将完整 protobuf 回复交给调用方。没有隐式重试，特别是注册、控制、指标和移除等可变更状态的 RPC。`Connect` 只初始化 BRPC Channel，不探测远端健康；首次 RPC 的传输结果才说明服务可达。连接初始化后可在多个线程并发使用；不要在并发调用过程中销毁客户端。ListAllManagedNodes 自动翻页，并在目录版本变化时失败而不是返回混合目录；高频变化的集群可改用 ListManagedNodes 分页并自行重试。

其他语言可直接调用 BRPC HTTP/JSON，例如：

~~~sh
curl -sS -H 'Content-Type: application/json' \
  -d '{"node_id":"node-01"}' \
  http://127.0.0.1:9100/zb.rpc.SchedulerService/GetManagedNode
~~~

成功须检查回复中的 status.code，不应仅以 HTTP 200 或 RPC 传输成功判定业务成功。当前接口未附加身份认证；部署时只向可信内网开放 Scheduler 控制端口。

## 工作流与完成条件

1. **加入**：显式注册时通过 ManagedNodeSpec 给出 node_id、kind、execution_mode、instance_generation；兼容的首次心跳自动注册使用心跳中的节点类型和默认模板。随后上报服务地址、设备容量/健康、readiness。Scheduler 本地状态可经 GetManagedNode 查询。需要做文件放置时，还必须在 MDS 的 GetCmsNodeCatalog 中看到同一身份的 Working 且满足读写准入。
2. **运行**：节点以报告者 epoch + 单调序号上报指标窗口。上报失败可按相同序号重试；成功后再确认本地窗口，避免重复计算。真实访问由节点指标汇总；ReportNodeAccessEvent 主要供 Trace/仿真注入。
3. **排空**：RemoveManagedNode 发起排空，不表示文件已迁移。调用方可通过 GetDrainMigration 查看进度和错误；只有 MDS/CMS 确认 Retired、源文件引用为零且源数据清空，才是完成。无可用目标或核验失败时保持阻塞，不得强制跳过元数据/数据安全检查。
4. **模型数据面**：从 MDS 文件位置获取逻辑节点 ID、实例代际与地址；磁盘前台 RPC 在 BRPC attachment 中设置 LogicalNodeRoute。旧代际、缺目标或 CMS 未确认的模型实例拒绝请求。模型读取的生成式字节不能当作真实副本迁移或内容校验。

单位：容量、IO 和带宽使用字节或字节/秒；报告时间使用毫秒，访问事件时间使用微秒；功率为毫瓦，累计能量为微焦。instance_generation 是服务实例代际，不是库存 generation、硬件 technology_generation、CMS 目录版本或文件版本。

配置 `MANAGED_STATE_SNAPSHOT_PATH` 后，启停/重启、管理状态切换、排空请求及外部指标窗口应答前同步保存管理状态和操作记录；指标按原 epoch/序号重试，控制命令仍不能盲目重试。应答若提示“state applied but persistence failed”，须先查询并处理存储错误。未配置路径时没有跨进程恢复承诺。恢复保留成功/失败操作及 ID 序列，在途操作标记为中断失败，调用方结合节点与 CMS 查询决定后续动作，不自动重复执行。

真实磁盘排空先调用 `BeginNodeDrain(node_id)`：回复须包含 `STATUS_OK` 且 `write_admission_closed=true`。源节点先拒绝新写，再等待在途对象写/文件提交结束，最后 fsync 关闭标记和目录；重启仍拒绝写计划、对象写和文件提交。该门禁用于退役，不因普通启动 RPC 自动打开；失败保持关闭并允许重试。复制和目标回读校验后，`InstallMigratedFile` 核对目标对象长度，保存源文件大小、对象单位、版本和修改时间，再向 MDS 条件提交位置。MDS 成功后才清理源对象和旧文件元数据，最终再次核验零引用。

每次复制重试先调用 MDS `GetFileLocation` 核对权威源；若位置已经提交到其他节点，则只清理旧源，避免响应丢失或重启后用旧内容覆盖目标上的后续写入。位置无法解析时保留源并报告阻塞。部署排空能力时须同步更新真实磁盘服务；旧节点未实现门禁/元数据接收 RPC 时明确失败，不退回无保护迁移。

## 验证

### 模型光盘迁移的 MDS 对齐接口

`MdsService.CommitModelOpticalMigration` 是我们排空链路所需的最小位置提交适配，只接受两端均为 CMS 中的 `VIRTUAL + OPTICAL_LIBRARY`；不执行冷目录扫描、真实刻录、Masstree 导入或真实盘片搬迁。

输入为不可复用的 `operation_id`、inode、期望版本、逻辑大小，以及完整的源/目标光盘位置和非零实例代际。受理时目标通过写入准入；模型目标完成持久接收及模拟刻录后再请求 MDS。完成提交检查 CMS 中目标仍 Working、健康、管理启用、就绪且地址/代际一致；不再要求新增空闲容量，允许刚封满盘而只读的目标完成已有 outbox。MDS 在同一位置事务边界检查源位置/版本并原子写入目标位置和幂等记录。相同操作 ID/清单重试返回已提交；ID 被不同清单复用、源位置改变、代际失效或目标不就绪时返回 `MDS_STALE_EPOCH`，调用方不得清理源。

成功仅表示权威位置已切换，后面还必须清理源逻辑记录、等待活跃读引用释放、确认 CMS 零引用及最终退役。传输失败保留目标 outbox，按相同 ID 重试；Scheduler 恢复后继续处理。当前适配服务于 RocksDB 中已有光盘位置的模型节点，不声称已处理 Masstree 冷目录位置重写。新增 RPC 在旧 MDS 上未实现时明确失败，不能降级为直接改 Scheduler 统计。

`OpticalNodeService.SendArchiveMetadata` 新增 `admissions` 尾部字段，模型端支持多文件共卷和逐文件幂等。部分受理时顶层返回失败，已受理项继续执行；只有 `admissions` 中的项被尝试，后续未尝试项不能算成功。旧真实光盘服务可忽略该字段，仍须按其既有完成契约验收，不能要求它凭此自动获得模型批次功能。

仍待三方冻结的接口是客户端光盘写会话、分片、提交与版本更新；当前正式 `OpticalNodeService` 没有这些 RPC。我们不另造只供模型使用的前台写流程，也不以归档下发冒充客户端直接写入。

### 真实文件化光盘库带数据退役

管理入口仍是 `SchedulerClient::RemoveManagedNode`，不要求 MDS 自己执行文件复制。
配套 MDS 条件提交与权威查询可使用 `MdsPlacementClient`；统一包含入口为 `SchedulerApi.h`。所有接口注释为中文，SDK 不隐式重试或清理源。
`OpticalNodeClient::ManageLibraryHandoff` 是底层交接 RPC，供管理编排和恢复使用：

`FREEZING → EXPORTED → RECEIVING → INSTALLING → ACTIVE → MDS 提交 → RELEASED → 零引用/停止/退役`

整库迁入**同介质几何、空白且空闲的真实接收库**，保留原盘片/镜像编号。复制的不是只有索引：清单包括 `.vdisc`、未封盘镜像/压缩产物及原引擎重启快照，每个文件带长度与 SHA256；数据 RPC 每块最多 1 MiB，目标持久前缀可查询并续传。后台一步最多 32 MiB/约 100ms（单 RPC 仍受其超时约束），文件游标避免重复扫描所有已传盘文件。`ACTIVATE` 完整校验后复用原 `Run()` 的索引/快照重建。暂不将两套已有编号空间的数据做在线合并；没有合适目标时保留源并明确阻塞。已接收库可再次作为迁出源，原接收必须先由 MDS 提交；权威解析支持交接链并拒绝循环/超深链。

源先持久关闭新归档、准备和读会话准入，等待已受理任务及读引用退出再冻结；已有读/释放请求仍能收尾。接收与安装阶段不开放前台准入。源、目标以及 Scheduler 重启后依据同一清单恢复，不重新分配目标或偷偷开启空引擎。交接文件为各节点根目录内 `library_handoff.pb`，不是节点身份配置。
接收时创建新的引擎实例并复用启动重建，避免复用旧 Shutdown 后已关闭的队列；保留不可变导入快照，覆盖 Run 消费快照后、ACTIVE 收据落盘前的恢复窗口。真实分块读写使用独立 `MIGRATION` 指标流，字节为实际传输内容（含必要快照/索引），不与前台访问重复累加；进程强杀前尚未上报的窗口不保证无损，不冒充物理电表读数。

MDS `CommitOpticalLibraryHandoff(record, commit)` 验证双方 CMS 身份、代际、地址、源排空状态与目标就绪，并回查源冻结清单、目标持久接收收据。`commit=false` 只查询同一清单是否已提交。提交后同步持久化整库节点映射；普通 inode、冷 Archive 目录和 Masstree 的位置解析均应用该映射，盘/镜像 ID 不变，不重写冷页面或改变其导入算法。失效代际不能自动变成新实例上的位置。零引用验证同时检查普通 inode 与冷目录，不以 Scheduler 自报计数代替 MDS。

源 `HANDOFF_RELEASE` 只能向其部署配置 `MDS_ADDRESS` 查询精确提交结果，未提交不得清理。成功后将旧活动目录移动到 `.handoff-retired/` 隔离保留，重建不再加载它们；不会不可恢复删除数据。源上报交接后的空活动库存后，仍执行 CMS 零引用、配置的停止执行器和通用退役门禁。**必须配置真实停止命令**；空命令不能作为物理节点停止确认。文件化光盘库进程停止不代表已测得服务器电源断电。

配套 `MdsService.CommitPhysicalOpticalArchive` 用于**已处于排空中的真实磁盘源**完成归档位置确认，复用 `ModelOpticalArchiveRequest/Reply` 的字段，但不接受模型后端：检查持久源写栅栏、真实源文件版本/大小、分片实际字节摘要、真实介质流式解压摘要、归档租约与 inode CAS。它不是正常在线归档自动回报或客户端光盘写协议，也不会替调用方发起磁盘退役。真实清单查询的 `digest_inode_id` 默认 0，不读载荷；显式指定时仅计算该 inode 的 `payload_sha256`，仍不伪造卷格式中不存在的文件版本。

对应完整系统入口：`tests/scheduler/physical_optical_handoff_integration_test.py`。使用新建私有外存目录、独立 Scheduler/MDS/真实磁盘/两个真实光盘进程和正式 FUSE/zbctl；不接触现有业务目录，也不代表实体机械光驱或电表验收。

### 其余验收与协作边界

- **本轮补齐的独立能力**：磁盘/光盘请求级失败和超时 ppm 配置、模型前台与归档/迁移分路计数、启停操作记录恢复、模型源租约过期后的 MDS 重新申领与本地持久化、全部现有协议的 C++ 调用入口。租约恢复仍核对权威源位置/大小/版本和本地源，不抢占有效的其他租约，不绕过 MDS 条件提交。
- **权威对账接入边界**：本地控制状态、CMS 身份和任务 outbox 已恢复；覆盖 MDS 全部命名空间（含冷目录）的反向逐文件对账仍需 MDS 使用自身权威清单与本模块库存/文件查询对齐。本模块不扫描或改写其他模块内部数据库，不能宣称已证明整个冷热命名空间一致。
- **共同契约依赖**：客户端/光盘/MDS 的前台写会话与修改语义、统一 `run_id` 和父子请求传递；我们负责对应模型及输出适配，不能算作纯外部工作。
- **其他模块主责、我们参与联调**：自动冷目录发现、真实归档完成回报、Masstree 发布及热元数据清理、实体光盘搬迁执行。当前验证文件级模型位置提交、迁移后 FUSE 读取，不等同于该目录级链路已验收。
- **已补的信息暴露**：真实/模型光盘的已完成盘片、镜像和文件清单可通过上述两项 RPC 拉取，不再把“没有可查询映射”列为外部阻塞。MDS 侧自动轮询、原任务对账和权威提交仍由 MDS 接入；推送回报不是这两个只读接口的行为。有数据真实光盘库的退役由 Scheduler 牵头衔接既有重建、MDS 位置切换和源清理，不等同于要求光盘另造一套迁移引擎。
- **未定义的跨模块协议**：光盘前台写协议确定后，模型端适配仍由我们完成；整库交接只支持上述文件化真实后端和兼容空白接收库，不宣称支持实体机械取盘、两个已有数据库的编号合并或不同介质格式转换。普通在线归档自动发现、发布和前台写会话不由退役接口代替。
- **已声明的部署限制**：不支持跨 Scheduler 的模型迁移；多卷共享一张盘的介质格式、归档阶段 Scheduler/MDS 同时崩溃和超声明并发的压力验收未覆盖。物理能耗/硬件健康须有真实采集来源，模型功耗不能充当实测。

- scheduler_real_drain_integration_test：真实 Scheduler、MDS 和磁盘进程检查持久写门禁、源进程异常重启、迁移版本接续和迁移后继续写入。
- scheduler_inprocess_disk_server_restart_test：独立 Scheduler/MDS 进程检查 CMS 准入、模型持久化及异常重启。
- scheduler_model_closure_integration_test：独立 Scheduler/MDS 进程检查模型迁移、代际与光盘归档；具备 FUSE 时通过正式客户端和 zbctl 检查准备及读取链路，数据为模型生成。
- tests/scheduler/real_system_smoke.py：真实外存、FUSE、MDS/CMS 和两个真实数据节点的读写、重启、排空与退役闭环；不测物理功耗。

单元/组件测试不交付；上述系统集成脚本仅手动运行，不注册到全仓 CTest。两个配套 RPC 驱动需显式构建，不参与默认 ALL；具体命令见 [系统测试说明](../../../tests/scheduler/README.md#构建及快速验证)。

本接口方便联调，但不声明物理电表、实体光盘刻录或其他模块的冷目录/Masstree 流程已经验收。
