# 光盘文件的 POSIX 演示

## 启动

在仓库根目录执行（本次改动包含客户端与节点的正常缓存处理逻辑，首次请重新构建）：

```bash
python3 tests/client/optical_read_demo.py
```

构建过后可加 `--skip-build`。脚本自动启动外部模拟服务、正式光盘节点，完成归档，
再挂载正式 FUSE 客户端。不会自动准备或预读文件。
需要 `/dev/fuse`、`fusermount3` 和当前用户的挂载权限。
结果默认保存在 `tests/client/results/optical-demo-<随机串>/`，已被 Git 忽略。

## 演示命令

在另一个 Bash 终端执行启动脚本打印的完整 `source .../demo-env.sh`，然后：

```bash
cd "$ZB_DEMO_MOUNT"
ls -lh
cat hello.txt
zb_prepare hello.txt
```

准备前的 `cat` 返回 `Resource temporarily unavailable`（EAGAIN），属于预期状态。
**`zb_prepare` 只提交任务，收到受理回复后立即返回，不等待文件准备完成。**
此时可以继续在终端执行其他操作；准备期间再次 `cat` 仍返回 EAGAIN。

启动终端会显示：

```text
[提交已返回] ... request=... task=...
[等待主动通知] ... 已等待 5s（本地计时，无状态轮询）
[收到节点就绪通知，准备耗时 ... 秒] ...
```

看到就绪通知后，在操作终端执行：

```bash
cat hello.txt
cat hello.txt
```

两次都应成功。内容为：

```text
Hello from the optical node!
This file was archived, prepared asynchronously, and read through FUSE.
```

无需 `zb_release`，也无需手动保存准备句柄。`cat` 退出时正常关闭读取会话，
光盘节点自行管理准备结果的缓存。

客户端通过首次请求建立的 brpc Stream 接收 `ReadReadyEvent`；提交用的 `zbctl` 进程退出后，
仍由常驻 FUSE 客户端接收通知。启动终端的计时和日志展示不发送状态查询 RPC。

## 大文件与校验

```bash
zb_prepare another.bin
# 收到节点就绪通知后：
cat another.bin > /dev/null
cp another.bin "$ZB_DEMO_RUN/readback.bin"
cmp "$ZB_DEMO_RUN/readback.bin" "$ZB_DEMO_EXPECTED/another.bin" && echo '文件内容一致'
```

`hello.txt`、`empty.bin` 和 `large.bin` 在同一镜像；`another.bin` 在另一张光盘，
可以用于第二次冷读。已加载镜像后，后续准备通常很快。
快捷命令 `zb_read 文件名` 可进行 POSIX 读取与内容比较；它不会隐式提交准备。
`zb_demo` 兼容快捷入口现在也只展示元数据并提交准备，随后返回供手动操作。

## 节点自动回收

- 沿用节点已有策略：READY 后默认保留 2 小时，清理线程每 10 分钟扫描一次。
- 到期且没有打开的读会话时，节点自动回收提取出的临时文件；不删除光盘归档原件。
- 已打开的读会话保护文件，普通 close 不撤销缓存。
- 回收与新打开使用同一把锁检查，避免先判空闲、后被打开、再删除的竞争。
- 回收后客户端再次 open 得到 EAGAIN，并清除本地过期准备记录；此时重新 `zb_prepare` 即可。
- 对同一有效准备重复 `zb_prepare` 复用原任务，不生成重复的准备产物。

旧版 SDK/协议的显式准备释放接口为兼容旧调用方保留，但本演示和新的正常读取流程均不调用它。
`ReleaseReadSession` 仍由 POSIX close 自动调用，它关闭一次读取会话，不负责决定准备缓存何时回收。
回收的真实两小时等待不作为快速测试；单元测试验证空闲判定、活跃读保护、并发打开与回收互斥，
客户端协议测试模拟节点回收并验证 EAGAIN、重新准备和再次读取。

## 结束

操作终端先执行 `cd /tmp`，再回启动终端按 Ctrl+C。脚本会卸载 FUSE 并停止各进程。
若终端仍占用挂载，会尝试分离本次挂载。日志和原始文件保留供复查。
`demo.json` 的 STOPPED 表示环境已退出，不代表执行了全部自动验收。

## 代码位置

| 位置 | 职责 |
| --- | --- |
| `optical_read_demo.py` | 一键启动、保持环境、展示客户端就绪事件和本地等待计时 |
| `optical_demo_commands.sh` | `zb_prepare` 仅调用提交；不等待、不发送缓存释放请求 |
| `client_optical_external_test.py` | 外部模拟、正常归档、实际 FUSE 读取与无显式释放的回归测试 |
| `src/client/optical/OpticalReadClient.cpp` | 重复准备复用、接收主动通知、节点回收后清理本地状态并返回 EAGAIN |
| `ReadSessionTable::ExpireIfIdle` | 节点内部原子判定空闲并阻止回收后的新打开 |
| `OpticalNodeManager::CleanupTaskProcessor` | 按原有有效期和扫描周期调用节点自动回收 |

外部模拟程序仍只提供 MDS、Scheduler、源磁盘接口。文件定位映射从节点现有归档日志读取；
实际读请求和就绪通知走正式光盘接口。设备动作使用节点内置模型，不是实体光驱性能测试。
