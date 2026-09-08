# ZBStorage 测试目录说明

## 目录用途

`tests/` 用于统一保存 ZBStorage 的测试程序、负载生成脚本、测试配置和结果处理工具。新增测试代码应优先放在本目录下，避免继续分散到业务代码和个人工作目录中。

## 目录结构

`tests/` 约定第一级模块目录，并在每个模块下统一使用 `results/` 保存测试结果。除 `results/` 外，模块负责人可根据实际需要组织代码、脚本、配置和小型示例数据。

```text
tests/
├── README.md
├── client/             
│   └── results/
├── scheduler/          
│   └── results/
├── simulation/         
│   └── results/
├── optical_node/       
│   └── results/
└── benchmark/          
    └── results/
```

目前暂定的目录结构。当前仓库中的 `tests/` 还只有本说明文件，各负责人开始实现测试时再创建对应模块目录和 `results/`。

| 目录 | 暂定内容 |
| --- | --- |
| `tests/client/` | 实际平台 Trace 播放、客户端时延分解、MDTest 所需的客户端配合代码、光磁组合测试入口 |
| `tests/scheduler/` | Scheduler、磁电存储节点、数据分布、实际节点能耗和仿真节点能耗测试 |
| `tests/simulation/` | Trace 生成与播放、批量文件集仿真、长期演进和响应时间模型测试 |
| `tests/optical_node/` | 光盘库归档与恢复、长期随机读取、实际光盘库能耗测试 |
| `tests/benchmark/` | FIO，FileBench，MDTest |

涉及多个模块的测试放在主负责人对应的目录中，其他人员提供接口、环境或统计数据。例如光磁组合测试入口由武家浩负责，因此代码和结果放在 `tests/client/`。

## 文件组织

测试文件名不作统一格式限制，只需要能够清楚表达测试内容，并避免与同目录内的文件重名。各负责人可以选择 C++、Python、Shell 或现有性能测试工具实现任务。

每项测试可在对应模块目录中提供简短的运行说明。配置和小型示例输入可以直接放在测试程序附近。

## 结果保存

测试生成的结果和日志放在对应模块的 `results/` 中，例如客户端测试放在 `tests/client/results/`，Scheduler 测试放在 `tests/scheduler/results/`，FIO、FileBench 和 MDTest 放在 `tests/benchmark/results/`。

运行参数、工具原始输出、客户端和服务端日志、汇总结果、Trace 及图表都按这一规则保存。`results/` 内部的文件名和目录结构由负责人自行安排，注意不要覆盖已有结果。跨模块测试放在主负责人对应模块的 `results/` 中。

不要在项目根目录、`tests/` 根目录或其他位置另建测试结果目录。各模块的 `results/` 已被 Git 忽略，大规模 Trace、临时测试文件、光盘镜像、原始结果和运行日志不提交到 Git。
