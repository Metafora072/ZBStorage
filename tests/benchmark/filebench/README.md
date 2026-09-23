# FileBench 文件系统负载测试

本测试通过 ZBStorage FUSE 挂载点运行 FileBench，每轮各执行一次文件创建、写入、读取和删除，并执行相应的打开与关闭操作。每次运行使用独立的数据目录，不会修改 FIO 测试文件。

## 目录

```text
filebench/
├── run_filebench.conf  # FileBench 工作负载和参数
├── run_filebench.sh    # 测试入口
└── log/                # 配置副本和原始输出，不提交 Git
```

## 运行

先启动 ZBStorage 并确认 `ROOT_PATH/mnt` 已挂载，然后在任意目录执行：

```bash
bash /home/pz/ZBStorage/tests/benchmark/filebench/run_filebench.sh
```

文件数量、文件大小、目录宽度、线程数、I/O 大小和运行时间都在 `run_filebench.conf` 中配置。每次运行会在 `log/` 下保存本次配置和 FileBench 普通文本输出，测试数据写入 `ROOT_PATH/mnt/virtual/filebench/<运行时间>/zb_data/`。
