# FIO 首 16 KiB 查询测试

本测试读取 ZBStorage FUSE 挂载点中预先准备的 1000 个文件，自动生成文件清单，然后对每个文件从偏移 0 读取一次 16 KiB 数据。脚本不创建、修改或删除测试文件。

## 目录

```text
fio/
├── config/
│   └── run_fio.conf  # 测试数据目录和 FIO 参数
├── log/              # 文件清单、iolog 和测试结果，不提交 Git
└── run_fio.sh        # 测试入口
```

## 运行

先启动 ZBStorage，确认 `ROOT_PATH/mnt` 已挂载，并在 `config/run_fio.conf` 指定的目录中准备好 1000 个文件。脚本不依赖当前工作目录，可以使用脚本的绝对路径运行：

```bash
bash /path/to/ZBStorage/tests/benchmark/fio/run_fio.sh
```

测试目录、单次读取大小和 FIO 参数都由 `config/run_fio.conf` 指定。每次运行都会在 `log/` 下创建独立目录，其中 `files.txt` 是文件清单，`first16k.iolog` 是 FIO 回放记录，`fio.txt` 是 FIO 普通文本格式的测试结果。
