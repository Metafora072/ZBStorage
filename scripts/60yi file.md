功能6完整执行流程如下，从编译 C++ 随机读程序开始。

**1. 编译随机读 C++ 程序**

在项目根目录执行：

```bash
cmake --build build --target random_read_6b_files -j$(nproc)
```

编译成功后，应生成：

```text
build/random_read_6b_files
```

该程序负责按照 60 亿文件目录结构随机生成文件路径，读取随机文件，并统计平均读取时延。

**2. 确认随机读脚本可用**

脚本路径为：

```text
scripts/random_read_6b_files.sh
```

该脚本会调用：

```text
build/random_read_6b_files
```

如果没有编译成功，执行脚本时会提示缺少可执行文件。

可以先单独测试脚本：

```bash
bash scripts/random_read_6b_files.sh /mnt/lkcdir -n 100
```

其中：

```text
/mnt/lkcdir
```

为 60 亿文件目录根路径，下面应包含：

```text
dir002
dir003
...
dir051
```

`-n 100` 表示随机读取 100 个文件。如果不指定 `-n`，默认也是读取 100 个文件。

**3. 编译 system_demo_tool**

由于功能6由 `system_demo_tool` 调度执行，因此修改代码后需要重新编译演示工具：

```bash
cmake --build build --target system_demo_tool -j$(nproc)
```

如果同时想确保两个目标都已编译，可以执行：

```bash
cmake --build build --target random_read_6b_files system_demo_tool -j$(nproc)
```

部分 CMake 版本不支持一次传多个 target 时，可以分开执行：

```bash
cmake --build build --target random_read_6b_files -j$(nproc)
cmake --build build --target system_demo_tool -j$(nproc)
```

**4. 启动演示菜单**

执行：

```bash
bash scripts/run_system_demo.sh
```

进入演示菜单后，可以看到功能6：

```text
6  50亿文件测试
```

**5. 使用功能6执行随机读脚本**

在菜单输入：

```text
6 script=scripts/random_read_6b_files.sh /mnt/lkcdir -n 100
```

也可以写成位置参数形式：

```text
6 scripts/random_read_6b_files.sh /mnt/lkcdir -n 100
```

其中：

```text
scripts/random_read_6b_files.sh
```

是功能6要执行的脚本路径。

```text
/mnt/lkcdir
```

是 60 亿文件目录根路径。

```text
-n 100
```

表示随机读取 100 个文件。

如果要读取 1000 个文件，可以输入：

```text
6 script=scripts/random_read_6b_files.sh /mnt/lkcdir -n 1000
```

**6. 功能6内部执行逻辑**

功能6会解析输入命令，取出脚本路径，并把脚本路径后面的参数原样传给脚本。

例如输入：

```text
6 script=scripts/random_read_6b_files.sh /mnt/lkcdir -n 100
```

功能6实际执行的命令为：

```bash
bash 'scripts/random_read_6b_files.sh' '/mnt/lkcdir' '-n' '100'
```

脚本再调用 C++ 程序：

```bash
build/random_read_6b_files /mnt/lkcdir -n 100
```

**7. 预期输出**

功能6会先输出脚本执行信息：

```text
==== 50yi file test ====
script_path=scripts/random_read_6b_files.sh
script_arg_1=/mnt/lkcdir
script_arg_2=-n
script_arg_3=100
command=bash 'scripts/random_read_6b_files.sh' '/mnt/lkcdir' '-n' '100'
```

随后随机读程序会输出文件总体统计和测试结果：

```text
文件总数：60亿，文件总大小：89.41TB
==== Random Read 6B Files ====
root=/mnt/lkcdir
top_dirs=dir002..dir051
level1_dirs_per_top=100
level2_dirs_per_level1=100
level3_dirs_per_level2=100
files_per_leaf_dir=120
total_file_space=6000000000
file_size_bytes=16384
samples_requested=100
samples_completed=100
samples_failed=0
bytes_read_total=1638400
wall_latency_ms=...
avg_read_latency_ms=...
min_read_latency_ms=...
p50_read_latency_ms=...
p95_read_latency_ms=...
max_read_latency_ms=...
```

最后功能6输出脚本返回码：

```text
exit_code=0
```

交互式菜单会显示：

```text
结果: 通过
摘要: 50亿文件测试完成
```

**8. 通过判定**

满足以下条件即可判定功能6测试通过：

```text
exit_code=0
samples_requested=100
samples_completed=100
samples_failed=0
avg_read_latency_ms 正常输出
```

如果读取数量改为 `-n 1000`，则应满足：

```text
samples_requested=1000
samples_completed=1000
samples_failed=0
```

核心指标是：

```text
avg_read_latency_ms
```

该值即随机读取 n 个文件的平均读取时延，单位为毫秒。

cmake --build build --target random_read_6b_files -j$(nproc)

6 script=scripts/random_read_6b_files.sh /mnt/lkcdir -n 100