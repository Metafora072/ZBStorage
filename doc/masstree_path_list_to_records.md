# 从目录树文件到元数据 Record 的生成过程

本文描述一条简化后的主流程：输入一个目录树文本文件，系统先把它解析成内存中的目录树结构，然后基于这棵树生成目录和文件对应的元数据 record。

本文重点说明数据结构和处理流程，不展开具体代码函数。

## 1. 这条链路要解决什么问题

目录树文本文件只描述路径层级关系，例如“有哪些目录、有哪些文件、它们挂在哪个父目录下面”。生成器需要把这份文本转换成更适合后续导入和查询的元数据 record。

本阶段主要产物是：

- `inode.records`
- `dentry.records`

其中：

- `inode.records` 保存每个目录或文件自身的 inode 属性
- `dentry.records` 保存父目录和子节点之间的目录项关系

可以把整体流程理解成：

```text
目录树文本
-> 内存中的目录树结构
-> inode 元数据记录
-> dentry 目录项记录
```

需要注意，`inode.records` 和 `dentry.records` 是中间 record 文件，不是最终的 Masstree key/value 索引。

## 2. 输入文件格式

输入文件是一个普通文本文件，每行表示一条相对路径。例如：

```text
.
./68124af5a3f44530be5fe4e1d37d433c
./68124af5a3f44530be5fe4e1d37d433c/headChange
./68124af5a3f44530be5fe4e1d37d433c/headChange/66ca87b9b36148b39f575aa469f17101.jpg
./505871cd1e7541f190e216a8ff463f40
./505871cd1e7541f190e216a8ff463f40/headChange
./505871cd1e7541f190e216a8ff463f40/headChange/d482a5542bbf401aa2cadc2570702d05.png
```

这类文件的核心含义是：每一行描述一个路径，所有路径合起来形成一棵目录树。

## 3. 相关输入参数

生成过程里最关键的输入信息包括：

- `path_list_file`
  目录树文本文件路径
- `source_mode=path_list`
  表示输入来源是路径列表文件
- `inode_start`
  inode 起始编号
- `path_list_leaf_nodes_are_files`
  控制叶子节点默认按文件还是目录解释

这里先按“输入文件描述一棵目录树，并直接为这棵树生成 record”的方式理解，不引入额外的模板复制目录。

## 4. 核心内存数据结构

输入文件不会直接变成 record，而是先解析成一棵内存树。

### 4.1 单个节点结构

每个目录或文件节点可以抽象成：

```cpp
struct PathListNode {
    std::string name;
    std::string relative_path;
    size_t parent;
    uint32_t depth;
    std::vector<size_t> children;
    bool explicit_entry;
    bool explicit_dir;
};
```

字段含义：

- `name`
  当前节点名字，不包含父路径
- `relative_path`
  从根开始的相对路径
- `parent`
  父节点在 `nodes` 数组里的下标
- `depth`
  当前节点深度
- `children`
  子节点在 `nodes` 数组里的下标列表
- `explicit_entry`
  这个节点是否在输入文件中显式出现过
- `explicit_dir`
  这个节点是否被输入明确标记成目录，通常对应原始路径带尾 `/`

### 4.2 整棵树结构

整棵树保存在一个数组中：

```cpp
std::vector<PathListNode> nodes;
```

这是一种数组式树结构：

- `nodes[0]` 是虚拟根节点
- 每个节点通过 `parent` 找父节点
- 每个节点通过 `children` 找子节点

这种表示方式的好处是后续可以直接用节点下标建立映射，例如：

```text
节点下标 -> inode 编号
```

## 5. 从文本到内存树

### 5.1 创建虚拟根节点

解析开始时，系统先创建一个虚拟根节点：

```text
name = "/"
relative_path = ""
parent = invalid
depth = 0
```

这个根节点不对应输入文件中的某一行，只是为了统一表达整棵树。

### 5.2 逐行读取和路径规范化

系统顺序读取输入文件中的每一行。每一行先做路径规范化：

- 去掉首尾空白
- 把 `\` 转成 `/`
- 去掉前导 `./`
- 去掉前导 `/`
- 合并重复的 `/`
- 去掉尾部 `/`
- 如果出现 `..`，直接报错
- 如果结果是空串或 `.`，则跳过

例如：

```text
.
```

会被视为空路径，不参与建树。

而：

```text
./68124af5.../headChange/66ca87....jpg
```

会被规范化为：

```text
68124af5.../headChange/66ca87....jpg
```

### 5.3 拆分路径段

规范化后的路径会按 `/` 拆成多个路径段：

```text
68124af5.../headChange/66ca87....jpg
```

拆成：

```text
["68124af5...", "headChange", "66ca87....jpg"]
```

### 5.4 插入数组式树

系统从虚拟根节点开始，按路径段逐层往下插入。

为了避免重复创建节点，解析过程中会维护一个辅助索引表：

```text
完整相对路径 -> nodes 下标
```

这里的 key 是完整相对路径，不是单个文件名或目录名。这样即使不同目录下都叫 `headChange`，也不会冲突。

例如：

```text
68124.../headChange -> 某个节点下标
505871.../headChange -> 另一个节点下标
```

插入规则是：

1. 从根节点开始
2. 拼出当前层级的完整相对路径
3. 如果这个完整路径已经存在，就复用已有节点
4. 如果不存在，就在 `nodes` 中创建新节点
5. 把新节点挂到父节点的 `children` 中
6. 当前输入行处理完成后，把最后一个节点标记为显式出现过

## 6. 样例解析结果

对下面几行输入：

```text
./68124af5a3f44530be5fe4e1d37d433c
./68124af5a3f44530be5fe4e1d37d433c/headChange
./68124af5a3f44530be5fe4e1d37d433c/headChange/66ca87b9b36148b39f575aa469f17101.jpg
```

内存树会形成：

```text
/
└── 68124af5a3f44530be5fe4e1d37d433c
    └── headChange
        └── 66ca87b9b36148b39f575aa469f17101.jpg
```

对应到 `nodes` 数组，可以理解成：

```text
nodes[0]: name="/", parent=invalid, children=[1]
nodes[1]: name="68124af5a3f44530be5fe4e1d37d433c", parent=0, children=[2]
nodes[2]: name="headChange", parent=1, children=[3]
nodes[3]: name="66ca87b9b36148b39f575aa469f17101.jpg", parent=2, children=[]
```

对完整样例来说，根节点下面会有多个一级目录，每个一级目录下面通常有 `headChange`，再往下挂具体图片文件。

## 7. 树构建后的整理和统计

内存树构建完成后，还会进行一轮整理和统计：

- 把每个节点的子节点按名字排序
- 判断每个节点是目录还是文件
- 统计目录数、文件数、最大深度
- 统计每一层的目录数和文件数
- 生成整棵树的指纹值

目录和文件的判断规则不是简单地看有没有在输入中出现，而是综合判断：

- 根节点一定是目录
- 有子节点的一定是目录
- 显式带尾 `/` 的节点一定是目录
- 叶子节点还要结合 `path_list_leaf_nodes_are_files` 配置判断

例如：

```text
./a5925fed739c451d800f64302582bd79
```

它是一个叶子节点。它后续被当成目录还是文件，要看配置和判定规则。

## 8. 从内存树到 inode 分配

生成 `inode.records` 前，需要先给内存树中的每个节点分配 inode。

inode 分配顺序是稳定的：

1. 虚拟根目录先分配 `inode_start`
2. 根目录下面的子节点按名字排序后依次处理
3. 每个子树按 DFS 前序遍历处理
4. 访问到一个节点时，立即为它分配一个 inode

因此，inode 分配顺序不是输入文件的原始行顺序，而是整理后的树遍历顺序。

## 9. 为什么需要节点到 inode 的映射

`nodes` 只保存树结构，表示“哪个节点是谁的父节点、哪个节点有哪些子节点”。它不保存最终分配出来的 inode 编号。

所以分配 inode 时，需要维护一个和 `nodes` 等长的数组：

```text
node_inode_ids[node_index] = inode_id
```

例如：

```text
nodes[2] = headChange
nodes[3] = 66ca87.jpg

node_inode_ids[2] = 1003
node_inode_ids[3] = 1004
```

这张表的作用是把“树节点关系”转换成“inode 关系”。

例如树里只知道：

```text
headChange 的孩子是 66ca87.jpg
```

但写目录项 record 时需要的是：

```text
parent_inode=1003
name="66ca87.jpg"
child_inode=1004
```

这里的父 inode 和子 inode 都要从 `node_inode_ids` 中查出来。

## 10. 生成 inode 属性

每访问到一个节点，系统先给它分配 inode，然后根据节点类型生成 inode 属性。

如果节点是目录，属性主要包括：

- `inode_id`
- `parent_inode_id`
- `type = DIR`
- `mode = 0755`
- `size = 4096`
- `nlink`
- `file_name`

如果节点是文件，属性主要包括：

- `inode_id`
- `parent_inode_id`
- `type = FILE`
- `mode = 0644`
- `size`
- `nlink = 1`
- `file_name`

这里的 inode 属性描述的是节点自身，不保存完整路径。完整路径关系由后续的 `dentry.records` 表达。

## 11. `inode.records` 的结构

`inode.records` 是一个顺序追加的二进制文件。每访问一个节点并生成 inode 属性后，就写出一条 inode record。

每条记录结构是：

```text
[u64 inode_id][u32 payload_len][payload]
```

字段含义：

- `inode_id`
  当前节点分配到的 inode 编号
- `payload_len`
  payload 的长度
- `payload`
  inode 属性的二进制编码结果

当前实现里，payload 使用 `UnifiedInodeRecord` 编码，长度固定为 `256` 字节。因此一条 inode record 通常是：

```text
8 字节 inode_id
4 字节 payload_len
256 字节 payload
```

逻辑上可以理解成：

```text
inode_id -> inode 元数据
```

但它仍然是顺序二进制 record 文件，不是最终的 Masstree key/value 文件。

## 12. `dentry.records` 的作用

`inode.records` 只保存每个节点自身的属性，不保存完整路径层级。

目录层级关系会写入 `dentry.records`。它描述的是：

```text
(parent_inode, name) -> (child_inode, type)
```

也就是说，`dentry.records` 负责把父目录 inode、子节点名字、子节点 inode 关联起来。

## 13. 总结

去掉额外展开逻辑后，这条链路可以概括成：

```text
目录树文本文件
-> 逐行规范化路径
-> 构造成 nodes 数组式内存树
-> 整理和统计树结构
-> 按稳定 DFS 顺序分配 inode
-> 写出 inode.records
-> 根据父子关系写出 dentry.records
```

核心点是：

**先把文本路径转成一棵可遍历的内存树，再由这棵树生成 inode 和 dentry 两类元数据 record。**
