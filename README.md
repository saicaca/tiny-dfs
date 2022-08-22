# TinyDFS

分布式文件系统学习项目

## 安装

需要自行安装 Golang 和 Thrift

在项目根目录下运行如下命令

```bash
thrift -r --gen go ./thrift/main.thrift
```

## 编译

以 Windows 平台为例

```bash
go build -o ./out/tdfs.exe ./src/user-client/
go build -o ./out/namenode.exe ./src/namenode-server/
go build -o ./out/datanode.exe ./src/datanode-server/
```

## 运行

### NameNode 服务器

**示例**

```bash
namenode -port 19100 -limit 2 -interval 30
```

**参数说明**

| 参数名称  | 说明                                     | 默认值 |
| --------- | ---------------------------------------- | ------ |
| -port     | 服务启动的端口号                         | 19100  |
| -limit    | 系统为文件保持的最小副本数量             | 2      |
| -interval | 对 DataNode 进行心跳检测的时间间隔（秒） | 30     |
| -h        | 显示帮助                                 | -      |

### DataNode 服务器

**示例**

```bash
datanode -nnaddr "localhost:19100" -port 19200 -root "./storage/" -space "1GB"
```

**参数说明**

| 参数名称 | 说明                                            | 默认值            |
| -------- | ----------------------------------------------- | ----------------- |
| -nnaddr  | NameNode 的 URL                                 | "localhost:19100" |
| -port    | 服务启动的端口号                                | 19200             |
| -root    | 用于保存文件数据的目录                          | "./storage/"      |
| -space   | 分配给 DataNode 的存储空间，如 "1.5GB"、"512mb" | "1GB"             |
| -h       | 显示帮助                                        | -                 |

### 用户客户端

#### 创建配置文件

在 `tdfs` 客户端文件的同目录下创建 `config.yml` 文件，填入 NameNode 地址列表（因当前仅支持单 NameNode 所以只填入一个即可）

```yaml
namenode:
  - <host>:<port>
  - <host>:<port>
  - <host>:<port>
```

#### 指令

##### `put`

别名：`p`

从本地上传一个文件到服务器

```bash
tdfs put <local_file_path> <remote_file_path>
```

##### `get`

别名：`g`

从服务器下载一个文件到本地

```bash
tdfs get <remote_file_path> <local_file_path>
```

##### `move`

别名：`mv`, `rename`

从服务器重命名或移动一个文件

```bash
tdfs move <old_file_path> <new_file_path>
```

##### `delete`

别名：`rm`

从服务器移除一个文件

```bash
tdfs delete <file_path>
```

##### `stat`

别名：`s`

显示服务器上指定文件的信息

```bash
tdfs stat <file_path>
```

##### `list`

别名：`ls`

显示服务器指定目录下的所有文件和子目录信息

```bash
tdfs list <dir_path>
```

##### `mkdir`

检查服务器上指定的目录是否可用

```bash
tdfs mkdir <dir_path>
```

##### `datanodes`

别名：`dn`

显示当前已连接的所有 DataNode 的信息

```bash
tdfs datanodes
```

##### `help`

显示帮助

```bash
tdfs help
```
