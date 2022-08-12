# TinyDFS

分布式文件系统学习项目

## 安装

需要自行安装 Thrift，在项目根目录下运行如下命令

```bash
thrift -r --gen go ./thrift/NameNode.thrift
thrift -r --gen go ./thrift/DataNode.thrift
```

