# PPCA 2016: Google File System

1或2人一组，完成一份简化版的 [Google File System](http://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) 实现，需要通过灰盒测试和压力测试。请在 Github 或者 Bitbucket 上建立私有 git 仓库，并使用 git 进行版本管理。

## 功能

你需要完成 Master, Chunkserver 和 Client 三者的编写。以下基本要求一定要达到的：

* 文件系统操作：`create`, `mkdir`
* 文件操作：`read`, `write`, `append`
* Chunkserver 掉线、上线不会影响正常使用
* Master 和 Chunkserver 的 Metadata 持久化（重启时能载入）
* 当存活的 Replica 数目过少时进行 Re-replication

以下是可选要求：

* Chunkserver 文件校验



## 代码框架

你可以依靠本仓库下的代码框架。你可以做任何修改，只要能通过灰盒测试以及压力测试即可。

## 灰盒测试

灰盒测试运行在本地，用来检验实现是否基本正确。很抱歉测试的粒度非常粗糙。测试的运行方法：

```bash
export GOPATH=/path/to/your/git/repository
go test -v
```

## 压力测试

压力测试会分布式地运行在所有机子上，通过大量的业务操作来检查实现中可能存在的问题，同时也兼有性能测试的功能。

压力测试有两个角色：负责控制整个测试的中心节点 *Center*，以及若干测试节点 *Node*，其中有一个测试节点运行Master，其余运行Chunkserver。测试节点可以以RPC的形式调用中心节点的函数，中心节点可以广播消息给所有测试节点。

压力测试分为以下三项：

* ConsistencyWriteSuccess: 测试在成功写入时的一致性
  1. 每个测试节点重复若干次往同一个文件的随机位置写入随机大小的数据
  2. 中心节点随机生成若干组校验区间
  3. 每个测试节点从中心节点获取所有校验区间
  4. 每个测试节点读取每个校验区间的数据并计算校验码
  5. 每个测试节点将所有校验区间的校验码发送给中心节点
  6. 中心节点判断所有测试节点的校验码是否一致
* AtomicAppendSuccess: 测试在成功追加时的原子性
  1. 每个测试节点重复 `N` 次往同一个文件追加随机大小的数据
     1. 产生随机大小的数据 `data`
     2. 计算数据的校验码 `checksum`
     3. 使用 `gob` 序列化四元组 `(node_id, ordinal, data, checksum)` 得到结果 `content`
     4. 在 `content` 前面追加8字节数据头：前4字节是魔术数 `0x20160808`，后4字节是 `len(content)`
     5. 以 `content` 作为追加的数据
  2. 每个测试节点把最后一次追加得到的偏移量汇报给中心节点
  3. 中心节点给每个测试节点分配若干校验区间
  4. 每个测试节点从中心节点获取所有校验区间
  5. 每个测试节点读取校验区间内的数据
     1. 根据数据头试图恢复四元组 `(node_id, ordinal, data, checksum)`
     2. 检查 `data` 的检验码是否为 `checksum`
     3. 记录检查正确的二元组 `(node_id, ordinal)`
  6. 每个测试节点将所有检查正确的二元组 `(node_id, ordinal)` 汇报给中心节点
  7. 中心节点检验集合 `{(node_id, ordinal)}` 的大小是否为 `测试节点数目 * N`
* FaultTolerance: 测试Chunkserver意外退出及重新上线时系统是否能正常运行
  1. 测试内容与上一个测试相同
  2. 每个测试节点每秒钟以一定概率关闭Chunkserver
  3. 被关闭的Chunkserver在随机时间后重新启动

测试的运行方法：

```bash
export GOPATH=/path/to/your/git/repository

# Add ID List to servers.txt
vim servers.txt

# Run Center
go run stress/stress_center.go -listen <center_addr>

# Run Node as Master
go run stress/stress_node.go -center <center_addr> -eth <eth> -id <master_id> -role master -listen <master_addr>

# Run Node as Chunkserver
go run stress/stress_node.go -center <center_addr> -eth <eth> -master <master_addr> -role chunkserver -listen <chunkserver_addr_i> -id <chunkserver_id_i>
```

* `<center_addr>`: Center 监听的地址
* `<eth>`: 网络适配器的名称。可以通过 `ifconfig` 查看。如果是在本地测试请使用 `lo`（注：本地测试的速度信息是不正确的）
* `<master_id>`: Master 的名称
* `<chunkserver_id_i>`: 第 `i` 台Chunkserver的名称
* `<chunkserver_addr_i>`: 第 `i` 台Chunkserver的监听地址
* `servers.txt`:
  * 第一行：Master 的名称
  * 后面的每一行：第 `i` 台Chunkserver的名称

