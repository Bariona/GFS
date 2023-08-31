## Understanding of GFS

[TOC]

(纪要)GFS 的几个特点:

1. single master节点 (实现简单, 避免一些一致性带来的麻烦)
   - 性能瓶颈问题: metadata存储, lease的引入
2. 每个文件分为多个chunk存储, 每个chunk的size较大(64MB)且是分布式存储在多个chunkserver上(论文默认数量: 3 chunkservers)
   - read, write上的问题
3. data-flow和control-flow分离以保证高效, consistency model保证对文件末尾的append操作是atomic的



### Why Chunk size: 64MB  

64MB is a relatively large chunk:

- pros: 
  - reduce metadata size
  - more likely to perform many operations on a given chunk, reducing network overhead by keeping a persistent TCP connection.

- cons: race condition happens (wr/rd at the same chunk) ↑, which leads to **consistency problem**.


### Why chunksever: 3 replicas

Use leases (timeout interval: 60s) to maintain a consistent mutation order across replicas. 

### Why master node: 1 (only one) 

![](static/layout.png)

- pros: easy to implement, and guarantee consistency (compared to multi-master nodes).

- cons: may become the system's performance bottleneck.


#### Metadata Storage: 

1. namespace: file name → array of chunk handles (nv, non-volatile)

2. chunk handle → list of chunksevers (v, volatile) *because you can recover it by requiring servers during the reboot process

      ​                              current version (nv)

      ​			      primary chunk (v)

      ​			      lease expiration (v)


3. chunk version (nv, in order to detect Stale Replica)
#### Optimizations:

1. **separate control flow (master) & data flow** (client $\leftrightarrow$ chunksever)
2. the client will cache some master's metadata (also to relieve control flow burden)
3. (mentioned above) enlarge chunk size to reduce metadata size, so that all metadata can be placed in DRAM

> e.g. suppose each chunk's metadata < 64 bit, then for a 1TB file, it only needs: $\frac{3\cdot 1\text{TB}}{64 \text{MB}} \times 64\text{bit}$ for metadata storage.



### Availability

~~consensus algorithm (Paxos, Raft)~~

in GFS, we consider 2 cases: metadata & file data.

#### Metadata 

i.e. the availability of the master node

solution: master-slave (主备思想) 

- primary master
- shadow master

maintain a write-ahead log (**WAL**).

pros: 

1. random Disk write → sequential log write (faster) 

2. sync between master & slave (faster, sync log instead of data)

   easier to replay

process: write WAL locally, then sync WAL, only then will we start file modification.

<!-- checkpoint: 	TODO: -->

#### File data

3 chunkserver replicas.

wr: write to 3 replicas, a crash will let the master node recreate another chunkserver. (maintain a checksum to detect errors)

##### Leases

permission of writes: transferred from master to client (60s)

> *Primary* only decides the order of write (control flow), data flow doesn't have to go through it (instead adopt nearby principle)
> 
> \*On traditional master-slave strategy, data/control flow always flows from master to slave.

##### Chunk Replacement

3 cases:

1. new chunk created
2. chunk re-replication (occurs when some servers crashed)
3. rebalancing := new + delete chunk (reduce specific sever's burden)

3 principles: 

1. new chunkserver's utilization is low
2. new chunkserver doesn't create chunks intensively recently
3. the new replica is not allowed to be on the same rack



### Read

代码流程: 

1. src/gfs/client/client.go 发起write操作

   ```go
   // ReadChunk reads data from the chunk at specific offset.
   // len(data)+offset  should be within chunk size.
   func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) 
   ```

2. src/gfs/master/master.go 返回对应chunk的replicas:

   ```go
   // RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
   func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error 
   ```

3. src/gfs/chunkserver/chunkserver.go 挑选离它最近的chunkserver, 发起RPC对server进行读:

   ```go
   // RPCReadChunk is called by client, read chunk data and return
   func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error
   ```

   ​

### Write & Append

- overwrite: doesn't require performance, but correctness should be guaranteed.
- append: performance↑, allow anomalies, but the append data should be preserved (see Consistency Section).

代码流程: 

1. src/gfs/client/client.go 发起write操作

   ```go
   // Write writes data to the file at specific offset.
   func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error
   ```

2. src/gfs/master/chunk_manager.go 中master节点返回lease, 并存储在 src/gfs/client/buffer.go 

   ```go
   // GetLeaseHolder returns the chunkserver that holds the lease of a chunk
   // (i.e. primary) and expire time of the lease. If no one has a lease,
   // grant one to a replica it chooses.
   func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) ([]gfs.ServerAddress, *gfs.Lease, error)
   ```

3. src/gfs/chunkserver/chunkserver.go 中client通过RPC调用chunkserver进行data的传输:

   ```go
   // RPCPushDataAndForward is called by client.
   // It saves client pushed data to memory buffer and forward to all other replicas.
   // Returns a DataID which represents the index in the memory buffer.
   func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error
   ```

4. client发起指令让所有chunkserver写入data:

   ```go
   // RPCWriteChunk is called by client to call the primary to 
   // apply chunk write to itself and asks secondaries to do the same.
   func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error
   ```

Requirement: write all 3 replicas.

#### Pipeline

![](static/pipeline.png)

1. `master node` chooses a `primary node` (among those nodes who has the latest chunk version) and **then increases the chunk version** (注: 只有当master认为当前没有primary的时候才会increase).
`master node` notifies primary and secondary nodes to increase the chunk version, then MS writes the #version.
2. MS inform the client of the prim & secs and then the client cached those information.
3. The client sends the data (starting to "propagate" among servers), waiting for all data to be stored locally on servers.
4. Once all the replicas have acknowledged receiving the data, the client sends a write request to the primary. 
The primary assigns consecutive serial numbers to all the mutations it receives, possibly from multiple clients, which provides the necessary serialization. It applies the mutation to its own local state in serial number order.
5. The primary forwards the write request to all secondary replicas. Each secondary replica applies mutations in the same serial number order assigned by the primary.
6. The secondaries all reply to the primary indicating that they have completed the operation.
7. The primary replies to the client. Any errors encountered at any of the replicas are reported to the client. In case of errors, the write may have succeeded at the primary and an arbitrary subset of the secondary replicas. (If it had failed at the primary, it would not have been assigned a serial number and forwarded.) The client request is considered to have failed, and the modified region is left in an inconsistent state. Our client code handles such errors by retrying the failed mutation. It will make a few attempts at steps (3) through (7) before falling back to a retry from the beginning of the write.

If a write by the application is large or straddles a chunk boundary, **GFS client code breaks** it down into multiple write operations.

> If any secondary failed to apply the mutation order, then the Op returns `fail` to the client. Then, you are not guaranteed to read the correct chunk version.

> Avoid split-brain: Lease only lasts for 60s.
#### Data/Control flow split

- [ ] overwrite: how to ensure consistency when several chunks succeeded & failed.

  > As for `append`, it shouldn't be a problem: if failed, TODO: older version data?


read Op:

- metadata cache miss: query master node, then chunkserver
- metadata cache hit: query chunkserver, if it doesn't have the corresponding chunk, query master again (meaning that the cache should be invalidated)
  - checksum.


### Consistency Model

![](static/consist.png)

- consistent: A file region is consistent if all clients will always see the same data, regardless of which replicas they read from.
- inconsistent:
- defined:  A region is defined after a file data mutation if it is consistent and clients will see what the mutation writes in its entirety. (stricter than consistent)


- [x] why concurrent success is `consistent but undefined`?

  Because there may be multiple chunks waiting to be modified 
  $⇒$ c1 (primary order: 1 2 3) c2 (primary order: 3 2 1)

- [x] why record append is `defined interspersed with inconsistent`?

  there may be error in some chunkservers => multi-append

- [x] what's padding?

  $⇒$ create a new chunk for append

### Snapshot

- revoke all leases first

  在master.go的

  ```go
  func (m *Master) RPCSnapshot(args gfs.SnapshotArg, reply *gfs.SnapshotReply) error
  ```

  函数中, 对file的对应chunk进行逐个invalid lease: 做法是通过RPC调用chunkserver标记某个chunk的lease已经失效(因为master无法知道client的信息而直接invalid client cache的lease, 所以采用这种间接的方式)

  ```go
  err := util.Call(ck.primary, "ChunkServer.RPCInvalidChunk", gfs.InvalidChunkArg{handle, true}, nil)
  ```

  ​


- COW (copy on write):

  在chunk_manager.go中会判断reference count是否大于1, 来进行split

  ```go
  // check reference count
  	if ck.refCnt > 0 {
  		cm.Lock()
  		cm.numChunkHandle += 1
  		newHandle := cm.numChunkHandle
  		cm.chunk[newHandle] = &chunkInfo{
  			location: ck.location,
  			version: ck.version,
  			expire: time.Now(),
  		}
  		cm.Unlock()
  		log.Info("ADDED CHUNK")

  		log.Printf("Master: Split Chunk (%v) -> copy (%v) at %v", handle, newHandle, ck.location)
  		err := util.CallAll(ck.location, "ChunkServer.RPCChunkCopy", gfs.ChunkCopyArg{handle, newHandle})
  		if err != nil {
  			log.Warn(err)
  		}
  		ck.refCnt--
  	}
  ```

  ​

### GC

- stale chunk detect (through heartBeat message):

  做法: 在write/append的时候维护chunk的版本号version, 每次写操作的时候version+=1, 如果在发放lease的时候发现某个本地版本的chunk过时, 就可以加入stale chunk的队列中, 等待删除:

  ```go
  // RPCGetLease is called by the client
  // it returns lease holder and secondaries of a chunk.
  // If no one holds the lease currently, grant one.
  func (m *Master) RPCGetLease(args gfs.GetLeaseArg, reply *gfs.GetLeaseReply) error {
  	stales, lease, err := m.cm.GetLeaseHolder(args.Handle)
  	m.csm.RemoveChunk(stales, args.Handle)
  	for _, staleServer := range stales {
  		err := m.csm.AddGarbage(staleServer, args.Handle)
  		if err != nil {
  			return err
  		}
  	}
  	...
  }
  ```

  同时, chunkserver.go中隔段时间会进行一次gc以删除stale chunks:

  ```go
  func (cs *ChunkServer) garbageCollect()
  ```

- Re-replication:

  在src/gfs/master.go中: 找到需要re-replica的chunk

  ```go
  func (m *Master) reReplication(handle gfs.ChunkHandle) error {
  	from, to, err := m.csm.ChooseReReplication(handle)
  	// send data & write down
  	var r gfs.CreateChunkReply
  	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &r)
  	
  	var r1 gfs.SendCopyReply
  	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &r1)

  	// update info 
  	err = m.cm.RegisterReplica(handle, to, false)
  	m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
  	return nil
  }
  ```



> **Rule of Thumb:** chunkserver 可以随意创建chunk, 即使过程中出现error导致master没有记录它已经创建了chunk, 也可以在后续GC的时候回收

### Master's Operations

#### HeartBeat

chunkserver $\rightarrow$ master: containing server's status

namespace manager (e.g. `/d1/d2/.../dn/leaf`):

#### Create (File) / Mkdir

1. acquire **read-locks** on the directory names `/d1, /d1/d2, ..., /d1/d2/.../dn-1`
2. acquire **write-locks** on `/d1/d2/.../dn` $\rightarrow$ in order to create a file/directory

src/gfs/master/namesapce_manager.go

```go
// acquire read lock along the parents (e.g. /d1/d2/.../dn/leaf):
// 
// acquire read-locks on the directory names /d1, /d1/d2, ..., /d1/d2/.../dn
// 
// If RLockLeaf = True, then acquire read-locks on /d1/d2/.../dn/leaf
func (nm *namespaceManager) lockParents(paths []string, RLockLeaf bool) (*nsTree, error) 

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error
```

#### GetChunkHandle

GetChunkHandle returns the chunk handle of (path, index). If the requested index is bigger than the number of chunks of this path by exactly one, create one.

create: 

1. get 3 chunkservers
   > use *HeartBeat* message to maintain alive servers
2. add chunk

#### GetLeaseHolder

GetLeaseHolder returns the chunkserver that holds the lease of a chunk (i.e. primary) and expire time of the lease. If no one has a lease, grant one to a replica it chooses.

3.1 Para 2.

#### WriteChunk

see the notes above



### Chunkserver's Operations

#### CreateChunk

lazy allocation

#### ReadChunk

~~cache information~~

#### WriteChunk

see the notes above