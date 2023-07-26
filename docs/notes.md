- how to support `record append`, `sequential read`?

### chunk size: 64MB  

64MB is a relatively large chunk:

- pros: 
  - reduce metadata size
  - more likely to perform many operations on a given chunk, reducing network overhead by keeping a persistent TCP connection.

- cons: race condition happens (wr/rd at the same chunk) ↑, which leads to consistency problem.


### chunksever: 3 replicas

Use leases (timeout interval: 60s) to maintain a consistent mutation order across replicas. 

### master node: 1 (only one) 

![](static/layout.png)

- pros: easy to implement, and guarantee consistency (compared to multi-master nodes).

- cons: may become the system's performance bottleneck.


#### Metadata: 

1. namespace: file name → array of chunk handles (nv, non-volatile)

2. chunk handle → list of chunksevers (v, volatile) *because you can recover it by requiring servers during the reboot process

      ​                              current version (nv)

      ​			      primary chunk (v)

      ​			      lease expiration (v)


3. chunk version (nv, in order to detect Stale Replica)
#### Optimizations

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

### Rd/Wr

write: overwrite & append
- overwrite: doesn't require performance, but correctness should be guaranteed.
- append: performance↑, allow anomalies, but the append data should be preserved (see Consistency Section).

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


- [x] why concurrent success is `consistent` but undefined`?

  Because there may be multiple chunks waiting to be modified 
  $⇒$ c1 (primary order: 1 2 3) c2 (primary order: 3 2 1)

- [ ] what's padding?
- [ ] why **atomic**?

### Snapshot
COW (copy on write)

### GC



> **Rule of Thumb:** chunkserver 可以随意创建chunk, 即使过程中出现error导致master没有记录它已经创建了chunk, 也可以在后续GC的时候回收

### Master's Operations

#### HeartBeat

chunkserver $\rightarrow$ master: containing server's status



namespace manager (e.g. `/d1/d2/.../dn/leaf`):

#### Create (File) / Mkdir

1. acquire read-locks on the directory names `/d1, /d1/d2, ..., /d1/d2/.../dn-1`
2. acquire write-locks on `/d1/d2/.../dn` $\rightarrow$ in order to create a file/directory

#### GetFileInfo

TODO: cached info locally?

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