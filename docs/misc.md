### Go relevant grammar:

1. interface{}: can accept any type
2. RPC rule: 
   方法只接受两个可序列化参数, 其中第二个参数是指针类型，并且返回一个 error 类型，同时必须是公开的方法。

3. defer在return之后, defer 栈


### To-Do List

- [ ] Fault Tolerance
    1. checksum
    2. stale replica detect
    3. WAL log

- [x] no global time: then how to determine Lease Expiration?

    Timestamp is only given by master, so it's ok.

- [x] ChunkLease extension
- [ ] CheckSum
- [x] Snapshot
- [x] GC
- [x] If chunkserver gets a new version number, but dead immediately, then what (stale content)?
      then the client gets a fault, where the client should know something went wrong.
- [x] separate meta-data file to be stored in the fs with an additional super-file(block) to describe the number of files
