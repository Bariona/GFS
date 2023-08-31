## <img src="docs/static/icon.png" width="40"> the Google File System

![](https://img.shields.io/badge/language-Go-blue)

> a toy implementation of the Google File System(GFS) in golang.

### Test

greybox_test.go: 1 master 5 chunkservers 


```bash
$ cd ./src/gfs
$ go test -v 
$ go test -run="TESTCASENAE1|TESTCASE2|..."
```



### Support Features

Read, Create, Delete, Write, Atomic Record Append, Garbage Collect (Stale Chunk Detection), Snapshot, Fault Tolerance (Chunk Re-replication)

Detailed Explanation: [see here](./docs/notes.md)

### Project Layout 

~~View the raw code to see an organized layout (~~

<details>
    <summary>layout</summary>
<p>


.
├── README.md
├── docs
│   ├── GFS.pdf
│   ├── config.md
│   ├── notes.md
│   └── static
│       ├── consist.png
│       ├── icon.png
│       ├── layout.png
│       └── pipeline.png
├── servers.txt
└── src
    ├── gfs
    │   ├── chunkserver
    │   │   ├── chunkserver.go
    │   │   └── download_buffer.go
    │   ├── client
    │   │   └── client.go
    │   ├── cmd
    │   │   └── main.go
    │   ├── common.go
    │   ├── go.mod
    │   ├── go.sum
    │   ├── graybox_test.go
    │   ├── master
    │   │   ├── chunk_manager.go
    │   │   ├── chunkserver_manager.go
    │   │   ├── master.go
    │   │   └── namesapce_manager.go
    │   ├── rpc_structs.go
    │   └── util
    │       ├── array_set.go
    │       └── util.go
    └── gfs_stress
        ├── atomic_append_success.go
        ├── cmd
        │   ├── center
        │   │   └── stress_center.go
        │   └── node
        │       └── stress_node.go
        ├── consistency_write_success.go
        ├── fault_tolerance.go
        ├── go.mod
        └── stress.go
</p>
</details>


### Reference

1.  [Effective Go](https://go.dev/doc/effective_go)
2.  [paper of GFS](https://research.google.com/archive/gfs-sosp2003.pdf)​
3.  MIT6.824 by Robert Morris
