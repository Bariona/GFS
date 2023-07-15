package master

import (
	// "fmt"
	"fmt"
	"net"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
	// "gfs/util"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.Tick(gfs.BackgroundInterval)
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			<-ticker

			err := m.BackgroundActivity()
			if err != nil {
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)
	// TODO: Lease Extensions
	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	lease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	reply.Expire = lease.expire
	reply.Primary = lease.primary
	reply.Secondaries = lease.secondaries
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	var err error
	reply.Locations, err = m.cm.GetReplicas(args.Handle)
	return err
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, reply *gfs.CreateFileReply) error {
	err := m.nm.Create(args.Path) // no need to reply
	return err
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, reply *gfs.MkdirReply) error {
	err := m.nm.Mkdir(args.Path) // no need to reply
	return err
}

// RPCGetFileInfo is called by client to get file information
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	err := m.nm.GetFileInfo(args.Path, reply)
	return err
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	path, filename := args.Path.ParseLeafname()
	paths := path.GetPaths()

	node, err := m.nm.lockParents(paths, true)
	defer m.nm.unlockParents(paths, true)
	if err != nil {
		return err
	}

	file, ok := node.children[filename]
	if !ok {
		// TODO: grey box test requires us to create a file (counter-intuitive behavior)
		// var r gfs.CreateFileReply
		// err := m.RPCCreateFile(gfs.CreateFileArg{Path: args.Path}, &r)
		// if err != nil {
		// 	return err
		// }
		// err = m.RPCGetChunkHandle(args, reply)
		// return err
		return fmt.Errorf("file %v doesn't exist", filename)
	}

	file.Lock()
	defer file.Unlock()

	if int64(args.Index) < file.chunks {
		reply.Handle, err = m.cm.GetChunk(args.Path, args.Index)
		return err
	}
	if int64(args.Index) > file.chunks {
		return fmt.Errorf("chunk index exceed in %v", args.Path)
	}

	// === add chunk ===
	file.chunks += 1
	servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	if err != nil {
		return err
	}
	reply.Handle, err = m.cm.CreateChunk(args.Path, servers)
	return err
}

// RPCList returns all files and directories under args.Path directory
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	var err error
	reply.Files, err = m.nm.List(args.Path)
	log.Printf("%v, %v", args.Path, reply.Files)
	return err
}