package master

import (
	"encoding/gob"
	"fmt"
	"gfs/util"
	"net"
	"net/rpc"
	"os"
	"path"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
)

// Master Server struct
type Master struct {
	address    	gfs.ServerAddress // master server address
	serverRoot 	string            // path to metadata storage
	l          	net.Listener
	shutdown   	chan struct{}
	dead 			 	bool

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

const (
	metaNMFile 	= "gfs-master.metaNM"
	metaCMFile 	= "gfs-master.metaCM"
	perm 			= os.FileMode(0644)
)


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

	log.Info("New Master!")
	err := m.loadMeta()
	if err != nil {
		log.Warn("Master Load Metadata error: ", err)
	}

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
				if !m.dead {
					log.Fatal("accept error:", err)
					log.Exit(1)
				}
			}
		}
	}()

	// Background Task
	go func() {
		heartBeatCh := time.Tick(gfs.BackgroundInterval)
		storeMetaCh := time.Tick(gfs.MasterMetaStoreInterval)
		for {
			select {
			case <- m.shutdown:
				return
			case <- storeMetaCh: 
				err := m.storeMeta()
				if err != nil {
					log.Warn("Master storeMeta error: ", err)
				}
			case <- heartBeatCh:
				err := m.BackgroundActivity()
				if err != nil {
					log.Fatal("Background error ", err)
				}
			}	
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	if !m.dead {
		log.Warn("Master shuts down")
		err := m.storeMeta()
		if err != nil {
			log.Warn("Master storemeta error: ", err)
		}
		m.dead = true
		close(m.shutdown)
		m.l.Close()
	}
}

func (m *Master) loadMeta() error {
	// meta of namespace manager
	filename := path.Join(m.serverRoot, metaNMFile)
	file, err := os.OpenFile(filename, os.O_RDONLY, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	var nmTreeMeta []persistNsTree
	dec := gob.NewDecoder(file)
	err = dec.Decode(&nmTreeMeta)
	if err != nil {
		return err
	}
	m.nm.decodeNsTree(nmTreeMeta)

	// meta of chunk manager
	filename = path.Join(m.serverRoot, metaCMFile)
	file, err = os.OpenFile(filename, os.O_RDONLY, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	var cmMeta persistCM
	dec = gob.NewDecoder(file)
	err = dec.Decode(&cmMeta)
	if err != nil {
		return err
	}
	m.cm.decodeCM(cmMeta)

	return nil
}

func (m *Master) storeMeta() error {
	// meta of namespace manager
	filename := path.Join(m.serverRoot, metaNMFile)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	nsTreeMeta := m.nm.encodeNsTree()

	enc := gob.NewEncoder(file)
	err = enc.Encode(nsTreeMeta)
	if err != nil {
		return err
	}

	// meta of chunk manager
	filename = path.Join(m.serverRoot, metaCMFile)
	file, err = os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	cmMeta := m.cm.encodeCM()

	enc = gob.NewEncoder(file)
	err = enc.Encode(cmMeta)
	if err != nil {
		return err
	}
	return nil
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	deadck := make(map[gfs.ChunkHandle]bool)
	deadAddrs := m.csm.DetectDeadServers()
	for _, addr := range deadAddrs {
		handles, err := m.csm.RemoveServer(addr)
		log.Info("DeadServer: ", addr, " handles: ", handles)
		if err != nil {
			return err
		}
		for _, handle := range handles {
			deadck[handle] = true
			m.cm.RemoveReplica(handle, addr)
		}
	}

	for handle, ok := range deadck {
		if !ok {
			continue
		}
		location, err := m.cm.GetReplicas(handle)
		if err != nil {
			return err
		}
		if len(location) < gfs.MinimumNumReplicas {
			// need re-replica
			// TODO: set chunk handling priority, pick existing chunkserver according to disk-util etc.
			if len(location) == 0 {
				log.Warn("ChunkHandle ", handle, " has lost in all replicas")
				continue
			}
			m.cm.Lock()
			m.cm.reReplicas = append(m.cm.reReplicas, handle)
			m.cm.Unlock()
		}
	} 

	handles := m.cm.GetRereplicas()
	if len(handles) == 0 {
		return nil
	}
	m.cm.RLock()
	defer m.cm.RUnlock()
	for _, handle := range handles {
		ckinfo := m.cm.chunk[handle]
		if ckinfo.expire.Before(time.Now()) {
			ckinfo.Lock() // don't grant lease during reReplication
			err := m.reReplication(handle)
			if err != nil {
				log.Warn("Re-replication: ", err)
			}
			ckinfo.Unlock()
		}
	}
	return nil
}

func (m *Master) reReplication(handle gfs.ChunkHandle) error {
	from, to, err := m.csm.ChooseReReplication(handle)
	log.Printf("Master: Re-replica from(%v) to(%v), handle: %v", from, to, handle)
	if err != nil {
		return err
	}
	// send data & write down
	var r gfs.CreateChunkReply
	err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &r)
	if err != nil {
		return err
	}

	var r1 gfs.SendCopyReply
	err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &r1)
	if err != nil {
		return err
	}

	// update info 
	err = m.cm.RegisterReplica(handle, to, false)
	if err != nil {
		return err
	}
	m.csm.AddChunk([]gfs.ServerAddress{to}, handle)
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	isFirst := m.csm.Heartbeat(args.Address, reply)

	// Lease Extension
	for _, handle := range args.LeaseExtensions {
		m.cm.ExtendLease(handle, args.Address)
	}

	if isFirst { // First Heart Beat
		log.Info("New Chunkserver ", args.Address)
		staleHandles, latestHandles, err := m.cm.StaleChunkDetect(args.Address)
		reply.Garbage = staleHandles
		if err != nil {
			return err
		}

		for _, handle := range latestHandles {
			m.csm.AddChunk([]gfs.ServerAddress{args.Address}, handle)
		}
	}

	return nil
}

// RPCGetLease is called by the client
// it returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetLease(args gfs.GetLeaseArg, reply *gfs.GetLeaseReply) error {
	stales, lease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}
	m.csm.RemoveChunk(stales, args.Handle)
	for _, staleServer := range stales {
		err := m.csm.AddGarbage(staleServer, args.Handle)
		if err != nil {
			return err
		}
	}

	reply.Expire = lease.Expire
	reply.Primary = lease.Primary
	reply.Secondaries = lease.Secondaries
	return nil
}

func (m *Master) RPCSnapshot(args gfs.SnapshotArg, reply *gfs.SnapshotReply) error {
	path, filename := args.Path.ParseLeafname()
	paths := path.GetPaths()

	node, err := m.nm.lockParents(paths, true)
	defer m.nm.unlockParents(paths, true)
	if err != nil {
		return err
	}

	file, ok := node.children[filename]
	if !ok {
		return fmt.Errorf("file %v doesn't exist", filename)
	}

	file.Lock()
	defer file.Unlock()

	invalidHandles := make([]gfs.ChunkHandle, 0)
	// === invalid leases ===
	for idx := int64(0); idx < file.chunks; idx++ {
		handle, err := m.cm.GetChunk(args.Path, gfs.ChunkIndex(idx))
		if err != nil {
			return err
		}
		err = m.cm.InvalidLease(handle, true)
		invalidHandles = append(invalidHandles, handle)
		if err != nil {
			return err
		}
	}


	
	// unfrozen
	for _, handle := range invalidHandles {
		err := m.cm.InvalidLease(handle, false)
		if err != nil {
			return err
		}
	}
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


// RPCGetChunkNum returns the number of chunk handles of the file of the path.
func (m *Master) RPCGetChunkNum(args gfs.GetChunkNumArg, reply *gfs.GetChunkNumReply) error {
	path, filename := args.Path.ParseLeafname()
	paths := path.GetPaths()

	node, err := m.nm.lockParents(paths, true)
	defer m.nm.unlockParents(paths, true)
	if err != nil {
		return err
	}

	file, ok := node.children[filename]
	if !ok {
		return fmt.Errorf("file %v doesn't exist", filename)
	}

	file.RLock()
	reply.Cnt = int(file.chunks)
	file.RUnlock()

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
	file.length += gfs.MaxChunkSize
	servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)
	if err != nil {
		return err
	}

	reply.Handle, err = m.cm.CreateChunk(args.Path, servers)
	if err != nil {
		return err
	}

	available := make([]gfs.ServerAddress, 0)
	errList := ""
	for _, server := range servers {
		err := util.Call(server, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: reply.Handle}, &gfs.CreateChunkReply{})
		if err != nil {
			errList += err.Error()
		} else {
			available = append(available, server)
		}
	}
	m.csm.AddChunk(available, reply.Handle)

	if errList == "" {
		return nil
	} else {
		return fmt.Errorf(errList)
	}
}

// RPCList returns all files and directories under args.Path directory
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	var err error
	reply.Files, err = m.nm.List(args.Path)
	log.Printf("%v, %v", args.Path, reply.Files)
	return err
}
