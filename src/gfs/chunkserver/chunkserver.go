package chunkserver

import (
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
	"gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	sync.RWMutex
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	// newborn								 	bool														// if chunkserver is new (reboot)
	dl                     	*downloadBuffer                	// expiring download buffer
	pendingLeaseExtensions 	*util.ArraySet                 	// pending lease extension
	chunk                  	map[gfs.ChunkHandle]*chunkInfo 	// chunk information
}

type Mutation struct {
	mtype   gfs.MutationType
	// version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	stale 				bool
	length 				gfs.Offset
	version       gfs.ChunkVersion               		// version number of the chunk in memory
	// newestVersion gfs.ChunkVersion               // allocated newest version number
	// mutations     map[gfs.ChunkVersion]*Mutation // mutation buffer
}

type persistChunkInfo struct {
	Handle 				gfs.ChunkHandle
	Length 				gfs.Offset
	Version       gfs.ChunkVersion               // version number of the chunk in disk
}

const (
	metaFile 	= "gfs-server.meta"
	perm 			= os.FileMode(0644)
)

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:    addr,
		shutdown:   make(chan struct{}),
		master:     masterAddr,
		serverRoot: serverRoot,

		// newborn:    true,	
		dl:         newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk: 			make(map[gfs.ChunkHandle]*chunkInfo),
	}

	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	err := cs.loadMeta()
	if err != nil {
		log.Warnf("Server %v Load Metadata error: %v", cs.address, err)
	}
	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	heartBeatCh := time.Tick(gfs.HeartbeatInterval)
	storeMetaCh := time.Tick(gfs.ServerMetaStoreInterval)
	go func() {
		for {
			select {
			case <- cs.shutdown:
				return
			case <- heartBeatCh:
				cs.heartBeat()
			case <- storeMetaCh:
				err := cs.storeMeta()
				if err != nil {
					log.Warnf("ChunkServer %v storeMeta error: %v", cs.address, err)
				}
			}
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// heartBeat of chunkserver
func (cs *ChunkServer) heartBeat() {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]gfs.ChunkHandle, len(pe))
	for i, v := range pe {
		le[i] = v.(gfs.ChunkHandle)
	}
	args := &gfs.HeartbeatArg{
		Address:         	cs.address,
		LeaseExtensions: 	le,
	}

	if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
		log.Warn("heartbeat rpc error ", err)
		// log.Exit(1)
	}
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		err := cs.storeMeta()
		if err != nil {
			log.Warn("ChunkServer ", cs.address, " storemeta error: ", err)
		}
		close(cs.shutdown)
		cs.l.Close()
	}
}

func (cs *ChunkServer) loadMeta() error {
	filename := path.Join(cs.serverRoot, metaFile)
	file, err := os.OpenFile(filename, os.O_RDONLY, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	var metas []persistChunkInfo
	dec := gob.NewDecoder(file)
	err = dec.Decode(&metas)
	if err != nil {
		return err
	}

	cs.Lock()
	defer cs.Unlock()

	log.Printf("Server %v: load %v chunks", cs.address, len(metas))

	for _, ckinfo := range metas {
		cs.chunk[ckinfo.Handle] = &chunkInfo{
			length: gfs.Offset(ckinfo.Length),
			version: ckinfo.Version,
		}
	}
	return nil
}

func (cs *ChunkServer) storeMeta() error {
	filename := path.Join(cs.serverRoot, metaFile)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	cs.RLock()
	defer cs.RUnlock()
	metas := make([]persistChunkInfo, 0)
	for handle, ckinfo := range cs.chunk {
		metas = append(metas, persistChunkInfo{
			Handle: handle,
			Length: ckinfo.length,
			Version: ckinfo.version,
			// TODO: persist with stale?
		})
	}

	log.Printf("Server %v: Store meta of %v chunks", cs.address, len(metas))

	enc := gob.NewEncoder(file)
	err = enc.Encode(metas)
	if err != nil {
		return err
	}
	return nil
}

func (cs *ChunkServer) RPCReportSelf(args gfs.ReportSelfArg, reply *gfs.ReportSelfReply) error {
	cs.RLock()
	defer cs.RUnlock()
	handles := make([]gfs.ChunkHandle, 0)
	versions := make([]gfs.ChunkVersion, 0)

	for handle, ckinfo := range cs.chunk {
		handles = append(handles, handle)
		versions = append(versions, ckinfo.version)
	}
	reply.Handles = handles
	reply.Versions = versions
	return nil
}

func (cs *ChunkServer) RPCGetNewChunkVersion(args gfs.GetNewChunkVerArg, reply *gfs.GetNewChunkVerReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok || ckinfo.stale {
		return fmt.Errorf("no such chunk %v while updating chunk version at %v", args.Handle, cs.address)
	} 

	ckinfo.Lock()
	defer ckinfo.Unlock()
	if ckinfo.version + gfs.ChunkVersion(1) == args.Version {
		ckinfo.version = args.Version
		reply.IsStale = false
	} else {
		reply.IsStale = true
		log.Printf("Server %v: Stale chunk %v", cs.address, args.Handle)
	}
	
	return nil
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	reply.DataID = cs.dl.New(args.Handle)

	var c []gfs.ServerAddress
	for _, addr := range args.ForwardTo {
		if addr != cs.address {
			c = append(c, addr)
		}
	}

	err := cs.RPCForwardData(
		gfs.ForwardDataArg{
			DataID: reply.DataID,
			Data: args.Data,
			ChainOrd: c,
		},
		&gfs.ForwardDataReply{},
	)
	return err
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {

	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("chunk %v already has data", cs.address)
	} 

	cs.dl.Set(args.DataID, args.Data)

	if len(args.ChainOrd) > 0 {
		next := args.ChainOrd[0]
		args.ChainOrd = args.ChainOrd[1:]
		err := util.Call(next, "ChunkServer.RPCForwardData", args, reply)
		if err != nil {
			return err
		}
	}

	return nil 
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	cs.Lock()
	defer cs.Unlock()
	if _, ok := cs.chunk[args.Handle]; ok {
		// array := make([]gfs.ChunkHandle, 0)
		// for i := range cs.chunk {
		// 	array = append(array, i)
		// }
		// log.Info("cur handles: ", array)
		return fmt.Errorf("chunk %v has exist on server %v", args.Handle, cs.address)
	}
	cs.chunk[args.Handle] = &chunkInfo{
		version: gfs.ChunkVersion(1),
		length: 0,
		// mutations: make(map[gfs.ChunkVersion]*Mutation),
	}
	return nil 
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok || ckinfo.stale {
		return fmt.Errorf("no such chunk %v while ReadChunk at %v", args.Handle, cs.address)
	} 

	ckinfo.RLock()
	defer ckinfo.RUnlock()

	reply.Data = make([]byte, args.Length)
	var err error
	reply.Length, err = cs.readChunk(args.Handle, args.Offset, reply.Data)
	// fileinfo, _ := os.Stat(filename)
	// log.Info(fileinfo.Size())
	// log.Printf("Server %v: read chunk %v at %v with length %v, buf len: %v, err %v", cs.address, args.Handle, args.Offset, reply.Length, len(reply.Data), err)

	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
	} else if err != nil {
		return err
	}
	return nil 
}

// RPCWriteChunk is called by client to call the primary to 
// apply chunk write to itself and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	handle := args.DataID.Handle
	
	cs.Lock()
	cs.pendingLeaseExtensions.Add(handle)
	ckinfo, ok := cs.chunk[handle]
	cs.Unlock()

	if !ok || ckinfo.stale {
		return fmt.Errorf("server: no such chunk %v while WriteChunk at %v", handle, cs.address)
	} 

	var mutArg = gfs.ApplyMutationArg{
		Mtype: gfs.MutationWrite,
		DataID: args.DataID,
		Offset: args.Offset, 
	}

	wait := make(chan error, 1)
	go func() {
		wait <- cs.RPCApplyMutation(mutArg, &gfs.ApplyMutationReply{})
	}()
	
	err := util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", mutArg)
	if err != nil {
		return err
	}

	err = <- wait
	if err != nil {
		return err
	}

	return nil
}

// RPCAppendChunk is called by client to call primary to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	handle := args.DataID.Handle

	cs.Lock()
	cs.pendingLeaseExtensions.Add(handle)
	ckinfo, ok := cs.chunk[handle]
	cs.Unlock()

	if !ok || ckinfo.stale {
		return fmt.Errorf("server: no such chunk %v while AppendChunk at %v", handle, cs.address)
	} 

	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("server: %v doesn't have data of ID %v" , cs.address, args.DataID)
	}
	cs.dl.Delete(args.DataID)

	ckinfo.Lock()
	defer ckinfo.Unlock()
	
	// pad or append
	var mtype gfs.MutationType
	newlen := ckinfo.length + gfs.Offset(len(data))
	reply.Offset = ckinfo.length

	if newlen > gfs.MaxChunkSize {
		mtype = gfs.MutationPad
		ckinfo.length = gfs.MaxChunkSize
		reply.ErrorCode = gfs.AppendExceedChunkSize
	} else {
		mtype = gfs.MutationAppend
		ckinfo.length = newlen
		reply.ErrorCode = gfs.Success
	}
	
	wait := make(chan error, 1)
	go func() {
		// do not call RPCApplyMutation to avoid re-lock ckinfo -> dead lock
		wait <- cs.applyMutation(handle, Mutation{mtype,	data,	reply.Offset})
	}()
	
	err := util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", gfs.ApplyMutationArg{mtype,	args.DataID, reply.Offset})
	if err != nil {
		return err
	}
	err = <- wait
	if err != nil {
		return err
	}

	return nil
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.DataID.Handle]
	cs.RUnlock()
	if !ok || ckinfo.stale {
		return fmt.Errorf("server: no such chunk %v while MutateChunk at %v", args.DataID.Handle, cs.address)
	} 

	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("server: %v doesn't have data of ID %v" , cs.address, args.DataID)
	}
	cs.dl.Delete(args.DataID)

	ckinfo.Lock()	
	defer ckinfo.Unlock()

	err := cs.applyMutation(
		args.DataID.Handle,
		Mutation{
			mtype: args.Mtype,
			data: data,
			offset: args.Offset,
		},
	)

	if err != nil {
		return err
	}

	return nil 
}

// RPCSendCCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok || ckinfo.stale {
		return fmt.Errorf("no such chunk %v while RPCSendCopy at %v", args.Handle, cs.address)
	}

	ckinfo.RLock()
	defer ckinfo.RUnlock()

	data := make([]byte, ckinfo.length)
	len, err := cs.readChunk(args.Handle, gfs.Offset(0), data)
	if err != nil || len != int(ckinfo.length) {
		return err
	}

	err = util.Call(
		args.Address, 
		"ChunkServer.RPCApplyCopy", 
		gfs.ApplyCopyArg{
			Handle: args.Handle,
			Data: data,
			Version: ckinfo.version,
		},
		&gfs.ApplyCopyReply{},
	)

	return err
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok || ckinfo.stale {
		return fmt.Errorf("no such chunk %v while RPCApplyCopy at %v", args.Handle, cs.address)
	}

	ckinfo.Lock()
	ckinfo.version = args.Version
	ckinfo.Unlock()
	
	err := cs.writeChunk(args.Handle, args.Data, gfs.Offset(0))
	if err != nil {
		return err
	}
	return nil
}

// readChunk is an auxiliary function that helps handle reading with locking
func (cs *ChunkServer) readChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	filename := path.Join(cs.serverRoot, fmt.Sprint(handle))
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, perm)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	var len int
	len, err = file.ReadAt(data, int64(offset))
	return len, err
}

// writeChunk is an auxiliary function that helps handle writing
func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset) error {
	newlen := int(offset) + len(data)
	if newlen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk %v exceed MaxChunkSize with offset %v, len(data): %v", handle, offset, len(data))
	}

	cs.Lock()
	ckinfo := cs.chunk[handle]
	if newlen > int(ckinfo.length) {
		ckinfo.length = gfs.Offset(newlen)
	}
	cs.Unlock()

	filename := path.Join(cs.serverRoot, fmt.Sprint(handle))
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	var l int
	l, err = file.WriteAt(data, int64(offset))
	// log.Printf("Server %v: write %v at chuhnk %v, %v %v err %v", cs.address, data, handle, offset, l, err)
	if err != nil {
		return err
	}
	if l != len(data) {
		return fmt.Errorf("Write error at chunkhandle %v, write %v, suppose write: %v", handle, l, len(data))
	}

	return nil
}

func (cs *ChunkServer) applyMutation(handle gfs.ChunkHandle, args Mutation) error {
	var err error
	
	switch args.mtype {
	case gfs.MutationWrite, gfs.MutationAppend:
		err = cs.writeChunk(handle, args.data, args.offset)
	case gfs.MutationPad:
		data := []byte{0}
		err = cs.writeChunk(handle, data, gfs.MaxChunkSize - 1)
	default:
		return fmt.Errorf("no such mtype")
	}

	if err != nil {
		log.Printf("chunk %v become stale at %v due to %v", handle, cs.address, err)
		cs.RLock()
		ckinfo := cs.chunk[handle]
		cs.RUnlock()
		ckinfo.stale = true
		return err
	}
	return nil
}