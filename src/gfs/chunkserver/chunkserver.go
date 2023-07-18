package chunkserver

import (
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

	dl                     *downloadBuffer                // expiring download buffer
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information
}

type Mutation struct {
	mtype   gfs.MutationType
	version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	version       gfs.ChunkVersion               // version number of the chunk in disk
	// newestVersion gfs.ChunkVersion               // allocated newest version number
	mutations     map[gfs.ChunkVersion]*Mutation // mutation buffer
}

const (
	perm = os.FileMode(0644)
)
// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:    addr,
		shutdown:   make(chan struct{}),
		master:     masterAddr,
		serverRoot: serverRoot,

		dl:         newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

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

	// Heartbeat
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			pe := cs.pendingLeaseExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}
			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}
			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error ", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
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
		return fmt.Errorf("chunk %v has exist on server %v", args.Handle, cs.address)
	}
	cs.chunk[args.Handle] = &chunkInfo{
		version: gfs.ChunkVersion(1),
		mutations: make(map[gfs.ChunkVersion]*Mutation),
	}
	return nil 
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	cs.RLock()
	ckinfo, ok := cs.chunk[args.Handle]
	cs.RUnlock()
	if !ok {
		return fmt.Errorf("no such chunk %v while ReadChunk at %v", args.Handle, cs.address)
	} 

	ckinfo.RLock()
	defer ckinfo.RUnlock()

	// read file
	filename := path.Join(cs.serverRoot, fmt.Sprint(args.Handle))
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, perm)
	if err != nil {
		return err
	}
	defer file.Close()

	reply.Data = make([]byte, args.Length)
	reply.Length, err = file.ReadAt(reply.Data, int64(args.Offset))
	// fileinfo, _ := os.Stat(filename)
	// log.Info(fileinfo.Size())
	log.Printf("Server %v: read chunk %v at %v with length %v", cs.address, args.Handle, args.Offset, reply.Length)

	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
	} else if err != nil {
		return err
	}
	return nil 
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	// TODO: version number
	var mutArg = gfs.ApplyMutationArg{
		Mtype: gfs.MutationWrite,
		// Version: ,
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

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	return nil 
}

// RPCApplyWriteChunk is called by primary to apply mutations
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("server: %v doesn't have data of ID %v" , cs.address, args.DataID)
	}
	cs.dl.Delete(args.DataID)

	cs.RLock()
	ckinfo, ok := cs.chunk[args.DataID.Handle]
	cs.RUnlock()
	if !ok {
		return fmt.Errorf("server: no such chunk %v while MutateChunk at %v", args.DataID.Handle, cs.address)
	} 

	ckinfo.Lock()
	defer ckinfo.Unlock()

	err := cs.applyMutation(
		args.DataID.Handle,
		Mutation{
			mtype: args.Mtype,
			version: ckinfo.version,
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
	return nil 
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	return nil 
}

func (cs *ChunkServer) writeChunk(handle gfs.ChunkHandle, data []byte, offset gfs.Offset) error {
	// cs.RLock()
	// ckinfo, ok := cs.chunk[handle]
	// if !ok {
	// 	return fmt.Errorf("Server %v: chunk %v doesn't exist", cs.address, handle)
	// }
	// cs.RUnlock()

	newlen := int(offset) + len(data)
	if newlen > gfs.MaxChunkSize {
		return fmt.Errorf("writeChunk %v exceed MaxChunkSize with offset %v, len(data): %v", handle, offset, len(data))
	}
	

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
	switch args.mtype {
	case gfs.MutationWrite:
		err := cs.writeChunk(handle, args.data, args.offset)
		if err != nil {
			return err
		}
	default:
		
	}
	return nil
}