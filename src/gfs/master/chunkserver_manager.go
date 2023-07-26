package master

import (
	"fmt"
	"gfs"
	"gfs/util"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers 		map[gfs.ServerAddress]*chunkServerInfo
}

// chunkServerInfo marks last Heartbeat & chunks: set of chunks that the chunkserver has
type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	return csm
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) bool {
	csm.Lock()
	defer csm.Unlock()
	server, ok := csm.servers[addr] 
	if !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks: make(map[gfs.ChunkHandle]bool),
		}
		return true
	} else {
		server.lastHeartbeat = time.Now()
		return false
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		server, ok := csm.servers[addr]
		if !ok {
			log.Warning("Master: no such server", addr)
		}
		if exist, ok := server.chunks[handle]; ok && exist {
			log.Warning("Master: server ", addr, " already has chunk ", handle)
		}
		server.chunks[handle] = true
	}
}

// RemoveChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) RemoveChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		server, ok := csm.servers[addr]
		if !ok {
			log.Warning("Master: no such server", addr)
		}
		if exist, ok := server.chunks[handle]; !ok || !exist {
			log.Warning("Master: server ", addr, " doesn't have chunk ", handle)
		}
		server.chunks[handle] = false
	}
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil

	for addr, info := range csm.servers {
		if exist, ok := info.chunks[handle]; ok && exist {
			from = addr
		} else {
			to = addr
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("Master: no enough servers for Re-replication, handle %v", handle)
	return 
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	log.Info("ChooseServers")
	csm.RLock()
	defer csm.RUnlock()

	len := len(csm.servers)
	var ret, addrs [] gfs.ServerAddress
	addrs = make([]gfs.ServerAddress, 0, len)
	ret = make([]gfs.ServerAddress, 0, num)

	for k := range csm.servers {
		addrs = append(addrs, k)
	}
	list, err := util.Sample(len, num)
	if err != nil {
		return nil, err
	}

	for i := range list {
		ret = append(ret, addrs[i])
	}

	return ret, nil
}

func Sample() {
	panic("unimplemented")
}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	csm.RLock()
	defer csm.RUnlock()

	deadServers := make([]gfs.ServerAddress, 0)
	for addr, server := range csm.servers {
		expireTime := server.lastHeartbeat.Add(gfs.ServerTimeout)
		if expireTime.Before(time.Now()) {
			deadServers = append(deadServers, addr)
		}
	}

	// for _, addr := range deadServers {
	// 	delete(csm.servers, addr)
	// }
	return deadServers
}

// RemoveServers removes metadata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	server, ok := csm.servers[addr]
	if !ok {
		return nil, fmt.Errorf("Master: server %v doesn't exist", addr)
	}

	delete(csm.servers, addr)
	
	handles = make([]gfs.ChunkHandle, 0)
	for handle, ok := range server.chunks {
		if ok {
			handles = append(handles, handle)
		}
	}

	delete(csm.servers, addr)
	return handles, nil
}
