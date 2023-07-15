package master

import (
	"gfs"
	"gfs/util"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
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
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
	csm.Lock()
	defer csm.Unlock()
	server, ok := csm.servers[addr] 
	if !ok {
		log.Info("new chunkserver ", addr)
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks: make(map[gfs.ChunkHandle]bool),
		}
	} else {
		server.lastHeartbeat = time.Now()
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	return nil
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	return "", "", nil
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	log.Info("ChooseServers")
	csm.RLock()
	defer csm.RUnlock()
	for _, server := range csm.servers {
		log.Info("server : ", server)
	}

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
	return nil
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	return nil, nil
}
