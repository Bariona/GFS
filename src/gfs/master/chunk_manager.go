package master

import (
	"fmt"
	"gfs"
	"math/rand"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location []gfs.ServerAddress// set of replica locations
	version  gfs.ChunkVersion
	primary  gfs.ServerAddress 	// primary chunkserver
	expire   time.Time         	// lease expire time
	path     gfs.Path
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}


func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {

	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	defer cm.RUnlock()
	replicas, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("not such ChunkHandle %v", handle)
	}
	return replicas.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()
	chunks, ok := cm.file[path]
	if !ok {
		return 0, fmt.Errorf("no such file %v", path)
	}
	if int(index) >= len(chunks.handles) {
		return 0, fmt.Errorf("chunk %v index out of bound %v", path, index)
	}
	chunkhandle := chunks.handles[index]
	return chunkhandle, nil
}

// GetLeaseHolder returns the chunkserver that holds the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grant one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	cm.RLock()
	chunkhandle, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no such ChunkHandle %v", handle)
	}

	chunkhandle.Lock()
	defer chunkhandle.Unlock()

	var prim gfs.ServerAddress
	var expire_time time.Time
	secondaries := make([]gfs.ServerAddress, 0, len(chunkhandle.location) - 1)
	if chunkhandle.expire.IsZero() || time.Now().After(chunkhandle.expire) {
		// grant a primary
		index := rand.Intn(len(chunkhandle.location))
		prim = chunkhandle.location[index]
		expire_time = time.Now().Add(gfs.LeaseExpire)
	} else {
		prim = chunkhandle.primary
		expire_time = chunkhandle.expire
	}
	
	for _, addr := range chunkhandle.location {
		if addr != prim {
			secondaries = append(secondaries, addr)
		}
	}

	l := &gfs.Lease {
		Primary: prim,
		Expire: expire_time,
		Secondaries: secondaries,
	}
	return l, nil
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()
	
	if !ok {
		return fmt.Errorf("Master: no such chunk ", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if ck.primary == primary && ck.expire.After(time.Now()) {
		ck.expire = time.Now().Add(gfs.LeaseExpire)
	} else {
		return fmt.Errorf("%v doesn't hold the lease anymore", handle)
	}
	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
	log.Info("Create Chunk ", addrs)
	cm.Lock()
	defer cm.Unlock()

	cm.numChunkHandle += 1
	chunkhandle := cm.numChunkHandle
	
	cm.chunk[chunkhandle] = &chunkInfo{
		path: path,
		location: addrs,
		version: gfs.ChunkVersion(1),
	}

	if _, ok := cm.file[path]; !ok {
		cm.file[path] = &fileInfo{
			handles: make([]gfs.ChunkHandle, 0),
		}
	}
	
	f := cm.file[path]
	f.handles = append(f.handles, chunkhandle)
	return chunkhandle, nil
}
