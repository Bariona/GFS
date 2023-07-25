package master

import (
	"fmt"
	"gfs"
	"gfs/util"
	"math/rand"
	"sort"

	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex


	reReplicas 	[]gfs.ChunkHandle
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
		reReplicas: make([]gfs.ChunkHandle, 0),
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// StaleChunkDetect is called by master to check stale chunks of a reboot chunkserver cs
func (cm *chunkManager) StaleChunkDetect(cs gfs.ServerAddress) ([]gfs.ChunkHandle, []gfs.ChunkHandle, error) {
	var r gfs.ReportSelfReply
	err := util.Call(cs, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("Report self address %v, chunk num: %v", cs, len(r.Handles))
	cm.Lock()
	defer cm.Unlock()

	staleHandles := make([]gfs.ChunkHandle, 0)
	latestHandles := make([]gfs.ChunkHandle, 0)

	for i, handle := range r.Handles {
		ckinfo := cm.chunk[handle]
		if ckinfo.version == r.Versions[i] {
			log.Printf("Master: Detect latest Chunk %v Version %v at server %v", handle, r.Versions[i], cs)
			ckinfo.location = append(ckinfo.location, cs)
			ckinfo.expire = time.Now()
			latestHandles = append(latestHandles, handle)
		} else {
			staleHandles = append(staleHandles, handle)
		}
	}

	return staleHandles, latestHandles, nil
}


// RemoveReplica adds a replica for a chunk
func (cm *chunkManager) RemoveReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	ckinfo, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return fmt.Errorf("Master: RemoveReplica no such handle %v", handle)
	}

	ckinfo.Lock()
	defer ckinfo.Unlock()
	newlocation := make([]gfs.ServerAddress, 0, len(ckinfo.location) - 1)
	for _, a := range ckinfo.location {
		if a != addr {
			newlocation = append(newlocation, a)
		}
	}
	ckinfo.location = newlocation
	ckinfo.expire = time.Now() // TODO: expire it now ?
	return nil
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	ckinfo, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return fmt.Errorf("Master: ResigerReplica no such Chunk %v", handle)
	}

	ckinfo.Lock()
	defer ckinfo.Unlock()
	ckinfo.location = append(ckinfo.location, addr)

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
	handle := chunks.handles[index]
	return handle, nil
}

// GetLeaseHolder returns the chunkserver that holds the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grant one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	cm.RLock()
	ckinfo, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no such ChunkHandle %v", handle)
	}

	ckinfo.Lock()
	defer ckinfo.Unlock()

	var prim gfs.ServerAddress
	var expireTime time.Time

	if len(ckinfo.location) < gfs.MinimumNumReplicas {
		cm.Lock()
		cm.reReplicas = append(cm.reReplicas, handle)
		cm.Unlock()
		
		if len(ckinfo.location) == 0 {
			return nil, fmt.Errorf("Master: During lease granting, chunk %v doesn't exist in any replica", handle)
		}
	}
	
	secondaries := make([]gfs.ServerAddress, 0, len(ckinfo.location) - 1)

	if time.Now().After(ckinfo.expire) {
		// grant a new lease
		index := rand.Intn(len(ckinfo.location))
		prim = ckinfo.location[index]
		expireTime = time.Now().Add(gfs.LeaseExpire)
		ckinfo.version += 1

		for _, addr := range ckinfo.location {
			var r gfs.GetNewChunkVerReply
			err := util.Call(
				addr, 
				"ChunkServer.RPCGetNewChunkVersion", 
				gfs.GetNewChunkVerArg{Handle: handle, Version: ckinfo.version}, 
				&r,
			)
			if err != nil {
				log.Warn("Server ", addr, " error occur at updating chunkversion", err)
			}
			if r.IsStale {
				// TODO: stale 
			}
		}

	} else {
		prim = ckinfo.primary
		expireTime = ckinfo.expire
	}
	
	for _, addr := range ckinfo.location {
		if addr != prim {
			secondaries = append(secondaries, addr)
		}
	}

	l := &gfs.Lease {
		Primary: prim,
		Expire: expireTime,
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
		return fmt.Errorf("Master: no such chunk %v", handle)
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
	handle := cm.numChunkHandle
	
	cm.chunk[handle] = &chunkInfo{
		path: path,
		location: addrs,
		version: gfs.ChunkVersion(1),
		expire: time.Now(),
	}

	if _, ok := cm.file[path]; !ok {
		cm.file[path] = &fileInfo{
			handles: make([]gfs.ChunkHandle, 0),
		}
	}
	
	f := cm.file[path]
	f.handles = append(f.handles, handle)
	return handle, nil
}


func (cm *chunkManager) GetRereplicas() []gfs.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()

	// TODO: sort by priority
	vec := make([]int, 0)
	for _, handle := range cm.reReplicas {
		if len(cm.chunk[handle].location) >= gfs.MinimumNumReplicas {
			log.Fatal("handle ", handle, " don't need to re-replica", handle)
			log.Exit(1)
		}
		vec = append(vec, int(handle))
	}
	sort.Ints(vec)
	
	unique := make([]gfs.ChunkHandle, 0)
	for i := 0; i < len(vec); i++ {
		if i > 0 && vec[i] == vec[i - 1] {
			continue
		}
		unique = append(unique, gfs.ChunkHandle(vec[i]))
	}
	cm.reReplicas = make([]gfs.ChunkHandle, 0)
	return unique
}