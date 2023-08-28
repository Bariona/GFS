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
	chunk 			map[gfs.ChunkHandle]*chunkInfo
	file  			map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location 	[]gfs.ServerAddress	// set of replica locations
	version  	gfs.ChunkVersion
	primary  	gfs.ServerAddress 	// primary chunkserver
	expire   	time.Time         	// lease expire time
	isFrozen	bool
	refCnt		int									// reference count
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type persistCM struct {
	NumChunkHandle gfs.ChunkHandle
	Handles 	[]gfs.ChunkHandle
	Versions 	[]gfs.ChunkVersion
	File  		map[gfs.Path][]gfs.ChunkHandle
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		reReplicas: make([]gfs.ChunkHandle, 0),
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

func (cm *chunkManager) encodeCM() persistCM {
	cm.RLock()
	defer cm.RUnlock()
	r := persistCM {
		Handles: make([]gfs.ChunkHandle, 0),
		Versions: make([]gfs.ChunkVersion, 0),
		File: make(map[gfs.Path][]gfs.ChunkHandle),
		NumChunkHandle: cm.numChunkHandle,
	}
	for k, v := range cm.file {
		r.File[k] = v.handles
	}
	for handle, info := range cm.chunk {
		r.Handles = append(r.Handles, handle)
		r.Versions = append(r.Versions, info.version)
	}
	return r
}

func (cm *chunkManager) decodeCM(meta persistCM) {
	cm.Lock()
	defer cm.Unlock()
	for k, v := range meta.File {
		cm.file[k] = &fileInfo{
			handles: v,
		}
	}
	cm.numChunkHandle = meta.NumChunkHandle
	for i := range meta.Handles {
		cm.chunk[meta.Handles[i]] = &chunkInfo{
			location: make([]gfs.ServerAddress, 0),
			version: meta.Versions[i],
			expire: time.Now(),
			// path: ,
		}
	}
}

// StaleChunkDetect is called by master to check stale chunks of a reboot chunkserver cs
// if the chunk is up-to-date, then add it into location
func (cm *chunkManager) StaleChunkDetect(cs gfs.ServerAddress) ([]gfs.ChunkHandle, []gfs.ChunkHandle, error) {
	var r gfs.ReportSelfReply
	err := util.Call(cs, "ChunkServer.RPCReportSelf", gfs.ReportSelfArg{}, &r)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("Report self address %v, chunk num: %v", cs, len(r.Handles))
	cm.RLock()
	defer cm.RUnlock()

	staleHandles := make([]gfs.ChunkHandle, 0)
	latestHandles := make([]gfs.ChunkHandle, 0)

	for i, handle := range r.Handles {
		ck, ok := cm.chunk[handle]
		if !ok {
			log.Warnf("Master: no such handle %v during stale detection", handle)
		}
		if ck.version == r.Versions[i] {
			// log.Printf("Master: Detect latest Chunk %v Version %v at server %v", handle, r.Versions[i], cs)
			// ck.location = append(ck.location, cs)
			cm.RegisterReplica(handle, cs, true)
			
			if ck.expire.After(time.Now()) {
				// ck.expire = time.Now()
				log.Fatal("Master: expire time wrong for chunk: ", handle)
			}
			latestHandles = append(latestHandles, handle)
		} else {
			staleHandles = append(staleHandles, handle)
		}
	}

	return staleHandles, latestHandles, nil
}


// RemoveReplica removes a replica for a chunk
func (cm *chunkManager) RemoveReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return fmt.Errorf("Master: RemoveReplica no such handle %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()
	newlocation := make([]gfs.ServerAddress, 0, len(ck.location) - 1)
	for _, a := range ck.location {
		if a != addr {
			newlocation = append(newlocation, a)
		}
	}
	ck.location = newlocation
	ck.expire = time.Now()
	return nil
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress, useLock bool) error {
	var ck *chunkInfo
	var ok bool

	if useLock {
		cm.RLock()
		ck, ok = cm.chunk[handle]
		cm.RUnlock()
		
		ck.Lock()
		defer ck.Unlock()
	} else {
		ck, ok = cm.chunk[handle]
	}

	if !ok {
		return fmt.Errorf("Master: ResigerReplica no such Chunk %v", handle)
	}
	ck.location = append(ck.location, addr)

	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	cm.RLock()
	defer cm.RUnlock()
	replicas, ok := cm.chunk[handle]
	if !ok {
		return nil, fmt.Errorf("Master: GetReplica no such ChunkHandle %v", handle)
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

func (cm *chunkManager) GetChunkInfo(handle gfs.ChunkHandle) (*chunkInfo, error) {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, fmt.Errorf("Master: RemoveReplica no such handle %v", handle)
	}
	return ck, nil
}

// GetLeaseHolder returns the chunkserver that holds the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grant one to a replica it chooses.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) ([]gfs.ServerAddress, *gfs.Lease, error) {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("no such ChunkHandle %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	staleServers := make([]gfs.ServerAddress, 0)

	if len(ck.location) < gfs.MinimumNumReplicas {
		cm.Lock()
		cm.reReplicas = append(cm.reReplicas, handle)
		cm.Unlock()
		
		if len(ck.location) == 0 {
			return nil, nil, fmt.Errorf("Master: During lease granting, chunk %v doesn't exist in any replica", handle)
		}
	}

	if time.Now().After(ck.expire) {
		e := util.Call(ck.primary, "ChunkServer.RPCInvalidChunk", gfs.InvalidChunkArg{handle, false}, nil)
		if e != nil {
			log.Warnf("Master: unfrozen chunk %v, error: %v, %v", handle, e, ck.primary)
		}

		// grant a new lease
		ck.version += 1
	
		var wg sync.WaitGroup
		wg.Add(len(ck.location))

		var lock sync.Mutex
		latest := make([]gfs.ServerAddress, 0)
		for _, addr := range ck.location {
			go func(addr gfs.ServerAddress) {
				var r gfs.GetNewChunkVerReply
				err := util.Call(
					addr, 
					"ChunkServer.RPCGetNewChunkVersion", 
					gfs.GetNewChunkVerArg{Handle: handle, Version: ck.version}, 
					&r,
				)
				
				if err == nil && !r.IsStale {
					lock.Lock()
					latest = append(latest, addr)
					lock.Unlock()
				} else {
					log.Warnf("Stale Chunk %v at server %v error %v", handle, addr, err)
					staleServers = append(staleServers, addr)
				}
				wg.Done()
			}(addr)
		}
		wg.Wait()

		ck.location = latest
		if len(latest) < gfs.MinimumNumReplicas {
			cm.Lock()
			cm.reReplicas = append(cm.reReplicas, handle)
			cm.Unlock()
			if len(latest) == 0 {
				log.Fatal("no latest version of chunk ", handle)
			}
		}

		index := rand.Intn(len(ck.location))
		ck.primary = ck.location[index]
		ck.expire = time.Now().Add(gfs.LeaseExpire)
	}
	

	// check reference count
	if ck.refCnt > 0 {
		cm.Lock()
		cm.numChunkHandle += 1
		newHandle := cm.numChunkHandle
		cm.chunk[newHandle] = &chunkInfo{
			location: ck.location,
			version: ck.version,
			expire: time.Now(),
		}
		cm.Unlock()
		log.Info("ADDED CHUNK")

		log.Printf("Master: Split Chunk (%v) -> copy (%v) at %v", handle, newHandle, ck.location)
		err := util.CallAll(ck.location, "ChunkServer.RPCChunkCopy", gfs.ChunkCopyArg{handle, newHandle})
		if err != nil {
			log.Warn(err)
		}
		ck.refCnt--
	}

	secondaries := make([]gfs.ServerAddress, 0, len(ck.location) - 1)
	for _, addr := range ck.location {
		if addr != ck.primary {
			secondaries = append(secondaries, addr)
		}
	}

	l := &gfs.Lease {
		Primary: ck.primary,
		Expire: ck.expire,
		Secondaries: secondaries,
	}
	return staleServers, l, nil
}

// InvalidLease invalids the lease of chunk 
func (cm *chunkManager) InvalidLease(handle gfs.ChunkHandle, invalidStamp bool) error {
	cm.RLock()
	ck, ok := cm.chunk[handle]
	cm.RUnlock()
	
	if !ok {
		return fmt.Errorf("Master: no such chunk %v", handle)
	}

	ck.Lock()
	defer ck.Unlock()

	if (invalidStamp && ck.expire.After(time.Now())) || (!invalidStamp && ck.isFrozen) {
		ck.isFrozen = invalidStamp
		err := util.Call(ck.primary, "ChunkServer.RPCInvalidChunk", gfs.InvalidChunkArg{handle, invalidStamp}, nil)
		if invalidStamp {
			log.Printf("\033[34mMaster\033[0m: Snapshot invalid chunk %v's lease, primary: %v", handle, ck.primary)
		}
		if err != nil {
			log.Fatal("InvalidLease error: ", err)
			return err
		}
	}

	return nil
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
		// path: path,
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

	// remove qualified elements
	list := make([]gfs.ChunkHandle, 0)
	for _, handle := range cm.reReplicas {
		if len(cm.chunk[handle].location) < gfs.MinimumNumReplicas {
			list = append(list, handle)
		}
	}
	cm.reReplicas = list

	// optional: sort by priority
	vec := make([]int, 0)
	for _, handle := range cm.reReplicas {
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

	return unique
}