package client

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"
)

type leaseBuffer struct {
	// item map[string]*bufferItem
	sync.RWMutex
	leases map[gfs.ChunkHandle] gfs.Lease
}

// type bufferItem struct {
// 	replias map[gfs.ChunkHandle]gfs.ServerAddress
// }

func newLeaseBuffer() *leaseBuffer {
	return &leaseBuffer {
		leases: make(map[gfs.ChunkHandle]gfs.Lease),
	}
}

func (l *leaseBuffer) queryLease(m gfs.ServerAddress, handle gfs.ChunkHandle) (gfs.Lease, error) {
	l.Lock()
	defer l.Unlock()
	if ret, ok := l.leases[handle]; ok {
		if ret.Expire.After(time.Now()) {
			return ret, nil
		}
	}

	// log.Printf("\033[34mClient\033[0m: Lease of chunk %v expired", handle)

	var r gfs.GetLeaseReply
	err := util.Call(m, "Master.RPCGetLease", gfs.GetLeaseArg{Handle: handle}, &r)
	if err != nil {
		return gfs.Lease{}, err
	}

	var lease gfs.Lease
	lease.Expire = r.Expire
	lease.Primary = r.Primary
	lease.Secondaries = r.Secondaries
	l.leases[handle] = lease
	return lease, nil
}