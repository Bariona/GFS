package client

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// use a buffer to store the lease sent by master
type leaseBuffer struct {
	sync.RWMutex
	leases 			map[gfs.ChunkHandle] gfs.Lease
}

func newLeaseBuffer() *leaseBuffer {
	return &leaseBuffer {
		leases: make(map[gfs.ChunkHandle]gfs.Lease),
	}
}

func (l *leaseBuffer) expireLease(handle gfs.ChunkHandle) {
	l.leases[handle] = gfs.Lease{
		Expire: time.Now(),
	}
}

func (l *leaseBuffer) queryLease(m gfs.ServerAddress, handle gfs.ChunkHandle) (gfs.Lease, error) {
	l.Lock()
	defer l.Unlock()
	if ls, ok := l.leases[handle]; ok {
		if ls.Expire.After(time.Now()) {
			return ls, nil
		}
	}

	log.Printf("\033[34mClient\033[0m: Lease of chunk %v expired", handle)

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