package gfs_stress

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
)

type Config struct {
	ID           string
	Role         string
	Listen       string
	Master       string
	Center       string
	NetInterface string
}

var (
	m    *master.Master
	cs   *chunkserver.ChunkServer
	c    *client.Client
	conf Config
	root string
)

type RPCStringMessage struct {
	ID      string
	Message string
}

func WritePID() {
	PIDFile := "/tmp/gfs.pid"
	f, err := os.OpenFile(PIDFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalln("cannot open ", PIDFile, ":", err)
	}
	if _, err = f.WriteString(fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		log.Fatalln("cannot write pid", err)
	}
}

var buf_ReadAndChecksum = make([]byte, 0, 64<<20)

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func ReadAndChecksum(path string, start, end int, monitor bool) ([]byte, error) {
	h := md5.New()
	offset := start
	buf := buf_ReadAndChecksum
	for offset < end {
		if monitor {
			resumeMonitor()
		}
		n, err := c.Read(gfs.Path(path), gfs.Offset(offset), buf[:min(cap(buf), end-offset)])
		if monitor {
			pauseMonitor()
		}
		buf = buf[:n]
		if err != nil && err != io.EOF {
			return nil, err
		}
		h.Write(buf)
		if err == io.EOF {
			break
		}
		offset += n
	}
	return h.Sum(nil), nil
}

var speedMonitor = struct {
	ch       chan bool
	interval time.Duration
	lock     sync.Mutex
	lastRx   int64
	lastTx   int64
	monRx    []int64
	monTx    []int64
}{ch: make(chan bool), interval: 10 * time.Millisecond}

type NetSpeed struct {
	Min, Max, Sum, Duration float64
}

func NewNetSpeed() NetSpeed {
	return NetSpeed{
		Min:      math.Inf(+1),
		Max:      math.Inf(-1),
		Sum:      0,
		Duration: 0,
	}
}

func (a *NetSpeed) Merge(b NetSpeed) {
	a.Min = math.Min(a.Min, b.Min)
	a.Max = math.Max(a.Max, b.Max)
	a.Sum = a.Sum + b.Sum
	a.Duration = math.Max(a.Duration, b.Duration)
}

func (a *NetSpeed) String() string {
	return fmt.Sprintf("local max = %4.2f, local min = %4.2f, aggregate avg = %4.2f (MBytes/s)", a.Max, a.Min, a.Sum/a.Duration)
}

func readNetBytes() (rx, tx int64) {
	buf, _ := ioutil.ReadFile(fmt.Sprintf("/sys/class/net/%s/statistics/rx_bytes", conf.NetInterface))
	s := strings.TrimSpace(string(buf))
	rx, _ = strconv.ParseInt(s, 10, 64)
	buf, _ = ioutil.ReadFile(fmt.Sprintf("/sys/class/net/%s/statistics/tx_bytes", conf.NetInterface))
	s = strings.TrimSpace(string(buf))
	tx, _ = strconv.ParseInt(s, 10, 64)
	return
}

func clearMonitor() {
	speedMonitor.lock.Lock()
	speedMonitor.monRx = speedMonitor.monRx[:0]
	speedMonitor.monTx = speedMonitor.monTx[:0]
}

func resumeMonitor() {
	speedMonitor.lastRx, speedMonitor.lastTx = readNetBytes()
	go func() {
		ticker := time.NewTicker(speedMonitor.interval)
		defer ticker.Stop()
		ch := ticker.C
		for {
			select {
			case <-speedMonitor.ch:
				return
			case <-ch:
			}
			rx, tx := readNetBytes()
			speedMonitor.monRx = append(speedMonitor.monRx, rx-speedMonitor.lastRx)
			speedMonitor.monTx = append(speedMonitor.monTx, tx-speedMonitor.lastTx)
			speedMonitor.lastRx, speedMonitor.lastTx = rx, tx
		}
	}()
}

func pauseMonitor() {
	speedMonitor.ch <- true
}

func reportMonitorStat(m []int64) NetSpeed {
	var min, max, sum float64 = math.Inf(1), math.Inf(-1), 0
	for _, v := range m {
		if v != 0 {
			min = math.Min(min, float64(v))
		}
		max = math.Max(max, float64(v))
		sum += float64(v)
	}
	itv := float64(speedMonitor.interval) / float64(time.Second)
	mb := float64(1 << 20)
	s := NetSpeed{
		Min:      min / itv / mb,
		Max:      max / itv / mb,
		Sum:      sum / mb,
		Duration: float64(len(m)) * itv,
	}
	return s
}

func reportMonitor() (rx, tx NetSpeed) {
	rx = reportMonitorStat(speedMonitor.monRx)
	tx = reportMonitorStat(speedMonitor.monTx)
	speedMonitor.lock.Unlock()
	return
}

func call(srv, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

func sendAck() {
	log.Println("sendAck")
	call(conf.Center, "RPC.Acknowledge", conf.ID, nil)
}

func waitMessage(expect string) {
	var msg string
	log.Printf("waitMessage(%s)\n", expect)
	for {
		call(conf.Center, "RPC.WhatToDo", struct{}{}, &msg)
		if msg == expect {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func asMaster() {
	m = master.NewAndServe(gfs.ServerAddress(conf.Listen), root)
}

func asChunkserver() {
	cs = chunkserver.NewAndServe(gfs.ServerAddress(conf.Listen), gfs.ServerAddress(conf.Master), root)
	c = client.NewClient(gfs.ServerAddress(conf.Master))

	runConsistencyWriteSuccess()
	runAtomicAppendSuccess()
	runFaultTolerance()
}

func Run(cfg Config) {
	conf = cfg

	// create temporary directory
	var err error
	root, err = ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	// panic handler
	defer func() {
		x := recover()
		if x != nil {
			call(conf.Center, "RPC.ReportFailure", RPCStringMessage{conf.ID, fmt.Sprintf("%v", x)}, nil)
		}
	}()

	// run
	waitMessage("wait")
	sendAck()
	switch conf.Role {
	case "master":
		asMaster()
	case "chunkserver":
		asChunkserver()
	}

	// shutdown
	waitMessage("Shutdown")
	sendAck()
	os.RemoveAll(root)
}
