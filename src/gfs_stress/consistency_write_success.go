package gfs_stress

import (
	"gfs"
	"log"
	"math/rand"
)

type ConsistencyWriteSuccess struct {
	filePath     string
	fileSize     int
	maxWriteSize int
	count        int
	checkPoint   []ConsistencyWriteSuccess_CheckPoint
	md5s         [][]byte
	initSpeed    NetSpeed
	writeSpeed   NetSpeed
	readSpeed    NetSpeed
}

type ConsistencyWriteSuccess_CheckPoint struct {
	Start, End int
}

type ConsistencyWriteSuccess_GetConfigReply struct {
	FilePath      string
	FileSize      int
	MaxWriteSize  int
	Count         int
	CheckPoint    []ConsistencyWriteSuccess_CheckPoint
	InitializerID string
}

type ConsistencyWriteSuccess_ReportCheckPointArg struct {
	ID         string
	MD5s       [][]byte
	InitSpeed  NetSpeed
	WriteSpeed NetSpeed
	ReadSpeed  NetSpeed
}

func (t *ConsistencyWriteSuccess) write() error {
	log.Println("write")
	clearMonitor()
	defer func() { _, t.writeSpeed = reportMonitor() }()
	buf := make([]byte, 0, t.maxWriteSize)
	for i := 0; i < t.count; i++ {
		log.Println("i=", i)
		size := rand.Intn(cap(buf))
		offset := rand.Intn(t.fileSize - size)
		buf = buf[:size]
		for i := 0; i < size; i++ {
			buf[i] = byte(rand.Int())
		}
		resumeMonitor()
		err := c.Write(gfs.Path(t.filePath), gfs.Offset(offset), buf)
		pauseMonitor()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *ConsistencyWriteSuccess) check() error {
	log.Println("check")
	clearMonitor()
	defer func() { t.readSpeed, _ = reportMonitor() }()
	for _, v := range t.checkPoint {
		hash, err := ReadAndChecksum(t.filePath, v.Start, v.End, true)
		if err != nil {
			return err
		}
		t.md5s = append(t.md5s, hash)
	}
	return nil
}

func (t *ConsistencyWriteSuccess) initRemoteFile() error {
	log.Println("initRemoteFile")
	if err := c.Create(gfs.Path(t.filePath)); err != nil {
		return err
	}

	clearMonitor()
	defer func() { _, t.initSpeed = reportMonitor() }()

	buf := make([]byte, 64<<20)
	for offset := 0; offset < t.fileSize; offset += len(buf) {
		d := t.fileSize - offset
		if d < cap(buf) {
			buf = buf[:d]
		}
		resumeMonitor()
		err := c.Write(gfs.Path(t.filePath), gfs.Offset(offset), buf)
		pauseMonitor()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *ConsistencyWriteSuccess) run() error {
	waitMessage("ConsistencyWriteSuccess:GetConfig")
	var r1 ConsistencyWriteSuccess_GetConfigReply
	call(conf.Center, "ConsistencyWriteSuccess.GetConfig", struct{}{}, &r1)
	t.checkPoint = r1.CheckPoint
	t.filePath = r1.FilePath
	t.fileSize = r1.FileSize
	t.count = r1.Count
	t.maxWriteSize = r1.MaxWriteSize
	if r1.InitializerID == conf.ID {
		if err := t.initRemoteFile(); err != nil {
			return err
		}
	}
	sendAck()

	waitMessage("ConsistencyWriteSuccess:Write")
	if err := t.write(); err != nil {
		return err
	}
	sendAck()

	waitMessage("ConsistencyWriteSuccess:Check")
	if err := t.check(); err != nil {
		return err
	}
	args := ConsistencyWriteSuccess_ReportCheckPointArg{conf.ID, t.md5s, t.initSpeed, t.writeSpeed, t.readSpeed}
	call(conf.Center, "ConsistencyWriteSuccess.ReportCheckPoint", args, nil)
	return nil
}

func runConsistencyWriteSuccess() {
	log.Println("========== Test: ConsistencyWriteSuccess")
	t := new(ConsistencyWriteSuccess)
	err := t.run()
	if err != nil {
		call(conf.Center, "RPC.ReportFailure", RPCStringMessage{conf.ID, err.Error()}, nil)
		log.Fatalln("Error:", err)
	}
}
