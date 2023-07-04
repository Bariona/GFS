package gfs_stress

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"gfs"
	"gfs/chunkserver"
	"io"
	"log"
	"math/rand"
	"reflect"
	"time"
)

type FaultTolerance struct {
	FaultTolerance_GetConfigReply
	maxOffset   gfs.Offset
	checkChunk  []int
	checkpoints []FaultTolerance_CheckPoint
	appendSpeed NetSpeed
	readSpeed   NetSpeed
	stop        chan bool
}

type FaultTolerance_CheckPoint struct {
	ID    string
	Count int
}

type FaultTolerance_ReportOffsetArg struct {
	ID     string
	Offset gfs.Offset
}

type FaultTolerance_ReportCheckArg struct {
	ID          string
	Found       []FaultTolerance_CheckPoint
	AppendSpeed NetSpeed
	ReadSpeed   NetSpeed
}

type FaultTolerance_Data struct {
	ID       string
	Count    int
	Data     []byte
	Checksum []byte
}

type FaultTolerance_GetConfigReply struct {
	FilePath      string
	MaxSize       int
	Count         int
	InitializerID string
	PrDown        float64
	MaxDownTime   time.Duration
}

func (t *FaultTolerance) initRemoteFile() error {
	log.Println("initRemoteFile")
	if err := c.Create(gfs.Path(t.FilePath)); err != nil {
		return err
	}
	return nil
}

func (t *FaultTolerance) append() error {
	log.Println("append")
	clearMonitor()
	defer func() { _, t.appendSpeed = reportMonitor() }()
	var buf bytes.Buffer
	h := md5.New()
	data := FaultTolerance_Data{
		ID:   conf.ID,
		Data: make([]byte, 0, t.MaxSize),
	}
	for i := 0; i < t.Count; i++ {
		log.Println("i=", i)

		data.Count = i
		size := rand.Intn(t.MaxSize)
		data.Data = data.Data[:size]
		for i := 0; i < size; i++ {
			data.Data[i] = byte(rand.Int())
		}
		data.Checksum = h.Sum(data.Data)

		buf.Reset()
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data); err != nil {
			return err
		}
		l := buf.Len()
		data := []byte{0x20, 0x16, 0x08, 0x08,
			byte((l >> 24) & 0xFF), byte((l >> 16) & 0xFF), byte((l >> 8) & 0xFF), byte(l & 0xFF)}
		data = append(data, buf.Bytes()...)

		resumeMonitor()
		offset, err := c.Append(gfs.Path(t.FilePath), data)
		pauseMonitor()
		if err != nil {
			return err
		}
		t.maxOffset = offset
	}
	return nil
}

func (t *FaultTolerance) check() error {
	log.Println("check")
	clearMonitor()
	defer func() { t.readSpeed, _ = reportMonitor() }()
	r := make([]byte, 0, gfs.MaxChunkSize)
	h := md5.New()
	for _, idx := range t.checkChunk {
		offset := idx * gfs.MaxChunkSize
		resumeMonitor()
		n, err := c.Read(gfs.Path(t.FilePath), gfs.Offset(offset), r[:cap(r)])
		pauseMonitor()
		r = r[:n]
		if err != nil && err != io.EOF {
			return err
		}

		p := 0
		for p+7 < n {
			if !(r[p] == 0x20 && r[p+1] == 0x16 && r[p+2] == 0x08 && r[p+3] == 0x08) {
				p++
				continue
			}
			len := (int(r[p+4]) << 24) | (int(r[p+5]) << 16) | (int(r[p+6]) << 8) | int(r[p+7])
			if p+7+len >= n {
				p++
				continue
			}
			data := r[p+8 : p+8+len]
			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			var d FaultTolerance_Data
			if err := dec.Decode(&d); err != nil {
				continue
			}
			checksum := h.Sum(d.Data)
			if !reflect.DeepEqual(checksum, d.Checksum) {
				return fmt.Errorf("checksum differs")
			}
			t.checkpoints = append(t.checkpoints, FaultTolerance_CheckPoint{d.ID, d.Count})
			p += len + 8
		}
	}
	return nil
}

func (t *FaultTolerance) faultMaker() {
	for {
		select {
		case <-t.stop:
			break
		default:
		}
		if rand.Float64() < t.PrDown {
			log.Println("Shutdown Chunkserver")
			cs.Shutdown()
			time.Sleep(t.MaxDownTime)
			log.Println("Restart Chunkserver")
			cs = chunkserver.NewAndServe(gfs.ServerAddress(conf.Listen), gfs.ServerAddress(conf.Master), root)
		}
		time.Sleep(1 * time.Second)
	}
}

func (t *FaultTolerance) run() error {
	waitMessage("FaultTolerance:GetConfig")
	var r1 FaultTolerance_GetConfigReply
	call(conf.Center, "FaultTolerance.GetConfig", struct{}{}, &r1)
	t.FaultTolerance_GetConfigReply = r1
	if r1.InitializerID == conf.ID {
		if err := t.initRemoteFile(); err != nil {
			return err
		}
	}
	sendAck()

	go t.faultMaker()

	waitMessage("FaultTolerance:Append")
	if err := t.append(); err != nil {
		return err
	}
	arg1 := FaultTolerance_ReportOffsetArg{conf.ID, t.maxOffset}
	call(conf.Center, "FaultTolerance.ReportOffset", arg1, nil)

	waitMessage("FaultTolerance:GetCheckChunk")
	call(conf.Center, "FaultTolerance.GetCheckChunk", conf.ID, &t.checkChunk)

	waitMessage("FaultTolerance:Check")
	if err := t.check(); err != nil {
		return err
	}
	arg2 := FaultTolerance_ReportCheckArg{conf.ID, t.checkpoints, t.appendSpeed, t.readSpeed}
	call(conf.Center, "FaultTolerance.ReportCheck", arg2, nil)
	return nil
}

func runFaultTolerance() {
	log.Println("========== Test: FaultTolerance")
	t := new(FaultTolerance)
	err := t.run()
	if err != nil {
		call(conf.Center, "RPC.ReportFailure", RPCStringMessage{conf.ID, err.Error()}, nil)
		log.Fatalln("Error:", err)
	}
}
