package gfs_stress

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"gfs"
	"io"
	"log"
	"math/rand"
	"reflect"
)

type AtomicAppendSuccess struct {
	AtomicAppendSuccess_GetConfigReply
	maxOffset   gfs.Offset
	checkChunk  []int
	checkpoints []AtomicAppendSuccess_CheckPoint
	appendSpeed NetSpeed
	readSpeed   NetSpeed
}

type AtomicAppendSuccess_CheckPoint struct {
	ID    string
	Count int
}

type AtomicAppendSuccess_ReportOffsetArg struct {
	ID     string
	Offset gfs.Offset
}

type AtomicAppendSuccess_ReportCheckArg struct {
	ID          string
	Found       []AtomicAppendSuccess_CheckPoint
	AppendSpeed NetSpeed
	ReadSpeed   NetSpeed
}

type AtomicAppendSuccess_Data struct {
	ID       string
	Count    int
	Data     []byte
	Checksum []byte
}

type AtomicAppendSuccess_GetConfigReply struct {
	FilePath      string
	MaxSize       int
	Count         int
	InitializerID string
}

func (t *AtomicAppendSuccess) initRemoteFile() error {
	log.Println("initRemoteFile")
	if err := c.Create(gfs.Path(t.FilePath)); err != nil {
		return err
	}
	return nil
}

func (t *AtomicAppendSuccess) append() error {
	log.Println("append")
	clearMonitor()
	defer func() { _, t.appendSpeed = reportMonitor() }()
	var buf bytes.Buffer
	h := md5.New()
	data := AtomicAppendSuccess_Data{
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

func (t *AtomicAppendSuccess) check() error {
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
			var d AtomicAppendSuccess_Data
			if err := dec.Decode(&d); err != nil {
				return err
			}
			checksum := h.Sum(d.Data)
			if !reflect.DeepEqual(checksum, d.Checksum) {
				return fmt.Errorf("checksum differs")
			}
			t.checkpoints = append(t.checkpoints, AtomicAppendSuccess_CheckPoint{d.ID, d.Count})
			p += len + 8
		}
	}
	return nil
}

func (t *AtomicAppendSuccess) run() error {
	waitMessage("AtomicAppendSuccess:GetConfig")
	var r1 AtomicAppendSuccess_GetConfigReply
	call(conf.Center, "AtomicAppendSuccess.GetConfig", struct{}{}, &r1)
	t.AtomicAppendSuccess_GetConfigReply = r1
	if r1.InitializerID == conf.ID {
		if err := t.initRemoteFile(); err != nil {
			return err
		}
	}
	sendAck()

	waitMessage("AtomicAppendSuccess:Append")
	if err := t.append(); err != nil {
		return err
	}
	arg1 := AtomicAppendSuccess_ReportOffsetArg{conf.ID, t.maxOffset}
	call(conf.Center, "AtomicAppendSuccess.ReportOffset", arg1, nil)

	waitMessage("AtomicAppendSuccess:GetCheckChunk")
	call(conf.Center, "AtomicAppendSuccess.GetCheckChunk", conf.ID, &t.checkChunk)

	waitMessage("AtomicAppendSuccess:Check")
	if err := t.check(); err != nil {
		return err
	}
	arg2 := AtomicAppendSuccess_ReportCheckArg{conf.ID, t.checkpoints, t.appendSpeed, t.readSpeed}
	call(conf.Center, "AtomicAppendSuccess.ReportCheck", arg2, nil)
	return nil
}

func runAtomicAppendSuccess() {
	log.Println("========== Test: AtomicAppendSuccess")
	t := new(AtomicAppendSuccess)
	err := t.run()
	if err != nil {
		call(conf.Center, "RPC.ReportFailure", RPCStringMessage{conf.ID, err.Error()}, nil)
		log.Fatalln("Error:", err)
	}
}
