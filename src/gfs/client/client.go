package client

import (
	"fmt"
	"gfs"
	"gfs/util"
	"io"
	"math/rand"
	"time"

	log "github.com/sirupsen/logrus"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
	buffer *leaseBuffer
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client {
		master: master,
		buffer: newLeaseBuffer(),
	}
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) error {
	var r gfs.CreateFileReply
	err := util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &r)
	return err
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	var r gfs.MkdirReply
	err := util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &r)
	return err
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	var r gfs.ListReply
	err := util.Call(c.master, "Master.RPCList", gfs.ListArg{Path: path}, &r)
	return r.Files, err
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	var r gfs.GetChunkHandleReply
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, &r)
	return r.Handle, err
}

// GetChunkNum returns the chunk handle of the file of the path
func (c *Client) GetChunkNum(path gfs.Path) (int, error) {
	var r gfs.GetChunkNumReply
	err := util.Call(c.master, "Master.RPCGetChunkNum", gfs.GetChunkHandleArg{Path: path}, &r)
	if r.Cnt == 0 { // empty file has 1 chunk
		r.Cnt = 1
	}
	return r.Cnt, err
}

// GetChunkReplicas returns the replicas of the corresponding chunkhandle.
func (c *Client) GetChunkReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	var r gfs.GetReplicasReply
	err := util.Call(c.master, "Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, &r)
	return r.Locations, err
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.
func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	bound := int(offset) + len(data)
	cur := offset / gfs.MaxChunkSize
	end := (bound - 1) / gfs.MaxChunkSize
	getn := 0

	var handle gfs.ChunkHandle
	for int(cur) <= end {
		// log.Info("Read cur: ", cur, " ", end)
		l := util.Max(int(offset), int(cur * gfs.MaxChunkSize))
		r := util.Min(bound, int((cur + 1) * gfs.MaxChunkSize))
		size := r - l
		handle, err = c.GetChunkHandle(path, gfs.ChunkIndex(cur))
		if err != nil {
			// log.Info("getchunk ", err)
			return 0, err
		}
		var read int
		for {
			read, err = c.ReadChunk(handle, gfs.Offset(l % gfs.MaxChunkSize), data[getn : getn + size])
			if err == nil {
				break
			} 
			if e, ok := err.(gfs.Error); ok && e.Code == gfs.ReadEOF {
				break
			}
			time.Sleep(50 * time.Millisecond)
			log.Info("wtf? retry read ", err)
		}
		// if read != size {
		// 	log.Printf("Read error at chunkhandle %v, read %v, suppose read: %v", chunkhandle, read, size)
		// }
		cur += 1
		getn += read
		if err != nil {
			break
		}
	}
	// log.Info("Read ", getn, " ", len(data), " error: ", err)
	if err != nil && err.(gfs.Error).Code == gfs.ReadEOF {
		return getn, io.EOF
	}
	return getn, err
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	bound := int(offset) + len(data)
	cur := offset / gfs.MaxChunkSize
	end := (bound - 1) / gfs.MaxChunkSize

	var getn int 
	// log.Info("Write: ", path, " ", offset, " ", len(data))
	for int(cur) <= end {
		l := util.Max(int(offset), int(cur * gfs.MaxChunkSize))
		r := util.Min(bound, int((cur + 1) * gfs.MaxChunkSize))
		chunkhandle, err := c.GetChunkHandle(path, gfs.ChunkIndex(cur))
		size := r - l
		if err != nil {
			return err
		}

		for {
			err = c.WriteChunk(chunkhandle, gfs.Offset(l % gfs.MaxChunkSize), data[getn : getn + size])
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
			log.Info("wtf? retry write ERROR INFO ", err)
		}
		if err != nil {
			return err
		}

		cur += 1
		getn += size
	}
	log.Info("Write: ", getn)
	return nil 
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	cnt, err := c.GetChunkNum(path)
	if err != nil {
		return 0, err
	}
	index := gfs.ChunkIndex(cnt - 1)
	
	var handle gfs.ChunkHandle
	var chunkOffset gfs.Offset
	for {
		handle, err = c.GetChunkHandle(path, index)
		if err != nil {
			return 0, err
		}

		// to guarantee that append at least once
		for {
			chunkOffset, err = c.AppendChunk(handle, data)
			if err == nil {
				break
			} 
			if e, ok := err.(gfs.Error); ok && e.Code == gfs.AppendExceedChunkSize {
				break
			}
			log.Warning("chunk ", handle, " failed at appending, retry again: ", err)
			time.Sleep(50 * time.Millisecond)
		}

		if err == nil {
			break
		} 
		if e, ok := err.(gfs.Error); ok && e.Code != gfs.AppendExceedChunkSize {
			break
		}
		
		index++
		log.Printf("padding %v, retry", index)
	}

	if err != nil {
		return 
	}
	offset = gfs.Offset(index) * gfs.MaxChunkSize + chunkOffset
	return 
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	// TODO: cache replicas locations
	location, err := c.GetChunkReplicas(handle)
	if err != nil {
		return 0, err
	}
	if len(location) == 0 {
		return 0, fmt.Errorf("no replicas of chunkhandle %v", handle)
	}
	if len(data) + int(offset) > gfs.MaxChunkSize {
		return 0, fmt.Errorf("read chunk %v exceed maximum chunksize with offset %v, len(data) = %v", handle, offset, len(data))
	}

	index := rand.Intn(len(location))
	
	// log.Printf("Client: read server %v Chunkindex %v, offset %v", location[index], index, offset)
	r := &gfs.ReadChunkReply{Data: data}
	err = util.Call(
		location[index], 
		"ChunkServer.RPCReadChunk", 
		gfs.ReadChunkArg{
			Handle: handle,
			Offset: offset,
			Length: len(data),
		},
		&r,
	)
	if r.ErrorCode == gfs.ReadEOF {
		return r.Length, gfs.Error{Code: r.ErrorCode, Err: "ReadEOF"}
	}
	if err != nil { // unknown error  
		return 0, err
	}
	
	return r.Length, nil
}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	if len(data) + int(offset) > gfs.MaxChunkSize {
		return fmt.Errorf("write chunk %v exceed maximum chunksize with offset %v, len(data) = %v", handle, offset, len(data))
	}

	lease, err := c.buffer.queryLease(c.master, handle)
	if err != nil {
		return err
	}
	location := lease.Secondaries
	location = append(location, lease.Primary)
	index := rand.Intn(len(location))
	
	// propagate data
	var r gfs.PushDataAndForwardReply
	err = util.Call(
		location[index], 
		"ChunkServer.RPCPushDataAndForward", 
		gfs.PushDataAndForwardArg {
			Handle: handle,
			Data: data,
			ForwardTo: location,
		},
		&r,
	)
	if err != nil {
		return err
	}
	
	// write data
	err = util.Call(
		lease.Primary,
		"ChunkServer.RPCWriteChunk",
		gfs.WriteChunkArg{
			DataID: r.DataID,
			Offset: offset,
			Secondaries: lease.Secondaries,
		},
		&gfs.WriteChunkReply{},
	)
	return err
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within 1/4 max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	
	if len(data) >= gfs.MaxAppendSize {
		return 0, fmt.Errorf("Append chunk %v exceed maximum chunksize with len(data) = %v", handle, len(data))
	}
	
	lease, err := c.buffer.queryLease(c.master, handle)
	if err != nil {
		return 0, err
	}
	location := lease.Secondaries
	location = append(location, lease.Primary)
	// log.Info("Append: available server : ", location, " data: ", data[1] - '0')
	index := rand.Intn(len(location))

	// propagate data
	var r gfs.PushDataAndForwardReply
	err = util.Call(
		location[index], 
		"ChunkServer.RPCPushDataAndForward", 
		gfs.PushDataAndForwardArg {
			Handle: handle,
			Data: data,
			ForwardTo: location,
		},
		&r,
	)
	if err != nil {
		return 0, err
	}

	var reply gfs.AppendChunkReply
	err = util.Call(
		lease.Primary, 
		"ChunkServer.RPCAppendChunk", 
		gfs.AppendChunkArg{
			DataID: r.DataID,
			Secondaries: lease.Secondaries,
		},
		&reply,
	)

	if err != nil {
		return reply.Offset, gfs.Error{gfs.UnknownError, err.Error()}
	}
	if reply.ErrorCode == gfs.AppendExceedChunkSize {
		return reply.Offset, gfs.Error{reply.ErrorCode, "padding"}
	}
	return reply.Offset, nil
}
 