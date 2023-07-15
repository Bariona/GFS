package client

import "gfs"

type clientBuffer struct {
	item map[string]*bufferItem
}

type bufferItem struct {
	replias map[gfs.ChunkHandle]gfs.ServerAddress
}

func newClientBuffer() *clientBuffer {
	return &clientBuffer{
		item: make(map[string]*bufferItem),
	}
}