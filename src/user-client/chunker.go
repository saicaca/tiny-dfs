package main

import (
	"math"
	"os"
)

type FileChunker struct {
	file      *os.File
	curr      uint64
	total     uint64
	chunkSize uint64
	fileSize  uint64
}

func NewFileChunker(path string) (*FileChunker, error) {
	chunker := &FileChunker{}
	var err error
	chunker.file, err = os.Open(path)
	if err != nil {
		return nil, err
	}

	info, _ := chunker.file.Stat()
	chunker.fileSize = uint64(info.Size())
	chunker.chunkSize = 64 * 1024 * 1024 // TODO should be configurable
	chunker.total = uint64(math.Ceil(float64(chunker.fileSize) / float64(chunker.chunkSize)))

	chunker.curr = 0
	return chunker, nil
}

func (c *FileChunker) GetNext() []byte {
	if !c.HasNext() {
		return nil
	}

	var partSize uint64 = c.chunkSize
	remain := c.fileSize - c.curr*c.chunkSize
	if remain < partSize {
		partSize = remain
	}

	partBuffer := make([]byte, partSize)
	_, _ = c.file.Read(partBuffer)
	c.curr++
	return partBuffer
}

func (c *FileChunker) HasNext() bool {
	return c.curr < c.total
}
