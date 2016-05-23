package main

import (
	"sync"
	"time"

	"github.com/applepi-icpc/exfs/blockmanager"
)

type blockBufferEntry struct {
	valid    bool
	key      uint64
	content  []byte
	dirty    bool
	lastUsed int64
}

type BlockBuffer struct {
	buffer []blockBufferEntry
	lock   sync.Mutex
	bm     blockmanager.BlockManager
}

const BufferSize = 4

func NewBlockBuffer(bm blockmanager.BlockManager) *BlockBuffer {
	return &BlockBuffer{
		buffer: make([]blockBufferEntry, BufferSize),
		bm:     bm,
	}
}

func (bb *BlockBuffer) getLeastUsed() int {
	res := -1
	for k, v := range bb.buffer {
		if !v.valid {
			return k
		}
		if res == -1 {
			res = k
		} else if bb.buffer[k].lastUsed < bb.buffer[res].lastUsed {
			res = k
		}
	}
	if bb.buffer[res].dirty {
		// log.Infof("CACHE: WRITE DIRTY (%d)", bb.buffer[res].key)
		// Ignore any error for it's not possible to handle it
		bb.bm.SetBlock(bb.buffer[res].key, bb.buffer[res].content)
	}
	// log.Infof("CACHE: EVICT (%d)", bb.buffer[res].key)
	bb.buffer[res].valid = false
	return res
}

func (bb *BlockBuffer) loadForRead(id uint64) ([]byte, error) {
	content, err := bb.bm.GetBlock(id)
	if err != nil {
		return nil, err
	}
	lru := bb.getLeastUsed()
	bb.buffer[lru] = blockBufferEntry{
		valid:    true,
		key:      id,
		content:  content,
		dirty:    false,
		lastUsed: time.Now().UnixNano(),
	}
	return content, nil
}

func (bb *BlockBuffer) GetBlock(id uint64) ([]byte, error) {
	bb.lock.Lock()
	defer bb.lock.Unlock()

	for k, v := range bb.buffer {
		if v.valid && v.key == id {
			// log.Infof("CACHE: READ HIT (%d)", id)
			bb.buffer[k].lastUsed = time.Now().UnixNano()
			return v.content, nil
		}
	}

	// log.Infof("CACHE: READ MISS (%d)", id)
	return bb.loadForRead(id)
}

// No need for real data.
func (bb *BlockBuffer) loadForWrite(id uint64, blk []byte) {
	lru := bb.getLeastUsed()
	bb.buffer[lru] = blockBufferEntry{
		valid:    true,
		key:      id,
		content:  blk,
		dirty:    true,
		lastUsed: time.Now().UnixNano(),
	}
}

func (bb *BlockBuffer) SetBlock(id uint64, blk []byte) error {
	if len(blk) > int(bb.bm.Blocksize()) {
		return blockmanager.ErrWriteTooLarge
	}

	bb.lock.Lock()
	defer bb.lock.Unlock()

	for k, v := range bb.buffer {
		if v.valid && v.key == id {
			// log.Infof("CACHE: WRITE HIT (%d)", id)
			bb.buffer[k].lastUsed = time.Now().UnixNano()
			bb.buffer[k].content = blk
			bb.buffer[k].dirty = true
			return nil
		}
	}

	// log.Infof("CACHE: WRITE MISS (%d)", id)
	bb.loadForWrite(id, blk)
	return nil
}

func (bb *BlockBuffer) RemoveBlock(id uint64) error {
	bb.lock.Lock()
	defer bb.lock.Unlock()

	err := bb.bm.RemoveBlock(id)
	if err != nil {
		return err
	}
	for k, v := range bb.buffer {
		if v.valid && v.key == id {
			bb.buffer[k].valid = false
		}
	}
	return nil
}

func (bb *BlockBuffer) AllocBlock() (uint64, error) {
	return bb.bm.AllocBlock()
}

func (bb *BlockBuffer) Blocksize() uint64 {
	return bb.bm.Blocksize()
}

func (bb *BlockBuffer) Blockstat() (total uint64, used uint64, free uint64, avail uint64) {
	return bb.bm.Blockstat()
}

func (bb *BlockBuffer) Flush() error {
	// log.Infof("CACHE: FLUSH")
	for k, v := range bb.buffer {
		if v.valid && v.dirty {
			// log.Infof("CACHE: WRITE DIRTY (%d)", v.key)
			err := bb.bm.SetBlock(v.key, v.content)
			if err != nil {
				return err
			}
			bb.buffer[k].dirty = false
		}
	}
	return nil
}
