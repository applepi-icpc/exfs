package main

import (
	"fmt"

	"sync/atomic"
)

const (
	MLBMSizeLimit uint64 = 1048576
)

func init() {
	fmt.Printf("EXFS: Running on limited mem block manager: %d bytes\n", MLBMSizeLimit)
}

// A simple block manager with size limit
type MemLimitedBlockManager struct {
	storage   map[uint64][]byte
	currentID uint64
}

func NewMemLimitedBlockManager() *MemLimitedBlockManager {
	return &MemLimitedBlockManager{
		storage:   make(map[uint64][]byte),
		currentID: 0,
	}
}

func (m *MemLimitedBlockManager) GetBlock(id uint64) ([]byte, error) {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return nil, ErrNoBlock
	}
	resReplica := make([]byte, len(res))
	copy(resReplica, res)
	return resReplica, nil
}

func (m *MemLimitedBlockManager) SetBlock(id uint64, blk []byte) error {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return ErrNoBlock
	}
	if uint64(len(blk)) > MLBMSizeLimit {
		return ErrWriteTooLarge
	}
	blkReplica := make([]byte, len(blk))
	copy(blkReplica, blk)
	m.storage[id] = blkReplica
	return nil
}

func (m *MemLimitedBlockManager) RemoveBlock(id uint64) error {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return ErrNoBlock
	}
	m.storage[id] = nil
	return nil
}

func (m *MemLimitedBlockManager) AllocBlock() (uint64, error) {
	res := atomic.AddUint64(&m.currentID, 1)
	m.storage[res] = make([]byte, 0)
	return res, nil
}

func (m *MemLimitedBlockManager) Blocksize() uint64 {
	return MLBMSizeLimit
}

func (m *MemLimitedBlockManager) Blockstat() (total uint64, used uint64, free uint64, avail uint64) {
	used = uint64(len(m.storage))

	// dummy.
	total = 2147483647
	free = total - used
	avail = free

	return
}
