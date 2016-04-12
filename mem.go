package main

import (
	"sync/atomic"
)

// A simple block manager
type MemBlockManager struct {
	storage   map[uint64][]byte
	currentID uint64
}

func NewMemBlockManager() *MemBlockManager {
	return &MemBlockManager{
		storage:   make(map[uint64][]byte),
		currentID: 0,
	}
}

func (m *MemBlockManager) GetBlock(id uint64) ([]byte, error) {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return nil, ErrNoBlock
	}
	resReplica := make([]byte, len(res))
	copy(resReplica, res)
	return resReplica, nil
}

func (m *MemBlockManager) SetBlock(id uint64, blk []byte) error {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return ErrNoBlock
	}
	blkReplica := make([]byte, len(blk))
	copy(blkReplica, blk)
	m.storage[id] = blkReplica
	return nil
}

func (m *MemBlockManager) RemoveBlock(id uint64) error {
	res, ok := m.storage[id]
	if !ok || res == nil {
		return ErrNoBlock
	}
	m.storage[id] = nil
	return nil
}

func (m *MemBlockManager) AllocBlock() (uint64, error) {
	res := atomic.AddUint64(&m.currentID, 1)
	m.storage[res] = make([]byte, 0)
	return res, nil
}

func (m *MemBlockManager) Blocksize() uint64 {
	return SizeUnlimited
}

func (m *MemBlockManager) Blockstat() (total uint64, used uint64, free uint64, avail uint64) {
	used = uint64(len(m.storage))

	// dummy.
	total = 2147483647
	free = total - used
	avail = free

	return
}
