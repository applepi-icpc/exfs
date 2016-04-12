package main

import (
	"fmt"
)

var (
	ErrNoBlock       = fmt.Errorf("no such block")
	ErrNoMoreBlocks  = fmt.Errorf("no more blocks")
	ErrWriteTooLarge = fmt.Errorf("write too large")
)

const (
	SizeUnlimited uint64 = 0
)

type BlockManager interface {
	GetBlock(id uint64) ([]byte, error)
	SetBlock(id uint64, blk []byte) error
	RemoveBlock(id uint64) error
	AllocBlock() (uint64, error)
	Blocksize() uint64

	// free: free blocks; avail: free blocks available to unprivileged user
	Blockstat() (total uint64, used uint64, free uint64, avail uint64)
}
