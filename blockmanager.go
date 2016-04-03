package main

import (
	"fmt"
)

var (
	ErrNoBlock      = fmt.Errorf("no such block")
	ErrNoMoreBlocks = fmt.Errorf("no more blocks")
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
}
