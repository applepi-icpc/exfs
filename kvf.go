package exfs

// #cgo pkg-config: zzkvf
/*
#include <stdlib.h>
#include <stdint.h>
#include "zhezhou-test.h"
*/
import "C"
import (
	"reflect"
	"unsafe"
)

const (
	KVFSizeLimit = 524288
)

type KVFBlockManager struct {
	h C.struct_KVFBlockManager
}

func NewKVFBlockManager(name string) *KVFBlockManager {
	ret := new(KVFBlockManager)
	cName := C.CString(name)
	C.NewKVFBlockManager(cName, &ret.h)
	C.free(unsafe.Pointer(cName))
	return ret
}

func (kvfm *KVFBlockManager) GetBlock(id uint64) ([]byte, error) {
	var (
		cBlk    unsafe.Pointer
		cBlkLen C.uint64_t
	)
	errcode := C.GetBlock(&kvfm.h, C.uint64_t(id), (**C.char)(&cBlk), &cBlkLen)
	if int(errcode) != 0 {
		return nil, ErrNoBlock
	}
	blkLen := int(cBlkLen)
	blk := make([]byte, blkLen)
	copy(blk, *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(cBlk),
		Len:  blkLen,
		Cap:  blkLen,
	})))
	C.FreeBlkData((*C.char)(cBlk))
	return blk, nil
}

func (kvfm *KVFBlockManager) SetBlock(id uint64, blk []byte) error {
	if len(blk) > KVFSizeLimit {
		return ErrWriteTooLarge
	}
	var (
		cBlk    = (*C.char)(unsafe.Pointer(&blk[0]))
		cBlkLen = C.uint64_t(len(blk))
	)
	errcode := C.SetBlock(&kvfm.h, C.uint64_t(id), cBlk, cBlkLen)
	if int(errcode) != 0 {
		return ErrNoBlock
	}
	return nil
}

func (kvfm *KVFBlockManager) RemoveBlock(id uint64) error {
	errcode := C.RemoveBlock(&kvfm.h, C.uint64_t(id))
	if int(errcode) != 0 {
		return ErrNoBlock
	}
	return nil
}

func (kvfm *KVFBlockManager) AllocBlock() (uint64, error) {
	var cKey C.uint64_t
	errcode := C.AllocBlock(&kvfm.h, &cKey)
	if int(errcode) != 0 {
		return 0, ErrNoMoreBlocks
	}
	return uint64(cKey), nil
}

func (kvfm *KVFBlockManager) Blocksize() uint64 {
	return uint64(KVFSizeLimit)
}

func (kvfm *KVFBlockManager) Blockstat() (total uint64, used uint64, free uint64, avail uint64) {
	var cTotal, cUsed, cFree, cAvail C.uint64_t
	C.Blockstat(&kvfm.h, &cTotal, &cUsed, &cFree, &cAvail)
	return uint64(cTotal), uint64(cUsed), uint64(cFree), uint64(cAvail)
}
