package kvf

// #cgo pkg-config: kvf
/*
#include <stdlib.h>
#include <stdint.h>
#include "zhezhou-test.h"
*/
import "C"
import (
	"encoding/json"
	"reflect"
	"unsafe"

	"github.com/applepi-icpc/exfs/blockmanager"
)

const (
	KVFSizeLimit = 262144
)

type KVFBlockManager struct {
	h C.struct_KVFBlockManager
}

/* func init() {
	C.string_t_allocator_init()
} */

const persistentKey = 0

func NewKVFBlockManager(name string) *KVFBlockManager {
	ret := new(KVFBlockManager)
	cName := C.CString(name)
	C.NewKVFBlockManager(cName, &ret.h)
	C.free(unsafe.Pointer(cName))
	return ret
}

func (kvfm *KVFBlockManager) Destroy() {
	C.DestroyKVFBlockManager(&kvfm.h)
}

func (kvfm *KVFBlockManager) GetBlock(id uint64) ([]byte, error) {
	var (
		cBlk    unsafe.Pointer
		cBlkLen C.uint64_t
	)
	errcode := C.GetBlock(&kvfm.h, C.uint64_t(id), (**C.char)((unsafe.Pointer)(&cBlk)), &cBlkLen)
	if int(errcode) != 0 {
		return nil, blockmanager.ErrNoBlock
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
		return blockmanager.ErrWriteTooLarge
	}
	var (
		cBlk    = (*C.char)(unsafe.Pointer(&blk[0]))
		cBlkLen = C.uint64_t(len(blk))
	)
	errcode := C.SetBlock(&kvfm.h, C.uint64_t(id), cBlk, cBlkLen)
	if int(errcode) != 0 {
		return blockmanager.ErrNoBlock
	}
	return nil
}

func (kvfm *KVFBlockManager) RemoveBlock(id uint64) error {
	errcode := C.RemoveBlock(&kvfm.h, C.uint64_t(id))
	if int(errcode) != 0 {
		return blockmanager.ErrNoBlock
	}
	return nil
}

func (kvfm *KVFBlockManager) AllocBlock() (uint64, error) {
	var cKey C.uint64_t
	errcode := C.AllocBlock(&kvfm.h, &cKey)
	if int(errcode) != 0 {
		return 0, blockmanager.ErrNoMoreBlocks
	}
	res := uint64(cKey)
	return res, nil
}

func (kvfm *KVFBlockManager) Blocksize() uint64 {
	return uint64(KVFSizeLimit)
}

func (kvfm *KVFBlockManager) Blockstat() (total uint64, used uint64, free uint64, avail uint64) {
	var cTotal, cUsed, cFree, cAvail C.uint64_t
	C.Blockstat(&kvfm.h, &cTotal, &cUsed, &cFree, &cAvail)
	return uint64(cTotal), uint64(cUsed), uint64(cFree), uint64(cAvail)
}

type persistantData struct {
	Root      uint64
	Files     uint64
	CurrentID uint64
	Usage     uint64
}

func (kvfm *KVFBlockManager) Store(root uint64, files uint64) error {
	data, err := json.Marshal(persistantData{root, files, uint64(kvfm.h.currentID), uint64(kvfm.h.usage)})
	if err != nil {
		return err
	}
	return kvfm.SetBlock(persistentKey, data)
}

func (kvfm *KVFBlockManager) Load() (root uint64, files uint64, err error) {
	data, err := kvfm.GetBlock(persistentKey)
	if err != nil {
		return 0, 0, err
	}
	var p persistantData
	err = json.Unmarshal(data, &p)
	if err != nil {
		return 0, 0, err
	}
	kvfm.h.currentID = C.uint64_t(p.CurrentID)
	kvfm.h.usage = C.uint64_t(p.Usage)
	return p.Root, p.Files, nil
}
