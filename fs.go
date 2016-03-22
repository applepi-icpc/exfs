package main

import (
	"time"

	"github.com/hanwen/go-fuse/fuse/pathfs"
	"github.com/qiniu/log"
)

type Exfs struct {
	pathfs.FileSystem

	blockManager BlockManager
	root         uint64
	debug        bool
}

func NewExfs(blockManager BlockManager) *Exfs {
	fs := &Exfs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		blockManager: blockManager,
		debug:        false,
	}

	// initialize: make a root

	return fs
}

func (fs *Exfs) createINode(mode uint32, uid uint32, gid uint32) (blkID uint64, ino INode, err error) {
	blkID, err = fs.blockManager.AllocBlock()
	if err != nil {
		blkID = 0
		return
	}

	ino = INode{
		Size:   0,
		Atime:  uint64(time.Now().UnixNano()),
		Mtime:  uint64(time.Now().UnixNano()),
		Ctime:  uint64(time.Now().UnixNano()),
		Mode:   mode,
		Uid:    uid,
		Gid:    gid,
		Blocks: make([]uint64, 0),
	}
	inoB := ino.Marshal()
	err = fs.blockManager.SetBlock(blkID, inoB)
	if err != nil {
		// fs.blockManager.rollback()
		blkID = 0
		ino = INode{}
	} else {
		// fs.blockManager.commit()
	}

	return
}

func (fs *Exfs) String() string {
	return "exampleFileSystem"
}

func (fs *Exfs) SetDebug(debug bool) {
	fs.debug = debug
}

func (fs *Exfs) logReadBlkError(blkID uint64, inodeBlkID uint64, err error) {
	if fs.debug {
		log.Errorf("Failed to read block %d for file %d: %s", blkID, inodeBlkID, err.Error())
	}
}

func (fs *Exfs) logWriteBlkError(blkID uint64, inodeBlkID uint64, err error) {
	if fs.debug {
		log.Errorf("Failed to write block %d for file %d: %s", blkID, inodeBlkID, err.Error())
	}
}

func (fs *Exfs) logSetSizeError(newSize uint64, inodeBlkID uint64, err error) {
	if fs.debug {
		log.Errorf("Failed to set size %d for file %d: %s", newSize, inodeBlkID, err.Error())
	}
}

func (fs *Exfs) getINode(name string) (blkID uint64, ino INode) {

}
