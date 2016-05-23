package main

import (
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/applepi-icpc/exfs/blockmanager"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

// TODO: R/W buffer
type ExfsFile struct {
	fs *Exfs

	inodeBlkID uint64
	inode      *INode

	opening bool
	lock    sync.RWMutex
}

func NewExfsFile(fs *Exfs, blkID uint64, inode *INode) *ExfsFile {
	return &ExfsFile{
		fs:         fs,
		inodeBlkID: blkID,
		inode:      inode,
		opening:    true,
	}
}

func (f *ExfsFile) SetInode(*nodefs.Inode) {}

func (f *ExfsFile) String() string {
	return fmt.Sprintf("exampleFileSystem File %d", f.inodeBlkID)
}

func (f *ExfsFile) InnerFile() nodefs.File {
	return nil
}

func sprintHeadTail(arr []byte) string {
	if len(arr) <= 6 {
		return fmt.Sprintf("%v", arr)
	} else {
		l := len(arr)
		return fmt.Sprintf("[%X %X %X ... %X %X %X]", arr[0], arr[1], arr[2], arr[l-3], arr[l-2], arr[l-1])
	}
}

func (f *ExfsFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	// log.Infof("Read file(%d): off = %d, end = %d, size = %d", f.inodeBlkID, off, off+int64(len(dest)), f.inode.Size)

	// f.lock.RLock()
	// defer f.lock.RUnlock()

	if !f.opening {
		return fuse.ReadResultData(nil), fuse.EBADF
	}
	if off < 0 {
		return fuse.ReadResultData(nil), fuse.EINVAL
	}

	end := int64(off) + int64(len(dest))
	if off >= int64(f.inode.Size) {
		return fuse.ReadResultData(nil), fuse.OK
	}
	if end > int64(f.inode.Size) {
		end = int64(f.inode.Size)
	}

	// read off:end
	blkSize := int64(f.fs.blockManager.Blocksize())
	if blkSize == int64(blockmanager.SizeUnlimited) {
		if len(f.inode.Blocks) == 0 { // no blocks allocated
			return fuse.ReadResultData(make([]byte, 0)), fuse.OK
		} else {
			blk, err := f.fs.blockManager.GetBlock(f.inode.Blocks[0])
			if err != nil {
				f.fs.logReadBlkError(f.inode.Blocks[0], f.inodeBlkID, err)
				return fuse.ReadResultData(nil), fuse.EIO
			}

			return fuse.ReadResultData(blk[off:end]), fuse.OK
		}
	} else {
		if len(f.inode.Blocks) == 0 { // no blocks allocated
			return fuse.ReadResultData(make([]byte, 0)), fuse.OK
		} else {
			firstBlk := off / blkSize
			lastBlk := (end - 1) / blkSize
			if firstBlk == lastBlk {
				blk, err := f.fs.blockManager.GetBlock(f.inode.Blocks[firstBlk])
				if err != nil {
					f.fs.logReadBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
					return fuse.ReadResultData(nil), fuse.EIO
				}

				blkOff := firstBlk * blkSize
				return fuse.ReadResultData(blk[off-blkOff : end-blkOff]), fuse.OK
			} else {
				var (
					res    = make([]byte, end-off)
					ptr    = int64(0)
					blkOff int64
					leng   int64
					blk    []byte
					err    error
				)

				blk, err = f.fs.blockManager.GetBlock(f.inode.Blocks[firstBlk])
				if err != nil {
					f.fs.logReadBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
					return fuse.ReadResultData(nil), fuse.EIO
				}
				blkOff = firstBlk * blkSize
				leng = blkSize - (off - blkOff) // off-blkOff : blkSize
				copy(res[ptr:ptr+leng], blk[off-blkOff:])
				// log.Infof("Read data[%d:%d] from block(%d)[%d:%d] (%d-th): %s", ptr, ptr+leng, f.inode.Blocks[firstBlk], off-blkOff, len(blk), firstBlk, sprintHeadTail(blk[off-blkOff:]))
				ptr += leng

				for i := firstBlk + 1; i <= lastBlk-1; i++ {
					blk, err = f.fs.blockManager.GetBlock(f.inode.Blocks[i])
					if err != nil {
						f.fs.logReadBlkError(f.inode.Blocks[i], f.inodeBlkID, err)
						return fuse.ReadResultData(nil), fuse.EIO
					}
					copy(res[ptr:ptr+blkSize], blk)
					// log.Infof("Read data[%d:%d] from block(%d)[%d:%d] (%d-th): %s", ptr, ptr+blkSize, f.inode.Blocks[i], 0, len(blk), i, sprintHeadTail(blk))
					ptr += blkSize
				}

				blk, err = f.fs.blockManager.GetBlock(f.inode.Blocks[lastBlk])
				if err != nil {
					f.fs.logReadBlkError(f.inode.Blocks[lastBlk], f.inodeBlkID, err)
					return fuse.ReadResultData(nil), fuse.EIO
				}
				blkOff = lastBlk * blkSize
				leng = end - blkOff // 0 : end-blkOff
				copy(res[ptr:], blk[:leng])
				// log.Infof("Read data[%d:%d] from block(%d)[%d:%d] (%d-th): %s", ptr, len(res), f.inode.Blocks[lastBlk], 0, leng, lastBlk, sprintHeadTail(blk[:leng]))

				return fuse.ReadResultData(res), fuse.OK
			}
		}
	}
}

func (f *ExfsFile) saveINode() (err error) {
	inoB := f.inode.Marshal()
	err = f.fs.blockManager.SetBlock(f.inodeBlkID, inoB)
	return
}

func (f *ExfsFile) setSize(newSize uint64) error {
	// log.Warnf("setSize file(%d): %d", f.inodeBlkID, newSize)

	// If any operation failed, f.inode would remain unchanged.
	// For f.fs.blockManager, it should support log to remain consistent.

	var err error

	succeed := false
	oriSize := f.inode.Size
	blkOri := uint64(len(f.inode.Blocks))
	oriBlocks := make([]uint64, blkOri)
	copy(oriBlocks, f.inode.Blocks)

	defer func() {
		if !succeed {
			f.inode.Size = oriSize
			f.inode.Blocks = oriBlocks
			// TODO: f.fs.blockManager.rollback()
		} else {
			// TODO: f.fs.blockManager.commit()
		}
	}()

	if newSize == 0 {
		f.inode.Size = 0
		f.inode.Blocks = make([]uint64, 0)
		err = f.saveINode()
		if err != nil {
			return err
		}

		// deallocate all blocks
		for _, blkID := range oriBlocks {
			err = f.fs.blockManager.RemoveBlock(blkID)
			if err != nil {
				return err
			}
		}
	} else { // newSize != 0
		blkSize := f.fs.blockManager.Blocksize()

		if blkSize == blockmanager.SizeUnlimited {
			if len(f.inode.Blocks) == 0 { // alloc a block
				blkID, err := f.fs.blockManager.AllocBlock()
				if err != nil {
					return err
				}

				f.inode.Blocks = []uint64{blkID}
				f.inode.Size = newSize
				err = f.saveINode()
				if err != nil {
					return err
				}

				blk := make([]byte, newSize)
				err = f.fs.blockManager.SetBlock(blkID, blk)
				if err != nil {
					return err
				}
			} else { // adjust the very block
				f.inode.Size = newSize
				err = f.saveINode()
				if err != nil {
					return err
				}

				blkID := f.inode.Blocks[0]
				blk, err := f.fs.blockManager.GetBlock(blkID)
				if err != nil {
					return err
				}

				if uint64(len(blk)) < newSize {
					blk = append(blk, make([]byte, newSize-uint64(len(blk)))...)
				} else if uint64(len(blk)) > newSize {
					blk = blk[:newSize]
				}
				err = f.fs.blockManager.SetBlock(blkID, blk)
				if err != nil {
					return err
				}
			}
		} else { // blkSize is limited
			blkNeeds := (newSize + blkSize - 1) / blkSize

			if newSize > oriSize { // alloc new blocks
				var lastBlk uint64
				if blkOri > 0 {
					lastBlk = f.inode.Blocks[blkOri-1]
				}

				if blkNeeds == blkOri { // adjust the last block
					f.inode.Size = newSize
					err = f.saveINode()
					if err != nil {
						return err
					}

					blk, err := f.fs.blockManager.GetBlock(lastBlk)
					if err != nil {
						return err
					}
					blk = append(blk, make([]byte, newSize-oriSize)...)
					err = f.fs.blockManager.SetBlock(lastBlk, blk)
					if err != nil {
						return err
					}
				} else { // alloc new blocks
					for i := blkOri; i < blkNeeds; i++ {
						blkID, err := f.fs.blockManager.AllocBlock()
						if err != nil {
							return err
						}
						f.inode.Blocks = append(f.inode.Blocks, blkID)
					}

					f.inode.Size = newSize
					err = f.saveINode()
					if err != nil {
						return err
					}

					// set original last block
					var (
						blk []byte
						err error
					)

					if blkOri > 0 {
						blk, err = f.fs.blockManager.GetBlock(lastBlk)
						if err != nil {
							return err
						}
						if uint64(len(blk)) < blkSize {
							blk = append(blk, make([]byte, blkSize-uint64(len(blk)))...)
							err = f.fs.blockManager.SetBlock(lastBlk, blk)
							if err != nil {
								return err
							}
						}
					}

					// set new full blocks
					for i := blkOri; i < blkNeeds-1; i++ {
						blkID := f.inode.Blocks[i]
						blk, err := f.fs.blockManager.GetBlock(blkID)
						if err != nil {
							return err
						}
						blk = make([]byte, blkSize)
						err = f.fs.blockManager.SetBlock(blkID, blk)
						if err != nil {
							return err
						}
					}

					// set new last block
					blkID := f.inode.Blocks[blkNeeds-1]
					blk = make([]byte, newSize-(blkNeeds-1)*blkSize)
					err = f.fs.blockManager.SetBlock(blkID, blk)
					if err != nil {
						return err
					}
				}
			} else if newSize < oriSize { // truncate
				f.inode.Blocks = f.inode.Blocks[:blkNeeds]
				f.inode.Size = newSize
				err = f.saveINode()
				if err != nil {
					return err
				}

				for i := blkNeeds; i < blkOri; i++ {
					blkID := oriBlocks[i]
					err = f.fs.blockManager.RemoveBlock(blkID)
					if err != nil {
						return err
					}
				}

				lastBlk := oriBlocks[blkNeeds-1]
				blk, err := f.fs.blockManager.GetBlock(lastBlk)
				if err != nil {
					return err
				}
				blk = blk[:newSize-(blkNeeds-1)*blkSize]
				err = f.fs.blockManager.SetBlock(lastBlk, blk)
				if err != nil {
					return err
				}
			}
		}
	}

	succeed = true
	return nil
}

func (f *ExfsFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	// log.Infof("Write file(%d): off = %d, end = %d, size = %d", f.inodeBlkID, off, off+int64(len(data)), f.inode.Size)

	// f.lock.Lock()
	// defer f.lock.Unlock()

	succeed := false
	defer func() {
		if succeed {
			f.inode.Mtime = uint64(time.Now().Unix())
			f.saveINode()
			// TODO: f.fs.blockManager.commit()
		} else {
			// TODO: f.fs.blockManager.rollback()
		}
	}()

	if !f.opening {
		return 0, fuse.EBADF
	}
	if off < 0 {
		return 0, fuse.EINVAL
	}
	end := int64(off) + int64(len(data))
	if end > int64(f.inode.Size) {
		err := f.setSize(uint64(end))
		if err != nil {
			f.fs.logSetSizeError(uint64(end), f.inodeBlkID, err)
			return 0, fuse.EIO
		}
	}

	// write off:end
	blkSize := int64(f.fs.blockManager.Blocksize())
	if blkSize == int64(blockmanager.SizeUnlimited) {
		if len(f.inode.Blocks) == 0 {
			succeed = true
			return 0, fuse.OK
		} else {
			blk, err := f.fs.blockManager.GetBlock(f.inode.Blocks[0])
			if err != nil {
				f.fs.logReadBlkError(f.inode.Blocks[0], f.inodeBlkID, err)
				return 0, fuse.EIO
			}
			copy(blk[off:end], data)
			err = f.fs.blockManager.SetBlock(f.inode.Blocks[0], blk)
			if err != nil {
				f.fs.logWriteBlkError(f.inode.Blocks[0], f.inodeBlkID, err)
				return 0, fuse.EIO
			}

			succeed = true
			return uint32(len(data)), fuse.OK
		}
	} else {
		if len(f.inode.Blocks) == 0 {
			return 0, fuse.OK
		} else {
			firstBlk := off / blkSize
			lastBlk := (end - 1) / blkSize
			if firstBlk == lastBlk {
				blk, err := f.fs.blockManager.GetBlock(f.inode.Blocks[firstBlk])
				if err != nil {
					f.fs.logReadBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
					return 0, fuse.EIO
				}

				blkOff := firstBlk * blkSize
				copy(blk[off-blkOff:end-blkOff], data)
				err = f.fs.blockManager.SetBlock(f.inode.Blocks[firstBlk], blk)
				if err != nil {
					f.fs.logWriteBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
					return 0, fuse.EIO
				}

				succeed = true
				return uint32(len(data)), fuse.OK
			} else {
				written = 0
				var (
					blkOff int64
					leng   uint32
					blk    []byte
					err    error
				)

				if off-blkOff != 0 {
					blk, err = f.fs.blockManager.GetBlock(f.inode.Blocks[firstBlk])
					if err != nil {
						f.fs.logReadBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
						return 0, fuse.EIO
					}
					blkOff = firstBlk * blkSize
					leng = uint32(blkSize - (off - blkOff))
					copy(blk[off-blkOff:], data[written:written+leng])
				} else {
					blk = data[written : written+uint32(blkSize)]
				}
				err = f.fs.blockManager.SetBlock(f.inode.Blocks[firstBlk], blk)
				// log.Infof("Write data[%d:%d] to block(%d)[%d:%d] (%d-th): %s", written, written+leng, f.inode.Blocks[firstBlk], off-blkOff, len(blk), firstBlk, sprintHeadTail(data[written:written+leng]))
				if err != nil {
					f.fs.logWriteBlkError(f.inode.Blocks[firstBlk], f.inodeBlkID, err)
					return 0, fuse.EIO
				}
				written += leng

				for i := firstBlk + 1; i <= lastBlk-1; i++ {
					err := f.fs.blockManager.SetBlock(f.inode.Blocks[i], data[written:written+uint32(blkSize)])
					// log.Infof("Write data[%d:%d] to block(%d)[%d:%d] (%d-th): %s", written, written+uint32(blkSize), f.inode.Blocks[i], 0, blkSize, i, sprintHeadTail(data[written:written+uint32(blkSize)]))
					if err != nil {
						f.fs.logWriteBlkError(f.inode.Blocks[i], f.inodeBlkID, err)
						return written, fuse.EIO
					}
					written += uint32(blkSize)
				}

				blkOff = lastBlk * blkSize
				leng = uint32(end - blkOff) // 0 : end-blkOff
				if leng != 0 {
					blk, err = f.fs.blockManager.GetBlock(f.inode.Blocks[lastBlk])
					if err != nil {
						f.fs.logReadBlkError(f.inode.Blocks[lastBlk], f.inodeBlkID, err)
						return written, fuse.EIO
					}
					copy(blk[:leng], data[written:])
				} else {
					blk = data[written:]
				}

				err = f.fs.blockManager.SetBlock(f.inode.Blocks[lastBlk], blk)
				// log.Infof("Write data[%d:%d] to block(%d)[%d:%d] (%d-th): %s", written, len(data), f.inode.Blocks[lastBlk], 0, leng, lastBlk, sprintHeadTail(data[written:]))
				if err != nil {
					f.fs.logWriteBlkError(f.inode.Blocks[lastBlk], f.inodeBlkID, err)
					return written, fuse.EIO
				}
				written += leng

				succeed = true
				return written, fuse.OK
			}
		}
	}
}

func (f *ExfsFile) Flush() fuse.Status {
	return fuse.OK
}

func (f *ExfsFile) Release() {
	// f.lock.Lock()
	// defer f.lock.Unlock()
	f.opening = false
}

func (f *ExfsFile) Close() {
	f.Flush()
	f.Release()
}

func (f *ExfsFile) Fsync(flags int) (code fuse.Status) {
	return fuse.OK
}

func (f *ExfsFile) Truncate(size uint64) fuse.Status {
	if !f.opening {
		return fuse.EBADF
	}
	if size < 0 {
		return fuse.EINVAL
	}

	// f.lock.Lock()
	// defer f.lock.Unlock()

	err := f.setSize(size)
	if err != nil {
		return fuse.EIO
	}

	f.inode.Mtime = uint64(time.Now().Unix())
	f.saveINode()
	return fuse.OK
}

func (f *ExfsFile) GetAttr(out *fuse.Attr) fuse.Status {
	if !f.opening {
		return fuse.EBADF
	}

	// f.lock.RLock()
	// defer f.lock.RUnlock()

	out.Size = f.inode.Size
	blkSize := int64(f.fs.blockManager.Blocksize())
	if blkSize != int64(blockmanager.SizeUnlimited) {
		out.Blocks = uint64(len(f.inode.Blocks))
		out.Blksize = uint32(blkSize)
	}
	out.Atime = f.inode.Atime
	out.Mtime = f.inode.Mtime
	out.Ctime = f.inode.Ctime
	out.Mode = f.inode.Mode
	out.Owner.Gid = f.inode.Gid
	out.Owner.Uid = f.inode.Uid
	return fuse.OK
}

func (f *ExfsFile) Chown(uid uint32, gid uint32) fuse.Status {
	if !f.opening {
		return fuse.EBADF
	}

	// f.lock.Lock()
	// defer f.lock.Unlock()

	f.inode.Gid = gid
	f.inode.Uid = uid
	f.inode.Ctime = uint64(time.Now().Unix())
	err := f.saveINode()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *ExfsFile) Chmod(perms uint32) fuse.Status {
	if !f.opening {
		return fuse.EBADF
	}

	// f.lock.Lock()
	// defer f.lock.Unlock()

	oldPerm := f.inode.Mode & 0777
	f.inode.Mode ^= oldPerm
	f.inode.Mode |= perms & 0777
	f.inode.Ctime = uint64(time.Now().Unix())
	err := f.saveINode()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *ExfsFile) Utimens(atime *time.Time, mtime *time.Time) fuse.Status {
	if !f.opening {
		return fuse.EBADF
	}

	// f.lock.Lock()
	// defer f.lock.Unlock()

	f.inode.Atime = uint64(atime.Unix())
	f.inode.Mtime = uint64(mtime.Unix())
	f.inode.Ctime = uint64(time.Now().Unix())
	err := f.saveINode()
	if err != nil {
		return fuse.EIO
	}
	return fuse.OK
}

func (f *ExfsFile) Allocate(off uint64, size uint64, mode uint32) (code fuse.Status) {
	if !f.opening {
		return fuse.EBADF
	}

	// default or FALLOC_FL_KEEP_SIZE
	if mode != 0 && mode != 1 {
		return fuse.Status(syscall.EOPNOTSUPP)
	}

	// f.lock.Lock()
	// defer f.lock.Unlock()

	if off+size > f.inode.Size && mode == 0 {
		err := f.setSize(off + size)
		if err != nil {
			return fuse.EIO
		}
	}

	return fuse.OK
}
