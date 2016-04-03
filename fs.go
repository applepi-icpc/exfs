package main

import (
	"fmt"
	"os"
	gopath "path"
	"strings"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type Exfs struct {
	pathfs.FileSystem

	blockManager BlockManager
	root         uint64
	debug        bool
}

func NewExfs(blockManager BlockManager, root uint64, newFS bool) (*Exfs, error) {
	fs := &Exfs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		blockManager: blockManager,
		root:         root,
		debug:        false,
	}

	// initialize: make a root
	if newFS {
		blkID, ino, status := fs.createINode(0755|uint32(os.ModeDir), 0, 0)
		if status != fuse.OK {
			err := fmt.Errorf("Failed to make inode for FS root: status %d", status)
			log.Error(err)
			return nil, err
		}
		f := NewExfsFile(fs, blkID, ino)
		defer f.Close()
		_, code := f.Write(make(Directory, 0).Marshal(), 0)
		if !code.Ok() {
			err := fmt.Errorf("Failed to write empty directory to root: %s", code.String())
			log.Error(err)
			return nil, err
		}
	}

	return fs, nil
}

func (fs *Exfs) createINode(mode uint32, uid uint32, gid uint32) (blkID uint64, ino *INode, status fuse.Status) {
	var err error
	blkID, err = fs.blockManager.AllocBlock()
	if err != nil {
		log.Errorf("Failed to alloc new block for inode: %s", err.Error())
		if err == ErrNoMoreBlocks {
			status = fuse.Status(syscall.ENOSPC)
		} else {
			status = fuse.EIO
		}
		blkID = 0
		return
	}

	ino = &INode{
		Size:   0,
		Atime:  uint64(time.Now().Unix()),
		Mtime:  uint64(time.Now().Unix()),
		Ctime:  uint64(time.Now().Unix()),
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
		ino = &INode{}
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

func (fs *Exfs) getINodeByBlkID(blkID uint64) (ino *INode, err error) {
	blk, err := fs.blockManager.GetBlock(blkID)
	if err != nil {
		fs.logReadBlkError(blkID, 0, err)
		return nil, err
	}
	ino, err = UnmarshalINode(blk)
	if err != nil {
		log.Errorf("Failed to read inode(%d): %s", blkID, err.Error())
		return nil, err
	}
	return
}

func readAll(file *ExfsFile) ([]byte, fuse.Status) {
	fileData := make([]byte, file.inode.Size)
	readResult, readStatus := file.Read(fileData, 0)
	if !readStatus.Ok() {
		err := fmt.Errorf("Failed to read file(%d) content: %s", file.inodeBlkID, readStatus.String())
		log.Error(err)
		return nil, fuse.EIO
	}
	fileData, status := readResult.Bytes(fileData)
	if !status.Ok() {
		err := fmt.Errorf("Failed to read file(%d) content: %s", file.inodeBlkID, status.String())
		log.Error(err)
		return nil, fuse.EIO
	}
	readResult.Done()
	return fileData, fuse.OK
}

func (fs *Exfs) getINode(name string, context *fuse.Context) (blkID uint64, ino *INode, status fuse.Status) {
	// TODO: Optimize with directory buffer

	var err error

	currentBlkID := fs.root
	dirs := strings.Split(gopath.Clean(name), string(os.PathSeparator))
	for _, v := range dirs {
		// get current inode
		inode, err := fs.getINodeByBlkID(currentBlkID)
		if err != nil {
			return 0, nil, fuse.EIO
		}

		// is it a directory?
		if !os.FileMode(inode.Mode).IsDir() {
			err := fmt.Errorf("File(%d) is not a directory", currentBlkID)
			log.Error(err)
			return 0, nil, fuse.ENOTDIR
		}

		// TODO: CHECK PERMISSION

		// create file
		file := NewExfsFile(fs, currentBlkID, inode)
		defer file.Close()

		fileData, status := readAll(file)
		if status != fuse.OK {
			return 0, nil, status
		}

		// read dir
		dirEntries, err := UnmarshalDirectory(fileData)
		if err != nil {
			log.Errorf("Failed to read directory(%d) contents: %s", currentBlkID, err.Error())
			return 0, nil, fuse.EIO
		}
		nextID, err := dirEntries.FindEntry(v)
		if err != nil {
			log.Errorf("Failed to find '%s' in directory(%d): %s", v, currentBlkID, err.Error())
			return 0, nil, fuse.ENOENT
		}

		// TODO: CHECK SYMLINK

		// loop
		currentBlkID = nextID
	}

	blkID = currentBlkID
	ino, err = fs.getINodeByBlkID(blkID)
	if err != nil {
		return 0, nil, fuse.EIO
	}

	status = fuse.OK
	return
}

func (fs *Exfs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return nil, status
	}

	// TODO: CHECK PERMISSION

	res := &fuse.Attr{
		Ino:   blkID,
		Size:  ino.Size,
		Atime: ino.Atime,
		Mtime: ino.Mtime,
		Ctime: ino.Ctime,
		Mode:  ino.Mode,
		// Nlink
		Owner: fuse.Owner{
			Uid: ino.Uid,
			Gid: ino.Gid,
		},
	}
	if fs.blockManager.Blocksize() != SizeUnlimited {
		res.Blocks = uint64(len(ino.Blocks))
		res.Blksize = uint32(fs.blockManager.Blocksize())
	}
	return res, fuse.OK
}

func (fs *Exfs) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION
	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Chmod(mode)
}

func (fs *Exfs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION
	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Chown(uid, gid)
}

func (fs *Exfs) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION
	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Utimens(Atime, Mtime)
}

func (fs *Exfs) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION
	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Truncate(size)
}

func (fs *Exfs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	_, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK SYMLINK

	// Permission checking
	if context.Uid == ino.Uid {
		return fuse.Status((ino.Mode & 0700) >> 6)
	} else if context.Gid == ino.Uid {
		return fuse.Status((ino.Mode & 0070) >> 3)
	} else {
		return fuse.Status((ino.Mode & 0007) >> 0)
	}
}

func (fs *Exfs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	dir, newName := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	fileData, status := readAll(file)
	if status != fuse.OK {
		return status
	}

	dirEntries, err := UnmarshalDirectory(fileData)
	if err != nil {
		log.Errorf("Failed to read directory(%d) contents: %s", blkID, err.Error())
		return fuse.EIO
	}

	for _, v := range dirEntries {
		if v.Filename == newName {
			return fuse.Status(syscall.EEXIST)
		}
	}

	newBlkID, newInode, status := fs.createINode(mode|uint32(os.ModeDir), context.Owner.Uid, context.Owner.Gid)
	if status != fuse.OK {
		return status
	}
	newDirEntries := NewExfsFile(fs, newBlkID, newInode)
	defer newDirEntries.Close()
	_, status = newDirEntries.Write(make(Directory, 0).Marshal(), 0)
	if status != fuse.OK {
		return status
	}

	dirEntries = append(dirEntries, DirectoryEntry{newName, newBlkID})

	newData := dirEntries.Marshal()
	status = file.Truncate(uint64(len(newData)))
	if status != fuse.OK {
		return status
	}
	written, status := file.Write(newData, 0)
	if status == fuse.OK && int(written) != len(newData) {
		log.Warnf("Warning: Mkdir: Write OK but length mismatch: %d <=> %d", written, len(newData))
	}
	return status
}

func (fs *Exfs) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	oldDir, oldFname := gopath.Split(oldName)
	blkID, ino, status := fs.getINode(oldDir, context)
	if status != fuse.OK {
		return status
	}

	newDir, newFname := gopath.Split(newName)
	nBlkID, nIno, status := fs.getINode(newDir, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	fileData, status := readAll(file)
	if status != fuse.OK {
		return status
	}
	dirEntries, err := UnmarshalDirectory(fileData)
	if err != nil {
		log.Errorf("Failed to read directory(%d) contents: %s", blkID, err.Error())
		return fuse.EIO
	}

	nFile := NewExfsFile(fs, nBlkID, nIno)
	defer nFile.Close()
	nFileData, status := readAll(nFile)
	if status != fuse.OK {
		return status
	}
	nDirEntries, err := UnmarshalDirectory(nFileData)
	if err != nil {
		log.Errorf("Failed to read directory(%d) contents: %s", nBlkID, err.Error())
		return fuse.EIO
	}

	var fileBlkID uint64
	found := false
	for k, v := range dirEntries {
		if v.Filename == oldFname {
			fileBlkID = v.INodeID
			dirEntries = append(dirEntries[:k], dirEntries[k+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fuse.ENOENT
	}
	oldpathIno, err := fs.getINodeByBlkID(fileBlkID)
	if err != nil {
		return fuse.EIO
	}

	for k, v := range nDirEntries {
		if v.Filename == newFname {
			// unlink the new one
			newpathIno, err := fs.getINodeByBlkID(v.INodeID)
			if err != nil {
				return fuse.EIO
			}
			if oldpathIno.Mode&uint32(os.ModeDir) == 0 && newpathIno.Mode&uint32(os.ModeDir) != 0 {
				return fuse.Status(syscall.EISDIR)
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range newpathIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)

			nDirEntries = append(nDirEntries[:k], nDirEntries[k+1:]...)
			break
		}
	}
	nDirEntries = append(nDirEntries, DirectoryEntry{newFname, fileBlkID})

	newData := dirEntries.Marshal()
	status = file.Truncate(uint64(len(newData)))
	if status != fuse.OK {
		return status
	}
	written, status := file.Write(newData, 0)
	if status == fuse.OK && int(written) != len(newData) {
		log.Warnf("Warning: Mkdir: Write OK but length mismatch: %d <=> %d", written, len(newData))
	}
	if status != fuse.OK {
		return status
	}

	nNewData := nDirEntries.Marshal()
	status = nFile.Truncate(uint64(len(nNewData)))
	if status != fuse.OK {
		return status
	}
	written, status = nFile.Write(nNewData, 0)
	if status == fuse.OK && int(written) != len(nNewData) {
		log.Warnf("Warning: Mkdir: Write OK but length mismatch: %d <=> %d", written, len(nNewData))
	}
	if status != fuse.OK {
		return status
	}

	return fuse.OK
}

func (fs *Exfs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	dir, fname := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK PERMISSION

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	fileData, status := readAll(file)
	if status != fuse.OK {
		return status
	}

	dirEntries, err := UnmarshalDirectory(fileData)
	if err != nil {
		log.Errorf("Failed to read directory(%d) contents: %s", blkID, err.Error())
		return fuse.EIO
	}

	found := false
	for k, v := range dirEntries {
		if v.Filename == fname {
			fIno, err := fs.getINodeByBlkID(v.INodeID)
			if err != nil {
				return fuse.EIO
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range fIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)

			dirEntries = append(dirEntries[:k], dirEntries[k+1:]...)
			found = true
			break
		}
	}
	if !found {
		return fuse.ENOENT
	}

	newData := dirEntries.Marshal()
	status = file.Truncate(uint64(len(newData)))
	if status != fuse.OK {
		return status
	}
	written, status := file.Write(newData, 0)
	if status == fuse.OK && int(written) != len(newData) {
		log.Warnf("Warning: Mkdir: Write OK but length mismatch: %d <=> %d", written, len(newData))
	}
	return status
}

func (fs *Exfs) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	blkID, ino, status := fs.getINode(name, context)
	if status == fuse.ENOENT && flags&uint32(os.O_CREATE) != 0 {
		return fs.Create(name, flags, 0644, context)
	} else if status != fuse.OK {
		return nil, status
	}

	// TODO: CHECK PERMISSION
	// TODO: CHECK SYMLINK

	f := NewExfsFile(fs, blkID, ino)
	f.inode.Atime = uint64(time.Now().Unix())
	if flags&fuse.O_ANYWRITE != 0 {
		f.inode.Mtime = uint64(time.Now().Unix())
	}
	f.saveINode()

	return f, fuse.OK
}

func (fs *Exfs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	dir, newName := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return nil, status
	}

	// TODO: CHECK PERMISSION

	dirFile := NewExfsFile(fs, blkID, ino)
	defer dirFile.Close()
	fileData, status := readAll(dirFile)
	if status != fuse.OK {
		return nil, status
	}

	dirEntries, err := UnmarshalDirectory(fileData)
	if err != nil {
		log.Errorf("Failed to read directory(%d) contents: %s", blkID, err.Error())
		return nil, fuse.EIO
	}

	newBlkID, newInode, status := fs.createINode(mode, context.Owner.Uid, context.Owner.Gid)
	if status != fuse.OK {
		return nil, status
	}

	for k, v := range dirEntries {
		if v.Filename == newName {
			// unlink the old one
			oldIno, err := fs.getINodeByBlkID(v.INodeID)
			if err != nil {
				return nil, fuse.EIO
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range oldIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)

			dirEntries = append(dirEntries[:k], dirEntries[k+1:]...)
			break
		}
	}
	dirEntries = append(dirEntries, DirectoryEntry{newName, newBlkID})

	newData := dirEntries.Marshal()
	status = dirFile.Truncate(uint64(len(newData)))
	if status != fuse.OK {
		return nil, status
	}
	written, status := dirFile.Write(newData, 0)
	if status == fuse.OK && int(written) != len(newData) {
		log.Warnf("Warning: Mkdir: Write OK but length mismatch: %d <=> %d", written, len(newData))
	}

	f := NewExfsFile(fs, newBlkID, newInode)

	return f, status
}
