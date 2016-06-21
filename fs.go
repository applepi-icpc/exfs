package main

import (
	"fmt"
	"os"
	gopath "path"
	"strings"
	"syscall"
	"time"

	"github.com/applepi-icpc/exfs/blockmanager"

	log "github.com/Sirupsen/logrus"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

type Perm uint32

func getPerm(ino *INode, context *fuse.Context) (perm Perm) {
	// root
	if context.Uid == 0 {
		return 0x7
	}

	if context.Uid == ino.Uid {
		perm = Perm((ino.Mode & 0700) >> 6)
	} else if context.Gid == ino.Uid {
		perm = Perm((ino.Mode & 0070) >> 3)
	} else {
		perm = Perm((ino.Mode & 0007) >> 0)
	}
	return
}

func (p Perm) Readable() bool {
	return (uint32(p) & 4) != 0
}

func (p Perm) Writable() bool {
	return (uint32(p) & 2) != 0
}

func (p Perm) Executable() bool {
	return (uint32(p) & 1) != 0
}

type Exfs struct {
	pathfs.FileSystem

	blockManager blockmanager.BlockManager
	root         uint64
	debug        bool

	files uint64
}

func NewExfs(blockManager blockmanager.BlockManager, uid uint32, gid uint32, root uint64, newFS bool) (*Exfs, error) {
	fs := &Exfs{
		FileSystem:   pathfs.NewDefaultFileSystem(),
		blockManager: blockManager,
		root:         root,
		debug:        false,
		files:        0,
	}

	saveLoader, persist := blockManager.(blockmanager.PersistentClass)

	// initialize: make a root
	if newFS || !persist {
		blkID, ino, status := fs.createINode(0755|uint32(syscall.S_IFDIR), uid, gid)
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
		fs.root = blkID
		fs.Store()
	} else {
		root, files, err := saveLoader.Load()
		if err != nil {
			return nil, err
		}
		fs.root = root
		fs.files = files
	}

	return fs, nil
}

// errors would be ignored
func (fs *Exfs) Store() {
	bm := fs.blockManager
	saveLoader, persist := bm.(blockmanager.PersistentClass)
	if persist {
		err := saveLoader.Store(fs.root, fs.files)
		if err != nil {
			log.Errorf("Failed to store into blockmanager: %s", err.Error())
		}
		// log.Infof("Stored into blockmanager")
	}
}

func (fs *Exfs) createINode(mode uint32, uid uint32, gid uint32) (blkID uint64, ino *INode, status fuse.Status) {
	// log.Infof("createINode: %o, %d, %d", mode, uid, gid)

	var err error
	blkID, err = fs.blockManager.AllocBlock()
	if err != nil {
		log.Errorf("Failed to alloc new block for inode: %s", err.Error())
		if err == blockmanager.ErrNoMoreBlocks {
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

	fs.files += 1
	fs.Store()

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
	// log.Infof("getINodeByBlkID: %d", blkID)

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
	// log.Infof("readAll: File(%d)", file.inodeBlkID)

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
	// log.Infof("getINode: %s", name)

	// TODO: Optimize with directory buffer

	var err error

	currentBlkID := fs.root
	cleaned := gopath.Clean(name)
	var dirs []string
	if cleaned == "." {
		dirs = make([]string, 0)
	} else {
		dirs = strings.Split(cleaned, string(os.PathSeparator))
	}
	for _, v := range dirs {
		// get current inode
		inode, err := fs.getINodeByBlkID(currentBlkID)
		if err != nil {
			log.Errorf("Failed to get INode(%d) by ID: %s", currentBlkID, err.Error())
			return 0, nil, fuse.EIO
		}

		// is it a directory?
		if inode.Mode&uint32(syscall.S_IFDIR) == 0 {
			err := fmt.Errorf("File(%d) is not a directory", currentBlkID)
			log.Warn(err)
			return 0, nil, fuse.ENOTDIR
		}

		// DONE: CHECK PERMISSION
		if !getPerm(inode, context).Executable() {
			return 0, nil, fuse.EACCES
		}

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
			log.Warnf("Failed to find '%s' in directory(%d): %s", v, currentBlkID, err.Error())
			return 0, nil, fuse.ENOENT
		}

		// TODO: CHECK SYMLINK

		// loop
		currentBlkID = nextID
	}

	blkID = currentBlkID
	ino, err = fs.getINodeByBlkID(blkID)
	if err != nil {
		log.Errorf("Failed to get INode(%d) by ID: %s", blkID, err.Error())
		return 0, nil, fuse.EIO
	}

	status = fuse.OK
	// log.Infof("getINode OK: %d, %v", blkID, ino)
	return
}

func (fs *Exfs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	log.Infof("GetAttr: %s", name)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return nil, status
	}

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
	if fs.blockManager.Blocksize() != blockmanager.SizeUnlimited {
		res.Blocks = uint64(len(ino.Blocks))
		res.Blksize = uint32(fs.blockManager.Blocksize())
	}
	log.Infof("  Attr: %v", res)
	return res, fuse.OK
}

func (fs *Exfs) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Infof("Chmod: %s, %o", name, mode)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Chmod(mode)
}

func (fs *Exfs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	log.Infof("Chown: %s, %d, %d", name, uid, gid)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// DOWN: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Chown(uid, gid)
}

func (fs *Exfs) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	log.Infof("Utimens: %s, %v, %v", name, Atime, Mtime)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Utimens(Atime, Mtime)
}

func (fs *Exfs) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	log.Infof("Truncate: %s, %d", name, size)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

	// TODO: CHECK SYMLINK

	file := NewExfsFile(fs, blkID, ino)
	defer file.Close()
	return file.Truncate(size)
}

func (fs *Exfs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	log.Infof("Access: %s, %d", name, mode)

	_, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return status
	}

	// TODO: CHECK SYMLINK

	// Permission checking
	if mode == syscall.F_OK {
		return fuse.OK
	}

	perm := uint32(getPerm(ino, context))
	if mode&perm == mode {
		return fuse.OK
	} else {
		return fuse.EACCES
	}
}

func (fs *Exfs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	log.Infof("Mkdir: %s, %d", name, mode)

	dir, newName := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

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

	newBlkID, newInode, status := fs.createINode(mode|uint32(syscall.S_IFDIR), context.Owner.Uid, context.Owner.Gid)
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
	log.Infof("Rename: %s, %s", oldName, newName)

	oldDir, oldFname := gopath.Split(oldName)
	blkID, ino, status := fs.getINode(oldDir, context)
	if status != fuse.OK {
		return status
	}

	newDir, newFname := gopath.Split(newName)
	var (
		nBlkID uint64
		nIno   *INode
	)
	if newDir != oldDir {
		nBlkID, nIno, status = fs.getINode(newDir, context)
		if status != fuse.OK {
			return status
		}
	} else {
		nBlkID = blkID
		nIno = ino
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}
	if !getPerm(nIno, context).Writable() {
		return fuse.EACCES
	}

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

	var (
		nFile       *ExfsFile
		nDirEntries *Directory
	)
	if blkID != nBlkID {
		nFile = NewExfsFile(fs, nBlkID, nIno)
		defer nFile.Close()
		nFileData, status := readAll(nFile)
		if status != fuse.OK {
			return status
		}
		_nDirEntries, err := UnmarshalDirectory(nFileData)
		if err != nil {
			log.Errorf("Failed to read directory(%d) contents: %s", nBlkID, err.Error())
			return fuse.EIO
		}
		nDirEntries = &_nDirEntries
	} else {
		nFile = file
		nDirEntries = &dirEntries
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
		log.Errorf("Failed to get INode(%d) by ID: %s", fileBlkID, err.Error())
		return fuse.EIO
	}

	for k, v := range *nDirEntries {
		if v.Filename == newFname {
			// unlink the new one
			newpathIno, err := fs.getINodeByBlkID(v.INodeID)
			if err != nil {
				log.Errorf("Failed to get INode(%d) by ID: %s", v.INodeID, err.Error())
				return fuse.EIO
			}
			if oldpathIno.Mode&uint32(syscall.S_IFDIR) == 0 && newpathIno.Mode&uint32(syscall.S_IFDIR) != 0 {
				return fuse.Status(syscall.EISDIR)
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range newpathIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)
			fs.files -= 1
			fs.Store()

			*nDirEntries = append((*nDirEntries)[:k], (*nDirEntries)[k+1:]...)
			break
		}
	}
	*nDirEntries = append((*nDirEntries), DirectoryEntry{newFname, fileBlkID})

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

func (fs *Exfs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	log.Infof("Rmdir: %s", name)

	dir, fname := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

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
				log.Errorf("Failed to get INode(%d) by ID: %s", v.INodeID, err.Error())
				return fuse.EIO
			}

			// TODO: HARDLINK

			// check if dir
			if fIno.Mode&uint32(syscall.S_IFDIR) == 0 {
				return fuse.ENOTDIR
			}
			// check if empty
			targetFile := NewExfsFile(fs, v.INodeID, fIno)
			defer targetFile.Close()
			targetData, status := readAll(targetFile)
			if status != fuse.OK {
				return status
			}
			targetEnt, err := UnmarshalDirectory(targetData)
			if err != nil {
				log.Errorf("Failed to read directory(%d) contents: %s", v.INodeID, err.Error())
				return fuse.EIO
			}
			if len(targetEnt) > 0 {
				log.Infof("ENOTEMPTY: %v", targetEnt)
				return fuse.Status(syscall.ENOTEMPTY)
			}

			for _, toRemoveBlkID := range fIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)
			fs.files -= 1
			fs.Store()

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

func (fs *Exfs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	log.Infof("Unlink: %s", name)

	dir, fname := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return fuse.EACCES
	}

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
				log.Errorf("Failed to get INode(%d) by ID: %s", v.INodeID, err.Error())
				return fuse.EIO
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range fIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)
			fs.files -= 1
			fs.Store()

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
	log.Infof("Open: %s, %X", name, flags)

	blkID, ino, status := fs.getINode(name, context)
	if status == fuse.ENOENT && flags&uint32(os.O_CREATE) != 0 {
		return fs.Create(name, flags, 0644, context)
	} else if status != fuse.OK {
		return nil, status
	}

	// DONE: CHECK PERMISSION
	if flags&fuse.O_ANYWRITE != 0 {
		if !getPerm(ino, context).Writable() {
			return nil, fuse.EACCES
		}
	} else {
		if !getPerm(ino, context).Readable() {
			return nil, fuse.EACCES
		}
	}

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
	log.Infof("Create: %s, %X, %o", name, flags, mode)

	dir, newName := gopath.Split(name)
	blkID, ino, status := fs.getINode(dir, context)
	if status != fuse.OK {
		return nil, status
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Writable() {
		return nil, fuse.EACCES
	}

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
				log.Errorf("Failed to get INode(%d) by ID: %s", v.INodeID, err.Error())
				return nil, fuse.EIO
			}

			// TODO: HARDLINK

			for _, toRemoveBlkID := range oldIno.Blocks {
				fs.blockManager.RemoveBlock(toRemoveBlkID)
			}
			fs.blockManager.RemoveBlock(v.INodeID)
			fs.files -= 1
			fs.Store()

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

func (fs *Exfs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	log.Infof("OpenDir: %s", name)

	blkID, ino, status := fs.getINode(name, context)
	if status != fuse.OK {
		return nil, status
	}
	if ino.Mode&uint32(syscall.S_IFDIR) == 0 {
		return nil, fuse.ENOTDIR
	}

	// DONE: CHECK PERMISSION
	if !getPerm(ino, context).Readable() {
		return nil, fuse.EACCES
	}

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

	stream = make([]fuse.DirEntry, len(dirEntries))
	for k, v := range dirEntries {
		fIno, err := fs.getINodeByBlkID(v.INodeID)
		if err != nil {
			log.Errorf("Failed to get INode(%d) by ID: %s", v.INodeID, err.Error())
			return stream[:k], fuse.EIO
		}
		stream[k] = fuse.DirEntry{
			Mode: fIno.Mode,
			Name: v.Filename,
		}
	}

	return stream, fuse.OK
}

func (fs *Exfs) StatFs(name string) *fuse.StatfsOut {
	btotal, _, bfree, bavail := fs.blockManager.Blockstat()
	res := &fuse.StatfsOut{
		Blocks:  btotal,
		Bfree:   bfree,
		Bavail:  bavail,
		Files:   fs.files,
		Ffree:   bfree,
		NameLen: 1024, // TODO: Apply this restrict
	}
	if fs.blockManager.Blocksize() != blockmanager.SizeUnlimited && fs.blockManager.Blocksize() <= 0xFFFFFFFF {
		res.Bsize = uint32(fs.blockManager.Blocksize())
	}
	return res
}
