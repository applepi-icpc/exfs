package main

import (
	"flag"
	"os/user"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/applepi-icpc/exfs/kvf"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

var (
	flagMountPoint = flag.String("p", "./fs", "Mount point")
	flagAllowOther = flag.Bool("allow_other", false, "Allow other users to access")
)

func main() {
	flag.Parse()
	mountPoint := *flagMountPoint

	u, err := user.Current()
	if err != nil {
		panic(err)
	}
	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		panic(err)
	}
	gid, err := strconv.Atoi(u.Gid)
	if err != nil {
		panic(err)
	}

	// fs, err := NewExfs(NewMemBlockManager(), 0, true)
	// fs, err := NewExfs(NewMemLimitedBlockManager(), uint32(uid), uint32(gid), 0, true)
	fs, err := NewExfs(kvf.NewKVFBlockManager("st_pool"), uint32(uid), uint32(gid), 0, true)
	fs.SetDebug(true)
	if err != nil {
		log.Fatalf("Create FS failed: %v\n", err)
	}
	nfs := pathfs.NewPathNodeFs(fs, nil)

	root := nfs.Root()
	conn := nodefs.NewFileSystemConnector(root, nil)
	server, err := fuse.NewServer(conn.RawFS(), mountPoint, &fuse.MountOptions{
		AllowOther: *flagAllowOther,
	})
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}

	server.Serve()
}
