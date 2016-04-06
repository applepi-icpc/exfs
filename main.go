package main

import (
	"flag"

	log "github.com/Sirupsen/logrus"
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

	// fs, err := NewExfs(NewMemBlockManager(), 0, true)
	fs, err := NewExfs(NewMemLimitedBlockManager(), 0, true)
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
