package main

import (
	"flag"
	"log"

	"github.com/hanwen/go-fuse/fuse/nodefs"
	"github.com/hanwen/go-fuse/fuse/pathfs"
)

func main() {
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  hello MOUNTPOINT")
	}
	// fs, err := NewExfs(NewMemBlockManager(), 0, true)
	fs, err := NewExfs(NewMemLimitedBlockManager(), 0, true)
	fs.SetDebug(true)
	if err != nil {
		log.Fatalf("Create FS failed: %v\n", err)
	}
	nfs := pathfs.NewPathNodeFs(fs, nil)
	server, _, err := nodefs.MountRoot(flag.Arg(0), nfs.Root(), nil)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	server.Serve()
}
