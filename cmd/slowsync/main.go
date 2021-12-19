package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"

	"github.com/andy2046/maths"
	"github.com/xaionaro-go/slowsync"
)

func usage() {
	fmt.Println("slowsync [options] <dir-from> <dir-to> [exclude-dir1 [exclude-dir2 [...]]]")
	os.Exit(int(syscall.EINVAL))
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func main() {
	dryRunPtr := flag.Bool("dry-run", false, "do not copy anything")
	srcFileTreeCachePtr := flag.String("src-filetree-cache", "", "enables the file tree cache of the source and set the path where to store it")
	srcBrokenFilesPtr := flag.String("src-broken-files", "", "enables the list of broken files and set the path to it")
	dstFileTreeCachePtr := flag.String("dst-filetree-cache", "", "enables the file tree cache of the destination and set the path where to store it")
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		usage()
	}

	srcDir := args[0]
	dstDir := args[1]

	var wg sync.WaitGroup
	var srcFileTree, dstFileTree slowsync.FileTree

	limits := slowsync.SetRLimits(1024*1024, 1024*1024*10)
	log.Printf("RLimits: %#+v", limits)
	debug.SetMaxThreads(int(limits.Cur))

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		srcFileTree, err = slowsync.GetFileTreeWrapper(srcDir, *srcFileTreeCachePtr, *srcBrokenFilesPtr, 0, maths.Uint64Var.Min(limits.Cur/uint64(len(os.Args))-480, 15000))
		panicIfError(err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		dstFileTree, err = slowsync.GetFileTreeWrapper(dstDir, *dstFileTreeCachePtr, "", 0, maths.Uint64Var.Min(limits.Cur/uint64(len(os.Args))-480, 15000))
		panicIfError(err)
	}()

	excludeFTChan := make(chan slowsync.FileTree, len(flag.Args()))
	for _, arg := range flag.Args()[2:] {
		wg.Add(1)
		go func(arg string) {
			defer wg.Done()
			cachePath := ""
			if *dstFileTreeCachePtr != "" {
				cachePath = *dstFileTreeCachePtr + "-" + strings.ReplaceAll(arg, "/", "-")
			}
			fileTree, err := slowsync.GetFileTreeWrapper(arg, cachePath, "", 0, maths.Uint64Var.Min(limits.Cur/uint64(len(os.Args))-480, 15000))
			panicIfError(err)
			excludeFTChan <- fileTree
		}(arg)
	}

	wg.Wait()
	close(excludeFTChan)

	var excludeFTs []slowsync.FileTree
	for ch := range excludeFTChan {
		excludeFTs = append(excludeFTs, ch)
	}

	panicIfError(srcFileTree.SyncTo(dstFileTree, excludeFTs, *dryRunPtr))
}
