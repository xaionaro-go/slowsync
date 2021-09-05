package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"syscall"

	"github.com/xaionaro-go/slowsync"
)

func usage() {
	fmt.Println("slowsync [options] <dir-from> <dir-to>")
	os.Exit(int(syscall.EINVAL))
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func getFileTree(dir, cachePath, brokenFilesList string, maxOpenFiles uint64) (fileTree slowsync.FileTree) {
	var err error
	if cachePath == "" {
		fileTree, err = slowsync.GetFileTree(dir, maxOpenFiles)
	} else {
		fileTree, err = slowsync.GetCachedFileTree(dir, cachePath, maxOpenFiles)
	}
	panicIfError(err)
	if brokenFilesList != "" {
		panicIfError(fileTree.SetBrokenFilesList(brokenFilesList))
	}
	return
}

func setRLimits() syscall.Rlimit {
	var rLimit syscall.Rlimit
	rLimit.Max = 1024 * 1024
	rLimit.Cur = 1024 * 1024
	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error setting rlimit", err)
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		fmt.Println("Error getting rlimit", err)
		rLimit.Cur = 1024
	}
	return rLimit
}

func main() {
	dryRunPtr := flag.Bool("dry-run", false, "do not copy anything")
	srcFileTreeCachePtr := flag.String("src-filetree-cache", "", "enables the file tree cache of the source and set the path where to store it")
	srcBrokenFilesPtr := flag.String("src-broken-files", "", "enables the list of broken files and set the path to it")
	dstFileTreeCachePtr := flag.String("dst-filetree-cache", "", "enables the file tree cache of the destination and set the path where to store it")
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		usage()
	}

	srcDir := args[0]
	dstDir := args[1]

	var wg sync.WaitGroup
	var srcFileTree, dstFileTree slowsync.FileTree

	limits := setRLimits()

	wg.Add(1)
	go func() {
		srcFileTree = getFileTree(srcDir, *srcFileTreeCachePtr, *srcBrokenFilesPtr, limits.Cur/2-480)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		dstFileTree = getFileTree(dstDir, *dstFileTreeCachePtr, "", limits.Cur/2-480)
		wg.Done()
	}()

	wg.Wait()

	panicIfError(srcFileTree.SyncTo(dstFileTree, *dryRunPtr))
}
