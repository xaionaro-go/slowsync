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

func getFileTree(dir, cachePath, brokenFilesList string) (fileTree slowsync.FileTree) {
	var err error
	if cachePath == "" {
		fileTree, err = slowsync.GetFileTree(dir)
	} else {
		fileTree, err = slowsync.GetCachedFileTree(dir, cachePath)
	}
	panicIfError(err)
	if brokenFilesList != "" {
		panicIfError(fileTree.SetBrokenFilesList(brokenFilesList))
	}
	return
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

	wg.Add(1)
	go func() {
		srcFileTree = getFileTree(srcDir, *srcFileTreeCachePtr, *srcBrokenFilesPtr)
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		dstFileTree = getFileTree(dstDir, *dstFileTreeCachePtr, "")
		wg.Done()
	}()

	wg.Wait()

	panicIfError(srcFileTree.SyncTo(dstFileTree, *dryRunPtr))
}
