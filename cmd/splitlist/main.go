package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha512"
	"flag"
	"fmt"
	"hash"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"syscall"

	"github.com/xaionaro-go/slowsync"
)

func usage() {
	fmt.Println("splitlist [options] <dir>")
	os.Exit(int(syscall.EINVAL))
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func main() {
	perms := os.FileMode(0644)
	levelsPtr := flag.Uint("dir-levels", 6, "how many levels to do")
	skipFirstCharsPtr := flag.Uint("skip-first-chars", 0, "how many characters of the hash to skip (from the beginning)")
	flag.Var((*fileModeVar)(&perms), "dir-perms", "permissions to create directories with")
	hashFuncNamePtr := flag.String("hash", "sha1", "which hash function to use: none, md5, sha1, sha512")
	netPprofPtr := flag.String("net-pprof", ":18095", "")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		usage()
	}

	if *netPprofPtr != "" {
		go func() {
			log.Println(http.ListenAndServe(*netPprofPtr, nil))
		}()
	}

	dir := args[0]
	levels := *levelsPtr
	var hasher hash.Hash
	switch strings.ToLower(*hashFuncNamePtr) {
	case "none":
		hasher = newDummyHasher()
	case "md5":
		hasher = md5.New()
	case "sha1":
		hasher = sha1.New()
	case "sha512":
		hasher = sha512.New()
	}

	fileTree, err := slowsync.GetFileTreeWrapper(dir, "", "", 1, 1)
	panicIfError(err)

	err = fileTree.SplitList(hasher, levels, perms, *skipFirstCharsPtr)
	panicIfError(err)
}
