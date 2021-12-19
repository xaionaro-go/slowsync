package main

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha512"
	"flag"
	"fmt"
	"hash"
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
	levelsPtr := flag.Uint("dir-levels", 3, "how many levels to do")
	flag.Var((*fileModeVar)(&perms), "dir-perms", "permissions to create directories with")
	hashFuncNamePtr := flag.String("hash", "sha1", "which hash function to use: none, md5, sha1, sha512")
	flag.Parse()
	args := flag.Args()
	if len(args) != 1 {
		usage()
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

	err = fileTree.SplitList(hasher, levels, perms)
	panicIfError(err)
}
