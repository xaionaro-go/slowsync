package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"
)

func usage() {
	fmt.Println("hashtreediff [options] <left hash tree file path> <right hash tree file path>")
	os.Exit(int(syscall.EINVAL))
}

func panicIfError(err error) {
	if err == nil {
		return
	}
	panic(err)
}

func main() {
	groupBy := flag.String("group-by", "digest", "the key field; possible values: digest, path")
	flag.Parse()
	args := flag.Args()
	if len(args) != 2 {
		usage()
	}

	leftHashTreeFilePath := args[0]
	rightHashTreeFilePath := args[1]

	var parseHashTreeFunc func(string) (HashTree, error)
	switch strings.ToLower(*groupBy) {
	case "digest":
		parseHashTreeFunc = parseHashTreeByDigest
	case "path":
		parseHashTreeFunc = parseHashTreeByPath
	}

	leftMap, err := parseHashTreeFunc(leftHashTreeFilePath)
	panicIfError(err)

	rightMap, err := parseHashTreeFunc(rightHashTreeFilePath)
	panicIfError(err)

	for key, leftItems := range leftMap {
		delete(leftMap, key)
		rightItems := rightMap[key]
		toLeft, toRight := leftItems.Diff(rightItems)
		for _, item := range toLeft {
			printItem("<", item)
		}
		for _, item := range toRight {
			printItem(">", item)
		}
	}
}
