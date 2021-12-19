package main

import (
	"fmt"

	"github.com/xaionaro-go/slowsync"
)

func printItem(prefix string, item *slowsync.HashTreeItem) {
	fmt.Printf("%s\t%s\n", prefix, item.String())
}
