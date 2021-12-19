package slowsync

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"
)

type HashTreeItem struct {
	Path       string
	Digest     []byte
	Size       uint64
	ModifyTime time.Time
	ChangeTime time.Time
	AccessTime time.Time
	Error      error
}

func unixTimeParse(s string) (time.Time, error) {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("unable to parse int64 '%s': %w", s, err)
	}
	return time.Unix(ts/time.Second.Nanoseconds(), ts%time.Second.Nanoseconds()), nil
}

func ParseHashTreeItem(s string) (*HashTreeItem, error) {
	parts := strings.SplitN(s, "\t", 7)
	if parts[0] != "hash" {
		return nil, fmt.Errorf("is not a hash line")
	}
	digest, err := hex.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("unable to unhex digest '%s': %w", parts[1], err)
	}
	modifyTime, err := unixTimeParse(parts[2])
	if err != nil {
		return nil, fmt.Errorf("unable to parse unixtime '%s': %w", parts[2], err)
	}
	changeTime, err := unixTimeParse(parts[3])
	if err != nil {
		return nil, fmt.Errorf("unable to parse unixtime '%s': %w", parts[3], err)
	}
	accessTime, err := unixTimeParse(parts[4])
	if err != nil {
		return nil, fmt.Errorf("unable to parse unixtime '%s': %w", parts[4], err)
	}
	size, err := strconv.ParseUint(parts[5], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to parse uint64 '%s': %w", parts[5], err)
	}
	filePath := parts[6]

	return &HashTreeItem{
		Path:       path.Clean(filePath),
		Digest:     digest,
		Size:       size,
		ModifyTime: modifyTime,
		ChangeTime: changeTime,
		AccessTime: accessTime,
	}, nil
}

func (item *HashTreeItem) Equal(cmp *HashTreeItem) bool {
	if item == nil && cmp == nil {
		return true
	}
	if item == nil || cmp == nil {
		return false
	}
	if item.Path != cmp.Path {
		return false
	}
	if bytes.Equal(item.Digest, cmp.Digest) {
		return false
	}
	if item.Size != cmp.Size {
		return false
	}
	if item.ModifyTime.Equal(cmp.ModifyTime) {
		return false
	}
	if item.ChangeTime.Equal(cmp.ChangeTime) {
		return false
	}
	if item.AccessTime.Equal(cmp.AccessTime) {
		return false
	}
	if (item.Error == nil) != (cmp.Error == nil) {
		return false
	}
	if item.Error != nil && (item.Error.Error() != cmp.Error.Error()) {
		return false
	}

	return true
}

func (item *HashTreeItem) String() string {
	if item.Error != nil {
		return fmt.Sprintf("error\t%s\t%s", item.Error.Error(), item.Path)
	}
	return fmt.Sprintf("hash\t%X\t%d\t%d\t%d\t%d\t%s",
		item.Digest,
		item.ModifyTime.UnixNano(), item.ChangeTime.UnixNano(), item.AccessTime.UnixNano(),
		item.Size, path.Clean(item.Path))
}

type HashTreeItems []*HashTreeItem

func (s HashTreeItems) Diff(cmp HashTreeItems) (toLeft, toRight HashTreeItems) {
	leftMap := map[string]*HashTreeItem{}
	rightMap := map[string]*HashTreeItem{}
	for _, item := range s {
		leftMap[item.String()] = item
	}
	for _, item := range cmp {
		rightMap[item.String()] = item
	}

	for key, item := range leftMap {
		if _, ok := rightMap[key]; ok {
			continue
		}
		toRight = append(toRight, item)
	}

	for key, item := range rightMap {
		if _, ok := leftMap[key]; ok {
			continue
		}
		toLeft = append(toLeft, item)
	}

	return
}
