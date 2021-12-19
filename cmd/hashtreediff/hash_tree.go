package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/xaionaro-go/slowsync"
)

type HashTree map[string]slowsync.HashTreeItems

func parseHashTreeByPath(filePath string) (HashTree, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open '%s': %w", filePath, err)
	}
	defer f.Close()

	result := make(HashTree)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "hash\t") {
			continue
		}
		item, err := slowsync.ParseHashTreeItem(line)
		if err != nil {
			return nil, fmt.Errorf("unable to parse line '%s': %w", line, err)
		}
		result[item.Path] = append(result[item.Path], item)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan '%s': %w", filePath, err)
	}
	return result, nil
}

func parseHashTreeByDigest(filePath string) (HashTree, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open '%s': %w", filePath, err)
	}
	defer f.Close()

	result := make(HashTree)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "hash\t") {
			continue
		}
		item, err := slowsync.ParseHashTreeItem(line)
		if err != nil {
			return nil, fmt.Errorf("unable to parse line '%s': %w", line, err)
		}
		result[string(item.Digest)] = append(result[string(item.Digest)], item)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan '%s': %w", filePath, err)
	}
	return result, nil
}
