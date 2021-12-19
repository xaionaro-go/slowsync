package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/hashicorp/go-multierror"
)

func parseDigetsFile(digestsFilePath string) (map[string][]byte, error) {
	var itemsErr error

	f, err := os.Open(digestsFilePath)
	if err != nil {
		return nil, fmt.Errorf("unable to open '%s': %w", digestsFilePath, err)
	}
	defer f.Close()

	result := make(map[string][]byte)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		digest, err := hex.DecodeString(parts[0])
		if err != nil {
			itemsErr = multierror.Append(itemsErr, fmt.Errorf("unable to unhex digest '%s': %w", parts[1], err))
		}
		filePath := parts[1]
		result[filePath] = digest
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan '%s': %w", digestsFilePath, err)
	}
	return result, itemsErr
}
