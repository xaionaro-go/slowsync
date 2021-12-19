package main

import (
	"flag"
	"os"
	"strconv"
)

var _ flag.Value = (*fileModeVar)(nil)

type fileModeVar os.FileMode

func (v *fileModeVar) Set(in string) error {
	newValue, err := strconv.ParseUint(in, 8, 64)
	if err != nil {
		return err
	}
	*v = (fileModeVar)(newValue)
	return nil
}

func (v fileModeVar) String() string {
	return strconv.FormatUint(uint64(v), 8)
}
