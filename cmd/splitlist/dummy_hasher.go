package main

import (
	"bytes"
	"hash"
)

var _ hash.Hash = &dummyHasher{}

type dummyHasher struct {
	bytes.Buffer
}

func newDummyHasher() *dummyHasher {
	return &dummyHasher{}
}

func (dummyHasher) BlockSize() int {
	panic("cannot use BlockSize in dummyHasher")
}

func (h *dummyHasher) Size() int {
	return h.Buffer.Len()
}

func (h *dummyHasher) Sum(salt []byte) []byte {
	if salt != nil {
		panic("not implemented, yet")
	}
	return h.Bytes()
}
