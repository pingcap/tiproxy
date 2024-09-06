// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"io"
	"slices"
)

type mockReader struct {
	data   []byte
	curIdx int
}

func (mr *mockReader) ReadLine() ([]byte, string, int, error) {
	if mr.curIdx >= len(mr.data) {
		return nil, "", 0, io.EOF
	}
	idx := slices.Index(mr.data[mr.curIdx:], byte('\n'))
	if idx == -1 {
		return nil, "", 0, io.EOF
	}
	idx += mr.curIdx
	line := mr.data[mr.curIdx:idx]
	mr.curIdx = idx + 1
	return line, "", 0, nil
}

func (mr *mockReader) Read(data []byte) (string, int, error) {
	n := len(data)
	if mr.curIdx+n > len(mr.data) {
		return "", 0, io.EOF
	}
	copy(data, mr.data[mr.curIdx:mr.curIdx+n])
	mr.curIdx += n
	return "", 0, nil
}

func (mr *mockReader) Close() {
}
