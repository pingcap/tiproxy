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

func (mr *mockReader) ReadLine() ([]byte, error) {
	if mr.curIdx >= len(mr.data) {
		return nil, io.EOF
	}
	idx := slices.Index(mr.data[mr.curIdx:], byte('\n'))
	if idx == -1 {
		return nil, io.EOF
	}
	idx += mr.curIdx
	line := mr.data[mr.curIdx:idx]
	mr.curIdx = idx + 1
	return line, nil
}

func (mr *mockReader) Read(n int) ([]byte, error) {
	if mr.curIdx+n > len(mr.data) {
		return nil, io.EOF
	}
	line := mr.data[mr.curIdx : mr.curIdx+n]
	mr.curIdx += n
	return line, nil
}
