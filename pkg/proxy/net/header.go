// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package net

type Header byte

// Header information.
const (
	OKHeader          Header = 0x00
	ErrHeader         Header = 0xff
	EOFHeader         Header = 0xfe
	AuthSwitchHeader  Header = 0xfe
	LocalInFileHeader Header = 0xfb
)

var headerStrings = map[Header]string{
	OKHeader:          "OK",
	ErrHeader:         "ERR",
	EOFHeader:         "EOF/AuthSwitch",
	LocalInFileHeader: "LOCAL_IN_FILE",
}

func (f Header) Byte() byte {
	return byte(f)
}

func (f Header) String() string {
	return headerStrings[f]
}
