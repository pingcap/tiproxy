// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import _ "unsafe"

//go:linkname Uint32N runtime.fastrandn
func Uint32N(a uint64) uint64

// Buf generates a random string using ASCII characters but avoid separator character.
// Ref https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435.
func GenerateSalt(buf *[20]byte) {
	for i := range buf {
		buf[i] = byte(Uint32N(127))
		for buf[i] == 0 || buf[i] == byte('$') {
			buf[i] = byte(Uint32N(127))
		}
	}
}
