// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"crypto/rand"

	"github.com/pingcap/tiproxy/lib/util/errors"
)

// Buf generates a random string using ASCII characters but avoid separator character.
// Ref https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435.
func GenerateSalt(buf *[20]byte) error {
	n, err := rand.Read(buf[:])
	if err != nil {
		return errors.Wrapf(errors.WithStack(err), "failed to generate salt")
	}
	if n < len(buf) {
		return errors.Errorf("failed to generate salt, len %d", n)
	}
	for i := range buf {
		buf[i] &= 0x7f
		if buf[i] == 0 || buf[i] == '$' {
			buf[i] += 1
		}
	}
	return nil
	// for i := range buf {
	// 	b := big.NewInt(int64(127))
	// 	n, err := rand.Int(rand.Reader, b)
	// 	for err != nil || n.Uint64() == 0 || n.Uint64() == uint64('$') {
	// 		n, err = rand.Int(rand.Reader, b)
	// 	}
	// 	buf[i] = byte(n.Uint64())
	// }
}
