// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import _ "unsafe"

//go:linkname Uint32N runtime.fastrandn
func Uint32N(a uint64) uint64

// Buf generates a random string using ASCII characters but avoid separator character.
// Ref https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435.
func GenerateSalt(size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(Uint32N(127))
		for buf[i] == 0 || buf[i] == byte('$') {
			buf[i] = byte(Uint32N(127))
		}
	}
	return buf
}
