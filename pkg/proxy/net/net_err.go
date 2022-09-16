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

package net

import (
	"io"
	"syscall"

	"github.com/pingcap/TiProxy/lib/util/errors"
)

// IsDisconnectError returns whether the error is caused by peer disconnection.
func IsDisconnectError(err error) bool {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, syscall.EPIPE), errors.Is(err, syscall.ECONNRESET):
		return true
	}
	return false
}
