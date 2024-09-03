// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package replay

type Exception interface {
	Critical() bool
	String() string
}

type otherException struct {
	err error
}

func (he otherException) Critical() bool {
	return true
}

func (he otherException) String() string {
	return he.err.Error()
}
