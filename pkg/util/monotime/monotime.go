// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package monotime

import (
	"time"
	_ "unsafe"
)

//go:noescape
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// Time is a monotonic clock time which is used to calculate duration.
// It's 2x faster than time.Time.
type Time int64

func Now() Time {
	return Time(nanotime())
}

func Since(t Time) time.Duration {
	return time.Duration(Time(nanotime()) - t)
}

func (t Time) Add(d time.Duration) Time {
	return t + Time(d)
}

func (t Time) Sub(d time.Duration) Time {
	return t - Time(d)
}

func (t Time) Before(u Time) bool {
	return t < u
}

func (t Time) After(u Time) bool {
	return t > u
}
