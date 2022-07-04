// Copyright 2020 Ipalfish, Inc.
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

package rate_limit

import (
	"errors"
	. "github.com/djshow832/weir/pkg/util/rate_limit_breaker"
	"sync"
	"sync/atomic"
)

var ErrRateLimited error = errors.New("rate limited")

// 基于滑动窗口的，并发安全的限流器。
type SlidingWindowRateLimiter struct {
	sw           *SlidingWindow // guarded by mu
	mu           *sync.Mutex    // guard sw
	qpsThreshold int64          // read/write through atomic operation
}

func NewSlidingWindowRateLimiter(qpsThreshold int64) *SlidingWindowRateLimiter {
	swrl := &SlidingWindowRateLimiter{
		// 滑动窗口覆盖 1s 时间，划分为 10 个 cell，每个 cell 时长为 100ms。
		sw:           NewSlidingWindow(10, 100),
		mu:           &sync.Mutex{},
		qpsThreshold: qpsThreshold,
	}
	return swrl
}

// 如果被限流，则返回 ErrRateLimited；未被限流，则返回 nil
func (swrl *SlidingWindowRateLimiter) Limit() error {
	nowMs := GetNowMs()
	qpsThreshold := atomic.LoadInt64(&swrl.qpsThreshold)

	swrl.mu.Lock()
	defer swrl.mu.Unlock()

	const HitMetric = "hit"
	hits := swrl.sw.GetHit(nowMs, HitMetric)
	actualDurationMs := swrl.sw.GetActualDurationMs(nowMs)
	// actualQPS = hits / (actualDurationMs / 1000)
	// actualQPS >= qpsThreshold  改写即得下述表达式。
	if hits*1000 >= qpsThreshold*actualDurationMs {
		return ErrRateLimited
	} else {
		swrl.sw.Hit(nowMs, HitMetric)
		return nil
	}
}

func (swrl *SlidingWindowRateLimiter) ChangeQpsThreshold(newQpsThreshold int64) {
	atomic.StoreInt64(&swrl.qpsThreshold, newQpsThreshold)
}
