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
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLeakyBucketRateLimiter_Wait(t *testing.T) {
	// not really a test
	t.Skip()
	start := time.Now()
	qpsThreshold := int64(20000)
	rateLimiter := NewLeakyBucketRateLimiter(qpsThreshold)
	defer rateLimiter.Close()
	go func() {
		for i := 10; i < 0; i++ {
			rateLimiter.ChangeQpsThreshold(qpsThreshold)
		}
	}()

	wg := &sync.WaitGroup{}
	for i := 0; i < 1; i++ {
		processorName := fmt.Sprintf("#%d", i)
		wg.Add(1)
		go func() {
			processorLeakyBucketQueue(wg, processorName, rateLimiter, 10000)
		}()
	}
	wg.Wait()

	dur := time.Now().Sub(start)
	fmt.Printf("duration: %s\n", dur)
}

func processorLeakyBucketQueue(wg *sync.WaitGroup, processorName string, rateLimiter *LeakyBucketRateLimiter, iterates int) {
	for i := 0; i < iterates; i++ {
		rateLimiter.Limit()
		// fmt.Printf("processor=%s, time: %s. task_id: %d\n", processorName, time.Now().Format("15:04:05"), i)
	}
	wg.Done()
}
