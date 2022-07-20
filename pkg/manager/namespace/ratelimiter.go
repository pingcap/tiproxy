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

package namespace

import (
	"context"
	"sync"

	"github.com/pingcap/TiProxy/pkg/util/rate_limit_breaker/rate_limit"
)

type NamespaceRateLimiter struct {
	limiters     *sync.Map
	qpsThreshold int
	scope        string
}

func NewNamespaceRateLimiter(scope string, qpsThreshold int) *NamespaceRateLimiter {
	return &NamespaceRateLimiter{
		limiters:     &sync.Map{},
		scope:        scope,
		qpsThreshold: qpsThreshold,
	}
}

func (n *NamespaceRateLimiter) Scope() string {
	return n.scope
}

func (n *NamespaceRateLimiter) Limit(ctx context.Context, key string) error {
	if n.qpsThreshold <= 0 {
		return nil
	}
	limiter, _ := n.limiters.LoadOrStore(key, rate_limit.NewSlidingWindowRateLimiter(int64(n.qpsThreshold)))
	return limiter.(*rate_limit.SlidingWindowRateLimiter).Limit()
}
