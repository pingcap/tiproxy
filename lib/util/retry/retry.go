// Copyright 2023 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package retry

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

const (
	InfiniteCnt = 0
)

func NewBackOff(ctx context.Context, retryInterval time.Duration, retryCnt uint64) backoff.BackOff {
	var bo backoff.BackOff
	bo = backoff.NewConstantBackOff(retryInterval)
	if ctx != nil {
		bo = backoff.WithContext(bo, ctx)
	}
	if retryCnt != InfiniteCnt {
		bo = backoff.WithMaxRetries(bo, retryCnt)
	}
	return bo
}

func Retry(o backoff.Operation, ctx context.Context, retryInterval time.Duration, retryCnt uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	bo := NewBackOff(ctx, retryInterval, retryCnt)
	return backoff.Retry(o, bo)
}

func RetryNotify(o backoff.Operation, ctx context.Context, retryInterval time.Duration, retryCnt uint64,
	notify backoff.Notify, notifyInterval uint64) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	bo := NewBackOff(ctx, retryInterval, retryCnt)
	var cnt uint64
	return backoff.RetryNotify(o, bo, func(err error, duration time.Duration) {
		if cnt%notifyInterval == 0 {
			notify(err, duration)
		}
		cnt++
	})
}
