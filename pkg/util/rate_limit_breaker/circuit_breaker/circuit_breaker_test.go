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

package circuit_breaker

import (
	"context"
	"errors"
	"testing"

	rateLimitBreaker "github.com/pingcap/TiProxy/pkg/util/rate_limit_breaker"
	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_Do_NoError(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		failureNum:           5,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
		size:                 10,
		cellIntervalMs:       1000,
	})

	successCount := 0
	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			successCount++
			return nil
		}, func(ctx context.Context, err error) error {
			errCount++
			return err
		})
	}
	assert.Equal(t, successCount, 1000)
	assert.Equal(t, errCount, 0)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)
}

func TestCircuitBreaker_Do_AlwaysError(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})

	successCount := 0
	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			successCount++
			return errors.New("just_error")
		}, func(ctx context.Context, err error) error {
			errCount++
			return err
		})
	}

	assert.True(t, successCount < 1000)
	assert.Equal(t, errCount, 1000)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)
}

func TestCircuitBreaker_ForceOpen(t *testing.T) {
	ctx := context.Background()
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            true,
	})

	errCount := 0
	for i := 0; i < 1000; i++ {
		cb.Do(ctx, func(ctx context.Context) error {
			return nil
		}, func(ctx context.Context, err error) error {
			errCount++
			return nil
		})
	}
	assert.Equal(t, errCount, 1000)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusForceOpen)
}

func TestCircuitBreaker_ChangeConfig_WithForceOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            true,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusForceOpen)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusForceOpen)

	// cancel forceOpen, status goes back to closed
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000,
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusClosed)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusClosed)
}

func TestCircuitBreaker_ChangeConfig_WithoutForceOpen(t *testing.T) {
	cb := NewCircuitBreaker(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})

	// 当前为 open 状态，然后修改配置(没有强制开启)，检查仍为 open 状态
	cb.status = CircuitBreakerStatusOpen
	cb.openStartMs = rateLimitBreaker.GetNowMs()
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 100000, // 100s
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusOpen)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusOpen)

	// 当前为 closed 状态，然后修改配置(没有强制开启)，检查仍为 closed 状态
	cb.status = CircuitBreakerStatusClosed
	cb.ChangeConfig(&CircuitBreakerConfig{
		minQPS:               10,
		failureRateThreshold: 10,
		OpenStatusDurationMs: 10000, // 10s
		forceOpen:            false,
	})
	assert.Equal(t, cb.status, CircuitBreakerStatusClosed)
	assert.Equal(t, cb.Status(), CircuitBreakerStatusClosed)
}
