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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNamespaceRateLimiter_Limit(t *testing.T) {
	ctx := context.Background()
	key1 := "hello"
	key2 := "world"
	rateLimiter := NewNamespaceRateLimiter("namespace", 2)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.Error(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key2))
	time.Sleep(time.Second)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
}

func TestNamespaceRateLimiter_ZeroThreshold(t *testing.T) {
	ctx := context.Background()
	key1 := "hello"
	rateLimiter := NewNamespaceRateLimiter("namespace", 0)
	require.NoError(t, rateLimiter.Limit(ctx, key1))
	require.NoError(t, rateLimiter.Limit(ctx, key1))
}
