// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package backendcluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiproxy/lib/util/errors"
	"github.com/stretchr/testify/require"
)

func TestNetworkRouterDialContextRejectsMissingCluster(t *testing.T) {
	router := NewNetworkRouter(&Manager{}, nilClusterTLS)
	_, err := router.DialContext(context.Background(), "tcp", "127.0.0.1:80", "missing")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBackendClusterNotFound))
}

func TestNetworkRouterHTTPClientRejectsMissingCluster(t *testing.T) {
	router := NewNetworkRouter(&Manager{}, nilClusterTLS)
	b := backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond), 0)
	_, err := router.HTTPClient("missing").Get("127.0.0.1:80", "/status", b, time.Second)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrBackendClusterNotFound))
}

func TestNetworkRouterDialContextFallsBackWithoutClusterName(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ln.Close())
	})

	accepted := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- struct{}{}
			_ = conn.Close()
		}
	}()

	router := NewNetworkRouter(&Manager{}, nilClusterTLS)
	conn, err := router.DialContext(context.Background(), "tcp", ln.Addr().String(), "")
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener was not reached through default dialer")
	}
}
