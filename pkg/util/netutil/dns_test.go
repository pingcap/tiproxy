// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package netutil

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestDNSDialerUsesConfiguredNameServerAndCache(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	dns := testkit.StartDNSServer(t, map[string][]string{
		"tidb.test": {"127.0.0.1"},
	})

	accepted := make(chan error, 2)
	for range 2 {
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				accepted <- err
				return
			}
			accepted <- conn.Close()
		}()
	}

	dialer := NewDNSDialer([]string{dns.Addr()})
	dialer.dialer.Timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("tidb.test", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	queryCount := dns.QueryCount("tidb.test")
	require.Greater(t, queryCount, 0)

	conn, err = dialer.DialContext(ctx, "tcp", net.JoinHostPort("tidb.test", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.Equal(t, queryCount, dns.QueryCount("tidb.test"))
	require.NoError(t, <-accepted)
	require.NoError(t, <-accepted)
}

func TestDNSDialerFallbackToSystemResolver(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	accepted := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			accepted <- err
			return
		}
		accepted <- conn.Close()
	}()

	dialer := NewDNSDialer(nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("localhost", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-accepted)
}

func TestDNSDialerTriesAllResolvedIPs(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	dns := testkit.StartDNSServer(t, map[string][]string{
		"tidb.test": {"127.0.0.2", "127.0.0.1"},
	})

	accepted := make(chan struct{}, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		if err := conn.Close(); err != nil {
			acceptErr <- err
			return
		}
		accepted <- struct{}{}
	}()

	dialer := NewDNSDialer([]string{dns.Addr()})
	dialer.dialer.Timeout = 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort("tidb.test", strconv.Itoa(int(port))))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("listener was not reached through resolved fallback IP")
	}
	select {
	case err := <-acceptErr:
		require.NoError(t, err)
	default:
	}
}

func TestDNSDialerCoalescesConcurrentLookupsAfterCacheExpiry(t *testing.T) {
	listener, addr := testkit.StartListener(t, "127.0.0.1:0")
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, port := testkit.ParseHostPort(t, addr)
	dns := testkit.StartDNSServer(t, map[string][]string{
		"tidb.test": {"127.0.0.1"},
	})

	dialer := NewDNSDialer([]string{dns.Addr()})
	dialer.cacheTTL = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	accepted := make(chan error, 9)
	for range cap(accepted) {
		go func() {
			conn, err := listener.Accept()
			if err == nil {
				err = conn.Close()
			}
			accepted <- err
		}()
	}

	targetAddr := net.JoinHostPort("tidb.test", strconv.Itoa(int(port)))
	conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	require.NoError(t, <-accepted)
	initialQueries := dns.QueryCount("tidb.test")
	require.Greater(t, initialQueries, 0)

	time.Sleep(dialer.cacheTTL + 10*time.Millisecond)

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for range cap(errCh) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
			if err == nil {
				err = conn.Close()
			}
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	for range cap(errCh) {
		require.NoError(t, <-accepted)
	}
	require.Equal(t, initialQueries*2, dns.QueryCount("tidb.test"))
}
