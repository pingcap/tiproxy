// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package netutil

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const defaultDNSCacheTTL = 5 * time.Second

type dnsCacheEntry struct {
	ips      []net.IP
	deadline time.Time
}

// DNSDialer routes DNS lookups to configured name servers and caches lookup results briefly.
// If no name servers are configured, it falls back to the system resolver and dialer.
type DNSDialer struct {
	cacheTTL   time.Duration
	nameServer []string
	resolver   *net.Resolver
	dialer     net.Dialer
	nextServer atomic.Uint64
	mu         struct {
		sync.Mutex
		cacheMap map[string]dnsCacheEntry
	}
}

func NewDNSDialer(nameServers []string) *DNSDialer {
	d := &DNSDialer{
		cacheTTL:   defaultDNSCacheTTL,
		nameServer: append([]string(nil), nameServers...),
		mu: struct {
			sync.Mutex
			cacheMap map[string]dnsCacheEntry
		}{
			cacheMap: make(map[string]dnsCacheEntry),
		},
	}
	if len(nameServers) == 0 {
		return d
	}
	d.resolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			server := d.nameServer[int(d.nextServer.Add(1)-1)%len(d.nameServer)]
			return d.dialer.DialContext(ctx, network, server)
		},
	}
	return d
}

func (d *DNSDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil || d.resolver == nil {
		return d.dialer.DialContext(ctx, network, addr)
	}
	ips, err := d.lookupNetIP(ctx, host)
	if err != nil {
		return nil, err
	}
	var dialErr error
	for _, ip := range ips {
		conn, err := d.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		dialErr = err
	}
	return nil, dialErr
}

func (d *DNSDialer) lookupNetIP(ctx context.Context, host string) ([]net.IP, error) {
	key := strings.TrimSuffix(strings.ToLower(host), ".")
	now := time.Now()
	d.mu.Lock()
	if entry, ok := d.mu.cacheMap[key]; ok && now.Before(entry.deadline) {
		ips := entry.ips
		d.mu.Unlock()
		return ips, nil
	}
	d.mu.Unlock()

	ips, err := d.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	ipList := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		ipList = append(ipList, append(net.IP(nil), ip.AsSlice()...))
	}
	d.mu.Lock()
	d.mu.cacheMap[key] = dnsCacheEntry{
		ips:      ipList,
		deadline: now.Add(d.cacheTTL),
	}
	d.mu.Unlock()
	return ipList, nil
}
