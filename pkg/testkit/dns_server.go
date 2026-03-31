// Copyright 2026 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/tiproxy/lib/util/waitgroup"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

type DNSServer struct {
	conn    *net.UDPConn
	records map[string][]net.IP
	mu      sync.Mutex
	queries map[string]int
	wg      waitgroup.WaitGroup
}

func StartDNSServer(t *testing.T, records map[string][]string) *DNSServer {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)

	server := &DNSServer{
		conn:    conn,
		records: make(map[string][]net.IP, len(records)),
		queries: make(map[string]int),
	}
	for name, ips := range records {
		key := normalizeDNSName(name)
		server.records[key] = make([]net.IP, 0, len(ips))
		for _, ip := range ips {
			server.records[key] = append(server.records[key], net.ParseIP(ip))
		}
	}
	server.wg.Run(func() {
		server.serve()
	})
	t.Cleanup(func() {
		require.NoError(t, server.Close())
	})
	return server
}

func (s *DNSServer) Addr() string {
	return s.conn.LocalAddr().String()
}

func (s *DNSServer) QueryCount(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queries[normalizeDNSName(name)]
}

func (s *DNSServer) Close() error {
	if s.conn != nil {
		err := s.conn.Close()
		s.wg.Wait()
		return err
	}
	return nil
}

func (s *DNSServer) serve() {
	buf := make([]byte, 1500)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		resp, err := s.handleQuery(buf[:n])
		if err != nil {
			continue
		}
		_, _ = s.conn.WriteToUDP(resp, addr)
	}
}

func (s *DNSServer) handleQuery(pkt []byte) ([]byte, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(pkt)
	if err != nil {
		return nil, err
	}
	question, err := parser.Question()
	if err != nil {
		return nil, err
	}
	name := normalizeDNSName(question.Name.String())
	s.mu.Lock()
	s.queries[name]++
	s.mu.Unlock()

	respHeader := dnsmessage.Header{
		ID:                 header.ID,
		Response:           true,
		RecursionAvailable: true,
	}
	builder := dnsmessage.NewBuilder(nil, respHeader)
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	if err := builder.Question(question); err != nil {
		return nil, err
	}
	if err := builder.StartAnswers(); err != nil {
		return nil, err
	}
	for _, ip := range s.records[name] {
		if ipv4 := ip.To4(); ipv4 != nil && question.Type == dnsmessage.TypeA {
			resource := dnsmessage.Resource{
				Header: dnsmessage.ResourceHeader{
					Name:  question.Name,
					Type:  dnsmessage.TypeA,
					Class: dnsmessage.ClassINET,
					TTL:   60,
				},
				Body: &dnsmessage.AResource{A: [4]byte(ipv4)},
			}
			if err := builder.AResource(resource.Header, *resource.Body.(*dnsmessage.AResource)); err != nil {
				return nil, err
			}
		}
	}
	return builder.Finish()
}

func normalizeDNSName(name string) string {
	return strings.TrimSuffix(strings.ToLower(name), ".")
}
