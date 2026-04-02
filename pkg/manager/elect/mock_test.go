// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package elect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/pingcap/tiproxy/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	eventTypeElected = iota
	eventTypeRetired
)

var _ Member = (*mockMember)(nil)

type mockMember struct {
	ch            chan int
	hangElectedMu sync.Mutex
	hangElectedCh chan struct{}
}

func newMockMember() *mockMember {
	return &mockMember{ch: make(chan int, 32)}
}

func (mo *mockMember) OnElected() {
	if ch := mo.getHangElectedCh(); ch != nil {
		<-ch
	}
	mo.ch <- eventTypeElected
}

func (mo *mockMember) OnRetired() {
	mo.ch <- eventTypeRetired
}

func (mo *mockMember) expectEvent(t *testing.T, expected ...int) {
	for _, exp := range expected {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		case event := <-mo.ch:
			require.Equal(t, exp, event)
		}
	}
}

func (mo *mockMember) hang(hang bool) {
	mo.hangElectedMu.Lock()
	defer mo.hangElectedMu.Unlock()

	if hang {
		if mo.hangElectedCh == nil {
			mo.hangElectedCh = make(chan struct{})
		}
		return
	}
	if mo.hangElectedCh != nil {
		close(mo.hangElectedCh)
		mo.hangElectedCh = nil
	}
}

func (mo *mockMember) getHangElectedCh() chan struct{} {
	mo.hangElectedMu.Lock()
	defer mo.hangElectedMu.Unlock()
	return mo.hangElectedCh
}

type etcdTestSuite struct {
	elecCfg ElectionConfig
	key     string
	elecs   []*election
	t       *testing.T
	lg      *zap.Logger
	server  *embed.Etcd
	client  *clientv3.Client
	kv      clientv3.KV
}

func newEtcdTestSuite(t *testing.T, key string) *etcdTestSuite {
	lg, _ := logger.CreateLoggerForTest(t)
	ts := &etcdTestSuite{
		t:       t,
		lg:      lg,
		elecCfg: electionConfigForTest(1),
		key:     key,
	}

	ts.startServer("0.0.0.0:0")
	endpoint := ts.server.Clients[0].Addr().String()
	cfg := etcd.ConfigForEtcdTest(endpoint)

	certMgr := cert.NewCertManager()
	err := certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)

	ts.client, err = etcd.InitEtcdClient(ts.lg, cfg, certMgr)
	require.NoError(t, err)
	ts.kv = clientv3.NewKV(ts.client)
	return ts
}

func (ts *etcdTestSuite) newElection(id string) *election {
	member := newMockMember()
	elec := NewElection(ts.lg, ts.client, ts.elecCfg, id, ts.key, member)
	ts.elecs = append(ts.elecs, elec)
	return elec
}

func (ts *etcdTestSuite) getElection(id string) *election {
	for _, elec := range ts.elecs {
		if elec.id == id {
			return elec
		}
	}
	ts.t.Fatalf("election not found, id %s", id)
	return nil
}

func (ts *etcdTestSuite) getOwnerID() string {
	var ownerID string
	for _, elec := range ts.elecs {
		var id string
		require.Eventually(ts.t, func() bool {
			var err error
			id, err = elec.GetOwnerID(context.Background())
			return err == nil
		}, 3*time.Second, 10*time.Millisecond)
		require.NotEmpty(ts.t, id)
		if len(ownerID) == 0 {
			ownerID = id
		} else {
			require.Equal(ts.t, ownerID, id)
		}
	}
	return ownerID
}

func (ts *etcdTestSuite) expectEvent(id string, event ...int) {
	elec := ts.getElection(id)
	elec.member.(*mockMember).expectEvent(ts.t, event...)
}

func (ts *etcdTestSuite) hang(id string, hang bool) {
	elec := ts.getElection(id)
	elec.member.(*mockMember).hang(hang)
}

func (ts *etcdTestSuite) close() {
	for _, elec := range ts.elecs {
		elec.Close()
	}
	if ts.client != nil {
		require.NoError(ts.t, ts.client.Close())
		ts.client = nil
	}
	if ts.server != nil {
		ts.server.Close()
		ts.server = nil
	}
}

func (ts *etcdTestSuite) startServer(addr string) {
	etcd, err := etcd.CreateEtcdServer(addr, ts.t.TempDir(), ts.lg)
	require.NoError(ts.t, err)
	ts.server = etcd
}

func (ts *etcdTestSuite) shutdownServer() string {
	require.NotNil(ts.t, ts.server)
	addr := ts.server.Clients[0].Addr().String()
	ts.server.Close()
	ts.server = nil
	return addr
}

func electionConfigForTest(ttl int) ElectionConfig {
	return ElectionConfig{
		SessionTTL: ttl,
		Timeout:    100 * time.Millisecond,
		RetryIntvl: 10 * time.Millisecond,
		RetryCnt:   2,
	}
}
