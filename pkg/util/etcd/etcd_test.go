// Copyright 2024 PingCAP, Inc.
// SPDX-License-Identifier: Apache-2.0

package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiproxy/lib/util/logger"
	"github.com/pingcap/tiproxy/pkg/manager/cert"
	"github.com/stretchr/testify/require"
)

func TestEtcdClient(t *testing.T) {
	lg, _ := logger.CreateLoggerForTest(t)
	server, err := CreateEtcdServer("0.0.0.0:0", t.TempDir(), lg)
	require.NoError(t, err)
	endpoint := server.Clients[0].Addr().String()

	cfg := ConfigForEtcdTest(endpoint)
	certMgr := cert.NewCertManager()
	err = certMgr.Init(cfg, lg, nil)
	require.NoError(t, err)
	client, err := InitEtcdClient(lg, cfg, certMgr)
	require.NoError(t, err)

	_, err = client.Put(context.Background(), "key", "value")
	require.NoError(t, err)
	kvs, err := GetKVs(context.Background(), client, "key", nil, 3*time.Second, 10*time.Millisecond, 3)
	require.NoError(t, err)
	require.Equal(t, "value", string(kvs[0].Value))

	require.NoError(t, client.Close())
	server.Close()
}
